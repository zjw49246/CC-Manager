from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import Float, and_, delete as sa_delete, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import FunctionElement

from backend.models.instance import Instance
from backend.models.log_entry import LogEntry
from backend.models.task import Task
from backend.models.test_harness import TestHarnessChildBinding
from backend.models.task_ssh_grant import TaskSSHGrant
from backend.models.user import User
from backend.models.worker_task_termination import WorkerTaskTerminationReceipt
from backend.services.process_identity import (
    persisted_process_is_definitively_dead,
)
from backend.services.task_creation import (
    purge_task_access_grants,
    stage_task_record,
)
from backend.services.test_harness_children import (
    CHILD_COMPLETED,
    CHILD_READY,
    CHILD_RUNNING,
    browser_binding_owner_identity,
    browser_child_binding_error,
    browser_child_owner_error,
)
from backend.services.test_harness_owner_fence import (
    TestHarnessOwnerGraphConflict,
    has_active_test_harness_owner_graph,
    lock_test_harness_owner,
    no_active_test_harness_owner_graph_predicate,
)
from backend.services.worker_routing_config import (
    has_pending_worker_routing,
)
from backend.services.worker_task_termination import (
    WorkerTaskTerminationConflict,
    active_worker_task_termination_receipt,
    no_active_worker_task_termination_predicate,
)


PR_REVIEW_SUPERSEDED_METADATA_KEY = "pr_review_superseded"
BASE_DELETABLE_TASK_STATUSES = frozenset(
    {"pending", "failed", "cancelled", "conflict", "completed"}
)
PLAN_DELETABLE_TASK_STATUSES = BASE_DELETABLE_TASK_STATUSES | {
    "plan_review",
    "superseded",
}
TASK_KIND_STANDALONE_PLAN = "standalone_plan"
TASK_KIND_RELATED_PLAN = "related_plan"
TASK_KIND_MAIN = "main"


async def _member_visible_project_ids(db: AsyncSession, user_id: int) -> list[int]:
    """Resolve member Project shares while excluding internal grouping rows."""

    from backend.models.project import Project, project_is_internal
    from backend.models.team_share import TeamProjectShare
    from backend.models.user_group import UserGroupMember

    group_ids = select(UserGroupMember.group_id).where(
        UserGroupMember.user_id == user_id
    )
    shared_ids = list((await db.execute(
        select(TeamProjectShare.project_id)
        .where(
            (
                (TeamProjectShare.target_type == "user")
                & (TeamProjectShare.target_id == user_id)
            )
            | (
                (TeamProjectShare.target_type == "group")
                & TeamProjectShare.target_id.in_(group_ids)
            )
        )
        .distinct()
    )).scalars())
    if not shared_ids:
        return []
    projects = list((await db.execute(
        select(Project).where(Project.id.in_(shared_ids))
    )).scalars())
    return sorted(project.id for project in projects if not project_is_internal(project))


class TaskWaitingCapabilityConflict(RuntimeError):
    """An ordinary Task mutation raced with its durable capability wait."""


async def fence_native_execution_principal(
    db: AsyncSession,
    *,
    user_id: int | None,
    role: str,
    principal_kind: str,
) -> bool:
    """Writer-fence one native user principal in the caller's transaction.

    Worker-side delegated principals deliberately have no local ``User`` row;
    their authority is revalidated by the Manager's generation-bound launch
    permit.  System/token principals likewise do not derive authority from a
    user record.  Only a native ``user`` principal therefore participates in
    this fence.
    """

    if principal_kind != "user":
        return True
    if type(user_id) is not int or user_id <= 0:
        return False
    fenced = await db.execute(
        update(User)
        .where(
            User.id == user_id,
            User.is_active.is_(True),
            User.role == role,
        )
        .values(role=User.role)
    )
    return fenced.rowcount == 1


def _dispatcher_scope_predicate():
    """Require durable Controller admission for Delivery-owned Tasks.

    A Delivery Task is only an execution shell. Ordinary pending Tasks remain
    dispatchable, while a Delivery shell additionally needs one active Turn
    belonging to a coding/running Run. Feature flags gate new
    Run admission, not recovery of work already committed before a restart.
    This is the final queue-level fence for orphans and any API path that
    accidentally writes ``pending`` directly.
    """

    from backend.models.delivery import (
        DELIVERY_TURN_ACTIVE_STATUSES,
        DeliveryRun,
        DeliveryTurn,
    )

    admitted_turn = (
        select(DeliveryTurn.id)
        .join(DeliveryRun, DeliveryRun.id == DeliveryTurn.run_id)
        .where(
            DeliveryTurn.task_id == Task.id,
            DeliveryTurn.run_id == Task.delivery_run_id,
            DeliveryTurn.active_run_id == Task.delivery_run_id,
            DeliveryTurn.status.in_(DELIVERY_TURN_ACTIVE_STATUSES),
            DeliveryRun.id == Task.delivery_run_id,
            DeliveryRun.developer_task_id == Task.id,
            DeliveryRun.phase == "coding",
            DeliveryRun.activity == "running",
        )
        .correlate(Task)
        .exists()
    )
    return or_(
        Task.mode != "delivery_loop",
        and_(
            Task.delivery_run_id.is_not(None),
            Task.delivery_role == "developer",
            admitted_turn,
        ),
    )


def project_ready_dispatch_predicate():
    """Hold Tasks whose local Project checkout is not ready yet.

    Background clones are asynchronous, so a Task may legitimately be created
    while its Project is still ``pending``/``cloning``/``initializing`` — or
    after the clone failed (``error``) and ``local_path`` points at a missing
    directory. Launching such a Task burns its retry budget on a bare
    ``[Errno 2]`` spawn failure. Keep it queued instead: the Project flipping
    to ``ready`` (clone completion or re-clone) makes it dispatchable again
    without touching ``Task.status``.

    Written as "no non-ready Project row exists" so Tasks without a Project
    (manual ``target_repo``) and Tasks whose Project row was deleted keep the
    existing dispatch behavior.
    """

    from backend.models.project import Project

    unready_project = (
        select(Project.id)
        .where(
            Project.id == Task.project_id,
            Project.status != "ready",
        )
        .correlate(Task)
        .exists()
    )
    return or_(Task.project_id.is_(None), ~unready_project)


def _task_kind_predicate(task_kind: str):
    if task_kind == TASK_KIND_STANDALONE_PLAN:
        return and_(Task.mode == "plan", Task.plan_target_task_id.is_(None))
    if task_kind == TASK_KIND_RELATED_PLAN:
        return and_(Task.mode == "plan", Task.plan_target_task_id.is_not(None))
    if task_kind == TASK_KIND_MAIN:
        return Task.mode != "plan"
    raise ValueError(f"Unsupported task kind: {task_kind}")


def ordinary_task_visibility_predicate():
    """Keep workflow-owned execution Tasks out of ordinary task lists.

    Older deployments created reviewer, finding-fix, and rebuttal Tasks as
    unarchived rows, so filtering on ``Task.archived`` alone can leak raw
    Controller protocol into the normal Tasks/Chat UI. Delivery developer
    shells and their pre-PR reviewer Tasks are likewise implementation records
    of the Delivery graph, not independent user Tasks. The stable PR Monitor
    display Task is the only exception: it is a read-only user-facing PR
    result entry while its durable ``PRMonitorRun.display_task_id`` link
    prevents an arbitrary Task from opting into the projection. Durable owner
    links classify both legacy and current workflows without relying on
    titles/tags.
    """

    from backend.models.code_review import CodeReviewRun
    from backend.services.pr_monitor_task_access import (
        pr_monitor_display_task_predicate,
        pr_monitor_owned_task_predicate,
    )

    delivery_developer = Task.__table__.alias("delivery_developer_task")
    delivery_code_review_task = (
        select(CodeReviewRun.id)
        .select_from(
            CodeReviewRun.__table__.join(
                delivery_developer,
                delivery_developer.c.id == CodeReviewRun.developer_task_id,
            )
        )
        .where(
            CodeReviewRun.reviewer_task_id == Task.id,
            or_(
                delivery_developer.c.mode == "delivery_loop",
                delivery_developer.c.delivery_run_id.is_not(None),
            ),
        )
        .correlate(Task)
        .exists()
    )
    return and_(
        Task.mode != "delivery_loop",
        Task.delivery_run_id.is_(None),
        or_(
            ~pr_monitor_owned_task_predicate(Task.id),
            pr_monitor_display_task_predicate(Task.id),
        ),
        ~delivery_code_review_task,
    )


def _ordinary_task_visibility_predicate():
    """Compatibility alias for the former private visibility helper."""

    return ordinary_task_visibility_predicate()


def pr_review_dispatch_predicate():
    """Only dispatch a reviewer Task while its durable owner is runnable.

    The first panel Task is also stored in ``PRReview.task_id`` for legacy
    compatibility.  A panel link must therefore take precedence over the
    single-review link; otherwise cancelling that reviewer run would still
    leave the Task runnable through the parent review's ``reviewing`` state.
    A deletion tombstone has no runnable owner graph and must never fall back
    to ordinary dispatch.  Correlated ``EXISTS`` predicates keep the same
    semantics on SQLite, PostgreSQL, and MySQL and can be repeated in the
    final Task claim CAS.
    """

    from backend.models.pr_monitor import (
        PRMonitorTaskTombstone,
        PRReview,
        PRReviewerRun,
    )

    tombstoned = (
        select(PRMonitorTaskTombstone.task_id)
        .where(PRMonitorTaskTombstone.task_id == Task.id)
        .correlate(Task)
        .exists()
    )
    panel_review = (
        select(PRReviewerRun.id)
        .where(PRReviewerRun.task_id == Task.id)
        .correlate(Task)
        .exists()
    )
    runnable_panel_review = (
        select(PRReviewerRun.id)
        .join(PRReview, PRReview.id == PRReviewerRun.pr_review_id)
        .where(
            PRReviewerRun.task_id == Task.id,
            PRReviewerRun.status.in_(("pending", "reviewing")),
            PRReview.status == "reviewing",
        )
        .correlate(Task)
        .exists()
    )
    single_review = (
        select(PRReview.id)
        .where(PRReview.task_id == Task.id)
        .correlate(Task)
        .exists()
    )
    runnable_single_review = (
        select(PRReview.id)
        .where(
            PRReview.task_id == Task.id,
            PRReview.status == "reviewing",
        )
        .correlate(Task)
        .exists()
    )
    return and_(
        ~tombstoned,
        or_(
            runnable_panel_review,
            and_(
                ~panel_review,
                or_(~single_review, runnable_single_review),
            ),
        ),
    )


def is_task_status_deletable(*, mode: str, status: str) -> bool:
    """Return whether a stopped Task generation may enter safe deletion."""

    statuses = (
        PLAN_DELETABLE_TASK_STATUSES
        if mode == "plan"
        else BASE_DELETABLE_TASK_STATUSES
    )
    return status in statuses


class _UnixTimestamp(FunctionElement):
    """Cross-dialect epoch conversion used by the mixed manual/time sort key."""

    type = Float()
    inherit_cache = True


@compiles(_UnixTimestamp, "sqlite")
def _compile_unix_timestamp_sqlite(element, compiler, **kw):
    value = compiler.process(list(element.clauses)[0], **kw)
    return f"CAST(strftime('%s', {value}) AS FLOAT)"


@compiles(_UnixTimestamp, "postgresql")
def _compile_unix_timestamp_postgresql(element, compiler, **kw):
    value = compiler.process(list(element.clauses)[0], **kw)
    return f"EXTRACT(EPOCH FROM {value})"


@compiles(_UnixTimestamp, "mysql")
def _compile_unix_timestamp_mysql(element, compiler, **kw):
    value = compiler.process(list(element.clauses)[0], **kw)
    return f"UNIX_TIMESTAMP({value})"


@compiles(_UnixTimestamp)
def _compile_unix_timestamp_default(element, compiler, **kw):
    value = compiler.process(list(element.clauses)[0], **kw)
    return f"EXTRACT(EPOCH FROM {value})"


def task_retry_not_superseded_predicate():
    """Return the cross-dialect SQL gate for retrying a PR review Task.

    PR synchronize persists this boolean marker in the same transaction as
    terminal status and the exact owner snapshot. Keeping the check in the
    retry UPDATE itself is essential: a request may have read the old terminal
    row before synchronize acquired the row lock, then resume only after the
    replacement review committed.
    """

    return (
        Task.metadata_[PR_REVIEW_SUPERSEDED_METADATA_KEY]
        .as_boolean()
        .is_not(True)
    )


def task_is_pr_review_superseded(task: Task | None) -> bool:
    return bool(
        task is not None
        and (task.metadata_ or {}).get(
            PR_REVIEW_SUPERSEDED_METADATA_KEY
        )
        is True
    )


TaskGenerationFence = tuple[
    int,
    int | None,
    datetime | None,
    datetime | None,
    str | None,
    int,
]

TaskDeleteFence = tuple[
    str,
    int | None,
    int,
    int | None,
    datetime | None,
    datetime | None,
    str | None,
    int,
]


@dataclass(frozen=True, slots=True)
class TaskDeletePreflight:
    """Exact target-owned Plan identity locked for one Task deletion."""

    task_id: int
    plan_ids: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class TestHarnessDeleteGraph:
    """Locked terminal Harness ownership graph for one Task deletion."""

    run_ids: tuple[str, ...]
    workspace_run_ids: tuple[str, ...]
    binding_ids: tuple[str, ...]
    evidence_storage_keys: tuple[str, ...]
    child_tasks: tuple[tuple[int, str, str, int, int], ...]
    child_instances: tuple[
        tuple[int, int, str, int | None, str | None, datetime | None], ...
    ]


async def _lock_test_harness_delete_graph(
    db: AsyncSession,
    task_id: int,
    task_incarnation_id: str | None,
) -> TestHarnessDeleteGraph | None:
    """Lock and prove that no live Browser child can outlive its owner."""

    from backend.models.test_harness import (
        BrowserReviewOperationReceipt,
        TestHarnessChildBinding,
        TestHarnessEvidence,
        TestHarnessRun,
        TestHarnessSandboxLease,
    )
    from backend.models.workspace_review import WorkspaceReviewRun
    from backend.services.test_harness_children import CHILD_TERMINAL_STATES
    from backend.services.test_harness_contracts import HARNESS_TERMINAL_STATUSES

    runs = list(
        (
            await db.execute(
                select(TestHarnessRun)
                .where(TestHarnessRun.task_id == task_id)
                .order_by(TestHarnessRun.id)
                .with_for_update()
            )
        ).scalars()
    )
    workspace_runs = list(
        (
            await db.execute(
                select(WorkspaceReviewRun)
                .where(WorkspaceReviewRun.task_id == task_id)
                .order_by(WorkspaceReviewRun.id)
                .with_for_update()
            )
        ).scalars()
    )
    bindings = list(
        (
            await db.execute(
                select(TestHarnessChildBinding)
                .where(TestHarnessChildBinding.owner_task_id == task_id)
                .order_by(TestHarnessChildBinding.id)
                .with_for_update()
            )
        ).scalars()
    )
    if any(
        run.status not in HARNESS_TERMINAL_STATUSES
        or run.cleanup_status != "completed"
        for run in runs
    ):
        return None
    if any(
        run.status not in {"completed", "failed", "cancelled"}
        or run.cleanup_status != "completed"
        for run in workspace_runs
    ):
        return None
    if any(binding.state not in CHILD_TERMINAL_STATES for binding in bindings):
        return None
    binding_ids = [binding.id for binding in bindings]
    operation_receipts = (
        list(
            (
                await db.execute(
                    select(BrowserReviewOperationReceipt)
                    .where(
                        BrowserReviewOperationReceipt.binding_id.in_(binding_ids)
                    )
                    .order_by(BrowserReviewOperationReceipt.id)
                    .with_for_update()
                )
            ).scalars()
        )
        if binding_ids
        else []
    )
    if any(receipt.status == "permitted" for receipt in operation_receipts):
        # A missing ACK is an unknown external side effect until exact child
        # reap marks it uncertain. Never erase that proof opportunistically.
        return None
    has_graph = bool(runs or workspace_runs or bindings)
    if has_graph and (
        not task_incarnation_id
        or any(
            run.owner_task_incarnation_id != task_incarnation_id
            for run in runs
        )
        or any(
            run.owner_task_incarnation_id != task_incarnation_id
            for run in workspace_runs
        )
        or any(
            binding.owner_task_incarnation_id != task_incarnation_id
            for binding in bindings
        )
    ):
        return None

    run_ids = {run.id for run in runs}
    workspace_run_ids = {run.id for run in workspace_runs}
    if any(
        binding.harness_run_id is not None
        and binding.harness_run_id not in run_ids
        for binding in bindings
    ) or any(
        binding.workspace_review_run_id is not None
        and binding.workspace_review_run_id not in workspace_run_ids
        for binding in bindings
    ):
        return None
    if any(
        run.workspace_review_run_id is not None
        and run.workspace_review_run_id not in workspace_run_ids
        for run in runs
    ):
        return None

    if run_ids:
        leases = list(
            (
                await db.execute(
                    select(TestHarnessSandboxLease)
                    .where(TestHarnessSandboxLease.run_id.in_(run_ids))
                    .order_by(TestHarnessSandboxLease.id)
                    .with_for_update()
                )
            ).scalars()
        )
        if any(lease.cleanup_status != "completed" for lease in leases):
            return None
        evidence_rows = list(
            (
                await db.execute(
                    select(TestHarnessEvidence)
                    .where(TestHarnessEvidence.run_id.in_(run_ids))
                    .order_by(TestHarnessEvidence.id)
                    .with_for_update()
                )
            ).scalars()
        )
    else:
        evidence_rows = []

    child_ids = sorted({binding.child_task_id for binding in bindings})
    referenced_child_ids = {
        int(agent_task_id)
        for agent_task_id in (
            *(run.agent_task_id for run in runs),
            *(run.agent_task_id for run in workspace_runs),
        )
        if agent_task_id is not None and agent_task_id != task_id
    }
    if referenced_child_ids != set(child_ids):
        # An old/incomplete pipeline without a Binding cannot be proven safe
        # to erase, and a Binding not referenced by its owning Run is equally
        # malformed. Startup recovery must reconcile it first.
        return None
    # Canonical Task creation uses SQLite AUTOINCREMENT / database sequences,
    # so a Browser child must sort after its extant owner.  Fail closed on
    # corrupt legacy rows instead of introducing owner->child vs child->owner
    # lock cycles in supported databases.
    if any(child_id <= task_id for child_id in child_ids):
        return None
    children = list(
        (
            await db.execute(
                select(Task)
                .where(Task.id.in_(child_ids))
                .order_by(Task.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalars()
    ) if child_ids else []
    children_by_id = {child.id: child for child in children}
    from backend.services.test_harness_children import (
        TASK_TERMINAL_STATUSES,
        browser_child_binding_error,
    )

    for binding in bindings:
        child = children_by_id.get(binding.child_task_id)
        legacy_untrusted_profile = (
            binding.launch_profile_version is None
            and binding.provider is None
            and binding.model is None
            and binding.reasoning_effort is None
            and binding.codex_service_tier is None
            and binding.task_mode is None
            and binding.launch_config_digest is None
        )
        if (
            child is None
            or not child.incarnation_id
            or binding.child_task_incarnation_id != child.incarnation_id
            or child.status not in TASK_TERMINAL_STATUSES
            or child.archived is not True
            or (
                not legacy_untrusted_profile
                and browser_child_binding_error(binding, child) is not None
            )
        ):
            return None

    child_instances: list[Instance] = []
    if child_ids:
        from backend.models.capability import CapabilityInvocation
        from backend.models.code_review import CodeReviewRun
        from backend.models.monitor_session import MonitorSession
        from backend.models.plan import Plan
        from backend.models.worker_task_termination import (
            WorkerTaskTerminationReceipt,
        )

        child_instances = list(
            (
                await db.execute(
                    select(Instance)
                    .where(Instance.current_task_id.in_(child_ids))
                    .order_by(Instance.id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalars()
        )
        from backend.main import dispatcher, instance_manager

        for instance in child_instances:
            dispatcher_lifecycle = getattr(
                dispatcher,
                "_running_tasks",
                {},
            ).get(instance.id)
            if (
                instance_manager.is_running(instance.id)
                or (
                    dispatcher_lifecycle is not None
                    and not dispatcher_lifecycle.done()
                )
                or instance.status == "running"
                or (
                    instance.pid is not None
                    and (
                        instance.status not in {"error", "stopped"}
                        or not persisted_process_is_definitively_dead(
                            instance.pid,
                            instance.process_identity,
                        )
                    )
                )
            ):
                return None
        unexpected_task_child = await db.scalar(
            select(Task.id)
            .where(
                or_(
                    Task.plan_target_task_id.in_(child_ids),
                    Task.supersedes_plan_task_id.in_(child_ids),
                )
            )
            .limit(1)
        )
        unexpected_capability = await db.scalar(
            select(CapabilityInvocation.id)
            .where(CapabilityInvocation.task_id.in_(child_ids))
            .limit(1)
        )
        unexpected_monitor = await db.scalar(
            select(MonitorSession.id)
            .where(MonitorSession.task_id.in_(child_ids))
            .limit(1)
        )
        unexpected_review = await db.scalar(
            select(CodeReviewRun.id)
            .where(
                or_(
                    CodeReviewRun.developer_task_id.in_(child_ids),
                    CodeReviewRun.reviewer_task_id.in_(child_ids),
                )
            )
            .limit(1)
        )
        unexpected_worker_receipt = await db.scalar(
            select(WorkerTaskTerminationReceipt.operation_id)
            .where(WorkerTaskTerminationReceipt.task_id.in_(child_ids))
            .limit(1)
        )
        nested_harness_owner = await db.scalar(
            select(TestHarnessRun.id)
            .where(TestHarnessRun.task_id.in_(child_ids))
            .limit(1)
        )
        nested_workspace_owner = await db.scalar(
            select(WorkspaceReviewRun.id)
            .where(WorkspaceReviewRun.task_id.in_(child_ids))
            .limit(1)
        )
        nested_browser_owner = await db.scalar(
            select(TestHarnessChildBinding.id)
            .where(TestHarnessChildBinding.owner_task_id.in_(child_ids))
            .limit(1)
        )
        nested_plan_owner = await db.scalar(
            select(Plan.id)
            .where(Plan.target_task_id.in_(child_ids))
            .limit(1)
        )
        if any(
            value is not None
            for value in (
                unexpected_task_child,
                unexpected_capability,
                unexpected_monitor,
                unexpected_review,
                unexpected_worker_receipt,
                nested_harness_owner,
                nested_workspace_owner,
                nested_browser_owner,
                nested_plan_owner,
            )
        ):
            return None

    return TestHarnessDeleteGraph(
        run_ids=tuple(sorted(run_ids)),
        workspace_run_ids=tuple(sorted(workspace_run_ids)),
        binding_ids=tuple(binding.id for binding in bindings),
        evidence_storage_keys=tuple(
            sorted({evidence.storage_path for evidence in evidence_rows})
        ),
        child_tasks=tuple(
            (
                child.id,
                child.incarnation_id,
                child.status,
                child.retry_count,
                child.turn_generation,
            )
            for child in children
        ),
        child_instances=tuple(
            (
                instance.id,
                int(instance.current_task_id),
                instance.status,
                instance.pid,
                instance.process_identity,
                instance.started_at,
            )
            for instance in child_instances
        ) if child_ids else (),
    )


async def _delete_test_harness_graph(
    db: AsyncSession,
    graph: TestHarnessDeleteGraph,
) -> None:
    """Delete the already-locked graph in dependency order."""

    from backend.models.test_harness import (
        BrowserReviewOperationReceipt,
        TestHarnessAttempt,
        TestHarnessChildBinding,
        TestHarnessEvent,
        TestHarnessEvidence,
        TestHarnessFinding,
        TestHarnessRun,
        TestHarnessSandboxLease,
    )
    from backend.models.workspace_review import WorkspaceReviewRun

    for (
        instance_id,
        child_task_id,
        status,
        pid,
        process_identity,
        started_at,
    ) in graph.child_instances:
        predicates = [
            Instance.id == instance_id,
            Instance.current_task_id == child_task_id,
            Instance.status == status,
            Instance.pid.is_(None) if pid is None else Instance.pid == pid,
            (
                Instance.process_identity.is_(None)
                if process_identity is None
                else Instance.process_identity == process_identity
            ),
            (
                Instance.started_at.is_(None)
                if started_at is None
                else Instance.started_at == started_at
            ),
        ]
        detached = await db.execute(
            update(Instance)
            .where(*predicates)
            .values(
                current_task_id=None,
                pid=None,
                process_identity=None,
            )
        )
        if detached.rowcount != 1:
            raise RuntimeError(
                "Browser child Instance generation changed during owner deletion"
            )

    if graph.binding_ids:
        await db.execute(
            sa_delete(BrowserReviewOperationReceipt).where(
                BrowserReviewOperationReceipt.binding_id.in_(graph.binding_ids)
            )
        )
        await db.execute(
            sa_delete(TestHarnessChildBinding).where(
                TestHarnessChildBinding.id.in_(graph.binding_ids)
            )
        )
    if graph.run_ids:
        for model in (
            TestHarnessEvidence,
            TestHarnessFinding,
            TestHarnessEvent,
            TestHarnessAttempt,
            TestHarnessSandboxLease,
        ):
            await db.execute(
                sa_delete(model).where(model.run_id.in_(graph.run_ids))
            )
        await db.execute(
            sa_delete(TestHarnessRun).where(
                TestHarnessRun.id.in_(graph.run_ids)
            )
        )
    if graph.workspace_run_ids:
        await db.execute(
            sa_delete(WorkspaceReviewRun).where(
                WorkspaceReviewRun.id.in_(graph.workspace_run_ids)
            )
        )
    for child_id, incarnation_id, status, retry_count, turn_generation in graph.child_tasks:
        await purge_task_access_grants(db, child_id)
        await db.execute(
            sa_delete(LogEntry).where(LogEntry.task_id == child_id)
        )
        deleted = await db.execute(
            sa_delete(Task).where(
                Task.id == child_id,
                Task.incarnation_id == incarnation_id,
                Task.status == status,
                Task.retry_count == retry_count,
                Task.turn_generation == turn_generation,
                Task.archived.is_(True),
            )
        )
        if deleted.rowcount != 1:
            raise RuntimeError(
                "Browser child Task generation changed during owner deletion"
            )


def task_generation_fence(task: Task) -> TaskGenerationFence:
    """Capture the mutable fields that distinguish retries of one Task id."""

    return (
        task.retry_count,
        task.instance_id,
        task.started_at,
        task.completed_at,
        task.pty_background_generation,
        task.turn_generation,
    )


def task_delete_fence(task: Task) -> TaskDeleteFence:
    """Capture every mutable field used to distinguish a deletable mirror."""

    return (
        task.status,
        task.worker_id,
        task.retry_count,
        task.instance_id,
        task.started_at,
        task.completed_at,
        task.pty_background_generation,
        task.turn_generation,
    )


def append_task_generation_predicates(
    predicates: list,
    generation_fence: TaskGenerationFence | None,
) -> None:
    if generation_fence is None:
        return
    (
        expected_retry_count,
        expected_instance_id,
        expected_started_at,
        expected_completed_at,
        expected_background_generation,
        expected_turn_generation,
    ) = generation_fence
    predicates.extend(
        [
            Task.retry_count == expected_retry_count,
            (
                Task.instance_id.is_(None)
                if expected_instance_id is None
                else Task.instance_id == expected_instance_id
            ),
            (
                Task.started_at.is_(None)
                if expected_started_at is None
                else Task.started_at == expected_started_at
            ),
            (
                Task.completed_at.is_(None)
                if expected_completed_at is None
                else Task.completed_at == expected_completed_at
            ),
            (
                Task.pty_background_generation.is_(None)
                if expected_background_generation is None
                else Task.pty_background_generation
                == expected_background_generation
            ),
            Task.turn_generation == expected_turn_generation,
        ]
    )


def _effective_key_expr(auto_sort_on_access: bool = True):
    """Build the SQL expression for task sort key.

    auto_sort_on_access=True:  COALESCE(sort_order, ts(last_accessed_at ?? created_at))
    auto_sort_on_access=False: COALESCE(sort_order, ts(created_at))
    """
    if auto_sort_on_access:
        fallback = _UnixTimestamp(
            func.coalesce(Task.last_accessed_at, Task.created_at)
        )
    else:
        fallback = _UnixTimestamp(Task.created_at)
    return func.coalesce(Task.sort_order, fallback)


class TaskQueue:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def create(self, **kwargs) -> Task:
        task = await stage_task_record(self.db, **kwargs)
        await self.db.commit()
        await self.db.refresh(task)
        return task

    async def get(self, task_id: int) -> Task | None:
        return await self.db.get(Task, task_id)

    async def _auto_sort_enabled(self) -> bool:
        from backend.models.global_settings import GlobalSettings
        gs = await self.db.get(GlobalSettings, 1)
        return gs.auto_sort_on_access if gs and gs.auto_sort_on_access is not None else True

    async def list_tasks(
        self, status: str | None = None, include_archived: bool = False,
        archived_only: bool = False,
        project_id: int | None = None, starred: bool | None = None,
        has_unread: bool | None = None,
        task_kind: str | None = None,
        limit: int = 50, offset: int = 0,
        user_id: int | None = None,
    ) -> list[Task]:
        auto_sort = await self._auto_sort_enabled()
        effective_key = _effective_key_expr(auto_sort)
        stmt = select(Task).where(
            Task.shared_from_id.is_(None),
            ordinary_task_visibility_predicate(),
        ).order_by(Task.starred.desc(), effective_key.desc(), Task.id.desc())
        if archived_only:
            stmt = stmt.where(Task.archived.is_(True))
        elif not include_archived:
            stmt = stmt.where(Task.archived.is_(False))
        if status:
            parts = [s.strip() for s in status.split(",") if s.strip()]
            stmt = stmt.where(Task.status.in_(parts)) if len(parts) > 1 else stmt.where(Task.status == parts[0])
        if project_id is not None:
            stmt = stmt.where(Task.project_id == project_id)
        if starred is not None:
            stmt = stmt.where(Task.starred == starred)
        if has_unread is not None:
            stmt = stmt.where(Task.has_unread == has_unread)
        if task_kind is not None:
            stmt = stmt.where(_task_kind_predicate(task_kind))
        # Worker is only an execution location. Member visibility comes from
        # Task ownership, an explicit Task share, or a Project share.
        if user_id is not None:
            from backend.models.team_share import TeamTaskShare, TeamProjectShare
            from backend.models.user_group import UserGroupMember
            from backend.models.pr_monitor import MonitoredRepo, PRMonitorRun
            from backend.services.pr_monitor_task_access import (
                pr_monitor_display_task_predicate,
            )
            user_group_ids_q = select(UserGroupMember.group_id).where(UserGroupMember.user_id == user_id)
            shared_task_ids_q = select(TeamTaskShare.task_id).where(
                TeamTaskShare.permission == "chat",
                (
                    ((TeamTaskShare.target_type == "user") & (TeamTaskShare.target_id == user_id))
                    | ((TeamTaskShare.target_type == "group") & TeamTaskShare.target_id.in_(user_group_ids_q))
                ),
            )
            visible_project_ids = await _member_visible_project_ids(self.db, user_id)
            display_task_ids_q = (
                select(PRMonitorRun.display_task_id)
                .join(MonitoredRepo, MonitoredRepo.id == PRMonitorRun.repo_id)
                .where(
                    PRMonitorRun.display_task_id.is_not(None),
                    MonitoredRepo.project_id.in_(visible_project_ids),
                )
            )
            display_task = pr_monitor_display_task_predicate(Task.id)
            stmt = stmt.where(
                (Task.created_by == user_id)
                | and_(Task.id.in_(shared_task_ids_q), ~display_task)
                | and_(Task.project_id.in_(visible_project_ids), ~display_task)
                | and_(Task.id.in_(display_task_ids_q), display_task)
            )
        stmt = stmt.limit(limit).offset(offset)
        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    async def count_tasks(
        self, status: str | None = None, include_archived: bool = False,
        archived_only: bool = False,
        project_id: int | None = None, starred: bool | None = None,
        has_unread: bool | None = None,
        task_kind: str | None = None,
        user_id: int | None = None,
    ) -> int:
        stmt = select(func.count(Task.id)).where(
            Task.shared_from_id.is_(None),
            ordinary_task_visibility_predicate(),
        )
        if archived_only:
            stmt = stmt.where(Task.archived.is_(True))
        elif not include_archived:
            stmt = stmt.where(Task.archived.is_(False))
        if status:
            parts = [s.strip() for s in status.split(",") if s.strip()]
            stmt = stmt.where(Task.status.in_(parts)) if len(parts) > 1 else stmt.where(Task.status == parts[0])
        if project_id is not None:
            stmt = stmt.where(Task.project_id == project_id)
        if starred is not None:
            stmt = stmt.where(Task.starred == starred)
        if has_unread is not None:
            stmt = stmt.where(Task.has_unread == has_unread)
        if task_kind is not None:
            stmt = stmt.where(_task_kind_predicate(task_kind))
        if user_id is not None:
            from backend.models.team_share import TeamTaskShare, TeamProjectShare
            from backend.models.user_group import UserGroupMember
            from backend.models.pr_monitor import MonitoredRepo, PRMonitorRun
            from backend.services.pr_monitor_task_access import (
                pr_monitor_display_task_predicate,
            )
            user_group_ids_q = select(UserGroupMember.group_id).where(UserGroupMember.user_id == user_id)
            shared_task_ids_q = select(TeamTaskShare.task_id).where(
                TeamTaskShare.permission == "chat",
                (
                    ((TeamTaskShare.target_type == "user") & (TeamTaskShare.target_id == user_id))
                    | ((TeamTaskShare.target_type == "group") & TeamTaskShare.target_id.in_(user_group_ids_q))
                ),
            )
            visible_project_ids = await _member_visible_project_ids(self.db, user_id)
            display_task_ids_q = (
                select(PRMonitorRun.display_task_id)
                .join(MonitoredRepo, MonitoredRepo.id == PRMonitorRun.repo_id)
                .where(
                    PRMonitorRun.display_task_id.is_not(None),
                    MonitoredRepo.project_id.in_(visible_project_ids),
                )
            )
            display_task = pr_monitor_display_task_predicate(Task.id)
            stmt = stmt.where(
                (Task.created_by == user_id)
                | and_(Task.id.in_(shared_task_ids_q), ~display_task)
                | and_(Task.project_id.in_(visible_project_ids), ~display_task)
                | and_(Task.id.in_(display_task_ids_q), display_task)
            )
        result = await self.db.execute(stmt)
        return result.scalar() or 0

    async def star(self, task_id: int) -> Task | None:
        task = await self.get(task_id)
        if not task:
            return None
        task.starred = not task.starred
        auto_sort = await self._auto_sort_enabled()
        effective_key = _effective_key_expr(auto_sort)
        group_max = (
            await self.db.execute(
                select(func.max(effective_key)).where(
                    Task.archived == False,  # noqa: E712
                    Task.starred == task.starred,
                    Task.id != task_id,
                )
            )
        ).scalar()
        if group_max is not None:
            task.sort_order = group_max + 60
        else:
            task.sort_order = datetime.utcnow().timestamp()
        await self.db.commit()
        await self.db.refresh(task)
        return task

    async def archive(self, task_id: int) -> Task | None:
        task = await self.get(task_id)
        if not task:
            return None
        task.archived = not task.archived
        await self.db.commit()
        await self.db.refresh(task)
        return task

    async def update_task(
        self,
        task_id: int,
        *,
        operation_lock_held: bool = False,
        expected_incarnation_id: str | None = None,
        reject_active_harness_owner_graph: bool = False,
        **kwargs,
    ) -> Task | None:
        """Update one Task only while no durable termination owns it.

        Public edits, Worker configuration saves, and read/unread toggles all
        converge here.  A process-local operation lock closes same-process
        admission races; the Task no-op/write CAS is still the authoritative
        cross-process boundary.  It deliberately starts from a fresh
        transaction so SQLite WAL cannot raise ``BUSY_SNAPSHOT`` when a
        receipt committed after an earlier API authorization read.
        """

        if not operation_lock_held:
            # Imported lazily because WorkerProxy itself depends on TaskQueue.
            from backend.services.worker_proxy import get_task_operation_lock

            await self.db.rollback()
            async with get_task_operation_lock(task_id):
                return await self.update_task(
                    task_id,
                    operation_lock_held=True,
                    expected_incarnation_id=expected_incarnation_id,
                    reject_active_harness_owner_graph=(
                        reject_active_harness_owner_graph
                    ),
                    **kwargs,
                )

        values = {}
        for key, value in kwargs.items():
            if value is None:
                mapped_attr = getattr(Task, key, None)
                columns = getattr(
                    getattr(mapped_attr, "property", None),
                    "columns",
                    (),
                )
                # Patch schemas use Optional both for "not supplied" and for
                # genuinely nullable fields.  The API has already removed
                # fields that were not supplied, so preserve explicit NULL only
                # when the mapped column permits it.
                if not columns or not columns[0].nullable:
                    continue
            values[key] = value

        if not values:
            return await self.get(task_id)
        waiting_capability_safe = set(values).issubset({"has_unread"})

        # This must be the first statement in the mutation transaction.  The
        # correlated receipt predicate and receipt admission's own Task write
        # then have one deterministic winner on every supported database.
        await self.db.rollback()
        changed = await self.db.execute(
            update(Task)
            .where(
                Task.id == task_id,
                *(
                    ()
                    if expected_incarnation_id is None
                    else (Task.incarnation_id == expected_incarnation_id,)
                ),
                *(
                    ()
                    if waiting_capability_safe
                    else (Task.status != "waiting_capability",)
                ),
                no_active_worker_task_termination_predicate(),
                *(
                    (no_active_test_harness_owner_graph_predicate(),)
                    if reject_active_harness_owner_graph
                    else ()
                ),
            )
            .values(**values)
            .execution_options(synchronize_session=False)
        )
        if changed.rowcount != 1:
            await self.db.rollback()
            if (
                reject_active_harness_owner_graph
                and await has_active_test_harness_owner_graph(
                    self.db,
                    task_id,
                )
            ):
                await self.db.rollback()
                raise TestHarnessOwnerGraphConflict(
                    "Task owns an active Test Harness, Workspace Review, or "
                    "Browser Agent graph; wait for it to finish before "
                    "editing the Task"
                )
            if await active_worker_task_termination_receipt(self.db, task_id):
                await self.db.rollback()
                raise WorkerTaskTerminationConflict(
                    f"Task {task_id} has an active Worker termination receipt"
                )
            if not waiting_capability_safe:
                current_status = await self.db.scalar(
                    select(Task.status).where(Task.id == task_id)
                )
                await self.db.rollback()
                if current_status == "waiting_capability":
                    raise TaskWaitingCapabilityConflict(
                        f"Task {task_id} is waiting for a capability resume"
                    )
            await self.db.rollback()
            return None
        await self.db.commit()
        self.db.expire_all()
        if expected_incarnation_id is None:
            return await self.db.get(Task, task_id)
        return await self.db.scalar(
            select(Task).where(
                Task.id == task_id,
                Task.incarnation_id == expected_incarnation_id,
            )
        )

    async def delete(
        self,
        task_id: int,
        *,
        owner_fence_held: bool = False,
        expected_fence: TaskDeleteFence | None = None,
        remote_worker_deleted: bool = False,
        before_delete: (
            Callable[[TaskDeletePreflight], Awaitable[bool]] | None
        ) = None,
        remote_delete_confirm: (
            Callable[[TaskDeletePreflight], Awaitable[bool]] | None
        ) = None,
        prepare_remote_worker_delete: (
            Callable[[TaskDeletePreflight], Awaitable[bool]] | None
        ) = None,
        worker_delete_operation_id: str | None = None,
    ) -> bool:
        from backend.services.test_harness_owner_fence import (
            test_harness_owner_fence,
        )

        kwargs = {
            "expected_fence": expected_fence,
            "remote_worker_deleted": remote_worker_deleted,
            "before_delete": before_delete,
            "remote_delete_confirm": remote_delete_confirm,
            "prepare_remote_worker_delete": prepare_remote_worker_delete,
            "worker_delete_operation_id": worker_delete_operation_id,
        }
        if owner_fence_held:
            return await self._delete_under_owner_fence(task_id, **kwargs)
        async with test_harness_owner_fence(task_id):
            return await self._delete_under_owner_fence(task_id, **kwargs)

    async def _delete_under_owner_fence(
        self,
        task_id: int,
        *,
        expected_fence: TaskDeleteFence | None = None,
        remote_worker_deleted: bool = False,
        before_delete: (
            Callable[[TaskDeletePreflight], Awaitable[bool]] | None
        ) = None,
        remote_delete_confirm: (
            Callable[[TaskDeletePreflight], Awaitable[bool]] | None
        ) = None,
        prepare_remote_worker_delete: (
            Callable[[TaskDeletePreflight], Awaitable[bool]] | None
        ) = None,
        worker_delete_operation_id: str | None = None,
    ) -> bool:
        callbacks = tuple(
            callback
            for callback in (
                before_delete,
                remote_delete_confirm,
                prepare_remote_worker_delete,
            )
            if callback is not None
        )
        if len(callbacks) > 1:
            raise ValueError(
                "Task delete callbacks are mutually exclusive"
            )
        if remote_delete_confirm is not None and not remote_worker_deleted:
            raise ValueError(
                "remote_delete_confirm requires remote_worker_deleted=True"
            )
        if prepare_remote_worker_delete is not None and remote_worker_deleted:
            raise ValueError(
                "prepare_remote_worker_delete precedes remote_worker_deleted"
            )
        if worker_delete_operation_id is not None and (
            not remote_worker_deleted
            or any(callback is not None for callback in callbacks)
        ):
            raise ValueError(
                "worker_delete_operation_id requires callback-free remote finalization"
            )
        task = await self.get(task_id)
        if not task:
            return False
        observed_incarnation_id = task.incarnation_id
        (
            observed_status,
            observed_worker_id,
            observed_retry_count,
            observed_instance_id,
            observed_started_at,
            observed_completed_at,
            observed_background_generation,
            observed_turn_generation,
        ) = expected_fence or task_delete_fence(task)
        worker_delete_preparing = prepare_remote_worker_delete is not None
        if observed_status == "waiting_capability":
            return False
        if (
            not remote_worker_deleted
            and not is_task_status_deletable(
                mode=task.mode,
                status=observed_status,
            )
        ):
            return False
        if (
            not (remote_worker_deleted or worker_delete_preparing)
            and task.pty_background_generation
        ):
            # The foreground status is terminal, but the persistent Claude
            # session still owns detached output for this Task.
            return False
        # A completed Claude turn may have released the ordinary Instance
        # maps while retaining a hot PTY Session for a short follow-up window.
        # That proof is runtime-owned even when the durable background marker
        # is already clear. Queue deletion is also used by internal callers,
        # so keep this guard here in addition to the HTTP cleanup path.
        if not (remote_worker_deleted or worker_delete_preparing) and (
            observed_worker_id is None
        ):
            from backend.main import instance_manager

            has_live_post_exit = getattr(
                instance_manager,
                "has_live_task_pty_post_exit",
                None,
            )
            if callable(has_live_post_exit) and has_live_post_exit(
                task_id,
                session_id=getattr(task, "session_id", None),
                instance_id=observed_instance_id,
                task_retry_count=observed_retry_count,
                task_turn_generation=observed_turn_generation,
            ):
                await self.db.rollback()
                return False
        # A Worker task is authoritative on the remote CCM.  Directly deleting
        # its Manager mirror would lose the only management handle while the
        # remote task/process can still exist.  The API opts in only after a
        # 2xx Worker response with an explicit deletion acknowledgement.
        if (observed_worker_id is not None) != (
            remote_worker_deleted or worker_delete_preparing
        ):
            return False
        active_delete_owner = await active_worker_task_termination_receipt(
            self.db,
            task_id,
        )
        if active_delete_owner is not None and (
            worker_delete_operation_id is None
            or active_delete_owner.operation_id != worker_delete_operation_id
        ):
            await self.db.rollback()
            return False
        if worker_delete_operation_id is not None and active_delete_owner is None:
            await self.db.rollback()
            return False

        # Code Review completion locks its aggregate in Developer Task ->
        # Invocation -> Execution -> Run -> Reviewer Task order.  A reviewer
        # Task is newly created with (and can never later be attached to) its
        # Run, so reject that immutable child before taking the reviewer Task
        # write lock; locking Task -> Run here would invert the completion
        # order and permit a database deadlock.
        from backend.models.code_review import CodeReviewResult, CodeReviewRun

        reviewer_run_id = await self.db.scalar(
            select(CodeReviewRun.id)
            .where(CodeReviewRun.reviewer_task_id == task_id)
            .limit(1)
        )
        if reviewer_run_id is not None:
            return False

        isolated_browser_marker = bool(
            isinstance(task.metadata_, dict)
            and task.metadata_.get("isolated_browser_agent") is True
        )
        reverse_browser_binding = await self.db.scalar(
            select(TestHarnessChildBinding.id)
            .where(TestHarnessChildBinding.child_task_id == task_id)
            .limit(1)
        )
        if isolated_browser_marker or reverse_browser_binding is not None:
            # A Browser child is created together with its immutable binding;
            # an existing Task can never be attached later. Reject it before
            # taking the child Task write lock, so owner deletion keeps the
            # sole Task lock order owner -> child.
            await self.db.rollback()
            return False

        task_predicates = [
            Task.id == task_id,
            Task.status == observed_status,
            (
                Task.worker_id.is_(None)
                if observed_worker_id is None
                else Task.worker_id == observed_worker_id
            ),
            Task.retry_count == observed_retry_count,
            Task.turn_generation == observed_turn_generation,
            (
                Task.instance_id.is_(None)
                if observed_instance_id is None
                else Task.instance_id == observed_instance_id
            ),
            (
                Task.started_at.is_(None)
                if observed_started_at is None
                else Task.started_at == observed_started_at
            ),
            (
                Task.completed_at.is_(None)
                if observed_completed_at is None
                else Task.completed_at == observed_completed_at
            ),
            (
                Task.pty_background_generation.is_(None)
                if observed_background_generation is None
                else Task.pty_background_generation
                == observed_background_generation
            ),
            no_active_worker_task_termination_predicate(
                allow_operation_id=worker_delete_operation_id,
            ),
        ]
        # Establish the global lifecycle DB lock order at Task first. A no-op
        # exact UPDATE is both a generation CAS and a current-write lock on
        # MySQL RR / PostgreSQL; SQLite serializes the following write
        # transaction. The final DELETE repeats this full fence for ABA safety.
        guarded = await self.db.execute(
            update(Task)
            .where(*task_predicates)
            .values(status=observed_status)
        )
        if not guarded.rowcount:
            await self.db.rollback()
            return False

        locked_worker_delete_receipt = None
        if worker_delete_operation_id is not None:
            locked_worker_delete_receipt = (
                await self.db.execute(
                    select(WorkerTaskTerminationReceipt)
                    .where(
                        WorkerTaskTerminationReceipt.operation_id
                        == worker_delete_operation_id,
                        WorkerTaskTerminationReceipt.task_id == task_id,
                        WorkerTaskTerminationReceipt.active_task_id == task_id,
                        WorkerTaskTerminationReceipt.side == "manager",
                        WorkerTaskTerminationReceipt.operation == "delete",
                        WorkerTaskTerminationReceipt.status == "awaiting_ack",
                    )
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            if locked_worker_delete_receipt is None:
                await self.db.rollback()
                return False

        test_harness_graph = await _lock_test_harness_delete_graph(
            self.db,
            task_id,
            task.incarnation_id,
        )
        if test_harness_graph is None:
            await self.db.rollback()
            return False

        # Capability lifecycle uses the same global Task -> Invocation ->
        # Execution lock order. Active work owns an external adapter handle or
        # an unconsumed result, so deletion must fail closed. Terminal history
        # is removed explicitly below because SQLite deployments may have
        # foreign-key enforcement disabled.
        from backend.models.capability import (
            ACTIVE_EXECUTION_STATUSES,
            ACTIVE_INVOCATION_STATUSES,
            TERMINAL_RESUME_OUTBOX_STATUSES,
            CapabilityExecution,
            CapabilityInvocation,
            CapabilityResumeOutbox,
        )

        capability_invocations = list(
            (
                await self.db.execute(
                    select(CapabilityInvocation)
                    .where(CapabilityInvocation.task_id == task_id)
                    .order_by(CapabilityInvocation.id)
                    .with_for_update()
                )
            ).scalars()
        )
        capability_invocation_ids = {
            invocation.id for invocation in capability_invocations
        }
        capability_executions = []
        if capability_invocation_ids:
            capability_executions = list(
                (
                    await self.db.execute(
                        select(CapabilityExecution)
                        .where(
                            CapabilityExecution.invocation_id.in_(
                                capability_invocation_ids
                            )
                        )
                        .order_by(
                            CapabilityExecution.invocation_id,
                            CapabilityExecution.id,
                        )
                        .with_for_update()
                    )
                ).scalars()
            )
        if any(
            invocation.status in ACTIVE_INVOCATION_STATUSES
            for invocation in capability_invocations
        ) or any(
            execution.status in ACTIVE_EXECUTION_STATUSES
            for execution in capability_executions
        ):
            await self.db.rollback()
            return False

        # Resume delivery is the final child in the global Task -> Invocation
        # -> Execution -> Outbox lock order.  A live row owns the exact G -> G+1
        # handoff even when its Invocation/Execution already became terminal.
        # Lock and reject it before inspecting adapter-specific reverse links.
        capability_outbox_predicates = [
            CapabilityResumeOutbox.task_id == task_id,
        ]
        if capability_invocation_ids:
            capability_outbox_predicates.append(
                CapabilityResumeOutbox.invocation_id.in_(
                    capability_invocation_ids
                )
            )
        capability_outboxes = list(
            (
                await self.db.execute(
                    select(CapabilityResumeOutbox)
                    .where(or_(*capability_outbox_predicates))
                    .order_by(CapabilityResumeOutbox.id)
                    .with_for_update()
                )
            ).scalars()
        )
        if any(
            outbox.status not in TERMINAL_RESUME_OUTBOX_STATUSES
            for outbox in capability_outboxes
        ):
            await self.db.rollback()
            return False

        capability_execution_ids = {
            execution.id for execution in capability_executions
        }

        # The Plan graph helper locks both legacy Task-shaped and first-class
        # Runs in one stable primary-key order before it locks any Plan. Runs
        # that also belong to the first-class graph are validated/deleted by
        # that graph; only pure legacy rows are handled locally below.
        from backend.models.plan_agent import (
            PlanAgentRun,
            PlanAgentRuntimeReceipt,
            PlanAgentStep,
        )
        from backend.services.plan_runtime_receipt import runtime_run_is_clean

        # First-class Plan completion/recovery takes Run -> Plan locks after
        # Capability Core.  Validate and lock the complete target-owned graph
        # in that same order while the exact Task generation remains fenced.
        # The helper never commits or rolls back; the final Task DELETE below
        # is therefore the single commit point for both aggregates.
        from backend.services.plan_deletion import (
            PlanDeletionConflict,
            lock_target_plan_delete_graph,
        )

        try:
            target_plan_graph = await lock_target_plan_delete_graph(
                self.db,
                task_id,
                capability_invocation_ids=capability_invocation_ids,
                capability_execution_ids=capability_execution_ids,
                capability_outbox_ids={
                    outbox.id for outbox in capability_outboxes
                },
            )
        except PlanDeletionConflict:
            await self.db.rollback()
            return False

        # These rows were included in the helper's Run lock tier. Refresh the
        # exact legacy subset without introducing a Plan -> Run lock inversion.
        legacy_plan_runs = list(
            (
                await self.db.execute(
                    select(PlanAgentRun)
                    .where(PlanAgentRun.plan_task_id == task_id)
                    .order_by(PlanAgentRun.id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalars()
        )

        target_plan_run_ids = (
            set(target_plan_graph.run_ids)
            if target_plan_graph is not None
            else set()
        )
        legacy_only_plan_runs = [
            run for run in legacy_plan_runs if run.id not in target_plan_run_ids
        ]
        legacy_only_plan_run_ids = {
            run.id for run in legacy_only_plan_runs
        }
        # A migrated/first-class Run must be owned by the closed target graph;
        # otherwise deleting only its legacy Task pointer would strand the
        # Plan aggregate. Pure legacy Runs have no first-class Plan identity.
        if any(run.plan_id is not None for run in legacy_only_plan_runs):
            await self.db.rollback()
            return False
        if any(
            run.status not in {"completed", "failed", "cancelled"}
            or run.instance_id is not None
            or run.last_execution_started_at is not None
            or run.open_input_request_id is not None
            or run.capability_execution_id is not None
            for run in legacy_only_plan_runs
        ):
            await self.db.rollback()
            return False

        legacy_plan_steps = []
        if legacy_only_plan_run_ids:
            legacy_plan_steps = list(
                (
                    await self.db.execute(
                        select(PlanAgentStep)
                        .where(
                            PlanAgentStep.run_id.in_(legacy_only_plan_run_ids)
                        )
                        .order_by(PlanAgentStep.id)
                        .with_for_update()
                        .execution_options(populate_existing=True)
                    )
                ).scalars()
            )
        legacy_plan_step_ids = {step.id for step in legacy_plan_steps}
        if any(
            step.status not in {"completed", "failed", "cancelled"}
            or step.plan_id is not None
            or step.plan_version_id is not None
            or step.input_request_id is not None
            for step in legacy_plan_steps
        ):
            await self.db.rollback()
            return False
        legacy_plan_steps_by_id = {
            step.id: step for step in legacy_plan_steps
        }
        legacy_plan_runs_by_id = {
            run.id: run for run in legacy_only_plan_runs
        }
        for run in legacy_only_plan_runs:
            if (
                run.source_run_id is not None
                and run.source_run_id not in legacy_only_plan_run_ids
            ) or (
                run.draft_step_id is not None
                and (
                    run.draft_step_id not in legacy_plan_step_ids
                    or legacy_plan_steps_by_id[run.draft_step_id].run_id
                    != run.id
                )
            ) or run.base_version_id is not None or run.result_version_id is not None:
                await self.db.rollback()
                return False

        legacy_runtime_receipts = []
        if legacy_only_plan_run_ids:
            receipt_predicates = [
                PlanAgentRuntimeReceipt.run_id.in_(legacy_only_plan_run_ids)
            ]
            if legacy_plan_step_ids:
                receipt_predicates.append(
                    PlanAgentRuntimeReceipt.step_id.in_(legacy_plan_step_ids)
                )
            legacy_runtime_receipts = list(
                (
                    await self.db.execute(
                        select(PlanAgentRuntimeReceipt)
                        .where(or_(*receipt_predicates))
                        .order_by(PlanAgentRuntimeReceipt.id)
                        .with_for_update()
                        .execution_options(populate_existing=True)
                    )
                ).scalars()
            )
        for receipt in legacy_runtime_receipts:
            step = legacy_plan_steps_by_id.get(receipt.step_id)
            run = legacy_plan_runs_by_id.get(receipt.run_id)
            if (
                run is None
                or step is None
                or step.run_id != run.id
                or receipt.status != "cleaned"
                or receipt.cleaned_at is None
                or receipt.run_generation != step.generation
            ):
                await self.db.rollback()
                return False
        for run in legacy_only_plan_runs:
            if not await runtime_run_is_clean(self.db, run_id=run.id):
                await self.db.rollback()
                return False

        # Prove both adapter tables have no reverse ownership of this Task's
        # Core aggregate.  Do not trust their denormalized developer_task_id:
        # a corrupt cross-link must not let ordinary Task deletion cascade or
        # strand a Code Review Run/Result through Invocation/Execution IDs.
        code_review_run_predicates = [
            CodeReviewRun.developer_task_id == task_id,
        ]
        code_review_result_predicates = [
            CodeReviewResult.developer_task_id == task_id,
        ]
        if capability_invocation_ids:
            code_review_run_predicates.append(
                CodeReviewRun.capability_invocation_id.in_(
                    capability_invocation_ids
                )
            )
            code_review_result_predicates.append(
                CodeReviewResult.capability_invocation_id.in_(
                    capability_invocation_ids
                )
            )
        if capability_execution_ids:
            code_review_run_predicates.append(
                CodeReviewRun.capability_execution_id.in_(
                    capability_execution_ids
                )
            )
            code_review_result_predicates.append(
                CodeReviewResult.capability_execution_id.in_(
                    capability_execution_ids
                )
            )
        linked_code_review_id = await self.db.scalar(
            select(CodeReviewRun.id)
            .where(or_(*code_review_run_predicates))
            .order_by(CodeReviewRun.id)
            .with_for_update()
            .limit(1)
        )
        if linked_code_review_id is not None:
            await self.db.rollback()
            return False
        linked_code_review_result_id = await self.db.scalar(
            select(CodeReviewResult.id)
            .where(or_(*code_review_result_predicates))
            .order_by(CodeReviewResult.id)
            .with_for_update()
            .limit(1)
        )
        if linked_code_review_result_id is not None:
            await self.db.rollback()
            return False

        # A failed task may be the only durable identity for an unmanaged
        # process retained by startup recovery. Never delete that evidence
        # while any reverse Instance owner may still be alive. A dead PID can
        # be detached, but only through an exact CAS so a concurrently changed
        # generation is not erased.
        result = await self.db.execute(
            select(Instance)
            .where(Instance.current_task_id == task_id)
            .with_for_update()
        )
        owner_rows = list(result.scalars().all())
        runtime_candidate_ids = {instance.id for instance in owner_rows}
        if observed_instance_id is not None:
            task_side_instance = (
                await self.db.execute(
                    select(Instance)
                    .where(Instance.id == observed_instance_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if (
                task_side_instance is None
                or task_side_instance.current_task_id in (None, task_id)
            ):
                runtime_candidate_ids.add(observed_instance_id)

        # PID probes only describe the direct parent. InstanceManager also
        # tracks process groups, container execs and output consumers, any of
        # which can remain live after that parent has a returncode.
        from backend.main import dispatcher, instance_manager

        for instance_id in runtime_candidate_ids:
            dispatcher_lifecycle = getattr(
                dispatcher,
                "_running_tasks",
                {},
            ).get(instance_id)
            if (
                instance_manager.is_running(instance_id)
                or (
                    dispatcher_lifecycle is not None
                    and not dispatcher_lifecycle.done()
                )
            ):
                await self.db.rollback()
                return False

        # Goal evaluators run as independent process groups. A cleanup failure
        # deliberately retains their exact handle until shutdown; deleting the
        # parent Task meanwhile would make an apparently successful per-task
        # cleanup hide that surviving process.
        from backend.services.goal_evaluator import (
            has_unreaped_goal_evaluator_for_task,
        )

        if has_unreaped_goal_evaluator_for_task(task_id):
            await self.db.rollback()
            return False

        from backend.services.plan_agent_runner import (
            has_unreaped_plan_agent_for_task,
        )

        if has_unreaped_plan_agent_for_task(task_id):
            await self.db.rollback()
            return False

        # Legacy Task-shaped child Plans are separate Task generations, not
        # the first-class ``plans.target_task_id`` graph handled above. Keep
        # their explicit parent reference fail-closed.
        related_plan_id = await self.db.scalar(
            select(Task.id)
            .where(Task.plan_target_task_id == task_id)
            .limit(1)
        )
        if related_plan_id is not None:
            await self.db.rollback()
            return False

        from backend.models.monitor_session import MonitorCheck, MonitorSession

        monitor_rows = list(
            (
                await self.db.execute(
                    select(MonitorSession)
                    .where(MonitorSession.task_id == task_id)
                    .with_for_update()
                )
            )
            .scalars()
            .all()
        )
        monitor_ids = {session.id for session in monitor_rows}
        if (
            not (remote_worker_deleted or worker_delete_preparing)
            and any(session.status == "running" for session in monitor_rows)
        ):
            await self.db.rollback()
            return False
        if any(
            session.agent_type == "monitor"
            and session.source == "ccm"
            and session.remote_id is None
            and (session.provider or "claude").lower() == "codex"
            and (
                session.codex_thread_id is not None
                or session.codex_home is not None
                or session.codex_cleanup_pending
                or session.codex_cleanup_error is not None
            )
            for session in monitor_rows
        ):
            # A terminal Codex Monitor row remains the durable owner of its
            # rollout until exact thread deletion succeeds. Removing the Task
            # (and therefore this child row) would turn a retryable cleanup
            # failure into an unowned native thread after restart.
            await self.db.rollback()
            return False

        # Auxiliary lifecycle cleanup deliberately retains exact task/process
        # handles when descendants cannot be proven reaped. Do not erase their
        # DB parent while that runtime evidence remains.
        aux_task_maps = (
            getattr(dispatcher, "_monitor_tasks", {}),
            getattr(dispatcher, "_sub_agent_tasks", {}),
        )
        aux_process_maps = (
            getattr(dispatcher, "_monitor_processes", {}),
            getattr(dispatcher, "_sub_agent_processes", {}),
            getattr(dispatcher, "_monitor_turn_handles", {}),
        )
        for session_id in monitor_ids:
            if any(
                (
                    runtime_task := task_map.get(session_id)
                ) is not None
                and not runtime_task.done()
                for task_map in aux_task_maps
            ) or any(
                session_id in process_map
                for process_map in aux_process_maps
            ):
                await self.db.rollback()
                return False

        for instance in owner_rows:
            if instance.pid is None:
                if instance.status == "running":
                    await self.db.rollback()
                    return False
                continue
            if (
                instance.status not in ("error", "stopped")
                or not persisted_process_is_definitively_dead(
                    instance.pid,
                    instance.process_identity,
                )
            ):
                await self.db.rollback()
                return False

        for instance in owner_rows:
            predicates = [
                Instance.id == instance.id,
                Instance.current_task_id == task_id,
                Instance.status == instance.status,
            ]
            if instance.pid is None:
                predicates.append(Instance.pid.is_(None))
            else:
                predicates.append(Instance.pid == instance.pid)
            predicates.append(
                Instance.process_identity.is_(None)
                if instance.process_identity is None
                else Instance.process_identity == instance.process_identity
            )
            predicates.append(
                Instance.started_at.is_(None)
                if instance.started_at is None
                else Instance.started_at == instance.started_at
            )
            detached = await self.db.execute(
                update(Instance)
                .where(*predicates)
                .values(
                    current_task_id=None,
                    pid=None,
                    process_identity=None,
                )
            )
            if not detached.rowcount:
                await self.db.rollback()
                return False

        # Expose only the Plan ids from the graph locked above; API callers
        # must not reconstruct a deletion receipt with a lock-free query. A
        # Manager mirror also retains this Task writer fence across its
        # authoritative Worker DELETE. Invoke either callback after every
        # Capability/Plan/runtime preflight and before the first local DELETE.
        delete_preflight = TaskDeletePreflight(
            task_id=task_id,
            plan_ids=(
                target_plan_graph.plan_ids
                if target_plan_graph is not None
                else ()
            ),
        )
        if worker_delete_operation_id is not None:
            from backend.services.worker_task_termination import (
                manager_delete_receipt_allows_finalize,
            )

            if not manager_delete_receipt_allows_finalize(
                locked_worker_delete_receipt,
                task,
                operation_id=worker_delete_operation_id,
                plan_ids=delete_preflight.plan_ids,
            ):
                await self.db.rollback()
                return False

        delete_confirm = (
            remote_delete_confirm
            or before_delete
            or prepare_remote_worker_delete
        )
        if delete_confirm is not None:
            try:
                delete_confirmed = await delete_confirm(delete_preflight)
            except BaseException:
                await self.db.rollback()
                raise
            if not delete_confirmed:
                await self.db.rollback()
                return False
        if prepare_remote_worker_delete is not None:
            # The callback staged the active pending_remote receipt in this
            # transaction after every local fail-closed check. Commit that
            # durable owner with the exact locked Task/Plan identity, then
            # return before the first local DELETE or remote mutation.
            await self.db.commit()
            return True

        try:
            await _delete_test_harness_graph(self.db, test_harness_graph)
        except BaseException:
            await self.db.rollback()
            raise

        # Neither task_shares nor team_task_shares can be left to database
        # cascades: SQLite may not enforce the former FK, while the latter has
        # no Task FK.  Keeping this inside the fenced delete transaction also
        # prevents future Task-id reuse from inheriting stale access.
        await purge_task_access_grants(self.db, task_id)
        await self.db.execute(sa_delete(LogEntry).where(LogEntry.task_id == task_id))
        if target_plan_graph is not None:
            from backend.services.plan_deletion import delete_target_plan_graph

            try:
                await delete_target_plan_graph(self.db, target_plan_graph)
            except PlanDeletionConflict:
                await self.db.rollback()
                # The complete graph was already locked and validated before
                # a possible remote delete. A row-count mismatch here is an
                # internal invariant/transaction failure, not a safe business
                # rejection after the authoritative Worker may be gone.
                raise
        if capability_outboxes:
            # SQLite deployments commonly run without FK enforcement.  Remove
            # terminal outbox history explicitly and before its Execution /
            # Invocation parents so every supported dialect has identical
            # deletion behavior.
            await self.db.execute(
                sa_delete(CapabilityResumeOutbox).where(
                    CapabilityResumeOutbox.id.in_(
                        [outbox.id for outbox in capability_outboxes]
                    )
                )
            )
        if capability_invocation_ids:
            await self.db.execute(
                sa_delete(CapabilityExecution).where(
                    CapabilityExecution.invocation_id.in_(
                        capability_invocation_ids
                    )
                )
            )
            await self.db.execute(
                sa_delete(CapabilityInvocation).where(
                    CapabilityInvocation.task_id == task_id
                )
            )
        if legacy_runtime_receipts:
            await self.db.execute(
                sa_delete(PlanAgentRuntimeReceipt).where(
                    PlanAgentRuntimeReceipt.id.in_(
                        [receipt.id for receipt in legacy_runtime_receipts]
                    )
                )
            )
        if legacy_plan_step_ids:
            await self.db.execute(
                sa_delete(PlanAgentStep).where(
                    PlanAgentStep.id.in_(legacy_plan_step_ids)
                )
            )
        if legacy_only_plan_run_ids:
            await self.db.execute(
                sa_delete(PlanAgentRun).where(
                    PlanAgentRun.id.in_(legacy_only_plan_run_ids)
                )
            )
        # SQLite does not enable foreign-key cascades consistently across all
        # supported deployment/test connection paths.  Keep SSH grants inside
        # the same generation-fenced transaction so a deleted Task can never
        # leave reusable authorization behind (or collide if its id is reused).
        await self.db.execute(
            sa_delete(TaskSSHGrant).where(TaskSSHGrant.task_id == task_id)
        )
        if monitor_ids:
            await self.db.execute(
                sa_delete(MonitorCheck).where(
                    MonitorCheck.monitor_session_id.in_(monitor_ids)
                )
            )
            await self.db.execute(
                sa_delete(MonitorSession).where(MonitorSession.task_id == task_id)
            )

        if worker_delete_operation_id is not None:
            consumed_delete_owner = await self.db.execute(
                sa_delete(WorkerTaskTerminationReceipt).where(
                    WorkerTaskTerminationReceipt.operation_id
                    == worker_delete_operation_id,
                    WorkerTaskTerminationReceipt.task_id == task_id,
                    WorkerTaskTerminationReceipt.active_task_id == task_id,
                    WorkerTaskTerminationReceipt.side == "manager",
                    WorkerTaskTerminationReceipt.operation == "delete",
                    WorkerTaskTerminationReceipt.status == "awaiting_ack",
                )
            )
            if consumed_delete_owner.rowcount != 1:
                await self.db.rollback()
                raise WorkerTaskTerminationConflict(
                    "Durable Worker Task deletion owner changed before commit"
                )

        # SQLite deployments do not globally enable FK enforcement, while
        # PostgreSQL/MySQL cascade this history.  Remove inactive tombstones
        # explicitly in the same Task-generation transaction so deletion has
        # identical semantics and downgrade is not blocked by orphan receipts.
        await self.db.execute(
            sa_delete(WorkerTaskTerminationReceipt).where(
                WorkerTaskTerminationReceipt.task_id == task_id,
                WorkerTaskTerminationReceipt.active_task_id.is_(None),
            )
        )

        # The terminal status and task-side owner observed above are the delete
        # generation fence. A concurrent retry may move this row to pending and
        # immediately launch it after our owner SELECT; an ORM ``delete(task)``
        # would then erase the live generation by primary key alone. Child-row
        # deletes are in the same transaction, so a lost CAS rolls all of them
        # back as well.
        deleted = await self.db.execute(
            sa_delete(Task).where(*task_predicates)
        )
        if not deleted.rowcount:
            await self.db.rollback()
            return False
        await self.db.commit()

        from backend.services.internal_service_auth import (
            revoke_internal_service_owner,
        )
        from backend.services.ask_user import ask_user_registry

        deleted_task_generations = (
            (task_id, observed_incarnation_id),
            *(
                (child_id, child_incarnation_id)
                for child_id, child_incarnation_id, *_rest
                in test_harness_graph.child_tasks
            ),
        )
        for deleted_task_id, deleted_incarnation_id in deleted_task_generations:
            revoke_internal_service_owner("task-turn", deleted_task_id)
            if deleted_incarnation_id:
                ask_user_registry.discard_for_task(
                    deleted_task_id,
                    deleted_incarnation_id,
                )

        if test_harness_graph.evidence_storage_keys:
            from backend.services.test_harness_artifacts import (
                test_harness_artifact_store,
            )

            for storage_key in test_harness_graph.evidence_storage_keys:
                if not test_harness_artifact_store.remove(storage_key):
                    # The authoritative Task/evidence rows are already gone;
                    # retain an explicit error rather than pretending a
                    # corrupt/symlink archive path was physically removed.
                    import logging

                    logging.getLogger(__name__).error(
                        "Could not remove deleted Test Harness evidence %s",
                        storage_key,
                    )
        return True

    async def dequeue(
        self,
        exclude_ids: set[int] | None = None,
        *,
        instance_id: int | None = None,
    ) -> Task | None:
        """Atomically claim the highest-priority pending task.

        Selecting an ORM row and mutating it afterwards lets two independent
        sessions return the same task.  Ralph loops run concurrently (and may
        also overlap the global dispatcher), so the status transition itself
        must be a compare-and-swap.  A loser retries and may claim the next
        pending task instead.

        ``instance_id`` lets Ralph persist ownership in the same atomic claim;
        this leaves no cancellation window where a task is ``in_progress`` but
        has no identifiable owner.
        """

        from backend.services.worker_node_control import (
            WorkerNodeDrainingConflict,
            fence_worker_node_mutation,
        )

        try:
            # On a Worker this is the first writer lock and remains held until
            # the Task claim commits. A drain claim that wins first prevents a
            # new generation; a dequeue that wins first is visible to the
            # later node proof. Manager queues skip the fence internally.
            await fence_worker_node_mutation(self.db)
        except WorkerNodeDrainingConflict:
            await self.db.rollback()
            return None

        blocked_ids = set(exclude_ids or ())
        while True:
            dispatcher_scope = _dispatcher_scope_predicate()
            pr_review_scope = pr_review_dispatch_predicate()
            project_ready_scope = project_ready_dispatch_predicate()
            stmt = (
                select(Task.id)
                # worker task 不走本地 instance；shadow task (shared_from_id) 不执行
                .where(
                    Task.status == "pending",
                    Task.worker_id.is_(None),
                    Task.shared_from_id.is_(None),
                    task_retry_not_superseded_predicate(),
                    no_active_worker_task_termination_predicate(),
                    dispatcher_scope,
                    pr_review_scope,
                    project_ready_scope,
                )
                .order_by(Task.priority.asc(), Task.created_at.asc())
                .limit(1)
            )
            if blocked_ids:
                stmt = stmt.where(Task.id.notin_(blocked_ids))

            candidate_id = (await self.db.execute(stmt)).scalar_one_or_none()
            if candidate_id is None:
                # Release the Worker node writer fence when the queue is idle.
                await self.db.rollback()
                return None
            candidate = await self.db.get(
                Task,
                candidate_id,
                populate_existing=True,
            )
            if candidate is None:
                self.db.expire_all()
                continue
            if has_pending_worker_routing(candidate):
                # JSON marker predicates are not portable across every
                # supported database.  A staged marker cannot legitimately be
                # added while status=pending, so this refreshed pre-CAS check
                # safely skips crash/corruption leftovers without claiming
                # them; final launch barriers independently fail closed.
                blocked_ids.add(candidate_id)
                continue
            binding = await self.db.scalar(
                select(TestHarnessChildBinding).where(
                    TestHarnessChildBinding.child_task_id == candidate_id
                )
            )
            isolated_browser_marker = bool(
                (candidate.metadata_ or {}).get("isolated_browser_agent") is True
            )
            isolated_browser_child = binding is not None or isolated_browser_marker
            if isolated_browser_child:
                if (
                    binding is None
                    or binding.state != CHILD_READY
                    or browser_child_binding_error(binding, candidate) is not None
                ):
                    # Missing, reserved, stopping and recovered bindings all
                    # fail closed. The complete immutable launch tuple is
                    # checked before the claim CAS; launch repeats it after
                    # Instance ownership is committed.
                    blocked_ids.add(candidate_id)
                    continue
                binding_id = binding.id
                owner_identity = browser_binding_owner_identity(binding)
                # Candidate/binding discovery is a read snapshot. End it
                # before the durable owner writer fence so SQLite WAL never
                # attempts a stale read->write upgrade after owner deletion.
                await self.db.rollback()
                self.db.expire_all()
                try:
                    owner = await lock_test_harness_owner(
                        self.db,
                        owner_identity,
                    )
                except RuntimeError:
                    await self.db.rollback()
                    self.db.expire_all()
                    blocked_ids.add(candidate_id)
                    continue
                binding = await self.db.scalar(
                    select(TestHarnessChildBinding)
                    .where(TestHarnessChildBinding.id == binding_id)
                    .execution_options(populate_existing=True)
                )
                candidate = await self.db.get(
                    Task,
                    candidate_id,
                    populate_existing=True,
                )
                if (
                    binding is None
                    or candidate is None
                    or binding.state != CHILD_READY
                    or browser_child_binding_error(binding, candidate) is not None
                    or browser_binding_owner_identity(binding) != owner_identity
                ):
                    await self.db.rollback()
                    self.db.expire_all()
                    blocked_ids.add(candidate_id)
                    continue
                if browser_child_owner_error(binding, owner) is not None:
                    await self.db.rollback()
                    self.db.expire_all()
                    blocked_ids.add(candidate_id)
                    continue
            from backend.services.worker_relay import (
                has_worker_execution_quarantine,
            )

            if has_worker_execution_quarantine(candidate.metadata_):
                blocked_ids.add(candidate_id)
                continue

            values = {
                "status": "in_progress",
                "started_at": datetime.utcnow(),
                "error_message": None,
                "turn_generation": Task.turn_generation + 1,
                # A source pointer belongs to exactly one logical turn.  Clear
                # the previous generation in the same CAS that creates G+1;
                # Step 2 will bind the initial hidden source before launch.
                "turn_source_log_id": None,
            }
            if instance_id is not None:
                values["instance_id"] = instance_id

            if isolated_browser_child:
                # Browser stop/admission paths serialize on the durable
                # binding before touching the child Task.  Keep the queue on
                # that same cross-process order; otherwise stop can hold the
                # binding while dequeue holds the child row and each waits on
                # the other under PostgreSQL/MySQL.
                binding_claimed = await self.db.execute(
                    update(TestHarnessChildBinding)
                    .where(
                        TestHarnessChildBinding.id == binding.id,
                        TestHarnessChildBinding.state == CHILD_READY,
                    )
                    .values(
                        state=CHILD_RUNNING,
                        claimed_retry_count=candidate.retry_count,
                        claimed_instance_id=instance_id,
                        error=None,
                    )
                )
                if not binding_claimed.rowcount:
                    await self.db.rollback()
                    self.db.expire_all()
                    continue
            claimed = await self.db.execute(
                update(Task)
                .where(
                    Task.id == candidate_id,
                    Task.status == "pending",
                    Task.worker_id.is_(None),
                    Task.shared_from_id.is_(None),
                    task_retry_not_superseded_predicate(),
                    no_active_worker_task_termination_predicate(),
                    dispatcher_scope,
                    pr_review_scope,
                    project_ready_scope,
                )
                .values(**values)
            )
            if not claimed.rowcount and isolated_browser_child:
                # The preceding binding transition belongs to this exact
                # claim.  A child CAS loser must restore it atomically rather
                # than publishing a phantom running Browser generation.
                await self.db.rollback()
                self.db.expire_all()
                continue
            claimed_candidate = None
            if claimed.rowcount:
                claimed_candidate = await self.db.get(
                    Task,
                    candidate_id,
                    populate_existing=True,
                )
            if claimed.rowcount and (
                claimed_candidate is None
                or not await fence_native_execution_principal(
                    self.db,
                    user_id=claimed_candidate.execution_user_id,
                    role=claimed_candidate.execution_user_role,
                    principal_kind=claimed_candidate.execution_principal_kind,
                )
            ):
                # Roll the uncommitted Task/binding claim back atomically.
                # Keep revoked work pending for explicit authority repair and
                # continue looking so it cannot starve unrelated queue items.
                await self.db.rollback()
                self.db.expire_all()
                blocked_ids.add(candidate_id)
                continue
            await self.db.commit()
            if not claimed.rowcount:
                # Another dispatcher won after our candidate SELECT.  Expire a
                # potentially stale identity-map entry and try the next row.
                self.db.expire_all()
                continue

            task = await self.db.get(Task, candidate_id)
            if task is not None:
                await self.db.refresh(task)
            return task

    async def mark_status(self, task_id: int, status: str, **extra) -> None:
        """Generic status update with optional extra fields."""
        values = {"status": status, **extra}
        if status in ("completed", "failed"):
            values.setdefault("completed_at", datetime.utcnow())
        await self.db.execute(
            update(Task)
            .where(
                Task.id == task_id,
                no_active_worker_task_termination_predicate(),
            )
            .values(**values)
        )
        await self.db.commit()

    async def mark_completed(
        self,
        task_id: int,
        *,
        expected_statuses: tuple[str, ...] = (
            "pending",
            "in_progress",
            "executing",
        ),
        instance_id: int | None = None,
        generation_fence: TaskGenerationFence | None = None,
    ) -> bool:
        """Complete an active claim without reviving a cancelled generation."""

        predicates = [
            Task.id == task_id,
            Task.status.in_(expected_statuses),
            no_active_worker_task_termination_predicate(),
        ]
        if instance_id is not None:
            predicates.append(Task.instance_id == instance_id)
        append_task_generation_predicates(predicates, generation_fence)
        result = await self.db.execute(
            update(Task)
            .where(*predicates)
            .values(status="completed", completed_at=datetime.utcnow(), error_message=None)
        )
        await self.db.commit()
        return bool(result.rowcount)

    async def mark_failed(
        self,
        task_id: int,
        error: str,
        *,
        expected_statuses: tuple[str, ...] = (
            "pending",
            "in_progress",
            "executing",
        ),
        instance_id: int | None = None,
        generation_fence: TaskGenerationFence | None = None,
    ) -> bool:
        """Fail only the still-active task generation that produced ``error``."""

        predicates = [
            Task.id == task_id,
            Task.status.in_(expected_statuses),
            no_active_worker_task_termination_predicate(),
        ]
        if instance_id is not None:
            predicates.append(Task.instance_id == instance_id)
        append_task_generation_predicates(predicates, generation_fence)
        result = await self.db.execute(
            update(Task)
            .where(*predicates)
            .values(status="failed", error_message=error, completed_at=datetime.utcnow())
        )
        await self.db.commit()
        return bool(result.rowcount)

    async def defer(
        self,
        task_id: int,
        reason: str,
        *,
        instance_id: int | None = None,
        generation_fence: TaskGenerationFence | None = None,
    ) -> bool:
        """Return an active task to pending without consuming retry budget.

        Account routing can be temporarily unavailable before a process starts
        (for example, every Codex account is cooling down or one account is
        under login maintenance).  This is scheduling backpressure, not an
        execution failure, so ``retry_count`` must remain unchanged.

        The active-status guard is intentional: a concurrent user cancellation
        must win instead of being overwritten back to ``pending``.
        """
        from backend.services.worker_node_control import (
            fence_worker_node_mutation,
        )

        await fence_worker_node_mutation(self.db)
        predicate = [
            Task.id == task_id,
            Task.status.in_(("in_progress", "executing")),
            task_retry_not_superseded_predicate(),
            no_active_worker_task_termination_predicate(),
        ]
        if instance_id is not None:
            predicate.append(Task.instance_id == instance_id)
        append_task_generation_predicates(predicate, generation_fence)
        task = await self.db.get(Task, task_id, populate_existing=True)
        binding = await self.db.scalar(
            select(TestHarnessChildBinding).where(
                TestHarnessChildBinding.child_task_id == task_id
            )
        )
        isolated_browser_child = binding is not None or bool(
            ((task.metadata_ or {}) if task is not None else {}).get(
                "isolated_browser_agent"
            )
            is True
        )
        if isolated_browser_child and (
            task is None
            or binding is None
            or binding.state != CHILD_RUNNING
            or browser_child_binding_error(binding, task) is not None
        ):
            await self.db.rollback()
            return False
        if isolated_browser_child and binding is not None:
            binding_id = binding.id
            owner_identity = browser_binding_owner_identity(binding)
            await self.db.rollback()
            self.db.expire_all()
            try:
                owner = await lock_test_harness_owner(
                    self.db,
                    owner_identity,
                )
            except RuntimeError:
                await self.db.rollback()
                return False
            binding = await self.db.scalar(
                select(TestHarnessChildBinding)
                .where(TestHarnessChildBinding.id == binding_id)
                .execution_options(populate_existing=True)
            )
            task = await self.db.get(Task, task_id, populate_existing=True)
            if (
                task is None
                or binding is None
                or binding.state != CHILD_RUNNING
                or browser_binding_owner_identity(binding) != owner_identity
                or browser_child_binding_error(binding, task) is not None
            ):
                await self.db.rollback()
                return False
            if browser_child_owner_error(binding, owner) is not None:
                await self.db.rollback()
                return False
        if isolated_browser_child:
            released = await self.db.execute(
                update(TestHarnessChildBinding)
                .where(
                    TestHarnessChildBinding.child_task_id == task_id,
                    TestHarnessChildBinding.state == CHILD_RUNNING,
                )
                .values(
                    state=CHILD_READY,
                    claimed_retry_count=None,
                    claimed_instance_id=None,
                    error=reason,
                )
            )
            if not released.rowcount:
                await self.db.rollback()
                return False
        result = await self.db.execute(
            update(Task)
            .where(*predicate)
            .values(
                status="pending",
                instance_id=None,
                error_message=reason,
                started_at=None,
                completed_at=None,
            )
        )
        if not result.rowcount and isolated_browser_child:
            # Do not commit the binding release unless the exact active child
            # generation was returned to pending in the same transaction.
            await self.db.rollback()
            return False
        await self.db.commit()
        return bool(result.rowcount)

    async def retry(
        self,
        task_id: int,
        *,
        expected_statuses: tuple[str, ...] = (
            "failed",
            "cancelled",
            "conflict",
            "completed",
            "pending",
        ),
        instance_id: int | None = None,
        generation_fence: TaskGenerationFence | None = None,
        rollback_on_miss: bool = False,
        task_updates: dict | None = None,
        expected_incarnation_id: str | None = None,
        expected_principal: dict | None = None,
        commit: bool = True,
    ) -> Task | None:
        """CAS a retryable task back to pending and release old ownership.

        Automatic lifecycle retries pass their active statuses explicitly.
        The default is intentionally terminal-only so a stale API/client retry
        cannot steal a currently executing task.  Clearing ``instance_id`` is
        essential: it is an active claim, not a trustworthy stop target after
        a slot has been recycled.
        """

        from backend.services.worker_node_control import (
            fence_worker_node_mutation,
        )

        await fence_worker_node_mutation(self.db)
        predicates = [
            Task.id == task_id,
            Task.status.in_(expected_statuses),
            Task.status != "waiting_capability",
            Task.pty_background_generation.is_(None),
            task_retry_not_superseded_predicate(),
            no_active_worker_task_termination_predicate(),
        ]
        if instance_id is not None:
            predicates.append(Task.instance_id == instance_id)
        append_task_generation_predicates(predicates, generation_fence)
        if expected_incarnation_id is not None:
            predicates.append(Task.incarnation_id == expected_incarnation_id)
        if expected_principal is not None:
            predicates.extend(
                [
                    Task.execution_user_role
                    == expected_principal.get("execution_user_role"),
                    Task.execution_mode
                    == expected_principal.get("execution_mode"),
                    Task.execution_principal_kind
                    == expected_principal.get("execution_principal_kind"),
                    (
                        Task.execution_user_id.is_(None)
                        if expected_principal.get("execution_user_id") is None
                        else Task.execution_user_id
                        == expected_principal.get("execution_user_id")
                    ),
                ]
            )
        values = {
            "status": "pending",
            "retry_count": Task.retry_count + 1,
            "instance_id": None,
            "error_message": None,
            "started_at": None,
            "completed_at": None,
            "pty_background_generation": None,
            # The source pointer is exact retry/generation provenance. Clear it
            # in the same CAS that advances retry_count so no reader can treat
            # the previous provider-boundary evidence as belonging to the retry.
            "turn_source_log_id": None,
        }
        if task_updates:
            values.update(task_updates)
        task = await self.db.get(Task, task_id, populate_existing=True)
        binding = await self.db.scalar(
            select(TestHarnessChildBinding).where(
                TestHarnessChildBinding.child_task_id == task_id
            )
        )
        isolated_browser_child = binding is not None or bool(
            ((task.metadata_ or {}) if task is not None else {}).get(
                "isolated_browser_agent"
            )
            is True
        )
        if isolated_browser_child:
            if (
                task is None
                or binding is None
                or binding.state not in {CHILD_READY, CHILD_RUNNING}
                or browser_child_binding_error(binding, task) is not None
                or bool(task_updates)
            ):
                await self.db.rollback()
                return None
            binding_id = binding.id
            owner_identity = browser_binding_owner_identity(binding)
            await self.db.rollback()
            self.db.expire_all()
            try:
                owner = await lock_test_harness_owner(
                    self.db,
                    owner_identity,
                )
            except RuntimeError:
                await self.db.rollback()
                return None
            binding = await self.db.scalar(
                select(TestHarnessChildBinding)
                .where(TestHarnessChildBinding.id == binding_id)
                .execution_options(populate_existing=True)
            )
            task = await self.db.get(Task, task_id, populate_existing=True)
            if (
                task is None
                or binding is None
                or binding.state not in {CHILD_READY, CHILD_RUNNING}
                or browser_binding_owner_identity(binding) != owner_identity
                or browser_child_binding_error(binding, task) is not None
            ):
                await self.db.rollback()
                return None
            if browser_child_owner_error(binding, owner) is not None:
                await self.db.rollback()
                return None
        if isolated_browser_child and binding is not None:
            released = await self.db.execute(
                update(TestHarnessChildBinding)
                .where(
                    TestHarnessChildBinding.id == binding.id,
                    TestHarnessChildBinding.state.in_(
                        (CHILD_READY, CHILD_RUNNING)
                    ),
                )
                .values(
                    state=CHILD_READY,
                    claimed_retry_count=None,
                    claimed_instance_id=None,
                    error=None,
                )
            )
            if not released.rowcount:
                await self.db.rollback()
                return None
        result = await self.db.execute(
            update(Task)
            .where(*predicates)
            .values(**values)
        )
        if not result.rowcount:
            if rollback_on_miss or isolated_browser_child:
                # A Browser retry has already staged its binding release, so
                # every child CAS miss must roll the whole transaction back.
                await self.db.rollback()
            else:
                # Preserve the historical transaction boundary for ordinary
                # standalone retries. Callers that staged ownership cleanup
                # in the same transaction opt into rollback_on_miss.
                await self.db.commit()
            return None
        if commit:
            await self.db.commit()
        else:
            await self.db.flush()
        self.db.expire_all()
        task = await self.get(task_id)
        if task is not None:
            await self.db.refresh(task)
        return task

    async def cancel(self, task_id: int) -> Task | None:
        result = await self.db.execute(
            update(Task)
            .where(
                Task.id == task_id,
                Task.status.in_(
                    (
                        "pending_activation",
                        "pending",
                        "in_progress",
                        "executing",
                        "merging",
                    )
                ),
                no_active_worker_task_termination_predicate(),
            )
            .values(status="cancelled", completed_at=datetime.utcnow())
        )
        if not result.rowcount:
            await self.db.rollback()
            return None

        from backend.models.monitor_session import MonitorSession
        await self.db.execute(
            update(MonitorSession)
            .where(MonitorSession.task_id == task_id, MonitorSession.status == "running")
            .values(
                status="cancelled",
                completed_at=datetime.utcnow(),
                next_check_at=None,
                active_turn_generation=None,
                turn_started_at=None,
            )
        )

        await self.db.commit()
        self.db.expire_all()
        task = await self.get(task_id)
        if task is None:
            return None
        await self.db.refresh(task)
        return task
