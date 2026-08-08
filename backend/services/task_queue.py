import errno
import os
from datetime import datetime

from sqlalchemy import Float, and_, delete as sa_delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import FunctionElement

from backend.models.instance import Instance
from backend.models.log_entry import LogEntry
from backend.models.task import Task
from backend.models.task_ssh_grant import TaskSSHGrant
from backend.services.task_creation import stage_task_record
from backend.services.worker_routing_config import (
    has_pending_worker_routing,
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


def _task_kind_predicate(task_kind: str):
    if task_kind == TASK_KIND_STANDALONE_PLAN:
        return and_(Task.mode == "plan", Task.plan_target_task_id.is_(None))
    if task_kind == TASK_KIND_RELATED_PLAN:
        return and_(Task.mode == "plan", Task.plan_target_task_id.is_not(None))
    if task_kind == TASK_KIND_MAIN:
        return Task.mode != "plan"
    raise ValueError(f"Unsupported task kind: {task_kind}")


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


def persisted_pid_is_definitively_dead(pid: int) -> bool:
    """Return True only when a signal-free PID probe proves ``ESRCH``.

    A successful ``kill(pid, 0)`` means the process still exists. Permission
    failures and every other probe error are uncertain, so destructive
    cleanup must preserve the persisted PID/owner evidence and fail closed.
    """

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except OSError as exc:
        return exc.errno == errno.ESRCH
    except Exception:
        return False
    return False


TaskGenerationFence = tuple[
    int,
    int | None,
    datetime | None,
    datetime | None,
    str | None,
]

TaskDeleteFence = tuple[
    str,
    int | None,
    int,
    int | None,
    datetime | None,
    datetime | None,
    str | None,
]


def task_generation_fence(task: Task) -> TaskGenerationFence:
    """Capture the mutable fields that distinguish retries of one Task id."""

    return (
        task.retry_count,
        task.instance_id,
        task.started_at,
        task.completed_at,
        task.pty_background_generation,
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
        stmt = select(Task).where(Task.shared_from_id.is_(None)).order_by(Task.starred.desc(), effective_key.desc(), Task.id.desc())
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
        # Team CCM: member sees tasks on own Workers, created by them, or shared to them
        if user_id is not None:
            from backend.models.worker import Worker
            from backend.models.team_share import TeamTaskShare, TeamProjectShare
            from backend.models.user_group import UserGroupMember
            owned_worker_ids_q = select(Worker.id).where(Worker.owner_user_id == user_id)
            user_group_ids_q = select(UserGroupMember.group_id).where(UserGroupMember.user_id == user_id)
            shared_task_ids_q = select(TeamTaskShare.task_id).where(
                ((TeamTaskShare.target_type == "user") & (TeamTaskShare.target_id == user_id))
                | ((TeamTaskShare.target_type == "group") & TeamTaskShare.target_id.in_(user_group_ids_q))
            )
            shared_project_ids_q = select(TeamProjectShare.project_id).where(
                ((TeamProjectShare.target_type == "user") & (TeamProjectShare.target_id == user_id))
                | ((TeamProjectShare.target_type == "group") & TeamProjectShare.target_id.in_(user_group_ids_q))
            )
            stmt = stmt.where(
                (Task.created_by == user_id)
                | Task.worker_id.in_(owned_worker_ids_q)
                | Task.id.in_(shared_task_ids_q)
                | Task.project_id.in_(shared_project_ids_q)
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
        stmt = select(func.count(Task.id)).where(Task.shared_from_id.is_(None))
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
            from backend.models.worker import Worker
            from backend.models.team_share import TeamTaskShare, TeamProjectShare
            from backend.models.user_group import UserGroupMember
            owned_worker_ids_q = select(Worker.id).where(Worker.owner_user_id == user_id)
            user_group_ids_q = select(UserGroupMember.group_id).where(UserGroupMember.user_id == user_id)
            shared_task_ids_q = select(TeamTaskShare.task_id).where(
                ((TeamTaskShare.target_type == "user") & (TeamTaskShare.target_id == user_id))
                | ((TeamTaskShare.target_type == "group") & TeamTaskShare.target_id.in_(user_group_ids_q))
            )
            shared_project_ids_q = select(TeamProjectShare.project_id).where(
                ((TeamProjectShare.target_type == "user") & (TeamProjectShare.target_id == user_id))
                | ((TeamProjectShare.target_type == "group") & TeamProjectShare.target_id.in_(user_group_ids_q))
            )
            stmt = stmt.where(
                (Task.created_by == user_id)
                | Task.worker_id.in_(owned_worker_ids_q)
                | Task.id.in_(shared_task_ids_q)
                | Task.project_id.in_(shared_project_ids_q)
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

    async def update_task(self, task_id: int, **kwargs) -> Task | None:
        task = await self.get(task_id)
        if not task:
            return None
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
            setattr(task, key, value)
        await self.db.commit()
        await self.db.refresh(task)
        return task

    async def delete(
        self,
        task_id: int,
        *,
        expected_fence: TaskDeleteFence | None = None,
        remote_worker_deleted: bool = False,
    ) -> bool:
        task = await self.get(task_id)
        if not task:
            return False
        (
            observed_status,
            observed_worker_id,
            observed_retry_count,
            observed_instance_id,
            observed_started_at,
            observed_completed_at,
            observed_background_generation,
        ) = expected_fence or task_delete_fence(task)
        if (
            not remote_worker_deleted
            and not is_task_status_deletable(
                mode=task.mode,
                status=observed_status,
            )
        ):
            return False
        if not remote_worker_deleted and task.pty_background_generation:
            # The foreground status is terminal, but the persistent Claude
            # session still owns detached output for this Task.
            return False
        # A Worker task is authoritative on the remote CCM.  Directly deleting
        # its Manager mirror would lose the only management handle while the
        # remote task/process can still exist.  The API opts in only after a
        # 2xx Worker response with an explicit deletion acknowledgement.
        if (observed_worker_id is not None) != remote_worker_deleted:
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

        # Plan history is a first-class child of its target Task. Refuse to
        # create dangling history; users can explicitly delete those Plans
        # before deleting the target.
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
            not remote_worker_deleted
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
                or not persisted_pid_is_definitively_dead(instance.pid)
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
                Instance.started_at.is_(None)
                if instance.started_at is None
                else Instance.started_at == instance.started_at
            )
            detached = await self.db.execute(
                update(Instance)
                .where(*predicates)
                .values(current_task_id=None, pid=None)
            )
            if not detached.rowcount:
                await self.db.rollback()
                return False

        await self.db.execute(sa_delete(LogEntry).where(LogEntry.task_id == task_id))
        from backend.models.plan_agent import PlanAgentRun, PlanAgentStep

        plan_run_ids = list(
            (
                await self.db.execute(
                    select(PlanAgentRun.id).where(
                        PlanAgentRun.plan_task_id == task_id
                    )
                )
            ).scalars().all()
        )
        if plan_run_ids:
            await self.db.execute(
                sa_delete(PlanAgentStep).where(
                    PlanAgentStep.run_id.in_(plan_run_ids)
                )
            )
            await self.db.execute(
                sa_delete(PlanAgentRun).where(
                    PlanAgentRun.id.in_(plan_run_ids)
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

        revoke_internal_service_owner("task-turn", task_id)
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

        blocked_ids = set(exclude_ids or ())
        while True:
            stmt = (
                select(Task.id)
                # worker task 不走本地 instance；shadow task (shared_from_id) 不执行
                .where(
                    Task.status == "pending",
                    Task.worker_id.is_(None),
                    Task.shared_from_id.is_(None),
                    task_retry_not_superseded_predicate(),
                )
                .order_by(Task.priority.asc(), Task.created_at.asc())
                .limit(1)
            )
            if blocked_ids:
                stmt = stmt.where(Task.id.notin_(blocked_ids))

            candidate_id = (await self.db.execute(stmt)).scalar_one_or_none()
            if candidate_id is None:
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

            values = {
                "status": "in_progress",
                "started_at": datetime.utcnow(),
                "error_message": None,
            }
            if instance_id is not None:
                values["instance_id"] = instance_id

            claimed = await self.db.execute(
                update(Task)
                .where(
                    Task.id == candidate_id,
                    Task.status == "pending",
                    Task.worker_id.is_(None),
                    Task.shared_from_id.is_(None),
                    task_retry_not_superseded_predicate(),
                )
                .values(**values)
            )
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
            update(Task).where(Task.id == task_id).values(**values)
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

        predicates = [Task.id == task_id, Task.status.in_(expected_statuses)]
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
        predicate = [
            Task.id == task_id,
            Task.status.in_(("in_progress", "executing")),
            task_retry_not_superseded_predicate(),
        ]
        if instance_id is not None:
            predicate.append(Task.instance_id == instance_id)
        append_task_generation_predicates(predicate, generation_fence)
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
    ) -> Task | None:
        """CAS a retryable task back to pending and release old ownership.

        Automatic lifecycle retries pass their active statuses explicitly.
        The default is intentionally terminal-only so a stale API/client retry
        cannot steal a currently executing task.  Clearing ``instance_id`` is
        essential: it is an active claim, not a trustworthy stop target after
        a slot has been recycled.
        """

        predicates = [
            Task.id == task_id,
            Task.status.in_(expected_statuses),
            Task.pty_background_generation.is_(None),
            task_retry_not_superseded_predicate(),
        ]
        if instance_id is not None:
            predicates.append(Task.instance_id == instance_id)
        append_task_generation_predicates(predicates, generation_fence)
        result = await self.db.execute(
            update(Task)
            .where(*predicates)
            .values(
                status="pending",
                retry_count=Task.retry_count + 1,
                instance_id=None,
                error_message=None,
                started_at=None,
                completed_at=None,
                pty_background_generation=None,
            )
        )
        if not result.rowcount:
            if rollback_on_miss:
                await self.db.rollback()
            else:
                # Preserve the historical transaction boundary for ordinary
                # standalone retries. Callers that staged ownership cleanup
                # in the same transaction opt into rollback_on_miss.
                await self.db.commit()
            return None
        await self.db.commit()
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
                Task.status.in_(("pending", "in_progress", "executing", "merging")),
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
