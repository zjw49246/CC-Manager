"""Shared FastAPI dependencies for user context and resource ownership."""

from fastapi import HTTPException, Request
from sqlalchemy import select, update


MANAGED_SSH_AUTH_REQUIRED_DETAIL = (
    "Managed SSH requires AUTH_TOKEN to be configured"
)


def require_managed_ssh_auth_configured() -> None:
    """Keep Manager-held SSH credentials closed in legacy open mode."""

    from backend.config import settings

    if not settings.auth_token:
        raise HTTPException(503, MANAGED_SSH_AUTH_REQUIRED_DETAIL)


def get_current_user_id(request: Request) -> int | None:
    return getattr(request.state, "user_id", None)


def get_current_user_role(request: Request) -> str:
    return getattr(request.state, "user_role", "member")


def task_execution_principal_from_request(
    request: Request,
    *,
    force_sandbox: bool = False,
) -> dict[str, object]:
    """Freeze a trusted HTTP identity for a Task turn or creation."""

    from backend.services.task_creation import task_execution_principal_values

    if force_sandbox:
        from backend.services.task_creation import (
            system_task_execution_principal_values,
        )

        return system_task_execution_principal_values()

    role = get_current_user_role(request)
    auth_type = getattr(request.state, "auth_type", None)
    if auth_type == "jwt":
        kind = "user"
        user_id = get_current_user_id(request)
    elif auth_type in {"token", "none", "worker_control_plane"}:
        from backend.config import settings

        if settings.ccm_node_role == "worker":
            # A Worker's deployment token authenticates only its Manager
            # control plane.  Manager-forwarded execution arrives through a
            # separate complete delegated-principal envelope; a direct Worker
            # HTTP request must never turn the control credential itself into
            # unrestricted model authority.
            kind = "system"
            user_id = None
            role = "member"
        else:
            # On the Manager, deployment-token and legacy auth-disabled
            # requests are explicit super-admin authority without a durable
            # User-row dependency.
            kind = "deployment_token"
            user_id = None
            role = "super_admin"
    else:
        # Internal-service and unknown identities must never synthesize a
        # human administrator at a Task creation boundary.
        kind = "system"
        user_id = None
        role = "member"
    return task_execution_principal_values(
        user_id=user_id,
        role=role,
        principal_kind=kind,
    )


def is_admin(request: Request) -> bool:
    """Both admin and super_admin have admin-level permissions."""
    return get_current_user_role(request) in ("admin", "super_admin")


def is_super_admin(request: Request) -> bool:
    """Only super_admin can promote users to admin."""
    return get_current_user_role(request) == "super_admin"


def require_admin(request: Request):
    """Raise 403 if not admin/super_admin."""
    # Scoped child credentials have already been restricted to an exact
    # method/path by authentication middleware.  Let those callbacks traverse
    # routers that are otherwise admin-only; this does not grant access to any
    # additional route.
    if getattr(request.state, "auth_type", None) == "internal_service":
        return
    if not is_admin(request):
        raise HTTPException(403, "Admin only")


def _require_forwarded_task_incarnation(request: Request, task) -> None:
    expected = getattr(request, "headers", {}).get(
        "x-ccm-task-incarnation"
    )
    if expected is None:
        return
    if not expected or expected != getattr(task, "incarnation_id", None):
        raise HTTPException(409, "Worker Task incarnation changed")


async def require_worker_task_incarnation_header(
    request: Request,
    task_id: int,
    db,
    *,
    write_fence: bool = False,
):
    """Require an exact Manager→Worker Task incarnation header.

    Deployment bearer authentication proves which Manager may call a Worker;
    it does not identify one logical Task incarnation.  Hidden task-scoped
    Worker routes must therefore require this header even when auth is disabled
    for a local development deployment, and must never fall back to ``db.get``
    after a missing/mismatched identity.
    """

    expected = getattr(request, "headers", {}).get(
        "x-ccm-task-incarnation"
    )
    if (
        not isinstance(expected, str)
        or len(expected) != 32
        or any(char not in "0123456789abcdef" for char in expected)
    ):
        raise HTTPException(
            409,
            "Worker Task incarnation header is required",
        )
    from backend.models.task import Task

    if write_fence:
        fenced = await db.execute(
            update(Task)
            .where(
                Task.id == task_id,
                Task.incarnation_id == expected,
            )
            .values(status=Task.status)
        )
        if fenced.rowcount != 1:
            await db.rollback()
            raise HTTPException(409, "Worker Task incarnation changed")

    statement = select(Task).where(
        Task.id == task_id,
        Task.incarnation_id == expected,
    )
    if write_fence:
        statement = statement.with_for_update()
    task = (
        await db.execute(
            statement.execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if task is None:
        raise HTTPException(409, "Worker Task incarnation changed")
    return task


async def require_worker_control_plane_task_incarnation(
    request: Request,
    task_id: int,
    db,
    *,
    write_fence: bool = False,
):
    """Fence a plain Manager→Worker Task route to one logical Task.

    The deployment bearer identifies the Manager control plane, not a Task
    incarnation.  Human/JWT and scoped internal-service requests retain their
    existing authorization contracts; only a Worker node accepting its broad
    control-plane credential must also present the exact Task identity.
    """

    from backend.config import settings

    if not (
        settings.ccm_node_role == "worker"
        and getattr(request.state, "auth_type", None)
        == "worker_control_plane"
    ):
        return None
    return await require_worker_task_incarnation_header(
        request,
        task_id,
        db,
        write_fence=write_fence,
    )


def require_internal_service(request: Request) -> None:
    """Allow scoped CCM callbacks (and the legacy deployment credential).

    Auth-disabled deployments intentionally retain their historical open
    semantics. New child processes receive an exact-route credential labelled
    ``internal_service``; ``token`` remains for deployment/Worker compatibility.
    """
    from backend.config import settings

    if settings.auth_token and getattr(request.state, "auth_type", None) not in {
        "token",
        "worker_control_plane",
        "internal_service",
    }:
        raise HTTPException(403, "Internal service authentication required")


def _internal_task_access_allowed(request: Request, task) -> bool:
    if getattr(request.state, "auth_type", None) != "internal_service":
        return False
    from backend.services.internal_service_auth import (
        internal_task_id,
    )

    claims = getattr(request.state, "internal_service_claims", None)
    return bool(
        internal_task_id(claims) == task.id
        and getattr(claims, "task_incarnation_id", None)
        == getattr(task, "incarnation_id", None)
    )


def internal_task_incarnation_id(
    request: Request,
    task_id: int,
) -> str | None:
    """Return the exact Task incarnation carried by a scoped callback."""

    if getattr(request.state, "auth_type", None) != "internal_service":
        return None
    claims = getattr(request.state, "internal_service_claims", None)
    incarnation_id = getattr(claims, "task_incarnation_id", None)
    if (
        getattr(claims, "task_id", None) != task_id
        or not incarnation_id
    ):
        raise HTTPException(403, "Internal service Task identity mismatch")
    return incarnation_id


async def require_internal_task_incarnation(
    request: Request,
    task_id: int,
    db,
    *,
    write_fence: bool = False,
):
    """Revalidate a scoped callback in the endpoint's own transaction.

    Middleware rejection is an early filter, not an authorization commit:
    another process can delete/import/reuse an integer Task id between the
    middleware session and the route session. Lock the exact incarnation in
    the transaction that reads or mutates the callback owner.
    """

    if getattr(request.state, "auth_type", None) != "internal_service":
        return None
    from backend.models.task import Task

    incarnation_id = internal_task_incarnation_id(request, task_id)
    assert incarnation_id is not None
    claims = getattr(request.state, "internal_service_claims", None)
    retry_count = getattr(claims, "task_retry_count", None)
    turn_generation = getattr(claims, "task_turn_generation", None)
    task_status = getattr(claims, "task_status", None)
    generation_values = (retry_count, turn_generation, task_status)
    if any(value is not None for value in generation_values) and any(
        value is None for value in generation_values
    ):
        raise HTTPException(403, "Internal service Task generation is invalid")
    identity_predicates = [
        Task.id == task_id,
        Task.incarnation_id == incarnation_id,
    ]
    if retry_count is not None:
        identity_predicates.extend((
            Task.retry_count == retry_count,
            Task.turn_generation == turn_generation,
            Task.status == task_status,
        ))
    stale_detail = (
        "Internal service SSH Task generation is stale"
        if getattr(claims, "audience", None) == "ccm_ssh"
        else (
            "Internal service Task generation is stale"
            if retry_count is not None
            else "Internal service Task incarnation is stale"
        )
    )
    if write_fence:
        # ``FOR UPDATE`` is ignored by SQLite. A no-op exact-identity UPDATE is
        # the portable writer barrier: delete/import/retry/next-turn/status
        # transition either wins before it (and this callback rejects) or
        # waits until the callback transaction has committed/rolled back.
        fenced = await db.execute(
            update(Task)
            .where(*identity_predicates)
            .values(status=Task.status)
        )
        if fenced.rowcount != 1:
            raise HTTPException(403, stale_detail)
        task = await db.get(Task, task_id, populate_existing=True)
    else:
        task = await db.scalar(
            select(Task)
            .where(*identity_predicates)
            .with_for_update()
        )
    if task is None:
        raise HTTPException(403, stale_detail)
    return task


def _member_group_ids(user_id: int):
    from backend.models.user_group import UserGroupMember

    return select(UserGroupMember.group_id).where(
        UserGroupMember.user_id == user_id
    )


async def _lock_user_group_membership_authority(user_id: int, db) -> None:
    """Fence group-derived ACLs against membership revocation.

    Resource effects already hold their Project/Task writer fence before
    reaching this helper.  The no-op membership UPDATE extends that same
    transaction across the final group-share lookup.  If revocation wins,
    the lookup sees no membership; if this effect wins, revocation waits until
    the effect commits.  SQLite's database writer and PostgreSQL/MySQL row
    locks therefore implement the same ordering without making read-only
    resource checks mutate the database.
    """

    from backend.models.user_group import UserGroupMember

    await db.execute(
        update(UserGroupMember)
        .where(UserGroupMember.user_id == user_id)
        .values(user_id=UserGroupMember.user_id)
        .execution_options(synchronize_session=False)
    )


async def lock_request_user_authority(request: Request, db) -> None:
    """Fence a JWT principal against disablement or role changes.

    Authentication freezes ``request.state`` before an endpoint begins.  A
    long-running effect admission must not keep using that cached role after a
    concurrent administrator has committed a demotion or disabled the User.
    Resource effect helpers already hold their Project/Task and membership
    fences when they reach this function, so the common database lock order is
    Project -> Task -> membership -> User.

    Deployment-token and scoped internal-service callers do not have a
    mutable User row; their authority is validated by their own credential
    boundary and therefore intentionally has no row to fence here.
    """

    if getattr(request.state, "auth_type", None) != "jwt":
        return
    user_id = get_current_user_id(request)
    expected_role = get_current_user_role(request)
    if (
        isinstance(user_id, bool)
        or not isinstance(user_id, int)
        or user_id <= 0
        or expected_role not in {"member", "admin", "super_admin"}
    ):
        raise HTTPException(403, "User authority is invalid")

    from backend.models.user import User

    fenced = await db.execute(
        update(User)
        .where(
            User.id == user_id,
            User.is_active.is_(True),
            User.role == expected_role,
        )
        .values(role=User.role)
        .execution_options(synchronize_session=False)
    )
    if fenced.rowcount != 1:
        raise HTTPException(
            409,
            "User was disabled or changed role while authorizing the effect",
        )


async def has_worker_access(
    request: Request,
    worker_id: int | None,
    db,
) -> bool:
    """Return whether the current identity may target one exact Worker.

    ``None`` means execution on the Manager itself and is therefore
    administrator-only.  Project access is handled separately: a member may
    still create work for a shared *local* Project, but cannot target the
    Manager for an unrelated task.
    """
    if is_admin(request):
        return True
    if worker_id is None:
        return False
    user_id = get_current_user_id(request)
    if not user_id:
        return False
    from backend.models.worker import Worker

    worker = await db.get(Worker, worker_id)
    return bool(worker and worker.owner_user_id == user_id)


async def require_worker_target_access(
    request: Request,
    worker_id: int | None,
    db,
) -> None:
    if worker_id is not None:
        from backend.models.worker import Worker

        if await db.get(Worker, worker_id) is None:
            raise HTTPException(404, "Worker not found")
    if not await has_worker_access(request, worker_id, db):
        raise HTTPException(403, "No access to target Worker")


async def has_project_access(
    request: Request,
    project_id: int,
    db,
    *,
    effect_fence: bool = False,
) -> bool:
    """Return whether the current identity may access one exact Project."""
    if is_admin(request):
        return True
    user_id = get_current_user_id(request)
    if not user_id:
        return False

    from backend.models.project import Project
    from backend.models.project import project_is_internal
    from backend.models.team_share import TeamProjectShare
    project = await db.get(Project, project_id)
    if project is None or project_is_internal(project):
        return False

    if effect_fence:
        await _lock_user_group_membership_authority(user_id, db)

    user_group_ids = _member_group_ids(user_id)
    shared = (
        await db.execute(
            select(TeamProjectShare.id)
            .where(
                TeamProjectShare.project_id == project_id,
                (
                    (TeamProjectShare.target_type == "user")
                    & (TeamProjectShare.target_id == user_id)
                )
                | (
                    (TeamProjectShare.target_type == "group")
                    & TeamProjectShare.target_id.in_(user_group_ids)
                ),
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    return shared is not None


async def require_project_access(
    request: Request,
    project_id: int,
    db,
) -> None:
    if not await has_project_access(request, project_id, db):
        raise HTTPException(403, "No access to this project")


async def _lock_project_effect_fence(project_id: int, db):
    """Take the Project/TeamProjectShare writer fence without deciding ACL.

    Task-scoped effects must lock their current Project before the Task row so
    a concurrent Project move or Project-share revocation is serialized.  A
    Task ``chat`` share is nevertheless an independent grant, so merely
    belonging to a Project must not make this lower-level lock require Project
    membership.  Callers decide the applicable resource ACL only after all
    required writer fences are held.
    """

    from backend.services.project_share_admission import (
        ProjectShareAdmissionError,
        lock_project_share_authority,
    )

    try:
        project = await lock_project_share_authority(db, project_id)
    except ProjectShareAdmissionError as exc:
        await db.rollback()
        raise HTTPException(409, str(exc)) from exc
    except ValueError as exc:
        await db.rollback()
        raise HTTPException(404, "Project not found") from exc
    return project


async def lock_project_effect_access(
    request: Request,
    project_id: int,
    db,
):
    """Take the TeamProjectShare writer fence and revalidate Project ACL."""

    project = await _lock_project_effect_fence(project_id, db)
    if not await has_project_access(
        request,
        project_id,
        db,
        effect_fence=True,
    ):
        raise HTTPException(403, "No access to this project")
    await lock_request_user_authority(request, db)
    return project


async def lock_project_worker_effect_access(
    request: Request,
    project_id: int,
    db,
):
    """Fence a Project-owned Worker assignment in global row-lock order.

    Durable work creation must not acquire the mutable User authority row and
    only then wait for the Worker lifecycle row: Worker destroy and execution
    admission both hold Worker before User.  Keep the Project boundary first
    (so Project moves/share revocation stay serialized), then the Project's
    current Worker, group membership, and finally the JWT User.
    """

    from backend.services.worker_assignment import fence_ready_worker_assignment
    from backend.services.worker_node_control import fence_worker_node_mutation

    await fence_worker_node_mutation(db)
    project = await _lock_project_effect_fence(project_id, db)
    await fence_ready_worker_assignment(db, project.worker_id)
    if not await has_project_access(
        request,
        project_id,
        db,
        effect_fence=True,
    ):
        raise HTTPException(403, "No access to this project")
    await lock_request_user_authority(request, db)
    return project


async def lock_worker_effect_access(
    request: Request,
    worker_id: int | None,
    db,
) -> None:
    """Fence a projectless Worker assignment before mutable User authority."""

    from backend.services.worker_assignment import fence_ready_worker_assignment
    from backend.services.worker_node_control import fence_worker_node_mutation

    await fence_worker_node_mutation(db)
    await fence_ready_worker_assignment(db, worker_id)
    await require_worker_target_access(request, worker_id, db)
    await lock_request_user_authority(request, db)


async def lock_task_effect_access(
    request: Request,
    task,
    db,
    *,
    allow_chat_share: bool,
    fence_worker_node: bool = False,
    worker_node_fence_held: bool = False,
    fence_worker_assignment: bool = False,
):
    """Fence Project- and Task-derived authority before one user effect.

    Lock order is optional Worker node-control -> Project -> Task -> Worker ->
    membership -> User.  ``fence_worker_assignment`` is reserved for effects
    which create new durable work on the Task's current Worker; ordinary Task
    controls must remain usable while a Worker is being reconciled.
    TeamProjectShare revocation uses the Project row and TeamTaskShare
    revocation uses the Task row, so whichever transaction wins determines
    whether the effect is admitted.  A concurrent Project move is retried once
    under its new Project fence.
    """

    from backend.models.task import Task
    from backend.services.task_sharing import lock_task_share_authority

    if worker_node_fence_held and not fence_worker_node:
        raise ValueError(
            "worker_node_fence_held requires fence_worker_node"
        )

    observed = task
    for _attempt in range(2):
        task_id = observed.id
        expected_project_id = observed.project_id
        if not (_attempt == 0 and worker_node_fence_held):
            await db.rollback()
            if fence_worker_node:
                from backend.services.worker_node_control import (
                    fence_worker_node_mutation,
                )

                await fence_worker_node_mutation(db)
        if expected_project_id is not None:
            await _lock_project_effect_fence(expected_project_id, db)
        current = await db.get(Task, task_id, populate_existing=True)
        if current is None or not await lock_task_share_authority(db, current):
            raise HTTPException(404, "Task not found")
        if current.project_id != expected_project_id:
            observed = current
            continue
        _require_forwarded_task_incarnation(request, current)
        if fence_worker_assignment:
            from backend.services.worker_assignment import (
                fence_ready_worker_assignment,
            )

            await fence_ready_worker_assignment(db, current.worker_id)
        if not await _task_access_allowed(
            request,
            current,
            db,
            allow_chat_share=allow_chat_share,
            effect_fence=True,
        ):
            raise HTTPException(
                403,
                (
                    "No access to this task"
                    if allow_chat_share
                    else "No permission to control this task"
                ),
            )
        await lock_request_user_authority(request, db)
        return current
    await db.rollback()
    raise HTTPException(409, "Task Project changed while authorizing the effect")


async def lock_task_effect_accesses(
    request: Request,
    tasks: list,
    db,
    *,
    allow_chat_share: bool,
    fence_worker_node: bool = False,
    fence_worker_assignment: bool = False,
):
    """Fence authority for several Tasks in one globally ordered transaction.

    A related Plan decision authorizes both the Plan and its target. Calling
    :func:`lock_task_effect_access` twice cannot provide that guarantee: the
    second call deliberately rolls back before taking ``Project -> Task``
    locks, releasing the first Task's authority.  This helper locks every
    Worker node-control first when requested, every distinct Project next,
    then every Task in primary-key order, optionally every assigned Worker in
    primary-key order, followed by group membership and the current JWT User.
    A concurrent Project move is retried from a fresh transaction so the order
    never becomes ``Task -> Project``.
    """

    from backend.models.task import Task
    from backend.services.task_sharing import lock_task_share_authority

    if not tasks:
        raise ValueError("at least one Task is required")
    observed_by_id = {}
    requested_ids: list[int] = []
    for task in tasks:
        task_id = getattr(task, "id", None)
        if isinstance(task_id, bool) or not isinstance(task_id, int) or task_id <= 0:
            raise ValueError("Task identity is invalid")
        if task_id in observed_by_id:
            continue
        observed_by_id[task_id] = task
        requested_ids.append(task_id)

    for _attempt in range(2):
        expected_project_ids = {
            task_id: observed_by_id[task_id].project_id
            for task_id in requested_ids
        }
        await db.rollback()
        if fence_worker_node:
            from backend.services.worker_node_control import (
                fence_worker_node_mutation,
            )

            await fence_worker_node_mutation(db)
        for project_id in sorted(
            {
                project_id
                for project_id in expected_project_ids.values()
                if project_id is not None
            }
        ):
            await _lock_project_effect_fence(project_id, db)

        current_by_id = {}
        for task_id in sorted(requested_ids):
            current = await db.get(Task, task_id, populate_existing=True)
            if current is None or not await lock_task_share_authority(db, current):
                raise HTTPException(404, "Task not found")
            current_by_id[task_id] = current

        if any(
            current_by_id[task_id].project_id
            != expected_project_ids[task_id]
            for task_id in requested_ids
        ):
            observed_by_id = current_by_id
            continue

        if fence_worker_assignment:
            from backend.services.worker_assignment import (
                fence_ready_worker_assignment,
            )

            for worker_id in sorted(
                {
                    current.worker_id
                    for current in current_by_id.values()
                    if current.worker_id is not None
                }
            ):
                await fence_ready_worker_assignment(db, worker_id)

        for task_id in requested_ids:
            current = current_by_id[task_id]
            _require_forwarded_task_incarnation(request, current)
            if not await _task_access_allowed(
                request,
                current,
                db,
                allow_chat_share=allow_chat_share,
                effect_fence=True,
            ):
                raise HTTPException(
                    403,
                    (
                        "No access to this task"
                        if allow_chat_share
                        else "No permission to control this task"
                    ),
                )
        await lock_request_user_authority(request, db)
        return [current_by_id[task_id] for task_id in requested_ids]

    await db.rollback()
    raise HTTPException(409, "Task Project changed while authorizing the effect")


async def _task_access_allowed(
    request: Request,
    task,
    db,
    *,
    allow_chat_share: bool,
    effect_fence: bool = False,
) -> bool:
    if _internal_task_access_allowed(request, task):
        return True
    # A scoped child credential is an exact Task/incarnation capability, not
    # a user identity.  If that identity no longer matches, fail immediately;
    # it must never fall through to administrator, creator, Project, or share
    # authorization (and those paths may require a database session that a
    # rejected callback has no reason to touch).
    if getattr(request.state, "auth_type", None) == "internal_service":
        return False
    # The stable PR Monitor display Task is readable but never mutable. Its
    # visibility follows the ordinary Project attached to the monitored repo;
    # the internal PR-Monitor grouping Project and stale Task/Project shares
    # are deliberately ignored.
    from backend.services.pr_monitor_task_access import (
        is_pr_monitor_display_task,
        pr_monitor_display_project_id,
    )
    if await is_pr_monitor_display_task(db, task):
        if not allow_chat_share:
            return False
        if is_admin(request):
            return True
        project_id = await pr_monitor_display_project_id(db, task.id)
        if project_id is None:
            return False
        return await has_project_access(
            request,
            project_id,
            db,
            effect_fence=effect_fence,
        )
    if is_admin(request):
        # Preserve the existing administrator diagnostics/terminal-review
        # workflow.  The boundary below prevents member grants from turning
        # Controller records into collaborative Tasks; it is not an attempt
        # to hide Manager internals from administrators.
        return True
    # Automated PR Monitor Tasks are Controller implementation records.  A
    # Project is assigned only for internal grouping/routing; Project or Task
    # shares must never expose raw prompts, patches, or chat history.
    from backend.services.pr_monitor_task_access import is_pr_monitor_owned_task

    if await is_pr_monitor_owned_task(db, task):
        return False
    user_id = get_current_user_id(request)
    if not user_id:
        return False
    if task.created_by == user_id:
        return True
    if task.project_id and await has_project_access(
        request,
        task.project_id,
        db,
        effect_fence=effect_fence,
    ):
        return True
    if not allow_chat_share:
        return False

    from backend.models.team_share import TeamTaskShare

    if effect_fence:
        await _lock_user_group_membership_authority(user_id, db)

    user_group_ids = _member_group_ids(user_id)
    shared = (
        await db.execute(
            select(TeamTaskShare.id)
            .where(
                TeamTaskShare.task_id == task.id,
                TeamTaskShare.permission == "chat",
                (
                    (TeamTaskShare.target_type == "user")
                    & (TeamTaskShare.target_id == user_id)
                )
                | (
                    (TeamTaskShare.target_type == "group")
                    & TeamTaskShare.target_id.in_(user_group_ids)
                ),
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    return shared is not None


async def require_task_access(request: Request, task, db):
    """Allow task owners/project collaborators and chat-only recipients."""
    _require_forwarded_task_incarnation(request, task)
    if not await _task_access_allowed(
        request,
        task,
        db,
        allow_chat_share=True,
    ):
        raise HTTPException(403, "No access to this task")


async def require_task_control(request: Request, task, db):
    """Require ownership/collaboration rights, excluding chat-only shares."""
    _require_forwarded_task_incarnation(request, task)
    if not await _task_access_allowed(
        request,
        task,
        db,
        allow_chat_share=False,
    ):
        raise HTTPException(403, "No permission to control this task")


async def require_worker_access(request: Request, worker):
    """Raise 403 if user has no access to this worker."""
    if is_admin(request):
        return
    user_id = get_current_user_id(request)
    if worker.owner_user_id == user_id:
        return
    raise HTTPException(403, "No access to this worker")
