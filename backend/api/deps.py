"""Shared FastAPI dependencies for user context and resource ownership."""

from fastapi import HTTPException, Request
from sqlalchemy import select


def get_current_user_id(request: Request) -> int | None:
    return getattr(request.state, "user_id", None)


def get_current_user_role(request: Request) -> str:
    return getattr(request.state, "user_role", "member")


def is_admin(request: Request) -> bool:
    """Both admin and super_admin have admin-level permissions."""
    return get_current_user_role(request) in ("admin", "super_admin")


def is_super_admin(request: Request) -> bool:
    """Only super_admin can promote users to admin."""
    return get_current_user_role(request) == "super_admin"


def require_admin(request: Request):
    """Raise 403 if not admin/super_admin."""
    if not is_admin(request):
        raise HTTPException(403, "Admin only")


def require_internal_service(request: Request) -> None:
    """Allow scoped CCM callbacks (and the legacy deployment credential).

    Auth-disabled deployments intentionally retain their historical open
    semantics. New child processes receive an exact-route credential labelled
    ``internal_service``; ``token`` remains for deployment/Worker compatibility.
    """
    from backend.config import settings

    if settings.auth_token and getattr(request.state, "auth_type", None) not in {
        "token",
        "internal_service",
    }:
        raise HTTPException(403, "Internal service authentication required")


def _internal_task_access_allowed(request: Request, task_id: int) -> bool:
    if getattr(request.state, "auth_type", None) != "internal_service":
        return False
    from backend.services.internal_service_auth import internal_task_id

    return internal_task_id(
        getattr(request.state, "internal_service_claims", None)
    ) == task_id


def _member_group_ids(user_id: int):
    from backend.models.user_group import UserGroupMember

    return select(UserGroupMember.group_id).where(
        UserGroupMember.user_id == user_id
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
) -> bool:
    """Return whether the current identity may access one exact Project."""
    if is_admin(request):
        return True
    user_id = get_current_user_id(request)
    if not user_id:
        return False

    from backend.models.project import Project
    from backend.models.team_share import TeamProjectShare
    from backend.models.worker import Worker

    project = await db.get(Project, project_id)
    if project is None:
        return False
    if project.worker_id is not None:
        worker = await db.get(Worker, project.worker_id)
        if worker and worker.owner_user_id == user_id:
            return True

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


async def _task_access_allowed(
    request: Request,
    task,
    db,
    *,
    allow_chat_share: bool,
) -> bool:
    if _internal_task_access_allowed(request, task.id):
        return True
    if is_admin(request):
        return True
    user_id = get_current_user_id(request)
    if not user_id:
        return False
    if task.created_by == user_id:
        return True
    if task.worker_id is not None and await has_worker_access(
        request,
        task.worker_id,
        db,
    ):
        return True
    if task.project_id and await has_project_access(
        request,
        task.project_id,
        db,
    ):
        return True
    if not allow_chat_share:
        return False

    from backend.models.team_share import TeamTaskShare

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
    if not await _task_access_allowed(
        request,
        task,
        db,
        allow_chat_share=True,
    ):
        raise HTTPException(403, "No access to this task")


async def require_task_control(request: Request, task, db):
    """Require ownership/collaboration rights, excluding chat-only shares."""
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
