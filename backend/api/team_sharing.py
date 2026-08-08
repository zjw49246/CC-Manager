"""Team CCM sharing API — share Projects/Tasks to users/groups."""

import logging
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from backend.database import get_db
from backend.models.team_share import TeamProjectShare, TeamTaskShare
from backend.models.project import Project
from backend.models.task import Task
from backend.models.worker import Worker
from backend.api.deps import get_current_user_id, get_current_user_role

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/team", tags=["team-sharing"])


class ShareBody(BaseModel):
    target_type: Literal["user", "group"] = "user"
    target_id: int = Field(gt=0)
    permission: Literal["chat"] = "chat"


class UnshareBody(BaseModel):
    target_type: Literal["user", "group"] = "user"
    target_id: int = Field(gt=0)


async def _require_share_target(
    target_type: Literal["user", "group"],
    target_id: int,
    db: AsyncSession,
) -> None:
    if target_type == "user":
        from backend.models.user import User

        target = await db.get(User, target_id)
    else:
        from backend.models.user_group import UserGroup

        target = await db.get(UserGroup, target_id)
    if target is None or (
        target_type == "user"
        and not getattr(target, "is_active", False)
    ):
        raise HTTPException(404, "Share target not found")


async def _can_share_project(user_id: int | None, user_role: str, project_id: int, db: AsyncSession) -> bool:
    """Admin can share any project. Worker owner can share projects on their worker."""
    project = await db.get(Project, project_id)
    if project is None:
        return False
    if user_role in ("admin", "super_admin"):
        return True
    if not user_id or project.worker_id is None:
        return False
    worker = await db.get(Worker, project.worker_id)
    return bool(worker and worker.owner_user_id == user_id)


async def _can_share_task(user_id: int | None, user_role: str, task: Task, db: AsyncSession) -> bool:
    """Admin can share any task. Creator can share their own task."""
    if user_role in ("admin", "super_admin"):
        return True
    if not user_id:
        return False
    return task.created_by == user_id


# --- Project sharing ---

@router.post("/projects/{project_id}/share")
async def share_project(project_id: int, body: ShareBody, request: Request, db: AsyncSession = Depends(get_db)):
    user_id = get_current_user_id(request)
    user_role = get_current_user_role(request)
    if await db.get(Project, project_id) is None:
        raise HTTPException(404, "Project not found")
    if not await _can_share_project(user_id, user_role, project_id, db):
        raise HTTPException(403, "No permission to share this project")
    await _require_share_target(body.target_type, body.target_id, db)
    locked_project = (
        await db.execute(
            select(Project).where(Project.id == project_id).with_for_update()
        )
    ).scalar_one_or_none()
    if locked_project is None:
        raise HTTPException(404, "Project not found")
    # Lock current Tasks in a stable order. Grant replacement takes the same
    # Task lock, closing the share-vs-grant race on databases with row locks.
    await db.execute(
        select(Task.id)
        .where(Task.project_id == project_id)
        .order_by(Task.id)
        .with_for_update()
    )
    from backend.services.task_ssh_access import project_has_task_ssh_grants

    if await project_has_task_ssh_grants(db, project_id):
        raise HTTPException(
            409,
            "Remove SSH grants from this Project's Tasks before sharing it",
        )
    existing = await db.execute(
        select(TeamProjectShare).where(
            TeamProjectShare.project_id == project_id,
            TeamProjectShare.target_type == body.target_type,
            TeamProjectShare.target_id == body.target_id,
        )
    )
    if existing.scalar_one_or_none():
        return {"ok": True, "message": "Already shared"}
    db.add(TeamProjectShare(
        project_id=project_id,
        target_type=body.target_type,
        target_id=body.target_id,
        shared_by=user_id or 0,
    ))
    await db.commit()
    # Notify via Feishu
    if body.target_type == "user":
        try:
            from backend.services.feishu_notify import notify_project_shared
            from backend.models.user import User
            sharer = await db.get(User, user_id) if user_id else None
            proj = await db.get(Project, project_id)
            if proj:
                import asyncio
                asyncio.create_task(notify_project_shared(
                    sharer.name if sharer else "Admin",
                    proj.name,
                    body.target_id,
                ))
        except Exception:
            pass
    return {"ok": True}


@router.delete("/projects/{project_id}/share")
async def unshare_project(project_id: int, body: UnshareBody, request: Request, db: AsyncSession = Depends(get_db)):
    user_id = get_current_user_id(request)
    user_role = get_current_user_role(request)
    if await db.get(Project, project_id) is None:
        raise HTTPException(404, "Project not found")
    if not await _can_share_project(user_id, user_role, project_id, db):
        raise HTTPException(403, "No permission to manage this project's sharing")
    await db.execute(
        delete(TeamProjectShare).where(
            TeamProjectShare.project_id == project_id,
            TeamProjectShare.target_type == body.target_type,
            TeamProjectShare.target_id == body.target_id,
        )
    )
    await db.commit()
    # Notify via Feishu (only for user targets, skip self-revoke)
    if body.target_type == "user" and body.target_id != user_id:
        try:
            from backend.services.feishu_notify import notify_project_unshared
            from backend.models.user import User
            revoker = await db.get(User, user_id) if user_id else None
            proj = await db.get(Project, project_id)
            if proj:
                import asyncio
                asyncio.create_task(notify_project_unshared(
                    revoker.name if revoker else "Admin",
                    proj.name,
                    body.target_id,
                ))
        except Exception:
            pass
    return {"ok": True}


@router.get("/projects/{project_id}/shares")
async def list_project_shares(project_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    user_id = get_current_user_id(request)
    user_role = get_current_user_role(request)
    if await db.get(Project, project_id) is None:
        raise HTTPException(404, "Project not found")
    if not await _can_share_project(user_id, user_role, project_id, db):
        raise HTTPException(403, "No permission to view this project's shares")
    result = await db.execute(
        select(TeamProjectShare).where(TeamProjectShare.project_id == project_id)
    )
    shares = result.scalars().all()
    return [{"id": s.id, "target_type": s.target_type, "target_id": s.target_id,
             "shared_by": s.shared_by, "created_at": s.created_at.isoformat()} for s in shares]


# --- Task sharing ---

@router.post("/tasks/{task_id}/share")
async def share_task(task_id: int, body: ShareBody, request: Request, db: AsyncSession = Depends(get_db)):
    user_id = get_current_user_id(request)
    user_role = get_current_user_role(request)
    task = await db.get(Task, task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    if not await _can_share_task(user_id, user_role, task, db):
        raise HTTPException(403, "No permission to share this task")
    await _require_share_target(body.target_type, body.target_id, db)
    task = (
        await db.execute(
            select(Task).where(Task.id == task_id).with_for_update()
        )
    ).scalar_one_or_none()
    if task is None:
        raise HTTPException(404, "Task not found")
    from backend.services.task_ssh_access import task_has_any_ssh_grants

    if await task_has_any_ssh_grants(db, task_id):
        raise HTTPException(
            409,
            "Remove this Task's SSH grants before sharing it",
        )
    # Verify target has Project access
    if task.project_id:
        proj_share = await db.execute(
            select(TeamProjectShare).where(
                TeamProjectShare.project_id == task.project_id,
                TeamProjectShare.target_type == body.target_type,
                TeamProjectShare.target_id == body.target_id,
            )
        )
        if not proj_share.scalar_one_or_none():
            raise HTTPException(400, "Target does not have Project access. Share the Project first.")
    existing = await db.execute(
        select(TeamTaskShare).where(
            TeamTaskShare.task_id == task_id,
            TeamTaskShare.target_type == body.target_type,
            TeamTaskShare.target_id == body.target_id,
        )
    )
    if existing.scalar_one_or_none():
        return {"ok": True, "message": "Already shared"}
    db.add(TeamTaskShare(
        task_id=task_id,
        target_type=body.target_type,
        target_id=body.target_id,
        permission=body.permission,
        shared_by=user_id or 0,
    ))
    await db.commit()
    if body.target_type == "user":
        try:
            from backend.services.feishu_notify import notify_task_shared
            from backend.models.user import User
            sharer = await db.get(User, user_id) if user_id else None
            import asyncio
            asyncio.create_task(notify_task_shared(
                sharer.name if sharer else "Admin",
                task.title or f"Task #{task_id}",
                body.target_id,
            ))
        except Exception:
            pass
    return {"ok": True}


@router.delete("/tasks/{task_id}/share")
async def unshare_task(task_id: int, body: UnshareBody, request: Request, db: AsyncSession = Depends(get_db)):
    user_id = get_current_user_id(request)
    user_role = get_current_user_role(request)
    task = await db.get(Task, task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    if not await _can_share_task(user_id, user_role, task, db):
        raise HTTPException(403, "No permission to manage this task's sharing")
    await db.execute(
        delete(TeamTaskShare).where(
            TeamTaskShare.task_id == task_id,
            TeamTaskShare.target_type == body.target_type,
            TeamTaskShare.target_id == body.target_id,
        )
    )
    await db.commit()
    # Notify via Feishu (only for user targets, skip self-revoke)
    if body.target_type == "user" and body.target_id != user_id:
        try:
            from backend.services.feishu_notify import notify_task_unshared
            from backend.models.user import User
            revoker = await db.get(User, user_id) if user_id else None
            import asyncio
            asyncio.create_task(notify_task_unshared(
                revoker.name if revoker else "Admin",
                task.title or f"Task #{task_id}",
                body.target_id,
            ))
        except Exception:
            pass
    return {"ok": True}


@router.get("/tasks/{task_id}/shares")
async def list_task_shares(task_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    user_id = get_current_user_id(request)
    user_role = get_current_user_role(request)
    task = await db.get(Task, task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    if not await _can_share_task(user_id, user_role, task, db):
        raise HTTPException(403, "No permission to view this task's shares")
    result = await db.execute(
        select(TeamTaskShare).where(TeamTaskShare.task_id == task_id)
    )
    shares = result.scalars().all()
    return [{"id": s.id, "target_type": s.target_type, "target_id": s.target_id,
             "permission": s.permission, "shared_by": s.shared_by,
             "created_at": s.created_at.isoformat()} for s in shares]


# --- Users list (for share dialogs) ---

@router.get("/users")
async def list_users(request: Request, db: AsyncSession = Depends(get_db)):
    from backend.models.user import User
    result = await db.execute(select(User).where(User.is_active == True).order_by(User.id))
    users = result.scalars().all()
    return [{"id": u.id, "email": u.email, "name": u.name, "role": u.role,
             "avatar_url": u.avatar_url} for u in users]


# --- User role management (super_admin only can promote to admin) ---

class UpdateRoleBody(BaseModel):
    role: str  # 'admin' | 'member'


@router.put("/users/{user_id}/role")
async def update_user_role(user_id: int, body: UpdateRoleBody, request: Request, db: AsyncSession = Depends(get_db)):
    from backend.api.deps import is_super_admin, is_admin
    from backend.models.user import User

    if body.role == "admin" and not is_super_admin(request):
        raise HTTPException(403, "Only super admin can promote users to admin")
    if body.role == "member" and not is_admin(request):
        raise HTTPException(403, "Only admin can change roles")
    if body.role not in ("admin", "member"):
        raise HTTPException(400, "Role must be 'admin' or 'member'")

    user = await db.get(User, user_id)
    if not user:
        raise HTTPException(404, "User not found")
    if user.role == "super_admin":
        raise HTTPException(400, "Cannot change super admin role")

    user.role = body.role
    await db.commit()
    return {"ok": True, "user_id": user_id, "role": body.role}


# --- User groups (for quick batch sharing) ---

class GroupCreate(BaseModel):
    name: str
    description: str = ""


class GroupMemberAdd(BaseModel):
    user_id: int


@router.get("/groups")
async def list_groups(request: Request, db: AsyncSession = Depends(get_db)):
    from backend.models.user_group import UserGroup, UserGroupMember
    from backend.models.user import User
    result = await db.execute(select(UserGroup).order_by(UserGroup.name))
    groups = result.scalars().all()
    user_lookup = {}
    if groups:
        ur = await db.execute(select(User).where(User.is_active == True))
        user_lookup = {u.id: {"id": u.id, "name": u.name, "email": u.email, "avatar_url": u.avatar_url} for u in ur.scalars().all()}
    out = []
    for g in groups:
        mr = await db.execute(select(UserGroupMember).where(UserGroupMember.group_id == g.id))
        members = [user_lookup.get(m.user_id, {"id": m.user_id, "name": str(m.user_id), "email": "", "avatar_url": ""}) for m in mr.scalars().all()]
        out.append({"id": g.id, "name": g.name, "description": g.description, "members": members, "created_at": g.created_at.isoformat() if g.created_at else ""})
    return out


@router.post("/groups")
async def create_group(body: GroupCreate, request: Request, db: AsyncSession = Depends(get_db)):
    from backend.api.deps import is_admin as _is_admin
    if not _is_admin(request):
        raise HTTPException(403, "Admin only")
    from backend.models.user_group import UserGroup
    existing = await db.execute(select(UserGroup).where(UserGroup.name == body.name))
    if existing.scalar_one_or_none():
        raise HTTPException(400, f"Group '{body.name}' already exists")
    group = UserGroup(name=body.name, description=body.description, created_by=get_current_user_id(request))
    db.add(group)
    await db.commit()
    await db.refresh(group)
    return {"id": group.id, "name": group.name, "description": group.description}


@router.put("/groups/{group_id}")
async def update_group(group_id: int, body: GroupCreate, request: Request, db: AsyncSession = Depends(get_db)):
    from backend.api.deps import is_admin as _is_admin
    if not _is_admin(request):
        raise HTTPException(403, "Admin only")
    from backend.models.user_group import UserGroup
    group = await db.get(UserGroup, group_id)
    if not group:
        raise HTTPException(404, "Group not found")
    group.name = body.name
    group.description = body.description
    await db.commit()
    return {"id": group.id, "name": group.name, "description": group.description}


@router.delete("/groups/{group_id}")
async def delete_group(group_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    from backend.api.deps import is_admin as _is_admin
    if not _is_admin(request):
        raise HTTPException(403, "Admin only")
    from backend.models.user_group import UserGroup, UserGroupMember
    group = await db.get(UserGroup, group_id)
    if not group:
        raise HTTPException(404, "Group not found")
    await db.execute(delete(UserGroupMember).where(UserGroupMember.group_id == group_id))
    await db.delete(group)
    await db.commit()
    return {"ok": True}


@router.post("/groups/{group_id}/members")
async def add_group_member(group_id: int, body: GroupMemberAdd, request: Request, db: AsyncSession = Depends(get_db)):
    from backend.api.deps import is_admin as _is_admin
    if not _is_admin(request):
        raise HTTPException(403, "Admin only")
    from backend.models.user_group import UserGroup, UserGroupMember
    group = await db.get(UserGroup, group_id)
    if not group:
        raise HTTPException(404, "Group not found")
    existing = await db.execute(
        select(UserGroupMember).where(UserGroupMember.group_id == group_id, UserGroupMember.user_id == body.user_id)
    )
    if existing.scalar_one_or_none():
        return {"ok": True, "message": "Already a member"}
    db.add(UserGroupMember(group_id=group_id, user_id=body.user_id))
    await db.commit()
    return {"ok": True}


@router.delete("/groups/{group_id}/members/{user_id}")
async def remove_group_member(group_id: int, user_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    from backend.api.deps import is_admin as _is_admin
    if not _is_admin(request):
        raise HTTPException(403, "Admin only")
    from backend.models.user_group import UserGroupMember
    await db.execute(
        delete(UserGroupMember).where(UserGroupMember.group_id == group_id, UserGroupMember.user_id == user_id)
    )
    await db.commit()
    return {"ok": True}
