"""Team CCM sharing API — share Projects/Tasks to users/groups."""

import logging
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import select, delete, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from backend.database import get_db
from backend.models.team_share import TeamProjectShare, TeamTaskShare
from backend.models.project import Project
from backend.models.task import Task
from backend.api.deps import (
    get_current_user_id,
    get_current_user_role,
    lock_request_user_authority,
    require_admin,
)
from backend.services.task_sharing import (
    _writable_share_block_reason,
    lock_task_share_authority,
)
from backend.services.project_share_admission import (
    ProjectShareAdmissionError,
    lock_project_share_authority,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/team", tags=["team-sharing"])


class ShareBody(BaseModel):
    target_type: Literal["user", "group"] = "user"
    target_id: int = Field(gt=0)
    permission: Literal["chat"] = "chat"


class UnshareBody(BaseModel):
    target_type: Literal["user", "group"] = "user"
    target_id: int = Field(gt=0)


async def _lock_actor_and_user_target(
    request: Request,
    target_id: int,
    db: AsyncSession,
    *,
    require_target_active: bool,
) -> None:
    """Lock a JWT actor and User target in deterministic id order.

    Share, Group membership, and role mutation can touch two User rows with
    opposite semantic roles.  Locking "actor then target" lets concurrent
    A→B and B→A operations deadlock on row-locking databases.  Numeric order
    keeps those paths compatible while retaining the actor's exact-role fence.
    """

    from backend.models.user import User

    actor_is_jwt = getattr(request.state, "auth_type", None) == "jwt"
    actor_id = get_current_user_id(request) if actor_is_jwt else None
    actor_role = get_current_user_role(request) if actor_is_jwt else None
    if actor_is_jwt and (
        isinstance(actor_id, bool)
        or not isinstance(actor_id, int)
        or actor_id <= 0
        or actor_role not in {"member", "admin", "super_admin"}
    ):
        raise HTTPException(403, "User authority is invalid")

    user_ids = sorted({target_id, *([actor_id] if actor_id is not None else [])})
    for user_id in user_ids:
        predicates = [User.id == user_id]
        if user_id == actor_id:
            predicates.extend((User.is_active.is_(True), User.role == actor_role))
        elif require_target_active:
            predicates.append(User.is_active.is_(True))
        locked = await db.execute(
            update(User)
            .where(*predicates)
            .values(id=User.id)
            .execution_options(synchronize_session=False)
        )
        if locked.rowcount != 1:
            if user_id == actor_id:
                raise HTTPException(
                    409,
                    "User was disabled or changed role while authorizing "
                    "the effect",
                )
            detail = "Active user not found" if require_target_active else "User not found"
            raise HTTPException(404, detail)


async def _lock_group_target_then_actor(
    request: Request,
    target_id: int,
    db: AsyncSession,
) -> None:
    """Use the common resource -> Group -> actor User lock order."""

    from backend.models.user_group import UserGroup

    locked = await db.execute(
        update(UserGroup)
        .where(UserGroup.id == target_id)
        .values(id=UserGroup.id)
    )
    if locked.rowcount != 1:
        raise HTTPException(404, "Share target not found")
    await lock_request_user_authority(request, db)


async def _lock_project_share_authority(
    project_id: int,
    db: AsyncSession,
) -> Project:
    """Lock Project before target/grant rows, including on SQLite."""

    try:
        return await lock_project_share_authority(db, project_id)
    except ProjectShareAdmissionError as exc:
        raise HTTPException(409, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(404, "Project not found") from exc


async def _can_share_project(user_id: int | None, user_role: str, project_id: int, db: AsyncSession) -> bool:
    """Only administrators may grant or inspect Project membership."""
    return (
        user_role in ("admin", "super_admin")
        and await db.get(Project, project_id) is not None
    )


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
    require_admin(request)
    user_id = get_current_user_id(request)
    user_role = get_current_user_role(request)
    project = await db.get(Project, project_id)
    if project is None:
        raise HTTPException(404, "Project not found")
    from backend.models.project import project_is_internal

    if project_is_internal(project):
        raise HTTPException(409, "Internal Projects cannot be shared")
    if not await _can_share_project(user_id, user_role, project_id, db):
        raise HTTPException(403, "No permission to share this project")
    project = await _lock_project_share_authority(project_id, db)
    if project_is_internal(project):
        raise HTTPException(409, "Internal Projects cannot be shared")
    if not await _can_share_project(user_id, user_role, project_id, db):
        raise HTTPException(403, "No permission to share this project")
    if body.target_type == "user":
        await _lock_actor_and_user_target(
            request,
            body.target_id,
            db,
            require_target_active=True,
        )
    else:
        await _lock_group_target_then_actor(request, body.target_id, db)
    existing = await db.execute(
        select(TeamProjectShare)
        .where(
            TeamProjectShare.project_id == project_id,
            TeamProjectShare.target_type == body.target_type,
            TeamProjectShare.target_id == body.target_id,
        )
        .with_for_update()
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
    require_admin(request)
    user_id = get_current_user_id(request)
    user_role = get_current_user_role(request)
    if await db.get(Project, project_id) is None:
        raise HTTPException(404, "Project not found")
    if not await _can_share_project(user_id, user_role, project_id, db):
        raise HTTPException(403, "No permission to manage this project's sharing")
    await _lock_project_share_authority(project_id, db)
    if not await _can_share_project(user_id, user_role, project_id, db):
        raise HTTPException(403, "No permission to manage this project's sharing")
    await lock_request_user_authority(request, db)
    shares = (
        await db.execute(
            select(TeamProjectShare)
            .where(
                TeamProjectShare.project_id == project_id,
                TeamProjectShare.target_type == body.target_type,
                TeamProjectShare.target_id == body.target_id,
            )
            .with_for_update()
        )
    ).scalars().all()
    for share in shares:
        await db.delete(share)
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
    require_admin(request)
    user_id = get_current_user_id(request)
    user_role = get_current_user_role(request)
    if await db.get(Project, project_id) is None:
        raise HTTPException(404, "Project not found")
    if not await _can_share_project(user_id, user_role, project_id, db):
        raise HTTPException(403, "No permission to view this project's shares")
    await _lock_project_share_authority(project_id, db)
    if not await _can_share_project(user_id, user_role, project_id, db):
        raise HTTPException(403, "No permission to view this project's shares")
    await lock_request_user_authority(request, db)
    result = await db.execute(
        select(TeamProjectShare)
        .where(TeamProjectShare.project_id == project_id)
        .with_for_update()
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
    if not await lock_task_share_authority(db, task):
        raise HTTPException(409, "Task changed while sharing")
    # The first check avoids taking a write lock for an obviously unauthorized
    # caller.  This second check is authoritative: the Task may have changed
    # while this request waited for the Task -> grant lock order.
    if not await _can_share_task(user_id, user_role, task, db):
        raise HTTPException(403, "No permission to share this task")
    from backend.api.tasks import _require_not_isolated_browser_child

    await _require_not_isolated_browser_child(db, task, action="shared")
    blocked = _writable_share_block_reason(task)
    if blocked is not None:
        raise HTTPException(409, blocked)
    if body.target_type == "user":
        await _lock_actor_and_user_target(
            request,
            body.target_id,
            db,
            require_target_active=True,
        )
    else:
        await _lock_group_target_then_actor(request, body.target_id, db)
    existing = await db.execute(
        select(TeamTaskShare)
        .where(
            TeamTaskShare.task_id == task_id,
            TeamTaskShare.target_type == body.target_type,
            TeamTaskShare.target_id == body.target_id,
        )
        .with_for_update()
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
    if not await lock_task_share_authority(db, task):
        raise HTTPException(409, "Task changed while unsharing")
    if not await _can_share_task(user_id, user_role, task, db):
        raise HTTPException(403, "No permission to manage this task's sharing")
    await lock_request_user_authority(request, db)
    shares = (
        await db.execute(
            select(TeamTaskShare)
            .where(
                TeamTaskShare.task_id == task_id,
                TeamTaskShare.target_type == body.target_type,
                TeamTaskShare.target_id == body.target_id,
            )
            .with_for_update()
        )
    ).scalars().all()
    for share in shares:
        await db.delete(share)
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
    if not await lock_task_share_authority(db, task):
        raise HTTPException(409, "Task changed while listing shares")
    if not await _can_share_task(user_id, user_role, task, db):
        raise HTTPException(403, "No permission to view this task's shares")
    await lock_request_user_authority(request, db)
    result = await db.execute(
        select(TeamTaskShare)
        .where(TeamTaskShare.task_id == task_id)
        .with_for_update()
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

    await _lock_actor_and_user_target(
        request,
        user_id,
        db,
        require_target_active=False,
    )
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
    await lock_request_user_authority(request, db)
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
    await db.rollback()
    locked = await db.execute(
        update(UserGroup)
        .where(UserGroup.id == group_id)
        .values(id=UserGroup.id)
    )
    if locked.rowcount != 1:
        raise HTTPException(404, "Group not found")
    await lock_request_user_authority(request, db)
    group = await db.get(UserGroup, group_id, populate_existing=True)
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
    await db.rollback()
    locked = await db.execute(
        update(UserGroup)
        .where(UserGroup.id == group_id)
        .values(id=UserGroup.id)
    )
    if locked.rowcount != 1:
        raise HTTPException(404, "Group not found")
    await lock_request_user_authority(request, db)
    group = await db.get(UserGroup, group_id, populate_existing=True)
    # Team share tables deliberately have no foreign keys.  Purge group-target
    # grants in the same transaction so a reused group id cannot inherit them.
    await db.execute(delete(TeamTaskShare).where(
        TeamTaskShare.target_type == "group",
        TeamTaskShare.target_id == group_id,
    ))
    await db.execute(delete(TeamProjectShare).where(
        TeamProjectShare.target_type == "group",
        TeamProjectShare.target_id == group_id,
    ))
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

    # Group is the stable serialization point for concurrent membership adds.
    # Taking a real writer lock before checking for an existing row makes
    # duplicate requests idempotent on SQLite, PostgreSQL, and MySQL alike.
    locked_group = await db.execute(
        update(UserGroup)
        .where(UserGroup.id == group_id)
        .values(id=UserGroup.id)
    )
    if locked_group.rowcount != 1:
        raise HTTPException(404, "Group not found")
    # Membership must never be staged for an absent/future or disabled User.
    # Locking actor and target by numeric User id also prevents reciprocal
    # role/membership operations from taking the two rows in opposite order.
    await _lock_actor_and_user_target(
        request,
        body.user_id,
        db,
        require_target_active=True,
    )
    existing = await db.execute(
        select(UserGroupMember).where(UserGroupMember.group_id == group_id, UserGroupMember.user_id == body.user_id)
    )
    if existing.scalar_one_or_none():
        return {"ok": True, "message": "Already a member"}
    db.add(UserGroupMember(group_id=group_id, user_id=body.user_id))
    try:
        await db.commit()
    except IntegrityError:
        # The database constraint is the last line of defence for imports or a
        # second process that did not participate in the Group writer lock.
        # Normalize only the exact duplicate into the endpoint's idempotent
        # response; unrelated integrity failures must remain visible.
        await db.rollback()
        duplicate = await db.scalar(
            select(UserGroupMember.id).where(
                UserGroupMember.group_id == group_id,
                UserGroupMember.user_id == body.user_id,
            ).limit(1)
        )
        if duplicate is not None:
            return {"ok": True, "message": "Already a member"}
        raise
    return {"ok": True}


@router.delete("/groups/{group_id}/members/{user_id}")
async def remove_group_member(group_id: int, user_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    from backend.api.deps import is_admin as _is_admin
    if not _is_admin(request):
        raise HTTPException(403, "Admin only")
    from backend.models.user_group import UserGroup, UserGroupMember
    locked_group = await db.execute(
        update(UserGroup)
        .where(UserGroup.id == group_id)
        .values(id=UserGroup.id)
    )
    if locked_group.rowcount != 1:
        raise HTTPException(404, "Group not found")
    await lock_request_user_authority(request, db)
    await db.execute(
        delete(UserGroupMember).where(UserGroupMember.group_id == group_id, UserGroupMember.user_id == user_id)
    )
    await db.commit()
    return {"ok": True}
