"""Task/Project sharing service — create shares, push to recipients, revoke."""

import logging
import secrets

import httpx
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.models.feishu_binding import FeishuUserBinding
from backend.models.task import Task
from backend.models.project import Project
from backend.models.task_share import TaskShare, ProjectShare
from backend.services.pr_review_runtime import is_pr_sandbox_task
from backend.services.project_share_admission import (
    lock_project_share_authority,
    project_has_active_share,
    require_project_agents_quiescent,
)

logger = logging.getLogger(__name__)


async def lock_task_share_authority(db: AsyncSession, task: Task) -> bool:
    """Fence a share write to the Task incarnation that authorized it.

    The no-op UPDATE is intentional: it takes the Task write lock before an
    ACL row is inserted, including on SQLite where ``FOR UPDATE`` is ignored.
    The random durable incarnation distinguishes a deleted Task from a later
    row that explicitly reused the same integer primary key.
    """

    incarnation_id = getattr(task, "incarnation_id", None)
    incarnation_predicate = (
        Task.incarnation_id.is_(None)
        if incarnation_id is None
        else Task.incarnation_id == incarnation_id
    )
    locked = await db.execute(
        update(Task)
        .where(Task.id == task.id, incarnation_predicate)
        .values(id=Task.id)
    )
    if locked.rowcount != 1:
        await db.rollback()
        return False
    await db.refresh(task)
    return True


def _writable_share_block_reason(task: Task) -> str | None:
    metadata = task.metadata_ if isinstance(task.metadata_, dict) else {}
    if metadata.get("isolated_browser_agent") is True:
        return (
            "Isolated Browser Agent Tasks cannot be shared as writable chat "
            "sessions; use their Harness owner"
        )
    if task.mode == "delivery_loop" or task.delivery_run_id is not None:
        return (
            "Delivery-owned Tasks cannot be shared as writable remote chat "
            "sessions; use Delivery Run controls"
        )
    if is_pr_sandbox_task(task):
        return (
            "Automated PR workflow Tasks cannot be shared as writable remote "
            "chat sessions; use the workflow result"
        )
    return None


async def _get_my_identity(db: AsyncSession) -> dict | None:
    result = await db.execute(select(FeishuUserBinding).limit(1))
    binding = result.scalar_one_or_none()
    if not binding:
        return None
    return {
        "open_id": binding.feishu_open_id,
        "name": binding.feishu_name or "",
        "avatar_url": binding.avatar_url or "",
    }


async def share_task(
    db: AsyncSession,
    task_id: int,
    targets: list[dict],
) -> list[dict]:
    """Share a task with one or more members.

    targets: list of {"open_id": str, "name": str, "ccm_url": str}
    Returns list of created share records (as dicts).
    """
    task = (
        await db.execute(
            select(Task).where(Task.id == task_id).with_for_update()
        )
    ).scalar_one_or_none()
    if not task:
        raise ValueError(f"Task {task_id} not found")
    if not await lock_task_share_authority(db, task):
        raise ValueError(f"Task {task_id} changed while sharing")
    from backend.services.task_ssh_access import task_has_any_ssh_grants

    if await task_has_any_ssh_grants(db, task_id):
        raise ValueError(
            "Remove this Task's SSH grants before sharing it"
        )
    blocked = _writable_share_block_reason(task)
    if blocked is not None:
        raise ValueError(blocked)

    identity = await _get_my_identity(db)
    if not identity:
        raise ValueError("Feishu not bound — cannot share")

    project_name = None
    if task.project_id:
        project = await db.get(Project, task.project_id)
        if project:
            project_name = project.name

    my_url = (settings.public_base_url or "").rstrip("/")

    created = []
    for target in targets:
        open_id = target["open_id"]
        # Skip sharing to self
        target_url = (target.get("ccm_url") or "").rstrip("/")
        if my_url and target_url and target_url == my_url:
            logger.debug("Skipping self-share to %s", target_url)
            continue
        # Skip if already shared to this person
        existing = await db.execute(
            select(TaskShare).where(
                TaskShare.task_id == task_id,
                TaskShare.shared_to_open_id == open_id,
                TaskShare.status == "active",
            ).with_for_update()
        )
        if existing.scalar_one_or_none():
            continue

        # Reactivate revoked share or create new
        revoked = await db.execute(
            select(TaskShare).where(
                TaskShare.task_id == task_id,
                TaskShare.shared_to_open_id == open_id,
                TaskShare.status == "revoked",
            ).with_for_update()
        )
        share = revoked.scalar_one_or_none()
        if share:
            share.status = "active"
            share.share_token = secrets.token_urlsafe(32)
            share.shared_to_name = target.get("name")
            share.shared_to_ccm_url = target["ccm_url"]
        else:
            share = TaskShare(
                task_id=task_id,
                shared_to_open_id=open_id,
                shared_to_name=target.get("name"),
                shared_to_ccm_url=target["ccm_url"],
                share_token=secrets.token_urlsafe(32),
            )
            db.add(share)

        await db.flush()

        # Push to recipient CCM (best-effort)
        pushed = await _push_share_to_recipient(
            ccm_url=target["ccm_url"],
            payload={
                "owner_ccm_url": settings.public_base_url,
                "owner_name": identity["name"],
                "owner_feishu_open_id": identity["open_id"],
                "remote_task_id": task_id,
                "share_token": share.share_token,
                "task_title": task.title,
                "task_description": task.description,
                "project_name": project_name,
            },
        )

        created.append({
            "id": share.id,
            "task_id": task_id,
            "shared_to_open_id": open_id,
            "shared_to_name": target.get("name"),
            "share_token": share.share_token,
            "pushed": pushed,
        })

    await db.commit()
    return created


async def revoke_task_share(
    db: AsyncSession,
    task_id: int,
    open_id: str,
) -> bool:
    task = await db.get(Task, task_id)
    if task is None or not await lock_task_share_authority(db, task):
        return False
    result = await db.execute(
        select(TaskShare).where(
            TaskShare.task_id == task_id,
            TaskShare.shared_to_open_id == open_id,
            TaskShare.status == "active",
        ).with_for_update()
    )
    share = result.scalar_one_or_none()
    if not share:
        return False

    share.status = "revoked"
    await db.commit()

    # Notify recipient to remove (best-effort)
    await _push_revoke_to_recipient(
        ccm_url=share.shared_to_ccm_url,
        owner_ccm_url=settings.public_base_url,
        remote_task_id=task_id,
    )
    return True


async def get_task_shares(db: AsyncSession, task_id: int) -> list[dict]:
    task = await db.get(Task, task_id)
    if task is None or not await lock_task_share_authority(db, task):
        return []
    result = await db.execute(
        select(TaskShare).where(
            TaskShare.task_id == task_id,
            TaskShare.status == "active",
        ).with_for_update()
    )
    shares = result.scalars().all()
    return [
        {
            "id": s.id,
            "shared_to_open_id": s.shared_to_open_id,
            "shared_to_name": s.shared_to_name,
            "shared_to_ccm_url": s.shared_to_ccm_url,
            "created_at": s.created_at.isoformat() if s.created_at else None,
        }
        for s in shares
    ]


# ---------- Project sharing ----------

async def share_project(
    db: AsyncSession,
    project_id: int,
    targets: list[dict],
    *,
    instance_manager=None,
    dispatcher=None,
) -> list[dict]:
    """Share a project (and all its current tasks) with members."""
    project = await lock_project_share_authority(db, project_id)
    # Only the visibility transition needs a bare-Agent veto. Once any Team
    # or Feishu share is active, adding/retrying another recipient remains
    # idempotent and must not reject a future legitimately isolated runtime.
    if not await project_has_active_share(db, project_id):
        await require_project_agents_quiescent(
            db,
            project,
            instance_manager=instance_manager,
            dispatcher=dispatcher,
        )
    await db.execute(
        select(Task.id)
        .where(Task.project_id == project_id)
        .order_by(Task.id)
        .with_for_update()
    )
    from backend.services.task_ssh_access import project_has_task_ssh_grants

    if await project_has_task_ssh_grants(db, project_id):
        raise ValueError(
            "Remove SSH grants from this Project's Tasks before sharing it"
        )

    created = []
    for target in targets:
        open_id = target["open_id"]
        existing = await db.execute(
            select(ProjectShare).where(
                ProjectShare.project_id == project_id,
                ProjectShare.shared_to_open_id == open_id,
                ProjectShare.status == "active",
            )
        )
        if existing.scalar_one_or_none():
            continue

        revoked = await db.execute(
            select(ProjectShare).where(
                ProjectShare.project_id == project_id,
                ProjectShare.shared_to_open_id == open_id,
                ProjectShare.status == "revoked",
            )
        )
        ps = revoked.scalar_one_or_none()
        if ps:
            ps.status = "active"
            ps.shared_to_name = target.get("name")
            ps.shared_to_ccm_url = target["ccm_url"]
        else:
            ps = ProjectShare(
                project_id=project_id,
                shared_to_open_id=open_id,
                shared_to_name=target.get("name"),
                shared_to_ccm_url=target["ccm_url"],
            )
            db.add(ps)
        await db.flush()

        created.append({
            "id": ps.id,
            "project_id": project_id,
            "shared_to_open_id": open_id,
            "shared_to_name": target.get("name"),
        })

    await db.commit()

    # Share all tasks in this project
    task_result = await db.execute(
        select(Task).where(Task.project_id == project_id)
    )
    tasks = task_result.scalars().all()
    for task in tasks:
        # Project visibility may be shared, but Controller-owned workflow
        # Tasks must never become writable remote shadow sessions.
        if _writable_share_block_reason(task) is not None:
            continue
        await share_task(db, task.id, targets)

    return created


async def revoke_project_share(
    db: AsyncSession,
    project_id: int,
    open_id: str,
) -> bool:
    result = await db.execute(
        select(ProjectShare).where(
            ProjectShare.project_id == project_id,
            ProjectShare.shared_to_open_id == open_id,
            ProjectShare.status == "active",
        )
    )
    ps = result.scalar_one_or_none()
    if not ps:
        return False

    ps.status = "revoked"

    # Revoke all task shares under this project for the same recipient
    task_result = await db.execute(
        select(Task).where(Task.project_id == project_id)
    )
    tasks = task_result.scalars().all()
    for task in tasks:
        await revoke_task_share(db, task.id, open_id)

    await db.commit()
    return True


async def get_project_shares(db: AsyncSession, project_id: int) -> list[dict]:
    result = await db.execute(
        select(ProjectShare).where(
            ProjectShare.project_id == project_id,
            ProjectShare.status == "active",
        )
    )
    shares = result.scalars().all()
    return [
        {
            "id": s.id,
            "shared_to_open_id": s.shared_to_open_id,
            "shared_to_name": s.shared_to_name,
            "shared_to_ccm_url": s.shared_to_ccm_url,
            "created_at": s.created_at.isoformat() if s.created_at else None,
        }
        for s in shares
    ]


# ---------- Push helpers ----------

async def _push_share_to_recipient(ccm_url: str, payload: dict) -> bool:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{ccm_url}/api/shared/receive",
                json=payload,
            )
            resp.raise_for_status()
        return True
    except Exception:
        logger.warning("Failed to push share to %s: %s", ccm_url, payload.get("remote_task_id"))
        return False


async def _push_revoke_to_recipient(ccm_url: str, owner_ccm_url: str, remote_task_id: int) -> bool:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{ccm_url}/api/shared/revoke",
                json={
                    "owner_ccm_url": owner_ccm_url,
                    "remote_task_id": remote_task_id,
                },
            )
            resp.raise_for_status()
        return True
    except Exception:
        logger.warning("Failed to push revoke to %s", ccm_url)
        return False


async def validate_share_token(db: AsyncSession, task_id: int, token: str) -> TaskShare | None:
    """Validate a share_token for a given task. Returns the TaskShare if valid."""
    result = await db.execute(
        select(TaskShare).where(
            TaskShare.task_id == task_id,
            TaskShare.share_token == token,
            TaskShare.status == "active",
        )
    )
    return result.scalar_one_or_none()
