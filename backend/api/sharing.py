"""Sharing API — sharer-side endpoints for task/project sharing."""

import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.database import get_db
from backend.services import task_sharing, feishu_notify
from backend.services.project_share_admission import ProjectShareAdmissionError
from backend.models.feishu_binding import FeishuUserBinding

logger = logging.getLogger(__name__)

router = APIRouter(tags=["sharing"])


class ShareTarget(BaseModel):
    open_id: str
    name: str | None = None
    ccm_url: str


class ShareRequest(BaseModel):
    targets: list[ShareTarget]


# ---------- Task sharing ----------

@router.post("/api/tasks/{task_id}/share")
async def share_task(task_id: int, req: ShareRequest, db: AsyncSession = Depends(get_db)):
    targets = [t.model_dump() for t in req.targets]
    try:
        created = await task_sharing.share_task(db, task_id, targets)
    except ValueError as e:
        raise HTTPException(400, str(e))

    # Send Feishu DM notifications (best-effort)
    result = await db.execute(select(FeishuUserBinding).limit(1))
    binding = result.scalar_one_or_none()
    sharer_name = binding.feishu_name if binding else "Someone"
    from backend.models.task import Task
    task = await db.get(Task, task_id)
    task_title = task.title if task else f"Task #{task_id}"

    from backend.config import settings
    for share in created:
        await feishu_notify.send_share_notification(
            recipient_open_id=share["shared_to_open_id"],
            sharer_name=sharer_name,
            task_title=task_title,
            ccm_url=settings.public_base_url,
        )

    return {"shares": created}


@router.delete("/api/tasks/{task_id}/share/{open_id}")
async def revoke_task_share(task_id: int, open_id: str, db: AsyncSession = Depends(get_db)):
    ok = await task_sharing.revoke_task_share(db, task_id, open_id)
    if not ok:
        raise HTTPException(404, "Share not found or already revoked")
    return {"ok": True}


@router.get("/api/tasks/{task_id}/shares")
async def get_task_shares(task_id: int, db: AsyncSession = Depends(get_db)):
    shares = await task_sharing.get_task_shares(db, task_id)
    return {"shares": shares}


# ---------- Project sharing ----------

@router.post("/api/projects/{project_id}/share")
async def share_project(project_id: int, req: ShareRequest, db: AsyncSession = Depends(get_db)):
    targets = [t.model_dump() for t in req.targets]
    try:
        created = await task_sharing.share_project(db, project_id, targets)
    except ProjectShareAdmissionError as e:
        raise HTTPException(409, str(e)) from e
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    return {"shares": created}


@router.delete("/api/projects/{project_id}/share/{open_id}")
async def revoke_project_share(project_id: int, open_id: str, db: AsyncSession = Depends(get_db)):
    ok = await task_sharing.revoke_project_share(db, project_id, open_id)
    if not ok:
        raise HTTPException(404, "Share not found or already revoked")
    return {"ok": True}


@router.get("/api/projects/{project_id}/shares")
async def get_project_shares(project_id: int, db: AsyncSession = Depends(get_db)):
    shares = await task_sharing.get_project_shares(db, project_id)
    return {"shares": shares}
