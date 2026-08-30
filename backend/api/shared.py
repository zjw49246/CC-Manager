"""Shared tasks API — receiver-side endpoints for tasks shared TO this CCM."""

import asyncio
import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.database import get_db
from backend.models.task_share import SharedTaskReceived

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/shared", tags=["shared"])


class ReceiveSharePayload(BaseModel):
    owner_ccm_url: str
    owner_name: str | None = None
    owner_feishu_open_id: str | None = None
    remote_task_id: int
    share_token: str
    task_title: str | None = None
    task_description: str | None = None
    project_name: str | None = None


class RevokeSharePayload(BaseModel):
    owner_ccm_url: str
    remote_task_id: int


@router.post("/receive")
async def receive_share(payload: ReceiveSharePayload, db: AsyncSession = Depends(get_db)):
    """Called by the sharer's CCM to push a share notification."""
    from backend.config import settings
    my_url = (settings.public_base_url or "").rstrip("/")
    incoming_url = (payload.owner_ccm_url or "").rstrip("/")
    if my_url and incoming_url and incoming_url == my_url:
        return {"ok": True, "skipped": "self-share"}

    from backend.models.task import Task

    result = await db.execute(
        select(SharedTaskReceived)
        .where(
            SharedTaskReceived.owner_ccm_url == payload.owner_ccm_url,
            SharedTaskReceived.remote_task_id == payload.remote_task_id,
        )
        .with_for_update()
    )
    existing = result.scalar_one_or_none()
    if existing:
        existing.share_token = payload.share_token
        existing.task_title = payload.task_title
        existing.task_description = payload.task_description
        existing.project_name = payload.project_name
        existing.owner_name = payload.owner_name
        existing.owner_feishu_open_id = payload.owner_feishu_open_id
        existing.status = "active"

        # ``local_task_id`` is only a hint until the Task proves the reverse
        # owner.  Repair legacy/deleted shadows instead of attaching a relay to
        # an unrelated Task that happens to have the same integer id.
        shadow = None
        if existing.local_task_id is not None:
            shadow = (
                await db.execute(
                    select(Task).where(
                        Task.id == existing.local_task_id,
                        Task.shared_from_id == existing.id,
                    )
                )
            ).scalar_one_or_none()
        if shadow is None:
            from backend.services.task_creation import stage_task_record

            shadow = await stage_task_record(
                db,
                title=payload.task_title or "",
                description=payload.task_description,
                status="pending",
                shared_from_id=existing.id,
            )
            existing.local_task_id = shadow.id
        await db.commit()
        await db.refresh(existing)
        # Replace any connection carrying the previous token before the new
        # detached identity is allowed to write.
        if existing.local_task_id:
            try:
                from backend.main import shared_relay
                if shared_relay:
                    await shared_relay.stop_relay(existing.id)
                    asyncio.create_task(
                        _start_relay_and_backfill(shared_relay, existing)
                    )
            except Exception:
                pass
        return {"ok": True}

    # New share: create shared_tasks_received + shadow task
    shared = SharedTaskReceived(**payload.model_dump())
    db.add(shared)
    await db.flush()

    # Create the shadow through the canonical boundary so a reused SQLite id
    # cannot inherit stale TaskShare/TeamTaskShare authority.
    from backend.services.task_creation import stage_task_record

    shadow = await stage_task_record(
        db,
        title=payload.task_title or "",
        description=payload.task_description,
        status="pending",
        shared_from_id=shared.id,
    )
    shared.local_task_id = shadow.id
    await db.commit()
    await db.refresh(shared)

    # Start relay + backfill in background
    try:
        from backend.main import shared_relay
        if shared_relay:
            asyncio.create_task(_start_relay_and_backfill(shared_relay, shared))
    except Exception:
        pass

    return {"ok": True}


async def _start_relay_and_backfill(relay, shared: SharedTaskReceived):
    """Background: fetch initial state, backfill history, start relay."""
    try:
        # Fetch live task config to update shadow
        from backend.services.shared_proxy import proxy_config
        from backend.database import async_session
        config = await proxy_config(shared.owner_ccm_url, shared.remote_task_id, shared.share_token)
        async with async_session() as db:
            from backend.services.shared_shadow import lock_owned_shadow

            owned = await lock_owned_shadow(db, shared)
            if owned is not None and config:
                _, shadow = owned
                shadow.status = config.get("status", "pending")
                shadow.title = config.get("title") or shadow.title
                shadow.description = config.get("description") or shadow.description
                shadow.model = config.get("model")
                shadow.provider = config.get("provider", "claude")
                shadow.session_id = config.get("session_id") or shadow.session_id
                shadow.target_repo = config.get("target_repo")
                shadow.error_message = config.get("error_message")
                shadow.attention_tag = config.get("attention_tag")
                await db.commit()
    except Exception:
        logger.debug("failed to fetch initial config for shared %d", shared.id)

    try:
        await relay.backfill_history(shared)
    except Exception:
        logger.debug("backfill failed for shared %d", shared.id)

    try:
        await relay.start_relay(shared)
    except Exception:
        logger.debug("start relay failed for shared %d", shared.id)


@router.post("/revoke")
async def receive_revoke(payload: RevokeSharePayload, db: AsyncSession = Depends(get_db)):
    """Called by the sharer's CCM to revoke a share."""
    result = await db.execute(
        select(SharedTaskReceived).where(
            SharedTaskReceived.owner_ccm_url == payload.owner_ccm_url,
            SharedTaskReceived.remote_task_id == payload.remote_task_id,
        )
    )
    record = result.scalar_one_or_none()
    if record:
        await _cleanup_shared(record, db)
    return {"ok": True}


@router.get("/tasks")
async def list_shared_tasks(enrich: bool = False, db: AsyncSession = Depends(get_db)):
    """List all tasks shared to this CCM.

    enrich=true: fetch live task info from each sharer's CCM (slower but complete).
    """
    result = await db.execute(
        select(SharedTaskReceived).where(
            SharedTaskReceived.status == "active"
        ).order_by(SharedTaskReceived.received_at.desc())
    )
    tasks = result.scalars().all()

    if not enrich:
        return {
            "tasks": [
                {
                    "id": t.id,
                    "owner_ccm_url": t.owner_ccm_url,
                    "owner_name": t.owner_name,
                    "remote_task_id": t.remote_task_id,
                    "share_token": t.share_token,
                    "local_task_id": t.local_task_id,
                    "task_title": t.task_title,
                    "task_description": t.task_description,
                    "project_name": t.project_name,
                    "received_at": t.received_at.isoformat() if t.received_at else None,
                }
                for t in tasks
            ]
        }

    # Enrich: fetch live config from each sharer in parallel
    import asyncio as _aio
    from backend.services.shared_proxy import proxy_config

    async def _enrich_one(t: SharedTaskReceived) -> dict:
        base = {
            "id": t.id,
            "owner_ccm_url": t.owner_ccm_url,
            "owner_name": t.owner_name,
            "remote_task_id": t.remote_task_id,
            "share_token": t.share_token,
            "local_task_id": t.local_task_id,
            "received_at": t.received_at.isoformat() if t.received_at else None,
        }
        try:
            config = await proxy_config(t.owner_ccm_url, t.remote_task_id, t.share_token)
            base["remote_task"] = config
        except Exception:
            base["remote_task"] = {
                "id": t.remote_task_id,
                "title": t.task_title,
                "description": t.task_description,
                "status": "unknown",
                "project_name": t.project_name,
            }
        return base

    enriched = await _aio.gather(*[_enrich_one(t) for t in tasks])
    return {"tasks": list(enriched)}


@router.delete("/{shared_id}")
async def leave_shared_task(shared_id: int, db: AsyncSession = Depends(get_db)):
    """Voluntarily leave a shared task."""
    record = await db.get(SharedTaskReceived, shared_id)
    if not record:
        raise HTTPException(404, "Shared task not found")
    await _cleanup_shared(record, db)
    return {"ok": True}


async def _cleanup_shared(record: SharedTaskReceived, db: AsyncSession):
    """Stop relay, cancel shadow task, delete shared record."""
    try:
        from backend.main import shared_relay
        if shared_relay:
            await shared_relay.stop_relay(record.id)
    except Exception:
        pass
    from backend.models.task import Task
    from backend.services.shared_shadow import lock_shared_record

    current = await lock_shared_record(db, record, require_active=False)
    if current is None:
        await db.rollback()
        return
    if current.local_task_id:
        shadow = (
            await db.execute(
                select(Task)
                .where(
                    Task.id == current.local_task_id,
                    Task.shared_from_id == current.id,
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        if shadow is not None:
            shadow.status = "cancelled"
            shadow.error_message = "Share revoked"
    await db.delete(current)
    await db.commit()


# ---------- Proxy endpoints: forward requests to the sharer's CCM ----------

async def _get_shared_record(shared_id: int, db: AsyncSession) -> SharedTaskReceived:
    record = await db.get(SharedTaskReceived, shared_id)
    if not record or record.status != "active":
        raise HTTPException(404, "Shared task not found")
    return record


@router.get("/{shared_id}/history")
async def proxy_history(
    shared_id: int,
    limit: int = 0,
    before_id: int = 0,
    compact: bool = True,
    db: AsyncSession = Depends(get_db),
):
    """Proxy chat history from the sharer's CCM."""
    record = await _get_shared_record(shared_id, db)
    from backend.services.shared_proxy import proxy_history as _proxy
    try:
        return await _proxy(
            record.owner_ccm_url, record.remote_task_id, record.share_token,
            limit=limit, before_id=before_id, compact=compact,
        )
    except Exception as e:
        logger.warning("proxy_history failed for shared %d: %s", shared_id, e)
        raise HTTPException(502, f"Cannot reach sharer CCM: {e}")


class SharedChatBody(BaseModel):
    message: str


@router.post("/{shared_id}/chat")
async def proxy_chat(
    shared_id: int,
    body: SharedChatBody,
    db: AsyncSession = Depends(get_db),
):
    """Proxy a chat message to the sharer's CCM."""
    record = await _get_shared_record(shared_id, db)

    # Get my feishu name for the [sender] prefix
    from backend.models.feishu_binding import FeishuUserBinding
    binding_result = await db.execute(select(FeishuUserBinding).limit(1))
    binding = binding_result.scalar_one_or_none()
    sender_name = binding.feishu_name if binding else None

    from backend.services.shared_proxy import proxy_chat as _proxy
    try:
        return await _proxy(
            record.owner_ccm_url, record.remote_task_id, record.share_token,
            message=body.message, sender_name=sender_name,
        )
    except Exception as e:
        logger.warning("proxy_chat failed for shared %d: %s", shared_id, e)
        raise HTTPException(502, f"Cannot reach sharer CCM: {e}")


@router.get("/{shared_id}/config")
async def proxy_config(
    shared_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Proxy task config from the sharer's CCM."""
    record = await _get_shared_record(shared_id, db)
    from backend.services.shared_proxy import proxy_config as _proxy
    try:
        return await _proxy(
            record.owner_ccm_url, record.remote_task_id, record.share_token,
        )
    except Exception as e:
        logger.warning("proxy_config failed for shared %d: %s", shared_id, e)
        raise HTTPException(502, f"Cannot reach sharer CCM: {e}")


@router.get("/{shared_id}/ping")
async def ping_sharer(shared_id: int, db: AsyncSession = Depends(get_db)):
    """Check if the sharer's CCM is online."""
    record = await _get_shared_record(shared_id, db)
    import httpx
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(f"{record.owner_ccm_url}/api/system/health")
            resp.raise_for_status()
        return {"online": True}
    except Exception:
        return {"online": False}
