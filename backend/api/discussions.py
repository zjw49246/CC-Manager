import asyncio

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.database import get_db
from backend.api.deps import (
    get_current_user_id,
    get_current_user_role,
    require_admin,
    require_project_access,
)
from backend.models.discussion import (
    Discussion,
    DiscussionAgent,
    DiscussionEvent,
    DiscussionMessage,
)
from backend.models.project import Project
from backend.schemas.discussion import (
    DiscussionCreate,
    DiscussionOut,
    DiscussionSendMessage,
)
from backend.services.discussion_service import (
    DISCUSSION_ACTIVE_STATUS,
    DISCUSSION_CLOSED_STATUS,
    DISCUSSION_CLOSING_STATUS,
    DiscussionProcessCleanupError,
    DiscussionSecurityError,
)

router = APIRouter(prefix="/api/discussions", tags=["discussions"])

_discussion_service = None


def _get_service():
    global _discussion_service
    if _discussion_service is None:
        from backend.main import broadcaster, dispatcher
        from backend.database import async_session
        from backend.services.discussion_service import DiscussionService
        _discussion_service = DiscussionService(
            db_factory=async_session,
            broadcaster=broadcaster,
            claude_pool_provider=lambda: dispatcher.pool,
        )
    return _discussion_service


async def _require_discussion_owner(request: Request, discussion: Discussion):
    """Only creator or admin can mutate a discussion."""
    role = get_current_user_role(request)
    if role in ("admin", "super_admin"):
        return
    user_id = get_current_user_id(request)
    if discussion.creator_user_id == user_id:
        return
    raise HTTPException(403, "Only the discussion creator or admin can perform this action")


@router.get("")
async def list_discussions(request: Request, db: AsyncSession = Depends(get_db)):
    user_id = get_current_user_id(request)
    user_role = get_current_user_role(request)
    stmt = select(Discussion).order_by(Discussion.created_at.desc())
    if user_role not in ("admin", "super_admin"):
        stmt = stmt.where(Discussion.creator_user_id == user_id)
    result = await db.execute(stmt)
    discussions = result.scalars().all()
    out = []
    for d in discussions:
        agents = await _get_agents(db, d.id)
        msg_count = await _count_messages(db, d.id)
        out.append({
            "id": d.id,
            "title": d.title,
            "project_id": d.project_id,
            "max_agents": d.max_agents,
            "facilitator_model": d.facilitator_model,
            "agent_model": d.agent_model,
            "status": d.status,
            "created_at": d.created_at.isoformat() if d.created_at else "",
            "agent_count": len(agents),
            "message_count": msg_count,
        })
    return out


@router.post("", status_code=201)
async def create_discussion(
    data: DiscussionCreate, request: Request, db: AsyncSession = Depends(get_db)
):
    # Discussions launch independent provider workloads.  Worker ownership is
    # infrastructure allocation, and TeamProjectShare is only a Project ACL;
    # neither is authority to create this Manager-level workload.
    require_admin(request)
    if data.project_id is not None:
        await require_project_access(request, data.project_id, db)
        # Access checks are read-only.  End that snapshot before taking the
        # cross-process Project writer fence used by first-share admission.
        await db.rollback()
        from backend.services.project_share_admission import (
            lock_project_share_authority,
            project_has_active_share,
        )

        try:
            await lock_project_share_authority(db, data.project_id)
        except ValueError as exc:
            await db.rollback()
            raise HTTPException(404, "Project not found") from exc
        try:
            shared = await project_has_active_share(db, data.project_id)
        except Exception as exc:
            await db.rollback()
            raise HTTPException(
                503,
                "Could not verify Project sharing state; Discussion creation "
                "is disabled",
            ) from exc
        if shared:
            await db.rollback()
            raise HTTPException(
                409,
                "Cannot create a provider-capable Discussion for a shared Project",
            )
    disc = Discussion(
        title=data.title,
        project_id=data.project_id,
        max_agents=data.max_agents,
        facilitator_model=data.facilitator_model,
        creator_user_id=get_current_user_id(request),
        agent_model=data.agent_model,
        status="active",
    )
    db.add(disc)
    await db.commit()
    await db.refresh(disc)
    return {
        "id": disc.id,
        "title": disc.title,
        "project_id": disc.project_id,
        "max_agents": disc.max_agents,
        "facilitator_model": disc.facilitator_model,
        "agent_model": disc.agent_model,
        "status": disc.status,
        "created_at": disc.created_at.isoformat() if disc.created_at else "",
        "agent_count": 0,
        "message_count": 0,
    }


@router.get("/{discussion_id}")
async def get_discussion(
    discussion_id: int, request: Request, db: AsyncSession = Depends(get_db)
):
    disc = await db.get(Discussion, discussion_id)
    if not disc:
        raise HTTPException(status_code=404, detail="Discussion not found")
    await _require_discussion_owner(request, disc)

    messages = await _get_messages(db, discussion_id)
    agents = await _get_agents(db, discussion_id)

    return {
        "id": disc.id,
        "title": disc.title,
        "project_id": disc.project_id,
        "max_agents": disc.max_agents,
        "facilitator_model": disc.facilitator_model,
        "agent_model": disc.agent_model,
        "status": disc.status,
        "created_at": disc.created_at.isoformat() if disc.created_at else "",
        "messages": messages,
        "agents": agents,
    }


@router.post("/{discussion_id}/messages")
async def send_broadcast_message(
    discussion_id: int,
    data: DiscussionSendMessage,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    require_admin(request)
    disc = await db.get(Discussion, discussion_id)
    if not disc:
        raise HTTPException(status_code=404, detail="Discussion not found")
    await _require_discussion_owner(request, disc)

    service = _get_service()
    try:
        agents = await service.send_broadcast(db, discussion_id, data.message)
    except DiscussionSecurityError as exc:
        raise HTTPException(409, str(exc)) from exc
    return {
        "ok": True,
        "agents": [
            {
                "id": a.id,
                "role_name": a.role_name,
                "status": a.status,
            }
            for a in agents
        ],
    }


@router.post("/{discussion_id}/agents/{agent_id}/chat")
async def send_agent_chat(
    discussion_id: int, request: Request,
    agent_id: int,
    data: DiscussionSendMessage,
    db: AsyncSession = Depends(get_db),
):
    require_admin(request)
    disc = await db.get(Discussion, discussion_id)
    if disc:
        await _require_discussion_owner(request, disc)
    agent = await db.get(DiscussionAgent, agent_id)
    if not agent or agent.discussion_id != discussion_id:
        raise HTTPException(status_code=404, detail="Agent not found")
    if agent.status == "running":
        raise HTTPException(status_code=409, detail="Agent is already running")
    if not agent.session_id:
        raise HTTPException(status_code=400, detail="Agent has no session to resume")

    service = _get_service()
    try:
        await service.send_to_agent(db, agent_id, data.message)
    except DiscussionSecurityError as exc:
        raise HTTPException(409, str(exc)) from exc
    return {"ok": True}


@router.post("/{discussion_id}/agents/{agent_id}/trigger")
async def trigger_agent(
    discussion_id: int, request: Request,
    agent_id: int,
    db: AsyncSession = Depends(get_db),
):
    require_admin(request)
    disc = await db.get(Discussion, discussion_id)
    if disc:
        await _require_discussion_owner(request, disc)
    agent = await db.get(DiscussionAgent, agent_id)
    if not agent or agent.discussion_id != discussion_id:
        raise HTTPException(status_code=404, detail="Agent not found")
    if agent.status == "running":
        raise HTTPException(status_code=409, detail="Agent is already running")

    service = _get_service()
    try:
        await service.trigger_agent(db, agent_id)
    except DiscussionSecurityError as exc:
        raise HTTPException(409, str(exc)) from exc
    return {"ok": True}


@router.post("/{discussion_id}/resume-all")
async def resume_all_agents(
    discussion_id: int, request: Request,
    db: AsyncSession = Depends(get_db),
):
    require_admin(request)
    disc = await db.get(Discussion, discussion_id)
    if disc:
        await _require_discussion_owner(request, disc)
    disc = await db.get(Discussion, discussion_id)
    if not disc:
        raise HTTPException(status_code=404, detail="Discussion not found")

    result = await db.execute(
        select(DiscussionAgent).where(
            DiscussionAgent.discussion_id == discussion_id,
            DiscussionAgent.status == "idle",
            DiscussionAgent.session_id.isnot(None),
        )
    )
    idle_agents = result.scalars().all()
    if not idle_agents:
        return {"ok": True, "resumed": 0}

    service = _get_service()
    resumed = 0
    for agent in idle_agents:
        try:
            await service.trigger_agent(db, agent.id)
            resumed += 1
        except DiscussionSecurityError as exc:
            raise HTTPException(409, str(exc)) from exc
        except Exception:
            pass
    return {"ok": True, "resumed": resumed}


@router.post("/{discussion_id}/add-agent")
async def add_agent(
    discussion_id: int, request: Request,
    db: AsyncSession = Depends(get_db),
):
    require_admin(request)
    disc = await db.get(Discussion, discussion_id)
    if disc:
        await _require_discussion_owner(request, disc)
    disc = await db.get(Discussion, discussion_id)
    if not disc:
        raise HTTPException(status_code=404, detail="Discussion not found")

    service = _get_service()
    try:
        agent = await service.add_agent(db, discussion_id)
    except DiscussionSecurityError as exc:
        raise HTTPException(409, str(exc)) from exc
    return {
        "ok": True,
        "agent": {
            "id": agent.id,
            "role_name": agent.role_name,
            "status": agent.status,
        },
    }


@router.post("/{discussion_id}/agents/{agent_id}/stop")
async def stop_agent(
    discussion_id: int, request: Request,
    agent_id: int,
    db: AsyncSession = Depends(get_db),
):
    disc = await db.get(Discussion, discussion_id)
    if disc:
        await _require_discussion_owner(request, disc)
    agent = await db.get(DiscussionAgent, agent_id)
    if not agent or agent.discussion_id != discussion_id:
        raise HTTPException(status_code=404, detail="Agent not found")

    service = _get_service()
    await service.stop_agent(agent_id)
    return {"ok": True}


@router.get("/{discussion_id}/agents/{agent_id}/events")
async def get_agent_events(
    discussion_id: int,
    agent_id: int,
    request: Request,
    limit: int = 500,
    db: AsyncSession = Depends(get_db),
):
    discussion = await db.get(Discussion, discussion_id)
    if discussion is None:
        raise HTTPException(status_code=404, detail="Discussion not found")
    await _require_discussion_owner(request, discussion)
    result = await db.execute(
        select(DiscussionEvent)
        .where(
            DiscussionEvent.discussion_id == discussion_id,
            DiscussionEvent.agent_id == agent_id,
        )
        .order_by(DiscussionEvent.id)
        .limit(limit)
    )
    events = result.scalars().all()
    return [_event_to_dict(e) for e in events]


@router.delete("/{discussion_id}")
async def delete_discussion(
    discussion_id: int, request: Request, db: AsyncSession = Depends(get_db)
):
    disc = await db.get(Discussion, discussion_id)
    if not disc:
        raise HTTPException(status_code=404, detail="Discussion not found")
    await _require_discussion_owner(request, disc)
    project_id = disc.project_id

    # Phase 1 publishes a durable, cross-process lease before any in-memory
    # runtime lock is taken.  Holding SQLite's Project writer transaction while
    # waiting for consumer finalization would deadlock those consumers on their
    # own terminal DB writes, so commit ``closing`` first.  Both active and
    # closing rows block first-share admission; provider launch accepts only
    # active rows.
    await db.rollback()
    project_fenced = False
    if project_id is not None:
        from backend.services.project_share_admission import (
            lock_project_share_authority,
        )

        try:
            await lock_project_share_authority(db, project_id)
            project_fenced = True
        except ValueError as exc:
            # Historical versions could delete the Project first and strand a
            # Discussion whose non-FK logical project_id still names it.  A
            # missing Project has no writer row left to lock, so fall back to
            # the Discussion row itself.  If the Project still exists, failure
            # to establish its shared writer fence remains a fail-closed 409.
            await db.rollback()
            project_still_exists = await db.get(Project, project_id)
            await db.rollback()
            if project_still_exists is not None:
                raise HTTPException(
                    409,
                    "Could not establish the Discussion Project deletion fence; retry",
                ) from exc
    if not project_fenced:
        fenced = await db.execute(
            update(Discussion)
            .where(
                Discussion.id == discussion_id,
                (
                    Discussion.project_id.is_(None)
                    if project_id is None
                    else Discussion.project_id == project_id
                ),
            )
            .values(id=Discussion.id)
        )
        if fenced.rowcount not in {0, 1}:
            await db.rollback()
            raise HTTPException(409, "Discussion deletion fence changed; retry")

    current = (
        await db.execute(
            select(Discussion)
            .where(Discussion.id == discussion_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if current is None:
        await db.rollback()
        raise HTTPException(status_code=404, detail="Discussion not found")
    if current.project_id != project_id:
        await db.rollback()
        raise HTTPException(409, "Discussion Project changed during deletion")
    if current.status not in {
        DISCUSSION_ACTIVE_STATUS,
        DISCUSSION_CLOSING_STATUS,
        DISCUSSION_CLOSED_STATUS,
    }:
        await db.rollback()
        raise HTTPException(409, "Discussion has an invalid lifecycle status")
    current.status = DISCUSSION_CLOSING_STATUS
    await db.commit()

    service = _get_service()
    try:
        async with service.deletion_barrier(discussion_id, db):
            # Re-read the complete graph while launch admission remains
            # fenced by the durable closing lease plus Discussion/Agent locks.
            agents_result = await db.execute(
                select(DiscussionAgent).where(
                    DiscussionAgent.discussion_id == discussion_id
                )
            )
            for agent in agents_result.scalars().all():
                await db.delete(agent)

            events_result = await db.execute(
                select(DiscussionEvent).where(
                    DiscussionEvent.discussion_id == discussion_id
                )
            )
            for event in events_result.scalars().all():
                await db.delete(event)

            msgs_result = await db.execute(
                select(DiscussionMessage).where(
                    DiscussionMessage.discussion_id == discussion_id
                )
            )
            for message in msgs_result.scalars().all():
                await db.delete(message)

            current = await db.get(
                Discussion,
                discussion_id,
                populate_existing=True,
            )
            if current is None:
                await db.rollback()
                raise HTTPException(status_code=404, detail="Discussion not found")
            if current.status != DISCUSSION_CLOSING_STATUS:
                await db.rollback()
                raise HTTPException(
                    409,
                    "Discussion deletion lease changed; retry",
                )
            await db.delete(current)
            await db.commit()
    except asyncio.CancelledError:
        # Phase 1 is already committed.  Preserve closing so no provider or
        # first-share admission can run; a later DELETE safely retries cleanup.
        raise
    except DiscussionProcessCleanupError as exc:
        raise HTTPException(
            409,
            "Discussion cleanup is incomplete and remains closing; retry DELETE: "
            f"{exc}",
        ) from exc
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            503,
            "Discussion deletion is incomplete and remains closing; retry DELETE",
        ) from exc
    return {"ok": True}


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

async def _get_messages(db: AsyncSession, discussion_id: int) -> list[dict]:
    result = await db.execute(
        select(DiscussionMessage)
        .where(DiscussionMessage.discussion_id == discussion_id)
        .order_by(DiscussionMessage.id)
    )
    return [
        {
            "id": m.id,
            "discussion_id": m.discussion_id,
            "role": m.role,
            "agent_role_name": m.agent_role_name,
            "content": m.content,
            "created_at": m.created_at.isoformat() if m.created_at else "",
        }
        for m in result.scalars().all()
    ]


async def _get_agents(db: AsyncSession, discussion_id: int) -> list[dict]:
    result = await db.execute(
        select(DiscussionAgent)
        .where(DiscussionAgent.discussion_id == discussion_id)
        .order_by(DiscussionAgent.id)
    )
    return [
        {
            "id": a.id,
            "discussion_id": a.discussion_id,
            "role_name": a.role_name,
            "session_id": a.session_id,
            "status": a.status,
            "created_at": a.created_at.isoformat() if a.created_at else "",
        }
        for a in result.scalars().all()
    ]


async def _count_messages(db: AsyncSession, discussion_id: int) -> int:
    from sqlalchemy import func
    result = await db.execute(
        select(func.count(DiscussionMessage.id))
        .where(DiscussionMessage.discussion_id == discussion_id)
    )
    return result.scalar() or 0


def _event_to_dict(e: DiscussionEvent) -> dict:
    ts = ""
    if e.timestamp:
        ts = e.timestamp.isoformat()
        if not ts.endswith("Z") and "+" not in ts:
            ts += "Z"
    return {
        "id": e.id,
        "discussion_id": e.discussion_id,
        "agent_id": e.agent_id,
        "event_type": e.event_type,
        "role": e.role,
        "content": e.content,
        "tool_name": e.tool_name,
        "tool_input": e.tool_input,
        "tool_output": e.tool_output,
        "is_error": e.is_error,
        "timestamp": ts,
    }
