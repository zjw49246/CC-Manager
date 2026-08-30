from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from backend.database import get_db
from backend.api.deps import require_task_access
from backend.models.task import Task
from backend.models.sub_agent import SubAgentReport, SubAgentSession
from backend.schemas.monitor_session import (
    MonitorCheckResponse,
    MonitorSessionResponse,
)

router = APIRouter(prefix="/api/tasks/{task_id}/sub-agents", tags=["sub-agents"])


@router.get("/sessions", response_model=list[MonitorSessionResponse])
async def list_all_sub_agent_sessions(
    task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Return the generic read model across CCM and provider-native agents."""

    task = await db.get(Task, task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    await require_task_access(request, task, db)
    rows = await db.execute(
        select(SubAgentSession)
        .where(SubAgentSession.task_id == task_id)
        .order_by(SubAgentSession.created_at.desc(), SubAgentSession.id.desc())
    )
    return list(rows.scalars())


@router.get(
    "/sessions/{session_id}/reports",
    response_model=list[MonitorCheckResponse],
)
async def list_all_sub_agent_reports(
    task_id: int,
    session_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Return reports for one row in the generic sub-agent read model."""

    task = await db.get(Task, task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    await require_task_access(request, task, db)
    session = await db.scalar(
        select(SubAgentSession).where(
            SubAgentSession.id == session_id,
            SubAgentSession.task_id == task_id,
        )
    )
    if session is None:
        raise HTTPException(404, "Sub-agent session not found")
    rows = await db.execute(
        select(SubAgentReport)
        .where(SubAgentReport.session_id == session_id)
        .order_by(SubAgentReport.created_at.desc(), SubAgentReport.id.desc())
    )
    return list(rows.scalars())


@router.get("/summary")
async def get_sub_agent_summary(
    task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """按类别汇总该 task 的子 agent（monitor / native-agent / native-monitor / ...）。"""
    task = await db.get(Task, task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    await require_task_access(request, task, db)

    rows = (
        await db.execute(
            select(
                SubAgentSession.agent_type,
                SubAgentSession.status,
                func.count().label("n"),
            )
            .where(SubAgentSession.task_id == task_id)
            .group_by(SubAgentSession.agent_type, SubAgentSession.status)
        )
    ).all()

    by_type: dict = {}
    for agent_type, status, n in rows:
        # running/completed 恒存在（前端直接读这两个键），其余状态按实际值附加
        bucket = by_type.setdefault(agent_type, {"running": 0, "completed": 0})
        bucket[status] = bucket.get(status, 0) + n

    return {"by_type": by_type}
