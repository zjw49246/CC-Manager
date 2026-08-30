"""Shared-access API — sharer-side endpoints accessed by remote CCMs via share_token.

These endpoints are PUBLIC (no admin auth_token needed). Authentication is
via the share_token query parameter, validated against the task_shares table.
"""

import json
import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import and_, not_, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.database import get_db
from backend.models.task import Task
from backend.models.task_share import TaskShare
from backend.models.log_entry import LogEntry
from backend.services.chat_event_identity import persisted_chat_event
from backend.services.stream_parser import detect_assistant_protocol_anomaly
from backend.services.worker_proxy import get_task_operation_lock

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/shared-access", tags=["shared-access"])


async def _validate_token(task_id: int, token: str, db: AsyncSession) -> TaskShare:
    result = await db.execute(
        select(TaskShare).where(
            TaskShare.task_id == task_id,
            TaskShare.share_token == token,
            TaskShare.status == "active",
        )
    )
    share = result.scalar_one_or_none()
    if not share:
        raise HTTPException(403, "Invalid or revoked share token")
    return share


@router.get("/{task_id}/history")
async def shared_history(
    task_id: int,
    token: str = Query(...),
    limit: int = 0,
    before_id: int = 0,
    compact: bool = True,
    db: AsyncSession = Depends(get_db),
):
    """Chat history for a shared task — same format as /api/tasks/{id}/chat/history."""
    await _validate_token(task_id, token, db)

    task = await db.get(Task, task_id)
    if not task:
        raise HTTPException(404, "Task not found")

    allowed = ["user_message", "message", "result", "tool_use", "tool_result", "system_init", "system_event", "thinking", "process_exit"]
    noisy_system = ["task_progress", "thinking_tokens", "token_usage", "api_request", "api_response"]
    cols = [
        LogEntry.id, LogEntry.role, LogEntry.event_type, LogEntry.content,
        LogEntry.tool_name, LogEntry.tool_input, LogEntry.tool_output,
        LogEntry.is_error, LogEntry.loop_iteration, LogEntry.timestamp,
        LogEntry.raw_json, LogEntry.task_retry_count,
    ]
    conditions = [
        LogEntry.task_id == task_id,
        LogEntry.event_type.in_(allowed),
        not_(and_(
            LogEntry.event_type == "system_event",
            LogEntry.content.in_(noisy_system),
        )),
    ]
    if before_id > 0:
        conditions.append(LogEntry.id < before_id)

    if limit > 0:
        stmt = (
            select(*cols)
            .where(*conditions)
            .order_by(LogEntry.id.desc())
            .limit(limit + 20)
        )
        result = await db.execute(stmt)
        rows = list(reversed(result.all()))
    else:
        stmt = (
            select(*cols)
            .where(*conditions)
            .order_by(LogEntry.id.asc())
        )
        result = await db.execute(stmt)
        rows = result.all()

    _TRUNCATE = 20_000

    messages = []
    for row in rows:
        tool_input = row.tool_input
        tool_output = row.tool_output
        if compact and row.event_type in ("tool_use", "tool_result"):
            if tool_input and len(tool_input) > 300:
                tool_input = tool_input[:300] + "..."
            if tool_output and len(tool_output) > 300:
                tool_output = tool_output[:300] + "..."
        else:
            if tool_input and len(tool_input) > _TRUNCATE:
                tool_input = tool_input[:_TRUNCATE] + f"\n... (truncated from {len(row.tool_input)} chars)"
            if tool_output and len(tool_output) > _TRUNCATE:
                tool_output = tool_output[:_TRUNCATE] + f"\n... (truncated from {len(row.tool_output)} chars)"

        msg: dict = {
            "id": row.id,
            "role": row.role,
            "event_type": row.event_type,
            "content": row.content,
            "tool_name": row.tool_name,
            "tool_input": tool_input,
            "tool_output": tool_output,
            "is_error": row.is_error,
            "loop_iteration": row.loop_iteration,
            "task_retry_count": row.task_retry_count,
            "timestamp": row.timestamp.isoformat() if row.timestamp else None,
            "protocol_anomaly": detect_assistant_protocol_anomaly(
                row.event_type,
                row.role,
                row.content,
                provider=task.provider,
            ),
        }
        if row.raw_json:
            try:
                raw = json.loads(row.raw_json)
                if raw.get("attachments"):
                    msg["attachments"] = raw["attachments"]
                if isinstance(raw.get("raw_content"), str):
                    msg["raw_content"] = raw["raw_content"]
                if isinstance(raw.get("sender_name"), str):
                    msg["sender_name"] = raw["sender_name"]
            except (json.JSONDecodeError, TypeError):
                pass
        messages.append(msg)

    return messages


class SharedChatMessage(BaseModel):
    message: str
    sender_name: str | None = None


@router.post("/{task_id}/chat")
async def shared_chat(
    task_id: int,
    body: SharedChatMessage,
    token: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """Send a message to a shared task — injected via the dispatcher queue."""
    # The PR completion consumer uses this same lock for its exact
    # reviewing->publishing->terminal transition.  Whichever side wins makes
    # one complete admission decision before a user row can be persisted.
    await db.rollback()
    async with get_task_operation_lock(task_id):
        db.expire_all()
        share = await _validate_token(task_id, token, db)
        task = await db.get(Task, task_id)
        if not task:
            raise HTTPException(404, "Task not found")
        from backend.api.tasks import (
            _require_not_delivery_owned_task,
            _require_not_isolated_browser_child,
        )

        _require_not_delivery_owned_task(
            task,
            action="sent shared-access chat messages",
        )
        await _require_not_isolated_browser_child(
            db,
            task,
            action="sent shared-access chat messages",
        )
        if not task.session_id:
            raise HTTPException(400, "Task has no active session")
        from backend.api.tasks import _require_pr_review_chat_allowed

        await _require_pr_review_chat_allowed(db, task_id)

        # Sender identity is display metadata only.  Keep it in the
        # persisted/UI copy, while the model receives the caller's original
        # message verbatim.
        sender = body.sender_name or share.shared_to_name or "Anonymous"
        prefixed = f"[{sender}] {body.message}"
        user_log = LogEntry(
            instance_id=1,
            task_id=task_id,
            event_type="user_message",
            role="user",
            content=prefixed,
            raw_json=json.dumps({
                "sender_name": sender,
                "raw_content": body.message,
            }),
            is_error=False,
        )
        db.add(user_log)
        await db.commit()

        from backend.main import broadcaster
        await broadcaster.broadcast(
            f"task:{task_id}",
            persisted_chat_event(user_log, {
                "event_type": "user_message",
                "role": "user",
                "content": prefixed,
                "sender_name": sender,
                "raw_content": body.message,
            }),
        )

        from backend.main import dispatcher
        from backend.services.dispatcher import (
            PRIORITY_USER,
            TaskStartPausedError,
        )

        try:
            await dispatcher.enqueue_message(
                task_id=task_id,
                prompt=body.message,
                priority=PRIORITY_USER,
                source="shared",
                source_log_id=user_log.id,
                initiating_user_id=None,
                initiating_user_role="member",
                execution_mode="sandbox",
                execution_principal_kind="system",
            )
        except TaskStartPausedError as exc:
            raise HTTPException(
                status_code=409,
                detail="服务即将重启，消息未进入执行队列，请重连后重试",
            ) from exc

    return {"ok": True, "queued": True}


@router.get("/{task_id}/config")
async def shared_config(
    task_id: int,
    token: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """Read-only task config for a shared task."""
    await _validate_token(task_id, token, db)

    task = await db.get(Task, task_id)
    if not task:
        raise HTTPException(404, "Task not found")

    project_name = None
    if task.project_id:
        from backend.models.project import Project
        project = await db.get(Project, task.project_id)
        if project:
            project_name = project.name

    return {
        "id": task.id,
        "title": task.title,
        "description": task.description,
        "status": task.status,
        "priority": task.priority,
        "mode": task.mode,
        "model": task.model,
        "provider": task.provider,
        "effort_level": task.effort_level,
        "project_id": task.project_id,
        "project_name": project_name,
        "session_id": task.session_id,
        "target_repo": task.target_repo,
        "target_branch": task.target_branch,
        "error_message": task.error_message,
        "attention_tag": task.attention_tag,
        "loop_progress": task.loop_progress,
        "plan_content": task.plan_content,
        "plan_approved": task.plan_approved,
        "created_at": task.created_at.isoformat() if task.created_at else None,
        "started_at": task.started_at.isoformat() if task.started_at else None,
        "completed_at": task.completed_at.isoformat() if task.completed_at else None,
    }
