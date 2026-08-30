"""Sub-Agent session CRUD + progress/result endpoints.

Parallel to backend/api/monitor.py but for one-shot sub-agent tasks
(agent_type="sub_agent").
"""
import asyncio
import json
from weakref import WeakValueDictionary
from datetime import datetime
from types import SimpleNamespace
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import select, func, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.database import get_db
from backend.api.deps import (
    internal_task_incarnation_id,
    lock_task_effect_access,
    require_internal_service,
    require_internal_task_incarnation,
    require_task_access,
    require_task_control,
)
from backend.models.task import Task
from backend.models.sub_agent import SubAgentSession, SubAgentReport
from backend.models.project import Project
from backend.services.task_queue import task_retry_not_superseded_predicate
from backend.services.cancellation import await_task_completion
from backend.services.worker_task_termination import (
    active_worker_task_termination_receipt,
    no_active_worker_task_termination_predicate,
)
from backend.services.worker_node_control import fence_worker_node_mutation

router = APIRouter(prefix="/api/tasks/{task_id}/sub-agent-sessions", tags=["sub-agent-tasks"])

MAX_SUB_AGENTS_PER_TASK = 3
_WORKER_MIRROR_META_KEY = "ccm_worker_mirror"
_sub_agent_admission_locks: WeakValueDictionary[int, asyncio.Lock] = (
    WeakValueDictionary()
)


def _sub_agent_admission_lock(task_id: int) -> asyncio.Lock:
    lock = _sub_agent_admission_locks.get(task_id)
    if lock is None:
        lock = asyncio.Lock()
        _sub_agent_admission_locks[task_id] = lock
    return lock


def _task_relay_generation(task: Task) -> dict[str, int]:
    """Freeze the Task generation carried by one Sub-Agent relay event."""

    return {
        "task_retry_count": task.retry_count,
        "task_turn_generation": task.turn_generation,
    }


async def _read_task_relay_generation(
    db: AsyncSession,
    task_id: int,
) -> dict[str, int]:
    row = (
        await db.execute(
            select(Task.retry_count, Task.turn_generation).where(
                Task.id == task_id
            )
        )
    ).one_or_none()
    if row is None:
        raise HTTPException(404, "Task not found")
    return {
        "task_retry_count": row.retry_count,
        "task_turn_generation": row.turn_generation,
    }


def _parse_worker_mirror_identity(
    meta: object,
) -> tuple[int, str, int] | None:
    """Parse the exact Worker/incarnation/remote identity of one mirror."""

    if not isinstance(meta, str):
        return None
    try:
        payload = json.loads(meta)
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, dict) or set(payload) != {_WORKER_MIRROR_META_KEY}:
        return None
    identity = payload.get(_WORKER_MIRROR_META_KEY)
    if not isinstance(identity, dict) or set(identity) != {
        "worker_id",
        "task_incarnation_id",
        "remote_id",
    }:
        return None
    worker_id = identity.get("worker_id")
    incarnation_id = identity.get("task_incarnation_id")
    remote_id = identity.get("remote_id")
    if (
        type(worker_id) is not int
        or worker_id <= 0
        or not isinstance(incarnation_id, str)
        or len(incarnation_id) != 32
        or any(char not in "0123456789abcdef" for char in incarnation_id)
        or type(remote_id) is not int
        or remote_id <= 0
    ):
        return None
    return worker_id, incarnation_id, remote_id


async def _settle_shielded(operation: asyncio.Task) -> asyncio.CancelledError | None:
    return await await_task_completion(operation)


async def _mark_sub_agent_admission_failed(
    db: AsyncSession,
    task_id: int,
    session_id: int,
) -> None:
    await db.execute(
        update(SubAgentSession)
        .where(
            SubAgentSession.id == session_id,
            SubAgentSession.task_id == task_id,
            SubAgentSession.agent_type == "sub_agent",
            SubAgentSession.source == "ccm",
            SubAgentSession.status == "running",
        )
        .values(status="failed", completed_at=datetime.utcnow())
    )
    await db.commit()


async def _commit_and_admit_sub_agent(
    db: AsyncSession,
    task_id: int,
    session: SubAgentSession,
    dispatcher,
) -> None:
    committed = False

    async def commit_and_start() -> None:
        nonlocal committed
        await db.commit()
        committed = True
        dispatcher.start_sub_agent_session(session)

    operation = asyncio.create_task(commit_and_start())
    delayed_cancellation = await _settle_shielded(operation)
    try:
        operation.result()
    except Exception as exc:
        if committed:
            cleanup = asyncio.create_task(
                _mark_sub_agent_admission_failed(db, task_id, session.id)
            )
            cleanup_cancellation = await _settle_shielded(cleanup)
            cleanup.result()
            delayed_cancellation = (
                delayed_cancellation or cleanup_cancellation
            )
        if delayed_cancellation is not None:
            raise delayed_cancellation
        if isinstance(exc, RuntimeError):
            raise HTTPException(503, str(exc)) from exc
        raise
    if delayed_cancellation is not None:
        raise delayed_cancellation


async def _sub_agent_session_or_error(
    db: AsyncSession,
    task_id: int,
    session_id: int,
) -> SubAgentSession:
    db.expire_all()
    session = await db.scalar(
        select(SubAgentSession).where(
            SubAgentSession.id == session_id,
            SubAgentSession.task_id == task_id,
            SubAgentSession.agent_type == "sub_agent",
            SubAgentSession.source == "ccm",
        )
    )
    if session is None:
        raise HTTPException(404, "Sub-agent session not found")
    raise HTTPException(400, "Sub-agent session is not running")


# ---- Pydantic schemas ----

class SubAgentSessionCreate(BaseModel):
    name: str
    prompt: str
    context: str = ""
    model: str | None = None


class SubAgentSessionResponse(BaseModel):
    id: int
    task_id: int
    agent_type: str
    source: str
    description: str
    monitor_context: str | None
    status: str
    checks_done: int
    last_summary: str | None
    created_at: datetime
    completed_at: datetime | None

    model_config = {"from_attributes": True}


class SubAgentProgressRequest(BaseModel):
    summary: str


class SubAgentResultRequest(BaseModel):
    result: str
    status: Literal["completed", "failed"] = "completed"


# ---- Endpoints ----

@router.post("", response_model=SubAgentSessionResponse, status_code=201)
async def create_sub_agent_session(
    task_id: int,
    body: SubAgentSessionCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Create a sub-agent session and start its subprocess via dispatcher."""
    # Route remote ownership before taking the local Task write barrier. The
    # proxy is a network await and must not retain a Manager DB transaction.
    task = await db.get(Task, task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    scoped_task = await require_internal_task_incarnation(
        request,
        task_id,
        db,
    )
    if scoped_task is not None:
        task = scoped_task
    await require_task_control(request, task, db)
    expected_incarnation_id = task.incarnation_id
    local_effect_probe = SimpleNamespace(
        id=task.id,
        project_id=task.project_id,
    )
    if task.worker_id is not None:
        if internal_task_incarnation_id(request, task_id) is not None:
            raise HTTPException(
                409,
                "Scoped Sub-Agent tools must execute on the Task's owning Worker",
            )
        from backend.main import worker_proxy
        if worker_proxy is None:
            raise HTTPException(503, "Worker 功能未启用")
        task = await lock_task_effect_access(
            request,
            task,
            db,
            allow_chat_share=False,
            fence_worker_node=True,
        )
        if task.status == "migrating":
            raise HTTPException(
                409,
                "Task migration is active; retry Sub-Agent creation after it settles",
            )
        if (
            task.incarnation_id != expected_incarnation_id
            or task.worker_id is None
        ):
            raise HTTPException(409, "Task Worker assignment changed")
        db.expunge(task)
        # Commit the no-op ACL writer transaction immediately before the
        # Worker POST.  It is the Manager-side effect admission point.
        await db.commit()
        return await worker_proxy.proxy_to_worker(
            task,
            "POST",
            f"/api/tasks/{task_id}/sub-agent-sessions",
            body=body.model_dump(),
            require_task_incarnation_fence=True,
        )
    await db.rollback()

    async with _sub_agent_admission_lock(task_id):
        try:
            # The keyed lock makes SQLite cap admission deterministic in one
            # CCM process; this Task write barrier additionally serializes
            # cancellation and other backend processes.
            task = await lock_task_effect_access(
                request,
                local_effect_probe,
                db,
                allow_chat_share=False,
                fence_worker_node=True,
            )
            if task.incarnation_id != expected_incarnation_id:
                raise HTTPException(409, "Task incarnation changed")
            guarded = await db.execute(
                update(Task)
                .where(
                    Task.id == task_id,
                    Task.incarnation_id == expected_incarnation_id,
                    Task.worker_id.is_(None),
                    Task.status.in_(("in_progress", "executing")),
                    task_retry_not_superseded_predicate(),
                    no_active_worker_task_termination_predicate(),
                )
                .values(status=Task.status)
            )
            if not guarded.rowcount:
                await db.rollback()
                await require_internal_task_incarnation(request, task_id, db)
                task = await db.scalar(
                    select(Task).where(
                        Task.id == task_id,
                        Task.incarnation_id == expected_incarnation_id,
                    )
                )
                if task is None:
                    raise HTTPException(409, "Task incarnation changed")
                if await active_worker_task_termination_receipt(db, task_id):
                    raise HTTPException(
                        409,
                        "Task has an active Worker termination receipt",
                    )
                if task.worker_id is not None:
                    if internal_task_incarnation_id(request, task_id) is not None:
                        raise HTTPException(
                            409,
                            "Scoped Sub-Agent tools must execute on the Task's "
                            "owning Worker",
                        )
                    from backend.main import worker_proxy
                    if worker_proxy is None:
                        raise HTTPException(503, "Worker 功能未启用")
                    task = await lock_task_effect_access(
                        request,
                        task,
                        db,
                        allow_chat_share=False,
                        fence_worker_node=True,
                    )
                    if task.status == "migrating":
                        raise HTTPException(
                            409,
                            "Task migration is active; retry Sub-Agent creation "
                            "after it settles",
                        )
                    if (
                        task.incarnation_id != expected_incarnation_id
                        or task.worker_id is None
                    ):
                        raise HTTPException(
                            409,
                            "Task Worker assignment changed",
                        )
                    db.expunge(task)
                    await db.commit()
                    return await worker_proxy.proxy_to_worker(
                        task,
                        "POST",
                        f"/api/tasks/{task_id}/sub-agent-sessions",
                        body=body.model_dump(),
                        require_task_incarnation_fence=True,
                    )
                raise HTTPException(
                    400,
                    "Cannot create sub-agent for inactive task",
                )
            db.expire_all()
            task = await db.scalar(
                select(Task).where(
                    Task.id == task_id,
                    Task.incarnation_id == expected_incarnation_id,
                )
            )
            if task is None:
                raise HTTPException(409, "Task incarnation changed")
            relay_generation = _task_relay_generation(task)
            provider = (task.provider or "claude").lower()
            if provider not in {"claude", "codex"}:
                raise HTTPException(
                    400,
                    "Sub-agents require a supported coding provider; this task "
                    f"runs on provider '{task.provider}'",
                )
            skills = task.enabled_skills or {}
            if not skills.get("sub-agent"):
                raise HTTPException(
                    403,
                    "Sub-Agent skill not enabled for this task",
                )

            running_count = await db.scalar(
                select(func.count(SubAgentSession.id)).where(
                    SubAgentSession.task_id == task_id,
                    SubAgentSession.agent_type == "sub_agent",
                    SubAgentSession.source == "ccm",
                    SubAgentSession.status == "running",
                )
            )
            if running_count >= MAX_SUB_AGENTS_PER_TASK:
                raise HTTPException(
                    429,
                    "Too many running sub-agents "
                    f"({running_count}/{MAX_SUB_AGENTS_PER_TASK}). "
                    "Stop an existing one first.",
                )

            from backend.main import dispatcher
            if getattr(dispatcher, "_shutting_down", False) is True:
                raise HTTPException(503, "Dispatcher is shutting down")

            session = SubAgentSession(
                task_id=task_id,
                agent_type="sub_agent",
                source="ccm",
                description=body.name,
                monitor_context=body.context or None,
                interval=0,
                max_checks=0,
                model=body.model,
                last_summary=body.prompt,
            )
            db.add(session)
            await db.flush()
            await _commit_and_admit_sub_agent(
                db,
                task_id,
                session,
                dispatcher,
            )
        except BaseException:
            if db.in_transaction():
                await db.rollback()
            raise

    await dispatcher.broadcaster.broadcast(
        f"task:{task_id}",
        {
            "event": "sub_agent_session_created",
            "sub_agent_session_id": session.id,
            "description": session.description,
            "agent_type": "sub_agent",
            "source": "ccm",
            "monitor_context": session.monitor_context,
            "status": "running",
            "checks_done": session.checks_done,
            "last_summary": session.last_summary,
            **relay_generation,
        },
    )

    return session


@router.get("", response_model=list[SubAgentSessionResponse])
async def list_sub_agent_sessions(
    task_id: int,
    request: Request,
    agent_type: Literal["sub_agent"] | None = None,
    db: AsyncSession = Depends(get_db),
):
    """List CCM one-shot Sub-Agent rows, including Manager Worker mirrors."""
    task = await db.get(Task, task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    scoped_task = await require_internal_task_incarnation(
        request,
        task_id,
        db,
    )
    if scoped_task is not None:
        task = scoped_task
    await require_task_access(request, task, db)
    stmt = select(SubAgentSession).where(
        SubAgentSession.task_id == task_id,
        SubAgentSession.agent_type == "sub_agent",
        SubAgentSession.source == "ccm",
    )
    stmt = stmt.order_by(SubAgentSession.created_at.desc())
    result = await db.execute(stmt)
    return list(result.scalars().all())


@router.get("/{session_id}", response_model=SubAgentSessionResponse)
async def get_sub_agent_session(
    task_id: int,
    session_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    task = await db.get(Task, task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    scoped_task = await require_internal_task_incarnation(
        request,
        task_id,
        db,
    )
    if scoped_task is not None:
        task = scoped_task
    await require_task_access(request, task, db)
    sa = await db.scalar(
        select(SubAgentSession).where(
            SubAgentSession.id == session_id,
            SubAgentSession.task_id == task_id,
            SubAgentSession.agent_type == "sub_agent",
            SubAgentSession.source == "ccm",
        )
    )
    if sa is None:
        raise HTTPException(404, "Sub-agent session not found")
    return sa


@router.delete("/{session_id}")
async def delete_sub_agent_session(
    task_id: int,
    session_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Stop/cancel a running sub-agent."""
    task = await db.get(Task, task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    scoped_task = await require_internal_task_incarnation(
        request,
        task_id,
        db,
    )
    if scoped_task is not None:
        task = scoped_task
    await require_task_control(request, task, db)
    expected_incarnation_id = task.incarnation_id

    task = await lock_task_effect_access(
        request,
        task,
        db,
        allow_chat_share=False,
        fence_worker_node=True,
    )
    if task.incarnation_id != expected_incarnation_id:
        raise HTTPException(409, "Task incarnation changed")
    if await active_worker_task_termination_receipt(
        db,
        task_id,
        for_update=True,
    ):
        raise HTTPException(
            409,
            "Task has an active Worker termination receipt",
        )
    sa = await db.scalar(
        select(SubAgentSession)
        .where(
            SubAgentSession.id == session_id,
            SubAgentSession.task_id == task_id,
            SubAgentSession.agent_type == "sub_agent",
            SubAgentSession.source == "ccm",
        )
        .with_for_update()
    )
    if sa is None:
        raise HTTPException(404, "Sub-agent session not found")
    relay_generation = _task_relay_generation(task)

    if task.worker_id is not None:
        identity = _parse_worker_mirror_identity(sa.meta)
        if (
            identity is None
            or identity
            != (task.worker_id, task.incarnation_id, sa.remote_id)
            or type(sa.remote_id) is not int
            or sa.remote_id <= 0
        ):
            raise HTTPException(
                409,
                "Sub-agent Worker mirror identity is missing or stale",
            )
        from backend.main import worker_proxy
        if worker_proxy is None:
            raise HTTPException(503, "Worker 功能未启用")
        remote_id = sa.remote_id
        mirror_meta = sa.meta
        db.expunge(task)
        # The authorization/receipt transaction must end before any network
        # await.  WorkerProxy rechecks this exact detached incarnation and
        # assignment under the shared per-Task operation lock.
        await db.commit()
        result = await worker_proxy.proxy_to_worker(
            task,
            "DELETE",
            f"/api/tasks/{task_id}/sub-agent-sessions/{remote_id}",
            require_task_incarnation_fence=True,
        )
        await db.execute(
            update(SubAgentSession)
            .where(
                SubAgentSession.id == session_id,
                SubAgentSession.task_id == task_id,
                SubAgentSession.agent_type == "sub_agent",
                SubAgentSession.source == "ccm",
                SubAgentSession.remote_id == remote_id,
                SubAgentSession.meta == mirror_meta,
                SubAgentSession.status == "running",
            )
            .values(status="stopped", completed_at=datetime.utcnow())
        )
        await db.commit()
        return result

    if sa.remote_id is not None:
        raise HTTPException(
            409,
            "Sub-agent Worker mirror no longer matches Task assignment",
        )

    status_event = {
        "event": "sub_agent_session_status",
        "sub_agent_session_id": session_id,
        "description": sa.description,
        "agent_type": "sub_agent",
        "source": "ccm",
        "monitor_context": sa.monitor_context,
        "status": "stopped",
        "checks_done": sa.checks_done,
        "last_summary": sa.last_summary,
        **relay_generation,
    }

    transitioned = await db.execute(
        update(SubAgentSession)
        .where(
            SubAgentSession.id == session_id,
            SubAgentSession.task_id == task_id,
            SubAgentSession.agent_type == "sub_agent",
            SubAgentSession.source == "ccm",
            SubAgentSession.status == "running",
        )
        .values(status="stopped", completed_at=datetime.utcnow())
    )
    await db.commit()

    from backend.main import dispatcher
    await dispatcher.stop_sub_agent_session_process(session_id)

    from backend.services.mcp_config import cleanup_sub_agent_mcp_config
    cleanup_sub_agent_mcp_config(session_id)

    if transitioned.rowcount:
        await dispatcher.broadcaster.broadcast(
            f"task:{task_id}",
            {
                **status_event,
            },
        )

    return {"ok": True}


@router.post("/{session_id}/progress")
async def sub_agent_report_progress(
    task_id: int,
    session_id: int,
    body: SubAgentProgressRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Sub-agent reports progress via MCP tool."""
    require_internal_service(request)
    await fence_worker_node_mutation(db)
    await require_internal_task_incarnation(
        request,
        task_id,
        db,
        write_fence=True,
    )
    advanced = await db.execute(
        update(SubAgentSession)
        .where(
            SubAgentSession.id == session_id,
            SubAgentSession.task_id == task_id,
            SubAgentSession.agent_type == "sub_agent",
            SubAgentSession.source == "ccm",
            SubAgentSession.status == "running",
        )
        .values(
            checks_done=SubAgentSession.checks_done + 1,
            last_summary=body.summary,
        )
    )
    if not advanced.rowcount:
        await db.rollback()
        await _sub_agent_session_or_error(db, task_id, session_id)
    state = (
        await db.execute(
            select(
                SubAgentSession.description,
                SubAgentSession.checks_done,
            )
            .where(
                SubAgentSession.id == session_id,
                SubAgentSession.task_id == task_id,
            )
            .with_for_update()
        )
    ).one()
    description, progress_count = state

    report = SubAgentReport(
        session_id=session_id,
        check_number=progress_count,
        status="progress",
        summary=body.summary,
    )
    db.add(report)

    # Write system_event log for progress
    from backend.models.log_entry import LogEntry
    import json as _json
    log_entry = LogEntry(
        instance_id=1,
        task_id=task_id,
        event_type="system_event",
        role="system",
        content=f"[Sub-Agent #{session_id}: {description}] {body.summary}",
        raw_json=_json.dumps({"source": "sub-agent", "sub_agent_session_id": session_id,
                              "progress_count": progress_count}),
        is_error=False,
    )
    db.add(log_entry)
    await db.commit()

    from backend.main import dispatcher
    await dispatcher.broadcaster.broadcast(
        f"task:{task_id}",
        {
            "event": "sub_agent_progress",
            "sub_agent_session_id": session_id,
            "progress_count": progress_count,
            "summary": body.summary,
            "description": description,
            "source": "sub-agent",
        },
    )

    return {"ok": True, "progress_count": progress_count}


@router.post("/{session_id}/result")
async def sub_agent_submit_result(
    task_id: int,
    session_id: int,
    body: SubAgentResultRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Sub-agent submits final result and marks completed."""
    require_internal_service(request)
    await fence_worker_node_mutation(db)
    await require_internal_task_incarnation(
        request,
        task_id,
        db,
        write_fence=True,
    )
    from backend.main import dispatcher

    relay_generation = await _read_task_relay_generation(db, task_id)
    queue_admission_fence = await dispatcher.snapshot_queue_admission(task_id)
    completed = await db.execute(
        update(SubAgentSession)
        .where(
            SubAgentSession.id == session_id,
            SubAgentSession.task_id == task_id,
            SubAgentSession.agent_type == "sub_agent",
            SubAgentSession.source == "ccm",
            SubAgentSession.status == "running",
        )
        .values(
            status=body.status,
            completed_at=datetime.utcnow(),
            last_summary=body.result[:500] if body.result else None,
            checks_done=SubAgentSession.checks_done + 1,
        )
    )
    if not completed.rowcount:
        await db.rollback()
        await _sub_agent_session_or_error(db, task_id, session_id)
    state = (
        await db.execute(
            select(
                SubAgentSession.description,
                SubAgentSession.monitor_context,
                SubAgentSession.checks_done,
                SubAgentSession.last_summary,
            )
            .where(
                SubAgentSession.id == session_id,
                SubAgentSession.task_id == task_id,
            )
            .with_for_update()
        )
    ).one()
    description, monitor_context, checks_done, last_summary = state

    report = SubAgentReport(
        session_id=session_id,
        check_number=checks_done,
        status=body.status,
        summary=body.result,
    )
    db.add(report)
    await db.commit()

    # Broadcast completion event (panel update only, no chat insert)
    await dispatcher.broadcaster.broadcast(
        f"task:{task_id}",
        {
            "event": "sub_agent_session_status",
            "sub_agent_session_id": session_id,
            "description": description,
            "agent_type": "sub_agent",
            "source": "ccm",
            "monitor_context": monitor_context,
            "status": body.status,
            "checks_done": checks_done,
            "last_summary": last_summary,
            **relay_generation,
        },
    )

    # Enqueue result into main session as user_message
    result_text = (
        f"[Sub-Agent: {description}] "
        f"任务{'完成' if body.status == 'completed' else '失败'}"
        f"\n\n{body.result}"
    )
    from backend.services.dispatcher import PRIORITY_MONITOR_COMPLETE
    await dispatcher.enqueue_message(
        task_id=task_id,
        prompt=result_text,
        priority=PRIORITY_MONITOR_COMPLETE,
        source="sub-agent:result",
        user_message_text=result_text,
        monitor_session_id=session_id,
        queue_admission_fence=queue_admission_fence,
    )

    # Kill the subprocess since it's done
    await dispatcher.stop_sub_agent_session_process(session_id)

    return {"ok": True, "status": body.status}


@router.get("/{session_id}/context")
async def get_sub_agent_context(
    task_id: int,
    session_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Get task context for a sub-agent."""
    require_internal_service(request)
    await require_internal_task_incarnation(request, task_id, db)
    sa = await db.scalar(
        select(SubAgentSession).where(
            SubAgentSession.id == session_id,
            SubAgentSession.task_id == task_id,
            SubAgentSession.agent_type == "sub_agent",
            SubAgentSession.source == "ccm",
        )
    )
    if sa is None:
        raise HTTPException(404, "Sub-agent session not found")

    task = await db.get(Task, task_id)
    context: dict = {
        "task_description": task.description if task else "",
        # Task has no separate prompt column. Keep the legacy response key for
        # MCP clients, backed by the canonical task description.
        "task_prompt": task.description if task else "",
        "sub_agent_prompt": sa.last_summary or "",
        "sub_agent_context": sa.monitor_context or "",
    }
    if task and task.project_id:
        project = await db.get(Project, task.project_id)
        if project:
            context["project_name"] = project.name
            context["project_path"] = project.local_path or ""

    return context
