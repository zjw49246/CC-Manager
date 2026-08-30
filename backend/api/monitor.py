import asyncio
import json
from weakref import WeakValueDictionary
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import and_, case, func, select, update
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
from backend.models.monitor_session import MonitorSession, MonitorCheck
from backend.schemas.monitor_session import (
    MonitorSessionCreate,
    MonitorSessionResponse,
    MonitorCheckCreate,
    MonitorCheckResponse,
    MonitorCompleteRequest,
)
from backend.services.task_queue import task_retry_not_superseded_predicate
from backend.services.cancellation import await_task_completion
from backend.services.worker_task_termination import (
    active_worker_task_termination_receipt,
    no_active_worker_task_termination_predicate,
)
from backend.services.worker_node_control import fence_worker_node_mutation

router = APIRouter(prefix="/api/tasks/{task_id}/monitor-sessions", tags=["monitor"])

MAX_CONCURRENT_MONITORS = 5
_WORKER_MIRROR_META_KEY = "ccm_worker_mirror"
_monitor_admission_locks: WeakValueDictionary[int, asyncio.Lock] = (
    WeakValueDictionary()
)


def _monitor_admission_lock(task_id: int) -> asyncio.Lock:
    """Return the single-process SQLite admission lock for one Task."""

    lock = _monitor_admission_locks.get(task_id)
    if lock is None:
        lock = asyncio.Lock()
        _monitor_admission_locks[task_id] = lock
    return lock


def _task_relay_generation(task: Task) -> dict[str, int]:
    """Freeze the Task generation carried by one monitor relay event."""

    return {
        "task_retry_count": task.retry_count,
        "task_turn_generation": task.turn_generation,
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


async def _settle_shielded(operation: asyncio.Task) -> asyncio.CancelledError | None:
    """Delay caller cancellation until a lifecycle-critical operation settles."""

    return await await_task_completion(operation)


async def _mark_monitor_admission_failed(
    db: AsyncSession,
    task_id: int,
    session_id: int,
) -> None:
    await db.execute(
        update(MonitorSession)
        .where(
            MonitorSession.id == session_id,
            MonitorSession.task_id == task_id,
            MonitorSession.agent_type == "monitor",
            MonitorSession.source == "ccm",
            MonitorSession.status == "running",
        )
        .values(status="failed", completed_at=datetime.utcnow())
    )
    await db.commit()


async def _commit_and_admit_monitor(
    db: AsyncSession,
    task_id: int,
    session: MonitorSession,
    dispatcher,
) -> None:
    """Atomically settle DB commit and synchronous dispatcher registration."""

    committed = False

    async def commit_and_start() -> None:
        nonlocal committed
        await db.commit()
        committed = True
        # Deliberately no await after commit: registration becomes visible in
        # the same event-loop slice in which the durable row becomes visible.
        dispatcher.start_monitor_session(session)

    operation = asyncio.create_task(commit_and_start())
    delayed_cancellation = await _settle_shielded(operation)
    try:
        operation.result()
    except Exception as exc:
        if committed:
            cleanup = asyncio.create_task(
                _mark_monitor_admission_failed(db, task_id, session.id)
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


async def _monitor_session_or_error(
    db: AsyncSession,
    task_id: int,
    session_id: int,
) -> MonitorSession:
    db.expire_all()
    session = await db.scalar(
        select(MonitorSession).where(
            MonitorSession.id == session_id,
            MonitorSession.task_id == task_id,
            MonitorSession.agent_type == "monitor",
            MonitorSession.source == "ccm",
        )
    )
    if session is None:
        raise HTTPException(404, "Monitor session not found")
    raise HTTPException(400, "Monitor session is not running")


def _monitor_callback_generation_predicate(
    turn_generation: int | None,
):
    """Fence a callback to its exact scheduled turn.

    ``None`` is retained only for isolated legacy/direct rows that have never
    entered scheduled-turn state. A real schedule has either a due timestamp
    or a non-null active generation, so omitting the token cannot bypass the
    fence before or during a turn.
    """

    if turn_generation is None:
        return and_(
            MonitorSession.active_turn_generation.is_(None),
            MonitorSession.next_check_at.is_(None),
            MonitorSession.turn_generation == 0,
        )
    return MonitorSession.active_turn_generation == turn_generation


async def _monitor_callback_error(
    db: AsyncSession,
    task_id: int,
    session_id: int,
) -> None:
    db.expire_all()
    session = await db.scalar(
        select(MonitorSession).where(
            MonitorSession.id == session_id,
            MonitorSession.task_id == task_id,
            MonitorSession.agent_type == "monitor",
            MonitorSession.source == "ccm",
        )
    )
    if session is None:
        raise HTTPException(404, "Monitor session not found")
    if session.status != "running":
        raise HTTPException(400, "Monitor session is not running")
    raise HTTPException(
        409,
        "Monitor turn generation is no longer active",
    )


def _require_monitor_capability(task: Task) -> None:
    """Enforce the same exact Task scope used by Task/Chat/MCP admission."""

    from backend.config import settings
    from backend.services.skill_context import (
        codex_monitor_supported_for_scope,
    )

    if codex_monitor_supported_for_scope(
        provider=task.provider,
        worker_id=task.worker_id,
        shared_from_id=task.shared_from_id,
        metadata=task.metadata_,
        codex_main_mcp_enabled=settings.codex_main_mcp_enabled,
    ):
        return
    raise HTTPException(
        400,
        "Codex Monitor requires a local, non-shared Task and enabled "
        "Codex main-task MCP",
    )


@router.post("", response_model=MonitorSessionResponse)
async def create_monitor_session(
    task_id: int,
    body: MonitorSessionCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    # Route Worker tasks before taking a local row lock: the proxy is a network
    # await and must never hold the Manager's Task transaction open.
    task = await db.get(Task, task_id)
    if not task:
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
    # Codex Worker and shared Tasks must fail before a proxy/network side
    # effect. Claude Worker routing remains unchanged.
    _require_monitor_capability(task)
    if task.worker_id is not None:
        if internal_task_incarnation_id(request, task_id) is not None:
            raise HTTPException(
                409,
                "Scoped Monitor tools must execute on the Task's owning Worker",
            )
        # Worker task：monitor 子进程依赖 task 所在机器的文件系统（ps/tail/signal
        # file），必须在 worker 上跑。本地镜像行由 relay 的 monitor_session_created
        # 事件落库（带 remote_id），这里直接透传 worker 响应。
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
                "Task migration is active; retry Monitor creation after it settles",
            )
        if (
            task.incarnation_id != expected_incarnation_id
            or task.worker_id is None
        ):
            raise HTTPException(409, "Task Worker assignment changed")
        db.expunge(task)
        # This commit is the Manager-side linearization point for the remote
        # effect.  A revocation that won first is rejected by the canonical
        # ACL fence; one that starts afterwards is ordered after admission.
        await db.commit()
        return await worker_proxy.proxy_to_worker(
            task, "POST", f"/api/tasks/{task_id}/monitor-sessions",
            body=body.model_dump(),
            require_task_incarnation_fence=True,
        )

    async with _monitor_admission_lock(task_id):
        try:
            # End the routing read before waiting for a write barrier.
            # ``FOR UPDATE`` alone is ignored by SQLite. The keyed lock keeps
            # same-process cap checks ordered, while this no-op Task UPDATE
            # also serializes cancellation and other backend processes.
            task = await lock_task_effect_access(
                request,
                task,
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
                db.expire_all()
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
                _require_monitor_capability(task)
                if task.worker_id is not None:
                    if internal_task_incarnation_id(request, task_id) is not None:
                        raise HTTPException(
                            409,
                            "Scoped Monitor tools must execute on the Task's "
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
                            "Task migration is active; retry Monitor creation "
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
                        f"/api/tasks/{task_id}/monitor-sessions",
                        body=body.model_dump(),
                        require_task_incarnation_fence=True,
                    )
                raise HTTPException(
                    400,
                    "Cannot create monitor for inactive task",
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
            _require_monitor_capability(task)
            relay_generation = _task_relay_generation(task)
            skills = task.enabled_skills or {}
            if not skills.get("monitor"):
                raise HTTPException(
                    403,
                    "Monitor skill not enabled for this task",
                )

            active_count = await db.scalar(
                select(func.count(MonitorSession.id)).where(
                    MonitorSession.task_id == task_id,
                    MonitorSession.agent_type == "monitor",
                    MonitorSession.source == "ccm",
                    MonitorSession.status == "running",
                )
            )
            if active_count >= MAX_CONCURRENT_MONITORS:
                raise HTTPException(
                    429,
                    "Too many active monitors "
                    f"({active_count}/{MAX_CONCURRENT_MONITORS}). "
                    "Stop an existing monitor first.",
                )

            from backend.main import dispatcher
            if getattr(dispatcher, "_shutting_down", False) is True:
                raise HTTPException(503, "Dispatcher is shutting down")

            ms = MonitorSession(
                task_id=task_id,
                agent_type="monitor",
                source="ccm",
                description=body.description,
                monitor_context=body.monitor_context,
                interval=body.interval,
                max_checks=body.max_checks,
                model=body.model,
                provider=(task.provider or "claude").lower(),
                next_check_at=datetime.utcnow(),
            )
            db.add(ms)
            await db.flush()
            await _commit_and_admit_monitor(
                db,
                task_id,
                ms,
                dispatcher,
            )
        except BaseException:
            if db.in_transaction():
                await db.rollback()
            raise

    await dispatcher.broadcaster.broadcast(
        f"task:{task_id}",
        {
            "event": "monitor_session_created",
            "monitor_session_id": ms.id,
            "description": ms.description,
            **relay_generation,
        },
    )

    return ms


@router.get("", response_model=list[MonitorSessionResponse])
async def list_monitor_sessions(
    task_id: int,
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
    result = await db.execute(
        select(MonitorSession)
        .where(
            MonitorSession.task_id == task_id,
            MonitorSession.agent_type == "monitor",
            MonitorSession.source == "ccm",
        )
        .order_by(MonitorSession.created_at.desc())
    )
    return list(result.scalars().all())


@router.get("/{session_id}", response_model=MonitorSessionResponse)
async def get_monitor_session(
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
    ms = await db.scalar(
        select(MonitorSession).where(
            MonitorSession.id == session_id,
            MonitorSession.task_id == task_id,
            MonitorSession.agent_type == "monitor",
            MonitorSession.source == "ccm",
        )
    )
    if ms is None:
        raise HTTPException(404, "Monitor session not found")
    return ms


@router.delete("/{session_id}")
async def delete_monitor_session(
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
    await require_task_control(request, task, db)
    expected_incarnation_id = task.incarnation_id

    # This is the final authorization/ownership boundary for both the local
    # runtime cleanup and a Worker DELETE.  It deliberately takes the Worker
    # node fence before the Task writer fence, then the termination receipt,
    # and re-reads the exact child row only after those locks are held.
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
    ms = await db.scalar(
        select(MonitorSession)
        .where(
            MonitorSession.id == session_id,
            MonitorSession.task_id == task_id,
            MonitorSession.agent_type == "monitor",
            MonitorSession.source == "ccm",
        )
        .with_for_update()
    )
    if ms is None:
        raise HTTPException(404, "Monitor session not found")
    relay_generation = _task_relay_generation(task)
    if task.worker_id is not None:
        # 本地行是镜像（id 是 Manager 自增），worker 端要用 remote_id
        identity = _parse_worker_mirror_identity(ms.meta)
        if (
            identity is None
            or identity
            != (task.worker_id, task.incarnation_id, ms.remote_id)
            or type(ms.remote_id) is not int
            or ms.remote_id <= 0
        ):
            raise HTTPException(
                409,
                "Monitor Worker mirror identity is missing or stale",
            )
        from backend.main import worker_proxy
        if worker_proxy is None:
            raise HTTPException(503, "Worker 功能未启用")
        remote_id = ms.remote_id
        mirror_meta = ms.meta
        db.expunge(task)
        # Release every DB writer lock before the network call.  The committed
        # ACL fence is the Manager-side linearization point for this effect.
        await db.commit()
        result = await worker_proxy.proxy_to_worker(
            task, "DELETE", f"/api/tasks/{task_id}/monitor-sessions/{remote_id}",
            require_task_incarnation_fence=True,
        )
        await db.execute(
            update(MonitorSession)
            .where(
                MonitorSession.id == session_id,
                MonitorSession.task_id == task_id,
                MonitorSession.agent_type == "monitor",
                MonitorSession.source == "ccm",
                MonitorSession.remote_id == remote_id,
                MonitorSession.meta == mirror_meta,
                MonitorSession.status == "running",
            )
            .values(
                status="cancelled",
                completed_at=datetime.utcnow(),
                next_check_at=None,
                active_turn_generation=None,
                turn_started_at=None,
            )
        )
        await db.commit()
        return result

    if ms.remote_id is not None:
        raise HTTPException(
            409,
            "Monitor Worker mirror no longer matches Task assignment",
        )

    transitioned = await db.execute(
        update(MonitorSession)
        .where(
            MonitorSession.id == session_id,
            MonitorSession.task_id == task_id,
            MonitorSession.agent_type == "monitor",
            MonitorSession.source == "ccm",
            MonitorSession.status == "running",
        )
        .values(
            status="cancelled",
            completed_at=datetime.utcnow(),
            next_check_at=None,
            active_turn_generation=None,
            turn_started_at=None,
        )
    )
    await db.commit()

    from backend.main import dispatcher
    try:
        await dispatcher.stop_monitor_session_process(
            session_id,
            terminal=True,
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        raise HTTPException(
            409,
            (
                "Monitor was cancelled, but runtime cleanup could not be "
                "confirmed; retry Stop"
            ),
        ) from exc

    from backend.services.mcp_config import cleanup_monitor_agent_mcp_config
    cleanup_monitor_agent_mcp_config(session_id)

    if transitioned.rowcount:
        await dispatcher.broadcaster.broadcast(
            f"task:{task_id}",
            {
                "event": "monitor_session_status",
                "monitor_session_id": session_id,
                "status": "cancelled",
                **relay_generation,
            },
        )

    return {"ok": True}


@router.get("/{session_id}/checks", response_model=list[MonitorCheckResponse])
async def get_monitor_checks(
    task_id: int,
    session_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    task = await db.get(Task, task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    await require_task_access(request, task, db)
    ms = await db.scalar(
        select(MonitorSession).where(
            MonitorSession.id == session_id,
            MonitorSession.task_id == task_id,
            MonitorSession.agent_type == "monitor",
            MonitorSession.source == "ccm",
        )
    )
    if ms is None:
        raise HTTPException(404, "Monitor session not found")
    result = await db.execute(
        select(MonitorCheck)
        .where(MonitorCheck.monitor_session_id == session_id)
        .order_by(MonitorCheck.created_at.desc())
    )
    return list(result.scalars().all())


@router.post("/{session_id}/checks", response_model=MonitorCheckResponse)
async def create_monitor_check(
    task_id: int,
    session_id: int,
    body: MonitorCheckCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Sub-agent reports a status check via MCP tool."""
    require_internal_service(request)
    await fence_worker_node_mutation(db)
    await require_internal_task_incarnation(
        request,
        task_id,
        db,
        write_fence=True,
    )
    import json as _json
    from backend.main import dispatcher
    from backend.models.log_entry import LogEntry

    relay_generation = await _read_task_relay_generation(db, task_id)
    queue_admission_fence = await dispatcher.snapshot_queue_admission(task_id)

    next_check = MonitorSession.checks_done + 1
    reaches_limit = next_check >= MonitorSession.max_checks
    completed_at = datetime.utcnow()
    advanced = await db.execute(
        update(MonitorSession)
        .where(
            MonitorSession.id == session_id,
            MonitorSession.task_id == task_id,
            MonitorSession.agent_type == "monitor",
            MonitorSession.source == "ccm",
            MonitorSession.status == "running",
            _monitor_callback_generation_predicate(
                body.turn_generation
            ),
        )
        # MySQL evaluates assignments in a single-table UPDATE from left to
        # right. Keep both limit expressions ahead of ``checks_done`` so they
        # see the same pre-increment generation as PostgreSQL and SQLite.
        .ordered_values(
            (
                MonitorSession.status,
                case(
                    (reaches_limit, "completed"),
                    else_=MonitorSession.status,
                ),
            ),
            (
                MonitorSession.completed_at,
                case(
                    (reaches_limit, completed_at),
                    else_=MonitorSession.completed_at,
                ),
            ),
            (MonitorSession.active_turn_generation, None),
            (MonitorSession.turn_started_at, None),
            (MonitorSession.next_check_at, None),
            (MonitorSession.consecutive_failures, 0),
            (MonitorSession.last_error, None),
            (MonitorSession.checks_done, next_check),
            (MonitorSession.last_summary, body.summary),
        )
    )
    if not advanced.rowcount:
        await db.rollback()
        await _monitor_callback_error(db, task_id, session_id)

    state = (
        await db.execute(
            select(
                MonitorSession.checks_done,
                MonitorSession.max_checks,
                MonitorSession.status,
                MonitorSession.interval,
            )
            .where(
                MonitorSession.id == session_id,
                MonitorSession.task_id == task_id,
            )
            .with_for_update()
        )
    ).one()
    (
        new_checks_done,
        max_checks,
        persisted_status,
        interval,
    ) = state
    auto_complete = persisted_status == "completed"
    if not auto_complete:
        await db.execute(
            update(MonitorSession)
            .where(
                MonitorSession.id == session_id,
                MonitorSession.task_id == task_id,
                MonitorSession.status == "running",
                MonitorSession.active_turn_generation.is_(None),
            )
            .values(
                next_check_at=completed_at
                + timedelta(seconds=interval)
            )
        )

    check = MonitorCheck(
        monitor_session_id=session_id,
        check_number=new_checks_done,
        status=body.status,
        summary=body.summary,
    )
    db.add(check)

    chat_injected = False
    if body.is_important and not auto_complete:
        monitor_log = LogEntry(
            instance_id=1,
            task_id=task_id,
            event_type="system_event",
            role="system",
            content=f"[Monitor #{session_id}] Check #{new_checks_done}: {body.summary}",
            raw_json=_json.dumps({"source": "monitor", "monitor_session_id": session_id,
                                  "check_number": new_checks_done, "is_important": body.is_important}),
            is_error=False,
        )
        db.add(monitor_log)

    if auto_complete:
        complete_log = LogEntry(
            instance_id=1,
            task_id=task_id,
            event_type="system_event",
            role="system",
            content=f"[Monitor #{session_id}] 监控完成: {body.summary}",
            raw_json=_json.dumps({"source": "monitor", "monitor_session_id": session_id,
                                  "check_number": new_checks_done, "is_important": True}),
            is_error=False,
        )
        db.add(complete_log)

    await db.commit()
    await db.refresh(check)

    if body.is_important and not auto_complete:
        from backend.services.dispatcher import PRIORITY_MONITOR_IMPORTANT
        report_prompt = (
            f"[Monitor #{session_id} 汇报] {body.summary}\n\n"
            "请向用户简要转达这个监控结果。"
        )
        admitted = await dispatcher.enqueue_message(
            task_id=task_id,
            prompt=report_prompt,
            priority=PRIORITY_MONITOR_IMPORTANT,
            source="monitor:report",
            user_message_text=f"[Monitor #{session_id}] {body.summary}",
            monitor_session_id=session_id,
            queue_admission_fence=queue_admission_fence,
        )
        chat_injected = admitted is not False

    await dispatcher.broadcaster.broadcast(
        f"task:{task_id}",
        {
            "event": "monitor_check",
            "monitor_session_id": session_id,
            "check_number": new_checks_done,
            "status": body.status,
            "summary": body.summary,
            "is_important": body.is_important,
            "chat_injected": chat_injected,
            "source": "monitor",
            **relay_generation,
        },
    )

    if auto_complete:
        await dispatcher.broadcaster.broadcast(
            f"task:{task_id}",
            {
                "event": "monitor_session_status",
                "monitor_session_id": session_id,
                "status": "completed",
                **relay_generation,
            },
        )
        from backend.services.dispatcher import PRIORITY_MONITOR_COMPLETE

        complete_prompt = (
            f"[Monitor #{session_id} 完成] 已达最大检查次数"
            f"（{max_checks}次）。最后状态: {body.summary}\n\n"
            "请向用户简要转达监控结果。"
        )
        await dispatcher.enqueue_message(
            task_id=task_id,
            prompt=complete_prompt,
            priority=PRIORITY_MONITOR_COMPLETE,
            source="monitor:complete",
            user_message_text=f"[Monitor #{session_id}] 监控完成: {body.summary}",
            monitor_session_id=session_id,
            queue_admission_fence=queue_admission_fence,
        )
    return check


@router.post("/{session_id}/complete")
async def complete_monitor_session(
    task_id: int,
    session_id: int,
    body: MonitorCompleteRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Sub-agent marks itself as complete."""
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
        update(MonitorSession)
        .where(
            MonitorSession.id == session_id,
            MonitorSession.task_id == task_id,
            MonitorSession.agent_type == "monitor",
            MonitorSession.source == "ccm",
            MonitorSession.status == "running",
            _monitor_callback_generation_predicate(
                body.turn_generation
            ),
        )
        .values(
            status="completed",
            completed_at=datetime.utcnow(),
            last_summary=body.reason,
            checks_done=MonitorSession.checks_done + 1,
            active_turn_generation=None,
            turn_started_at=None,
            next_check_at=None,
            consecutive_failures=0,
            last_error=None,
        )
    )
    if not completed.rowcount:
        await db.rollback()
        await _monitor_callback_error(db, task_id, session_id)
    checks_done = await db.scalar(
        select(MonitorSession.checks_done)
        .where(
            MonitorSession.id == session_id,
            MonitorSession.task_id == task_id,
        )
        .with_for_update()
    )
    check = MonitorCheck(
        monitor_session_id=session_id,
        check_number=checks_done,
        status="completed",
        summary=body.reason,
    )
    db.add(check)
    await db.commit()

    import json as _json

    chat_injected = False

    await dispatcher.broadcaster.broadcast(
        f"task:{task_id}",
        {
            "event": "monitor_check",
            "monitor_session_id": session_id,
            "check_number": checks_done,
            "status": "completed",
            "summary": body.reason,
            "is_important": False,
            "chat_injected": False,
            "source": "monitor",
            **relay_generation,
        },
    )
    await dispatcher.broadcaster.broadcast(
        f"task:{task_id}",
        {
            "event": "monitor_session_status",
            "monitor_session_id": session_id,
            "status": "completed",
            **relay_generation,
        },
    )

    # Check if the last report_status already notified the main agent
    # (is_important=True). Only skip if the MOST RECENT check was important,
    # not any historical one.
    from backend.models.log_entry import LogEntry
    last_report_log = await db.scalar(
        select(LogEntry.raw_json)
        .where(
            LogEntry.task_id == task_id,
            LogEntry.event_type == "system_event",
            LogEntry.raw_json.like(f'%"monitor_session_id": {session_id}%'),
            LogEntry.raw_json.like('%"check_number"%'),
        )
        .order_by(LogEntry.id.desc())
    )
    already_notified = False
    if last_report_log:
        try:
            already_notified = _json.loads(last_report_log).get("is_important", False)
        except (ValueError, TypeError):
            pass
    if not already_notified:
        from backend.services.dispatcher import PRIORITY_MONITOR_COMPLETE
        complete_log = LogEntry(
            instance_id=1,
            task_id=task_id,
            event_type="system_event",
            role="system",
            content=f"[Monitor #{session_id}] 监控完成: {body.reason}",
            raw_json=_json.dumps({"source": "monitor", "monitor_session_id": session_id,
                                  "check_number": checks_done, "is_important": True}),
            is_error=False,
        )
        db.add(complete_log)
        await db.commit()

        complete_prompt = (
            f"[Monitor #{session_id} 完成] {body.reason}\n\n"
            "请向用户简要转达监控结果。"
        )
        await dispatcher.enqueue_message(
            task_id=task_id,
            prompt=complete_prompt,
            priority=PRIORITY_MONITOR_COMPLETE,
            source="monitor:complete",
            user_message_text=f"[Monitor #{session_id}] 监控完成: {body.reason}",
            monitor_session_id=session_id,
            queue_admission_fence=queue_admission_fence,
        )

    return {"ok": True, "message": "Session completed. Your task is done — stop all activity now."}
