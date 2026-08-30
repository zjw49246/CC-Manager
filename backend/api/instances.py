from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import delete as sa_delete
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.api.deps import require_admin
from backend.database import get_db
from backend.models.instance import Instance
from backend.models.log_entry import LogEntry
from backend.models.task import Task
from backend.schemas.instance import (
    InstanceCreate,
    InstanceResponse,
    InstanceStopRequest,
)
from backend.schemas.log_entry import LogEntryResponse
from backend.services.instance_capacity import (
    instance_capacity_lock,
    occupied_slot_predicate,
)
from backend.services.pr_review_runtime import is_pr_sandbox_task
from backend.services.process_identity import (
    persisted_process_is_definitively_dead,
)
from backend.services.stream_parser import detect_assistant_protocol_anomaly

router = APIRouter(
    prefix="/api/instances",
    tags=["instances"],
    dependencies=[Depends(require_admin)],
)
dispatcher_router = APIRouter(
    prefix="/api/dispatcher",
    tags=["dispatcher"],
    dependencies=[Depends(require_admin)],
)
def _instance_generation_predicates(instance: Instance) -> list:
    """Build a complete persisted-generation fence for one Instance row."""

    return [
        Instance.id == instance.id,
        Instance.status == instance.status,
        (
            Instance.current_task_id.is_(None)
            if instance.current_task_id is None
            else Instance.current_task_id == instance.current_task_id
        ),
        (
            Instance.current_plan_run_id.is_(None)
            if instance.current_plan_run_id is None
            else Instance.current_plan_run_id == instance.current_plan_run_id
        ),
        (
            Instance.pid.is_(None)
            if instance.pid is None
            else Instance.pid == instance.pid
        ),
        (
            Instance.process_identity.is_(None)
            if instance.process_identity is None
            else Instance.process_identity == instance.process_identity
        ),
        (
            Instance.started_at.is_(None)
            if instance.started_at is None
            else Instance.started_at == instance.started_at
        ),
    ]


async def _lock_instance_current(
    db: AsyncSession,
    instance_id: int,
) -> Instance | None:
    """Use a locking/current read instead of a MySQL RR snapshot reload."""

    return (
        await db.execute(
            select(Instance)
            .where(Instance.id == instance_id)
            .with_for_update()
        )
    ).scalar_one_or_none()


async def _delete_exact_instance_generation(
    db: AsyncSession,
    instance: Instance,
) -> bool:
    """Delete only the exact generation approved under the lifecycle lock."""

    deleted = await db.execute(
        sa_delete(Instance).where(*_instance_generation_predicates(instance))
    )
    return deleted.rowcount == 1


async def _reconcile_dead_terminal_pid(
    db: AsyncSession,
    instance: Instance,
) -> bool:
    """Detach a terminal persisted generation only after a definitive ESRCH.

    The caller must hold InstanceManager's lifecycle lock. The exact status,
    PID and task owner predicates keep a stale cleanup request from clearing a
    newer generation that changed while the OS probe was in progress.

    Death is proven from the full recorded identity (PID, start ticks and boot
    id) so a reused PID number cannot keep a dead generation pinned forever.
    """

    pid = instance.pid
    if pid is None or not persisted_process_is_definitively_dead(
        pid,
        instance.process_identity,
    ):
        return False
    predicates = [
        Instance.id == instance.id,
        Instance.status == instance.status,
        Instance.pid == pid,
        (
            Instance.process_identity.is_(None)
            if instance.process_identity is None
            else Instance.process_identity == instance.process_identity
        ),
    ]
    if instance.current_task_id is None:
        predicates.append(Instance.current_task_id.is_(None))
    else:
        predicates.append(Instance.current_task_id == instance.current_task_id)
    if instance.current_plan_run_id is None:
        predicates.append(Instance.current_plan_run_id.is_(None))
    else:
        predicates.append(
            Instance.current_plan_run_id == instance.current_plan_run_id
        )
    predicates.append(
        Instance.started_at.is_(None)
        if instance.started_at is None
        else Instance.started_at == instance.started_at
    )
    reconciled = await db.execute(
        update(Instance)
        .where(*predicates)
        .values(
            pid=None,
            process_identity=None,
            current_task_id=None,
            current_plan_run_id=None,
        )
    )
    await db.commit()
    return bool(reconciled.rowcount)


def _instance_response(
    instance: Instance,
    *,
    task_retry_count: int | None,
    task_turn_generation: int | None,
) -> InstanceResponse:
    """Expose the exact Task generation currently owned by an Instance."""

    return InstanceResponse.model_validate(instance).model_copy(
        update={
            "current_task_retry_count": task_retry_count,
            "current_task_turn_generation": task_turn_generation,
        }
    )


def _instance_with_task_generation_query():
    return select(
        Instance,
        Task.retry_count,
        Task.turn_generation,
    ).outerjoin(Task, Task.id == Instance.current_task_id)


@router.get("", response_model=list[InstanceResponse])
async def list_instances(db: AsyncSession = Depends(get_db)):
    rows = (
        await db.execute(
            _instance_with_task_generation_query().order_by(Instance.id)
        )
    ).all()
    return [
        _instance_response(
            instance,
            task_retry_count=task_retry_count,
            task_turn_generation=task_turn_generation,
        )
        for instance, task_retry_count, task_turn_generation in rows
    ]


@router.post("", response_model=InstanceResponse, status_code=201)
async def create_instance(
    request: Request,
    body: InstanceCreate,
    db: AsyncSession = Depends(get_db),
):
    require_admin(request)
    # The Dispatcher uses the same lock for its count-and-create transaction.
    # Without it two simultaneous API/dispatcher admissions can both observe a
    # free slot and exceed the configured hard cap.
    async with instance_capacity_lock:
        from backend.main import dispatcher

        cap = dispatcher.max_concurrent_instances
        if cap > 0:
            live_count = await db.scalar(
                select(func.count(Instance.id)).where(
                    occupied_slot_predicate()
                )
            )
            if (live_count or 0) >= cap:
                raise HTTPException(
                    status_code=409,
                    detail=f"Instance capacity limit reached ({cap})",
                )

        instance = Instance(
            name=body.name,
            config=body.config,
        )
        db.add(instance)
        await db.commit()
        await db.refresh(instance)
    return instance


@router.delete("/cleanup")
async def cleanup_instances(request: Request, db: AsyncSession = Depends(get_db)):
    require_admin(request)
    from backend.main import dispatcher, instance_manager, ralph_loop
    result = await db.execute(
        select(Instance).where(Instance.status.in_(["error", "stopped"]))
    )
    target_ids = [instance.id for instance in result.scalars().all()]
    # Do not retain a MySQL REPEATABLE READ snapshot while waiting for an
    # in-process lifecycle lock. The lock holder may need to commit the newer
    # Instance generation before it can release that lock.
    await db.rollback()
    deleted = 0
    skipped_running: list[int] = []
    for instance_id in target_ids:
        # New Ralph loops cannot be started anymore, but an upgraded process
        # may still have one. Reap it before deleting its slot.
        if await ralph_loop.stop(instance_id) is False:
            skipped_running.append(instance_id)
            continue
        lifecycle_lock = instance_manager._instance_lifecycle_lock(instance_id)
        async with dispatcher._instance_claim_lock, lifecycle_lock:
            if instance_id in dispatcher._instance_claim_owners:
                skipped_running.append(instance_id)
                continue
            db.expire_all()
            inst = await _lock_instance_current(db, instance_id)
            if inst is None:
                await db.rollback()
                continue
            if (
                instance_manager.is_running(instance_id)
                or inst.status not in ("error", "stopped")
            ):
                skipped_running.append(instance_id)
                await db.rollback()
                continue
            if inst.pid is not None:
                if not await _reconcile_dead_terminal_pid(db, inst):
                    skipped_running.append(instance_id)
                    await db.rollback()
                    continue
                db.expire_all()
                inst = await _lock_instance_current(db, instance_id)
                if inst is None:
                    await db.rollback()
                    continue
            if inst.current_task_id is not None or inst.current_plan_run_id is not None:
                skipped_running.append(instance_id)
                await db.rollback()
                continue
            if not await _delete_exact_instance_generation(db, inst):
                skipped_running.append(instance_id)
                await db.rollback()
                continue
            # Commit while the exact Instance lifecycle lock is still held so
            # a concurrent cleanup/delete/launch cannot observe the old row.
            await db.commit()
            deleted += 1
    if dispatcher.status().get("running"):
        await dispatcher._ensure_instances()
    return {
        "ok": True,
        "deleted": deleted,
        "skipped_running": skipped_running,
    }


@router.get("/{instance_id}", response_model=InstanceResponse)
async def get_instance(instance_id: int, db: AsyncSession = Depends(get_db)):
    row = (
        await db.execute(
            _instance_with_task_generation_query().where(
                Instance.id == instance_id
            )
        )
    ).one_or_none()
    if row is None:
        raise HTTPException(404, "Instance not found")
    instance, task_retry_count, task_turn_generation = row
    return _instance_response(
        instance,
        task_retry_count=task_retry_count,
        task_turn_generation=task_turn_generation,
    )


@router.delete("/{instance_id}")
async def delete_instance(
    instance_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    require_admin(request)
    from backend.main import dispatcher, instance_manager, ralph_loop
    instance = await db.get(Instance, instance_id)
    if not instance:
        raise HTTPException(404, "Instance not found")
    # The initial lookup is only an early 404. Release its RR snapshot before
    # waiting for a lifecycle owner that may itself be committing this row.
    await db.rollback()

    # Prevent a legacy Ralph loop from claiming this slot while deletion waits
    # for InstanceManager's launch/stop admission lock.
    if await ralph_loop.stop(instance_id) is False:
        raise HTTPException(
            status_code=409,
            detail="Ralph loop did not stop; instance was not deleted",
        )
    lifecycle_lock = instance_manager._instance_lifecycle_lock(instance_id)
    async with dispatcher._instance_claim_lock, lifecycle_lock:
        if (
            instance_id in dispatcher._instance_claim_owners
            or instance_id in dispatcher._active_local_instance_ids()
        ):
            raise HTTPException(
                status_code=409,
                detail="Instance is reserved for a task lifecycle; retry after refresh",
            )
        db.expire_all()
        instance = await _lock_instance_current(db, instance_id)
        if instance is None:
            await db.rollback()
            raise HTTPException(404, "Instance not found")
        if (
            instance_manager.is_running(instance_id)
            or instance.status == "running"
        ):
            await db.rollback()
            raise HTTPException(
                status_code=409,
                detail="Instance is active; stop it before deleting",
            )
        if instance.pid is not None:
            observed_pid = instance.pid
            if not await _reconcile_dead_terminal_pid(db, instance):
                await db.rollback()
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Instance PID {observed_pid} may still be alive; "
                        "stop or reconcile it before deleting"
                    ),
                )
            db.expire_all()
            instance = await _lock_instance_current(db, instance_id)
            if instance is None:
                await db.rollback()
                raise HTTPException(404, "Instance not found")
        if instance.current_task_id is not None or instance.current_plan_run_id is not None:
            await db.rollback()
            raise HTTPException(
                status_code=409,
                detail="Instance still owns work; reconcile it before deleting",
            )
        if not await _delete_exact_instance_generation(db, instance):
            await db.rollback()
            raise HTTPException(
                status_code=409,
                detail="Instance generation changed while deleting; refresh and retry",
            )
        await db.commit()
    return {"ok": True}


@router.post("/{instance_id}/stop")
async def stop_instance(
    instance_id: int,
    body: InstanceStopRequest,
    db: AsyncSession = Depends(get_db),
):
    from backend.main import instance_manager, ralph_loop
    from backend.services.worker_proxy import get_task_operation_lock
    from backend.services.worker_task_termination import (
        active_worker_task_termination_receipt,
    )

    # The Task operation lock is shared by receipt admission, migration, chat,
    # and every Manager->Worker mutation.  Re-read the complete owner only
    # after entering it: an administrator may have clicked Stop before a
    # durable termination request committed, then waited behind that request.
    await db.rollback()
    async with get_task_operation_lock(body.expected_task_id):
        instance = await db.get(Instance, instance_id)
        if instance is None:
            raise HTTPException(404, "Instance not found")
        exact_owner = (
            await db.execute(
                select(Instance, Task)
                .join(Task, Task.id == Instance.current_task_id)
                .where(
                    Instance.id == instance_id,
                    Instance.current_plan_run_id.is_(None),
                    Instance.current_task_id == body.expected_task_id,
                    (
                        Instance.pid.is_(None)
                        if body.expected_pid is None
                        else Instance.pid == body.expected_pid
                    ),
                    (
                        Instance.started_at.is_(None)
                        if body.expected_started_at is None
                        else Instance.started_at == body.expected_started_at
                    ),
                    Task.id == body.expected_task_id,
                    Task.instance_id == instance_id,
                    Task.turn_generation
                    == body.expected_task_turn_generation,
                )
            )
        ).one_or_none()
        if exact_owner is None:
            raise HTTPException(
                409,
                "Instance Task or process generation changed; refresh before stopping",
            )
        instance, task = exact_owner
        if await active_worker_task_termination_receipt(
            db,
            body.expected_task_id,
        ):
            await db.rollback()
            raise HTTPException(
                409,
                "Task has an active Worker termination receipt",
            )
        if task.mode == "delivery_loop" or task.delivery_run_id is not None:
            raise HTTPException(
                409,
                "Delivery Developer instances are controlled by DeliveryRun; "
                "the process was not stopped",
            )
        if is_pr_sandbox_task(task):
            raise HTTPException(
                409,
                "Automated PR and Delivery Capability reviewer instances are "
                "workflow-controlled; the process was not stopped",
            )

        # The operation lock is an admission preflight, not a lifecycle lease.
        # Ralph shutdown can wait for InstanceManager/consumer cleanup, whose
        # own terminal path may need this same Task lock. Release both the DB
        # snapshot and process-local lock before any lifecycle wait. A receipt
        # admitted afterwards still wins InstanceManager's final SQL gate via
        # ``yield_to_worker_task_termination=True`` below.
        await db.rollback()

    # Stop the producer first so it cannot claim another task immediately
    # after InstanceManager has reaped the current process.
    ralph_was_running = ralph_loop.is_running(instance_id)
    if await ralph_loop.stop(instance_id) is False:
        raise HTTPException(
            status_code=409,
            detail="Ralph loop did not stop; instance process was not changed",
        )
    ok = await instance_manager.stop(
        instance_id,
        expected_task_id=body.expected_task_id,
        expected_task_turn_generation=(
            body.expected_task_turn_generation
        ),
        expected_pid=body.expected_pid,
        expected_started_at=body.expected_started_at,
        terminal_consumer_timeout=30.0,
        consumer_cancel_timeout=10.0,
        yield_to_worker_task_termination=True,
    )
    if not ok:
        db.expire_all()
        if await active_worker_task_termination_receipt(
            db,
            body.expected_task_id,
        ):
            await db.rollback()
            raise HTTPException(
                409,
                "Task has an active Worker termination receipt",
            )
        remaining_exact_owner = await db.scalar(
            select(Instance.id)
            .join(Task, Task.id == Instance.current_task_id)
            .where(
                Instance.id == instance_id,
                Instance.current_task_id == body.expected_task_id,
                Task.id == body.expected_task_id,
                Task.instance_id == instance_id,
                Task.turn_generation
                == body.expected_task_turn_generation,
                (
                    Instance.pid.is_(None)
                    if body.expected_pid is None
                    else Instance.pid == body.expected_pid
                ),
                (
                    Instance.started_at.is_(None)
                    if body.expected_started_at is None
                    else Instance.started_at == body.expected_started_at
                ),
            )
            .with_for_update()
        )
        await db.rollback()
        if remaining_exact_owner is None and ralph_was_running:
            return {"ok": True}
        raise HTTPException(
            409,
            "Instance process cleanup could not be confirmed or its owner changed",
        )
    return {"ok": True}


@router.post("/{instance_id}/run")
async def run_task_on_instance(instance_id: int):
    """Retired: direct launch bypassed TaskQueue ownership and status CAS."""
    raise HTTPException(
        status_code=410,
        detail="Direct Instance execution was removed; create or retry a Task instead",
    )


@router.get("/{instance_id}/logs", response_model=list[LogEntryResponse])
async def get_logs(
    instance_id: int,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    after_id: int | None = Query(default=None, ge=0),
    event_type: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    instance = await db.get(Instance, instance_id)
    if instance is None:
        raise HTTPException(404, "Instance not found")
    if after_id is not None and offset:
        raise HTTPException(
            status_code=422,
            detail="after_id and offset cannot be used together",
        )

    # An Instance is reused across providers, so its current provider is not
    # authoritative for historical rows. Prefer the Task that owns each log
    # and retain the Instance only as the legacy/orphan fallback.
    stmt = (
        select(LogEntry, Task.provider)
        .outerjoin(Task, Task.id == LogEntry.task_id)
        .where(LogEntry.instance_id == instance_id)
    )
    if event_type:
        stmt = stmt.where(LogEntry.event_type == event_type)
    if after_id is not None:
        # Cursor pages are oldest-first so callers can advance monotonically
        # and recover every persisted event missed during a WebSocket outage.
        stmt = (
            stmt.where(LogEntry.id > after_id)
            .order_by(LogEntry.id.asc())
            .limit(limit)
        )
    else:
        # Preserve the historical endpoint contract for initial/latest-page
        # loads and existing callers.
        stmt = stmt.order_by(LogEntry.id.desc()).limit(limit).offset(offset)
    result = await db.execute(stmt)
    entries = list(result.all())
    responses = []
    for entry, task_provider in entries:
        response = LogEntryResponse.model_validate(entry)
        response.protocol_anomaly = detect_assistant_protocol_anomaly(
            entry.event_type,
            entry.role,
            entry.content,
            provider=task_provider or instance.provider,
        )
        responses.append(response)
    return responses


@router.post("/{instance_id}/ralph/start")
async def start_ralph_loop(instance_id: int):
    """Retired: GlobalDispatcher is the only supported task dequeue owner."""
    raise HTTPException(
        status_code=410,
        detail="Ralph Loop was retired; use the global Dispatcher instead",
    )


@router.post("/{instance_id}/ralph/stop")
async def stop_ralph_loop(instance_id: int):
    """Stop the Ralph Loop for an instance."""
    from backend.main import ralph_loop
    if await ralph_loop.stop(instance_id) is False:
        raise HTTPException(
            status_code=409,
            detail="Ralph loop did not stop; exact loop evidence was retained",
        )
    return {"ok": True}


@router.get("/{instance_id}/ralph/status")
async def ralph_loop_status(instance_id: int):
    from backend.main import ralph_loop
    return {"running": ralph_loop.is_running(instance_id)}


# ── Dispatcher endpoints ──

@dispatcher_router.get("/status")
async def dispatcher_status():
    from backend.main import dispatcher
    return dispatcher.status()


@dispatcher_router.post("/start")
async def start_dispatcher(request: Request):
    require_admin(request)
    from backend.main import start_dispatcher_runtime
    try:
        await start_dispatcher_runtime()
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"ok": True, "message": "Dispatcher started"}


@dispatcher_router.post("/stop")
async def stop_dispatcher(request: Request):
    require_admin(request)
    from backend.main import stop_dispatcher_runtime
    try:
        await stop_dispatcher_runtime()
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"ok": True, "message": "Dispatcher stopped"}
