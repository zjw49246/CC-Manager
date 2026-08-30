"""Human-facing endpoints for the generic Capability Core."""

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy.ext.asyncio import AsyncSession

from backend.api.deps import (
    get_current_user_id,
    lock_task_effect_access,
    require_task_control,
)
from backend.database import get_db
from backend.models.task import Task
from backend.schemas.capability import (
    CapabilityExecutionResource,
    CapabilityInvocationCancel,
    CapabilityInvocationConsume,
    CapabilityInvocationCreate,
    CapabilityInvocationCreateResource,
    CapabilityInvocationResource,
    CapabilityResultResource,
)
from backend.services.capability_result import resolve_capability_result
from backend.services.capability_service import (
    CapabilityConflictError,
    CapabilityDisabledError,
    CapabilityError,
    CapabilityNotFoundError,
    CapabilityUnavailableError,
    CapabilityUnsupportedScopeError,
    CapabilityValidationError,
    active_execution_for,
    cancel_invocation,
    consume_ready_invocation,
    create_human_invocation,
    get_invocation,
    list_task_invocations,
)


router = APIRouter(prefix="/api", tags=["capabilities"])


def _http_error(exc: CapabilityError) -> HTTPException:
    if isinstance(exc, CapabilityNotFoundError):
        return HTTPException(404, str(exc))
    if isinstance(exc, CapabilityConflictError):
        return HTTPException(409, str(exc))
    if isinstance(exc, CapabilityValidationError):
        return HTTPException(422, str(exc))
    if isinstance(exc, CapabilityUnsupportedScopeError):
        return HTTPException(409, str(exc))
    if isinstance(exc, (CapabilityDisabledError, CapabilityUnavailableError)):
        return HTTPException(503, str(exc))
    return HTTPException(500, "Capability operation failed")


async def _resource(
    db: AsyncSession,
    invocation,
) -> CapabilityInvocationResource:
    resource = CapabilityInvocationResource.model_validate(invocation)
    execution = await active_execution_for(db, invocation.id)
    if execution is not None:
        resource.active_execution = CapabilityExecutionResource.model_validate(
            execution
        )
    return resource


@router.post(
    "/tasks/{task_id}/capability-invocations",
    response_model=CapabilityInvocationCreateResource,
)
async def create_capability_invocation(
    task_id: int,
    body: CapabilityInvocationCreate,
    request: Request,
    response: Response,
    db: AsyncSession = Depends(get_db),
):
    task = await db.get(Task, task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    await require_task_control(request, task, db)
    from backend.api.tasks import _require_not_delivery_owned_task

    _require_not_delivery_owned_task(
        task,
        action="given ad-hoc capability invocations",
    )

    async def lock_effect_task(
        locked_db: AsyncSession,
        observed_task: Task,
        worker_node_fence_held: bool,
    ) -> Task:
        locked_task = await lock_task_effect_access(
            request,
            observed_task,
            locked_db,
            allow_chat_share=False,
            fence_worker_node=worker_node_fence_held,
            worker_node_fence_held=worker_node_fence_held,
        )
        await require_task_control(request, locked_task, locked_db)
        _require_not_delivery_owned_task(
            locked_task,
            action="given ad-hoc capability invocations",
        )
        return locked_task

    try:
        invocation, created = await create_human_invocation(
            db,
            task_id=task_id,
            capability_key=body.capability,
            request_payload=body.request,
            idempotency_key=body.idempotency_key,
            requested_by_user_id=get_current_user_id(request),
            lock_effect_task=lock_effect_task,
        )
    except CapabilityError as exc:
        raise _http_error(exc) from exc
    response.status_code = 201 if created else 200
    return CapabilityInvocationCreateResource(
        invocation=await _resource(db, invocation),
        created=created,
    )


@router.get(
    "/tasks/{task_id}/capability-invocations",
    response_model=list[CapabilityInvocationResource],
)
async def list_capability_invocations(
    task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    task = await db.get(Task, task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    # Invocation records include the raw capability input, frozen native
    # session/instance generation and executor audit.  They are control-plane
    # resources, not part of a chat-only Task share.
    await require_task_control(request, task, db)
    invocations = await list_task_invocations(db, task_id)
    return [await _resource(db, invocation) for invocation in invocations]


@router.get(
    "/capability-invocations/{invocation_id}",
    response_model=CapabilityInvocationResource,
)
async def read_capability_invocation(
    invocation_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    try:
        invocation = await get_invocation(db, invocation_id)
    except CapabilityError as exc:
        raise _http_error(exc) from exc
    task = await db.get(Task, invocation.task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    await require_task_control(request, task, db)
    return await _resource(db, invocation)


@router.get(
    "/capability-invocations/{invocation_id}/result",
    response_model=CapabilityResultResource,
)
async def read_capability_result(
    invocation_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    try:
        invocation = await get_invocation(db, invocation_id)
    except CapabilityError as exc:
        raise _http_error(exc) from exc
    task = await db.get(Task, invocation.task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    await require_task_control(request, task, db)
    try:
        resolved = await resolve_capability_result(db, invocation)
        return CapabilityResultResource.model_validate(resolved.as_payload())
    except CapabilityError as exc:
        raise _http_error(exc) from exc


@router.post(
    "/capability-invocations/{invocation_id}/consume",
    response_model=CapabilityInvocationResource,
)
async def consume_capability_invocation(
    invocation_id: int,
    body: CapabilityInvocationConsume,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    try:
        observed = await get_invocation(db, invocation_id)
    except CapabilityError as exc:
        raise _http_error(exc) from exc
    task = await db.get(Task, observed.task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    await require_task_control(request, task, db)
    from backend.api.tasks import _require_not_delivery_owned_task

    _require_not_delivery_owned_task(
        task,
        action="had capability results consumed outside its Delivery Run",
    )

    async def lock_effect_task(
        locked_db: AsyncSession,
        observed_task: Task,
        _worker_node_fence_held: bool,
    ) -> Task:
        locked_task = await lock_task_effect_access(
            request,
            observed_task,
            locked_db,
            allow_chat_share=False,
        )
        await require_task_control(request, locked_task, locked_db)
        _require_not_delivery_owned_task(
            locked_task,
            action="had capability results consumed outside its Delivery Run",
        )
        return locked_task

    try:
        invocation = await consume_ready_invocation(
            db,
            invocation_id=invocation_id,
            expected_state_version=body.expected_state_version,
            lock_effect_task=lock_effect_task,
        )
    except CapabilityError as exc:
        raise _http_error(exc) from exc
    return await _resource(db, invocation)


@router.post(
    "/capability-invocations/{invocation_id}/cancel",
    response_model=CapabilityInvocationResource,
)
async def cancel_capability_invocation(
    invocation_id: int,
    body: CapabilityInvocationCancel,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    try:
        observed = await get_invocation(db, invocation_id)
    except CapabilityError as exc:
        raise _http_error(exc) from exc
    task = await db.get(Task, observed.task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    await require_task_control(request, task, db)
    from backend.api.tasks import _require_not_delivery_owned_task

    _require_not_delivery_owned_task(
        task,
        action="had capabilities cancelled outside its Delivery Run",
    )

    async def lock_effect_task(
        locked_db: AsyncSession,
        observed_task: Task,
        _worker_node_fence_held: bool,
    ) -> Task:
        locked_task = await lock_task_effect_access(
            request,
            observed_task,
            locked_db,
            allow_chat_share=False,
        )
        await require_task_control(request, locked_task, locked_db)
        _require_not_delivery_owned_task(
            locked_task,
            action="had capabilities cancelled outside its Delivery Run",
        )
        return locked_task

    try:
        invocation = await cancel_invocation(
            db,
            invocation_id=invocation_id,
            expected_state_version=body.expected_state_version,
            lock_effect_task=lock_effect_task,
        )
    except CapabilityError as exc:
        raise _http_error(exc) from exc
    return await _resource(db, invocation)
