"""Request-aware Task response projection.

Human Task responses deliberately expose only the stable UI contract.  A
Worker deployment credential and a scoped internal-service credential use the
complete wire model instead.  Members whose only possible authority is a
Task-level chat share receive an even narrower top-level projection.
"""

from fastapi import Request
from fastapi.responses import JSONResponse

from backend.api.deps import (
    get_current_user_id,
    has_project_access,
    is_admin,
)
from backend.schemas.task import (
    InternalTaskResponse,
    SharedTaskResponse,
    TaskResponse,
)


def internal_task_wire_request(request: Request) -> bool:
    """Return whether this request is an authenticated machine wire call."""

    return getattr(request.state, "auth_type", None) in {
        "worker_control_plane",
        "internal_service",
    }


async def _chat_share_only_projection(
    request: Request,
    task,
    db,
) -> bool:
    """Conservatively identify Tasks without broader human authority.

    Endpoints perform their normal ACL check before projecting the response.
    Once admin, creator and Project-derived authority have been excluded, the
    only supported member read authority is a direct/group chat share.  If a
    future caller forgets its ACL check, choosing the narrow view here is still
    the fail-closed result.
    """

    if is_admin(request):
        return False
    user_id = get_current_user_id(request)
    if user_id is not None and getattr(task, "created_by", None) == user_id:
        return False
    project_id = getattr(task, "project_id", None)
    if project_id is not None and user_id is not None:
        cache = getattr(request.state, "_task_projection_project_access", None)
        if not isinstance(cache, dict):
            cache = {}
            request.state._task_projection_project_access = cache
        if project_id not in cache:
            cache[project_id] = await has_project_access(
                request,
                project_id,
                db,
            )
        if cache[project_id]:
            return False
    return True


async def task_response(
    request: Request,
    task,
    db,
    *,
    status_code: int = 200,
) -> JSONResponse:
    """Serialize one Task using its authenticated request scope."""

    model = await task_response_model(request, task, db)
    return JSONResponse(
        status_code=status_code,
        content=model.model_dump(mode="json"),
    )


async def task_response_model(request: Request, task, db):
    """Build the request-scoped DTO without committing to an HTTP envelope."""

    if internal_task_wire_request(request):
        return InternalTaskResponse.model_validate(task)
    from backend.services.pr_monitor_task_access import is_pr_monitor_display_task

    if await is_pr_monitor_display_task(db, task):
        # Display Tasks are readable projections, never editable Chat Tasks.
        # Keep the normal public DTO (the description is bounded/safe) while
        # marking the response read-only so the UI cannot offer controls.
        model = TaskResponse.model_validate(task)
        model.access_scope = "chat"
        return model
    if await _chat_share_only_projection(request, task, db):
        return SharedTaskResponse.model_validate(task)
    return TaskResponse.model_validate(task)


async def task_list_response(
    request: Request,
    tasks: list,
    db,
) -> JSONResponse:
    """Serialize a mixed ACL Task list without leaking the broadest item."""

    payload = []
    for task in tasks:
        model = await task_response_model(request, task, db)
        payload.append(model.model_dump(mode="json"))
    return JSONResponse(content=payload)
