from datetime import datetime, timezone
import hashlib
import json

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy import delete, desc, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from backend.database import get_db
from backend.api.deps import (
    get_current_user_id,
    lock_project_effect_access,
    lock_project_worker_effect_access,
    require_project_access,
    task_execution_principal_from_request,
)
from backend.api.task_projection import task_response
from backend.models.delivery import DeliveryRun
from backend.models.project import Project
from backend.models.project_todo import ProjectTodo
from backend.models.task import Task
from backend.schemas.project_todo import (
    ProjectTodoCreate,
    ProjectTodoResponse,
    ProjectTodoTaskCreate,
    ProjectTodoUpdate,
)
from backend.schemas.task import TaskResponse
from backend.services.project_readiness import (
    ProjectNotDispatchableError,
    require_project_dispatchable,
)
from backend.services.task_creation import stage_task_record

router = APIRouter(prefix="/api/projects/{project_id}/todos", tags=["project-todos"])

_TODO_TASK_ADMISSION_KEY = "project_todo_task_admission"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


async def _require_project(project_id: int, db: AsyncSession) -> Project:
    project = await db.get(Project, project_id)
    if not project:
        raise HTTPException(404, "Project not found")
    return project


async def _lock_authorized_project_effect(
    request: Request,
    project_id: int,
    db: AsyncSession,
    *,
    fence_worker_assignment: bool = False,
) -> Project:
    """Fence one Todo write against concurrent Project ACL revocation.

    Team Project share add/remove uses the Project row as its portable writer
    boundary.  Every Todo mutation must take that same boundary and then
    re-read the caller's ACL inside the resulting transaction.  The optimistic
    check at the route entry avoids taking a write lock for callers that were
    already unauthorized; this check is the authoritative one after waiting.
    """

    # End the optimistic ACL/read snapshot before SQLite's portable writer
    # fence. No durable Todo mutation has started at this point.
    await db.rollback()
    if fence_worker_assignment:
        return await lock_project_worker_effect_access(
            request,
            project_id,
            db,
        )
    return await lock_project_effect_access(request, project_id, db)


async def _require_todo(
    project_id: int,
    todo_id: int,
    db: AsyncSession,
    *,
    for_update: bool = False,
) -> ProjectTodo:
    statement = select(ProjectTodo).where(
        ProjectTodo.id == todo_id,
        ProjectTodo.project_id == project_id,
    )
    if for_update:
        statement = statement.with_for_update().execution_options(
            populate_existing=True
        )
    result = await db.execute(statement)
    todo = result.scalar_one_or_none()
    if not todo:
        raise HTTPException(404, "Todo not found")
    return todo


def _todo_has_no_delivery_owner():
    return ~select(DeliveryRun.id).where(
        DeliveryRun.source_todo_id == ProjectTodo.id
    ).exists()


async def _delivery_owner_id(db: AsyncSession, todo_id: int) -> int | None:
    return await db.scalar(
        select(DeliveryRun.id)
        .where(DeliveryRun.source_todo_id == todo_id)
        .limit(1)
    )


async def _require_unowned_todo(db: AsyncSession, todo: ProjectTodo) -> None:
    owner_id = await _delivery_owner_id(db, todo.id)
    if owner_id is not None:
        raise HTTPException(
            409,
            f"Todo is owned by Delivery Run {owner_id} and is immutable",
        )


def _todo_task_request_hash(body: ProjectTodoTaskCreate) -> str:
    payload = {
        "schema_version": 1,
        **body.model_dump(mode="json"),
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


async def _replayed_todo_task(
    db: AsyncSession,
    *,
    todo: ProjectTodo,
    request_hash: str,
) -> Task | None:
    """Return the one exact Task already claimed by this Todo, if any."""

    if todo.created_task_id is None:
        return None
    task = await db.get(Task, todo.created_task_id, populate_existing=True)
    marker = (
        (task.metadata_ or {}).get(_TODO_TASK_ADMISSION_KEY)
        if task is not None and isinstance(task.metadata_, dict)
        else None
    )
    if (
        task is None
        or task.project_id != todo.project_id
        or todo.task_request_hash != request_hash
        or not isinstance(marker, dict)
        or marker.get("schema_version") != 1
        or marker.get("todo_id") != todo.id
        or marker.get("request_hash") != request_hash
    ):
        raise HTTPException(
            409,
            "Todo is already claimed by a different or unverifiable Task request",
        )
    return task


@router.get("", response_model=list[ProjectTodoResponse])
async def list_project_todos(
    project_id: int,
    request: Request,
    include_archived: bool = False,
    db: AsyncSession = Depends(get_db),
):
    await require_project_access(request, project_id, db)
    await _require_project(project_id, db)
    stmt = select(ProjectTodo).where(ProjectTodo.project_id == project_id)
    if not include_archived:
        stmt = stmt.where(ProjectTodo.status != "archived")
    result = await db.execute(
        stmt.order_by(desc(ProjectTodo.sort_order), desc(ProjectTodo.id))
    )
    return list(result.scalars().all())


@router.post("", response_model=ProjectTodoResponse, status_code=201)
async def create_project_todo(
    project_id: int,
    body: ProjectTodoCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    await require_project_access(request, project_id, db)
    title = body.title.strip()
    prompt = body.prompt.strip()
    if not title or not prompt:
        raise HTTPException(400, "Title and prompt are required")

    await db.rollback()
    await _lock_authorized_project_effect(request, project_id, db)

    # New todos go to the top: one step above the current max sort_order.
    max_sort_order = await db.scalar(
        select(func.coalesce(func.max(ProjectTodo.sort_order), 0)).where(ProjectTodo.project_id == project_id)
    )
    todo = ProjectTodo(
        project_id=project_id,
        title=title,
        prompt=prompt,
        status="open",
        sort_order=(max_sort_order or 0) + 100,
    )
    db.add(todo)
    await db.commit()
    await db.refresh(todo)
    return todo


@router.patch("/{todo_id}", response_model=ProjectTodoResponse)
async def update_project_todo(
    project_id: int,
    todo_id: int,
    body: ProjectTodoUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Partial update. Also the canonical way to archive (status='archived')
    or restore (status='open') — DELETE is reserved for permanent removal."""
    await require_project_access(request, project_id, db)
    await db.rollback()
    await _lock_authorized_project_effect(request, project_id, db)
    todo = await _require_todo(project_id, todo_id, db, for_update=True)
    await _require_unowned_todo(db, todo)
    updates = body.model_dump(exclude_unset=True)

    if "title" in updates and updates["title"] is not None:
        title = updates["title"].strip()
        if not title:
            raise HTTPException(400, "Title is required")
        updates["title"] = title
    if "prompt" in updates and updates["prompt"] is not None:
        prompt = updates["prompt"].strip()
        if not prompt:
            raise HTTPException(400, "Prompt is required")
        updates["prompt"] = prompt

    guarded = await db.execute(
        update(ProjectTodo)
        .where(
            ProjectTodo.id == todo.id,
            ProjectTodo.project_id == project_id,
            _todo_has_no_delivery_owner(),
        )
        .values(**updates, updated_at=_utcnow())
        .execution_options(synchronize_session=False)
    )
    if guarded.rowcount != 1:
        owner_id = await _delivery_owner_id(db, todo.id)
        await db.rollback()
        if owner_id is not None:
            raise HTTPException(
                409,
                f"Todo is owned by Delivery Run {owner_id} and is immutable",
            )
        raise HTTPException(409, "Todo changed while it was being updated")

    await db.commit()
    await db.refresh(todo)
    return todo


@router.delete("/{todo_id}")
async def delete_project_todo(
    project_id: int,
    todo_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Permanently delete a todo. To hide without destroying, PATCH status='archived'."""
    await require_project_access(request, project_id, db)
    await db.rollback()
    await _lock_authorized_project_effect(request, project_id, db)
    todo = await _require_todo(project_id, todo_id, db, for_update=True)
    await _require_unowned_todo(db, todo)
    deleted = await db.execute(
        delete(ProjectTodo).where(
            ProjectTodo.id == todo.id,
            ProjectTodo.project_id == project_id,
            _todo_has_no_delivery_owner(),
        )
    )
    if deleted.rowcount != 1:
        owner_id = await _delivery_owner_id(db, todo.id)
        await db.rollback()
        if owner_id is not None:
            raise HTTPException(
                409,
                f"Todo is owned by Delivery Run {owner_id} and is immutable",
            )
        raise HTTPException(409, "Todo changed while it was being deleted")
    await db.commit()
    return {"ok": True}


@router.post("/{todo_id}/task", response_model=TaskResponse, status_code=201)
async def create_task_from_todo(
    project_id: int,
    todo_id: int,
    body: ProjectTodoTaskCreate,
    request: Request,
    response: Response,
    db: AsyncSession = Depends(get_db),
):
    """Atomically create an ordinary Task and claim its source Todo.

    The conditional claim shares the Todo row with Delivery admission.  A
    normal Task and a Delivery Run can therefore never both survive a race.
    """

    await require_project_access(request, project_id, db)
    await db.rollback()
    project = await _lock_authorized_project_effect(
        request,
        project_id,
        db,
        fence_worker_assignment=True,
    )
    todo = await _require_todo(project_id, todo_id, db, for_update=True)
    await _require_unowned_todo(db, todo)
    request_hash = _todo_task_request_hash(body)
    task = await _replayed_todo_task(
        db,
        todo=todo,
        request_hash=request_hash,
    )
    if task is not None:
        # The Todo is the single-use idempotency domain.  Closing this read
        # transaction also refreshes the response boundary after a prior POST
        # committed but its HTTP response was lost.
        await db.commit()
        response.status_code = 200
    elif todo.status != "open":
        raise HTTPException(409, "Todo is not open or is already claimed")

    if task is None:
        try:
            require_project_dispatchable(project)
        except ProjectNotDispatchableError as exc:
            raise HTTPException(422, exc.detail) from exc
        try:
            task = await stage_task_record(
                db,
                title=body.title,
                description=body.prompt,
                status="pending",
                priority=0,
                project_id=project.id,
                target_repo=project.local_path,
                target_branch=project.default_branch or "main",
                worker_id=project.worker_id,
                created_by=get_current_user_id(request),
                # Worker is an execution location, not a lower-trust user.
                # Keep the Manager-side native principal here; forwarding
                # converts it to the delegated wire form after the Worker
                # protocol preflight.  Purpose-built Plan/Review children use
                # their own system principal at their separate boundaries.
                **task_execution_principal_from_request(request),
                provider=body.provider,
                model=body.model,
                codex_service_tier=body.codex_service_tier,
                effort_level=body.effort_level,
                timeout_hours=body.timeout_hours,
                mode="auto",
                metadata_={
                    _TODO_TASK_ADMISSION_KEY: {
                        "schema_version": 1,
                        "todo_id": todo.id,
                        "request_hash": request_hash,
                    }
                },
            )
            claimed = await db.execute(
                update(ProjectTodo)
                .where(
                    ProjectTodo.id == todo.id,
                    ProjectTodo.project_id == project.id,
                    ProjectTodo.status == "open",
                    ProjectTodo.created_task_id.is_(None),
                    ProjectTodo.task_request_hash.is_(None),
                    _todo_has_no_delivery_owner(),
                )
                .values(
                    status="done",
                    created_task_id=task.id,
                    task_request_hash=request_hash,
                    updated_at=_utcnow(),
                )
            )
            if claimed.rowcount != 1:
                raise HTTPException(
                    409,
                    "Todo was claimed by another Task or Delivery Run",
                )
            await db.commit()
            await db.refresh(task)
        except HTTPException:
            await db.rollback()
            raise
        except ValueError as exc:
            await db.rollback()
            raise HTTPException(422, str(exc)) from exc
        except IntegrityError as exc:
            await db.rollback()
            raise HTTPException(409, "Todo Task admission conflicted") from exc

    assert task is not None
    try:
        from backend.main import dispatcher

        if dispatcher is not None:
            dispatcher.wake()
    except (ImportError, AttributeError):
        pass
    return await task_response(
        request,
        task,
        db,
        status_code=response.status_code or 201,
    )
