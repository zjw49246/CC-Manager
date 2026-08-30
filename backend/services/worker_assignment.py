"""Durable admission fence for assigning work to a Worker.

Worker status reads are advisory: a destroy request can claim the Worker after
the read but before a Project, Task, Plan, or MonitoredRepo commits its
``worker_id``.  Every writer uses the portable no-op UPDATE below in the same
transaction as that pointer.  The Worker lifecycle transition uses the same
row, so exactly one ordering wins:

* assignment first: destroy waits, then observes the committed owner; or
* destroy first: assignment observes a non-ready Worker and fails closed.
"""

from __future__ import annotations

from fastapi import HTTPException
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.worker import Worker


class WorkerAssignmentConflict(HTTPException):
    """The requested Worker cannot accept a new durable owner."""

    def __init__(
        self,
        worker_id: int,
        *,
        status: str | None,
        bootstrap_step: str | None,
    ) -> None:
        self.worker_id = worker_id
        self.status = status
        self.bootstrap_step = bootstrap_step
        if status is None:
            detail = f"Worker {worker_id} does not exist"
        else:
            detail = (
                f"Worker {worker_id} is not ready for assignment "
                f"(status={status}, bootstrap_step={bootstrap_step or 'none'})"
            )
        super().__init__(status_code=409, detail=detail)


async def fence_ready_worker_assignment(
    db: AsyncSession,
    worker_id: int | None,
) -> None:
    """Fence one optional Worker assignment without owning the transaction."""

    if worker_id is None:
        return
    fenced = await db.execute(
        update(Worker)
        .where(
            Worker.id == worker_id,
            Worker.status == "ready",
            Worker.bootstrap_step.is_(None),
        )
        # Preserve lifecycle identity.  ``updated_at`` has a Python onupdate
        # hook, so assigning it to itself is required for a true no-op fence.
        .values(status=Worker.status, updated_at=Worker.updated_at)
    )
    if fenced.rowcount == 1:
        return
    row = (
        await db.execute(
            select(Worker.status, Worker.bootstrap_step).where(
                Worker.id == worker_id
            )
        )
    ).one_or_none()
    raise WorkerAssignmentConflict(
        worker_id,
        status=row.status if row is not None else None,
        bootstrap_step=row.bootstrap_step if row is not None else None,
    )
