"""Crash recovery for Worker-local Project materialization jobs."""

from collections.abc import Callable

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.database import async_session
from backend.models.project import Project
from backend.services.worker_node_control import (
    fence_worker_node_receipt_resolution,
)


ACTIVE_PROJECT_MATERIALIZATION_STATUSES = frozenset({
    "pending",
    "cloning",
    "initializing",
})
_INTERRUPTED_PROJECT_ERROR = (
    "Worker restarted before Project materialization completed; retry setup"
)


async def recover_interrupted_worker_project_materializations(
    db_factory: Callable[[], AsyncSession] = async_session,
) -> int:
    """Fail crash-left Worker clone/init rows before runtime admission opens.

    Project clone jobs are in-process coroutines.  A process restart proves
    that no such coroutine remains, so an active status can no longer make
    progress.  Preserve the Project as retryable ``error`` state instead of
    leaving drain proof permanently blocked by an ownerless job.
    """

    if settings.ccm_node_role != "worker":
        return 0
    async with db_factory() as db:
        # Recovery resolves ownership admitted before a possible durable drain
        # claim; it must not reopen admission on a draining Worker.
        await fence_worker_node_receipt_resolution(db)
        recovered = await db.execute(
            update(Project)
            .where(
                Project.worker_id.is_(None),
                Project.status.in_(ACTIVE_PROJECT_MATERIALIZATION_STATUSES),
            )
            .values(
                status="error",
                error_message=_INTERRUPTED_PROJECT_ERROR,
            )
            .execution_options(synchronize_session=False)
        )
        await db.commit()
        return int(recovered.rowcount or 0)
