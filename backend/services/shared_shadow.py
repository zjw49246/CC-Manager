"""Ownership fences for receiver-side shared-task shadow rows."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.task import Task
from backend.models.task_share import SharedTaskReceived


def _identity_predicates(shared: SharedTaskReceived) -> tuple:
    """Pin a detached relay object to the exact received-share identity."""

    return (
        SharedTaskReceived.id == shared.id,
        SharedTaskReceived.owner_ccm_url == shared.owner_ccm_url,
        SharedTaskReceived.remote_task_id == shared.remote_task_id,
        SharedTaskReceived.share_token == shared.share_token,
    )


async def lock_shared_record(
    db: AsyncSession,
    shared: SharedTaskReceived,
    *,
    require_active: bool = True,
) -> SharedTaskReceived | None:
    """Lock and reload the exact received-share row represented by ``shared``."""

    predicates = list(_identity_predicates(shared))
    if require_active:
        predicates.append(SharedTaskReceived.status == "active")
    return (
        await db.execute(
            select(SharedTaskReceived)
            .where(*predicates)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()


async def lock_owned_shadow(
    db: AsyncSession,
    shared: SharedTaskReceived,
    *,
    require_active: bool = True,
) -> tuple[SharedTaskReceived, Task] | None:
    """Lock a share then its shadow and prove both ownership directions.

    ``local_task_id`` alone is not authority.  A stale relay must also prove
    that the current Task still names the same ``SharedTaskReceived`` row as
    its reverse owner before it can append logs or change Task state.
    """

    current = await lock_shared_record(
        db,
        shared,
        require_active=require_active,
    )
    if current is None or current.local_task_id is None:
        return None
    expected_local_task_id = shared.local_task_id
    if (
        expected_local_task_id is not None
        and current.local_task_id != expected_local_task_id
    ):
        return None
    task = (
        await db.execute(
            select(Task)
            .where(
                Task.id == current.local_task_id,
                Task.shared_from_id == current.id,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if task is None:
        return None
    return current, task
