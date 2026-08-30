"""Startup reconciliation for interrupted mutating Task SSH effects."""

from datetime import datetime

from sqlalchemy import update

from backend.database import async_session
from backend.models.task_ssh_effect import TaskSSHEffectReceipt


async def recover_interrupted_task_ssh_effects(
    db_factory=async_session,
) -> int:
    """Conservatively settle every crash-left ``running`` receipt.

    A Manager restart cannot prove whether the remote side effect happened.
    Marking it ambiguous permanently forbids blind replay while releasing the
    SQLite permit triggers that protect the effect's authorization graph.
    This must run after migrations and before any runtime Task writer starts.
    """

    now = datetime.utcnow()
    async with db_factory() as db:
        changed = await db.execute(
            update(TaskSSHEffectReceipt)
            .where(TaskSSHEffectReceipt.status == "running")
            .values(
                status="ambiguous",
                outcome_code="manager_restart_unknown",
                result_payload=None,
                result_digest=None,
                result_compacted=False,
                completed_at=now,
                updated_at=now,
            )
        )
        await db.commit()
        return int(changed.rowcount or 0)
