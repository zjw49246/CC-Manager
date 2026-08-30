"""Small real-receipt builders shared by admission regression tests."""

import uuid
from datetime import datetime, timedelta

from backend.models.task import Task
from backend.services.worker_task_termination import (
    canonical_json_digest,
    create_or_resume_manager_receipt,
    stage_worker_receipt,
)


async def persist_active_worker_receipt(
    db_factory,
    task_id: int,
    *,
    operation: str = "stop_session",
    executing: bool = False,
):
    """Stage the Worker-side receipt without executing its cleanup."""

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task is not None
        operation_id = uuid.uuid4().hex
        payload = {
            "version": 2,
            "operation_id": operation_id,
            "task_id": task.id,
            "operation": operation,
            "manager_worker_id": 41,
            "expected_remote": {
                "status": task.status,
                "retry_count": task.retry_count,
                "turn_generation": task.turn_generation,
            },
            "manager_handoff": None,
        }
        receipt = await stage_worker_receipt(
            db,
            task_id=task.id,
            operation_id=operation_id,
            operation=operation,
            request_payload=payload,
            request_digest=canonical_json_digest(payload),
        )
        assert receipt.status == "accepted"
        if executing:
            now = datetime.utcnow()
            receipt.status = "executing"
            receipt.state_version += 1
            receipt.execution_token = uuid.uuid4().hex
            receipt.next_reconcile_at = now + timedelta(seconds=90)
            receipt.updated_at = now
            await db.commit()
        assert receipt.active_task_id == task.id
        return receipt


async def persist_active_manager_receipt(
    db_factory,
    task_id: int,
    *,
    operation: str = "stop_session",
):
    """Commit a Manager pending_remote receipt for a Worker-owned Task."""

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task is not None
        receipt = await create_or_resume_manager_receipt(
            db,
            task,
            operation=operation,
        )
        assert receipt.status == "pending_remote"
        assert receipt.active_task_id == task.id
        return receipt
