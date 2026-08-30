"""Best-effort realtime invalidations for durable capability state."""

import logging

from backend.models.capability import CapabilityInvocation


logger = logging.getLogger(__name__)


async def broadcast_capability_event(
    event_type: str,
    invocation: CapabilityInvocation,
    *,
    created: bool | None = None,
) -> None:
    payload: dict = {
        "event": event_type,
        "invocation_id": invocation.id,
        "task_id": invocation.task_id,
        "capability": invocation.capability_key,
        "status": invocation.status,
        "state_version": invocation.state_version,
    }
    if created is not None:
        payload["created"] = created
    try:
        from backend.main import broadcaster

        await broadcaster.broadcast("capabilities", payload)
        await broadcaster.broadcast(f"task:{invocation.task_id}", payload)
    except Exception:
        logger.exception(
            "Capability event broadcast failed for invocation %s",
            invocation.id,
        )
