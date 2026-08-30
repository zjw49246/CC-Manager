"""Best-effort WebSocket invalidations for durable Delivery state."""

import logging


logger = logging.getLogger(__name__)


async def broadcast_delivery_event(
    event: str,
    *,
    run_id: int,
    project_id: int | None = None,
    state_version: int | None = None,
    cycle_id: int | None = None,
) -> None:
    payload: dict = {"event": event, "run_id": run_id}
    if project_id is not None:
        payload["project_id"] = project_id
    if state_version is not None:
        payload["state_version"] = state_version
    if cycle_id is not None:
        payload["cycle_id"] = cycle_id
    try:
        from backend.main import broadcaster

        await broadcaster.broadcast("deliveries", payload)
        await broadcaster.broadcast(f"delivery:{run_id}", payload)
    except Exception:
        logger.exception("Delivery event broadcast failed for Run %s", run_id)
