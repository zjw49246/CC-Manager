"""Authoritative identity fields for persisted Task Chat events."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from backend.models.log_entry import LogEntry
from backend.services.stream_parser import detect_assistant_protocol_anomaly


def _utc_isoformat(value: datetime) -> str:
    """Serialize CCM's UTC-naive database timestamps as explicit UTC."""

    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return value.isoformat().replace("+00:00", "Z")


def persisted_chat_event(
    entry: LogEntry,
    payload: Mapping[str, Any],
    *,
    provider: object = "claude",
) -> dict[str, Any]:
    """Attach the local committed LogEntry identity to a WS payload.

    Callers must commit (or at least flush) ``entry`` before using this helper.
    Identity fields intentionally override any remote/untrusted values already
    present in ``payload``.
    """

    if entry.id is None or entry.task_id is None or entry.timestamp is None:
        raise RuntimeError(
            "Persisted chat events require a flushed LogEntry id, task_id, "
            "and timestamp"
        )
    normalized_payload = dict(payload)
    anomaly = detect_assistant_protocol_anomaly(
        entry.event_type,
        entry.role,
        entry.content,
        provider=provider,
    )
    if anomaly:
        # The marker is derived from the committed local row.  A remote relay
        # may omit it or provide an arbitrary value, but neither can override
        # this canonical result.
        normalized_payload["protocol_anomaly"] = anomaly
    else:
        normalized_payload.pop("protocol_anomaly", None)
    return {
        **normalized_payload,
        "id": entry.id,
        "task_id": entry.task_id,
        "timestamp": _utc_isoformat(entry.timestamp),
    }
