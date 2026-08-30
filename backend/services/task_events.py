"""Task status_change 广播收口。

约定：任何写 Task.status 的路径，commit 之后必须广播 status_change 到
"tasks" 频道（broadcaster 会自动镜像到 task:{id} 频道）。此前 cancel/retry/
plan 审批/stop-session/stale 兜底/worker 断连等路径只写库不广播，导致
ChatView（WS 驱动）与任务列表（轮询驱动）状态分叉（2026-07 状态显示大排查）。

必须在 db.commit() 之后调用——先广播会让手快的客户端立刻回读到旧状态。
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


# A pending row is a delete-on-ACK publication marker.  It deliberately uses
# an event type outside every public chat-history allow-list: the raw payload
# contains internal generation fences and must never render as conversation.
PTY_TERMINAL_PUBLICATION_EVENT_TYPE = (
    "pty_terminal_publication_pending"
)
PTY_TERMINAL_PUBLICATION_VERSION = 1


@dataclass(frozen=True, slots=True)
class PtyTerminalPublication:
    """Validated immutable payload stored in one pending LogEntry."""

    idempotency_key: str
    task_id: int
    incarnation_id: str | None
    retry_count: int
    turn_generation: int
    session_id: str
    source_background_generation: str
    status: str
    instance_id: int | None
    started_at: datetime | None
    completed_at: datetime | None
    events: tuple[tuple[str, dict[str, Any]], ...]


def _datetime_text(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _parse_datetime(value: object, *, field: str) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError(f"PTY terminal publication {field} is invalid")
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            f"PTY terminal publication {field} is invalid"
        ) from exc


def pty_terminal_publication_idempotency_key(
    *,
    task_id: int,
    incarnation_id: str | None,
    retry_count: int,
    turn_generation: int,
    session_id: str,
    source_background_generation: str,
) -> str:
    """Bind every replay to one exact Task/session/background generation."""

    identity = {
        "task_id": task_id,
        "incarnation_id": incarnation_id,
        "retry_count": retry_count,
        "turn_generation": turn_generation,
        "session_id": session_id,
        "source_background_generation": source_background_generation,
    }
    digest = hashlib.sha256(
        json.dumps(
            identity,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    ).hexdigest()
    return f"pty-terminal-v{PTY_TERMINAL_PUBLICATION_VERSION}:{digest}"


def build_pty_terminal_publication_payload(
    *,
    task_id: int,
    incarnation_id: str | None,
    retry_count: int,
    turn_generation: int,
    session_id: str,
    source_background_generation: str,
    status: str,
    instance_id: int | None,
    started_at: datetime | None,
    completed_at: datetime | None,
) -> str:
    """Build the immutable event batch committed with the terminal Task row."""

    event_id = pty_terminal_publication_idempotency_key(
        task_id=task_id,
        incarnation_id=incarnation_id,
        retry_count=retry_count,
        turn_generation=turn_generation,
        session_id=session_id,
        source_background_generation=source_background_generation,
    )
    background_payload = {
        "event": "background_activity",
        "event_type": "background_activity",
        "event_id": event_id,
        "task_id": task_id,
        "task_retry_count": retry_count,
        "task_turn_generation": turn_generation,
        "background_active": False,
    }
    events: list[dict[str, Any]] = [
        {"channel": "tasks", "data": background_payload},
        {"channel": f"task:{task_id}", "data": background_payload},
    ]
    if status == "completed":
        events.extend(
            (
                {
                    "channel": "tasks",
                    "data": {
                        "event": "status_change",
                        "event_id": event_id,
                        "task_id": task_id,
                        "task_retry_count": retry_count,
                        "task_turn_generation": turn_generation,
                        "new_status": "completed",
                        "background_active": False,
                    },
                },
                {
                    "channel": f"task:{task_id}",
                    "data": {
                        "event_type": "process_exit",
                        "event_id": event_id,
                        "task_id": task_id,
                        "task_retry_count": retry_count,
                        "task_turn_generation": turn_generation,
                        "exit_code": 0,
                        "stderr": None,
                        "background": True,
                    },
                },
            )
        )
    payload = {
        "version": PTY_TERMINAL_PUBLICATION_VERSION,
        "kind": "pty_background_terminal",
        "idempotency_key": event_id,
        "identity": {
            "task_id": task_id,
            "incarnation_id": incarnation_id,
            "retry_count": retry_count,
            "turn_generation": turn_generation,
            "session_id": session_id,
            "source_background_generation": (
                source_background_generation
            ),
            # These are the committed post-transition fields rechecked by the
            # publisher.  The source marker is retained separately above.
            "status": status,
            "instance_id": instance_id,
            "started_at": _datetime_text(started_at),
            "completed_at": _datetime_text(completed_at),
        },
        "events": events,
    }
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def parse_pty_terminal_publication_payload(
    raw_json: object,
) -> PtyTerminalPublication:
    """Fail closed on a malformed or identity-inconsistent outbox row."""

    if not isinstance(raw_json, str) or not raw_json:
        raise ValueError("PTY terminal publication payload is absent")
    try:
        payload = json.loads(raw_json)
    except (TypeError, ValueError) as exc:
        raise ValueError("PTY terminal publication payload is invalid") from exc
    if (
        not isinstance(payload, dict)
        or payload.get("version") != PTY_TERMINAL_PUBLICATION_VERSION
        or payload.get("kind") != "pty_background_terminal"
    ):
        raise ValueError("PTY terminal publication envelope is invalid")
    identity = payload.get("identity")
    events = payload.get("events")
    if not isinstance(identity, dict) or not isinstance(events, list):
        raise ValueError("PTY terminal publication shape is invalid")

    task_id = identity.get("task_id")
    retry_count = identity.get("retry_count")
    turn_generation = identity.get("turn_generation")
    incarnation_id = identity.get("incarnation_id")
    session_id = identity.get("session_id")
    source_generation = identity.get("source_background_generation")
    status = identity.get("status")
    instance_id = identity.get("instance_id")
    if (
        type(task_id) is not int
        or task_id <= 0
        or type(retry_count) is not int
        or retry_count < 0
        or type(turn_generation) is not int
        or turn_generation < 0
        or (
            incarnation_id is not None
            and (
                not isinstance(incarnation_id, str)
                or len(incarnation_id) != 32
                or any(
                    char not in "0123456789abcdef"
                    for char in incarnation_id
                )
            )
        )
        or not isinstance(session_id, str)
        or not session_id
        or not isinstance(source_generation, str)
        or not source_generation
        or status not in {"in_progress", "executing", "completed"}
        or (
            instance_id is not None
            and (type(instance_id) is not int or instance_id <= 0)
        )
    ):
        raise ValueError("PTY terminal publication identity is invalid")
    expected_key = pty_terminal_publication_idempotency_key(
        task_id=task_id,
        incarnation_id=incarnation_id,
        retry_count=retry_count,
        turn_generation=turn_generation,
        session_id=session_id,
        source_background_generation=source_generation,
    )
    if payload.get("idempotency_key") != expected_key:
        raise ValueError("PTY terminal publication idempotency key is invalid")

    started_at = _parse_datetime(identity.get("started_at"), field="started_at")
    completed_at = _parse_datetime(
        identity.get("completed_at"), field="completed_at"
    )
    # Identity-only validation is insufficient: a corrupted row could retain
    # the right Task generation while changing new_status, exit_code, event
    # order, or another semantic field. Rebuild the only allowed batch from
    # the validated identity and require structural equality, including every
    # key, value, and list position.
    expected_payload = json.loads(
        build_pty_terminal_publication_payload(
            task_id=task_id,
            incarnation_id=incarnation_id,
            retry_count=retry_count,
            turn_generation=turn_generation,
            session_id=session_id,
            source_background_generation=source_generation,
            status=status,
            instance_id=instance_id,
            started_at=started_at,
            completed_at=completed_at,
        )
    )
    canonical_events = json.dumps(
        events,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    expected_events = json.dumps(
        expected_payload["events"],
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    if canonical_events != expected_events:
        raise ValueError("PTY terminal publication event batch is invalid")
    selected_events = tuple(
        (item["channel"], dict(item["data"])) for item in events
    )

    return PtyTerminalPublication(
        idempotency_key=expected_key,
        task_id=task_id,
        incarnation_id=incarnation_id,
        retry_count=retry_count,
        turn_generation=turn_generation,
        session_id=session_id,
        source_background_generation=source_generation,
        status=status,
        instance_id=instance_id,
        started_at=started_at,
        completed_at=completed_at,
        events=selected_events,
    )


async def broadcast_status_change(
    task_id: int,
    new_status: str,
    instance_id: int | None = None,
    *,
    background_active: bool | None = None,
) -> None:
    """Broadcast a task status_change on the "tasks" channel (best-effort)."""
    try:
        from backend.main import broadcaster

        data: dict = {
            "event": "status_change",
            "task_id": task_id,
            "new_status": new_status,
        }
        if instance_id is not None:
            data["instance_id"] = instance_id
        if type(background_active) is bool:
            data["background_active"] = background_active
        await broadcaster.broadcast("tasks", data)
    except Exception:
        # 广播失败不能影响状态写入本身；前端有 5s 轮询兜底
        logger.exception("status_change broadcast failed for task %s", task_id)
