"""WorkerRelay — Worker CCM 事件中继（elastic-worker 设计 §6/§7/§11）。

每个 Worker 一条 WS 连接，订阅 `tasks` 全局 channel + 各活跃 task 的
`task:{id}` channel。收到事件后：
1. chat 类事件双写 Manager DB（LogEntry，instance_id=None）——历史永远查本地，
   Worker 离线/销毁后日志依然完整
2. 同步 task 状态/cost/plan/loop/goal/monitor 到 Manager DB
3. 镜像广播到 Manager 前端的同名 channel（前端零改动）

已知陷阱（实现处有注释）：worker 的 instance_manager 广播前会 pop session_id
（relay 永远收不到，由 chat 代理从响应同步）；广播 payload 不含 raw_json；
status_change 用 "new_status" 键；monitor 事件用 "event" 而非 "event_type" 键；
worker 的 MonitorSession.id 与本地自增会碰撞（用 remote_id 列翻译）。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone

import httpx
import websockets
from fastapi import HTTPException
from sqlalchemy import exists, func, or_, select, update

from backend.config import settings
from backend.models.log_entry import LogEntry
from backend.models.monitor_session import MonitorCheck, MonitorSession
from backend.models.sub_agent import SubAgentReport, SubAgentSession
from backend.models.task import Task
from backend.models.worker import Worker
from backend.models.worker_turn_handoff import WorkerTurnHandoffReceipt
from backend.services.chat_event_identity import persisted_chat_event
from backend.services.cancellation import await_task_completion
from backend.services.legacy_plan_execution import (
    LegacyPlanExecutionCarrierProof,
    legacy_approved_execution_carrier_proof,
    legacy_plan_execution_snapshot_matches_proof,
)
from backend.services.pr_review_runtime import (
    PR_REVIEW_TERMINAL_CHAT_HEADER,
    PR_REVIEW_TERMINAL_CHAT_HEADER_VALUE,
    is_pr_review_fix_task,
    is_pr_review_task,
)
from backend.services.task_queue import PR_REVIEW_SUPERSEDED_METADATA_KEY
from backend.services.task_creation import (
    delegated_task_execution_principal_values,
    task_execution_principal_values,
)
from backend.services.test_harness_owner_fence import (
    no_active_test_harness_owner_graph_predicate,
)
from backend.services.worker_plan_decision import (
    worker_plan_decision_gate_receipt,
    worker_plan_decision_is_prepared,
)
from backend.services.worker_launch_admission import (
    WORKER_CONTEXT_PREFLIGHT_PROOF_KEY,
    WORKER_CONTEXT_RETRY_MARKER_METADATA_KEY,
    WORKER_CONTEXT_RETRY_MARKER_VERSION,
    WORKER_EXACT_LAUNCH_MARKER_METADATA_KEY,
    WORKER_EXACT_LAUNCH_MARKER_VERSION,
    WORKER_LAUNCH_ADMISSION_EVENT,
    WorkerLaunchAdmissionRequest,
    build_worker_launch_admission_response,
    parse_codex_context_preflight_relay_proof,
    parse_worker_launch_admission_request,
)

_TASK_STATUSES = frozenset(
    {
        "pending",
        "in_progress",
        "executing",
        "plan_review",
        "merging",
        "migrating",
        "completed",
        "failed",
        "cancelled",
        "conflict",
    }
)
_TERMINAL_TASK_STATUSES = frozenset(
    {"completed", "failed", "cancelled", "conflict"}
)
_WORKER_BACKGROUND_MIRROR_SENTINEL = "worker-relay:background-active:v1"
_WORKER_CHILD_MIRROR_META_KEY = "ccm_worker_mirror"
_NATIVE_SUB_AGENT_MIRROR_VERSION = 1
_SUB_AGENT_TERMINAL_STATUSES = frozenset({"completed", "failed", "stopped"})
_NATIVE_SUB_AGENT_TERMINAL_STATUSES = frozenset(
    {"completed", "failed", "cancelled"}
)
_FP_PREFIX = 20_000  # Match the history endpoint's exact observable prefix.
WORKER_HANDOFF_RECOVERY_BASE_DELAY = 1.0
WORKER_HANDOFF_RECOVERY_MAX_DELAY = 60.0
LEGACY_CARRIER_RECOVERY_BASE_DELAY = 1.0
LEGACY_CARRIER_RECOVERY_MAX_DELAY = 60.0

_LEGACY_CARRIER_EXECUTION_STATUSES = frozenset(
    {
        "pending",
        "in_progress",
        "executing",
        "merging",
        "completed",
        "failed",
        "cancelled",
        "conflict",
    }
)

# Worker receipt states are deliberately split by replay safety, not merely by
# whether G+1 has been assigned.  ``claimed`` still precedes every provider
# side effect and may replay the exact G+1 envelope.  ``launching`` is written
# immediately before the first possible provider side effect, so it is exact
# generation evidence but must never be replayed automatically.
_WORKER_HANDOFF_REPLAYABLE_STATUSES = frozenset({"accepted", "claimed"})
_WORKER_HANDOFF_POST_BOUNDARY_STATUSES = frozenset({"launching", "launched"})
_WORKER_HANDOFF_BOUND_GENERATION_STATUSES = frozenset(
    {"claimed", "launching", "launched"}
)
_TURN_SCOPES = frozenset({"source", "foreground", "autonomous", "orphan"})
_ACTUAL_TRANSPORTS = frozenset(
    {"claude_pty", "claude_exec", "codex_app_server", "codex_exec"}
)
WORKER_TERMINATION_UNCERTAINTY_METADATA_KEY = (
    "worker_termination_uncertainty_v1"
)
WORKER_MANUAL_RETRY_PROTOCOL = 1
WORKER_MANUAL_RETRY_RECEIPT_METADATA_KEY = (
    "ccm_worker_manual_retry_receipt_v1"
)
WORKER_REMOTE_MATERIALIZED_METADATA_KEY = (
    "ccm_worker_remote_materialized_v1"
)
LEGACY_PLAN_CARRIER_CONFLICT_METADATA_KEY = (
    "legacy_plan_carrier_conflict_v1"
)
_WORKER_TERMINATION_OPERATIONS = frozenset({"cancel", "stop-session"})


def _handoff_payload_digest(payload: dict) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


_WORKER_TURN_HANDOFF_RECEIPT_FIELDS = frozenset({
    "worker_turn_handoff_id",
    "worker_turn_handoff_retry_count",
    "worker_turn_handoff_from_generation",
})
_MANAGER_HANDOFF_REQUEST_ENVELOPE_VERSION = 2


def worker_turn_handoff_request_identity(
    replay_payload: dict,
    admitted_routing: object,
) -> dict:
    """Build the provider-neutral identity shared by both handoff receipts.

    The random receipt id and its separately fenced retry/generation baseline
    are deliberately excluded from the request digest.  The admitted provider
    route is included because the HTTP ``expected_routing`` object is only the
    caller's expectation, not the Worker's authoritative admission result.
    """

    if not isinstance(replay_payload, dict):
        raise TypeError("Worker handoff replay payload must be an object")
    if (
        not isinstance(admitted_routing, (list, tuple))
        or len(admitted_routing) != 3
        or not isinstance(admitted_routing[0], str)
        or (
            admitted_routing[1] is not None
            and not isinstance(admitted_routing[1], str)
        )
        or not isinstance(admitted_routing[2], str)
    ):
        raise ValueError("Worker handoff admitted routing is invalid")
    identity = {
        key: value
        for key, value in replay_payload.items()
        if key not in _WORKER_TURN_HANDOFF_RECEIPT_FIELDS
    }
    identity["admitted_routing"] = list(admitted_routing)
    return identity


def task_execution_principal_payload(task_or_payload) -> dict[str, object]:
    """Return the canonical four-field execution authority snapshot."""

    if isinstance(task_or_payload, dict):
        get = task_or_payload.get
    else:
        get = lambda name: getattr(task_or_payload, name, None)
    return {
        "execution_user_id": get("execution_user_id"),
        "execution_user_role": get("execution_user_role"),
        "execution_mode": get("execution_mode"),
        "execution_principal_kind": get("execution_principal_kind"),
    }


def canonical_delegated_principal_payload(task_or_payload) -> dict[str, object] | None:
    """Canonicalize a Manager/native or Worker/delegated principal for wire use."""

    principal = task_execution_principal_payload(task_or_payload)
    try:
        canonical = task_execution_principal_values(
            user_id=principal["execution_user_id"],
            role=principal["execution_user_role"],
            principal_kind=principal["execution_principal_kind"],
        )
        if canonical != principal:
            return None
        return delegated_task_execution_principal_values(
            user_id=canonical["execution_user_id"],
            role=canonical["execution_user_role"],
            principal_kind=canonical["execution_principal_kind"],
        )
    except (TypeError, ValueError):
        return None


def canonical_manager_principal_from_delegated(
    task_or_payload,
) -> dict[str, object] | None:
    """Map a proven Worker wire principal back to Manager-native authority."""

    delegated = canonical_delegated_principal_payload(task_or_payload)
    if delegated is None:
        return None
    kind = delegated["execution_principal_kind"]
    if kind == "delegated_user":
        native_kind = "user"
    elif kind == "delegated_deployment_token":
        native_kind = "deployment_token"
    elif kind == "system":
        native_kind = "system"
    else:
        return None
    try:
        return task_execution_principal_values(
            user_id=delegated["execution_user_id"],
            role=delegated["execution_user_role"],
            principal_kind=native_kind,
        )
    except (TypeError, ValueError):
        return None


def worker_principal_digest(task_or_payload) -> str | None:
    principal = canonical_delegated_principal_payload(task_or_payload)
    if principal is None:
        return None
    return _handoff_payload_digest(principal)


def worker_manual_retry_request_identity(payload: dict) -> dict:
    """Return the immutable portion covered by a manual-retry request digest."""

    if not isinstance(payload, dict):
        raise TypeError("Worker manual retry payload must be an object")
    identity = dict(payload)
    identity.pop("request_digest", None)
    return identity


def worker_manual_retry_request_digest(payload: dict) -> str:
    return _handoff_payload_digest(worker_manual_retry_request_identity(payload))


def worker_manual_retry_receipt(metadata: object) -> dict | None:
    if not isinstance(metadata, dict):
        return None
    receipt = metadata.get(WORKER_MANUAL_RETRY_RECEIPT_METADATA_KEY)
    return receipt if isinstance(receipt, dict) else None


def worker_manual_retry_is_prepared(metadata: object) -> bool:
    receipt = worker_manual_retry_receipt(metadata)
    return bool(
        receipt is not None
        and receipt.get("version") == WORKER_MANUAL_RETRY_PROTOCOL
        and receipt.get("side") == "manager"
        and receipt.get("state") == "prepared"
    )


def worker_remote_task_is_materialized(metadata: object) -> bool:
    return bool(
        isinstance(metadata, dict)
        and metadata.get(WORKER_REMOTE_MATERIALIZED_METADATA_KEY) is True
    )


@dataclass(frozen=True)
class WorkerTaskGeneration:
    """Exact Manager-side mirror generation owned by one Worker.

    ``worker_id`` is part of the generation, not merely routing metadata.  A
    delayed response/event from Worker A must not be able to update the same
    task id after it has moved local, moved to Worker B, or been retried on A.
    """

    task_id: int
    worker_id: int
    incarnation_id: str | None
    execution_user_id: int | None
    execution_user_role: str
    execution_mode: str
    execution_principal_kind: str
    status: str
    retry_count: int
    turn_generation: int
    instance_id: int | None
    started_at: datetime | None
    completed_at: datetime | None
    pty_background_generation: str | None
    worker_turn_handoff_id: str | None
    worker_turn_handoff_worker_id: int | None
    worker_turn_handoff_retry_count: int | None
    worker_turn_handoff_from_generation: int | None
    worker_turn_handoff_source_log_id: int | None
    worker_turn_handoff_acknowledged: bool | None
    termination_uncertainty_present: bool = False
    termination_uncertainty: object | None = None
    legacy_carrier_conflict_present: bool = False
    legacy_carrier_conflict: object | None = None
    # Provider is needed when canonicalizing relayed chat payloads.  It is
    # intentionally not part of the generation marker: provider routing is
    # already fenced by the Task row and is not an event identity field.
    provider: str = field(default="claude", compare=False)


def has_worker_termination_uncertainty(metadata: object) -> bool:
    """Return whether a durable remote-termination quarantine is present.

    Presence, not validity, is the safety boundary.  A malformed marker must
    fail closed until an operator repairs it; treating malformed JSON as
    absent would re-enable retry/migration of a possibly terminated Worker
    generation.
    """

    return (
        isinstance(metadata, dict)
        and WORKER_TERMINATION_UNCERTAINTY_METADATA_KEY in metadata
    )


def has_worker_execution_quarantine(metadata: object) -> bool:
    """Return whether automatic Worker execution/migration must stay closed."""

    return bool(
        has_worker_termination_uncertainty(metadata)
        or worker_manual_retry_is_prepared(metadata)
        or worker_plan_decision_is_prepared(metadata)
        or (
            isinstance(metadata, dict)
            and LEGACY_PLAN_CARRIER_CONFLICT_METADATA_KEY in metadata
        )
    )


def _generation_marker_payload(generation: WorkerTaskGeneration) -> dict:
    def dt(value: datetime | None) -> str | None:
        return value.isoformat(timespec="microseconds") if value else None

    return {
        "task_id": generation.task_id,
        "worker_id": generation.worker_id,
        "incarnation_id": generation.incarnation_id,
        "execution_principal": {
            "execution_user_id": generation.execution_user_id,
            "execution_user_role": generation.execution_user_role,
            "execution_mode": generation.execution_mode,
            "execution_principal_kind": generation.execution_principal_kind,
        },
        "status": generation.status,
        "retry_count": generation.retry_count,
        "turn_generation": generation.turn_generation,
        "instance_id": generation.instance_id,
        "started_at": dt(generation.started_at),
        "completed_at": dt(generation.completed_at),
        "pty_background_generation": generation.pty_background_generation,
        "worker_turn_handoff_id": generation.worker_turn_handoff_id,
        "worker_turn_handoff_worker_id": (
            generation.worker_turn_handoff_worker_id
        ),
        "worker_turn_handoff_retry_count": (
            generation.worker_turn_handoff_retry_count
        ),
        "worker_turn_handoff_from_generation": (
            generation.worker_turn_handoff_from_generation
        ),
        "worker_turn_handoff_source_log_id": (
            generation.worker_turn_handoff_source_log_id
        ),
        "worker_turn_handoff_acknowledged": (
            generation.worker_turn_handoff_acknowledged
        ),
    }


def _valid_termination_uncertainty(
    generation: WorkerTaskGeneration,
) -> bool:
    """Validate that a quarantine marker names this exact pre-conflict row."""

    if (
        generation.status != "conflict"
        or not generation.termination_uncertainty_present
        or not isinstance(generation.termination_uncertainty, dict)
    ):
        return False
    marker = generation.termination_uncertainty
    source = marker.get("source_generation")
    if (
        marker.get("version") != 1
        or marker.get("operation") not in _WORKER_TERMINATION_OPERATIONS
        or not isinstance(marker.get("operation_id"), str)
        or len(marker["operation_id"]) != 32
        or not isinstance(marker.get("created_at"), str)
        or not isinstance(source, dict)
        or source.get("status") not in _TASK_STATUSES
        or (
            source.get("completed_at") is not None
            and _remote_datetime(source.get("completed_at")) is None
        )
    ):
        return False
    current_as_source = _generation_marker_payload(generation)
    current_as_source["status"] = source["status"]
    current_as_source["completed_at"] = source.get("completed_at")
    return current_as_source == source


def worker_task_generation(
    task: Task,
    *,
    expected_worker_id: int | None = None,
) -> WorkerTaskGeneration | None:
    worker_id = task.worker_id
    if (
        type(worker_id) is not int
        or task.shared_from_id is not None
        or (
            expected_worker_id is not None
            and worker_id != expected_worker_id
        )
    ):
        return None
    metadata = task.metadata_ if isinstance(task.metadata_, dict) else {}
    return WorkerTaskGeneration(
        task_id=task.id,
        worker_id=worker_id,
        incarnation_id=task.incarnation_id,
        execution_user_id=task.execution_user_id,
        execution_user_role=task.execution_user_role,
        execution_mode=task.execution_mode,
        execution_principal_kind=task.execution_principal_kind,
        status=task.status,
        retry_count=task.retry_count,
        turn_generation=task.turn_generation,
        instance_id=task.instance_id,
        started_at=task.started_at,
        completed_at=task.completed_at,
        pty_background_generation=task.pty_background_generation,
        worker_turn_handoff_id=task.worker_turn_handoff_id,
        worker_turn_handoff_worker_id=task.worker_turn_handoff_worker_id,
        worker_turn_handoff_retry_count=task.worker_turn_handoff_retry_count,
        worker_turn_handoff_from_generation=(
            task.worker_turn_handoff_from_generation
        ),
        worker_turn_handoff_source_log_id=(
            task.worker_turn_handoff_source_log_id
        ),
        worker_turn_handoff_acknowledged=(
            task.worker_turn_handoff_acknowledged
        ),
        provider=(task.provider or "claude").lower(),
        termination_uncertainty_present=(
            WORKER_TERMINATION_UNCERTAINTY_METADATA_KEY in metadata
        ),
        termination_uncertainty=metadata.get(
            WORKER_TERMINATION_UNCERTAINTY_METADATA_KEY
        ),
        legacy_carrier_conflict_present=(
            LEGACY_PLAN_CARRIER_CONFLICT_METADATA_KEY in metadata
        ),
        legacy_carrier_conflict=metadata.get(
            LEGACY_PLAN_CARRIER_CONFLICT_METADATA_KEY
        ),
    )


def _nullable_eq(column, value):
    return column.is_(None) if value is None else column == value


def worker_task_generation_predicates(
    generation: WorkerTaskGeneration,
) -> tuple:
    return (
        Task.id == generation.task_id,
        Task.worker_id == generation.worker_id,
        Task.shared_from_id.is_(None),
        _nullable_eq(Task.incarnation_id, generation.incarnation_id),
        _nullable_eq(Task.execution_user_id, generation.execution_user_id),
        Task.execution_user_role == generation.execution_user_role,
        Task.execution_mode == generation.execution_mode,
        Task.execution_principal_kind == generation.execution_principal_kind,
        Task.status == generation.status,
        Task.retry_count == generation.retry_count,
        Task.turn_generation == generation.turn_generation,
        _nullable_eq(Task.instance_id, generation.instance_id),
        _nullable_eq(Task.started_at, generation.started_at),
        _nullable_eq(Task.completed_at, generation.completed_at),
        _nullable_eq(
            Task.pty_background_generation,
            generation.pty_background_generation,
        ),
        _nullable_eq(
            Task.worker_turn_handoff_id,
            generation.worker_turn_handoff_id,
        ),
        _nullable_eq(
            Task.worker_turn_handoff_worker_id,
            generation.worker_turn_handoff_worker_id,
        ),
        _nullable_eq(
            Task.worker_turn_handoff_retry_count,
            generation.worker_turn_handoff_retry_count,
        ),
        _nullable_eq(
            Task.worker_turn_handoff_from_generation,
            generation.worker_turn_handoff_from_generation,
        ),
        _nullable_eq(
            Task.worker_turn_handoff_source_log_id,
            generation.worker_turn_handoff_source_log_id,
        ),
        _nullable_eq(
            Task.worker_turn_handoff_acknowledged,
            generation.worker_turn_handoff_acknowledged,
        ),
    )


def _worker_task_termination_apply_predicate(
    operation_id: str | None = None,
):
    """Return the final SQL ordering gate for a Manager Task write.

    Ordinary relay writers require the active termination slot to be empty.
    The exact termination reconciler instead names the one Manager receipt
    whose Task snapshot it is allowed to apply.  Keeping this as part of the
    final Task UPDATE closes the cross-process gap after an earlier receipt
    lookup (``SELECT FOR UPDATE`` is a no-op on SQLite).
    """

    if operation_id is None:
        from backend.services.worker_task_termination import (
            no_active_worker_task_termination_predicate,
        )

        return no_active_worker_task_termination_predicate()

    from backend.models.worker_task_termination import (
        WorkerTaskTerminationReceipt,
    )

    return exists(
        select(WorkerTaskTerminationReceipt.operation_id).where(
            WorkerTaskTerminationReceipt.operation_id == operation_id,
            WorkerTaskTerminationReceipt.task_id == Task.id,
            WorkerTaskTerminationReceipt.active_task_id == Task.id,
            WorkerTaskTerminationReceipt.side == "manager",
        )
    )


def _worker_task_generation_write_predicates(
    generation: WorkerTaskGeneration,
    *,
    worker_termination_operation_id: str | None = None,
) -> tuple:
    """Bind a Task mutation to both generation and termination ownership."""

    return (
        *worker_task_generation_predicates(generation),
        _worker_task_termination_apply_predicate(
            worker_termination_operation_id
        ),
    )


async def _active_worker_task_termination_exists(db, task_id: int) -> bool:
    from backend.services.worker_task_termination import (
        active_worker_task_termination_receipt,
    )

    return bool(await active_worker_task_termination_receipt(db, task_id))


def _same_worker_turn_handoff_generation(
    current: WorkerTaskGeneration,
    observed: WorkerTaskGeneration,
) -> bool:
    """Match one reservation while allowing only its durable ACK bit to move."""

    return bool(
        _valid_worker_turn_handoff(current)
        and _has_worker_turn_handoff(current)
        and _valid_worker_turn_handoff(observed)
        and _has_worker_turn_handoff(observed)
        and current.task_id == observed.task_id
        and current.worker_id == observed.worker_id
        and current.incarnation_id == observed.incarnation_id
        and current.execution_user_id == observed.execution_user_id
        and current.execution_user_role == observed.execution_user_role
        and current.execution_mode == observed.execution_mode
        and current.execution_principal_kind
        == observed.execution_principal_kind
        and current.status == observed.status
        and current.retry_count == observed.retry_count
        and current.turn_generation == observed.turn_generation
        and current.instance_id == observed.instance_id
        and current.started_at == observed.started_at
        and current.completed_at == observed.completed_at
        and current.pty_background_generation
        == observed.pty_background_generation
        and current.worker_turn_handoff_id
        == observed.worker_turn_handoff_id
        and current.worker_turn_handoff_worker_id
        == observed.worker_turn_handoff_worker_id
        and current.worker_turn_handoff_retry_count
        == observed.worker_turn_handoff_retry_count
        and current.worker_turn_handoff_from_generation
        == observed.worker_turn_handoff_from_generation
        and current.worker_turn_handoff_source_log_id
        == observed.worker_turn_handoff_source_log_id
        and not current.termination_uncertainty_present
        and not current.legacy_carrier_conflict_present
    )


async def _acquire_worker_turn_handoff_effect_fence(
    db,
    observed: WorkerTaskGeneration,
) -> WorkerTaskGeneration | None:
    """Order one remote handoff effect before/after termination admission.

    The no-op UPDATE holds the Task write lock across the caller's HTTP effect.
    If termination admission committed first, the correlated receipt predicate
    makes the UPDATE miss and no POST is attempted.
    """

    current = await read_worker_task_generation(
        db,
        observed.task_id,
        observed.worker_id,
    )
    if current is None or not _same_worker_turn_handoff_generation(
        current,
        observed,
    ):
        return None
    fenced = await db.execute(
        update(Task)
        .where(
            *_worker_task_generation_write_predicates(current),
            no_active_test_harness_owner_graph_predicate(),
        )
        .values(status=Task.status)
    )
    if fenced.rowcount != 1:
        await db.rollback()
        return None
    return current


async def read_worker_task_generation(
    db,
    task_id: int,
    worker_id: int,
) -> WorkerTaskGeneration | None:
    """Read DB-normalized generation fields for one exact Worker assignment."""

    row = (
        await db.execute(
            select(
                Task.id,
                Task.worker_id,
                Task.incarnation_id,
                Task.execution_user_id,
                Task.execution_user_role,
                Task.execution_mode,
                Task.execution_principal_kind,
                Task.status,
                Task.retry_count,
                Task.turn_generation,
                Task.instance_id,
                Task.started_at,
                Task.completed_at,
                Task.pty_background_generation,
                Task.worker_turn_handoff_id,
                Task.worker_turn_handoff_worker_id,
                Task.worker_turn_handoff_retry_count,
                Task.worker_turn_handoff_from_generation,
                Task.worker_turn_handoff_source_log_id,
                Task.worker_turn_handoff_acknowledged,
                Task.provider,
                Task.metadata_,
            ).where(
                Task.id == task_id,
                Task.worker_id == worker_id,
                Task.shared_from_id.is_(None),
            )
        )
    ).one_or_none()
    if row is None:
        return None
    return WorkerTaskGeneration(
        task_id=row.id,
        worker_id=row.worker_id,
        incarnation_id=row.incarnation_id,
        execution_user_id=row.execution_user_id,
        execution_user_role=row.execution_user_role,
        execution_mode=row.execution_mode,
        execution_principal_kind=row.execution_principal_kind,
        status=row.status,
        retry_count=row.retry_count,
        turn_generation=row.turn_generation,
        instance_id=row.instance_id,
        started_at=row.started_at,
        completed_at=row.completed_at,
        pty_background_generation=row.pty_background_generation,
        worker_turn_handoff_id=row.worker_turn_handoff_id,
        worker_turn_handoff_worker_id=row.worker_turn_handoff_worker_id,
        worker_turn_handoff_retry_count=row.worker_turn_handoff_retry_count,
        worker_turn_handoff_from_generation=(
            row.worker_turn_handoff_from_generation
        ),
        worker_turn_handoff_source_log_id=(
            row.worker_turn_handoff_source_log_id
        ),
        worker_turn_handoff_acknowledged=(
            row.worker_turn_handoff_acknowledged
        ),
        provider=(row.provider or "claude").lower(),
        termination_uncertainty_present=(
            isinstance(row.metadata_, dict)
            and WORKER_TERMINATION_UNCERTAINTY_METADATA_KEY in row.metadata_
        ),
        termination_uncertainty=(
            row.metadata_.get(WORKER_TERMINATION_UNCERTAINTY_METADATA_KEY)
            if isinstance(row.metadata_, dict)
            else None
        ),
        legacy_carrier_conflict_present=(
            isinstance(row.metadata_, dict)
            and LEGACY_PLAN_CARRIER_CONFLICT_METADATA_KEY in row.metadata_
        ),
        legacy_carrier_conflict=(
            row.metadata_.get(LEGACY_PLAN_CARRIER_CONFLICT_METADATA_KEY)
            if isinstance(row.metadata_, dict)
            else None
        ),
    )


async def mark_worker_task_materialized(
    db,
    observed: WorkerTaskGeneration,
) -> bool:
    """Persist that initial create crossed the remote Task-row boundary."""

    fenced = await db.execute(
        update(Task)
        .where(*_worker_task_generation_write_predicates(observed))
        .values(status=Task.status)
    )
    if fenced.rowcount != 1:
        await db.rollback()
        current = await db.get(Task, observed.task_id, populate_existing=True)
        return bool(
            current is not None
            and current.worker_id == observed.worker_id
            and current.incarnation_id == observed.incarnation_id
            and worker_remote_task_is_materialized(current.metadata_)
        )
    current = (
        await db.execute(
            select(Task)
            .where(*_worker_task_generation_write_predicates(observed))
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if current is None:
        await db.rollback()
        return False
    metadata = dict(current.metadata_ or {})
    metadata[WORKER_REMOTE_MATERIALIZED_METADATA_KEY] = True
    changed = await db.execute(
        update(Task)
        .where(*_worker_task_generation_write_predicates(observed))
        .values(metadata_=metadata)
    )
    if changed.rowcount != 1:
        await db.rollback()
        return False
    await db.commit()
    return True


async def quarantine_uncertain_worker_termination(
    db,
    observed: WorkerTaskGeneration,
    *,
    operation: str,
    error: str,
) -> WorkerTaskGeneration | None:
    """Quarantine an exact Worker generation after an unproved mutation.

    The marker retains the complete pre-conflict generation.  A later relay
    readback may clear it only through ``apply_authoritative_worker_task`` and
    only when the same Worker retry/turn is authoritatively terminal.
    """

    if operation not in _WORKER_TERMINATION_OPERATIONS:
        raise ValueError(f"Unsupported Worker termination operation: {operation}")
    fenced = await db.execute(
        update(Task)
        .where(*_worker_task_generation_write_predicates(observed))
        .values(status=Task.status)
    )
    if fenced.rowcount != 1:
        await db.rollback()
        return None
    locked = (
        await db.execute(
            select(Task)
            .where(*_worker_task_generation_write_predicates(observed))
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if locked is None:
        await db.rollback()
        return None
    metadata = dict(locked.metadata_ or {})
    if WORKER_TERMINATION_UNCERTAINTY_METADATA_KEY in metadata:
        await db.rollback()
        return None
    metadata[WORKER_TERMINATION_UNCERTAINTY_METADATA_KEY] = {
        "version": 1,
        "operation": operation,
        "operation_id": uuid.uuid4().hex,
        "created_at": datetime.utcnow().isoformat(timespec="microseconds"),
        "source_generation": _generation_marker_payload(observed),
    }
    locked.metadata_ = metadata
    locked.status = "conflict"
    locked.completed_at = datetime.utcnow()
    locked.error_message = error
    await db.flush()
    resulting = await read_worker_task_generation(
        db,
        observed.task_id,
        observed.worker_id,
    )
    if (
        resulting is None
        or not _valid_termination_uncertainty(resulting)
    ):
        await db.rollback()
        return None
    await db.commit()
    return resulting


_WORKER_TURN_HANDOFF_CLEAR_VALUES = {
    "worker_turn_handoff_id": None,
    "worker_turn_handoff_worker_id": None,
    "worker_turn_handoff_retry_count": None,
    "worker_turn_handoff_from_generation": None,
    "worker_turn_handoff_source_log_id": None,
    "worker_turn_handoff_acknowledged": None,
}


async def _settle_manager_handoff_receipt(
    db,
    observed: WorkerTaskGeneration,
    *,
    status: str,
    reason: str | None = None,
) -> bool:
    """Advance the Manager receipt in the caller's Task-marker transaction."""

    if status not in {"completed", "cancelled"}:
        raise ValueError("invalid Manager handoff settlement status")
    if not _valid_worker_turn_handoff(observed) or not _has_worker_turn_handoff(
        observed
    ):
        return False
    changed = await db.execute(
        update(WorkerTurnHandoffReceipt)
        .where(
            WorkerTurnHandoffReceipt.handoff_id
            == observed.worker_turn_handoff_id,
            WorkerTurnHandoffReceipt.task_id == observed.task_id,
            WorkerTurnHandoffReceipt.source_log_id
            == observed.worker_turn_handoff_source_log_id,
            WorkerTurnHandoffReceipt.side == "manager",
            WorkerTurnHandoffReceipt.worker_id == observed.worker_id,
            WorkerTurnHandoffReceipt.retry_count
            == observed.worker_turn_handoff_retry_count,
            WorkerTurnHandoffReceipt.from_generation
            == observed.worker_turn_handoff_from_generation,
            WorkerTurnHandoffReceipt.status.in_(
                ("prepared", "acknowledged")
            ),
        )
        .values(
            status=status,
            cancel_reason=(reason[:2000] if reason else None),
            updated_at=datetime.utcnow(),
        )
    )
    return changed.rowcount == 1


def _has_worker_turn_handoff(generation: WorkerTaskGeneration) -> bool:
    return generation.worker_turn_handoff_id is not None


def _valid_worker_turn_handoff(generation: WorkerTaskGeneration) -> bool:
    """Validate the complete durable reservation shape and baseline."""

    if not _has_worker_turn_handoff(generation):
        return all(
            value is None
            for value in (
                generation.worker_turn_handoff_worker_id,
                generation.worker_turn_handoff_retry_count,
                generation.worker_turn_handoff_from_generation,
                generation.worker_turn_handoff_source_log_id,
                generation.worker_turn_handoff_acknowledged,
            )
        )
    return (
        isinstance(generation.worker_turn_handoff_id, str)
        and bool(generation.worker_turn_handoff_id)
        and len(generation.worker_turn_handoff_id) <= 32
        and type(generation.worker_turn_handoff_worker_id) is int
        and generation.worker_turn_handoff_worker_id == generation.worker_id
        and type(generation.worker_turn_handoff_retry_count) is int
        and generation.worker_turn_handoff_retry_count >= 0
        and type(generation.worker_turn_handoff_from_generation) is int
        and generation.worker_turn_handoff_from_generation >= 0
        and type(generation.worker_turn_handoff_source_log_id) is int
        and generation.worker_turn_handoff_source_log_id > 0
        and type(generation.worker_turn_handoff_acknowledged) is bool
    )


def _handoff_authorizes_next_turn(
    generation: WorkerTaskGeneration,
    *,
    retry_count: int,
    turn_generation: int,
) -> bool:
    return (
        _valid_worker_turn_handoff(generation)
        and _has_worker_turn_handoff(generation)
        and generation.retry_count
        == generation.worker_turn_handoff_retry_count
        and generation.turn_generation
        == generation.worker_turn_handoff_from_generation
        and retry_count == generation.worker_turn_handoff_retry_count
        and turn_generation
        == generation.worker_turn_handoff_from_generation + 1
    )


async def reserve_worker_turn_handoff(
    db,
    observed: WorkerTaskGeneration,
    *,
    handoff_id: str,
    source_log_id: int,
    request_payload: dict,
    request_digest: str,
    replay_payload: dict | None = None,
    terminal_pr_review_chat: bool = False,
) -> WorkerTaskGeneration | None:
    """Reserve exactly one Worker G -> G+1 follow-up before network I/O."""

    if await _active_worker_task_termination_exists(db, observed.task_id):
        return None

    if (
        not _valid_worker_turn_handoff(observed)
        or _has_worker_turn_handoff(observed)
        or observed.termination_uncertainty_present
        or observed.legacy_carrier_conflict_present
        or not handoff_id
        or len(handoff_id) > 32
        or type(source_log_id) is not int
        or source_log_id <= 0
        or not isinstance(request_payload, dict)
        or not isinstance(request_digest, str)
        or len(request_digest) != 64
        or (replay_payload is not None and not isinstance(replay_payload, dict))
        or type(terminal_pr_review_chat) is not bool
    ):
        return None
    try:
        if _handoff_payload_digest(request_payload) != request_digest:
            return None
        stored_request_payload = request_payload
        if replay_payload is not None:
            admitted_routing = request_payload.get("admitted_routing")
            if (
                worker_turn_handoff_request_identity(
                    replay_payload,
                    admitted_routing,
                )
                != request_payload
            ):
                return None
            if (
                replay_payload.get("worker_turn_handoff_id") != handoff_id
                or replay_payload.get("worker_turn_handoff_retry_count")
                != observed.retry_count
                or replay_payload.get("worker_turn_handoff_from_generation")
                != observed.turn_generation
                or replay_payload.get("worker_turn_handoff_incarnation_id")
                != observed.incarnation_id
            ):
                return None
            replay_principal = canonical_delegated_principal_payload(
                replay_payload
            )
            identity_principal = canonical_delegated_principal_payload(
                request_payload
            )
            if (
                replay_principal is None
                or identity_principal is None
                or replay_principal != identity_principal
            ):
                return None
            stored_request_payload = {
                "version": _MANAGER_HANDOFF_REQUEST_ENVELOPE_VERSION,
                "identity": request_payload,
                "replay_payload": replay_payload,
            }
    except (TypeError, ValueError, UnicodeError):
        return None
    changed = await db.execute(
        update(Task)
        .where(
            *_worker_task_generation_write_predicates(observed),
            no_active_test_harness_owner_graph_predicate(),
        )
        .values(
            worker_turn_handoff_id=handoff_id,
            worker_turn_handoff_worker_id=observed.worker_id,
            worker_turn_handoff_retry_count=observed.retry_count,
            worker_turn_handoff_from_generation=observed.turn_generation,
            worker_turn_handoff_source_log_id=source_log_id,
            worker_turn_handoff_acknowledged=False,
        )
    )
    if changed.rowcount != 1:
        await db.rollback()
        return None
    db.add(
        WorkerTurnHandoffReceipt(
            handoff_id=handoff_id,
            task_id=observed.task_id,
            source_log_id=source_log_id,
            side="manager",
            worker_id=observed.worker_id,
            retry_count=observed.retry_count,
            from_generation=observed.turn_generation,
            status="prepared",
            request_payload=stored_request_payload,
            request_digest=request_digest,
            terminal_pr_review_chat=terminal_pr_review_chat,
        )
    )
    try:
        await db.flush()
    except Exception:
        await db.rollback()
        return None
    resulting = await read_worker_task_generation(
        db,
        observed.task_id,
        observed.worker_id,
    )
    if resulting is None or not _valid_worker_turn_handoff(resulting):
        await db.rollback()
        return None
    return resulting


async def acknowledge_worker_turn_handoff(
    db,
    reserved: WorkerTaskGeneration,
    *,
    session_id: str | None = None,
) -> WorkerTaskGeneration | None:
    """Record the proxy ACK without guessing whether Worker claimed G+1 yet.

    If relay evidence already advanced the exact reservation, this ACK clears
    it. Otherwise the acknowledged marker remains until the Worker emits G+1.
    """

    if not _valid_worker_turn_handoff(reserved) or not _has_worker_turn_handoff(
        reserved
    ):
        return None
    from_generation = reserved.worker_turn_handoff_from_generation
    admission = await db.execute(
        update(Task)
        .where(
            Task.id == reserved.task_id,
            Task.worker_id == reserved.worker_id,
            Task.shared_from_id.is_(None),
            Task.retry_count == reserved.retry_count,
            Task.turn_generation.in_((from_generation, from_generation + 1)),
            Task.worker_turn_handoff_id == reserved.worker_turn_handoff_id,
            Task.worker_turn_handoff_worker_id
            == reserved.worker_turn_handoff_worker_id,
            Task.worker_turn_handoff_retry_count
            == reserved.worker_turn_handoff_retry_count,
            Task.worker_turn_handoff_from_generation
            == reserved.worker_turn_handoff_from_generation,
            Task.worker_turn_handoff_source_log_id
            == reserved.worker_turn_handoff_source_log_id,
            _worker_task_termination_apply_predicate(),
        )
        .values(status=Task.status)
    )
    if admission.rowcount != 1:
        await db.rollback()
        return None
    task = (
        await db.execute(
            select(Task)
            .where(
                Task.id == reserved.task_id,
                Task.worker_id == reserved.worker_id,
                Task.shared_from_id.is_(None),
                Task.retry_count == reserved.retry_count,
                Task.worker_turn_handoff_id
                == reserved.worker_turn_handoff_id,
                Task.worker_turn_handoff_worker_id
                == reserved.worker_turn_handoff_worker_id,
                Task.worker_turn_handoff_retry_count
                == reserved.worker_turn_handoff_retry_count,
                Task.worker_turn_handoff_from_generation
                == reserved.worker_turn_handoff_from_generation,
                Task.worker_turn_handoff_source_log_id
                == reserved.worker_turn_handoff_source_log_id,
                Task.turn_generation.in_(
                    (from_generation, from_generation + 1)
                ),
                _worker_task_termination_apply_predicate(),
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if task is None:
        return None
    current = worker_task_generation(task, expected_worker_id=reserved.worker_id)
    if (
        current is None
        or not _valid_worker_turn_handoff(current)
        or current.termination_uncertainty_present
        or current.legacy_carrier_conflict_present
    ):
        return None
    if task.turn_generation not in {from_generation, from_generation + 1}:
        return None
    receipt = (
        await db.execute(
            select(WorkerTurnHandoffReceipt)
            .where(
                WorkerTurnHandoffReceipt.handoff_id
                == reserved.worker_turn_handoff_id,
                WorkerTurnHandoffReceipt.task_id == reserved.task_id,
                WorkerTurnHandoffReceipt.source_log_id
                == reserved.worker_turn_handoff_source_log_id,
                WorkerTurnHandoffReceipt.side == "manager",
                WorkerTurnHandoffReceipt.worker_id == reserved.worker_id,
                WorkerTurnHandoffReceipt.retry_count
                == reserved.worker_turn_handoff_retry_count,
                WorkerTurnHandoffReceipt.from_generation
                == reserved.worker_turn_handoff_from_generation,
                WorkerTurnHandoffReceipt.status.in_(
                    ("prepared", "acknowledged")
                ),
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if receipt is None:
        return None
    # HTTP acceptance is useful recovery state, but is not exact evidence that
    # this queue receipt owns G+1.  Keep the marker until a launched Worker
    # receipt and a Manager-durable event/history/snapshot are committed
    # together.
    task.worker_turn_handoff_acknowledged = True
    receipt.status = "acknowledged"
    receipt.updated_at = datetime.utcnow()
    if session_id:
        task.session_id = session_id
    await db.flush()
    return worker_task_generation(task, expected_worker_id=reserved.worker_id)


def _remote_datetime(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def authoritative_worker_task_values(
    remote_task: dict,
    *,
    task_id: int,
    incarnation_id: str | None,
) -> dict | None:
    """Validate a Worker task snapshot and return mirror-safe fields.

    ``retry_count`` is mandatory.  Status events do not currently carry a
    remote generation, so callers must use the authoritative Worker GET
    response.  Accepting a status-only payload would let a delayed event from a
    prior retry overwrite a newer retry on the same Worker.
    """

    if (
        not isinstance(remote_task, dict)
        or type(remote_task.get("id")) is not int
        or remote_task["id"] != task_id
        or not incarnation_id
        or remote_task.get("incarnation_id") != incarnation_id
        or remote_task.get("status") not in _TASK_STATUSES
        or type(remote_task.get("retry_count")) is not int
        or remote_task["retry_count"] < 0
        or type(remote_task.get("turn_generation")) is not int
        or remote_task["turn_generation"] < 0
    ):
        return None

    from backend.services.task_creation import (
        TASK_EXECUTION_WORKER_PRINCIPAL_KINDS,
        task_execution_principal_values,
    )

    principal_fields = {
        field: remote_task.get(field)
        for field in (
            "execution_user_id",
            "execution_user_role",
            "execution_mode",
            "execution_principal_kind",
        )
    }
    if (
        principal_fields["execution_principal_kind"]
        not in TASK_EXECUTION_WORKER_PRINCIPAL_KINDS
    ):
        return None
    try:
        canonical_principal = task_execution_principal_values(
            user_id=principal_fields["execution_user_id"],
            role=principal_fields["execution_user_role"],
            principal_kind=principal_fields["execution_principal_kind"],
        )
    except ValueError:
        return None
    if canonical_principal != principal_fields:
        return None

    status = remote_task["status"]
    values: dict = {
        "status": status,
        "retry_count": remote_task["retry_count"],
        "turn_generation": remote_task["turn_generation"],
    }
    remote_background_active = remote_task.get(
        "background_active",
        _WORKER_BACKGROUND_MIRROR_SENTINEL,
    )
    if type(remote_background_active) is bool:
        # The Worker generation token is deliberately not part of TaskResponse.
        # Mirror only its strict public boolean into a Manager-owned sentinel;
        # never accept a remote token or a truthy/falsey lookalike.
        values["pty_background_generation"] = (
            _WORKER_BACKGROUND_MIRROR_SENTINEL
            if remote_background_active
            else None
        )
    for field in (
        "plan_approved",
        "error_message",
        "loop_progress",
        "session_id",
        "plan_content",
        "plan_applied_to_session_id",
        "goal_turns_used",
        "goal_last_reason",
    ):
        if field in remote_task:
            values[field] = remote_task[field]

    for field in (
        "plan_approved_at",
        "plan_applied_at",
    ):
        if field in remote_task:
            parsed = _remote_datetime(remote_task[field])
            if remote_task[field] is None or parsed is not None:
                values[field] = parsed

    if "started_at" in remote_task:
        started_at = _remote_datetime(remote_task["started_at"])
        if remote_task["started_at"] is None or started_at is not None:
            values["started_at"] = started_at

    if status in _TERMINAL_TASK_STATUSES:
        completed_at = _remote_datetime(remote_task.get("completed_at"))
        values["completed_at"] = (
            completed_at
            if completed_at is not None
            else datetime.utcnow()
        )
        if (
            status in ("failed", "conflict")
            and not remote_task.get("error_message")
        ):
            values["error_message"] = (
                "Worker task failed without an error message"
                if status == "failed"
                else "Worker task ended with an unresolved conflict"
            )
        elif status not in ("failed", "conflict"):
            values["error_message"] = remote_task.get("error_message")
    elif "completed_at" in remote_task:
        completed_at = _remote_datetime(remote_task["completed_at"])
        if remote_task["completed_at"] is None or completed_at is not None:
            values["completed_at"] = completed_at

    return values


async def apply_authoritative_worker_task(
    db,
    observed: WorkerTaskGeneration,
    remote_task: dict,
    *,
    metadata_updates: dict | None = None,
    worker_turn_handoff_id: str | None = None,
    worker_termination_operation_id: str | None = None,
    worker_plan_decision_operation_id: str | None = None,
    commit: bool = True,
) -> WorkerTaskGeneration | None:
    """CAS an authoritative Worker snapshot onto its exact observed mirror."""

    # A semantic legacy-carrier split is permanent quarantine evidence.  A
    # later generic Worker operation may re-subscribe the task, but ordinary
    # snapshots must never turn that Manager conflict back into remote state.
    if observed.legacy_carrier_conflict_present:
        return None
    from backend.services.worker_task_termination import (
        manager_receipt_allows_authoritative_apply,
    )

    if not await manager_receipt_allows_authoritative_apply(
        db,
        observed.task_id,
        worker_termination_operation_id,
    ):
        # Relay/status GETs are read-only while an exact termination receipt
        # owns the Manager mirror.  Only that operation may apply its terminal
        # Worker result and settle any linked G->G+1 handoff atomically.
        return None
    values = authoritative_worker_task_values(
        remote_task,
        task_id=observed.task_id,
        incarnation_id=observed.incarnation_id,
    )
    if values is None or not _valid_worker_turn_handoff(observed):
        return None
    principal_values = task_execution_principal_payload(remote_task)
    # Worker rows must always expose an internally delegated (or system)
    # principal.  Accepting a native ``user``/``deployment_token`` kind here
    # would let an old/misconfigured Worker blur the control-plane boundary.
    if canonical_delegated_principal_payload(remote_task) != principal_values:
        return None
    manager_task = await db.get(Task, observed.task_id)
    if manager_task is not None and worker_manual_retry_is_prepared(
        manager_task.metadata_
    ):
        # Only apply_authoritative_worker_retry may cross N -> N+1 while the
        # exact Manager outbox owns the mutation.  A same-principal snapshot is
        # not a substitute for the Worker's durable operation receipt.
        return None
    prepared_plan_decision = (
        worker_plan_decision_gate_receipt(manager_task.metadata_)
        if manager_task is not None
        else None
    )
    if worker_plan_decision_is_prepared(
        manager_task.metadata_ if manager_task is not None else None
    ) and not (
        isinstance(prepared_plan_decision, dict)
        and worker_plan_decision_operation_id is not None
        and prepared_plan_decision.get("side") == "manager"
        and prepared_plan_decision.get("state") == "prepared"
        and prepared_plan_decision.get("operation_id")
        == worker_plan_decision_operation_id
    ):
        # Relay snapshots cannot prove which non-repeatable terminal decision
        # committed.  Only the exact decision receipt readback may advance the
        # Manager mirror while its outbox is prepared.
        return None
    manager_principal = (
        task_execution_principal_payload(manager_task)
        if manager_task is not None
        else None
    )
    manager_wire_principal = (
        canonical_delegated_principal_payload(manager_task)
        if manager_task is not None
        else None
    )
    # The Manager mirror is the control-plane authority and must retain its
    # native ``user``/``deployment_token``/``system`` kind.  A delegated kind
    # is valid only on the Worker wire/row; accepting one in the Manager DB
    # would make a later local retry or migration indistinguishable from a
    # trusted local principal.
    if (
        manager_principal is None
        or manager_wire_principal is None
        or canonical_manager_principal_from_delegated(manager_task)
        != manager_principal
    ):
        return None
    remote_retry_count = values["retry_count"]
    remote_turn_generation = values["turn_generation"]
    clearing_termination_uncertainty = (
        observed.termination_uncertainty_present
    )
    if clearing_termination_uncertainty:
        # Version-1 metadata quarantine predates durable remote receipts.  It
        # has no request digest or remote operation id, so even an apparently
        # terminal snapshot cannot prove which mutation produced it.  Keep it
        # fail-closed for explicit operator reconciliation; never auto-clear it
        # through the v2 receipt path.
        return None
    else:
        adopting_handoff = _handoff_authorizes_next_turn(
            observed,
            retry_count=remote_retry_count,
            turn_generation=remote_turn_generation,
        )
        same_turn = remote_turn_generation == observed.turn_generation
        termination_settles_same_turn_handoff = bool(
            worker_termination_operation_id is not None
            and commit is False
            and _has_worker_turn_handoff(observed)
            and worker_turn_handoff_id == observed.worker_turn_handoff_id
            and remote_retry_count == observed.retry_count
            and same_turn
            and observed.turn_generation
            == observed.worker_turn_handoff_from_generation
            and values["status"] in _TERMINAL_TASK_STATUSES
        )
        if adopting_handoff:
            if worker_turn_handoff_id != observed.worker_turn_handoff_id:
                return None
            receipt = await db.get(
                WorkerTurnHandoffReceipt,
                observed.worker_turn_handoff_id,
            )
            if (
                receipt is None
                or receipt.side != "manager"
                or receipt.task_id != observed.task_id
                or receipt.worker_id != observed.worker_id
                or receipt.retry_count != observed.retry_count
                or receipt.from_generation != observed.turn_generation
                or not isinstance(receipt.request_payload, dict)
                or _handoff_payload_digest(receipt.request_payload)
                != receipt.request_digest
            ):
                return None
            handoff_principal = canonical_delegated_principal_payload(
                receipt.request_payload
            )
            if handoff_principal is None:
                return None
            handoff_manager_principal = (
                canonical_manager_principal_from_delegated(
                    receipt.request_payload
                )
            )
            if handoff_manager_principal is None:
                return None
            manager_wire_principal = handoff_principal
            manager_principal = handoff_manager_principal
        elif not same_turn or remote_retry_count < observed.retry_count:
            return None
        elif _has_worker_turn_handoff(observed):
            # A reservation may only carry its exact retry into G+1.  Do not
            # let a concurrent/replayed Worker retry borrow the reservation.
            if remote_retry_count != observed.retry_count:
                return None
            if (
                observed.turn_generation
                == observed.worker_turn_handoff_from_generation
                and observed.status in _TERMINAL_TASK_STATUSES
                and not termination_settles_same_turn_handoff
            ):
                return None
    if principal_values != manager_wire_principal:
        return None
    # Authority changes only when the exact remote generation has been proven.
    # The Worker delegated envelope is comparison evidence only; persist its
    # canonical native counterpart on the Manager mirror.
    values.update(manager_principal)
    if (
        remote_retry_count != observed.retry_count
        or remote_turn_generation != observed.turn_generation
    ):
        # A logical-turn source is exact-generation-local. Mirror adoption has
        # the same ownership semantics as a local TaskQueue claim: the old
        # retry/turn's source must not remain addressable after the Worker
        # proves a replacement. A later source binding must independently
        # validate a durable row for the newly accepted generation.
        values["turn_source_log_id"] = None
    merged_metadata_updates = dict(metadata_updates or {})
    merged_metadata_updates[WORKER_REMOTE_MATERIALIZED_METADATA_KEY] = True
    remote_metadata = remote_task.get("metadata_") or {}
    if (
        isinstance(remote_metadata, dict)
        and remote_metadata.get(PR_REVIEW_SUPERSEDED_METADATA_KEY) is True
    ):
        # This reserved lifecycle marker must survive every authoritative
        # Worker→Manager path, including a normal relay GET after the hidden
        # termination response was lost.
        merged_metadata_updates[PR_REVIEW_SUPERSEDED_METADATA_KEY] = True
    if isinstance(remote_metadata, dict):
        # Plan audit summaries are safe Worker-authoritative lifecycle data.
        # Do not replace unrelated Manager-owned metadata wholesale.
        for key in (
            "plan_agent_run_id",
            "plan_review_verdict",
            "plan_review_feedback",
            "plan_review_exhausted",
        ):
            if key in remote_metadata:
                merged_metadata_updates[key] = remote_metadata[key]
    if merged_metadata_updates or clearing_termination_uncertainty:
        # Lock the exact mirror before merging JSON in Python. PostgreSQL JSON
        # has no equality operator, so comparing the whole document in the CAS
        # is not portable; the row lock protects unrelated Manager metadata
        # such as ``pr_review_id`` from being overwritten by the Worker marker.
        locked = (
            await db.execute(
                select(Task)
                .where(
                    *_worker_task_generation_write_predicates(
                        observed,
                        worker_termination_operation_id=(
                            worker_termination_operation_id
                        ),
                    )
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        if locked is None:
            await db.rollback()
            return None
        metadata = dict(locked.metadata_ or {})
        if clearing_termination_uncertainty:
            if (
                WORKER_TERMINATION_UNCERTAINTY_METADATA_KEY not in metadata
                or metadata[WORKER_TERMINATION_UNCERTAINTY_METADATA_KEY]
                != observed.termination_uncertainty
            ):
                await db.rollback()
                return None
            metadata.pop(WORKER_TERMINATION_UNCERTAINTY_METADATA_KEY)
        metadata.update(merged_metadata_updates)
        values["metadata_"] = metadata
    adoption_predicates = (
        (no_active_test_harness_owner_graph_predicate(),)
        if adopting_handoff
        else ()
    )
    changed = await db.execute(
        update(Task)
        .where(
            *_worker_task_generation_write_predicates(
                observed,
                worker_termination_operation_id=worker_termination_operation_id,
            ),
            *adoption_predicates,
        )
        .values(**values)
    )
    if changed.rowcount != 1:
        await db.rollback()
        return None
    resulting = await read_worker_task_generation(
        db,
        observed.task_id,
        observed.worker_id,
    )
    if resulting is None:
        await db.rollback()
        return None
    if commit:
        await db.commit()
    else:
        await db.flush()
    return resulting


def worker_manual_retry_source_generation(
    task: Task,
    observed: WorkerTaskGeneration,
) -> dict | None:
    """Freeze the exact source row authorized to advance retry N -> N+1."""

    if worker_task_generation(task, expected_worker_id=observed.worker_id) != observed:
        return None
    manager_principal = task_execution_principal_payload(task)
    principal_digest = worker_principal_digest(task)
    if (
        not task.incarnation_id
        or principal_digest is None
        or canonical_manager_principal_from_delegated(task)
        != manager_principal
    ):
        return None
    return {
        "task_id": task.id,
        "worker_id": observed.worker_id,
        "incarnation_id": task.incarnation_id,
        "status": task.status,
        "retry_count": task.retry_count,
        "turn_generation": task.turn_generation,
        "principal_digest": principal_digest,
    }


def _manual_retry_receipt_matches(
    receipt: object,
    *,
    operation_id: str,
    request_digest: str,
    source_generation: dict,
    target_principal: dict,
) -> bool:
    if not isinstance(receipt, dict):
        return False
    result = receipt.get("result_generation")
    return bool(
        receipt.get("version") == WORKER_MANUAL_RETRY_PROTOCOL
        and receipt.get("side") == "worker"
        and receipt.get("state") == "committed"
        and receipt.get("operation_id") == operation_id
        and receipt.get("request_digest") == request_digest
        and receipt.get("source_generation") == source_generation
        and receipt.get("target_principal") == target_principal
        and receipt.get("source_principal_digest")
        == source_generation.get("principal_digest")
        and receipt.get("target_principal_digest")
        == _handoff_payload_digest(target_principal)
        and isinstance(result, dict)
        and result.get("status") == "pending"
        and result.get("retry_count") == source_generation.get("retry_count") + 1
        and result.get("turn_generation")
        == source_generation.get("turn_generation")
    )


async def apply_authoritative_worker_retry(
    db,
    observed: WorkerTaskGeneration,
    remote_response: dict,
    *,
    operation_id: str,
    request_digest: str,
    commit: bool = True,
) -> WorkerTaskGeneration | None:
    """Adopt only the exact durable Worker retry receipt prepared by Manager.

    Ordinary relay snapshots intentionally require principal equality.  Manual
    retry is the one legal principal transition, so it has a dedicated CAS that
    proves Manager outbox + Worker receipt + source incarnation/generation and
    target delegated authority before changing the mirror.
    """

    if not isinstance(remote_response, dict):
        return None
    remote_task = remote_response.get("task")
    remote_receipt = remote_response.get("receipt")
    if not isinstance(remote_task, dict):
        return None

    # Acquire the portable Task writer fence before reading/merging JSON.  On
    # SQLite this no-op UPDATE establishes the write transaction; on row-locking
    # databases the following SELECT FOR UPDATE owns the exact row.
    fenced = await db.execute(
        update(Task)
        .where(
            *_worker_task_generation_write_predicates(observed),
            no_active_test_harness_owner_graph_predicate(),
        )
        .values(status=Task.status)
    )
    if fenced.rowcount != 1:
        await db.rollback()
        return None
    current = (
        await db.execute(
            select(Task)
            .where(*_worker_task_generation_write_predicates(observed))
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if current is None:
        await db.rollback()
        return None
    marker = worker_manual_retry_receipt(current.metadata_)
    source_generation = worker_manual_retry_source_generation(current, observed)
    if (
        marker is None
        or source_generation is None
        or marker.get("version") != WORKER_MANUAL_RETRY_PROTOCOL
        or marker.get("side") != "manager"
        or marker.get("state") != "prepared"
        or marker.get("operation_id") != operation_id
        or marker.get("request_digest") != request_digest
        or marker.get("source_generation") != source_generation
    ):
        await db.rollback()
        return None
    target_principal = marker.get("target_principal")
    target_manager_principal = marker.get("target_manager_principal")
    if (
        not isinstance(target_principal, dict)
        or canonical_delegated_principal_payload(target_principal)
        != target_principal
        or not isinstance(target_manager_principal, dict)
        or canonical_manager_principal_from_delegated(target_principal)
        != target_manager_principal
        or not _manual_retry_receipt_matches(
            remote_receipt,
            operation_id=operation_id,
            request_digest=request_digest,
            source_generation=source_generation,
            target_principal=target_principal,
        )
    ):
        await db.rollback()
        return None
    result_generation = remote_receipt["result_generation"]
    if (
        remote_task.get("id") != observed.task_id
        or remote_task.get("incarnation_id")
        != source_generation["incarnation_id"]
        or type(remote_task.get("retry_count")) is not int
        or remote_task.get("retry_count") < result_generation["retry_count"]
        or type(remote_task.get("turn_generation")) is not int
        or remote_task.get("turn_generation")
        < result_generation["turn_generation"]
        or task_execution_principal_payload(remote_task) != target_principal
    ):
        await db.rollback()
        return None

    metadata = dict(current.metadata_ or {})
    metadata[WORKER_MANUAL_RETRY_RECEIPT_METADATA_KEY] = {
        **marker,
        "state": "acknowledged",
        "worker_receipt": remote_receipt,
    }
    metadata[WORKER_REMOTE_MATERIALIZED_METADATA_KEY] = True
    # The Worker may already have dequeued pending N+1 into turn G+1 before a
    # lost HTTP response is reconciled.  The durable receipt still proves the
    # exact retry commit; first adopt its inert pending result, then let the
    # normal relay apply the separately proven dequeue/turn transition.
    remote_values = {
        "status": "pending",
        "retry_count": result_generation["retry_count"],
        "turn_generation": result_generation["turn_generation"],
        "instance_id": None,
        "error_message": None,
        "started_at": None,
        "completed_at": None,
        "pty_background_generation": None,
        **target_manager_principal,
    }
    remote_values["metadata_"] = metadata
    remote_values["turn_source_log_id"] = None
    changed = await db.execute(
        update(Task)
        .where(
            *_worker_task_generation_write_predicates(observed),
            Task.incarnation_id == source_generation["incarnation_id"],
            Task.execution_user_role == current.execution_user_role,
            Task.execution_mode == current.execution_mode,
            Task.execution_principal_kind == current.execution_principal_kind,
            (
                Task.execution_user_id.is_(None)
                if current.execution_user_id is None
                else Task.execution_user_id == current.execution_user_id
            ),
            no_active_test_harness_owner_graph_predicate(),
        )
        .values(**remote_values)
    )
    if changed.rowcount != 1:
        await db.rollback()
        return None
    resulting = await read_worker_task_generation(
        db,
        observed.task_id,
        observed.worker_id,
    )
    if resulting is None:
        await db.rollback()
        return None
    if commit:
        await db.commit()
    else:
        await db.flush()
    return resulting


def _validated_generation_history(
    remote: object,
    *,
    retry_count: int,
    turn_generation: int,
) -> list[dict] | None:
    """Validate one complete Worker history response and select one turn.

    Old rows written before exact-turn identity may coexist in a migrated
    database.  They are ignored, never imported, while every newer row must
    carry a valid scope/transport shape.  Returning ``None`` means the caller
    cannot prove that the requested generation's history is complete.
    """

    if isinstance(remote, dict):
        remote = remote.get("messages")
    if not isinstance(remote, list) or not all(
        isinstance(message, dict) for message in remote
    ):
        return None
    non_user_messages = [
        message
        for message in remote
        if message.get("event_type") != "user_message"
    ]
    scoped_messages = [
        message
        for message in non_user_messages
        if type(message.get("task_retry_count")) is int
        and type(message.get("task_turn_generation")) is int
        and _validated_native_turn_id(message) is not _INVALID_NATIVE_TURN_ID
        and _validated_turn_scope(message) is not _INVALID_TURN_SCOPE
        and _valid_relay_log_metadata(message)
    ]
    legacy_unscoped_messages = [
        message
        for message in non_user_messages
        if message.get("task_turn_generation") is None
        and (
            message.get("task_retry_count") is None
            or type(message.get("task_retry_count")) is int
        )
        and _validated_native_turn_id(message) is not _INVALID_NATIVE_TURN_ID
        and _validated_turn_scope(message) is not _INVALID_TURN_SCOPE
        and _valid_relay_log_metadata(message)
    ]
    if (
        len(scoped_messages) + len(legacy_unscoped_messages)
        != len(non_user_messages)
    ):
        return None
    return [
        message
        for message in scoped_messages
        if message["task_retry_count"] == retry_count
        and message["task_turn_generation"] == turn_generation
    ]


async def apply_authoritative_legacy_plan_execution_carrier(
    db,
    observed: WorkerTaskGeneration,
    remote_task: dict,
    remote_history: object,
    *,
    expected_proof_digest: str,
    remote_proof: LegacyPlanExecutionCarrierProof,
) -> WorkerTaskGeneration | None:
    """Adopt one already-existing legacy carrier after two-sided proof.

    This is intentionally narrower than ordinary Worker reconciliation.  It
    can only advance a still-pending Manager mirror, never creates/reposts a
    Task, and commits the exact remote history together with the lifecycle
    adoption.  That single transaction removes the crash window where a
    terminal mirror could otherwise lose the only recovery subscription before
    its output tail was durable on the Manager.
    """

    if await _active_worker_task_termination_exists(db, observed.task_id):
        return None
    if (
        observed.legacy_carrier_conflict_present
        or observed.termination_uncertainty_present
        or observed.status not in _LEGACY_CARRIER_EXECUTION_STATUSES
        or _has_worker_turn_handoff(observed)
        or not isinstance(expected_proof_digest, str)
        or len(expected_proof_digest) != 64
        or not isinstance(remote_proof, LegacyPlanExecutionCarrierProof)
        or remote_proof.task_id != observed.task_id
        or remote_proof.proof_digest != expected_proof_digest
        or remote_proof.task_status not in _LEGACY_CARRIER_EXECUTION_STATUSES
        or not legacy_plan_execution_snapshot_matches_proof(
            remote_task,
            remote_proof,
        )
    ):
        return None
    values = authoritative_worker_task_values(
        remote_task,
        task_id=observed.task_id,
        incarnation_id=observed.incarnation_id,
    )
    if (
        values is None
        or canonical_manager_principal_from_delegated(observed)
        != task_execution_principal_payload(observed)
        or canonical_delegated_principal_payload(observed)
        != task_execution_principal_payload(remote_task)
        or values["status"] != remote_proof.task_status
        or values["retry_count"] != remote_proof.retry_count
        or values["turn_generation"] != remote_proof.turn_generation
        or remote_proof.retry_count < observed.retry_count
        or remote_proof.turn_generation < observed.turn_generation
    ):
        return None
    remote_entries = _validated_generation_history(
        remote_history,
        retry_count=remote_proof.retry_count,
        turn_generation=remote_proof.turn_generation,
    )
    if remote_entries is None:
        return None

    local_proof = await legacy_approved_execution_carrier_proof(
        db,
        observed.task_id,
        for_update=True,
    )
    if (
        local_proof is None
        or local_proof.proof_digest != expected_proof_digest
        or local_proof.task_status != observed.status
        or local_proof.retry_count != observed.retry_count
        or local_proof.turn_generation != observed.turn_generation
    ):
        return None

    if (
        remote_proof.retry_count != observed.retry_count
        or remote_proof.turn_generation != observed.turn_generation
    ):
        values["turn_source_log_id"] = None
    changed = await db.execute(
        update(Task)
        .where(
            *_worker_task_generation_write_predicates(observed),
            Task.mode == "plan",
            Task.plan_approved.is_(True),
        )
        .values(**values)
    )
    if changed.rowcount != 1:
        await db.rollback()
        return None

    local_rows = (
        await db.execute(
            select(
                LogEntry.event_type,
                LogEntry.role,
                LogEntry.content,
                LogEntry.tool_name,
                LogEntry.tool_input,
                LogEntry.tool_output,
                LogEntry.loop_iteration,
                LogEntry.native_turn_id,
                LogEntry.turn_scope,
                LogEntry.actual_transport,
            ).where(
                LogEntry.task_id == observed.task_id,
                LogEntry.task_retry_count == remote_proof.retry_count,
                LogEntry.task_turn_generation == remote_proof.turn_generation,
                LogEntry.event_type != "user_message",
            )
        )
    ).all()
    missing = _missing_by_fingerprint(
        [dict(row._mapping) for row in local_rows],
        remote_entries,
    )
    for message in missing:
        turn_scope = _validated_turn_scope(message)
        db.add(
            LogEntry(
                instance_id=None,
                task_id=observed.task_id,
                task_retry_count=remote_proof.retry_count,
                task_turn_generation=remote_proof.turn_generation,
                native_turn_id=_validated_native_turn_id(message),
                turn_scope=turn_scope,
                actual_transport=_validated_actual_transport(
                    message,
                    turn_scope,
                ),
                event_type=message.get("event_type") or "message",
                role=message.get("role"),
                content=message.get("content"),
                tool_name=message.get("tool_name"),
                tool_input=message.get("tool_input"),
                tool_output=message.get("tool_output"),
                raw_json=message.get("raw_json"),
                is_error=message.get("is_error", False),
                loop_iteration=message.get("loop_iteration"),
            )
        )
    resulting = await read_worker_task_generation(
        db,
        observed.task_id,
        observed.worker_id,
    )
    if (
        resulting is None
        or resulting.status != remote_proof.task_status
        or resulting.retry_count != remote_proof.retry_count
        or resulting.turn_generation != remote_proof.turn_generation
    ):
        await db.rollback()
        return None
    await db.commit()
    return resulting


def _entry_fingerprint(e: dict) -> tuple:
    """Stable identity for a relayed log entry, comparable between the local DB
    copy and the remote chat/history payload.  Only tool payloads are capped,
    exactly at the history endpoint's observable truncation boundary; message
    content is returned in full and must never be collapsed by a shorter
    convenience prefix."""
    def p(s):
        return (s or "")[:_FP_PREFIX]
    return (
        e.get("event_type") or "",
        e.get("role") or "",
        e.get("content") or "",
        e.get("tool_name") or "",
        p(e.get("tool_input")),
        p(e.get("tool_output")),
        e.get("loop_iteration"),
        # Scope is durable terminal-arbitration evidence.  A legacy NULL row
        # must not suppress a later exact copy carrying an authoritative scope.
        e.get("turn_scope"),
        e.get("actual_transport"),
        # Native turns can retry/rebind within one logical task generation.
        # Identical text from two such turns is two pieces of evidence, not a
        # reconnect duplicate.
        e.get("native_turn_id"),
    )


def _missing_by_fingerprint(local_entries: list[dict], remote_entries: list[dict]) -> list[dict]:
    """Remote entries not already present locally, matched by fingerprint multiset.

    Order- and race-tolerant: unlike count-based tail slicing
    (``remote[local_count:]``), a mid-stream gap or a concurrent live-relay insert
    cannot make an already-present entry be re-inserted — the duplicate-message-
    on-reconnect bug.
    """
    have = Counter(_entry_fingerprint(e) for e in local_entries)
    missing: list[dict] = []
    for r in remote_entries:
        fp = _entry_fingerprint(r)
        if have.get(fp, 0) > 0:
            have[fp] -= 1
        else:
            missing.append(r)
    return missing

logger = logging.getLogger(__name__)

# 与 worker instance_manager 实际入库/广播的 chat 事件对齐
CHAT_EVENT_TYPES = {
    "user_message", "message", "result", "tool_use", "tool_result",
    "system_init", "system_event", "thinking", "process_exit",
}


def _validated_sub_agent_relay_payload(
    data: dict,
    *,
    terminal: bool,
) -> dict[str, object] | None:
    """Validate one Worker-owned CCM Sub-Agent mirror snapshot."""

    remote_id = data.get("sub_agent_session_id")
    description = data.get("description")
    monitor_context = data.get("monitor_context")
    status = data.get("status")
    checks_done = data.get("checks_done")
    last_summary = data.get("last_summary")
    if (
        type(remote_id) is not int
        or remote_id <= 0
        or data.get("agent_type") != "sub_agent"
        or data.get("source") != "ccm"
        or not isinstance(description, str)
        or len(description) > 500
        or (monitor_context is not None and not isinstance(monitor_context, str))
        or type(checks_done) is not int
        or checks_done < 0
        or (last_summary is not None and not isinstance(last_summary, str))
    ):
        return None
    if terminal:
        if status not in _SUB_AGENT_TERMINAL_STATUSES:
            return None
    elif status != "running":
        return None
    return {
        "remote_id": remote_id,
        "description": description,
        "monitor_context": monitor_context,
        "status": status,
        "checks_done": checks_done,
        "last_summary": last_summary,
    }


def _validated_native_sub_agent_relay_payload(
    data: dict,
    *,
    terminal: bool,
) -> dict[str, object] | None:
    """Validate a complete Worker-owned native child snapshot."""

    remote_id = data.get("sub_agent_session_id")
    agent_type = data.get("agent_type")
    provider = data.get("provider")
    description = data.get("description")
    status = data.get("status")
    checks_done = data.get("checks_done")
    last_summary = data.get("last_summary")
    codex_thread_id = data.get("codex_thread_id")
    native_sequence = data.get("native_sequence")
    model = data.get("model")
    reasoning_effort = data.get("reasoning_effort")
    if (
        data.get("native_mirror_version")
        != _NATIVE_SUB_AGENT_MIRROR_VERSION
        or type(remote_id) is not int
        or remote_id <= 0
        or agent_type not in {"native-agent", "native-monitor"}
        or data.get("source") != "native"
        or provider not in {"claude", "codex"}
        or not isinstance(description, str)
        or len(description) > 500
        or type(checks_done) is not int
        or checks_done < 0
        or (
            last_summary is not None
            and (
                not isinstance(last_summary, str)
                or len(last_summary) > 2000
            )
        )
        or (
            codex_thread_id is not None
            and (
                not isinstance(codex_thread_id, str)
                or not codex_thread_id
                or codex_thread_id != codex_thread_id.strip()
                or len(codex_thread_id) > 255
            )
        )
        or (provider == "codex" and codex_thread_id is None)
        or (provider != "codex" and codex_thread_id is not None)
        or (
            provider == "codex"
            and (
                type(native_sequence) is not int
                or native_sequence <= 0
            )
        )
        or (provider != "codex" and native_sequence is not None)
        or (
            model is not None
            and (not isinstance(model, str) or len(model) > 100)
        )
        or (
            reasoning_effort is not None
            and (
                not isinstance(reasoning_effort, str)
                or len(reasoning_effort) > 20
            )
        )
    ):
        return None
    if terminal:
        if status not in _NATIVE_SUB_AGENT_TERMINAL_STATUSES:
            return None
    elif status != "running":
        return None
    return {
        "remote_id": remote_id,
        "agent_type": agent_type,
        "provider": provider,
        "description": description,
        "status": status,
        "checks_done": checks_done,
        "last_summary": last_summary,
        "codex_thread_id": codex_thread_id,
        "native_sequence": native_sequence,
        "model": model,
        "reasoning_effort": reasoning_effort,
    }


def _worker_child_mirror_meta(
    *,
    worker_id: int,
    task_incarnation_id: str,
    remote_id: int,
    native_sequence: int | None = None,
) -> str:
    payload = {
        _WORKER_CHILD_MIRROR_META_KEY: {
            "worker_id": worker_id,
            "task_incarnation_id": task_incarnation_id,
            "remote_id": remote_id,
        }
    }
    if native_sequence is not None:
        payload["native_sequence"] = native_sequence
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _worker_child_mirror_snapshot_identity(
    meta: object,
) -> tuple[tuple[int, str, int], int | None] | None:
    if not isinstance(meta, str):
        return None
    try:
        payload = json.loads(meta)
    except (TypeError, ValueError):
        return None
    if (
        not isinstance(payload, dict)
        or set(payload)
        not in (
            {_WORKER_CHILD_MIRROR_META_KEY},
            {_WORKER_CHILD_MIRROR_META_KEY, "native_sequence"},
        )
    ):
        return None
    identity = payload.get(_WORKER_CHILD_MIRROR_META_KEY)
    if not isinstance(identity, dict) or set(identity) != {
        "worker_id",
        "task_incarnation_id",
        "remote_id",
    }:
        return None
    worker_id = identity.get("worker_id")
    incarnation_id = identity.get("task_incarnation_id")
    remote_id = identity.get("remote_id")
    if (
        type(worker_id) is not int
        or worker_id <= 0
        or not isinstance(incarnation_id, str)
        or len(incarnation_id) != 32
        or any(char not in "0123456789abcdef" for char in incarnation_id)
        or type(remote_id) is not int
        or remote_id <= 0
    ):
        return None
    native_sequence = payload.get("native_sequence")
    if native_sequence is not None and (
        type(native_sequence) is not int or native_sequence <= 0
    ):
        return None
    return (worker_id, incarnation_id, remote_id), native_sequence


def worker_child_mirror_identity(
    meta: object,
) -> tuple[int, str, int] | None:
    parsed = _worker_child_mirror_snapshot_identity(meta)
    return parsed[0] if parsed is not None else None


def _exact_worker_child_mirror(
    mirrors: list[SubAgentSession],
    expected_identity: tuple[int, str, int],
) -> tuple[SubAgentSession | None, bool]:
    """Select one exact mirror without aliasing historical Worker rows.

    A Task can move from Worker A to Worker B while both Workers independently
    allocate the same numeric child id.  Valid mirrors for other Worker/task
    incarnations are history and may coexist.  A row with missing or malformed
    identity is ambiguous, however, so the whole mutation must fail closed.
    """

    exact: list[SubAgentSession] = []
    for mirror in mirrors:
        identity = worker_child_mirror_identity(mirror.meta)
        if identity is None:
            return None, False
        if identity == expected_identity:
            exact.append(mirror)
    if len(exact) > 1:
        return None, False
    return (exact[0] if exact else None), True

# Unlike status/background/plan notifications, these events apply payload
# fields directly to the Manager mirror.  They therefore cannot use the
# Manager's current generation at receive time as their identity: every
# producer must freeze both counters when the event is created, and the relay
# must drop missing, malformed, or stale identities before any DB mutation or
# frontend forwarding.
EXACT_GENERATION_RELAY_EVENT_TYPES = frozenset({
    "context_usage",
    "loop_iteration_end",
    "goal_evaluation",
    "message_delta",
    "thinking_delta",
    "monitor_session_created",
    "monitor_check",
    "monitor_session_status",
    "sub_agent_count",
    "sub_agent_session_created",
    "sub_agent_report",
    "sub_agent_session_status",
})

_INVALID_NATIVE_TURN_ID = object()
_INVALID_TURN_SCOPE = object()
_INVALID_ACTUAL_TRANSPORT = object()


def _validated_native_turn_id(payload: dict):
    """Return a bounded native id, ``None``, or an invalid sentinel."""

    value = payload.get("native_turn_id")
    if value is None:
        return None
    if isinstance(value, str) and len(value) <= 200:
        return value
    return _INVALID_NATIVE_TURN_ID


def _validated_turn_scope(payload: dict):
    """Return an exact scope, legacy ``None``, or an invalid sentinel.

    Rolling-upgrade Workers legitimately omit this newly introduced field.
    Such events remain visible but cannot become terminal evidence.  Explicit
    unknown values are protocol violations and must fail closed instead of
    being guessed as foreground output.
    """

    value = payload.get("turn_scope")
    if value is None:
        return None
    return (
        value
        if type(value) is str and value in _TURN_SCOPES
        else _INVALID_TURN_SCOPE
    )


def _validated_actual_transport(payload: dict, turn_scope):
    """Validate immutable source transport without inferring a route.

    The current relay intentionally does not map a Worker's source row onto a
    distinct Manager source row. Consequently output events with a missing
    route stay NULL and Worker Auto Capability must remain gated off; neither
    provider nor model metadata may fill that proof gap.
    """

    value = payload.get("actual_transport")
    if value is None:
        return None
    if (
        turn_scope != "source"
        or type(value) is not str
        or value not in _ACTUAL_TRANSPORTS
    ):
        return _INVALID_ACTUAL_TRANSPORT
    return value


def _valid_relay_log_metadata(payload: dict) -> bool:
    scope = _validated_turn_scope(payload)
    return bool(
        scope is not _INVALID_TURN_SCOPE
        and _validated_actual_transport(payload, scope)
        is not _INVALID_ACTUAL_TRANSPORT
    )


class WorkerRelay:
    def __init__(self, db_factory, broadcaster):
        self.db_factory = db_factory
        self.broadcaster = broadcaster
        self._ws: dict[int, object] = {}            # worker_id -> ws connection
        self._tasks: dict[int, set[int]] = {}       # worker_id -> relayed task ids
        self._loops: dict[int, asyncio.Task] = {}    # worker_id -> relay loop（强引用）
        self._closing: set[int] = set()
        self._connection_locks: dict[int, asyncio.Lock] = {}
        self._reconnect_tasks: dict[int, set[asyncio.Task]] = {}
        self._handoff_recovery_tasks: dict[
            tuple[int, int, str], asyncio.Task
        ] = {}
        self._legacy_carrier_recovery_tasks: dict[
            tuple[int, int, str], asyncio.Task
        ] = {}
        # A recovery stays strongly owned through its final readback, but live
        # events must queue behind the operation lock instead of being dropped
        # once the initial semantic adoption has committed.
        self._legacy_carrier_recovery_released: set[
            tuple[int, int, str]
        ] = set()
        self._shutting_down = False
        self._shutdown_task: asyncio.Task | None = None

    # ------------------------------------------------------------------
    # 连接管理
    # ------------------------------------------------------------------

    @staticmethod
    def _ws_url(worker: Worker) -> str:
        return f"ws://{worker.private_ip}:{worker.ccm_port}/ws"

    @staticmethod
    def _api(worker: Worker, path: str) -> str:
        return f"http://{worker.private_ip}:{worker.ccm_port}{path}"

    @staticmethod
    def _require_authenticated_control_plane(worker: Worker) -> None:
        if (
            not isinstance(settings.auth_token, str)
            or not settings.auth_token.strip()
        ):
            raise HTTPException(
                503,
                "AUTH_TOKEN must be configured before Worker relay operations",
            )
        if (
            not isinstance(worker.auth_token, str)
            or not worker.auth_token.strip()
        ):
            raise HTTPException(
                503,
                "Worker relay authentication credential is unavailable",
            )

    @classmethod
    def _headers(cls, worker: Worker) -> dict:
        cls._require_authenticated_control_plane(worker)
        return {"Authorization": f"Bearer {worker.auth_token}"}

    def _connection_lock(self, worker_id: int) -> asyncio.Lock:
        return self._connection_locks.setdefault(worker_id, asyncio.Lock())

    def _assert_open(self) -> None:
        if self._shutting_down:
            raise RuntimeError("Worker relay is shutting down")

    async def start(self) -> None:
        """Open a fresh runtime generation after a fully completed shutdown."""

        if not self._shutting_down and self._shutdown_task is None:
            return
        shutdown_task = self._shutdown_task
        if shutdown_task is None or not shutdown_task.done():
            raise RuntimeError("Worker relay shutdown is still in progress")
        # Propagate a failed/cancelled shutdown instead of reopening on an
        # uncertain resource snapshot.
        shutdown_task.result()
        owned_registries = (
            self._ws,
            self._tasks,
            self._loops,
            self._reconnect_tasks,
            self._handoff_recovery_tasks,
            self._legacy_carrier_recovery_tasks,
            self._legacy_carrier_recovery_released,
        )
        if any(owned_registries):
            raise RuntimeError(
                "Worker relay shutdown left owned resources behind"
            )
        self._closing.clear()
        self._shutdown_task = None
        self._shutting_down = False

    def _schedule_reconnect(
        self,
        worker: Worker,
        task_ids: set[int],
    ) -> None:
        """Start and strongly own one reconnect attempt for ``worker``."""

        worker_id = worker.id
        if self._shutting_down or worker_id in self._closing:
            return
        task = asyncio.create_task(self._reconnect(worker, task_ids))
        worker_tasks = self._reconnect_tasks.setdefault(worker_id, set())
        worker_tasks.add(task)

        def cleanup(done: asyncio.Task) -> None:
            registered = self._reconnect_tasks.get(worker_id)
            if registered is not None:
                registered.discard(done)
                if not registered:
                    self._reconnect_tasks.pop(worker_id, None)
            if done.cancelled():
                return
            try:
                error = done.exception()
            except asyncio.CancelledError:
                return
            if error is not None:
                logger.error(
                    "worker %s relay reconnect task failed",
                    worker_id,
                    exc_info=(type(error), error, error.__traceback__),
                )

        task.add_done_callback(cleanup)

    async def _ensure_connection_locked(self, worker: Worker):
        self._assert_open()
        if worker.id in self._ws:
            # A replacement connection may have been installed while an older
            # relay is backing off.  Keep the subscription owner present even
            # when no new socket needs to be created.
            self._tasks.setdefault(worker.id, set())
            return
        self._closing.discard(worker.id)
        ws = await websockets.connect(
            self._ws_url(worker),
            additional_headers=self._headers(worker),
            open_timeout=15,
        )
        try:
            # Global shutdown may win while connect() is awaiting the network.
            # Never publish that late socket into the relay maps.
            self._assert_open()
            await ws.send(
                json.dumps({"action": "subscribe", "channels": ["tasks"]})
            )
        except BaseException:
            try:
                await ws.close()
            except Exception:
                pass
            raise
        self._ws[worker.id] = ws
        self._tasks.setdefault(worker.id, set())
        loop_task = asyncio.create_task(self._relay_loop(ws, worker))
        self._loops[worker.id] = loop_task
        logger.info("worker relay connected: worker %s (%s)", worker.id, worker.private_ip)

    async def ensure_connection(self, worker: Worker):
        async with self._connection_lock(worker.id):
            await self._ensure_connection_locked(worker)

    async def subscribe_task(self, worker: Worker, task_id: int):
        """幂等订阅某 task 的事件中继。必须在向 worker 创建/操作 task 之前调用，
        否则初始事件会丢。"""
        async with self._connection_lock(worker.id):
            await self._ensure_connection_locked(worker)
            self._assert_open()
            if task_id in self._tasks.get(worker.id, set()):
                return
            ws = self._ws[worker.id]
            await ws.send(
                json.dumps({
                    "action": "subscribe",
                    "channels": [f"task:{task_id}"],
                })
            )
            self._tasks[worker.id].add(task_id)

    def unsubscribe_task(self, worker_id: int, task_id: int):
        """迁移后停止中继该 task（_handle 按 self._tasks 过滤，移除即生效）。"""
        self._tasks.get(worker_id, set()).discard(task_id)

    def _legacy_carrier_recovery_active(
        self,
        worker_id: int,
        task_id: int,
    ) -> bool:
        """Return whether live relay must defer to semantic carrier readback."""

        return any(
            key[0] == worker_id
            and key[1] == task_id
            and key not in self._legacy_carrier_recovery_released
            and not recovery.done()
            for key, recovery in self._legacy_carrier_recovery_tasks.items()
        )

    def ensure_legacy_plan_execution_carrier_recovery(
        self,
        worker: Worker,
        task_id: int,
        expected_proof_digest: str,
        proxy,
    ) -> None:
        """Own read-only recovery of one pre-Plan-v2 Worker carrier.

        Registration happens synchronously before the first network await so
        the relay can subscribe without letting an unproved live Worker event
        mutate the pending Manager mirror.  Full history/snapshot readback then
        commits with the exact local generation before normal relay resumes.
        """

        if (
            self._shutting_down
            or worker.id in self._closing
            or type(task_id) is not int
            or task_id <= 0
            or not isinstance(expected_proof_digest, str)
            or len(expected_proof_digest) != 64
            or any(
                char not in "0123456789abcdef"
                for char in expected_proof_digest
            )
        ):
            return
        for key, existing in self._legacy_carrier_recovery_tasks.items():
            if (
                key[0] == worker.id
                and key[1] == task_id
                and not existing.done()
            ):
                return
        key = (worker.id, task_id, expected_proof_digest)
        recovery = asyncio.create_task(
            self._legacy_plan_execution_carrier_recovery_loop(
                worker.id,
                task_id,
                expected_proof_digest,
                proxy,
            )
        )
        self._legacy_carrier_recovery_tasks[key] = recovery

        def cleanup(done: asyncio.Task) -> None:
            if self._legacy_carrier_recovery_tasks.get(key) is done:
                self._legacy_carrier_recovery_tasks.pop(key, None)
            self._legacy_carrier_recovery_released.discard(key)
            if done.cancelled():
                return
            try:
                error = done.exception()
            except asyncio.CancelledError:
                return
            if error is not None:
                logger.error(
                    "Legacy Plan carrier recovery failed for task %s",
                    task_id,
                    exc_info=(type(error), error, error.__traceback__),
                )

        recovery.add_done_callback(cleanup)

    async def _conflict_legacy_plan_execution_carrier(
        self,
        observed: WorkerTaskGeneration,
        *,
        expected_proof_digest: str,
        error: str,
    ) -> WorkerTaskGeneration | None:
        """Durably expose a proven semantic split without replaying either side."""

        async with self.db_factory() as db:
            if await _active_worker_task_termination_exists(
                db,
                observed.task_id,
            ):
                return None
            local_proof = await legacy_approved_execution_carrier_proof(
                db,
                observed.task_id,
                for_update=True,
            )
            if (
                local_proof is None
                or local_proof.proof_digest != expected_proof_digest
                or local_proof.task_status != observed.status
                or local_proof.retry_count != observed.retry_count
                or local_proof.turn_generation != observed.turn_generation
            ):
                return None
            locked = (
                await db.execute(
                    select(Task)
                    .where(
                        *_worker_task_generation_write_predicates(observed),
                        Task.mode == "plan",
                        Task.plan_approved.is_(True),
                    )
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            if locked is None:
                await db.rollback()
                return None
            metadata = dict(locked.metadata_ or {})
            if LEGACY_PLAN_CARRIER_CONFLICT_METADATA_KEY in metadata:
                await db.rollback()
                return None
            metadata[LEGACY_PLAN_CARRIER_CONFLICT_METADATA_KEY] = {
                "version": 1,
                "operation_id": uuid.uuid4().hex,
                "created_at": datetime.utcnow().isoformat(
                    timespec="microseconds"
                ),
                "expected_proof_digest": expected_proof_digest,
                "source_generation": _generation_marker_payload(observed),
            }
            changed = await db.execute(
                update(Task)
                .where(
                    *_worker_task_generation_write_predicates(observed),
                    Task.mode == "plan",
                    Task.plan_approved.is_(True),
                )
                .values(
                    metadata_=metadata,
                    status="conflict",
                    completed_at=datetime.utcnow(),
                    error_message=error,
                )
            )
            if changed.rowcount != 1:
                await db.rollback()
                return None
            resulting = await read_worker_task_generation(
                db,
                observed.task_id,
                observed.worker_id,
            )
            if (
                resulting is None
                or resulting.status != "conflict"
                or not resulting.legacy_carrier_conflict_present
            ):
                await db.rollback()
                return None
            await db.commit()
            return resulting

    async def _quarantine_legacy_plan_execution_carrier(
        self,
        observed: WorkerTaskGeneration,
        *,
        expected_proof_digest: str,
        remote_proof: LegacyPlanExecutionCarrierProof,
    ) -> WorkerTaskGeneration | None:
        if (
            remote_proof.task_id != observed.task_id
            or remote_proof.proof_digest == expected_proof_digest
        ):
            return None
        return await self._conflict_legacy_plan_execution_carrier(
            observed,
            expected_proof_digest=expected_proof_digest,
            error=(
                "Legacy Plan execution carrier differs between Manager and "
                "Worker; remote execution was quarantined without replay"
            ),
        )

    async def _legacy_plan_execution_carrier_recovery_loop(
        self,
        worker_id: int,
        task_id: int,
        expected_proof_digest: str,
        proxy,
    ) -> None:
        """Retry semantic readback until the remote carrier leaves pending."""

        from backend.services.worker_proxy import get_task_operation_lock

        key = (worker_id, task_id, expected_proof_digest)
        delay = LEGACY_CARRIER_RECOVERY_BASE_DELAY
        adopted = False
        while not self._shutting_down and worker_id not in self._closing:
            resulting: WorkerTaskGeneration | None = None
            try:
                async with get_task_operation_lock(task_id):
                    if self._shutting_down or worker_id in self._closing:
                        return
                    observed = await self._observe_task_generation(
                        worker_id,
                        task_id,
                    )
                    if (
                        observed is None
                        or (
                            observed.status != "pending"
                            and not adopted
                        )
                    ):
                        return
                    previous_status = observed.status
                    async with self.db_factory() as db:
                        if await _active_worker_task_termination_exists(
                            db,
                            task_id,
                        ):
                            return
                        local_proof = (
                            await legacy_approved_execution_carrier_proof(
                                db,
                                task_id,
                            )
                        )
                        worker = await db.get(Worker, worker_id)
                    if (
                        local_proof is None
                        or local_proof.proof_digest
                        != expected_proof_digest
                        or local_proof.task_status != observed.status
                        or local_proof.retry_count != observed.retry_count
                        or local_proof.turn_generation
                        != observed.turn_generation
                    ):
                        return
                    if worker is None or worker.status != "ready":
                        raise RuntimeError(
                            "legacy Plan carrier Worker is not ready"
                        )

                    # Subscribe only after the in-memory quarantine above is
                    # registered, and before any proof/snapshot readback.
                    await self.subscribe_task(worker, task_id)
                    remote_proof = (
                        await proxy.get_legacy_plan_execution_carrier_proof(
                            worker,
                            task_id,
                        )
                    )
                    if remote_proof is None:
                        resulting = await (
                            self._conflict_legacy_plan_execution_carrier(
                                observed,
                                expected_proof_digest=expected_proof_digest,
                                error=(
                                    "Legacy Plan execution carrier is absent "
                                    "on its assigned Worker; automatic replay "
                                    "was quarantined"
                                ),
                            )
                        )
                    elif remote_proof.proof_digest != expected_proof_digest:
                        resulting = (
                            await self._quarantine_legacy_plan_execution_carrier(
                                observed,
                                expected_proof_digest=expected_proof_digest,
                                remote_proof=remote_proof,
                            )
                        )
                    elif resulting is None:
                        async with httpx.AsyncClient(timeout=30) as client:
                            remote_task = await self._fetch_task_snapshot(
                                worker,
                                task_id,
                                observed.incarnation_id,
                                client=client,
                            )
                            history_response = await client.get(
                                self._api(
                                    worker,
                                    f"/api/tasks/{task_id}/chat/history?compact=false",
                                ),
                                headers={
                                    **self._headers(worker),
                                    "X-CCM-Task-Incarnation": (
                                        observed.incarnation_id
                                    ),
                                },
                            )
                            remote_history = (
                                history_response.json()
                                if history_response.status_code == 200
                                else None
                            )
                        proof_after = (
                            await proxy.get_legacy_plan_execution_carrier_proof(
                                worker,
                                task_id,
                            )
                        )
                        if proof_after != remote_proof:
                            raise RuntimeError(
                                "legacy Plan carrier changed during readback"
                            )
                        if (
                            remote_task is not None
                            and remote_history is not None
                            and not legacy_plan_execution_snapshot_matches_proof(
                                remote_task,
                                remote_proof,
                            )
                        ):
                            resulting = await (
                                self._conflict_legacy_plan_execution_carrier(
                                    observed,
                                    expected_proof_digest=expected_proof_digest,
                                    error=(
                                        "Legacy Plan execution carrier Task "
                                        "snapshot differs from its semantic "
                                        "proof; remote execution was quarantined"
                                    ),
                                )
                            )
                        elif remote_task is not None and remote_history is not None:
                            async with self.db_factory() as db:
                                resulting = await (
                                    apply_authoritative_legacy_plan_execution_carrier(
                                        db,
                                        observed,
                                        remote_task,
                                        remote_history,
                                        expected_proof_digest=(
                                            expected_proof_digest
                                        ),
                                        remote_proof=remote_proof,
                                    )
                                )
                    if resulting is not None and resulting.status == "conflict":
                        # Conflict is a permanent quarantine.  Keeping the
                        # socket's routing hint would let a fresh remote
                        # snapshot overwrite it after this recovery exits.
                        self.unsubscribe_task(worker_id, task_id)
                    elif resulting is not None and resulting.status != "pending":
                        adopted = True
                        # Stop dropping live events before the closing readback,
                        # while the operation lock still makes them wait.  A
                        # second stable proof/snapshot/history pass catches every
                        # event dropped during the initial quarantine.
                        self._legacy_carrier_recovery_released.add(key)
                        try:
                            closing_proof = (
                                await proxy.get_legacy_plan_execution_carrier_proof(
                                    worker,
                                    task_id,
                                )
                            )
                            if closing_proof is None:
                                raise RuntimeError(
                                    "legacy Plan carrier disappeared during "
                                    "closing readback"
                                )
                            async with httpx.AsyncClient(timeout=30) as client:
                                closing_task = await self._fetch_task_snapshot(
                                    worker,
                                    task_id,
                                    observed.incarnation_id,
                                    client=client,
                                )
                                closing_history_response = await client.get(
                                    self._api(
                                        worker,
                                        f"/api/tasks/{task_id}/chat/history?compact=false",
                                    ),
                                    headers={
                                        **self._headers(worker),
                                        "X-CCM-Task-Incarnation": (
                                            observed.incarnation_id
                                        ),
                                    },
                                )
                                closing_history = (
                                    closing_history_response.json()
                                    if closing_history_response.status_code == 200
                                    else None
                                )
                            closing_proof_after = (
                                await proxy.get_legacy_plan_execution_carrier_proof(
                                    worker,
                                    task_id,
                                )
                            )
                            if (
                                closing_proof_after != closing_proof
                                or closing_proof.proof_digest
                                != expected_proof_digest
                                or closing_task is None
                                or closing_history is None
                                or not legacy_plan_execution_snapshot_matches_proof(
                                    closing_task,
                                    closing_proof,
                                )
                            ):
                                raise RuntimeError(
                                    "legacy Plan carrier closing readback was "
                                    "not stable"
                                )
                            async with self.db_factory() as db:
                                closing_result = await (
                                    apply_authoritative_legacy_plan_execution_carrier(
                                        db,
                                        resulting,
                                        closing_task,
                                        closing_history,
                                        expected_proof_digest=expected_proof_digest,
                                        remote_proof=closing_proof,
                                    )
                                )
                            if closing_result is None:
                                raise RuntimeError(
                                    "legacy Plan carrier closing generation "
                                    "could not be committed"
                                )
                            resulting = closing_result
                        except BaseException:
                            self._legacy_carrier_recovery_released.discard(key)
                            raise
                    if resulting is not None and resulting.status != previous_status:
                        await self._publish_status_generation(
                            resulting,
                            notify_completion=False,
                        )
                    if resulting is not None and resulting.status != "pending":
                        return
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.info(
                    "Legacy Plan carrier recovery deferred for task %s",
                    task_id,
                    exc_info=True,
                )
            await asyncio.sleep(delay)
            delay = min(
                max(delay * 2, LEGACY_CARRIER_RECOVERY_BASE_DELAY),
                LEGACY_CARRIER_RECOVERY_MAX_DELAY,
            )

    async def _stop_worker_impl(self, worker_id: int) -> None:
        """Close one Worker's socket and await every owned background task."""

        owned_tasks: set[asyncio.Task] = set()
        async with self._connection_lock(worker_id):
            self._closing.add(worker_id)
            ws = self._ws.pop(worker_id, None)
            self._tasks.pop(worker_id, None)
            loop_task = self._loops.pop(worker_id, None)
            reconnect_tasks = list(self._reconnect_tasks.pop(worker_id, set()))
            recovery_items = [
                (key, task)
                for key, task in list(self._handoff_recovery_tasks.items())
                if key[0] == worker_id
            ]
            legacy_recovery_items = [
                (key, task)
                for key, task in list(
                    self._legacy_carrier_recovery_tasks.items()
                )
                if key[0] == worker_id
            ]
            if loop_task is not None:
                owned_tasks.add(loop_task)
            owned_tasks.update(reconnect_tasks)
            owned_tasks.update(task for _key, task in recovery_items)
            owned_tasks.update(
                task for _key, task in legacy_recovery_items
            )
            current = asyncio.current_task()
            for task in owned_tasks:
                if task is not current and not task.done():
                    task.cancel()
            if ws is not None:
                try:
                    await ws.close()
                except Exception:
                    logger.debug(
                        "worker relay socket close failed for worker %s",
                        worker_id,
                        exc_info=True,
                    )

        awaitable_tasks = [
            task for task in owned_tasks if task is not asyncio.current_task()
        ]
        if awaitable_tasks:
            await asyncio.gather(*awaitable_tasks, return_exceptions=True)
        for key, task in recovery_items:
            if self._handoff_recovery_tasks.get(key) is task:
                self._handoff_recovery_tasks.pop(key, None)
        for key, task in legacy_recovery_items:
            if self._legacy_carrier_recovery_tasks.get(key) is task:
                self._legacy_carrier_recovery_tasks.pop(key, None)
            self._legacy_carrier_recovery_released.discard(key)

    async def stop_worker(self, worker_id: int):
        """断开并停止重连（worker 关机/销毁前必须调，否则重连风暴）。"""

        operation = asyncio.create_task(self._stop_worker_impl(worker_id))
        # A cancelled API/lifespan caller must not abandon a half-closed
        # socket with reconnect or handoff producers still running.
        cancellation = await await_task_completion(operation)
        operation.result()
        if cancellation is not None:
            raise cancellation

    async def _shutdown_impl(self) -> None:
        worker_ids = (
            set(self._connection_locks)
            | set(self._ws)
            | set(self._tasks)
            | set(self._loops)
            | set(self._reconnect_tasks)
            | {key[0] for key in self._handoff_recovery_tasks}
            | {key[0] for key in self._legacy_carrier_recovery_tasks}
        )
        results = await asyncio.gather(
            *(self._stop_worker_impl(worker_id) for worker_id in worker_ids),
            return_exceptions=True,
        )

        # The shutdown flag prevents new registrations, but take one final
        # snapshot so a task which was between creation and map insertion when
        # shutdown began cannot escape the first worker-id snapshot.
        leftovers = {
            *self._loops.values(),
            *(
                task
                for tasks in self._reconnect_tasks.values()
                for task in tasks
            ),
            *self._handoff_recovery_tasks.values(),
            *self._legacy_carrier_recovery_tasks.values(),
        }
        current = asyncio.current_task()
        for task in leftovers:
            if task is not current and not task.done():
                task.cancel()
        awaitable_leftovers = [
            task for task in leftovers if task is not current
        ]
        if awaitable_leftovers:
            await asyncio.gather(*awaitable_leftovers, return_exceptions=True)

        # A connect already in flight when the admission fence closed will
        # close its own late socket before publishing it.  Still drain the map
        # defensively so shutdown's postcondition never depends on that path.
        leftover_sockets = list(self._ws.values())
        self._ws.clear()
        if leftover_sockets:
            await asyncio.gather(
                *(socket.close() for socket in leftover_sockets),
                return_exceptions=True,
            )
        self._loops.clear()
        self._reconnect_tasks.clear()
        self._handoff_recovery_tasks.clear()
        self._legacy_carrier_recovery_tasks.clear()
        self._legacy_carrier_recovery_released.clear()
        self._tasks.clear()

        failures = [result for result in results if isinstance(result, BaseException)]
        if failures:
            raise RuntimeError(
                f"Worker relay shutdown failed for {len(failures)} worker(s)"
            ) from failures[0]

    async def shutdown(self) -> None:
        """Idempotently quiesce every Manager-side Worker relay producer."""

        if self._shutdown_task is None:
            # This synchronous transition is the global admission fence. Every
            # connection/recovery path checks it before its next side effect.
            self._shutting_down = True
            self._shutdown_task = asyncio.create_task(self._shutdown_impl())
        operation = self._shutdown_task
        cancellation = await await_task_completion(operation)
        operation.result()
        if cancellation is not None:
            raise cancellation

    async def recover(self, worker: Worker):
        """worker 恢复（开机/健康自动恢复/Manager 重启）后重建中继 + 补日志。"""
        if self._shutting_down:
            return
        async with self.db_factory() as db:
            result = await db.execute(
                select(Task).where(
                    Task.worker_id == worker.id,
                    _worker_task_termination_apply_predicate(),
                    or_(
                        Task.status.in_(
                            ["executing", "in_progress", "plan_review"]
                        ),
                        (
                            (Task.status == "completed")
                            & Task.pty_background_generation.isnot(None)
                        ),
                        # A lost stop/cancel response is deliberately stored
                        # as conflict. Re-subscribe it after restart so one
                        # exact terminal GET can converge the durable marker;
                        # ordinary/legacy conflicts are filtered below.
                        Task.status == "conflict",
                        # A completed Manager mirror may already have ACKed a
                        # follow-up while the Worker has not emitted the first
                        # exact G+1 event yet.  The durable reservation is an
                        # active relay obligation in its own right: after a
                        # Manager restart we must re-subscribe and backfill it,
                        # otherwise the first G+1 event can be lost forever and
                        # the handoff marker can never collect its second piece
                        # of evidence.
                        Task.worker_turn_handoff_id.isnot(None),
                    ),
                )
            )
            active = [
                task
                for task in result.scalars().all()
                if task.status != "conflict"
                or has_worker_termination_uncertainty(task.metadata_)
            ]
        for t in active:
            if self._shutting_down:
                return
            try:
                await self.subscribe_task(worker, t.id)
            except Exception:
                logger.exception("recover: subscribe task %s on worker %s failed", t.id, worker.id)
                return
        if active and not self._shutting_down:
            # Backfill performs one bounded reconciliation pass for every
            # durable handoff and arms the long-lived retry loop when it does
            # not settle.  Do not perform another synchronous replay here:
            # one unreachable Worker would otherwise block recovery of every
            # other Task for the full HTTP retry budget.
            await self._backfill_missing_logs(worker, {t.id for t in active})

    async def _observe_task_generation(
        self,
        worker_id: int,
        task_id: int,
    ) -> WorkerTaskGeneration | None:
        async with self.db_factory() as db:
            return await read_worker_task_generation(db, task_id, worker_id)

    async def _observe_or_adopt_event_generation(
        self,
        worker_id: int,
        task_id: int,
        *,
        retry_count: int,
        turn_generation: int,
        worker_turn_handoff_id: str | None = None,
    ) -> WorkerTaskGeneration | None:
        """Resolve an exact event against current or one reserved next turn.

        A bare Worker ``G+1`` is never accepted.  The only widening is the
        durable reservation written before the matching proxy request.  The
        Task row is locked while it is consumed, so relay may safely beat the
        proxy HTTP ACK without opening a global ``+1`` allowance.
        """

        async with self.db_factory() as db:
            task = (
                await db.execute(
                    select(Task)
                    .where(
                        Task.id == task_id,
                        Task.worker_id == worker_id,
                        Task.shared_from_id.is_(None),
                        _worker_task_termination_apply_predicate(),
                    )
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if task is None:
                await db.rollback()
                return None
            observed = worker_task_generation(
                task,
                expected_worker_id=worker_id,
            )
            if observed is None or not _valid_worker_turn_handoff(observed):
                await db.rollback()
                return None
            if (
                retry_count == observed.retry_count
                and turn_generation == observed.turn_generation
            ):
                if (
                    _has_worker_turn_handoff(observed)
                    and turn_generation
                    == observed.worker_turn_handoff_from_generation + 1
                    and worker_turn_handoff_id
                    != observed.worker_turn_handoff_id
                ):
                    await db.rollback()
                    return None
                # Once a follow-up is reserved from an already-terminal G,
                # delayed payload events from G cannot be terminal evidence
                # for the new request.  An active G may still finish normally
                # while one queued follow-up is waiting behind it.
                if (
                    _has_worker_turn_handoff(observed)
                    and observed.turn_generation
                    == observed.worker_turn_handoff_from_generation
                    and observed.status in _TERMINAL_TASK_STATUSES
                ):
                    await db.rollback()
                    return None
                await db.rollback()
                return observed
            if (
                observed.termination_uncertainty_present
                or observed.legacy_carrier_conflict_present
            ):
                await db.rollback()
                return None
            if not _handoff_authorizes_next_turn(
                observed,
                retry_count=retry_count,
                turn_generation=turn_generation,
            ) or worker_turn_handoff_id != observed.worker_turn_handoff_id:
                await db.rollback()
                return None

            # Do not clear the marker here.  This transaction only adopts the
            # generation fence.  A live chat event clears it in the same
            # transaction that persists the Manager LogEntry; status recovery
            # keeps it until exact history is backfilled.  Marking the task
            # active guarantees restart recovery remains subscribed between
            # those two commits.
            # SELECT .. FOR UPDATE is ignored by SQLite.  Adopt through a
            # complete conditional write so a concurrent cancel/retry/migrate
            # that changes any observed ownership field wins instead of being
            # overwritten by this session's stale ORM snapshot.
            adopted = await db.execute(
                update(Task)
                .where(
                    *_worker_task_generation_write_predicates(observed),
                    no_active_test_harness_owner_graph_predicate(),
                )
                .values(
                    turn_generation=turn_generation,
                    turn_source_log_id=None,
                    status="executing",
                    completed_at=None,
                )
            )
            if adopted.rowcount != 1:
                await db.rollback()
                return None
            resulting = await read_worker_task_generation(
                db,
                task_id,
                worker_id,
            )
            if (
                resulting is None
                or resulting.retry_count != retry_count
                or resulting.turn_generation != turn_generation
                or resulting.status != "executing"
            ):
                await db.rollback()
                return None
            await db.commit()
            return resulting

    async def _fetch_task_snapshot(
        self,
        worker: Worker,
        task_id: int,
        incarnation_id: str,
        *,
        client=None,
    ) -> dict | None:
        headers = self._headers(worker)
        headers["X-CCM-Task-Incarnation"] = incarnation_id

        async def fetch(http_client):
            response = await http_client.get(
                self._api(worker, f"/api/tasks/{task_id}"),
                headers=headers,
            )
            if response.status_code != 200:
                return None
            payload = response.json()
            return (
                payload
                if isinstance(payload, dict)
                and payload.get("incarnation_id") == incarnation_id
                else None
            )

        try:
            if client is not None:
                return await fetch(client)
            async with httpx.AsyncClient(timeout=15) as http_client:
                return await fetch(http_client)
        except Exception:
            logger.warning(
                "fetch task %s from worker %s failed",
                task_id,
                worker.id,
            )
            return None

    async def _fetch_worker_turn_handoff_receipt(
        self,
        worker: Worker,
        task_id: int,
        handoff_id: str,
        incarnation_id: str,
        *,
        client=None,
    ) -> dict | None:
        headers = self._headers(worker)
        headers["X-CCM-Task-Incarnation"] = incarnation_id

        async def fetch(http_client):
            response = await http_client.get(
                self._api(
                    worker,
                    f"/api/tasks/{task_id}/worker-turn-handoffs/{handoff_id}",
                ),
                headers=headers,
            )
            if response.status_code == 404:
                return None
            if response.status_code != 200:
                return None
            payload = response.json()
            return (
                payload
                if isinstance(payload, dict)
                and payload.get("incarnation_id") == incarnation_id
                else None
            )

        try:
            if client is not None:
                return await fetch(client)
            async with httpx.AsyncClient(timeout=15) as http_client:
                return await fetch(http_client)
        except Exception:
            logger.warning(
                "fetch Worker turn handoff %s for task %s from worker %s failed",
                handoff_id,
                task_id,
                worker.id,
            )
            return None

    async def _manager_worker_turn_handoff_request(
        self,
        observed: WorkerTaskGeneration,
    ) -> dict | None:
        """Load and verify the Manager's exact replay envelope."""

        if not _valid_worker_turn_handoff(observed) or not _has_worker_turn_handoff(
            observed
        ):
            return None
        async with self.db_factory() as db:
            receipt = await db.get(
                WorkerTurnHandoffReceipt,
                observed.worker_turn_handoff_id,
            )
            source_log = await db.get(
                LogEntry,
                observed.worker_turn_handoff_source_log_id,
            )
            if (
                receipt is None
                or receipt.side != "manager"
                or receipt.task_id != observed.task_id
                or receipt.source_log_id
                != observed.worker_turn_handoff_source_log_id
                or receipt.worker_id != observed.worker_id
                or receipt.retry_count
                != observed.worker_turn_handoff_retry_count
                or receipt.from_generation
                != observed.worker_turn_handoff_from_generation
                or receipt.status not in {"prepared", "acknowledged"}
                or source_log is None
                or source_log.task_id != observed.task_id
                or source_log.event_type != "user_message"
                or not isinstance(receipt.request_payload, dict)
            ):
                return None
            stored_payload = receipt.request_payload
            if stored_payload.get("version") == (
                _MANAGER_HANDOFF_REQUEST_ENVELOPE_VERSION
            ):
                if set(stored_payload) != {
                    "version",
                    "identity",
                    "replay_payload",
                }:
                    return None
                identity = stored_payload.get("identity")
                payload = stored_payload.get("replay_payload")
                if not isinstance(identity, dict) or not isinstance(payload, dict):
                    return None
                try:
                    rebuilt_identity = worker_turn_handoff_request_identity(
                        payload,
                        identity.get("admitted_routing"),
                    )
                except (TypeError, ValueError):
                    return None
                if rebuilt_identity != identity:
                    return None
            else:
                # Compatibility for one in-flight receipt written by a
                # pre-v2 Manager.  Its digest covered the complete HTTP body.
                identity = stored_payload
                payload = stored_payload
            try:
                actual_digest = _handoff_payload_digest(identity)
            except (TypeError, ValueError, UnicodeError):
                return None
            if (
                actual_digest != receipt.request_digest
                or payload.get("worker_turn_handoff_id")
                != observed.worker_turn_handoff_id
                or payload.get("worker_turn_handoff_retry_count")
                != observed.worker_turn_handoff_retry_count
                or payload.get("worker_turn_handoff_from_generation")
                != observed.worker_turn_handoff_from_generation
                or payload.get("worker_turn_handoff_incarnation_id")
                != observed.incarnation_id
            ):
                return None
            principal = canonical_delegated_principal_payload(payload)
            if (
                principal is None
                or task_execution_principal_payload(payload) != principal
                or canonical_delegated_principal_payload(identity) != principal
                or task_execution_principal_payload(identity) != principal
            ):
                return None
            return {
                "payload": dict(payload),
                "request_digest": receipt.request_digest,
                "principal_digest": _handoff_payload_digest(principal),
                "terminal_pr_review_chat": receipt.terminal_pr_review_chat,
            }

    async def _require_worker_delegated_principal_protocol(
        self,
        worker: Worker,
        *,
        client=None,
    ) -> bool:
        """Read-only mixed-version gate used before every recovery mutation."""

        from backend.services.worker_proxy import (
            WORKER_DELEGATED_PRINCIPAL_PROTOCOL,
        )

        async def check(http_client) -> bool:
            response = await http_client.get(
                self._api(worker, "/api/system/config"),
                headers=self._headers(worker),
            )
            if response.status_code != 200:
                return False
            try:
                payload = response.json()
            except Exception:
                return False
            return bool(
                isinstance(payload, dict)
                and payload.get("worker_delegated_principal_protocol")
                == WORKER_DELEGATED_PRINCIPAL_PROTOCOL
                and payload.get("worker_task_incarnation_proxy_version") == 1
            )

        try:
            if client is not None:
                return await check(client)
            async with httpx.AsyncClient(timeout=30) as http_client:
                return await check(http_client)
        except asyncio.CancelledError:
            raise
        except Exception:
            return False

    async def _post_worker_turn_handoff_request(
        self,
        worker: Worker,
        observed: WorkerTaskGeneration,
        replay: dict,
        *,
        client=None,
    ) -> bool:
        try:
            async with self.db_factory() as db:
                fenced = await _acquire_worker_turn_handoff_effect_fence(
                    db,
                    observed,
                )
                if fenced is None:
                    return False
                if not await self._require_worker_delegated_principal_protocol(
                    worker,
                    client=client,
                ):
                    await db.rollback()
                    return False
                headers = self._headers(worker)
                if not observed.incarnation_id:
                    await db.rollback()
                    return False
                headers["X-CCM-Task-Incarnation"] = observed.incarnation_id
                if replay["terminal_pr_review_chat"]:
                    headers[PR_REVIEW_TERMINAL_CHAT_HEADER] = (
                        PR_REVIEW_TERMINAL_CHAT_HEADER_VALUE
                    )

                async def send(http_client):
                    response = await http_client.post(
                        self._api(
                            worker,
                            f"/api/tasks/{observed.task_id}/chat",
                        ),
                        headers=headers,
                        json=replay["payload"],
                    )
                    return 200 <= response.status_code < 300

                if client is not None:
                    accepted = await send(client)
                else:
                    async with httpx.AsyncClient(timeout=60) as http_client:
                        accepted = await send(http_client)
                if accepted:
                    await db.commit()
                else:
                    await db.rollback()
                return accepted
        except Exception:
            logger.warning(
                "replay Worker turn handoff %s for task %s on worker %s failed",
                observed.worker_turn_handoff_id,
                observed.task_id,
                worker.id,
            )
            return False

    @staticmethod
    def _remote_handoff_matches(
        observed: WorkerTaskGeneration,
        receipt: dict,
        replay: dict,
    ) -> bool:
        status = receipt.get("status")
        remote_task_id = receipt.get("task_id")
        remote_retry_count = receipt.get("retry_count")
        remote_from_generation = receipt.get("from_generation")
        turn_generation = receipt.get("turn_generation")
        if status in _WORKER_HANDOFF_BOUND_GENERATION_STATUSES:
            valid_turn = (
                type(turn_generation) is int
                and type(observed.worker_turn_handoff_from_generation) is int
                and turn_generation
                == observed.worker_turn_handoff_from_generation + 1
            )
        elif status in {"accepted", "cancelled"}:
            valid_turn = turn_generation is None
        else:
            valid_turn = False
        return bool(
            receipt.get("handoff_id") == observed.worker_turn_handoff_id
            and type(remote_task_id) is int
            and remote_task_id == observed.task_id
            and receipt.get("incarnation_id") == observed.incarnation_id
            and type(remote_retry_count) is int
            and remote_retry_count == observed.worker_turn_handoff_retry_count
            and type(remote_from_generation) is int
            and remote_from_generation
            == observed.worker_turn_handoff_from_generation
            and valid_turn
            and isinstance(receipt.get("response"), dict)
            and receipt.get("request_digest") == replay.get("request_digest")
            and receipt.get("principal_digest") == replay.get("principal_digest")
        )

    async def _acknowledge_recovered_worker_turn_handoff(
        self,
        observed: WorkerTaskGeneration,
        receipt: dict,
    ) -> bool:
        async with self.db_factory() as db:
            acknowledged = await acknowledge_worker_turn_handoff(
                db,
                observed,
                session_id=receipt["response"].get("session_id"),
            )
            if acknowledged is None:
                await db.rollback()
                return False
            await db.commit()
            return True

    async def _cancel_recovered_worker_turn_handoff(
        self,
        observed: WorkerTaskGeneration,
        receipt: dict,
    ) -> bool:
        """Consume exact remote cancellation and clear the Manager marker."""

        async with self.db_factory() as db:
            fenced = await db.execute(
                update(Task)
                .where(*_worker_task_generation_write_predicates(observed))
                .values(status=Task.status)
            )
            if fenced.rowcount != 1:
                await db.rollback()
                return False
            task = (
                await db.execute(
                    select(Task)
                    .where(*_worker_task_generation_write_predicates(observed))
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if task is None:
                await db.rollback()
                return False
            current = worker_task_generation(
                task,
                expected_worker_id=observed.worker_id,
            )
            if current is None or not (
                await _settle_manager_handoff_receipt(
                    db,
                    current,
                    status="cancelled",
                    reason=str(
                        receipt.get("cancel_reason")
                        or "Worker cancelled the queued follow-up before launch"
                    ),
                )
            ):
                await db.rollback()
                return False
            for field, value in _WORKER_TURN_HANDOFF_CLEAR_VALUES.items():
                setattr(task, field, value)
            await db.commit()
            return True

    async def _resume_worker_turn_handoff(
        self,
        worker: Worker,
        observed: WorkerTaskGeneration,
        replay: dict,
        *,
        client=None,
    ) -> bool:
        if (
            observed.termination_uncertainty_present
            or observed.legacy_carrier_conflict_present
        ):
            return False
        try:
            async with self.db_factory() as db:
                fenced = await _acquire_worker_turn_handoff_effect_fence(
                    db,
                    observed,
                )
                if fenced is None:
                    return False
                if not await self._require_worker_delegated_principal_protocol(
                    worker,
                    client=client,
                ):
                    await db.rollback()
                    return False
                if not observed.incarnation_id:
                    await db.rollback()
                    return False
                headers = self._headers(worker)
                headers["X-CCM-Task-Incarnation"] = observed.incarnation_id
                async def send(http_client):
                    return await http_client.post(
                        self._api(
                            worker,
                            f"/api/tasks/{observed.task_id}/worker-turn-handoffs/"
                            f"{observed.worker_turn_handoff_id}/resume",
                        ),
                        headers=headers,
                    )
                if client is not None:
                    response = await send(client)
                else:
                    async with httpx.AsyncClient(timeout=30) as http_client:
                        response = await send(http_client)
                payload = response.json() if response.status_code == 200 else None
                resumed = bool(
                    isinstance(payload, dict)
                    and self._remote_handoff_matches(observed, payload, replay)
                    # Only accepted/claimed callers invoke this endpoint.  The
                    # response may already be post-boundary because the queue
                    # can advance while the resume request is in flight.
                    and payload.get("status")
                    in (
                        _WORKER_HANDOFF_REPLAYABLE_STATUSES
                        | _WORKER_HANDOFF_POST_BOUNDARY_STATUSES
                    )
                )
                if resumed:
                    await db.commit()
                else:
                    await db.rollback()
                return resumed
        except Exception:
            logger.warning(
                "resume Worker turn handoff %s for task %s on worker %s failed",
                observed.worker_turn_handoff_id,
                observed.task_id,
                worker.id,
            )
            return False

    async def _resume_accepted_worker_turn_handoff(
        self,
        worker: Worker,
        observed: WorkerTaskGeneration,
        *,
        attempts: int = 3,
        client=None,
        operation_lock_held: bool = False,
    ) -> bool:
        """Recover a missing/accepted receipt using the exact durable POST."""

        if (
            not _has_worker_turn_handoff(observed)
            or observed.termination_uncertainty_present
            or observed.legacy_carrier_conflict_present
        ):
            return False
        if not operation_lock_held:
            from backend.services.worker_proxy import get_task_operation_lock

            async with get_task_operation_lock(observed.task_id):
                current = await self._observe_task_generation(
                    observed.worker_id,
                    observed.task_id,
                )
                if (
                    current is None
                    or current.worker_turn_handoff_id
                    != observed.worker_turn_handoff_id
                    or current.termination_uncertainty_present
                    or current.legacy_carrier_conflict_present
                ):
                    return False
                return await self._resume_accepted_worker_turn_handoff(
                    worker,
                    current,
                    attempts=attempts,
                    client=client,
                    operation_lock_held=True,
                )

        async with self.db_factory() as db:
            if await _active_worker_task_termination_exists(
                db,
                observed.task_id,
            ):
                return False
        replay = await self._manager_worker_turn_handoff_request(observed)
        if replay is None:
            return False
        for attempt in range(max(1, attempts)):
            receipt = await self._fetch_worker_turn_handoff_receipt(
                worker,
                observed.task_id,
                observed.worker_turn_handoff_id,
                observed.incarnation_id,
                client=client,
            )
            if receipt is None:
                if not await self._post_worker_turn_handoff_request(
                    worker,
                    observed,
                    replay,
                    client=client,
                ):
                    if attempt + 1 < attempts:
                        await asyncio.sleep(0)
                    continue
                receipt = await self._fetch_worker_turn_handoff_receipt(
                    worker,
                    observed.task_id,
                    observed.worker_turn_handoff_id,
                    observed.incarnation_id,
                    client=client,
                )
            if not isinstance(receipt, dict) or not self._remote_handoff_matches(
                observed,
                receipt,
                replay,
            ):
                return False
            if receipt.get("status") == "cancelled":
                return await self._cancel_recovered_worker_turn_handoff(
                    observed,
                    receipt,
                )
            if (
                receipt.get("status")
                in _WORKER_HANDOFF_POST_BOUNDARY_STATUSES
            ):
                # The exact G+1 crossed the provider boundary.  It is safe to
                # acknowledge and reconcile its events/history/snapshot, but it
                # must never be sent through /resume again.
                return await self._acknowledge_recovered_worker_turn_handoff(
                    observed,
                    receipt,
                )
            if not await self._acknowledge_recovered_worker_turn_handoff(
                observed,
                receipt,
            ):
                return False
            if await self._resume_worker_turn_handoff(
                worker,
                observed,
                replay,
                client=client,
            ):
                return True
            if attempt + 1 < attempts:
                current = await self._observe_task_generation(
                    observed.worker_id,
                    observed.task_id,
                )
                if (
                    current is None
                    or current.worker_turn_handoff_id
                    != observed.worker_turn_handoff_id
                ):
                    return False
                observed = current
                await asyncio.sleep(0)
        return False

    def ensure_worker_turn_handoff_recovery(
        self,
        worker: Worker,
        observed: WorkerTaskGeneration,
    ) -> None:
        """Keep retrying a durable handoff until its Manager marker settles."""

        if (
            self._shutting_down
            or worker.id in self._closing
            or not _has_worker_turn_handoff(observed)
            or observed.termination_uncertainty_present
            or observed.legacy_carrier_conflict_present
        ):
            return
        key = (
            worker.id,
            observed.task_id,
            observed.worker_turn_handoff_id,
        )
        existing = self._handoff_recovery_tasks.get(key)
        if existing is not None and not existing.done():
            return
        task = asyncio.create_task(
            self._worker_turn_handoff_recovery_loop(*key)
        )
        self._handoff_recovery_tasks[key] = task

        def cleanup(done: asyncio.Task) -> None:
            if self._handoff_recovery_tasks.get(key) is done:
                self._handoff_recovery_tasks.pop(key, None)
            if done.cancelled():
                return
            try:
                error = done.exception()
            except asyncio.CancelledError:
                return
            if error is not None:
                logger.error(
                    "Worker turn handoff recovery task failed for task %s",
                    observed.task_id,
                    exc_info=(type(error), error, error.__traceback__),
                )

        task.add_done_callback(cleanup)

    async def _worker_turn_handoff_recovery_loop(
        self,
        worker_id: int,
        task_id: int,
        handoff_id: str,
    ) -> None:
        from backend.services.worker_proxy import get_task_operation_lock

        delay = WORKER_HANDOFF_RECOVERY_BASE_DELAY
        try:
            while (
                not self._shutting_down
                and worker_id not in self._closing
            ):
                try:
                    deferred_completions: list[
                        WorkerTaskGeneration
                    ] = []
                    settled = False
                    async with get_task_operation_lock(task_id):
                        if (
                            self._shutting_down
                            or worker_id in self._closing
                        ):
                            return
                        observed = await self._observe_task_generation(
                            worker_id,
                            task_id,
                        )
                        if (
                            observed is None
                            or observed.worker_turn_handoff_id != handoff_id
                        ):
                            return
                        async with self.db_factory() as db:
                            if await _active_worker_task_termination_exists(
                                db,
                                task_id,
                            ):
                                return
                        async with self.db_factory() as db:
                            worker = await db.get(Worker, worker_id)
                        if worker is not None and worker.status == "ready":
                            recovered = await (
                                self._resume_accepted_worker_turn_handoff(
                                    worker,
                                    observed,
                                    attempts=1,
                                    operation_lock_held=True,
                                )
                            )
                            if recovered:
                                await (
                                    self._backfill_missing_logs_with_operation_lock(
                                        worker,
                                        {task_id},
                                        deferred_completions=(
                                            deferred_completions
                                        ),
                                    )
                                )
                                current = await self._observe_task_generation(
                                    worker_id,
                                    task_id,
                                )
                                if (
                                    current is None
                                    or current.worker_turn_handoff_id
                                    != handoff_id
                                ):
                                    settled = True
                    # Completion itself takes the same Task operation lock in
                    # Dispatcher.  Mirror normal backfill and notify only
                    # after releasing the recovery iteration's fence.
                    for generation in deferred_completions:
                        await self._notify_completed_pr_review(generation)
                    if settled:
                        return
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "Worker turn handoff recovery iteration failed for "
                        "task %s",
                        task_id,
                    )
                await asyncio.sleep(delay)
                delay = min(
                    max(delay * 2, WORKER_HANDOFF_RECOVERY_BASE_DELAY),
                    WORKER_HANDOFF_RECOVERY_MAX_DELAY,
                )
        except asyncio.CancelledError:
            raise

    @staticmethod
    def _launched_handoff_proves_generation(
        observed: WorkerTaskGeneration,
        receipt: dict | None,
        *,
        retry_count: int,
        turn_generation: int,
        request_digest: str,
        principal_digest: str,
    ) -> bool:
        return bool(
            _valid_worker_turn_handoff(observed)
            and _has_worker_turn_handoff(observed)
            and isinstance(receipt, dict)
            and receipt.get("handoff_id")
            == observed.worker_turn_handoff_id
            and receipt.get("task_id") == observed.task_id
            and receipt.get("incarnation_id") == observed.incarnation_id
            # ``launching`` already crossed the durable provider-side-effect
            # boundary.  It therefore proves the same exact G+1 identity as
            # ``launched`` for relay adoption, even though only the latter says
            # InstanceManager returned successfully.
            and receipt.get("status")
            in _WORKER_HANDOFF_POST_BOUNDARY_STATUSES
            and receipt.get("retry_count")
            == observed.worker_turn_handoff_retry_count
            and receipt.get("from_generation")
            == observed.worker_turn_handoff_from_generation
            and receipt.get("turn_generation") == turn_generation
            and receipt.get("request_digest") == request_digest
            and receipt.get("principal_digest") == principal_digest
            and retry_count
            == observed.worker_turn_handoff_retry_count
            and turn_generation
            == observed.worker_turn_handoff_from_generation + 1
        )

    async def _launched_handoff_id_for_generation(
        self,
        worker: Worker,
        observed: WorkerTaskGeneration,
        *,
        retry_count: int,
        turn_generation: int,
        client=None,
    ) -> str | None:
        if not _handoff_authorizes_next_turn(
            observed,
            retry_count=retry_count,
            turn_generation=turn_generation,
        ) and not (
            _valid_worker_turn_handoff(observed)
            and _has_worker_turn_handoff(observed)
            and retry_count == observed.worker_turn_handoff_retry_count
            and turn_generation
            == observed.worker_turn_handoff_from_generation + 1
            and observed.turn_generation == turn_generation
        ):
            return None
        receipt = await self._fetch_worker_turn_handoff_receipt(
            worker,
            observed.task_id,
            observed.worker_turn_handoff_id,
            observed.incarnation_id,
            client=client,
        )
        replay = await self._manager_worker_turn_handoff_request(observed)
        if replay is None:
            return None
        if self._launched_handoff_proves_generation(
            observed,
            receipt,
            retry_count=retry_count,
            turn_generation=turn_generation,
            request_digest=replay["request_digest"],
            principal_digest=replay["principal_digest"],
        ):
            return observed.worker_turn_handoff_id
        return None

    async def _launched_handoff_id_for_snapshot(
        self,
        worker: Worker,
        observed: WorkerTaskGeneration,
        remote_task: dict,
        *,
        client=None,
    ) -> str | None:
        values = authoritative_worker_task_values(
            remote_task,
            task_id=observed.task_id,
            incarnation_id=observed.incarnation_id,
        )
        if values is None:
            return None
        if not _handoff_authorizes_next_turn(
            observed,
            retry_count=values["retry_count"],
            turn_generation=values["turn_generation"],
        ):
            return None
        return await self._launched_handoff_id_for_generation(
            worker,
            observed,
            retry_count=values["retry_count"],
            turn_generation=values["turn_generation"],
            client=client,
        )

    async def _publish_status_generation(
        self,
        generation: WorkerTaskGeneration,
        payload: dict | None = None,
        *,
        notify_completion: bool = True,
    ) -> bool:
        """Publish while holding a no-op write lock on the exact result row."""

        async with self.db_factory() as db:
            guarded = await db.execute(
                update(Task)
                .where(*_worker_task_generation_write_predicates(generation))
                .values(status=generation.status)
            )
            if guarded.rowcount != 1:
                await db.rollback()
                return False
            event = {
                "event": "status_change",
                "task_id": generation.task_id,
                "new_status": generation.status,
            }
            if payload:
                event.update(
                    {
                        key: value
                        for key, value in payload.items()
                        if key not in (
                            "instance_id",
                            "worker_id",
                            "pty_background_generation",
                        )
                    }
                )
                event["event"] = "status_change"
                event["task_id"] = generation.task_id
                event["new_status"] = generation.status
            # The just-committed authoritative snapshot wins over a possibly
            # stale/spoofed boolean carried by the status event itself.
            event["background_active"] = (
                generation.pty_background_generation is not None
            )
            try:
                await self.broadcaster.broadcast("tasks", event)
            except Exception:
                logger.exception(
                    "failed to publish Worker status for task %s",
                    generation.task_id,
                )
            await db.commit()
        if notify_completion:
            await self._notify_completed_pr_review(generation)
        return True

    async def _notify_completed_pr_review(
        self,
        generation: WorkerTaskGeneration,
    ) -> None:
        """Consume a Manager-owned PR workflow's exact Worker terminal state.

        Worker TaskCreate intentionally does not receive Manager metadata such
        as ``pr_review_id`` or ``pr_finding_action_id``, so the Worker-side
        Dispatcher cannot finalize the PRReview/fix action. The Manager must
        do it after the authoritative status relay has committed and only when
        no remote PTY background epoch remains. Successful generations also
        require a complete history backfill before patch parsing.
        """

        if (
            generation.status not in _TERMINAL_TASK_STATUSES
            or generation.pty_background_generation is not None
        ):
            return
        try:
            async with self.db_factory() as db:
                task = (
                    await db.execute(
                        select(Task).where(
                            *worker_task_generation_predicates(generation)
                        )
                    )
                ).scalar_one_or_none()
                worker = await db.get(Worker, generation.worker_id)
                if task is not None:
                    db.expunge(task)
            if task is None or worker is None:
                return
            fix_task = is_pr_review_fix_task(task)
            review_task = is_pr_review_task(task)
            if not fix_task and not review_task:
                return

            if generation.status != "completed":
                # Ordinary PR-review failure semantics remain owned by the
                # existing Manager/Worker recovery flow. A fix action has no
                # such fallback: every unsuccessful terminal generation must
                # settle its durable ``running`` action.
                if not fix_task:
                    return
                confirmed = await self._observe_task_generation(
                    generation.worker_id,
                    generation.task_id,
                )
                if confirmed != generation:
                    logger.info(
                        "discarding Worker PR fix failure for stale "
                        "generation of task %s",
                        generation.task_id,
                    )
                    return
                from backend.main import dispatcher

                if dispatcher is not None:
                    error = task.error_message or (
                        "PR fix Task ended with terminal status "
                        f"{generation.status}"
                    )
                    await dispatcher._handle_pr_review_failure(task, error)
                return

            # A Worker status event may overtake a disconnected task-channel
            # tail. Pull the authoritative history first, but explicitly skip
            # status synchronization here: publishing that status would call
            # this completion hook recursively.
            synced = await self._backfill_missing_logs(
                worker,
                {generation.task_id},
                sync_status=False,
            )
            if generation.task_id not in synced:
                logger.warning(
                    "deferring Worker PR review completion for task %s "
                    "because exact-generation history could not be synced",
                    generation.task_id,
                )
                return

            # The history request and DB insert are asynchronous boundaries.
            # Retry/reassignment/background handoff may have won meanwhile, so
            # the dispatcher callback must borrow no newer generation.
            confirmed = await self._observe_task_generation(
                generation.worker_id,
                generation.task_id,
            )
            if confirmed != generation:
                logger.info(
                    "discarding Worker PR review completion for stale "
                    "generation of task %s",
                    generation.task_id,
                )
                return

            from backend.main import dispatcher

            if dispatcher is not None:
                await dispatcher._handle_pty_background_completion(
                    generation.task_id
                )
        except Exception:
            logger.exception(
                "failed to finalize Worker PR workflow for task %s",
                generation.task_id,
            )

    async def _publish_background_generation(
        self,
        generation: WorkerTaskGeneration,
        *,
        channels: tuple[str, ...],
        notify_completion: bool = True,
    ) -> bool:
        """Publish a controlled background marker for one exact mirror.

        The no-op update is a second CAS fence between the authoritative GET
        commit and WebSocket publication.  A retry, reassignment, or newer
        marker transition therefore suppresses the stale event.
        """

        valid_channels = {
            "tasks",
            f"task:{generation.task_id}",
        }
        selected_channels = tuple(
            dict.fromkeys(
                channel
                for channel in channels
                if channel in valid_channels
            )
        )
        if not selected_channels:
            return False
        async with self.db_factory() as db:
            guarded = await db.execute(
                update(Task)
                .where(*_worker_task_generation_write_predicates(generation))
                .values(
                    pty_background_generation=(
                        generation.pty_background_generation
                    )
                )
            )
            if guarded.rowcount != 1:
                await db.rollback()
                return False
            event = {
                "event": "background_activity",
                "event_type": "background_activity",
                "task_id": generation.task_id,
                "background_active": (
                    generation.pty_background_generation is not None
                ),
            }
            try:
                for selected_channel in selected_channels:
                    await self.broadcaster.broadcast(
                        selected_channel,
                        event,
                    )
            except Exception:
                logger.exception(
                    "failed to publish Worker background marker for task %s",
                    generation.task_id,
                )
            await db.commit()
        if notify_completion:
            await self._notify_completed_pr_review(generation)
        return True

    @staticmethod
    def _handoff_launch_principal(
        receipt: WorkerTurnHandoffReceipt,
    ) -> dict[str, object] | None:
        """Recover the exact delegated principal from a Manager handoff."""

        stored = receipt.request_payload
        if not isinstance(stored, dict):
            return None
        if stored.get("version") == _MANAGER_HANDOFF_REQUEST_ENVELOPE_VERSION:
            identity = stored.get("identity")
            replay = stored.get("replay_payload")
            if not isinstance(identity, dict) or not isinstance(replay, dict):
                return None
            try:
                if (
                    _handoff_payload_digest(identity) != receipt.request_digest
                    or worker_turn_handoff_request_identity(
                        replay,
                        identity.get("admitted_routing"),
                    )
                    != identity
                ):
                    return None
            except (TypeError, ValueError, UnicodeError):
                return None
            replay_principal = canonical_delegated_principal_payload(replay)
            identity_principal = canonical_delegated_principal_payload(identity)
            return (
                replay_principal
                if replay_principal is not None
                and replay_principal == identity_principal
                else None
            )
        try:
            if _handoff_payload_digest(stored) != receipt.request_digest:
                return None
        except (TypeError, ValueError, UnicodeError):
            return None
        return canonical_delegated_principal_payload(stored)

    @staticmethod
    def _manual_retry_launch_authorized(
        task: Task,
        observed: WorkerTaskGeneration,
        request: WorkerLaunchAdmissionRequest,
        target_manager_principal: dict[str, object],
    ) -> bool:
        """Authorize a Worker dequeue which raced Manager retry adoption."""

        marker = worker_manual_retry_receipt(task.metadata_)
        if (
            marker is None
            or marker.get("version") != WORKER_MANUAL_RETRY_PROTOCOL
            or marker.get("side") != "manager"
            or marker.get("worker_id") != observed.worker_id
            or marker.get("target_principal") != request.principal
            or marker.get("target_manager_principal")
            != target_manager_principal
            or marker.get("target_principal_digest")
            != request.principal_digest
            or not isinstance(marker.get("request"), dict)
            or marker.get("request_digest")
            != worker_manual_retry_request_digest(marker["request"])
        ):
            return False
        source = marker.get("source_generation")
        if not isinstance(source, dict):
            return False
        if marker.get("state") == "prepared":
            current_source = worker_manual_retry_source_generation(task, observed)
            return bool(
                current_source is not None
                and source == current_source
                and request.retry_count == source.get("retry_count") + 1
                and request.turn_generation
                == source.get("turn_generation") + 1
            )
        if marker.get("state") != "acknowledged":
            return False
        worker_receipt = marker.get("worker_receipt")
        operation_id = marker.get("operation_id")
        request_digest = marker.get("request_digest")
        if not (
            isinstance(operation_id, str)
            and isinstance(request_digest, str)
            and _manual_retry_receipt_matches(
                worker_receipt,
                operation_id=operation_id,
                request_digest=request_digest,
                source_generation=source,
                target_principal=request.principal,
            )
        ):
            return False
        result = worker_receipt.get("result_generation")
        return bool(
            isinstance(result, dict)
            and task.status == "pending"
            and task_execution_principal_payload(task)
            == target_manager_principal
            and task.retry_count == result.get("retry_count")
            and task.turn_generation == result.get("turn_generation")
            and request.retry_count == task.retry_count
            and request.turn_generation == task.turn_generation + 1
        )

    @staticmethod
    def _context_retry_marker_for_request(
        request: WorkerLaunchAdmissionRequest,
        *,
        worker_id: int,
        rejected_actual_transport: str,
    ) -> dict[str, object] | None:
        authority = request.context_retry
        if authority is None:
            return None
        return {
            "version": WORKER_CONTEXT_RETRY_MARKER_VERSION,
            "worker_id": worker_id,
            "incarnation_id": request.incarnation_id,
            "retry_count": request.retry_count,
            "from_generation": authority.from_generation,
            "turn_generation": request.turn_generation,
            "authority_id": authority.authority_id,
            "rejected_source_log_id": authority.source_log_id,
            "claimed_source_log_id": authority.claimed_source_log_id,
            "rejected_actual_transport": rejected_actual_transport,
            "actual_transport": request.actual_transport,
            "principal_digest": request.principal_digest,
        }

    @staticmethod
    def _exact_launch_marker_for_request(
        request: WorkerLaunchAdmissionRequest,
        *,
        worker_id: int,
    ) -> dict[str, object]:
        """Stable authority for one Manager-admitted Worker generation."""

        return {
            "version": WORKER_EXACT_LAUNCH_MARKER_VERSION,
            "worker_id": worker_id,
            "incarnation_id": request.incarnation_id,
            "retry_count": request.retry_count,
            "turn_generation": request.turn_generation,
            "principal_digest": request.principal_digest,
            "actual_transport": request.actual_transport,
        }

    @staticmethod
    def _exact_launch_marker_matches_request(
        marker: object,
        request: WorkerLaunchAdmissionRequest,
        *,
        worker_id: int,
    ) -> bool:
        expected = WorkerRelay._exact_launch_marker_for_request(
            request,
            worker_id=worker_id,
        )
        return bool(
            isinstance(marker, dict)
            and set(marker) == set(expected)
            and marker == expected
        )

    @staticmethod
    def _exact_launch_marker_is_immediate_predecessor(
        marker: object,
        request: WorkerLaunchAdmissionRequest,
        *,
        worker_id: int,
    ) -> bool:
        """Accept only the immediately preceding admitted generation."""

        expected_keys = set(
            WorkerRelay._exact_launch_marker_for_request(
                request,
                worker_id=worker_id,
            )
        )
        if not isinstance(marker, dict) or set(marker) != expected_keys:
            return False
        marker_retry = marker.get("retry_count")
        marker_generation = marker.get("turn_generation")
        return bool(
            marker.get("version") == WORKER_EXACT_LAUNCH_MARKER_VERSION
            and marker.get("worker_id") == worker_id
            and marker.get("incarnation_id") == request.incarnation_id
            and type(marker_retry) is int
            and type(marker_generation) is int
            and marker_retry >= 0
            and marker_generation >= 1
            and marker_generation + 1 == request.turn_generation
            and request.retry_count in {marker_retry, marker_retry + 1}
            and isinstance(marker.get("principal_digest"), str)
            and len(marker["principal_digest"]) == 64
            and all(
                char in "0123456789abcdef"
                for char in marker["principal_digest"]
            )
            and marker.get("actual_transport")
            in {
                "claude_pty",
                "claude_exec",
                "codex_app_server",
                "codex_exec",
            }
        )

    @staticmethod
    def _context_retry_marker_matches_request(
        marker: object,
        request: WorkerLaunchAdmissionRequest,
        *,
        worker_id: int,
    ) -> bool:
        authority = request.context_retry
        if authority is None or not isinstance(marker, dict):
            return False
        expected = WorkerRelay._context_retry_marker_for_request(
            request,
            worker_id=worker_id,
            rejected_actual_transport=marker.get(
                "rejected_actual_transport"
            ),
        )
        return bool(
            isinstance(expected, dict)
            and set(marker) == set(expected)
            and marker == expected
            and marker.get("rejected_actual_transport")
            in {"codex_app_server", "codex_exec"}
        )

    async def _manager_context_retry_preflight_proof(
        self,
        db,
        *,
        task: Task,
        observed: WorkerTaskGeneration,
        request: WorkerLaunchAdmissionRequest,
        worker: Worker,
    ) -> str | None:
        """Re-prove exact structured overflow from Manager-persisted events."""

        authority = request.context_retry
        if authority is None:
            return None
        rows = list(
            (
                await db.execute(
                    select(LogEntry)
                    .where(
                        LogEntry.task_id == task.id,
                        LogEntry.task_retry_count == authority.retry_count,
                        LogEntry.task_turn_generation
                        == authority.from_generation,
                        LogEntry.turn_scope == "foreground",
                    )
                    .order_by(LogEntry.id)
                )
            )
            .scalars()
            .all()
        )
        if not rows:
            return None
        proofs = [
            parse_codex_context_preflight_relay_proof(row.raw_json)
            for row in rows
        ]
        if any(proof is None for proof in proofs):
            return None
        typed_proofs = [proof for proof in proofs if proof is not None]
        common_matches = all(
            proof["retry_count"] == authority.retry_count
            and proof["turn_generation"] == authority.from_generation
            and proof["source_log_id"] == authority.source_log_id
            for proof in typed_proofs
        )
        transports = {
            proof["actual_transport"] for proof in typed_proofs
        }
        if not common_matches or len(transports) != 1:
            return None
        proof_transport = next(iter(transports))

        terminal = rows[-1]
        terminal_proof = typed_proofs[-1]
        content = terminal.content
        if not (
            terminal.event_type == "system_event"
            and terminal.role is None
            and terminal.is_error is True
            and terminal_proof["raw_type"] == "turn.failed"
            and terminal_proof.get("codex_error_info")
            == "ContextWindowExceeded"
            and isinstance(content, str)
            and terminal_proof.get("message_sha256")
            == hashlib.sha256(content.encode("utf-8")).hexdigest()
        ):
            return None
        seen_start_types: set[str] = set()
        for row, proof in zip(
            rows[:-1], typed_proofs[:-1], strict=True
        ):
            raw_type = proof["raw_type"]
            if not (
                row.event_type == "system_event"
                and row.is_error is False
                and raw_type in {"thread.started", "turn.started"}
                and raw_type not in seen_start_types
            ):
                return None
            seen_start_types.add(raw_type)

        metadata = task.metadata_ if isinstance(task.metadata_, dict) else {}
        marker = metadata.get(WORKER_CONTEXT_RETRY_MARKER_METADATA_KEY)
        current_request_marker = (
            self._context_retry_marker_matches_request(
                marker,
                request,
                worker_id=worker.id,
            )
            and marker.get("rejected_actual_transport")
            == proof_transport
        )
        predecessor_marker = bool(
            isinstance(marker, dict)
            and marker.get("version")
            == WORKER_CONTEXT_RETRY_MARKER_VERSION
            and marker.get("worker_id") == worker.id
            and marker.get("incarnation_id") == request.incarnation_id
            and marker.get("retry_count") == authority.retry_count
            and marker.get("turn_generation")
            == authority.from_generation
            and marker.get("claimed_source_log_id")
            == authority.source_log_id
            and marker.get("actual_transport") == proof_transport
            and marker.get("principal_digest")
            == request.principal_digest
        )
        exact_launch_marker = metadata.get(
            WORKER_EXACT_LAUNCH_MARKER_METADATA_KEY
        )
        exact_launch_lineage = bool(
            isinstance(exact_launch_marker, dict)
            and exact_launch_marker
            == {
                "version": WORKER_EXACT_LAUNCH_MARKER_VERSION,
                "worker_id": worker.id,
                "incarnation_id": request.incarnation_id,
                "retry_count": authority.retry_count,
                "turn_generation": authority.from_generation,
                "principal_digest": request.principal_digest,
                "actual_transport": proof_transport,
            }
        )
        handoff_lineage = False
        if authority.from_generation >= 1:
            receipts = list(
                (
                    await db.execute(
                        select(WorkerTurnHandoffReceipt)
                        .where(
                            WorkerTurnHandoffReceipt.task_id == task.id,
                            WorkerTurnHandoffReceipt.side == "manager",
                            WorkerTurnHandoffReceipt.worker_id == worker.id,
                            WorkerTurnHandoffReceipt.retry_count
                            == authority.retry_count,
                            WorkerTurnHandoffReceipt.from_generation
                            == authority.from_generation - 1,
                            WorkerTurnHandoffReceipt.status == "completed",
                        )
                        .with_for_update()
                    )
                )
                .scalars()
                .all()
            )
            if len(receipts) == 1:
                source = await db.get(
                    LogEntry, receipts[0].source_log_id
                )
                handoff_lineage = bool(
                    source is not None
                    and source.task_id == task.id
                    and source.event_type == "user_message"
                    and self._handoff_launch_principal(receipts[0])
                    == request.principal
                )
        if not (
            current_request_marker
            or predecessor_marker
            or exact_launch_lineage
            or handoff_lineage
        ):
            return None
        return str(proof_transport)

    async def _authorize_worker_launch_admission(
        self,
        request: WorkerLaunchAdmissionRequest,
        worker: Worker,
    ) -> tuple[bool, str]:
        """Revalidate one delegated principal in the authoritative Manager DB."""

        if settings.ccm_node_role != "manager":
            return False, "not_manager"
        target_manager_principal = canonical_manager_principal_from_delegated(
            request.principal
        )
        if target_manager_principal is None:
            return False, "invalid_principal"

        from backend.models.user import User

        async with self.db_factory() as db:
            if not isinstance(worker.auth_token, str) or not worker.auth_token:
                await db.rollback()
                return False, "worker_not_ready"

            # The no-op write is the portable Task writer fence.  The request
            # is already inside the Manager's per-Task operation lock, while
            # this predicate also orders cross-process termination admission.
            # Keep the database lock order aligned with Manager chat
            # admission: Project/Task first, then User.  Taking User first here
            # would invert that order and can deadlock on PostgreSQL/MySQL when
            # a follow-up and the Worker's final launch admission race.
            fenced = await db.execute(
                update(Task)
                .where(
                    Task.id == request.task_id,
                    Task.worker_id == worker.id,
                    Task.shared_from_id.is_(None),
                    Task.incarnation_id == request.incarnation_id,
                    _worker_task_termination_apply_predicate(),
                )
                .values(status=Task.status)
            )
            if fenced.rowcount != 1:
                await db.rollback()
                return False, "generation_changed"
            task = (
                await db.execute(
                    select(Task)
                    .where(
                        Task.id == request.task_id,
                        Task.worker_id == worker.id,
                        Task.shared_from_id.is_(None),
                        Task.incarnation_id == request.incarnation_id,
                        _worker_task_termination_apply_predicate(),
                    )
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            if task is None:
                await db.rollback()
                return False, "generation_changed"

            # Global cross-process lifecycle order is Task -> Worker.  Lock the
            # authoritative Worker row only after the Task writer fence and
            # recheck both readiness and the exact relay credential here.  On
            # PostgreSQL/MySQL SELECT FOR UPDATE serializes with stop/destroy;
            # on SQLite the preceding no-op Task UPDATE already owns the
            # database write transaction.  Avoiding a no-op Worker UPDATE is
            # intentional: its ORM onupdate would advance ``updated_at`` and
            # invalidate an otherwise exact destroy lifecycle claim.
            current_worker = (
                await db.execute(
                    select(Worker)
                    .where(
                        Worker.id == worker.id,
                        Worker.status == "ready",
                        Worker.bootstrap_step.is_(None),
                        Worker.auth_token == worker.auth_token,
                    )
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            if current_worker is None:
                await db.rollback()
                return False, "worker_not_ready"

            observed = worker_task_generation(
                task,
                expected_worker_id=worker.id,
            )
            if observed is None or not _valid_worker_turn_handoff(observed):
                await db.rollback()
                return False, "generation_changed"

            # The transport is selected by the Worker's local runtime, but
            # the authoritative Manager still owns the Task's provider.  A
            # mixed-version or corrupted Worker must not borrow a valid Task
            # generation to obtain a permit for a different provider.  Fast
            # Codex has no exec fallback because that route cannot prove the
            # requested priority tier before model input.
            provider = str(task.provider or "claude").strip().lower()
            allowed_transports = {
                "claude": {"claude_pty", "claude_exec"},
                "codex": {"codex_app_server", "codex_exec"},
            }.get(provider)
            if (
                allowed_transports is None
                or request.actual_transport not in allowed_transports
                or (
                    provider == "codex"
                    and str(task.codex_service_tier or "default")
                    .strip()
                    .lower()
                    == "priority"
                    and request.actual_transport != "codex_app_server"
                )
            ):
                await db.rollback()
                return False, "transport_changed"

            if request.principal["execution_principal_kind"] == "delegated_user":
                principal_gate = await db.execute(
                    update(User)
                    .where(
                        User.id == request.principal["execution_user_id"],
                        User.is_active.is_(True),
                        User.role
                        == request.principal["execution_user_role"],
                    )
                    .values(role=User.role)
                )
                if principal_gate.rowcount != 1:
                    await db.rollback()
                    return False, "principal_revoked"

            if request.context_retry is not None:
                retry_authority = request.context_retry
                rejected_transport = (
                    await self._manager_context_retry_preflight_proof(
                        db,
                        task=task,
                        observed=observed,
                        request=request,
                        worker=current_worker,
                    )
                )
                if rejected_transport is None:
                    await db.rollback()
                    return False, "context_preflight_unproven"
                next_marker = self._context_retry_marker_for_request(
                    request,
                    worker_id=current_worker.id,
                    rejected_actual_transport=rejected_transport,
                )
                if next_marker is None:
                    await db.rollback()
                    return False, "context_preflight_unproven"
                principal_current = (
                    task_execution_principal_payload(task)
                    == target_manager_principal
                )
                already_advanced = bool(
                    principal_current
                    and task.status in {"in_progress", "executing"}
                    and task.retry_count == request.retry_count
                    and task.turn_generation == request.turn_generation
                )
                existing_context_marker = (
                    task.metadata_.get(
                        WORKER_CONTEXT_RETRY_MARKER_METADATA_KEY
                    )
                    if isinstance(task.metadata_, dict)
                    else None
                )
                existing_exact_launch_marker = (
                    task.metadata_.get(
                        WORKER_EXACT_LAUNCH_MARKER_METADATA_KEY
                    )
                    if isinstance(task.metadata_, dict)
                    else None
                )
                if already_advanced and not (
                    self._context_retry_marker_matches_request(
                        existing_context_marker,
                        request,
                        worker_id=current_worker.id,
                    )
                    and existing_context_marker.get(
                        "rejected_actual_transport"
                    )
                    == rejected_transport
                    and self._exact_launch_marker_matches_request(
                        existing_exact_launch_marker,
                        request,
                        worker_id=current_worker.id,
                    )
                ):
                    await db.rollback()
                    return False, "generation_changed"
                if not already_advanced:
                    if not (
                        principal_current
                        and task.status == "failed"
                        and task.retry_count == retry_authority.retry_count
                        and task.retry_count == request.retry_count
                        and task.turn_generation
                        == retry_authority.from_generation
                        and request.turn_generation
                        == retry_authority.from_generation + 1
                    ):
                        await db.rollback()
                        return False, "generation_changed"
                    # This is a fresh authority, independent from the already
                    # consumed ordinary-chat handoff.  Advance only the exact
                    # failed G and leave that old receipt untouched.
                    next_metadata = dict(task.metadata_ or {})
                    next_metadata[
                        WORKER_CONTEXT_RETRY_MARKER_METADATA_KEY
                    ] = next_marker
                    next_metadata[
                        WORKER_EXACT_LAUNCH_MARKER_METADATA_KEY
                    ] = self._exact_launch_marker_for_request(
                        request,
                        worker_id=current_worker.id,
                    )
                    advanced = await db.execute(
                        update(Task)
                        .where(
                            Task.id == task.id,
                            Task.worker_id == worker.id,
                            Task.shared_from_id.is_(None),
                            Task.incarnation_id == request.incarnation_id,
                            Task.status == "failed",
                            Task.retry_count == retry_authority.retry_count,
                            Task.turn_generation
                            == retry_authority.from_generation,
                            Task.execution_user_id
                            == target_manager_principal[
                                "execution_user_id"
                            ],
                            Task.execution_user_role
                            == target_manager_principal[
                                "execution_user_role"
                            ],
                            Task.execution_mode
                            == target_manager_principal["execution_mode"],
                            Task.execution_principal_kind
                            == target_manager_principal[
                                "execution_principal_kind"
                            ],
                            _worker_task_termination_apply_predicate(),
                        )
                        .values(
                            status="executing",
                            turn_generation=request.turn_generation,
                            completed_at=None,
                            error_message=None,
                            session_id=None,
                            turn_source_log_id=None,
                            metadata_=next_metadata,
                        )
                    )
                    if advanced.rowcount != 1:
                        await db.rollback()
                        return False, "generation_changed"
                await db.commit()
                return True, "admitted"

            exact_current = bool(
                task.status in {"in_progress", "executing"}
                and task.retry_count == request.retry_count
                and task.turn_generation == request.turn_generation
                and task_execution_principal_payload(task)
                == target_manager_principal
            )
            handoff_current = False
            if _handoff_authorizes_next_turn(
                observed,
                retry_count=request.retry_count,
                turn_generation=request.turn_generation,
            ):
                receipt = (
                    await db.execute(
                        select(WorkerTurnHandoffReceipt)
                        .where(
                            WorkerTurnHandoffReceipt.handoff_id
                            == observed.worker_turn_handoff_id,
                            WorkerTurnHandoffReceipt.task_id == task.id,
                            WorkerTurnHandoffReceipt.source_log_id
                            == observed.worker_turn_handoff_source_log_id,
                            WorkerTurnHandoffReceipt.side == "manager",
                            WorkerTurnHandoffReceipt.worker_id == worker.id,
                            WorkerTurnHandoffReceipt.retry_count
                            == observed.worker_turn_handoff_retry_count,
                            WorkerTurnHandoffReceipt.from_generation
                            == observed.worker_turn_handoff_from_generation,
                            WorkerTurnHandoffReceipt.status.in_(
                                ("prepared", "acknowledged")
                            ),
                        )
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                handoff_current = bool(
                    receipt is not None
                    and self._handoff_launch_principal(receipt)
                    == request.principal
                )

            manual_retry_current = self._manual_retry_launch_authorized(
                task,
                observed,
                request,
                target_manager_principal,
            )
            if not (
                exact_current
                or handoff_current
                or manual_retry_current
            ):
                await db.rollback()
                return False, "generation_changed"

            # Commit the precise launch identity before the Worker may cross
            # the provider boundary.  Repeating an identical generation after
            # a lost response is safe; changing its transport or principal is
            # not.  The same marker later proves the predecessor generation
            # for a structured Codex context-window retry.
            metadata = task.metadata_ if isinstance(task.metadata_, dict) else {}
            existing_exact_launch_marker = metadata.get(
                WORKER_EXACT_LAUNCH_MARKER_METADATA_KEY
            )
            marker_matches = self._exact_launch_marker_matches_request(
                existing_exact_launch_marker,
                request,
                worker_id=current_worker.id,
            )
            marker_advances = (
                self._exact_launch_marker_is_immediate_predecessor(
                    existing_exact_launch_marker,
                    request,
                    worker_id=current_worker.id,
                )
            )
            if existing_exact_launch_marker is not None and not (
                marker_matches or marker_advances
            ):
                await db.rollback()
                return False, "generation_changed"
            if not marker_matches:
                next_metadata = dict(metadata)
                next_metadata[
                    WORKER_EXACT_LAUNCH_MARKER_METADATA_KEY
                ] = self._exact_launch_marker_for_request(
                    request,
                    worker_id=current_worker.id,
                )
                marked = await db.execute(
                    update(Task)
                    .where(
                        Task.id == task.id,
                        Task.worker_id == worker.id,
                        Task.shared_from_id.is_(None),
                        Task.incarnation_id == request.incarnation_id,
                        Task.status == task.status,
                        Task.retry_count == task.retry_count,
                        Task.turn_generation == task.turn_generation,
                        _worker_task_termination_apply_predicate(),
                    )
                    .values(metadata_=next_metadata)
                )
                if marked.rowcount != 1:
                    await db.rollback()
                    return False, "generation_changed"
            await db.commit()
            return True, "admitted"

    async def _send_worker_launch_admission_response(
        self,
        *,
        ws,
        worker: Worker,
        request: WorkerLaunchAdmissionRequest,
        admitted: bool,
        reason_code: str,
    ) -> None:
        """Send only on the exact authenticated relay socket that requested."""

        if not isinstance(worker.auth_token, str) or not worker.auth_token:
            return
        response = build_worker_launch_admission_response(
            request,
            worker_id=worker.id,
            admitted=admitted,
            reason_code=reason_code,
            control_token=worker.auth_token,
        )
        async with self._connection_lock(worker.id):
            if (
                self._shutting_down
                or worker.id in self._closing
                or self._ws.get(worker.id) is not ws
            ):
                return
            await ws.send(json.dumps(response))

    # ------------------------------------------------------------------
    # 事件中继主循环
    # ------------------------------------------------------------------

    async def _relay_loop(self, ws, worker: Worker):
        try:
            async for raw in ws:
                try:
                    await self._handle(json.loads(raw), worker, ws=ws)
                except Exception:
                    logger.exception("relay handle error (worker %s)", worker.id)
        except (websockets.ConnectionClosed, OSError):
            pass
        except asyncio.CancelledError:
            return
        async with self._connection_lock(worker.id):
            if (
                not self._shutting_down
                and worker.id not in self._closing
                and self._ws.get(worker.id) is ws
            ):
                logger.warning(
                    "worker %s relay disconnected, reconnecting",
                    worker.id,
                )
                self._ws.pop(worker.id, None)
                if self._loops.get(worker.id) is asyncio.current_task():
                    self._loops.pop(worker.id, None)
                # Detach only the subscriptions owned by this exact dead
                # socket while still holding the connection lock.  Popping in
                # _reconnect raced subscribe_task(), which could install a new
                # socket/set before this task first ran.
                task_ids = self._tasks.pop(worker.id, set())
                self._schedule_reconnect(worker, task_ids)

    async def _reconnect(
        self,
        worker: Worker,
        task_ids: set[int] | None = None,
    ):
        worker_id = worker.id
        if self._shutting_down or worker_id in self._closing:
            return
        if task_ids is None:
            # Compatibility for direct recovery callers/tests.  The relay-loop
            # path always supplies its lock-protected snapshot.
            async with self._connection_lock(worker.id):
                if self._shutting_down or worker_id in self._closing:
                    return
                task_ids = self._tasks.pop(worker.id, set())
        # Capture the generations owned by this disconnected relay before any
        # backoff/network await.  Reconnect exhaustion belongs only to these
        # generations; a retry on the same Worker is a distinct generation.
        disconnected_generations: dict[int, WorkerTaskGeneration] = {}
        async with self.db_factory() as db:
            for task_id in task_ids:
                generation = await read_worker_task_generation(
                    db,
                    task_id,
                    worker_id,
                )
                if (
                    generation is not None
                    and (
                        generation.status in ("executing", "in_progress")
                        or (
                            generation.status == "completed"
                            and generation.pty_background_generation
                            is not None
                        )
                    )
                ):
                    disconnected_generations[task_id] = generation
        for attempt in range(10):
            if self._shutting_down or worker_id in self._closing:
                return
            await asyncio.sleep(min(2 ** attempt, 60))
            if self._shutting_down or worker_id in self._closing:
                return
            try:
                # Re-fetch worker from DB to get latest IP/token after stop/start
                async with self.db_factory() as db:
                    fresh = await db.get(Worker, worker_id)
                    if not fresh or fresh.status in ("terminated", "destroying"):
                        return
                await self.ensure_connection(fresh)
                current_task_ids: set[int] = set()
                for tid in task_ids:
                    if (
                        await self._observe_task_generation(worker_id, tid)
                        is None
                    ):
                        continue
                    await self.subscribe_task(fresh, tid)
                    current_task_ids.add(tid)
                await self._backfill_missing_logs(fresh, current_task_ids)
                logger.info("worker %s relay reconnected", worker_id)
                return
            except Exception:
                if self._shutting_down or worker_id in self._closing:
                    return
                continue
        # Reconnect exhaustion cannot prove the Worker stopped.  Preserve the
        # exact active/background generation so retry and migration remain
        # blocked, while recording a durable quarantine reason for later
        # Worker readback/reconciliation.
        if self._shutting_down or worker_id in self._closing:
            return
        logger.error("worker %s relay reconnect exhausted", worker.id)
        quarantined_generations: list[WorkerTaskGeneration] = []
        for tid, observed in disconnected_generations.items():
            async with self.db_factory() as db:
                quarantined = await db.execute(
                    update(Task)
                    .where(*_worker_task_generation_write_predicates(observed))
                    .values(
                        error_message=(
                            f"Worker {worker.name} relay reconnect exhausted; "
                            "remote execution outcome is uncertain and automatic "
                            "retry/migration remains blocked pending reconciliation"
                        ),
                    )
                )
                if quarantined.rowcount != 1:
                    await db.rollback()
                    continue
                resulting = await read_worker_task_generation(
                    db,
                    tid,
                    worker_id,
                )
                if resulting is None:
                    await db.rollback()
                    continue
                await db.commit()
                quarantined_generations.append(resulting)
        for generation in quarantined_generations:
            await self._publish_status_generation(
                generation,
                payload={
                    "relay_state": "uncertain",
                    "error_message": (
                        f"Worker {worker.name} relay reconnect exhausted; "
                        "remote execution outcome is uncertain"
                    ),
                },
                notify_completion=False,
            )

    async def _handle(self, msg: dict, worker: Worker, *, ws=None):
        """Handle one relay event under the shared Task operation fence.

        Consuming the first reserved G+1 event may durably advance the mirror
        and clear its handoff marker before the event's own log/write/broadcast
        finishes.  Holding the same fence used by chat, retry, and migration
        keeps that multi-transaction relay step indivisible to Manager-side
        Task operations.
        """

        channel = msg.get("channel", "")
        data = msg.get("data", msg)
        if not isinstance(data, dict):
            return
        task_id = data.get("task_id")
        if not task_id and channel.startswith("task:"):
            try:
                task_id = int(channel.split(":", 1)[1])
            except (ValueError, IndexError):
                return
        if not task_id or task_id not in self._tasks.get(worker.id, set()):
            return

        # Import lazily: worker_proxy imports this module for the generation
        # helpers, while the lock registry lives there for all Manager→Worker
        # mutation paths.
        from backend.services.worker_proxy import get_task_operation_lock

        if data.get("event_type") == WORKER_LAUNCH_ADMISSION_EVENT:
            request = parse_worker_launch_admission_request(data)
            if request is None or request.task_id != task_id or ws is None:
                return
            # The operation/DB fences are released before touching the socket.
            # A blocked send must never prevent termination, retry, or relay
            # reconciliation from acquiring the Task's shared operation lock.
            async with get_task_operation_lock(task_id):
                admitted, reason_code = (
                    await self._authorize_worker_launch_admission(
                        request,
                        worker,
                    )
                )
            await self._send_worker_launch_admission_response(
                ws=ws,
                worker=worker,
                request=request,
                admitted=admitted,
                reason_code=reason_code,
            )
            return

        async with get_task_operation_lock(task_id):
            completion = await self._handle_with_operation_lock(msg, worker)
        # PR completion itself takes the same operation lock in Dispatcher.
        # Run it only after the relay event's fence is released; asyncio.Lock
        # is deliberately non-reentrant.
        if completion is not None:
            await self._notify_completed_pr_review(completion)

    async def _handle_with_operation_lock(
        self,
        msg: dict,
        worker: Worker,
    ):
        channel = msg.get("channel", "")
        data = msg.get("data", msg)
        if not isinstance(data, dict):
            return
        # monitor 事件用 "event" 键，chat 事件用 "event_type"，status_change 用 "event"
        event_type = data.get("event_type") or data.get("event")
        ccm_sub_agent_relay_event = bool(
            event_type
            in {"sub_agent_session_created", "sub_agent_session_status"}
            and data.get("event") == event_type
        )
        native_sub_agent_relay_event = bool(
            event_type
            in {
                "sub_agent_session_created",
                "sub_agent_report",
                "sub_agent_session_status",
            }
            and data.get("native_mirror_version")
            == _NATIVE_SUB_AGENT_MIRROR_VERSION
            and data.get("source") == "native"
        )
        if (
            event_type
            in {
                "sub_agent_session_created",
                "sub_agent_report",
                "sub_agent_session_status",
            }
            and "native_mirror_version" in data
            and not native_sub_agent_relay_event
        ):
            # The key opts into the durable mirror contract.  Unsupported or
            # partial declared versions must not silently fall back to the
            # legacy notification-only namespace.
            return

        # task_id：data 里有就用，没有从 channel 名解析（task:{id} 的 chat 事件不带）
        task_id = data.get("task_id")
        if not task_id and channel.startswith("task:"):
            try:
                task_id = int(channel.split(":", 1)[1])
            except (ValueError, IndexError):
                return
        if not task_id or task_id not in self._tasks.get(worker.id, set()):
            return
        # Check quarantine only under the per-Task operation lock.  Events that
        # arrive during proof/readback must wait for the recovery handshake;
        # dropping them before lock acquisition would lose a terminal edge.
        if self._legacy_carrier_recovery_active(worker.id, task_id):
            return

        # Manager-side chat/application bookkeeping is the canonical owner of
        # both events.  In particular, Worker-local Plan/Version ids are not
        # valid in the Manager database and must never reach its subscribers;
        # the proxy broadcasts the canonical event after its mirror commits.
        if event_type in {"user_message", "plan_version_applied"}:
            return

        event_retry_count: int | None = None
        event_turn_generation: int | None = None
        native_turn_id = None
        turn_scope = None
        actual_transport = None
        worker_turn_handoff_id: str | None = None
        generation_scoped_event = (
            (
                event_type in EXACT_GENERATION_RELAY_EVENT_TYPES
                and (
                    event_type
                    not in {
                        "sub_agent_session_created",
                        "sub_agent_report",
                        "sub_agent_session_status",
                    }
                    or ccm_sub_agent_relay_event
                    or native_sub_agent_relay_event
                )
            )
            or event_type in CHAT_EVENT_TYPES
        )
        if event_type in CHAT_EVENT_TYPES:
            native_turn_id = _validated_native_turn_id(data)
            if native_turn_id is _INVALID_NATIVE_TURN_ID:
                return
            turn_scope = _validated_turn_scope(data)
            if turn_scope is _INVALID_TURN_SCOPE:
                return
            actual_transport = _validated_actual_transport(data, turn_scope)
            if actual_transport is _INVALID_ACTUAL_TRANSPORT:
                return
        if generation_scoped_event:
            event_retry_count = data.get("task_retry_count")
            event_turn_generation = data.get("task_turn_generation")
            if (
                type(event_retry_count) is not int
                or type(event_turn_generation) is not int
            ):
                return
            pre_observed = await self._observe_task_generation(
                worker.id,
                task_id,
            )
            if pre_observed is None:
                return
            if (
                _has_worker_turn_handoff(pre_observed)
                and event_retry_count
                == pre_observed.worker_turn_handoff_retry_count
                and event_turn_generation
                == pre_observed.worker_turn_handoff_from_generation + 1
            ):
                worker_turn_handoff_id = (
                    await self._launched_handoff_id_for_generation(
                        worker,
                        pre_observed,
                        retry_count=event_retry_count,
                        turn_generation=event_turn_generation,
                    )
                )
                if worker_turn_handoff_id is None:
                    return
            observed = await self._observe_or_adopt_event_generation(
                worker.id,
                task_id,
                retry_count=event_retry_count,
                turn_generation=event_turn_generation,
                worker_turn_handoff_id=worker_turn_handoff_id,
            )
        else:
            observed = await self._observe_task_generation(worker.id, task_id)
        if observed is None:
            # Subscription state is only a routing hint.  The durable worker_id
            # assignment is the authority after migrations.
            return

        if event_type in {
            "plan_application_delivery_failed",
            "plan_application_delivery_uncertain",
            "plan_application_delivery_resolved",
        }:
            receipt_key = data.get("receipt_key")
            delivery_status = data.get("delivery_status")
            if (
                not isinstance(receipt_key, str)
                or not receipt_key
                or len(receipt_key) > 200
            ):
                return
            from backend.services.plan_events import broadcast_plan_event
            from backend.services.plan_service import (
                fence_worker_plan_application_receipt,
                preserve_uncertain_plan_application,
                release_unstarted_plan_application,
                resolve_uncertain_plan_application,
            )

            async with self.db_factory() as db:
                try:
                    receipt = await fence_worker_plan_application_receipt(
                        db,
                        receipt_key=receipt_key,
                        target_task_id=task_id,
                        expected_worker_id=worker.id,
                    )
                except HTTPException:
                    await db.rollback()
                    return
                if receipt is None:
                    await db.rollback()
                    return
                if event_type == "plan_application_delivery_failed":
                    if delivery_status not in {"failed", "cancelled"}:
                        await db.rollback()
                        return
                    released = await release_unstarted_plan_application(
                        db,
                        receipt_key=receipt_key,
                        delivery_status=delivery_status,
                        error=str(data.get("error") or "")[:2000],
                        expected_worker_id=worker.id,
                    )
                elif event_type == "plan_application_delivery_uncertain":
                    if receipt.delivery_status not in {
                        "pending",
                        "queued",
                        "launching",
                        "uncertain",
                    }:
                        await db.rollback()
                        return
                    evidence = data.get("launch_evidence")
                    plan_ids = await preserve_uncertain_plan_application(
                        db,
                        receipt=receipt,
                        error=str(data.get("error") or "")[:2000],
                        launch_evidence=(
                            evidence if isinstance(evidence, dict) else None
                        ),
                        response=(
                            receipt.response
                            if isinstance(receipt.response, dict)
                            else None
                        ),
                    )
                    released = (plan_ids, receipt.target_task_id)
                else:
                    action = data.get("action")
                    note = str(data.get("note") or "Worker resolution")[:2000]
                    if action not in {"confirm_launched", "release_for_retry"}:
                        await db.rollback()
                        return
                    already_resolved = bool(
                        isinstance(receipt.delivery_resolution, dict)
                        and receipt.delivery_resolution.get("action") == action
                    )
                    if not already_resolved:
                        if receipt.delivery_status not in {
                            "pending",
                            "queued",
                            "launching",
                            "uncertain",
                        }:
                            await db.rollback()
                            return
                        await preserve_uncertain_plan_application(
                            db,
                            receipt=receipt,
                            error=(
                                str(data.get("error") or "")[:2000]
                                or "Worker launch required manual reconciliation"
                            ),
                            launch_evidence=(
                                data.get("launch_evidence")
                                if isinstance(data.get("launch_evidence"), dict)
                                else receipt.launch_evidence
                            ),
                            response=(
                                receipt.response
                                if isinstance(receipt.response, dict)
                                else None
                            ),
                        )
                    released = await resolve_uncertain_plan_application(
                        db,
                        receipt_key=receipt_key,
                        action=action,
                        note=note,
                        actor_id=None,
                    )
                    delivery_status = (
                        "launched"
                        if action == "confirm_launched"
                        else "cancelled"
                    )
                if released is None:
                    await db.rollback()
                    return
                plan_ids, target_task_id = released
                if target_task_id != task_id:
                    await db.rollback()
                    return
                await db.commit()
            for plan_id in plan_ids:
                await broadcast_plan_event(
                    event=event_type,
                    plan_id=plan_id,
                    target_task_id=task_id,
                    broadcaster=self.broadcaster,
                    receipt_key=receipt_key,
                    delivery_status=delivery_status,
                )
            await self.broadcaster.broadcast(
                f"task:{task_id}",
                {key: value for key, value in data.items() if key != "instance_id"},
            )
            return

        if event_type in CHAT_EVENT_TYPES:
            if (
                event_retry_count != observed.retry_count
                or event_turn_generation != observed.turn_generation
            ):
                # Chat/result events are terminal evidence for generation-
                # sensitive consumers such as PR Monitor.  A delayed event from
                # an older retry must never borrow the Manager's current retry
                # merely because the task id and Worker assignment still match.
                return

        # 2) chat 事件双写 LogEntry（instance_id=None；广播 payload 无 raw_json，存 None）
        persisted_forward = None
        if event_type in CHAT_EVENT_TYPES:
            context_preflight_proof = (
                parse_codex_context_preflight_relay_proof(
                    data.get(WORKER_CONTEXT_PREFLIGHT_PROOF_KEY)
                )
                if WORKER_CONTEXT_PREFLIGHT_PROOF_KEY in data
                else None
            )
            async with self.db_factory() as db:
                guard_values = {"status": observed.status}
                if (
                    worker_turn_handoff_id is not None
                    and observed.worker_turn_handoff_id
                    == worker_turn_handoff_id
                    and observed.turn_generation
                    == observed.worker_turn_handoff_from_generation + 1
                ):
                    # Clearing and persisting the exact G+1 event share this
                    # transaction.  A crash before it commits leaves the
                    # marker/recovery subscription intact.
                    guard_values.update(_WORKER_TURN_HANDOFF_CLEAR_VALUES)
                if (
                    data.get("role") == "assistant"
                    and event_type in ("message", "result")
                ):
                    guard_values["has_unread"] = True
                guarded = await db.execute(
                    update(Task)
                    .where(*_worker_task_generation_write_predicates(observed))
                    .values(**guard_values)
                )
                if guarded.rowcount != 1:
                    await db.rollback()
                    return
                if worker_turn_handoff_id is not None and not (
                    await _settle_manager_handoff_receipt(
                        db,
                        observed,
                        status="completed",
                    )
                ):
                    await db.rollback()
                    return
                entry = LogEntry(
                    instance_id=None,
                    task_id=task_id,
                    task_retry_count=event_retry_count,
                    task_turn_generation=event_turn_generation,
                    native_turn_id=native_turn_id,
                    turn_scope=turn_scope,
                    actual_transport=actual_transport,
                    event_type=event_type,
                    role=data.get("role"),
                    content=data.get("content"),
                    tool_name=data.get("tool_name"),
                    tool_input=data.get("tool_input"),
                    tool_output=data.get("tool_output"),
                    raw_json=(
                        json.dumps(
                            context_preflight_proof,
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                        if context_preflight_proof is not None
                        else None
                    ),
                    is_error=data.get("is_error", False),
                    loop_iteration=data.get("loop_iteration"),
                )
                db.add(entry)
                await db.commit()
                persisted_forward = persisted_chat_event(
                    entry,
                    {
                        key: value
                        for key, value in data.items()
                        if key not in (
                            "instance_id",
                            "raw_json",
                            "task_retry_count",
                            "task_turn_generation",
                            "native_turn_id",
                            WORKER_CONTEXT_PREFLIGHT_PROOF_KEY,
                        )
                    },
                    provider=observed.provider,
                )
                persisted_forward["task_retry_count"] = event_retry_count
                persisted_forward["task_turn_generation"] = event_turn_generation
                if native_turn_id is not None:
                    persisted_forward["native_turn_id"] = native_turn_id
                persisted_forward["turn_scope"] = turn_scope
                persisted_forward["actual_transport"] = actual_transport
            # session_id 同步：worker 广播前 pop 了 session_id，首条事件到达时从 Worker 拉取
            if event_type == "system_init":
                session_observed = await self._observe_task_generation(
                    worker.id,
                    task_id,
                )
                if session_observed is not None:
                    remote_task = await self._fetch_task_snapshot(
                        worker,
                        task_id,
                        session_observed.incarnation_id,
                    )
                    remote_values = (
                        authoritative_worker_task_values(
                            remote_task,
                            task_id=task_id,
                            incarnation_id=session_observed.incarnation_id,
                        )
                        if remote_task is not None
                        else None
                    )
                    if (
                        remote_values is not None
                        and remote_values["retry_count"]
                        == session_observed.retry_count
                        and remote_values["turn_generation"]
                        == session_observed.turn_generation
                        and remote_values.get("session_id")
                    ):
                        async with self.db_factory() as db:
                            session_synced = await db.execute(
                                update(Task)
                                .where(
                                    *_worker_task_generation_write_predicates(
                                        session_observed
                                    ),
                                    Task.session_id.is_(None),
                                )
                                .values(
                                    session_id=remote_values["session_id"]
                                )
                            )
                            if session_synced.rowcount == 1:
                                await db.commit()
                            else:
                                await db.rollback()

        # 2b) Skill evolution from Worker tool failures
        if (
            event_type == "tool_result"
            and data.get("is_error")
            and data.get("tool_name")
        ):
            try:
                from backend.services.skill_evolution import evolve_on_failure
                async with self.db_factory() as db:
                    await evolve_on_failure(
                        tool_name=data["tool_name"],
                        error=str(data.get("tool_output", ""))[:500],
                        context=str(data.get("tool_input", ""))[:300],
                        db=db,
                        worker_id=worker.id,
                    )
            except Exception:
                logger.debug("worker skill evolution failed", exc_info=True)

        # 3) 字段同步
        if event_type == "background_activity":
            event_background_active = data.get("background_active")
            if type(event_background_active) is not bool:
                return
            # WebSocket ordering is not authoritative.  Re-read the Worker and
            # accept the event only when its strict boolean agrees with that
            # fresh snapshot, then CAS it onto the exact Manager generation.
            remote_task = await self._fetch_task_snapshot(
                worker,
                task_id,
                observed.incarnation_id,
            )
            if (
                remote_task is None
                or type(remote_task.get("background_active")) is not bool
                or remote_task["background_active"]
                is not event_background_active
            ):
                return
            snapshot_handoff_id = (
                await self._launched_handoff_id_for_snapshot(
                    worker,
                    observed,
                    remote_task,
                )
            )
            async with self.db_factory() as db:
                resulting = await apply_authoritative_worker_task(
                    db,
                    observed,
                    remote_task,
                    worker_turn_handoff_id=snapshot_handoff_id,
                )
            if (
                resulting is None
                or (
                    resulting.pty_background_generation is not None
                ) != event_background_active
            ):
                return
            published = await self._publish_background_generation(
                resulting,
                channels=(channel,),
                notify_completion=False,
            )
            return resulting if published else None

        if event_type == "status_change":
            new_status = data.get("new_status")
            if not isinstance(new_status, str):
                return
            # status_change itself carries no remote retry generation.  Resolve
            # it against the authoritative Worker task before touching the
            # Manager mirror; a mismatching status means this queued event is
            # stale and must be dropped.
            remote_task = await self._fetch_task_snapshot(
                worker,
                task_id,
                observed.incarnation_id,
            )
            if (
                remote_task is None
                or remote_task.get("status") != new_status
            ):
                return
            snapshot_handoff_id = (
                await self._launched_handoff_id_for_snapshot(
                    worker,
                    observed,
                    remote_task,
                )
            )
            async with self.db_factory() as db:
                resulting = await apply_authoritative_worker_task(
                    db,
                    observed,
                    remote_task,
                    worker_turn_handoff_id=snapshot_handoff_id,
                )
            if resulting is not None:
                published = await self._publish_status_generation(
                    resulting,
                    data,
                    notify_completion=False,
                )
                return resulting if published else None
            return None

        elif event_type == "context_usage":
            async with self.db_factory() as db:
                changed = await db.execute(
                    update(Task)
                    .where(*_worker_task_generation_write_predicates(observed))
                    .values(
                        context_window_usage={
                        k: v for k, v in data.items()
                        if k not in (
                            "event_type",
                            "task_id",
                            "task_retry_count",
                            "task_turn_generation",
                        )
                        }
                    )
                )
                if changed.rowcount == 1:
                    await db.commit()
                else:
                    await db.rollback()
                    return

        elif event_type == "plan_ready":
            # plan_ready carries neither plan_content nor a remote generation.
            # Resolve both from one authoritative snapshot.
            remote_task = await self._fetch_task_snapshot(
                worker,
                task_id,
                observed.incarnation_id,
            )
            if (
                remote_task is None
                or remote_task.get("status") != "plan_review"
            ):
                return
            snapshot_handoff_id = (
                await self._launched_handoff_id_for_snapshot(
                    worker,
                    observed,
                    remote_task,
                )
            )
            async with self.db_factory() as db:
                resulting = await apply_authoritative_worker_task(
                    db,
                    observed,
                    remote_task,
                    worker_turn_handoff_id=snapshot_handoff_id,
                )
            if resulting is None:
                return

        elif event_type == "loop_iteration_end":
            async with self.db_factory() as db:
                values = {"status": observed.status}
                if data.get("progress"):
                    values["loop_progress"] = data["progress"]
                changed = await db.execute(
                    update(Task)
                    .where(*_worker_task_generation_write_predicates(observed))
                    .values(**values)
                )
                if changed.rowcount == 1:
                    await db.commit()
                else:
                    await db.rollback()
                    return

        elif event_type == "goal_evaluation":
            async with self.db_factory() as db:
                values = {"status": observed.status}
                if data.get("turn") is not None:
                    values["goal_turns_used"] = data["turn"]
                if data.get("reason"):
                    values["goal_last_reason"] = data["reason"]
                changed = await db.execute(
                    update(Task)
                    .where(*_worker_task_generation_write_predicates(observed))
                    .values(**values)
                )
                if changed.rowcount == 1:
                    await db.commit()
                else:
                    await db.rollback()
                    return

        elif event_type == "monitor_session_created":
            remote_id = data.get("monitor_session_id")
            description = data.get("description")
            if (
                type(remote_id) is not int
                or remote_id <= 0
                or not isinstance(description, str)
                or len(description) > 500
                or type(worker.id) is not int
                or worker.id <= 0
                or not isinstance(observed.incarnation_id, str)
                or len(observed.incarnation_id) != 32
                or any(
                    char not in "0123456789abcdef"
                    for char in observed.incarnation_id
                )
            ):
                return
            expected_identity = (
                worker.id,
                observed.incarnation_id,
                remote_id,
            )
            mirror_meta = _worker_child_mirror_meta(
                worker_id=worker.id,
                task_incarnation_id=observed.incarnation_id,
                remote_id=remote_id,
            )
            async with self.db_factory() as db:
                guarded = await db.execute(
                    update(Task)
                    .where(*_worker_task_generation_write_predicates(observed))
                    .values(status=observed.status)
                )
                if guarded.rowcount != 1:
                    await db.rollback()
                    return
                mirrors = list(
                    (
                        await db.execute(
                            select(MonitorSession)
                            .where(
                                MonitorSession.task_id == task_id,
                                MonitorSession.remote_id == remote_id,
                            )
                            .with_for_update()
                        )
                    ).scalars()
                )
                mirror, trusted_identity_set = _exact_worker_child_mirror(
                    mirrors,
                    expected_identity,
                )
                if not trusted_identity_set:
                    await db.rollback()
                    return
                if mirror is not None:
                    if (
                        mirror.agent_type != "monitor"
                        or mirror.source != "ccm"
                        or mirror.status != "running"
                        or mirror.description != description
                    ):
                        await db.rollback()
                        return
                else:
                    mirror = MonitorSession(
                        remote_id=remote_id,
                        task_id=task_id,
                        agent_type="monitor",
                        source="ccm",
                        description=description,
                        status="running",
                        meta=mirror_meta,
                    )
                    db.add(mirror)
                    await db.flush()
                canonical_forward = {
                    **data,
                    "monitor_session_id": mirror.id,
                    "description": mirror.description,
                }
                await db.commit()
            persisted_forward = canonical_forward

        elif event_type == "monitor_check":
            remote_id = data.get("monitor_session_id")
            check_number = data.get("check_number")
            if (
                type(remote_id) is not int
                or remote_id <= 0
                or type(check_number) is not int
                or check_number < 0
                or type(worker.id) is not int
                or worker.id <= 0
                or not isinstance(observed.incarnation_id, str)
                or len(observed.incarnation_id) != 32
                or any(
                    char not in "0123456789abcdef"
                    for char in observed.incarnation_id
                )
            ):
                return
            expected_identity = (
                worker.id,
                observed.incarnation_id,
                remote_id,
            )
            async with self.db_factory() as db:
                guarded = await db.execute(
                    update(Task)
                    .where(*_worker_task_generation_write_predicates(observed))
                    .values(status=observed.status)
                )
                if guarded.rowcount != 1:
                    await db.rollback()
                    return
                mirrors = list(
                    (
                        await db.execute(
                            select(MonitorSession)
                            .where(
                                MonitorSession.task_id == task_id,
                                MonitorSession.remote_id == remote_id,
                            )
                            .with_for_update()
                        )
                    ).scalars()
                )
                mirror, trusted_identity_set = _exact_worker_child_mirror(
                    mirrors,
                    expected_identity,
                )
                if (
                    not trusted_identity_set
                    or mirror is None
                    or mirror.agent_type != "monitor"
                    or mirror.source != "ccm"
                ):
                    await db.rollback()
                    return
                db.add(MonitorCheck(
                    monitor_session_id=mirror.id,
                    check_number=check_number,
                    status=data.get("status") or "",
                    summary=data.get("summary"),
                    full_output=data.get("full_output"),
                ))
                mirror.checks_done = check_number
                mirror.last_summary = data.get("summary")
                canonical_forward = {
                    **data,
                    "monitor_session_id": mirror.id,
                    "check_number": check_number,
                }
                await db.commit()
            persisted_forward = canonical_forward

        elif event_type == "monitor_session_status":
            remote_id = data.get("monitor_session_id")
            status = data.get("status")
            if (
                type(remote_id) is not int
                or remote_id <= 0
                or status not in {"completed", "failed", "cancelled"}
                or type(worker.id) is not int
                or worker.id <= 0
                or not isinstance(observed.incarnation_id, str)
                or len(observed.incarnation_id) != 32
                or any(
                    char not in "0123456789abcdef"
                    for char in observed.incarnation_id
                )
            ):
                return
            expected_identity = (
                worker.id,
                observed.incarnation_id,
                remote_id,
            )
            async with self.db_factory() as db:
                guarded = await db.execute(
                    update(Task)
                    .where(*_worker_task_generation_write_predicates(observed))
                    .values(status=observed.status)
                )
                if guarded.rowcount != 1:
                    await db.rollback()
                    return
                mirrors = list(
                    (
                        await db.execute(
                            select(MonitorSession)
                            .where(
                                MonitorSession.task_id == task_id,
                                MonitorSession.remote_id == remote_id,
                            )
                            .with_for_update()
                        )
                    ).scalars()
                )
                mirror, trusted_identity_set = _exact_worker_child_mirror(
                    mirrors,
                    expected_identity,
                )
                if (
                    not trusted_identity_set
                    or mirror is None
                    or mirror.agent_type != "monitor"
                    or mirror.source != "ccm"
                ):
                    await db.rollback()
                    return
                if mirror.status in {"completed", "failed", "cancelled"}:
                    if mirror.status != status:
                        await db.rollback()
                        return
                elif mirror.status == "running":
                    mirror.status = status
                    mirror.completed_at = datetime.utcnow()
                else:
                    await db.rollback()
                    return
                canonical_forward = {
                    **data,
                    "monitor_session_id": mirror.id,
                    "status": mirror.status,
                }
                await db.commit()
            persisted_forward = canonical_forward

        elif native_sub_agent_relay_event:
            incoming_status = data.get("status")
            terminal = (
                isinstance(incoming_status, str)
                and incoming_status in _NATIVE_SUB_AGENT_TERMINAL_STATUSES
            )
            if (
                event_type
                in {"sub_agent_session_created", "sub_agent_report"}
                and terminal
            ):
                return
            if (
                event_type == "sub_agent_session_status"
                and data.get("status") == "running"
                and data.get("provider") != "codex"
            ):
                return
            payload = _validated_native_sub_agent_relay_payload(
                data,
                terminal=terminal,
            )
            check_number = data.get("check_number")
            if event_type == "sub_agent_report" and (
                type(check_number) is not int
                or check_number <= 0
                or payload is None
                or check_number != payload["checks_done"]
            ):
                return
            if (
                payload is None
                or type(worker.id) is not int
                or worker.id <= 0
                or not isinstance(observed.incarnation_id, str)
                or len(observed.incarnation_id) != 32
                or any(
                    char not in "0123456789abcdef"
                    for char in observed.incarnation_id
                )
            ):
                return
            remote_id = payload["remote_id"]
            assert type(remote_id) is int
            expected_identity = (
                worker.id,
                observed.incarnation_id,
                remote_id,
            )
            mirror_meta = _worker_child_mirror_meta(
                worker_id=worker.id,
                task_incarnation_id=observed.incarnation_id,
                remote_id=remote_id,
                native_sequence=payload["native_sequence"],
            )
            async with self.db_factory() as db:
                guarded = await db.execute(
                    update(Task)
                    .where(*_worker_task_generation_write_predicates(observed))
                    .values(status=observed.status)
                )
                if guarded.rowcount != 1:
                    await db.rollback()
                    return
                mirrors = list(
                    (
                        await db.execute(
                            select(SubAgentSession)
                            .where(
                                SubAgentSession.task_id == task_id,
                                SubAgentSession.remote_id == remote_id,
                            )
                            .with_for_update()
                        )
                    ).scalars()
                )
                mirror, trusted_identity_set = _exact_worker_child_mirror(
                    mirrors,
                    expected_identity,
                )
                if not trusted_identity_set:
                    await db.rollback()
                    return

                if mirror is not None and (
                    mirror.agent_type != payload["agent_type"]
                    or mirror.source != "native"
                    or mirror.provider != payload["provider"]
                    or mirror.codex_thread_id != payload["codex_thread_id"]
                ):
                    await db.rollback()
                    return

                add_report = False
                same_sequence = False
                if mirror is not None and payload["provider"] == "codex":
                    parsed_identity = _worker_child_mirror_snapshot_identity(
                        mirror.meta
                    )
                    incoming_sequence = payload["native_sequence"]
                    if (
                        parsed_identity is None
                        or type(incoming_sequence) is not int
                    ):
                        await db.rollback()
                        return
                    stored_sequence = parsed_identity[1]
                    if (
                        type(stored_sequence) is not int
                        or incoming_sequence < stored_sequence
                    ):
                        await db.rollback()
                        return
                    same_sequence = incoming_sequence == stored_sequence
                    if same_sequence and (
                        mirror.status != payload["status"]
                        or mirror.description != payload["description"]
                        or mirror.model != payload["model"]
                        or mirror.codex_effort_level
                        != payload["reasoning_effort"]
                        or mirror.checks_done != payload["checks_done"]
                        or mirror.last_summary != payload["last_summary"]
                    ):
                        await db.rollback()
                        return

                if mirror is None:
                    initial_status = (
                        payload["status"] if terminal else "running"
                    )
                    mirror = SubAgentSession(
                        task_id=task_id,
                        remote_id=remote_id,
                        agent_type=payload["agent_type"],
                        source="native",
                        description=payload["description"],
                        interval=0,
                        max_checks=0,
                        model=payload["model"],
                        provider=payload["provider"],
                        status=initial_status,
                        checks_done=payload["checks_done"],
                        last_summary=payload["last_summary"],
                        codex_thread_id=payload["codex_thread_id"],
                        codex_effort_level=payload["reasoning_effort"],
                        meta=mirror_meta,
                        completed_at=(
                            datetime.utcnow() if terminal else None
                        ),
                    )
                    db.add(mirror)
                    await db.flush()
                    add_report = event_type == "sub_agent_report"
                elif same_sequence:
                    # Exact transport replay: retain one DB/report edge while
                    # allowing the canonical snapshot to be forwarded again.
                    pass
                elif event_type == "sub_agent_session_created":
                    if (
                        mirror.provider == "codex"
                        or mirror.status != "running"
                        or mirror.description != payload["description"]
                        or mirror.checks_done != payload["checks_done"]
                        or mirror.last_summary != payload["last_summary"]
                    ):
                        # A new Codex sequence cannot legitimately recreate an
                        # existing remote row.  Unsequenced Claude snapshots
                        # retain their exact replay compatibility only.
                        await db.rollback()
                        return
                elif event_type == "sub_agent_report":
                    if mirror.status != "running":
                        await db.rollback()
                        return
                    assert type(check_number) is int
                    if mirror.checks_done > check_number:
                        await db.rollback()
                        return
                    if mirror.checks_done == check_number:
                        if mirror.last_summary != payload["last_summary"]:
                            await db.rollback()
                            return
                    else:
                        mirror.checks_done = check_number
                        mirror.last_summary = payload["last_summary"]
                        mirror.description = payload["description"]
                        mirror.model = payload["model"]
                        mirror.codex_effort_level = payload["reasoning_effort"]
                        add_report = True
                else:
                    assert event_type == "sub_agent_session_status"
                    if payload["status"] == "running":
                        if (
                            mirror.provider != "codex"
                            or mirror.status
                            not in {
                                "running",
                                *_NATIVE_SUB_AGENT_TERMINAL_STATUSES,
                            }
                        ):
                            await db.rollback()
                            return
                        mirror.status = "running"
                        mirror.description = payload["description"]
                        mirror.model = payload["model"]
                        mirror.checks_done = payload["checks_done"]
                        mirror.last_summary = payload["last_summary"]
                        mirror.codex_effort_level = payload["reasoning_effort"]
                        mirror.completed_at = None
                    elif mirror.status in _NATIVE_SUB_AGENT_TERMINAL_STATUSES:
                        if mirror.status != payload["status"]:
                            await db.rollback()
                            return
                    elif mirror.status == "running":
                        mirror.status = payload["status"]
                        mirror.description = payload["description"]
                        mirror.model = payload["model"]
                        mirror.checks_done = payload["checks_done"]
                        mirror.last_summary = payload["last_summary"]
                        mirror.codex_effort_level = payload["reasoning_effort"]
                        mirror.completed_at = datetime.utcnow()
                    else:
                        await db.rollback()
                        return

                if mirror.provider == "codex":
                    mirror.meta = mirror_meta

                if add_report:
                    assert type(check_number) is int
                    db.add(SubAgentReport(
                        session_id=mirror.id,
                        check_number=check_number,
                        status="running",
                        summary=payload["last_summary"],
                    ))
                canonical_forward = {
                    **data,
                    "sub_agent_session_id": mirror.id,
                    "agent_type": mirror.agent_type,
                    "source": "native",
                    "provider": mirror.provider,
                    "description": mirror.description,
                    "model": mirror.model,
                    "reasoning_effort": mirror.codex_effort_level,
                    "status": mirror.status,
                    "checks_done": mirror.checks_done,
                    "last_summary": mirror.last_summary,
                    "codex_thread_id": mirror.codex_thread_id,
                }
                await db.commit()
            persisted_forward = canonical_forward

        elif (
            event_type == "sub_agent_session_created"
            and ccm_sub_agent_relay_event
        ):
            payload = _validated_sub_agent_relay_payload(
                data,
                terminal=False,
            )
            if (
                payload is None
                or type(worker.id) is not int
                or worker.id <= 0
                or not isinstance(observed.incarnation_id, str)
                or len(observed.incarnation_id) != 32
                or any(
                    char not in "0123456789abcdef"
                    for char in observed.incarnation_id
                )
            ):
                return
            remote_id = payload["remote_id"]
            assert type(remote_id) is int
            expected_identity = (
                worker.id,
                observed.incarnation_id,
                remote_id,
            )
            mirror_meta = _worker_child_mirror_meta(
                worker_id=worker.id,
                task_incarnation_id=observed.incarnation_id,
                remote_id=remote_id,
            )
            async with self.db_factory() as db:
                guarded = await db.execute(
                    update(Task)
                    .where(*_worker_task_generation_write_predicates(observed))
                    .values(status=observed.status)
                )
                if guarded.rowcount != 1:
                    await db.rollback()
                    return
                mirrors = list(
                    (
                        await db.execute(
                            select(SubAgentSession)
                            .where(
                                SubAgentSession.task_id == task_id,
                                SubAgentSession.remote_id == remote_id,
                            )
                            .with_for_update()
                        )
                    ).scalars()
                )
                mirror, trusted_identity_set = _exact_worker_child_mirror(
                    mirrors,
                    expected_identity,
                )
                if not trusted_identity_set:
                    await db.rollback()
                    return
                if mirror is not None:
                    if (
                        mirror.agent_type != "sub_agent"
                        or mirror.source != "ccm"
                        or mirror.status != "running"
                        or mirror.description != payload["description"]
                        or mirror.monitor_context != payload["monitor_context"]
                        or mirror.checks_done != payload["checks_done"]
                        or mirror.last_summary != payload["last_summary"]
                    ):
                        # A delayed created event must not revive a terminal
                        # mirror or alias another child category/source.
                        await db.rollback()
                        return
                else:
                    mirror = SubAgentSession(
                        task_id=task_id,
                        remote_id=remote_id,
                        agent_type="sub_agent",
                        source="ccm",
                        description=payload["description"],
                        monitor_context=payload["monitor_context"],
                        interval=0,
                        max_checks=0,
                        status="running",
                        checks_done=payload["checks_done"],
                        last_summary=payload["last_summary"],
                        meta=mirror_meta,
                    )
                    db.add(mirror)
                    await db.flush()
                local_id = mirror.id
                canonical_forward = {
                    **data,
                    "sub_agent_session_id": local_id,
                    "description": mirror.description,
                    "agent_type": "sub_agent",
                    "source": "ccm",
                    "monitor_context": mirror.monitor_context,
                    "status": mirror.status,
                    "checks_done": mirror.checks_done,
                    "last_summary": mirror.last_summary,
                }
                await db.commit()
            persisted_forward = canonical_forward

        elif (
            event_type == "sub_agent_session_status"
            and ccm_sub_agent_relay_event
        ):
            payload = _validated_sub_agent_relay_payload(
                data,
                terminal=True,
            )
            if (
                payload is None
                or type(worker.id) is not int
                or worker.id <= 0
                or not isinstance(observed.incarnation_id, str)
                or len(observed.incarnation_id) != 32
                or any(
                    char not in "0123456789abcdef"
                    for char in observed.incarnation_id
                )
            ):
                return
            remote_id = payload["remote_id"]
            assert type(remote_id) is int
            expected_identity = (
                worker.id,
                observed.incarnation_id,
                remote_id,
            )
            mirror_meta = _worker_child_mirror_meta(
                worker_id=worker.id,
                task_incarnation_id=observed.incarnation_id,
                remote_id=remote_id,
            )
            async with self.db_factory() as db:
                guarded = await db.execute(
                    update(Task)
                    .where(*_worker_task_generation_write_predicates(observed))
                    .values(status=observed.status)
                )
                if guarded.rowcount != 1:
                    await db.rollback()
                    return
                mirrors = list(
                    (
                        await db.execute(
                            select(SubAgentSession)
                            .where(
                                SubAgentSession.task_id == task_id,
                                SubAgentSession.remote_id == remote_id,
                            )
                            .with_for_update()
                        )
                    ).scalars()
                )
                mirror, trusted_identity_set = _exact_worker_child_mirror(
                    mirrors,
                    expected_identity,
                )
                if not trusted_identity_set:
                    await db.rollback()
                    return
                if mirror is not None:
                    if (
                        mirror.agent_type != "sub_agent"
                        or mirror.source != "ccm"
                    ):
                        await db.rollback()
                        return
                    if mirror.status in _SUB_AGENT_TERMINAL_STATUSES:
                        if mirror.status != payload["status"]:
                            # First terminal evidence wins.  A duplicate from
                            # the Worker can acknowledge it but never rewrite
                            # or revive it with a different terminal result.
                            await db.rollback()
                            return
                    elif mirror.status == "running":
                        mirror.status = payload["status"]
                        mirror.description = payload["description"]
                        mirror.monitor_context = payload["monitor_context"]
                        mirror.checks_done = payload["checks_done"]
                        mirror.last_summary = payload["last_summary"]
                        mirror.completed_at = datetime.utcnow()
                    else:
                        await db.rollback()
                        return
                else:
                    # Status is a complete snapshot so a lost/reordered
                    # created event does not leave the Manager without a row.
                    mirror = SubAgentSession(
                        task_id=task_id,
                        remote_id=remote_id,
                        agent_type="sub_agent",
                        source="ccm",
                        description=payload["description"],
                        monitor_context=payload["monitor_context"],
                        interval=0,
                        max_checks=0,
                        status=payload["status"],
                        checks_done=payload["checks_done"],
                        last_summary=payload["last_summary"],
                        meta=mirror_meta,
                        completed_at=datetime.utcnow(),
                    )
                    db.add(mirror)
                    await db.flush()
                local_id = mirror.id
                canonical_forward = {
                    **data,
                    "sub_agent_session_id": local_id,
                    "description": mirror.description,
                    "agent_type": "sub_agent",
                    "source": "ccm",
                    "monitor_context": mirror.monitor_context,
                    "status": mirror.status,
                    "checks_done": mirror.checks_done,
                    "last_summary": mirror.last_summary,
                }
                await db.commit()
            persisted_forward = canonical_forward

        # 4) 镜像广播到来源同名 channel（剥 worker 的 instance_id，对 Manager 无意义）
        forward = persisted_forward or {
            k: v for k, v in data.items() if k != "instance_id"
        }
        if channel.startswith("task:"):
            await self.broadcaster.broadcast(f"task:{task_id}", forward)
        elif channel == "tasks":
            await self.broadcaster.broadcast("tasks", forward)

    # ------------------------------------------------------------------
    # Worker API 辅助
    # ------------------------------------------------------------------

    async def _backfill_missing_logs(
        self,
        worker: Worker,
        task_ids: set[int],
        *,
        sync_status: bool = True,
    ) -> set[int]:
        """Backfill each Task while excluding chat/retry/migration mutations."""

        from backend.services.worker_proxy import get_task_operation_lock

        synced: set[int] = set()
        for task_id in sorted(task_ids):
            deferred_completions: list[WorkerTaskGeneration] = []
            async with get_task_operation_lock(task_id):
                synced.update(
                    await self._backfill_missing_logs_with_operation_lock(
                        worker,
                        {task_id},
                        sync_status=sync_status,
                        deferred_completions=deferred_completions,
                    )
                )
            # Dispatcher completion also takes this lock. Keep it outside the
            # backfill fence for the same non-reentrant reason as live relay.
            for generation in deferred_completions:
                await self._notify_completed_pr_review(generation)
        return synced

    async def _backfill_missing_logs_with_operation_lock(
        self,
        worker: Worker,
        task_ids: set[int],
        *,
        sync_status: bool = True,
        deferred_completions: list[WorkerTaskGeneration] | None = None,
    ) -> set[int]:
        """断连/重启后补日志。用「非 user_message 条数」对比（user_message 由
        chat 代理直接入 Manager DB，不经 relay，按总条数比会错位重复）。

        Returns task ids whose history response was valid and committed under
        the exact observed Manager generation. ``sync_status=False`` is used
        by the completion hook to avoid recursively publishing the same
        completed status while it closes a possible task-channel log gap.
        """
        history_synced: set[int] = set()
        async with httpx.AsyncClient(timeout=30) as client:
            for tid in task_ids:
                try:
                    history_observed = await self._observe_task_generation(
                        worker.id,
                        tid,
                    )
                    if history_observed is None:
                        continue
                    if (
                        _has_worker_turn_handoff(history_observed)
                        and history_observed.turn_generation
                        == history_observed.worker_turn_handoff_from_generation
                    ):
                        await self._resume_accepted_worker_turn_handoff(
                            worker,
                            history_observed,
                            attempts=1,
                            client=client,
                            operation_lock_held=True,
                        )
                        history_observed = await self._observe_task_generation(
                            worker.id,
                            tid,
                        )
                        if history_observed is None:
                            continue
                        if _has_worker_turn_handoff(history_observed):
                            self.ensure_worker_turn_handoff_recovery(
                                worker,
                                history_observed,
                            )
                    history_handoff_id = None
                    if (
                        _has_worker_turn_handoff(history_observed)
                        and history_observed.turn_generation
                        == history_observed.worker_turn_handoff_from_generation + 1
                    ):
                        history_handoff_id = (
                            await self._launched_handoff_id_for_generation(
                                worker,
                                history_observed,
                                retry_count=history_observed.retry_count,
                                turn_generation=history_observed.turn_generation,
                                client=client,
                            )
                        )
                    history_response = await client.get(
                        self._api(
                            worker,
                            f"/api/tasks/{tid}/chat/history?compact=false",
                        ),
                        headers=self._headers(worker),
                    )
                    if history_response.status_code == 200:
                        remote = history_response.json()
                        if isinstance(remote, dict):
                            remote = remote.get("messages")
                        if not isinstance(remote, list):
                            remote = None
                        if remote is None:
                            if not sync_status:
                                continue
                        else:
                            non_user_messages = [
                                message
                                for message in remote
                                if isinstance(message, dict)
                                and message.get("event_type") != "user_message"
                            ]
                            scoped_messages = [
                                message
                                for message in non_user_messages
                                if type(message.get("task_retry_count")) is int
                                and type(
                                    message.get("task_turn_generation")
                                ) is int
                                and _validated_native_turn_id(message)
                                is not _INVALID_NATIVE_TURN_ID
                                and _validated_turn_scope(message)
                                is not _INVALID_TURN_SCOPE
                                and _valid_relay_log_metadata(message)
                            ]
                            # Rows persisted by a pre-turn-generation Worker
                            # legitimately serialize ``turn_generation=NULL``.
                            # They are neither evidence nor import candidates,
                            # but must not poison an otherwise exact current
                            # terminal history after a rolling upgrade.
                            legacy_unscoped_messages = [
                                message
                                for message in non_user_messages
                                if message.get("task_turn_generation") is None
                                and (
                                    message.get("task_retry_count") is None
                                    or type(message.get("task_retry_count")) is int
                                )
                                and _validated_native_turn_id(message)
                                is not _INVALID_NATIVE_TURN_ID
                                and _validated_turn_scope(message)
                                is not _INVALID_TURN_SCOPE
                                and _valid_relay_log_metadata(message)
                            ]
                            history_protocol_valid = (
                                all(isinstance(message, dict) for message in remote)
                                and len(scoped_messages)
                                + len(legacy_unscoped_messages)
                                == len(non_user_messages)
                            )
                            remote_non_user = [
                                message
                                for message in scoped_messages
                                if message["task_retry_count"]
                                == history_observed.retry_count
                                and message["task_turn_generation"]
                                == history_observed.turn_generation
                            ] if history_protocol_valid else []
                            # A non-empty history whose non-user records all
                            # belong to another generation normally cannot
                            # prove that the current generation's tail was
                            # returned.  One exact exception is a terminal G+1
                            # already proven by its launched handoff receipt:
                            # a successful full-history response may
                            # legitimately contain only G's old assistant tail
                            # plus G+1's user row.  In that case the empty
                            # current-generation non-user slice is itself the
                            # complete terminal tail and may settle the marker.
                            terminal_empty_handoff_history = bool(
                                history_handoff_id is not None
                                and history_observed.status
                                in _TERMINAL_TASK_STATUSES
                                and not remote_non_user
                            )
                            history_protocol_valid = (
                                history_protocol_valid
                                and (
                                    not non_user_messages
                                    or bool(remote_non_user)
                                    or terminal_empty_handoff_history
                                )
                            )
                            if not history_protocol_valid:
                                logger.warning(
                                    "worker %s returned unscoped or non-current "
                                    "history for task %s generation %s/%s",
                                    worker.id,
                                    tid,
                                    history_observed.retry_count,
                                    history_observed.turn_generation,
                                )
                            else:
                                async with self.db_factory() as db:
                                    guard_values = {
                                        "status": history_observed.status
                                    }
                                    clearing_history_handoff = bool(
                                        history_handoff_id is not None
                                        and (
                                            remote_non_user
                                            or history_observed.status
                                            in _TERMINAL_TASK_STATUSES
                                        )
                                    )
                                    if clearing_history_handoff:
                                        # The exact remote history and its
                                        # local copies commit together with
                                        # marker cleanup.  A crash on either
                                        # side leaves recover() subscribed.
                                        guard_values.update(
                                            _WORKER_TURN_HANDOFF_CLEAR_VALUES
                                        )
                                    guarded = await db.execute(
                                        update(Task)
                                        .where(
                                            *_worker_task_generation_write_predicates(
                                                history_observed
                                            )
                                        )
                                        .values(**guard_values)
                                    )
                                    if guarded.rowcount != 1:
                                        await db.rollback()
                                    else:
                                        if clearing_history_handoff and not (
                                            await _settle_manager_handoff_receipt(
                                                db,
                                                history_observed,
                                                status="completed",
                                            )
                                        ):
                                            await db.rollback()
                                            continue
                                        # Re-read after acquiring the Task
                                        # generation lock so a live relay
                                        # insert which won the race is included
                                        # in fingerprint deduplication.
                                        local_rows = (
                                            await db.execute(
                                                select(
                                                    LogEntry.event_type,
                                                    LogEntry.role,
                                                    LogEntry.content,
                                                    LogEntry.tool_name,
                                                    LogEntry.tool_input,
                                                    LogEntry.tool_output,
                                                    LogEntry.loop_iteration,
                                                    LogEntry.native_turn_id,
                                                    LogEntry.turn_scope,
                                                    LogEntry.actual_transport,
                                                ).where(
                                                    LogEntry.task_id == tid,
                                                    LogEntry.task_retry_count
                                                    == history_observed.retry_count,
                                                    LogEntry.task_turn_generation
                                                    == history_observed.turn_generation,
                                                    LogEntry.event_type
                                                    != "user_message",
                                                )
                                            )
                                        ).all()
                                        local_entries = [
                                            dict(row._mapping)
                                            for row in local_rows
                                        ]
                                        missing = _missing_by_fingerprint(
                                            local_entries,
                                            remote_non_user,
                                        )
                                        for message in missing:
                                            db.add(
                                                LogEntry(
                                                    instance_id=None,
                                                    task_id=tid,
                                                    task_retry_count=(
                                                        history_observed.retry_count
                                                    ),
                                                    task_turn_generation=(
                                                        history_observed.turn_generation
                                                    ),
                                                    native_turn_id=(
                                                        _validated_native_turn_id(
                                                            message
                                                        )
                                                    ),
                                                    turn_scope=(
                                                        _validated_turn_scope(
                                                            message
                                                        )
                                                    ),
                                                    actual_transport=(
                                                        _validated_actual_transport(
                                                            message,
                                                            _validated_turn_scope(
                                                                message
                                                            ),
                                                        )
                                                    ),
                                                    event_type=(
                                                        message.get("event_type")
                                                        or "message"
                                                    ),
                                                    role=message.get("role"),
                                                    content=message.get("content"),
                                                    tool_name=message.get(
                                                        "tool_name"
                                                    ),
                                                    tool_input=message.get(
                                                        "tool_input"
                                                    ),
                                                    tool_output=message.get(
                                                        "tool_output"
                                                    ),
                                                    raw_json=message.get(
                                                        "raw_json"
                                                    ),
                                                    is_error=message.get(
                                                        "is_error",
                                                        False,
                                                    ),
                                                    loop_iteration=message.get(
                                                        "loop_iteration"
                                                    ),
                                                )
                                            )
                                        await db.commit()
                                        history_synced.add(tid)
                                        if missing:
                                            logger.info(
                                                "backfilled %d log entries for "
                                                "task %s",
                                                len(missing),
                                                tid,
                                            )

                    if not sync_status:
                        continue

                    # The status request gets its own pre-request observation.
                    # Never re-read the current Task only after the network
                    # response: that would let an old response borrow a newer
                    # local/Worker assignment.
                    status_observed = await self._observe_task_generation(
                        worker.id,
                        tid,
                    )
                    if status_observed is None:
                        continue
                    remote_task = await self._fetch_task_snapshot(
                        worker,
                        tid,
                        status_observed.incarnation_id,
                        client=client,
                    )
                    if remote_task is None:
                        continue
                    snapshot_handoff_id = (
                        await self._launched_handoff_id_for_snapshot(
                            worker,
                            status_observed,
                            remote_task,
                            client=client,
                        )
                    )
                    async with self.db_factory() as db:
                        resulting = await apply_authoritative_worker_task(
                            db,
                            status_observed,
                            remote_task,
                            worker_turn_handoff_id=snapshot_handoff_id,
                        )
                    generation_advanced = bool(
                        resulting is not None
                        and (
                            resulting.retry_count
                            != status_observed.retry_count
                            or resulting.turn_generation
                            != status_observed.turn_generation
                        )
                    )
                    if generation_advanced:
                        # A recovery snapshot can be the first exact evidence
                        # for a reserved G+1.  The history request above was
                        # deliberately fenced to G, so immediately re-read the
                        # same Worker history under the adopted identity before
                        # releasing the operation lock.  Otherwise a completed
                        # G+1 could clear the marker and never be selected by a
                        # later recovery pass, permanently losing its tail.
                        exact_synced = await (
                            self._backfill_missing_logs_with_operation_lock(
                                worker,
                                {tid},
                                sync_status=False,
                                deferred_completions=deferred_completions,
                            )
                        )
                        if tid in exact_synced:
                            history_synced.add(tid)
                            # Exact history may have cleared the durable
                            # handoff marker.  Completion publication must use
                            # that post-commit generation, otherwise its CAS
                            # still expects the now-removed marker and silently
                            # loses the terminal notification.
                            resulting = await self._observe_task_generation(
                                worker.id,
                                tid,
                            )
                        else:
                            history_synced.discard(tid)
                    if (
                        resulting is not None
                        and resulting.status != status_observed.status
                    ):
                        published = await self._publish_status_generation(
                            resulting,
                            notify_completion=False,
                        )
                        if published and deferred_completions is not None:
                            deferred_completions.append(resulting)
                    elif (
                        resulting is not None
                        and resulting.pty_background_generation
                        != status_observed.pty_background_generation
                    ):
                        published = await self._publish_background_generation(
                            resulting,
                            channels=("tasks", f"task:{tid}"),
                            notify_completion=False,
                        )
                        if published and deferred_completions is not None:
                            deferred_completions.append(resulting)
                    elif (
                        generation_advanced
                        and tid in history_synced
                        and deferred_completions is not None
                    ):
                        # A same-status completed G -> G+1 recovery has no
                        # status/background publication to trigger the Manager
                        # PR finalizer. Exact history synchronization is still
                        # a complete terminal notification boundary.
                        deferred_completions.append(resulting)
                except Exception:
                    logger.exception("backfill task %s from worker %s failed", tid, worker.id)
        return history_synced
