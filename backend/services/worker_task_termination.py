"""Durable exact-generation Manager/Worker Task termination protocol.

The Manager and Worker have independent databases.  Each side therefore owns
one :class:`WorkerTaskTerminationReceipt` with the same random operation id.
The Manager row is committed before the first network request; the Worker row
is committed before queue/process side effects.  Recovery always reads the
remote receipt before it retries the idempotent PUT and never replays a public
``cancel``/``stop-session`` mutation blindly.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable

from sqlalchemy import and_, exists, false, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from backend.models.instance import Instance
from backend.models.log_entry import LogEntry
from backend.models.task import Task
from backend.models.worker import Worker
from backend.models.worker_task_termination import (
    MANAGER_ACTIVE_TASK_TERMINATION_STATUSES,
    MANAGER_TASK_TERMINATION_STATUSES,
    WORKER_TASK_TERMINATION_SOURCE_STATUSES,
    WORKER_ACTIVE_TASK_TERMINATION_STATUSES,
    WORKER_TASK_TERMINATION_STATUSES,
    WorkerTaskTerminationReceipt,
)
from backend.models.worker_turn_handoff import WorkerTurnHandoffReceipt
from backend.services.pr_review_runtime import is_pr_sandbox_task
from backend.services.cancellation import finish_awaitable, settle_awaitable
from backend.services.skill_context import is_worker_managed_task_metadata
from backend.services.worker_node_control import (
    fence_worker_node_receipt_resolution,
    require_worker_node_destroy_cleanup_claim,
)

logger = logging.getLogger(__name__)

WORKER_DESTROY_DRAIN_CLAIM_HEADER = "X-CCM-Worker-Drain-Claim"
WORKER_DESTROY_TASK_INCARCATION_HEADER = "X-CCM-Destroy-Task-Incarnation"
WORKER_DESTROY_TASK_RETRY_HEADER = "X-CCM-Destroy-Task-Retry"
WORKER_DESTROY_TASK_TURN_HEADER = "X-CCM-Destroy-Task-Turn"

TERMINAL_TASK_STATUSES = frozenset(
    {"completed", "failed", "cancelled", "conflict"}
)
_ACTIVE_TASK_STATUSES = frozenset(
    {"pending", "in_progress", "executing", "merging"}
)
_VALID_OPERATIONS = frozenset({"cancel", "stop_session", "supersede"})
_MANAGER_ONLY_DELETE_OPERATION = "delete"
WORKER_TASK_PLAN_DELETE_PROTOCOL_VERSION = 1
_OPERATION_SOURCE_STATUSES = {
    "cancel": _ACTIVE_TASK_STATUSES | TERMINAL_TASK_STATUSES,
    "stop_session": frozenset({"pending", "in_progress", "executing"})
    | TERMINAL_TASK_STATUSES,
    # Internal PR/Code Review generations use the same complete process stop
    # as stop-session, but retain a distinct durable intent so crash recovery
    # cannot confuse an administrator stop with a supersede replacement.
    "supersede": _ACTIVE_TASK_STATUSES | TERMINAL_TASK_STATUSES,
}
_RECEIPT_NOT_FOUND = "receipt_not_found"
_TASK_NOT_FOUND = "task_not_found"
_MAX_ERROR_LENGTH = 4000
_INITIAL_RECONCILE_SECONDS = 1.0
_MAX_RECONCILE_SECONDS = 30.0
_WORKER_EXECUTION_LEASE_SECONDS = 90.0
_WORKER_EXECUTION_HEARTBEAT_SECONDS = 20.0
_WORKER_EXECUTION_HEARTBEAT_RETRY_SECONDS = 1.0
_REQUEST_KEYS = frozenset(
    {
        "version",
        "operation_id",
        "task_id",
        "operation",
        "manager_worker_id",
        "expected_remote",
        "manager_handoff",
    }
)
_EXPECTED_REMOTE_KEYS = frozenset(
    {"status", "retry_count", "turn_generation"}
)
_MANAGER_HANDOFF_KEYS = frozenset(
    {
        "handoff_id",
        "worker_id",
        "retry_count",
        "from_generation",
        "source_log_id",
        "acknowledged",
    }
)
_KNOWN_TASK_STATUSES = frozenset(WORKER_TASK_TERMINATION_SOURCE_STATUSES)
_RECEIPT_WIRE_KEYS = frozenset(
    {
        "version",
        "operation_id",
        "task_id",
        "side",
        "worker_id",
        "operation",
        "status",
        "state_version",
        "source",
        "request_payload",
        "request_digest",
        "result_payload",
        "result_digest",
        "attempt_count",
        "reconcile_count",
        "last_error",
        "accepted_at",
        "completed_at",
        "ack_intent_at",
        "acknowledged_at",
        "created_at",
        "updated_at",
    }
)
_SOURCE_WIRE_KEYS = frozenset(
    {
        "incarnation_id",
        "status",
        "retry_count",
        "turn_generation",
        "source_log_id",
        "instance_id",
        "started_at",
        "completed_at",
        "session_id",
        "pty_background_generation",
    }
)
_RESULT_SUCCESS_KEYS = frozenset(
    {
        "version",
        "operation_id",
        "task_id",
        "operation",
        "request_digest",
        "task",
        "response",
    }
)
_RESULT_REJECTED_KEYS = frozenset(
    {
        "version",
        "operation_id",
        "task_id",
        "operation",
        "request_digest",
        "rejected",
        "error",
    }
)
_RESULT_TASK_KEYS = frozenset(
    {
        "id",
        "status",
        "retry_count",
        "turn_generation",
        "instance_id",
        "started_at",
        "completed_at",
        "session_id",
        "error_message",
        "background_active",
    }
)


class WorkerTaskTerminationError(RuntimeError):
    """Base class for durable termination protocol failures."""


class WorkerTaskTerminationConflict(WorkerTaskTerminationError):
    """The receipt or Task no longer has the exact frozen identity."""


class WorkerTaskTerminationPending(WorkerTaskTerminationError):
    """The operation is durable but has not reached an authoritative result."""


@dataclass(frozen=True)
class ManagerTerminationOutcome:
    operation_id: str
    operation: str
    status: str
    result_payload: dict | None


@dataclass(frozen=True)
class ManagerTaskDeleteOutcome:
    """A durable Worker delete whose Manager graph commit completed."""

    operation_id: str
    task_id: int
    worker_id: int
    plan_ids: tuple[int, ...]


_DELETE_REQUEST_KEYS = frozenset(
    {
        "version",
        "operation_id",
        "task_id",
        "operation",
        "manager_worker_id",
        "source",
        "plan_ids",
        "plan_cascade_protocol",
    }
)
_DELETE_SOURCE_KEYS = frozenset(
    {
        "incarnation_id",
        "status",
        "retry_count",
        "turn_generation",
        "source_log_id",
        "instance_id",
        "started_at",
        "completed_at",
        "session_id",
        "pty_background_generation",
        "worker_turn_handoff_id",
        "worker_turn_handoff_worker_id",
        "worker_turn_handoff_retry_count",
        "worker_turn_handoff_from_generation",
        "worker_turn_handoff_source_log_id",
        "worker_turn_handoff_acknowledged",
    }
)
_DELETE_RESULT_KEYS = frozenset(
    {
        "version",
        "operation_id",
        "task_id",
        "operation",
        "request_digest",
        "proof_kind",
        "plan_cascade_protocol",
        "deleted_plan_ids",
        "remaining_target_plan_ids",
        "task_exists",
    }
)


@dataclass(frozen=True)
class _WorkerTerminationExecutionFence:
    task_id: int
    operation_id: str
    operation: str
    request_digest: str
    execution_token: str
    state_version: int
    lease_expires_at: datetime
    source_task_incarnation_id: str | None
    source_task_status: str
    source_task_retry_count: int
    source_task_turn_generation: int
    accepted_at: datetime | None
    created_at: datetime


def canonical_json_digest(payload: object) -> str:
    """Hash one strict, portable JSON value."""

    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _utc_naive(value: datetime | None) -> datetime | None:
    if value is None or value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _wire_datetime(value: datetime | None) -> str | None:
    value = _utc_naive(value)
    return value.isoformat(timespec="microseconds") if value else None


def _timeline_now(*previous: datetime | None) -> datetime:
    """Advance a receipt timeline despite remote skew or local NTP rollback."""

    values = [datetime.utcnow()]
    values.extend(value for value in previous if value is not None)
    return max(_utc_naive(value) for value in values if value is not None)


def _parse_wire_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise WorkerTaskTerminationConflict("invalid receipt datetime")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise WorkerTaskTerminationConflict("invalid receipt datetime") from exc
    return _utc_naive(parsed)


def no_active_worker_task_termination_predicate(
    *,
    allow_operation_id: str | None = None,
):
    """SQL gate shared by Task SELECT and final claim CAS paths.

    Receipt-owned cleanup may explicitly name its exact operation id.  In that
    case the predicate still rejects every different active receipt instead of
    disabling arbitration wholesale.
    """

    active = select(WorkerTaskTerminationReceipt.operation_id).where(
        WorkerTaskTerminationReceipt.active_task_id == Task.id
    )
    if allow_operation_id is not None:
        active = active.where(
            WorkerTaskTerminationReceipt.operation_id != allow_operation_id
        )
    return ~exists(active)


_RUNTIME_IDENTITY_UNSET = object()


def worker_task_runtime_persistence_predicate(
    *,
    task_retry_count: int | None,
    task_turn_generation: int | None,
    instance_id: int | None = None,
    session_id: str | None = None,
    pty_background_generation: str | None | object = _RUNTIME_IDENTITY_UNSET,
):
    """Allow only one exact pre-stop generation to finish output persistence.

    An active ``stop_session`` receipt freezes the Worker's local Task
    incarnation, retry, turn, Instance and optional PTY epoch before effects.
    Phase-one destroy may let callbacks matching that complete identity append
    their final audit/output while the stop consumer drains.  No predicate in
    this exception is based on ``task_id`` alone, and it never grants launch,
    status-generation advancement, or a different termination operation.
    """

    if (
        type(task_retry_count) is not int
        or type(task_turn_generation) is not int
    ):
        return no_active_worker_task_termination_predicate()
    exact = [
        WorkerTaskTerminationReceipt.active_task_id == Task.id,
        WorkerTaskTerminationReceipt.side == "worker",
        WorkerTaskTerminationReceipt.operation == "stop_session",
        WorkerTaskTerminationReceipt.status.in_(("accepted", "executing")),
        or_(
            and_(
                WorkerTaskTerminationReceipt.source_task_incarnation_id.is_(None),
                Task.incarnation_id.is_(None),
            ),
            WorkerTaskTerminationReceipt.source_task_incarnation_id
            == Task.incarnation_id,
        ),
        WorkerTaskTerminationReceipt.source_task_retry_count
        == task_retry_count,
        WorkerTaskTerminationReceipt.source_task_turn_generation
        == task_turn_generation,
    ]
    if instance_id is not None:
        exact.append(
            WorkerTaskTerminationReceipt.source_task_instance_id == instance_id
        )
    if session_id is not None:
        exact.append(
            WorkerTaskTerminationReceipt.source_task_session_id == session_id
        )
    if pty_background_generation is not _RUNTIME_IDENTITY_UNSET:
        exact.append(
            (
                WorkerTaskTerminationReceipt
                .source_task_pty_background_generation.is_(None)
                if pty_background_generation is None
                else WorkerTaskTerminationReceipt
                .source_task_pty_background_generation
                == pty_background_generation
            )
        )
    return or_(
        no_active_worker_task_termination_predicate(),
        exists(
            select(WorkerTaskTerminationReceipt.operation_id).where(*exact)
        ),
    )


def worker_task_termination_authority_predicate(
    *,
    operation_id: str | None,
    operation: str | None = None,
    execution_token: str | None = None,
    state_version: int | None = None,
    lease_valid_at: datetime | None = None,
):
    """Require no receipt for ordinary writes or one exact live Worker owner.

    Exact callers must supply a Python timestamp captured *after* acquiring the
    Task/receipt (and, where relevant, Instance) locks for their transaction.
    Database ``CURRENT_TIMESTAMP`` is deliberately unsuitable here: PostgreSQL
    exposes the transaction start time, and a bind captured before a lock wait
    can likewise let an expired executor pass after it finally acquires the
    row.
    """

    if operation_id is None:
        if any(
            value is not None
            for value in (operation, execution_token, state_version)
        ):
            return false()
        return no_active_worker_task_termination_predicate()
    if (
        operation not in _VALID_OPERATIONS
        or execution_token is None
        or state_version is None
        or lease_valid_at is None
    ):
        return false()
    predicates = [
        WorkerTaskTerminationReceipt.active_task_id == Task.id,
        WorkerTaskTerminationReceipt.operation_id == operation_id,
        WorkerTaskTerminationReceipt.side == "worker",
        WorkerTaskTerminationReceipt.status == "executing",
        WorkerTaskTerminationReceipt.operation == operation,
        WorkerTaskTerminationReceipt.execution_token == execution_token,
        WorkerTaskTerminationReceipt.state_version == state_version,
        WorkerTaskTerminationReceipt.next_reconcile_at.is_not(None),
        WorkerTaskTerminationReceipt.next_reconcile_at
        > _utc_naive(lease_valid_at),
    ]
    return exists(
        select(WorkerTaskTerminationReceipt.operation_id).where(*predicates)
    )


def worker_task_termination_authority_matches(
    receipt: WorkerTaskTerminationReceipt | None,
    *,
    operation_id: str | None,
    operation: str | None = None,
    execution_token: str | None = None,
    state_version: int | None = None,
    lease_valid_at: datetime | None = None,
) -> bool:
    """Validate the same authority contract against one locked receipt row."""

    if operation_id is None:
        return bool(
            operation is None
            and execution_token is None
            and state_version is None
            and receipt is None
        )
    if (
        receipt is None
        or operation not in _VALID_OPERATIONS
        or execution_token is None
        or state_version is None
        or lease_valid_at is None
        or receipt.next_reconcile_at is None
    ):
        return False
    return bool(
        receipt.operation_id == operation_id
        and receipt.side == "worker"
        and receipt.status == "executing"
        and receipt.operation == operation
        and receipt.execution_token == execution_token
        and receipt.state_version == state_version
        and receipt.active_task_id == receipt.task_id
        and _utc_naive(receipt.next_reconcile_at)
        > _utc_naive(lease_valid_at)
    )


def local_task_termination_effect_authority_matches(
    task: Task | None,
    receipt: WorkerTaskTerminationReceipt | None,
    *,
    operation_id: str | None,
    operation: str | None = None,
    execution_token: str | None = None,
    state_version: int | None = None,
    lease_valid_at: datetime | None = None,
) -> bool:
    """Fence Worker-managed local copies behind an exact durable receipt.

    Ordinary Manager-local Tasks retain the historical no-receipt authority.
    A Task imported onto a Worker is locally shaped the same way, so its
    durable metadata marker is the additional proof that direct public
    cancel/stop requests must fail closed instead of bypassing Manager
    reconciliation.
    """

    if task is None:
        return False
    if is_worker_managed_task_metadata(task.metadata_) and operation_id is None:
        return False
    return worker_task_termination_authority_matches(
        receipt,
        operation_id=operation_id,
        operation=operation,
        execution_token=execution_token,
        state_version=state_version,
        lease_valid_at=lease_valid_at,
    )


async def active_worker_task_termination_receipt(
    db: AsyncSession,
    task_id: int,
    *,
    for_update: bool = False,
) -> WorkerTaskTerminationReceipt | None:
    stmt = (
        select(WorkerTaskTerminationReceipt)
        .where(WorkerTaskTerminationReceipt.active_task_id == task_id)
        .execution_options(populate_existing=True)
    )
    if for_update:
        stmt = stmt.with_for_update()
    rows = list((await db.execute(stmt)).scalars())
    if len(rows) > 1:
        raise WorkerTaskTerminationConflict(
            f"Task {task_id} has multiple active termination receipts"
        )
    return rows[0] if rows else None


async def ensure_no_active_worker_task_termination(
    db: AsyncSession,
    task_id: int,
    *,
    allow_operation_id: str | None = None,
) -> None:
    receipt = await active_worker_task_termination_receipt(db, task_id)
    if receipt is not None and receipt.operation_id != allow_operation_id:
        raise WorkerTaskTerminationConflict(
            f"Task {task_id} has active Worker termination operation "
            f"{receipt.operation_id}"
        )


async def manager_receipt_allows_authoritative_apply(
    db: AsyncSession,
    task_id: int,
    operation_id: str | None,
) -> bool:
    """Return whether a Worker snapshot belongs to the active receipt owner."""

    active = await active_worker_task_termination_receipt(db, task_id)
    if active is None:
        return operation_id is None
    return bool(
        operation_id is not None
        and active.operation_id == operation_id
        and active.side == "manager"
        and active.status in MANAGER_ACTIVE_TASK_TERMINATION_STATUSES
    )


def _task_source_values(task: Task, *, manager_side: bool) -> dict:
    values = {
        "source_task_incarnation_id": task.incarnation_id,
        "source_task_status": task.status,
        "source_task_retry_count": task.retry_count,
        "source_task_turn_generation": task.turn_generation,
        "source_task_source_log_id": task.turn_source_log_id,
        "source_task_instance_id": task.instance_id,
        "source_task_started_at": _utc_naive(task.started_at),
        "source_task_completed_at": _utc_naive(task.completed_at),
        "source_task_session_id": task.session_id,
        "source_task_pty_background_generation": (
            task.pty_background_generation
        ),
    }
    if manager_side and task.worker_turn_handoff_id is not None:
        values.update(
            source_worker_turn_handoff_id=task.worker_turn_handoff_id,
            source_worker_turn_handoff_worker_id=(
                task.worker_turn_handoff_worker_id
            ),
            source_worker_turn_handoff_retry_count=(
                task.worker_turn_handoff_retry_count
            ),
            source_worker_turn_handoff_from_generation=(
                task.worker_turn_handoff_from_generation
            ),
            source_worker_turn_handoff_source_log_id=(
                task.worker_turn_handoff_source_log_id
            ),
            source_worker_turn_handoff_acknowledged=(
                task.worker_turn_handoff_acknowledged
            ),
        )
    else:
        values.update(
            source_worker_turn_handoff_id=None,
            source_worker_turn_handoff_worker_id=None,
            source_worker_turn_handoff_retry_count=None,
            source_worker_turn_handoff_from_generation=None,
            source_worker_turn_handoff_source_log_id=None,
            source_worker_turn_handoff_acknowledged=None,
        )
    return values


def _delete_source_payload_from_task(task: Task) -> dict:
    """Freeze every Task field that may distinguish the Manager mirror."""

    return {
        "incarnation_id": task.incarnation_id,
        "status": task.status,
        "retry_count": task.retry_count,
        "turn_generation": task.turn_generation,
        "source_log_id": task.turn_source_log_id,
        "instance_id": task.instance_id,
        "started_at": _wire_datetime(task.started_at),
        "completed_at": _wire_datetime(task.completed_at),
        "session_id": task.session_id,
        "pty_background_generation": task.pty_background_generation,
        "worker_turn_handoff_id": task.worker_turn_handoff_id,
        "worker_turn_handoff_worker_id": task.worker_turn_handoff_worker_id,
        "worker_turn_handoff_retry_count": task.worker_turn_handoff_retry_count,
        "worker_turn_handoff_from_generation": (
            task.worker_turn_handoff_from_generation
        ),
        "worker_turn_handoff_source_log_id": (
            task.worker_turn_handoff_source_log_id
        ),
        "worker_turn_handoff_acknowledged": (
            task.worker_turn_handoff_acknowledged
        ),
    }


def _canonical_positive_ids(value: object) -> tuple[int, ...] | None:
    if not isinstance(value, list):
        return None
    ids: list[int] = []
    for item in value:
        if type(item) is not int or item <= 0:
            return None
        ids.append(item)
    if ids != sorted(set(ids)):
        return None
    return tuple(ids)


def _manager_delete_request_payload(
    task: Task,
    *,
    operation_id: str,
    plan_ids: tuple[int, ...],
) -> dict:
    return {
        "version": 1,
        "operation_id": operation_id,
        "task_id": task.id,
        "operation": _MANAGER_ONLY_DELETE_OPERATION,
        "manager_worker_id": task.worker_id,
        "source": _delete_source_payload_from_task(task),
        "plan_ids": list(plan_ids),
        "plan_cascade_protocol": WORKER_TASK_PLAN_DELETE_PROTOCOL_VERSION,
    }


def _manager_delete_request_is_valid(
    receipt: WorkerTaskTerminationReceipt,
) -> bool:
    payload = receipt.request_payload
    if (
        not isinstance(payload, dict)
        or set(payload) != _DELETE_REQUEST_KEYS
        or payload.get("version") != 1
        or payload.get("operation_id") != receipt.operation_id
        or payload.get("task_id") != receipt.task_id
        or payload.get("operation") != _MANAGER_ONLY_DELETE_OPERATION
        or payload.get("manager_worker_id") != receipt.worker_id
        or payload.get("plan_cascade_protocol")
        != WORKER_TASK_PLAN_DELETE_PROTOCOL_VERSION
        or _canonical_positive_ids(payload.get("plan_ids")) is None
        or not isinstance(payload.get("source"), dict)
        or set(payload["source"]) != _DELETE_SOURCE_KEYS
        or not _valid_digest(receipt.request_digest)
    ):
        return False
    try:
        if canonical_json_digest(payload) != receipt.request_digest:
            return False
    except (TypeError, ValueError, UnicodeError):
        return False
    source = payload["source"]
    frozen_source = {
        "incarnation_id": receipt.source_task_incarnation_id,
        "status": receipt.source_task_status,
        "retry_count": receipt.source_task_retry_count,
        "turn_generation": receipt.source_task_turn_generation,
        "source_log_id": receipt.source_task_source_log_id,
        "instance_id": receipt.source_task_instance_id,
        "started_at": _wire_datetime(receipt.source_task_started_at),
        "completed_at": _wire_datetime(receipt.source_task_completed_at),
        "session_id": receipt.source_task_session_id,
        "pty_background_generation": (
            receipt.source_task_pty_background_generation
        ),
        "worker_turn_handoff_id": receipt.source_worker_turn_handoff_id,
        "worker_turn_handoff_worker_id": (
            receipt.source_worker_turn_handoff_worker_id
        ),
        "worker_turn_handoff_retry_count": (
            receipt.source_worker_turn_handoff_retry_count
        ),
        "worker_turn_handoff_from_generation": (
            receipt.source_worker_turn_handoff_from_generation
        ),
        "worker_turn_handoff_source_log_id": (
            receipt.source_worker_turn_handoff_source_log_id
        ),
        "worker_turn_handoff_acknowledged": (
            receipt.source_worker_turn_handoff_acknowledged
        ),
    }
    return bool(source == frozen_source)


def manager_delete_receipt_plan_ids(
    receipt: WorkerTaskTerminationReceipt,
) -> tuple[int, ...]:
    """Return the digest-bound Plan identity or fail closed."""

    if not _manager_delete_request_is_valid(receipt):
        raise WorkerTaskTerminationConflict(
            "Manager Task deletion request identity is invalid"
        )
    plan_ids = _canonical_positive_ids(receipt.request_payload.get("plan_ids"))
    if plan_ids is None:  # Kept explicit for type narrowing and corruption.
        raise WorkerTaskTerminationConflict(
            "Manager Task deletion Plan identity is invalid"
        )
    return plan_ids


def manager_delete_receipt_task_fence(
    receipt: WorkerTaskTerminationReceipt,
) -> tuple[
    str,
    int | None,
    int,
    int | None,
    datetime | None,
    datetime | None,
    str | None,
    int,
]:
    """Rebuild the exact TaskQueue delete fence from durable columns."""

    if not _manager_delete_request_is_valid(receipt):
        raise WorkerTaskTerminationConflict(
            "Manager Task deletion request identity is invalid"
        )
    return (
        receipt.source_task_status,
        receipt.worker_id,
        receipt.source_task_retry_count,
        receipt.source_task_instance_id,
        receipt.source_task_started_at,
        receipt.source_task_completed_at,
        receipt.source_task_pty_background_generation,
        receipt.source_task_turn_generation,
    )


def manager_delete_receipt_allows_finalize(
    receipt: WorkerTaskTerminationReceipt | None,
    task: Task,
    *,
    operation_id: str,
    plan_ids: tuple[int, ...],
) -> bool:
    """Validate the exact active owner and its committed remote proof."""

    if (
        receipt is None
        or receipt.operation_id != operation_id
        or receipt.task_id != task.id
        or receipt.active_task_id != task.id
        or receipt.side != "manager"
        or receipt.operation != _MANAGER_ONLY_DELETE_OPERATION
        or receipt.status != "awaiting_ack"
        or receipt.worker_id != task.worker_id
        or not _receipt_source_matches_task(
            receipt,
            task,
            include_manager_handoff=True,
        )
        or not _manager_delete_request_is_valid(receipt)
        or receipt.result_payload is None
        or not _valid_digest(receipt.result_digest)
    ):
        return False
    try:
        result_valid = bool(
            set(receipt.result_payload) == _DELETE_RESULT_KEYS
            and canonical_json_digest(receipt.result_payload)
            == receipt.result_digest
            and receipt.result_payload.get("version") == 1
            and receipt.result_payload.get("operation_id") == operation_id
            and receipt.result_payload.get("task_id") == task.id
            and receipt.result_payload.get("operation")
            == _MANAGER_ONLY_DELETE_OPERATION
            and receipt.result_payload.get("request_digest")
            == receipt.request_digest
            and receipt.result_payload.get("proof_kind")
            in {"delete_receipt", "delete_audit"}
            and receipt.result_payload.get("plan_cascade_protocol")
            == WORKER_TASK_PLAN_DELETE_PROTOCOL_VERSION
            and receipt.result_payload.get("task_exists") is False
            and receipt.result_payload.get("remaining_target_plan_ids") == []
        )
    except (TypeError, ValueError, UnicodeError):
        return False
    return bool(
        result_valid
        and manager_delete_receipt_plan_ids(receipt) == plan_ids
        and _canonical_positive_ids(
            receipt.result_payload.get("deleted_plan_ids")
        )
        == plan_ids
    )


def _receipt_source_matches_task(
    receipt: WorkerTaskTerminationReceipt,
    task: Task,
    *,
    include_manager_handoff: bool,
) -> bool:
    expected = _task_source_values(task, manager_side=include_manager_handoff)
    return all(getattr(receipt, key) == value for key, value in expected.items())


def _receipt_logical_source_matches_task(
    receipt: WorkerTaskTerminationReceipt,
    task: Task,
) -> bool:
    """Match a Worker generation while allowing its existing process to finish."""

    return bool(
        receipt.task_id == task.id
        and task.worker_id is None
        and task.shared_from_id is None
        and receipt.source_task_incarnation_id == task.incarnation_id
        and receipt.source_task_retry_count == task.retry_count
        and receipt.source_task_turn_generation == task.turn_generation
    )


def _receipt_result_matches_task(
    receipt: WorkerTaskTerminationReceipt,
    task: Task,
) -> bool:
    payload = receipt.result_payload
    snapshot = payload.get("task") if isinstance(payload, dict) else None
    try:
        result_digest_valid = bool(
            isinstance(payload, dict)
            and _valid_digest(receipt.result_digest)
            and canonical_json_digest(payload) == receipt.result_digest
            and _valid_result_payload_identity(
                payload,
                {
                    "operation_id": receipt.operation_id,
                    "task_id": receipt.task_id,
                    "operation": receipt.operation,
                    "request_digest": receipt.request_digest,
                    "request_payload": receipt.request_payload,
                },
            )
        )
    except (TypeError, ValueError, UnicodeError):
        result_digest_valid = False
    if (
        not result_digest_valid
        or not isinstance(snapshot, dict)
        or type(snapshot.get("background_active")) is not bool
    ):
        return False
    # Worker instance/log/session ids and wall-clock values are node-local.
    # In particular, MySQL may persist Manager DATETIME without the Worker's
    # microseconds.  Resume an awaiting-ACK operation only through portable
    # logical identity plus this Manager-local Task incarnation.
    return bool(
        receipt.task_id == task.id
        and receipt.source_task_incarnation_id == task.incarnation_id
        and payload.get("version") == 2
        and payload.get("operation_id") == receipt.operation_id
        and payload.get("task_id") == receipt.task_id
        and payload.get("operation") == receipt.operation
        and payload.get("request_digest") == receipt.request_digest
        and snapshot.get("id") == task.id
        and snapshot.get("status") == task.status
        and snapshot.get("retry_count") == task.retry_count
        and snapshot.get("turn_generation") == task.turn_generation
        and snapshot.get("background_active")
        == (task.pty_background_generation is not None)
    )


def _manager_request_payload(
    task: Task,
    *,
    operation_id: str,
    operation: str,
) -> dict:
    handoff = None
    if task.worker_turn_handoff_id is not None:
        handoff = {
            "handoff_id": task.worker_turn_handoff_id,
            "worker_id": task.worker_turn_handoff_worker_id,
            "retry_count": task.worker_turn_handoff_retry_count,
            "from_generation": task.worker_turn_handoff_from_generation,
            "source_log_id": task.worker_turn_handoff_source_log_id,
            "acknowledged": task.worker_turn_handoff_acknowledged,
        }
    return {
        "version": 2,
        "operation_id": operation_id,
        "task_id": task.id,
        "operation": operation,
        "manager_worker_id": task.worker_id,
        "expected_remote": {
            # Only logical identity crosses CCM databases.  Instance ids,
            # incarnation ids, source logs, sessions and PTY epochs are
            # deliberately node-local; the Worker freezes those exact fields
            # in its own receipt after this logical fence matches.
            "status": task.status,
            "retry_count": task.retry_count,
            "turn_generation": task.turn_generation,
        },
        "manager_handoff": handoff,
    }


def _valid_operation_id(operation_id: object) -> bool:
    return bool(
        isinstance(operation_id, str)
        and len(operation_id) == 32
        and all(char in "0123456789abcdef" for char in operation_id)
    )


def _valid_digest(digest: object) -> bool:
    return bool(
        isinstance(digest, str)
        and len(digest) == 64
        and all(char in "0123456789abcdef" for char in digest)
    )


def _valid_nonnegative_int(value: object) -> bool:
    return type(value) is int and value >= 0


def _valid_positive_int(value: object) -> bool:
    return type(value) is int and value > 0


def _validate_request_payload(
    *,
    task_id: int,
    operation_id: str,
    operation: str,
    payload: object,
    digest: str,
) -> dict:
    if not isinstance(payload, dict) or set(payload) != _REQUEST_KEYS:
        raise WorkerTaskTerminationConflict("invalid termination request shape")
    expected = payload["expected_remote"]
    handoff = payload["manager_handoff"]
    if (
        not _valid_positive_int(task_id)
        or not _valid_operation_id(operation_id)
        or operation not in _VALID_OPERATIONS
        or type(payload["version"]) is not int
        or payload["version"] != 2
        or payload["operation_id"] != operation_id
        or type(payload["task_id"]) is not int
        or payload["task_id"] != task_id
        or payload["operation"] != operation
        or not _valid_positive_int(payload["manager_worker_id"])
        or not isinstance(expected, dict)
        or set(expected) != _EXPECTED_REMOTE_KEYS
        or not isinstance(expected["status"], str)
        or expected["status"] not in _KNOWN_TASK_STATUSES
        or not _valid_nonnegative_int(expected["retry_count"])
        or not _valid_nonnegative_int(expected["turn_generation"])
        or not _valid_digest(digest)
    ):
        raise WorkerTaskTerminationConflict("invalid termination request identity")
    if handoff is not None:
        if (
            not isinstance(handoff, dict)
            or set(handoff) != _MANAGER_HANDOFF_KEYS
            or not _valid_operation_id(handoff["handoff_id"])
            or not _valid_positive_int(handoff["worker_id"])
            or handoff["worker_id"] != payload["manager_worker_id"]
            or not _valid_nonnegative_int(handoff["retry_count"])
            or handoff["retry_count"] != expected["retry_count"]
            or not _valid_nonnegative_int(handoff["from_generation"])
            or expected["turn_generation"]
            not in {
                handoff["from_generation"],
                handoff["from_generation"] + 1,
            }
            or not _valid_positive_int(handoff["source_log_id"])
            or type(handoff["acknowledged"]) is not bool
        ):
            raise WorkerTaskTerminationConflict(
                "invalid termination Manager handoff identity"
            )
    try:
        actual = canonical_json_digest(payload)
    except (TypeError, ValueError, UnicodeError) as exc:
        raise WorkerTaskTerminationConflict(
            "termination request is not canonical JSON"
        ) from exc
    if actual != digest:
        raise WorkerTaskTerminationConflict("termination request digest changed")
    return payload


def _receipt_request_matches_frozen_source(
    receipt: WorkerTaskTerminationReceipt,
) -> bool:
    """Verify that stored request evidence names the frozen source fields."""

    try:
        payload = _validate_request_payload(
            task_id=receipt.task_id,
            operation_id=receipt.operation_id,
            operation=receipt.operation,
            payload=receipt.request_payload,
            digest=receipt.request_digest,
        )
    except WorkerTaskTerminationConflict:
        return False
    expected = payload["expected_remote"]
    if expected != {
        "status": receipt.source_task_status,
        "retry_count": receipt.source_task_retry_count,
        "turn_generation": receipt.source_task_turn_generation,
    }:
        return False
    handoff = payload["manager_handoff"]
    frozen_handoff = (
        None
        if receipt.source_worker_turn_handoff_id is None
        else {
            "handoff_id": receipt.source_worker_turn_handoff_id,
            "worker_id": receipt.source_worker_turn_handoff_worker_id,
            "retry_count": receipt.source_worker_turn_handoff_retry_count,
            "from_generation": (
                receipt.source_worker_turn_handoff_from_generation
            ),
            "source_log_id": (
                receipt.source_worker_turn_handoff_source_log_id
            ),
            "acknowledged": (
                receipt.source_worker_turn_handoff_acknowledged
            ),
        }
    )
    return bool(
        payload["manager_worker_id"] == receipt.worker_id
        and handoff == frozen_handoff
    )


async def create_or_resume_manager_receipt(
    db: AsyncSession,
    task: Task,
    *,
    operation: str,
    destroy_claim: object | None = None,
) -> WorkerTaskTerminationReceipt:
    """Persist the Manager operation id before any Worker network request."""

    if operation not in _VALID_OPERATIONS:
        raise ValueError(f"unsupported Worker termination operation: {operation}")
    expected_worker_id = task.worker_id
    expected_incarnation_id = task.incarnation_id
    expected_retry_count = task.retry_count
    expected_turn_generation = task.turn_generation
    if type(expected_worker_id) is not int or task.shared_from_id is not None:
        raise WorkerTaskTerminationConflict(
            f"Task {task.id} is not Worker-authoritative"
        )

    # Global DB lock order is Task -> receipt.  Re-read the exact source after
    # acquiring the Task write lock instead of trusting the API identity map.
    task = (
        await db.execute(
            select(Task)
            .where(
                Task.id == task.id,
                Task.worker_id == expected_worker_id,
                Task.shared_from_id.is_(None),
                (
                    Task.incarnation_id.is_(None)
                    if expected_incarnation_id is None
                    else Task.incarnation_id == expected_incarnation_id
                ),
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if task is None:
        raise WorkerTaskTerminationConflict(
            "Manager Task changed before termination receipt admission"
        )
    if operation == "supersede" and not is_pr_sandbox_task(task):
        raise WorkerTaskTerminationConflict(
            "supersede termination is restricted to review sandbox Tasks"
        )

    # Serialize lifecycle admission in the same durable transaction.  The
    # global order is Task -> Worker -> termination receipt, matching Worker
    # destroy (all Tasks in id order -> Worker).  SELECT FOR UPDATE covers
    # PostgreSQL/MySQL; the conditional self-UPDATE is also a SQLite write/CAS
    # barrier.  Explicitly retaining ``updated_at`` prevents its Python
    # ``onupdate`` hook from invalidating an opaque destroy claim.
    if destroy_claim is None:
        # Existing exact receipts remain observable/reconcilable while a
        # Worker is temporarily stopping/error.  Lock by identity first and
        # require ``ready`` only if this call would allocate a new operation.
        worker_predicates = (Worker.id == expected_worker_id,)
    else:
        try:
            from backend.services.worker_proxy import (
                _worker_destroy_lifecycle_predicates,
            )

            if destroy_claim.worker_id != expected_worker_id:
                raise ValueError("destroy claim Worker changed")
            worker_predicates = _worker_destroy_lifecycle_predicates(
                destroy_claim
            )
        except (AttributeError, TypeError, ValueError) as exc:
            raise WorkerTaskTerminationConflict(
                "invalid Worker destroy lifecycle claim"
            ) from exc
    lifecycle_barrier = await db.execute(
        update(Worker)
        .where(*worker_predicates)
        .values(status=Worker.status, updated_at=Worker.updated_at)
    )
    if lifecycle_barrier.rowcount != 1:
        raise WorkerTaskTerminationConflict(
            "Worker lifecycle changed before termination receipt admission"
        )
    locked_worker = (
        await db.execute(
            select(Worker)
            .where(*worker_predicates)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if locked_worker is None or (
        destroy_claim is not None
        and locked_worker.auth_token != destroy_claim.auth_token
    ):
        raise WorkerTaskTerminationConflict(
            "Worker lifecycle identity changed before termination admission"
        )

    active = await active_worker_task_termination_receipt(
        db, task.id, for_update=True
    )
    if active is not None:
        if (
            active.side == "manager"
            and active.worker_id == task.worker_id
            and active.operation == operation
            and (
                _receipt_source_matches_task(
                    active, task, include_manager_handoff=True
                )
                or (
                    active.status == "awaiting_ack"
                    and _receipt_result_matches_task(active, task)
                )
            )
        ):
            # Admission locks are a short DB fence, never a network lease.
            # Reconciliation performs remote GET/PUT/ACK after this helper;
            # release Task+Worker+receipt locks before returning the durable id.
            await db.commit()
            return active
        raise WorkerTaskTerminationConflict(
            f"Task {task.id} already has a different active termination"
        )

    # A completed supersede is a durable fact about this exact review Task
    # generation.  Repeated PR monitor/recovery passes must reuse it rather
    # than allocate an unbounded series of logically identical operations.
    # The resulting Task snapshot, not the old source G, is authoritative: a
    # claimed handoff may have made the settled result G+1.
    if operation == "supersede":
        from backend.services.task_queue import (
            PR_REVIEW_SUPERSEDED_METADATA_KEY,
        )

        historical = list(
            (
                await db.execute(
                    select(WorkerTaskTerminationReceipt)
                    .where(
                        WorkerTaskTerminationReceipt.task_id == task.id,
                        WorkerTaskTerminationReceipt.side == "manager",
                        WorkerTaskTerminationReceipt.worker_id
                        == task.worker_id,
                        WorkerTaskTerminationReceipt.operation == "supersede",
                        WorkerTaskTerminationReceipt.status == "settled",
                        WorkerTaskTerminationReceipt.active_task_id.is_(None),
                    )
                    .order_by(
                        WorkerTaskTerminationReceipt.created_at.desc(),
                        WorkerTaskTerminationReceipt.operation_id.desc(),
                    )
                    .with_for_update()
                )
            ).scalars()
        )
        metadata = task.metadata_ if isinstance(task.metadata_, dict) else {}
        for settled in historical:
            if (
                metadata.get(PR_REVIEW_SUPERSEDED_METADATA_KEY) is True
                and _receipt_request_matches_frozen_source(settled)
                and _receipt_result_matches_task(settled, task)
            ):
                await db.commit()
                return settled
    if task.status not in _OPERATION_SOURCE_STATUSES[operation]:
        raise WorkerTaskTerminationConflict(
            f"Manager Task cannot run {operation} from {task.status}"
        )
    if destroy_claim is None and locked_worker.status != "ready":
        raise WorkerTaskTerminationConflict(
            "Worker is not ready for a new termination receipt"
        )
    if (
        task.retry_count != expected_retry_count
        or task.turn_generation != expected_turn_generation
    ):
        raise WorkerTaskTerminationConflict(
            "Manager Task changed before termination receipt admission"
        )

    operation_id = secrets.token_hex(16)
    payload = _manager_request_payload(
        task,
        operation_id=operation_id,
        operation=operation,
    )
    now = datetime.utcnow()
    receipt = WorkerTaskTerminationReceipt(
        operation_id=operation_id,
        task_id=task.id,
        active_task_id=task.id,
        side="manager",
        worker_id=task.worker_id,
        operation=operation,
        status="pending_remote",
        state_version=1,
        request_payload=payload,
        request_digest=canonical_json_digest(payload),
        attempt_count=0,
        reconcile_count=0,
        next_reconcile_at=now,
        created_at=now,
        updated_at=now,
        **_task_source_values(task, manager_side=True),
    )
    db.add(receipt)
    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise WorkerTaskTerminationConflict(
            f"Task {task.id} termination was admitted concurrently"
        ) from exc
    return receipt


async def stage_manager_task_delete_receipt(
    db: AsyncSession,
    task: Task,
    *,
    plan_ids: tuple[int, ...],
) -> WorkerTaskTerminationReceipt:
    """Stage one Manager-only delete owner in the caller's locked transaction.

    ``TaskQueue.delete`` calls this only after it has locked and validated the
    complete Task/Capability/Plan graph.  The caller commits this row together
    with that read/write fence and returns before any Worker request is made.
    """

    if (
        type(task.worker_id) is not int
        or task.worker_id <= 0
        or task.shared_from_id is not None
    ):
        raise WorkerTaskTerminationConflict(
            "Task is not an authoritative Worker mirror"
        )
    canonical_plan_ids = tuple(sorted(set(plan_ids)))
    if canonical_plan_ids != plan_ids or any(
        type(plan_id) is not int or plan_id <= 0
        for plan_id in canonical_plan_ids
    ):
        raise WorkerTaskTerminationConflict(
            "Task deletion Plan identity is not canonical"
        )
    if await active_worker_task_termination_receipt(
        db,
        task.id,
        for_update=True,
    ):
        raise WorkerTaskTerminationConflict(
            "Task already has an active Worker termination owner"
        )

    operation_id = secrets.token_hex(16)
    request_payload = _manager_delete_request_payload(
        task,
        operation_id=operation_id,
        plan_ids=canonical_plan_ids,
    )
    now = datetime.utcnow()
    receipt = WorkerTaskTerminationReceipt(
        operation_id=operation_id,
        task_id=task.id,
        active_task_id=task.id,
        side="manager",
        worker_id=task.worker_id,
        operation=_MANAGER_ONLY_DELETE_OPERATION,
        status="pending_remote",
        state_version=1,
        execution_token=None,
        request_payload=request_payload,
        request_digest=canonical_json_digest(request_payload),
        result_payload=None,
        result_digest=None,
        attempt_count=0,
        reconcile_count=0,
        next_reconcile_at=now,
        last_error=None,
        accepted_at=None,
        completed_at=None,
        ack_intent_at=None,
        acknowledged_at=None,
        created_at=now,
        updated_at=now,
        **_task_source_values(task, manager_side=True),
    )
    db.add(receipt)
    await db.flush()
    return receipt


def receipt_not_found_payload(task_id: int, operation_id: str) -> dict:
    return {
        "version": 2,
        "task_id": task_id,
        "operation_id": operation_id,
        "status": _RECEIPT_NOT_FOUND,
    }


def task_not_found_payload(task_id: int, operation_id: str) -> dict:
    return {
        "version": 2,
        "task_id": task_id,
        "operation_id": operation_id,
        "status": _TASK_NOT_FOUND,
    }


def _receipt_source_payload(receipt: WorkerTaskTerminationReceipt) -> dict:
    return {
        "incarnation_id": receipt.source_task_incarnation_id,
        "status": receipt.source_task_status,
        "retry_count": receipt.source_task_retry_count,
        "turn_generation": receipt.source_task_turn_generation,
        "source_log_id": receipt.source_task_source_log_id,
        "instance_id": receipt.source_task_instance_id,
        "started_at": _wire_datetime(receipt.source_task_started_at),
        "completed_at": _wire_datetime(receipt.source_task_completed_at),
        "session_id": receipt.source_task_session_id,
        "pty_background_generation": (
            receipt.source_task_pty_background_generation
        ),
    }


def _valid_wire_datetime(value: object, *, required: bool = False) -> bool:
    if value is None:
        return not required
    try:
        return _parse_wire_datetime(value) is not None
    except WorkerTaskTerminationConflict:
        return False


def _valid_result_payload_identity(
    result: object,
    wire: dict,
) -> bool:
    if not isinstance(result, dict):
        return False
    common = bool(
        type(result.get("version")) is int
        and result.get("version") == 2
        and result.get("operation_id") == wire.get("operation_id")
        and type(result.get("task_id")) is int
        and result.get("task_id") == wire.get("task_id")
        and result.get("operation") == wire.get("operation")
        and result.get("request_digest") == wire.get("request_digest")
    )
    if not common:
        return False
    if result.get("rejected") is True:
        return bool(
            set(result) == _RESULT_REJECTED_KEYS
            and isinstance(result.get("error"), str)
        )
    task = result.get("task")
    if (
        set(result) != _RESULT_SUCCESS_KEYS
        or not isinstance(result.get("response"), dict)
        or not isinstance(task, dict)
        or set(task) != _RESULT_TASK_KEYS
    ):
        return False
    task_shape_valid = bool(
        type(task.get("id")) is int
        and task.get("id") == wire.get("task_id")
        and task.get("status") in TERMINAL_TASK_STATUSES
        and _valid_nonnegative_int(task.get("retry_count"))
        and _valid_nonnegative_int(task.get("turn_generation"))
        and (
            task.get("instance_id") is None
            or _valid_positive_int(task.get("instance_id"))
        )
        and _valid_wire_datetime(task.get("started_at"))
        and _valid_wire_datetime(task.get("completed_at"))
        and (
            task.get("session_id") is None
            or isinstance(task.get("session_id"), str)
        )
        and (
            task.get("error_message") is None
            or isinstance(task.get("error_message"), str)
        )
        and task.get("background_active") is False
    )
    if not task_shape_valid:
        return False

    # The result Task is the authoritative resulting generation.  It need not
    # equal the Worker's source G: an already-claimed handoff may legitimately
    # make the termination settle G+1.  It must, however, be tied to the exact
    # digest-bound handoff identity.  Without a handoff there is no authority
    # to advance generations at all.
    request = wire.get("request_payload")
    if not isinstance(request, dict):
        return False
    expected = request.get("expected_remote")
    handoff = request.get("manager_handoff")
    if not isinstance(expected, dict):
        return False
    if handoff is None:
        return bool(
            task.get("retry_count") == expected.get("retry_count")
            and task.get("turn_generation")
            == expected.get("turn_generation")
        )
    if not isinstance(handoff, dict):
        return False
    return bool(
        task.get("retry_count") == handoff.get("retry_count")
        and task.get("turn_generation")
        in {
            handoff.get("from_generation"),
            (
                handoff.get("from_generation") + 1
                if type(handoff.get("from_generation")) is int
                else None
            ),
        }
    )


def _receipt_wire_is_structurally_valid(wire: object) -> bool:
    """Validate a complete stored/wire receipt without cross-node equality."""

    if not isinstance(wire, dict) or set(wire) != _RECEIPT_WIRE_KEYS:
        return False
    side = wire.get("side")
    status = wire.get("status")
    if side == "manager":
        status_valid = status in MANAGER_TASK_TERMINATION_STATUSES
        worker_valid = _valid_positive_int(wire.get("worker_id"))
    elif side == "worker":
        status_valid = status in WORKER_TASK_TERMINATION_STATUSES
        worker_valid = wire.get("worker_id") is None
    else:
        return False
    source = wire.get("source")
    if (
        not status_valid
        or not worker_valid
        or type(wire.get("version")) is not int
        or wire.get("version") != 2
        or not _valid_operation_id(wire.get("operation_id"))
        or not _valid_positive_int(wire.get("task_id"))
        or wire.get("operation") not in _VALID_OPERATIONS
        or not _valid_nonnegative_int(wire.get("state_version"))
        or wire.get("state_version") < 1
        or not _valid_nonnegative_int(wire.get("attempt_count"))
        or not _valid_nonnegative_int(wire.get("reconcile_count"))
        or (
            wire.get("last_error") is not None
            and not isinstance(wire.get("last_error"), str)
        )
        or not isinstance(source, dict)
        or set(source) != _SOURCE_WIRE_KEYS
        or source.get("status") not in _KNOWN_TASK_STATUSES
        or not _valid_nonnegative_int(source.get("retry_count"))
        or not _valid_nonnegative_int(source.get("turn_generation"))
        or (
            source.get("incarnation_id") is not None
            and not isinstance(source.get("incarnation_id"), str)
        )
        or (
            source.get("source_log_id") is not None
            and not _valid_positive_int(source.get("source_log_id"))
        )
        or (
            source.get("instance_id") is not None
            and not _valid_positive_int(source.get("instance_id"))
        )
        or not _valid_wire_datetime(source.get("started_at"))
        or not _valid_wire_datetime(source.get("completed_at"))
        or (
            source.get("session_id") is not None
            and not isinstance(source.get("session_id"), str)
        )
        or (
            source.get("pty_background_generation") is not None
            and not isinstance(source.get("pty_background_generation"), str)
        )
        or not _valid_wire_datetime(wire.get("accepted_at"))
        or not _valid_wire_datetime(wire.get("completed_at"))
        or not _valid_wire_datetime(wire.get("ack_intent_at"))
        or not _valid_wire_datetime(wire.get("acknowledged_at"))
        or not _valid_wire_datetime(wire.get("created_at"), required=True)
        or not _valid_wire_datetime(wire.get("updated_at"), required=True)
    ):
        return False
    accepted_required = status in {
        "awaiting_ack",
        "settled",
        "accepted",
        "executing",
        "succeeded",
        "acknowledged",
        "rejected",
    }
    completed_required = status in {
        "awaiting_ack",
        "settled",
        "succeeded",
        "acknowledged",
        "rejected",
    }
    acknowledged_required = status in {"settled", "acknowledged"} or (
        side == "manager" and status == "rejected"
    )
    if (
        (
            status != "conflict"
            and accepted_required != (wire.get("accepted_at") is not None)
        )
        or (
            status != "conflict"
            and completed_required != (wire.get("completed_at") is not None)
        )
        or (
            status != "conflict"
            and acknowledged_required
            != (wire.get("acknowledged_at") is not None)
        )
        or (side == "worker" and wire.get("ack_intent_at") is not None)
        or (
            side == "manager"
            and status == "pending_remote"
            and wire.get("ack_intent_at") is not None
        )
        or (
            side == "manager"
            and status in {"settled", "rejected"}
            and wire.get("ack_intent_at") is None
        )
    ):
        return False
    try:
        _validate_request_payload(
            task_id=wire["task_id"],
            operation_id=wire["operation_id"],
            operation=wire["operation"],
            payload=wire.get("request_payload"),
            digest=wire.get("request_digest"),
        )
    except WorkerTaskTerminationConflict:
        return False
    result = wire.get("result_payload")
    result_digest = wire.get("result_digest")
    if result is None or result_digest is None:
        return result is None and result_digest is None and status in {
            "pending_remote",
            "accepted",
            "executing",
            "conflict",
        }
    try:
        result_valid = bool(
            _valid_digest(result_digest)
            and canonical_json_digest(result) == result_digest
            and _valid_result_payload_identity(result, wire)
        )
    except (TypeError, ValueError, UnicodeError):
        return False
    if not result_valid:
        return False
    rejected_result = result.get("rejected") is True
    if side == "worker" and status == "rejected":
        return rejected_result
    if side == "worker" and status == "succeeded":
        return not rejected_result
    if side == "manager" and status == "settled":
        return not rejected_result
    if side == "manager" and status == "rejected":
        return rejected_result
    return status in {"awaiting_ack", "settled", "rejected", "acknowledged", "conflict"}


def serialize_receipt(receipt: WorkerTaskTerminationReceipt) -> dict:
    """Return the strict wire representation used by GET/PUT/ACK."""

    wire = {
        "version": 2,
        "operation_id": receipt.operation_id,
        "task_id": receipt.task_id,
        "side": receipt.side,
        "worker_id": receipt.worker_id,
        "operation": receipt.operation,
        "status": receipt.status,
        "state_version": receipt.state_version,
        "source": _receipt_source_payload(receipt),
        "request_payload": receipt.request_payload,
        "request_digest": receipt.request_digest,
        "result_payload": receipt.result_payload,
        "result_digest": receipt.result_digest,
        "attempt_count": receipt.attempt_count,
        "reconcile_count": receipt.reconcile_count,
        "last_error": receipt.last_error,
        "accepted_at": _wire_datetime(receipt.accepted_at),
        "completed_at": _wire_datetime(receipt.completed_at),
        "ack_intent_at": _wire_datetime(receipt.ack_intent_at),
        "acknowledged_at": _wire_datetime(receipt.acknowledged_at),
        "created_at": _wire_datetime(receipt.created_at),
        "updated_at": _wire_datetime(receipt.updated_at),
    }
    if not _receipt_wire_is_structurally_valid(wire):
        raise WorkerTaskTerminationConflict(
            "stored termination receipt failed structural validation"
        )
    return wire


def _remote_expected_matches_task(payload: dict, task: Task) -> bool:
    expected = payload["expected_remote"]
    return bool(
        expected.get("status") == task.status
        and expected.get("retry_count") == task.retry_count
        and expected.get("turn_generation") == task.turn_generation
    )


def _worker_handoff_payload_proof_is_valid(
    receipt: WorkerTurnHandoffReceipt,
) -> bool:
    """Validate only Worker-local handoff evidence and namespaces."""

    queue_payload = receipt.queue_payload
    request_payload = receipt.request_payload
    try:
        return bool(
            isinstance(queue_payload, dict)
            and receipt.queue_payload_digest
            == canonical_json_digest(queue_payload)
            and isinstance(request_payload, dict)
            and receipt.request_digest == canonical_json_digest(request_payload)
            and queue_payload.get("source_log_id") == receipt.source_log_id
            and queue_payload.get("worker_turn_handoff_id")
            == receipt.handoff_id
            and queue_payload.get("worker_turn_handoff_retry_count")
            == receipt.retry_count
            and queue_payload.get("worker_turn_handoff_from_generation")
            == receipt.from_generation
            and isinstance(receipt.response, dict)
        )
    except (TypeError, ValueError, UnicodeError):
        return False


async def _matching_worker_handoff_generation(
    db: AsyncSession,
    task: Task,
    payload: dict,
) -> WorkerTurnHandoffReceipt | None:
    handoff = payload.get("manager_handoff")
    expected = payload.get("expected_remote")
    if not isinstance(handoff, dict) or not isinstance(expected, dict):
        return None
    handoff_id = handoff.get("handoff_id")
    if not _valid_operation_id(handoff_id):
        return None
    receipt = await db.get(WorkerTurnHandoffReceipt, handoff_id)
    if (
        receipt is None
        or receipt.side != "worker"
        or receipt.task_id != task.id
        or receipt.retry_count != handoff.get("retry_count")
        or receipt.from_generation != handoff.get("from_generation")
        or task.retry_count != receipt.retry_count
        or not _worker_handoff_payload_proof_is_valid(receipt)
    ):
        return None
    if receipt.status == "accepted":
        return receipt if _remote_expected_matches_task(payload, task) else None
    if (
        receipt.status in {"claimed", "launching", "launched"}
        and receipt.claimed_turn_generation == receipt.from_generation + 1
        and task.turn_generation == receipt.claimed_turn_generation
    ):
        return receipt
    return None


async def _cancel_preboundary_worker_handoff(
    db: AsyncSession,
    handoff: WorkerTurnHandoffReceipt | None,
    task: Task,
) -> None:
    """Cancel only accepted/claimed-before-transport handoff evidence."""

    if handoff is None or handoff.status not in {"accepted", "claimed"}:
        return
    queue_payload = handoff.queue_payload
    if not _worker_handoff_payload_proof_is_valid(handoff):
        raise WorkerTaskTerminationConflict(
            "Worker handoff payload/digest is invalid during termination stage"
        )
    original_status = handoff.status
    original_claimed_generation = handoff.claimed_turn_generation

    async def refresh_after_cas_miss() -> str:
        refreshed = (
            await db.execute(
                select(WorkerTurnHandoffReceipt)
                .where(
                    WorkerTurnHandoffReceipt.handoff_id == handoff.handoff_id,
                    WorkerTurnHandoffReceipt.task_id == handoff.task_id,
                    WorkerTurnHandoffReceipt.source_log_id
                    == handoff.source_log_id,
                    WorkerTurnHandoffReceipt.side == "worker",
                    WorkerTurnHandoffReceipt.retry_count == handoff.retry_count,
                    WorkerTurnHandoffReceipt.from_generation
                    == handoff.from_generation,
                )
                # MySQL REPEATABLE READ needs a locking/current read after a
                # failed status CAS; a plain SELECT may keep returning the old
                # claimed snapshot after the boundary callback committed.
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
        if (
            refreshed is None
            or not _worker_handoff_payload_proof_is_valid(refreshed)
        ):
            raise WorkerTaskTerminationConflict(
                "Worker handoff identity changed during termination stage"
            )
        if refreshed.status in {"launching", "launched"}:
            if (
                refreshed.claimed_turn_generation
                != refreshed.from_generation + 1
            ):
                raise WorkerTaskTerminationConflict(
                    "Worker handoff launch generation is invalid"
                )
            return refreshed.status
        if refreshed.status == "cancelled":
            return refreshed.status
        raise WorkerTaskTerminationConflict(
            "Worker handoff changed during pre-boundary cancellation"
        )

    if handoff.status == "claimed":
        source = (
            await db.execute(
                select(LogEntry).where(
                    LogEntry.id == handoff.source_log_id,
                    LogEntry.task_id == task.id,
                    LogEntry.task_retry_count == handoff.retry_count,
                    LogEntry.task_turn_generation
                    == handoff.claimed_turn_generation,
                )
            )
        ).scalar_one_or_none()
        # A concrete transport is the provider side-effect boundary.  Preserve
        # launching/launched evidence and terminate that exact G+1 instead.
        if source is None:
            raise WorkerTaskTerminationConflict(
                "Claimed Worker handoff source proof is missing"
            )
        if source.actual_transport is not None:
            promoted = await db.execute(
                update(WorkerTurnHandoffReceipt)
                .where(
                    WorkerTurnHandoffReceipt.handoff_id == handoff.handoff_id,
                    WorkerTurnHandoffReceipt.task_id == handoff.task_id,
                    WorkerTurnHandoffReceipt.source_log_id
                    == handoff.source_log_id,
                    WorkerTurnHandoffReceipt.side == "worker",
                    WorkerTurnHandoffReceipt.retry_count == handoff.retry_count,
                    WorkerTurnHandoffReceipt.from_generation
                    == handoff.from_generation,
                    WorkerTurnHandoffReceipt.claimed_turn_generation
                    == original_claimed_generation,
                    WorkerTurnHandoffReceipt.status == "claimed",
                )
                .values(status="launching", updated_at=datetime.utcnow())
                .execution_options(synchronize_session=False)
            )
            if promoted.rowcount != 1:
                refreshed_status = await refresh_after_cas_miss()
                if refreshed_status not in {"launching", "launched"}:
                    raise WorkerTaskTerminationConflict(
                        "Worker handoff crossed its provider boundary while "
                        "cancellation was resolving"
                    )
            return
    delivery_key = (
        queue_payload.get("delivery_key")
        if isinstance(queue_payload, dict)
        else None
    )
    if isinstance(delivery_key, str):
        cancelled = await db.execute(
            update(WorkerTurnHandoffReceipt)
            .where(
                WorkerTurnHandoffReceipt.handoff_id == handoff.handoff_id,
                WorkerTurnHandoffReceipt.task_id == handoff.task_id,
                WorkerTurnHandoffReceipt.source_log_id == handoff.source_log_id,
                WorkerTurnHandoffReceipt.side == "worker",
                WorkerTurnHandoffReceipt.retry_count == handoff.retry_count,
                WorkerTurnHandoffReceipt.from_generation
                == handoff.from_generation,
                WorkerTurnHandoffReceipt.status == original_status,
                (
                    WorkerTurnHandoffReceipt.claimed_turn_generation.is_(None)
                    if original_claimed_generation is None
                    else WorkerTurnHandoffReceipt.claimed_turn_generation
                    == original_claimed_generation
                ),
            )
            .values(
                status="cancelled",
                claimed_turn_generation=None,
                cancel_reason="Cancelled by exact Worker termination receipt",
                updated_at=datetime.utcnow(),
            )
            .execution_options(synchronize_session=False)
        )
        if cancelled.rowcount != 1:
            refreshed_status = await refresh_after_cas_miss()
            if refreshed_status in {"launching", "launched"}:
                return
        # Reuse the Plan outbox's complete digest/link/application release
        # contract.  A half-cancelled Plan+handoff would be replayable after a
        # restart, so any missing/malformed linked proof aborts this whole Task
        # transaction and leaves the termination gate fail-closed.
        from backend.main import dispatcher

        released = await dispatcher._release_plan_delivery_with_worker_handoff(
            db,
            receipt_key=delivery_key,
            delivery_status="cancelled",
            error="Cancelled by exact Worker termination receipt",
            expected_worker_handoff=(
                task.id,
                handoff.source_log_id,
                handoff.handoff_id,
                handoff.retry_count,
                handoff.from_generation,
            ),
        )
        if released is None:
            raise WorkerTaskTerminationConflict(
                "Linked Plan delivery could not be cancelled atomically"
            )
        return
    cancelled = await db.execute(
        update(WorkerTurnHandoffReceipt)
        .where(
            WorkerTurnHandoffReceipt.handoff_id == handoff.handoff_id,
            WorkerTurnHandoffReceipt.task_id == handoff.task_id,
            WorkerTurnHandoffReceipt.source_log_id == handoff.source_log_id,
            WorkerTurnHandoffReceipt.side == "worker",
            WorkerTurnHandoffReceipt.retry_count == handoff.retry_count,
            WorkerTurnHandoffReceipt.from_generation == handoff.from_generation,
            WorkerTurnHandoffReceipt.status == original_status,
            (
                WorkerTurnHandoffReceipt.claimed_turn_generation.is_(None)
                if original_claimed_generation is None
                else WorkerTurnHandoffReceipt.claimed_turn_generation
                == original_claimed_generation
            ),
        )
        .values(
            status="cancelled",
            claimed_turn_generation=None,
            cancel_reason="Cancelled by exact Worker termination receipt",
            updated_at=datetime.utcnow(),
        )
        .execution_options(synchronize_session=False)
    )
    if cancelled.rowcount != 1:
        refreshed_status = await refresh_after_cas_miss()
        if refreshed_status in {"launching", "launched", "cancelled"}:
            return


async def stage_worker_receipt(
    db: AsyncSession,
    *,
    task_id: int,
    operation_id: str,
    operation: str,
    request_payload: dict,
    request_digest: str,
    destroy_drain_claim: str | None = None,
    destroy_task_incarnation_id: str | None = None,
    destroy_task_retry_count: int | None = None,
    destroy_task_turn_generation: int | None = None,
) -> WorkerTaskTerminationReceipt:
    """Commit Worker ``accepted`` and its Task/handoff gate before effects."""

    payload = _validate_request_payload(
        task_id=task_id,
        operation_id=operation_id,
        operation=operation,
        payload=request_payload,
        digest=request_digest,
    )
    # Global Worker admission order is NodeControl -> Task -> receipt.  A
    # destroy claim may be installed while the HTTP request is waiting on its
    # per-Task in-process lock, so the earlier route-level Task read is not an
    # admission fence.  Start a fresh transaction and serialize here, at the
    # actual durable prepare boundary.
    await db.rollback()
    node_draining = await fence_worker_node_receipt_resolution(db)
    destroy_cleanup = bool(
        node_draining
        and destroy_drain_claim is not None
        and operation == "stop_session"
        and isinstance(destroy_task_incarnation_id, str)
        and bool(destroy_task_incarnation_id)
        and type(destroy_task_retry_count) is int
        and type(destroy_task_turn_generation) is int
    )
    if not node_draining and destroy_drain_claim is not None:
        await db.rollback()
        raise WorkerTaskTerminationConflict(
            "Worker destroy cleanup claim was supplied before node drain"
        )
    task = (
        await db.execute(
            select(Task)
            .where(
                Task.id == task_id,
                Task.worker_id.is_(None),
                Task.shared_from_id.is_(None),
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if task is None:
        raise WorkerTaskTerminationConflict("Worker Task is absent or non-local")
    if destroy_cleanup and (
        task.incarnation_id != destroy_task_incarnation_id
        or task.retry_count != destroy_task_retry_count
        or task.turn_generation != destroy_task_turn_generation
    ):
        await db.rollback()
        raise WorkerTaskTerminationConflict(
            "Worker destroy cleanup Task incarnation or generation changed"
        )
    if operation == "supersede" and not is_pr_sandbox_task(task):
        raise WorkerTaskTerminationConflict(
            "supersede termination is restricted to review sandbox Tasks"
        )
    existing = (
        await db.execute(
            select(WorkerTaskTerminationReceipt)
            .where(WorkerTaskTerminationReceipt.operation_id == operation_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if existing is not None:
        if (
            existing.task_id != task_id
            or existing.side != "worker"
            or existing.operation != operation
            or existing.request_digest != request_digest
            or existing.request_payload != payload
        ):
            raise WorkerTaskTerminationConflict(
                "termination operation id was reused with a different request"
            )
        # This exact receipt crossed the node fence before phase one. Replaying
        # it is read/continuation of already-durable ownership, not admission
        # of a new stop effect, so it remains idempotent without a destroy
        # header. The later executor/ACK paths retain their own exact gates.
        return existing
    if node_draining:
        if not destroy_cleanup:
            await db.rollback()
            raise WorkerTaskTerminationConflict(
                "Worker node destruction has begun; only exact claimed "
                "stop_session cleanup is admitted"
            )
        try:
            await require_worker_node_destroy_cleanup_claim(
                db,
                claim=destroy_drain_claim,
            )
        except (ValueError, HTTPException) as exc:
            await db.rollback()
            raise WorkerTaskTerminationConflict(str(exc)) from exc
    active = await active_worker_task_termination_receipt(
        db, task_id, for_update=True
    )
    if active is not None:
        raise WorkerTaskTerminationConflict(
            "Worker Task already has an active termination operation"
        )

    handoff = await _matching_worker_handoff_generation(db, task, payload)
    if not _remote_expected_matches_task(payload, task) and handoff is None:
        raise WorkerTaskTerminationConflict(
            "Worker Task no longer matches the requested exact generation"
        )
    if task.status not in _OPERATION_SOURCE_STATUSES[operation]:
        raise WorkerTaskTerminationConflict(
            f"Worker Task cannot run {operation} from {task.status}"
        )
    await _cancel_preboundary_worker_handoff(db, handoff, task)

    now = datetime.utcnow()
    receipt = WorkerTaskTerminationReceipt(
        operation_id=operation_id,
        task_id=task_id,
        active_task_id=task_id,
        side="worker",
        worker_id=None,
        operation=operation,
        status="accepted",
        state_version=1,
        request_payload=payload,
        request_digest=request_digest,
        attempt_count=0,
        reconcile_count=0,
        next_reconcile_at=now,
        accepted_at=now,
        created_at=now,
        updated_at=now,
        **_task_source_values(task, manager_side=False),
    )
    # Preserve the cross-node handoff identity as receipt evidence.  The Worker
    # Task itself intentionally has no Manager worker_id/marker columns set.
    manager_handoff = payload.get("manager_handoff")
    if isinstance(manager_handoff, dict):
        receipt.source_worker_turn_handoff_id = manager_handoff.get("handoff_id")
        receipt.source_worker_turn_handoff_worker_id = manager_handoff.get(
            "worker_id"
        )
        receipt.source_worker_turn_handoff_retry_count = manager_handoff.get(
            "retry_count"
        )
        receipt.source_worker_turn_handoff_from_generation = manager_handoff.get(
            "from_generation"
        )
        receipt.source_worker_turn_handoff_source_log_id = manager_handoff.get(
            "source_log_id"
        )
        receipt.source_worker_turn_handoff_acknowledged = manager_handoff.get(
            "acknowledged"
        )
    db.add(receipt)
    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise WorkerTaskTerminationConflict(
            "Worker termination was admitted concurrently"
        ) from exc
    return receipt


def _task_result_snapshot(task: Task) -> dict:
    return {
        "id": task.id,
        "status": task.status,
        "retry_count": task.retry_count,
        "turn_generation": task.turn_generation,
        "instance_id": task.instance_id,
        "started_at": _wire_datetime(task.started_at),
        "completed_at": _wire_datetime(task.completed_at),
        "session_id": task.session_id,
        "error_message": task.error_message,
        "background_active": task.pty_background_generation is not None,
    }


async def _force_pending_stop_terminal(
    db: AsyncSession,
    receipt: _WorkerTerminationExecutionFence,
) -> None:
    """Remote stop-session never leaves a Worker-local pending auto claim."""

    await db.rollback()
    task_lock = await db.execute(
        update(Task)
        .where(
            Task.id == receipt.task_id,
            (
                Task.incarnation_id.is_(None)
                if receipt.source_task_incarnation_id is None
                else Task.incarnation_id
                == receipt.source_task_incarnation_id
            ),
            Task.retry_count == receipt.source_task_retry_count,
            Task.turn_generation == receipt.source_task_turn_generation,
        )
        .values(status=Task.status)
        .execution_options(synchronize_session=False)
    )
    if task_lock.rowcount != 1:
        await db.rollback()
        raise WorkerTaskTerminationConflict(
            "Worker pending Task changed before terminal stop lock"
        )
    locked_receipt = await active_worker_task_termination_receipt(
        db,
        receipt.task_id,
        for_update=True,
    )
    task = await db.get(Task, receipt.task_id, populate_existing=True)
    if task is None:
        await db.rollback()
        raise WorkerTaskTerminationConflict("Worker Task disappeared after stop")
    if task.status != "pending":
        await db.rollback()
        return
    if (
        task.incarnation_id != receipt.source_task_incarnation_id
        or task.retry_count != receipt.source_task_retry_count
        or task.turn_generation != receipt.source_task_turn_generation
        or task.pty_background_generation is not None
    ):
        raise WorkerTaskTerminationConflict(
            "Worker pending Task changed generation during stop"
        )
    owner = await db.scalar(
        select(Instance.id)
        .where(Instance.current_task_id == task.id)
        .with_for_update()
    )
    if owner is not None:
        await db.rollback()
        raise WorkerTaskTerminationConflict(
            "Worker pending Task still has a process owner"
        )
    completed_at = datetime.utcnow()
    if not worker_task_termination_authority_matches(
        locked_receipt,
        operation_id=receipt.operation_id,
        operation=receipt.operation,
        execution_token=receipt.execution_token,
        state_version=receipt.state_version,
        lease_valid_at=completed_at,
    ):
        await db.rollback()
        raise WorkerTaskTerminationConflict(
            "Worker pending stop lost its live execution lease"
        )
    changed = await db.execute(
        update(Task)
        .where(
            Task.id == task.id,
            (
                Task.incarnation_id.is_(None)
                if receipt.source_task_incarnation_id is None
                else Task.incarnation_id
                == receipt.source_task_incarnation_id
            ),
            Task.status == "pending",
            Task.retry_count == receipt.source_task_retry_count,
            Task.turn_generation == receipt.source_task_turn_generation,
            Task.pty_background_generation.is_(None),
            worker_task_termination_authority_predicate(
                operation_id=receipt.operation_id,
                operation=receipt.operation,
                execution_token=receipt.execution_token,
                state_version=receipt.state_version,
                lease_valid_at=completed_at,
            ),
        )
        .values(status="completed", completed_at=completed_at)
    )
    if changed.rowcount != 1:
        raise WorkerTaskTerminationConflict(
            "Worker pending Task changed before terminal stop commit"
        )
    task_id = task.id
    await db.commit()

    # The terminal write and its event are separate effects.  Re-acquire a
    # fresh writer snapshot after the commit and hold the exact receipt token
    # through publication, so an expired executor cannot publish after a
    # takeover advanced state_version.
    publication_task_lock = await db.execute(
        update(Task)
        .where(
            Task.id == task_id,
            (
                Task.incarnation_id.is_(None)
                if receipt.source_task_incarnation_id is None
                else Task.incarnation_id
                == receipt.source_task_incarnation_id
            ),
            Task.status == "completed",
            Task.retry_count == receipt.source_task_retry_count,
            Task.turn_generation == receipt.source_task_turn_generation,
            Task.pty_background_generation.is_(None),
        )
        .values(status=Task.status)
        .execution_options(synchronize_session=False)
    )
    if publication_task_lock.rowcount != 1:
        await db.rollback()
        raise WorkerTaskTerminationConflict(
            "Worker pending stop generation changed before publication"
        )
    publication_receipt = await active_worker_task_termination_receipt(
        db,
        task_id,
        for_update=True,
    )
    publication_owner = await db.scalar(
        select(Instance.id)
        .where(Instance.current_task_id == task_id)
        .with_for_update()
    )
    publication_valid_at = datetime.utcnow()
    if publication_owner is not None or not (
        worker_task_termination_authority_matches(
            publication_receipt,
            operation_id=receipt.operation_id,
            operation=receipt.operation,
            execution_token=receipt.execution_token,
            state_version=receipt.state_version,
            lease_valid_at=publication_valid_at,
        )
    ):
        await db.rollback()
        raise WorkerTaskTerminationConflict(
            "Worker pending stop lost receipt authority before publication"
        )
    publication = await db.execute(
        update(Task)
        .where(
            Task.id == task_id,
            (
                Task.incarnation_id.is_(None)
                if receipt.source_task_incarnation_id is None
                else Task.incarnation_id
                == receipt.source_task_incarnation_id
            ),
            Task.status == "completed",
            Task.retry_count == receipt.source_task_retry_count,
            Task.turn_generation == receipt.source_task_turn_generation,
            Task.pty_background_generation.is_(None),
            worker_task_termination_authority_predicate(
                operation_id=receipt.operation_id,
                operation=receipt.operation,
                execution_token=receipt.execution_token,
                state_version=receipt.state_version,
                lease_valid_at=publication_valid_at,
            ),
        )
        .values(status=Task.status)
        .execution_options(synchronize_session=False)
    )
    if publication.rowcount != 1:
        await db.rollback()
        raise WorkerTaskTerminationConflict(
            "Worker pending stop lost receipt authority before publication"
        )
    from backend.services.task_events import broadcast_status_change

    await broadcast_status_change(task_id, "completed", background_active=False)
    await db.commit()


async def _worker_task_is_safe_terminal(
    db: AsyncSession,
    receipt: _WorkerTerminationExecutionFence,
) -> Task | None:
    task = await db.get(Task, receipt.task_id, populate_existing=True)
    if (
        task is None
        or task.incarnation_id != receipt.source_task_incarnation_id
        or task.retry_count != receipt.source_task_retry_count
        or task.turn_generation != receipt.source_task_turn_generation
        or task.status not in TERMINAL_TASK_STATUSES
        or task.pty_background_generation is not None
    ):
        return None
    from backend.services.worker_drain_proof import (
        exact_worker_task_terminal_cleanup_is_proven,
    )

    proven = await exact_worker_task_terminal_cleanup_is_proven(
        db,
        task,
        source_status=receipt.source_task_status,
    )
    return task if proven else None


async def _mark_worker_task_superseded(
    db: AsyncSession,
    fence: _WorkerTerminationExecutionFence,
) -> Task:
    """Persist the review replacement gate under the exact Worker receipt.

    The receipt, rather than a process-local lock, is the cross-process owner
    of this write.  This helper is deliberately idempotent: recovery may enter
    after stop-session committed but before the receipt result was recorded.
    """

    from backend.services.task_queue import PR_REVIEW_SUPERSEDED_METADATA_KEY

    await db.rollback()
    task_barrier = await db.execute(
        update(Task)
        .where(
            Task.id == fence.task_id,
            Task.worker_id.is_(None),
            Task.shared_from_id.is_(None),
        )
        .values(status=Task.status)
        .execution_options(synchronize_session=False)
    )
    if task_barrier.rowcount != 1:
        await db.rollback()
        raise WorkerTaskTerminationConflict(
            "supersede receipt lost its review sandbox Task"
        )
    locked_receipt = await active_worker_task_termination_receipt(
        db,
        fence.task_id,
        for_update=True,
    )
    task = (
        await db.execute(
            select(Task)
            .where(
                Task.id == fence.task_id,
                Task.worker_id.is_(None),
                Task.shared_from_id.is_(None),
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if task is None or not is_pr_sandbox_task(task):
        await db.rollback()
        raise WorkerTaskTerminationConflict(
            "supersede receipt lost its review sandbox Task"
        )
    if (
        task.incarnation_id != fence.source_task_incarnation_id
        or task.retry_count != fence.source_task_retry_count
        or task.turn_generation != fence.source_task_turn_generation
        or task.status not in TERMINAL_TASK_STATUSES
        or task.pty_background_generation is not None
    ):
        await db.rollback()
        raise WorkerTaskTerminationConflict(
            "supersede receipt lost its exact terminal Task generation"
        )
    if fence.source_task_status in _ACTIVE_TASK_STATUSES:
        if task.status != "completed" or task.completed_at is None:
            await db.rollback()
            raise WorkerTaskTerminationConflict(
                "supersede receipt did not complete its active Task generation"
            )
    elif fence.source_task_status not in TERMINAL_TASK_STATUSES:
        await db.rollback()
        raise WorkerTaskTerminationConflict(
            "supersede receipt has an invalid source Task status"
        )
    owner = await db.scalar(
        select(Instance.id)
        .where(Instance.current_task_id == task.id)
        .with_for_update()
    )
    if owner is not None:
        await db.rollback()
        raise WorkerTaskTerminationConflict(
            "supersede receipt Task still has a process owner"
        )
    lease_valid_at = datetime.utcnow()
    if not worker_task_termination_authority_matches(
        locked_receipt,
        operation_id=fence.operation_id,
        operation="supersede",
        execution_token=fence.execution_token,
        state_version=fence.state_version,
        lease_valid_at=lease_valid_at,
    ):
        await db.rollback()
        raise WorkerTaskTerminationConflict(
            "supersede receipt lost its live execution lease"
        )

    # Make the Task UPDATE itself the final cross-dialect ownership CAS, not
    # merely a stale read followed by an unconditional marker write.
    metadata = dict(task.metadata_ or {})
    metadata[PR_REVIEW_SUPERSEDED_METADATA_KEY] = True
    values: dict[str, object] = {"metadata_": metadata}
    if fence.source_task_status in _ACTIVE_TASK_STATUSES:
        values["error_message"] = "Superseded by new PR push"
    changed = await db.execute(
        update(Task)
        .where(
            Task.id == task.id,
            Task.worker_id.is_(None),
            Task.shared_from_id.is_(None),
            (
                Task.incarnation_id.is_(None)
                if fence.source_task_incarnation_id is None
                else Task.incarnation_id == fence.source_task_incarnation_id
            ),
            Task.status == task.status,
            Task.retry_count == fence.source_task_retry_count,
            Task.turn_generation == fence.source_task_turn_generation,
            Task.pty_background_generation.is_(None),
            worker_task_termination_authority_predicate(
                operation_id=fence.operation_id,
                operation="supersede",
                execution_token=fence.execution_token,
                state_version=fence.state_version,
                lease_valid_at=lease_valid_at,
            ),
        )
        .values(**values)
        .execution_options(synchronize_session=False)
    )
    if changed.rowcount != 1:
        await db.rollback()
        raise WorkerTaskTerminationConflict(
            "supersede receipt changed before its marker commit"
        )
    task_id = task.id
    await db.commit()
    db.expire_all()
    marked = await db.get(Task, task_id)
    if (
        marked is None
        or not isinstance(marked.metadata_, dict)
        or marked.metadata_.get(PR_REVIEW_SUPERSEDED_METADATA_KEY) is not True
    ):
        raise WorkerTaskTerminationConflict(
            "supersede receipt marker did not persist"
        )
    return marked


async def execute_worker_receipt(
    db: AsyncSession,
    operation_id: str,
) -> WorkerTaskTerminationReceipt:
    """Claim a renewable Worker lease and execute exactly once logically."""

    receipt_task_id = await db.scalar(
        select(WorkerTaskTerminationReceipt.task_id).where(
            WorkerTaskTerminationReceipt.operation_id == operation_id,
            WorkerTaskTerminationReceipt.side == "worker",
        )
    )
    await db.rollback()
    if receipt_task_id is None:
        raise WorkerTaskTerminationConflict("Worker termination receipt not found")
    # Global lock order is Task -> receipt.  The self UPDATE is the SQLite WAL
    # write barrier that SELECT FOR UPDATE cannot provide; release it before
    # runtime effects so InstanceManager's independent sessions cannot deadlock.
    task_barrier = await db.execute(
        update(Task)
        .where(Task.id == receipt_task_id)
        .values(status=Task.status)
        .execution_options(synchronize_session=False)
    )
    if task_barrier.rowcount != 1:
        await db.rollback()
        raise WorkerTaskTerminationConflict("Worker Task disappeared")
    receipt = (
        await db.execute(
            select(WorkerTaskTerminationReceipt)
            .where(
                WorkerTaskTerminationReceipt.operation_id == operation_id,
                WorkerTaskTerminationReceipt.side == "worker",
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if receipt is None:
        raise WorkerTaskTerminationConflict("Worker termination receipt not found")
    if receipt.status in {"succeeded", "acknowledged", "rejected", "conflict"}:
        await db.rollback()
        terminal_receipt = await db.get(
            WorkerTaskTerminationReceipt,
            operation_id,
            populate_existing=True,
        )
        if terminal_receipt is None:
            raise WorkerTaskTerminationConflict(
                "Worker termination receipt disappeared"
            )
        return terminal_receipt
    if receipt.status not in {"accepted", "executing"}:
        await db.rollback()
        raise WorkerTaskTerminationConflict("invalid Worker receipt state")

    now = _timeline_now(receipt.accepted_at, receipt.created_at)
    if (
        receipt.status == "executing"
        and receipt.next_reconcile_at is not None
        and _utc_naive(receipt.next_reconcile_at) > now
    ):
        # Another process owns a live renewable lease.  Returning its durable
        # state lets duplicate PUT/coordinator passes converge without effects.
        await db.rollback()
        current = await db.get(
            WorkerTaskTerminationReceipt,
            operation_id,
            populate_existing=True,
        )
        if current is None:
            raise WorkerTaskTerminationConflict("Worker receipt disappeared")
        return current

    previous_status = receipt.status
    previous_state_version = receipt.state_version
    previous_token = receipt.execution_token
    execution_token = secrets.token_hex(16)
    claim_predicates = [
        WorkerTaskTerminationReceipt.operation_id == operation_id,
        WorkerTaskTerminationReceipt.side == "worker",
        WorkerTaskTerminationReceipt.status == previous_status,
        WorkerTaskTerminationReceipt.state_version == previous_state_version,
        WorkerTaskTerminationReceipt.active_task_id == receipt.task_id,
        WorkerTaskTerminationReceipt.request_digest == receipt.request_digest,
    ]
    if previous_status == "accepted":
        claim_predicates.append(
            WorkerTaskTerminationReceipt.execution_token.is_(None)
        )
    else:
        claim_predicates.extend(
            (
                WorkerTaskTerminationReceipt.execution_token == previous_token,
                WorkerTaskTerminationReceipt.next_reconcile_at <= now,
            )
        )
    lease_expires_at = now + timedelta(
        seconds=_WORKER_EXECUTION_LEASE_SECONDS
    )
    advanced = await db.execute(
        update(WorkerTaskTerminationReceipt)
        .where(*claim_predicates)
        .values(
            status="executing",
            state_version=WorkerTaskTerminationReceipt.state_version + 1,
            execution_token=execution_token,
            attempt_count=WorkerTaskTerminationReceipt.attempt_count + 1,
            next_reconcile_at=lease_expires_at,
            last_error=None,
            updated_at=now,
        )
        .execution_options(synchronize_session=False)
    )
    if advanced.rowcount != 1:
        await db.rollback()
        current = await db.get(
            WorkerTaskTerminationReceipt,
            operation_id,
            populate_existing=True,
        )
        if current is None:
            raise WorkerTaskTerminationConflict("Worker receipt disappeared")
        return current
    fence = _WorkerTerminationExecutionFence(
        task_id=receipt.task_id,
        operation_id=receipt.operation_id,
        operation=receipt.operation,
        request_digest=receipt.request_digest,
        execution_token=execution_token,
        state_version=previous_state_version + 1,
        lease_expires_at=lease_expires_at,
        source_task_incarnation_id=receipt.source_task_incarnation_id,
        source_task_status=receipt.source_task_status,
        source_task_retry_count=receipt.source_task_retry_count,
        source_task_turn_generation=receipt.source_task_turn_generation,
        accepted_at=receipt.accepted_at,
        created_at=receipt.created_at,
    )
    await db.commit()

    heartbeat_factory = async_sessionmaker(
        db.bind,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    heartbeat_stop = asyncio.Event()
    heartbeat_lost = asyncio.Event()
    heartbeat = asyncio.create_task(
        _heartbeat_worker_execution(
            heartbeat_factory,
            fence,
            stop=heartbeat_stop,
            ownership_lost=heartbeat_lost,
        ),
        name=f"worker-termination-heartbeat:{operation_id}",
    )
    effect = asyncio.create_task(
        _execute_owned_worker_receipt(db, fence),
        name=f"worker-termination-effect:{operation_id}",
    )
    lost_waiter = asyncio.create_task(
        heartbeat_lost.wait(),
        name=f"worker-termination-lease-loss:{operation_id}",
    )
    try:
        completed, _ = await asyncio.wait(
            {effect, lost_waiter},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if lost_waiter in completed and effect not in completed:
            # The last lease we proved has expired (or an exact heartbeat CAS
            # lost).  Stop the old effect task before a new coordinator may
            # adopt the operation.  Its inner Task/receipt writes also carry
            # token+version+live-lease CAS predicates as a second fence.
            effect.cancel()
            await asyncio.gather(effect, return_exceptions=True)
            heartbeat_stop.set()
            await heartbeat
            await db.rollback()
            current = await db.get(
                WorkerTaskTerminationReceipt,
                operation_id,
                populate_existing=True,
            )
            if current is None:
                raise WorkerTaskTerminationConflict(
                    "Worker receipt disappeared after lease loss"
                )
            return current
        resulting = await effect
    except asyncio.CancelledError:
        async def cancel_execution() -> None:
            effect.cancel()
            await asyncio.gather(effect, return_exceptions=True)
            heartbeat_stop.set()
            await heartbeat
            if not lost_waiter.done():
                lost_waiter.cancel()
            await asyncio.gather(lost_waiter, return_exceptions=True)
            await record_worker_reconcile_error(
                db,
                operation_id,
                "Worker termination execution was cancelled",
                execution_token=execution_token,
                expected_state_version=fence.state_version,
            )

        operation, _ = await settle_awaitable(cancel_execution())
        operation.result()
        raise
    except WorkerTaskTerminationConflict as exc:
        heartbeat_stop.set()
        await heartbeat
        marked = await mark_worker_receipt_conflict(
            db,
            operation_id,
            exc,
            execution_token=execution_token,
            expected_state_version=fence.state_version,
        )
        if marked:
            current = await db.get(
                WorkerTaskTerminationReceipt,
                operation_id,
                populate_existing=True,
            )
            if current is None:
                raise
            return current
        await db.rollback()
        current = await db.get(
            WorkerTaskTerminationReceipt,
            operation_id,
            populate_existing=True,
        )
        if current is None:
            raise
        return current
    except Exception as exc:
        heartbeat_stop.set()
        await heartbeat
        await record_worker_reconcile_error(
            db,
            operation_id,
            exc,
            execution_token=execution_token,
            expected_state_version=fence.state_version,
        )
        raise
    else:
        heartbeat_stop.set()
        await heartbeat
        if heartbeat_lost.is_set():
            await db.rollback()
            current = await db.get(
                WorkerTaskTerminationReceipt,
                operation_id,
                populate_existing=True,
            )
            if current is None:
                raise WorkerTaskTerminationConflict("Worker receipt disappeared")
            return current
        return resulting
    finally:
        if not lost_waiter.done():
            lost_waiter.cancel()
            await asyncio.gather(lost_waiter, return_exceptions=True)


async def _heartbeat_worker_execution(
    db_factory,
    fence: _WorkerTerminationExecutionFence,
    *,
    stop: asyncio.Event,
    ownership_lost: asyncio.Event,
) -> None:
    """Renew one exact execution token without changing its state version."""

    lease_expires_at = _utc_naive(fence.lease_expires_at)
    wait_seconds = _WORKER_EXECUTION_HEARTBEAT_SECONDS
    while True:
        if stop.is_set():
            return
        now = datetime.utcnow()
        remaining_seconds = (lease_expires_at - now).total_seconds()
        if remaining_seconds <= 0:
            # We cannot authorize more effects after the last lease we proved
            # has expired.  A later DB read/CAS decides whether another owner
            # actually took over; a temporary DB outage alone is not treated
            # as proof that ownership changed.
            ownership_lost.set()
            return
        try:
            await asyncio.wait_for(
                stop.wait(),
                timeout=min(wait_seconds, remaining_seconds),
            )
            return
        except asyncio.TimeoutError:
            pass
        now = datetime.utcnow()
        if now >= lease_expires_at:
            ownership_lost.set()
            return
        try:
            async with db_factory() as heartbeat_db:
                # Obtain the receipt writer lock first, then sample wall time.
                # A timestamp captured before this statement could remain
                # stale while waiting behind another transaction and renew an
                # already-expired owner on SQLite/PostgreSQL alike.
                barrier = await heartbeat_db.execute(
                    update(WorkerTaskTerminationReceipt)
                    .where(
                        WorkerTaskTerminationReceipt.operation_id
                        == fence.operation_id,
                        WorkerTaskTerminationReceipt.side == "worker",
                        WorkerTaskTerminationReceipt.status == "executing",
                        WorkerTaskTerminationReceipt.state_version
                        == fence.state_version,
                        WorkerTaskTerminationReceipt.execution_token
                        == fence.execution_token,
                        WorkerTaskTerminationReceipt.active_task_id
                        == fence.task_id,
                        WorkerTaskTerminationReceipt.request_digest
                        == fence.request_digest,
                    )
                    .values(
                        state_version=(
                            WorkerTaskTerminationReceipt.state_version
                        )
                    )
                    .execution_options(synchronize_session=False)
                )
                if barrier.rowcount != 1:
                    await heartbeat_db.rollback()
                    ownership_lost.set()
                    return
                locked = await heartbeat_db.get(
                    WorkerTaskTerminationReceipt,
                    fence.operation_id,
                    populate_existing=True,
                    with_for_update=True,
                )
                now = datetime.utcnow()
                if not worker_task_termination_authority_matches(
                    locked,
                    operation_id=fence.operation_id,
                    operation=fence.operation,
                    execution_token=fence.execution_token,
                    state_version=fence.state_version,
                    lease_valid_at=now,
                ):
                    await heartbeat_db.rollback()
                    ownership_lost.set()
                    return
                renewed = await heartbeat_db.execute(
                    update(WorkerTaskTerminationReceipt)
                    .where(
                        WorkerTaskTerminationReceipt.operation_id
                        == fence.operation_id,
                        WorkerTaskTerminationReceipt.side == "worker",
                        WorkerTaskTerminationReceipt.status == "executing",
                        WorkerTaskTerminationReceipt.state_version
                        == fence.state_version,
                        WorkerTaskTerminationReceipt.execution_token
                        == fence.execution_token,
                        WorkerTaskTerminationReceipt.active_task_id
                        == fence.task_id,
                        WorkerTaskTerminationReceipt.request_digest
                        == fence.request_digest,
                        WorkerTaskTerminationReceipt.next_reconcile_at.is_not(
                            None
                        ),
                        WorkerTaskTerminationReceipt.next_reconcile_at > now,
                    )
                    .values(
                        next_reconcile_at=now
                        + timedelta(seconds=_WORKER_EXECUTION_LEASE_SECONDS),
                        updated_at=now,
                    )
                    .execution_options(synchronize_session=False)
                )
                await heartbeat_db.commit()
        except asyncio.CancelledError:
            raise
        except Exception:
            # A transient WAL lock/transport failure is not evidence that a
            # different token won.  Retry promptly while the last committed
            # lease remains live; if it cannot be renewed before expiry the
            # local executor fails closed above.
            logger.warning(
                "Worker termination %s heartbeat failed transiently",
                fence.operation_id,
                exc_info=True,
            )
            wait_seconds = min(
                _WORKER_EXECUTION_HEARTBEAT_RETRY_SECONDS,
                _WORKER_EXECUTION_HEARTBEAT_SECONDS,
            )
            continue
        if renewed.rowcount != 1:
            ownership_lost.set()
            return
        lease_expires_at = now + timedelta(
            seconds=_WORKER_EXECUTION_LEASE_SECONDS
        )
        wait_seconds = _WORKER_EXECUTION_HEARTBEAT_SECONDS


async def _commit_worker_execution_authority_barrier(
    db: AsyncSession,
    fence: _WorkerTerminationExecutionFence,
) -> None:
    """Commit one fresh live-lease Task CAS before a direct runtime effect."""

    await db.rollback()
    task_lock = await db.execute(
        update(Task)
        .where(Task.id == fence.task_id)
        .values(status=Task.status)
        .execution_options(synchronize_session=False)
    )
    if task_lock.rowcount != 1:
        await db.rollback()
        raise WorkerTaskTerminationConflict("Worker Task disappeared")
    locked_receipt = await active_worker_task_termination_receipt(
        db,
        fence.task_id,
        for_update=True,
    )
    lease_valid_at = datetime.utcnow()
    if not worker_task_termination_authority_matches(
        locked_receipt,
        operation_id=fence.operation_id,
        operation=fence.operation,
        execution_token=fence.execution_token,
        state_version=fence.state_version,
        lease_valid_at=lease_valid_at,
    ):
        await db.rollback()
        raise WorkerTaskTerminationConflict(
            "Worker termination execution lease is no longer authoritative"
        )
    guarded = await db.execute(
        update(Task)
        .where(
            Task.id == fence.task_id,
            worker_task_termination_authority_predicate(
                operation_id=fence.operation_id,
                operation=fence.operation,
                execution_token=fence.execution_token,
                state_version=fence.state_version,
                lease_valid_at=lease_valid_at,
            ),
        )
        .values(status=Task.status)
        .execution_options(synchronize_session=False)
    )
    if guarded.rowcount != 1:
        await db.rollback()
        raise WorkerTaskTerminationConflict(
            "Worker termination execution lease is no longer authoritative"
        )
    await db.commit()


async def _execute_owned_worker_receipt(
    db: AsyncSession,
    fence: _WorkerTerminationExecutionFence,
) -> WorkerTaskTerminationReceipt:
    """Run effects and finalize only under one exact execution token."""

    # Every receipt, including one admitted from an already-terminal source,
    # installs the exact durable Harness terminal gate and drains the complete
    # owner graph.  A terminal status alone is not cleanup evidence: Browser
    # child and Workspace cleanup can legitimately outlive it.
    from backend.services.test_harness import (
        TestHarnessService,
        test_harness_service,
    )
    from backend.services.test_harness_owner_fence import (
        TestHarnessOwnerIdentity,
    )

    if not fence.source_task_incarnation_id:
        raise WorkerTaskTerminationConflict(
            "Worker termination receipt has no durable Task incarnation"
        )
    expected_harness_owner = TestHarnessOwnerIdentity(
        task_id=fence.task_id,
        incarnation_id=fence.source_task_incarnation_id,
        retry_count=fence.source_task_retry_count,
        turn_generation=fence.source_task_turn_generation,
        status=fence.source_task_status,
    )
    harness_service = test_harness_service
    configured_bind = getattr(
        getattr(harness_service, "db_factory", None),
        "kw",
        {},
    ).get("bind")
    if configured_bind is not db.bind:
        harness_service = TestHarnessService(
            db_factory=async_sessionmaker(
                db.bind,
                class_=AsyncSession,
                expire_on_commit=False,
            )
        )

    async def require_live_receipt_after_owner_lock(
        locked_db: AsyncSession,
    ) -> None:
        """Prove this exact lease after the Harness Task writer is held."""

        locked_receipt = await active_worker_task_termination_receipt(
            locked_db,
            fence.task_id,
            for_update=True,
        )
        lease_valid_at = datetime.utcnow()
        if (
            not worker_task_termination_authority_matches(
                locked_receipt,
                operation_id=fence.operation_id,
                operation=fence.operation,
                execution_token=fence.execution_token,
                state_version=fence.state_version,
                lease_valid_at=lease_valid_at,
            )
            or locked_receipt is None
            or locked_receipt.request_digest != fence.request_digest
        ):
            raise WorkerTaskTerminationConflict(
                "Worker termination execution lease is no longer authoritative"
            )

    # Establish the ordinary Worker Task -> receipt writer order before
    # entering graph cleanup.  The terminal-gate callback below repeats the
    # proof in the gate's own transaction, so a lease that expires between
    # these two boundaries still cannot authorize the durable effect.
    try:
        await _commit_worker_execution_authority_barrier(db, fence)
    except WorkerTaskTerminationConflict:
        await db.rollback()
        current = await db.get(
            WorkerTaskTerminationReceipt,
            fence.operation_id,
            populate_existing=True,
        )
        if current is None:
            raise
        return current

    try:
        async with harness_service.owner_stop_fence(
            fence.task_id,
            reason="Worker termination receipt drained owner graph",
            expected_identity=expected_harness_owner,
            locked_owner_validator=require_live_receipt_after_owner_lock,
        ):
            pass
    except Exception as exc:
        raise WorkerTaskTerminationConflict(
            f"Worker owner graph cleanup could not be proven: {exc}"
        ) from exc

    already_terminal = await _worker_task_is_safe_terminal(db, fence)
    if already_terminal is None:

        # Keep API lifecycle behavior centralized.  These functions own the
        # exact queue/launch/process/auxiliary cleanup contract and are safe to
        # invoke again after a crash while the receipt still fences admission.
        from backend.api.tasks import (
            _cancel_local_task_impl,
            _stop_task_session_local_impl,
        )

        current = await db.get(Task, fence.task_id, populate_existing=True)
        if fence.operation == "cancel":
            operation_result = await _cancel_local_task_impl(
                fence.task_id,
                db,
                expected_identity=expected_harness_owner,
                worker_termination_operation_id=fence.operation_id,
                worker_termination_execution_token=fence.execution_token,
                worker_termination_state_version=fence.state_version,
            )
            response = {"ok": True, "task": _task_result_snapshot(operation_result)}
        elif current is not None and current.status == "pending":
            # A restart loses the in-memory queue object, so the public local
            # stop helper would correctly return "no running session".  The
            # durable internal receipt is stronger evidence: close any rebuilt
            # queue admission and terminalize the exact pending generation.
            from backend.main import dispatcher

            async with dispatcher.task_queue_cancellation_lease(fence.task_id):
                # Acquiring the in-memory queue lease may wait past the last
                # receipt lease we observed. Re-prove ownership only after it
                # is held and immediately before the queue effect.
                await _commit_worker_execution_authority_barrier(db, fence)
                cleared = await dispatcher.abort_task_queue(
                    fence.task_id,
                    cancel_durable=False,
                    durable_db=db,
                )
                await _force_pending_stop_terminal(db, fence)
            response = {
                "ok": True,
                "stopped": False,
                "cleared_messages": cleared,
                "recovered_pending": True,
            }
        else:
            response = await _stop_task_session_local_impl(
                fence.task_id,
                db,
                expected_identity=expected_harness_owner,
                worker_termination_operation_id=fence.operation_id,
                worker_supersede=(fence.operation == "supersede"),
                worker_termination_execution_token=fence.execution_token,
                worker_termination_state_version=fence.state_version,
            )
            await _force_pending_stop_terminal(db, fence)
        terminal_task = await _worker_task_is_safe_terminal(db, fence)
        if terminal_task is None:
            raise WorkerTaskTerminationConflict(
                "Worker termination did not prove a terminal owner-free Task"
            )
    else:
        terminal_task = already_terminal
        response = {"ok": True, "recovered": True}

    if fence.operation == "supersede":
        # This write is part of the receipt result, not follow-up best effort.
        # Recovery repeats it after a crash between process stop and result
        # commit, including when the source Task was terminal at admission.
        terminal_task = await _mark_worker_task_superseded(db, fence)

    result_payload = {
        "version": 2,
        "operation_id": fence.operation_id,
        "task_id": fence.task_id,
        "operation": fence.operation,
        "request_digest": fence.request_digest,
        "task": _task_result_snapshot(terminal_task),
        "response": response,
    }
    result_digest = canonical_json_digest(result_payload)
    await db.rollback()
    task_barrier = await db.execute(
        update(Task)
        .where(Task.id == fence.task_id)
        .values(status=Task.status)
        .execution_options(synchronize_session=False)
    )
    if task_barrier.rowcount != 1:
        await db.rollback()
        raise WorkerTaskTerminationConflict("Worker Task disappeared")
    locked_receipt = await active_worker_task_termination_receipt(
        db,
        fence.task_id,
        for_update=True,
    )
    lease_valid_at = datetime.utcnow()
    if not worker_task_termination_authority_matches(
        locked_receipt,
        operation_id=fence.operation_id,
        operation=fence.operation,
        execution_token=fence.execution_token,
        state_version=fence.state_version,
        lease_valid_at=lease_valid_at,
    ):
        await db.rollback()
        current = await db.get(
            WorkerTaskTerminationReceipt,
            fence.operation_id,
            populate_existing=True,
        )
        if current is None:
            raise WorkerTaskTerminationConflict("Worker receipt disappeared")
        return current
    now = _timeline_now(fence.accepted_at, fence.created_at)
    completed = await db.execute(
        update(WorkerTaskTerminationReceipt)
        .where(
            WorkerTaskTerminationReceipt.operation_id == fence.operation_id,
            WorkerTaskTerminationReceipt.side == "worker",
            WorkerTaskTerminationReceipt.status == "executing",
            WorkerTaskTerminationReceipt.active_task_id == fence.task_id,
            WorkerTaskTerminationReceipt.request_digest == fence.request_digest,
            WorkerTaskTerminationReceipt.execution_token
            == fence.execution_token,
            WorkerTaskTerminationReceipt.state_version == fence.state_version,
            WorkerTaskTerminationReceipt.next_reconcile_at.is_not(None),
            WorkerTaskTerminationReceipt.next_reconcile_at > lease_valid_at,
        )
        .values(
            status="succeeded",
            state_version=WorkerTaskTerminationReceipt.state_version + 1,
            execution_token=None,
            result_payload=result_payload,
            result_digest=result_digest,
            completed_at=now,
            next_reconcile_at=None,
            last_error=None,
            updated_at=now,
        )
    )
    if completed.rowcount != 1:
        await db.rollback()
        current = await db.get(
            WorkerTaskTerminationReceipt,
            fence.operation_id,
            populate_existing=True,
        )
        if current is None:
            raise WorkerTaskTerminationConflict("Worker receipt disappeared")
        return current
    await db.commit()
    db.expire_all()
    resulting = await db.get(
        WorkerTaskTerminationReceipt,
        fence.operation_id,
    )
    if resulting is None:
        raise WorkerTaskTerminationConflict("Worker receipt disappeared")
    return resulting


async def mark_worker_receipt_conflict(
    db: AsyncSession,
    operation_id: str,
    error: BaseException | str,
    *,
    execution_token: str | None = None,
    expected_state_version: int | None = None,
) -> bool:
    """Quarantine only the exact accepted/executing owner generation."""

    detail = str(error)[:_MAX_ERROR_LENGTH]
    await db.rollback()
    receipt_task_id = await db.scalar(
        select(WorkerTaskTerminationReceipt.task_id).where(
            WorkerTaskTerminationReceipt.operation_id == operation_id,
            WorkerTaskTerminationReceipt.side == "worker",
        )
    )
    await db.rollback()
    if receipt_task_id is None:
        return False
    task_barrier = await db.execute(
        update(Task)
        .where(Task.id == receipt_task_id)
        .values(status=Task.status)
        .execution_options(synchronize_session=False)
    )
    if task_barrier.rowcount != 1:
        await db.rollback()
        return False
    locked_receipt = await active_worker_task_termination_receipt(
        db,
        receipt_task_id,
        for_update=True,
    )
    fresh_now = datetime.utcnow()
    predicates = [
        WorkerTaskTerminationReceipt.operation_id == operation_id,
        WorkerTaskTerminationReceipt.side == "worker",
    ]
    if execution_token is None:
        # Preflight callers may quarantine an unclaimed accepted receipt, but
        # can never overwrite a concurrently executing lease owner.
        predicates.extend(
            (
                WorkerTaskTerminationReceipt.status == "accepted",
                WorkerTaskTerminationReceipt.execution_token.is_(None),
            )
        )
    else:
        if not (
            locked_receipt is not None
            and locked_receipt.status == "executing"
            and locked_receipt.execution_token == execution_token
            and locked_receipt.next_reconcile_at is not None
            and _utc_naive(locked_receipt.next_reconcile_at) > fresh_now
            and (
                expected_state_version is None
                or locked_receipt.state_version == expected_state_version
            )
        ):
            await db.rollback()
            return False
        predicates.extend(
            (
                WorkerTaskTerminationReceipt.status == "executing",
                WorkerTaskTerminationReceipt.execution_token == execution_token,
                WorkerTaskTerminationReceipt.next_reconcile_at.is_not(None),
                WorkerTaskTerminationReceipt.next_reconcile_at
                > fresh_now,
            )
        )
    if expected_state_version is not None:
        predicates.append(
            WorkerTaskTerminationReceipt.state_version
            == expected_state_version
        )
    changed = await db.execute(
        update(WorkerTaskTerminationReceipt)
        .where(*predicates)
        .values(
            status="conflict",
            state_version=WorkerTaskTerminationReceipt.state_version + 1,
            execution_token=None,
            last_error=detail,
            next_reconcile_at=None,
            updated_at=fresh_now,
        )
        .execution_options(synchronize_session=False)
    )
    await db.commit()
    return changed.rowcount == 1


async def persist_worker_preflight_rejection(
    db: AsyncSession,
    *,
    task_id: int,
    operation_id: str,
    operation: str,
    request_payload: dict,
    request_digest: str,
    error: str,
) -> WorkerTaskTerminationReceipt | None:
    """Persist a proved pre-side-effect rejection for Manager convergence."""

    try:
        payload = _validate_request_payload(
            task_id=task_id,
            operation_id=operation_id,
            operation=operation,
            payload=request_payload,
            digest=request_digest,
        )
    except WorkerTaskTerminationConflict:
        return None
    # The stage attempt rolled back before this fallback.  Reacquire the
    # node-wide receipt-resolution fence in a fresh transaction instead of
    # reusing any pre-stage snapshot: drain may have won during that rollback.
    # Existing exact receipts remain replayable, but the drain claim itself is
    # the durable proof that a missing operation had no side effect, so a new
    # rejected tombstone must never be created after drain.
    await db.rollback()
    node_draining = await fence_worker_node_receipt_resolution(db)
    task = (
        await db.execute(
            select(Task)
            .where(
                Task.id == task_id,
                Task.worker_id.is_(None),
                Task.shared_from_id.is_(None),
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if task is None:
        await db.rollback()
        return None
    existing = (
        await db.execute(
            select(WorkerTaskTerminationReceipt)
            .where(WorkerTaskTerminationReceipt.operation_id == operation_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if existing is not None:
        if (
            existing.task_id == task_id
            and existing.side == "worker"
            and existing.operation == operation
            and existing.request_digest == request_digest
            and existing.request_payload == payload
        ):
            return existing
        await db.rollback()
        return None
    if node_draining:
        await db.rollback()
        return None
    now = datetime.utcnow()
    result_payload = {
        "version": 2,
        "operation_id": operation_id,
        "task_id": task_id,
        "operation": operation,
        "request_digest": request_digest,
        "rejected": True,
        "error": error[:_MAX_ERROR_LENGTH],
    }
    receipt = WorkerTaskTerminationReceipt(
        operation_id=operation_id,
        task_id=task_id,
        active_task_id=task_id,
        side="worker",
        worker_id=None,
        operation=operation,
        status="rejected",
        state_version=1,
        request_payload=payload,
        request_digest=request_digest,
        result_payload=result_payload,
        result_digest=canonical_json_digest(result_payload),
        attempt_count=0,
        reconcile_count=0,
        last_error=error[:_MAX_ERROR_LENGTH],
        accepted_at=now,
        completed_at=now,
        created_at=now,
        updated_at=now,
        **_task_source_values(task, manager_side=False),
    )
    db.add(receipt)
    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        return None
    return receipt


async def reject_manager_receipt(
    db: AsyncSession,
    operation_id: str,
    remote: dict,
) -> WorkerTaskTerminationReceipt:
    """Durably store a pre-effect rejection before acknowledging the Worker."""

    receipt_task_id = await db.scalar(
        select(WorkerTaskTerminationReceipt.task_id).where(
            WorkerTaskTerminationReceipt.operation_id == operation_id,
            WorkerTaskTerminationReceipt.side == "manager",
        )
    )
    await db.rollback()
    if receipt_task_id is None:
        raise WorkerTaskTerminationConflict("Manager receipt not found")
    task = (
        await db.execute(
            select(Task).where(Task.id == receipt_task_id).with_for_update()
        )
    ).scalar_one_or_none()
    receipt = (
        await db.execute(
            select(WorkerTaskTerminationReceipt)
            .where(
                WorkerTaskTerminationReceipt.operation_id == operation_id,
                WorkerTaskTerminationReceipt.side == "manager",
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if task is None or receipt is None:
        raise WorkerTaskTerminationConflict("Manager rejection identity changed")
    if (
        not _valid_remote_receipt(remote, receipt)
        or remote.get("status") != "rejected"
        or not isinstance(remote.get("result_payload"), dict)
        or remote["result_payload"].get("rejected") is not True
    ):
        raise WorkerTaskTerminationConflict("Worker rejection proof is invalid")
    if receipt.status in {"awaiting_ack", "rejected"}:
        if (
            receipt.result_payload == remote.get("result_payload")
            and receipt.result_digest == remote.get("result_digest")
        ):
            await db.rollback()
            resumed = await db.get(
                WorkerTaskTerminationReceipt,
                operation_id,
                populate_existing=True,
            )
            if resumed is not None:
                return resumed
        raise WorkerTaskTerminationConflict(
            "Manager rejection changed after durable apply"
        )
    if receipt.status != "pending_remote":
        raise WorkerTaskTerminationConflict("Manager rejection identity changed")
    if (
        task.worker_id != receipt.worker_id
        or task.shared_from_id is not None
        or not _receipt_source_matches_task(
            receipt, task, include_manager_handoff=True
        )
    ):
        raise WorkerTaskTerminationConflict(
            "Manager Task changed before termination rejection commit"
        )
    now = _timeline_now(receipt.created_at)
    receipt.status = "awaiting_ack"
    receipt.state_version += 1
    receipt.result_payload = remote["result_payload"]
    receipt.result_digest = remote["result_digest"]
    receipt.completed_at = now
    receipt.accepted_at = now
    receipt.next_reconcile_at = now
    receipt.last_error = str(
        remote["result_payload"].get("error") or "Worker rejected termination"
    )[:_MAX_ERROR_LENGTH]
    receipt.updated_at = now
    await db.commit()
    return receipt


async def mark_manager_receipt_conflict(
    db: AsyncSession,
    operation_id: str,
    error: BaseException | str,
    *,
    expected_status: str,
    expected_state_version: int,
    expected_request_digest: str,
) -> bool:
    """Quarantine a non-reconcilable Manager protocol/identity mismatch."""

    await db.rollback()
    changed = await db.execute(
        update(WorkerTaskTerminationReceipt)
        .where(
            WorkerTaskTerminationReceipt.operation_id == operation_id,
            WorkerTaskTerminationReceipt.side == "manager",
            WorkerTaskTerminationReceipt.status == expected_status,
            WorkerTaskTerminationReceipt.state_version
            == expected_state_version,
            WorkerTaskTerminationReceipt.request_digest
            == expected_request_digest,
            WorkerTaskTerminationReceipt.active_task_id.is_not(None),
        )
        .values(
            status="conflict",
            state_version=WorkerTaskTerminationReceipt.state_version + 1,
            last_error=str(error)[:_MAX_ERROR_LENGTH],
            next_reconcile_at=None,
            updated_at=datetime.utcnow(),
        )
        .execution_options(synchronize_session=False)
    )
    await db.commit()
    return changed.rowcount == 1


async def record_worker_reconcile_error(
    db: AsyncSession,
    operation_id: str,
    error: BaseException | str,
    *,
    execution_token: str | None = None,
    expected_state_version: int | None = None,
) -> bool:
    """Keep cleanup uncertainty retryable without releasing its active gate."""

    await db.rollback()
    receipt_task_id = await db.scalar(
        select(WorkerTaskTerminationReceipt.task_id).where(
            WorkerTaskTerminationReceipt.operation_id == operation_id,
            WorkerTaskTerminationReceipt.side == "worker",
        )
    )
    await db.rollback()
    if receipt_task_id is None:
        return False
    task_barrier = await db.execute(
        update(Task)
        .where(Task.id == receipt_task_id)
        .values(status=Task.status)
        .execution_options(synchronize_session=False)
    )
    if task_barrier.rowcount != 1:
        await db.rollback()
        return False
    receipt = await active_worker_task_termination_receipt(
        db,
        receipt_task_id,
        for_update=True,
    )
    now = datetime.utcnow()
    if (
        receipt is None
        or execution_token is None
        or (
            expected_state_version is not None
            and receipt.state_version != expected_state_version
        )
        or not worker_task_termination_authority_matches(
            receipt,
            operation_id=operation_id,
            operation=receipt.operation,
            execution_token=execution_token,
            state_version=receipt.state_version,
            lease_valid_at=now,
        )
    ):
        await db.rollback()
        return False
    attempts = receipt.reconcile_count + 1
    delay = min(
        _MAX_RECONCILE_SECONDS,
        _INITIAL_RECONCILE_SECONDS * (2 ** min(attempts - 1, 5)),
    )
    changed = await db.execute(
        update(WorkerTaskTerminationReceipt)
        .where(
            WorkerTaskTerminationReceipt.operation_id == operation_id,
            WorkerTaskTerminationReceipt.side == "worker",
            WorkerTaskTerminationReceipt.status == "executing",
            WorkerTaskTerminationReceipt.execution_token == execution_token,
            WorkerTaskTerminationReceipt.state_version == receipt.state_version,
            WorkerTaskTerminationReceipt.next_reconcile_at.is_not(None),
            WorkerTaskTerminationReceipt.next_reconcile_at > now,
        )
        .values(
            reconcile_count=attempts,
            next_reconcile_at=now + timedelta(seconds=delay),
            last_error=str(error)[:_MAX_ERROR_LENGTH],
            updated_at=now,
        )
        .execution_options(synchronize_session=False)
    )
    await db.commit()
    return changed.rowcount == 1


async def acknowledge_worker_receipt(
    db: AsyncSession,
    *,
    task_id: int,
    operation_id: str,
    request_digest: str,
    result_digest: str,
) -> WorkerTaskTerminationReceipt:
    """Release the Worker active slot only after exact Manager ACK."""

    task = (
        await db.execute(
            select(Task).where(Task.id == task_id).with_for_update()
        )
    ).scalar_one_or_none()
    if task is None:
        raise WorkerTaskTerminationConflict("Worker Task not found")
    receipt = (
        await db.execute(
            select(WorkerTaskTerminationReceipt)
            .where(
                WorkerTaskTerminationReceipt.operation_id == operation_id,
                WorkerTaskTerminationReceipt.task_id == task_id,
                WorkerTaskTerminationReceipt.side == "worker",
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if receipt is None:
        raise WorkerTaskTerminationConflict("Worker termination receipt not found")
    if (
        receipt.request_digest != request_digest
        or receipt.result_digest != result_digest
        or not isinstance(receipt.result_payload, dict)
        or canonical_json_digest(receipt.result_payload) != result_digest
    ):
        raise WorkerTaskTerminationConflict("Worker ACK digest changed")
    if receipt.status == "acknowledged":
        return receipt
    if (
        receipt.status not in {"succeeded", "rejected"}
        or receipt.active_task_id != task_id
    ):
        raise WorkerTaskTerminationConflict("Worker receipt is not ACK-ready")
    if receipt.status == "succeeded":
        fence = _WorkerTerminationExecutionFence(
            task_id=receipt.task_id,
            operation_id=receipt.operation_id,
            operation=receipt.operation,
            request_digest=receipt.request_digest,
            execution_token="",
            state_version=receipt.state_version,
            lease_expires_at=_timeline_now(
                receipt.completed_at,
                receipt.accepted_at,
            ),
            source_task_incarnation_id=receipt.source_task_incarnation_id,
            source_task_status=receipt.source_task_status,
            source_task_retry_count=receipt.source_task_retry_count,
            source_task_turn_generation=receipt.source_task_turn_generation,
            accepted_at=receipt.accepted_at,
            created_at=receipt.created_at,
        )
        terminal = await _worker_task_is_safe_terminal(db, fence)
        if terminal is None:
            raise WorkerTaskTerminationConflict(
                "Worker Task is not safely terminal at ACK"
            )
    elif not _receipt_logical_source_matches_task(receipt, task):
        raise WorkerTaskTerminationConflict(
            "Worker Task changed after preflight rejection"
        )
    now = _timeline_now(receipt.completed_at, receipt.accepted_at)
    receipt.status = "acknowledged"
    receipt.active_task_id = None
    receipt.state_version += 1
    receipt.acknowledged_at = now
    receipt.updated_at = now
    await db.commit()
    return receipt


def _valid_remote_receipt(
    remote: object,
    manager: WorkerTaskTerminationReceipt,
) -> bool:
    if not isinstance(remote, dict):
        return False
    if remote.get("status") in {_RECEIPT_NOT_FOUND, _TASK_NOT_FOUND}:
        expected = (
            receipt_not_found_payload(manager.task_id, manager.operation_id)
            if remote.get("status") == _RECEIPT_NOT_FOUND
            else task_not_found_payload(manager.task_id, manager.operation_id)
        )
        return bool(
            remote == expected
        )
    try:
        request_valid = bool(
            _receipt_wire_is_structurally_valid(remote)
            and remote.get("operation_id") == manager.operation_id
            and remote.get("task_id") == manager.task_id
            and remote.get("side") == "worker"
            and remote.get("worker_id") is None
            and remote.get("operation") == manager.operation
            and remote.get("request_payload") == manager.request_payload
            and remote.get("request_digest") == manager.request_digest
            and canonical_json_digest(remote.get("request_payload"))
            == manager.request_digest
        )
    except (TypeError, ValueError, UnicodeError):
        return False
    return request_valid


async def _publish_manager_result_if_current(
    db: AsyncSession,
    *,
    resulting,
    operation_id: str,
    operation: str,
    expected_receipt_state_version: int,
    expected_request_digest: str,
    expected_result_digest: str,
) -> bool:
    """Publish one Manager terminal result behind its exact durable fences.

    ``apply_manager_result`` must commit the authoritative Task and receipt
    before making the WebSocket effect visible.  Reacquire the locks in the
    global Task -> receipt order afterwards and hold both through publication.
    The no-op Task UPDATE is also the SQLite write/CAS barrier; ``FOR UPDATE``
    alone would not stop another WAL coordinator from settling the receipt and
    advancing the Task to its next logical turn.
    """

    from backend.services.worker_relay import worker_task_generation_predicates

    await db.rollback()
    guarded = await db.execute(
        update(Task)
        .where(*worker_task_generation_predicates(resulting))
        .values(status=resulting.status)
        .execution_options(synchronize_session=False)
    )
    if guarded.rowcount != 1:
        await db.rollback()
        return False

    receipt = (
        await db.execute(
            select(WorkerTaskTerminationReceipt)
            .where(
                WorkerTaskTerminationReceipt.operation_id == operation_id,
                WorkerTaskTerminationReceipt.task_id == resulting.task_id,
                WorkerTaskTerminationReceipt.side == "manager",
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    try:
        receipt_is_current = bool(
            receipt is not None
            and receipt.worker_id == resulting.worker_id
            and receipt.operation == operation
            and receipt.status in {"awaiting_ack", "settled"}
            and receipt.state_version >= expected_receipt_state_version
            and (
                (
                    receipt.status == "awaiting_ack"
                    and receipt.active_task_id == resulting.task_id
                )
                or (
                    receipt.status == "settled"
                    and receipt.active_task_id is None
                )
            )
            and receipt.request_digest == expected_request_digest
            and receipt.result_digest == expected_result_digest
            and isinstance(receipt.result_payload, dict)
            and canonical_json_digest(receipt.result_payload)
            == expected_result_digest
        )
    except (TypeError, ValueError, UnicodeError):
        receipt_is_current = False
    if not receipt_is_current:
        await db.rollback()
        return False

    try:
        from backend.main import broadcaster

        await broadcaster.broadcast(
            "tasks",
            {
                "event": "status_change",
                "task_id": resulting.task_id,
                "task_retry_count": resulting.retry_count,
                "task_turn_generation": resulting.turn_generation,
                "new_status": resulting.status,
                "background_active": (
                    resulting.pty_background_generation is not None
                ),
            },
        )
    except asyncio.CancelledError:
        operation, _ = await settle_awaitable(db.rollback())
        operation.result()
        raise
    except Exception:
        # The durable transition already committed.  WebSocket publication is
        # best-effort and polling remains the repair path, but always release
        # the publication locks before returning.
        logger.exception(
            "Manager termination status publication failed for task %s",
            resulting.task_id,
        )
        await db.rollback()
        return True
    await db.commit()
    return True


async def apply_manager_result(
    db: AsyncSession,
    operation_id: str,
    remote: dict,
) -> WorkerTaskTerminationReceipt:
    """Atomically mirror terminal G/G+1, settle handoff, and store result."""

    from backend.services.worker_relay import (
        _WORKER_TURN_HANDOFF_CLEAR_VALUES,
        _settle_manager_handoff_receipt,
        apply_authoritative_worker_task,
        canonical_delegated_principal_payload,
        read_worker_task_generation,
        worker_task_generation,
        worker_task_generation_predicates,
    )
    from backend.services.task_queue import PR_REVIEW_SUPERSEDED_METADATA_KEY

    receipt_task_id = await db.scalar(
        select(WorkerTaskTerminationReceipt.task_id).where(
            WorkerTaskTerminationReceipt.operation_id == operation_id,
            WorkerTaskTerminationReceipt.side == "manager",
        )
    )
    await db.rollback()
    if receipt_task_id is None:
        raise WorkerTaskTerminationConflict("Manager receipt is not result-ready")
    task = (
        await db.execute(
            select(Task)
            .where(Task.id == receipt_task_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if task is None:
        raise WorkerTaskTerminationConflict("Manager Task disappeared")
    receipt = (
        await db.execute(
            select(WorkerTaskTerminationReceipt)
            .where(
                WorkerTaskTerminationReceipt.operation_id == operation_id,
                WorkerTaskTerminationReceipt.side == "manager",
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if receipt is None:
        raise WorkerTaskTerminationConflict("Manager receipt is not result-ready")
    if not _valid_remote_receipt(remote, receipt):
        raise WorkerTaskTerminationConflict("Worker receipt identity is invalid")
    if remote.get("status") != "succeeded":
        raise WorkerTaskTerminationPending("Worker termination is not complete")
    if receipt.status in {"awaiting_ack", "settled"}:
        if (
            receipt.result_payload == remote.get("result_payload")
            and receipt.result_digest == remote.get("result_digest")
        ):
            await db.rollback()
            resumed = await db.get(
                WorkerTaskTerminationReceipt,
                operation_id,
                populate_existing=True,
            )
            if resumed is not None:
                return resumed
        raise WorkerTaskTerminationConflict(
            "Manager termination result changed after durable apply"
        )
    if receipt.status != "pending_remote":
        raise WorkerTaskTerminationConflict("Manager receipt is not result-ready")
    result_payload = remote.get("result_payload")
    if not isinstance(result_payload, dict) or not isinstance(
        result_payload.get("task"), dict
    ):
        raise WorkerTaskTerminationConflict("Worker result omitted Task snapshot")

    if (
        task.worker_id != receipt.worker_id
        or task.shared_from_id is not None
        or not _receipt_source_matches_task(
        receipt, task, include_manager_handoff=True
        )
    ):
        raise WorkerTaskTerminationConflict(
            "Manager Task changed before termination result apply"
        )
    observed = worker_task_generation(task, expected_worker_id=receipt.worker_id)
    if observed is None:
        raise WorkerTaskTerminationConflict("Manager Worker generation is invalid")
    terminal_snapshot = dict(result_payload["task"])
    terminal_principal = canonical_delegated_principal_payload(task)
    if terminal_principal is None:
        raise WorkerTaskTerminationConflict(
            "Manager Task execution principal is invalid"
        )
    # Termination receipt v2 intentionally carries only node-neutral Task
    # lifecycle fields.  It is an authority-reducing operation: the exact
    # Manager receipt and generation prove which remote Task was stopped, and
    # the Manager remains the principal authority.  Supply the Manager's
    # canonical delegated projection solely to the common mirror CAS instead
    # of weakening that CAS or trusting a node-local Worker principal field.
    terminal_snapshot["incarnation_id"] = task.incarnation_id
    terminal_snapshot.update(terminal_principal)
    resulting = await apply_authoritative_worker_task(
        db,
        observed,
        terminal_snapshot,
        metadata_updates=(
            {PR_REVIEW_SUPERSEDED_METADATA_KEY: True}
            if receipt.operation == "supersede"
            else None
        ),
        worker_turn_handoff_id=receipt.source_worker_turn_handoff_id,
        worker_termination_operation_id=operation_id,
        commit=False,
    )
    if resulting is None or resulting.status not in TERMINAL_TASK_STATUSES:
        raise WorkerTaskTerminationConflict(
            "Manager rejected the exact Worker terminal result"
        )

    if receipt.source_worker_turn_handoff_id is not None:
        settled_handoff = await _settle_manager_handoff_receipt(
            db,
            observed,
            status="cancelled",
            reason="Settled by exact Worker termination receipt",
        )
        if not settled_handoff:
            raise WorkerTaskTerminationConflict(
                "Manager handoff receipt could not be settled atomically"
            )
        cleared = await db.execute(
            update(Task)
            .where(*worker_task_generation_predicates(resulting))
            .values(**_WORKER_TURN_HANDOFF_CLEAR_VALUES)
        )
        if cleared.rowcount != 1:
            raise WorkerTaskTerminationConflict(
                "Manager handoff marker changed before terminal apply"
            )
        refreshed = await read_worker_task_generation(
            db, receipt.task_id, receipt.worker_id
        )
        if refreshed is None:
            raise WorkerTaskTerminationConflict("Manager Task disappeared")
        resulting = refreshed

    now = _timeline_now(receipt.created_at)
    receipt.status = "awaiting_ack"
    receipt.state_version += 1
    receipt.result_payload = result_payload
    receipt.result_digest = remote.get("result_digest")
    # Remote timestamps are evidence only.  Mixing the Worker's wall clock
    # into this Manager-local CHECK timeline can invert accepted/completed
    # under ordinary clock skew.
    receipt.accepted_at = now
    receipt.completed_at = now
    receipt.next_reconcile_at = now
    receipt.last_error = None
    receipt.updated_at = now
    publication_operation = receipt.operation
    publication_receipt_state_version = receipt.state_version
    publication_request_digest = receipt.request_digest
    publication_result_digest = receipt.result_digest
    await db.commit()
    if resulting.status != observed.status:
        await _publish_manager_result_if_current(
            db,
            resulting=resulting,
            operation_id=operation_id,
            operation=publication_operation,
            expected_receipt_state_version=(
                publication_receipt_state_version
            ),
            expected_request_digest=publication_request_digest,
            expected_result_digest=publication_result_digest,
        )
    return receipt


async def record_manager_ack_intent(
    db: AsyncSession,
    operation_id: str,
) -> WorkerTaskTerminationReceipt:
    """Commit ACK intent before the first request that may release Worker gate."""

    task_id = await db.scalar(
        select(WorkerTaskTerminationReceipt.task_id).where(
            WorkerTaskTerminationReceipt.operation_id == operation_id,
            WorkerTaskTerminationReceipt.side == "manager",
        )
    )
    await db.rollback()
    if task_id is None:
        raise WorkerTaskTerminationConflict("Manager receipt not found")
    task = (
        await db.execute(select(Task).where(Task.id == task_id).with_for_update())
    ).scalar_one_or_none()
    if task is None:
        raise WorkerTaskTerminationConflict("Manager Task disappeared")
    receipt = (
        await db.execute(
            select(WorkerTaskTerminationReceipt)
            .where(
                WorkerTaskTerminationReceipt.operation_id == operation_id,
                WorkerTaskTerminationReceipt.side == "manager",
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if receipt is not None and receipt.status in {"settled", "rejected"}:
        await db.rollback()
        resumed = await db.get(
            WorkerTaskTerminationReceipt,
            operation_id,
            populate_existing=True,
        )
        if resumed is not None:
            return resumed
    if (
        receipt is None
        or receipt.status != "awaiting_ack"
        or receipt.active_task_id != task_id
        or not isinstance(receipt.result_payload, dict)
        or not _valid_digest(receipt.result_digest)
    ):
        raise WorkerTaskTerminationConflict("Manager receipt is not ACK-ready")
    try:
        if canonical_json_digest(receipt.result_payload) != receipt.result_digest:
            raise WorkerTaskTerminationConflict("Manager result digest changed")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise WorkerTaskTerminationConflict("Manager result digest changed") from exc
    now = _timeline_now(
        receipt.ack_intent_at,
        receipt.completed_at,
        receipt.accepted_at,
    )
    if receipt.ack_intent_at is None:
        receipt.ack_intent_at = now
    receipt.state_version += 1
    receipt.attempt_count += 1
    receipt.next_reconcile_at = now
    receipt.updated_at = now
    await db.commit()
    return receipt


async def settle_manager_receipt(
    db: AsyncSession,
    operation_id: str,
    remote: dict,
) -> WorkerTaskTerminationReceipt:
    receipt_task_id = await db.scalar(
        select(WorkerTaskTerminationReceipt.task_id).where(
            WorkerTaskTerminationReceipt.operation_id == operation_id,
            WorkerTaskTerminationReceipt.side == "manager",
        )
    )
    await db.rollback()
    if receipt_task_id is None:
        raise WorkerTaskTerminationConflict("Manager receipt not found")
    task = (
        await db.execute(
            select(Task).where(Task.id == receipt_task_id).with_for_update()
        )
    ).scalar_one_or_none()
    if task is None:
        raise WorkerTaskTerminationConflict("Manager Task disappeared")
    receipt = (
        await db.execute(
            select(WorkerTaskTerminationReceipt)
            .where(
                WorkerTaskTerminationReceipt.operation_id == operation_id,
                WorkerTaskTerminationReceipt.side == "manager",
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if receipt is None:
        raise WorkerTaskTerminationConflict("Manager receipt not found")
    if receipt.status in {"settled", "rejected"}:
        return receipt
    if (
        receipt.status != "awaiting_ack"
        or receipt.ack_intent_at is None
        or not _valid_remote_receipt(remote, receipt)
    ):
        raise WorkerTaskTerminationConflict("Manager ACK readback is invalid")
    remote_status = remote.get("status")
    if remote_status == "acknowledged":
        if remote.get("result_digest") != receipt.result_digest:
            raise WorkerTaskTerminationConflict("Manager ACK digest changed")
    elif remote_status != _TASK_NOT_FOUND:
        raise WorkerTaskTerminationPending("Worker ACK has not settled")
    now = _timeline_now(
        receipt.ack_intent_at,
        receipt.completed_at,
        receipt.accepted_at,
    )
    rejected = receipt.result_payload.get("rejected") is True
    receipt.status = "rejected" if rejected else "settled"
    receipt.active_task_id = None
    receipt.state_version += 1
    receipt.acknowledged_at = now
    receipt.next_reconcile_at = None
    if not rejected:
        receipt.last_error = None
    receipt.updated_at = now
    await db.commit()
    return receipt


ProxyRequest = Callable[..., Awaitable[object]]
DeleteProtocolCheck = Callable[..., Awaitable[None]]


async def _locked_manager_delete_rows(
    db: AsyncSession,
    operation_id: str,
) -> tuple[Task, WorkerTaskTerminationReceipt]:
    """Lock an exact delete aggregate in global Task -> receipt order."""

    task_id = await db.scalar(
        select(WorkerTaskTerminationReceipt.task_id).where(
            WorkerTaskTerminationReceipt.operation_id == operation_id,
            WorkerTaskTerminationReceipt.side == "manager",
            WorkerTaskTerminationReceipt.operation
            == _MANAGER_ONLY_DELETE_OPERATION,
        )
    )
    await db.rollback()
    if task_id is None:
        raise WorkerTaskTerminationConflict(
            "Manager Task deletion receipt not found"
        )
    task = (
        await db.execute(
            select(Task).where(Task.id == task_id).with_for_update()
        )
    ).scalar_one_or_none()
    if task is None:
        raise WorkerTaskTerminationConflict(
            "Manager Task disappeared before deletion commit"
        )
    receipt = (
        await db.execute(
            select(WorkerTaskTerminationReceipt)
            .where(
                WorkerTaskTerminationReceipt.operation_id == operation_id,
                WorkerTaskTerminationReceipt.task_id == task_id,
                WorkerTaskTerminationReceipt.side == "manager",
                WorkerTaskTerminationReceipt.operation
                == _MANAGER_ONLY_DELETE_OPERATION,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if receipt is None:
        raise WorkerTaskTerminationConflict(
            "Manager Task deletion receipt disappeared"
        )
    return task, receipt


async def mark_manager_task_delete_remote_possible(
    db: AsyncSession,
    operation_id: str,
) -> WorkerTaskTerminationReceipt:
    """Commit the ambiguity boundary before the first Worker DELETE byte."""

    task, receipt = await _locked_manager_delete_rows(db, operation_id)
    if (
        receipt.status != "pending_remote"
        or receipt.active_task_id != task.id
        or receipt.worker_id != task.worker_id
        or not _receipt_source_matches_task(
            receipt,
            task,
            include_manager_handoff=True,
        )
        or not _manager_delete_request_is_valid(receipt)
    ):
        await db.rollback()
        raise WorkerTaskTerminationConflict(
            "Manager Task changed before the remote deletion boundary"
        )
    now = _timeline_now(receipt.created_at)
    # ``conflict`` is the model's durable quarantine state.  For operation
    # ``delete`` it specifically means remote_possible: recovery is restricted
    # to the read-only cascade audit until that audit proves no mutation.
    receipt.status = "conflict"
    receipt.state_version += 1
    receipt.attempt_count += 1
    receipt.accepted_at = now
    receipt.completed_at = None
    receipt.next_reconcile_at = now
    receipt.last_error = "Worker Task deletion crossed the remote boundary"
    receipt.updated_at = now
    await db.commit()
    return receipt


async def reject_manager_task_delete_preboundary(
    db: AsyncSession,
    operation_id: str,
    detail: str,
) -> WorkerTaskTerminationReceipt:
    """Release a prepared owner when no Worker DELETE could have been sent."""

    task, receipt = await _locked_manager_delete_rows(db, operation_id)
    if (
        receipt.status != "pending_remote"
        or receipt.active_task_id != task.id
        or receipt.worker_id != task.worker_id
        or not _receipt_source_matches_task(
            receipt,
            task,
            include_manager_handoff=True,
        )
        or not _manager_delete_request_is_valid(receipt)
    ):
        await db.rollback()
        raise WorkerTaskTerminationConflict(
            "Manager Task deletion preflight owner changed"
        )
    now = _timeline_now(receipt.created_at)
    result = {
        "version": 1,
        "operation_id": receipt.operation_id,
        "task_id": receipt.task_id,
        "operation": _MANAGER_ONLY_DELETE_OPERATION,
        "request_digest": receipt.request_digest,
        "rejected": True,
        "error": detail[:_MAX_ERROR_LENGTH],
    }
    receipt.status = "rejected"
    receipt.active_task_id = None
    receipt.state_version += 1
    receipt.result_payload = result
    receipt.result_digest = canonical_json_digest(result)
    receipt.accepted_at = now
    receipt.completed_at = now
    receipt.ack_intent_at = now
    receipt.acknowledged_at = now
    receipt.next_reconcile_at = None
    receipt.last_error = detail[:_MAX_ERROR_LENGTH]
    receipt.updated_at = now
    await db.commit()
    return receipt


def _normalize_manager_task_delete_proof(
    receipt: WorkerTaskTerminationReceipt,
    remote: object,
    *,
    proof_kind: str,
) -> dict | None:
    expected_plan_ids = manager_delete_receipt_plan_ids(receipt)
    if proof_kind == "delete_receipt":
        if (
            not isinstance(remote, dict)
            or set(remote)
            != {
                "ok",
                "plan_cascade_protocol",
                "deleted_plan_ids",
                "remaining_target_plan_ids",
            }
            or remote.get("ok") is not True
            or remote.get("plan_cascade_protocol")
            != WORKER_TASK_PLAN_DELETE_PROTOCOL_VERSION
            or _canonical_positive_ids(remote.get("deleted_plan_ids"))
            != expected_plan_ids
            or remote.get("remaining_target_plan_ids") != []
        ):
            return None
    elif proof_kind == "delete_audit":
        if (
            not isinstance(remote, dict)
            or set(remote)
            != {
                "plan_cascade_protocol",
                "task_exists",
                "remaining_target_plan_ids",
            }
            or remote.get("plan_cascade_protocol")
            != WORKER_TASK_PLAN_DELETE_PROTOCOL_VERSION
            or remote.get("task_exists") is not False
            or remote.get("remaining_target_plan_ids") != []
        ):
            return None
    else:
        raise ValueError("unknown Worker Task deletion proof kind")
    return {
        "version": 1,
        "operation_id": receipt.operation_id,
        "task_id": receipt.task_id,
        "operation": _MANAGER_ONLY_DELETE_OPERATION,
        "request_digest": receipt.request_digest,
        "proof_kind": proof_kind,
        "plan_cascade_protocol": WORKER_TASK_PLAN_DELETE_PROTOCOL_VERSION,
        "deleted_plan_ids": list(expected_plan_ids),
        "remaining_target_plan_ids": [],
        "task_exists": False,
    }


async def record_manager_task_delete_proof(
    db: AsyncSession,
    operation_id: str,
    remote: object,
    *,
    proof_kind: str,
) -> WorkerTaskTerminationReceipt:
    """Persist exact remote absence before deleting any Manager graph row."""

    task, receipt = await _locked_manager_delete_rows(db, operation_id)
    if (
        receipt.status != "conflict"
        or receipt.active_task_id != task.id
        or receipt.worker_id != task.worker_id
        or not _receipt_source_matches_task(
            receipt,
            task,
            include_manager_handoff=True,
        )
        or not _manager_delete_request_is_valid(receipt)
    ):
        await db.rollback()
        raise WorkerTaskTerminationConflict(
            "Manager Task deletion receipt is not proof-ready"
        )
    normalized = _normalize_manager_task_delete_proof(
        receipt,
        remote,
        proof_kind=proof_kind,
    )
    if normalized is None:
        await db.rollback()
        raise WorkerTaskTerminationConflict(
            "Worker did not prove the exact Task and Plan cascade deletion"
        )
    now = _timeline_now(receipt.accepted_at, receipt.created_at)
    receipt.status = "awaiting_ack"
    receipt.state_version += 1
    receipt.result_payload = normalized
    receipt.result_digest = canonical_json_digest(normalized)
    receipt.accepted_at = receipt.accepted_at or now
    receipt.completed_at = now
    receipt.next_reconcile_at = now
    receipt.last_error = None
    receipt.updated_at = now
    await db.commit()
    return receipt


async def _record_manager_task_delete_error(
    db: AsyncSession,
    operation_id: str,
    detail: str,
) -> None:
    await db.rollback()
    receipt = await db.get(
        WorkerTaskTerminationReceipt,
        operation_id,
        populate_existing=True,
    )
    if (
        receipt is None
        or receipt.side != "manager"
        or receipt.operation != _MANAGER_ONLY_DELETE_OPERATION
        or receipt.status not in {"pending_remote", "conflict", "awaiting_ack"}
        or receipt.active_task_id != receipt.task_id
    ):
        await db.rollback()
        return
    attempts = receipt.reconcile_count + 1
    delay = min(
        _MAX_RECONCILE_SECONDS,
        _INITIAL_RECONCILE_SECONDS * (2 ** min(attempts - 1, 5)),
    )
    now = datetime.utcnow()
    await db.execute(
        update(WorkerTaskTerminationReceipt)
        .where(
            WorkerTaskTerminationReceipt.operation_id == operation_id,
            WorkerTaskTerminationReceipt.side == "manager",
            WorkerTaskTerminationReceipt.operation
            == _MANAGER_ONLY_DELETE_OPERATION,
            WorkerTaskTerminationReceipt.status == receipt.status,
            WorkerTaskTerminationReceipt.state_version == receipt.state_version,
            WorkerTaskTerminationReceipt.request_digest
            == receipt.request_digest,
            WorkerTaskTerminationReceipt.active_task_id == receipt.task_id,
        )
        .values(
            reconcile_count=attempts,
            next_reconcile_at=now + timedelta(seconds=delay),
            last_error=detail[:_MAX_ERROR_LENGTH],
            updated_at=now,
        )
        .execution_options(synchronize_session=False)
    )
    await db.commit()


async def finalize_manager_task_delete_receipt(
    db: AsyncSession,
    operation_id: str,
) -> ManagerTaskDeleteOutcome:
    """Consume awaiting_ack and atomically delete receipt + local graph."""

    receipt = await db.get(
        WorkerTaskTerminationReceipt,
        operation_id,
        populate_existing=True,
    )
    if (
        receipt is None
        or receipt.side != "manager"
        or receipt.operation != _MANAGER_ONLY_DELETE_OPERATION
        or receipt.status != "awaiting_ack"
    ):
        raise WorkerTaskTerminationConflict(
            "Manager Task deletion receipt is not locally finalizable"
        )
    task_id = receipt.task_id
    worker_id = receipt.worker_id
    if type(worker_id) is not int:
        raise WorkerTaskTerminationConflict(
            "Manager Task deletion lost its Worker route"
        )
    plan_ids = manager_delete_receipt_plan_ids(receipt)
    expected_fence = manager_delete_receipt_task_fence(receipt)
    await db.rollback()

    from backend.services.task_queue import TaskQueue

    deleted = await TaskQueue(db).delete(
        task_id,
        expected_fence=expected_fence,
        remote_worker_deleted=True,
        worker_delete_operation_id=operation_id,
    )
    if not deleted:
        raise WorkerTaskTerminationPending(
            "Manager Task/Plan graph is not ready for exact deletion finalization"
        )
    return ManagerTaskDeleteOutcome(
        operation_id=operation_id,
        task_id=task_id,
        worker_id=worker_id,
        plan_ids=plan_ids,
    )


async def _finish_delete_finalize_despite_cancellation(
    awaitable: Awaitable[ManagerTaskDeleteOutcome],
) -> ManagerTaskDeleteOutcome:
    return await finish_awaitable(awaitable)


async def reconcile_manager_task_delete_receipt(
    db: AsyncSession,
    operation_id: str,
    *,
    proxy_request: ProxyRequest,
    protocol_check: DeleteProtocolCheck,
) -> ManagerTaskDeleteOutcome:
    """Recover one remote-first delete without ever blindly replaying DELETE."""

    receipt = await db.get(
        WorkerTaskTerminationReceipt,
        operation_id,
        populate_existing=True,
    )
    if (
        receipt is None
        or receipt.side != "manager"
        or receipt.operation != _MANAGER_ONLY_DELETE_OPERATION
        or receipt.active_task_id != receipt.task_id
        or not _manager_delete_request_is_valid(receipt)
    ):
        raise WorkerTaskTerminationConflict(
            "Manager Task deletion receipt is invalid"
        )
    if receipt.status == "awaiting_ack":
        try:
            return await _finish_delete_finalize_despite_cancellation(
                finalize_manager_task_delete_receipt(db, operation_id)
            )
        except WorkerTaskTerminationPending as exc:
            await _record_manager_task_delete_error(db, operation_id, str(exc))
            raise
    if receipt.status not in {"pending_remote", "conflict"}:
        raise WorkerTaskTerminationConflict(
            f"Manager Task deletion cannot recover from {receipt.status}"
        )

    route = type(
        "TaskDeleteRoute",
        (),
        {
            "id": receipt.task_id,
            "worker_id": receipt.worker_id,
            # WorkerProxy re-reads the authoritative Manager mirror under the
            # operation lock.  Preserve the receipt's exact incarnation here;
            # an id/Worker-only recovery route would now fail closed before
            # DELETE/audit, or worse become ambiguous after integer-id reuse.
            "incarnation_id": receipt.source_task_incarnation_id,
        },
    )()
    delete_path = f"/api/tasks/{receipt.task_id}"
    audit_path = f"{delete_path}/plan-delete-audit"
    call_delete = receipt.status == "pending_remote"

    if call_delete:
        try:
            await protocol_check(
                route,
                operation_id,
                operation_lock_held=True,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await reject_manager_task_delete_preboundary(
                db,
                operation_id,
                str(exc),
            )
            raise WorkerTaskTerminationPending(
                "Worker cannot prove Task/Plan deletion protocol support; "
                "the pre-boundary owner was released for a safe retry"
            ) from exc
        receipt = await mark_manager_task_delete_remote_possible(db, operation_id)
        try:
            remote = await proxy_request(
                route,
                "DELETE",
                delete_path,
                require_json=True,
                allow_task_absent=True,
                operation_lock_held=True,
                quarantine_on_transport_uncertainty=True,
                require_task_incarnation_fence=True,
            )
        except asyncio.CancelledError:
            operation, _ = await settle_awaitable(
                _record_manager_task_delete_error(
                    db,
                    operation_id,
                    "Manager Task deletion was cancelled after remote_possible",
                )
            )
            operation.result()
            raise
        except Exception:
            remote = None
        normalized = _normalize_manager_task_delete_proof(
            receipt,
            remote,
            proof_kind="delete_receipt",
        )
        if normalized is not None:
            receipt = await record_manager_task_delete_proof(
                db,
                operation_id,
                remote,
                proof_kind="delete_receipt",
            )
            return await _finish_delete_finalize_despite_cancellation(
                finalize_manager_task_delete_receipt(db, operation_id)
            )

    # A pre-existing remote_possible state, a lost ACK, or any non-exact
    # DELETE response is reconciled only through this read-only postcondition.
    try:
        audit = await proxy_request(
            route,
            "GET",
            audit_path,
            require_json=True,
            surface_endpoint_not_found=True,
            operation_lock_held=True,
            require_task_incarnation_fence=True,
        )
    except asyncio.CancelledError:
        operation, _ = await settle_awaitable(
            _record_manager_task_delete_error(
                db,
                operation_id,
                "Manager Task deletion audit was cancelled",
            )
        )
        operation.result()
        raise
    except Exception as exc:
        await _record_manager_task_delete_error(db, operation_id, str(exc))
        raise WorkerTaskTerminationPending(
            "Worker Task deletion outcome could not be audited"
        ) from exc

    receipt = await db.get(
        WorkerTaskTerminationReceipt,
        operation_id,
        populate_existing=True,
    )
    if receipt is None:
        raise WorkerTaskTerminationConflict(
            "Manager Task deletion receipt disappeared during audit"
        )
    if (
        _normalize_manager_task_delete_proof(
            receipt,
            audit,
            proof_kind="delete_audit",
        )
        is not None
    ):
        await record_manager_task_delete_proof(
            db,
            operation_id,
            audit,
            proof_kind="delete_audit",
        )
        return await _finish_delete_finalize_despite_cancellation(
            finalize_manager_task_delete_receipt(db, operation_id)
        )
    detail = "Worker returned an invalid or mismatched Task/Plan deletion audit"
    await _record_manager_task_delete_error(db, operation_id, detail)
    raise WorkerTaskTerminationPending(detail)


def _manager_receipt_monotonic_progress(
    current: WorkerTaskTerminationReceipt,
    *,
    operation_id: str,
    expected_status: str,
    expected_state_version: int,
    expected_request_digest: str,
    expected_result_digest: str | None,
) -> bool:
    allowed = {
        "pending_remote": {"awaiting_ack", "settled", "rejected"},
        "awaiting_ack": {"awaiting_ack", "settled", "rejected"},
    }
    return bool(
        current.operation_id == operation_id
        and current.side == "manager"
        and current.request_digest == expected_request_digest
        and current.state_version > expected_state_version
        and current.status in allowed.get(expected_status, set())
        and (
            expected_result_digest is None
            or current.result_digest == expected_result_digest
        )
        and isinstance(current.result_payload, dict)
        and _valid_digest(current.result_digest)
        and canonical_json_digest(current.result_payload)
        == current.result_digest
    )


async def reconcile_manager_receipt(
    db: AsyncSession,
    operation_id: str,
    *,
    proxy_request: ProxyRequest,
) -> ManagerTerminationOutcome:
    """Perform one query-before-write Manager reconciliation pass."""

    receipt = await db.get(WorkerTaskTerminationReceipt, operation_id)
    if receipt is None or receipt.side != "manager":
        raise WorkerTaskTerminationConflict("Manager termination receipt not found")
    if receipt.operation == _MANAGER_ONLY_DELETE_OPERATION:
        raise WorkerTaskTerminationConflict(
            "Manager Task deletion requires the delete reconciler"
        )
    if receipt.status == "settled":
        return ManagerTerminationOutcome(
            operation_id=operation_id,
            operation=receipt.operation,
            status=receipt.status,
            result_payload=receipt.result_payload,
        )
    if receipt.status in {"rejected", "conflict"}:
        raise WorkerTaskTerminationConflict(
            receipt.last_error or f"Manager termination is {receipt.status}"
        )

    conflict_fence = (
        receipt.status,
        receipt.state_version,
        receipt.request_digest,
        receipt.result_digest,
    )

    route = type("TerminationRoute", (), {
        "id": receipt.task_id,
        "worker_id": receipt.worker_id,
        # Destroy-only PUT admission must carry the exact generation frozen
        # by this receipt.  A task_id/worker_id-only synthetic route cannot
        # produce the generation-bound cleanup headers and otherwise makes
        # every active Task destroy stop permanently fail after its first GET.
        "incarnation_id": receipt.source_task_incarnation_id,
        "retry_count": receipt.source_task_retry_count,
        "turn_generation": receipt.source_task_turn_generation,
    })()
    path = (
        f"/api/tasks/{receipt.task_id}/termination-receipts/"
        f"{receipt.operation_id}"
    )
    try:
        remote = await proxy_request(
            route,
            "GET",
            path,
            require_json=True,
            operation_lock_held=True,
        )
        if not _valid_remote_receipt(remote, receipt):
            raise WorkerTaskTerminationConflict(
                "Worker returned an invalid termination receipt"
            )
        remote_status = remote.get("status")
        if remote_status == _RECEIPT_NOT_FOUND:
            if receipt.status != "pending_remote":
                raise WorkerTaskTerminationConflict(
                    "Worker forgot an already-applied termination receipt"
                )
            remote = await proxy_request(
                route,
                "PUT",
                path,
                body={
                    "operation": receipt.operation,
                    "request_payload": receipt.request_payload,
                    "request_digest": receipt.request_digest,
                },
                require_json=True,
                operation_lock_held=True,
            )
            if not _valid_remote_receipt(remote, receipt):
                raise WorkerTaskTerminationConflict(
                    "Worker returned an invalid termination PUT receipt"
                )
            remote_status = remote.get("status")

        if receipt.status == "pending_remote":
            if remote_status == "succeeded":
                receipt = await apply_manager_result(db, operation_id, remote)
            elif remote_status == "rejected":
                receipt = await reject_manager_receipt(db, operation_id, remote)
            elif remote_status in {"accepted", "executing"}:
                raise WorkerTaskTerminationPending(
                    "Worker termination is executing"
                )
            elif remote_status == "conflict":
                raise WorkerTaskTerminationConflict(
                    remote.get("last_error")
                    or "Worker termination is conflicted"
                )
            elif remote_status == _TASK_NOT_FOUND:
                raise WorkerTaskTerminationConflict(
                    "Worker Task disappeared before termination admission"
                )
            else:
                raise WorkerTaskTerminationConflict(
                    "Worker receipt state is impossible before result commit"
                )
            conflict_fence = (
                receipt.status,
                receipt.state_version,
                receipt.request_digest,
                receipt.result_digest,
            )

        if receipt.status == "awaiting_ack":
            result_rejected = bool(
                isinstance(receipt.result_payload, dict)
                and receipt.result_payload.get("rejected") is True
            )
            expected_pre_ack = "rejected" if result_rejected else "succeeded"
            if remote_status == expected_pre_ack:
                receipt = await record_manager_ack_intent(db, operation_id)
                conflict_fence = (
                    receipt.status,
                    receipt.state_version,
                    receipt.request_digest,
                    receipt.result_digest,
                )
                if receipt.status == "awaiting_ack":
                    remote = await proxy_request(
                        route,
                        "POST",
                        path + "/ack",
                        body={
                            "request_digest": receipt.request_digest,
                            "result_digest": receipt.result_digest,
                        },
                        require_json=True,
                        operation_lock_held=True,
                    )
                    if not _valid_remote_receipt(remote, receipt):
                        raise WorkerTaskTerminationConflict(
                            "Worker returned an invalid termination ACK"
                        )
                    remote_status = remote.get("status")
            elif remote_status in {"succeeded", "rejected"}:
                raise WorkerTaskTerminationConflict(
                    "Worker result kind changed before ACK"
                )
            elif remote_status in {
                "accepted",
                "executing",
                "conflict",
                _RECEIPT_NOT_FOUND,
            }:
                raise WorkerTaskTerminationConflict(
                    "Worker receipt regressed after Manager result commit"
                )

            if receipt.status in {"settled", "rejected"}:
                pass
            elif remote_status in {"acknowledged", _TASK_NOT_FOUND}:
                receipt = await settle_manager_receipt(db, operation_id, remote)
            else:
                raise WorkerTaskTerminationPending("Worker ACK is pending")
    except WorkerTaskTerminationPending as exc:
        await _record_manager_reconcile_error(db, operation_id, str(exc))
        raise
    except asyncio.CancelledError:
        operation, _ = await settle_awaitable(
            _record_manager_reconcile_error(
                db, operation_id, "Manager reconciliation was cancelled"
            )
        )
        operation.result()
        raise
    except WorkerTaskTerminationConflict as exc:
        (
            expected_status,
            expected_state_version,
            expected_request_digest,
            expected_result_digest,
        ) = conflict_fence
        marked = await mark_manager_receipt_conflict(
            db,
            operation_id,
            exc,
            expected_status=expected_status,
            expected_state_version=expected_state_version,
            expected_request_digest=expected_request_digest,
        )
        if not marked:
            await db.rollback()
            current = await db.get(
                WorkerTaskTerminationReceipt,
                operation_id,
                populate_existing=True,
            )
            if current is not None and _manager_receipt_monotonic_progress(
                current,
                operation_id=operation_id,
                expected_status=expected_status,
                expected_state_version=expected_state_version,
                expected_request_digest=expected_request_digest,
                expected_result_digest=expected_result_digest,
            ):
                if current.status == "settled":
                    return ManagerTerminationOutcome(
                        operation_id=operation_id,
                        operation=current.operation,
                        status=current.status,
                        result_payload=current.result_payload,
                    )
                if current.status == "rejected":
                    raise WorkerTaskTerminationConflict(
                        current.last_error or "Worker termination was rejected"
                    ) from exc
                return await reconcile_manager_receipt(
                    db,
                    operation_id,
                    proxy_request=proxy_request,
                )
        raise
    except Exception as exc:
        await _record_manager_reconcile_error(db, operation_id, str(exc))
        raise WorkerTaskTerminationPending(
            "Worker termination readback is temporarily unavailable"
        ) from exc

    if receipt.status == "rejected":
        raise WorkerTaskTerminationConflict(
            receipt.last_error or "Worker termination was rejected"
        )
    return ManagerTerminationOutcome(
        operation_id=operation_id,
        operation=receipt.operation,
        status=receipt.status,
        result_payload=receipt.result_payload,
    )


async def _record_manager_reconcile_error(
    db: AsyncSession,
    operation_id: str,
    detail: str,
) -> None:
    await db.rollback()
    receipt = await db.get(WorkerTaskTerminationReceipt, operation_id)
    if (
        receipt is None
        or receipt.side != "manager"
        or receipt.status not in {"pending_remote", "awaiting_ack"}
    ):
        await db.rollback()
        return
    attempts = receipt.reconcile_count + 1
    delay = min(
        _MAX_RECONCILE_SECONDS,
        _INITIAL_RECONCILE_SECONDS * (2 ** min(attempts - 1, 5)),
    )
    now = datetime.utcnow()
    await db.execute(
        update(WorkerTaskTerminationReceipt)
        .where(
            WorkerTaskTerminationReceipt.operation_id == operation_id,
            WorkerTaskTerminationReceipt.side == "manager",
            WorkerTaskTerminationReceipt.status == receipt.status,
            WorkerTaskTerminationReceipt.state_version == receipt.state_version,
            WorkerTaskTerminationReceipt.request_digest
            == receipt.request_digest,
        )
        .values(
            reconcile_count=attempts,
            next_reconcile_at=now + timedelta(seconds=delay),
            last_error=detail[:_MAX_ERROR_LENGTH],
            updated_at=now,
        )
        .execution_options(synchronize_session=False)
    )
    await db.commit()


class WorkerTaskTerminationCoordinator:
    """Recover accepted Worker work and incomplete Manager reconciliations."""

    def __init__(self, db_factory, *, worker_proxy=None, poll_seconds: float = 2.0):
        self.db_factory = db_factory
        self.worker_proxy = worker_proxy
        self.poll_seconds = poll_seconds
        self._wake = asyncio.Event()
        self._runner: asyncio.Task | None = None
        self._stopping = False

    async def start(self) -> None:
        if self._runner is not None and not self._runner.done():
            return
        self._stopping = False
        self._runner = asyncio.create_task(
            self._run(), name="worker-task-termination-recovery"
        )

    async def shutdown(self) -> None:
        self._stopping = True
        self._wake.set()
        runner = self._runner
        if runner is None:
            return
        runner.cancel()
        try:
            await runner
        except asyncio.CancelledError:
            pass
        self._runner = None

    def wake(self) -> None:
        self._wake.set()

    async def _run(self) -> None:
        while not self._stopping:
            try:
                await self.recover_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Worker termination recovery pass failed")
            self._wake.clear()
            try:
                await asyncio.wait_for(
                    self._wake.wait(), timeout=self.poll_seconds
                )
            except asyncio.TimeoutError:
                pass

    async def recover_once(self, *, include_manager: bool = True) -> None:
        now = datetime.utcnow()
        async with self.db_factory() as db:
            rows = list(
                (
                    await db.execute(
                        select(
                            WorkerTaskTerminationReceipt.operation_id,
                            WorkerTaskTerminationReceipt.task_id,
                            WorkerTaskTerminationReceipt.side,
                            WorkerTaskTerminationReceipt.status,
                            WorkerTaskTerminationReceipt.operation,
                        ).where(
                            WorkerTaskTerminationReceipt.active_task_id.is_not(None),
                            WorkerTaskTerminationReceipt.next_reconcile_at.is_not(
                                None
                            ),
                            WorkerTaskTerminationReceipt.next_reconcile_at <= now,
                            or_(
                                and_(
                                    WorkerTaskTerminationReceipt.side == "worker",
                                    WorkerTaskTerminationReceipt.status.in_(
                                        ("accepted", "executing")
                                    ),
                                ),
                                and_(
                                    WorkerTaskTerminationReceipt.side == "manager",
                                    or_(
                                        WorkerTaskTerminationReceipt.status.in_(
                                            ("pending_remote", "awaiting_ack")
                                        ),
                                        and_(
                                            WorkerTaskTerminationReceipt.operation
                                            == _MANAGER_ONLY_DELETE_OPERATION,
                                            WorkerTaskTerminationReceipt.status
                                            == "conflict",
                                        ),
                                    ),
                                ),
                            ),
                        )
                    )
                ).all()
            )
        for operation_id, task_id, side, status, operation in rows:
            from backend.services.worker_proxy import get_task_operation_lock

            async with get_task_operation_lock(task_id):
                if side == "worker" and status in {"accepted", "executing"}:
                    async with self.db_factory() as db:
                        try:
                            await execute_worker_receipt(db, operation_id)
                        except asyncio.CancelledError:
                            raise
                        except WorkerTaskTerminationConflict as exc:
                            logger.exception(
                                "Worker termination %s identity conflict",
                                operation_id,
                            )
                            await mark_worker_receipt_conflict(
                                db, operation_id, exc
                            )
                        except Exception as exc:
                            logger.exception(
                                "Worker termination %s cleanup remains pending",
                                operation_id,
                            )
                            await record_worker_reconcile_error(
                                db, operation_id, exc
                            )
                elif (
                    include_manager
                    and side == "manager"
                    and operation == _MANAGER_ONLY_DELETE_OPERATION
                    and status in {"pending_remote", "conflict", "awaiting_ack"}
                    and self.worker_proxy is not None
                ):
                    async with self.db_factory() as db:
                        try:
                            await reconcile_manager_task_delete_receipt(
                                db,
                                operation_id,
                                proxy_request=self.worker_proxy.proxy_to_worker,
                                protocol_check=(
                                    self.worker_proxy
                                    .require_task_plan_delete_protocol
                                ),
                            )
                        except WorkerTaskTerminationPending:
                            pass
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            logger.exception(
                                "Manager Task deletion %s recovery failed",
                                operation_id,
                            )
                elif (
                    include_manager
                    and
                    side == "manager"
                    and status in {"pending_remote", "awaiting_ack"}
                    and self.worker_proxy is not None
                ):
                    async with self.db_factory() as db:
                        try:
                            await reconcile_manager_receipt(
                                db,
                                operation_id,
                                proxy_request=self.worker_proxy.proxy_to_worker,
                            )
                        except WorkerTaskTerminationPending:
                            pass
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            logger.exception(
                                "Manager termination %s recovery failed",
                                operation_id,
                            )
