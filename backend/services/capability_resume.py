"""Crash-safe continuation of Tasks that yielded to a Capability.

The Capability executor and the Dispatcher deliberately meet at a durable
outbox rather than an in-memory callback.  This module owns the provider-
neutral database state machine and a small polling coordinator.  It does not
own Dispatcher queues or provider processes.

Lock ordering is part of the public contract.  Mutating entry points acquire
``capability_task_lock`` before the database aggregate, then lock
Task -> Invocation -> Executions -> Outbox.  ``*_in_tx`` helpers require their
caller to have established the same ordering and never commit or roll back.
The startup-only ``reconcile_stale_resume_in_tx`` instead accepts an already
locked Task while global admission is closed; it must not invert that database
lock by acquiring the process-local fence afterwards.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
import secrets
from typing import Any, Literal, cast

from sqlalchemy import case, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.capability import (
    ACTIVE_EXECUTION_STATUSES,
    CapabilityExecution,
    CapabilityInvocation,
    CapabilityResumeOutbox,
)
from backend.models.log_entry import LogEntry
from backend.models.task import Task
from backend.services.capability_service import (
    CapabilityConflictError,
    CapabilityNotFoundError,
    _end_routing_read,
    _lock_task,
    capability_task_lock,
)
from backend.services.cancellation import finish_awaitable
from backend.services.worker_task_termination import (
    active_worker_task_termination_receipt,
)


logger = logging.getLogger(__name__)

RESUME_PAYLOAD_VERSION = 1
RESUME_PAYLOAD_TYPE = "capability_resume"
RESUME_PROMPT_OPEN_TAG = "<ccm_capability_result>"
RESUME_PROMPT_CLOSE_TAG = "</ccm_capability_result>"

_READY_INVOCATION_STATUSES = {"ready", "resuming"}
_RESULT_INVOCATION_STATUSES = _READY_INVOCATION_STATUSES | {"completed"}
_TERMINAL_FAILURE_INVOCATION_STATUSES = {"failed", "cancelled", "stale"}
_MATERIALIZABLE_INVOCATION_STATUSES = (
    _READY_INVOCATION_STATUSES | _TERMINAL_FAILURE_INVOCATION_STATUSES
)
_ACTIVE_OUTBOX_STATUSES = {"pending", "ready", "claiming", "claimed"}
_PUBLISHABLE_OUTBOX_STATUSES = {"ready", "claiming", "claimed"}
_ACTUAL_TRANSPORTS = {
    "claude_pty",
    "claude_exec",
    "codex_app_server",
    "codex_exec",
}


class CapabilityResumeError(RuntimeError):
    """Base class for durable resume state-machine failures."""


class CapabilityResumeNotFoundError(CapabilityResumeError):
    pass


class CapabilityResumeConflictError(CapabilityResumeError):
    pass


class CapabilityResumeIntegrityError(CapabilityResumeError):
    """Durable rows no longer describe one exact continuation."""


@dataclass(frozen=True, slots=True)
class ResumeEnvelope:
    """Detached queue envelope reconstructed from one verified outbox."""

    outbox_id: int
    task_id: int
    invocation_id: int
    status: str
    lease_token: str | None
    prompt: str
    current_message: str
    queue_timestamp: float
    provider: str
    model: str | None
    service_tier: str
    from_generation: int
    request_retry_count: int
    request_session_id: str | None
    payload_hash: str
    claimed_generation: int | None
    source_log_id: int | None
    execution_user_id: int | None
    execution_user_role: str
    execution_mode: str
    execution_principal_kind: str


@dataclass(frozen=True, slots=True)
class ResumeTurnClaim:
    """Evidence produced with the atomic Task G -> G+1 claim."""

    envelope: ResumeEnvelope
    retry_count: int
    turn_generation: int
    source_log_id: int
    replay: bool


@dataclass(slots=True)
class _LockedResumeAggregate:
    task: Task
    invocation: CapabilityInvocation
    executions: list[CapabilityExecution]
    outbox: CapabilityResumeOutbox


ResumePublisher = Callable[[int], Awaitable[bool | None]]


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError, UnicodeError) as exc:
        raise CapabilityResumeIntegrityError(
            "Capability resume payload is not finite JSON data"
        ) from exc


def _payload_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _text_hash(value: str) -> str:
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise CapabilityResumeIntegrityError(
            "Capability request output is not valid UTF-8"
        ) from exc
    return hashlib.sha256(encoded).hexdigest()


def _utc_timestamp(value: datetime) -> float:
    aware = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return aware.astimezone(timezone.utc).timestamp()


def _resume_prompt(outcome: dict[str, Any]) -> str:
    return (
        "The CCM capability you requested has reached a terminal result. "
        "Treat the following JSON as the authoritative result for that "
        "request, then continue the same task and logical session.\n"
        f"{RESUME_PROMPT_OPEN_TAG}\n"
        f"{_canonical_json(outcome)}\n"
        f"{RESUME_PROMPT_CLOSE_TAG}"
    )


def _detached_payload(value: dict[str, Any]) -> dict[str, Any]:
    decoded = json.loads(_canonical_json(value))
    assert isinstance(decoded, dict)
    return cast(dict[str, Any], decoded)


async def _outbox_route(
    db: AsyncSession,
    outbox_id: int,
) -> tuple[int, int] | None:
    if type(outbox_id) is not int or outbox_id <= 0:
        raise CapabilityResumeNotFoundError("Capability resume outbox not found")
    row = (
        await db.execute(
            select(
                CapabilityResumeOutbox.task_id,
                CapabilityResumeOutbox.invocation_id,
            ).where(CapabilityResumeOutbox.id == outbox_id)
        )
    ).one_or_none()
    if row is None:
        return None
    return int(row.task_id), int(row.invocation_id)


async def _lock_resume_aggregate(
    db: AsyncSession,
    *,
    task: Task,
    outbox_id: int,
    expected_invocation_id: int | None = None,
) -> _LockedResumeAggregate:
    """Lock Invocation -> Executions -> Outbox after ``task`` is locked."""

    route = await _outbox_route(db, outbox_id)
    if route is None or route[0] != task.id:
        raise CapabilityResumeNotFoundError("Capability resume outbox not found")
    invocation_id = route[1]
    if (
        expected_invocation_id is not None
        and invocation_id != expected_invocation_id
    ):
        raise CapabilityResumeIntegrityError(
            "Capability resume invocation identity changed"
        )

    invocation = (
        await db.execute(
            select(CapabilityInvocation)
            .where(CapabilityInvocation.id == invocation_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if invocation is None or invocation.task_id != task.id:
        raise CapabilityResumeIntegrityError(
            "Capability resume lost its Invocation"
        )
    executions = list(
        (
            await db.execute(
                select(CapabilityExecution)
                .where(CapabilityExecution.invocation_id == invocation.id)
                .order_by(CapabilityExecution.attempt, CapabilityExecution.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalars()
    )
    outbox = (
        await db.execute(
            select(CapabilityResumeOutbox)
            .where(CapabilityResumeOutbox.id == outbox_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if (
        outbox is None
        or outbox.task_id != task.id
        or outbox.invocation_id != invocation.id
    ):
        raise CapabilityResumeIntegrityError(
            "Capability resume outbox changed while locking its aggregate"
        )
    return _LockedResumeAggregate(task, invocation, executions, outbox)


async def _lock_from_outbox(
    db: AsyncSession,
    outbox_id: int,
    *,
    require_termination_clear: bool = False,
) -> _LockedResumeAggregate:
    route = await _outbox_route(db, outbox_id)
    if route is None:
        raise CapabilityResumeNotFoundError("Capability resume outbox not found")
    # End the routing read before taking the process-local Task fence.  This is
    # required for SQLite WAL as well as the global process/DB lock order.
    await _end_routing_read(db)
    task = await _lock_task(
        db,
        route[0],
        require_termination_clear=require_termination_clear,
    )
    return await _lock_resume_aggregate(
        db,
        task=task,
        outbox_id=outbox_id,
        expected_invocation_id=route[1],
    )


def _request_identity_matches(
    task: Task,
    invocation: CapabilityInvocation,
    outbox: CapabilityResumeOutbox,
) -> bool:
    return bool(
        invocation.task_id == task.id
        and invocation.source == "agent_request"
        and invocation.purpose == "advisory"
        and invocation.resume_policy == "resume_task"
        and invocation.request_task_incarnation_id == task.incarnation_id
        and invocation.request_task_incarnation_id
        == outbox.request_task_incarnation_id
        and invocation.request_task_retry_count == task.retry_count
        and invocation.request_task_retry_count
        == outbox.request_task_retry_count
        and invocation.request_task_turn_generation
        == outbox.from_turn_generation
        and invocation.request_task_session_id == task.session_id
        and invocation.request_task_session_id
        == outbox.request_task_session_id
        and invocation.request_source_log_id == outbox.request_source_log_id
        and invocation.request_output_log_id == outbox.request_output_log_id
        and invocation.request_terminal_log_id
        == outbox.request_terminal_log_id
        and invocation.request_native_turn_id == outbox.request_native_turn_id
        and outbox.request_execution_user_id == task.execution_user_id
        and outbox.request_execution_user_role == task.execution_user_role
        and outbox.request_execution_mode == task.execution_mode
        and outbox.request_execution_principal_kind
        == task.execution_principal_kind
        and outbox.active_task_id in (None, task.id)
        and outbox.active_invocation_id in (None, invocation.id)
    )


def _validate_task_scope(task: Task) -> None:
    if (
        task.mode != "auto"
        or task.worker_id is not None
        or task.shared_from_id is not None
        or task.delivery_run_id is not None
        or task.delivery_role is not None
        or task.plan_target_task_id is not None
    ):
        raise CapabilityResumeIntegrityError(
            "Capability resume Task is outside the local Auto scope"
        )


def _validate_aggregate_identity(
    aggregate: _LockedResumeAggregate,
    *,
    expected_task_statuses: set[str] | None = None,
) -> None:
    task = aggregate.task
    invocation = aggregate.invocation
    outbox = aggregate.outbox
    _validate_task_scope(task)
    if not _request_identity_matches(task, invocation, outbox):
        raise CapabilityResumeIntegrityError(
            "Capability resume request identity changed"
        )
    if expected_task_statuses is not None and task.status not in expected_task_statuses:
        raise CapabilityResumeIntegrityError(
            f"Capability resume Task is in unexpected status {task.status!r}"
        )
    if outbox.status in _ACTIVE_OUTBOX_STATUSES and (
        outbox.active_task_id != task.id
        or outbox.active_invocation_id != invocation.id
    ):
        raise CapabilityResumeIntegrityError(
            "Capability resume active slot changed"
        )
    if invocation.status in _READY_INVOCATION_STATUSES:
        if invocation.active_task_id != task.id:
            raise CapabilityResumeIntegrityError(
                "Resumable Invocation lost its active Task slot"
            )
    elif invocation.status in _TERMINAL_FAILURE_INVOCATION_STATUSES | {
        "completed"
    }:
        if invocation.active_task_id is not None:
            raise CapabilityResumeIntegrityError(
                "Terminal Invocation still owns an active Task slot"
            )


async def _validate_request_logs(
    db: AsyncSession,
    aggregate: _LockedResumeAggregate,
) -> None:
    task = aggregate.task
    invocation = aggregate.invocation
    outbox = aggregate.outbox
    log_ids = {
        outbox.request_source_log_id,
        outbox.request_output_log_id,
        outbox.request_terminal_log_id,
    }
    rows = list(
        (
            await db.execute(
                select(LogEntry)
                .where(LogEntry.id.in_(log_ids))
                .order_by(LogEntry.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalars()
    )
    by_id = {row.id: row for row in rows}
    source = by_id.get(outbox.request_source_log_id)
    output = by_id.get(outbox.request_output_log_id)
    terminal = by_id.get(outbox.request_terminal_log_id)
    if source is None or output is None or terminal is None:
        raise CapabilityResumeIntegrityError(
            "Capability resume lost its exact terminal logs"
        )
    if (
        source.task_id != task.id
        or source.task_retry_count != outbox.request_task_retry_count
        or source.task_turn_generation != outbox.from_turn_generation
        or source.turn_scope != "source"
        or source.actual_transport not in _ACTUAL_TRANSPORTS
    ):
        raise CapabilityResumeIntegrityError(
            "Capability resume source log is stale or malformed"
        )
    for row in (output, terminal):
        if (
            row.task_id != task.id
            or row.task_retry_count != outbox.request_task_retry_count
            or row.task_turn_generation != outbox.from_turn_generation
            or row.turn_scope != "foreground"
        ):
            raise CapabilityResumeIntegrityError(
                "Capability resume terminal output belongs to another turn"
            )
    if not isinstance(output.content, str):
        raise CapabilityResumeIntegrityError(
            "Capability resume terminal output is not text"
        )
    if _text_hash(output.content) != invocation.request_output_hash:
        raise CapabilityResumeIntegrityError(
            "Capability resume terminal output hash changed"
        )
    native_turn_id = outbox.request_native_turn_id
    if native_turn_id is not None and (
        output.native_turn_id != native_turn_id
        or terminal.native_turn_id != native_turn_id
    ):
        raise CapabilityResumeIntegrityError(
            "Capability resume native turn identity changed"
        )


def _validate_execution_terminal(
    aggregate: _LockedResumeAggregate,
    terminal_status: str,
) -> None:
    executions = aggregate.executions
    if not executions:
        raise CapabilityResumeIntegrityError(
            "Capability Invocation has no Execution audit row"
        )
    active = [
        execution
        for execution in executions
        if execution.active_invocation_id == aggregate.invocation.id
    ]
    if active:
        raise CapabilityResumeIntegrityError(
            "Terminal Capability Invocation still has an active Execution"
        )
    if terminal_status == "failed" and executions[-1].status != "failed":
        raise CapabilityResumeIntegrityError(
            "Failed Capability Invocation lacks a final failed Execution"
        )
    if terminal_status == "cancelled" and not any(
        execution.status == "cancelled" for execution in executions
    ):
        raise CapabilityResumeIntegrityError(
            "Cancelled Capability Invocation lacks a cancelled Execution"
        )
    if terminal_status == "stale" and not any(
        execution.status in {"stale", "completed"} for execution in executions
    ):
        raise CapabilityResumeIntegrityError(
            "Stale Capability Invocation lacks immutable terminal evidence"
        )


async def _build_frozen_payload(
    db: AsyncSession,
    aggregate: _LockedResumeAggregate,
) -> tuple[str, dict[str, Any], str]:
    invocation = aggregate.invocation
    task = aggregate.task
    outbox = aggregate.outbox
    if invocation.status in _RESULT_INVOCATION_STATUSES:
        from backend.services.capability_result import resolve_capability_result

        try:
            resolved = await resolve_capability_result(db, invocation)
        except CapabilityConflictError as exc:
            raise CapabilityResumeIntegrityError(
                "Capability resume result lost its exact authoritative graph"
            ) from exc
        terminal_status = "completed"
        result_payload: dict[str, Any] | None = {
            "execution_id": resolved.execution_id,
            "kind": resolved.kind,
            "id": resolved.id,
            "hash": resolved.hash,
            "resource_url": resolved.resource_url,
            "data": resolved.data,
        }
        error_payload = None
    elif invocation.status in _TERMINAL_FAILURE_INVOCATION_STATUSES:
        terminal_status = invocation.status
        _validate_execution_terminal(aggregate, terminal_status)
        result_payload = None
        error_payload = {
            "code": invocation.error_code
            or f"capability_{terminal_status}",
            "message": invocation.error_message
            or f"Capability invocation {terminal_status}",
        }
    else:
        raise CapabilityResumeConflictError(
            "Capability Invocation has not reached a resumable terminal state"
        )

    outcome = {
        "invocation_id": invocation.id,
        "capability": invocation.capability_key,
        "status": terminal_status,
        "request_reason": invocation.request_reason,
        "result": result_payload,
        "error": error_payload,
    }
    prompt = _resume_prompt(outcome)
    created_at = outbox.created_at
    if not isinstance(created_at, datetime):
        raise CapabilityResumeIntegrityError(
            "Capability resume outbox lacks a creation timestamp"
        )
    payload = {
        "schema_version": RESUME_PAYLOAD_VERSION,
        "type": RESUME_PAYLOAD_TYPE,
        "task": {
            "id": task.id,
            "incarnation_id": task.incarnation_id,
            "retry_count": outbox.request_task_retry_count,
            "from_turn_generation": outbox.from_turn_generation,
            "session_id": outbox.request_task_session_id,
            "execution_principal": {
                "user_id": outbox.request_execution_user_id,
                "role": outbox.request_execution_user_role,
                "mode": outbox.request_execution_mode,
                "kind": outbox.request_execution_principal_kind,
            },
        },
        "routing": {
            "provider": (task.provider or "claude").lower(),
            "model": task.model,
            "service_tier": task.codex_service_tier or "default",
        },
        "queue": {
            "timestamp": _utc_timestamp(created_at),
            "prompt": prompt,
            "current_message": prompt,
        },
        "outcome": outcome,
    }
    return terminal_status, payload, _payload_hash(payload)


def _frozen_columns_match(
    outbox: CapabilityResumeOutbox,
    *,
    terminal_status: str,
    payload: dict[str, Any],
    payload_hash: str,
    invocation: CapabilityInvocation,
) -> bool:
    expected_result = (
        (
            invocation.result_kind,
            invocation.result_id,
            invocation.result_hash,
        )
        if terminal_status == "completed"
        else (None, None, None)
    )
    return bool(
        outbox.invocation_terminal_status == terminal_status
        and (
            outbox.invocation_result_kind,
            outbox.invocation_result_id,
            outbox.invocation_result_hash,
        )
        == expected_result
        and outbox.invocation_error_code
        == (None if terminal_status == "completed" else invocation.error_code)
        and outbox.invocation_error_message
        == (None if terminal_status == "completed" else invocation.error_message)
        and outbox.resume_payload == payload
        and outbox.resume_payload_hash == payload_hash
    )


def _envelope_from_payload(
    outbox: CapabilityResumeOutbox,
    payload: dict[str, Any],
) -> ResumeEnvelope:
    try:
        routing = payload["routing"]
        queue = payload["queue"]
        task_payload = payload["task"]
        principal = task_payload["execution_principal"]
        prompt = queue["prompt"]
        current_message = queue["current_message"]
        timestamp = queue["timestamp"]
        provider = routing["provider"]
        model = routing["model"]
        service_tier = routing["service_tier"]
    except (KeyError, TypeError) as exc:
        raise CapabilityResumeIntegrityError(
            "Capability resume payload shape changed"
        ) from exc
    if (
        payload.get("schema_version") != RESUME_PAYLOAD_VERSION
        or payload.get("type") != RESUME_PAYLOAD_TYPE
        or task_payload.get("id") != outbox.task_id
        or task_payload.get("incarnation_id")
        != outbox.request_task_incarnation_id
        or task_payload.get("retry_count") != outbox.request_task_retry_count
        or task_payload.get("from_turn_generation")
        != outbox.from_turn_generation
        or task_payload.get("session_id") != outbox.request_task_session_id
        or principal.get("user_id") != outbox.request_execution_user_id
        or principal.get("role") != outbox.request_execution_user_role
        or principal.get("mode") != outbox.request_execution_mode
        or principal.get("kind") != outbox.request_execution_principal_kind
        or not isinstance(prompt, str)
        or not isinstance(current_message, str)
        or current_message != prompt
        or not isinstance(timestamp, (int, float))
        or isinstance(timestamp, bool)
        or not isinstance(provider, str)
        or (model is not None and not isinstance(model, str))
        or not isinstance(service_tier, str)
        or not isinstance(outbox.resume_payload_hash, str)
    ):
        raise CapabilityResumeIntegrityError(
            "Capability resume payload identity changed"
        )
    return ResumeEnvelope(
        outbox_id=outbox.id,
        task_id=outbox.task_id,
        invocation_id=outbox.invocation_id,
        status=outbox.status,
        lease_token=outbox.lease_token,
        prompt=prompt,
        current_message=current_message,
        queue_timestamp=float(timestamp),
        provider=provider,
        model=model,
        service_tier=service_tier,
        from_generation=outbox.from_turn_generation,
        request_retry_count=outbox.request_task_retry_count,
        request_session_id=outbox.request_task_session_id,
        payload_hash=outbox.resume_payload_hash,
        claimed_generation=outbox.claimed_turn_generation,
        source_log_id=outbox.resume_source_log_id,
        execution_user_id=outbox.request_execution_user_id,
        execution_user_role=outbox.request_execution_user_role,
        execution_mode=outbox.request_execution_mode,
        execution_principal_kind=outbox.request_execution_principal_kind,
    )


async def _verified_envelope(
    db: AsyncSession,
    aggregate: _LockedResumeAggregate,
    *,
    expected_lease_token: str | None = None,
) -> ResumeEnvelope:
    outbox = aggregate.outbox
    if not isinstance(outbox.resume_payload, dict):
        raise CapabilityResumeIntegrityError(
            "Capability resume payload is missing"
        )
    terminal_status, rebuilt, rebuilt_hash = await _build_frozen_payload(
        db, aggregate
    )
    if not _frozen_columns_match(
        outbox,
        terminal_status=terminal_status,
        payload=rebuilt,
        payload_hash=rebuilt_hash,
        invocation=aggregate.invocation,
    ):
        raise CapabilityResumeIntegrityError(
            "Capability resume payload or result hash changed"
        )
    if expected_lease_token is not None and outbox.lease_token != expected_lease_token:
        raise CapabilityResumeConflictError(
            "Capability resume publication lease changed"
        )
    return _envelope_from_payload(outbox, _detached_payload(outbox.resume_payload))


def _terminalize_active_invocation(
    aggregate: _LockedResumeAggregate,
    *,
    status: Literal["failed", "cancelled"],
    error_code: str,
    error_message: str,
    now: datetime,
) -> None:
    invocation = aggregate.invocation
    if invocation.status in {"ready", "resuming"}:
        invocation.status = status
        invocation.state_version += 1
        invocation.active_task_id = None
        invocation.error_code = error_code
        invocation.error_message = error_message
        invocation.completed_at = now
    elif invocation.status not in _TERMINAL_FAILURE_INVOCATION_STATUSES | {
        "completed"
    }:
        raise CapabilityResumeConflictError(
            "Active Capability execution must be stopped before its resume outbox"
        )


def _terminalize_outbox_locked(
    aggregate: _LockedResumeAggregate,
    *,
    status: Literal["failed", "cancelled"],
    error_code: str,
    error_message: str,
    now: datetime | None = None,
) -> None:
    now = now or datetime.utcnow()
    code = (error_code or f"resume_{status}")[:64]
    message = str(error_message or f"Capability resume {status}")[:2000]
    outbox = aggregate.outbox
    if outbox.status in {"completed", "cancelled", "failed"}:
        return
    _terminalize_active_invocation(
        aggregate,
        status=status,
        error_code=code,
        error_message=message,
        now=now,
    )
    outbox.status = status
    outbox.state_version += 1
    outbox.active_task_id = None
    outbox.active_invocation_id = None
    outbox.lease_token = None
    outbox.lease_expires_at = None
    outbox.next_attempt_at = None
    outbox.error_code = code
    outbox.error_message = message
    outbox.updated_at = now
    outbox.completed_at = now


async def materialize_resume_outbox(
    db: AsyncSession,
    outbox_id: int,
) -> ResumeEnvelope | None:
    """Freeze one terminal Invocation outcome and make it publishable.

    ``None`` means the Capability is still active or the outbox is already
    terminal.  Integrity failures are committed as a failed outbox before the
    exception is re-raised, so polling can never spin on corrupted authority.
    """

    route = await _outbox_route(db, outbox_id)
    if route is None:
        raise CapabilityResumeNotFoundError("Capability resume outbox not found")
    await _end_routing_read(db)
    aggregate: _LockedResumeAggregate | None = None
    integrity_error: CapabilityResumeIntegrityError | None = None
    async with capability_task_lock(route[0]):
        try:
            aggregate = await _lock_from_outbox(
                db,
                outbox_id,
                require_termination_clear=True,
            )
            outbox = aggregate.outbox
            invocation = aggregate.invocation
            if outbox.status in {"completed", "cancelled", "failed", "launched"}:
                await db.rollback()
                return None
            if invocation.status not in _MATERIALIZABLE_INVOCATION_STATUSES:
                await db.rollback()
                return None
            _validate_aggregate_identity(
                aggregate,
                expected_task_statuses={"waiting_capability"},
            )
            expected_generation = (
                outbox.claimed_turn_generation
                if outbox.status == "claimed"
                else outbox.from_turn_generation
            )
            if aggregate.task.turn_generation != expected_generation:
                raise CapabilityResumeIntegrityError(
                    "Capability resume Task generation changed before materialization"
                )
            await _validate_request_logs(db, aggregate)
            terminal_status, payload, digest = await _build_frozen_payload(
                db, aggregate
            )
            if outbox.status == "pending":
                outbox.invocation_terminal_status = terminal_status
                if terminal_status == "completed":
                    outbox.invocation_result_kind = invocation.result_kind
                    outbox.invocation_result_id = invocation.result_id
                    outbox.invocation_result_hash = invocation.result_hash
                    outbox.invocation_error_code = None
                    outbox.invocation_error_message = None
                else:
                    outbox.invocation_result_kind = None
                    outbox.invocation_result_id = None
                    outbox.invocation_result_hash = None
                    outbox.invocation_error_code = invocation.error_code
                    outbox.invocation_error_message = invocation.error_message
                outbox.resume_payload = _detached_payload(payload)
                outbox.resume_payload_hash = digest
                outbox.status = "ready"
                outbox.state_version += 1
                outbox.ready_at = datetime.utcnow()
                outbox.updated_at = outbox.ready_at
                outbox.error_code = None
                outbox.error_message = None
                if invocation.status == "ready":
                    invocation.status = "resuming"
                    invocation.state_version += 1
                    invocation.updated_at = outbox.ready_at
            elif outbox.status == "ready":
                if not _frozen_columns_match(
                    outbox,
                    terminal_status=terminal_status,
                    payload=payload,
                    payload_hash=digest,
                    invocation=invocation,
                ):
                    raise CapabilityResumeIntegrityError(
                        "Capability resume frozen result changed"
                    )
                if invocation.status == "ready":
                    invocation.status = "resuming"
                    invocation.state_version += 1
                    invocation.updated_at = datetime.utcnow()
            else:
                # claiming is publication-owned and claimed is Task-owned;
                # neither may be rematerialized into a new payload.
                envelope = await _verified_envelope(db, aggregate)
                await db.rollback()
                return envelope
            await db.commit()
        except CapabilityResumeIntegrityError as exc:
            integrity_error = exc
            if aggregate is None:
                await db.rollback()
            else:
                try:
                    _terminalize_outbox_locked(
                        aggregate,
                        status="failed",
                        error_code="resume_integrity_failed",
                        error_message=str(exc),
                    )
                    if aggregate.task.status == "waiting_capability":
                        aggregate.task.status = "failed"
                        aggregate.task.completed_at = datetime.utcnow()
                        aggregate.task.error_message = str(exc)[:2000]
                    await db.commit()
                except BaseException:
                    await db.rollback()
                    raise
        except BaseException:
            await db.rollback()
            raise

    if integrity_error is not None:
        raise integrity_error
    # Return a fresh detached read; this also catches a commit acknowledgement
    # implementation that accidentally wrote a non-canonical JSON shape.
    envelope = await load_resume_envelope(db, outbox_id)
    if envelope is None:
        raise CapabilityResumeIntegrityError(
            "Materialized Capability resume disappeared after commit"
        )
    return envelope


async def load_resume_envelope(
    db: AsyncSession,
    outbox_id: int,
    *,
    expected_lease_token: str | None = None,
    for_update: bool = False,
) -> ResumeEnvelope | None:
    """Rebuild and verify a detached resume envelope.

    ``for_update`` is intended only for a caller that already holds the Task
    row and ``capability_task_lock``.  Normal routing callers must use the
    read-only default and then a dedicated mutation helper.
    """

    route = await _outbox_route(db, outbox_id)
    if route is None:
        return None
    if for_update:
        task = await db.get(Task, route[0], populate_existing=True)
        if task is None:
            return None
        aggregate = await _lock_resume_aggregate(
            db,
            task=task,
            outbox_id=outbox_id,
            expected_invocation_id=route[1],
        )
    else:
        task = await db.get(Task, route[0], populate_existing=True)
        invocation = await db.get(
            CapabilityInvocation,
            route[1],
            populate_existing=True,
        )
        outbox = await db.get(
            CapabilityResumeOutbox,
            outbox_id,
            populate_existing=True,
        )
        if task is None or invocation is None or outbox is None:
            return None
        executions = list(
            (
                await db.execute(
                    select(CapabilityExecution)
                    .where(CapabilityExecution.invocation_id == invocation.id)
                    .order_by(CapabilityExecution.attempt, CapabilityExecution.id)
                    .execution_options(populate_existing=True)
                )
            ).scalars()
        )
        aggregate = _LockedResumeAggregate(task, invocation, executions, outbox)
    if aggregate.outbox.status not in {
        "ready",
        "claiming",
        "claimed",
        "launched",
        "completed",
    }:
        return None
    _validate_aggregate_identity(aggregate)
    return await _verified_envelope(
        db,
        aggregate,
        expected_lease_token=expected_lease_token,
    )


async def claim_resume_publication(
    db: AsyncSession,
    outbox_id: int,
    *,
    lease_seconds: float = 30.0,
) -> ResumeEnvelope | None:
    """Claim a durable queue-publication lease and commit it before publish."""

    if lease_seconds <= 0:
        raise ValueError("lease_seconds must be positive")
    route = await _outbox_route(db, outbox_id)
    if route is None:
        return None
    await _end_routing_read(db)
    async with capability_task_lock(route[0]):
        try:
            aggregate = await _lock_from_outbox(db, outbox_id)
            outbox = aggregate.outbox
            now = datetime.utcnow()
            if outbox.status not in _PUBLISHABLE_OUTBOX_STATUSES:
                await db.rollback()
                return None
            if aggregate.task.status != "waiting_capability":
                await db.rollback()
                return None
            if outbox.status in {"ready", "claiming"}:
                expected_generation = outbox.from_turn_generation
                if outbox.resume_source_log_id is not None:
                    raise CapabilityResumeIntegrityError(
                        "Unclaimed Capability resume already has a source"
                    )
            else:
                expected_generation = outbox.claimed_turn_generation
                if (
                    expected_generation != outbox.from_turn_generation + 1
                    or outbox.resume_source_log_id is None
                ):
                    raise CapabilityResumeIntegrityError(
                        "Claimed Capability resume lacks exact G+1 evidence"
                    )
            if aggregate.task.turn_generation != expected_generation:
                raise CapabilityResumeIntegrityError(
                    "Capability resume publication Task generation changed"
                )
            if outbox.next_attempt_at is not None and outbox.next_attempt_at > now:
                await db.rollback()
                return None
            if (
                outbox.lease_token is not None
                and outbox.lease_expires_at is not None
                and outbox.lease_expires_at > now
            ):
                await db.rollback()
                return None
            if outbox.status == "claimed":
                await _validate_waiting_claimed_resume_locked(db, aggregate)
            else:
                await _verified_envelope(db, aggregate)
            token = secrets.token_hex(32)
            if outbox.status in {"ready", "claiming"}:
                outbox.status = "claiming"
            outbox.lease_token = token
            outbox.lease_expires_at = now + timedelta(seconds=lease_seconds)
            outbox.attempt_count += 1
            outbox.next_attempt_at = None
            outbox.error_code = None
            outbox.error_message = None
            outbox.state_version += 1
            outbox.updated_at = now
            await db.commit()
        except BaseException:
            await db.rollback()
            raise
    return await load_resume_envelope(
        db,
        outbox_id,
        expected_lease_token=token,
    )


async def _lock_claimed_resume_source(
    db: AsyncSession,
    aggregate: _LockedResumeAggregate,
    *,
    require_instance_match: bool = True,
) -> LogEntry:
    task = aggregate.task
    outbox = aggregate.outbox
    if (
        outbox.claimed_turn_generation != outbox.from_turn_generation + 1
        or task.turn_generation != outbox.claimed_turn_generation
        or task.turn_source_log_id != outbox.resume_source_log_id
    ):
        raise CapabilityResumeIntegrityError(
            "Claimed Capability resume G+1 identity changed"
        )
    source = (
        await db.execute(
            select(LogEntry)
            .where(LogEntry.id == outbox.resume_source_log_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    from backend.services.terminal_arbitration import source_shape_is_canonical

    if (
        source is None
        or source.task_id != task.id
        or source.task_retry_count != task.retry_count
        or source.task_turn_generation != task.turn_generation
        or source.turn_scope != "source"
        or (
            require_instance_match
            and source.instance_id != task.instance_id
        )
        or not source_shape_is_canonical(source)
    ):
        raise CapabilityResumeIntegrityError(
            "Capability resume source is not exact canonical G+1 evidence"
        )
    return source


async def _validate_waiting_claimed_resume_locked(
    db: AsyncSession,
    aggregate: _LockedResumeAggregate,
) -> None:
    task = aggregate.task
    outbox = aggregate.outbox
    _validate_aggregate_identity(
        aggregate,
        expected_task_statuses={"waiting_capability"},
    )
    await _verified_envelope(db, aggregate)
    if task.instance_id is not None or outbox.status != "claimed":
        raise CapabilityResumeIntegrityError(
            "Waiting Capability resume retained an execution owner"
        )
    source = await _lock_claimed_resume_source(
        db,
        aggregate,
        require_instance_match=False,
    )
    if (
        type(source.instance_id) is not int
        or source.instance_id <= 0
        or source.actual_transport is not None
    ):
        raise CapabilityResumeIntegrityError(
            "Waiting Capability resume already crossed its provider boundary"
        )


async def _restore_claimed_resume_to_waiting_locked(
    db: AsyncSession,
    aggregate: _LockedResumeAggregate,
    *,
    now: datetime,
) -> None:
    """Undo only a proven pre-provider G+1 claim.

    The source row and G+1 identity remain immutable replay evidence.  Only
    the ephemeral Instance owner is released.  A source with concrete
    ``actual_transport`` may already have caused an external effect and is
    therefore never made replayable by this helper.
    """

    task = aggregate.task
    outbox = aggregate.outbox
    _validate_aggregate_identity(
        aggregate,
        expected_task_statuses={"executing"},
    )
    await _verified_envelope(db, aggregate)
    if (
        outbox.status != "claimed"
        or outbox.claimed_turn_generation != outbox.from_turn_generation + 1
        or task.turn_generation != outbox.claimed_turn_generation
        or type(task.instance_id) is not int
        or task.instance_id <= 0
    ):
        raise CapabilityResumeIntegrityError(
            "Claimed Capability resume Task identity changed before release"
        )
    source = await _lock_claimed_resume_source(db, aggregate)
    if source.actual_transport is not None:
        raise CapabilityResumeIntegrityError(
            "Claimed Capability resume is not provably before provider launch"
        )
    task.status = "waiting_capability"
    task.instance_id = None
    task.completed_at = None
    task.error_message = None
    outbox.updated_at = now


async def release_resume_publication(
    db: AsyncSession,
    outbox_id: int,
    *,
    lease_token: str,
    error_code: str,
    error_message: str,
    retry_after_seconds: float = 0.0,
) -> bool:
    """Release one exact publication lease without cancelling the outbox."""

    if len(lease_token) != 64:
        return False
    if retry_after_seconds < 0:
        raise ValueError("retry_after_seconds cannot be negative")
    route = await _outbox_route(db, outbox_id)
    if route is None:
        return False
    await _end_routing_read(db)
    async with capability_task_lock(route[0]):
        try:
            aggregate = await _lock_from_outbox(db, outbox_id)
            outbox = aggregate.outbox
            if (
                outbox.status not in {"claiming", "claimed"}
                or outbox.lease_token != lease_token
            ):
                await db.rollback()
                return False
            now = datetime.utcnow()
            if outbox.status == "claiming":
                _validate_aggregate_identity(
                    aggregate,
                    expected_task_statuses={"waiting_capability"},
                )
                if aggregate.task.turn_generation != outbox.from_turn_generation:
                    raise CapabilityResumeIntegrityError(
                        "Capability resume claiming generation changed"
                    )
                outbox.status = "ready"
            else:
                if aggregate.task.status == "waiting_capability":
                    await _validate_waiting_claimed_resume_locked(db, aggregate)
                else:
                    await _restore_claimed_resume_to_waiting_locked(
                        db,
                        aggregate,
                        now=now,
                    )
            outbox.lease_token = None
            outbox.lease_expires_at = None
            outbox.next_attempt_at = now + timedelta(
                seconds=retry_after_seconds
            )
            outbox.error_code = (error_code or "publication_released")[:64]
            outbox.error_message = str(error_message or "Publication released")[
                :2000
            ]
            outbox.state_version += 1
            outbox.updated_at = now
            await db.commit()
            return True
        except BaseException:
            await db.rollback()
            raise


async def recover_expired_resume_publication(
    db: AsyncSession,
    outbox_id: int,
) -> bool:
    """Release an expired claiming/claimed lease for crash recovery."""

    route = await _outbox_route(db, outbox_id)
    if route is None:
        return False
    await _end_routing_read(db)
    async with capability_task_lock(route[0]):
        try:
            aggregate = await _lock_from_outbox(db, outbox_id)
            outbox = aggregate.outbox
            now = datetime.utcnow()
            if (
                outbox.status not in {"claiming", "claimed"}
                or outbox.lease_token is None
                or outbox.lease_expires_at is None
                or outbox.lease_expires_at > now
            ):
                await db.rollback()
                return False
            if outbox.status == "claiming":
                _validate_aggregate_identity(
                    aggregate,
                    expected_task_statuses={"waiting_capability"},
                )
                if aggregate.task.turn_generation != outbox.from_turn_generation:
                    raise CapabilityResumeIntegrityError(
                        "Capability resume claiming generation changed"
                    )
                outbox.status = "ready"
            else:
                if aggregate.task.status == "waiting_capability":
                    await _validate_waiting_claimed_resume_locked(db, aggregate)
                else:
                    await _restore_claimed_resume_to_waiting_locked(
                        db,
                        aggregate,
                        now=now,
                    )
            outbox.lease_token = None
            outbox.lease_expires_at = None
            outbox.next_attempt_at = now
            outbox.error_code = "publication_lease_expired"
            outbox.error_message = (
                "Recovered an expired Capability resume publication lease"
            )
            outbox.state_version += 1
            outbox.updated_at = now
            await db.commit()
            return True
        except BaseException:
            await db.rollback()
            raise


async def claim_resume_turn_locked(
    db: AsyncSession,
    *,
    task: Task,
    outbox_id: int,
    lease_token: str,
    instance_id: int,
    transport: str | None = None,
) -> ResumeTurnClaim:
    """Atomically claim fresh G+1 or replay the already-claimed same G+1.

    The caller owns ``capability_task_lock(task.id)``, the locked Task row, and
    the transaction.  This helper never commits or rolls back.
    """

    if type(instance_id) is not int or instance_id <= 0:
        raise CapabilityResumeConflictError("Resume Instance id is invalid")
    if len(lease_token) != 64:
        raise CapabilityResumeConflictError("Resume publication lease is invalid")
    aggregate = await _lock_resume_aggregate(
        db,
        task=task,
        outbox_id=outbox_id,
    )
    outbox = aggregate.outbox
    if await active_worker_task_termination_receipt(db, task.id):
        raise CapabilityResumeConflictError(
            "Task has an active Worker termination receipt"
        )
    envelope = await _verified_envelope(
        db,
        aggregate,
        expected_lease_token=lease_token,
    )
    current_route = (
        (task.provider or "claude").lower(),
        task.model,
        task.codex_service_tier or "default",
    )
    if current_route != (
        envelope.provider,
        envelope.model,
        envelope.service_tier,
    ):
        raise CapabilityResumeIntegrityError(
            "Capability resume provider/model/tier changed"
        )
    if task.session_id != envelope.request_session_id:
        raise CapabilityResumeIntegrityError(
            "Capability resume native session changed"
        )

    now = datetime.utcnow()
    if (
        outbox.lease_expires_at is None
        or outbox.lease_expires_at <= now
    ):
        raise CapabilityResumeConflictError(
            "Capability resume publication lease expired"
        )
    if outbox.status == "claiming":
        if task.status != "waiting_capability":
            raise CapabilityResumeConflictError(
                "Fresh Capability resume Task is not waiting"
            )
        if task.turn_generation != outbox.from_turn_generation:
            raise CapabilityResumeIntegrityError(
                "Fresh Capability resume baseline generation changed"
            )
        replay = False
        task.turn_generation += 1
    elif outbox.status == "claimed":
        if task.status not in {"waiting_capability", "executing"}:
            raise CapabilityResumeConflictError(
                "Claimed Capability resume Task is not replayable"
            )
        if (
            outbox.claimed_turn_generation != outbox.from_turn_generation + 1
            or task.turn_generation != outbox.claimed_turn_generation
            or outbox.resume_source_log_id is None
        ):
            raise CapabilityResumeIntegrityError(
                "Claimed Capability resume G+1 identity changed"
            )
        existing_source = (
            await db.execute(
                select(LogEntry)
                .where(LogEntry.id == outbox.resume_source_log_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
        if existing_source is None or existing_source.actual_transport is not None:
            raise CapabilityResumeConflictError(
                "Claimed Capability resume already crossed its provider boundary"
            )
        replay = True
    else:
        raise CapabilityResumeConflictError(
            "Capability resume publication is not claimable"
        )

    task.status = "executing"
    task.instance_id = instance_id
    task.completed_at = None
    task.error_message = None
    from backend.services.terminal_arbitration import bind_turn_source

    source = await bind_turn_source(
        db,
        task=task,
        source_log_id=None,
        instance_id=instance_id,
        transport=transport,
    )
    if replay and source.id != outbox.resume_source_log_id:
        raise CapabilityResumeIntegrityError(
            "Capability resume replay bound a different source"
        )
    outbox.status = "claimed"
    outbox.resume_source_log_id = source.id
    outbox.claimed_turn_generation = task.turn_generation
    outbox.claimed_at = outbox.claimed_at or now
    # Keep the exact queue owner's lease through the pre-provider boundary.
    # A Task/outbox commit can succeed while the consumer loses the commit
    # acknowledgement.  The same QueuedMessage must then be able to reload and
    # replay this exact G+1 rather than orphaning an executing Task.  Launch
    # promotion or explicit release is the boundary that clears the lease.
    outbox.lease_token = lease_token
    outbox.next_attempt_at = None
    outbox.error_code = None
    outbox.error_message = None
    outbox.state_version += 1
    outbox.updated_at = now

    claimed_envelope = ResumeEnvelope(
        outbox_id=envelope.outbox_id,
        task_id=envelope.task_id,
        invocation_id=envelope.invocation_id,
        status="claimed",
        lease_token=lease_token,
        prompt=envelope.prompt,
        current_message=envelope.current_message,
        queue_timestamp=envelope.queue_timestamp,
        provider=envelope.provider,
        model=envelope.model,
        service_tier=envelope.service_tier,
        from_generation=envelope.from_generation,
        request_retry_count=envelope.request_retry_count,
        request_session_id=envelope.request_session_id,
        payload_hash=envelope.payload_hash,
        claimed_generation=task.turn_generation,
        source_log_id=source.id,
        execution_user_id=envelope.execution_user_id,
        execution_user_role=envelope.execution_user_role,
        execution_mode=envelope.execution_mode,
        execution_principal_kind=envelope.execution_principal_kind,
    )
    return ResumeTurnClaim(
        envelope=claimed_envelope,
        retry_count=task.retry_count,
        turn_generation=task.turn_generation,
        source_log_id=source.id,
        replay=replay,
    )


async def mark_resume_launch_boundary_in_tx(
    db: AsyncSession,
    *,
    task: Task,
    outbox_id: int,
    retry_count: int,
    turn_generation: int,
    source_log_id: int,
) -> bool:
    """Promote exact claimed evidence after source.actual_transport commits."""

    aggregate = await _lock_resume_aggregate(
        db,
        task=task,
        outbox_id=outbox_id,
    )
    outbox = aggregate.outbox
    if outbox.status == "launched":
        return bool(
            outbox.claimed_turn_generation == turn_generation
            and outbox.resume_source_log_id == source_log_id
            and outbox.resume_actual_transport in _ACTUAL_TRANSPORTS
        )
    if outbox.status != "claimed":
        return False
    if (
        task.retry_count != retry_count
        or task.turn_generation != turn_generation
        or task.turn_source_log_id != source_log_id
        or outbox.request_task_retry_count != retry_count
        or outbox.claimed_turn_generation != turn_generation
        or outbox.resume_source_log_id != source_log_id
    ):
        raise CapabilityResumeIntegrityError(
            "Capability resume launch boundary identity changed"
        )
    await _verified_envelope(db, aggregate)
    source = (
        await db.execute(
            select(LogEntry)
            .where(LogEntry.id == source_log_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if (
        source is None
        or source.task_id != task.id
        or source.task_retry_count != retry_count
        or source.task_turn_generation != turn_generation
        or source.turn_scope != "source"
        or source.actual_transport not in _ACTUAL_TRANSPORTS
    ):
        raise CapabilityResumeConflictError(
            "Capability resume source has no exact provider boundary"
        )
    now = datetime.utcnow()
    outbox.status = "launched"
    outbox.active_task_id = None
    outbox.active_invocation_id = None
    outbox.resume_actual_transport = source.actual_transport
    outbox.lease_token = None
    outbox.lease_expires_at = None
    outbox.next_attempt_at = None
    outbox.error_code = None
    outbox.error_message = None
    outbox.launched_at = now
    outbox.updated_at = now
    outbox.state_version += 1
    return True


async def mark_resume_launch_boundary(
    db_factory: Callable[[], Any],
    *,
    outbox_id: int,
    task_id: int,
    retry_count: int,
    turn_generation: int,
    source_log_id: int,
) -> bool:
    """Own the Task fence and commit the exact provider-boundary callback."""

    async with capability_task_lock(task_id):
        async with db_factory() as db:
            try:
                task = await _lock_task(
                    db,
                    task_id,
                    require_termination_clear=True,
                )
                changed = await mark_resume_launch_boundary_in_tx(
                    db,
                    task=task,
                    outbox_id=outbox_id,
                    retry_count=retry_count,
                    turn_generation=turn_generation,
                    source_log_id=source_log_id,
                )
                if changed:
                    await db.commit()
                else:
                    await db.rollback()
                return changed
            except BaseException:
                await db.rollback()
                raise


async def fail_or_cancel_resume_outbox_in_tx(
    db: AsyncSession,
    *,
    task: Task,
    outbox_id: int,
    status: Literal["failed", "cancelled"],
    error_code: str,
    error_message: str,
) -> bool:
    """Terminalize a post-Capability outbox and any resuming Invocation."""

    aggregate = await _lock_resume_aggregate(
        db,
        task=task,
        outbox_id=outbox_id,
    )
    if aggregate.outbox.status in {"completed", "cancelled", "failed"}:
        return False
    _terminalize_outbox_locked(
        aggregate,
        status=status,
        error_code=error_code,
        error_message=error_message,
    )
    return True


async def cancel_task_resume_outbox_in_tx(
    db: AsyncSession,
    task: Task,
    *,
    reason: str,
) -> bool:
    """Cancel the one active resume outbox during an exact Task-wide stop."""

    ids = list(
        (
            await db.scalars(
                select(CapabilityResumeOutbox.id).where(
                    CapabilityResumeOutbox.task_id == task.id,
                    CapabilityResumeOutbox.status.in_(
                        ("pending", "ready", "claiming", "claimed", "launched")
                    ),
                )
            )
        ).all()
    )
    if not ids:
        return False
    if len(ids) != 1:
        raise CapabilityResumeIntegrityError(
            "Task has multiple active Capability resume outboxes"
        )
    aggregate = await _lock_resume_aggregate(
        db,
        task=task,
        outbox_id=ids[0],
    )
    if aggregate.outbox.status in {"completed", "cancelled", "failed"}:
        return False
    now = datetime.utcnow()
    message = str(reason or "Task cancelled")[:2000]
    invocation = aggregate.invocation
    active_executions = [
        execution
        for execution in aggregate.executions
        if execution.status in ACTIVE_EXECUTION_STATUSES
    ]
    if invocation.status in {"running", "waiting_user", "cancelling"}:
        raise CapabilityResumeConflictError(
            "Capability executor must reach a durable terminal state before "
            "Task-wide resume cancellation"
        )
    if invocation.status == "queued":
        if (
            len(active_executions) != 1
            or active_executions[0].status != "queued"
            or active_executions[0].handle_kind is not None
            or active_executions[0].handle_id is not None
            or active_executions[0].handle_generation is not None
            or active_executions[0].lease_token is not None
            or active_executions[0].lease_expires_at is not None
            or active_executions[0].heartbeat_at is not None
            or active_executions[0].started_at is not None
        ):
            raise CapabilityResumeIntegrityError(
                "Queued Capability cancellation lacks a no-runtime proof"
            )
        execution = active_executions[0]
        execution.status = "cancelled"
        execution.state_version += 1
        execution.active_invocation_id = None
        execution.lease_token = None
        execution.lease_expires_at = None
        execution.error_code = "task_cancelled"
        execution.error_message = message
        execution.completed_at = now
    elif active_executions:
        raise CapabilityResumeIntegrityError(
            "Terminal/ready Capability Invocation retained an active Execution"
        )
    if invocation.status in {"queued", "ready", "resuming"}:
        invocation.status = "cancelled"
        invocation.state_version += 1
        invocation.active_task_id = None
        invocation.error_code = "task_cancelled"
        invocation.error_message = message
        invocation.completed_at = now
        invocation.updated_at = now
    _terminalize_outbox_locked(
        aggregate,
        status="cancelled",
        error_code="task_cancelled",
        error_message=message,
        now=now,
    )
    return True


async def settle_previous_resume_in_terminal_tx(
    db: AsyncSession,
    task: Task,
) -> bool:
    """Complete the exact G+1 outbox before interpreting its terminal output."""

    if (
        type(task.turn_source_log_id) is not int
        or task.turn_source_log_id <= 0
    ):
        return False
    ids = list(
        (
            await db.scalars(
                select(CapabilityResumeOutbox.id).where(
                    CapabilityResumeOutbox.task_id == task.id,
                    CapabilityResumeOutbox.request_task_incarnation_id
                    == task.incarnation_id,
                    CapabilityResumeOutbox.request_task_retry_count
                    == task.retry_count,
                    CapabilityResumeOutbox.claimed_turn_generation
                    == task.turn_generation,
                    CapabilityResumeOutbox.resume_source_log_id
                    == task.turn_source_log_id,
                    CapabilityResumeOutbox.status.in_(("claimed", "launched")),
                )
            )
        ).all()
    )
    if not ids:
        return False
    if len(ids) != 1:
        raise CapabilityResumeIntegrityError(
            "Task terminal generation has multiple Capability resume outboxes"
        )
    aggregate = await _lock_resume_aggregate(
        db,
        task=task,
        outbox_id=ids[0],
    )
    outbox = aggregate.outbox
    await _verified_envelope(db, aggregate)
    source = (
        await db.execute(
            select(LogEntry)
            .where(LogEntry.id == task.turn_source_log_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if (
        source is None
        or source.task_id != task.id
        or source.task_retry_count != task.retry_count
        or source.task_turn_generation != task.turn_generation
        or source.turn_scope != "source"
        or source.actual_transport not in _ACTUAL_TRANSPORTS
    ):
        raise CapabilityResumeIntegrityError(
            "Terminal Capability resume lacks exact provider evidence"
        )
    if outbox.status == "claimed":
        outbox.status = "launched"
        outbox.active_task_id = None
        outbox.active_invocation_id = None
        outbox.resume_actual_transport = source.actual_transport
        outbox.launched_at = datetime.utcnow()
        outbox.state_version += 1
    elif outbox.resume_actual_transport != source.actual_transport:
        raise CapabilityResumeIntegrityError(
            "Capability resume launch transport changed"
        )

    now = datetime.utcnow()
    outbox.status = "completed"
    outbox.active_task_id = None
    outbox.active_invocation_id = None
    outbox.lease_token = None
    outbox.lease_expires_at = None
    outbox.next_attempt_at = None
    outbox.completed_at = now
    outbox.updated_at = now
    outbox.state_version += 1
    invocation = aggregate.invocation
    if invocation.status == "resuming":
        invocation.status = "completed"
        invocation.state_version += 1
        invocation.active_task_id = None
        invocation.completed_at = now
        invocation.updated_at = now
    elif invocation.status not in _TERMINAL_FAILURE_INVOCATION_STATUSES | {
        "completed"
    }:
        raise CapabilityResumeIntegrityError(
            "Capability resume terminal settlement found an active Invocation"
        )
    return True


def _fail_stale_resume_locked(
    aggregate: _LockedResumeAggregate | None,
    task: Task,
    *,
    error_code: str,
    error_message: str,
    retain_instance_evidence: bool,
) -> None:
    """Fail closed without pretending an uncertain provider effect is absent."""

    now = datetime.utcnow()
    if aggregate is not None:
        try:
            _terminalize_outbox_locked(
                aggregate,
                status="failed",
                error_code=error_code,
                error_message=error_message,
                now=now,
            )
        except CapabilityResumeConflictError:
            # A corrupt executing Task can point at an Invocation that is
            # still owned by a Capability executor.  Do not silently cancel
            # that independent runtime here.  Failing the Task still removes
            # all permission to publish or replay its resume outbox.
            pass
    task.status = "failed"
    if not retain_instance_evidence:
        task.instance_id = None
    task.completed_at = now
    task.error_message = str(error_message)[:2000]


async def reconcile_stale_resume_in_tx(
    db: AsyncSession,
    task: Task,
    *,
    has_live_runtime: bool,
    unmanaged_pid: int | None,
) -> Literal["not_resume", "replayable", "failed", "preserved"]:
    """Reconcile an active Task's exact resume generation during startup.

    The caller owns the locked Task row and has fenced Task/capability
    admission (normally Dispatcher startup reconciliation).  This helper
    never commits or rolls back and does not acquire the process-local
    ``capability_task_lock`` after a database lock.  It locks Invocation ->
    Executions -> Outbox -> resume source, matching the aggregate-wide order.
    """

    if not isinstance(has_live_runtime, bool):
        raise TypeError("has_live_runtime must be a bool")
    if unmanaged_pid is not None and (
        type(unmanaged_pid) is not int or unmanaged_pid <= 0
    ):
        raise ValueError("unmanaged_pid must be a positive integer or None")

    outbox_ids = list(
        (
            await db.scalars(
                select(CapabilityResumeOutbox.id)
                .where(
                    CapabilityResumeOutbox.task_id == task.id,
                    CapabilityResumeOutbox.status.in_(
                        ("pending", "ready", "claiming", "claimed", "launched")
                    ),
                )
                .order_by(CapabilityResumeOutbox.id)
            )
        ).all()
    )
    if not outbox_ids:
        return "not_resume"
    if len(outbox_ids) != 1:
        _fail_stale_resume_locked(
            None,
            task,
            error_code="resume_recovery_ambiguous",
            error_message=(
                "Recovered multiple active Capability resume outboxes; "
                "automatic replay was blocked"
            ),
            retain_instance_evidence=bool(
                has_live_runtime or unmanaged_pid is not None
            ),
        )
        return "failed"

    aggregate = await _lock_resume_aggregate(
        db,
        task=task,
        outbox_id=outbox_ids[0],
    )
    outbox = aggregate.outbox

    async def fail(code: str, message: str) -> Literal["failed"]:
        _fail_stale_resume_locked(
            aggregate,
            task,
            error_code=code,
            error_message=message,
            retain_instance_evidence=bool(
                has_live_runtime or unmanaged_pid is not None
            ),
        )
        return "failed"

    try:
        if outbox.status not in {"claimed", "launched"}:
            return await fail(
                "resume_recovery_state_invalid",
                "Executing Task has an unclaimed Capability resume outbox",
            )
        _validate_aggregate_identity(
            aggregate,
            expected_task_statuses={"executing"},
        )
        await _verified_envelope(db, aggregate)
        source = await _lock_claimed_resume_source(db, aggregate)
        if source.actual_transport not in _ACTUAL_TRANSPORTS | {None}:
            raise CapabilityResumeIntegrityError(
                "Capability resume source has an invalid provider transport"
            )
        if outbox.status == "launched":
            if (
                source.actual_transport not in _ACTUAL_TRANSPORTS
                or outbox.resume_actual_transport != source.actual_transport
                or outbox.launched_at is None
            ):
                raise CapabilityResumeIntegrityError(
                    "Launched Capability resume lost its provider evidence"
                )
        elif source.actual_transport in _ACTUAL_TRANSPORTS:
            # The aggregate and source are already locked in canonical order;
            # promote in place instead of re-entering the public helper and
            # attempting to acquire aggregate locks while holding the source.
            now = datetime.utcnow()
            outbox.status = "launched"
            outbox.active_task_id = None
            outbox.active_invocation_id = None
            outbox.resume_actual_transport = source.actual_transport
            outbox.lease_token = None
            outbox.lease_expires_at = None
            outbox.next_attempt_at = None
            outbox.error_code = None
            outbox.error_message = None
            outbox.launched_at = now
            outbox.updated_at = now
            outbox.state_version += 1
    except (CapabilityConflictError, CapabilityResumeError) as exc:
        return await fail(
            "resume_recovery_integrity_failed",
            f"Capability resume recovery failed closed: {exc}",
        )

    if unmanaged_pid is not None:
        return await fail(
            "resume_unmanaged_runtime",
            (
                f"Unmanaged process PID {unmanaged_pid} may still be running "
                "after manager restart; Capability resume replay was blocked"
            ),
        )
    if has_live_runtime:
        return "preserved"

    # No runtime and no concrete provider boundary is the only replayable
    # restart state.  Preserve the exact hidden source and G+1 generation so
    # the next queue owner reuses, rather than increments, the logical turn.
    if outbox.status == "claimed" and source.actual_transport is None:
        try:
            await _restore_claimed_resume_to_waiting_locked(
                db,
                aggregate,
                now=datetime.utcnow(),
            )
        except CapabilityResumeError as exc:
            return await fail(
                "resume_recovery_integrity_failed",
                f"Capability resume replay proof failed: {exc}",
            )
        now = datetime.utcnow()
        outbox.lease_token = None
        outbox.lease_expires_at = None
        outbox.next_attempt_at = now
        outbox.error_code = "resume_restart_replay"
        outbox.error_message = (
            "Recovered an exact pre-provider Capability resume generation"
        )
        outbox.state_version += 1
        outbox.updated_at = now
        return "replayable"

    return await fail(
        "resume_runtime_lost_after_launch",
        (
            "CCM restarted after a Capability resume crossed its provider "
            "boundary; automatic replay was blocked"
        ),
    )


class CapabilityResumeCoordinator:
    """Poll and publish durable Capability resume outboxes."""

    def __init__(
        self,
        *,
        db_factory: Callable[[], Any],
        publisher: ResumePublisher,
        poll_interval_seconds: float = 2.0,
        max_concurrency: int = 4,
        scan_limit: int = 64,
        initial_backoff_seconds: float = 1.0,
        max_backoff_seconds: float = 60.0,
    ) -> None:
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be at least one")
        if scan_limit < max_concurrency:
            raise ValueError("scan_limit must be at least max_concurrency")
        if initial_backoff_seconds <= 0:
            raise ValueError("initial_backoff_seconds must be positive")
        if max_backoff_seconds < initial_backoff_seconds:
            raise ValueError(
                "max_backoff_seconds must be at least initial_backoff_seconds"
            )
        self.db_factory = db_factory
        self.publisher = publisher
        self.poll_interval_seconds = float(poll_interval_seconds)
        self.max_concurrency = max_concurrency
        self.scan_limit = scan_limit
        self.initial_backoff_seconds = float(initial_backoff_seconds)
        self.max_backoff_seconds = float(max_backoff_seconds)

        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._scan_lock = asyncio.Lock()
        self._lifecycle_lock = asyncio.Lock()
        self._inflight_lock = asyncio.Lock()
        self._inflight: dict[int, asyncio.Task[None]] = {}
        self._failure_counts: dict[int, int] = {}
        self._wake_event = asyncio.Event()
        self._stop_event = asyncio.Event()
        self._runner: asyncio.Task[None] | None = None

    @property
    def is_running(self) -> bool:
        return self._runner is not None and not self._runner.done()

    def wake(self) -> None:
        self._wake_event.set()

    async def start(self) -> None:
        async with self._lifecycle_lock:
            if self.is_running:
                return
            self._stop_event.clear()
            self._wake_event.clear()
            await self.run_once(recovery=True, scan_limit=None)
            if self._stop_event.is_set():
                return
            self._runner = asyncio.create_task(
                self._run_loop(),
                name="capability-resume-coordinator",
            )
            logger.info("CapabilityResumeCoordinator started")

    async def shutdown(self) -> None:
        async def settle() -> None:
            async with self._lifecycle_lock:
                self._stop_event.set()
                self._wake_event.set()
                runner = self._runner
            try:
                if runner is not None:
                    await runner
                while True:
                    async with self._inflight_lock:
                        pending = tuple(
                            task
                            for task in self._inflight.values()
                            if not task.done()
                        )
                    if not pending:
                        break
                    await asyncio.gather(*pending)
            finally:
                async with self._lifecycle_lock:
                    if self._runner is runner:
                        self._runner = None
            logger.info("CapabilityResumeCoordinator stopped")

        await finish_awaitable(settle())

    async def _run_loop(self) -> None:
        try:
            while not self._stop_event.is_set():
                try:
                    await self.run_once()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Capability resume coordinator scan failed")
                self._wake_event.clear()
                if self._stop_event.is_set():
                    break
                try:
                    await asyncio.wait_for(
                        self._wake_event.wait(),
                        timeout=self.poll_interval_seconds,
                    )
                except asyncio.TimeoutError:
                    pass
        finally:
            self._wake_event.clear()

    async def _scan_ids(self, *, scan_limit: int | None) -> list[int]:
        now = datetime.utcnow()
        priority = case(
            (CapabilityResumeOutbox.status == "pending", 0),
            (CapabilityResumeOutbox.status == "claiming", 1),
            (CapabilityResumeOutbox.status == "ready", 2),
            else_=3,
        )
        statement = (
            select(CapabilityResumeOutbox.id)
            .join(Task, Task.id == CapabilityResumeOutbox.task_id)
            .where(
                Task.status == "waiting_capability",
                CapabilityResumeOutbox.status.in_(
                    ("pending", "ready", "claiming", "claimed")
                ),
                or_(
                    CapabilityResumeOutbox.next_attempt_at.is_(None),
                    CapabilityResumeOutbox.next_attempt_at <= now,
                ),
                or_(
                    CapabilityResumeOutbox.lease_token.is_(None),
                    CapabilityResumeOutbox.lease_expires_at.is_(None),
                    CapabilityResumeOutbox.lease_expires_at <= now,
                ),
            )
            .order_by(
                priority,
                CapabilityResumeOutbox.created_at,
                CapabilityResumeOutbox.id,
            )
        )
        if scan_limit is not None:
            statement = statement.limit(scan_limit)
        async with self.db_factory() as db:
            return list((await db.scalars(statement)).all())

    async def run_once(
        self,
        *,
        recovery: bool = False,
        scan_limit: int | None = None,
    ) -> None:
        if self._stop_event.is_set() and not recovery:
            return
        if scan_limit is None and not recovery:
            scan_limit = self.scan_limit
        async with self._scan_lock:
            outbox_ids = await self._scan_ids(scan_limit=scan_limit)
            callbacks: list[asyncio.Task[None]] = []
            async with self._inflight_lock:
                for outbox_id in outbox_ids:
                    current = self._inflight.get(outbox_id)
                    if current is not None and not current.done():
                        callbacks.append(current)
                        continue
                    callback = asyncio.create_task(
                        self._process_outbox(outbox_id),
                        name=f"capability-resume-{outbox_id}",
                    )
                    self._inflight[outbox_id] = callback
                    callback.add_done_callback(
                        lambda finished, outbox_id=outbox_id: asyncio.create_task(
                            self._discard_inflight(outbox_id, finished)
                        )
                    )
                    callbacks.append(callback)
            if callbacks:
                await asyncio.gather(*dict.fromkeys(callbacks))

    async def _discard_inflight(
        self,
        outbox_id: int,
        callback: asyncio.Task[None],
    ) -> None:
        async with self._inflight_lock:
            if self._inflight.get(outbox_id) is callback:
                self._inflight.pop(outbox_id, None)

    async def _process_outbox(self, outbox_id: int) -> None:
        async with self._semaphore:
            if self._stop_event.is_set():
                return
            try:
                async with self.db_factory() as db:
                    envelope = await materialize_resume_outbox(db, outbox_id)
                if envelope is None or envelope.status in {"claiming", "claimed"}:
                    async with self.db_factory() as db:
                        await recover_expired_resume_publication(db, outbox_id)
                    async with self.db_factory() as db:
                        envelope = await load_resume_envelope(db, outbox_id)
                if envelope is None or envelope.status not in {
                    "ready",
                    "claimed",
                }:
                    return
                await self.publisher(outbox_id)
            except asyncio.CancelledError:
                raise
            except (CapabilityNotFoundError, CapabilityResumeNotFoundError):
                self._failure_counts.pop(outbox_id, None)
            except Exception as exc:
                failures = self._failure_counts.get(outbox_id, 0) + 1
                self._failure_counts[outbox_id] = failures
                delay = min(
                    self.max_backoff_seconds,
                    self.initial_backoff_seconds * (2 ** (failures - 1)),
                )
                await self._defer_unleased(
                    outbox_id,
                    delay=delay,
                    error=exc,
                )
                logger.warning(
                    "Capability resume outbox %s failed; retrying in %.1fs",
                    outbox_id,
                    delay,
                    exc_info=exc,
                )
            else:
                self._failure_counts.pop(outbox_id, None)

    async def _defer_unleased(
        self,
        outbox_id: int,
        *,
        delay: float,
        error: Exception,
    ) -> None:
        route: tuple[int, int] | None
        async with self.db_factory() as db:
            route = await _outbox_route(db, outbox_id)
            if route is None:
                return
            await _end_routing_read(db)
            async with capability_task_lock(route[0]):
                try:
                    aggregate = await _lock_from_outbox(db, outbox_id)
                    outbox = aggregate.outbox
                    if (
                        outbox.status in {"ready", "claimed"}
                        and outbox.lease_token is None
                    ):
                        now = datetime.utcnow()
                        outbox.next_attempt_at = now + timedelta(seconds=delay)
                        outbox.error_code = "resume_coordinator_retry"
                        outbox.error_message = str(error)[:2000]
                        outbox.state_version += 1
                        outbox.updated_at = now
                        await db.commit()
                    else:
                        await db.rollback()
                except BaseException:
                    await db.rollback()
                    logger.exception(
                        "Could not persist Capability resume retry backoff for %s",
                        outbox_id,
                    )


__all__ = [
    "CapabilityResumeConflictError",
    "CapabilityResumeCoordinator",
    "CapabilityResumeError",
    "CapabilityResumeIntegrityError",
    "CapabilityResumeNotFoundError",
    "ResumeEnvelope",
    "ResumeTurnClaim",
    "cancel_task_resume_outbox_in_tx",
    "claim_resume_publication",
    "claim_resume_turn_locked",
    "fail_or_cancel_resume_outbox_in_tx",
    "load_resume_envelope",
    "mark_resume_launch_boundary",
    "mark_resume_launch_boundary_in_tx",
    "materialize_resume_outbox",
    "reconcile_stale_resume_in_tx",
    "recover_expired_resume_publication",
    "release_resume_publication",
    "settle_previous_resume_in_terminal_tx",
]
