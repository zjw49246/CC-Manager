"""Transactional core for provider-neutral task capabilities.

The service owns durable state and idempotency only.  It deliberately does not
start an executor while a database transaction is open.  Delivery controllers
claim queued executions through this in-process API and invoke the registered
adapter after the claim has committed.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import secrets
from typing import Any, Generic, Literal, TypeVar

from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.models.capability import (
    ACTIVE_EXECUTION_STATUSES,
    ACTIVE_INVOCATION_STATUSES,
    TERMINAL_INVOCATION_STATUSES,
    CapabilityExecution,
    CapabilityInvocation,
)
from backend.models.delivery import DeliveryRun
from backend.models.log_entry import LogEntry
from backend.models.task import Task
from backend.services.capability_events import broadcast_capability_event
from backend.services.capability_registry import CAPABILITY_KEY_RE, resolve_capability
from backend.services.cancellation import finish_awaitable
from backend.services.worker_task_termination import (
    no_active_worker_task_termination_predicate,
)


class CapabilityError(RuntimeError):
    """Base class for errors that API/controller callers may map explicitly."""


class CapabilityDisabledError(CapabilityError):
    pass


class CapabilityNotFoundError(CapabilityError):
    pass


class CapabilityConflictError(CapabilityError):
    pass


class CapabilityValidationError(CapabilityError):
    pass


class CapabilityUnsupportedScopeError(CapabilityError):
    pass


class CapabilityUnavailableError(CapabilityError):
    pass


_task_locks: dict[int, tuple[asyncio.AbstractEventLoop, asyncio.Lock]] = {}
MAX_CAPABILITY_REQUEST_BYTES = 32 * 1024

_StageValue = TypeVar("_StageValue")
_CompletionValue = TypeVar("_CompletionValue")


@dataclass(frozen=True, slots=True)
class StagedCapabilityHandle(Generic[_StageValue]):
    """Durable executor handle produced by a DB-only staging callback.

    The callback passed to :func:`stage_and_claim_execution` may insert and
    flush adapter-owned rows, but it must not commit, roll back, start a
    process, or acquire ``capability_task_lock`` recursively.  The Core owns
    the only commit that publishes both those rows and this handle.
    """

    handle_kind: str
    handle_id: str
    value: _StageValue
    handle_generation: int | None = None
    lease_token: str | None = None
    lease_expires_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class ValidatedCapabilityOutput(Generic[_CompletionValue]):
    """Exact output returned by a DB-only completion validator."""

    output_kind: str
    output_id: int
    output_hash: str
    value: _CompletionValue


StageCapabilityCallback = Callable[
    [AsyncSession, Task, CapabilityInvocation, CapabilityExecution],
    Awaitable[StagedCapabilityHandle[_StageValue]],
]
ValidateCapabilityOutputCallback = Callable[
    [AsyncSession, Task, CapabilityInvocation, CapabilityExecution],
    Awaitable[ValidatedCapabilityOutput[_CompletionValue]],
]
LockedTaskAuthorizationCallback = Callable[[AsyncSession, Task], Awaitable[None]]
LockedTaskEffectCallback = Callable[
    [AsyncSession, Task, bool],
    Awaitable[Task],
]


def capability_task_lock(task_id: int) -> asyncio.Lock:
    """Return the process-local half of the per-Task admission fence."""

    loop = asyncio.get_running_loop()
    entry = _task_locks.get(task_id)
    if entry is None or entry[0] is not loop:
        if entry is not None and entry[1].locked():
            raise RuntimeError(
                "Capability Task lock is active on a different event loop"
            )
        entry = (loop, asyncio.Lock())
        _task_locks[task_id] = entry
    return entry[1]


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise CapabilityValidationError(
            "Capability payload must be finite JSON data"
        ) from exc


def capability_value_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _validate_request(request_payload: dict) -> tuple[dict, str]:
    if not isinstance(request_payload, dict):
        raise CapabilityValidationError("Capability request must be a JSON object")
    frozen = deepcopy(request_payload)
    canonical = _canonical_json(frozen).encode("utf-8")
    if len(canonical) > MAX_CAPABILITY_REQUEST_BYTES:
        raise CapabilityValidationError(
            f"Capability request exceeds {MAX_CAPABILITY_REQUEST_BYTES} UTF-8 bytes"
        )
    return frozen, hashlib.sha256(canonical).hexdigest()


def _validate_hash(value: str, *, field: str) -> str:
    normalized = value.strip().lower()
    if len(normalized) != 64 or any(ch not in "0123456789abcdef" for ch in normalized):
        raise CapabilityValidationError(f"{field} must be a SHA-256 hex digest")
    return normalized


def _task_subject(task: Task) -> tuple[dict, str]:
    subject = {
        "task_id": task.id,
        "incarnation_id": task.incarnation_id,
        "retry_count": task.retry_count,
        "turn_generation": task.turn_generation,
        "instance_id": task.instance_id,
        "started_at": task.started_at.isoformat() if task.started_at else None,
        "session_id": task.session_id,
        "title": task.title,
        "description": task.description,
        "project_id": task.project_id,
        "target_repo": task.target_repo,
        "last_cwd": task.last_cwd,
        "target_branch": task.target_branch,
        "mode": task.mode,
        "provider": task.provider,
        "model": task.model,
        "codex_service_tier": task.codex_service_tier,
        "effort_level": task.effort_level,
        "priority": task.priority,
        "timeout_hours": task.timeout_hours,
    }
    return subject, capability_value_hash(subject)


def _ensure_local_task(task: Task) -> None:
    if task.worker_id is not None:
        raise CapabilityUnsupportedScopeError(
            "Capabilities cannot be created on a remote Worker task"
        )
    if task.shared_from_id is not None:
        raise CapabilityUnsupportedScopeError(
            "Capabilities cannot be created on a shared shadow task"
        )
    if task.status == "migrating":
        raise CapabilityUnsupportedScopeError(
            "Capabilities cannot be created while a task is migrating"
        )


def _ensure_public_human_task(task: Task) -> None:
    """Reject a human lifecycle mutation that crossed Delivery admission."""

    if task.mode == "delivery_loop" or task.delivery_run_id is not None:
        raise CapabilityConflictError(
            "Delivery-owned Tasks cannot use the public advisory Capability lifecycle"
        )


async def _ensure_admitted_delivery_controller_task(
    db: AsyncSession,
    task: Task,
) -> None:
    """Allow a disabled Core to finish only an already-admitted Delivery Run.

    The rollout switches gate admission of new work.  A Delivery Run and its
    Developer Task are admitted durably in one transaction, so disabling the
    switches later must not strand that Run between cycles.  The reverse
    ownership check keeps this controller-only exception from becoming a
    general internal bypass.  Deliberately do not lock the Run here: Capability
    Core owns Task-first lock ordering, while the Delivery Controller owns
    Run-first ordering.
    """

    if (
        task.mode != "delivery_loop"
        or task.delivery_run_id is None
        or task.delivery_role != "developer"
    ):
        raise CapabilityDisabledError("Capability Core is disabled")

    admitted_run_id = await db.scalar(
        select(DeliveryRun.id)
        .where(
            DeliveryRun.id == task.delivery_run_id,
            DeliveryRun.developer_task_id == task.id,
            DeliveryRun.activity != "terminal",
        )
        .limit(1)
    )
    if admitted_run_id is None:
        raise CapabilityDisabledError("Capability Core is disabled")


def _same_logical_request(
    invocation: CapabilityInvocation,
    *,
    capability_key: str,
    source: str,
    purpose: str,
    resume_policy: str,
    input_hash: str,
) -> bool:
    return (
        invocation.capability_key == capability_key
        and invocation.source == source
        and invocation.purpose == purpose
        and invocation.resume_policy == resume_policy
        and invocation.input_hash == input_hash
    )


async def _find_idempotent(
    db: AsyncSession,
    *,
    task_id: int,
    idempotency_key: str,
) -> CapabilityInvocation | None:
    return (
        await db.execute(
            select(CapabilityInvocation).where(
                CapabilityInvocation.task_id == task_id,
                CapabilityInvocation.idempotency_key == idempotency_key,
            )
        )
    ).scalar_one_or_none()


async def _lock_task(
    db: AsyncSession,
    task_id: int,
    *,
    require_termination_clear: bool = False,
) -> Task:
    """Acquire the first lock in Task -> Invocation -> Execution order."""

    predicates = [Task.id == task_id]
    if require_termination_clear:
        predicates.append(no_active_worker_task_termination_predicate())
    guarded = await db.execute(
        update(Task).where(*predicates).values(status=Task.status)
    )
    if not guarded.rowcount:
        if require_termination_clear and await db.scalar(
            select(Task.id).where(Task.id == task_id)
        ) is not None:
            raise CapabilityConflictError(
                "Task has an active Worker termination receipt"
            )
        raise CapabilityNotFoundError("Task not found")
    task = (
        await db.execute(
            select(Task)
            .where(Task.id == task_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if task is None:
        raise CapabilityNotFoundError("Task not found")
    return task


async def _create_invocation(
    db: AsyncSession,
    *,
    task_id: int,
    capability_key: str,
    request_payload: dict,
    idempotency_key: str,
    source: Literal["human_request", "delivery_controller"],
    purpose: Literal["advisory", "required_gate"],
    resume_policy: Literal["attach_only", "controller"],
    requested_by_user_id: int | None,
    request_source_log_id: int | None = None,
    authorize_locked_task: LockedTaskAuthorizationCallback | None = None,
    lock_effect_task: LockedTaskEffectCallback | None = None,
) -> tuple[CapabilityInvocation, bool]:
    capability_key = capability_key.strip()
    idempotency_key = idempotency_key.strip()
    if not CAPABILITY_KEY_RE.fullmatch(capability_key):
        raise CapabilityValidationError("Invalid capability key")
    if not idempotency_key or len(idempotency_key) > 128:
        raise CapabilityValidationError("Invalid idempotency key")
    payload, input_hash = _validate_request(request_payload)

    # Replay is deliberately checked before the rollout switch. Disabling new
    # work must not turn a lost HTTP response into a second logical request.
    existing = await _find_idempotent(
        db,
        task_id=task_id,
        idempotency_key=idempotency_key,
    )
    if existing is not None:
        if not _same_logical_request(
            existing,
            capability_key=capability_key,
            source=source,
            purpose=purpose,
            resume_policy=resume_policy,
            input_hash=input_hash,
        ):
            raise CapabilityConflictError(
                "Idempotency key was already used for a different request"
            )
        if authorize_locked_task is None and lock_effect_task is None:
            return existing, False

    if (
        existing is None
        and not settings.capability_core_enabled
        and source != "delivery_controller"
    ):
        raise CapabilityDisabledError("Capability Core is disabled")

    definition = resolve_capability(capability_key) if existing is None else None
    if existing is None and definition is None:
        raise CapabilityUnavailableError(
            f"Capability {capability_key!r} is not registered"
        )

    observed_task = await db.get(Task, task_id)
    if observed_task is None:
        raise CapabilityNotFoundError("Task not found")

    # The idempotency probe above opened a read transaction.  End it before
    # the Task write fence so a receipt committed by another SQLite WAL
    # connection wins cleanly instead of producing BUSY_SNAPSHOT.
    await _end_routing_read(db)
    worker_node_fence_held = existing is None
    async with capability_task_lock(task_id):
        try:
            if worker_node_fence_held:
                from backend.services.worker_node_control import (
                    fence_worker_node_mutation,
                )

                await fence_worker_node_mutation(db)
            if lock_effect_task is None:
                task = await _lock_task(
                    db,
                    task_id,
                    require_termination_clear=True,
                )
            else:
                task = await lock_effect_task(
                    db,
                    observed_task,
                    worker_node_fence_held,
                )
                if task.id != task_id:
                    raise CapabilityConflictError(
                        "Capability effect fence returned a different Task"
                    )
                termination_clear = await db.execute(
                    update(Task)
                    .where(
                        Task.id == task.id,
                        no_active_worker_task_termination_predicate(),
                    )
                    .values(status=Task.status)
                )
                if termination_clear.rowcount != 1:
                    raise CapabilityConflictError(
                        "Task has an active Worker termination receipt"
                    )
            _ensure_local_task(task)
            if authorize_locked_task is not None:
                await authorize_locked_task(db, task)
            if source == "human_request":
                _ensure_public_human_task(task)

            existing = await _find_idempotent(
                db,
                task_id=task_id,
                idempotency_key=idempotency_key,
            )
            if existing is not None:
                if not _same_logical_request(
                    existing,
                    capability_key=capability_key,
                    source=source,
                    purpose=purpose,
                    resume_policy=resume_policy,
                    input_hash=input_hash,
                ):
                    raise CapabilityConflictError(
                        "Idempotency key was already used for a different request"
                    )
                await db.commit()
                return existing, False

            if not settings.capability_core_enabled and source != "delivery_controller":
                raise CapabilityDisabledError("Capability Core is disabled")
            if definition is None:
                definition = resolve_capability(capability_key)
            if definition is None:
                raise CapabilityUnavailableError(
                    f"Capability {capability_key!r} is not registered"
                )

            if not settings.capability_core_enabled:
                if source != "delivery_controller":
                    raise CapabilityDisabledError("Capability Core is disabled")
                await _ensure_admitted_delivery_controller_task(db, task)

            active_id = await db.scalar(
                select(CapabilityInvocation.id)
                .where(CapabilityInvocation.active_task_id == task_id)
                .limit(1)
            )
            if active_id is not None:
                raise CapabilityConflictError(
                    f"Task already has active capability invocation {active_id}"
                )

            subject_ref, subject_hash = _task_subject(task)
            if request_source_log_id is None:
                request_source_log_id = await db.scalar(
                    select(func.max(LogEntry.id)).where(
                        LogEntry.task_id == task.id,
                        LogEntry.role.in_(("user", "assistant")),
                        LogEntry.event_type.in_(("message", "user_message")),
                    )
                )
            executor_config = deepcopy(definition.executor_config)
            policy_snapshot = deepcopy(definition.policy_snapshot)
            invocation = CapabilityInvocation(
                task_id=task.id,
                capability_key=definition.capability_key,
                source=source,
                purpose=purpose,
                status="queued",
                state_version=1,
                idempotency_key=idempotency_key,
                input_payload=payload,
                input_hash=input_hash,
                subject_kind="task_generation",
                subject_ref=subject_ref,
                subject_hash=subject_hash,
                executor_kind=definition.executor_kind,
                executor_config=executor_config,
                executor_config_hash=capability_value_hash(executor_config),
                policy_snapshot=policy_snapshot,
                policy_hash=capability_value_hash(policy_snapshot),
                resume_policy=resume_policy,
                max_attempts=definition.max_attempts,
                active_task_id=task.id,
                requested_by_user_id=requested_by_user_id,
                request_task_retry_count=task.retry_count,
                request_task_instance_id=task.instance_id,
                request_task_started_at=task.started_at,
                request_task_session_id=task.session_id,
                request_source_log_id=request_source_log_id,
            )
            db.add(invocation)
            await db.flush()
            db.add(
                CapabilityExecution(
                    invocation_id=invocation.id,
                    attempt=1,
                    status="queued",
                    state_version=1,
                    active_invocation_id=invocation.id,
                    idempotency_key=f"{invocation.id}:1",
                    executor_kind=invocation.executor_kind,
                    input_hash=invocation.input_hash,
                )
            )
            await db.commit()
        except CapabilityError:
            await db.rollback()
            raise
        except IntegrityError as exc:
            await db.rollback()
            concurrent = await _find_idempotent(
                db,
                task_id=task_id,
                idempotency_key=idempotency_key,
            )
            if concurrent is not None and _same_logical_request(
                concurrent,
                capability_key=capability_key,
                source=source,
                purpose=purpose,
                resume_policy=resume_policy,
                input_hash=input_hash,
            ):
                return concurrent, False
            raise CapabilityConflictError(
                "A concurrent capability request won admission"
            ) from exc
        except BaseException:
            await _rollback_safely(db)
            raise

    await broadcast_capability_event(
        "capability_invocation_created",
        invocation,
        created=True,
    )
    return invocation, True


async def create_human_invocation(
    db: AsyncSession,
    *,
    task_id: int,
    capability_key: str,
    request_payload: dict,
    idempotency_key: str,
    requested_by_user_id: int | None,
    authorize_locked_task: LockedTaskAuthorizationCallback | None = None,
    lock_effect_task: LockedTaskEffectCallback | None = None,
) -> tuple[CapabilityInvocation, bool]:
    """Create the only public contract: advisory + attach-only."""

    return await _create_invocation(
        db,
        task_id=task_id,
        capability_key=capability_key,
        request_payload=request_payload,
        idempotency_key=idempotency_key,
        source="human_request",
        purpose="advisory",
        resume_policy="attach_only",
        requested_by_user_id=requested_by_user_id,
        authorize_locked_task=authorize_locked_task,
        lock_effect_task=lock_effect_task,
    )


async def create_controller_invocation(
    db: AsyncSession,
    *,
    task_id: int,
    capability_key: str,
    request_payload: dict,
    idempotency_key: str,
    purpose: Literal["advisory", "required_gate"] = "required_gate",
    request_source_log_id: int | None = None,
) -> tuple[CapabilityInvocation, bool]:
    """In-process entry point reserved for a delivery-loop controller."""

    return await _create_invocation(
        db,
        task_id=task_id,
        capability_key=capability_key,
        request_payload=request_payload,
        idempotency_key=idempotency_key,
        source="delivery_controller",
        purpose=purpose,
        resume_policy="controller",
        requested_by_user_id=None,
        request_source_log_id=request_source_log_id,
    )


async def create_agent_invocation(
    db: AsyncSession,
    *,
    expected=None,
    **legacy_unfenced_fields,
):
    """Admit only a controller-proven exact terminal Task generation.

    The loose historical stub accepted arbitrary keyword shapes only to reject
    them. Keep that rejection explicit for stale callers; the enabled entry
    point requires the typed expectation produced at a provider terminal
    boundary and atomically creates its resume outbox.
    """

    if expected is None or legacy_unfenced_fields:
        raise CapabilityUnsupportedScopeError(
            "agent_request requires an exact terminal Task generation"
        )
    from backend.services.agent_capability_admission import (
        AgentTerminalExpectation,
        admit_agent_terminal_action,
    )

    if not isinstance(expected, AgentTerminalExpectation):
        raise CapabilityValidationError(
            "agent_request expectation has an invalid type"
        )
    return await admit_agent_terminal_action(db, expected=expected)


async def get_invocation(
    db: AsyncSession,
    invocation_id: int,
) -> CapabilityInvocation:
    invocation = await db.get(CapabilityInvocation, invocation_id)
    if invocation is None:
        raise CapabilityNotFoundError("Capability invocation not found")
    return invocation


async def list_task_invocations(
    db: AsyncSession,
    task_id: int,
) -> list[CapabilityInvocation]:
    return list(
        (
            await db.execute(
                select(CapabilityInvocation)
                .where(CapabilityInvocation.task_id == task_id)
                .order_by(
                    CapabilityInvocation.created_at.desc(),
                    CapabilityInvocation.id.desc(),
                )
            )
        ).scalars()
    )


async def active_execution_for(
    db: AsyncSession,
    invocation_id: int,
) -> CapabilityExecution | None:
    return (
        await db.execute(
            select(CapabilityExecution)
            .where(
                CapabilityExecution.invocation_id == invocation_id,
                CapabilityExecution.active_invocation_id == invocation_id,
            )
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()


async def _invocation_task_id(
    db: AsyncSession,
    invocation_id: int,
) -> int:
    task_id = await db.scalar(
        select(CapabilityInvocation.task_id).where(
            CapabilityInvocation.id == invocation_id
        )
    )
    if task_id is None:
        raise CapabilityNotFoundError("Capability invocation not found")
    return task_id


async def _lock_aggregate(
    db: AsyncSession,
    invocation_id: int,
    *,
    require_termination_clear: bool = False,
) -> tuple[Task, CapabilityInvocation, list[CapabilityExecution]]:
    task_id = await _invocation_task_id(db, invocation_id)
    task = await _lock_task(
        db,
        task_id,
        require_termination_clear=require_termination_clear,
    )
    return await _lock_aggregate_after_task(db, invocation_id, task)


async def _lock_aggregate_after_task(
    db: AsyncSession,
    invocation_id: int,
    task: Task,
) -> tuple[Task, CapabilityInvocation, list[CapabilityExecution]]:
    invocation = (
        await db.execute(
            select(CapabilityInvocation)
            .where(CapabilityInvocation.id == invocation_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if invocation is None or invocation.task_id != task.id:
        raise CapabilityNotFoundError("Capability invocation not found")
    executions = list(
        (
            await db.execute(
                select(CapabilityExecution)
                .where(CapabilityExecution.invocation_id == invocation.id)
                .order_by(CapabilityExecution.attempt)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalars()
    )
    return task, invocation, executions


def _expect_version(actual: int, expected: int, *, resource: str) -> None:
    if actual != expected:
        raise CapabilityConflictError(
            f"Stale {resource} state version: expected {expected}, current {actual}"
        )


def _active_execution(
    invocation: CapabilityInvocation,
    executions: list[CapabilityExecution],
) -> CapabilityExecution:
    active = [
        execution
        for execution in executions
        if execution.active_invocation_id == invocation.id
    ]
    if len(active) != 1:
        raise CapabilityConflictError(
            "Capability invocation does not have exactly one active execution"
        )
    return active[0]


def _exact_completed_output_execution(
    invocation: CapabilityInvocation,
    executions: list[CapabilityExecution],
) -> CapabilityExecution:
    completed = [
        execution
        for execution in executions
        if execution.status == "completed"
        and execution.output_kind == invocation.result_kind
        and execution.output_id == invocation.result_id
        and execution.output_hash == invocation.result_hash
    ]
    if len(completed) != 1:
        raise CapabilityConflictError(
            "Ready capability has no exact completed output execution"
        )
    return completed[0]


async def _commit_transition(
    db: AsyncSession,
    invocation: CapabilityInvocation,
    *,
    event_type: str,
) -> None:
    invocation.updated_at = datetime.utcnow()
    await db.commit()
    await broadcast_capability_event(event_type, invocation)


async def _rollback_safely(db: AsyncSession) -> None:
    """Finish rollback even when the caller was cancelled mid-transition."""

    # A second cancellation must not leave a transaction containing
    # adapter-owned staged rows open for accidental reuse.
    await finish_awaitable(db.rollback())


async def _end_routing_read(db: AsyncSession) -> None:
    """End the pre-fence lookup without discarding or publishing mutations."""

    if db.new or db.dirty or db.deleted:
        raise CapabilityConflictError(
            "Capability atomic operations require a clean database session"
        )
    await db.commit()


def _validate_handle(
    *,
    handle_kind: str,
    handle_id: str,
) -> tuple[str, str]:
    kind = handle_kind.strip()
    identifier = handle_id.strip()
    if not kind or not identifier:
        raise CapabilityValidationError("Executor handle kind and id are required")
    return kind, identifier


def _claim_locked_execution(
    invocation: CapabilityInvocation,
    execution: CapabilityExecution,
    *,
    handle_kind: str,
    handle_id: str,
    handle_generation: int | None,
    lease_token: str | None,
    lease_expires_at: datetime | None,
) -> None:
    if invocation.status != "queued" or execution.status != "queued":
        raise CapabilityConflictError("Capability execution is not claimable")
    if execution.handle_kind is not None or execution.handle_id is not None:
        raise CapabilityConflictError(
            "Queued capability execution already has a durable handle"
        )
    now = datetime.utcnow()
    invocation.status = "running"
    invocation.state_version += 1
    execution.status = "running"
    execution.state_version += 1
    execution.handle_kind = handle_kind
    execution.handle_id = handle_id
    execution.handle_generation = handle_generation
    execution.lease_token = lease_token or secrets.token_hex(32)
    execution.lease_expires_at = lease_expires_at
    execution.heartbeat_at = now
    execution.started_at = now


def _validated_output_fields(
    *,
    output_kind: str,
    output_id: int,
    output_hash: str,
) -> tuple[str, int, str]:
    kind = output_kind.strip()
    if not kind:
        raise CapabilityValidationError("Output kind is required")
    if isinstance(output_id, bool) or not isinstance(output_id, int) or output_id <= 0:
        raise CapabilityValidationError("output_id must be a positive integer")
    return kind, output_id, _validate_hash(output_hash, field="output_hash")


def _complete_locked_execution(
    invocation: CapabilityInvocation,
    execution: CapabilityExecution,
    *,
    output_kind: str,
    output_id: int,
    output_hash: str,
) -> None:
    if (
        invocation.status not in {"running", "waiting_user"}
        or execution.status not in {"running", "waiting_user"}
    ):
        raise CapabilityConflictError(
            "Capability execution cannot complete from its current state"
        )
    now = datetime.utcnow()
    execution.status = "completed"
    execution.state_version += 1
    execution.active_invocation_id = None
    execution.output_kind = output_kind
    execution.output_id = output_id
    execution.output_hash = output_hash
    execution.completed_at = now
    invocation.status = "ready"
    invocation.state_version += 1
    invocation.result_kind = output_kind
    invocation.result_id = output_id
    invocation.result_hash = output_hash
    invocation.ready_at = now


async def stage_and_claim_execution(
    db: AsyncSession,
    *,
    invocation_id: int,
    expected_invocation_version: int,
    expected_execution_version: int,
    stage: StageCapabilityCallback[_StageValue],
) -> tuple[CapabilityInvocation, CapabilityExecution, _StageValue]:
    """Atomically stage adapter rows and claim their exact durable handle.

    Lock order is always process-local Task fence, then fresh
    Task -> Invocation -> all Executions database rows.  ``stage`` runs only
    after those locks are held and shares the single Core-owned commit.  This
    closes the crash window where an adapter row could become durable without
    its CapabilityExecution handle (or vice versa).
    """

    task_id = await _invocation_task_id(db, invocation_id)
    # Resolve routing before the process-local fence, then end that read-only
    # transaction so the critical section always starts process lock first.
    await _end_routing_read(db)
    invocation: CapabilityInvocation | None = None
    async with capability_task_lock(task_id):
        try:
            from backend.services.worker_node_control import (
                fence_worker_node_mutation,
            )

            await fence_worker_node_mutation(db)
            task, invocation, executions = await _lock_aggregate(
                db,
                invocation_id,
                require_termination_clear=True,
            )
            execution = _active_execution(invocation, executions)
            _expect_version(
                invocation.state_version,
                expected_invocation_version,
                resource="invocation",
            )
            _expect_version(
                execution.state_version,
                expected_execution_version,
                resource="execution",
            )
            if invocation.status != "queued" or execution.status != "queued":
                raise CapabilityConflictError(
                    "Capability execution is not claimable"
                )
            if execution.handle_kind is not None or execution.handle_id is not None:
                raise CapabilityConflictError(
                    "Queued capability execution already has a durable handle"
                )

            transaction = db.get_transaction()
            if transaction is None:
                raise CapabilityConflictError(
                    "Capability staging transaction is unavailable"
                )
            staged = await stage(db, task, invocation, execution)
            if not isinstance(staged, StagedCapabilityHandle):
                raise CapabilityValidationError(
                    "Capability staging callback returned an invalid handle"
                )
            if db.get_transaction() is not transaction:
                raise CapabilityConflictError(
                    "Capability staging callback ended the owned transaction"
                )
            kind, identifier = _validate_handle(
                handle_kind=staged.handle_kind,
                handle_id=staged.handle_id,
            )
            await db.flush()
            _claim_locked_execution(
                invocation,
                execution,
                handle_kind=kind,
                handle_id=identifier,
                handle_generation=staged.handle_generation,
                lease_token=staged.lease_token,
                lease_expires_at=staged.lease_expires_at,
            )
            invocation.updated_at = datetime.utcnow()
            await db.commit()
            value = staged.value
        except BaseException:
            await _rollback_safely(db)
            raise

    assert invocation is not None
    await broadcast_capability_event("capability_invocation_running", invocation)
    return invocation, execution, value


async def validate_and_complete_execution(
    db: AsyncSession,
    *,
    invocation_id: int,
    expected_invocation_version: int,
    expected_execution_version: int,
    validate: ValidateCapabilityOutputCallback[_CompletionValue],
) -> tuple[CapabilityInvocation, CapabilityExecution, _CompletionValue]:
    """Validate adapter output under the aggregate locks and publish it once.

    The validator may lock and inspect adapter-owned rows but must remain
    DB-only and must not end the transaction.  Its identity checks and the
    Capability transition therefore share one commit boundary.
    """

    task_id = await _invocation_task_id(db, invocation_id)
    await _end_routing_read(db)
    invocation: CapabilityInvocation | None = None
    async with capability_task_lock(task_id):
        try:
            task, invocation, executions = await _lock_aggregate(db, invocation_id)
            execution = _active_execution(invocation, executions)
            _expect_version(
                invocation.state_version,
                expected_invocation_version,
                resource="invocation",
            )
            _expect_version(
                execution.state_version,
                expected_execution_version,
                resource="execution",
            )
            transaction = db.get_transaction()
            if transaction is None:
                raise CapabilityConflictError(
                    "Capability completion transaction is unavailable"
                )
            validated = await validate(db, task, invocation, execution)
            if not isinstance(validated, ValidatedCapabilityOutput):
                raise CapabilityValidationError(
                    "Capability completion validator returned invalid output"
                )
            if db.get_transaction() is not transaction:
                raise CapabilityConflictError(
                    "Capability completion validator ended the owned transaction"
                )
            kind, output_id, output_hash = _validated_output_fields(
                output_kind=validated.output_kind,
                output_id=validated.output_id,
                output_hash=validated.output_hash,
            )
            _complete_locked_execution(
                invocation,
                execution,
                output_kind=kind,
                output_id=output_id,
                output_hash=output_hash,
            )
            invocation.updated_at = datetime.utcnow()
            await db.commit()
            value = validated.value
        except BaseException:
            await _rollback_safely(db)
            raise

    assert invocation is not None
    await broadcast_capability_event("capability_invocation_ready", invocation)
    return invocation, execution, value


async def claim_execution(
    db: AsyncSession,
    *,
    invocation_id: int,
    expected_invocation_version: int,
    expected_execution_version: int,
    handle_kind: str,
    handle_id: str,
    handle_generation: int | None = None,
    lease_token: str | None = None,
    lease_expires_at: datetime | None = None,
) -> tuple[CapabilityInvocation, CapabilityExecution]:
    if not handle_kind.strip() or not handle_id.strip():
        raise CapabilityValidationError("Executor handle kind and id are required")
    task_id = await _invocation_task_id(db, invocation_id)
    await _end_routing_read(db)
    async with capability_task_lock(task_id):
        try:
            from backend.services.worker_node_control import (
                fence_worker_node_mutation,
            )

            await fence_worker_node_mutation(db)
            _, invocation, executions = await _lock_aggregate(
                db,
                invocation_id,
                require_termination_clear=True,
            )
            execution = _active_execution(invocation, executions)
            _expect_version(
                invocation.state_version,
                expected_invocation_version,
                resource="invocation",
            )
            _expect_version(
                execution.state_version,
                expected_execution_version,
                resource="execution",
            )
            if invocation.status != "queued" or execution.status != "queued":
                raise CapabilityConflictError("Capability execution is not claimable")
            now = datetime.utcnow()
            invocation.status = "running"
            invocation.state_version += 1
            execution.status = "running"
            execution.state_version += 1
            execution.handle_kind = handle_kind.strip()
            execution.handle_id = handle_id.strip()
            execution.handle_generation = handle_generation
            execution.lease_token = lease_token or secrets.token_hex(32)
            execution.lease_expires_at = lease_expires_at
            execution.heartbeat_at = now
            execution.started_at = now
            await _commit_transition(
                db,
                invocation,
                event_type="capability_invocation_running",
            )
            return invocation, execution
        except CapabilityError:
            await db.rollback()
            raise
        except BaseException:
            await _rollback_safely(db)
            raise


async def mark_execution_waiting(
    db: AsyncSession,
    *,
    invocation_id: int,
    expected_invocation_version: int,
    expected_execution_version: int,
) -> tuple[CapabilityInvocation, CapabilityExecution]:
    task_id = await _invocation_task_id(db, invocation_id)
    async with capability_task_lock(task_id):
        try:
            _, invocation, executions = await _lock_aggregate(db, invocation_id)
            execution = _active_execution(invocation, executions)
            _expect_version(invocation.state_version, expected_invocation_version, resource="invocation")
            _expect_version(execution.state_version, expected_execution_version, resource="execution")
            if invocation.status != "running" or execution.status != "running":
                raise CapabilityConflictError("Capability execution is not running")
            invocation.status = "waiting_user"
            invocation.state_version += 1
            execution.status = "waiting_user"
            execution.state_version += 1
            execution.heartbeat_at = datetime.utcnow()
            await _commit_transition(
                db,
                invocation,
                event_type="capability_invocation_waiting_user",
            )
            return invocation, execution
        except CapabilityError:
            await db.rollback()
            raise


async def resume_waiting_execution(
    db: AsyncSession,
    *,
    invocation_id: int,
    expected_invocation_version: int,
    expected_execution_version: int,
) -> tuple[CapabilityInvocation, CapabilityExecution]:
    """CAS a user-answered execution back to running."""

    task_id = await _invocation_task_id(db, invocation_id)
    await _end_routing_read(db)
    async with capability_task_lock(task_id):
        try:
            from backend.services.worker_node_control import (
                fence_worker_node_mutation,
            )

            await fence_worker_node_mutation(db)
            _, invocation, executions = await _lock_aggregate(
                db,
                invocation_id,
                require_termination_clear=True,
            )
            execution = _active_execution(invocation, executions)
            _expect_version(
                invocation.state_version,
                expected_invocation_version,
                resource="invocation",
            )
            _expect_version(
                execution.state_version,
                expected_execution_version,
                resource="execution",
            )
            if (
                invocation.status != "waiting_user"
                or execution.status != "waiting_user"
            ):
                raise CapabilityConflictError(
                    "Capability execution is not waiting for user input"
                )
            invocation.status = "running"
            invocation.state_version += 1
            execution.status = "running"
            execution.state_version += 1
            execution.heartbeat_at = datetime.utcnow()
            await _commit_transition(
                db,
                invocation,
                event_type="capability_invocation_running",
            )
            return invocation, execution
        except CapabilityError:
            await db.rollback()
            raise
        except BaseException:
            await _rollback_safely(db)
            raise


# Descriptive alias for adapters that report observed state rather than an
# input-answer action.
mark_execution_running = resume_waiting_execution


async def complete_execution(
    db: AsyncSession,
    *,
    invocation_id: int,
    expected_invocation_version: int,
    expected_execution_version: int,
    output_kind: str,
    output_id: int,
    output_hash: str,
) -> tuple[CapabilityInvocation, CapabilityExecution]:
    if not output_kind.strip():
        raise CapabilityValidationError("Output kind is required")
    if isinstance(output_id, bool) or not isinstance(output_id, int) or output_id <= 0:
        raise CapabilityValidationError("output_id must be a positive integer")
    output_hash = _validate_hash(output_hash, field="output_hash")
    task_id = await _invocation_task_id(db, invocation_id)
    async with capability_task_lock(task_id):
        try:
            _, invocation, executions = await _lock_aggregate(db, invocation_id)
            execution = _active_execution(invocation, executions)
            _expect_version(invocation.state_version, expected_invocation_version, resource="invocation")
            _expect_version(execution.state_version, expected_execution_version, resource="execution")
            if invocation.status not in {"running", "waiting_user"} or execution.status not in {"running", "waiting_user"}:
                raise CapabilityConflictError("Capability execution cannot complete from its current state")
            now = datetime.utcnow()
            execution.status = "completed"
            execution.state_version += 1
            execution.active_invocation_id = None
            execution.output_kind = output_kind.strip()
            execution.output_id = output_id
            execution.output_hash = output_hash
            execution.completed_at = now
            invocation.status = "ready"
            invocation.state_version += 1
            invocation.result_kind = execution.output_kind
            invocation.result_id = execution.output_id
            invocation.result_hash = execution.output_hash
            invocation.ready_at = now
            # ready intentionally keeps active_task_id. Only a successful
            # consumer acknowledgement releases admission for the next call.
            await _commit_transition(
                db,
                invocation,
                event_type="capability_invocation_ready",
            )
            return invocation, execution
        except CapabilityError:
            await db.rollback()
            raise


async def fail_execution(
    db: AsyncSession,
    *,
    invocation_id: int,
    expected_invocation_version: int,
    expected_execution_version: int,
    error_code: str,
    error_message: str,
    retry: bool = True,
) -> tuple[CapabilityInvocation, CapabilityExecution, CapabilityExecution | None]:
    task_id = await _invocation_task_id(db, invocation_id)
    async with capability_task_lock(task_id):
        try:
            _, invocation, executions = await _lock_aggregate(db, invocation_id)
            execution = _active_execution(invocation, executions)
            _expect_version(invocation.state_version, expected_invocation_version, resource="invocation")
            _expect_version(execution.state_version, expected_execution_version, resource="execution")
            if execution.status not in ACTIVE_EXECUTION_STATUSES:
                raise CapabilityConflictError("Capability execution is already terminal")
            now = datetime.utcnow()
            execution.status = "failed"
            execution.state_version += 1
            execution.active_invocation_id = None
            execution.error_code = error_code[:64] or "executor_failed"
            execution.error_message = error_message
            execution.completed_at = now
            await db.flush()

            replacement = None
            if retry and execution.attempt < invocation.max_attempts:
                next_attempt = execution.attempt + 1
                replacement = CapabilityExecution(
                    invocation_id=invocation.id,
                    attempt=next_attempt,
                    status="queued",
                    state_version=1,
                    active_invocation_id=invocation.id,
                    idempotency_key=f"{invocation.id}:{next_attempt}",
                    executor_kind=invocation.executor_kind,
                    input_hash=invocation.input_hash,
                )
                db.add(replacement)
                invocation.status = "queued"
                invocation.error_code = None
                invocation.error_message = None
            else:
                invocation.status = "failed"
                invocation.active_task_id = None
                invocation.error_code = execution.error_code
                invocation.error_message = error_message
                invocation.completed_at = now
            invocation.state_version += 1
            await _commit_transition(
                db,
                invocation,
                event_type=(
                    "capability_invocation_retrying"
                    if replacement is not None
                    else "capability_invocation_failed"
                ),
            )
            return invocation, execution, replacement
        except CapabilityError:
            await db.rollback()
            raise
        except IntegrityError as exc:
            await db.rollback()
            raise CapabilityConflictError(
                "Concurrent capability execution retry won"
            ) from exc


async def mark_execution_stale(
    db: AsyncSession,
    *,
    invocation_id: int,
    expected_invocation_version: int,
    expected_execution_version: int,
    error_code: str,
    error_message: str,
) -> tuple[CapabilityInvocation, CapabilityExecution]:
    """Terminalize an attempt whose immutable subject no longer matches.

    ``stale`` is intentionally non-retryable at the Capability Core layer. A
    controller must capture a new subject and create a new logical invocation;
    retrying the old input would blur two code snapshots into one audit row.
    """

    task_id = await _invocation_task_id(db, invocation_id)
    async with capability_task_lock(task_id):
        try:
            _, invocation, executions = await _lock_aggregate(db, invocation_id)
            execution = _active_execution(invocation, executions)
            _expect_version(
                invocation.state_version,
                expected_invocation_version,
                resource="invocation",
            )
            _expect_version(
                execution.state_version,
                expected_execution_version,
                resource="execution",
            )
            if execution.status not in ACTIVE_EXECUTION_STATUSES:
                raise CapabilityConflictError(
                    "Capability execution is already terminal"
                )
            now = datetime.utcnow()
            code = error_code[:64] or "subject_stale"
            execution.status = "stale"
            execution.state_version += 1
            execution.active_invocation_id = None
            execution.error_code = code
            execution.error_message = error_message
            execution.completed_at = now
            invocation.status = "stale"
            invocation.state_version += 1
            invocation.active_task_id = None
            invocation.error_code = code
            invocation.error_message = error_message
            invocation.completed_at = now
            await _commit_transition(
                db,
                invocation,
                event_type="capability_invocation_stale",
            )
            return invocation, execution
        except CapabilityError:
            await db.rollback()
            raise


async def mark_ready_invocation_stale(
    db: AsyncSession,
    *,
    invocation_id: int,
    expected_invocation_version: int,
    expected_execution_version: int,
    error_code: str,
    error_message: str,
) -> tuple[CapabilityInvocation, CapabilityExecution]:
    """Invalidate a ready result whose immutable external subject changed.

    The completed Execution and its output remain immutable audit evidence;
    only the still-active ready Invocation becomes terminal ``stale`` and
    releases the Task admission slot.
    """

    task_id = await _invocation_task_id(db, invocation_id)
    async with capability_task_lock(task_id):
        try:
            _, invocation, executions = await _lock_aggregate(db, invocation_id)
            execution = _exact_completed_output_execution(invocation, executions)
            _expect_version(
                invocation.state_version,
                expected_invocation_version,
                resource="invocation",
            )
            _expect_version(
                execution.state_version,
                expected_execution_version,
                resource="execution",
            )
            if invocation.status != "ready" or invocation.active_task_id is None:
                raise CapabilityConflictError("Capability result is not ready")
            now = datetime.utcnow()
            invocation.status = "stale"
            invocation.state_version += 1
            invocation.active_task_id = None
            invocation.error_code = error_code[:64] or "result_subject_stale"
            invocation.error_message = error_message
            invocation.completed_at = now
            await _commit_transition(
                db,
                invocation,
                event_type="capability_invocation_stale",
            )
            return invocation, execution
        except CapabilityError:
            await db.rollback()
            raise


async def consume_ready_invocation(
    db: AsyncSession,
    *,
    invocation_id: int,
    expected_state_version: int,
    allow_workflow_owned: bool = False,
    authorize_locked_task: LockedTaskAuthorizationCallback | None = None,
    lock_effect_task: LockedTaskEffectCallback | None = None,
) -> CapabilityInvocation:
    task_id = await _invocation_task_id(db, invocation_id)
    observed_task = await db.get(Task, task_id)
    if observed_task is None:
        raise CapabilityNotFoundError("Task not found")
    await _end_routing_read(db)
    async with capability_task_lock(task_id):
        try:
            if lock_effect_task is None:
                task, invocation, executions = await _lock_aggregate(
                    db,
                    invocation_id,
                )
            else:
                task = await lock_effect_task(db, observed_task, False)
                if task.id != task_id:
                    raise CapabilityConflictError(
                        "Capability effect fence returned a different Task"
                    )
                task, invocation, executions = await _lock_aggregate_after_task(
                    db,
                    invocation_id,
                    task,
                )
            if authorize_locked_task is not None:
                await authorize_locked_task(db, task)
            if not allow_workflow_owned:
                _ensure_public_human_task(task)
            if not allow_workflow_owned and (
                invocation.source != "human_request"
                or invocation.resume_policy != "attach_only"
            ):
                raise CapabilityConflictError(
                    "Workflow-owned Capability results cannot be consumed "
                    "through the public advisory lifecycle"
                )
            _expect_version(invocation.state_version, expected_state_version, resource="invocation")
            if invocation.status != "ready":
                raise CapabilityConflictError("Capability result is not ready")
            _exact_completed_output_execution(invocation, executions)
            invocation.status = "completed"
            invocation.state_version += 1
            invocation.active_task_id = None
            invocation.completed_at = datetime.utcnow()
            await _commit_transition(
                db,
                invocation,
                event_type="capability_invocation_completed",
            )
            return invocation
        except CapabilityError:
            await db.rollback()
            raise
        except BaseException:
            await _rollback_safely(db)
            raise


async def cancel_invocation(
    db: AsyncSession,
    *,
    invocation_id: int,
    expected_state_version: int,
    allow_workflow_owned: bool = False,
    authorize_locked_task: LockedTaskAuthorizationCallback | None = None,
    lock_effect_task: LockedTaskEffectCallback | None = None,
) -> CapabilityInvocation:
    """Request cancellation; queued/ready work terminates synchronously."""

    task_id = await _invocation_task_id(db, invocation_id)
    observed_task = await db.get(Task, task_id)
    if observed_task is None:
        raise CapabilityNotFoundError("Task not found")
    await _end_routing_read(db)
    async with capability_task_lock(task_id):
        try:
            if lock_effect_task is None:
                task, invocation, executions = await _lock_aggregate(
                    db,
                    invocation_id,
                )
            else:
                task = await lock_effect_task(db, observed_task, False)
                if task.id != task_id:
                    raise CapabilityConflictError(
                        "Capability effect fence returned a different Task"
                    )
                task, invocation, executions = await _lock_aggregate_after_task(
                    db,
                    invocation_id,
                    task,
                )
            if authorize_locked_task is not None:
                await authorize_locked_task(db, task)
            if not allow_workflow_owned:
                _ensure_public_human_task(task)
            if not allow_workflow_owned and (
                invocation.source != "human_request"
                or invocation.resume_policy != "attach_only"
            ):
                raise CapabilityConflictError(
                    "Workflow-owned Capabilities must be cancelled through "
                    "their owning Task or Controller lifecycle"
                )
            _expect_version(invocation.state_version, expected_state_version, resource="invocation")
            if invocation.status in TERMINAL_INVOCATION_STATUSES:
                await db.commit()
                return invocation

            now = datetime.utcnow()
            active = [
                execution
                for execution in executions
                if execution.active_invocation_id == invocation.id
            ]
            execution = active[0] if len(active) == 1 else None
            if invocation.status in {"queued", "ready", "resuming"}:
                if execution is not None:
                    execution.status = "cancelled"
                    execution.state_version += 1
                    execution.active_invocation_id = None
                    execution.completed_at = now
                invocation.status = "cancelled"
                invocation.active_task_id = None
                invocation.completed_at = now
                event_type = "capability_invocation_cancelled"
            elif execution is not None and execution.status in {
                "running",
                "waiting_user",
            }:
                execution.status = "cancelling"
                execution.state_version += 1
                invocation.status = "cancelling"
                event_type = "capability_invocation_cancelling"
            else:
                raise CapabilityConflictError(
                    "Capability invocation cannot be cancelled safely"
                )
            invocation.state_version += 1
            await _commit_transition(db, invocation, event_type=event_type)
            return invocation
        except CapabilityError:
            await db.rollback()
            raise
        except BaseException:
            await _rollback_safely(db)
            raise


async def mark_execution_cancelled(
    db: AsyncSession,
    *,
    invocation_id: int,
    expected_invocation_version: int,
    expected_execution_version: int,
) -> tuple[CapabilityInvocation, CapabilityExecution]:
    """Finalize cancellation after the adapter proves its handle is stopped."""

    task_id = await _invocation_task_id(db, invocation_id)
    async with capability_task_lock(task_id):
        try:
            _, invocation, executions = await _lock_aggregate(db, invocation_id)
            execution = _active_execution(invocation, executions)
            _expect_version(invocation.state_version, expected_invocation_version, resource="invocation")
            _expect_version(execution.state_version, expected_execution_version, resource="execution")
            if invocation.status != "cancelling" or execution.status != "cancelling":
                raise CapabilityConflictError("Capability cancellation is not pending")
            now = datetime.utcnow()
            execution.status = "cancelled"
            execution.state_version += 1
            execution.active_invocation_id = None
            execution.completed_at = now
            invocation.status = "cancelled"
            invocation.state_version += 1
            invocation.active_task_id = None
            invocation.completed_at = now
            await _commit_transition(
                db,
                invocation,
                event_type="capability_invocation_cancelled",
            )
            return invocation, execution
        except CapabilityError:
            await db.rollback()
            raise
