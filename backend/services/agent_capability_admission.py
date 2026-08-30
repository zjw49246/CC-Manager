"""Exact terminal-output admission for model-requested capabilities.

The provider consumers call the locked primitive from their existing
Task -> Instance terminal transaction.  Tests and recovery callers may use the
standalone wrapper, which owns the process-local Task fence and commit.  No
executor is started here: Invocation, first Execution, resume outbox, and the
Task ``waiting_capability`` transition become visible in one commit.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
import hashlib
import re
from typing import Literal

from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.models.capability import (
    CapabilityExecution,
    CapabilityInvocation,
    CapabilityResumeOutbox,
)
from backend.models.log_entry import LogEntry
from backend.models.task import Task
from backend.services.auto_capability_policy import (
    validate_auto_capability_task_scope,
)
from backend.services.capability_protocol import (
    CapabilityProtocolError,
    CapabilityRequestAction,
    parse_capability_terminal_action,
)
from backend.services.capability_registry import resolve_capability
from backend.services.capability_service import (
    CapabilityConflictError,
    CapabilityValidationError,
    _end_routing_read,
    _lock_task,
    _task_subject,
    _validate_request,
    capability_task_lock,
    capability_value_hash,
)
from backend.services.terminal_arbitration import (
    select_terminal_output,
    source_alias_original_log_id,
)


AgentTerminalOutcome = Literal[
    "ordinary_completion",
    "waiting_capability",
    "protocol_failed",
    "stale",
]

_PLAN_REQUEST_FIELDS = frozenset({"prompt", "title"})
_CODE_REVIEW_REQUEST_FIELDS = frozenset({"base_sha", "head_sha"})
_FULL_GIT_SHA_RE = re.compile(r"[0-9a-f]{40}\Z")


@dataclass(frozen=True, slots=True)
class AgentTerminalExpectation:
    """Pre-transaction identity of the provider turn that just settled."""

    task_id: int
    task_incarnation_id: str
    retry_count: int
    turn_generation: int
    instance_id: int
    source_log_id: int


@dataclass(frozen=True, slots=True)
class AgentTerminalAdmission:
    outcome: AgentTerminalOutcome
    task_id: int
    task_status: str
    invocation_id: int | None = None
    outbox_id: int | None = None
    output_log_id: int | None = None
    terminal_log_id: int | None = None
    created: bool = False
    error_code: str | None = None
    error_message: str | None = None


async def publish_agent_terminal_admission_locked(
    db: AsyncSession,
    admission: AgentTerminalAdmission | None,
) -> bool:
    """Publish a creation invalidation only for its surviving Task generation.

    The caller owns ``capability_task_lock(admission.task_id)`` and the
    transaction boundary.  The no-op Task update is both a database row fence
    and a generation check; callers must commit it after the best-effort
    broadcast or roll it back when this function returns ``False``.
    """

    if (
        admission is None
        or not admission.created
        or admission.invocation_id is None
    ):
        return False
    invocation = await db.get(
        CapabilityInvocation,
        admission.invocation_id,
        populate_existing=True,
    )
    if (
        invocation is None
        or invocation.task_id != admission.task_id
        or invocation.source != "agent_request"
        or type(invocation.request_task_retry_count) is not int
        or type(invocation.request_task_turn_generation) is not int
        or type(invocation.request_source_log_id) is not int
    ):
        return False

    from backend.services.worker_task_termination import (
        no_active_worker_task_termination_predicate,
    )

    task_guard = await db.execute(
        update(Task)
        .where(
            Task.id == invocation.task_id,
            Task.status == "waiting_capability",
            Task.incarnation_id == invocation.request_task_incarnation_id,
            Task.retry_count == invocation.request_task_retry_count,
            Task.turn_generation == invocation.request_task_turn_generation,
            Task.turn_source_log_id == invocation.request_source_log_id,
            (
                Task.instance_id.is_(None)
                if invocation.request_task_instance_id is None
                else Task.instance_id == invocation.request_task_instance_id
            ),
            no_active_worker_task_termination_predicate(),
        )
        .values(status=Task.status)
    )
    if not task_guard.rowcount:
        return False
    from backend.services.capability_events import broadcast_capability_event

    await broadcast_capability_event(
        "capability_invocation_created",
        invocation,
        created=True,
    )
    return True


def _expectation_is_well_formed(expected: AgentTerminalExpectation) -> bool:
    return bool(
        type(expected.task_id) is int
        and expected.task_id > 0
        and isinstance(expected.task_incarnation_id, str)
        and len(expected.task_incarnation_id) == 32
        and type(expected.retry_count) is int
        and expected.retry_count >= 0
        and type(expected.turn_generation) is int
        and expected.turn_generation >= 0
        and type(expected.instance_id) is int
        and expected.instance_id > 0
        and type(expected.source_log_id) is int
        and expected.source_log_id > 0
    )


def _task_matches_expectation(
    task: Task,
    expected: AgentTerminalExpectation,
) -> bool:
    return bool(
        task.id == expected.task_id
        and task.incarnation_id == expected.task_incarnation_id
        and task.retry_count == expected.retry_count
        and task.turn_generation == expected.turn_generation
        and task.instance_id == expected.instance_id
        and task.turn_source_log_id == expected.source_log_id
    )


def _mark_protocol_failed(
    task: Task,
    *,
    code: str,
    message: str,
    output_log_id: int | None = None,
    terminal_log_id: int | None = None,
) -> AgentTerminalAdmission:
    normalized_code = (code or "capability_protocol_rejected")[:64]
    normalized_message = str(message or "Capability request was rejected")
    task.status = "failed"
    task.completed_at = datetime.utcnow()
    task.error_message = (
        f"Auto capability request rejected ({normalized_code}): "
        f"{normalized_message}"
    )[:2000]
    task.pty_background_generation = None
    return AgentTerminalAdmission(
        outcome="protocol_failed",
        task_id=task.id,
        task_status=task.status,
        output_log_id=output_log_id,
        terminal_log_id=terminal_log_id,
        error_code=normalized_code,
        error_message=normalized_message,
    )


async def _select_exact_terminal(
    db: AsyncSession,
    task: Task,
) -> tuple[LogEntry, LogEntry, LogEntry, str | None] | None:
    source_id = task.turn_source_log_id
    if type(source_id) is not int or source_id <= 0:
        return None
    source = (
        await db.execute(
            select(LogEntry)
            .where(LogEntry.id == source_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if source is None:
        return None
    original = None
    original_id = source_alias_original_log_id(source)
    if original_id is not None:
        original = (
            await db.execute(
                select(LogEntry)
                .where(LogEntry.id == original_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
    rows = list(
        (
            await db.execute(
                select(LogEntry)
                .where(
                    LogEntry.task_id == task.id,
                    LogEntry.task_retry_count == task.retry_count,
                    LogEntry.task_turn_generation == task.turn_generation,
                )
                .order_by(LogEntry.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        )
        .scalars()
        .all()
    )
    selected = select_terminal_output(
        task.provider or "claude",
        source,
        rows,
        source_original=original,
    )
    if selected is None:
        return None
    return (
        source,
        selected.output_log,
        selected.terminal_log,
        selected.native_turn_id,
    )


def _output_hash(content: str) -> str:
    try:
        encoded = content.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise CapabilityValidationError(
            "Terminal assistant output is not valid UTF-8"
        ) from exc
    return hashlib.sha256(encoded).hexdigest()


def _validate_agent_capability_request(
    action: CapabilityRequestAction,
) -> tuple[dict, str]:
    """Validate the capability-specific model contract before persistence.

    Human and Delivery Controller invocations deliberately retain Capability
    Core's generic JSON-object contract.  This boundary applies only to the
    terminal protocol taught to an Auto Task, so malformed model requests
    cannot consume budget and wait for an executor merely to reject them.
    """

    payload, input_hash = _validate_request(action.request)
    fields = frozenset(payload)

    if action.capability == "plan":
        if "prompt" not in fields or not fields.issubset(_PLAN_REQUEST_FIELDS):
            raise CapabilityValidationError(
                "Plan request must contain exactly prompt and optional title"
            )
        prompt = payload["prompt"]
        if not isinstance(prompt, str) or not prompt.strip():
            raise CapabilityValidationError(
                "Plan request prompt must be a non-empty string"
            )
        if "title" in payload and not isinstance(payload["title"], str):
            raise CapabilityValidationError(
                "Plan request title must be a string"
            )
        return payload, input_hash

    if action.capability == "code_review":
        if fields != _CODE_REVIEW_REQUEST_FIELDS:
            raise CapabilityValidationError(
                "Code Review request must contain exactly base_sha and head_sha"
        )
        for field in ("base_sha", "head_sha"):
            value = payload[field]
            if (
                not isinstance(value, str)
                or _FULL_GIT_SHA_RE.fullmatch(value) is None
            ):
                raise CapabilityValidationError(
                    f"Code Review {field} must be a full lowercase 40-character Git SHA"
                )
        return payload, input_hash

    raise CapabilityValidationError(
        f"Auto capability {action.capability!r} has no request contract"
    )


def _same_agent_identity(
    invocation: CapabilityInvocation,
    outbox: CapabilityResumeOutbox | None,
    *,
    task: Task,
    action: CapabilityRequestAction,
    source: LogEntry,
    output: LogEntry,
    terminal: LogEntry,
    native_turn_id: str | None,
    output_hash: str,
    input_hash: str,
) -> bool:
    return bool(
        invocation.source == "agent_request"
        and invocation.capability_key == action.capability
        and invocation.purpose == "advisory"
        and invocation.resume_policy == "resume_task"
        and invocation.input_payload == action.request
        and invocation.input_hash == input_hash
        and invocation.request_task_incarnation_id == task.incarnation_id
        and invocation.request_task_retry_count == task.retry_count
        and invocation.request_task_instance_id == task.instance_id
        and invocation.request_task_started_at == task.started_at
        and invocation.request_task_session_id == task.session_id
        and invocation.request_task_turn_generation == task.turn_generation
        and invocation.request_source_log_id == source.id
        and invocation.request_output_log_id == output.id
        and invocation.request_terminal_log_id == terminal.id
        and invocation.request_reason == action.reason
        and invocation.request_protocol_version == action.schema_version
        and invocation.request_output_hash == output_hash
        and invocation.request_native_turn_id == native_turn_id
        and outbox is not None
        and outbox.task_id == task.id
        and outbox.invocation_id == invocation.id
        and outbox.request_task_incarnation_id == task.incarnation_id
        and outbox.request_task_retry_count == task.retry_count
        and outbox.from_turn_generation == task.turn_generation
        and outbox.request_task_session_id == task.session_id
        and outbox.request_source_log_id == source.id
        and outbox.request_output_log_id == output.id
        and outbox.request_terminal_log_id == terminal.id
        and outbox.request_native_turn_id == native_turn_id
    )


async def admit_agent_terminal_action_locked(
    db: AsyncSession,
    task: Task,
    *,
    expected: AgentTerminalExpectation,
) -> AgentTerminalAdmission:
    """Interpret and stage one exact output inside a caller-owned transaction.

    The caller must already hold ``capability_task_lock(task.id)`` and the Task
    row lock.  This function flushes generated ids but never commits, rolls
    back, broadcasts, starts an executor, or releases an Instance.
    """

    if not _expectation_is_well_formed(expected):
        raise CapabilityValidationError("Invalid Agent terminal expectation")
    if not _task_matches_expectation(task, expected):
        return AgentTerminalAdmission(
            outcome="stale",
            task_id=expected.task_id,
            task_status=task.status,
        )
    if (
        task.worker_id is not None
        or task.shared_from_id is not None
        or task.mode != "auto"
        or task.delivery_run_id is not None
        or task.delivery_role is not None
        or task.plan_target_task_id is not None
    ):
        return AgentTerminalAdmission(
            outcome="ordinary_completion",
            task_id=task.id,
            task_status=task.status,
        )

    try:
        policy = validate_auto_capability_task_scope(
            task.capability_policy,
            task_id=None,
            mode=task.mode,
            worker_id=task.worker_id,
            shared_from_id=task.shared_from_id,
            delivery_run_id=task.delivery_run_id,
            delivery_role=task.delivery_role,
            plan_target_task_id=task.plan_target_task_id,
        )
    except ValueError as exc:
        if task.status == "waiting_capability":
            raise CapabilityConflictError(
                "Waiting capability Task has an invalid persisted policy"
            ) from exc
        return _mark_protocol_failed(
            task,
            code="invalid_capability_policy",
            message=str(exc),
        )
    if policy is None:
        return AgentTerminalAdmission(
            outcome="ordinary_completion",
            task_id=task.id,
            task_status=task.status,
        )

    selected = await _select_exact_terminal(db, task)
    if selected is None:
        if task.status == "waiting_capability":
            raise CapabilityConflictError(
                "Waiting capability Task lost its exact terminal output"
            )
        if not (
            settings.capability_core_enabled
            and settings.auto_capability_enabled
        ):
            return AgentTerminalAdmission(
                outcome="ordinary_completion",
                task_id=task.id,
                task_status=task.status,
            )
        return _mark_protocol_failed(
            task,
            code="terminal_output_unproven",
            message="The successful provider turn has no canonical terminal output",
        )
    source, output, terminal, native_turn_id = selected
    if not isinstance(output.content, str):
        if task.status == "waiting_capability":
            raise CapabilityConflictError(
                "Waiting capability Task has a non-text terminal output"
            )
        return _mark_protocol_failed(
            task,
            code="terminal_output_invalid",
            message="The canonical terminal output is not text",
            output_log_id=output.id,
            terminal_log_id=terminal.id,
        )

    try:
        action = parse_capability_terminal_action(
            output.content,
            allowed_capabilities=tuple(policy["capabilities"]),
        )
    except CapabilityProtocolError as exc:
        if task.status == "waiting_capability":
            raise CapabilityConflictError(
                "Waiting capability Task terminal action no longer parses"
            ) from exc
        return _mark_protocol_failed(
            task,
            code=exc.code,
            message=str(exc),
            output_log_id=output.id,
            terminal_log_id=terminal.id,
        )
    if action is None:
        if task.status == "waiting_capability":
            raise CapabilityConflictError(
                "Waiting capability Task lost its terminal request marker"
            )
        return AgentTerminalAdmission(
            outcome="ordinary_completion",
            task_id=task.id,
            task_status=task.status,
            output_log_id=output.id,
            terminal_log_id=terminal.id,
        )

    output_hash = _output_hash(output.content)
    try:
        request_payload, input_hash = _validate_agent_capability_request(action)
    except CapabilityValidationError as exc:
        if task.status == "waiting_capability":
            raise CapabilityConflictError(
                "Waiting capability Task request payload no longer validates"
            ) from exc
        return _mark_protocol_failed(
            task,
            code="invalid_capability_request",
            message=str(exc),
            output_log_id=output.id,
            terminal_log_id=terminal.id,
        )

    existing = (
        await db.execute(
            select(CapabilityInvocation)
            .where(
                CapabilityInvocation.task_id == task.id,
                CapabilityInvocation.request_output_log_id == output.id,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if existing is not None:
        outbox = (
            await db.execute(
                select(CapabilityResumeOutbox)
                .where(CapabilityResumeOutbox.invocation_id == existing.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
        if not _same_agent_identity(
            existing,
            outbox,
            task=task,
            action=action,
            source=source,
            output=output,
            terminal=terminal,
            native_turn_id=native_turn_id,
            output_hash=output_hash,
            input_hash=input_hash,
        ):
            raise CapabilityConflictError(
                "Durable capability request no longer matches its terminal output"
            )
        if task.status != "waiting_capability" or outbox is None:
            raise CapabilityConflictError(
                "Durable capability request is not in its waiting Task state"
            )
        return AgentTerminalAdmission(
            outcome="waiting_capability",
            task_id=task.id,
            task_status=task.status,
            invocation_id=existing.id,
            outbox_id=outbox.id,
            output_log_id=output.id,
            terminal_log_id=terminal.id,
            created=False,
        )

    if task.status not in {"executing", "in_progress"}:
        return _mark_protocol_failed(
            task,
            code="capability_admission_state_changed",
            message="Task is no longer at the exact provider terminal boundary",
            output_log_id=output.id,
            terminal_log_id=terminal.id,
        )
    if not settings.capability_core_enabled:
        return _mark_protocol_failed(
            task,
            code="capability_core_disabled",
            message="Capability Core was disabled before terminal admission",
            output_log_id=output.id,
            terminal_log_id=terminal.id,
        )
    if not settings.auto_capability_enabled:
        return _mark_protocol_failed(
            task,
            code="auto_capability_disabled",
            message="Auto capability admission was disabled before terminal admission",
            output_log_id=output.id,
            terminal_log_id=terminal.id,
        )

    definition = resolve_capability(action.capability)
    if definition is None:
        return _mark_protocol_failed(
            task,
            code="capability_unavailable",
            message=f"Capability {action.capability!r} is not registered",
            output_log_id=output.id,
            terminal_log_id=terminal.id,
        )
    active_id = await db.scalar(
        select(CapabilityInvocation.id)
        .where(CapabilityInvocation.active_task_id == task.id)
        .limit(1)
        .with_for_update()
    )
    if active_id is not None:
        return _mark_protocol_failed(
            task,
            code="capability_slot_busy",
            message=f"Task already has active capability invocation {active_id}",
            output_log_id=output.id,
            terminal_log_id=terminal.id,
        )

    total_used = int(
        await db.scalar(
            select(func.count(CapabilityInvocation.id)).where(
                CapabilityInvocation.task_id == task.id,
                CapabilityInvocation.source == "agent_request",
                CapabilityInvocation.request_task_incarnation_id
                == task.incarnation_id,
            )
        )
        or 0
    )
    capability_used = int(
        await db.scalar(
            select(func.count(CapabilityInvocation.id)).where(
                CapabilityInvocation.task_id == task.id,
                CapabilityInvocation.source == "agent_request",
                CapabilityInvocation.request_task_incarnation_id
                == task.incarnation_id,
                CapabilityInvocation.capability_key == action.capability,
            )
        )
        or 0
    )
    if total_used >= policy["max_invocations"]:
        return _mark_protocol_failed(
            task,
            code="capability_budget_exhausted",
            message="Task exhausted its total Auto capability budget",
            output_log_id=output.id,
            terminal_log_id=terminal.id,
        )
    if capability_used >= policy["capabilities"][action.capability]:
        return _mark_protocol_failed(
            task,
            code="capability_budget_exhausted",
            message=(
                f"Task exhausted its {action.capability!r} Auto capability budget"
            ),
            output_log_id=output.id,
            terminal_log_id=terminal.id,
        )

    subject_ref, subject_hash = _task_subject(task)
    executor_config = deepcopy(definition.executor_config)
    policy_snapshot = deepcopy(definition.policy_snapshot)
    idempotency_key = (
        f"agent:v1:{task.incarnation_id}:{task.retry_count}:"
        f"{task.turn_generation}:{output.id}"
    )
    invocation = CapabilityInvocation(
        task_id=task.id,
        capability_key=definition.capability_key,
        source="agent_request",
        purpose="advisory",
        status="queued",
        state_version=1,
        idempotency_key=idempotency_key,
        input_payload=request_payload,
        input_hash=input_hash,
        subject_kind="task_generation",
        subject_ref=subject_ref,
        subject_hash=subject_hash,
        executor_kind=definition.executor_kind,
        executor_config=executor_config,
        executor_config_hash=capability_value_hash(executor_config),
        policy_snapshot=policy_snapshot,
        policy_hash=capability_value_hash(policy_snapshot),
        resume_policy="resume_task",
        max_attempts=definition.max_attempts,
        active_task_id=task.id,
        requested_by_user_id=None,
        request_task_incarnation_id=task.incarnation_id,
        request_task_retry_count=task.retry_count,
        request_task_instance_id=task.instance_id,
        request_task_started_at=task.started_at,
        request_task_session_id=task.session_id,
        request_task_turn_generation=task.turn_generation,
        request_source_log_id=source.id,
        request_output_log_id=output.id,
        request_terminal_log_id=terminal.id,
        request_reason=action.reason,
        request_protocol_version=action.schema_version,
        request_output_hash=output_hash,
        request_native_turn_id=native_turn_id,
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
    outbox = CapabilityResumeOutbox(
        task_id=task.id,
        invocation_id=invocation.id,
        active_task_id=task.id,
        active_invocation_id=invocation.id,
        status="pending",
        state_version=1,
        request_task_incarnation_id=task.incarnation_id,
        request_task_retry_count=task.retry_count,
        from_turn_generation=task.turn_generation,
        request_task_session_id=task.session_id,
        request_source_log_id=source.id,
        request_output_log_id=output.id,
        request_terminal_log_id=terminal.id,
        request_native_turn_id=native_turn_id,
        request_execution_user_id=task.execution_user_id,
        request_execution_user_role=task.execution_user_role,
        request_execution_mode=task.execution_mode,
        request_execution_principal_kind=task.execution_principal_kind,
    )
    db.add(outbox)
    task.status = "waiting_capability"
    task.completed_at = None
    task.error_message = None
    task.pty_background_generation = None
    await db.flush()
    return AgentTerminalAdmission(
        outcome="waiting_capability",
        task_id=task.id,
        task_status=task.status,
        invocation_id=invocation.id,
        outbox_id=outbox.id,
        output_log_id=output.id,
        terminal_log_id=terminal.id,
        created=True,
    )


async def admit_agent_terminal_action(
    db: AsyncSession,
    *,
    expected: AgentTerminalExpectation,
) -> AgentTerminalAdmission:
    """Standalone atomic admission/replay wrapper."""

    if not _expectation_is_well_formed(expected):
        raise CapabilityValidationError("Invalid Agent terminal expectation")
    await _end_routing_read(db)
    result: AgentTerminalAdmission | None = None
    try:
        async with capability_task_lock(expected.task_id):
            task = await _lock_task(
                db,
                expected.task_id,
                require_termination_clear=True,
            )
            result = await admit_agent_terminal_action_locked(
                db,
                task,
                expected=expected,
            )
            await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise CapabilityConflictError(
            "A concurrent Agent capability admission won"
        ) from exc
    except BaseException:
        await db.rollback()
        raise

    assert result is not None
    if result.created and result.invocation_id is not None:
        async with capability_task_lock(result.task_id):
            if await publish_agent_terminal_admission_locked(db, result):
                await db.commit()
            else:
                await db.rollback()
    return result
