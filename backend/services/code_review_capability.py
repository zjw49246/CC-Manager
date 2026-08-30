"""Provider-neutral pre-PR code-review Capability adapter.

The coding Task never waits on a reviewer process.  Each execution attempt
owns a separate, tool-free Task whose complete input is an immutable Git
snapshot.  The adapter later claims only output from that exact Task retry
generation and re-verifies the Git subject before publishing a durable result.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
import hashlib
import inspect
from typing import Any

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.capability import CapabilityExecution, CapabilityInvocation
from backend.models.code_review import CodeReviewResult, CodeReviewRun
from backend.models.log_entry import LogEntry
from backend.models.task import Task
from backend.services.capability_registry import CapabilityDefinition
from backend.services.capability_service import (
    CapabilityConflictError,
    CapabilityNotFoundError,
    CapabilityValidationError,
    StagedCapabilityHandle,
    ValidatedCapabilityOutput,
    active_execution_for,
    cancel_invocation,
    capability_value_hash,
    fail_execution,
    mark_execution_cancelled,
    mark_execution_stale,
    mark_ready_invocation_stale,
    stage_and_claim_execution,
    validate_and_complete_execution,
)
from backend.services.code_review_subject import (
    CodeReviewSubjectError,
    RepositoryStateError,
    SubjectChangedError,
    capture_commit_range_subject,
    verify_commit_range_subject,
)
from backend.services.pr_review_runtime import PRE_PR_CODE_REVIEW_TAG
from backend.services.structured_code_review import (
    CODE_REVIEWER_ROLE,
    CommitRangeSubject,
    build_structured_review_prompt,
    parse_structured_review_output,
)
from backend.services.task_creation import (
    stage_task_record,
    system_task_execution_principal_values,
)
from backend.services.task_termination import (
    LocalTaskGeneration,
    TaskTerminationConflict,
    local_task_generation,
    terminate_authoritative_task_generation,
)


CODE_REVIEW_CAPABILITY_KEY = "code_review"
CODE_REVIEW_EXECUTOR_KIND = "code_review_task"
CODE_REVIEW_RUN_HANDLE_KIND = "code_review_run"
CODE_REVIEW_RESULT_OUTPUT_KIND = "code_review_result"

WakeCallback = Callable[[], Awaitable[None] | None]


class CodeReviewCancellationUnconfirmed(CapabilityConflictError):
    """The exact reviewer Task generation could not be proven stopped."""


class _CodeReviewTransitionStale(RuntimeError):
    """A locked transition no longer matches its pre-lock snapshot."""


@dataclass(frozen=True, slots=True)
class CodeReviewCapabilityObservation:
    invocation_id: int
    execution_id: int
    status: str
    run_id: int | None = None
    run_status: str | None = None
    reviewer_task_id: int | None = None
    reviewer_task_status: str | None = None
    result_id: int | None = None
    verdict: str | None = None
    findings_count: int | None = None
    output_hash: str | None = None
    error_code: str | None = None
    error_message: str | None = None


def code_review_capability_definition(
    *,
    executor: "CodeReviewCapabilityExecutor | None" = None,
    provider: str | None = None,
    model: str | None = None,
    effort_level: str | None = None,
    codex_service_tier: str | None = None,
    timeout_hours: float = 1.0,
    max_attempts: int = 2,
) -> CapabilityDefinition:
    """Build a registry entry without mutating the global registry."""

    if provider is not None and provider not in {"claude", "codex"}:
        raise ValueError("code-review provider must be 'claude' or 'codex'")
    if not isinstance(timeout_hours, (int, float)) or timeout_hours <= 0:
        raise ValueError("code-review timeout_hours must be positive")
    return CapabilityDefinition(
        capability_key=CODE_REVIEW_CAPABILITY_KEY,
        executor_kind=CODE_REVIEW_EXECUTOR_KIND,
        executor_config={
            "provider": provider,
            "model": model,
            "effort_level": effort_level,
            "codex_service_tier": codex_service_tier,
            "timeout_hours": float(timeout_hours),
        },
        policy_snapshot={
            "local_only": True,
            "surface": "pre_pr",
            "tool_free": True,
            "immutable_commit_range": True,
            "publishes_github": False,
        },
        max_attempts=max_attempts,
        executor=executor,
    )


async def _maybe_call(callback: WakeCallback | None) -> None:
    if callback is None:
        return
    result = callback()
    if inspect.isawaitable(result):
        await result


async def _latest_execution(
    db: AsyncSession,
    invocation_id: int,
) -> CapabilityExecution | None:
    return (
        await db.execute(
            select(CapabilityExecution)
            .where(CapabilityExecution.invocation_id == invocation_id)
            .order_by(CapabilityExecution.attempt.desc())
            .limit(1)
        )
    ).scalar_one_or_none()


async def _is_schema_validation_retry(
    db: AsyncSession,
    execution: CapabilityExecution,
) -> bool:
    """Return whether this attempt follows a strict-output validation failure."""

    if execution.attempt <= 1:
        return False
    previous = (
        await db.execute(
            select(CapabilityExecution).where(
                CapabilityExecution.invocation_id == execution.invocation_id,
                CapabilityExecution.attempt == execution.attempt - 1,
            )
        )
    ).scalar_one_or_none()
    return (
        previous is not None
        and previous.status == "failed"
        and previous.error_code == "code_review_output_invalid"
    )


async def _load_invocation(
    db: AsyncSession,
    invocation_id: int,
) -> CapabilityInvocation:
    invocation = await db.get(CapabilityInvocation, invocation_id)
    if invocation is None:
        raise CapabilityNotFoundError("Capability invocation not found")
    if invocation.capability_key != CODE_REVIEW_CAPABILITY_KEY:
        raise CapabilityValidationError(
            "Invocation is not a pre-PR Code Review capability"
        )
    if invocation.executor_kind != CODE_REVIEW_EXECUTOR_KIND:
        raise CapabilityValidationError(
            "Invocation does not use the Code Review Task executor"
        )
    return invocation


def _parse_run_handle(execution: CapabilityExecution) -> int:
    if (
        execution.handle_kind != CODE_REVIEW_RUN_HANDLE_KIND
        or execution.handle_id is None
    ):
        raise CapabilityConflictError(
            "Code Review execution has no exact durable run handle"
        )
    try:
        run_id = int(execution.handle_id)
    except (TypeError, ValueError) as exc:
        raise CapabilityConflictError("Code Review run handle is invalid") from exc
    if run_id <= 0 or str(run_id) != execution.handle_id:
        raise CapabilityConflictError("Code Review run handle is invalid")
    return run_id


async def _run_for_execution(
    db: AsyncSession,
    execution_id: int,
) -> CodeReviewRun | None:
    return (
        await db.execute(
            select(CodeReviewRun).where(
                CodeReviewRun.capability_execution_id == execution_id
            )
        )
    ).scalar_one_or_none()


def _review_task_prompt_hash(task: Task) -> str:
    return hashlib.sha256((task.description or "").encode("utf-8")).hexdigest()


async def _load_exact_run_task(
    db: AsyncSession,
    invocation: CapabilityInvocation,
    execution: CapabilityExecution,
) -> tuple[CodeReviewRun, Task]:
    run_id = _parse_run_handle(execution)
    run = await db.get(CodeReviewRun, run_id)
    if run is None:
        raise CapabilityConflictError("Code Review run handle no longer exists")
    if (
        run.capability_invocation_id != invocation.id
        or run.capability_execution_id != execution.id
        or run.developer_task_id != invocation.task_id
        or run.attempt != execution.attempt
    ):
        raise CapabilityConflictError(
            "Code Review run does not belong to this capability execution"
        )
    reviewer_task = await db.get(Task, run.reviewer_task_id)
    if reviewer_task is None:
        raise CapabilityConflictError("Code Review reviewer Task no longer exists")
    _validate_run_task_identity(
        invocation,
        execution,
        run,
        reviewer_task,
        require_claimed_handle=True,
    )
    return run, reviewer_task


def _validate_run_task_identity(
    invocation: CapabilityInvocation,
    execution: CapabilityExecution,
    run: CodeReviewRun,
    reviewer_task: Task,
    *,
    require_claimed_handle: bool,
) -> None:
    metadata = reviewer_task.metadata_ or {}
    if (
        reviewer_task.worker_id is not None
        or reviewer_task.shared_from_id is not None
        or reviewer_task.retry_count != run.reviewer_task_retry_count
        or (
            require_claimed_handle
            and execution.handle_generation != run.reviewer_task_retry_count
        )
        or PRE_PR_CODE_REVIEW_TAG not in (reviewer_task.tags or [])
        or metadata.get("code_review_run_id") != run.id
        or metadata.get("capability_invocation_id") != invocation.id
        or metadata.get("capability_execution_id") != execution.id
        or metadata.get("code_review_subject_hash") != run.subject_hash
        or _review_task_prompt_hash(reviewer_task) != run.prompt_hash
    ):
        raise CapabilityConflictError(
            "Code Review reviewer Task generation or immutable prompt changed"
        )


async def _result_for_run(
    db: AsyncSession,
    run_id: int,
) -> CodeReviewResult | None:
    return (
        await db.execute(
            select(CodeReviewResult).where(CodeReviewResult.run_id == run_id)
        )
    ).scalar_one_or_none()


def _observation(
    invocation: CapabilityInvocation,
    execution: CapabilityExecution,
    *,
    run: CodeReviewRun | None = None,
    reviewer_task: Task | None = None,
    result: CodeReviewResult | None = None,
) -> CodeReviewCapabilityObservation:
    return CodeReviewCapabilityObservation(
        invocation_id=invocation.id,
        execution_id=execution.id,
        status=invocation.status,
        run_id=run.id if run is not None else None,
        run_status=run.status if run is not None else None,
        reviewer_task_id=(
            reviewer_task.id
            if reviewer_task is not None
            else (run.reviewer_task_id if run is not None else None)
        ),
        reviewer_task_status=(
            reviewer_task.status if reviewer_task is not None else None
        ),
        result_id=result.id if result is not None else execution.output_id,
        verdict=result.verdict if result is not None else None,
        findings_count=(len(result.findings) if result is not None else None),
        output_hash=execution.output_hash,
        error_code=(
            execution.error_code
            or invocation.error_code
            or (run.error_code if run is not None else None)
        ),
        error_message=(
            execution.error_message
            or invocation.error_message
            or (run.error_message if run is not None else None)
        ),
    )


def _developer_generation_matches(
    invocation: CapabilityInvocation,
    task: Task,
) -> bool:
    subject = invocation.subject_ref
    expected_core = {
        "task_id": task.id,
        "retry_count": task.retry_count,
        "instance_id": task.instance_id,
        "started_at": task.started_at.isoformat() if task.started_at else None,
        "session_id": task.session_id,
    }
    if invocation.source == "agent_request":
        expected_core.update(
            incarnation_id=task.incarnation_id,
            turn_generation=task.turn_generation,
        )
    return (
        invocation.subject_kind == "task_generation"
        and isinstance(subject, dict)
        and all(subject.get(key) == value for key, value in expected_core.items())
        and invocation.subject_hash == capability_value_hash(subject)
        and (
            invocation.source != "agent_request"
            or (
                invocation.request_task_incarnation_id == task.incarnation_id
                and invocation.request_task_turn_generation
                == task.turn_generation
            )
        )
    )


def _task_subject_value(
    invocation: CapabilityInvocation,
    task: Task,
    field: str,
) -> object:
    """Read an invocation-time Task field with legacy-row compatibility."""

    subject = invocation.subject_ref
    if isinstance(subject, dict) and field in subject:
        return subject[field]
    return getattr(task, field)


def _request_subject(invocation: CapabilityInvocation) -> tuple[str, str]:
    payload = invocation.input_payload
    if not isinstance(payload, dict) or set(payload) != {"base_sha", "head_sha"}:
        raise CapabilityValidationError(
            "Code Review input must contain exactly base_sha and head_sha"
        )
    base_sha = payload.get("base_sha")
    head_sha = payload.get("head_sha")
    if not isinstance(base_sha, str) or not isinstance(head_sha, str):
        raise CapabilityValidationError("Code Review commit IDs must be strings")
    return base_sha, head_sha


def _review_route(
    invocation: CapabilityInvocation,
    developer_task: Task,
) -> dict[str, Any]:
    config = invocation.executor_config
    if not isinstance(config, dict):
        raise CapabilityValidationError(
            "Code Review executor configuration must be an object"
        )
    provider = config.get("provider") or _task_subject_value(
        invocation, developer_task, "provider"
    )
    if provider not in {"claude", "codex"}:
        raise CapabilityValidationError(
            "Code Review provider must be 'claude' or 'codex'"
        )
    developer_provider = _task_subject_value(
        invocation, developer_task, "provider"
    )
    if not isinstance(developer_provider, str):
        raise CapabilityValidationError(
            "Code Review Task subject has an invalid provider"
        )
    same_provider = provider == (developer_provider or "claude").lower()
    model = config.get("model")
    if model is None and same_provider:
        model = _task_subject_value(invocation, developer_task, "model")
    effort_level = config.get("effort_level") or _task_subject_value(
        invocation, developer_task, "effort_level"
    )
    service_tier = config.get("codex_service_tier")
    if service_tier is None:
        service_tier = (
            _task_subject_value(
                invocation,
                developer_task,
                "codex_service_tier",
            )
            if provider == "codex" and same_provider
            else "default"
        )
    timeout_hours = config.get("timeout_hours", 1.0)
    if (
        isinstance(timeout_hours, bool)
        or not isinstance(timeout_hours, (int, float))
        or timeout_hours <= 0
    ):
        raise CapabilityValidationError(
            "Code Review timeout_hours must be positive"
        )
    for field, value in (
        ("model", model),
        ("effort_level", effort_level),
        ("codex_service_tier", service_tier),
    ):
        if value is not None and not isinstance(value, str):
            raise CapabilityValidationError(
                f"Code Review {field} must be a string or null"
            )
    return {
        "provider": provider,
        "model": model,
        "effort_level": effort_level,
        "codex_service_tier": service_tier,
        "timeout_hours": float(timeout_hours),
    }


def _subject_from_run(run: CodeReviewRun) -> CommitRangeSubject:
    subject = CommitRangeSubject(
        base_sha=run.base_sha,
        head_sha=run.head_sha,
        head_tree_sha=run.head_tree_sha,
        patch_sha256=run.patch_sha256,
    )
    if subject.as_dict() != run.subject_ref:
        raise CapabilityConflictError(
            "Code Review run subject fields are internally inconsistent"
        )
    if capability_value_hash(run.subject_ref) != run.subject_hash:
        raise CapabilityConflictError("Code Review run subject hash is invalid")
    return subject


def _set_run_terminal(
    run: CodeReviewRun,
    *,
    status: str,
    error_code: str | None = None,
    error_message: str | None = None,
) -> None:
    if run.status != status:
        run.status = status
        run.state_version += 1
    run.error_code = error_code
    run.error_message = error_message
    run.updated_at = datetime.utcnow()
    run.completed_at = datetime.utcnow()


async def _terminate_exact_reviewer_task(
    db: AsyncSession,
    reviewer_task: Task,
    *,
    reason: str,
) -> None:
    expected: LocalTaskGeneration = local_task_generation(reviewer_task)
    try:
        await terminate_authoritative_task_generation(
            reviewer_task.id,
            db,
            reason=reason,
            expected_local_generation=expected,
            allow_delivery_effect_stop=True,
        )
    except TaskTerminationConflict as exc:
        raise CodeReviewCancellationUnconfirmed(
            f"Reviewer Task #{reviewer_task.id} cleanup was not confirmed"
        ) from exc


def _reviewer_may_be_active(task: Task) -> bool:
    return task.status in {"pending", "in_progress", "executing", "merging"} or (
        task.pty_background_generation is not None
    )


async def _exact_review_outputs(
    db: AsyncSession,
    *,
    reviewer_task: Task,
    subject: CommitRangeSubject,
) -> tuple[dict[str, object], LogEntry]:
    if (
        reviewer_task.status != "completed"
        or reviewer_task.started_at is None
        or reviewer_task.completed_at is None
        or reviewer_task.instance_id is None
        or reviewer_task.pty_background_generation is not None
    ):
        raise ValueError(
            "Reviewer Task has no complete, settled local execution generation"
        )
    generation_predicates = [
        LogEntry.task_id == reviewer_task.id,
        LogEntry.task_retry_count == reviewer_task.retry_count,
        LogEntry.instance_id == reviewer_task.instance_id,
        LogEntry.timestamp >= reviewer_task.started_at,
        LogEntry.timestamp <= reviewer_task.completed_at,
    ]
    tool_event = await db.scalar(
        select(LogEntry.id)
        .where(*generation_predicates, LogEntry.event_type == "tool_use")
        .limit(1)
    )
    if tool_event is not None:
        raise ValueError("Tool-free reviewer Task emitted a tool-use event")
    rows = list(
        (
            await db.execute(
                select(LogEntry)
                .where(
                    *generation_predicates,
                    LogEntry.is_error.is_(False),
                    or_(
                        LogEntry.event_type == "result",
                        and_(
                            LogEntry.event_type == "message",
                            LogEntry.role == "assistant",
                        ),
                    ),
                )
                .order_by(LogEntry.id)
            )
        ).scalars()
    )
    parsed_by_hash: dict[str, tuple[dict[str, object], list[LogEntry]]] = {}
    validation_errors: list[str] = []
    for row in rows:
        if not row.content:
            continue
        try:
            parsed = parse_structured_review_output(
                row.content,
                expected_subject=subject,
                surface="pre_pr",
                expected_role=CODE_REVIEWER_ROLE,
            )
        except ValueError as exc:
            validation_errors.append(str(exc))
            continue
        key = capability_value_hash(parsed)
        parsed_by_hash.setdefault(key, (parsed, []))[1].append(row)
    if not parsed_by_hash:
        message = "Reviewer Task produced no valid structured result for its subject"
        if validation_errors:
            message += f": {validation_errors[-1]}"
        raise ValueError(message)
    if len(parsed_by_hash) != 1:
        raise ValueError(
            "Reviewer Task produced multiple different structured results"
        )
    parsed, matching_rows = next(iter(parsed_by_hash.values()))
    # Prefer the provider's terminal result event over a mirrored assistant
    # message, then choose the last exact duplicate deterministically.
    output_row = max(
        matching_rows,
        key=lambda row: (row.event_type == "result", row.id),
    )
    return parsed, output_row


def _semantic_result_hash(
    *,
    run: CodeReviewRun,
    reviewer_task: Task,
    output_log: LogEntry,
    parsed: dict[str, object],
    verdict: str,
) -> str:
    return capability_value_hash(
        {
            "schema_version": 1,
            "kind": CODE_REVIEW_RESULT_OUTPUT_KIND,
            "run_id": run.id,
            "capability_invocation_id": run.capability_invocation_id,
            "capability_execution_id": run.capability_execution_id,
            "developer_task_id": run.developer_task_id,
            "reviewer_task_id": reviewer_task.id,
            "reviewer_task_retry_count": reviewer_task.retry_count,
            "reviewer_task_instance_id": reviewer_task.instance_id,
            "reviewer_task_started_at": reviewer_task.started_at.isoformat(),
            "reviewer_task_completed_at": reviewer_task.completed_at.isoformat(),
            "output_log_id": output_log.id,
            "subject": run.subject_ref,
            "role": parsed["role"],
            "verdict": verdict,
            "summary": parsed["summary"],
            "findings": parsed["findings"],
        }
    )


class CodeReviewCapabilityExecutor:
    """Drive an execution through one exact tool-free reviewer Task."""

    def __init__(self, *, wake_callback: WakeCallback | None = None) -> None:
        self._wake_callback = wake_callback

    async def _fail(
        self,
        db: AsyncSession,
        *,
        invocation: CapabilityInvocation,
        execution: CapabilityExecution,
        run: CodeReviewRun | None,
        reviewer_task: Task | None,
        error_code: str,
        error_message: str,
        retry: bool,
    ) -> CodeReviewCapabilityObservation:
        if run is not None and run.status == "running":
            _set_run_terminal(
                run,
                status="failed",
                error_code=error_code,
                error_message=error_message,
            )
        invocation, failed, replacement = await fail_execution(
            db,
            invocation_id=invocation.id,
            expected_invocation_version=invocation.state_version,
            expected_execution_version=execution.state_version,
            error_code=error_code,
            error_message=error_message,
            retry=retry,
        )
        return _observation(
            invocation,
            replacement or failed,
            run=run,
            reviewer_task=reviewer_task,
        )

    async def _stale(
        self,
        db: AsyncSession,
        *,
        invocation: CapabilityInvocation,
        execution: CapabilityExecution,
        run: CodeReviewRun | None,
        reviewer_task: Task | None,
        error_message: str,
    ) -> CodeReviewCapabilityObservation:
        if reviewer_task is not None and _reviewer_may_be_active(reviewer_task):
            run_id = run.id if run is not None else None
            reviewer_task_id = reviewer_task.id
            await _terminate_exact_reviewer_task(
                db,
                reviewer_task,
                reason="Code Review subject became stale",
            )
            # Authoritative Task termination deliberately rolls back/expires
            # this shared session while proving process ownership. Re-read the
            # whole capability aggregate before its next state-version CAS.
            invocation = await _load_invocation(db, invocation.id)
            execution = await active_execution_for(db, invocation.id)
            if execution is None:
                raise CapabilityConflictError(
                    "Stale Code Review lost its active execution"
                )
            if run_id is not None:
                run = await db.get(CodeReviewRun, run_id, populate_existing=True)
                if run is None:
                    raise CapabilityConflictError(
                        "Stale Code Review lost its durable run"
                    )
            reviewer_task = await db.get(
                Task, reviewer_task_id, populate_existing=True
            )
            if reviewer_task is None:
                raise CapabilityConflictError(
                    "Stale Code Review lost its reviewer Task evidence"
                )
        if run is not None:
            _set_run_terminal(
                run,
                status="stale",
                error_code="code_review_subject_stale",
                error_message=error_message,
            )
        invocation, execution = await mark_execution_stale(
            db,
            invocation_id=invocation.id,
            expected_invocation_version=invocation.state_version,
            expected_execution_version=execution.state_version,
            error_code="code_review_subject_stale",
            error_message=error_message,
        )
        return _observation(
            invocation,
            execution,
            run=run,
            reviewer_task=reviewer_task,
        )

    async def ensure_started(
        self,
        db: AsyncSession,
        *,
        invocation_id: int,
    ) -> CodeReviewCapabilityObservation:
        invocation = await _load_invocation(db, invocation_id)
        execution = await active_execution_for(db, invocation.id)
        if execution is None:
            execution = await _latest_execution(db, invocation.id)
            if execution is None:
                raise CapabilityConflictError(
                    "Code Review capability has no execution attempt"
                )

        if invocation.status != "queued" or execution.status != "queued":
            if execution.handle_id is None:
                if invocation.status in {
                    "ready",
                    "completed",
                    "failed",
                    "cancelled",
                    "stale",
                }:
                    return _observation(invocation, execution)
                raise CapabilityConflictError(
                    "Started Code Review capability lost its durable handle"
                )
            return await self.observe(db, invocation_id=invocation.id)

        existing_run = await _run_for_execution(db, execution.id)
        if existing_run is not None:
            invocation_id = invocation.id
            existing_run_id = existing_run.id

            async def claim_existing(
                stage_db: AsyncSession,
                _locked_task: Task,
                locked_invocation: CapabilityInvocation,
                locked_execution: CapabilityExecution,
            ) -> StagedCapabilityHandle[tuple[CodeReviewRun, Task]]:
                locked_run = await _run_for_execution(
                    stage_db, locked_execution.id
                )
                if locked_run is None:
                    raise CapabilityConflictError(
                        "Staged Code Review run disappeared during recovery"
                    )
                locked_reviewer = await stage_db.get(
                    Task,
                    locked_run.reviewer_task_id,
                    populate_existing=True,
                )
                if locked_reviewer is None:
                    raise CapabilityConflictError(
                        "Staged Code Review run lost its reviewer Task"
                    )
                _validate_run_task_identity(
                    locked_invocation,
                    locked_execution,
                    locked_run,
                    locked_reviewer,
                    require_claimed_handle=False,
                )
                return StagedCapabilityHandle(
                    handle_kind=CODE_REVIEW_RUN_HANDLE_KIND,
                    handle_id=str(locked_run.id),
                    handle_generation=locked_run.reviewer_task_retry_count,
                    value=(locked_run, locked_reviewer),
                )

            try:
                invocation, execution, staged = await stage_and_claim_execution(
                    db,
                    invocation_id=invocation.id,
                    expected_invocation_version=invocation.state_version,
                    expected_execution_version=execution.state_version,
                    stage=claim_existing,
                )
            except CapabilityConflictError as exc:
                await db.rollback()
                invocation = await _load_invocation(db, invocation_id)
                execution = await active_execution_for(db, invocation.id)
                if execution is None:
                    execution = await _latest_execution(db, invocation.id)
                if (
                    execution is not None
                    and execution.handle_id is not None
                    and invocation.status != "queued"
                ):
                    return await self.observe(db, invocation_id=invocation.id)
                if execution is None:
                    raise
                existing_run = await db.get(
                    CodeReviewRun,
                    existing_run_id,
                    populate_existing=True,
                )
                return await self._fail(
                    db,
                    invocation=invocation,
                    execution=execution,
                    run=existing_run,
                    reviewer_task=None,
                    error_code="code_review_handle_invalid",
                    error_message=str(exc),
                    retry=False,
                )
            existing_run, reviewer_task = staged
            return _observation(
                invocation,
                execution,
                run=existing_run,
                reviewer_task=reviewer_task,
            )

        developer_task = await db.get(Task, invocation.task_id)
        if developer_task is None:
            return await self._fail(
                db,
                invocation=invocation,
                execution=execution,
                run=None,
                reviewer_task=None,
                error_code="code_review_task_missing",
                error_message="Code Review developer Task not found",
                retry=False,
            )
        if developer_task.worker_id is not None:
            return await self._stale(
                db,
                invocation=invocation,
                execution=execution,
                run=None,
                reviewer_task=None,
                error_message=(
                    "Developer Task moved to a remote Worker before review capture"
                ),
            )
        if developer_task.shared_from_id is not None:
            return await self._stale(
                db,
                invocation=invocation,
                execution=execution,
                run=None,
                reviewer_task=None,
                error_message=(
                    "Developer Task became a shared shadow before review capture"
                ),
            )
        if developer_task.status == "migrating":
            return await self._stale(
                db,
                invocation=invocation,
                execution=execution,
                run=None,
                reviewer_task=None,
                error_message="Developer Task began migrating before review capture",
            )
        if not _developer_generation_matches(invocation, developer_task):
            return await self._stale(
                db,
                invocation=invocation,
                execution=execution,
                run=None,
                reviewer_task=None,
                error_message=(
                    "Developer Task generation changed before review capture"
                ),
            )

        try:
            base_sha, head_sha = _request_subject(invocation)
            repo_path = _task_subject_value(
                invocation, developer_task, "last_cwd"
            ) or _task_subject_value(invocation, developer_task, "target_repo")
            if not isinstance(repo_path, str) or not repo_path.strip():
                raise CapabilityValidationError(
                    "Code Review developer Task has no repository path"
                )
            captured = capture_commit_range_subject(
                repo_path,
                base_sha,
                expected_head_sha=head_sha,
            )
            retry_after_schema_failure = await _is_schema_validation_retry(
                db,
                execution,
            )
            prompt = build_structured_review_prompt(
                subject=captured.subject,
                material=captured.prompt_material(),
                guidance=captured.prompt_guidance(),
                surface="pre_pr",
                expected_role=CODE_REVIEWER_ROLE,
                retry_after_schema_failure=retry_after_schema_failure,
            )
            route = _review_route(invocation, developer_task)
        except (SubjectChangedError, RepositoryStateError) as exc:
            return await self._stale(
                db,
                invocation=invocation,
                execution=execution,
                run=None,
                reviewer_task=None,
                error_message=str(exc),
            )
        except (CodeReviewSubjectError, CapabilityValidationError, ValueError) as exc:
            return await self._fail(
                db,
                invocation=invocation,
                execution=execution,
                run=None,
                reviewer_task=None,
                error_code="code_review_subject_invalid",
                error_message=str(exc),
                retry=False,
            )

        subject_ref = captured.subject.as_dict()
        subject_hash = capability_value_hash(subject_ref)
        prompt_hash = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
        review_project_id = _task_subject_value(
            invocation, developer_task, "project_id"
        )
        review_target_branch = _task_subject_value(
            invocation, developer_task, "target_branch"
        )
        review_priority = _task_subject_value(
            invocation, developer_task, "priority"
        )
        if review_project_id is not None and type(review_project_id) is not int:
            return await self._fail(
                db,
                invocation=invocation,
                execution=execution,
                run=None,
                reviewer_task=None,
                error_code="code_review_subject_invalid",
                error_message="Code Review Task subject has an invalid project_id",
                retry=False,
            )
        if not isinstance(review_target_branch, str):
            return await self._fail(
                db,
                invocation=invocation,
                execution=execution,
                run=None,
                reviewer_task=None,
                error_code="code_review_subject_invalid",
                error_message=(
                    "Code Review Task subject has an invalid target_branch"
                ),
                retry=False,
            )
        if type(review_priority) is not int:
            return await self._fail(
                db,
                invocation=invocation,
                execution=execution,
                run=None,
                reviewer_task=None,
                error_code="code_review_subject_invalid",
                error_message="Code Review Task subject has an invalid priority",
                retry=False,
            )

        async def stage_review(
            stage_db: AsyncSession,
            locked_developer: Task,
            locked_invocation: CapabilityInvocation,
            locked_execution: CapabilityExecution,
        ) -> StagedCapabilityHandle[tuple[CodeReviewRun, Task]]:
            if (
                not _developer_generation_matches(
                    locked_invocation, locked_developer
                )
                or locked_developer.worker_id is not None
                or locked_developer.shared_from_id is not None
                or locked_developer.status == "migrating"
            ):
                raise _CodeReviewTransitionStale(
                    "Developer Task changed while review admission was locking"
                )
            reviewer = await stage_task_record(
                stage_db,
                title=(
                    f"Pre-PR Code Review: Task #{locked_developer.id} "
                    f"@ {captured.head_sha[:12]}"
                ),
                description=prompt,
                status="pending",
                priority=review_priority,
                project_id=review_project_id,
                target_repo="",
                target_branch=review_target_branch,
                mode="auto",
                max_retries=0,
                provider=route["provider"],
                model=route["model"],
                codex_service_tier=route["codex_service_tier"],
                effort_level=route["effort_level"],
                timeout_hours=route["timeout_hours"],
                enable_workflows=False,
                enabled_skills={},
                tags=[PRE_PR_CODE_REVIEW_TAG],
                metadata_={},
                created_by=(
                    locked_invocation.requested_by_user_id
                    or locked_developer.created_by
                ),
                worker_id=None,
                **system_task_execution_principal_values(),
            )
            staged_run = CodeReviewRun(
                capability_invocation_id=locked_invocation.id,
                capability_execution_id=locked_execution.id,
                attempt=locked_execution.attempt,
                status="running",
                state_version=1,
                developer_task_id=locked_developer.id,
                reviewer_task_id=reviewer.id,
                reviewer_task_retry_count=reviewer.retry_count,
                repo_path=captured.repo_path,
                base_sha=captured.base_sha,
                head_sha=captured.head_sha,
                head_tree_sha=captured.head_tree_sha,
                patch_sha256=captured.patch_sha256,
                subject_ref=subject_ref,
                subject_hash=subject_hash,
                prompt_hash=prompt_hash,
            )
            stage_db.add(staged_run)
            await stage_db.flush()
            reviewer.metadata_ = {
                "code_review_run_id": staged_run.id,
                "capability_invocation_id": locked_invocation.id,
                "capability_execution_id": locked_execution.id,
                "code_review_subject_hash": subject_hash,
                "code_review_base_sha": captured.base_sha,
                "code_review_head_sha": captured.head_sha,
            }
            return StagedCapabilityHandle(
                handle_kind=CODE_REVIEW_RUN_HANDLE_KIND,
                handle_id=str(staged_run.id),
                handle_generation=reviewer.retry_count,
                value=(staged_run, reviewer),
            )

        invocation_id = invocation.id
        try:
            invocation, execution, staged = await stage_and_claim_execution(
                db,
                invocation_id=invocation.id,
                expected_invocation_version=invocation.state_version,
                expected_execution_version=execution.state_version,
                stage=stage_review,
            )
            run, reviewer_task = staged
        except _CodeReviewTransitionStale as exc:
            invocation = await _load_invocation(db, invocation_id)
            execution = await active_execution_for(db, invocation.id)
            if execution is None:
                raise CapabilityConflictError(
                    "Stale Code Review admission lost its queued execution"
                ) from exc
            return await self._stale(
                db,
                invocation=invocation,
                execution=execution,
                run=None,
                reviewer_task=None,
                error_message=str(exc),
            )
        except ValueError as exc:
            invocation = await _load_invocation(db, invocation_id)
            execution = await active_execution_for(db, invocation.id)
            if execution is None:
                raise CapabilityConflictError(
                    "Invalid Code Review route lost its queued execution"
                ) from exc
            return await self._fail(
                db,
                invocation=invocation,
                execution=execution,
                run=None,
                reviewer_task=None,
                error_code="code_review_route_invalid",
                error_message=str(exc),
                retry=False,
            )
        except CapabilityConflictError:
            # Another observer may have admitted the exact same immutable
            # review while this caller waited on the aggregate lock. Re-read
            # the winner instead of surfacing a spurious failure.
            await db.rollback()
            invocation = await _load_invocation(db, invocation_id)
            execution = await active_execution_for(db, invocation.id)
            if execution is None:
                execution = await _latest_execution(db, invocation.id)
            if (
                execution is not None
                and execution.handle_id is not None
                and invocation.status != "queued"
            ):
                return await self.observe(db, invocation_id=invocation.id)
            raise
        except Exception:
            await db.rollback()
            raise

        await _maybe_call(self._wake_callback)
        return _observation(
            invocation,
            execution,
            run=run,
            reviewer_task=reviewer_task,
        )

    async def observe(
        self,
        db: AsyncSession,
        *,
        invocation_id: int,
    ) -> CodeReviewCapabilityObservation:
        invocation = await _load_invocation(db, invocation_id)
        execution = await active_execution_for(db, invocation.id)
        if execution is None:
            execution = await _latest_execution(db, invocation.id)
        if execution is None:
            raise CapabilityConflictError(
                "Code Review capability has no execution attempt"
            )

        if invocation.status in {"ready", "completed", "failed", "cancelled", "stale"}:
            run = None
            reviewer_task = None
            result = None
            if execution.handle_id is not None:
                run, reviewer_task = await _load_exact_run_task(
                    db, invocation, execution
                )
                result = await _result_for_run(db, run.id)
            if invocation.status == "ready" and run is not None:
                try:
                    verify_commit_range_subject(
                        run.repo_path,
                        _subject_from_run(run),
                    )
                except CodeReviewSubjectError as exc:
                    invocation_id = invocation.id
                    try:
                        invocation, execution = await mark_ready_invocation_stale(
                            db,
                            invocation_id=invocation_id,
                            expected_invocation_version=invocation.state_version,
                            expected_execution_version=execution.state_version,
                            error_code="code_review_subject_stale",
                            error_message=str(exc),
                        )
                    except CapabilityConflictError:
                        # A controller may consume the result while this
                        # observer verifies Git. Preserve that committed
                        # winner; otherwise surface the conflicting mutation.
                        await db.rollback()
                        current = await _load_invocation(db, invocation_id)
                        if current.status in {"completed", "stale"}:
                            return await self.observe(
                                db,
                                invocation_id=invocation_id,
                            )
                        raise
            return _observation(
                invocation,
                execution,
                run=run,
                reviewer_task=reviewer_task,
                result=result,
            )
        if invocation.status == "queued":
            return await self.ensure_started(db, invocation_id=invocation.id)
        if invocation.status == "cancelling":
            return await self.cancel(db, invocation_id=invocation.id)
        if invocation.status != "running" or execution.status != "running":
            raise CapabilityConflictError(
                "Code Review invocation and execution states are inconsistent"
            )

        try:
            run, reviewer_task = await _load_exact_run_task(
                db, invocation, execution
            )
            subject = _subject_from_run(run)
        except CapabilityConflictError as exc:
            return await self._fail(
                db,
                invocation=invocation,
                execution=execution,
                run=None,
                reviewer_task=None,
                error_code="code_review_handle_invalid",
                error_message=str(exc),
                retry=False,
            )

        try:
            verify_commit_range_subject(run.repo_path, subject)
        except (SubjectChangedError, RepositoryStateError) as exc:
            return await self._stale(
                db,
                invocation=invocation,
                execution=execution,
                run=run,
                reviewer_task=reviewer_task,
                error_message=str(exc),
            )
        except CodeReviewSubjectError as exc:
            return await self._fail(
                db,
                invocation=invocation,
                execution=execution,
                run=run,
                reviewer_task=reviewer_task,
                error_code="code_review_subject_invalid",
                error_message=str(exc),
                retry=False,
            )

        if reviewer_task.status in {"pending", "in_progress", "executing", "merging"}:
            return _observation(
                invocation,
                execution,
                run=run,
                reviewer_task=reviewer_task,
            )
        if reviewer_task.status in {"failed", "conflict"}:
            return await self._fail(
                db,
                invocation=invocation,
                execution=execution,
                run=run,
                reviewer_task=reviewer_task,
                error_code="code_review_task_failed",
                error_message=(
                    reviewer_task.error_message
                    or f"Reviewer Task ended as {reviewer_task.status}"
                ),
                retry=True,
            )
        if reviewer_task.status == "cancelled":
            return await self._finish_cancelled(
                db,
                invocation=invocation,
                execution=execution,
                run=run,
                reviewer_task=reviewer_task,
            )
        if reviewer_task.status != "completed":
            return await self._fail(
                db,
                invocation=invocation,
                execution=execution,
                run=run,
                reviewer_task=reviewer_task,
                error_code="code_review_task_invalid_state",
                error_message=(
                    f"Reviewer Task has unsupported status {reviewer_task.status!r}"
                ),
                retry=False,
            )

        try:
            # Minimize the Git-check-to-DB-commit window. The completion
            # validator below repeats every Task/run/log identity check while
            # holding the Capability aggregate locks.
            verify_commit_range_subject(run.repo_path, subject)
        except (SubjectChangedError, RepositoryStateError) as exc:
            return await self._stale(
                db,
                invocation=invocation,
                execution=execution,
                run=run,
                reviewer_task=reviewer_task,
                error_message=str(exc),
            )
        except CodeReviewSubjectError as exc:
            return await self._fail(
                db,
                invocation=invocation,
                execution=execution,
                run=run,
                reviewer_task=reviewer_task,
                error_code="code_review_output_invalid",
                error_message=str(exc),
                retry=True,
            )

        run_id = run.id
        reviewer_task_id = reviewer_task.id

        async def validate_output(
            validate_db: AsyncSession,
            locked_developer: Task,
            locked_invocation: CapabilityInvocation,
            locked_execution: CapabilityExecution,
        ) -> ValidatedCapabilityOutput[
            tuple[CodeReviewRun, Task, CodeReviewResult]
        ]:
            if not _developer_generation_matches(
                locked_invocation, locked_developer
            ):
                raise _CodeReviewTransitionStale(
                    "Developer Task generation changed before review completion"
                )
            locked_run = (
                await validate_db.execute(
                    select(CodeReviewRun)
                    .where(CodeReviewRun.id == run_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            locked_reviewer = (
                await validate_db.execute(
                    select(Task)
                    .where(Task.id == reviewer_task_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            if locked_run is None or locked_reviewer is None:
                raise ValueError(
                    "Code Review completion lost its exact run or reviewer Task"
                )
            try:
                _validate_run_task_identity(
                    locked_invocation,
                    locked_execution,
                    locked_run,
                    locked_reviewer,
                    require_claimed_handle=True,
                )
                locked_subject = _subject_from_run(locked_run)
            except CapabilityConflictError as exc:
                raise ValueError(str(exc)) from exc
            if locked_run.status != "running":
                raise ValueError(
                    f"Code Review run cannot complete from {locked_run.status!r}"
                )
            existing_result = await _result_for_run(validate_db, locked_run.id)
            if existing_result is not None:
                raise ValueError(
                    "Code Review execution already has a durable result"
                )
            parsed, output_log = await _exact_review_outputs(
                validate_db,
                reviewer_task=locked_reviewer,
                subject=locked_subject,
            )
            verdict = (
                "approved"
                if parsed["verdict"] == "pass"
                else "changes_requested"
            )
            result_hash = _semantic_result_hash(
                run=locked_run,
                reviewer_task=locked_reviewer,
                output_log=output_log,
                parsed=parsed,
                verdict=verdict,
            )
            validated_result = CodeReviewResult(
                run_id=locked_run.id,
                capability_invocation_id=locked_invocation.id,
                capability_execution_id=locked_execution.id,
                developer_task_id=locked_invocation.task_id,
                reviewer_task_id=locked_reviewer.id,
                reviewer_task_retry_count=locked_reviewer.retry_count,
                reviewer_task_instance_id=locked_reviewer.instance_id,
                reviewer_task_started_at=locked_reviewer.started_at,
                reviewer_task_completed_at=locked_reviewer.completed_at,
                output_log_id=output_log.id,
                schema_version=1,
                role=str(parsed["role"]),
                verdict=verdict,
                summary=str(parsed["summary"]),
                findings=list(parsed["findings"]),
                subject_ref=dict(parsed["subject"]),
                subject_hash=locked_run.subject_hash,
                result_hash=result_hash,
            )
            validate_db.add(validated_result)
            await validate_db.flush()
            _set_run_terminal(locked_run, status="completed")
            return ValidatedCapabilityOutput(
                output_kind=CODE_REVIEW_RESULT_OUTPUT_KIND,
                output_id=validated_result.id,
                output_hash=validated_result.result_hash,
                value=(locked_run, locked_reviewer, validated_result),
            )

        invocation_id = invocation.id
        try:
            invocation, execution, validated = (
                await validate_and_complete_execution(
                    db,
                    invocation_id=invocation.id,
                    expected_invocation_version=invocation.state_version,
                    expected_execution_version=execution.state_version,
                    validate=validate_output,
                )
            )
            run, reviewer_task, result = validated
        except _CodeReviewTransitionStale as exc:
            invocation = await _load_invocation(db, invocation_id)
            execution = await active_execution_for(db, invocation.id)
            if execution is None:
                return await self.observe(db, invocation_id=invocation.id)
            run, reviewer_task = await _load_exact_run_task(
                db, invocation, execution
            )
            return await self._stale(
                db,
                invocation=invocation,
                execution=execution,
                run=run,
                reviewer_task=reviewer_task,
                error_message=str(exc),
            )
        except ValueError as exc:
            invocation = await _load_invocation(db, invocation_id)
            execution = await active_execution_for(db, invocation.id)
            if execution is None:
                return await self.observe(db, invocation_id=invocation.id)
            try:
                run, reviewer_task = await _load_exact_run_task(
                    db, invocation, execution
                )
            except CapabilityConflictError as identity_exc:
                return await self._fail(
                    db,
                    invocation=invocation,
                    execution=execution,
                    run=None,
                    reviewer_task=None,
                    error_code="code_review_handle_invalid",
                    error_message=str(identity_exc),
                    retry=False,
                )
            return await self._fail(
                db,
                invocation=invocation,
                execution=execution,
                run=run,
                reviewer_task=reviewer_task,
                error_code="code_review_output_invalid",
                error_message=str(exc),
                retry=True,
            )
        except CapabilityConflictError:
            # Completion is idempotent under concurrent observers: once a
            # peer publishes the exact result, return that durable winner.
            await db.rollback()
            invocation = await _load_invocation(db, invocation_id)
            execution = await active_execution_for(db, invocation.id)
            if execution is None:
                execution = await _latest_execution(db, invocation.id)
            if execution is not None and invocation.status in {
                "ready",
                "completed",
                "failed",
                "cancelled",
                "stale",
            }:
                return await self.observe(db, invocation_id=invocation.id)
            raise
        return _observation(
            invocation,
            execution,
            run=run,
            reviewer_task=reviewer_task,
            result=result,
        )

    async def recover(
        self,
        db: AsyncSession,
        *,
        invocation_id: int,
    ) -> CodeReviewCapabilityObservation:
        invocation = await _load_invocation(db, invocation_id)
        if invocation.status == "queued":
            return await self.ensure_started(db, invocation_id=invocation.id)
        return await self.observe(db, invocation_id=invocation.id)

    async def _finish_cancelled(
        self,
        db: AsyncSession,
        *,
        invocation: CapabilityInvocation,
        execution: CapabilityExecution,
        run: CodeReviewRun,
        reviewer_task: Task,
    ) -> CodeReviewCapabilityObservation:
        if invocation.status != "cancelling":
            invocation = await cancel_invocation(
                db,
                invocation_id=invocation.id,
                expected_state_version=invocation.state_version,
                allow_workflow_owned=True,
            )
            execution = await active_execution_for(db, invocation.id)
            if execution is None:
                latest = await _latest_execution(db, invocation.id)
                if latest is None:
                    raise CapabilityConflictError(
                        "Cancelled Code Review lost its execution"
                    )
                return _observation(
                    invocation,
                    latest,
                    run=run,
                    reviewer_task=reviewer_task,
                )
        _set_run_terminal(run, status="cancelled")
        invocation, execution = await mark_execution_cancelled(
            db,
            invocation_id=invocation.id,
            expected_invocation_version=invocation.state_version,
            expected_execution_version=execution.state_version,
        )
        return _observation(
            invocation,
            execution,
            run=run,
            reviewer_task=reviewer_task,
        )

    async def cancel(
        self,
        db: AsyncSession,
        *,
        invocation_id: int,
    ) -> CodeReviewCapabilityObservation:
        invocation = await _load_invocation(db, invocation_id)
        execution = await active_execution_for(db, invocation.id)
        if execution is None:
            execution = await _latest_execution(db, invocation.id)
        if execution is None:
            raise CapabilityConflictError(
                "Code Review capability has no execution attempt"
            )
        if invocation.status in {"cancelled", "failed", "completed", "stale"}:
            run = None
            reviewer_task = None
            result = None
            if execution.handle_id is not None:
                run, reviewer_task = await _load_exact_run_task(
                    db, invocation, execution
                )
                result = await _result_for_run(db, run.id)
            return _observation(
                invocation,
                execution,
                run=run,
                reviewer_task=reviewer_task,
                result=result,
            )
        if invocation.status == "ready":
            invocation = await cancel_invocation(
                db,
                invocation_id=invocation.id,
                expected_state_version=invocation.state_version,
                allow_workflow_owned=True,
            )
            return _observation(invocation, execution)
        if invocation.status == "queued":
            invocation = await cancel_invocation(
                db,
                invocation_id=invocation.id,
                expected_state_version=invocation.state_version,
                allow_workflow_owned=True,
            )
            return _observation(invocation, execution)

        run, reviewer_task = await _load_exact_run_task(db, invocation, execution)
        if invocation.status != "cancelling":
            invocation = await cancel_invocation(
                db,
                invocation_id=invocation.id,
                expected_state_version=invocation.state_version,
                allow_workflow_owned=True,
            )
            execution = await active_execution_for(db, invocation.id)
            if execution is None:
                raise CapabilityConflictError(
                    "Cancelling Code Review lost its active execution"
                )
        if _reviewer_may_be_active(reviewer_task):
            run_id = run.id
            reviewer_task_id = reviewer_task.id
            await _terminate_exact_reviewer_task(
                db,
                reviewer_task,
                reason="Code Review capability was cancelled",
            )
            invocation = await _load_invocation(db, invocation.id)
            execution = await active_execution_for(db, invocation.id)
            if execution is None:
                raise CapabilityConflictError(
                    "Cancelling Code Review lost its active execution"
                )
            run = await db.get(CodeReviewRun, run_id, populate_existing=True)
            if run is None:
                raise CapabilityConflictError(
                    "Cancelling Code Review lost its durable run"
                )
            reviewer_task = await db.get(
                Task, reviewer_task_id, populate_existing=True
            )
            if reviewer_task is None or _reviewer_may_be_active(reviewer_task):
                raise CodeReviewCancellationUnconfirmed(
                    "Reviewer Task remained active after authoritative stop"
                )
        return await self._finish_cancelled(
            db,
            invocation=invocation,
            execution=execution,
            run=run,
            reviewer_task=reviewer_task,
        )
