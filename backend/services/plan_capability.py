"""Capability adapter for the first-class Planner/Reviewer pipeline.

The adapter deliberately contains no HTTP or global-dispatcher imports.  A
controller can inject a dispatcher wake callback and the exact Plan Run stop
callback when it wires this adapter into its lifecycle.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
import inspect

from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.models.capability import CapabilityExecution, CapabilityInvocation
from backend.models.instance import Instance
from backend.models.plan import Plan, PlanVersion
from backend.models.plan_agent import PlanAgentRun, PlanAgentStep
from backend.models.task import Task
from backend.schemas.plan import resolve_plan_pipeline_config
from backend.services.capability_registry import CapabilityDefinition
from backend.services.capability_service import (
    CapabilityConflictError,
    CapabilityNotFoundError,
    CapabilityUnsupportedScopeError,
    CapabilityUnavailableError,
    CapabilityValidationError,
    StagedCapabilityHandle,
    ValidatedCapabilityOutput,
    active_execution_for,
    capability_value_hash,
    cancel_invocation,
    claim_execution,
    fail_execution,
    mark_execution_cancelled,
    mark_execution_waiting,
    resume_waiting_execution,
    stage_and_claim_execution,
    validate_and_complete_execution,
)
from backend.services.plan_agent_runner import (
    active_plan_run_ids,
    cancel_plan_run_runtime,
)
from backend.services.plan_events import broadcast_plan_event
from backend.services.plan_input_safety import contains_high_confidence_secret
from backend.services.plan_pipeline_settings import effective_plan_pipeline_config
from backend.services.plan_service import (
    ACTIVE_RUN_STATUSES,
    PLAN_RUN_HANDLE_GENERATION,
    fence_capability_run_cancellation,
    finalize_capability_run_cancellation,
    plan_operation_lock,
    release_capability_run_owner_after_cleanup,
    stage_plan_with_run,
)
from backend.services.plan_tasks import (
    capture_repo_revision,
    capture_task_context,
    latest_task_log_id,
)


PLAN_CAPABILITY_KEY = "plan"
PLAN_EXECUTOR_KIND = "plan_agent"
PLAN_RUN_HANDLE_KIND = "plan_agent_run"
# The handle generation identifies the staged adapter handle, not a mutable
# provider turn.  Every first-class Plan Run is staged at generation zero;
# subsequent PlanAgentRun generations are independent dispatcher owner fences.
# Keeping this value immutable avoids an Execution -> Run / Run -> Execution
# lock inversion between Capability cancellation and Plan dispatch.
PLAN_VERSION_OUTPUT_KIND = "plan_version"

WakeCallback = Callable[[], Awaitable[None] | None]
PlanRunStopCallback = Callable[[int, int | None], Awaitable[bool]]


class PlanCapabilityCancellationUnconfirmed(CapabilityConflictError):
    """The Plan Run may still have a live lifecycle or process owner."""


class PlanCapabilityResultInvalid(CapabilityConflictError):
    """A completed Plan pipeline failed its exact identity proof."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class PlanCapabilityObservation:
    invocation_id: int
    execution_id: int
    status: str
    run_status: str | None
    plan_id: int | None
    run_id: int | None
    run_generation: int | None
    input_request_id: int | None = None
    output_version_id: int | None = None
    output_hash: str | None = None
    error_code: str | None = None
    error_message: str | None = None


def plan_capability_definition(
    *,
    executor: "PlanCapabilityExecutor | None" = None,
    pipeline_config: dict | None = None,
    max_attempts: int = 2,
) -> CapabilityDefinition:
    """Build the registry entry without mutating the process-global registry."""

    return CapabilityDefinition(
        capability_key=PLAN_CAPABILITY_KEY,
        executor_kind=PLAN_EXECUTOR_KIND,
        executor_config={"pipeline_config": pipeline_config},
        policy_snapshot={"local_only": True, "review_required": True},
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


def _map_plan_http_error(exc: HTTPException) -> Exception:
    """Keep FastAPI transport exceptions out of the Capability executor."""

    detail = str(exc.detail)
    if exc.status_code == 404:
        return CapabilityNotFoundError(detail)
    if exc.status_code in {400, 422}:
        return CapabilityValidationError(detail)
    if exc.status_code in {409, 429}:
        return CapabilityConflictError(detail)
    if exc.status_code >= 500:
        return CapabilityUnavailableError(detail)
    return CapabilityValidationError(detail)


def _task_generation_matches(
    invocation: CapabilityInvocation,
    task: Task,
) -> bool:
    """Validate the immutable request generation, allowing future fields."""

    subject = invocation.subject_ref
    if not isinstance(subject, dict):
        return False
    expected = {
        "task_id": task.id,
        "retry_count": task.retry_count,
        "instance_id": task.instance_id,
        "started_at": task.started_at.isoformat() if task.started_at else None,
        "session_id": task.session_id,
    }
    if invocation.source == "agent_request":
        expected.update(
            incarnation_id=task.incarnation_id,
            turn_generation=task.turn_generation,
        )
    return (
        invocation.subject_kind == "task_generation"
        and all(subject.get(key) == value for key, value in expected.items())
        and invocation.subject_hash == capability_value_hash(subject)
        and invocation.request_task_retry_count == task.retry_count
        and invocation.request_task_instance_id == task.instance_id
        and invocation.request_task_started_at == task.started_at
        and invocation.request_task_session_id == task.session_id
        and (
            invocation.source != "agent_request"
            or (
                invocation.request_task_incarnation_id == task.incarnation_id
                and invocation.request_task_turn_generation
                == task.turn_generation
            )
        )
    )


def _subject_value(
    invocation: CapabilityInvocation,
    key: str,
    fallback,
):
    subject = invocation.subject_ref
    return subject.get(key, fallback) if isinstance(subject, dict) else fallback


def _parse_handle(execution: CapabilityExecution) -> int:
    if execution.handle_kind != PLAN_RUN_HANDLE_KIND or execution.handle_id is None:
        raise CapabilityConflictError(
            "Plan capability execution has no exact PlanAgentRun handle"
        )
    try:
        run_id = int(execution.handle_id)
    except (TypeError, ValueError) as exc:
        raise CapabilityConflictError("PlanAgentRun handle is invalid") from exc
    if run_id <= 0 or str(run_id) != execution.handle_id:
        raise CapabilityConflictError("PlanAgentRun handle is invalid")
    return run_id


async def _load_invocation(
    db: AsyncSession,
    invocation_id: int,
) -> CapabilityInvocation:
    invocation = await db.get(
        CapabilityInvocation,
        invocation_id,
        populate_existing=True,
    )
    if invocation is None:
        raise CapabilityNotFoundError("Capability invocation not found")
    if invocation.capability_key != PLAN_CAPABILITY_KEY:
        raise CapabilityValidationError("Invocation is not a Plan capability")
    if invocation.executor_kind != PLAN_EXECUTOR_KIND:
        raise CapabilityValidationError("Invocation does not use the Plan executor")
    return invocation


async def _load_exact_run(
    db: AsyncSession,
    invocation: CapabilityInvocation,
    execution: CapabilityExecution,
) -> tuple[PlanAgentRun, Plan]:
    run_id = _parse_handle(execution)
    run = await db.get(PlanAgentRun, run_id)
    if run is None or run.plan_id is None:
        raise CapabilityConflictError("PlanAgentRun handle no longer exists")
    plan = await db.get(Plan, run.plan_id)
    if plan is None:
        raise CapabilityConflictError("PlanAgentRun lost its owning Plan")
    if (
        plan.target_task_id != invocation.task_id
        or run.plan_id != plan.id
        or run.run_type != "capability"
        or run.capability_execution_id != execution.id
        or execution.handle_generation != PLAN_RUN_HANDLE_GENERATION
    ):
        raise CapabilityConflictError(
            "PlanAgentRun handle does not belong to this capability execution"
        )
    if run.worker_id is not None or plan.worker_id is not None:
        raise CapabilityUnsupportedScopeError(
            "Plan capabilities do not support remote Worker runs"
        )
    return run, plan


async def _terminal_run_has_complete_absence_proof(
    db: AsyncSession,
    *,
    run: PlanAgentRun,
    plan: Plan,
) -> bool:
    """Prove a concurrently terminal Run needs no destructive cancellation.

    A provider lifecycle publishes ``completed``/``failed`` only after it has
    released both owner directions.  Cancellation may win the Capability CAS
    immediately before that terminal Plan transaction wins.  In that race the
    upper cancellation may converge, but it must neither rewrite the terminal
    Plan nor signal a provider from incomplete evidence.
    """

    if (
        run.status not in {"completed", "failed"}
        or run.plan_id != plan.id
        or plan.active_run_id is not None
        or run.instance_id is not None
        or run.last_execution_started_at is not None
        or run.id in active_plan_run_ids()
    ):
        return False
    reverse_owner = await db.scalar(
        select(Instance.id)
        .where(Instance.current_plan_run_id == run.id)
        .with_for_update()
        .limit(1)
    )
    if reverse_owner is not None:
        return False
    from backend.services.plan_runtime_receipt import runtime_run_is_clean

    return await runtime_run_is_clean(db, run_id=run.id)


def plan_version_output_hash(version: PlanVersion) -> str:
    """Hash the immutable planning/review result, not mutable human decisions."""

    return capability_value_hash(
        {
            "schema_version": 1,
            "kind": PLAN_VERSION_OUTPUT_KIND,
            "id": version.id,
            "plan_id": version.plan_id,
            "version_number": version.version_number,
            "parent_version_id": version.parent_version_id,
            "produced_by_run_id": version.produced_by_run_id,
            "produced_by_step_id": version.produced_by_step_id,
            "content": version.content,
            "context_session_id": version.context_session_id,
            "context_log_id": version.context_log_id,
            "repo_revision": version.repo_revision,
            "reviewer_repo_revision": version.reviewer_repo_revision,
            "review_verdict": version.review_verdict,
            "review_feedback": version.review_feedback,
            "reviewed_by_step_id": version.reviewed_by_step_id,
            "review_exhausted": version.review_exhausted,
            "reviewed_at": (
                version.reviewed_at.isoformat() if version.reviewed_at else None
            ),
        }
    )


async def _validate_completed_plan_output(
    db: AsyncSession,
    task: Task,
    invocation: CapabilityInvocation,
    execution: CapabilityExecution,
) -> ValidatedCapabilityOutput[tuple[PlanAgentRun, Plan, PlanVersion]]:
    """Lock and prove the exact Planner + Reviewer result identities."""

    run_id = _parse_handle(execution)
    run = (
        await db.execute(
            select(PlanAgentRun)
            .where(PlanAgentRun.id == run_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if run is None or run.plan_id is None:
        raise PlanCapabilityResultInvalid(
            "plan_handle_invalid",
            "PlanAgentRun handle no longer exists",
        )
    plan = (
        await db.execute(
            select(Plan)
            .where(Plan.id == run.plan_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    version = None
    if run.result_version_id is not None:
        version = (
            await db.execute(
                select(PlanVersion)
                .where(PlanVersion.id == run.result_version_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()

    if (
        plan is None
        or version is None
        or not _task_generation_matches(invocation, task)
        or plan.target_task_id != invocation.task_id
        or plan.worker_id is not None
        or plan.archived_at is not None
        or run.plan_id != plan.id
        or run.worker_id is not None
        or run.run_type != "capability"
        or run.capability_execution_id != execution.id
        or execution.handle_generation != PLAN_RUN_HANDLE_GENERATION
        or run.status != "completed"
        or run.current_stage != "complete"
        or run.result_version_id != version.id
        or run.instance_id is not None
        or plan.active_run_id is not None
        or plan.current_version_id != version.id
        or version.plan_id != plan.id
        or version.produced_by_run_id != run.id
        or version.superseded_by_version_id is not None
        or not bool(version.content and version.content.strip())
    ):
        raise PlanCapabilityResultInvalid(
            "plan_result_invalid",
            "Completed Plan Run has no exact active immutable PlanVersion",
        )

    if (
        run.draft_step_id is None
        or version.produced_by_step_id is None
        or version.reviewed_by_step_id is None
    ):
        raise PlanCapabilityResultInvalid(
            "plan_result_invalid",
            "Completed Plan Run lost its Planner or Reviewer Step identity",
        )
    step_ids = {
        run.draft_step_id,
        version.produced_by_step_id,
        version.reviewed_by_step_id,
    }
    steps = list(
        (
            await db.execute(
                select(PlanAgentStep)
                .where(PlanAgentStep.id.in_(step_ids))
                .order_by(PlanAgentStep.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalars()
    )
    by_id = {step.id: step for step in steps}
    planner = by_id.get(version.produced_by_step_id)
    reviewer = by_id.get(version.reviewed_by_step_id)
    planner_valid = (
        planner is not None
        and run.draft_step_id == planner.id
        and planner.run_id == run.id
        and planner.plan_id == plan.id
        and planner.step_type == "planner"
        and planner.status == "completed"
        and planner.round == run.round
        # Planner and reviewer are separate dispatcher claims.  The exact
        # draft Step may precede the final reviewer by more than one
        # generation when a cleaned reviewer claim is recovered and retried.
        and planner.generation >= 1
        and planner.plan_version_id == version.id
    )
    reviewer_valid = (
        reviewer is not None
        and reviewer.run_id == run.id
        and reviewer.plan_id == plan.id
        and reviewer.step_type == "reviewer"
        and reviewer.status == "completed"
        and reviewer.round == run.round
        and reviewer.generation == run.generation
        and planner is not None
        and planner.generation < reviewer.generation
    )
    if not planner_valid or not reviewer_valid:
        raise PlanCapabilityResultInvalid(
            "plan_result_invalid",
            "PlanVersion is not backed by the exact completed Planner and Reviewer Steps",
        )

    owner_id = await db.scalar(
        select(Instance.id)
        .where(Instance.current_plan_run_id == run.id)
        .with_for_update()
        .limit(1)
    )
    if owner_id is not None:
        raise PlanCapabilityResultInvalid(
            "plan_result_invalid",
            f"Completed Plan Run #{run.id} still owns Instance #{owner_id}",
        )

    if (
        run.review_exhausted
        or version.review_exhausted
        or run.review_verdict != "approve"
        or version.review_verdict != "approve"
        or version.reviewed_at is None
    ):
        raise PlanCapabilityResultInvalid(
            "plan_review_not_approved",
            (
                "Plan review did not approve the exact result "
                f"(run={run.review_verdict!r}, version={version.review_verdict!r}, "
                f"exhausted={run.review_exhausted or version.review_exhausted})"
            ),
        )

    return ValidatedCapabilityOutput(
        output_kind=PLAN_VERSION_OUTPUT_KIND,
        output_id=version.id,
        output_hash=plan_version_output_hash(version),
        value=(run, plan, version),
    )


def _observation(
    invocation: CapabilityInvocation,
    execution: CapabilityExecution,
    *,
    run: PlanAgentRun | None = None,
) -> PlanCapabilityObservation:
    return PlanCapabilityObservation(
        invocation_id=invocation.id,
        execution_id=execution.id,
        status=invocation.status,
        run_status=run.status if run is not None else None,
        plan_id=run.plan_id if run is not None else None,
        run_id=run.id if run is not None else None,
        run_generation=run.generation if run is not None else None,
        input_request_id=(run.open_input_request_id if run is not None else None),
        output_version_id=execution.output_id,
        output_hash=execution.output_hash,
        error_code=execution.error_code or invocation.error_code,
        error_message=execution.error_message or invocation.error_message,
    )


def _request_text(invocation: CapabilityInvocation, task: Task) -> tuple[str, str]:
    payload = invocation.input_payload
    if not isinstance(payload, dict):
        raise CapabilityValidationError("Plan capability input must be an object")
    task_description = _subject_value(
        invocation,
        "description",
        task.description,
    )
    task_title = _subject_value(invocation, "title", task.title)
    prompt = payload.get("prompt", payload.get("request", task_description))
    if not isinstance(prompt, str) or not prompt.strip():
        raise CapabilityValidationError("Plan capability requires a non-empty prompt")
    raw_title = payload.get("title")
    if raw_title is not None and not isinstance(raw_title, str):
        raise CapabilityValidationError("Plan capability title must be a string")
    title = (raw_title or f"Plan for #{task.id}: {task_title}").strip()
    if not title:
        title = f"Plan for Task #{task.id}"
    if contains_high_confidence_secret([prompt, title]):
        raise CapabilityValidationError(
            "Plan text cannot store API keys or access tokens"
        )
    return prompt.strip(), title[:200]


class PlanCapabilityExecutor:
    """Drive one CapabilityExecution through an exact first-class Plan Run."""

    def __init__(
        self,
        *,
        wake_callback: WakeCallback | None = None,
        stop_callback: PlanRunStopCallback | None = None,
    ) -> None:
        self._wake_callback = wake_callback
        self._stop_callback = stop_callback

    async def ensure_started(
        self,
        db: AsyncSession,
        *,
        invocation_id: int,
    ) -> PlanCapabilityObservation:
        invocation = await _load_invocation(db, invocation_id)
        execution = await active_execution_for(db, invocation.id)
        if execution is None:
            execution = await _latest_execution(db, invocation.id)
            if execution is None:
                raise CapabilityConflictError(
                    "Plan capability has no execution attempt"
                )

        if invocation.status != "queued" or execution.status != "queued":
            if execution.handle_id is None:
                if invocation.status in {"ready", "completed", "failed", "cancelled"}:
                    return _observation(invocation, execution)
                raise CapabilityConflictError(
                    "Started Plan capability lost its durable handle"
                )
            return await self.observe(db, invocation_id=invocation.id)

        # A queued row with an existing handle can arise only from explicit
        # recovery data. Re-claim that exact handle; never create another Plan.
        if execution.handle_id is not None or execution.handle_kind is not None:
            try:
                run, _plan = await _load_exact_run(db, invocation, execution)
                invocation, execution = await claim_execution(
                    db,
                    invocation_id=invocation.id,
                    expected_invocation_version=invocation.state_version,
                    expected_execution_version=execution.state_version,
                    handle_kind=PLAN_RUN_HANDLE_KIND,
                    handle_id=str(run.id),
                    handle_generation=PLAN_RUN_HANDLE_GENERATION,
                )
            except (
                CapabilityConflictError,
                CapabilityUnsupportedScopeError,
            ) as exc:
                await db.rollback()
                invocation = await _load_invocation(db, invocation.id)
                execution = await active_execution_for(db, invocation.id)
                if execution is None:
                    raise CapabilityConflictError(
                        "Invalid queued Plan handle lost its execution"
                    ) from exc
                return await self._fail_observation(
                    db,
                    invocation=invocation,
                    execution=execution,
                    run=None,
                    error_code="plan_handle_invalid",
                    error_message=str(exc),
                    retry=False,
                )
            return _observation(invocation, execution, run=run)

        task = await db.get(Task, invocation.task_id, populate_existing=True)
        if task is None:
            raise CapabilityNotFoundError("Capability Task not found")
        if task.worker_id is not None:
            raise CapabilityUnsupportedScopeError(
                "Plan capabilities cannot run on a remote Worker Task"
            )
        if task.shared_from_id is not None:
            raise CapabilityUnsupportedScopeError(
                "Plan capabilities cannot run on a shared shadow Task"
            )
        if task.status == "migrating":
            raise CapabilityUnsupportedScopeError(
                "Plan capabilities cannot run while a Task is migrating"
            )
        if not _task_generation_matches(invocation, task):
            return await self._fail_observation(
                db,
                invocation=invocation,
                execution=execution,
                run=None,
                error_code="plan_subject_stale",
                error_message=(
                    "Capability Task generation changed before Plan staging"
                ),
                retry=False,
            )

        prompt, title = _request_text(invocation, task)
        try:
            base_pipeline = await effective_plan_pipeline_config(db)
            configured_pipeline = (invocation.executor_config or {}).get(
                "pipeline_config"
            )
            pipeline = resolve_plan_pipeline_config(
                configured_pipeline,
                base_config=base_pipeline,
            )
        except ValidationError as exc:
            raise CapabilityValidationError(
                "Plan capability pipeline configuration is invalid"
            ) from exc
        if not pipeline.reviewer.enabled:
            raise CapabilityValidationError(
                "Plan capability requires the reviewer stage to be enabled"
            )

        context_log_id = invocation.request_source_log_id
        if context_log_id is None:
            # Compatibility for invocations created before request-boundary
            # capture was introduced. New invocations always persist the
            # boundary in Capability Core.
            context_log_id = await latest_task_log_id(db, task.id)
        context_snapshot = await capture_task_context(
            db,
            task.id,
            through_log_id=context_log_id,
            max_chars=settings.plan_transcript_max_chars,
        )
        target_repo = _subject_value(
            invocation,
            "last_cwd",
            task.last_cwd,
        ) or _subject_value(
            invocation,
            "target_repo",
            task.target_repo,
        )
        target_branch = _subject_value(
            invocation,
            "target_branch",
            task.target_branch,
        )
        project_id = _subject_value(
            invocation,
            "project_id",
            task.project_id,
        )
        priority = _subject_value(invocation, "priority", task.priority)
        timeout_hours = _subject_value(
            invocation,
            "timeout_hours",
            task.timeout_hours,
        )
        context_session_id = invocation.request_task_session_id
        repo_revision = await capture_repo_revision(target_repo)

        pipeline_payload = pipeline.model_dump(mode="json")

        async def stage_plan(
            stage_db: AsyncSession,
            locked_task: Task,
            locked_invocation: CapabilityInvocation,
            locked_execution: CapabilityExecution,
        ) -> StagedCapabilityHandle[tuple[Plan, PlanAgentRun]]:
            if not _task_generation_matches(locked_invocation, locked_task):
                raise CapabilityConflictError(
                    "Capability Task generation changed during Plan staging"
                )
            existing_run = (
                await stage_db.execute(
                    select(PlanAgentRun)
                    .where(
                        PlanAgentRun.capability_execution_id
                        == locked_execution.id
                    )
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            if existing_run is not None:
                existing_plan = (
                    await stage_db.execute(
                        select(Plan)
                        .where(Plan.id == existing_run.plan_id)
                        .with_for_update()
                        .execution_options(populate_existing=True)
                    )
                ).scalar_one_or_none()
                if (
                    existing_plan is None
                    or existing_run.run_type != "capability"
                    or existing_run.plan_id != existing_plan.id
                    or existing_plan.target_task_id != locked_invocation.task_id
                ):
                    raise CapabilityConflictError(
                        "Orphaned Plan Run reverse handle is invalid"
                    )
                plan, run = existing_plan, existing_run
            else:
                try:
                    plan, run = await stage_plan_with_run(
                        stage_db,
                        title=title,
                        initial_request=prompt,
                        attachments=None,
                        target_task_id=locked_task.id,
                        project_id=project_id,
                        target_repo=target_repo,
                        target_branch=target_branch,
                        worker_id=None,
                        priority=priority,
                        timeout_hours=timeout_hours,
                        created_by=locked_invocation.requested_by_user_id,
                        pipeline_config=pipeline_payload,
                        context_session_id=context_session_id,
                        context_log_id=context_log_id,
                        context_snapshot=context_snapshot,
                        repo_revision=repo_revision,
                        run_type="capability",
                        capability_execution_id=locked_execution.id,
                    )
                except HTTPException as exc:
                    raise _map_plan_http_error(exc) from exc
            return StagedCapabilityHandle(
                handle_kind=PLAN_RUN_HANDLE_KIND,
                handle_id=str(run.id),
                handle_generation=PLAN_RUN_HANDLE_GENERATION,
                value=(plan, run),
            )

        try:
            invocation, execution, staged = await stage_and_claim_execution(
                db,
                invocation_id=invocation.id,
                expected_invocation_version=invocation.state_version,
                expected_execution_version=execution.state_version,
                stage=stage_plan,
            )
            plan, run = staged
        except CapabilityConflictError:
            await db.rollback()
            current = await _load_invocation(db, invocation_id)
            current_execution = await active_execution_for(db, current.id)
            if (
                current.status != "queued"
                or current_execution is None
                or current_execution.handle_id is not None
            ):
                return await self.observe(db, invocation_id=current.id)
            raise

        await broadcast_plan_event(
            event="plan_created",
            plan_id=plan.id,
            target_task_id=plan.target_task_id,
            run_id=run.id,
            source="capability",
            capability_invocation_id=invocation.id,
        )
        await _maybe_call(self._wake_callback)
        return _observation(invocation, execution, run=run)

    async def _fail_observation(
        self,
        db: AsyncSession,
        *,
        invocation: CapabilityInvocation,
        execution: CapabilityExecution,
        run: PlanAgentRun | None,
        error_code: str,
        error_message: str,
        retry: bool,
    ) -> PlanCapabilityObservation:
        invocation, failed, replacement = await fail_execution(
            db,
            invocation_id=invocation.id,
            expected_invocation_version=invocation.state_version,
            expected_execution_version=execution.state_version,
            error_code=error_code,
            error_message=error_message,
            retry=retry,
        )
        selected = replacement or failed
        return _observation(invocation, selected, run=run)

    async def observe(
        self,
        db: AsyncSession,
        *,
        invocation_id: int,
    ) -> PlanCapabilityObservation:
        invocation = await _load_invocation(db, invocation_id)
        execution = await active_execution_for(db, invocation.id)
        if execution is None:
            execution = await _latest_execution(db, invocation.id)
        if execution is None:
            raise CapabilityConflictError("Plan capability has no execution attempt")

        if invocation.status in {"ready", "completed", "failed", "cancelled", "stale"}:
            run = None
            if execution.handle_id is not None:
                run, _plan = await _load_exact_run(db, invocation, execution)
            return _observation(invocation, execution, run=run)
        if invocation.status == "queued":
            return await self.ensure_started(db, invocation_id=invocation.id)
        if invocation.status == "cancelling":
            return await self.cancel(db, invocation_id=invocation.id)

        try:
            run, plan = await _load_exact_run(db, invocation, execution)
        except (CapabilityConflictError, CapabilityUnsupportedScopeError) as exc:
            return await self._fail_observation(
                db,
                invocation=invocation,
                execution=execution,
                run=None,
                error_code="plan_handle_invalid",
                error_message=str(exc),
                retry=False,
            )

        if run.status in {"queued", "running"}:
            if invocation.status == "waiting_user":
                invocation, execution = await resume_waiting_execution(
                    db,
                    invocation_id=invocation.id,
                    expected_invocation_version=invocation.state_version,
                    expected_execution_version=execution.state_version,
                )
            elif invocation.status != "running":
                raise CapabilityConflictError(
                    "Plan execution and capability state are inconsistent"
                )
            return _observation(invocation, execution, run=run)

        if run.status == "waiting_user":
            if invocation.status == "running":
                invocation, execution = await mark_execution_waiting(
                    db,
                    invocation_id=invocation.id,
                    expected_invocation_version=invocation.state_version,
                    expected_execution_version=execution.state_version,
                )
            elif invocation.status != "waiting_user":
                raise CapabilityConflictError(
                    "Plan input wait and capability state are inconsistent"
                )
            return _observation(invocation, execution, run=run)

        if run.status == "failed":
            return await self._fail_observation(
                db,
                invocation=invocation,
                execution=execution,
                run=run,
                error_code="plan_run_failed",
                error_message=run.error or "Plan Run failed without an error",
                retry=True,
            )

        if run.status == "cancelled":
            return await self.cancel(db, invocation_id=invocation.id)

        if run.status != "completed":
            return await self._fail_observation(
                db,
                invocation=invocation,
                execution=execution,
                run=run,
                error_code="plan_run_invalid_state",
                error_message=f"Plan Run has unsupported status {run.status!r}",
                retry=False,
            )

        observed_run_id = run.id
        observed_invocation_id = invocation.id
        try:
            invocation, execution, validated = (
                await validate_and_complete_execution(
                    db,
                    invocation_id=invocation.id,
                    expected_invocation_version=invocation.state_version,
                    expected_execution_version=execution.state_version,
                    validate=_validate_completed_plan_output,
                )
            )
            run, _plan, _version = validated
        except PlanCapabilityResultInvalid as exc:
            await db.rollback()
            invocation = await _load_invocation(db, observed_invocation_id)
            execution = await active_execution_for(db, invocation.id)
            if execution is None:
                raise CapabilityConflictError(
                    "Invalid Plan result lost its active execution"
                ) from exc
            run = await db.get(
                PlanAgentRun,
                observed_run_id,
                populate_existing=True,
            )
            return await self._fail_observation(
                db,
                invocation=invocation,
                execution=execution,
                run=run,
                error_code=exc.code,
                error_message=str(exc),
                retry=False,
            )
        return _observation(invocation, execution, run=run)

    async def recover(
        self,
        db: AsyncSession,
        *,
        invocation_id: int,
    ) -> PlanCapabilityObservation:
        """Idempotently resume from only the durable Invocation/Run records."""

        invocation = await _load_invocation(db, invocation_id)
        if invocation.status == "queued":
            return await self.ensure_started(db, invocation_id=invocation.id)
        return await self.observe(db, invocation_id=invocation.id)

    async def cancel(
        self,
        db: AsyncSession,
        *,
        invocation_id: int,
        stop_callback: PlanRunStopCallback | None = None,
    ) -> PlanCapabilityObservation:
        invocation = await _load_invocation(db, invocation_id)
        execution = await active_execution_for(db, invocation.id)
        if execution is None:
            execution = await _latest_execution(db, invocation.id)
        if execution is None:
            raise CapabilityConflictError("Plan capability has no execution attempt")

        if invocation.status in {"cancelled", "failed", "completed", "stale"}:
            run = None
            if execution.handle_id is not None:
                run, _plan = await _load_exact_run(db, invocation, execution)
            return _observation(invocation, execution, run=run)

        # Validate every persisted handle component before the Capability Core
        # cancellation CAS.  Otherwise a corrupted immutable generation could
        # move Invocation/Execution to ``cancelling`` and only then fail Plan
        # ownership validation, leaving an unrecoverable half-transition.
        has_handle_evidence = any(
            value is not None
            for value in (
                execution.handle_kind,
                execution.handle_id,
                execution.handle_generation,
            )
        )
        if has_handle_evidence:
            await _load_exact_run(db, invocation, execution)
        elif invocation.status != "queued":
            raise CapabilityConflictError(
                "Started Plan capability lost its durable handle"
            )

        # Recovery may leave the atomically staged handle on queued Core rows.
        # Claim that same immutable handle before requesting cancellation so
        # Capability Core cannot synchronously publish cancelled while the
        # staged Plan still occupies its active-run slot.
        if invocation.status == "queued" and has_handle_evidence:
            invocation, execution = await claim_execution(
                db,
                invocation_id=invocation.id,
                expected_invocation_version=invocation.state_version,
                expected_execution_version=execution.state_version,
                handle_kind=PLAN_RUN_HANDLE_KIND,
                handle_id=execution.handle_id,
                handle_generation=PLAN_RUN_HANDLE_GENERATION,
            )
            await _load_exact_run(db, invocation, execution)

        if invocation.status != "cancelling":
            invocation = await cancel_invocation(
                db,
                invocation_id=invocation.id,
                expected_state_version=invocation.state_version,
                allow_workflow_owned=True,
            )
            if invocation.status == "cancelled":
                return _observation(invocation, execution)
            execution = await active_execution_for(db, invocation.id)
            if execution is None:
                raise CapabilityConflictError(
                    "Cancelling Plan capability lost its active execution"
                )

        run, plan = await _load_exact_run(db, invocation, execution)
        run_id = run.id
        plan_id = plan.id
        execution_id = execution.id
        terminal_plan_outcome = False

        # Fence the Plan generation before touching its process. The durable
        # ``cancelling`` state retains the Instance id for safe retries but is
        # not claimable by the Plan dispatcher or answer endpoint.
        await db.commit()
        async with plan_operation_lock(plan_id):
            run = await db.get(
                PlanAgentRun,
                run_id,
                with_for_update=True,
                populate_existing=True,
            )
            plan = await db.get(
                Plan,
                plan_id,
                with_for_update=True,
                populate_existing=True,
            )
            if (
                run is None
                or plan is None
                or run.plan_id != plan.id
                or run.run_type != "capability"
                or run.capability_execution_id != execution_id
            ):
                await db.rollback()
                raise PlanCapabilityCancellationUnconfirmed(
                    "Plan Run disappeared while cancellation was in progress"
                )
            runtime_was_maybe_active = (
                run.status in {"running", "cancelling"}
                or run.instance_id is not None
                or run.last_execution_started_at is not None
            )
            if run.status in ACTIVE_RUN_STATUSES or run.status == "cancelling":
                try:
                    run = await fence_capability_run_cancellation(
                        db,
                        plan=plan,
                        run=run,
                    )
                except HTTPException as exc:
                    raise PlanCapabilityCancellationUnconfirmed(
                        str(exc.detail)
                    ) from exc
            elif run.status in {"completed", "failed"}:
                terminal_plan_outcome = True
                if not await _terminal_run_has_complete_absence_proof(
                    db,
                    run=run,
                    plan=plan,
                ):
                    await db.rollback()
                    raise PlanCapabilityCancellationUnconfirmed(
                        "Terminal Plan Run runtime evidence is incomplete"
                    )
                await db.commit()
            elif run.status != "cancelled":
                await db.rollback()
                raise PlanCapabilityCancellationUnconfirmed(
                    f"Plan Run stopped in unknown status {run.status!r}"
                )
            else:
                await db.commit()

            # The fence refreshes the Run after its atomic UPDATE.  A local
            # dispatcher claim may have won after the SELECT above, so stop
            # decisions must use this exact post-fence owner/generation rather
            # than the stale pre-fence ORM snapshot.
            owned_instance_id = run.instance_id
            runtime_may_be_active = (
                runtime_was_maybe_active
                or run.instance_id is not None
                or run.last_execution_started_at is not None
            )

        selected_stopper = stop_callback or self._stop_callback
        if (
            not terminal_plan_outcome
            and (runtime_may_be_active or owned_instance_id is not None)
        ):
            if selected_stopper is None:
                raise PlanCapabilityCancellationUnconfirmed(
                    "An active Plan Run requires an injected dispatcher stop callback"
                )
            stopped = await selected_stopper(run_id, owned_instance_id)
            if not stopped:
                raise PlanCapabilityCancellationUnconfirmed(
                    f"Plan Run #{run_id} dispatcher stop was not confirmed"
                )

        # Sweep any provider process retained outside the dispatcher lifecycle,
        # then prove both the runtime registry and every Instance reverse owner.
        if not terminal_plan_outcome:
            await cancel_plan_run_runtime(run_id)
            if run_id in active_plan_run_ids():
                raise PlanCapabilityCancellationUnconfirmed(
                    f"Plan Run #{run_id} runtime cleanup is not confirmed"
                )

            await db.commit()
            async with plan_operation_lock(plan_id):
                run = await db.get(
                    PlanAgentRun,
                    run_id,
                    with_for_update=True,
                    populate_existing=True,
                )
                plan = await db.get(
                    Plan,
                    plan_id,
                    with_for_update=True,
                    populate_existing=True,
                )
                if run is None or plan is None or run.plan_id != plan.id:
                    await db.rollback()
                    raise PlanCapabilityCancellationUnconfirmed(
                        "Plan Run disappeared before cancellation finalized"
                    )
                try:
                    run = await release_capability_run_owner_after_cleanup(
                        db,
                        plan=plan,
                        run=run,
                    )
                    run = await finalize_capability_run_cancellation(
                        db,
                        plan=plan,
                        run=run,
                    )
                except HTTPException as exc:
                    raise PlanCapabilityCancellationUnconfirmed(
                        str(exc.detail)
                    ) from exc

        remaining_owner = await db.scalar(
            select(Instance.id)
            .where(Instance.current_plan_run_id == run_id)
            .limit(1)
        )
        run = await db.get(
            PlanAgentRun,
            run_id,
            populate_existing=True,
        )
        if (
            remaining_owner is not None
            or run is None
            or run.status not in {"completed", "failed", "cancelled"}
            or run.instance_id is not None
            or run.last_execution_started_at is not None
            or run_id in active_plan_run_ids()
        ):
            await db.rollback()
            raise PlanCapabilityCancellationUnconfirmed(
                f"Plan Run #{run_id} cancellation evidence is incomplete"
            )
        plan = await db.get(Plan, plan_id, populate_existing=True)
        if plan is None:
            await db.rollback()
            raise PlanCapabilityCancellationUnconfirmed(
                "Plan disappeared while cancellation was being confirmed"
            )
        if terminal_plan_outcome and not await _terminal_run_has_complete_absence_proof(
            db,
            run=run,
            plan=plan,
        ):
            await db.rollback()
            raise PlanCapabilityCancellationUnconfirmed(
                "Terminal Plan Run runtime evidence changed before publication"
            )

        # Re-read after Plan cancellation commits so the capability CAS uses
        # the exact current versions and only publishes cancellation once all
        # lower-level runtime evidence is gone.
        await db.commit()
        invocation = await _load_invocation(db, invocation_id)
        execution = await active_execution_for(db, invocation.id)
        if execution is None:
            latest = await _latest_execution(db, invocation.id)
            if latest is None:
                raise CapabilityConflictError(
                    "Cancelling Plan capability lost its execution"
                )
            return _observation(invocation, latest, run=run)
        invocation, execution = await mark_execution_cancelled(
            db,
            invocation_id=invocation.id,
            expected_invocation_version=invocation.state_version,
            expected_execution_version=execution.state_version,
        )
        run = await db.get(PlanAgentRun, run_id, populate_existing=True)
        plan = await db.get(Plan, plan_id, populate_existing=True)
        if run is None or plan is None:
            raise PlanCapabilityCancellationUnconfirmed(
                "Plan cancellation audit rows disappeared"
            )
        await broadcast_plan_event(
            event="plan_run_status_changed",
            plan_id=plan.id,
            target_task_id=plan.target_task_id,
            run_id=run.id,
            status=run.status,
            source="capability",
            capability_invocation_id=invocation.id,
        )
        return _observation(invocation, execution, run=run)
