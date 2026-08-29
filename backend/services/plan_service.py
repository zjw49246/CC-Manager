"""Transactional aggregate operations for first-class versioned Plans."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from functools import wraps
import json
from typing import Iterable

from fastapi import HTTPException
from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.plan import (
    Plan,
    PlanApplication,
    PlanApplicationAttempt,
    PlanApplicationReceipt,
    PlanInputRequest,
    PlanLegacyTaskLink,
    PlanVersion,
)
from backend.models.plan_agent import PlanAgentRun, PlanAgentStep
from backend.models.log_entry import LogEntry
from backend.models.task import Task
from backend.services.task_creation import stage_task_record
from backend.services.plan_tasks import MAX_ACTIVE_PLANS_PER_TASK
from backend.schemas.plan_resource import (
    PlanApplicationAttemptResource,
    PlanApplicationResource,
    PlanInputAnswer,
    PlanInputRequestResponse,
    PlanQuestion,
    PlanResource,
    PlanRunResource,
    PlanStepResource,
    PlanVersionResource,
)


ACTIVE_RUN_STATUSES = frozenset({"queued", "running", "waiting_user"})
TERMINAL_RUN_STATUSES = frozenset({"completed", "failed", "cancelled"})
_plan_locks: defaultdict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
_target_plan_locks: defaultdict[int, asyncio.Lock] = defaultdict(asyncio.Lock)


@dataclass(frozen=True)
class PlanExecutionTaskResult:
    """Exact, idempotent result of applying a Plan Version as a new Task."""

    plan: Plan
    version: PlanVersion
    application: PlanApplication
    task: Task
    created: bool


def plan_operation_lock(plan_id: int) -> asyncio.Lock:
    return _plan_locks[plan_id]


async def _remove_receipt_applications(
    db: AsyncSession,
    receipt: PlanApplicationReceipt,
    *,
    delivery_status: str,
    error: str,
) -> list[int]:
    applications = list(
        (
            await db.execute(
                select(PlanApplication).where(
                    PlanApplication.application_receipt_key == receipt.receipt_key
                )
            )
        ).scalars()
    )
    plan_ids = list(dict.fromkeys(item.plan_id for item in applications))
    existing_attempt_versions = set(
        (
            await db.execute(
                select(PlanApplicationAttempt.plan_version_id).where(
                    PlanApplicationAttempt.application_receipt_key
                    == receipt.receipt_key
                )
            )
        ).scalars()
    )
    released_at = datetime.utcnow()
    for application in applications:
        if application.plan_version_id in existing_attempt_versions:
            continue
        db.add(
            PlanApplicationAttempt(
                plan_id=application.plan_id,
                plan_version_id=application.plan_version_id,
                application_receipt_key=receipt.receipt_key,
                application_type=application.application_type,
                target_task_id=application.target_task_id,
                target_session_id=application.target_session_id,
                user_log_id=application.user_log_id,
                execution_task_id=application.execution_task_id,
                applied_by=application.applied_by,
                application_created_at=application.created_at,
                released_at=released_at,
            )
        )
    # Persist the immutable attempt before deleting the active application.
    # The receipt row lock serializes resolution; the unique key remains the
    # final cross-process idempotency fence.
    await db.flush()
    await db.execute(
        delete(PlanApplication).where(
            PlanApplication.application_receipt_key == receipt.receipt_key
        )
    )
    log = await db.get(LogEntry, receipt.manager_user_log_id)
    if log is not None:
        try:
            metadata = json.loads(log.raw_json or "{}")
        except (TypeError, json.JSONDecodeError):
            metadata = {}
        if not isinstance(metadata, dict):
            metadata = {}
        metadata.pop("applied_plans", None)
        metadata["plan_delivery"] = {
            "status": delivery_status,
            "error": error[:2000],
        }
        log.raw_json = json.dumps(metadata)
    return plan_ids


async def release_unstarted_plan_application(
    db: AsyncSession,
    *,
    receipt_key: str,
    delivery_status: str,
    error: str,
    expected_worker_id: int | None = None,
) -> tuple[list[int], int | None] | None:
    """Release a Version application only after prelaunch is proven.

    The caller owns that proof. ``launching``/``uncertain`` receipts must never
    enter here because an external turn may already exist.
    """

    query = (
        select(PlanApplicationReceipt)
        .where(
            PlanApplicationReceipt.receipt_key == receipt_key,
            PlanApplicationReceipt.delivery_status.in_(["pending", "queued"]),
        )
        .with_for_update()
    )
    if expected_worker_id is not None:
        query = query.where(PlanApplicationReceipt.worker_id == expected_worker_id)
    receipt = (await db.execute(query)).scalar_one_or_none()
    if receipt is None:
        return None

    plan_ids = await _remove_receipt_applications(
        db,
        receipt,
        delivery_status=delivery_status,
        error=error,
    )
    receipt.delivery_status = delivery_status
    receipt.delivery_error = error[:2000]
    receipt.updated_at = datetime.utcnow()
    return plan_ids, receipt.target_task_id


async def release_unstarted_plan_applications_for_task(
    db: AsyncSession,
    *,
    target_task_id: int,
    delivery_status: str,
    error: str,
) -> list[tuple[str, list[int], int | None]]:
    """Cancel every durable local outbox row not yet past queue admission."""

    keys = list(
        (
            await db.execute(
                select(PlanApplicationReceipt.receipt_key)
                .where(
                    PlanApplicationReceipt.target_task_id == target_task_id,
                    PlanApplicationReceipt.outbox_payload.isnot(None),
                    PlanApplicationReceipt.delivery_status.in_(["pending", "queued"]),
                )
                .with_for_update()
            )
        ).scalars()
    )
    released: list[tuple[str, list[int], int | None]] = []
    for receipt_key in keys:
        result = await release_unstarted_plan_application(
            db,
            receipt_key=receipt_key,
            delivery_status=delivery_status,
            error=error,
        )
        if result is not None:
            plan_ids, task_id = result
            released.append((receipt_key, plan_ids, task_id))
    return released


async def preserve_uncertain_plan_application(
    db: AsyncSession,
    *,
    receipt: PlanApplicationReceipt,
    error: str,
    launch_evidence: dict | None,
    response: dict | None = None,
    applied_by: int | None = None,
) -> list[int]:
    """Conservatively consume every Version while a Worker launch is ambiguous."""

    version_ids = list(dict.fromkeys(receipt.plan_version_ids or []))
    versions = list(
        (
            await db.execute(select(PlanVersion).where(PlanVersion.id.in_(version_ids)))
        ).scalars()
    )
    versions_by_id = {version.id: version for version in versions}
    if set(versions_by_id) != set(version_ids):
        raise HTTPException(
            409,
            "Plan delivery receipt references a missing Version",
        )
    plans = list(
        (
            await db.execute(
                select(Plan).where(
                    Plan.id.in_({version.plan_id for version in versions})
                )
            )
        ).scalars()
    )
    plans_by_id = {plan.id: plan for plan in plans}
    target = await db.get(Task, receipt.target_task_id)
    existing = {
        application.plan_version_id: application
        for application in (
            await db.execute(
                select(PlanApplication).where(
                    PlanApplication.plan_version_id.in_(version_ids)
                )
            )
        ).scalars()
    }
    approved: list[tuple[Plan, PlanVersion]] = []
    for version_id in version_ids:
        version = versions_by_id[version_id]
        plan = plans_by_id.get(version.plan_id)
        if plan is None:
            raise HTTPException(409, "Plan delivery receipt lost its Plan")
        approved.append((plan, version))
        application = existing.get(version_id)
        if application is not None:
            if application.application_receipt_key != receipt.receipt_key:
                raise HTTPException(
                    409,
                    "Plan Version has a different application receipt",
                )
            continue
        db.add(
            PlanApplication(
                plan_id=plan.id,
                plan_version_id=version.id,
                application_type="chat_message",
                target_task_id=receipt.target_task_id,
                target_session_id=target.session_id if target is not None else None,
                user_log_id=receipt.manager_user_log_id,
                applied_by=applied_by,
                application_receipt_key=receipt.receipt_key,
            )
        )

    log = await db.get(LogEntry, receipt.manager_user_log_id)
    if log is not None:
        try:
            metadata = json.loads(log.raw_json or "{}")
        except (TypeError, json.JSONDecodeError):
            metadata = {}
        if not isinstance(metadata, dict):
            metadata = {}
        metadata["applied_plans"] = versioned_plan_snapshots(approved)
        metadata["plan_delivery"] = {
            "status": "uncertain",
            "error": error[:2000],
        }
        log.raw_json = json.dumps(metadata)

    receipt.status = "committed" if response is not None else receipt.status
    if response is not None:
        receipt.response = response
    receipt.delivery_status = "uncertain"
    receipt.delivery_error = error[:2000]
    if isinstance(launch_evidence, dict):
        receipt.launch_evidence = launch_evidence
    receipt.updated_at = datetime.utcnow()
    await db.flush()
    return list(dict.fromkeys(plan.id for plan, _version in approved))


async def resolve_uncertain_plan_application(
    db: AsyncSession,
    *,
    receipt_key: str,
    action: str,
    note: str,
    actor_id: int | None,
) -> tuple[list[int], int | None]:
    """Resolve one ambiguous launch after an administrator checks evidence."""

    if action not in {"confirm_launched", "release_for_retry"}:
        raise HTTPException(422, "Unknown Plan delivery resolution action")
    receipt = (
        await db.execute(
            select(PlanApplicationReceipt)
            .where(PlanApplicationReceipt.receipt_key == receipt_key)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if receipt is None:
        raise HTTPException(404, "Plan application receipt not found")
    prior_resolution = receipt.delivery_resolution
    if isinstance(prior_resolution, dict) and prior_resolution.get("action") == action:
        if actor_id is not None and prior_resolution.get("resolved_by") is None:
            enriched_resolution = dict(prior_resolution)
            enriched_resolution["resolved_by"] = actor_id
            enriched_resolution["note"] = note[:2000]
            enriched_resolution["manager_confirmed_at"] = datetime.utcnow().isoformat()
            receipt.delivery_resolution = enriched_resolution
            receipt.updated_at = datetime.utcnow()
        plan_ids = list(
            dict.fromkeys(
                (
                    await db.execute(
                        select(PlanVersion.plan_id).where(
                            PlanVersion.id.in_(receipt.plan_version_ids or [])
                        )
                    )
                ).scalars()
            )
        )
        return plan_ids, receipt.target_task_id
    if receipt.delivery_status != "uncertain":
        raise HTTPException(
            409,
            f"Plan delivery is {receipt.delivery_status}, not uncertain",
        )

    now = datetime.utcnow()
    resolution = {
        "action": action,
        "note": note[:2000],
        "resolved_by": actor_id,
        "resolved_at": now.isoformat(),
        "previous_status": "uncertain",
    }
    if action == "confirm_launched":
        plan_ids = list(
            dict.fromkeys(
                (
                    await db.execute(
                        select(PlanVersion.plan_id).where(
                            PlanVersion.id.in_(receipt.plan_version_ids or [])
                        )
                    )
                ).scalars()
            )
        )
        receipt.delivery_status = "launched"
        receipt.delivery_error = None
    else:
        plan_ids = await _remove_receipt_applications(
            db,
            receipt,
            delivery_status="cancelled",
            error=(
                "Administrator confirmed that the ambiguous delivery did not launch: "
                f"{note}"
            ),
        )
        if not plan_ids:
            plan_ids = list(
                dict.fromkeys(
                    (
                        await db.execute(
                            select(PlanVersion.plan_id).where(
                                PlanVersion.id.in_(receipt.plan_version_ids or [])
                            )
                        )
                    ).scalars()
                )
            )
        receipt.delivery_status = "cancelled"
        receipt.delivery_error = (
            "Administrator confirmed that the ambiguous delivery did not launch"
        )
    receipt.delivery_resolution = resolution
    receipt.updated_at = now
    return plan_ids, receipt.target_task_id


def _serialize_related_plan_creation(function):
    """Keep the target fence through COUNT, INSERT, and commit in-process."""

    @wraps(function)
    async def wrapped(*args, **kwargs):
        target_task_id = kwargs.get("target_task_id")
        if target_task_id is None:
            return await function(*args, **kwargs)
        async with _target_plan_locks[target_task_id]:
            return await function(*args, **kwargs)

    return wrapped


async def _fence_target_task(
    db: AsyncSession,
    *,
    target_task_id: int | None,
    expected_worker_id: int | None,
) -> None:
    """Serialize a new active Run against an exact Task migration claim."""

    if target_task_id is None:
        return
    worker_predicate = (
        Task.worker_id.is_(None)
        if expected_worker_id is None
        else Task.worker_id == expected_worker_id
    )
    fenced = await db.execute(
        update(Task)
        .where(
            Task.id == target_task_id,
            Task.status != "migrating",
            worker_predicate,
        )
        # A matched-row UPDATE takes the same database write lock used by the
        # migration claim without changing user-visible Task state.
        .values(status=Task.status)
    )
    if fenced.rowcount != 1:
        await db.rollback()
        raise HTTPException(409, "Plan target is changing execution location")


def _public_attachments(items: list[dict] | None) -> list[dict] | None:
    if not items:
        return None
    return [
        {key: item[key] for key in ("url", "name", "is_image") if key in item}
        for item in items
        if isinstance(item, dict)
    ] or None


def input_request_resource(
    input_request: PlanInputRequest,
) -> PlanInputRequestResponse:
    return PlanInputRequestResponse.model_validate(input_request).model_copy(
        update={"attachments": _public_attachments(input_request.attachments)}
    )


@_serialize_related_plan_creation
async def create_plan_with_run(
    db: AsyncSession,
    *,
    title: str,
    initial_request: str,
    attachments: list[dict] | None,
    target_task_id: int | None,
    project_id: int | None,
    target_repo: str | None,
    target_branch: str | None,
    worker_id: int | None,
    priority: int,
    timeout_hours: float | None,
    created_by: int | None,
    pipeline_config: dict,
    context_session_id: str | None,
    context_log_id: int | None,
    context_snapshot: str | None,
    repo_revision: dict | None,
    forked_from_version_id: int | None = None,
    base_version_id: int | None = None,
    run_type: str = "initial",
) -> tuple[Plan, PlanAgentRun]:
    now = datetime.utcnow()
    await _fence_target_task(
        db,
        target_task_id=target_task_id,
        expected_worker_id=worker_id,
    )
    if target_task_id is not None:
        # The target Task write fence above serializes this COUNT -> INSERT
        # boundary across processes and all supported databases. Both ordinary
        # creation and Fork enter through this service boundary.
        active_count = int(
            await db.scalar(
                select(func.count(Plan.id)).where(
                    Plan.target_task_id == target_task_id,
                    Plan.archived_at.is_(None),
                    Plan.active_run_id.isnot(None),
                )
            )
            or 0
        )
        if active_count >= MAX_ACTIVE_PLANS_PER_TASK:
            await db.rollback()
            raise HTTPException(
                429,
                f"Task already has {MAX_ACTIVE_PLANS_PER_TASK} active Plans",
            )
    plan = Plan(
        title=title[:200],
        initial_request=initial_request,
        initial_attachments=attachments or None,
        target_task_id=target_task_id,
        project_id=project_id,
        target_repo=target_repo,
        target_branch=target_branch,
        worker_id=worker_id,
        priority=priority,
        timeout_hours=timeout_hours,
        created_by=created_by,
        pipeline_config=pipeline_config,
        forked_from_version_id=forked_from_version_id,
        created_at=now,
        updated_at=now,
    )
    db.add(plan)
    await db.flush()
    run = PlanAgentRun(
        plan_id=plan.id,
        plan_task_id=None,
        run_type=run_type,
        status="queued",
        current_stage="planner",
        base_version_id=base_version_id,
        request_text=initial_request,
        attachments=attachments or None,
        context_session_id=context_session_id,
        context_log_id=context_log_id,
        context_snapshot=context_snapshot,
        repo_revision=repo_revision,
        worker_id=worker_id,
        pipeline_config=pipeline_config,
        round=1,
        generation=0,
        max_interactions=pipeline_config.get("max_interactions", 3),
        updated_at=now,
    )
    db.add(run)
    await db.flush()
    plan.active_run_id = run.id
    await db.commit()
    await db.refresh(plan)
    await db.refresh(run)
    return plan, run


async def create_plan_run(
    db: AsyncSession,
    *,
    plan: Plan,
    run_type: str,
    request_text: str,
    attachments: list[dict] | None,
    base_version_id: int | None,
    expected_current_version_id: int | None,
    context_session_id: str | None,
    context_log_id: int | None,
    context_snapshot: str | None,
    repo_revision: dict | None,
    source_run_id: int | None = None,
) -> PlanAgentRun:
    """Create one Run under the Plan's durable active-run fence."""

    if plan.archived_at is not None:
        raise HTTPException(409, "Archived Plan cannot start a Run")
    if plan.active_run_id is not None:
        raise HTTPException(409, f"Plan already has active Run #{plan.active_run_id}")
    if expected_current_version_id != plan.current_version_id:
        raise HTTPException(409, "Plan current Version changed")
    if base_version_id is not None:
        base = await db.get(PlanVersion, base_version_id)
        if base is None or base.plan_id != plan.id:
            raise HTTPException(400, "Base Version does not belong to this Plan")

    await _fence_target_task(
        db,
        target_task_id=plan.target_task_id,
        expected_worker_id=plan.worker_id,
    )

    now = datetime.utcnow()
    run = PlanAgentRun(
        plan_id=plan.id,
        plan_task_id=None,
        run_type=run_type,
        source_run_id=source_run_id,
        status="queued",
        current_stage="planner",
        base_version_id=base_version_id,
        request_text=request_text,
        attachments=attachments or None,
        context_session_id=context_session_id,
        context_log_id=context_log_id,
        context_snapshot=context_snapshot,
        repo_revision=repo_revision,
        worker_id=plan.worker_id,
        pipeline_config=plan.pipeline_config,
        round=1,
        generation=0,
        max_interactions=dict(plan.pipeline_config).get("max_interactions", 3),
        updated_at=now,
    )
    db.add(run)
    await db.flush()
    claimed = await db.execute(
        update(Plan)
        .where(
            Plan.id == plan.id,
            Plan.active_run_id.is_(None),
            (
                Plan.current_version_id.is_(None)
                if expected_current_version_id is None
                else Plan.current_version_id == expected_current_version_id
            ),
            Plan.lock_version == plan.lock_version,
        )
        .values(
            active_run_id=run.id,
            lock_version=Plan.lock_version + 1,
            updated_at=now,
        )
    )
    if claimed.rowcount != 1:
        await db.rollback()
        raise HTTPException(409, "Plan changed while creating the Run")
    await db.commit()
    await db.refresh(run)
    return run


async def complete_plan_run_with_version(
    db: AsyncSession,
    *,
    plan: Plan,
    run: PlanAgentRun,
    planner_step: PlanAgentStep,
    content: str,
    repo_revision: dict | None,
    reviewer_step_id: int | None,
    verdict: str,
    feedback: str,
    exhausted: bool,
    reviewer_repo_revision: dict | None,
    completed_at: datetime,
) -> PlanVersion:
    """Atomically publish one completed pipeline candidate as a Version."""

    existing = (
        await db.execute(
            select(PlanVersion).where(
                PlanVersion.produced_by_step_id == planner_step.id
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        if (
            existing.plan_id != plan.id
            or existing.produced_by_run_id != run.id
            or existing.content != content
        ):
            raise RuntimeError("Planner Step Version identity changed")
        version = existing
    else:
        next_number = (
            int(
                await db.scalar(
                    select(
                        func.coalesce(func.max(PlanVersion.version_number), 0)
                    ).where(PlanVersion.plan_id == plan.id)
                )
                or 0
            )
            + 1
        )
        version = PlanVersion(
            plan_id=plan.id,
            version_number=next_number,
            parent_version_id=plan.current_version_id,
            produced_by_run_id=run.id,
            produced_by_step_id=planner_step.id,
            content=content,
            context_session_id=run.context_session_id,
            context_log_id=run.context_log_id,
            context_snapshot=run.context_snapshot,
            repo_revision=repo_revision,
            human_decision="pending",
        )
        db.add(version)
        await db.flush()
    previous_id = version.parent_version_id
    if previous_id is not None and previous_id != version.id:
        await db.execute(
            update(PlanVersion)
            .where(
                PlanVersion.id == previous_id,
                PlanVersion.plan_id == plan.id,
                PlanVersion.superseded_by_version_id.is_(None),
            )
            .values(superseded_by_version_id=version.id)
        )

    version.review_verdict = "exhausted" if exhausted else verdict
    version.review_feedback = feedback
    version.reviewed_by_step_id = reviewer_step_id
    version.review_exhausted = exhausted
    version.reviewed_at = completed_at
    version.reviewer_repo_revision = reviewer_repo_revision
    planner_step.plan_version_id = version.id
    run.result_version_id = version.id
    run.status = "completed"
    run.current_stage = "complete"
    run.review_verdict = verdict
    run.review_feedback = feedback
    run.review_exhausted = exhausted
    run.finished_at = completed_at
    run.updated_at = completed_at
    plan.current_version_id = version.id
    plan.active_run_id = None
    plan.lock_version += 1
    plan.updated_at = completed_at
    await db.commit()
    await db.refresh(version)
    return version


def _answer_map(answers: Iterable[PlanInputAnswer | dict]) -> dict[str, object]:
    result: dict[str, object] = {}
    for answer in answers:
        item = answer.model_dump() if isinstance(answer, PlanInputAnswer) else answer
        question_id = item.get("question_id")
        if not isinstance(question_id, str) or question_id in result:
            raise HTTPException(422, "Answers must use unique valid question_id values")
        result[question_id] = item.get("value")
    return result


def validate_input_answers(
    questions: list[dict],
    answers: Iterable[PlanInputAnswer | dict],
    *,
    response_text: str | None = None,
) -> list[dict]:
    """Validate all questions without imposing a question-count limit.

    A non-empty free-form response is an intentional escape hatch when none
    of a required choice question's model-generated options fit. The resumed
    Planner receives both the null structured answer and this response and can
    ask a narrower follow-up if the explanation is still insufficient.
    """

    parsed = [PlanQuestion.model_validate(question) for question in questions]
    by_id = {question.id: question for question in parsed}
    values = _answer_map(answers)
    has_free_form_answer = bool(response_text and response_text.strip())
    unknown = set(values) - set(by_id)
    if unknown:
        raise HTTPException(422, f"Unknown question ids: {sorted(unknown)}")
    normalized: list[dict] = []
    for question in parsed:
        value = values.get(question.id)
        if (
            question.required
            and (value is None or value == "" or value == [])
            and not has_free_form_answer
        ):
            raise HTTPException(
                422,
                f"Question {question.id!r} requires an answer or a non-empty "
                "additional response",
            )
        if value is None:
            normalized.append({"question_id": question.id, "value": None})
            continue
        if question.response_type == "text":
            if not isinstance(value, str) or len(value) > 50_000:
                raise HTTPException(422, f"Question {question.id!r} requires text")
        elif question.response_type == "single_choice":
            allowed = {option.value for option in question.options}
            if not isinstance(value, str) or value not in allowed:
                raise HTTPException(
                    422, f"Question {question.id!r} has an invalid choice"
                )
        else:
            allowed = {option.value for option in question.options}
            if (
                not isinstance(value, list)
                or any(
                    not isinstance(item, str) or item not in allowed for item in value
                )
                or len(value) != len(set(value))
            ):
                raise HTTPException(
                    422, f"Question {question.id!r} has invalid choices"
                )
        normalized.append({"question_id": question.id, "value": value})
    return normalized


async def answer_input_request(
    db: AsyncSession,
    *,
    plan: Plan,
    run: PlanAgentRun,
    input_request: PlanInputRequest,
    expected_generation: int,
    idempotency_key: str,
    answers: Iterable[PlanInputAnswer | dict],
    response_text: str | None,
    attachments: list[dict] | None,
    answered_by: int | None,
) -> PlanInputRequest:
    if (
        input_request.answer_idempotency_key == idempotency_key
        and input_request.status == "answered"
    ):
        return input_request
    if plan.active_run_id != run.id or run.status != "waiting_user":
        raise HTTPException(409, "Plan Run is no longer waiting for input")
    if run.generation != expected_generation:
        raise HTTPException(409, "Plan Run generation changed")
    if run.open_input_request_id != input_request.id or input_request.status != "open":
        raise HTTPException(409, "Input request is no longer open")
    normalized = validate_input_answers(
        input_request.questions,
        answers,
        response_text=response_text,
    )
    from backend.services.plan_input_safety import contains_high_confidence_secret

    if contains_high_confidence_secret(
        [response_text, *[item.get("value") for item in normalized]]
    ):
        raise HTTPException(
            422,
            "Plan answers cannot store API keys or access tokens. "
            "Save the credential in Settings → Secrets and answer with its name/reference.",
        )
    now = datetime.utcnow()
    updated = await db.execute(
        update(PlanInputRequest)
        .where(
            PlanInputRequest.id == input_request.id,
            PlanInputRequest.run_id == run.id,
            PlanInputRequest.status == "open",
        )
        .values(
            status="answered",
            answers=normalized,
            response_text=response_text,
            attachments=attachments or None,
            answered_by=answered_by,
            answered_at=now,
            answer_idempotency_key=idempotency_key,
        )
    )
    resumed = await db.execute(
        update(PlanAgentRun)
        .where(
            PlanAgentRun.id == run.id,
            PlanAgentRun.plan_id == plan.id,
            PlanAgentRun.status == "waiting_user",
            PlanAgentRun.generation == expected_generation,
            PlanAgentRun.open_input_request_id == input_request.id,
        )
        .values(
            status="queued",
            current_stage="planner",
            open_input_request_id=None,
            generation=PlanAgentRun.generation + 1,
            updated_at=now,
        )
    )
    if updated.rowcount != 1 or resumed.rowcount != 1:
        await db.rollback()
        raise HTTPException(409, "Input request was answered concurrently")
    await db.commit()
    await db.refresh(input_request)
    return input_request


async def decide_version(
    db: AsyncSession,
    *,
    plan: Plan,
    version: PlanVersion,
    decision: str,
    decided_by: int | None,
    expected_current_version_id: int,
) -> PlanVersion:
    if (
        plan.current_version_id != expected_current_version_id
        or version.id != expected_current_version_id
    ):
        raise HTTPException(409, "Plan current Version changed")
    if plan.active_run_id is not None:
        raise HTTPException(409, "Plan has an active Run")
    if (
        version.review_verdict not in {"approve", "disabled", "exhausted"}
        and not version.review_exhausted
    ):
        raise HTTPException(409, "Version is not ready for a human decision")
    if version.human_decision != "pending":
        if version.human_decision == decision:
            return version
        raise HTTPException(409, f"Version was already {version.human_decision}")
    changed = await db.execute(
        update(PlanVersion)
        .where(
            PlanVersion.id == version.id,
            PlanVersion.plan_id == plan.id,
            PlanVersion.human_decision == "pending",
            PlanVersion.superseded_by_version_id.is_(None),
        )
        .values(
            human_decision=decision,
            decided_at=datetime.utcnow(),
            decided_by=decided_by,
        )
    )
    if changed.rowcount != 1:
        await db.rollback()
        raise HTTPException(409, "Version decision changed concurrently")
    await db.commit()
    await db.refresh(version)
    return version


async def materialize_execution_task(
    db: AsyncSession,
    *,
    plan_id: int,
    version_id: int,
    expected_current_version_id: int,
    confirm_stale: bool,
    approve_if_pending: bool,
    actor_id: int | None,
    execution_metadata: dict | None = None,
) -> PlanExecutionTaskResult:
    """Idempotently apply one standalone Plan Version as an execution Task.

    This is the canonical in-process boundary for UI/API callers and future
    orchestrators.  The exact Plan Version is the idempotency key: replaying
    the operation returns its existing Task and never creates a second one.
    Authorization and post-commit wake/broadcast behavior remain adapter
    concerns and must be handled by the caller.
    """

    async with plan_operation_lock(plan_id):
        plan = await db.get(Plan, plan_id, populate_existing=True)
        version = await db.get(PlanVersion, version_id, populate_existing=True)
        if plan is None:
            raise HTTPException(404, "Plan not found")
        if version is None or version.plan_id != plan.id:
            raise HTTPException(404, "Plan Version not found")
        if plan.target_task_id is not None:
            raise HTTPException(400, "Only standalone Plans create execution Tasks")
        if (
            plan.current_version_id != expected_current_version_id
            or version.id != expected_current_version_id
        ):
            raise HTTPException(
                409,
                {
                    "code": "plan_version_changed",
                    "message": "Plan current Version changed",
                    "plan_id": plan.id,
                    "current_version_id": plan.current_version_id,
                    "active_run_id": plan.active_run_id,
                },
            )

        from backend.services.plan_staleness import version_staleness

        stale = await version_staleness(db, plan, version)
        if stale["hard_conflict"]:
            raise HTTPException(
                409,
                {
                    "code": "plan_hard_conflict",
                    "message": "Execution target is unavailable",
                    **stale,
                },
            )
        if stale["stale"] and not confirm_stale:
            raise HTTPException(
                409,
                {
                    "code": "plan_stale",
                    "message": "Plan Version context is stale",
                    **stale,
                },
            )
        if version.human_decision == "pending" and approve_if_pending:
            version = await decide_version(
                db,
                plan=plan,
                version=version,
                decision="approved",
                decided_by=actor_id,
                expected_current_version_id=expected_current_version_id,
            )
        if version.human_decision != "approved":
            raise HTTPException(409, "Plan Version must be approved")

        existing = (
            await db.execute(
                select(PlanApplication).where(
                    PlanApplication.plan_version_id == version.id
                )
            )
        ).scalar_one_or_none()
        if existing is not None:
            if (
                existing.application_type != "execution_task"
                or existing.execution_task_id is None
            ):
                raise HTTPException(409, "Plan Version was already applied")
            task = await db.get(Task, existing.execution_task_id)
            if task is None:
                raise HTTPException(
                    409,
                    "Plan Version execution Task is missing",
                )
            return PlanExecutionTaskResult(
                plan=plan,
                version=version,
                application=existing,
                task=task,
                created=False,
            )

        metadata = dict(execution_metadata or {})
        # These audit keys are authoritative and cannot be overridden by an
        # embedding orchestrator's optional correlation metadata.
        metadata.update(
            {
                "created_from_plan_id": plan.id,
                "created_from_plan_version_id": version.id,
            }
        )
        task = await stage_task_record(
            db,
            title=f"Execute {plan.title} · v{version.version_number}"[:200],
            description=(
                "[Approved implementation plan]\n"
                "Implement the exact approved Plan Version below.\n\n"
                f'<plan id="{plan.id}" version="{version.version_number}">\n'
                f"{version.content}\n</plan>\n\n"
                f"[Original planning request]\n{plan.initial_request}"
            ),
            status="pending",
            priority=plan.priority,
            timeout_hours=plan.timeout_hours,
            project_id=plan.project_id,
            target_repo=plan.target_repo,
            target_branch=plan.target_branch,
            merge_status="pending",
            worker_id=plan.worker_id,
            created_by=actor_id,
            mode="auto",
            metadata_=metadata,
        )
        application = PlanApplication(
            plan_id=plan.id,
            plan_version_id=version.id,
            application_type="execution_task",
            execution_task_id=task.id,
            applied_by=actor_id,
        )
        db.add(application)
        try:
            await db.commit()
        except IntegrityError:
            # The database uniqueness fence is authoritative across API
            # processes.  A concurrent winner may have committed after our
            # pre-check; discard this transaction's Task and return that exact
            # application instead of exposing a false failure to a retrying
            # orchestrator.
            await db.rollback()
            existing = (
                await db.execute(
                    select(PlanApplication).where(
                        PlanApplication.plan_version_id == version_id
                    )
                )
            ).scalar_one_or_none()
            if (
                existing is None
                or existing.application_type != "execution_task"
                or existing.execution_task_id is None
            ):
                raise
            existing_task = await db.get(Task, existing.execution_task_id)
            refreshed_plan = await db.get(Plan, plan_id, populate_existing=True)
            refreshed_version = await db.get(
                PlanVersion,
                version_id,
                populate_existing=True,
            )
            if (
                existing_task is None
                or refreshed_plan is None
                or refreshed_version is None
            ):
                raise HTTPException(
                    409,
                    "Plan Version execution Task is missing",
                )
            return PlanExecutionTaskResult(
                plan=refreshed_plan,
                version=refreshed_version,
                application=existing,
                task=existing_task,
                created=False,
            )
        except Exception:
            await db.rollback()
            raise
        await db.refresh(application)
        await db.refresh(task)
        return PlanExecutionTaskResult(
            plan=plan,
            version=version,
            application=application,
            task=task,
            created=True,
        )


async def cancel_run(
    db: AsyncSession, *, plan: Plan, run: PlanAgentRun
) -> PlanAgentRun:
    if plan.active_run_id != run.id or run.status not in ACTIVE_RUN_STATUSES:
        if run.status == "cancelled":
            return run
        raise HTTPException(409, "Plan Run is no longer active")
    now = datetime.utcnow()
    execution_seconds = float(run.execution_seconds or 0)
    if run.last_execution_started_at is not None:
        execution_seconds += max(
            0.0,
            (now - run.last_execution_started_at).total_seconds(),
        )
    if run.open_input_request_id is not None:
        await db.execute(
            update(PlanInputRequest)
            .where(
                PlanInputRequest.id == run.open_input_request_id,
                PlanInputRequest.status.in_(["prepared", "open"]),
            )
            .values(status="cancelled", cancelled_at=now)
        )
    changed = await db.execute(
        update(PlanAgentRun)
        .where(
            PlanAgentRun.id == run.id,
            PlanAgentRun.plan_id == plan.id,
            PlanAgentRun.status.in_(ACTIVE_RUN_STATUSES),
        )
        .values(
            status="cancelled",
            open_input_request_id=None,
            instance_id=None,
            execution_seconds=execution_seconds,
            last_execution_started_at=None,
            generation=PlanAgentRun.generation + 1,
            error="Cancelled by user",
            updated_at=now,
            finished_at=now,
        )
    )
    released = await db.execute(
        update(Plan)
        .where(Plan.id == plan.id, Plan.active_run_id == run.id)
        .values(
            active_run_id=None,
            lock_version=Plan.lock_version + 1,
            updated_at=now,
        )
    )
    if changed.rowcount != 1 or released.rowcount != 1:
        await db.rollback()
        raise HTTPException(409, "Plan Run changed while cancelling")
    await db.commit()
    await db.refresh(run)
    return run


async def resolve_legacy_task(
    db: AsyncSession, task_id: int
) -> PlanLegacyTaskLink | None:
    return await db.get(PlanLegacyTaskLink, task_id)


async def approved_versions_for_message(
    db: AsyncSession,
    *,
    target,
    version_ids: list[int] | None,
    confirmed_stale_version_ids: list[int] | None = None,
) -> list[tuple[Plan, PlanVersion]]:
    """Resolve exact approved Versions in caller order for one chat turn."""

    raw_ids = version_ids or []
    ids: list[int] = []
    for value in raw_ids:
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError("plan_version_ids must contain positive integers")
        if value in ids:
            raise ValueError("plan_version_ids must not contain duplicates")
        ids.append(value)
    if not ids:
        return []
    versions = {
        row.id: row
        for row in (
            await db.execute(select(PlanVersion).where(PlanVersion.id.in_(ids)))
        ).scalars()
    }
    plan_ids = {row.plan_id for row in versions.values()}
    plans = {
        row.id: row
        for row in (
            await db.execute(select(Plan).where(Plan.id.in_(plan_ids)))
        ).scalars()
    }
    confirmed = set(confirmed_stale_version_ids or [])
    from backend.services.plan_staleness import version_staleness

    result: list[tuple[Plan, PlanVersion]] = []
    for version_id in ids:
        version = versions.get(version_id)
        plan = plans.get(version.plan_id) if version is not None else None
        if version is None or plan is None:
            raise ValueError(f"Plan Version #{version_id} was not found")
        if plan.target_task_id != target.id:
            raise ValueError(
                f"Plan Version #{version_id} is not associated with Task #{target.id}"
            )
        if version.human_decision != "approved" or not version.content:
            raise ValueError(f"Plan Version #{version_id} is not approved and ready")
        applied = await db.scalar(
            select(PlanApplication.id)
            .where(PlanApplication.plan_version_id == version.id)
            .limit(1)
        )
        if applied is not None:
            raise ValueError(f"Plan Version #{version_id} has already been applied")
        staleness = await version_staleness(db, plan, version)
        if staleness["hard_conflict"]:
            error = ValueError(
                f"Plan Version #{version.id} has a non-bypassable target conflict"
            )
            setattr(error, "staleness", staleness)
            setattr(error, "plan_version_id", version.id)
            raise error
        if staleness["stale"] and version.id not in confirmed:
            error = ValueError(
                f"Plan Version #{version.id} context changed; confirm stale application"
            )
            setattr(error, "staleness", staleness)
            setattr(error, "plan_version_id", version.id)
            raise error
        result.append((plan, version))
    return result


def versioned_plan_snapshots(
    approved: list[tuple[Plan, PlanVersion]],
) -> list[dict[str, object]]:
    return [
        {
            # Legacy display readers require id/title/content. ``id`` remains
            # the stable Plan id while the new fields preserve exact identity.
            "id": plan.id,
            "plan_id": plan.id,
            "version_id": version.id,
            "version_number": version.version_number,
            "title": plan.title or f"Plan #{plan.id}",
            "content": version.content,
        }
        for plan, version in approved
    ]


def build_versioned_plan_prompt(
    approved: list[tuple[Plan, PlanVersion]], user_prompt: str
) -> str:
    if not approved:
        return user_prompt
    parts = [
        "[Approved Plan Versions explicitly selected by the user for this turn]",
        (
            "The Versions below are immutable context for the current instruction. "
            "Approval alone grants no permission beyond that instruction."
        ),
    ]
    for plan, version in approved:
        parts.append(
            f'<approved_plan plan_id="{plan.id}" version_id="{version.id}" '
            f'version="{version.version_number}">\n{version.content}\n</approved_plan>'
        )
    parts.extend(["[User instruction for this turn]", user_prompt])
    return "\n\n".join(parts)


async def _version_resource(
    db: AsyncSession, version: PlanVersion | None
) -> PlanVersionResource | None:
    if version is None:
        return None
    applied = (
        await db.scalar(
            select(PlanApplication.id)
            .where(PlanApplication.plan_version_id == version.id)
            .limit(1)
        )
        is not None
    )
    if applied:
        display_state = "applied"
    elif version.human_decision == "rejected":
        display_state = "rejected"
    elif version.human_decision == "approved":
        display_state = "approved"
    elif version.superseded_by_version_id is not None:
        display_state = "superseded"
    elif (
        version.review_verdict in {"approve", "disabled", "exhausted"}
        or version.review_exhausted
    ):
        display_state = "awaiting_review"
    else:
        display_state = "draft"
    return PlanVersionResource.model_validate(version).model_copy(
        update={"applied": applied, "display_state": display_state}
    )


async def _application_resources(
    db: AsyncSession,
    applications: list[PlanApplication],
) -> list[PlanApplicationResource]:
    execution_task_ids = {
        item.execution_task_id
        for item in applications
        if item.application_type == "execution_task"
        and item.execution_task_id is not None
    }
    available_execution_task_ids = (
        set(
            (
                await db.execute(select(Task.id).where(Task.id.in_(execution_task_ids)))
            ).scalars()
        )
        if execution_task_ids
        else set()
    )
    receipt_keys = {
        item.application_receipt_key
        for item in applications
        if item.application_receipt_key is not None
    }
    receipts = (
        {
            row.receipt_key: row
            for row in (
                await db.execute(
                    select(PlanApplicationReceipt).where(
                        PlanApplicationReceipt.receipt_key.in_(receipt_keys)
                    )
                )
            ).scalars()
        }
        if receipt_keys
        else {}
    )
    return [
        PlanApplicationResource.model_validate(item).model_copy(
            update={
                "execution_task_available": (
                    item.execution_task_id in available_execution_task_ids
                    if item.application_type == "execution_task"
                    and item.execution_task_id is not None
                    else None
                ),
                "delivery_status": (
                    receipts[item.application_receipt_key].delivery_status
                    if item.application_receipt_key in receipts
                    else None
                ),
                "delivery_error": (
                    receipts[item.application_receipt_key].delivery_error
                    if item.application_receipt_key in receipts
                    else None
                ),
                "launch_evidence": (
                    receipts[item.application_receipt_key].launch_evidence
                    if item.application_receipt_key in receipts
                    else None
                ),
                "delivery_resolution": (
                    receipts[item.application_receipt_key].delivery_resolution
                    if item.application_receipt_key in receipts
                    else None
                ),
            }
        )
        for item in applications
    ]


async def _application_attempt_resources(
    db: AsyncSession,
    attempts: list[PlanApplicationAttempt],
) -> list[PlanApplicationAttemptResource]:
    receipt_keys = {item.application_receipt_key for item in attempts}
    receipts = {
        row.receipt_key: row
        for row in (
            await db.execute(
                select(PlanApplicationReceipt).where(
                    PlanApplicationReceipt.receipt_key.in_(receipt_keys)
                )
            )
        ).scalars()
    }
    return [
        PlanApplicationAttemptResource.model_validate(item).model_copy(
            update={
                "delivery_status": (
                    receipts[item.application_receipt_key].delivery_status
                    if item.application_receipt_key in receipts
                    else "missing"
                ),
                "delivery_error": (
                    receipts[item.application_receipt_key].delivery_error
                    if item.application_receipt_key in receipts
                    else "Plan application receipt is missing"
                ),
                "launch_evidence": (
                    receipts[item.application_receipt_key].launch_evidence
                    if item.application_receipt_key in receipts
                    else None
                ),
                "delivery_resolution": (
                    receipts[item.application_receipt_key].delivery_resolution
                    if item.application_receipt_key in receipts
                    else None
                ),
            }
        )
        for item in attempts
    ]


async def _run_resource(
    db: AsyncSession, run: PlanAgentRun | None, *, include_audit: bool = False
) -> PlanRunResource | None:
    if run is None:
        return None
    steps: list[PlanStepResource] = []
    inputs: list[PlanInputRequestResponse] = []
    if include_audit:
        steps = [
            PlanStepResource.model_validate(row)
            for row in (
                await db.execute(
                    select(PlanAgentStep)
                    .where(PlanAgentStep.run_id == run.id)
                    .order_by(PlanAgentStep.id)
                )
            ).scalars()
        ]
        inputs = [
            input_request_resource(row)
            for row in (
                await db.execute(
                    select(PlanInputRequest)
                    .where(PlanInputRequest.run_id == run.id)
                    .order_by(PlanInputRequest.id)
                )
            ).scalars()
        ]
    return PlanRunResource.model_validate(run).model_copy(
        update={"steps": steps, "input_requests": inputs}
    )


async def plan_resource(
    db: AsyncSession, plan: Plan, *, include_audit: bool = False
) -> PlanResource:
    resources = await plan_resources(db, [plan], include_audit=include_audit)
    return resources[0]


async def plan_resources(
    db: AsyncSession,
    plans: list[Plan],
    *,
    include_audit: bool = False,
) -> list[PlanResource]:
    """Build a Plan list with a bounded set of aggregate preload queries."""

    if not plans:
        return []
    plan_ids = [plan.id for plan in plans]
    version_ids = {
        plan.current_version_id for plan in plans if plan.current_version_id is not None
    }
    active_run_ids = {
        plan.active_run_id for plan in plans if plan.active_run_id is not None
    }
    versions = (
        {
            row.id: row
            for row in (
                await db.execute(
                    select(PlanVersion).where(PlanVersion.id.in_(version_ids))
                )
            ).scalars()
        }
        if version_ids
        else {}
    )
    active_runs = (
        {
            row.id: row
            for row in (
                await db.execute(
                    select(PlanAgentRun).where(PlanAgentRun.id.in_(active_run_ids))
                )
            ).scalars()
        }
        if active_run_ids
        else {}
    )
    latest_run_ids = (
        select(func.max(PlanAgentRun.id))
        .where(PlanAgentRun.plan_id.in_(plan_ids))
        .group_by(PlanAgentRun.plan_id)
    )
    latest_runs = {
        row.plan_id: row
        for row in (
            await db.execute(
                select(PlanAgentRun).where(PlanAgentRun.id.in_(latest_run_ids))
            )
        ).scalars()
    }
    open_input_ids = {
        run.open_input_request_id
        for run in active_runs.values()
        if run.open_input_request_id is not None
    }
    open_inputs = (
        {
            row.id: row
            for row in (
                await db.execute(
                    select(PlanInputRequest).where(
                        PlanInputRequest.id.in_(open_input_ids)
                    )
                )
            ).scalars()
        }
        if open_input_ids
        else {}
    )
    applications = list(
        (
            await db.execute(
                select(PlanApplication)
                .where(PlanApplication.plan_id.in_(plan_ids))
                .order_by(
                    PlanApplication.plan_id,
                    PlanApplication.created_at,
                    PlanApplication.id,
                )
            )
        ).scalars()
    )
    applications_by_plan: defaultdict[int, list[PlanApplication]] = defaultdict(list)
    for application in applications:
        applications_by_plan[application.plan_id].append(application)
    application_resources = await _application_resources(db, applications)
    application_resource_by_id = {item.id: item for item in application_resources}
    application_attempts = (
        list(
            (
                await db.execute(
                    select(PlanApplicationAttempt)
                    .where(PlanApplicationAttempt.plan_id.in_(plan_ids))
                    .order_by(
                        PlanApplicationAttempt.plan_id,
                        PlanApplicationAttempt.released_at,
                        PlanApplicationAttempt.id,
                    )
                )
            ).scalars()
        )
        if include_audit
        else []
    )
    application_attempts_by_plan: defaultdict[int, list[PlanApplicationAttempt]] = (
        defaultdict(list)
    )
    for attempt in application_attempts:
        application_attempts_by_plan[attempt.plan_id].append(attempt)
    application_attempt_resources = (
        await _application_attempt_resources(
            db,
            application_attempts,
        )
        if application_attempts
        else []
    )
    application_attempt_resource_by_id = {
        item.id: item for item in application_attempt_resources
    }
    applied_version_ids = {item.plan_version_id for item in applications}
    legacy_plan_ids = set(
        (
            await db.execute(
                select(PlanLegacyTaskLink.plan_id).where(
                    PlanLegacyTaskLink.plan_id.in_(plan_ids)
                )
            )
        ).scalars()
    )

    steps_by_run: defaultdict[int, list[PlanStepResource]] = defaultdict(list)
    inputs_by_run: defaultdict[int, list[PlanInputRequestResponse]] = defaultdict(list)
    if include_audit and active_run_ids:
        for row in (
            await db.execute(
                select(PlanAgentStep)
                .where(PlanAgentStep.run_id.in_(active_run_ids))
                .order_by(PlanAgentStep.run_id, PlanAgentStep.id)
            )
        ).scalars():
            steps_by_run[row.run_id].append(PlanStepResource.model_validate(row))
        for row in (
            await db.execute(
                select(PlanInputRequest)
                .where(PlanInputRequest.run_id.in_(active_run_ids))
                .order_by(PlanInputRequest.run_id, PlanInputRequest.id)
            )
        ).scalars():
            inputs_by_run[row.run_id].append(input_request_resource(row))

    result: list[PlanResource] = []
    for plan in plans:
        current = versions.get(plan.current_version_id)
        active = active_runs.get(plan.active_run_id)
        latest = latest_runs.get(plan.id)
        plan_applications = applications_by_plan[plan.id]
        current_application = next(
            (
                item
                for item in plan_applications
                if current is not None and item.plan_version_id == current.id
            ),
            None,
        )
        if plan.archived_at is not None:
            display_state = "archived"
        elif active is not None and active.status == "waiting_user":
            display_state = "waiting_user"
        elif active is not None and active.status in {"queued", "running"}:
            display_state = active.current_stage or "running"
        elif current_application is not None:
            display_state = "applied"
        elif current is not None and current.human_decision == "approved":
            display_state = "approved"
        elif current is not None and current.human_decision == "rejected":
            display_state = "rejected"
        elif current is not None and (
            current.review_verdict in {"approve", "disabled", "exhausted"}
            or current.review_exhausted
        ):
            display_state = "awaiting_review"
        elif latest is not None and latest.status in {"failed", "cancelled"}:
            display_state = latest.status
        else:
            display_state = "draft"
        payload = {
            column: getattr(plan, column)
            for column in (
                "id",
                "title",
                "initial_request",
                "initial_attachments",
                "target_task_id",
                "project_id",
                "target_repo",
                "target_branch",
                "worker_id",
                "priority",
                "timeout_hours",
                "created_by",
                "pipeline_config",
                "current_version_id",
                "active_run_id",
                "forked_from_version_id",
                "archived_at",
                "closed_at",
                "lock_version",
                "created_at",
                "updated_at",
            )
        }
        payload["initial_attachments"] = _public_attachments(plan.initial_attachments)
        current_resource = None
        if current is not None:
            if current.id in applied_version_ids:
                version_state = "applied"
            elif current.human_decision == "rejected":
                version_state = "rejected"
            elif current.human_decision == "approved":
                version_state = "approved"
            elif current.superseded_by_version_id is not None:
                version_state = "superseded"
            elif (
                current.review_verdict in {"approve", "disabled", "exhausted"}
                or current.review_exhausted
            ):
                version_state = "awaiting_review"
            else:
                version_state = "draft"
            current_resource = PlanVersionResource.model_validate(current).model_copy(
                update={
                    "applied": current.id in applied_version_ids,
                    "display_state": version_state,
                }
            )
        active_resource = None
        if active is not None:
            active_resource = PlanRunResource.model_validate(active).model_copy(
                update={
                    "steps": steps_by_run[active.id] if include_audit else [],
                    "input_requests": (
                        inputs_by_run[active.id] if include_audit else []
                    ),
                }
            )
        result.append(
            PlanResource(
                **payload,
                display_state=display_state,
                legacy=plan.id in legacy_plan_ids,
                latest_run_status=latest.status if latest else None,
                latest_run_error=(
                    latest.error
                    if latest is not None and latest.status == "failed"
                    else None
                ),
                application=(
                    application_resource_by_id.get(current_application.id)
                    if current_application is not None
                    else None
                ),
                applications=[
                    application_resource_by_id[item.id] for item in plan_applications
                ],
                application_attempts=[
                    application_attempt_resource_by_id[item.id]
                    for item in application_attempts_by_plan[plan.id]
                ],
                current_version=current_resource,
                active_run=active_resource,
                open_input_request=(
                    input_request_resource(open_inputs[active.open_input_request_id])
                    if active is not None
                    and active.open_input_request_id in open_inputs
                    else None
                ),
            )
        )
    return result


async def run_resource(
    db: AsyncSession, run: PlanAgentRun, *, include_audit: bool = True
) -> PlanRunResource:
    resource = await _run_resource(db, run, include_audit=include_audit)
    assert resource is not None
    return resource


async def version_resource(
    db: AsyncSession, version: PlanVersion
) -> PlanVersionResource:
    resource = await _version_resource(db, version)
    assert resource is not None
    return resource


async def apply_worker_plan_outcome(
    db: AsyncSession,
    *,
    plan: Plan,
    run: PlanAgentRun,
    worker_id: int,
    expected_generation: int,
    payload: dict,
) -> PlanAgentRun:
    """Import one exact Worker pause while keeping Manager ids authoritative."""

    if payload.get("protocol") != 3:
        raise RuntimeError("Worker Plan outcome protocol mismatch")
    base_worker_version_id = payload.get("base_worker_version_id")
    if isinstance(base_worker_version_id, bool) or (
        base_worker_version_id is not None
        and not isinstance(base_worker_version_id, int)
    ):
        raise RuntimeError("Worker Plan outcome has invalid base Version identity")
    manager_base = (
        await db.get(PlanVersion, run.base_version_id)
        if run.base_version_id is not None
        else None
    )
    if (
        manager_base is not None
        and manager_base.plan_id != plan.id
        and run.run_type != "fork"
    ):
        raise RuntimeError("Plan Run base Version belongs to another Plan")
    remote = PlanRunResource.model_validate(payload.get("run"))
    remote_versions = [
        PlanVersionResource.model_validate(item) for item in payload.get("versions", [])
    ]
    if (
        plan.worker_id != worker_id
        or run.worker_id != worker_id
        or plan.active_run_id != run.id
        or run.status != "running"
        or run.generation != expected_generation
        or remote.id != run.id
        or remote.plan_id != plan.id
        or remote.status not in {"waiting_user", "completed", "failed", "cancelled"}
    ):
        raise RuntimeError("Worker Plan outcome no longer owns this Run generation")

    step_by_remote: dict[int, PlanAgentStep] = {}
    for item in remote.steps:
        if item.run_id != remote.id or item.plan_id != plan.id:
            raise RuntimeError("Worker Plan Step belongs to another Run or Plan")
        step = (
            await db.execute(
                select(PlanAgentStep).where(
                    PlanAgentStep.worker_id == worker_id,
                    PlanAgentStep.worker_step_id == item.id,
                )
            )
        ).scalar_one_or_none()
        if step is None:
            step = PlanAgentStep(
                run_id=run.id,
                plan_id=plan.id,
                worker_id=worker_id,
                worker_step_id=item.id,
                generation=item.generation,
                step_type=item.step_type,
                round=item.round,
                provider=item.provider,
                model=item.model,
                effort=item.effort,
                route_slot=item.route_slot,
                status=item.status,
                output=item.output,
                error=item.error,
                last_delta_at=item.last_delta_at,
                streamed_output_chars=item.streamed_output_chars,
                last_event_type=item.last_event_type,
                started_at=item.started_at,
                finished_at=item.finished_at,
            )
            db.add(step)
            await db.flush()
        elif (
            step.run_id != run.id
            or step.plan_id != plan.id
            or step.step_type != item.step_type
            or step.round != item.round
            or step.generation != item.generation
            or step.provider != item.provider
            or step.model != item.model
            or step.effort != item.effort
            or step.route_slot != item.route_slot
            or step.status != item.status
            or step.output != item.output
            or step.error != item.error
            or step.last_delta_at != item.last_delta_at
            or step.streamed_output_chars != item.streamed_output_chars
            or step.last_event_type != item.last_event_type
        ):
            raise RuntimeError("Worker Plan Step mapping collides with another Run")
        step_by_remote[item.id] = step

    version_by_remote: dict[int, PlanVersion] = {}
    for item in sorted(remote_versions, key=lambda version: version.version_number):
        if item.plan_id != plan.id:
            raise RuntimeError("Worker Plan Version belongs to another Plan")
        version = (
            await db.execute(
                select(PlanVersion).where(
                    PlanVersion.worker_id == worker_id,
                    PlanVersion.worker_version_id == item.id,
                )
            )
        ).scalar_one_or_none()
        parent = (
            manager_base
            if item.parent_version_id is not None
            and item.parent_version_id == base_worker_version_id
            else version_by_remote.get(item.parent_version_id)
        )
        if item.parent_version_id is not None and parent is None:
            raise RuntimeError("Worker Plan Version parent was not imported")
        produced = step_by_remote.get(item.produced_by_step_id)
        reviewed = step_by_remote.get(item.reviewed_by_step_id)
        if version is None:
            version = PlanVersion(
                plan_id=plan.id,
                worker_id=worker_id,
                worker_version_id=item.id,
                version_number=item.version_number,
                parent_version_id=parent.id if parent is not None else None,
                produced_by_run_id=run.id,
                produced_by_step_id=produced.id if produced is not None else None,
                content=item.content,
                # Manager log/session ids are the authoritative staleness
                # coordinate; Worker-local ids are not comparable here.
                context_session_id=run.context_session_id,
                context_log_id=run.context_log_id,
                # Context snapshots remain Manager-owned and are deliberately
                # not exposed by the public Version resource protocol.
                context_snapshot=run.context_snapshot,
                repo_revision=item.repo_revision,
                reviewer_repo_revision=item.reviewer_repo_revision,
                human_decision="pending",
                created_at=item.created_at,
            )
            db.add(version)
            await db.flush()
            if (
                parent is manager_base
                and manager_base is not None
                and manager_base.plan_id == plan.id
                and manager_base.superseded_by_version_id is None
            ):
                manager_base.superseded_by_version_id = version.id
        elif (
            version.plan_id != plan.id
            or version.version_number != item.version_number
            or version.content != item.content
        ):
            raise RuntimeError("Worker Plan Version mapping changed immutable content")
        version.review_verdict = item.review_verdict
        version.review_feedback = item.review_feedback
        version.reviewed_by_step_id = reviewed.id if reviewed is not None else None
        version.review_exhausted = item.review_exhausted
        version.reviewed_at = item.reviewed_at
        version.reviewer_repo_revision = item.reviewer_repo_revision
        version_by_remote[item.id] = version
        if produced is not None:
            produced.plan_version_id = version.id

    for item in remote_versions:
        version = version_by_remote[item.id]
        successor = version_by_remote.get(item.superseded_by_version_id)
        if item.superseded_by_version_id is not None and successor is None:
            raise RuntimeError("Worker Plan Version successor was not imported")
        if successor is not None:
            version.superseded_by_version_id = successor.id

    input_by_remote: dict[int, PlanInputRequest] = {}
    for item in remote.input_requests:
        input_request = (
            await db.execute(
                select(PlanInputRequest).where(
                    PlanInputRequest.worker_id == worker_id,
                    PlanInputRequest.worker_input_request_id == item.id,
                )
            )
        ).scalar_one_or_none()
        source = step_by_remote.get(item.source_step_id)
        if source is None:
            raise RuntimeError("Worker InputRequest has no imported source Step")
        if input_request is None:
            input_request = PlanInputRequest(
                plan_id=plan.id,
                run_id=run.id,
                worker_id=worker_id,
                worker_input_request_id=item.id,
                source_step_id=source.id,
                requested_by=item.requested_by,
                reason=item.reason,
                questions=[
                    question.model_dump(mode="json") for question in item.questions
                ],
                status=item.status,
                answers=item.answers,
                response_text=item.response_text,
                attachments=item.attachments,
                answered_by=item.answered_by,
                idempotency_key=f"worker:{worker_id}:input:{item.id}",
                opened_at=item.opened_at,
                answered_at=item.answered_at,
                created_at=item.created_at,
            )
            db.add(input_request)
            await db.flush()
        elif input_request.run_id != run.id or input_request.plan_id != plan.id:
            raise RuntimeError("Worker InputRequest mapping collides with another Run")
        input_by_remote[item.id] = input_request
        source.input_request_id = input_request.id

    latest = max(
        version_by_remote.values(),
        key=lambda version: version.version_number,
        default=None,
    )
    result_version = version_by_remote.get(remote.result_version_id)
    run.current_stage = remote.current_stage
    run.round = remote.round
    # Keep the Manager claim generation authoritative on the Manager. The
    # Worker generation only fences Worker-local execution/input operations.
    run.execution_seconds = remote.execution_seconds
    run.last_execution_started_at = None
    run.result_version_id = result_version.id if result_version is not None else None
    run.draft_content = remote.draft_content
    draft_step = step_by_remote.get(remote.draft_step_id)
    if remote.draft_step_id is not None and draft_step is None:
        raise RuntimeError("Worker Plan draft has no imported Planner Step")
    run.draft_step_id = draft_step.id if draft_step is not None else None
    run.draft_repo_revision = remote.draft_repo_revision
    run.interaction_count = remote.interaction_count
    run.review_verdict = remote.review_verdict
    run.review_feedback = remote.review_feedback
    run.review_exhausted = remote.review_exhausted
    run.error = remote.error
    run.updated_at = datetime.utcnow()
    if latest is not None:
        plan.current_version_id = latest.id

    if remote.status == "waiting_user":
        open_input = input_by_remote.get(remote.open_input_request_id)
        if open_input is None or open_input.status != "open":
            raise RuntimeError("Worker waiting Run has no exact open InputRequest")
        run.status = "waiting_user"
        run.open_input_request_id = open_input.id
    else:
        if remote.status == "completed" and result_version is None:
            raise RuntimeError("Worker completed Run has no exact result Version")
        run.status = remote.status
        run.open_input_request_id = None
        run.finished_at = remote.finished_at or datetime.utcnow()
        plan.active_run_id = None
    plan.lock_version += 1
    plan.updated_at = datetime.utcnow()
    await db.commit()
    await db.refresh(run)
    return run
