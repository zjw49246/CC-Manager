"""Read-only progress projection for one autonomous Delivery Run.

The controller remains the only orchestration writer.  This module joins the
durable Plan, Task, Review, Harness and PR facts into a bounded public view so
the UI never has to guess progress from phase names or expose model reasoning.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.capability import CapabilityExecution
from backend.models.code_review import CodeReviewRun
from backend.models.delivery import (
    DeliveryCycle,
    DeliveryRun,
    DeliveryTransition,
    DeliveryTurn,
)
from backend.models.log_entry import LogEntry
from backend.models.plan import PlanInputRequest
from backend.models.plan_agent import PlanAgentRun, PlanAgentStep
from backend.models.pr_monitor import PRMonitorRun
from backend.models.task import Task
from backend.models.test_harness import (
    TestHarnessAttempt,
    TestHarnessEvent,
    TestHarnessEvidence,
    TestHarnessFinding,
    TestHarnessRun,
)
from backend.schemas.delivery import (
    DeliveryAgentActivity,
    DeliveryFrontendReviewProgress,
    DeliveryPlanInputRequestProjection,
    DeliveryPlanInputProjection,
    DeliveryPlanRunProjection,
    DeliveryProgressResponse,
    DeliveryStageProgress,
    DeliveryTimelineEvent,
)


_STAGE_ORDER = (
    "planning",
    "coding",
    "pre_review",
    "frontend_review",
    "publishing",
    "monitoring",
)
_STAGE_LABELS = {
    "planning": "Plan",
    "coding": "Development",
    "pre_review": "Code review",
    "frontend_review": "Frontend review",
    "publishing": "Publish PR",
    "monitoring": "CI & PR review",
}
_TRANSITION_TITLES = {
    "created": "Delivery created",
    "plan_requested": "Planning started",
    "plan_ready": "Plan approved",
    "code_started": "Developer turn started",
    "code_completed": "Developer turn completed",
    "report_completed": "Read-only report completed",
    "developer_no_progress": "Developer made no commit",
    "review_requested": "Code review started",
    "review_approved": "Code review approved",
    "review_changes_requested": "Code review requested changes",
    "frontend_review_requested": "Frontend review started",
    "frontend_review_passed": "Frontend review passed",
    "frontend_review_profile_passed": "Preview profile passed",
    "frontend_review_skipped": "Frontend review skipped",
    "frontend_review_changes_requested": "Frontend review found issues",
    "publish_started": "Publishing pull request",
    "pr_bound": "Pull request is being monitored",
    "monitor_blocked": "PR gate requested another repair",
    "monitor_refresh": "PR gate refreshed",
    "monitor_ready": "Delivery gate passed",
    "pause": "Delivery paused",
    "resume": "Delivery resumed",
    "retry": "Delivery retry started",
    "cancel": "Delivery cancelled",
    "fail": "Delivery failed",
    "supersede": "Delivery superseded",
}


@dataclass(slots=True)
class _PlanProjection:
    plan_run: PlanAgentRun | None = None
    step: PlanAgentStep | None = None
    input_projection: DeliveryPlanInputProjection | None = None
    steps: list[PlanAgentStep] | None = None


def _max_datetime(values: Iterable[datetime | None]) -> datetime | None:
    present = [value for value in values if value is not None]
    return max(present) if present else None


def _text_size(entry: LogEntry) -> int:
    return sum(
        len(value)
        for value in (entry.content, entry.tool_input, entry.tool_output)
        if isinstance(value, str)
    )


def _transition_stage(transition: DeliveryTransition) -> str:
    after = transition.after_state if isinstance(transition.after_state, dict) else {}
    before = (
        transition.before_state if isinstance(transition.before_state, dict) else {}
    )
    phase = after.get("phase")
    if phase == "done" or phase not in _STAGE_ORDER:
        phase = before.get("phase")
    return phase if phase in _STAGE_ORDER else "planning"


def _transition_detail(transition: DeliveryTransition) -> str | None:
    metadata = transition.metadata_ if isinstance(transition.metadata_, dict) else {}
    for key in ("reason", "summary", "skip_reason", "error_message"):
        value = metadata.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()[:2000]
    after = transition.after_state if isinstance(transition.after_state, dict) else {}
    for key in ("error_message", "pause_reason", "wait_reason"):
        value = after.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()[:2000]
    return None


async def _execution_for_invocation(
    db: AsyncSession,
    invocation_id: int | None,
) -> CapabilityExecution | None:
    if invocation_id is None:
        return None
    return (
        await db.execute(
            select(CapabilityExecution)
            .where(CapabilityExecution.invocation_id == invocation_id)
            .order_by(CapabilityExecution.attempt.desc())
            .limit(1)
        )
    ).scalar_one_or_none()


async def _plan_projection(
    db: AsyncSession,
    cycle: DeliveryCycle | None,
) -> _PlanProjection:
    if cycle is None or cycle.plan_invocation_id is None:
        return _PlanProjection(steps=[])
    execution = await _execution_for_invocation(db, cycle.plan_invocation_id)
    if (
        execution is None
        or execution.handle_kind != "plan_agent_run"
        or not isinstance(execution.handle_id, str)
        or not execution.handle_id.isdigit()
    ):
        return _PlanProjection(steps=[])
    plan_run = await db.get(PlanAgentRun, int(execution.handle_id))
    if plan_run is None or plan_run.plan_id is None:
        return _PlanProjection(steps=[])
    steps = list(
        (
            await db.execute(
                select(PlanAgentStep)
                .where(PlanAgentStep.run_id == plan_run.id)
                .order_by(PlanAgentStep.id)
            )
        ).scalars()
    )
    input_projection = None
    if plan_run.open_input_request_id is not None:
        input_request = await db.get(
            PlanInputRequest,
            plan_run.open_input_request_id,
        )
        if (
            input_request is not None
            and input_request.plan_id == plan_run.plan_id
            and input_request.run_id == plan_run.id
            and input_request.status == "open"
        ):
            input_projection = DeliveryPlanInputProjection(
                plan_id=plan_run.plan_id,
                run=DeliveryPlanRunProjection.model_validate(plan_run),
                request=DeliveryPlanInputRequestProjection.model_validate(
                    input_request
                ),
            )
    return _PlanProjection(
        plan_run=plan_run,
        step=steps[-1] if steps else None,
        input_projection=input_projection,
        steps=steps,
    )


async def delivery_run_attention_required(
    db: AsyncSession,
    run: DeliveryRun,
) -> bool:
    """Return whether the Run currently has an actionable human decision."""

    if run.activity == "paused":
        return True
    if run.activity == "terminal":
        # Terminal failures remain prominent in Run detail, but without a
        # durable acknowledgement model they must not keep the global action
        # badge lit forever and obscure a fresh Plan input.
        return False
    if run.phase != "planning" or run.current_cycle_id is None:
        return False
    cycle = await db.get(DeliveryCycle, run.current_cycle_id)
    if cycle is None or cycle.run_id != run.id:
        return False
    projection = await _plan_projection(db, cycle)
    return projection.input_projection is not None


async def _task_logs(
    db: AsyncSession,
    task: Task | None,
    *,
    retry_count: int | None = None,
    turn_generation: int | None = None,
) -> list[LogEntry]:
    if task is None:
        return []
    statement = select(LogEntry).where(LogEntry.task_id == task.id)
    if retry_count is not None:
        statement = statement.where(LogEntry.task_retry_count == retry_count)
    if turn_generation is not None:
        statement = statement.where(LogEntry.task_turn_generation == turn_generation)
    rows = list(
        (await db.execute(statement.order_by(LogEntry.id.desc()).limit(200))).scalars()
    )
    rows.reverse()
    return rows


def _task_agent(
    *,
    role: str,
    task: Task,
    logs: list[LogEntry],
    status: str,
    headline: str,
) -> DeliveryAgentActivity:
    output_logs = [
        item
        for item in logs
        if item.role == "assistant"
        or item.tool_name is not None
        or item.event_type in {"assistant", "result", "tool_use", "tool_result"}
    ]
    latest = logs[-1] if logs else None
    if latest is not None and latest.tool_name:
        activity_kind = "tool"
        detail = f"Using {latest.tool_name}"
    elif output_logs and status in {"in_progress", "executing", "running"}:
        activity_kind = "streaming"
        detail = "Public assistant or tool activity received"
    elif status in {"in_progress", "executing", "running"}:
        activity_kind = "working"
        detail = "Model turn started; waiting for its first public output"
    elif status in {"completed", "delivery_waiting"}:
        activity_kind = "completed"
        detail = "Agent turn completed"
    else:
        activity_kind = "waiting"
        detail = task.error_message
    return DeliveryAgentActivity(
        role=role,  # type: ignore[arg-type]
        provider=task.provider,
        model=task.model,
        effort=task.effort_level,
        service_tier=task.codex_service_tier,
        status=status,
        activity_kind=activity_kind,
        headline=headline,
        detail=detail,
        started_at=task.started_at,
        first_output_at=output_logs[0].timestamp if output_logs else None,
        last_activity_at=(latest.timestamp if latest is not None else task.started_at),
        output_chars=sum(_text_size(item) for item in output_logs),
    )


def _plan_agent(projection: _PlanProjection) -> DeliveryAgentActivity | None:
    plan_run = projection.plan_run
    if plan_run is None:
        return None
    step = projection.step
    if projection.input_projection is not None:
        activity_kind = "waiting_user"
        headline = "Plan needs your decision"
    elif step is None:
        activity_kind = "queued" if plan_run.status == "queued" else "working"
        headline = "Planner is starting"
    elif step.status in {"running", "queued"}:
        activity_kind = "streaming" if step.streamed_output_chars else "working"
        headline = (
            "Plan reviewer is checking the proposal"
            if step.step_type == "reviewer"
            else "Planner is preparing the implementation plan"
        )
    else:
        activity_kind = "completed" if step.status == "completed" else step.status
        headline = (
            "Plan reviewer finished"
            if step.step_type == "reviewer"
            else "Planner finished"
        )
    role = "plan_reviewer" if step and step.step_type == "reviewer" else "planner"
    output_chars = step.streamed_output_chars if step is not None else 0
    if step is not None and step.output:
        output_chars = max(output_chars, len(step.output))
    return DeliveryAgentActivity(
        role=role,
        provider=step.provider if step is not None else None,
        model=step.model if step is not None else None,
        effort=step.effort if step is not None else None,
        status=plan_run.status,
        activity_kind=activity_kind,
        headline=headline,
        detail=(step.error if step is not None else plan_run.error),
        started_at=step.started_at if step is not None else plan_run.created_at,
        # Plan transports only persist a delta timestamp, not the first delta.
        # A completed output is the earliest independently durable proof.
        first_output_at=(
            step.finished_at
            if step is not None and step.output and step.finished_at is not None
            else None
        ),
        last_activity_at=(
            _max_datetime([step.last_delta_at, step.finished_at, step.started_at])
            if step is not None
            else plan_run.updated_at
        ),
        output_chars=output_chars,
    )


async def _frontend_projection(
    db: AsyncSession,
    run: DeliveryRun,
    cycle: DeliveryCycle | None,
) -> tuple[
    DeliveryFrontendReviewProgress,
    TestHarnessRun | None,
    TestHarnessAttempt | None,
    list[TestHarnessEvent],
]:
    policy = run.policy_snapshot if isinstance(run.policy_snapshot, dict) else {}
    frontend_policy = policy.get("frontend_review")
    mode = frontend_policy.get("mode") if isinstance(frontend_policy, dict) else "off"
    if mode not in {"auto", "required", "off"}:
        mode = "off"
    harness = None
    attempt = None
    events: list[TestHarnessEvent] = []
    finding_count = 0
    evidence_count = 0
    archive_state = None
    if cycle is not None and cycle.frontend_review_run_id:
        harness = await db.get(TestHarnessRun, cycle.frontend_review_run_id)
    if harness is not None:
        attempts = list(
            (
                await db.execute(
                    select(TestHarnessAttempt)
                    .where(TestHarnessAttempt.run_id == harness.id)
                    .order_by(TestHarnessAttempt.ordinal)
                )
            ).scalars()
        )
        attempt = attempts[-1] if attempts else None
        archive_state = attempt.archive_state if attempt is not None else None
        events = list(
            (
                await db.execute(
                    select(TestHarnessEvent)
                    .where(TestHarnessEvent.run_id == harness.id)
                    .order_by(TestHarnessEvent.sequence)
                )
            ).scalars()
        )
        finding_count = len(
            list(
                (
                    await db.execute(
                        select(TestHarnessFinding.id).where(
                            TestHarnessFinding.run_id == harness.id
                        )
                    )
                ).scalars()
            )
        )
        evidence_count = len(
            list(
                (
                    await db.execute(
                        select(TestHarnessEvidence.id).where(
                            TestHarnessEvidence.run_id == harness.id
                        )
                    )
                ).scalars()
            )
        )
    return (
        DeliveryFrontendReviewProgress(
            policy=mode,
            run_id=harness.id if harness is not None else None,
            status=harness.status if harness is not None else None,
            stage=harness.stage if harness is not None else None,
            verdict=(
                harness.verdict
                if harness is not None
                else cycle.frontend_review_verdict
                if cycle is not None
                else None
            ),
            report=harness.report if harness is not None else None,
            error=harness.error if harness is not None else None,
            cleanup_status=harness.cleanup_status if harness is not None else None,
            evidence_archive_state=archive_state,
            finding_count=finding_count,
            evidence_count=evidence_count,
            skip_reason=(
                cycle.frontend_review_skip_reason if cycle is not None else None
            ),
        ),
        harness,
        attempt,
        events,
    )


def _stage_summary(
    key: str,
    *,
    run: DeliveryRun,
    cycle: DeliveryCycle | None,
    monitor: PRMonitorRun | None,
    frontend: DeliveryFrontendReviewProgress,
) -> str:
    if key == "planning":
        return (
            f"Cycle {cycle.cycle_number}: Plan version {cycle.plan_version_id}"
            if cycle is not None and cycle.plan_version_id is not None
            else f"Cycle {run.cycle_count or 1}: waiting for an approved plan"
        )
    if key == "coding":
        return f"{run.turn_count} Developer turn{'s' if run.turn_count != 1 else ''}"
    if key == "pre_review":
        return (
            f"Verdict: {cycle.review_verdict.replace('_', ' ')}"
            if cycle is not None and cycle.review_verdict
            else "Exact commit-range review"
        )
    if key == "frontend_review":
        if frontend.skip_reason:
            return frontend.skip_reason
        if frontend.verdict:
            return f"Browser verdict: {frontend.verdict}"
        if frontend.policy == "off":
            return "Disabled by Delivery policy"
        return "Black-box browser validation"
    if key == "publishing":
        return (
            f"Pull request #{run.pr_number}" if run.pr_number else run.delivery_branch
        )
    if monitor is not None:
        return f"Monitor #{monitor.id}: {monitor.status.replace('_', ' ')}"
    return "Waiting for exact-head CI and PR review"


def _stage_progress(
    *,
    run: DeliveryRun,
    cycle: DeliveryCycle | None,
    transitions: list[DeliveryTransition],
    monitor: PRMonitorRun | None,
    frontend: DeliveryFrontendReviewProgress,
) -> list[DeliveryStageProgress]:
    current_phase = run.phase
    terminal_state = None
    report_completed = False
    if run.phase == "done":
        last = transitions[-1] if transitions else None
        report_completed = bool(
            last is not None
            and last.cause == "report_completed"
            and run.outcome == "success"
        )
        before = (
            last.before_state if last and isinstance(last.before_state, dict) else {}
        )
        current_phase = before.get("phase", "monitoring")
        terminal_state = (
            "completed"
            if run.outcome == "success"
            else "cancelled"
            if run.outcome == "cancelled"
            else "failed"
        )
    current_index = (
        _STAGE_ORDER.index(current_phase) if current_phase in _STAGE_ORDER else 0
    )
    cycle_started = cycle.created_at if cycle is not None else run.created_at
    relevant = [item for item in transitions if item.created_at >= cycle_started]
    result: list[DeliveryStageProgress] = []
    for index, key in enumerate(_STAGE_ORDER):
        entered = [
            item.created_at
            for item in relevant
            if (
                isinstance(item.after_state, dict)
                and item.after_state.get("phase") == key
            )
        ]
        if key == "planning":
            entered.insert(0, cycle_started)
        exited = [
            item.created_at
            for item in relevant
            if (
                isinstance(item.before_state, dict)
                and item.before_state.get("phase") == key
                and isinstance(item.after_state, dict)
                and item.after_state.get("phase") != key
            )
        ]
        if key == "frontend_review" and frontend.skip_reason:
            state = "skipped"
        elif (
            key == "frontend_review"
            and frontend.policy == "off"
            and index > current_index
        ):
            state = "skipped"
        elif report_completed:
            state = "completed" if index <= _STAGE_ORDER.index("coding") else "skipped"
        elif terminal_state == "completed":
            state = "completed"
        elif terminal_state is not None:
            state = (
                "completed"
                if index < current_index
                else terminal_state
                if index == current_index
                else "pending"
            )
        elif index < current_index:
            state = "completed"
        elif index == current_index:
            state = run.activity
        else:
            state = "pending"
        result.append(
            DeliveryStageProgress(
                key=key,  # type: ignore[arg-type]
                label=_STAGE_LABELS[key],
                state=state,  # type: ignore[arg-type]
                summary=_stage_summary(
                    key,
                    run=run,
                    cycle=cycle,
                    monitor=monitor,
                    frontend=frontend,
                ),
                started_at=max(entered) if entered else None,
                completed_at=max(exited) if exited else None,
            )
        )
    return result


def _headline(
    run: DeliveryRun,
    *,
    plan_input: DeliveryPlanInputProjection | None,
    active_agent: DeliveryAgentActivity | None,
    frontend: DeliveryFrontendReviewProgress,
) -> tuple[str, str | None, bool, str | None]:
    if plan_input is not None:
        return (
            "Plan needs your decision",
            plan_input.request.reason,
            True,
            "plan_input",
        )
    if run.activity == "paused":
        return "Delivery is paused", run.pause_reason, True, "paused"
    if run.activity == "terminal":
        if run.outcome == "success":
            return "Delivery completed", run.pr_url, False, None
        return (
            f"Delivery {run.outcome or 'stopped'}",
            run.error_message,
            True,
            "terminal_error",
        )
    if active_agent is not None:
        return active_agent.headline, active_agent.detail, False, None
    if run.phase == "frontend_review" and frontend.skip_reason:
        return "Frontend review skipped", frontend.skip_reason, False, None
    labels = {
        "planning": "Preparing the implementation plan",
        "coding": "Waiting for the Developer turn",
        "pre_review": "Checking the exact code change",
        "frontend_review": "Preparing black-box frontend review",
        "publishing": "Publishing the pull request",
        "monitoring": "Waiting for CI and PR review",
    }
    detail = run.wait_reason.replace("_", " ") if run.wait_reason else None
    return labels.get(run.phase, "Delivery is running"), detail, False, None


async def build_delivery_progress(
    db: AsyncSession,
    run: DeliveryRun,
) -> DeliveryProgressResponse:
    cycles = list(
        (
            await db.execute(
                select(DeliveryCycle)
                .where(DeliveryCycle.run_id == run.id)
                .order_by(DeliveryCycle.cycle_number)
            )
        ).scalars()
    )
    cycle = next((item for item in cycles if item.id == run.current_cycle_id), None)
    transitions = list(
        (
            await db.execute(
                select(DeliveryTransition)
                .where(DeliveryTransition.run_id == run.id)
                .order_by(DeliveryTransition.state_version)
            )
        ).scalars()
    )
    turns = list(
        (
            await db.execute(
                select(DeliveryTurn)
                .where(DeliveryTurn.run_id == run.id)
                .order_by(DeliveryTurn.generation)
            )
        ).scalars()
    )
    task = await db.get(Task, run.developer_task_id) if run.developer_task_id else None
    plan = await _plan_projection(db, cycle)
    frontend, harness, harness_attempt, harness_events = await _frontend_projection(
        db,
        run,
        cycle,
    )
    monitor = (
        await db.get(PRMonitorRun, run.pr_monitor_run_id)
        if run.pr_monitor_run_id
        else None
    )

    active_agent: DeliveryAgentActivity | None = None
    task_logs: list[LogEntry] = []
    review_logs: list[LogEntry] = []
    if run.phase == "planning":
        active_agent = _plan_agent(plan)
    elif run.phase == "coding" and task is not None:
        current_turn = turns[-1] if turns else None
        task_logs = await _task_logs(
            db,
            task,
            retry_count=(current_turn.task_retry_count if current_turn else None),
            turn_generation=(current_turn.generation if current_turn else None),
        )
        active_agent = _task_agent(
            role="developer",
            task=task,
            logs=task_logs,
            status=task.status,
            headline="Developer is implementing the plan",
        )
    elif run.phase == "pre_review" and cycle is not None:
        review_run = (
            (
                await db.execute(
                    select(CodeReviewRun)
                    .where(
                        CodeReviewRun.capability_invocation_id
                        == cycle.review_invocation_id
                    )
                    .order_by(CodeReviewRun.attempt.desc())
                    .limit(1)
                )
            ).scalar_one_or_none()
            if cycle.review_invocation_id
            else None
        )
        reviewer = (
            await db.get(Task, review_run.reviewer_task_id) if review_run else None
        )
        if reviewer is not None and review_run is not None:
            review_logs = await _task_logs(
                db,
                reviewer,
                retry_count=review_run.reviewer_task_retry_count,
            )
            active_agent = _task_agent(
                role="code_reviewer",
                task=reviewer,
                logs=review_logs,
                status=review_run.status,
                headline="Code reviewer is checking the exact commit range",
            )
    elif run.phase == "frontend_review" and harness is not None:
        latest_event = harness_events[-1] if harness_events else None
        first_public = next(
            (
                item.created_at
                for item in harness_events
                if item.event_type in {"observation", "decision", "action", "result"}
            ),
            None,
        )
        active_agent = DeliveryAgentActivity(
            role="browser_reviewer",
            provider=harness_attempt.provider if harness_attempt else None,
            model=harness_attempt.model if harness_attempt else None,
            effort=harness_attempt.reasoning_effort if harness_attempt else None,
            service_tier=harness_attempt.codex_service_tier
            if harness_attempt
            else None,
            status=harness.status,
            activity_kind=(
                "completed"
                if harness.status in {"completed", "failed", "cancelled", "stale"}
                else "browser"
            ),
            headline="Browser reviewer is validating the user-visible flow",
            detail=latest_event.title if latest_event is not None else harness.stage,
            started_at=harness.started_at or harness.created_at,
            first_output_at=first_public,
            last_activity_at=(
                latest_event.created_at
                if latest_event is not None
                else harness.started_at
            ),
            output_chars=len(harness.report or ""),
        )

    timeline: list[DeliveryTimelineEvent] = []
    for transition in transitions:
        timeline.append(
            DeliveryTimelineEvent(
                id=f"transition:{transition.id}",
                stage=_transition_stage(transition),
                kind=transition.cause,
                source="delivery",
                title=_TRANSITION_TITLES.get(
                    transition.cause,
                    transition.cause.replace("_", " ").title(),
                ),
                detail=_transition_detail(transition),
                status=(
                    transition.after_state.get("activity")
                    if isinstance(transition.after_state, dict)
                    else None
                ),
                created_at=transition.created_at,
            )
        )
    for step in plan.steps or []:
        timeline.append(
            DeliveryTimelineEvent(
                id=f"plan-step:{step.id}",
                stage="planning",
                kind=f"plan_{step.step_type}",
                source="plan",
                title=("Plan reviewer" if step.step_type == "reviewer" else "Planner"),
                detail=" · ".join(
                    value
                    for value in (step.provider, step.model, step.route_slot)
                    if isinstance(value, str) and value
                ),
                status=step.status,
                created_at=step.started_at,
            )
        )
    for turn in turns:
        timeline.append(
            DeliveryTimelineEvent(
                id=f"turn:{turn.id}",
                stage="coding",
                kind="developer_turn",
                source="task",
                title=f"Developer turn {turn.generation}",
                detail=turn.last_error,
                status=turn.status,
                created_at=turn.started_at or turn.created_at,
            )
        )
    for event in harness_events:
        timeline.append(
            DeliveryTimelineEvent(
                id=f"harness:{event.id}",
                stage="frontend_review",
                kind=event.event_type,
                source="test_harness",
                title=event.title,
                detail=event.detail,
                status=event.stage,
                created_at=event.created_at,
            )
        )
    timeline.sort(key=lambda item: (item.created_at, item.id))
    timeline = timeline[-200:]

    headline, detail, attention, attention_kind = _headline(
        run,
        plan_input=plan.input_projection,
        active_agent=active_agent,
        frontend=frontend,
    )
    last_activity = _max_datetime(
        [
            # ``DeliveryRun.updated_at`` also changes on controller lease
            # renew/release, which is not user-visible progress. Use only
            # public lifecycle/agent/Monitor evidence so a stuck run does not
            # appear active merely because its controller is healthy.
            run.created_at,
            active_agent.last_activity_at if active_agent else None,
            timeline[-1].created_at if timeline else None,
            monitor.updated_at if monitor is not None else None,
        ]
    )
    return DeliveryProgressResponse(
        run_id=run.id,
        state_version=run.state_version,
        phase=run.phase,
        activity=run.activity,
        headline=headline,
        detail=detail,
        attention_required=attention,
        attention_kind=attention_kind,
        last_activity_at=last_activity,
        stages=_stage_progress(
            run=run,
            cycle=cycle,
            transitions=transitions,
            monitor=monitor,
            frontend=frontend,
        ),
        active_agent=active_agent,
        events=timeline,
        plan_id=plan.plan_run.plan_id if plan.plan_run is not None else None,
        plan_input=plan.input_projection,
        frontend_review=frontend,
    )
