"""Durable Manager-side state for Worker-owned Plan Run dispatch.

The receipt separates a provably pre-import claim from a request whose HTTP
ack may have been lost.  Recovery may requeue only ``prepared``.  Once a
receipt is ``remote_possible``, callers must use the Worker's read-only exact
identity audit and may never repeat the mutating import blindly.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Sequence

from sqlalchemy import or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.instance import Instance
from backend.models.plan import Plan, PlanInputRequest, PlanVersion
from backend.models.plan_agent import (
    PlanAgentRun,
    PlanAgentRuntimeReceipt,
    PlanAgentStep,
    PlanAgentWorkerDispatchReceipt,
)


WORKER_PLAN_DISPATCH_PROTOCOL = 1
_TERMINAL_RUN_STATUSES = frozenset({"completed", "failed", "cancelled"})
_TERMINAL_STEP_STATUSES = frozenset({"completed", "failed", "cancelled"})


class WorkerPlanDispatchConflict(RuntimeError):
    """The receipt no longer describes the exact active aggregate."""


@dataclass(frozen=True)
class WorkerPlanDispatchSnapshot:
    id: int
    plan_id: int
    run_id: int
    target_task_id: int | None
    worker_id: int
    run_generation: int
    status: str
    payload_digest: str | None


def snapshot_worker_dispatch_receipt(
    receipt: PlanAgentWorkerDispatchReceipt,
) -> WorkerPlanDispatchSnapshot:
    _validate_receipt_shape(receipt)
    return WorkerPlanDispatchSnapshot(
        id=receipt.id,
        plan_id=receipt.plan_id,
        run_id=receipt.run_id,
        target_task_id=receipt.target_task_id,
        worker_id=receipt.worker_id,
        run_generation=receipt.run_generation,
        status=receipt.status,
        payload_digest=receipt.payload_digest,
    )


def _valid_digest(value: str | None) -> bool:
    return bool(
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _require_exact_int(
    resource: dict,
    field: str,
    *,
    optional: bool = False,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int | None:
    """Read one JSON integer without accepting Pydantic coercions or bools."""

    value = resource.get(field)
    if value is None and optional:
        return None
    if type(value) is not int:
        raise WorkerPlanDispatchConflict(
            f"Worker terminal Plan field {field} is not an exact integer"
        )
    if minimum is not None and value < minimum:
        raise WorkerPlanDispatchConflict(
            f"Worker terminal Plan field {field} is outside its valid range"
        )
    if maximum is not None and value > maximum:
        raise WorkerPlanDispatchConflict(
            f"Worker terminal Plan field {field} is outside its valid range"
        )
    return value


def _is_finite_nonnegative_number(value: object) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return value >= 0 and math.isfinite(value)
    except OverflowError:
        return False


def validate_worker_plan_outcome_graph(
    payload: dict,
    *,
    plan_id: int,
    run_id: int,
    require_version_closure: bool = True,
) -> None:
    """Validate exact raw JSON identity and references for a Worker outcome.

    Plan resource schemas intentionally accept ordinary API coercions.  A
    Worker readback is instead an immutable identity proof, so its raw JSON
    ids and every available child edge are checked before schema parsing or
    any answer replay side effect.
    """

    if not isinstance(payload, dict) or payload.get("protocol") != 3:
        raise WorkerPlanDispatchConflict(
            "Worker Plan terminal cancellation-race payload is invalid"
        )
    remote = payload.get("run")
    versions = payload.get("versions")
    base_worker_version_id = payload.get("base_worker_version_id")
    if (
        not isinstance(remote, dict)
        or not isinstance(versions, list)
        or (
            base_worker_version_id is not None
            and (
                type(base_worker_version_id) is not int
                or base_worker_version_id <= 0
            )
        )
    ):
        raise WorkerPlanDispatchConflict(
            "Worker Plan terminal cancellation-race payload is invalid"
        )

    if (
        _require_exact_int(remote, "id", minimum=1) != run_id
        or _require_exact_int(remote, "plan_id", minimum=1) != plan_id
        or remote.get("status")
        not in {"queued", "running", "waiting_user", "completed", "failed", "cancelled"}
    ):
        raise WorkerPlanDispatchConflict(
            "Worker Plan terminal cancellation-race Run identity is invalid"
        )
    run_round = _require_exact_int(remote, "round", minimum=1)
    run_generation = _require_exact_int(remote, "generation", minimum=0)
    interaction_count = _require_exact_int(
        remote,
        "interaction_count",
        minimum=0,
    )
    max_interactions = _require_exact_int(
        remote,
        "max_interactions",
        minimum=0,
        maximum=5,
    )
    if interaction_count > max_interactions:
        raise WorkerPlanDispatchConflict(
            "Worker Plan interaction counters are inconsistent"
        )
    for field in (
        "source_run_id",
        "base_version_id",
        "result_version_id",
        "draft_step_id",
        "instance_id",
        "worker_id",
        "open_input_request_id",
    ):
        _require_exact_int(remote, field, optional=True, minimum=1)
    if remote.get("worker_id") is not None:
        raise WorkerPlanDispatchConflict(
            "Worker terminal Plan Run must be Worker-local"
        )
    if type(remote.get("review_exhausted")) is not bool:
        raise WorkerPlanDispatchConflict(
            "Worker Plan review exhaustion marker is invalid"
        )
    execution_seconds = remote.get("execution_seconds")
    if not _is_finite_nonnegative_number(execution_seconds):
        raise WorkerPlanDispatchConflict(
            "Worker Plan terminal execution time is invalid"
        )
    if remote.get("base_version_id") != base_worker_version_id:
        raise WorkerPlanDispatchConflict(
            "Worker Plan terminal base Version identity is invalid"
        )
    remote_status = remote["status"]
    is_terminal = remote_status in {"completed", "failed", "cancelled"}

    raw_steps = remote.get("steps", [])
    raw_inputs = remote.get("input_requests", [])
    if not isinstance(raw_steps, list) or not isinstance(raw_inputs, list):
        raise WorkerPlanDispatchConflict(
            "Worker Plan terminal child collections are invalid"
        )
    if len(raw_inputs) != interaction_count:
        raise WorkerPlanDispatchConflict(
            "Worker Plan interaction count does not match its InputRequests"
        )

    step_ids: set[int] = set()
    steps_by_id: dict[int, dict] = {}
    step_version_refs: list[int] = []
    step_input_refs: list[int] = []
    step_input_by_id: dict[int, int | None] = {}
    for item in raw_steps:
        if not isinstance(item, dict):
            raise WorkerPlanDispatchConflict(
                "Worker Plan terminal Step is invalid"
            )
        step_id = _require_exact_int(item, "id", minimum=1)
        if (
            step_id in step_ids
            or _require_exact_int(item, "run_id", minimum=1) != run_id
            or _require_exact_int(item, "plan_id", minimum=1) != plan_id
            or item.get("step_type") not in {"planner", "reviewer"}
            or item.get("status")
            not in {"running", "completed", "failed", "cancelled"}
        ):
            raise WorkerPlanDispatchConflict(
                "Worker Plan terminal Step identity is invalid"
            )
        step_ids.add(step_id)
        steps_by_id[step_id] = item
        step_round = _require_exact_int(item, "round", minimum=1)
        step_generation = _require_exact_int(item, "generation", minimum=0)
        if step_round > run_round or step_generation > run_generation:
            raise WorkerPlanDispatchConflict(
                "Worker Plan Step counters exceed their Run"
            )
        if "streamed_output_chars" in item:
            _require_exact_int(item, "streamed_output_chars", minimum=0)
        version_ref = _require_exact_int(
            item,
            "plan_version_id",
            optional=True,
            minimum=1,
        )
        input_ref = _require_exact_int(
            item,
            "input_request_id",
            optional=True,
            minimum=1,
        )
        step_input_by_id[step_id] = input_ref
        if version_ref is not None:
            step_version_refs.append(version_ref)
        if input_ref is not None:
            step_input_refs.append(input_ref)

    version_ids: set[int] = set()
    version_numbers: set[int] = set()
    version_edges: list[tuple[str, int]] = []
    for item in versions:
        if not isinstance(item, dict):
            raise WorkerPlanDispatchConflict(
                "Worker Plan terminal Version is invalid"
            )
        version_id = _require_exact_int(item, "id", minimum=1)
        version_number = _require_exact_int(item, "version_number", minimum=1)
        if (
            version_id in version_ids
            or version_number in version_numbers
            or _require_exact_int(item, "plan_id", minimum=1) != plan_id
            or _require_exact_int(
                item,
                "produced_by_run_id",
                minimum=1,
            )
            != run_id
            or type(item.get("review_exhausted")) is not bool
            or type(item.get("applied")) is not bool
        ):
            raise WorkerPlanDispatchConflict(
                "Worker Plan terminal Version identity is invalid"
        )
        version_ids.add(version_id)
        version_numbers.add(version_number)
        for field in (
            "parent_version_id",
            "produced_by_step_id",
            "context_log_id",
            "reviewed_by_step_id",
            "decided_by",
            "superseded_by_version_id",
        ):
            reference = _require_exact_int(
                item,
                field,
                optional=True,
                minimum=1,
            )
            if reference is not None and field not in {"context_log_id", "decided_by"}:
                version_edges.append((field, reference))

    input_ids: set[int] = set()
    input_statuses: dict[int, str | None] = {}
    input_source_steps: list[int] = []
    input_source_by_id: dict[int, int] = {}
    for item in raw_inputs:
        if not isinstance(item, dict):
            raise WorkerPlanDispatchConflict(
                "Worker Plan terminal InputRequest is invalid"
            )
        input_id = _require_exact_int(item, "id", minimum=1)
        if (
            input_id in input_ids
            or _require_exact_int(item, "plan_id", minimum=1) != plan_id
            or _require_exact_int(item, "run_id", minimum=1) != run_id
            or item.get("status")
            not in {"prepared", "open", "answered", "cancelled"}
        ):
            raise WorkerPlanDispatchConflict(
                "Worker Plan terminal InputRequest identity is invalid"
        )
        input_ids.add(input_id)
        input_statuses[input_id] = item.get("status")
        if is_terminal and item.get("status") not in {"answered", "cancelled"}:
            raise WorkerPlanDispatchConflict(
                "Worker terminal Plan has a non-terminal InputRequest"
            )
        source_step_id = _require_exact_int(
            item,
            "source_step_id",
            minimum=1,
        )
        input_source_steps.append(source_step_id)
        input_source_by_id[input_id] = source_step_id
        _require_exact_int(item, "answered_by", optional=True, minimum=1)

    result_version_id = remote.get("result_version_id")
    open_input_request_id = remote.get("open_input_request_id")
    if (
        (
            require_version_closure
            and result_version_id is not None
            and result_version_id not in version_ids
        )
        or (remote_status == "completed" and result_version_id is None)
        or (
            remote.get("draft_step_id") is not None
            and remote.get("draft_step_id") not in step_ids
        )
        or (
            require_version_closure
            and any(reference not in version_ids for reference in step_version_refs)
        )
        or any(reference not in input_ids for reference in step_input_refs)
        or any(reference not in step_ids for reference in input_source_steps)
        or (
            remote_status == "waiting_user"
            and (
                open_input_request_id is None
                or input_statuses.get(open_input_request_id) != "open"
            )
        )
        or (remote_status != "waiting_user" and open_input_request_id is not None)
    ):
        raise WorkerPlanDispatchConflict(
            "Worker Plan terminal child graph has a dangling reference"
        )
    if is_terminal and any(
        item.get("status") not in _TERMINAL_STEP_STATUSES for item in raw_steps
    ):
        raise WorkerPlanDispatchConflict(
            "Worker terminal Plan has a non-terminal Step"
        )
    if is_terminal and (
        remote.get("finished_at") is None
        or remote.get("last_execution_started_at") is not None
        or remote.get("instance_id") is not None
        or remote.get("open_input_request_id") is not None
        or any(item.get("finished_at") is None for item in raw_steps)
    ):
        raise WorkerPlanDispatchConflict(
            "Worker terminal Plan Run lifecycle is incomplete"
        )
    if any(
        input_source_by_id.get(input_id) != step_id
        for step_id, input_id in step_input_by_id.items()
        if input_id is not None
    ) or any(
        step_input_by_id.get(step_id) != input_id
        for input_id, step_id in input_source_by_id.items()
    ):
        raise WorkerPlanDispatchConflict(
            "Worker Plan Step and InputRequest edges are inconsistent"
        )
    if any(
        item.get("requested_by")
        != steps_by_id[input_source_by_id[item["id"]]].get("step_type")
        for item in raw_inputs
    ):
        raise WorkerPlanDispatchConflict(
            "Worker Plan InputRequest source type is inconsistent"
        )
    for field, reference in version_edges:
        if field == "parent_version_id":
            if (
                require_version_closure
                and reference != base_worker_version_id
                and reference not in version_ids
            ):
                raise WorkerPlanDispatchConflict(
                    "Worker Plan terminal Version parent is invalid"
                )
        elif field in {"produced_by_step_id", "reviewed_by_step_id"}:
            if reference not in step_ids:
                raise WorkerPlanDispatchConflict(
                    "Worker Plan terminal Version Step edge is invalid"
                )
        elif (
            field == "superseded_by_version_id"
            and require_version_closure
            and reference not in version_ids
        ):
            raise WorkerPlanDispatchConflict(
                "Worker Plan terminal Version successor is invalid"
            )


def validate_worker_terminal_outcome_graph(
    payload: dict,
    *,
    plan_id: int,
    run_id: int,
) -> None:
    """Require a complete immutable terminal graph after an exact cancel race."""

    validate_worker_plan_outcome_graph(
        payload,
        plan_id=plan_id,
        run_id=run_id,
    )
    remote = payload["run"]
    if remote.get("status") not in {"completed", "failed", "cancelled"}:
        raise WorkerPlanDispatchConflict(
            "Worker Plan terminal cancellation-race status is invalid"
        )
    if remote["status"] == "cancelled":
        # The common graph validator above proves that every Step/Input is
        # terminal and every available edge closes.  Cancellation may win in
        # any stage, but it cannot publish a Version: Version creation and a
        # completed Run are one Worker transaction.  Reject a damaged or
        # malicious Worker that tries to advance Manager Plan state through
        # the cancellation path.
        if (
            remote.get("result_version_id") is not None
            or payload["versions"]
            or any(
                step.get("plan_version_id") is not None
                for step in remote.get("steps", [])
            )
        ):
            raise WorkerPlanDispatchConflict(
                "Worker cancelled Plan unexpectedly published a result"
            )
        return
    if remote["status"] == "failed":
        if (
            remote.get("error") is None
            or remote.get("result_version_id") is not None
            or payload["versions"]
            or any(
                step.get("plan_version_id") is not None
                for step in remote.get("steps", [])
            )
        ):
            raise WorkerPlanDispatchConflict(
                "Worker failed Plan unexpectedly published a result"
            )
        return

    result_version_id = remote["result_version_id"]
    base_worker_version_id = payload.get("base_worker_version_id")
    versions_by_id = {item["id"]: item for item in payload["versions"]}
    steps_by_id = {item["id"]: item for item in remote.get("steps", [])}
    result_version = versions_by_id.get(result_version_id)
    if (
        remote.get("current_stage") != "complete"
        or remote.get("error") is not None
        or len(versions_by_id) != 1
        or result_version is None
        or result_version_id == base_worker_version_id
        or result_version.get("parent_version_id") != base_worker_version_id
        or (
            base_worker_version_id is None
            and result_version.get("version_number") != 1
        )
    ):
        raise WorkerPlanDispatchConflict(
            "Worker completed Plan has no exact result Version"
        )
    produced_by_step_id = result_version.get("produced_by_step_id")
    planner_step = steps_by_id.get(produced_by_step_id)
    if (
        planner_step is None
        or planner_step.get("step_type") != "planner"
        or planner_step.get("status") != "completed"
        or planner_step.get("round") != remote.get("round")
        or planner_step.get("plan_version_id") != result_version_id
        or remote.get("draft_step_id") != produced_by_step_id
        or remote.get("draft_content") != result_version.get("content")
        or not isinstance(result_version.get("content"), str)
        or not result_version["content"].strip()
        or remote.get("draft_repo_revision")
        != result_version.get("repo_revision")
        or result_version.get("superseded_by_version_id") is not None
    ):
        raise WorkerPlanDispatchConflict(
            "Worker completed Plan result Version has no completed Planner Step"
        )
    if any(
        step.get("plan_version_id") is not None
        and versions_by_id.get(step["plan_version_id"], {}).get(
            "produced_by_step_id"
        )
        != step_id
        for step_id, step in steps_by_id.items()
    ) or any(
        version.get("produced_by_step_id") is not None
        and steps_by_id.get(version["produced_by_step_id"], {}).get(
            "plan_version_id"
        )
        != version_id
        for version_id, version in versions_by_id.items()
    ):
        raise WorkerPlanDispatchConflict(
            "Worker Plan Step and Version edges are inconsistent"
        )

    reviewed_by_step_id = result_version.get("reviewed_by_step_id")
    reviewer_step = None
    if reviewed_by_step_id is not None:
        reviewer_step = steps_by_id.get(reviewed_by_step_id)
        if (
            reviewer_step is None
            or reviewer_step.get("step_type") != "reviewer"
            or reviewer_step.get("status") != "completed"
            or reviewer_step.get("round") != remote.get("round")
            or reviewer_step.get("generation") != remote.get("generation")
            or reviewer_step.get("plan_version_id") is not None
            or planner_step.get("generation") >= reviewer_step.get("generation")
        ):
            raise WorkerPlanDispatchConflict(
                "Worker completed Plan result Version has an invalid Reviewer Step"
            )
    version_exhausted = result_version.get("review_exhausted")
    run_exhausted = remote.get("review_exhausted")
    version_verdict = result_version.get("review_verdict")
    run_verdict = remote.get("review_verdict")
    disabled = (
        version_verdict == "disabled"
        and run_verdict == "disabled"
        and version_exhausted is False
        and run_exhausted is False
        and reviewed_by_step_id is None
        and planner_step.get("generation") == remote.get("generation")
    )
    approved = (
        version_verdict == "approve"
        and run_verdict == "approve"
        and version_exhausted is False
        and run_exhausted is False
        and reviewer_step is not None
    )
    exhausted = (
        version_verdict == "exhausted"
        and run_verdict == "revise"
        and version_exhausted is True
        and run_exhausted is True
        and reviewer_step is not None
    )
    if (
        result_version.get("human_decision") != "pending"
        or result_version.get("decided_at") is not None
        or result_version.get("decided_by") is not None
        or result_version.get("applied") is not False
        or result_version.get("reviewed_at") is None
        or result_version.get("reviewed_at") != remote.get("finished_at")
        or result_version.get("review_feedback")
        != remote.get("review_feedback")
        or not (disabled or approved or exhausted)
    ):
        raise WorkerPlanDispatchConflict(
            "Worker completed Plan result Version review state is inconsistent"
        )


def _validate_receipt_shape(
    receipt: PlanAgentWorkerDispatchReceipt,
) -> None:
    if receipt.protocol != WORKER_PLAN_DISPATCH_PROTOCOL:
        raise WorkerPlanDispatchConflict(
            "Worker Plan dispatch receipt protocol is invalid"
        )
    if receipt.status == "prepared":
        valid = (
            receipt.payload_digest is None
            and receipt.remote_status is None
            and receipt.settlement_reason is None
            and receipt.settled_at is None
        )
    elif receipt.status == "remote_possible":
        valid = (
            _valid_digest(receipt.payload_digest)
            and receipt.remote_status is None
            and receipt.settlement_reason is None
            and receipt.settled_at is None
        )
    elif receipt.status == "settled":
        valid = receipt.settled_at is not None and (
            (
                receipt.settlement_reason
                in {"not_launched", "preflight_failed"}
                and receipt.payload_digest is None
                and receipt.remote_status is None
            )
            or (
                receipt.settlement_reason == "remote_cancelled"
                and (
                    receipt.payload_digest is None
                    or _valid_digest(receipt.payload_digest)
                )
                and receipt.remote_status == "cancelled"
            )
            or (
                receipt.settlement_reason == "remote_pause"
                and _valid_digest(receipt.payload_digest)
                and receipt.remote_status
                in {"waiting_user", "completed", "failed", "cancelled"}
            )
            or (
                receipt.settlement_reason == "remote_absent"
                and _valid_digest(receipt.payload_digest)
                and receipt.remote_status is None
            )
            or (
                receipt.settlement_reason == "identity_conflict"
                and _valid_digest(receipt.payload_digest)
                and receipt.remote_status == "conflict"
            )
            or (
                receipt.settlement_reason == "legacy_terminal"
                and receipt.payload_digest is None
                and receipt.remote_status
                in {"completed", "failed", "cancelled"}
            )
        )
    else:
        valid = False
    if not valid:
        raise WorkerPlanDispatchConflict(
            "Worker Plan dispatch receipt state is malformed"
        )


def _worker_dispatch_history_advanced(
    receipt: PlanAgentWorkerDispatchReceipt,
) -> bool:
    """Return whether a settled generation can legitimately have a successor."""

    return (
        receipt.settlement_reason in {"not_launched", "remote_absent"}
        and receipt.remote_status is None
    ) or (
        receipt.settlement_reason == "remote_pause"
        and receipt.remote_status == "waiting_user"
    )


def _worker_dispatch_terminal_matches(
    receipt: PlanAgentWorkerDispatchReceipt,
    run: PlanAgentRun,
) -> bool:
    """Match the last immutable dispatch outcome to the mirrored Run terminal."""

    outcome = (receipt.settlement_reason, receipt.remote_status)
    if receipt.settlement_reason == "legacy_terminal":
        return receipt.remote_status == run.status
    if run.status == "completed":
        return outcome == ("remote_pause", "completed")
    if run.status == "failed":
        return outcome in {
            ("remote_pause", "failed"),
            ("preflight_failed", None),
            ("identity_conflict", "conflict"),
        }
    if run.status == "cancelled":
        return outcome == ("remote_pause", "cancelled")
    return False


def worker_mirror_cleanup_is_clean(
    *,
    plan: Plan,
    run: PlanAgentRun,
    steps: Sequence[PlanAgentStep],
    input_requests: Sequence[PlanInputRequest],
    versions: Sequence[PlanVersion],
    runtime_receipts: Sequence[PlanAgentRuntimeReceipt],
    dispatch_receipts: Sequence[PlanAgentWorkerDispatchReceipt],
) -> bool:
    """Return whether a Manager-side Worker Run has exact cleanup evidence.

    Worker Steps are read-only mirrors of provider attempts executed on the
    Worker.  The Manager must never invent local process receipts for them.
    Their authoritative evidence is the immutable imported Step identity plus
    the settled dispatch outcome for that generation.  The Worker performs
    its own local runtime-receipt validation before a remote delete can
    succeed.
    """

    worker_id = run.worker_id
    if (
        worker_id is None
        or isinstance(worker_id, bool)
        or not isinstance(worker_id, int)
        or worker_id <= 0
        or plan.id is None
        or run.id is None
        or run.plan_id != plan.id
        or plan.worker_id != worker_id
        or plan.active_run_id is not None
        or run.run_type == "capability"
        or run.capability_execution_id is not None
        or run.status not in _TERMINAL_RUN_STATUSES
        or isinstance(run.generation, bool)
        or not isinstance(run.generation, int)
        or run.generation < 0
        or run.instance_id is not None
        or run.last_execution_started_at is not None
        or run.open_input_request_id is not None
        or run.finished_at is None
        or isinstance(run.interaction_count, bool)
        or not isinstance(run.interaction_count, int)
        or run.interaction_count < 0
        or isinstance(run.max_interactions, bool)
        or not isinstance(run.max_interactions, int)
        or run.max_interactions < 0
        or run.interaction_count > run.max_interactions
    ):
        return False

    receipts_by_generation: dict[int, PlanAgentWorkerDispatchReceipt] = {}
    for receipt in dispatch_receipts:
        try:
            _validate_receipt_shape(receipt)
        except WorkerPlanDispatchConflict:
            return False
        if (
            receipt.plan_id != plan.id
            or receipt.run_id != run.id
            or receipt.target_task_id != plan.target_task_id
            or receipt.worker_id != worker_id
            or receipt.protocol != WORKER_PLAN_DISPATCH_PROTOCOL
            or receipt.status != "settled"
            or isinstance(receipt.run_generation, bool)
            or not isinstance(receipt.run_generation, int)
            or receipt.run_generation < 0
            or receipt.run_generation > run.generation
            or receipt.run_generation in receipts_by_generation
        ):
            return False
        receipts_by_generation[receipt.run_generation] = receipt

    # A migration-only legacy proof is deliberately not mixed with exact HTTP
    # receipts: it records that the pre-receipt schema already held one
    # terminal mirror, while the authoritative Worker DELETE still validates
    # the remote runtime before Manager deletion.
    receipt_generations = set(receipts_by_generation)
    legacy_receipts = [
        receipt
        for receipt in dispatch_receipts
        if receipt.settlement_reason == "legacy_terminal"
    ]
    if legacy_receipts:
        if (
            len(dispatch_receipts) != 1
            or len(legacy_receipts) != 1
            or legacy_receipts[0].run_generation != run.generation
            or run.cancellation_target_generation is not None
            or not _worker_dispatch_terminal_matches(legacy_receipts[0], run)
        ):
            return False
        historical_receipts: Sequence[PlanAgentWorkerDispatchReceipt] = ()
        imported_boundary = True
        legacy_proof = True
    elif run.status == "cancelled" and run.cancellation_target_generation is not None:
        legacy_proof = False
        # An exact Worker cancellation ACK advances Manager G to G+1. Input
        # answering may itself advance G before the dispatcher creates a new
        # receipt, so cancellation history may contain honest gaps (including
        # no receipt at all before first dispatch). The durable target marker,
        # not an inferred counter offset, authenticates that shape.
        target_generation = run.cancellation_target_generation
        if (
            isinstance(target_generation, bool)
            or not isinstance(target_generation, int)
            or target_generation < 0
            or run.generation != target_generation + 1
            or any(
                generation > target_generation + 1
                for generation in receipt_generations
            )
        ):
            return False
        target_receipt = receipts_by_generation.get(target_generation)
        successor_receipt = receipts_by_generation.get(target_generation + 1)
        if successor_receipt is not None:
            # G already settled as waiting_user before exact cancellation.
            # Preserve that immutable pause and record the cancellation
            # readback as G+1; the Run itself is also G+1 with target marker G.
            if (
                target_receipt is None
                or (
                    target_receipt.settlement_reason,
                    target_receipt.remote_status,
                )
                != ("remote_pause", "waiting_user")
                or (
                    successor_receipt.settlement_reason,
                    successor_receipt.remote_status,
                )
                != ("remote_pause", "cancelled")
            ):
                return False
            historical_receipts = tuple(
                receipt
                for generation, receipt in sorted(receipts_by_generation.items())
                if generation <= target_generation
            )
        else:
            if target_receipt is not None and (
                target_receipt.settlement_reason,
                target_receipt.remote_status,
            ) not in {
                ("not_launched", None),
                ("remote_absent", None),
                ("remote_pause", "waiting_user"),
                ("remote_pause", "cancelled"),
            }:
                return False
            historical_receipts = tuple(
                receipt
                for generation, receipt in sorted(receipts_by_generation.items())
                if generation < target_generation
            )
        imported_boundary = any(
            receipt.settlement_reason == "remote_pause"
            for receipt in dispatch_receipts
        )
    else:
        legacy_proof = False
        if run.cancellation_target_generation is not None:
            return False
        # New, non-ACK terminals have one receipt for every Manager claim from
        # zero through the terminal generation. Older schemas are represented
        # only by the explicit legacy_terminal branch above.
        if receipt_generations != set(range(run.generation + 1)):
            return False
        terminal_receipt = receipts_by_generation[run.generation]
        if not _worker_dispatch_terminal_matches(terminal_receipt, run):
            return False
        historical_receipts = tuple(
            receipts_by_generation[generation]
            for generation in range(run.generation)
        )
        imported_boundary = any(
            receipt.settlement_reason == "remote_pause"
            for receipt in dispatch_receipts
        )

    if not all(
        _worker_dispatch_history_advanced(receipt)
        for receipt in historical_receipts
    ):
        return False

    # A Manager-side runtime receipt for a Worker mirror is contradictory:
    # either the Run was executed locally despite its Worker owner or a proof
    # was fabricated from remote metadata.  Both cases must remain blocked.
    if runtime_receipts:
        return False

    remote_step_ids: set[int] = set()
    steps_by_id: dict[int, PlanAgentStep] = {}
    for step in steps:
        if (
            isinstance(step.id, bool)
            or not isinstance(step.id, int)
            or step.id <= 0
            or step.run_id != run.id
            or step.plan_id != plan.id
            or step.worker_id != worker_id
            or isinstance(step.worker_step_id, bool)
            or not isinstance(step.worker_step_id, int)
            or step.worker_step_id <= 0
            or step.worker_step_id in remote_step_ids
            or isinstance(step.generation, bool)
            or not isinstance(step.generation, int)
            or step.generation < 0
            or step.provider not in {"claude", "codex"}
            or step.status not in _TERMINAL_STEP_STATUSES
            or step.started_at is None
            or step.finished_at is None
        ):
            return False
        remote_step_ids.add(step.worker_step_id)
        steps_by_id[step.id] = step

    remote_input_ids: set[int] = set()
    inputs_by_id: dict[int, PlanInputRequest] = {}
    input_source_ids: set[int] = set()
    for input_request in input_requests:
        source = steps_by_id.get(input_request.source_step_id)
        if (
            isinstance(input_request.id, bool)
            or not isinstance(input_request.id, int)
            or input_request.id <= 0
            or input_request.run_id != run.id
            or input_request.plan_id != plan.id
            or input_request.worker_id != worker_id
            or isinstance(input_request.worker_input_request_id, bool)
            or not isinstance(input_request.worker_input_request_id, int)
            or input_request.worker_input_request_id <= 0
            or input_request.worker_input_request_id in remote_input_ids
            or source is None
            or input_request.source_step_id in input_source_ids
            or source.input_request_id != input_request.id
            or source.step_type != input_request.requested_by
            or input_request.status not in {"answered", "cancelled"}
            or input_request.opened_at is None
            or (
                input_request.status == "answered"
                and input_request.answered_at is None
            )
        ):
            return False
        remote_input_ids.add(input_request.worker_input_request_id)
        input_source_ids.add(input_request.source_step_id)
        inputs_by_id[input_request.id] = input_request

    if any(
        step.input_request_id is not None
        and (
            step.input_request_id not in inputs_by_id
            or inputs_by_id[step.input_request_id].source_step_id != step.id
        )
        for step in steps
    ):
        return False

    if not legacy_proof and len(input_requests) != run.interaction_count:
        return False

    remote_version_ids: set[int] = set()
    versions_by_id: dict[int, PlanVersion] = {}
    for version in versions:
        producer = steps_by_id.get(version.produced_by_step_id)
        reviewer = (
            steps_by_id.get(version.reviewed_by_step_id)
            if version.reviewed_by_step_id is not None
            else None
        )
        if (
            isinstance(version.id, bool)
            or not isinstance(version.id, int)
            or version.id <= 0
            or version.plan_id != plan.id
            or version.worker_id != worker_id
            or isinstance(version.worker_version_id, bool)
            or not isinstance(version.worker_version_id, int)
            or version.worker_version_id <= 0
            or version.worker_version_id in remote_version_ids
            or version.produced_by_run_id != run.id
            or producer is None
            or producer.plan_version_id != version.id
            or producer.step_type != "planner"
            or (
                version.reviewed_by_step_id is not None
                and (
                    reviewer is None
                    or reviewer.step_type != "reviewer"
                )
            )
        ):
            return False
        remote_version_ids.add(version.worker_version_id)
        versions_by_id[version.id] = version

    if any(
        step.plan_version_id is not None
        and (
            step.plan_version_id not in versions_by_id
            or versions_by_id[step.plan_version_id].produced_by_step_id != step.id
        )
        for step in steps
    ):
        return False

    draft_step = (
        steps_by_id.get(run.draft_step_id)
        if run.draft_step_id is not None
        else None
    )
    if run.draft_step_id is not None and (
        draft_step is None or draft_step.step_type != "planner"
    ):
        return False
    if not legacy_proof:
        if run.status == "completed":
            result = versions_by_id.get(run.result_version_id)
            if (
                len(versions) != 1
                or result is None
                or draft_step is None
                or result.produced_by_step_id != draft_step.id
                or draft_step.plan_version_id != result.id
                or draft_step.status != "completed"
                or run.draft_content != result.content
                or run.draft_repo_revision != result.repo_revision
            ):
                return False
        elif (
            run.result_version_id is not None
            or versions
            or any(step.plan_version_id is not None for step in steps)
        ):
            return False

    had_waiting_user_import = any(
        receipt.settlement_reason == "remote_pause"
        and receipt.remote_status == "waiting_user"
        for receipt in dispatch_receipts
    )
    # A waiting_user readback can only be accepted with one exact open
    # InputRequest and its source Step. Those rows are durable imported
    # evidence even after a later answer/cancel turns the Input terminal; if
    # either collection disappears, the Worker still holds the only complete
    # graph and node destruction must fail closed. A completed outcome likewise
    # always has at least its producing Planner Step.
    if (
        (had_waiting_user_import and (not steps or not input_requests))
        or (run.status == "completed" and not steps)
    ):
        return False
    # Worker Step generations belong to the Worker's local Run and are not
    # comparable with Manager claim generations. Their mirror can only have
    # entered this database through a remote_pause outcome (or the explicit
    # pre-receipt migration proof), so require that import boundary without
    # fabricating a cross-domain numeric equality.
    return not steps or imported_boundary


async def worker_mirror_run_is_clean(
    db: AsyncSession,
    *,
    run_id: int,
) -> bool:
    """Load and validate one complete Manager-side Worker Run proof."""

    run = await db.get(PlanAgentRun, run_id, populate_existing=True)
    if run is None or run.plan_id is None or run.worker_id is None:
        return False
    plan = await db.get(Plan, run.plan_id, populate_existing=True)
    if plan is None:
        return False
    steps = list(
        (
            await db.execute(
                select(PlanAgentStep)
                .where(PlanAgentStep.run_id == run.id)
                .order_by(PlanAgentStep.id)
            )
        ).scalars()
    )
    input_requests = list(
        (
            await db.execute(
                select(PlanInputRequest)
                .where(PlanInputRequest.run_id == run.id)
                .order_by(PlanInputRequest.id)
            )
        ).scalars()
    )
    versions = list(
        (
            await db.execute(
                select(PlanVersion)
                .where(PlanVersion.produced_by_run_id == run.id)
                .order_by(PlanVersion.version_number, PlanVersion.id)
            )
        ).scalars()
    )
    step_ids = {step.id for step in steps}
    runtime_predicates = [PlanAgentRuntimeReceipt.run_id == run.id]
    if step_ids:
        runtime_predicates.append(
            PlanAgentRuntimeReceipt.step_id.in_(sorted(step_ids))
        )
    runtime_receipts = list(
        (
            await db.execute(
                select(PlanAgentRuntimeReceipt)
                .where(or_(*runtime_predicates))
                .order_by(PlanAgentRuntimeReceipt.id)
            )
        ).scalars()
    )
    dispatch_receipts = list(
        (
            await db.execute(
                select(PlanAgentWorkerDispatchReceipt)
                .where(PlanAgentWorkerDispatchReceipt.run_id == run.id)
                .order_by(
                    PlanAgentWorkerDispatchReceipt.run_generation,
                    PlanAgentWorkerDispatchReceipt.id,
                )
            )
        ).scalars()
    )
    return worker_mirror_cleanup_is_clean(
        plan=plan,
        run=run,
        steps=steps,
        input_requests=input_requests,
        versions=versions,
        runtime_receipts=runtime_receipts,
        dispatch_receipts=dispatch_receipts,
    )


def new_prepared_worker_dispatch_receipt(
    *,
    plan: Plan,
    run: PlanAgentRun,
) -> PlanAgentWorkerDispatchReceipt:
    if (
        plan.id is None
        or run.id is None
        or run.plan_id != plan.id
        or run.worker_id is None
        or plan.worker_id != run.worker_id
        or plan.active_run_id != run.id
    ):
        raise WorkerPlanDispatchConflict(
            "Worker Plan claim has an inconsistent aggregate identity"
        )
    return PlanAgentWorkerDispatchReceipt(
        plan_id=plan.id,
        run_id=run.id,
        target_task_id=plan.target_task_id,
        worker_id=run.worker_id,
        run_generation=run.generation,
        protocol=WORKER_PLAN_DISPATCH_PROTOCOL,
        status="prepared",
    )


def _validate_receipt_identity(
    *,
    receipt: PlanAgentWorkerDispatchReceipt,
    plan: Plan,
    run: PlanAgentRun,
    generation: int,
    require_active: bool = True,
    allow_cancelling_successor: bool = False,
) -> None:
    _validate_receipt_shape(receipt)
    if (
        receipt.protocol != WORKER_PLAN_DISPATCH_PROTOCOL
        or receipt.plan_id != plan.id
        or receipt.run_id != run.id
        or receipt.target_task_id != plan.target_task_id
        or receipt.worker_id != run.worker_id
        or plan.worker_id != run.worker_id
        or receipt.run_generation != generation
        or (
            run.generation != generation
            and not (
                allow_cancelling_successor
                and run.status in {"cancelling", "cancelled"}
                and run.cancellation_target_generation == generation
                and run.generation == generation + 1
            )
        )
        or run.plan_id != plan.id
        or (require_active and plan.active_run_id != run.id)
    ):
        raise WorkerPlanDispatchConflict(
            "Worker Plan dispatch receipt identity changed"
        )


async def load_worker_dispatch_receipt(
    db: AsyncSession,
    *,
    run_id: int,
    generation: int,
    for_update: bool = False,
) -> PlanAgentWorkerDispatchReceipt | None:
    query = select(PlanAgentWorkerDispatchReceipt).where(
        PlanAgentWorkerDispatchReceipt.run_id == run_id,
        PlanAgentWorkerDispatchReceipt.run_generation == generation,
    )
    if for_update:
        query = query.with_for_update()
    return (await db.execute(query)).scalar_one_or_none()


async def fence_worker_dispatch_target(
    db: AsyncSession,
    *,
    receipt: PlanAgentWorkerDispatchReceipt,
) -> WorkerPlanDispatchSnapshot:
    """Restart from a fresh writer transaction and fence the target Task.

    Callers have to read the receipt once to discover its immutable Task and
    Worker routing identity.  On SQLite WAL that read cannot safely be
    upgraded after another connection commits: the otherwise harmless Task
    UPDATE would raise ``SQLITE_BUSY_SNAPSHOT``.  Freeze scalar identity,
    discard the routing read transaction, then make the Task fence the first
    write when one exists.  A portable no-op Run update follows so standalone
    Worker Plans (which have no target Task) also acquire a real SQLite writer
    fence before ``SELECT ... FOR UPDATE``.  Every caller reloads the remaining
    aggregate afterwards in canonical Run -> Plan -> receipt order and
    revalidates the exact generation.
    """

    snapshot = snapshot_worker_dispatch_receipt(receipt)
    if db.new or db.dirty or db.deleted:
        raise WorkerPlanDispatchConflict(
            "Worker Plan dispatch routing fence requires a clean transaction"
        )
    await db.rollback()
    from backend.services.plan_service import fence_plan_target_task

    await fence_plan_target_task(
        db,
        target_task_id=snapshot.target_task_id,
        expected_worker_id=snapshot.worker_id,
    )
    run_fenced = await db.execute(
        update(PlanAgentRun)
        .where(
            PlanAgentRun.id == snapshot.run_id,
            PlanAgentRun.plan_id == snapshot.plan_id,
            PlanAgentRun.worker_id == snapshot.worker_id,
            PlanAgentRun.generation == snapshot.run_generation,
        )
        .values(updated_at=PlanAgentRun.updated_at)
    )
    if run_fenced.rowcount != 1:
        await db.rollback()
        raise WorkerPlanDispatchConflict(
            "Worker Plan Run changed before its dispatch writer fence"
        )
    return snapshot


async def mark_worker_dispatch_remote_possible(
    db_factory,
    *,
    receipt_id: int,
    plan_id: int,
    run_id: int,
    worker_id: int,
    generation: int,
    payload_digest: str,
) -> WorkerPlanDispatchSnapshot:
    """Commit the remote boundary before the Worker import HTTP request."""

    if not _valid_digest(payload_digest):
        raise WorkerPlanDispatchConflict("Worker Plan payload digest is invalid")
    async with db_factory() as db:
        frozen_receipt = await db.get(
            PlanAgentWorkerDispatchReceipt,
            receipt_id,
            populate_existing=True,
        )
        if frozen_receipt is None:
            raise WorkerPlanDispatchConflict(
                "Worker Plan dispatch receipt disappeared"
            )
        await fence_worker_dispatch_target(db, receipt=frozen_receipt)
        # All Plan outcome writers use Run -> Plan -> receipt ordering.
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
        receipt = await db.get(
            PlanAgentWorkerDispatchReceipt,
            receipt_id,
            with_for_update=True,
            populate_existing=True,
        )
        if run is None or plan is None or receipt is None:
            await db.rollback()
            raise WorkerPlanDispatchConflict(
                "Worker Plan dispatch aggregate disappeared"
            )
        _validate_receipt_identity(
            receipt=receipt,
            plan=plan,
            run=run,
            generation=generation,
        )
        if (
            run.status != "running"
            or run.worker_id != worker_id
            or receipt.worker_id != worker_id
        ):
            await db.rollback()
            raise WorkerPlanDispatchConflict(
                "Worker Plan Run no longer owns the claimed generation"
            )
        if receipt.status == "remote_possible":
            if receipt.payload_digest != payload_digest:
                await db.rollback()
                raise WorkerPlanDispatchConflict(
                    "Worker Plan retry changed immutable payload identity"
                )
            await db.commit()
            return snapshot_worker_dispatch_receipt(receipt)
        if receipt.status != "prepared" or receipt.payload_digest is not None:
            await db.rollback()
            raise WorkerPlanDispatchConflict(
                "Worker Plan dispatch boundary is already terminal"
            )
        receipt.status = "remote_possible"
        receipt.payload_digest = payload_digest
        receipt.last_error = None
        receipt.updated_at = datetime.utcnow()
        await db.commit()
        return snapshot_worker_dispatch_receipt(receipt)


def settle_worker_dispatch_receipt(
    *,
    receipt: PlanAgentWorkerDispatchReceipt,
    plan: Plan,
    run: PlanAgentRun,
    generation: int,
    reason: str,
    remote_status: str | None,
    last_error: str | None = None,
    allow_cancelling_successor: bool = False,
) -> None:
    """Stage a terminal receipt mutation in the caller's transaction."""

    _validate_receipt_identity(
        receipt=receipt,
        plan=plan,
        run=run,
        generation=generation,
        require_active=False,
        allow_cancelling_successor=allow_cancelling_successor,
    )
    if receipt.status == "settled":
        if (
            receipt.settlement_reason != reason
            or receipt.remote_status != remote_status
        ):
            raise WorkerPlanDispatchConflict(
                "Worker Plan dispatch receipt settled with another outcome"
            )
        return
    if receipt.status not in {"prepared", "remote_possible"}:
        raise WorkerPlanDispatchConflict(
            "Worker Plan dispatch receipt status is invalid"
        )
    allowed_reasons = (
        {"not_launched", "preflight_failed", "remote_cancelled"}
        if receipt.status == "prepared"
        else {
            "remote_pause",
            "remote_absent",
            "identity_conflict",
            "remote_cancelled",
        }
    )
    if reason not in allowed_reasons:
        raise WorkerPlanDispatchConflict(
            "Worker Plan dispatch settlement contradicts its boundary state"
        )
    now = datetime.utcnow()
    receipt.status = "settled"
    receipt.remote_status = remote_status
    receipt.settlement_reason = reason[:50]
    receipt.last_error = last_error[:4000] if last_error else None
    receipt.updated_at = now
    receipt.settled_at = now
    _validate_receipt_shape(receipt)


async def record_worker_dispatch_error(
    db_factory,
    *,
    receipt_id: int,
    error: str,
) -> None:
    """Persist reconciliation diagnostics without weakening the boundary."""

    async with db_factory() as db:
        receipt = await db.get(
            PlanAgentWorkerDispatchReceipt,
            receipt_id,
            with_for_update=True,
            populate_existing=True,
        )
        if receipt is None or receipt.status == "settled":
            await db.rollback()
            return
        receipt.last_error = error[:4000]
        receipt.updated_at = datetime.utcnow()
        await db.commit()


async def fence_worker_mirror_cancellation(
    db: AsyncSession,
    *,
    plan_id: int,
    run_id: int,
    worker_id: int,
    generation: int,
    payload_digest: str,
) -> PlanAgentRun:
    """Durably publish exact Worker cancellation intent before its RPC."""

    if not _valid_digest(payload_digest):
        raise WorkerPlanDispatchConflict("Worker Plan cancellation digest is invalid")
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
    receipts = list(
        (
            await db.execute(
                select(PlanAgentWorkerDispatchReceipt)
                .where(PlanAgentWorkerDispatchReceipt.run_id == run_id)
                .order_by(
                    PlanAgentWorkerDispatchReceipt.run_generation,
                    PlanAgentWorkerDispatchReceipt.id,
                )
                .with_for_update()
            )
        ).scalars()
    )
    for receipt in receipts:
        _validate_receipt_shape(receipt)
    digests = {
        receipt.payload_digest
        for receipt in receipts
        if receipt.payload_digest is not None
    }
    if (
        run is None
        or plan is None
        or run.plan_id != plan.id
        or run.worker_id != worker_id
        or plan.worker_id != worker_id
        or plan.active_run_id != run.id
        or run.status not in {"queued", "running", "waiting_user"}
        or run.generation != generation
        or run.instance_id is not None
        or await db.scalar(
            select(Instance.id)
            .where(Instance.current_plan_run_id == run.id)
            .limit(1)
        )
        is not None
        or digests != {payload_digest}
        or any(
            receipt.plan_id != plan.id
            or receipt.target_task_id != plan.target_task_id
            or receipt.worker_id != worker_id
            or receipt.run_generation > generation
            for receipt in receipts
        )
    ):
        raise WorkerPlanDispatchConflict(
            "Worker Plan cancellation aggregate identity changed"
        )
    expected_plan_lock_version = plan.lock_version
    expected_input_request_id = run.open_input_request_id
    expected_target_task_id = plan.target_task_id

    # The API may have read this aggregate before ``remote_possible`` committed.
    # End that WAL snapshot and make Run the first writer.  Exact generation
    # predicates mean a Worker ACK can never absorb a newer claim/generation.
    await db.rollback()
    now = datetime.utcnow()
    changed = await db.execute(
        update(PlanAgentRun)
        .where(
            PlanAgentRun.id == run_id,
            PlanAgentRun.plan_id == plan_id,
            PlanAgentRun.worker_id == worker_id,
            PlanAgentRun.status.in_(["queued", "running", "waiting_user"]),
            PlanAgentRun.generation == generation,
            PlanAgentRun.instance_id.is_(None),
            (
                PlanAgentRun.open_input_request_id.is_(None)
                if expected_input_request_id is None
                else PlanAgentRun.open_input_request_id
                == expected_input_request_id
            ),
        )
        .values(
            status="cancelling",
            open_input_request_id=None,
            cancellation_target_generation=generation,
            generation=generation + 1,
            error="Cancellation requested",
            updated_at=now,
        )
    )
    if changed.rowcount != 1:
        await db.rollback()
        raise WorkerPlanDispatchConflict(
            "Worker Plan cancellation intent lost its generation fence"
        )
    plan_changed = await db.execute(
        update(Plan)
        .where(
            Plan.id == plan_id,
            Plan.worker_id == worker_id,
            Plan.active_run_id == run_id,
            Plan.lock_version == expected_plan_lock_version,
        )
        .values(
            lock_version=Plan.lock_version + 1,
            updated_at=now,
        )
    )
    if plan_changed.rowcount != 1:
        await db.rollback()
        raise WorkerPlanDispatchConflict(
            "Worker Plan changed while fencing cancellation"
        )

    # Re-lock the complete receipt history only after Run -> Plan. The first
    # read above authenticated the caller's digest; this second read is the
    # authoritative post-WAL-fence state.
    receipts = list(
        (
            await db.execute(
                select(PlanAgentWorkerDispatchReceipt)
                .where(PlanAgentWorkerDispatchReceipt.run_id == run_id)
                .order_by(
                    PlanAgentWorkerDispatchReceipt.run_generation,
                    PlanAgentWorkerDispatchReceipt.id,
                )
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalars()
    )
    for receipt in receipts:
        _validate_receipt_shape(receipt)
    digests = {
        receipt.payload_digest
        for receipt in receipts
        if receipt.payload_digest is not None
    }
    if (
        digests != {payload_digest}
        or any(
            receipt.plan_id != plan_id
            or receipt.target_task_id != expected_target_task_id
            or receipt.worker_id != worker_id
            or receipt.run_generation > generation
            for receipt in receipts
        )
    ):
        await db.rollback()
        raise WorkerPlanDispatchConflict(
            "Worker Plan dispatch history changed during cancellation"
        )

    if expected_input_request_id is not None:
        input_changed = await db.execute(
            update(PlanInputRequest)
            .where(
                PlanInputRequest.id == expected_input_request_id,
                PlanInputRequest.plan_id == plan_id,
                PlanInputRequest.run_id == run_id,
                PlanInputRequest.status.in_(["prepared", "open"]),
            )
            .values(status="cancelled", cancelled_at=now)
        )
        if input_changed.rowcount != 1:
            await db.rollback()
            raise WorkerPlanDispatchConflict(
                "Worker Plan input changed while fencing cancellation"
            )
    await db.commit()
    fenced = await db.get(PlanAgentRun, run_id, populate_existing=True)
    if fenced is None:
        raise WorkerPlanDispatchConflict(
            "Worker Plan Run disappeared after cancellation fence"
        )
    # Keep the post-commit refresh as an explicit cancellation point. The API
    # shields this mutation and must still reap the exact old lifecycle if its
    # request is cancelled after durable intent publication.
    await db.refresh(fenced)
    return fenced


async def finalize_worker_mirror_cancellation(
    db: AsyncSession,
    *,
    plan_id: int,
    run_id: int,
    worker_id: int,
    target_generation: int,
    payload_digest: str,
    remote_state: str,
) -> PlanAgentRun:
    """Publish terminal Manager state only after exact pre-import absence."""

    if not _valid_digest(payload_digest) or remote_state != "absent":
        raise WorkerPlanDispatchConflict(
            "Worker Plan cancellation outcome identity is invalid"
        )
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
    receipts = list(
        (
            await db.execute(
                select(PlanAgentWorkerDispatchReceipt)
                .where(PlanAgentWorkerDispatchReceipt.run_id == run_id)
                .order_by(
                    PlanAgentWorkerDispatchReceipt.run_generation,
                    PlanAgentWorkerDispatchReceipt.id,
                )
                .with_for_update()
            )
        ).scalars()
    )
    for receipt in receipts:
        _validate_receipt_shape(receipt)
    digests = {
        receipt.payload_digest
        for receipt in receipts
        if receipt.payload_digest is not None
    }
    if (
        run is None
        or plan is None
        or run.plan_id != plan.id
        or run.worker_id != worker_id
        or plan.worker_id != worker_id
        or plan.active_run_id != run.id
        or run.status != "cancelling"
        or run.cancellation_target_generation != target_generation
        or run.generation != target_generation + 1
        or run.instance_id is not None
        or digests != {payload_digest}
        or any(
            receipt.plan_id != plan.id
            or receipt.target_task_id != plan.target_task_id
            or receipt.worker_id != worker_id
            or receipt.run_generation > target_generation
            for receipt in receipts
        )
    ):
        raise WorkerPlanDispatchConflict(
            "Worker Plan cancellation intent changed before settlement"
        )

    target_receipt = next(
        (
            receipt
            for receipt in receipts
            if receipt.run_generation == target_generation
        ),
        None,
    )
    if target_receipt is not None and target_receipt.status != "settled":
        if target_receipt.status == "prepared":
            reason = "not_launched"
            remote_status = None
        else:
            reason = "remote_absent"
            remote_status = None
        settle_worker_dispatch_receipt(
            receipt=target_receipt,
            plan=plan,
            run=run,
            # The receipt describes G while the durable cancellation intent is
            # G+1. Validate its frozen identity against a temporary generation
            # view without weakening the persisted Run fence.
            generation=target_generation,
            reason=reason,
            remote_status=remote_status,
            allow_cancelling_successor=True,
        )

    now = datetime.utcnow()
    execution_seconds = float(run.execution_seconds or 0)
    if run.last_execution_started_at is not None:
        execution_seconds += max(
            0.0,
            (now - run.last_execution_started_at).total_seconds(),
        )
    changed = await db.execute(
        update(PlanAgentRun)
        .where(
            PlanAgentRun.id == run_id,
            PlanAgentRun.plan_id == plan_id,
            PlanAgentRun.worker_id == worker_id,
            PlanAgentRun.status == "cancelling",
            PlanAgentRun.cancellation_target_generation == target_generation,
            PlanAgentRun.generation == target_generation + 1,
        )
        .values(
            status="cancelled",
            execution_seconds=execution_seconds,
            last_execution_started_at=None,
            error="Cancelled by user",
            updated_at=now,
            finished_at=now,
        )
    )
    released = await db.execute(
        update(Plan)
        .where(Plan.id == plan_id, Plan.active_run_id == run_id)
        .values(
            active_run_id=None,
            lock_version=Plan.lock_version + 1,
            updated_at=now,
        )
    )
    if changed.rowcount != 1 or released.rowcount != 1:
        await db.rollback()
        raise WorkerPlanDispatchConflict(
            "Worker Plan cancellation settlement lost its aggregate fence"
        )
    await db.commit()
    await db.refresh(run)
    return run


async def apply_worker_terminal_after_cancellation_race(
    db: AsyncSession,
    *,
    plan_id: int,
    run_id: int,
    worker_id: int,
    target_generation: int,
    payload_digest: str,
    payload: dict,
) -> PlanAgentRun:
    """Import the real terminal outcome which beat an exact cancel request."""

    if not _valid_digest(payload_digest):
        raise WorkerPlanDispatchConflict(
            "Worker Plan terminal cancellation-race digest is invalid"
        )
    validate_worker_terminal_outcome_graph(
        payload,
        plan_id=plan_id,
        run_id=run_id,
    )
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
    receipts = list(
        (
            await db.execute(
                select(PlanAgentWorkerDispatchReceipt)
                .where(PlanAgentWorkerDispatchReceipt.run_id == run_id)
                .order_by(
                    PlanAgentWorkerDispatchReceipt.run_generation,
                    PlanAgentWorkerDispatchReceipt.id,
                )
                .with_for_update()
            )
        ).scalars()
    )
    for receipt in receipts:
        _validate_receipt_shape(receipt)
    digests = {
        receipt.payload_digest
        for receipt in receipts
        if receipt.payload_digest is not None
    }
    if (
        run is None
        or plan is None
        or run.plan_id != plan.id
        or run.worker_id != worker_id
        or plan.worker_id != worker_id
        or plan.active_run_id != run.id
        or run.status != "cancelling"
        or run.cancellation_target_generation != target_generation
        or run.generation != target_generation + 1
        or run.instance_id is not None
        or digests != {payload_digest}
        or any(
            receipt.plan_id != plan.id
            or receipt.target_task_id != plan.target_task_id
            or receipt.worker_id != worker_id
            or receipt.run_generation > target_generation
            for receipt in receipts
        )
    ):
        raise WorkerPlanDispatchConflict(
            "Worker Plan terminal cancellation-race intent changed"
        )

    cancelled_outcome = payload["run"]["status"] == "cancelled"
    observation_generation = target_generation
    dispatch_receipt = next(
        (
            receipt
            for receipt in receipts
            if receipt.run_generation == target_generation
        ),
        None,
    )
    if dispatch_receipt is not None and dispatch_receipt.status == "settled":
        if not _worker_dispatch_history_advanced(dispatch_receipt):
            raise WorkerPlanDispatchConflict(
                "Worker Plan terminal outcome contradicts settled dispatch history"
            )
        # A waiting-user pause was already an immutable outcome for G.  The
        # cancellation fence's G+1 becomes a new readback observation rather
        # than rewriting that history.
        observation_generation = target_generation + 1
        dispatch_receipt = None
    if dispatch_receipt is None:
        dispatch_receipt = PlanAgentWorkerDispatchReceipt(
            plan_id=plan.id,
            run_id=run.id,
            target_task_id=plan.target_task_id,
            worker_id=worker_id,
            run_generation=observation_generation,
            protocol=WORKER_PLAN_DISPATCH_PROTOCOL,
            status="remote_possible",
            payload_digest=payload_digest,
        )
        db.add(dispatch_receipt)
        await db.flush()
    elif dispatch_receipt.status == "prepared":
        dispatch_receipt.status = "remote_possible"
        dispatch_receipt.payload_digest = payload_digest
        dispatch_receipt.last_error = None
        dispatch_receipt.updated_at = datetime.utcnow()
        _validate_receipt_shape(dispatch_receipt)
    elif (
        dispatch_receipt.status != "remote_possible"
        or dispatch_receipt.payload_digest != payload_digest
    ):
        raise WorkerPlanDispatchConflict(
            "Worker Plan terminal outcome lost its dispatch receipt"
        )

    # A completed/failed outcome beat the cancellation and restores the
    # Worker's terminal generation. A cancelled outcome is the cancellation
    # success itself: preserve Manager G+1 + target G while importing every
    # child and settling receipt G atomically.
    if not cancelled_outcome:
        run.status = "running"
        run.cancellation_target_generation = None
        run.generation = observation_generation
        run.error = None
        run.updated_at = datetime.utcnow()
    from backend.services.plan_service import apply_worker_plan_outcome

    try:
        return await apply_worker_plan_outcome(
            db,
            plan=plan,
            run=run,
            worker_id=worker_id,
            expected_generation=observation_generation,
            payload=payload,
            worker_dispatch_receipt_id=dispatch_receipt.id,
            allow_cancelling_successor=cancelled_outcome,
        )
    except RuntimeError as exc:
        raise WorkerPlanDispatchConflict(
            f"Worker Plan terminal graph could not be imported: {exc}"
        ) from exc
