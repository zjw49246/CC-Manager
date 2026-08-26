import asyncio
import logging
import os
import re
import shutil
import uuid
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, or_, select, update as sa_update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.database import get_db
from backend.models.task import Task
from backend.models.task_migration import TaskMigrationOperation
from backend.models.instance import Instance
from backend.models.test_harness import TestHarnessChildBinding
from backend.schemas.task import (
    TaskActionRequest,
    PlanApprovalRequest,
    TaskCreate,
    InternalTaskSkillsUpdate,
    TaskMigrationImportCommit,
    TaskMigrationImport,
    TaskMigrationImportRollback,
    TaskMigrationImportResponse,
    InternalTaskResponse,
    TaskResponse,
    TaskRoutingExpectation,
    TaskTerminationRequest,
    TaskTerminationSnapshot,
    TaskUpdate,
    WorkerRoutingConfigRequest,
    WorkerRoutingConfigSnapshot,
    WorkerManualRetryRequest,
    WorkerManualRetryResponse,
    WorkerPlanDecisionRequest,
    WorkerPlanDecisionAbsentResponse,
    WorkerPlanDecisionResponse,
)
from backend.api.task_projection import task_list_response, task_response
from backend.services.task_queue import (
    TaskDeletePreflight,
    TaskQueue,
    TaskWaitingCapabilityConflict,
    is_task_status_deletable,
    task_delete_fence,
)
from backend.services.task_creation import (
    SOURCE_TASK_INCARNATION_METADATA_KEY,
    TASK_EXECUTION_WORKER_PRINCIPAL_KINDS,
    prepare_task_create_values,
    purge_task_access_grants,
    stage_task_record,
    delegated_task_execution_principal_values,
    task_execution_principal_values,
    validate_task_service_tier_configuration,
)
from backend.services.pr_review_runtime import (
    is_pr_review_fix_task,
    is_pr_review_task,
    is_pr_sandbox_task,
)
from backend.services.project_readiness import (
    ProjectNotDispatchableError,
    require_project_dispatchable,
)
from backend.services.process_identity import (
    persisted_process_liveness,
)
from backend.services.task_skill_overrides import (
    clear_temporary_skills_marker,
)
from backend.services.test_harness_owner_fence import (
    TEST_HARNESS_TERMINAL_GATE_KEY,
    TestHarnessOwnerIdentity,
    TestHarnessOwnerGraphConflict,
    no_active_test_harness_owner_graph_predicate,
    require_no_active_test_harness_owner_graph,
    test_harness_owner_fence,
    test_harness_owner_terminal_gate_matches,
    test_harness_owner_identity,
)
from backend.services.skill_tool_rpc import (
    SKILL_TOOL_RPC_NAMES,
    execute_skill_tool_rpc,
)
from backend.services.task_termination import (
    TaskLaunchTerminationConflict,
    _finish_despite_cancellation as _finish_task_operation,
    lock_task_generation as _lock_task_generation,
    read_persisted_task_completed_at as _read_persisted_task_completed_at,
    remaining_task_process_generations as _remaining_task_process_generations,
    stop_task_process as _stop_task_process,
    task_generation_fence as _task_generation_fence,
)
from backend.services.worker_relay import (
    WorkerTaskGeneration,
    WORKER_MANUAL_RETRY_PROTOCOL,
    WORKER_MANUAL_RETRY_RECEIPT_METADATA_KEY,
    WORKER_REMOTE_MATERIALIZED_METADATA_KEY,
    apply_authoritative_worker_retry,
    apply_authoritative_worker_task,
    canonical_delegated_principal_payload,
    has_worker_execution_quarantine,
    worker_task_generation,
    worker_task_generation_predicates,
    worker_manual_retry_receipt,
    worker_manual_retry_is_prepared,
    worker_manual_retry_request_digest,
    worker_manual_retry_source_generation,
    worker_principal_digest,
    worker_remote_task_is_materialized,
)
from backend.services.worker_proxy import (
    WorkerDestroyLifecycleClaim,
    WorkerEndpointNotFoundError,
    WorkerTaskMutationOutcomeUncertainError,
    get_task_operation_lock,
)
from backend.services.worker_node_control import (
    fence_worker_node_mutation,
    fence_worker_node_receipt_resolution,
)
from backend.services.worker_plan_decision import (
    WORKER_PLAN_DECISION_GATE_RECEIPT_FIELD,
    WORKER_PLAN_DECISION_PROTOCOL,
    WORKER_PLAN_DECISION_RECEIPT_METADATA_KEY,
    worker_plan_decision_absent_response_matches,
    worker_plan_decision_gate_receipt,
    worker_plan_decision_receipt_digest,
    worker_plan_decision_request_digest,
    worker_plan_decision_request_matches,
    worker_plan_decision_response_matches,
    worker_plan_decision_worker_receipt,
    worker_plan_decision_worker_receipt_matches,
)
from backend.services.worker_routing_config import (
    InvalidWorkerRoutingMarker,
    WORKER_ROUTING_SAFE_STATUSES,
    WorkerMigrationImportReservation,
    WorkerRoutingPending,
    WorkerRoutingTuple,
    has_pending_worker_routing,
    read_pending_worker_routing,
    read_worker_migration_import_commit_receipt,
    read_worker_migration_import_reservation,
    task_routing_tuple,
    with_worker_migration_import_reservation,
    with_worker_migration_import_commit_receipt,
    with_pending_worker_routing,
    without_pending_worker_routing,
)
from backend.services.worker_task_termination import (
    WORKER_DESTROY_DRAIN_CLAIM_HEADER,
    WORKER_DESTROY_TASK_INCARCATION_HEADER,
    WORKER_DESTROY_TASK_RETRY_HEADER,
    WORKER_DESTROY_TASK_TURN_HEADER,
    WorkerTaskTerminationConflict as DurableWorkerTerminationConflict,
    WorkerTaskTerminationPending,
    active_worker_task_termination_receipt,
    acknowledge_worker_receipt,
    create_or_resume_manager_receipt,
    execute_worker_receipt,
    local_task_termination_effect_authority_matches,
    mark_worker_receipt_conflict,
    no_active_worker_task_termination_predicate,
    persist_worker_preflight_rejection,
    receipt_not_found_payload,
    reconcile_manager_receipt,
    reconcile_manager_task_delete_receipt,
    serialize_receipt,
    stage_manager_task_delete_receipt,
    stage_worker_receipt,
    task_not_found_payload,
    worker_task_termination_authority_predicate,
)
from backend.services.auto_capability_policy import (
    validate_auto_capability_task_scope,
)
from backend.services.skill_context import is_worker_managed_task_metadata
from backend.api.deps import (
    get_current_user_id,
    get_current_user_role,
    internal_task_incarnation_id,
    is_admin,
    lock_project_effect_access,
    lock_project_worker_effect_access,
    lock_request_user_authority,
    lock_task_effect_access,
    lock_task_effect_accesses,
    lock_worker_effect_access,
    require_admin,
    require_internal_service,
    require_internal_task_incarnation,
    require_project_access,
    require_task_access,
    require_task_control,
    require_worker_target_access,
    require_worker_control_plane_task_incarnation,
    require_worker_task_incarnation_header,
    task_execution_principal_from_request,
)

router = APIRouter(prefix="/api/tasks", tags=["tasks"])
logger = logging.getLogger(__name__)
_MANUAL_RETRYABLE_STATUSES = frozenset({"failed", "cancelled", "conflict", "completed"})


def _require_dispatchable_project(project) -> None:
    """HTTP adapter for the Project readiness gate (error projects → 422)."""
    try:
        require_project_dispatchable(project)
    except ProjectNotDispatchableError as exc:
        raise HTTPException(422, exc.detail) from exc


_TASK_CONTROL_EFFECT_GATE_FIELD = "task_control_effect"
_TASK_CONTROL_EFFECT_GATE_VERSION_FIELD = "task_control_effect_version"
_TASK_CONTROL_EFFECT_GATE_STATE_FIELD = "task_control_effect_state"
_TASK_CONTROL_EFFECT_GATE_VERSION = 1
_TASK_CONTROL_EFFECT_ACTIVE = "active"
_TASK_CONTROL_EFFECT_SETTLED = "settled"
_TASK_CONTROL_DURABLE_EFFECTS = frozenset(
    {"stop_session", "cancel", "delete", "plan_approve", "plan_reject"}
)
_WORKER_ROUTING_CONFIG_FIELDS = frozenset({"provider", "model", "codex_service_tier"})
_WORKER_SKILL_CONFIG_FIELDS = frozenset(
    {"enabled_skills", "selected_user_skills", "metadata_"}
)
_TASK_EXECUTION_PRINCIPAL_FIELDS = frozenset({
    "execution_user_id",
    "execution_user_role",
    "execution_mode",
    "execution_principal_kind",
})
_WORKER_INITIAL_GENERATION_FIELDS = frozenset({
    "source_retry_count",
    "source_turn_generation",
})


def _task_control_effect_gate_matches(
    task: Task,
    identity: TestHarnessOwnerIdentity,
    effect: str,
) -> bool:
    if effect not in _TASK_CONTROL_DURABLE_EFFECTS:
        return False
    metadata = task.metadata_ if isinstance(task.metadata_, dict) else {}
    gate = metadata.get(TEST_HARNESS_TERMINAL_GATE_KEY)
    gate_state = (
        gate.get(
            _TASK_CONTROL_EFFECT_GATE_STATE_FIELD,
            _TASK_CONTROL_EFFECT_ACTIVE,
        )
        if isinstance(gate, dict)
        else None
    )
    return bool(
        test_harness_owner_terminal_gate_matches(task, identity)
        and isinstance(gate, dict)
        and gate.get(_TASK_CONTROL_EFFECT_GATE_FIELD) == effect
        and gate.get(_TASK_CONTROL_EFFECT_GATE_VERSION_FIELD)
        == _TASK_CONTROL_EFFECT_GATE_VERSION
        and gate_state
        in {
            _TASK_CONTROL_EFFECT_ACTIVE,
            _TASK_CONTROL_EFFECT_SETTLED,
        }
    )


def _task_control_effect_gate_is_active(
    task: Task,
    identity: TestHarnessOwnerIdentity,
    effect: str,
) -> bool:
    metadata = task.metadata_ if isinstance(task.metadata_, dict) else {}
    gate = metadata.get(TEST_HARNESS_TERMINAL_GATE_KEY)
    return bool(
        _task_control_effect_gate_matches(task, identity, effect)
        and isinstance(gate, dict)
        and gate.get(
            _TASK_CONTROL_EFFECT_GATE_STATE_FIELD,
            _TASK_CONTROL_EFFECT_ACTIVE,
        )
        == _TASK_CONTROL_EFFECT_ACTIVE
    )


def _task_control_effect_authorization(request: Request) -> dict[str, object]:
    """Return the exact human/control-plane actor frozen by a control gate."""

    return {
        "authorized_user_id": get_current_user_id(request),
        "authorized_user_role": get_current_user_role(request),
        "authorization_type": getattr(request.state, "auth_type", None),
    }


def _build_worker_plan_decision_prepared_marker(
    identity: TestHarnessOwnerIdentity,
    *,
    effect: str,
    request_base: dict,
    operation_id: str | None = None,
    prepared_at: str | None = None,
) -> dict:
    """Build and validate the immutable Manager-side Worker decision outbox."""

    expected_action = {
        "plan_approve": "approve",
        "plan_reject": "reject",
    }.get(effect)
    if (
        expected_action is None
        or request_base.get("protocol_version")
        != WORKER_PLAN_DECISION_PROTOCOL
        or request_base.get("action") != expected_action
        or request_base.get("task_id") != identity.task_id
        or request_base.get("source_incarnation_id") != identity.incarnation_id
        or request_base.get("expected_status") != identity.status
        or request_base.get("expected_retry_count") != identity.retry_count
        or request_base.get("expected_turn_generation")
        != identity.turn_generation
        or "operation_id" in request_base
        or "request_digest" in request_base
    ):
        raise ValueError("invalid Worker Plan decision request base")
    routing = request_base.get("routing")
    manager_worker_id = request_base.get("manager_worker_id")
    target_task_id = request_base.get("plan_target_task_id")
    target_incarnation_id = request_base.get("plan_target_incarnation_id")
    if (
        not isinstance(routing, dict)
        or set(routing) != {"provider", "model", "codex_service_tier"}
        or not isinstance(routing.get("provider"), str)
        or not routing["provider"]
        or routing.get("codex_service_tier") not in {"default", "priority"}
        or type(manager_worker_id) is not int
        or manager_worker_id <= 0
        or (target_task_id is None) != (target_incarnation_id is None)
        or (
            target_task_id is not None
            and (
                type(target_task_id) is not int
                or target_task_id <= 0
                or not isinstance(target_incarnation_id, str)
                or len(target_incarnation_id) != 32
                or (
                    target_task_id == identity.task_id
                    and target_incarnation_id != identity.incarnation_id
                )
            )
        )
    ):
        raise ValueError("invalid Worker Plan decision routing or target")

    if operation_id is None:
        operation_id = uuid.uuid4().hex
    if (
        not isinstance(operation_id, str)
        or len(operation_id) != 32
        or any(char not in "0123456789abcdef" for char in operation_id)
    ):
        raise ValueError("invalid Worker Plan decision operation")
    if prepared_at is None:
        prepared_at = datetime.utcnow().isoformat(timespec="microseconds")
    if not isinstance(prepared_at, str) or not prepared_at:
        raise ValueError("invalid Worker Plan decision preparation time")

    request_payload = {
        **request_base,
        "operation_id": operation_id,
    }
    request_payload["request_digest"] = worker_plan_decision_request_digest(
        request_payload
    )
    return {
        "protocol_version": WORKER_PLAN_DECISION_PROTOCOL,
        "side": "manager",
        "state": "prepared",
        "action": expected_action,
        "operation_id": operation_id,
        "request_digest": request_payload["request_digest"],
        "request": request_payload,
        "prepared_at": prepared_at,
    }


async def _commit_task_control_effect_gate(
    request: Request,
    db: AsyncSession,
    task: Task,
    *,
    effect: str,
    worker_plan_decision_request_base: dict | None = None,
) -> TestHarnessOwnerIdentity:
    """Commit the ACL decision before any process or network effect.

    The caller must already hold ``Project -> Task -> membership -> User`` via
    ``lock_task_effect_access`` (or its multi-Task variant).  Extending the
    existing Harness terminal-generation marker keeps one durable owner for
    both late Browser admission and the user-requested control effect.  The
    marker is committed while those ACL writer fences are still held, so a
    concurrent revoke is serialized either wholly before or wholly after this
    exact effect admission.  Worker Plan decisions additionally freeze their
    complete wire request in this same commit; there is no actor-only outbox
    window for a later request to borrow.
    """

    if effect not in _TASK_CONTROL_DURABLE_EFFECTS:
        raise ValueError(f"unsupported Task control effect: {effect}")
    identity = test_harness_owner_identity(task)
    authorization = _task_control_effect_authorization(request)
    prepared_marker = None
    if worker_plan_decision_request_base is not None:
        routing = worker_plan_decision_request_base.get("routing")
        if (
            task.worker_id
            != worker_plan_decision_request_base.get("manager_worker_id")
            or not isinstance(routing, dict)
            or (task.provider or "claude").lower()
            != str(routing.get("provider", "")).lower()
            or task.model != routing.get("model")
            or (task.codex_service_tier or "default")
            != routing.get("codex_service_tier")
            or task.plan_target_task_id
            != worker_plan_decision_request_base.get("plan_target_task_id")
        ):
            raise HTTPException(
                409,
                "Plan Worker routing changed before atomic decision admission",
            )
        prepared_marker = _build_worker_plan_decision_prepared_marker(
            identity,
            effect=effect,
            request_base=worker_plan_decision_request_base,
        )
    elif (
        effect in {"plan_approve", "plan_reject"}
        and task.worker_id is not None
    ):
        raise ValueError(
            "Worker Plan decisions require an atomic immutable request"
        )
    metadata = dict(task.metadata_ or {})
    raw_gate = metadata.get(TEST_HARNESS_TERMINAL_GATE_KEY)
    if test_harness_owner_terminal_gate_matches(task, identity):
        same_effect = _task_control_effect_gate_matches(
            task,
            identity,
            effect,
        )
        settled_effect = bool(
            isinstance(raw_gate, dict)
            and raw_gate.get(_TASK_CONTROL_EFFECT_GATE_VERSION_FIELD)
            == _TASK_CONTROL_EFFECT_GATE_VERSION
            and raw_gate.get(_TASK_CONTROL_EFFECT_GATE_STATE_FIELD)
            == _TASK_CONTROL_EFFECT_SETTLED
            and raw_gate.get(_TASK_CONTROL_EFFECT_GATE_FIELD)
            in _TASK_CONTROL_DURABLE_EFFECTS
        )
        if not (same_effect or settled_effect):
            raise HTTPException(
                409,
                "Task generation is already owned by a different durable "
                "control effect",
            )
        if same_effect and not settled_effect:
            if not isinstance(raw_gate, dict):  # Matcher proved this above.
                raise HTTPException(409, "Task control effect gate is invalid")
            existing_marker = raw_gate.get(
                WORKER_PLAN_DECISION_GATE_RECEIPT_FIELD
            )
            if worker_plan_decision_request_base is None:
                if existing_marker is not None:
                    raise HTTPException(
                        409,
                        "Active Worker Plan decision cannot be reused as a "
                        "local control effect",
                    )
            else:
                if any(
                    raw_gate.get(key) != value
                    for key, value in authorization.items()
                ):
                    raise HTTPException(
                        409,
                        "Active Worker Plan decision belongs to a different "
                        "authorized actor",
                    )
                if existing_marker is None:
                    # Compatibility for a pre-upgrade crash that committed the
                    # actor-only gate.  Only that exact actor may finish
                    # freezing a request; a second actor cannot borrow it.
                    gate = dict(raw_gate)
                    gate[WORKER_PLAN_DECISION_GATE_RECEIPT_FIELD] = (
                        prepared_marker
                    )
                    metadata[TEST_HARNESS_TERMINAL_GATE_KEY] = gate
                    task.metadata_ = metadata
                elif isinstance(existing_marker, dict):
                    try:
                        expected_marker = (
                            _build_worker_plan_decision_prepared_marker(
                                identity,
                                effect=effect,
                                request_base=(
                                    worker_plan_decision_request_base
                                ),
                                operation_id=existing_marker.get(
                                    "operation_id"
                                ),
                                prepared_at=existing_marker.get(
                                    "prepared_at"
                                ),
                            )
                        )
                    except (TypeError, ValueError) as exc:
                        raise HTTPException(
                            409,
                            "Active Worker Plan decision receipt is invalid",
                        ) from exc
                    if existing_marker != expected_marker:
                        raise HTTPException(
                            409,
                            "Active Worker Plan decision has a different "
                            "immutable request",
                        )
                    prepared_marker = expected_marker
                else:
                    raise HTTPException(
                        409,
                        "Active Worker Plan decision receipt is malformed",
                    )
        # A successfully settled control leaves the base Harness terminal gate
        # in place: reopening Run/Workspace admission for the same exact owner
        # generation would violate the cleanup proof.  A later user control is
        # nevertheless allowed to take over after a fresh ACL writer fence.
        # Preserve the graph ids captured by TestHarnessService while replacing
        # only this control receipt's identity and audit fields.
        if settled_effect:
            gate = dict(raw_gate)
            gate.update(
                {
                    "reason": f"Task control effect admitted: {effect}",
                    _TASK_CONTROL_EFFECT_GATE_FIELD: effect,
                    _TASK_CONTROL_EFFECT_GATE_VERSION_FIELD: (
                        _TASK_CONTROL_EFFECT_GATE_VERSION
                    ),
                    _TASK_CONTROL_EFFECT_GATE_STATE_FIELD: (
                        _TASK_CONTROL_EFFECT_ACTIVE
                    ),
                    **authorization,
                }
            )
            gate.pop("task_control_effect_settled_at", None)
            if prepared_marker is not None:
                gate[WORKER_PLAN_DECISION_GATE_RECEIPT_FIELD] = prepared_marker
            else:
                gate.pop(WORKER_PLAN_DECISION_GATE_RECEIPT_FIELD, None)
            metadata[TEST_HARNESS_TERMINAL_GATE_KEY] = gate
            task.metadata_ = metadata
    else:
        gate = {
            "incarnation_id": identity.incarnation_id,
            "retry_count": identity.retry_count,
            "turn_generation": identity.turn_generation,
            "status": identity.status,
            "reason": f"Task control effect admitted: {effect}",
            _TASK_CONTROL_EFFECT_GATE_FIELD: effect,
            _TASK_CONTROL_EFFECT_GATE_VERSION_FIELD: (
                _TASK_CONTROL_EFFECT_GATE_VERSION
            ),
            _TASK_CONTROL_EFFECT_GATE_STATE_FIELD: _TASK_CONTROL_EFFECT_ACTIVE,
            **authorization,
        }
        if prepared_marker is not None:
            gate[WORKER_PLAN_DECISION_GATE_RECEIPT_FIELD] = prepared_marker
        metadata[TEST_HARNESS_TERMINAL_GATE_KEY] = gate
        task.metadata_ = metadata
    await db.commit()
    return identity


async def _settle_task_control_effect_gate(
    db: AsyncSession,
    identity: TestHarnessOwnerIdentity,
    *,
    effect: str,
) -> bool:
    """Mark one completed control effect reusable without reopening Harness.

    This is intentionally called only after the local effect returned a known
    result (including the exact no-running-session 400).  Transport/process
    uncertainty leaves the receipt active and therefore fail closed.  The
    exact Task writer CAS prevents a late settlement from crossing a new
    retry, turn, status, or incarnation.
    """

    if effect not in _TASK_CONTROL_DURABLE_EFFECTS:
        raise ValueError(f"unsupported Task control effect: {effect}")
    await db.rollback()
    fenced = await db.execute(
        sa_update(Task)
        .where(
            Task.id == identity.task_id,
            Task.incarnation_id == identity.incarnation_id,
            Task.retry_count == identity.retry_count,
            Task.turn_generation == identity.turn_generation,
            Task.status == identity.status,
        )
        .values(status=identity.status)
        .execution_options(synchronize_session=False)
    )
    if fenced.rowcount != 1:
        await db.rollback()
        return False
    owner = await db.get(Task, identity.task_id, populate_existing=True)
    if owner is not None and _task_control_effect_gate_matches(
        owner,
        identity,
        effect,
    ):
        existing_gate = (owner.metadata_ or {}).get(
            TEST_HARNESS_TERMINAL_GATE_KEY
        )
        if (
            isinstance(existing_gate, dict)
            and existing_gate.get(_TASK_CONTROL_EFFECT_GATE_STATE_FIELD)
            == _TASK_CONTROL_EFFECT_SETTLED
        ):
            await db.rollback()
            return True
    if owner is None or not _task_control_effect_gate_is_active(
        owner,
        identity,
        effect,
    ):
        await db.rollback()
        raise HTTPException(
            409,
            "Task control effect gate changed before it could be settled",
        )
    metadata = dict(owner.metadata_ or {})
    raw_gate = metadata.get(TEST_HARNESS_TERMINAL_GATE_KEY)
    if not isinstance(raw_gate, dict):  # Defensive; matcher proved this above.
        await db.rollback()
        raise HTTPException(409, "Task control effect gate is invalid")
    gate = dict(raw_gate)
    gate[_TASK_CONTROL_EFFECT_GATE_STATE_FIELD] = _TASK_CONTROL_EFFECT_SETTLED
    gate["task_control_effect_settled_at"] = datetime.utcnow().isoformat(
        timespec="microseconds"
    )
    metadata[TEST_HARNESS_TERMINAL_GATE_KEY] = gate
    owner.metadata_ = metadata
    await db.commit()
    return True


def _task_control_effect_gate_validator(
    identity: TestHarnessOwnerIdentity,
    effect: str,
):
    """Return a Harness-gate callback that proves the admitted exact effect."""

    async def validate(locked_db: AsyncSession) -> None:
        owner = await locked_db.get(
            Task,
            identity.task_id,
            populate_existing=True,
        )
        if owner is None or not _task_control_effect_gate_is_active(
            owner,
            identity,
            effect,
        ):
            raise RuntimeError(
                "Task control effect gate changed before terminal cleanup"
            )

    return validate


async def _prepare_worker_plan_decision_receipt(
    db: AsyncSession,
    identity: TestHarnessOwnerIdentity,
    *,
    effect: str,
    request_base: dict,
) -> dict:
    """Writer-fence and replay an atomically admitted Plan decision outbox.

    The Task control gate and complete immutable wire envelope were committed
    together while the human ACL writer fence was held.  This pass verifies
    the exact Worker/routing/target rows before any network effect; it must
    never manufacture an outbox after the actor-only gate has committed.
    """

    expected_action = {
        "plan_approve": "approve",
        "plan_reject": "reject",
    }.get(effect)
    if (
        expected_action is None
        or request_base.get("protocol_version")
        != WORKER_PLAN_DECISION_PROTOCOL
        or request_base.get("action") != expected_action
        or request_base.get("task_id") != identity.task_id
        or request_base.get("source_incarnation_id") != identity.incarnation_id
        or request_base.get("expected_status") != identity.status
        or request_base.get("expected_retry_count") != identity.retry_count
        or request_base.get("expected_turn_generation")
        != identity.turn_generation
        or "operation_id" in request_base
        or "request_digest" in request_base
    ):
        raise ValueError("invalid Worker Plan decision request base")
    routing = request_base.get("routing")
    manager_worker_id = request_base.get("manager_worker_id")
    target_task_id = request_base.get("plan_target_task_id")
    target_incarnation_id = request_base.get("plan_target_incarnation_id")
    if (
        not isinstance(routing, dict)
        or set(routing) != {"provider", "model", "codex_service_tier"}
        or not isinstance(routing.get("provider"), str)
        or not routing["provider"]
        or routing.get("codex_service_tier") not in {"default", "priority"}
        or type(manager_worker_id) is not int
        or manager_worker_id <= 0
        or (target_task_id is None) != (target_incarnation_id is None)
        or (
            target_task_id is not None
            and (
                type(target_task_id) is not int
                or target_task_id <= 0
                or not isinstance(target_incarnation_id, str)
                or len(target_incarnation_id) != 32
                or (
                    target_task_id == identity.task_id
                    and target_incarnation_id != identity.incarnation_id
                )
            )
        )
    ):
        raise ValueError("invalid Worker Plan decision routing or target")

    await db.rollback()
    # Match the multi-Task writer order used by ACL paths.  The target is not
    # mutated by a rejection, but its immutable incarnation is part of the
    # receipt; locking both rows prevents a concurrent delete/recreate from
    # stranding an already-published outbox.
    lock_ids = sorted({identity.task_id, target_task_id} - {None})
    for lock_id in lock_ids:
        if lock_id == identity.task_id:
            predicates = (
                Task.id == identity.task_id,
                Task.incarnation_id == identity.incarnation_id,
                Task.retry_count == identity.retry_count,
                Task.turn_generation == identity.turn_generation,
                Task.status == identity.status,
                Task.worker_id == manager_worker_id,
                func.lower(func.coalesce(Task.provider, "claude"))
                == routing["provider"].lower(),
                Task.model == routing.get("model"),
                func.coalesce(Task.codex_service_tier, "default")
                == routing["codex_service_tier"],
                Task.plan_target_task_id == target_task_id,
            )
        else:
            predicates = (
                Task.id == target_task_id,
                Task.incarnation_id == target_incarnation_id,
            )
        writer = await db.execute(
            sa_update(Task)
            .where(*predicates)
            .values(status=Task.status)
            .execution_options(synchronize_session=False)
        )
        if writer.rowcount != 1:
            await db.rollback()
            raise HTTPException(
                409,
                "Plan generation, Worker routing, or target changed before "
                "decision preparation",
            )
    owner = await db.get(Task, identity.task_id, populate_existing=True)
    if owner is None or not _task_control_effect_gate_is_active(
        owner,
        identity,
        effect,
    ):
        await db.rollback()
        raise HTTPException(
            409,
            "Plan decision effect gate changed before Worker preparation",
        )
    if (
        owner.worker_id != manager_worker_id
        or (owner.provider or "claude").lower()
        != routing["provider"].lower()
        or owner.model != routing.get("model")
        or (owner.codex_service_tier or "default")
        != routing["codex_service_tier"]
        or owner.plan_target_task_id != target_task_id
    ):
        await db.rollback()
        raise HTTPException(
            409,
            "Plan Worker routing changed before decision preparation",
        )
    if target_task_id is not None:
        target = await db.get(Task, target_task_id, populate_existing=True)
        if target is None or target.incarnation_id != target_incarnation_id:
            await db.rollback()
            raise HTTPException(
                409,
                "Plan target changed before Worker decision preparation",
            )
    metadata = dict(owner.metadata_ or {})
    raw_gate = metadata.get(TEST_HARNESS_TERMINAL_GATE_KEY)
    if not isinstance(raw_gate, dict):
        await db.rollback()
        raise HTTPException(409, "Plan decision effect gate is invalid")
    gate = dict(raw_gate)
    existing = gate.get(WORKER_PLAN_DECISION_GATE_RECEIPT_FIELD)
    if existing is None:
        await db.rollback()
        raise HTTPException(
            409,
            "Worker Plan decision was not atomically frozen with its actor",
        )
    if existing is not None and not isinstance(existing, dict):
        await db.rollback()
        raise HTTPException(409, "Worker Plan decision receipt is malformed")

    operation_id = existing.get("operation_id")
    if (
        not isinstance(operation_id, str)
        or len(operation_id) != 32
        or any(char not in "0123456789abcdef" for char in operation_id)
    ):
        await db.rollback()
        raise HTTPException(409, "Worker Plan decision operation is invalid")
    request_payload = {
        **request_base,
        "operation_id": operation_id,
    }
    request_payload["request_digest"] = (
        worker_plan_decision_request_digest(request_payload)
    )

    if not (
        existing.get("protocol_version") == WORKER_PLAN_DECISION_PROTOCOL
        and existing.get("side") == "manager"
        and existing.get("state") == "prepared"
        and existing.get("action") == expected_action
        and existing.get("operation_id") == operation_id
        and existing.get("request_digest") == request_payload["request_digest"]
        and existing.get("request") == request_payload
        and worker_plan_decision_request_matches(
            existing["request"],
            operation_id=operation_id,
            request_digest=request_payload["request_digest"],
        )
    ):
        await db.rollback()
        raise HTTPException(
            409,
            "Active Worker Plan decision has a different immutable request",
        )
    await db.rollback()
    return existing


def _settled_worker_plan_decision_metadata(
    task: Task,
    identity: TestHarnessOwnerIdentity,
    *,
    effect: str,
    marker: dict,
    worker_receipt: dict,
) -> dict:
    """Build the exact Manager metadata committed with Worker state adoption."""

    if not _task_control_effect_gate_is_active(task, identity, effect):
        raise HTTPException(
            409,
            "Plan decision effect gate changed before receipt settlement",
        )
    metadata = dict(task.metadata_ or {})
    raw_gate = metadata.get(TEST_HARNESS_TERMINAL_GATE_KEY)
    if not isinstance(raw_gate, dict):
        raise HTTPException(409, "Plan decision effect gate is invalid")
    gate = dict(raw_gate)
    if gate.get(WORKER_PLAN_DECISION_GATE_RECEIPT_FIELD) != marker:
        raise HTTPException(409, "Worker Plan decision outbox changed")
    settled_at = datetime.utcnow().isoformat(timespec="microseconds")
    settled_marker = dict(marker)
    settled_marker.update(
        {
            "state": "applied",
            "worker_receipt_digest": worker_plan_decision_receipt_digest(
                worker_receipt
            ),
            "worker_receipt": worker_receipt,
            "settled_at": settled_at,
        }
    )
    gate[WORKER_PLAN_DECISION_GATE_RECEIPT_FIELD] = settled_marker
    gate[_TASK_CONTROL_EFFECT_GATE_STATE_FIELD] = _TASK_CONTROL_EFFECT_SETTLED
    gate["task_control_effect_settled_at"] = settled_at
    metadata[TEST_HARNESS_TERMINAL_GATE_KEY] = gate
    return metadata


def _internal_worker_execution_principal(
    body: TaskCreate,
    *,
    require_system: bool = False,
) -> dict[str, object]:
    """Validate one complete Manager-delegated Worker authority envelope."""

    present = _TASK_EXECUTION_PRINCIPAL_FIELDS.intersection(
        body.model_fields_set
    )
    if present != _TASK_EXECUTION_PRINCIPAL_FIELDS:
        raise HTTPException(
            422,
            "Internal Worker Task creation requires a complete execution "
            "principal envelope",
        )
    kind = body.execution_principal_kind
    if kind not in TASK_EXECUTION_WORKER_PRINCIPAL_KINDS:
        raise HTTPException(
            422,
            "Worker Tasks accept only delegated or system principals",
        )
    if require_system and kind != "system":
        raise HTTPException(
            422,
            "Migrated inert Tasks require a system execution principal",
        )
    try:
        canonical = task_execution_principal_values(
            user_id=body.execution_user_id,
            role=body.execution_user_role,
            principal_kind=kind,
        )
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    supplied = {
        field: getattr(body, field)
        for field in _TASK_EXECUTION_PRINCIPAL_FIELDS
    }
    if supplied != canonical:
        raise HTTPException(
            422,
            "Worker Task execution principal role/mode mismatch",
        )
    return canonical


def _require_worker_task_id_node_role() -> None:
    """Reject explicit-id transport on a Manager or misconfigured Worker."""

    if settings.ccm_node_role != "worker":
        raise HTTPException(
            409,
            "Explicit Manager-mirrored Task ids require "
            "CCM_NODE_ROLE=worker",
        )


def _internal_worker_initial_generation(body: TaskCreate) -> tuple[int, int]:
    """Validate the exact Manager generation that precedes Worker dequeue."""

    present = _WORKER_INITIAL_GENERATION_FIELDS.intersection(
        body.model_fields_set
    )
    if present != _WORKER_INITIAL_GENERATION_FIELDS:
        raise HTTPException(
            422,
            "Internal Worker Task creation requires source retry/turn "
            "generation",
        )
    if body.source_incarnation_id is None:
        raise HTTPException(
            422,
            "Internal Worker Task creation requires source incarnation",
        )
    # Pydantic has already enforced N >= 0 and G >= 1.
    assert body.source_retry_count is not None
    assert body.source_turn_generation is not None
    return body.source_retry_count, body.source_turn_generation


class InternalSkillToolCall(BaseModel):
    """Strict payload accepted only from the frozen task Skills MCP wrapper."""

    model_config = ConfigDict(extra="forbid")

    tool: Literal[
        "ccm_command_help",
        "ccm_read_skill",
        "ccm_read_user_skill",
        "ccm_create_skill",
        "ccm_distill",
        "ccm_enable_skill",
        "ccm_disable_skill",
    ]
    arguments: dict = Field(default_factory=dict)
_WORKER_CONFIG_SYNC_UNSAFE_FIELDS = frozenset(
    {"worker_id", "project_id", "target_repo"}
)
_LOCAL_ROUTING_EDITABLE_STATUSES = WORKER_ROUTING_SAFE_STATUSES | {"pending"}
_WORKER_SKILL_EDITABLE_STATUSES = WORKER_ROUTING_SAFE_STATUSES | {"pending"}
_PR_REVIEW_CHAT_TERMINAL_STATUSES = frozenset(
    {"approved", "merged", "commented", "error"}
)
_PLAN_CASCADE_PROTOCOL_VERSION = 1


def _require_not_waiting_capability(task: Task, *, action: str) -> None:
    """Keep ordinary Task management outside the durable resume protocol."""

    if task.status == "waiting_capability":
        raise HTTPException(
            409,
            f"Task is waiting for its requested capability and cannot be {action} "
            "until the durable resume completes",
        )


class WorkerTerminationPutRequest(BaseModel):
    """Strict Manager->Worker durable termination request."""

    model_config = ConfigDict(extra="forbid")

    operation: Literal["cancel", "stop_session", "supersede"]
    request_payload: dict
    request_digest: str = Field(min_length=64, max_length=64)


class WorkerTerminationAckRequest(BaseModel):
    """Manager proof that the exact Worker result is durable locally."""

    model_config = ConfigDict(extra="forbid")

    request_digest: str = Field(min_length=64, max_length=64)
    result_digest: str = Field(min_length=64, max_length=64)


def _require_not_delivery_owned_task(task: Task, *, action: str) -> None:
    """Route lifecycle mutations through DeliveryRun, never its worker Task."""

    if task.mode == "delivery_loop" or task.delivery_run_id is not None:
        run_id = task.delivery_run_id
        suffix = f" #{run_id}" if isinstance(run_id, int) else ""
        raise HTTPException(
            409,
            f"Delivery-owned Tasks cannot be manually {action}; use Delivery Run"
            f"{suffix} controls so the Plan/Code/Review/PR evidence stays fenced",
        )


async def _require_not_isolated_browser_child(
    db: AsyncSession,
    task: Task,
    *,
    action: str,
) -> None:
    """Keep every public mutation on the durable Browser owner, never child."""

    from backend.services.test_harness_children import (
        browser_child_public_mutation_error,
    )

    metadata = task.metadata_ if isinstance(task.metadata_, dict) else {}
    has_binding = metadata.get("isolated_browser_agent") is True
    if not has_binding:
        has_binding = (
            await db.scalar(
                select(TestHarnessChildBinding.id)
                .where(TestHarnessChildBinding.child_task_id == task.id)
                .limit(1)
            )
            is not None
        )
    detail = browser_child_public_mutation_error(
        task,
        has_binding=has_binding,
    )
    if detail is not None:
        raise HTTPException(409, f"{detail}; it cannot be {action}")


async def _require_no_active_harness_owner_graph(
    db: AsyncSession,
    task_id: int,
) -> None:
    try:
        await require_no_active_test_harness_owner_graph(db, task_id)
    except TestHarnessOwnerGraphConflict as exc:
        raise HTTPException(409, str(exc)) from exc


async def _require_migration_import_eligible(
    db: AsyncSession,
    task: Task,
    body: TaskMigrationImport,
) -> str | None:
    """Validate one exact destination row before a Worker import refresh."""

    await _require_not_isolated_browser_child(
        db,
        task,
        action="migration-imported",
    )
    from backend.models.test_harness import TestHarnessRun
    from backend.models.workspace_review import WorkspaceReviewRun

    owns_harness_graph = (
        await db.scalar(
            select(TestHarnessChildBinding.id)
            .where(TestHarnessChildBinding.owner_task_id == task.id)
            .limit(1)
        )
        or await db.scalar(
            select(TestHarnessRun.id)
            .where(TestHarnessRun.task_id == task.id)
            .limit(1)
        )
        or await db.scalar(
            select(WorkspaceReviewRun.id)
            .where(WorkspaceReviewRun.task_id == task.id)
            .limit(1)
        )
    )
    if owns_harness_graph is not None:
        raise HTTPException(
            409,
            "Tasks with durable Browser Harness evidence cannot be "
            "migration-imported",
        )
    _require_not_delivery_owned_task(task, action="migration-imported")
    if task.mode == "plan" or task.canonical_plan_id is not None:
        raise HTTPException(
            409,
            "Existing Plan carriers are immutable and cannot be "
            "migration-imported",
        )
    if task.mode != body.mode:
        raise HTTPException(
            409,
            "Migration import cannot change an existing Task mode",
        )
    if task.capability_policy is not None:
        raise HTTPException(
            409,
            "Destination Task has an immutable local Auto capability policy "
            "and cannot be migration-imported",
        )
    bound_source_incarnation = (task.metadata_ or {}).get(
        SOURCE_TASK_INCARNATION_METADATA_KEY
    )
    if body.source_incarnation_id is None and (
        not isinstance(task.incarnation_id, str)
        or re.fullmatch(r"[0-9a-f]{32}", task.incarnation_id) is None
    ):
        raise HTTPException(
            409,
            "Destination Task has no verifiable incarnation identity",
        )
    if bound_source_incarnation is not None and (
        not isinstance(bound_source_incarnation, str)
        or bound_source_incarnation != task.incarnation_id
    ):
        raise HTTPException(
            409,
            "Destination Task has a corrupt source incarnation binding",
        )
    if (
        bound_source_incarnation is not None
        and body.source_incarnation_id != bound_source_incarnation
    ):
        raise HTTPException(
            409,
            "Destination Task is bound to a different source incarnation",
        )
    return bound_source_incarnation


async def _migration_import_has_durable_dependents(
    db: AsyncSession,
    task_id: int,
) -> bool:
    """Refuse rollback if the inert import accumulated any durable history."""

    from backend.database import Base
    from backend.models.capability import (
        CapabilityInvocation,
        CapabilityResumeOutbox,
    )
    from backend.models.log_entry import LogEntry
    from backend.models.plan import Plan
    from backend.models.plan_agent import PlanAgentRun
    from backend.models.sub_agent import SubAgentSession
    from backend.models.task_ssh_effect import TaskSSHEffectReceipt
    from backend.models.worker_task_termination import (
        WorkerTaskTerminationReceipt,
    )

    # Most ownership tables use real foreign keys. Query them generically so
    # a newly added durable child cannot be silently cascaded by this narrow
    # rollback protocol. Several audit/history tables deliberately avoid FKs
    # so Task deletion can never erase their evidence; those are listed below.
    for table in Base.metadata.tables.values():
        for foreign_key in table.foreign_keys:
            if (
                foreign_key.column.table.name == Task.__tablename__
                and foreign_key.column.name == "id"
            ):
                found = await db.scalar(
                    select(foreign_key.parent)
                    .select_from(table)
                    .where(foreign_key.parent == task_id)
                    .limit(1)
                )
                if found is not None:
                    return True

    logical_references = (
        select(LogEntry.id).where(LogEntry.task_id == task_id),
        select(SubAgentSession.id).where(SubAgentSession.task_id == task_id),
        select(TaskSSHEffectReceipt.id).where(
            TaskSSHEffectReceipt.task_id == task_id
        ),
        select(WorkerTaskTerminationReceipt.operation_id).where(
            WorkerTaskTerminationReceipt.task_id == task_id
        ),
        select(CapabilityInvocation.id).where(
            CapabilityInvocation.task_id == task_id
        ),
        select(CapabilityResumeOutbox.id).where(
            CapabilityResumeOutbox.task_id == task_id
        ),
        select(Plan.id).where(Plan.target_task_id == task_id),
        select(PlanAgentRun.id).where(PlanAgentRun.plan_task_id == task_id),
        select(Task.id).where(Task.plan_target_task_id == task_id),
    )
    for statement in logical_references:
        if await db.scalar(statement.limit(1)) is not None:
            return True
    return False


def _require_no_pending_worker_turn_handoff(task: Task) -> None:
    """Freeze execution-affecting edits across one Manager->Worker G+1."""

    if task.worker_turn_handoff_id is not None:
        raise HTTPException(
            409,
            "Task configuration cannot change while a Worker follow-up is "
            "waiting for its exact remote turn generation",
        )


async def _require_no_pr_review_publication(
    db: AsyncSession,
    task_id: int,
) -> None:
    """Fence Task generation changes while its GitHub outbox is publishing."""

    from backend.models.pr_monitor import PRReview, PRReviewerRun

    publishing = await db.execute(
        select(PRReview.id).distinct()
        .outerjoin(
            PRReviewerRun,
            PRReviewerRun.pr_review_id == PRReview.id,
        )
        .where(
            or_(
                PRReview.task_id == task_id,
                PRReviewerRun.task_id == task_id,
            ),
            PRReview.status.in_(("publishing", "superseding")),
        )
    )
    if publishing.scalar_one_or_none() is not None:
        raise HTTPException(
            409,
            "PR review publication/synchronization is in progress; this Task "
            "generation is frozen",
        )


async def _require_not_pr_review_task_mutation(
    db: AsyncSession,
    task_id: int,
    *,
    action: str,
) -> None:
    """Keep automated review Tasks immutable outside their backend workflow."""

    from backend.models.pr_monitor import PRReview

    linked = await db.execute(
        select(PRReview.id)
        .where(PRReview.task_id == task_id)
        .limit(1)
    )
    task = await db.get(Task, task_id)
    if (
        linked.scalar_one_or_none() is not None
        or (task is not None and is_pr_sandbox_task(task))
    ):
        raise HTTPException(
            409,
            f"Automated PR workflow Tasks cannot be manually {action}; wait "
            "for the workflow outcome or a new PR snapshot",
        )


async def _require_pr_review_chat_allowed(
    db: AsyncSession,
    task_id: int,
    *,
    trusted_unlinked_terminal: bool = False,
) -> bool:
    """Allow discussion only after the automated review is durably terminal.

    ``reviewing`` must remain immutable until its exact completed Task
    generation has been claimed by the GitHub publication outbox.  A chat turn
    admitted in that window could otherwise replace the generation before the
    completion consumer verifies it.  Once publication is terminal, later
    turns cannot change the already-recorded GitHub action and are safe.

    Worker mirrors deliberately retain only the ``pr-review`` tag, not the
    Manager's PRReview row.  ``trusted_unlinked_terminal`` is therefore
    reserved for an internally authenticated Manager -> Worker request whose
    Manager-side terminal check ran while holding the Task operation lock.

    Returns ``True`` for a PR review Task and ``False`` for an ordinary Task.
    """

    from backend.models.pr_monitor import PRReview, PRReviewerRun

    task = await db.get(Task, task_id)
    if task is None:
        return False
    if is_pr_review_fix_task(task):
        raise HTTPException(
            409,
            "Automated PR fix Tasks cannot accept manual discussion or live "
            "injection; wait for the generated patch outcome",
        )
    metadata = task.metadata_ or {}
    tags = task.tags
    from backend.services.pr_review_runtime import PRE_PR_CODE_REVIEW_TAG

    pre_pr_capability_marker = (
        isinstance(tags, (list, tuple, set, dict))
        and PRE_PR_CODE_REVIEW_TAG in tags
    ) or (
        type(metadata.get("code_review_run_id")) is int
        and type(metadata.get("capability_invocation_id")) is int
        and type(metadata.get("capability_execution_id")) is int
    )
    if pre_pr_capability_marker:
        raise HTTPException(
            409,
            "Pre-PR Code Review Capability Tasks cannot accept manual "
            "discussion or live injection; their immutable prompt and exact "
            "structured verdict are Controller-owned",
        )
    tag_marker = (
        isinstance(tags, (list, tuple, set, dict))
        and "pr-review" in tags
    )
    task_marker = is_pr_review_task(task)

    def allow_terminal_discussion() -> bool:
        if task.provider == "codex":
            # Automated Codex reviews run in a tool-free isolated thread.
            # That transport intentionally refuses native resume, so a
            # terminal follow-up would otherwise open a context-less thread
            # containing only the user's new message.
            raise HTTPException(
                409,
                "Terminal discussion is unavailable for isolated Codex PR "
                "review Tasks; start a separate Task with the review context",
            )
        return True

    linked = list((await db.execute(
        select(PRReview.status).distinct()
        .outerjoin(
            PRReviewerRun,
            PRReviewerRun.pr_review_id == PRReview.id,
        )
        .where(
            or_(
                PRReview.task_id == task_id,
                PRReviewerRun.task_id == task_id,
            )
        )
    )).scalars().all())
    if linked:
        # One Task belongs to exactly one immutable review snapshot.  Multiple
        # links or an unknown state indicate corrupt/partially migrated state
        # and must fail closed rather than guessing which review is current.
        if (
            len(linked) == 1
            and linked[0] in _PR_REVIEW_CHAT_TERMINAL_STATUSES
            and metadata.get("pr_review_superseded") is not True
        ):
            return allow_terminal_discussion()
        raise HTTPException(
            409,
            "Automated PR review discussion is available only after its "
            "GitHub review workflow is terminal",
        )

    if not task_marker:
        return False
    if (
        trusted_unlinked_terminal
        and tag_marker
        and metadata.get("pr_review_superseded") is not True
    ):
        return allow_terminal_discussion()
    raise HTTPException(
        409,
        "Automated PR review Task has no locally verified terminal review "
        "state",
    )


async def _require_pr_review_retryable(
    db: AsyncSession,
    task_id: int,
) -> None:
    """Do not run a Task generation whose linked review is already terminal."""

    from backend.models.pr_monitor import PRReview, PRReviewerRun

    result = await db.execute(
        select(PRReview.status).distinct()
        .outerjoin(
            PRReviewerRun,
            PRReviewerRun.pr_review_id == PRReview.id,
        )
        .where(
            or_(
                PRReview.task_id == task_id,
                PRReviewerRun.task_id == task_id,
            )
        )
        .limit(1)
    )
    review_status = result.scalar_one_or_none()
    task = await db.get(Task, task_id)
    task_marker = bool(task is not None and is_pr_sandbox_task(task))
    if review_status is not None:
        if review_status in {"pending", "waiting_ci", "reviewing"}:
            detail = (
                "Automated PR review Tasks cannot be manually retried; push a "
                "new PR snapshot instead"
            )
        else:
            detail = (
                "This PR review is already terminal; wait for a new PR snapshot "
                "instead of retrying its old Task"
            )
        raise HTTPException(
            409,
            detail,
        )
    if task_marker:
        raise HTTPException(
            409,
            "Automated PR workflow Tasks cannot be manually retried; wait for "
            "the workflow outcome or push a new PR snapshot instead",
        )


class _WorkerRoutingConfirmationUnavailable(HTTPException):
    """Worker ack/reconcile outcome could not be read after Manager commit."""

    def __init__(self):
        super().__init__(
            503,
            "Worker routing synchronization outcome could not be confirmed",
        )


def _require_expected_task_routing(
    task: Task,
    expected: TaskRoutingExpectation | None,
    *,
    effective_model: str | None,
) -> tuple[str, str | None, str]:
    """Reject a user action issued from a stale routing view."""

    actual = (
        (task.provider or "claude").lower(),
        effective_model,
        task.codex_service_tier or "default",
    )
    if expected is None:
        return actual
    requested = (
        expected.provider.lower(),
        expected.model,
        expected.codex_service_tier,
    )
    if requested != actual:
        raise HTTPException(
            409,
            "Task execution configuration changed since this page was "
            "loaded; refresh before starting another turn",
        )
    return actual


def _explicit_command_skills(message: str | None) -> dict[str, bool]:
    """Return the temporary Skills requested by one leading $command."""

    from backend.services.command_registry import parse_command

    command, _command_args = parse_command(message or "")
    return dict(command.required_skills or {}) if command else {}


async def _validate_skill_configuration(
    db: AsyncSession,
    *,
    provider: str | None,
    enabled_skills: dict | None,
    selected_user_skills: list[int] | None,
    user_skill_snapshots: list[dict] | None = None,
    worker_id: int | None = None,
    shared_from_id: int | None = None,
    metadata: dict | None = None,
) -> list[int] | None:
    """Validate and normalize task-scoped Skill selections."""

    from backend.config import settings as app_settings
    from backend.models.user_skill import UserSkill
    from backend.services.skill_context import (
        codex_monitor_supported_for_scope,
        normalize_user_skill_ids,
        skill_supported,
        user_skill_snapshot_from_mapping,
    )

    provider = (provider or "claude").lower()
    codex_monitor_enabled = codex_monitor_supported_for_scope(
        provider=provider,
        worker_id=worker_id,
        shared_from_id=shared_from_id,
        metadata=metadata,
        codex_main_mcp_enabled=app_settings.codex_main_mcp_enabled,
    )
    unsupported = sorted(
        name
        for name, enabled in (enabled_skills or {}).items()
        if enabled
        and not skill_supported(
            provider,
            name,
            codex_monitor_enabled=codex_monitor_enabled,
        )
    )
    if unsupported:
        raise HTTPException(
            400,
            "Provider "
            f"{(provider or 'claude').lower()} does not support Skills: "
            + ", ".join(unsupported),
        )

    normalized = normalize_user_skill_ids(selected_user_skills)
    unavailable_without_main_mcp = sorted(
        name
        for name, enabled in (enabled_skills or {}).items()
        if enabled and name != "sub-agent"
    )
    if (
        provider == "codex"
        and not app_settings.codex_main_mcp_enabled
        and (unavailable_without_main_mcp or normalized)
    ):
        raise HTTPException(
            400,
            "Codex main-task MCP is disabled; only Sub-Agent can be enabled",
        )
    if not normalized:
        return [] if selected_user_skills is not None else None
    found = set()
    for value in user_skill_snapshots or []:
        if not isinstance(value, dict):
            continue
        snapshot = user_skill_snapshot_from_mapping(value)
        if snapshot is not None:
            found.add(snapshot.id)
    if user_skill_snapshots is None:
        found.update(
            (await db.execute(select(UserSkill.id).where(UserSkill.id.in_(normalized))))
            .scalars()
            .all()
        )
    missing = [skill_id for skill_id in normalized if skill_id not in found]
    if missing:
        raise HTTPException(
            400,
            "Selected User Skills do not exist: "
            + ", ".join(str(skill_id) for skill_id in missing),
        )
    return normalized


def _find_session_jsonl(session_id: str, provider: str = "claude") -> Path | None:
    """Locate a provider session JSONL on disk.

    Codex stores rollouts under ``$CODEX_HOME/sessions/YYYY/MM/DD``.  This
    branch must run before the Claude pool lookup: treating a valid Codex
    rollout as a missing Claude session makes every follow-up abandon native
    history/cache and start a new thread.

    Pool deployments split sessions across multiple ~/.claude-account-N dirs,
    so a lookup that only checks ~/.claude / CLAUDE_CONFIG_DIR (and only the
    exact last_cwd-encoded project subdir) misses sessions created under a pool
    account and silently degrades recovery to a lossy summary (prod task #725).
    We reuse the pool's own locator (searches every account dir) and glob across
    all project subdirs so cwd-encoding differences don't hide the file either.
    """
    if (provider or "claude").lower() == "codex":
        homes_to_check: list[Path] = []

        # Pool account homes are the primary source of truth in multi-account
        # deployments.  Include disabled/cooling accounts too: their rollout
        # history remains valid even when the credentials cannot run a turn.
        try:
            from backend.main import codex_pool

            if codex_pool:
                for account in codex_pool.list_accounts():
                    codex_home = account.get("codex_home")
                    if codex_home:
                        homes_to_check.append(Path(codex_home).expanduser())
        except Exception:
            pass

        env_home = os.environ.get("CODEX_HOME")
        if env_home:
            homes_to_check.append(Path(env_home).expanduser())
        homes_to_check.append(Path.home() / ".codex")

        # Disk fallback covers removed pool entries and legacy account naming
        # such as ~/.codex-account-2.  A missing sessions/ child is harmless.
        try:
            homes_to_check.extend(
                path for path in sorted(Path.home().glob(".codex*")) if path.is_dir()
            )
        except OSError:
            pass

        seen: set[str] = set()
        for codex_home in homes_to_check:
            key = os.path.abspath(str(codex_home))
            if key in seen:
                continue
            seen.add(key)
            try:
                match = next(
                    (
                        path
                        for path in codex_home.glob(
                            f"sessions/*/*/*/rollout-*-{session_id}.jsonl"
                        )
                        if path.is_file()
                    ),
                    None,
                )
                if match:
                    return match
            except OSError:
                continue
        return None

    config_dir: str | None = None
    transition_error: tuple[str, Exception] | None = None
    try:
        from backend.main import dispatcher

        if dispatcher and dispatcher.pool:
            config_dir = dispatcher.pool.locate_session_config_dir(session_id)
    except Exception:
        config_dir = None
    # Try pool locator result first, then env CLAUDE_CONFIG_DIR, then default
    dirs_to_check = []
    if config_dir:
        dirs_to_check.append(config_dir)
    env_dir = os.environ.get("CLAUDE_CONFIG_DIR")
    if env_dir and env_dir not in dirs_to_check:
        dirs_to_check.append(env_dir)
    default_dir = os.path.expanduser("~/.claude")
    if default_dir not in dirs_to_check:
        dirs_to_check.append(default_dir)
    for d in dirs_to_check:
        try:
            match = next(Path(d).glob(f"projects/*/{session_id}.jsonl"), None)
            if match:
                return match
        except OSError:
            pass
    # Fallback: scan all ~/.claude* dirs on disk. Covers accounts that were
    # removed from the pool but whose config dirs still exist on disk.
    home = Path.home()
    try:
        for d in sorted(home.iterdir()):
            if not d.name.startswith(".claude") or not d.is_dir():
                continue
            try:
                match = next(d.glob(f"projects/*/{session_id}.jsonl"), None)
                if match:
                    return match
            except OSError:
                continue
    except OSError:
        pass
    return None


async def _clone_session(source_task_id: int, db: AsyncSession) -> dict | None:
    """Clone a Claude Code session file from a source task, returning new session_id and last_cwd."""
    source = await db.get(Task, source_task_id)
    if not source or not source.session_id or not source.last_cwd:
        return None

    # A Codex rollout embeds its thread id in both the filename and session
    # metadata.  Copying it under a random filename does not create a valid new
    # thread, so keep this legacy clone operation Claude-only.
    if (source.provider or "claude").lower() != "claude":
        return None

    source_jsonl = _find_session_jsonl(source.session_id, provider="claude")
    if source_jsonl is None:
        return None

    new_session_id = str(uuid.uuid4())
    dest_jsonl = source_jsonl.parent / f"{new_session_id}.jsonl"
    shutil.copy2(source_jsonl, dest_jsonl)

    return {"session_id": new_session_id, "last_cwd": source.last_cwd}


def _get_queue(db: AsyncSession = Depends(get_db)) -> TaskQueue:
    return TaskQueue(db)


@router.get("/count")
async def count_tasks(
    request: Request,
    status: str | None = None,
    include_archived: bool = False,
    archived_only: bool = False,
    project_id: int | None = None,
    starred: bool | None = None,
    has_unread: bool | None = None,
    task_kind: Literal["standalone_plan", "related_plan", "main"] | None = None,
    queue: TaskQueue = Depends(_get_queue),
):
    user_id = get_current_user_id(request)
    user_role = get_current_user_role(request)
    if user_role not in ("admin", "super_admin") and user_id is None:
        return {"total": 0}
    total = await queue.count_tasks(
        status=status,
        include_archived=include_archived,
        archived_only=archived_only,
        project_id=project_id,
        starred=starred,
        has_unread=has_unread,
        task_kind=task_kind,
        user_id=user_id if user_role not in ("admin", "super_admin") else None,
    )
    return {"total": total}


@router.get("", response_model=list[TaskResponse])
async def list_tasks(
    request: Request,
    status: str | None = None,
    include_archived: bool = False,
    archived_only: bool = False,
    project_id: int | None = None,
    starred: bool | None = None,
    has_unread: bool | None = None,
    task_kind: Literal["standalone_plan", "related_plan", "main"] | None = None,
    limit: int = 50,
    offset: int = 0,
    queue: TaskQueue = Depends(_get_queue),
    db: AsyncSession = Depends(get_db),
):
    user_id = get_current_user_id(request)
    user_role = get_current_user_role(request)
    if user_role not in ("admin", "super_admin") and user_id is None:
        return []
    tasks = await queue.list_tasks(
        status=status,
        include_archived=include_archived,
        archived_only=archived_only,
        project_id=project_id,
        starred=starred,
        has_unread=has_unread,
        task_kind=task_kind,
        limit=limit,
        offset=offset,
        user_id=user_id if user_role not in ("admin", "super_admin") else None,
    )
    return await task_list_response(request, tasks, db)


@router.post("", response_model=TaskResponse, status_code=201)
async def create_task(
    request: Request,
    body: TaskCreate,
    queue: TaskQueue = Depends(_get_queue),
    db: AsyncSession = Depends(get_db),
):
    user_id = get_current_user_id(request)
    if (
        settings.ccm_node_role == "worker"
        and getattr(request.state, "auth_type", None)
        == "worker_control_plane"
        and body.id is None
    ):
        # A Worker is not an independent user-facing scheduler.  Ordinary
        # Tasks are admitted by the authoritative Manager and arrive with the
        # Manager's low-range id, exact incarnation/generation, and delegated
        # principal.  Worker-local high-range Tasks are derived internally
        # (for example a Harness Browser child) and must not be creatable with
        # the broad deployment bearer alone.
        raise HTTPException(
            403,
            "Worker control-plane Task creation requires an explicit "
            "Manager-mirrored Task identity",
        )
    if body.id is not None:
        # Only Manager -> Worker forwarding may choose the globally allocated
        # Task id.  Letting an ordinary JWT caller choose it makes identifier
        # reuse and cross-node identity collisions externally controllable.
        require_internal_service(request)
        _require_worker_task_id_node_role()
        execution_principal = _internal_worker_execution_principal(body)
        source_retry_count, source_turn_generation = (
            _internal_worker_initial_generation(body)
        )
    elif body.source_incarnation_id is not None:
        raise HTTPException(
            422,
            "source_incarnation_id requires internal explicit-id forwarding",
        )
    else:
        if _WORKER_INITIAL_GENERATION_FIELDS.intersection(
            body.model_fields_set
        ):
            raise HTTPException(
                422,
                "Worker source retry/turn generation fields are internal-only",
            )
        if _TASK_EXECUTION_PRINCIPAL_FIELDS.intersection(
            body.model_fields_set
        ):
            raise HTTPException(
                422,
                "Task execution principal fields are internal-only",
            )
        execution_principal = task_execution_principal_from_request(request)
        source_retry_count = None
        source_turn_generation = None
    if body.mode == "plan":
        raise HTTPException(
            410,
            "Legacy mode=plan Task creation is closed; use POST /api/plans",
        )
    if body.session_id is not None or body.last_cwd is not None:
        raise HTTPException(
            422,
            "session_id and last_cwd are internal execution state; use "
            "clone_from_task_id or the migration-import protocol",
        )
    if body.secret_ids:
        require_admin(request)
    if body.ssh_grants:
        from backend.api.deps import require_managed_ssh_auth_configured

        require_managed_ssh_auth_configured()
        require_admin(request)
    data = body.model_dump()
    data.pop("source_retry_count", None)
    data.pop("source_turn_generation", None)
    if body.id is not None:
        # The Worker row must remain inert until its local TaskQueue owns the
        # first provider boundary.  Its dequeue increments G-1 to the exact G
        # already claimed by the Manager.
        data["retry_count"] = source_retry_count
        data["turn_generation"] = source_turn_generation - 1
    data["created_by"] = user_id
    data.update(execution_principal)
    supersedes: Task | None = None
    target: Task | None = None
    if data.get("mode") == "plan":
        from backend.schemas.plan import resolve_plan_pipeline_config
        from backend.services.plan_pipeline_settings import (
            effective_plan_pipeline_config,
        )

        base_pipeline = await effective_plan_pipeline_config(db)

        pipeline = resolve_plan_pipeline_config(
            data.get("plan_pipeline_config"),
            base_config=base_pipeline,
            legacy_provider=(
                data.get("provider") if "provider" in body.model_fields_set else None
            ),
            legacy_model=(
                data.get("model") if "model" in body.model_fields_set else None
            ),
            legacy_effort=(
                data.get("effort_level")
                if "effort_level" in body.model_fields_set
                else None
            ),
        )
        data["plan_pipeline_config"] = pipeline.model_dump(mode="json")
        data["provider"] = pipeline.planner.primary.provider
        data["model"] = pipeline.planner.primary.model
        data["effort_level"] = pipeline.planner.primary.effort
    elif data.get("plan_pipeline_config") is not None:
        raise HTTPException(
            422,
            "plan_pipeline_config requires mode='plan'",
        )

    # Resolve the exact execution target before persisting anything.  A member
    # owning Worker A must not be able to name Worker B (or the Manager) merely
    # because some Worker exists in their account.
    project = None
    if body.project_id is not None:
        from backend.models.project import Project

        project = await db.get(Project, body.project_id)
        if project is None:
            raise HTTPException(404, "Project not found")
        await require_project_access(request, project.id, db)
        _require_dispatchable_project(project)
        if body.worker_id is not None and body.worker_id != project.worker_id:
            raise HTTPException(
                400,
                "Task Worker must match the selected Project location",
            )
        if (
            body.target_repo is not None
            and body.target_repo != project.local_path
        ):
            raise HTTPException(
                422,
                "Task target_repo must match the selected Project local_path",
            )
        data["worker_id"] = project.worker_id
        # Project is the execution authority for its repository path.  Never
        # retain a client-supplied alias (or a stale path copied from a Task).
        data["target_repo"] = project.local_path

    target_worker_id = data.get("worker_id")
    if project is None:
        # Worker ownership is infrastructure management, not a repository ACL.
        # A member may create ordinary work only inside a Project explicitly
        # assigned to them. Internal Manager→Worker materialization already
        # passed Manager admission and keeps its explicit-id protocol here.
        if body.id is None and not is_admin(request):
            raise HTTPException(
                403,
                "Project access is required to create a Task",
            )
        await require_worker_target_access(request, target_worker_id, db)
    if body.frontend_review is not None and target_worker_id is not None:
        raise HTTPException(
            400,
            "Frontend Review Goal currently requires a Manager-local Project",
        )

    try:
        normalized_policy = validate_auto_capability_task_scope(
            data.get("capability_policy"),
            task_id=data.get("id"),
            mode=data.get("mode"),
            worker_id=target_worker_id,
            shared_from_id=data.get("shared_from_id"),
            delivery_run_id=data.get("delivery_run_id"),
            delivery_role=data.get("delivery_role"),
            plan_target_task_id=data.get("plan_target_task_id"),
        )
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    if normalized_policy is None:
        data.pop("capability_policy", None)
    else:
        data["capability_policy"] = normalized_policy

    if data.get("id") is None:
        data.pop("id", None)  # 未指定 → 正常自增；指定 → 用 Manager 分配的全局 ID
    image_paths = data.pop("image_paths", None)
    file_paths = data.pop("file_paths", None)
    attachments = data.pop("attachments", None)
    if (
        file_paths is not None
        or image_paths is not None
        or attachments is not None
    ):
        from backend.api.uploads import (
            UploadAttachmentValidationError,
            validate_upload_attachments,
        )

        try:
            validated_uploads = validate_upload_attachments(
                file_paths=file_paths,
                image_paths=image_paths,
                attachments=attachments,
            )
        except UploadAttachmentValidationError as exc:
            raise HTTPException(422, str(exc)) from exc
        file_paths = [upload.path for upload in validated_uploads]
        image_paths = [
            upload.path for upload in validated_uploads if upload.is_image
        ]
        attachments = [
            upload.public_dict() for upload in validated_uploads
        ]
    secret_ids = data.pop("secret_ids", None)
    ssh_grants = data.pop("ssh_grants", None) or []
    clone_from_task_id = data.pop("clone_from_task_id", None)
    frontend_review = data.pop("frontend_review", None)
    user_skill_snapshots = data.pop("user_skill_snapshots", None)
    if user_skill_snapshots is not None:
        require_admin(request)
    meta = data.get("metadata_") or {}
    all_paths = file_paths or image_paths
    if all_paths:
        # ``file_paths`` is the canonical complete ordering.  Keep the image
        # subset separately for prompt wording and old readers; never label a
        # non-image attachment as an image merely because TaskCreate's legacy
        # schema used ``image_paths`` for every file.
        meta["file_paths"] = all_paths
        meta["image_paths"] = image_paths or []
    if attachments:
        meta["attachments"] = attachments
    if secret_ids:
        meta["secret_ids"] = secret_ids
    if frontend_review is not None:
        from backend.services.frontend_review_goal import (
            FRONTEND_REVIEW_METADATA_KEY,
            build_frontend_review_goal_condition,
            frontend_review_goal_config,
        )

        normalized_frontend_review = frontend_review_goal_config({
            FRONTEND_REVIEW_METADATA_KEY: frontend_review,
        })
        if normalized_frontend_review is None:  # defensive: schema validates this
            raise HTTPException(422, "Invalid Frontend Review Goal configuration")
        meta[FRONTEND_REVIEW_METADATA_KEY] = normalized_frontend_review
        data["mode"] = "goal"
        data["goal_max_turns"] = normalized_frontend_review["max_iterations"]
        data["goal_condition"] = build_frontend_review_goal_condition(
            data.get("goal_condition")
        )
    if body.id is not None or user_skill_snapshots is not None:
        from backend.services.skill_context import (
            USER_SKILL_SNAPSHOTS_METADATA_KEY,
            WORKER_MANAGED_TASK_METADATA_KEY,
        )

    if body.id is not None:
        # Every explicit-id create is a Manager -> Worker mirror, even when
        # no User Skill snapshot is needed.  The provider boundary requires
        # this durable marker before accepting a delegated principal; tying
        # it only to the optional snapshot would strand otherwise valid
        # Worker Tasks at launch time.
        meta[WORKER_MANAGED_TASK_METADATA_KEY] = True
    if user_skill_snapshots is not None:
        meta[USER_SKILL_SNAPSHOTS_METADATA_KEY] = user_skill_snapshots
    if meta:
        data["metadata_"] = meta

    if data.get("plan_target_task_id") is not None:
        if data.get("mode") != "plan":
            raise HTTPException(422, "plan_target_task_id requires mode='plan'")
        target = await db.get(Task, data["plan_target_task_id"])
        if target is None:
            raise HTTPException(404, "Plan target Task not found")
        await require_task_control(request, target, db)
        if not target.session_id:
            raise HTTPException(
                400,
                "Run the target Task before creating a session Plan",
            )
        if target.shared_from_id is not None:
            raise HTTPException(
                409,
                "Shared shadow tasks cannot own Plan Tasks",
            )
        if (
            data.get("worker_id") != target.worker_id
            or data.get("project_id") != target.project_id
        ):
            raise HTTPException(
                422,
                "Related Plan must use the target Task's Project and Worker",
            )
        from backend.services.plan_tasks import (
            ACTIVE_PLAN_STATUSES,
            MAX_ACTIVE_PLANS_PER_TASK,
        )

        active_count = await db.scalar(
            select(func.count(Task.id)).where(
                Task.plan_target_task_id == target.id,
                Task.mode == "plan",
                Task.status.in_(ACTIVE_PLAN_STATUSES),
            )
        )
        if int(active_count or 0) >= MAX_ACTIVE_PLANS_PER_TASK:
            raise HTTPException(
                429,
                f"Task already has {MAX_ACTIVE_PLANS_PER_TASK} active Plans",
            )
        supersedes_id = data.get("supersedes_plan_task_id")
        if supersedes_id is not None:
            supersedes = await db.get(Task, supersedes_id)
            if (
                supersedes is None
                or supersedes.mode != "plan"
                or supersedes.plan_target_task_id != target.id
            ):
                raise HTTPException(
                    400,
                    "Superseded Plan does not belong to this Task",
                )
            await require_task_control(request, supersedes, db)
            if supersedes.status != "plan_review":
                raise HTTPException(
                    409,
                    "Only a Plan awaiting review can be superseded",
                )
        # The execution node owns its LogEntry ids and repository. Re-capture
        # the same target boundary locally instead of trusting a Manager-side
        # watermark whose integer id has no cross-database meaning.
        from backend.services.plan_tasks import capture_task_context, latest_task_log_id

        local_context_log_id = await latest_task_log_id(db, target.id)
        data["plan_context_session_id"] = target.session_id
        data["plan_context_log_id"] = local_context_log_id
        data["plan_context_snapshot"] = await capture_task_context(
            db,
            target.id,
            through_log_id=local_context_log_id,
            max_chars=settings.plan_transcript_max_chars,
        )
        from backend.services.plan_tasks import capture_repo_revision

        data["plan_repo_revision"] = await capture_repo_revision(
            target.last_cwd or target.target_repo
        )
    elif data.get("mode") != "plan":
        for plan_only_field in (
            "plan_context_session_id",
            "plan_context_log_id",
            "plan_context_snapshot",
            "plan_repo_revision",
            "supersedes_plan_task_id",
        ):
            if data.get(plan_only_field) is not None:
                raise HTTPException(
                    422,
                    f"{plan_only_field} requires mode='plan'",
                )
    elif data.get("supersedes_plan_task_id") is not None:
        supersedes = await db.get(Task, data["supersedes_plan_task_id"])
        if (
            supersedes is None
            or supersedes.mode != "plan"
            or supersedes.plan_target_task_id is not None
        ):
            raise HTTPException(
                400,
                "Standalone Plan can only supersede another standalone Plan",
            )
        await require_task_control(request, supersedes, db)
        if supersedes.status != "plan_review":
            raise HTTPException(
                409,
                "Only a Plan awaiting review can be superseded",
            )
        if (
            data.get("project_id") != supersedes.project_id
            or data.get("worker_id") != supersedes.worker_id
        ):
            raise HTTPException(
                422,
                "A standalone Plan revision must keep its Project and Worker",
            )

    if clone_from_task_id:
        source = await db.get(Task, clone_from_task_id)
        if source is None:
            raise HTTPException(404, "Clone source task not found")
        await require_task_control(request, source, db)
        _require_not_delivery_owned_task(source, action="used as clone sources")
        if source.mode == "plan" or source.canonical_plan_id is not None:
            raise HTTPException(
                409,
                "Plan Tasks cannot be used as clone sources; materialize the "
                "approved canonical Plan through its execution workflow",
            )
        if "attention_tag" not in body.model_fields_set:
            data["attention_tag"] = source.attention_tag
        cloned = await _clone_session(clone_from_task_id, db)
        if cloned:
            data["session_id"] = cloned["session_id"]
            data["last_cwd"] = cloned["last_cwd"]

    if data.get("mode") == "plan" and data.get("plan_repo_revision") is None:
        from backend.services.plan_tasks import capture_repo_revision

        if data.get("worker_id") is None:
            data["plan_repo_revision"] = await capture_repo_revision(
                data.get("last_cwd") or data.get("target_repo")
            )
    validation_skills = dict(data.get("enabled_skills") or {})
    validation_skills.update(_explicit_command_skills(data.get("description")))
    data["selected_user_skills"] = await _validate_skill_configuration(
        db,
        provider=data.get("provider"),
        enabled_skills=validation_skills,
        selected_user_skills=data.get("selected_user_skills"),
        user_skill_snapshots=user_skill_snapshots,
        worker_id=data.get("worker_id"),
        shared_from_id=data.get("shared_from_id"),
        metadata=data.get("metadata_"),
    )
    try:
        validate_task_service_tier_configuration(
            provider=data.get("provider"),
            model=data.get("model"),
            codex_service_tier=data.get("codex_service_tier"),
            mode=data.get("mode"),
            goal_evaluator_model=data.get("goal_evaluator_model"),
        )
        if data.get("mode") == "plan":
            from backend.schemas.plan import PlanPipelineConfig

            pipeline = PlanPipelineConfig.model_validate(data["plan_pipeline_config"])
            for route in (
                pipeline.planner.primary,
                pipeline.planner.fallback,
                pipeline.reviewer.primary,
                pipeline.reviewer.fallback,
            ):
                validate_task_service_tier_configuration(
                    provider=route.provider,
                    model=route.model,
                    codex_service_tier="default",
                    mode="plan",
                    goal_evaluator_model=None,
                )
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc

    from backend.services.task_ssh_access import (
        TaskSSHAccessError,
        prepare_task_ssh_grants,
        task_ssh_grant_rows,
    )

    try:
        prepared_ssh_grants = await prepare_task_ssh_grants(
            db,
            ssh_grants,
            worker_id=data.get("worker_id"),
            shared_from_id=data.get("shared_from_id"),
            metadata=data.get("metadata_"),
            project_id=data.get("project_id"),
        )
    except TaskSSHAccessError as exc:
        raise HTTPException(exc.status_code, exc.detail) from exc

    if supersedes is None:
        if project is not None:
            # All validation above is optimistic.  TeamProjectShare removal
            # uses this same Project writer fence; re-read the authoritative
            # Project after any wait immediately before Task materialization.
            # The Worker-node fence is globally outermost and remains held
            # through the Project/User authority checks and Task insert.
            project_id = project.id
            await db.rollback()
            await fence_worker_node_mutation(db)
            project = await lock_project_worker_effect_access(
                request,
                project_id,
                db,
            )
            _require_dispatchable_project(project)
            if body.worker_id is not None and body.worker_id != project.worker_id:
                raise HTTPException(
                    400,
                    "Task Worker must match the selected Project location",
                )
            if (
                body.target_repo is not None
                and body.target_repo != project.local_path
            ):
                raise HTTPException(
                    422,
                    "Task target_repo must match the selected Project local_path",
                )
            data["worker_id"] = project.worker_id
            data["target_repo"] = project.local_path
        else:
            # Projectless Task creation is administrator-only, but the role in
            # request.state was frozen by authentication before the lengthy
            # validation above. End that read snapshot, establish the outer
            # node fence, then lock Worker before the exact active JWT role in
            # the transaction which will publish the Task. Deployment/control
            # credentials have no mutable User row.
            await db.rollback()
            await fence_worker_node_mutation(db)
            await lock_worker_effect_access(
                request,
                data.get("worker_id"),
                db,
            )
        task = await stage_task_record(db, **data)
        if prepared_ssh_grants:
            db.add_all(task_ssh_grant_rows(
                task.id,
                prepared_ssh_grants,
                created_by=user_id,
            ))
        await db.commit()
        await db.refresh(task)
    else:
        superseded_id = supersedes.id
        superseded_target_id = supersedes.plan_target_task_id
        supersedes_snapshot = (
            supersedes.incarnation_id,
            supersedes.mode,
            supersedes.status,
            supersedes.plan_target_task_id,
            supersedes.project_id,
            supersedes.worker_id,
        )
        supersedes_probe = SimpleNamespace(
            id=superseded_id,
            project_id=supersedes.project_id,
        )
        target_snapshot = None
        target_probe = None
        if superseded_target_id is not None:
            if target is None or target.id != superseded_target_id:
                raise HTTPException(
                    409,
                    "Superseded Plan target changed while creating a revision",
                )
            if (
                supersedes.project_id != target.project_id
                or supersedes.worker_id != target.worker_id
            ):
                raise HTTPException(
                    409,
                    "Superseded Plan routing no longer matches its target",
                )
            target_snapshot = (
                target.incarnation_id,
                target.status,
                target.session_id,
                target.shared_from_id,
                target.project_id,
                target.worker_id,
            )
            target_probe = SimpleNamespace(
                id=target.id,
                project_id=target.project_id,
            )
        metadata = dict(data.get("metadata_") or {})
        metadata["revised_from_plan_task_id"] = superseded_id
        data["metadata_"] = metadata
        from backend.services.plan_tasks import (
            mark_plan_superseded,
            PlanTerminalQuiescenceError,
            run_plan_terminal_transition,
        )

        async with get_task_operation_lock(superseded_id):
            async def authorize_legacy_supersede_effect() -> None:
                probes = [supersedes_probe]
                if target_probe is not None:
                    probes.append(target_probe)
                locked = await lock_task_effect_accesses(
                    request,
                    probes,
                    db,
                    allow_chat_share=False,
                    fence_worker_node=True,
                    fence_worker_assignment=True,
                )
                locked_by_id = {item.id: item for item in locked}
                current_supersedes = locked_by_id.get(superseded_id)
                if current_supersedes is None or supersedes_snapshot != (
                    current_supersedes.incarnation_id,
                    current_supersedes.mode,
                    current_supersedes.status,
                    current_supersedes.plan_target_task_id,
                    current_supersedes.project_id,
                    current_supersedes.worker_id,
                ):
                    raise HTTPException(
                        409,
                        "Plan changed while its revision was being created",
                    )
                if superseded_target_id is None:
                    return
                current_target = locked_by_id.get(superseded_target_id)
                if current_target is None or target_snapshot != (
                    current_target.incarnation_id,
                    current_target.status,
                    current_target.session_id,
                    current_target.shared_from_id,
                    current_target.project_id,
                    current_target.worker_id,
                ):
                    raise HTTPException(
                        409,
                        "Plan target changed while creating the revision",
                    )
                if await active_worker_task_termination_receipt(
                    db,
                    current_target.id,
                ):
                    raise HTTPException(
                        409,
                        "Plan target has an active Worker termination receipt",
                    )

            async def commit_legacy_standalone_supersede() -> Task:
                db.expire_all()
                current_supersedes = await db.get(
                    Task,
                    superseded_id,
                    populate_existing=True,
                )
                if (
                    current_supersedes is None
                    or current_supersedes.mode != "plan"
                    or current_supersedes.plan_target_task_id
                    != superseded_target_id
                ):
                    raise HTTPException(
                        400,
                        "Superseded Plan routing changed while creating "
                        "the revision",
                    )
                if current_supersedes.status != "plan_review":
                    raise HTTPException(
                        409,
                        "Only a Plan awaiting review can be superseded",
                    )
                if superseded_target_id is not None:
                    exact_active_count = await db.scalar(
                        select(func.count(Task.id)).where(
                            Task.plan_target_task_id == superseded_target_id,
                            Task.mode == "plan",
                            Task.status.in_(ACTIVE_PLAN_STATUSES),
                        )
                    )
                    if int(exact_active_count or 0) >= MAX_ACTIVE_PLANS_PER_TASK:
                        raise HTTPException(
                            429,
                            "Task already has "
                            f"{MAX_ACTIVE_PLANS_PER_TASK} active Plans",
                        )
                staged = await stage_task_record(db, **data)
                if prepared_ssh_grants:
                    db.add_all(task_ssh_grant_rows(
                        staged.id,
                        prepared_ssh_grants,
                        created_by=user_id,
                    ))
                if not await mark_plan_superseded(
                    db,
                    current_supersedes,
                    successor_id=staged.id,
                ):
                    raise HTTPException(
                        409,
                        "Plan changed while its revision was being created",
                    )
                return staged

            try:
                task = await run_plan_terminal_transition(
                    db,
                    superseded_id,
                    "superseded",
                    commit_legacy_standalone_supersede,
                    authorize_effect_boundary=authorize_legacy_supersede_effect,
                )
            except PlanTerminalQuiescenceError as exc:
                raise HTTPException(409, str(exc)) from exc
        await db.refresh(task)
    # Eliminate the dispatcher's historical 0-2s polling delay.  Importing
    # here avoids a module cycle during application construction.
    try:
        from backend.main import dispatcher

        if dispatcher:
            dispatcher.wake()
    except Exception:
        pass

    return await task_response(request, task, db, status_code=201)


def _migration_import_prepare_reservation(
    body: TaskMigrationImport,
) -> WorkerMigrationImportReservation:
    source_incarnation_id = body.source_incarnation_id
    if source_incarnation_id is None:
        # The Pydantic transport validator rejects this before the endpoint,
        # but retain a fail-closed boundary for direct/internal callers.
        raise HTTPException(
            422,
            "Migration import requires a source incarnation identity",
        )
    return WorkerMigrationImportReservation(
        operation_id=body.migration_operation_id,
        operation_sequence=body.migration_operation_sequence,
        incarnation_id=source_incarnation_id,
        retry_count=body.retry_count,
        turn_generation=body.turn_generation,
        source_status=body.source_status,
    )


def _worker_migration_operation_matches(
    operation: TaskMigrationOperation,
    reservation: WorkerMigrationImportReservation,
) -> bool:
    return (
        operation.side == "worker"
        and operation.operation_id == reservation.operation_id
        and operation.operation_sequence == reservation.operation_sequence
        and operation.task_incarnation_id == reservation.incarnation_id
        and operation.retry_count == reservation.retry_count
        and operation.turn_generation == reservation.turn_generation
        and operation.source_status == reservation.source_status
        and operation.source_worker_id is None
        and operation.target_worker_id is None
    )


async def _latest_worker_migration_operation(
    db: AsyncSession,
    task_id: int,
) -> TaskMigrationOperation | None:
    operation = (
        await db.execute(
            select(TaskMigrationOperation)
            .where(TaskMigrationOperation.task_id == task_id)
            .order_by(TaskMigrationOperation.operation_sequence.desc())
            .limit(1)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if operation is not None and operation.side != "worker":
        raise HTTPException(
            409,
            "Worker migration registry contains a foreign Manager operation",
        )
    return operation


def _new_worker_migration_operation(
    *,
    task_id: int,
    reservation: WorkerMigrationImportReservation,
    phase: Literal["prepared", "rolled_back"],
) -> TaskMigrationOperation:
    return TaskMigrationOperation(
        operation_id=reservation.operation_id,
        operation_sequence=reservation.operation_sequence,
        side="worker",
        active_task_id=task_id if phase == "prepared" else None,
        task_id=task_id,
        task_incarnation_id=reservation.incarnation_id,
        retry_count=reservation.retry_count,
        turn_generation=reservation.turn_generation,
        source_worker_id=None,
        target_worker_id=None,
        source_status=reservation.source_status,
        phase=phase,
        instance_id=None,
        started_at=None,
        completed_at=None,
    )


async def _admit_worker_migration_prepare(
    db: AsyncSession,
    *,
    task_id: int,
    reservation: WorkerMigrationImportReservation,
) -> TaskMigrationOperation:
    """Install or replay one exact prepared receipt under node control."""

    latest = await _latest_worker_migration_operation(db, task_id)
    if latest is not None:
        if reservation.operation_sequence < latest.operation_sequence:
            raise HTTPException(409, "Migration import operation is stale")
        if reservation.operation_sequence == latest.operation_sequence:
            if not _worker_migration_operation_matches(latest, reservation):
                raise HTTPException(
                    409,
                    "Migration import sequence belongs to another operation",
                )
            if latest.phase == "prepared":
                return latest
            raise HTTPException(
                409,
                f"Migration import operation is already {latest.phase}",
            )
        if latest.phase == "prepared":
            raise HTTPException(
                409,
                "A previous migration import is still prepared",
            )

    operation = _new_worker_migration_operation(
        task_id=task_id,
        reservation=reservation,
        phase="prepared",
    )
    db.add(operation)
    try:
        await db.flush()
    except IntegrityError as exc:
        await db.rollback()
        raise HTTPException(
            409,
            "Migration import operation conflicts with durable history",
        ) from exc
    return operation


@router.post(
    "/migration-import",
    response_model=TaskMigrationImportResponse,
    status_code=201,
)
async def import_migrated_task(
    request: Request,
    body: TaskMigrationImport,
    db: AsyncSession = Depends(get_db),
):
    """Create or refresh an inert task copied from a Manager.

    A normal task create commits ``pending`` and immediately wakes the local
    dispatcher.  Task migration used to call that endpoint and cancel in a
    second request, leaving a real window where the destination Worker could
    claim and execute the imported task.  This admin-only endpoint persists
    only a non-dispatchable source status in the same transaction and never
    wakes the dispatcher.

    Existing inactive copies are refreshed with a status CAS.  If a legacy
    copy has already become active, fail closed instead of cancelling work
    which may really be running.
    """
    require_internal_service(request)
    _require_worker_task_id_node_role()
    migration_principal = _internal_worker_execution_principal(
        body,
        require_system=True,
    )
    import_reservation = _migration_import_prepare_reservation(body)

    if body.mode == "delivery_loop":
        raise HTTPException(
            409,
            "Delivery-owned Tasks cannot be imported; DeliveryRun remains "
            "the only orchestration authority",
        )

    existing = await db.get(Task, body.id)
    if existing is not None:
        await _require_migration_import_eligible(db, existing, body)

    data = body.model_dump()
    # ``source_incarnation_id`` is a transport-only name.  New imports map it
    # inside ``stage_task_record``; the UPDATE path must perform the same
    # mapping explicitly because it writes Task columns directly.  An omitted
    # source fence deliberately preserves the destination row's incarnation.
    source_incarnation_id = data.pop("source_incarnation_id", None)
    data.pop("migration_operation_id")
    data.pop("migration_operation_sequence")
    source_status = data.pop("source_status")
    user_skill_snapshots = data.pop("user_skill_snapshots", None)
    frontend_review = data.pop("frontend_review", None)
    ssh_grants = data.pop("ssh_grants", None)
    if ssh_grants:
        raise HTTPException(
            400,
            "Managed SSH grants cannot be imported to a Worker",
        )
    for transient_field in (
        "source_retry_count",
        "source_turn_generation",
        "image_paths",
        "file_paths",
        "attachments",
        "secret_ids",
        "clone_from_task_id",
    ):
        data.pop(transient_field, None)
    from backend.services.skill_context import (
        USER_SKILL_SNAPSHOTS_METADATA_KEY,
        WORKER_MANAGED_TASK_METADATA_KEY,
    )

    migration_metadata = {
        WORKER_MANAGED_TASK_METADATA_KEY: True,
    }
    if existing is not None:
        existing_binding = (existing.metadata_ or {}).get(
            SOURCE_TASK_INCARNATION_METADATA_KEY
        )
        if existing_binding is not None:
            migration_metadata[SOURCE_TASK_INCARNATION_METADATA_KEY] = (
                existing_binding
            )
    if source_incarnation_id is not None:
        migration_metadata[SOURCE_TASK_INCARNATION_METADATA_KEY] = (
            source_incarnation_id
        )
    # The Task marker is a convenient exact-generation mirror; the separate
    # TaskMigrationOperation row is the durable ordering authority that also
    # survives rollback deletion of this Task.
    migration_metadata = with_worker_migration_import_reservation(
        migration_metadata,
        import_reservation,
    )
    if user_skill_snapshots is not None:
        migration_metadata[USER_SKILL_SNAPSHOTS_METADATA_KEY] = user_skill_snapshots
    if frontend_review is not None:
        from backend.services.frontend_review_goal import (
            FRONTEND_REVIEW_METADATA_KEY,
            build_frontend_review_goal_condition,
            frontend_review_goal_config,
        )

        normalized_frontend_review = frontend_review_goal_config({
            FRONTEND_REVIEW_METADATA_KEY: frontend_review,
        })
        if normalized_frontend_review is not None:
            migration_metadata[FRONTEND_REVIEW_METADATA_KEY] = (
                normalized_frontend_review
            )
            data["mode"] = "goal"
            data["goal_max_turns"] = normalized_frontend_review["max_iterations"]
            data["goal_condition"] = build_frontend_review_goal_condition(
                data.get("goal_condition")
            )
    data["metadata_"] = migration_metadata
    data.update(
        worker_id=None,
        status=source_status,
        created_by=get_current_user_id(request),
    )
    # Migration imports are inert transport state, not permission-bearing
    # turns.  Preserve the Manager's explicit fail-closed system principal;
    # never derive authority from the Worker's deployment bearer token.
    data.update(migration_principal)

    data = prepare_task_create_values(data)
    validation_skills = dict(data.get("enabled_skills") or {})
    validation_skills.update(_explicit_command_skills(data.get("description")))
    data["selected_user_skills"] = await _validate_skill_configuration(
        db,
        provider=data.get("provider"),
        enabled_skills=validation_skills,
        selected_user_skills=data.get("selected_user_skills"),
        user_skill_snapshots=user_skill_snapshots,
        worker_id=None,
        shared_from_id=None,
        metadata=data.get("metadata_"),
    )
    try:
        validate_task_service_tier_configuration(
            provider=data.get("provider"),
            model=data.get("model"),
            codex_service_tier=data.get("codex_service_tier"),
            mode=data.get("mode"),
            goal_evaluator_model=data.get("goal_evaluator_model"),
        )
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc

    if existing is None:
        # The first visible state is already inert.  In particular there is no
        # pending commit and no dispatcher.wake() between create and cancel.
        # Persist the prepared receipt and Task in one node-fenced transaction
        # so neither a concurrent drain nor a rollback can observe only one.
        await db.rollback()
        async with get_task_operation_lock(body.id):
            await fence_worker_node_mutation(db)
            await _admit_worker_migration_prepare(
                db,
                task_id=body.id,
                reservation=import_reservation,
            )
            if await db.get(Task, body.id) is not None:
                await db.rollback()
                raise HTTPException(
                    409,
                    "Destination task appeared during migration import; retry",
                )
            try:
                task = await stage_task_record(
                    db,
                    source_incarnation_id=source_incarnation_id,
                    **data,
                )
                await db.commit()
                await db.refresh(task)
                return task
            except HTTPException:
                await db.rollback()
                raise
            except Exception:
                await db.rollback()
                raise

    old_status = existing.status
    observed_incarnation_id = existing.incarnation_id
    existing_generation = _task_generation_fence(body.id, existing)
    if old_status in ("in_progress", "executing", "migrating"):
        raise HTTPException(
            409,
            f"Destination task {body.id} is active ({old_status})",
        )

    values = {key: value for key, value in data.items() if key != "id"}
    if source_incarnation_id is not None:
        values["incarnation_id"] = source_incarnation_id
    # End every validation read before competing with receipt admission.  The
    # no-op/write CAS below must be the first statement of a fresh transaction
    # so a receipt that committed through another SQLite WAL connection cannot
    # turn this into BUSY_SNAPSHOT or be overwritten by the stale import.
    await db.rollback()
    async with get_task_operation_lock(body.id):
        # Existing-row refresh is the same Worker-local mutation as the first
        # import INSERT.  Take node-control first so an irreversible drain and
        # this complete Task transaction have one deterministic winner.
        await fence_worker_node_mutation(db)
        await _admit_worker_migration_prepare(
            db,
            task_id=body.id,
            reservation=import_reservation,
        )
        fence_predicates = (
            *existing_generation,
            (
                Task.incarnation_id.is_(None)
                if observed_incarnation_id is None
                else Task.incarnation_id == observed_incarnation_id
            ),
            Task.mode != "delivery_loop",
            Task.delivery_run_id.is_(None),
            no_active_worker_task_termination_predicate(),
        )
        writer_fence = await db.execute(
            sa_update(Task)
            .where(*fence_predicates)
            .values(id=Task.id)
            .execution_options(synchronize_session=False)
        )
        if writer_fence.rowcount != 1:
            await db.rollback()
            if await active_worker_task_termination_receipt(db, body.id):
                await db.rollback()
                raise HTTPException(
                    409,
                    "Destination task has an active Worker termination receipt",
                )
            await db.rollback()
            raise HTTPException(
                409,
                "Destination task changed during migration import",
            )
        db.expire_all()
        locked_existing = await db.get(
            Task,
            body.id,
            with_for_update=True,
            populate_existing=True,
        )
        if locked_existing is None:
            await db.rollback()
            raise HTTPException(
                409,
                "Destination task disappeared during migration import",
        )
        try:
            await _require_migration_import_eligible(
                db,
                locked_existing,
                body,
            )
        except HTTPException:
            await db.rollback()
            raise

        result = await db.execute(
            sa_update(Task)
            .where(*fence_predicates)
            .values(**values)
            .execution_options(synchronize_session=False)
        )
        if result.rowcount != 1:
            await db.rollback()
            if await active_worker_task_termination_receipt(db, body.id):
                await db.rollback()
                raise HTTPException(
                    409,
                    "Destination task has an active Worker termination receipt",
                )
            await db.rollback()
            raise HTTPException(
                409,
                "Destination task changed during migration import",
            )
        await purge_task_access_grants(db, body.id)
        db.expire_all()
        task = await db.get(Task, body.id)
        if task is None:
            await db.rollback()
            raise HTTPException(
                409,
                "Destination task disappeared during migration import",
            )
        publication_generation = _task_generation_fence(body.id, task)
        resulting_incarnation_id = task.incarnation_id
        await db.commit()

        if old_status != source_status:
            # The imported state is already durable before publication.  Hold
            # a fresh exact-result no-op write across the WebSocket await so a
            # retry, migration, or termination receipt cannot cross this old
            # status event.  A receipt which wins after the import commit owns
            # subsequent publication; the import itself remains successful.
            await fence_worker_node_mutation(db)
            guarded = await db.execute(
                sa_update(Task)
                .where(
                    *publication_generation,
                    (
                        Task.incarnation_id.is_(None)
                        if resulting_incarnation_id is None
                        else Task.incarnation_id == resulting_incarnation_id
                    ),
                    no_active_worker_task_termination_predicate(),
                )
                .values(status=source_status)
                .execution_options(synchronize_session=False)
            )
            if guarded.rowcount == 1:
                from backend.services.task_events import broadcast_status_change

                await broadcast_status_change(task.id, source_status)
                await db.commit()
            else:
                await db.rollback()
                db.expire_all()
                task = await db.get(Task, body.id)
                if task is None:
                    raise HTTPException(
                        409,
                        "Destination task disappeared after migration import",
                    )
        return task


def _migration_import_operation(
    body: TaskMigrationImportRollback | TaskMigrationImportCommit,
) -> WorkerMigrationImportReservation:
    return WorkerMigrationImportReservation(
        operation_id=body.operation_id,
        operation_sequence=body.operation_sequence,
        incarnation_id=body.incarnation_id,
        retry_count=body.retry_count,
        turn_generation=body.turn_generation,
        source_status=body.source_status,
    )


async def _require_exact_worker_migration_operation(
    db: AsyncSession,
    *,
    task_id: int,
    reservation: WorkerMigrationImportReservation,
    allowed_phases: frozenset[str],
) -> TaskMigrationOperation:
    latest = await _latest_worker_migration_operation(db, task_id)
    if latest is None:
        raise HTTPException(409, "Migration import operation is unknown")
    if reservation.operation_sequence < latest.operation_sequence:
        raise HTTPException(409, "Migration import operation is stale")
    if reservation.operation_sequence > latest.operation_sequence:
        raise HTTPException(409, "Migration import operation was never prepared")
    if not _worker_migration_operation_matches(latest, reservation):
        raise HTTPException(
            409,
            "Migration import sequence belongs to another operation",
        )
    if latest.phase not in allowed_phases:
        raise HTTPException(
            409,
            f"Migration import operation is already {latest.phase}",
        )
    return latest


async def _persist_missing_worker_migration_rollback(
    db: AsyncSession,
    *,
    task_id: int,
    reservation: WorkerMigrationImportReservation,
    allow_create: bool,
) -> TaskMigrationOperation | None:
    """Record rollback-before-import so a delayed prepare cannot resurrect."""

    latest = await _latest_worker_migration_operation(db, task_id)
    if latest is not None:
        if reservation.operation_sequence < latest.operation_sequence:
            raise HTTPException(409, "Migration import operation is stale")
        if reservation.operation_sequence == latest.operation_sequence:
            if not _worker_migration_operation_matches(latest, reservation):
                raise HTTPException(
                    409,
                    "Migration import sequence belongs to another operation",
                )
            if latest.phase == "committed":
                raise HTTPException(
                    409,
                    "Committed migration import cannot be rolled back",
                )
            return latest
        if latest.phase == "prepared":
            raise HTTPException(
                409,
                "A previous migration import is still prepared",
            )

    if not allow_create:
        # The irreversible node drain claim is the durable proof that a
        # delayed import cannot materialize after this acknowledgement.
        return None

    operation = _new_worker_migration_operation(
        task_id=task_id,
        reservation=reservation,
        phase="rolled_back",
    )
    db.add(operation)
    try:
        await db.flush()
    except IntegrityError as exc:
        await db.rollback()
        raise HTTPException(
            409,
            "Migration rollback conflicts with durable history",
        ) from exc
    return operation


async def _lock_migration_import_operation(
    db: AsyncSession,
    body: TaskMigrationImportRollback | TaskMigrationImportCommit,
    *,
    allow_missing: bool,
    require_delete_safe: bool,
    allow_committed: bool,
) -> tuple[Task | None, str]:
    """Lock scalar generation first, then compare JSON in Python.

    PostgreSQL's generic ``JSON`` type has no equality operator, while SQLite
    JSON text equality is key-order-sensitive.  A Task no-op writer gives all
    supported databases the required serialization; the exact reservation is
    then validated from the locked row without a non-portable JSON predicate.
    """

    predicates = [
        Task.id == body.task_id,
        Task.incarnation_id == body.incarnation_id,
        Task.retry_count == body.retry_count,
        Task.turn_generation == body.turn_generation,
        Task.status == body.source_status,
        Task.worker_id.is_(None),
        Task.shared_from_id.is_(None),
        no_active_worker_task_termination_predicate(),
    ]
    if require_delete_safe:
        predicates.extend((
            Task.instance_id.is_(None),
            Task.pty_background_generation.is_(None),
            no_active_test_harness_owner_graph_predicate(),
        ))
    guarded = await db.execute(
        sa_update(Task)
        .where(*predicates)
        .values(status=Task.status)
        .execution_options(synchronize_session=False)
    )
    if guarded.rowcount != 1:
        await db.rollback()
        current = await db.get(Task, body.task_id)
        if current is None and allow_missing:
            await db.rollback()
            return None, "missing"
        await db.rollback()
        raise HTTPException(
            409,
            "Destination migration import generation changed",
        )

    db.expire_all()
    task = await db.get(
        Task,
        body.task_id,
        with_for_update=True,
        populate_existing=True,
    )
    if task is None:
        await db.rollback()
        if allow_missing:
            return None, "missing"
        raise HTTPException(409, "Destination migration import disappeared")

    expected = _migration_import_operation(body)
    try:
        reservation = read_worker_migration_import_reservation(task)
        committed = read_worker_migration_import_commit_receipt(task)
    except InvalidWorkerRoutingMarker as exc:
        await db.rollback()
        raise HTTPException(409, str(exc)) from exc
    if reservation == expected:
        return task, "prepared"
    if allow_committed and committed == expected:
        return task, "committed"
    await db.rollback()
    raise HTTPException(
        409,
        "Destination migration import operation changed",
    )


@router.post("/migration-import/commit", include_in_schema=False)
async def commit_migrated_task_import(
    request: Request,
    body: TaskMigrationImportCommit,
    db: AsyncSession = Depends(get_db),
):
    """Permanently disable rollback after the Manager pointer commits."""

    require_internal_service(request)
    _require_worker_task_id_node_role()
    reservation = _migration_import_operation(body)
    await db.rollback()
    async with get_task_operation_lock(body.task_id):
        await fence_worker_node_receipt_resolution(db)
        operation = await _require_exact_worker_migration_operation(
            db,
            task_id=body.task_id,
            reservation=reservation,
            allowed_phases=frozenset({"prepared", "committed"}),
        )
        # ``_lock_migration_import_operation`` expires the identity map after
        # its scalar writer fence.  Snapshot the already row-locked phase so a
        # later synchronous attribute read cannot trigger async lazy loading.
        operation_phase = operation.phase
        task, state = await _lock_migration_import_operation(
            db,
            body,
            allow_missing=False,
            require_delete_safe=False,
            allow_committed=True,
        )
        assert task is not None
        if (operation_phase, state) not in {
            ("prepared", "prepared"),
            ("committed", "committed"),
        }:
            await db.rollback()
            raise HTTPException(
                409,
                "Migration import receipt and Task marker disagree",
            )
        if state == "prepared":
            task.metadata_ = with_worker_migration_import_commit_receipt(
                task.metadata_,
                reservation,
            )
            operation.phase = "committed"
            operation.active_task_id = None
            operation.updated_at = datetime.utcnow()
            await db.commit()
        else:
            await db.rollback()
        return {
            "ok": True,
            "committed": True,
            "task_id": body.task_id,
            "operation_id": body.operation_id,
            "operation_sequence": body.operation_sequence,
        }


@router.post("/migration-import/rollback", include_in_schema=False)
async def rollback_migrated_task_import(
    request: Request,
    body: TaskMigrationImportRollback,
    queue: TaskQueue = Depends(_get_queue),
    db: AsyncSession = Depends(get_db),
):
    """Remove only the exact inert mirror from one failed migration attempt."""

    require_internal_service(request)
    _require_worker_task_id_node_role()
    reservation = _migration_import_operation(body)
    await db.rollback()
    async with get_task_operation_lock(body.task_id):
        # Process-local order is Task operation -> Harness owner. Inside that
        # lease, the database writer order remains node-control -> Task. Never
        # hold the node row while waiting for the owner fence: Workspace/Harness
        # materialization holds owner first and then fences node-control.
        async with test_harness_owner_fence(body.task_id):
            node_draining = await fence_worker_node_receipt_resolution(db)
            operation = await _persist_missing_worker_migration_rollback(
                db,
                task_id=body.task_id,
                reservation=reservation,
                allow_create=not node_draining,
            )
            if operation is None or operation.phase == "rolled_back":
                await db.commit()
                return {
                    "ok": True,
                    "removed": False,
                    "task_id": body.task_id,
                    "operation_id": body.operation_id,
                    "operation_sequence": body.operation_sequence,
                }

            existing_task = await db.get(Task, body.task_id)
            if existing_task is None:
                operation.phase = "rolled_back"
                operation.active_task_id = None
                operation.updated_at = datetime.utcnow()
                await db.commit()
                return {
                    "ok": True,
                    "removed": False,
                    "task_id": body.task_id,
                    "operation_id": body.operation_id,
                    "operation_sequence": body.operation_sequence,
                }

            task, _state = await _lock_migration_import_operation(
                db,
                body,
                allow_missing=True,
                require_delete_safe=True,
                allow_committed=False,
            )
            if task is None:
                # The Task was present under the same node/process fences, so
                # losing it here is an unprovable cross-process mutation.
                raise HTTPException(
                    409,
                    "Destination migration import disappeared during rollback",
                )
            if await _migration_import_has_durable_dependents(db, body.task_id):
                await db.rollback()
                raise HTTPException(
                    409,
                    "Destination migration import owns durable history; refusing cleanup",
                )

            async def settle_rollback_receipt(_preflight) -> bool:
                settled = await db.execute(
                    sa_update(TaskMigrationOperation)
                    .where(
                        TaskMigrationOperation.operation_id
                        == reservation.operation_id,
                        TaskMigrationOperation.operation_sequence
                        == reservation.operation_sequence,
                        TaskMigrationOperation.side == "worker",
                        TaskMigrationOperation.active_task_id == body.task_id,
                        TaskMigrationOperation.task_id == body.task_id,
                        TaskMigrationOperation.phase == "prepared",
                    )
                    .values(
                        phase="rolled_back",
                        active_task_id=None,
                        updated_at=datetime.utcnow(),
                    )
                    .execution_options(synchronize_session=False)
                )
                return settled.rowcount == 1

            removed = await queue.delete(
                body.task_id,
                owner_fence_held=True,
                expected_fence=task_delete_fence(task),
                before_delete=settle_rollback_receipt,
            )
            if not removed:
                await db.rollback()
                raise HTTPException(
                    409,
                    "Destination migration import cleanup graph changed",
                )
            return {
                "ok": True,
                "removed": True,
                "task_id": body.task_id,
                "operation_id": body.operation_id,
                "operation_sequence": body.operation_sequence,
            }


@router.get("/{task_id}/legacy-plan-execution-carrier-proof")
async def get_legacy_plan_execution_carrier_proof(
    task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Return semantic proof for an existing migrated Worker carrier.

    This endpoint is deliberately read-only and internal-only.  It cannot
    create, approve, retry, or wake a Task; the Manager must subscribe before
    readback and let the Worker's own dispatcher execute the already-present
    carrier.
    """

    require_internal_service(request)
    if await db.get(Task, task_id) is None:
        raise HTTPException(404, "Task not found")
    from backend.services.legacy_plan_execution import (
        legacy_approved_execution_carrier_proof,
    )
    proof = await legacy_approved_execution_carrier_proof(db, task_id)
    if proof is None:
        raise HTTPException(
            409,
            "Task is not an exact migrated approved Plan execution carrier",
        )
    return proof.to_wire()


@router.get("/{task_id}", response_model=TaskResponse)
async def get_task(
    task_id: int,
    request: Request,
    queue: TaskQueue = Depends(_get_queue),
    db: AsyncSession = Depends(get_db),
):
    task = await require_worker_control_plane_task_incarnation(
        request,
        task_id,
        db,
    )
    if task is None:
        task = await queue.get(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    await require_internal_task_incarnation(request, task_id, db)
    await require_task_access(request, task, db)
    return await task_response(request, task, db)


def _normalized_task_update_values(updates: dict) -> dict:
    """Mirror TaskQueue's explicit-NULL handling for one fenced UPDATE."""

    normalized = {}
    for key, value in updates.items():
        if value is None:
            mapped_attr = getattr(Task, key, None)
            columns = getattr(
                getattr(mapped_attr, "property", None),
                "columns",
                (),
            )
            if not columns or not columns[0].nullable:
                continue
        normalized[key] = value
    return normalized


def _routing_request_tuple(
    body: WorkerRoutingConfigRequest,
) -> WorkerRoutingTuple:
    return WorkerRoutingTuple(
        provider=body.provider,
        model=body.model,
        codex_service_tier=body.codex_service_tier,
    )


def _codex_account_binding(task: Task) -> object | None:
    metadata = task.metadata_
    if not isinstance(metadata, dict):
        return None
    return metadata.get("codex_account_id")


def _resolve_codex_thread_routing_home(task: Task) -> str:
    """Resolve one thread home without guessing between rollout copies."""

    from backend.main import codex_pool
    from backend.services.codex_app_server import normalize_codex_home

    binding = _codex_account_binding(task)
    if codex_pool is not None and binding is not None:
        bound_home = codex_pool.home_for_account(str(binding))
        if not bound_home:
            raise HTTPException(
                409,
                "Codex routing change was blocked because the Task's bound "
                "account home no longer exists",
            )
        return codex_pool.canonical_home(bound_home)

    if codex_pool is not None and task.session_id:
        try:
            matches = codex_pool.locate_session_homes(task.session_id)
        except Exception as exc:
            raise HTTPException(
                409,
                "Codex routing change was blocked because the native thread "
                "home could not be resolved",
            ) from exc
        if len(matches) > 1:
            raise HTTPException(
                409,
                "Codex routing change was blocked because the native thread "
                "exists in multiple account homes without an authoritative "
                "Task binding",
            )
        if len(matches) == 1:
            return codex_pool.canonical_home(matches[0])
        if getattr(codex_pool, "enabled", False):
            raise HTTPException(
                409,
                "Codex routing change was blocked because the native thread "
                "has no authoritative account home",
            )

    return normalize_codex_home(None)


async def _hold_codex_thread_routing_quiescence(
    stack: AsyncExitStack,
    task: Task,
    candidate: WorkerRoutingTuple,
) -> None:
    """Reserve one idle native thread through the caller's routing commit."""

    if (
        not task.session_id
        or (task.provider or "").lower() != "codex"
        or task_routing_tuple(task) == candidate
    ):
        return
    codex_home = _resolve_codex_thread_routing_home(task)
    from backend.main import instance_manager

    try:
        await stack.enter_async_context(
            instance_manager.codex_thread_routing_guard(
                codex_home,
                task.session_id,
            )
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.warning(
            "Codex routing change could not prove thread quiescence: "
            "task=%s session=%s home=%s error=%s",
            task.id,
            task.session_id,
            codex_home,
            type(exc).__name__,
        )
        raise HTTPException(
            409,
            "Codex routing change was blocked because its native thread or "
            "Goal could not be proven idle",
        ) from exc


def _routing_guard_generation_changed(current: Task, observed: Task) -> bool:
    return (
        current.session_id != observed.session_id
        or task_routing_tuple(current) != task_routing_tuple(observed)
        or _codex_account_binding(current) != _codex_account_binding(observed)
    )


def _worker_routing_snapshot(task: Task) -> dict:
    try:
        pending = read_pending_worker_routing(task)
    except InvalidWorkerRoutingMarker as exc:
        raise HTTPException(
            409,
            "Worker Task has an invalid routing synchronization marker",
        ) from exc
    return {
        "id": task.id,
        "status": task.status,
        "worker_id": task.worker_id,
        "shared_from_id": task.shared_from_id,
        **task_routing_tuple(task).as_dict(),
        "pending": pending.as_dict() if pending is not None else None,
    }


async def _lock_worker_local_routing_task(
    task_id: int,
    request: Request,
    db: AsyncSession,
    *,
    safe_status_required: bool,
    allowed_statuses: frozenset[str] | None = None,
) -> Task:
    """Acquire the portable Task write barrier and return its strict snapshot."""

    probe = await db.get(Task, task_id, populate_existing=True)
    if probe is None:
        raise HTTPException(404, "Task not found")
    current = await lock_task_effect_access(
        request,
        probe,
        db,
        allow_chat_share=False,
    )

    predicates = [
        Task.id == task_id,
        Task.worker_id.is_(None),
        Task.shared_from_id.is_(None),
        Task.pty_background_generation.is_(None),
        no_active_worker_task_termination_predicate(),
    ]
    if allowed_statuses is not None:
        predicates.append(Task.status.in_(allowed_statuses))
    elif safe_status_required:
        predicates.append(Task.status.in_(WORKER_ROUTING_SAFE_STATUSES))
    guarded = await db.execute(
        sa_update(Task).where(*predicates).values(status=Task.status)
    )
    if guarded.rowcount != 1:
        await db.rollback()
        db.expire_all()
        current = await db.get(Task, task_id)
        if current is None:
            raise HTTPException(404, "Task not found")
        await require_task_control(request, current, db)
        _require_not_delivery_owned_task(current, action="routing-configured")
        _require_not_waiting_capability(current, action="edited")
        if await active_worker_task_termination_receipt(db, task_id):
            await db.rollback()
            raise HTTPException(
                409,
                "Task has an active Worker termination receipt",
            )
        if current.worker_id is not None or current.shared_from_id is not None:
            raise HTTPException(
                409,
                "Routing synchronization endpoints only accept Worker-local Tasks",
            )
        if allowed_statuses is not None:
            detail = (
                "Task routing config cannot change after an execution claim "
                "became active"
            )
        else:
            detail = (
                "Worker Task routing config cannot change while it is pending or active"
            )
        raise HTTPException(409, detail)
    current = await db.get(Task, task_id, populate_existing=True)
    if current is None:
        await db.rollback()
        raise HTTPException(404, "Task not found")
    await require_task_control(request, current, db)
    _require_not_delivery_owned_task(current, action="routing-configured")
    _require_not_waiting_capability(current, action="edited")
    reverse_owner = (
        await db.execute(
            select(Instance.id)
            .where(Instance.current_task_id == task_id)
            .with_for_update()
            .limit(1)
        )
    ).scalar_one_or_none()
    if reverse_owner is not None:
        await db.rollback()
        raise HTTPException(
            409,
            "Task still has an active or unconfirmed Instance generation; "
            "routing configuration cannot change until process cleanup is "
            "complete",
        )
    return current


async def _running_routing_sub_agent_id(
    db: AsyncSession,
    task_id: int,
) -> int | None:
    """Return a child generation that can still emit with the old route."""

    from backend.models.sub_agent import SubAgentSession

    return (
        await db.execute(
            select(SubAgentSession.id)
            .where(
                SubAgentSession.task_id == task_id,
                SubAgentSession.status == "running",
                (
                    (SubAgentSession.agent_type == "sub_agent")
                    & (SubAgentSession.source == "ccm")
                )
                | (SubAgentSession.source == "native"),
            )
            .limit(1)
        )
    ).scalar_one_or_none()


@router.post(
    "/{task_id}/routing-config/stage",
    response_model=WorkerRoutingConfigSnapshot,
    include_in_schema=False,
)
async def stage_worker_routing_config(
    task_id: int,
    body: WorkerRoutingConfigRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Durably block launches and record a candidate without changing live config."""

    require_admin(request)
    task = await db.get(Task, task_id)
    if task is not None:
        await require_task_control(request, task, db)
        _require_not_delivery_owned_task(task, action="routing-configured")
    await db.rollback()
    candidate = _routing_request_tuple(body)

    async with get_task_operation_lock(task_id):
        # A terminal status may be published before a pre-owner launch or an
        # existing process generation is fully reaped.  Settle that hidden
        # reservation before taking the durable Task→Instance barrier below.
        db.expire_all()
        observed = await db.get(Task, task_id)
        if observed is None:
            raise HTTPException(404, "Task not found")
        await require_task_control(request, observed, db)
        _require_not_delivery_owned_task(observed, action="routing-configured")
        try:
            validate_task_service_tier_configuration(
                provider=candidate.provider,
                model=candidate.model,
                codex_service_tier=candidate.codex_service_tier,
                mode=observed.mode,
                goal_evaluator_model=observed.goal_evaluator_model,
            )
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        if observed.worker_id is not None or observed.shared_from_id is not None:
            raise HTTPException(
                409,
                "Routing synchronization endpoints only accept Worker-local Tasks",
            )
        if observed.status not in WORKER_ROUTING_SAFE_STATUSES:
            raise HTTPException(
                409,
                "Worker Task routing config cannot change while it is pending "
                "or active",
            )
        if candidate.provider != observed.provider and observed.session_id is not None:
            raise HTTPException(
                409,
                "Task provider cannot change while an existing native session "
                "may still emit output; start a new Task instead",
            )
        observed_instance_id = observed.instance_id
        db.expunge(observed)
        await db.rollback()
        await _settle_task_launch_barrier(task_id, observed_instance_id)

        async with AsyncExitStack() as routing_stack:
            await _hold_codex_thread_routing_quiescence(
                routing_stack,
                observed,
                candidate,
            )
            db.expire_all()
            current = await _lock_worker_local_routing_task(
                task_id,
                request,
                db,
                safe_status_required=True,
            )
            if _routing_guard_generation_changed(current, observed):
                await db.rollback()
                raise HTTPException(
                    409,
                    "Worker Task native session or routing generation changed "
                    "while quiescence was being verified",
                )
            try:
                pending = read_pending_worker_routing(current)
            except InvalidWorkerRoutingMarker as exc:
                await db.rollback()
                raise HTTPException(
                    409,
                    "Worker Task has an invalid routing synchronization marker",
                ) from exc

            # A Codex CCM sub-agent can still be between account resolution and
            # start_turn after its parent task became terminal.  Keep stage
            # behind that exact running child generation; its final launch gate
            # provides the opposite ordering when stage wins first.
            running_sub_agent = await _running_routing_sub_agent_id(
                db,
                task_id,
            )
            if running_sub_agent is not None:
                await db.rollback()
                raise HTTPException(
                    409,
                    "Worker Task routing config cannot be staged while a CCM "
                    "sub-agent is running",
                )

            if (
                candidate.provider != current.provider
                and current.session_id is not None
            ):
                await db.rollback()
                raise HTTPException(
                    409,
                    "Task provider cannot change while an existing native "
                    "session may still emit output; start a new Task instead",
                )

            requested = WorkerRoutingPending(body.op_id, candidate)
            if pending is not None:
                if pending != requested:
                    await db.rollback()
                    raise HTTPException(
                        409,
                        "Worker Task already has a different routing "
                        "synchronization operation pending",
                    )
                snapshot = _worker_routing_snapshot(current)
                await db.rollback()
                return snapshot

            current.metadata_ = with_pending_worker_routing(
                current.metadata_,
                requested,
            )
            await db.commit()
            snapshot = _worker_routing_snapshot(current)
            await db.rollback()
            return snapshot


@router.get(
    "/{task_id}/routing-config/status",
    response_model=WorkerRoutingConfigSnapshot,
    include_in_schema=False,
)
async def read_worker_routing_config(
    task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Return the live tuple and durable pending candidate for convergence."""

    require_admin(request)
    async with get_task_operation_lock(task_id):
        task = await db.get(Task, task_id)
        if task is None:
            raise HTTPException(404, "Task not found")
        await require_task_control(request, task, db)
        if task.worker_id is not None or task.shared_from_id is not None:
            raise HTTPException(
                409,
                "Routing synchronization endpoints only accept Worker-local Tasks",
            )
        return _worker_routing_snapshot(task)


@router.post(
    "/{task_id}/routing-config/ack",
    response_model=WorkerRoutingConfigSnapshot,
    include_in_schema=False,
)
async def ack_worker_routing_config(
    task_id: int,
    body: WorkerRoutingConfigRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Atomically promote the staged candidate and clear its launch fence."""

    require_admin(request)
    task = await db.get(Task, task_id)
    if task is not None:
        await require_task_control(request, task, db)
        _require_not_delivery_owned_task(task, action="routing-configured")
    await db.rollback()
    candidate = _routing_request_tuple(body)
    async with get_task_operation_lock(task_id):
        db.expire_all()
        current = await _lock_worker_local_routing_task(
            task_id,
            request,
            db,
            safe_status_required=True,
        )
        try:
            pending = read_pending_worker_routing(current)
        except InvalidWorkerRoutingMarker as exc:
            await db.rollback()
            raise HTTPException(
                409,
                "Worker Task has an invalid routing synchronization marker",
            ) from exc
        requested = WorkerRoutingPending(body.op_id, candidate)
        if pending is None:
            if task_routing_tuple(current) != candidate:
                await db.rollback()
                raise HTTPException(
                    409,
                    "Worker routing ack has no matching pending or applied tuple",
                )
            snapshot = _worker_routing_snapshot(current)
            await db.rollback()
            return snapshot
        if pending != requested:
            await db.rollback()
            raise HTTPException(
                409,
                "Worker routing ack does not match the pending operation",
            )

        current.provider = candidate.provider
        current.model = candidate.model
        current.codex_service_tier = candidate.codex_service_tier
        current.metadata_ = without_pending_worker_routing(current.metadata_)
        await db.commit()
        return _worker_routing_snapshot(current)


@router.post(
    "/{task_id}/routing-config/reconcile",
    response_model=WorkerRoutingConfigSnapshot,
    include_in_schema=False,
)
async def reconcile_worker_routing_config(
    task_id: int,
    body: WorkerRoutingConfigRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Abort an orphan stage by restoring the Manager-authoritative live tuple."""

    require_admin(request)
    task = await db.get(Task, task_id)
    if task is not None:
        await require_task_control(request, task, db)
        _require_not_delivery_owned_task(task, action="routing-configured")
    await db.rollback()
    authoritative = _routing_request_tuple(body)

    async with get_task_operation_lock(task_id):
        db.expire_all()
        current = await _lock_worker_local_routing_task(
            task_id,
            request,
            db,
            safe_status_required=True,
        )
        try:
            validate_task_service_tier_configuration(
                provider=authoritative.provider,
                model=authoritative.model,
                codex_service_tier=authoritative.codex_service_tier,
                mode=current.mode,
                goal_evaluator_model=current.goal_evaluator_model,
            )
        except ValueError as exc:
            await db.rollback()
            raise HTTPException(422, str(exc)) from exc
        try:
            pending = read_pending_worker_routing(current)
        except InvalidWorkerRoutingMarker as exc:
            await db.rollback()
            raise HTTPException(
                409,
                "Worker Task has an invalid routing synchronization marker",
            ) from exc
        if pending is None:
            if task_routing_tuple(current) != authoritative:
                await db.rollback()
                raise HTTPException(
                    409,
                    "Worker routing differs from Manager without a pending operation",
                )
            snapshot = _worker_routing_snapshot(current)
            await db.rollback()
            return snapshot
        if pending.op_id != body.op_id:
            await db.rollback()
            raise HTTPException(
                409,
                "Worker routing reconcile does not match the pending operation",
            )

        current.provider = authoritative.provider
        current.model = authoritative.model
        current.codex_service_tier = authoritative.codex_service_tier
        current.metadata_ = without_pending_worker_routing(current.metadata_)
        await db.commit()
        return _worker_routing_snapshot(current)


def _validate_remote_worker_routing_snapshot(
    value,
    *,
    task_id: int,
) -> WorkerRoutingConfigSnapshot:
    try:
        snapshot = WorkerRoutingConfigSnapshot.model_validate(value)
    except Exception as exc:
        raise HTTPException(
            502,
            "Worker returned an invalid routing synchronization snapshot",
        ) from exc
    if snapshot.id != task_id:
        raise HTTPException(
            502,
            "Worker returned a routing snapshot for a different Task",
        )
    return snapshot


def _snapshot_routing_tuple(
    snapshot: WorkerRoutingConfigSnapshot,
) -> WorkerRoutingTuple:
    return WorkerRoutingTuple(
        provider=snapshot.provider,
        model=snapshot.model,
        codex_service_tier=snapshot.codex_service_tier,
    )


async def _read_remote_worker_routing(
    task: Task,
    *,
    operation_lock_held: bool,
    surface_endpoint_not_found: bool = False,
) -> WorkerRoutingConfigSnapshot:
    result = await _proxy(
        task,
        "GET",
        f"/api/tasks/{task.id}/routing-config/status",
        require_json=True,
        surface_endpoint_not_found=surface_endpoint_not_found,
        operation_lock_held=operation_lock_held,
    )
    return _validate_remote_worker_routing_snapshot(result, task_id=task.id)


def _validate_legacy_worker_routing_snapshot(
    value,
    *,
    task: Task,
) -> WorkerRoutingConfigSnapshot:
    """Validate the ordinary Task response used by a pre-routing-protocol Worker."""

    if not isinstance(value, dict):
        raise HTTPException(
            502,
            "Legacy Worker returned an invalid Task routing confirmation",
        )
    required = {"id", "status", "provider", "model"}
    if not required.issubset(value):
        raise HTTPException(
            502,
            "Legacy Worker Task response omitted required routing fields",
        )
    if value["id"] != task.id:
        raise HTTPException(
            502,
            "Legacy Worker returned routing for a different Task",
        )
    if not isinstance(value["status"], str) or not value["status"]:
        raise HTTPException(
            502,
            "Legacy Worker returned an invalid Task status",
        )

    authoritative = task_routing_tuple(task)
    if authoritative.codex_service_tier != "default":
        raise HTTPException(
            409,
            "Legacy Worker cannot confirm Codex Fast routing; execution was blocked",
        )
    remote = WorkerRoutingTuple(
        provider=value["provider"],
        model=value["model"],
        # Workers predating the routing protocol also predate service tiers.
        codex_service_tier=value.get("codex_service_tier", "default"),
    )
    if remote != authoritative:
        raise HTTPException(
            409,
            "Legacy Worker Task routing does not exactly match the Manager; "
            "execution was blocked",
        )
    return WorkerRoutingConfigSnapshot(
        id=task.id,
        status=value["status"],
        worker_id=None,
        shared_from_id=None,
        provider=remote.provider,
        model=remote.model,
        codex_service_tier=remote.codex_service_tier,
        pending=None,
    )


async def _read_legacy_worker_routing(
    task: Task,
    *,
    operation_lock_held: bool,
) -> WorkerRoutingConfigSnapshot:
    result = await _proxy(
        task,
        "GET",
        f"/api/tasks/{task.id}",
        require_json=True,
        operation_lock_held=operation_lock_held,
        require_task_incarnation_fence=True,
    )
    return _validate_legacy_worker_routing_snapshot(result, task=task)


async def _confirm_worker_routing_mutation(
    task: Task,
    *,
    path: str,
    payload: dict,
    expected: WorkerRoutingTuple,
    operation_lock_held: bool,
) -> WorkerRoutingConfigSnapshot:
    """Confirm ack/reconcile, recovering only a lost success response."""

    try:
        result = await _proxy(
            task,
            "POST",
            path,
            payload,
            require_json=True,
            operation_lock_held=operation_lock_held,
        )
        snapshot = _validate_remote_worker_routing_snapshot(
            result,
            task_id=task.id,
        )
    except Exception as mutation_error:
        try:
            snapshot = await _read_remote_worker_routing(
                task,
                operation_lock_held=operation_lock_held,
            )
        except Exception:
            raise _WorkerRoutingConfirmationUnavailable() from mutation_error
    if snapshot.pending is not None or _snapshot_routing_tuple(snapshot) != expected:
        raise HTTPException(
            502,
            "Worker routing synchronization remains pending or divergent",
        )
    return snapshot


async def _ensure_worker_routing_ready(
    task: Task,
    *,
    operation_lock_held: bool,
    allow_legacy_standard: bool = True,
) -> WorkerRoutingConfigSnapshot:
    """Converge an orphan stage, then prove Worker live config equals Manager."""

    authoritative = task_routing_tuple(task)
    try:
        snapshot = await _read_remote_worker_routing(
            task,
            operation_lock_held=operation_lock_held,
            surface_endpoint_not_found=allow_legacy_standard,
        )
    except WorkerEndpointNotFoundError:
        snapshot = await _read_legacy_worker_routing(
            task,
            operation_lock_held=operation_lock_held,
        )
    pending = snapshot.pending
    if pending is not None:
        pending_tuple = WorkerRoutingTuple(
            provider=pending.provider,
            model=pending.model,
            codex_service_tier=pending.codex_service_tier,
        )
        payload = {
            "op_id": pending.op_id,
            **authoritative.as_dict(),
        }
        action = "ack" if pending_tuple == authoritative else "reconcile"
        snapshot = await _confirm_worker_routing_mutation(
            task,
            path=f"/api/tasks/{task.id}/routing-config/{action}",
            payload=payload,
            expected=authoritative,
            operation_lock_held=operation_lock_held,
        )
    if (
        snapshot.pending is not None
        or _snapshot_routing_tuple(snapshot) != authoritative
    ):
        raise HTTPException(
            409,
            "Worker routing config does not exactly match the Manager; execution "
            "was blocked",
        )
    return snapshot


def _require_no_pending_worker_routing(task: Task) -> None:
    if has_pending_worker_routing(task):
        raise HTTPException(
            409,
            "Task routing configuration synchronization is pending; execution "
            "is blocked until Manager and Worker converge",
        )


async def _update_local_task_with_routing_config(
    task_id: int,
    updates: dict,
    request: Request,
    queue: TaskQueue,
) -> Task:
    """Atomically save a local route only while no generation can use the old one."""

    mixed = set(updates).difference(_WORKER_ROUTING_CONFIG_FIELDS)
    if mixed:
        raise HTTPException(
            409,
            "Task routing changes may only contain provider, model, and Codex "
            "service tier; save other fields separately",
        )

    await queue.db.rollback()
    async with get_task_operation_lock(task_id):
        queue.db.expire_all()
        observed = await queue.db.get(Task, task_id)
        if observed is None:
            raise HTTPException(404, "Task not found")
        await require_task_control(request, observed, queue.db)
        _require_not_waiting_capability(observed, action="edited")
        if observed.worker_id is not None or observed.shared_from_id is not None:
            raise HTTPException(
                409,
                "Task execution authority changed before routing update",
            )
        if observed.status not in _LOCAL_ROUTING_EDITABLE_STATUSES:
            raise HTTPException(
                409,
                "Task routing config cannot change after an execution claim "
                "became active; wait for the current turn to finish",
            )
        normalized = _normalized_task_update_values(updates)
        candidate = WorkerRoutingTuple(
            provider=normalized.get("provider", observed.provider),
            model=(normalized["model"] if "model" in normalized else observed.model),
            codex_service_tier=normalized.get(
                "codex_service_tier",
                observed.codex_service_tier,
            ),
        )
        try:
            validate_task_service_tier_configuration(
                provider=candidate.provider,
                model=candidate.model,
                codex_service_tier=candidate.codex_service_tier,
                mode=observed.mode,
                goal_evaluator_model=observed.goal_evaluator_model,
            )
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        if candidate.provider != observed.provider and observed.session_id is not None:
            raise HTTPException(
                409,
                "Task provider cannot change while an existing native session "
                "may still emit output; start a new Task instead",
            )
        observed_instance_id = observed.instance_id
        queue.db.expunge(observed)
        await queue.db.rollback()
        await _settle_task_launch_barrier(task_id, observed_instance_id)

        async with AsyncExitStack() as routing_stack:
            await _hold_codex_thread_routing_quiescence(
                routing_stack,
                observed,
                candidate,
            )
            queue.db.expire_all()
            current = await _lock_worker_local_routing_task(
                task_id,
                request,
                queue.db,
                safe_status_required=False,
                allowed_statuses=_LOCAL_ROUTING_EDITABLE_STATUSES,
            )
            await _require_no_active_harness_owner_graph(
                queue.db,
                task_id,
            )
            if _routing_guard_generation_changed(current, observed):
                await queue.db.rollback()
                raise HTTPException(
                    409,
                    "Task native session or routing generation changed while "
                    "quiescence was being verified",
                )
            _require_no_pending_worker_routing(current)
            if await _running_routing_sub_agent_id(queue.db, task_id) is not None:
                await queue.db.rollback()
                raise HTTPException(
                    409,
                    "Task routing config cannot change while a sub-agent is running",
                )

            current.provider = candidate.provider
            current.model = candidate.model
            current.codex_service_tier = candidate.codex_service_tier
            await queue.db.commit()
            await queue.db.refresh(current)
            return current


async def _update_worker_task_with_routing_config(
    task_id: int,
    updates: dict,
    request: Request,
    queue: TaskQueue,
    *,
    expected_worker_id: int,
) -> Task:
    """Run stage → exact Manager CAS → Worker ack under cancellation shielding."""

    unsafe = _WORKER_CONFIG_SYNC_UNSAFE_FIELDS.intersection(updates)
    if unsafe:
        raise HTTPException(
            409,
            "Worker location/project changes must be saved separately from "
            "provider, model, or Codex Fast changes",
        )
    mixed = set(updates).difference(_WORKER_ROUTING_CONFIG_FIELDS)
    if mixed:
        raise HTTPException(
            409,
            "Worker routing changes may only contain provider, model, and "
            "Codex service tier; save other fields separately",
        )

    await queue.db.rollback()
    async with get_task_operation_lock(task_id):
        queue.db.expire_all()
        current = await queue.db.get(Task, task_id)
        if current is None:
            raise HTTPException(404, "Task not found")
        current = await lock_task_effect_access(
            request,
            current,
            queue.db,
            allow_chat_share=False,
        )
        _require_not_waiting_capability(current, action="edited")
        if current.worker_id != expected_worker_id:
            raise HTTPException(
                409,
                "Task Worker assignment changed before config synchronization",
            )
        _require_no_pending_worker_turn_handoff(current)
        if current.status not in WORKER_ROUTING_SAFE_STATUSES:
            raise HTTPException(
                409,
                "Worker Task config cannot change while it is pending or active; "
                "wait for the current Worker turn to finish",
            )

        normalized = _normalized_task_update_values(updates)
        candidate = WorkerRoutingTuple(
            provider=normalized.get("provider", current.provider),
            model=(normalized["model"] if "model" in normalized else current.model),
            codex_service_tier=normalized.get(
                "codex_service_tier",
                current.codex_service_tier,
            ),
        )
        try:
            validate_task_service_tier_configuration(
                provider=candidate.provider,
                model=candidate.model,
                codex_service_tier=candidate.codex_service_tier,
                mode=current.mode,
                goal_evaluator_model=current.goal_evaluator_model,
            )
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        normalized.update(candidate.as_dict())

        observed = worker_task_generation(
            current,
            expected_worker_id=expected_worker_id,
        )
        if observed is None:
            raise HTTPException(409, "Task Worker assignment changed")
        previous = task_routing_tuple(current)
        op_id = uuid.uuid4().hex
        payload = {"op_id": op_id, **candidate.as_dict()}
        # Never retain the Manager DB read transaction over Worker network
        # calls.  Relay is then free to advance status, and the one exact CAS
        # below will detect that generation change instead of re-fencing it.
        queue.db.expunge(current)
        await queue.db.rollback()

        # Resolve any durable stage left by a prior crashed request before
        # starting a different operation.  A marker with our current tuple is
        # a lost ack; a different candidate is safely aborted to our tuple.
        await _ensure_worker_routing_ready(
            current,
            operation_lock_held=True,
            allow_legacy_standard=False,
        )

        # A timeout here is intentionally not read back into success.  The
        # Worker may have staged the marker, but Manager has not committed, so
        # it must remain blocked for the next explicit convergence attempt.
        staged_result = await _proxy(
            current,
            "POST",
            f"/api/tasks/{task_id}/routing-config/stage",
            payload,
            require_json=True,
            operation_lock_held=True,
        )
        staged = _validate_remote_worker_routing_snapshot(
            staged_result,
            task_id=task_id,
        )
        if (
            staged.status not in WORKER_ROUTING_SAFE_STATUSES
            or _snapshot_routing_tuple(staged) != previous
            or staged.pending is None
            or staged.pending.op_id != op_id
            or WorkerRoutingTuple(
                provider=staged.pending.provider,
                model=staged.pending.model,
                codex_service_tier=staged.pending.codex_service_tier,
            )
            != candidate
        ):
            raise HTTPException(
                502,
                "Worker did not strictly confirm the staged routing candidate",
            )

        predicates = [
            *worker_task_generation_predicates(observed),
            Task.provider == previous.provider,
            (
                Task.model.is_(None)
                if previous.model is None
                else Task.model == previous.model
            ),
            Task.codex_service_tier == previous.codex_service_tier,
        ]
        current = await lock_task_effect_access(
            request,
            current,
            queue.db,
            allow_chat_share=False,
        )
        changed = await queue.db.execute(
            sa_update(Task)
            .where(
                *predicates,
                no_active_test_harness_owner_graph_predicate(),
            )
            .values(**normalized)
        )
        if changed.rowcount != 1:
            await queue.db.rollback()
            await _require_no_active_harness_owner_graph(
                queue.db,
                task_id,
            )
            raise HTTPException(
                409,
                "Task Worker generation changed while routing config was "
                "staged; Worker remains safely blocked",
            )
        await queue.db.commit()
        queue.db.expire_all()
        updated = await queue.db.get(Task, task_id)
        if updated is None:
            raise HTTPException(
                409,
                "Task disappeared after routing config commit; Worker remains "
                "safely blocked",
            )

        try:
            await _confirm_worker_routing_mutation(
                updated,
                path=f"/api/tasks/{task_id}/routing-config/ack",
                payload=payload,
                expected=candidate,
                operation_lock_held=True,
            )
        except _WorkerRoutingConfirmationUnavailable:
            # Manager commit is the configuration commit point.  Returning an
            # error here would leave the UI displaying its old Fast/Standard
            # badge even though every subsequent execution is governed by the
            # new authoritative tuple.  The Worker either applied it already
            # or still has the durable stage marker, which blocks execution
            # until the next retry/chat preflight converges it.
            logger.warning(
                "Worker routing ack could not be confirmed after Manager "
                "commit; task=%s worker=%s op=%s remains execution-fenced",
                task_id,
                expected_worker_id,
                op_id,
            )
        return updated


async def _update_worker_task_with_skill_configuration(
    task_id: int,
    updates: dict,
    request: Request,
    queue: TaskQueue,
    *,
    expected_worker_id: int,
) -> Task:
    """Serialize Manager-authoritative Skill saves with Worker execution."""

    await queue.db.rollback()
    async with get_task_operation_lock(task_id):
        queue.db.expire_all()
        current = await queue.db.get(Task, task_id)
        if current is None:
            raise HTTPException(404, "Task not found")
        current = await lock_task_effect_access(
            request,
            current,
            queue.db,
            allow_chat_share=False,
        )
        _require_not_waiting_capability(current, action="edited")
        if current.worker_id != expected_worker_id:
            raise HTTPException(
                409,
                "Task Worker assignment changed before Skill configuration "
                "could be saved",
            )
        _require_no_pending_worker_turn_handoff(current)
        if current.status not in _WORKER_SKILL_EDITABLE_STATUSES:
            raise HTTPException(
                409,
                "Worker Task Skill configuration cannot change after an "
                "execution claim became active; wait for the current Worker "
                "turn to finish",
            )
        try:
            updated = await queue.update_task(
                task_id,
                operation_lock_held=True,
                expected_incarnation_id=internal_task_incarnation_id(
                    request,
                    task_id,
                ),
                reject_active_harness_owner_graph=True,
                **updates,
            )
        except (
            DurableWorkerTerminationConflict,
            TestHarnessOwnerGraphConflict,
            TaskWaitingCapabilityConflict,
        ) as exc:
            raise HTTPException(409, str(exc)) from exc
        if updated is None:
            raise HTTPException(404, "Task not found")
        return updated


async def _update_worker_task_with_handoff_fence(
    task_id: int,
    updates: dict,
    request: Request,
    queue: TaskQueue,
    *,
    expected_worker_id: int,
) -> Task:
    """Serialize ordinary Worker Task edits with exact chat handoffs."""

    await queue.db.rollback()
    async with get_task_operation_lock(task_id):
        queue.db.expire_all()
        current = await queue.db.get(Task, task_id)
        if current is None:
            raise HTTPException(404, "Task not found")
        current = await lock_task_effect_access(
            request,
            current,
            queue.db,
            allow_chat_share=False,
        )
        _require_not_waiting_capability(current, action="edited")
        if current.worker_id != expected_worker_id:
            raise HTTPException(
                409,
                "Task Worker assignment changed before configuration could be saved",
            )
        _require_no_pending_worker_turn_handoff(current)
        try:
            updated = await queue.update_task(
                task_id,
                operation_lock_held=True,
                expected_incarnation_id=internal_task_incarnation_id(
                    request,
                    task_id,
                ),
                reject_active_harness_owner_graph=True,
                **updates,
            )
        except (
            DurableWorkerTerminationConflict,
            TestHarnessOwnerGraphConflict,
            TaskWaitingCapabilityConflict,
        ) as exc:
            raise HTTPException(409, str(exc)) from exc
        if updated is None:
            raise HTTPException(404, "Task not found")
        return updated


async def _update_task_impl(
    task_id: int,
    body: TaskUpdate,
    request: Request,
    queue: TaskQueue = Depends(_get_queue),
):
    task = await require_worker_control_plane_task_incarnation(
        request,
        task_id,
        queue.db,
        write_fence=True,
    )
    if task is None:
        task = await queue.get(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    await require_task_control(request, task, queue.db)
    _require_not_delivery_owned_task(task, action="edited")
    _require_not_waiting_capability(task, action="edited")
    await _require_not_isolated_browser_child(
        queue.db,
        task,
        action="edited",
    )
    await _require_not_pr_review_task_mutation(
        queue.db,
        task_id,
        action="edited",
    )
    updates = body.model_dump(exclude_unset=True)
    user_skill_snapshots = updates.pop("user_skill_snapshots", None)
    if user_skill_snapshots is not None:
        require_admin(request)
    # Project membership lets a member create and operate Tasks inside that
    # exact Project.  It is not authority to detach the Task into a
    # projectless scope, reassign it to another Project, or move its execution
    # infrastructure.  Those are administrator-owned topology changes.
    if (
        "project_id" in updates
        and updates["project_id"] != task.project_id
    ):
        require_admin(request)
    if "worker_id" in updates:
        requested_worker_id = updates["worker_id"]
        normalized_worker_id = (
            None if requested_worker_id == -1 else requested_worker_id
        )
        if normalized_worker_id != task.worker_id:
            require_admin(request)
    try:
        effective_policy_worker_id = task.worker_id
        if "worker_id" in updates:
            requested_worker_id = updates["worker_id"]
            effective_policy_worker_id = (
                None if requested_worker_id == -1 else requested_worker_id
            )
        validate_auto_capability_task_scope(
            task.capability_policy,
            mode=updates.get("mode", task.mode),
            worker_id=effective_policy_worker_id,
            shared_from_id=task.shared_from_id,
            delivery_run_id=task.delivery_run_id,
            delivery_role=task.delivery_role,
            plan_target_task_id=task.plan_target_task_id,
        )
        validate_task_service_tier_configuration(
            provider=updates.get("provider", task.provider),
            model=updates.get("model", task.model),
            codex_service_tier=updates.get(
                "codex_service_tier",
                task.codex_service_tier,
            ),
            mode=updates.get("mode", task.mode),
            goal_evaluator_model=updates.get(
                "goal_evaluator_model",
                task.goal_evaluator_model,
            ),
        )
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    if "enabled_skills" in updates:
        # An explicit save is authoritative even when its JSON happens to equal
        # a currently active one-turn override. Clearing the generation marker
        # lets lifecycle cleanup distinguish that user write from its own
        # temporary value.
        updates["metadata_"] = clear_temporary_skills_marker(task.metadata_)
    if user_skill_snapshots is not None:
        from backend.services.skill_context import (
            USER_SKILL_SNAPSHOTS_METADATA_KEY,
            WORKER_MANAGED_TASK_METADATA_KEY,
        )

        metadata = dict(updates.get("metadata_") or task.metadata_ or {})
        metadata[USER_SKILL_SNAPSHOTS_METADATA_KEY] = user_skill_snapshots
        metadata[WORKER_MANAGED_TASK_METADATA_KEY] = True
        updates["metadata_"] = metadata

    # "off" sentinel → explicit NULL. Do this before the Worker branch so both
    # mirrors receive the same normalized value in a combined config update.
    if updates.get("system_prompt_mode") == "off":
        updates["system_prompt_mode"] = None

    # Validate the effective Skill configuration before routing
    # synchronization or Worker migration can create externally visible state.
    effective_provider = updates.get("provider", task.provider)
    effective_description = updates.get("description", task.description)
    effective_worker_id = task.worker_id
    if "worker_id" in updates:
        requested_worker_id = updates["worker_id"]
        effective_worker_id = None if requested_worker_id == -1 else requested_worker_id
    effective_metadata = updates.get("metadata_", task.metadata_)
    command_skills = _explicit_command_skills(effective_description)
    skill_configuration_changed = (
        bool(
            {
                "provider",
                "enabled_skills",
                "selected_user_skills",
                "worker_id",
            }
            & updates.keys()
        )
        or user_skill_snapshots is not None
        or bool(command_skills and "description" in updates)
    )
    if skill_configuration_changed:
        from backend.services.skill_context import (
            USER_SKILL_SNAPSHOTS_METADATA_KEY,
        )

        effective_skills = dict(
            updates.get(
                "enabled_skills",
                task.enabled_skills,
            )
            or {}
        )
        effective_skills.update(command_skills)
        effective_user_skills = updates.get(
            "selected_user_skills",
            task.selected_user_skills,
        )
        normalized_user_skills = await _validate_skill_configuration(
            queue.db,
            provider=effective_provider,
            enabled_skills=effective_skills,
            selected_user_skills=effective_user_skills,
            user_skill_snapshots=(
                user_skill_snapshots
                if user_skill_snapshots is not None
                else (task.metadata_ or {}).get(USER_SKILL_SNAPSHOTS_METADATA_KEY)
            ),
            worker_id=effective_worker_id,
            shared_from_id=task.shared_from_id,
            metadata=effective_metadata,
        )
        if "selected_user_skills" in updates:
            updates["selected_user_skills"] = normalized_user_skills

    worker_id_supplied = "worker_id" in updates
    target_project = None
    target_project_id = updates.get("project_id", task.project_id)
    if target_project_id is not None:
        from backend.models.project import Project

        target_project = await queue.db.get(Project, target_project_id)
        if target_project is None:
            raise HTTPException(404, "Project not found")
        await require_project_access(request, target_project_id, queue.db)
        if (
            "target_repo" in updates
            and updates["target_repo"] != target_project.local_path
        ):
            raise HTTPException(
                400,
                "Task target_repo must match the selected Project local_path",
            )
        if "project_id" in updates:
            # Project.local_path is the sole workspace authority.  Never carry
            # a caller-supplied or stale host path across a Project move.
            _require_dispatchable_project(target_project)
            updates["target_repo"] = target_project.local_path
        if (
            "project_id" in updates
            and not worker_id_supplied
            and task.worker_id != target_project.worker_id
        ):
            raise HTTPException(
                400,
                "Task Worker must match the selected Project location",
            )
    elif "project_id" in updates and not worker_id_supplied:
        await require_worker_target_access(request, task.worker_id, queue.db)

    # 执行位置切换走 TaskMigrator（同 mode/model 一样在 task 详情改，
    # 但语义是迁移而非改字段）。-1 = 切回本机
    if "worker_id" in updates:
        target = updates.pop("worker_id")
        if target == -1:
            target = None
        if target_project is not None and target != target_project.worker_id:
            raise HTTPException(
                400,
                "Task Worker must match the selected Project location",
            )
        if target_project is None:
            await require_worker_target_access(request, target, queue.db)
        if task.worker_id != target:
            from backend.main import task_migrator

            if task_migrator is None:
                raise HTTPException(503, "Worker 功能未启用")
            from backend.services.task_migrator import MigrationError

            try:
                # 同步执行：迁移结束后才返回，前端拿到的就是最终状态。
                # 大工作目录会久——前端按钮置灰 + migrating 状态广播兜底
                if updates:
                    await task_migrator.migrate(
                        task_id,
                        target,
                        task_updates=updates,
                    )
                else:
                    await task_migrator.migrate(task_id, target)
            except MigrationError as e:
                raise HTTPException(409, str(e))
            # migrate 在独立 session 写库；当前 DI session 的 identity map
            # 还缓存着旧 worker_id，必须 expire 否则响应返回迁移前的值
            queue.db.expire_all()
            migrated = await queue.get(task_id)
            if not migrated:
                raise HTTPException(404, "Task not found")
            return migrated

    # An already-forwarded Worker owns the executable Task row. Synchronize
    # its complete routing tuple before making the Manager mirror visible.
    if task.worker_id is not None and _WORKER_ROUTING_CONFIG_FIELDS.intersection(
        updates
    ):
        return await _finish_task_operation(
            _update_worker_task_with_routing_config(
                task_id,
                updates,
                request,
                queue,
                expected_worker_id=task.worker_id,
            )
        )
    if task.worker_id is None and _WORKER_ROUTING_CONFIG_FIELDS.intersection(updates):
        return await _finish_task_operation(
            _update_local_task_with_routing_config(
                task_id,
                updates,
                request,
                queue,
            )
        )

    # Skill-only edits remain Manager-authoritative until the next turn, but
    # they must commit under the same lock as retry/chat/plan approval.  This
    # gives every execution admission one unambiguous final tuple to sync.
    if task.worker_id is not None and _WORKER_SKILL_CONFIG_FIELDS.intersection(updates):
        return await _finish_task_operation(
            _update_worker_task_with_skill_configuration(
                task_id,
                updates,
                request,
                queue,
                expected_worker_id=task.worker_id,
            )
        )

    if not updates:
        task = await queue.get(task_id)
        if not task:
            raise HTTPException(404, "Task not found")
        return task
    if task.worker_id is not None:
        return await _finish_task_operation(
            _update_worker_task_with_handoff_fence(
                task_id,
                updates,
                request,
                queue,
                expected_worker_id=task.worker_id,
            )
        )
    task = await lock_task_effect_access(
        request,
        task,
        queue.db,
        allow_chat_share=False,
    )
    try:
        task = await queue.update_task(
            task_id,
            expected_incarnation_id=internal_task_incarnation_id(
                request,
                task_id,
            ),
            reject_active_harness_owner_graph=True,
            **updates,
        )
    except (
        DurableWorkerTerminationConflict,
        TestHarnessOwnerGraphConflict,
        TaskWaitingCapabilityConflict,
    ) as exc:
        raise HTTPException(409, str(exc)) from exc
    if not task:
        raise HTTPException(404, "Task not found")
    return task


@router.put("/{task_id}", response_model=TaskResponse)
async def update_task(
    task_id: int,
    body: TaskUpdate,
    request: Request,
    queue: TaskQueue = Depends(_get_queue),
):
    updated = await _update_task_impl(task_id, body, request, queue)
    return await task_response(request, updated, queue.db)


@router.put(
    "/{task_id}/internal/enabled-skills",
    response_model=InternalTaskResponse,
)
async def update_task_enabled_skills_internal(
    task_id: int,
    body: InternalTaskSkillsUpdate,
    request: Request,
    queue: TaskQueue = Depends(_get_queue),
):
    """Apply only the skill toggle exposed to the scoped skills MCP."""

    require_internal_service(request)
    await require_internal_task_incarnation(
        request,
        task_id,
        queue.db,
        write_fence=True,
    )
    return await update_task(
        task_id,
        TaskUpdate(enabled_skills=body.enabled_skills),
        request,
        queue,
    )


@router.post("/{task_id}/internal/skill-tools")
async def execute_task_skill_tool_internal(
    task_id: int,
    body: InternalSkillToolCall,
    request: Request,
    queue: TaskQueue = Depends(_get_queue),
):
    """Execute the privileged half of one frozen Skills MCP tool call."""

    require_internal_service(request)
    task = await require_internal_task_incarnation(
        request,
        task_id,
        queue.db,
        write_fence=True,
    )
    # Keep the runtime allow-list duplicated at the typed HTTP boundary so a
    # future schema edit cannot silently broaden the Manager effect surface.
    if body.tool not in SKILL_TOOL_RPC_NAMES:
        raise HTTPException(400, "Unsupported CCM skill tool")
    if task is None:
        task = await queue.get(task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    outcome = await execute_skill_tool_rpc(
        task,
        body.tool,
        body.arguments,
        queue.db,
    )
    if outcome.enabled_skills is not None:
        await update_task(
            task_id,
            TaskUpdate(enabled_skills=outcome.enabled_skills),
            request,
            queue,
        )
    return {"result": outcome.result}


async def _settle_task_launch_barrier(
    task_id: int,
    instance_id: int | None,
) -> None:
    """Prove a pre-owner launch aborted after the Task became terminal."""

    from backend.services.task_termination import settle_task_launch_barrier

    try:
        await settle_task_launch_barrier(task_id, instance_id)
    except TaskLaunchTerminationConflict as exc:
        raise HTTPException(
            409,
            str(exc),
        ) from exc


async def _retry_local_task_safely(
    task_id: int,
    queue: TaskQueue,
    db: AsyncSession,
    *,
    expected_identity: TestHarnessOwnerIdentity,
    task_updates: dict | None = None,
    expected_incarnation_id: str | None = None,
    expected_principal: dict | None = None,
    effect_request: Request | None = None,
    commit: bool = True,
) -> Task | None:
    """Retry only after closing every Harness owner from the old generation."""

    from backend.services.test_harness import test_harness_service

    # The Harness service uses an independent session while cancelling Runs
    # and durable Browser children.  Release this request's read snapshot
    # before waiting for that writer, then keep the owner fence until the new
    # Task generation is durable so no stale-generation Run can materialize in
    # between cleanup and retry.
    await db.rollback()
    async with test_harness_service.owner_stop_fence(
        task_id,
        reason="Owner Task was retried",
        expected_identity=expected_identity,
    ):
        return await _retry_local_task_under_harness_owner_fence(
            task_id,
            queue,
            db,
            task_updates=task_updates,
            expected_incarnation_id=expected_incarnation_id,
            expected_principal=expected_principal,
            effect_request=effect_request,
            commit=commit,
        )


async def _retry_local_task_under_harness_owner_fence(
    task_id: int,
    queue: TaskQueue,
    db: AsyncSession,
    *,
    task_updates: dict | None = None,
    expected_incarnation_id: str | None = None,
    expected_principal: dict | None = None,
    effect_request: Request | None = None,
    commit: bool = True,
) -> Task | None:
    """Retry without discarding evidence of a possibly-live orphan process.

    Startup recovery intentionally retains ``Task.instance_id`` plus the
    Instance PID/current owner when it cannot prove that an unmanaged process
    died.  The retry endpoint is the only normal path that releases that
    terminal claim, so it must reconcile under InstanceManager's exact
    lifecycle lock before ``TaskQueue.retry`` clears the task-side owner.
    """

    from backend.main import instance_manager

    db.expire_all()
    task = await db.get(Task, task_id)
    if task is None:
        return None

    observed_status = task.status
    if observed_status not in _MANUAL_RETRYABLE_STATUSES:
        raise HTTPException(
            status_code=409,
            detail=f"Task status {observed_status} is not retryable",
        )
    observed_generation = (
        task.retry_count,
        task.instance_id,
        task.started_at,
        task.completed_at,
        task.pty_background_generation,
        task.turn_generation,
    )
    reverse_owner_ids = set(
        (
            await db.execute(
                select(Instance.id).where(Instance.current_task_id == task_id)
            )
        )
        .scalars()
        .all()
    )
    candidate_ids = set(reverse_owner_ids)
    if task.instance_id is not None:
        candidate_ids.add(task.instance_id)

    # Release the discovery snapshot before waiting for lifecycle locks. A
    # launch holder may need to commit Task/Instance ownership before releasing
    # that lock, and MySQL RR would otherwise keep all lock-internal reads on
    # the stale generation.
    await db.rollback()

    # Take every relevant lifecycle lock in stable order. This covers the
    # one-sided recovery state where Task.instance_id is NULL but an Instance
    # still names the task, and avoids deadlocks between two malformed rows.
    async with AsyncExitStack() as stack:
        for instance_id in sorted(candidate_ids):
            await stack.enter_async_context(
                instance_manager._instance_lifecycle_lock(instance_id)
            )

        db.expire_all()
        current_task = await db.get(Task, task_id)
        if current_task is None:
            return None
        current_generation = (
            current_task.retry_count,
            current_task.instance_id,
            current_task.started_at,
            current_task.completed_at,
            current_task.pty_background_generation,
            current_task.turn_generation,
        )
        if (
            current_task.status != observed_status
            or current_generation != observed_generation
        ):
            raise HTTPException(
                status_code=409,
                detail=(
                    "Task ownership changed or generation changed while retrying; "
                    "refresh and try again"
                ),
            )

        if effect_request is not None:
            # This is the last transaction before the retry generation is
            # published.  Take Project -> Task -> group-membership authority
            # only after Harness cleanup and lifecycle-lock discovery have
            # ended their read snapshots, then retain it through queue.retry.
            current_task = await lock_task_effect_access(
                effect_request,
                current_task,
                db,
                allow_chat_share=False,
            )
            current_generation = (
                current_task.retry_count,
                current_task.instance_id,
                current_task.started_at,
                current_task.completed_at,
                current_task.pty_background_generation,
                current_task.turn_generation,
            )
            if (
                current_task.status != observed_status
                or current_generation != observed_generation
            ):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "Task ownership changed or generation changed while "
                        "retrying; refresh and try again"
                    ),
                )

        # Take the Task row/current-generation lock before any Instance row.
        # cancel/delete use the same Task -> Instance order; without this
        # guard retry could hold Instance while cancellation waits for it and
        # then block on cancellation's Task lock.
        guarded_task = await db.execute(
            sa_update(Task)
            .where(*_task_generation_fence(task_id, current_task))
            .values(status=current_task.status)
        )
        if not guarded_task.rowcount:
            await db.rollback()
            raise HTTPException(
                status_code=409,
                detail=(
                    "Task ownership changed or generation changed while retrying; "
                    "refresh and try again"
                ),
            )

        owner_result = await db.execute(
            select(Instance)
            .where(Instance.current_task_id == task_id)
            .with_for_update()
        )
        reverse_owners = list(owner_result.scalars().all())
        current_candidate_ids = {instance.id for instance in reverse_owners}
        if current_task.instance_id is not None:
            current_candidate_ids.add(current_task.instance_id)
        if not current_candidate_ids.issubset(candidate_ids):
            raise HTTPException(
                status_code=409,
                detail=("Task ownership changed while retrying; refresh and try again"),
            )

        # A task-side link without a reverse owner can still point at a
        # pre-commit managed generation. Treat it as uncertain unless the slot
        # now explicitly belongs to another task.
        if current_task.instance_id is not None:
            task_side_instance = (
                await db.execute(
                    select(Instance)
                    .where(Instance.id == current_task.instance_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if (
                task_side_instance is not None
                and task_side_instance.current_task_id in (None, task_id)
                and instance_manager.is_running(current_task.instance_id)
            ):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Instance {current_task.instance_id} still has a live "
                        "managed generation; stop it before retrying"
                    ),
                )

        for instance in reverse_owners:
            if instance_manager.is_running(instance.id):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Instance {instance.id} still has a live managed "
                        "generation; stop it before retrying"
                    ),
                )
            pid = instance.pid
            if pid is not None:
                # Compare the whole recorded identity, not just the PID number:
                # after PID reuse or a host restart an unrelated process can
                # answer to it and pin this task as un-retryable forever.
                liveness = persisted_process_liveness(
                    pid,
                    instance.process_identity,
                )
                if liveness == "alive":
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"Unmanaged process PID {pid} is still alive; "
                            "stop it before retrying"
                        ),
                    )
                if liveness != "dead":
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"Unmanaged process PID {pid} may still be alive; "
                            "stop or reconcile it before retrying"
                        ),
                    )
            elif instance.status == "running":
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Instance {instance.id} still has an uncertain running "
                        "owner; stop or reconcile it before retrying"
                    ),
                )

            instance_predicates = [
                Instance.id == instance.id,
                Instance.current_task_id == task_id,
                Instance.status == instance.status,
                (Instance.pid.is_(None) if pid is None else Instance.pid == pid),
                (
                    Instance.process_identity.is_(None)
                    if instance.process_identity is None
                    else Instance.process_identity == instance.process_identity
                ),
                (
                    Instance.started_at.is_(None)
                    if instance.started_at is None
                    else Instance.started_at == instance.started_at
                ),
            ]
            cleared = await db.execute(
                sa_update(Instance)
                .where(*instance_predicates)
                .values(
                    status="error",
                    current_task_id=None,
                    pid=None,
                    process_identity=None,
                )
            )
            if not cleared.rowcount:
                await db.rollback()
                raise HTTPException(
                    status_code=409,
                    detail="Instance ownership changed while retrying; try again",
                )

        if (
            task_updates is not None
            and task_updates.get("execution_principal_kind") == "user"
        ):
            from backend.models.user import User

            # Authentication froze the caller's role before Harness cleanup
            # and lifecycle reconciliation began.  Lock and revalidate the
            # durable User row in the same transaction as the final retry CAS
            # so a concurrent disable/demotion cannot first persist a stale
            # admin/unrestricted principal and rely on the later provider
            # boundary to clean it up.
            principal_gate = await db.execute(
                sa_update(User)
                .where(
                    User.id == task_updates.get("execution_user_id"),
                    User.is_active.is_(True),
                    User.role == task_updates.get("execution_user_role"),
                )
                .values(role=User.role)
            )
            if principal_gate.rowcount != 1:
                await db.rollback()
                raise HTTPException(
                    409,
                    "Task retry principal is no longer active or its role "
                    "changed",
                )

        retried = await queue.retry(
            task_id,
            expected_statuses=(observed_status,),
            generation_fence=observed_generation,
            rollback_on_miss=True,
            task_updates=task_updates,
            expected_incarnation_id=expected_incarnation_id,
            expected_principal=expected_principal,
            commit=commit,
        )
        if retried is None:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Task ownership changed or generation changed while retrying; "
                    "refresh and try again"
                ),
            )
        return retried


@router.get("/{task_id}/plan-delete-audit", include_in_schema=False)
async def audit_worker_task_plan_delete(
    task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Prove whether an exact Worker Task still owns first-class Plans."""

    require_internal_service(request)
    from backend.models.plan import Plan

    plan_ids = list(
        (
            await db.execute(
                select(Plan.id)
                .where(Plan.target_task_id == task_id)
                .order_by(Plan.id)
            )
        ).scalars()
    )
    return {
        "plan_cascade_protocol": _PLAN_CASCADE_PROTOCOL_VERSION,
        "task_exists": await db.get(Task, task_id) is not None,
        "remaining_target_plan_ids": plan_ids,
    }


@router.delete("/{task_id}")
async def delete_task(
    task_id: int,
    request: Request,
    queue: TaskQueue = Depends(_get_queue),
    db: AsyncSession = Depends(get_db),
):
    task = await require_worker_control_plane_task_incarnation(
        request,
        task_id,
        db,
        write_fence=True,
    )
    if task is None:
        task = await db.get(Task, task_id)
    if task:
        await require_task_control(request, task, db)
    from backend.main import instance_manager, task_migrator, worker_proxy

    if task is None:
        raise HTTPException(404, "Task not found")
    _require_not_delivery_owned_task(task, action="deleted")
    _require_not_waiting_capability(task, action="deleted")
    await _require_not_isolated_browser_child(db, task, action="deleted")
    await _require_no_pr_review_publication(db, task_id)
    await _require_not_pr_review_task_mutation(
        db,
        task_id,
        action="deleted",
    )
    if (
        task.worker_id is None
        and task.pty_background_generation is not None
    ):
        raise HTTPException(
            409,
            "Task still has active Claude PTY background output",
        )

    if task is not None and task.worker_id is not None:
        # A Worker task has two durable copies, but only the remote copy owns
        # its process lifecycle.  Serialize against migration so an A→B→A ABA
        # cannot rebuild the task after the remote delete and still satisfy the
        # Manager mirror fence.
        await db.rollback()
        migration_lock = None
        if task_migrator is not None:
            migration_lock = task_migrator._locks.setdefault(
                task_id,
                asyncio.Lock(),
            )
            if migration_lock.locked():
                raise HTTPException(
                    409,
                    "Task is being migrated; retry deletion after migration",
                )
        if worker_proxy is None:
            raise HTTPException(503, "Worker 功能未启用")
        worker_operation_lock = worker_proxy.task_operation_lock(task_id)

        async with AsyncExitStack() as stack:
            if migration_lock is not None:
                await stack.enter_async_context(migration_lock)
            await stack.enter_async_context(worker_operation_lock)

            db.expire_all()
            worker_task = await db.get(Task, task_id)
            if worker_task is None:
                raise HTTPException(404, "Task not found")
            worker_task = await lock_task_effect_access(
                request,
                worker_task,
                db,
                allow_chat_share=False,
            )
            _require_not_delivery_owned_task(worker_task, action="deleted")
            _require_not_waiting_capability(worker_task, action="deleted")
            await _require_not_isolated_browser_child(
                db,
                worker_task,
                action="deleted",
            )
            await _require_no_pr_review_publication(db, task_id)
            await _require_not_pr_review_task_mutation(
                db,
                task_id,
                action="deleted",
            )
            if worker_task.worker_id is None:
                raise HTTPException(
                    409,
                    "Task moved back to this Manager; refresh before deleting",
                )
            active_delete = await active_worker_task_termination_receipt(
                db,
                task_id,
            )
            operation_id: str
            if active_delete is not None:
                if not (
                    active_delete.side == "manager"
                    and active_delete.operation == "delete"
                    and active_delete.worker_id == worker_task.worker_id
                    and active_delete.active_task_id == task_id
                    and active_delete.status
                    in {"pending_remote", "conflict", "awaiting_ack"}
                ):
                    raise HTTPException(
                        409,
                        "Task has a different active Worker termination receipt",
                    )
                operation_id = active_delete.operation_id
                await db.rollback()
            else:
                if not is_task_status_deletable(
                    mode=worker_task.mode,
                    status=worker_task.status,
                ):
                    raise HTTPException(
                        400,
                        "Cannot delete task (not in deletable state)",
                    )
                delete_fence = task_delete_fence(worker_task)
                staged_operation_id: str | None = None

                async def stage_durable_delete(
                    preflight: TaskDeletePreflight,
                ) -> bool:
                    nonlocal staged_operation_id
                    locked_task = await db.get(
                        Task,
                        task_id,
                        populate_existing=True,
                    )
                    if locked_task is None:
                        return False
                    receipt = await stage_manager_task_delete_receipt(
                        db,
                        locked_task,
                        plan_ids=preflight.plan_ids,
                    )
                    staged_operation_id = receipt.operation_id
                    return True

                prepared = await queue.delete(
                    task_id,
                    expected_fence=delete_fence,
                    prepare_remote_worker_delete=stage_durable_delete,
                )
                if not prepared or staged_operation_id is None:
                    raise HTTPException(
                        409,
                        "Worker Task deletion preflight could not freeze the "
                        "exact local Task/Plan graph; no remote mutation was "
                        "attempted",
                    )
                operation_id = staged_operation_id

            try:
                outcome = await _finish_task_operation(
                    reconcile_manager_task_delete_receipt(
                        db,
                        operation_id,
                        proxy_request=_proxy,
                        protocol_check=(
                            worker_proxy.require_task_plan_delete_protocol
                        ),
                    )
                )
            except WorkerTaskTerminationPending as exc:
                raise HTTPException(
                    503,
                    "Worker Task deletion is durably quarantined and will "
                    f"continue by read-only reconciliation: {exc}",
                ) from exc
            except DurableWorkerTerminationConflict as exc:
                raise HTTPException(
                    409,
                    f"Worker Task deletion identity conflict: {exc}",
                ) from exc
            worker_proxy.relay.unsubscribe_task(outcome.worker_id, task_id)
        return {
            "ok": True,
            "plan_cascade_protocol": _PLAN_CASCADE_PROTOCOL_VERSION,
            "deleted_plan_ids": list(outcome.plan_ids),
            "remaining_target_plan_ids": [],
        }

    deleted_local_plan_ids: list[int] = []
    from backend.services.test_harness import test_harness_service

    await db.rollback()
    async with get_task_operation_lock(task_id):
        db.expire_all()
        current = await db.get(Task, task_id)
        if current is None:
            raise HTTPException(404, "Task not found")
        current = await lock_task_effect_access(
            request,
            current,
            db,
            allow_chat_share=False,
        )
        _require_not_delivery_owned_task(current, action="deleted")
        _require_not_waiting_capability(current, action="deleted")
        await _require_not_isolated_browser_child(db, current, action="deleted")
        await _require_no_pr_review_publication(db, task_id)
        await _require_not_pr_review_task_mutation(
            db,
            task_id,
            action="deleted",
        )
        if current.worker_id is not None or current.shared_from_id is not None:
            raise HTTPException(
                409,
                "Task execution location changed before local deletion",
            )
        if current.pty_background_generation is not None:
            raise HTTPException(
                409,
                "Task still has active Claude PTY background output",
            )
        if await active_worker_task_termination_receipt(db, task_id):
            raise HTTPException(
                409,
                "Task has an active Worker termination receipt",
            )
        if not is_task_status_deletable(
            mode=current.mode,
            status=current.status,
        ):
            raise HTTPException(
                400,
                "Cannot delete task (not found or not in deletable state)",
            )
        delete_fence = task_delete_fence(current)
        expected_harness_owner = await _commit_task_control_effect_gate(
            request,
            db,
            current,
            effect="delete",
        )
        async with test_harness_service.owner_stop_fence(
            task_id,
            reason="Owner Task was deleted",
            expected_identity=expected_harness_owner,
            locked_owner_validator=_task_control_effect_gate_validator(
                expected_harness_owner,
                "delete",
            ),
        ):
            db.expire_all()
            fenced_task = await db.get(Task, task_id)
            if fenced_task is None or not _task_control_effect_gate_matches(
                fenced_task,
                expected_harness_owner,
                "delete",
            ):
                raise HTTPException(
                    409,
                    "Task deletion effect gate changed before lifecycle cleanup",
                )
            lifecycle_ids = set(
                (
                    await db.execute(
                        select(Instance.id).where(
                            Instance.current_task_id == task_id
                        )
                    )
                )
                .scalars()
                .all()
            )
            if fenced_task.instance_id is not None:
                task_side_instance = await db.get(
                    Instance,
                    fenced_task.instance_id,
                )
                if (
                    task_side_instance is not None
                    and task_side_instance.current_task_id in (None, task_id)
                ):
                    lifecycle_ids.add(fenced_task.instance_id)
            # FullMirror may have released the normal Instance maps while
            # retaining the exact native Claude Session for a follow-up.
            # Stop that post-exit proof before taking the regular lifecycle
            # locks and before allowing TaskQueue to remove the durable Task
            # identity. Capture all generation fields before rollback because
            # the cleanup itself must run outside the DB read transaction.
            retained_session_id = fenced_task.session_id
            retained_instance_id = fenced_task.instance_id
            retained_retry_count = fenced_task.retry_count
            retained_turn_generation = fenced_task.turn_generation
            # Do not wait on a lifecycle lock while retaining a read transaction:
            # launch holds that lock while committing Task/Instance metadata.
            await db.rollback()

            cleanup_pty = getattr(
                instance_manager,
                "cleanup_task_pty_for_delete",
                None,
            )
            if callable(cleanup_pty):
                cleaned = await cleanup_pty(
                    task_id,
                    session_id=retained_session_id,
                    instance_id=retained_instance_id,
                    task_retry_count=retained_retry_count,
                    task_turn_generation=retained_turn_generation,
                )
                if not cleaned:
                    raise HTTPException(
                        409,
                        "Task still has an active Claude PTY session; "
                        "retry deletion after it is stopped",
                    )

            # Serialize deletion with the complete launch/spawn/persist window. A
            # terminal Task can otherwise disappear just before a child is registered;
            # the launch would eventually abort, but shutdown in that gap would have no
            # durable Task evidence.
            async with AsyncExitStack() as stack:
                for instance_id in sorted(lifecycle_ids):
                    await stack.enter_async_context(
                        instance_manager._instance_lifecycle_lock(instance_id)
                    )

                async def capture_delete_preflight(
                    preflight: TaskDeletePreflight,
                ) -> bool:
                    locked_task = await db.get(
                        Task,
                        task_id,
                        populate_existing=True,
                    )
                    if locked_task is None or not (
                        _task_control_effect_gate_matches(
                            locked_task,
                            expected_harness_owner,
                            "delete",
                        )
                    ):
                        raise HTTPException(
                            409,
                            "Task deletion effect gate changed before commit",
                        )
                    deleted_local_plan_ids.extend(preflight.plan_ids)
                    return True

                ok = await queue.delete(
                    task_id,
                    expected_fence=delete_fence,
                    before_delete=capture_delete_preflight,
                )
    if not ok:
        db.expire_all()
        current = await db.get(Task, task_id)
        if current is not None:
            _require_not_waiting_capability(current, action="deleted")
        raise HTTPException(
            400, "Cannot delete task (not found or not in deletable state)"
        )
    return {
        "ok": True,
        "plan_cascade_protocol": _PLAN_CASCADE_PROTOCOL_VERSION,
        "deleted_plan_ids": deleted_local_plan_ids,
        "remaining_target_plan_ids": [],
    }


async def _worker_task_or_none(db: AsyncSession, task_id: int) -> Task | None:
    """task 在 Worker 上则返回之（代理路径），本机返回 None。"""
    task = await db.get(Task, task_id)
    return task if (task and task.worker_id is not None) else None


async def _proxy(
    task: Task,
    method: str,
    path: str,
    body=None,
    *,
    require_json: bool = False,
    allow_task_absent: bool = False,
    surface_endpoint_not_found: bool = False,
    operation_lock_held: bool = False,
    quarantine_on_transport_uncertainty: bool = False,
    require_task_incarnation_fence: bool = False,
):
    from backend.main import worker_proxy

    if worker_proxy is None:
        raise HTTPException(503, "Worker 功能未启用")
    if (
        require_json
        or allow_task_absent
        or surface_endpoint_not_found
        or operation_lock_held
        or quarantine_on_transport_uncertainty
        or require_task_incarnation_fence
    ):
        proxy_options = {
            "require_json": require_json,
            "allow_task_absent": allow_task_absent,
            "operation_lock_held": operation_lock_held,
        }
        if surface_endpoint_not_found:
            proxy_options["surface_endpoint_not_found"] = True
        if quarantine_on_transport_uncertainty:
            proxy_options["quarantine_on_transport_uncertainty"] = True
        if require_task_incarnation_fence:
            proxy_options["require_task_incarnation_fence"] = True
        return await worker_proxy.proxy_to_worker(
            task,
            method,
            path,
            body,
            **proxy_options,
        )
    return await worker_proxy.proxy_to_worker(task, method, path, body)


async def _sync_worker_skill_selection_before_execution(task: Task) -> None:
    """Confirm Manager Skills on the Worker before an executable transition."""

    from backend.main import worker_proxy

    if worker_proxy is None:
        raise HTTPException(503, "Worker 功能未启用")
    worker = await worker_proxy.require_ready_worker(task.worker_id)
    await worker_proxy.sync_task_skill_selection(worker, task)


async def _sync_task_from_worker_response(
    db: AsyncSession,
    task: Task,
    result,
    *,
    observed: WorkerTaskGeneration,
):
    """代理响应是 worker 的 task JSON 时，同步关键字段（status 等 relay 也会同步，
    这里立即写一份让 API 响应不滞后）。

    ``observed`` 必须在代理网络请求前捕获。响应回来后只允许 CAS 那个
    Worker assignment/generation，不能重新读取当前 Task 后把旧响应套到新代次。
    """

    task_id = observed.task_id
    resulting = await apply_authoritative_worker_task(db, observed, result)
    if resulting is None:
        await db.rollback()
        raise HTTPException(
            409,
            "Task Worker assignment or generation changed while the request "
            "was in flight",
        )
    await _publish_worker_status_transition(
        db,
        observed_status=observed.status,
        resulting=resulting,
    )

    db.expire_all()
    current = await db.get(Task, task_id)
    if current is None:
        raise HTTPException(
            409,
            "Task disappeared while the Worker request was in flight",
        )
    return current


async def _publish_worker_status_transition(
    db: AsyncSession,
    *,
    observed_status: str,
    resulting: WorkerTaskGeneration,
) -> None:
    """Publish one exact Worker status transition behind its generation CAS."""

    if resulting.status == observed_status:
        return
    # Relay disconnects can hide the Worker's broadcast. Hold an exact-result
    # no-op UPDATE across publication so an old event cannot cross a retry or
    # migration generation.
    guarded = await db.execute(
        sa_update(Task)
        .where(
            *worker_task_generation_predicates(resulting),
            no_active_worker_task_termination_predicate(),
        )
        .values(status=resulting.status)
    )
    if guarded.rowcount != 1:
        await db.rollback()
        raise HTTPException(
            409,
            "Task Worker assignment or generation changed before status "
            "publication",
        )
    from backend.services.task_events import broadcast_status_change

    await broadcast_status_change(resulting.task_id, resulting.status)
    await db.commit()


async def _internal_worker_termination_task(
    task_id: int,
    request: Request,
    db: AsyncSession,
) -> Task:
    """Authorize the v2 durable Manager->Worker termination protocol."""

    require_internal_service(request)
    task = await db.get(Task, task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    await require_task_control(request, task, db)
    if task.worker_id is not None or task.shared_from_id is not None:
        raise HTTPException(409, "Termination receipt Task is not Worker-local")
    return task


def _worker_destroy_cleanup_identity(
    request: Request,
) -> tuple[str | None, str | None, int | None, int | None]:
    """Parse the all-or-nothing exact identity for phase-one destroy cleanup.

    The Worker bearer token authenticates the Manager control plane. These
    additional headers narrow one PUT to the already-installed node drain and
    the Manager's exact Task incarnation/retry/turn snapshot; they are never
    accepted piecemeal or on an ordinary pre-drain termination request.
    """

    raw_claim = request.headers.get(WORKER_DESTROY_DRAIN_CLAIM_HEADER)
    raw_incarnation = request.headers.get(
        WORKER_DESTROY_TASK_INCARCATION_HEADER
    )
    raw_retry = request.headers.get(WORKER_DESTROY_TASK_RETRY_HEADER)
    raw_turn = request.headers.get(WORKER_DESTROY_TASK_TURN_HEADER)
    values = (raw_claim, raw_incarnation, raw_retry, raw_turn)
    if all(value is None for value in values):
        return None, None, None, None
    if any(value is None for value in values):
        raise HTTPException(
            409,
            "Worker destroy cleanup identity headers are incomplete",
        )
    assert raw_claim is not None
    assert raw_incarnation is not None
    assert raw_retry is not None
    assert raw_turn is not None
    if (
        len(raw_claim) != 64
        or any(char not in "0123456789abcdef" for char in raw_claim)
        or len(raw_incarnation) != 32
        or any(char not in "0123456789abcdef" for char in raw_incarnation)
    ):
        raise HTTPException(
            409,
            "Worker destroy cleanup identity is malformed",
        )

    def parse_generation(raw: str) -> int:
        if not raw.isascii() or not raw.isdecimal():
            raise HTTPException(
                409,
                "Worker destroy cleanup generation is malformed",
            )
        parsed = int(raw)
        if parsed < 0 or str(parsed) != raw:
            raise HTTPException(
                409,
                "Worker destroy cleanup generation is malformed",
            )
        return parsed

    return (
        raw_claim,
        raw_incarnation,
        parse_generation(raw_retry),
        parse_generation(raw_turn),
    )


@router.get(
    "/{task_id}/termination-receipts/{operation_id}",
    include_in_schema=False,
)
async def get_worker_termination_receipt(
    task_id: int,
    operation_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Read one Worker receipt; absence is an explicit idempotency sentinel."""

    require_internal_service(request)
    from backend.models.worker_task_termination import WorkerTaskTerminationReceipt

    receipt = await db.get(WorkerTaskTerminationReceipt, operation_id)
    if receipt is not None:
        if receipt.task_id != task_id or receipt.side != "worker":
            raise HTTPException(409, "Termination receipt identity changed")
        try:
            return serialize_receipt(receipt)
        except DurableWorkerTerminationConflict as exc:
            raise HTTPException(409, str(exc)) from exc
    task = await db.get(Task, task_id)
    if task is None:
        return task_not_found_payload(task_id, operation_id)
    await require_task_control(request, task, db)
    if task.worker_id is not None or task.shared_from_id is not None:
        raise HTTPException(409, "Termination receipt Task is not Worker-local")
    return receipt_not_found_payload(task_id, operation_id)


@router.put(
    "/{task_id}/termination-receipts/{operation_id}",
    include_in_schema=False,
)
async def put_worker_termination_receipt(
    task_id: int,
    operation_id: str,
    body: WorkerTerminationPutRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Accept durably, then stop/cancel the exact Worker-local generation."""

    require_internal_service(request)
    from backend.services.task_termination import task_termination_operation_locks

    await db.rollback()
    async with task_termination_operation_locks((task_id,)):
        await _internal_worker_termination_task(task_id, request, db)
        (
            destroy_drain_claim,
            destroy_task_incarnation_id,
            destroy_task_retry_count,
            destroy_task_turn_generation,
        ) = _worker_destroy_cleanup_identity(request)
        try:
            receipt = await stage_worker_receipt(
                db,
                task_id=task_id,
                operation_id=operation_id,
                operation=body.operation,
                request_payload=body.request_payload,
                request_digest=body.request_digest,
                destroy_drain_claim=destroy_drain_claim,
                destroy_task_incarnation_id=destroy_task_incarnation_id,
                destroy_task_retry_count=destroy_task_retry_count,
                destroy_task_turn_generation=(
                    destroy_task_turn_generation
                ),
            )
            if receipt.status in {"accepted", "executing"}:
                receipt = await _finish_task_operation(
                    execute_worker_receipt(db, operation_id)
                )
        except DurableWorkerTerminationConflict as exc:
            # If acceptance never committed, persist a digest-bound rejected
            # tombstone so Manager GET can prove PUT had no side effect and
            # release its gate.  Once accepted/executing exists, the same error
            # is an active conflict quarantine instead.
            rejected = await persist_worker_preflight_rejection(
                db,
                task_id=task_id,
                operation_id=operation_id,
                operation=body.operation,
                request_payload=body.request_payload,
                request_digest=body.request_digest,
                error=str(exc),
            )
            if rejected is not None and rejected.status == "rejected":
                try:
                    return serialize_receipt(rejected)
                except DurableWorkerTerminationConflict as serialization_exc:
                    raise HTTPException(409, str(serialization_exc)) from serialization_exc
            await mark_worker_receipt_conflict(db, operation_id, exc)
            raise HTTPException(409, str(exc)) from exc
        try:
            return serialize_receipt(receipt)
        except DurableWorkerTerminationConflict as exc:
            raise HTTPException(409, str(exc)) from exc


@router.post(
    "/{task_id}/termination-receipts/{operation_id}/ack",
    include_in_schema=False,
)
async def ack_worker_termination_receipt(
    task_id: int,
    operation_id: str,
    body: WorkerTerminationAckRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Release the Worker active gate after exact Manager result commit."""

    require_internal_service(request)
    await db.rollback()
    async with get_task_operation_lock(task_id):
        await _internal_worker_termination_task(task_id, request, db)
        try:
            receipt = await acknowledge_worker_receipt(
                db,
                task_id=task_id,
                operation_id=operation_id,
                request_digest=body.request_digest,
                result_digest=body.result_digest,
            )
        except DurableWorkerTerminationConflict as exc:
            raise HTTPException(409, str(exc)) from exc
        try:
            return serialize_receipt(receipt)
        except DurableWorkerTerminationConflict as exc:
            raise HTTPException(409, str(exc)) from exc


async def _internal_pr_review_termination_task(
    task_id: int,
    request: Request,
    db: AsyncSession,
) -> Task:
    """Authorize one hidden Manager→Worker termination protocol request."""

    require_internal_service(request)
    task = await db.get(Task, task_id)
    if task:
        await require_task_control(request, task, db)
        await _require_no_pr_review_publication(db, task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    if not is_pr_sandbox_task(task):
        raise HTTPException(
            400,
            "Exact-generation termination is restricted to PR workflow tasks",
        )
    return task


@router.get(
    "/{task_id}/terminate-generation",
    response_model=TaskTerminationSnapshot,
    include_in_schema=False,
)
async def get_task_termination_generation(
    task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Return the Worker's exact opaque generation to its Manager only."""

    return await _internal_pr_review_termination_task(task_id, request, db)


@router.post(
    "/{task_id}/terminate-generation",
    response_model=InternalTaskResponse,
    include_in_schema=False,
)
async def terminate_task_generation(
    task_id: int,
    body: TaskTerminationRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Reject the pre-receipt mutation protocol without side effects."""

    await _internal_pr_review_termination_task(task_id, request, db)
    raise HTTPException(
        409,
        "Legacy termination mutation is disabled; use the durable "
        "termination receipt protocol",
    )

async def _require_local_termination_effect_authority(
    task_id: int,
    db: AsyncSession,
    *,
    worker_termination_operation_id: str | None,
    expected_operation: str,
    worker_termination_execution_token: str | None = None,
    worker_termination_state_version: int | None = None,
) -> None:
    """Fence queue/process effects against the durable termination owner."""

    task, receipt = await _lock_local_termination_effect_authority(
        task_id,
        db,
    )
    lease_valid_at = datetime.utcnow()
    authorized = local_task_termination_effect_authority_matches(
        task,
        receipt,
        operation_id=worker_termination_operation_id,
        operation=(
            expected_operation
            if worker_termination_operation_id is not None
            else None
        ),
        execution_token=worker_termination_execution_token,
        state_version=worker_termination_state_version,
        lease_valid_at=lease_valid_at,
    )
    await db.rollback()
    if not authorized:
        raise HTTPException(
            409,
            "Task termination effects are owned by a different durable "
            "Worker receipt",
        )


async def _lock_local_termination_effect_authority(
    task_id: int,
    db: AsyncSession,
):
    """Acquire the portable Task writer barrier, then the active receipt row."""

    await db.rollback()
    task_lock = await db.execute(
        sa_update(Task)
        .where(Task.id == task_id)
        .values(status=Task.status)
        .execution_options(synchronize_session=False)
    )
    if task_lock.rowcount != 1:
        await db.rollback()
        raise HTTPException(404, "Task not found")
    task = await db.get(Task, task_id, populate_existing=True)
    if task is None:
        await db.rollback()
        raise HTTPException(404, "Task not found")
    receipt = await active_worker_task_termination_receipt(
        db,
        task_id,
        for_update=True,
    )
    return task, receipt


@dataclass(frozen=True)
class _ExecutingCapabilityResumeClaim:
    """Exact durable + volatile identity needed to quiesce claimed G+1."""

    outbox_id: int
    invocation_id: int
    lease_token: str
    task_incarnation_id: str
    retry_count: int
    turn_generation: int
    instance_id: int
    session_id: str | None
    from_turn_generation: int
    request_source_log_id: int
    request_output_log_id: int
    request_terminal_log_id: int
    request_native_turn_id: str | None
    resume_source_log_id: int


async def _executing_pre_provider_capability_resume_claim(
    db: AsyncSession,
    task: Task,
) -> _ExecutingCapabilityResumeClaim | None:
    """Return only one canonical live-worker G+1 claim.

    This is a quiescence hint, not terminal authority.  The queue worker still
    owns ``capability_task_lock`` in this window, so this helper deliberately
    performs read-only inspection and never waits on that lock.  After the
    worker is joined, the caller must freshly prove the restored waiting
    aggregate before cancelling anything durable.
    """

    if task.status != "executing":
        return None

    from backend.models.capability import (
        ACTIVE_EXECUTION_STATUSES,
        CapabilityExecution,
        CapabilityInvocation,
        CapabilityResumeOutbox,
    )
    from backend.models.log_entry import LogEntry
    from backend.services.terminal_arbitration import source_shape_is_canonical

    claimed_outboxes = list(
        (
            await db.execute(
                select(CapabilityResumeOutbox)
                .where(
                    CapabilityResumeOutbox.task_id == task.id,
                    CapabilityResumeOutbox.status == "claimed",
                )
                .order_by(CapabilityResumeOutbox.id)
                .execution_options(populate_existing=True)
            )
        ).scalars()
    )
    if not claimed_outboxes:
        return None
    if len(claimed_outboxes) != 1:
        raise HTTPException(
            409,
            "Executing Task has multiple claimed Capability resume outboxes",
        )
    outbox = claimed_outboxes[0]
    invocation = await db.get(
        CapabilityInvocation,
        outbox.invocation_id,
        populate_existing=True,
    )
    executions = list(
        (
            await db.execute(
                select(CapabilityExecution)
                .where(CapabilityExecution.invocation_id == outbox.invocation_id)
                .order_by(CapabilityExecution.attempt, CapabilityExecution.id)
                .execution_options(populate_existing=True)
            )
        ).scalars()
    )
    source = (
        await db.get(
            LogEntry,
            outbox.resume_source_log_id,
            populate_existing=True,
        )
        if type(outbox.resume_source_log_id) is int
        else None
    )
    exact = bool(
        task.mode == "auto"
        and task.worker_id is None
        and task.shared_from_id is None
        and task.delivery_run_id is None
        and task.delivery_role is None
        and task.plan_target_task_id is None
        and type(task.instance_id) is int
        and task.instance_id > 0
        and task.completed_at is None
        and task.pty_background_generation is None
        and outbox.request_task_incarnation_id == task.incarnation_id
        and outbox.request_task_retry_count == task.retry_count
        and outbox.request_task_session_id == task.session_id
        and outbox.from_turn_generation + 1 == task.turn_generation
        and outbox.claimed_turn_generation == task.turn_generation
        and outbox.resume_source_log_id == task.turn_source_log_id
        and type(outbox.resume_source_log_id) is int
        and outbox.resume_source_log_id > 0
        and isinstance(outbox.lease_token, str)
        and len(outbox.lease_token) == 64
        and outbox.lease_expires_at is not None
        and outbox.claimed_at is not None
        and outbox.resume_actual_transport is None
        and outbox.launched_at is None
        and outbox.active_task_id == task.id
        and invocation is not None
        and outbox.active_invocation_id == invocation.id
        and invocation.task_id == task.id
        and invocation.status == "resuming"
        and invocation.source == "agent_request"
        and invocation.purpose == "advisory"
        and invocation.resume_policy == "resume_task"
        and invocation.active_task_id == task.id
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
        and not any(
            execution.status in ACTIVE_EXECUTION_STATUSES
            or execution.active_invocation_id is not None
            for execution in executions
        )
        and source is not None
        and source.task_id == task.id
        and source.task_retry_count == task.retry_count
        and source.task_turn_generation == task.turn_generation
        and source.turn_scope == "source"
        and source.instance_id == task.instance_id
        and source.actual_transport is None
        and source_shape_is_canonical(source)
    )
    if not exact:
        raise HTTPException(
            409,
            "Executing Task Capability resume identity cannot be proven",
        )
    assert invocation is not None
    assert source is not None
    assert isinstance(outbox.lease_token, str)
    return _ExecutingCapabilityResumeClaim(
        outbox_id=outbox.id,
        invocation_id=invocation.id,
        lease_token=outbox.lease_token,
        task_incarnation_id=task.incarnation_id,
        retry_count=task.retry_count,
        turn_generation=task.turn_generation,
        instance_id=task.instance_id,
        session_id=task.session_id,
        from_turn_generation=outbox.from_turn_generation,
        request_source_log_id=outbox.request_source_log_id,
        request_output_log_id=outbox.request_output_log_id,
        request_terminal_log_id=outbox.request_terminal_log_id,
        request_native_turn_id=outbox.request_native_turn_id,
        resume_source_log_id=source.id,
    )


async def _waiting_capability_outbox_is_pre_provider(
    db: AsyncSession,
    task: Task,
    outbox,
) -> bool:
    """Accept baseline G or a released, provably pre-provider G+1 claim."""

    from backend.models.log_entry import LogEntry
    from backend.services.terminal_arbitration import source_shape_is_canonical

    if outbox.status in {"completed", "launched"}:
        return False
    claimed_shape = (
        outbox.status == "claimed"
        or outbox.claimed_turn_generation is not None
        or outbox.resume_source_log_id is not None
        or outbox.claimed_at is not None
    )
    if not claimed_shape:
        return bool(
            task.turn_generation == outbox.from_turn_generation
            and outbox.claimed_turn_generation is None
            and outbox.resume_source_log_id is None
            and outbox.claimed_at is None
            and outbox.resume_actual_transport is None
            and outbox.launched_at is None
        )
    if (
        outbox.claimed_turn_generation != outbox.from_turn_generation + 1
        or task.turn_generation != outbox.claimed_turn_generation
        or task.turn_source_log_id != outbox.resume_source_log_id
        or task.instance_id is not None
        or type(outbox.resume_source_log_id) is not int
        or outbox.resume_source_log_id <= 0
        or outbox.resume_actual_transport is not None
        or outbox.launched_at is not None
    ):
        return False
    source = await db.get(
        LogEntry,
        outbox.resume_source_log_id,
        populate_existing=True,
    )
    return bool(
        source is not None
        and source.task_id == task.id
        and source.task_retry_count == task.retry_count
        and source.task_turn_generation == task.turn_generation
        and source.turn_scope == "source"
        and source.actual_transport is None
        and type(source.instance_id) is int
        and source.instance_id > 0
        and source_shape_is_canonical(source)
    )


async def _cancel_waiting_task_capability_before_queue_abort(
    task_id: int,
    db: AsyncSession,
) -> bool:
    """Stop an exact Auto Capability before cancelling its resume outbox.

    ``clear_task_queue`` deliberately refuses to cancel an outbox while its
    executor may still own a runtime.  A Task-wide stop therefore has to
    settle the linked Invocation first, without holding a Task database lock
    across the executor callback.  The surrounding queue-cancellation lease
    prevents a pre-provider resume from being admitted while this runs.
    """

    from backend.models.capability import (
        ACTIVE_EXECUTION_STATUSES,
        TERMINAL_INVOCATION_STATUSES,
        CapabilityExecution,
        CapabilityInvocation,
        CapabilityResumeOutbox,
    )
    from backend.services.capability_registry import resolve_capability
    from backend.services.capability_service import (
        CapabilityError,
        cancel_invocation,
    )

    await db.rollback()
    db.expire_all()
    task = await db.get(Task, task_id, populate_existing=True)
    if task is None:
        raise HTTPException(404, "Task not found")
    executing_claim = None
    if task.status == "executing":
        executing_claim = (
            await _executing_pre_provider_capability_resume_claim(db, task)
        )
        if executing_claim is None:
            await db.rollback()
            return False
    elif task.status != "waiting_capability":
        await db.rollback()
        return False

    # Stop an already-dequeued resume before changing the Invocation.  The
    # Dispatcher keeps admission closed while allowing that consumer to
    # release a claimed-but-pre-provider G+1 back to waiting state.
    from backend.main import dispatcher
    from backend.services.dispatcher import TaskQueueAbortTimeoutError

    await db.rollback()
    try:
        quiesced = (
            await dispatcher.quiesce_task_queue_consumer_for_capability_cancel(
                task_id,
                **(
                    {
                        "expected_outbox_id": executing_claim.outbox_id,
                        "expected_lease_token": executing_claim.lease_token,
                        "expected_retry_count": executing_claim.retry_count,
                        "expected_turn_generation": (
                            executing_claim.turn_generation
                        ),
                        "expected_instance_id": executing_claim.instance_id,
                    }
                    if executing_claim is not None
                    else {}
                ),
            )
        )
    except TaskQueueAbortTimeoutError as exc:
        raise HTTPException(
            409,
            "Capability resume queue worker could not be proven stopped",
        ) from exc
    if executing_claim is not None and not quiesced:
        raise HTTPException(
            409,
            "Capability resume queue worker identity cannot be proven",
        )
    db.expire_all()
    task = await db.get(Task, task_id, populate_existing=True)
    if task is None:
        raise HTTPException(404, "Task not found")
    if task.status != "waiting_capability":
        await db.rollback()
        if executing_claim is not None:
            raise HTTPException(
                409,
                "Capability resume did not restore its exact pre-provider claim",
            )
        return False
    if executing_claim is not None and (
        task.incarnation_id != executing_claim.task_incarnation_id
        or task.retry_count != executing_claim.retry_count
        or task.turn_generation != executing_claim.turn_generation
        or task.session_id != executing_claim.session_id
        or task.instance_id is not None
        or task.turn_source_log_id != executing_claim.resume_source_log_id
    ):
        await db.rollback()
        raise HTTPException(
            409,
            "Capability resume Task identity changed while its worker stopped",
        )

    outboxes = list(
        (
            await db.execute(
                select(CapabilityResumeOutbox)
                .where(
                    CapabilityResumeOutbox.task_id == task.id,
                    CapabilityResumeOutbox.request_task_incarnation_id
                    == task.incarnation_id,
                    CapabilityResumeOutbox.request_task_retry_count
                    == task.retry_count,
                )
                .order_by(CapabilityResumeOutbox.id)
                .execution_options(populate_existing=True)
            )
        ).scalars()
    )
    live_outboxes = [
        outbox
        for outbox in outboxes
        if outbox.status in {"pending", "ready", "claiming", "claimed", "launched"}
    ]
    if len(live_outboxes) > 1:
        await db.rollback()
        raise HTTPException(
            409,
            "Waiting Task has multiple live Capability resume outboxes",
        )
    if live_outboxes:
        outbox = live_outboxes[0]
    else:
        # Recover a process crash after the outbox commit but before the Task
        # terminal CAS.  Only a row whose G or claimed G+1 equals the current
        # Task generation is eligible.
        terminal_candidates = [
            candidate
            for candidate in outboxes
            if candidate.status in {"cancelled", "failed", "completed"}
            and task.turn_generation
            in {
                candidate.from_turn_generation,
                candidate.claimed_turn_generation,
            }
        ]
        if len(terminal_candidates) != 1:
            await db.rollback()
            raise HTTPException(
                409,
                "Waiting Task does not have one exact Capability resume outbox",
            )
        outbox = terminal_candidates[0]
    if executing_claim is not None and (
        outbox.id != executing_claim.outbox_id
        or outbox.invocation_id != executing_claim.invocation_id
        or outbox.from_turn_generation
        != executing_claim.from_turn_generation
        or outbox.request_source_log_id
        != executing_claim.request_source_log_id
        or outbox.request_output_log_id
        != executing_claim.request_output_log_id
        or outbox.request_terminal_log_id
        != executing_claim.request_terminal_log_id
        or outbox.request_native_turn_id
        != executing_claim.request_native_turn_id
        or outbox.resume_source_log_id
        != executing_claim.resume_source_log_id
    ):
        await db.rollback()
        raise HTTPException(
            409,
            "Capability resume aggregate changed while its worker stopped",
        )
    if outbox.status == "completed":
        await db.rollback()
        raise HTTPException(
            409,
            "Capability resume already completed for the waiting Task generation",
        )

    invocation = await db.get(
        CapabilityInvocation,
        outbox.invocation_id,
        populate_existing=True,
    )
    executions = list(
        (
            await db.execute(
                select(CapabilityExecution)
                .where(CapabilityExecution.invocation_id == outbox.invocation_id)
                .order_by(CapabilityExecution.attempt, CapabilityExecution.id)
                .execution_options(populate_existing=True)
            )
        ).scalars()
    )
    exact_identity = bool(
        invocation is not None
        and invocation.task_id == task.id
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
        and (
            (
                outbox.status in {"pending", "ready", "claiming", "claimed"}
                and outbox.active_task_id == task.id
                and outbox.active_invocation_id == invocation.id
            )
            or (
                outbox.status in {"cancelled", "failed"}
                and outbox.active_task_id is None
                and outbox.active_invocation_id is None
            )
        )
    )
    if not exact_identity or not await _waiting_capability_outbox_is_pre_provider(
        db,
        task,
        outbox,
    ):
        await db.rollback()
        raise HTTPException(
            409,
            "Waiting Task Capability identity cannot be proven",
        )
    assert invocation is not None

    active = [
        execution
        for execution in executions
        if execution.status in ACTIVE_EXECUTION_STATUSES
        or execution.active_invocation_id is not None
    ]
    outbox_id = outbox.id
    invocation_id = invocation.id
    invocation_status = invocation.status
    invocation_state_version = invocation.state_version
    expected_identity = (
        task.incarnation_id,
        task.retry_count,
        task.turn_generation,
        task.session_id,
        outbox.from_turn_generation,
        outbox.request_source_log_id,
        outbox.request_output_log_id,
        outbox.request_terminal_log_id,
        outbox.request_native_turn_id,
    )

    transition_error: tuple[str, BaseException] | None = None
    try:
        if invocation_status == "queued":
            # A queued row is safe to settle without an adapter only when no
            # durable field can possibly identify an already-started runtime.
            if (
                len(active) != 1
                or active[0].status != "queued"
                or active[0].active_invocation_id != invocation.id
                or active[0].executor_kind != invocation.executor_kind
                or active[0].handle_kind is not None
                or active[0].handle_id is not None
                or active[0].handle_generation is not None
                or active[0].lease_token is not None
                or active[0].lease_expires_at is not None
                or active[0].heartbeat_at is not None
                or active[0].started_at is not None
            ):
                raise HTTPException(
                    409,
                    "Queued Capability lacks a durable no-runtime proof",
                )
            await db.rollback()
            await cancel_invocation(
                db,
                invocation_id=invocation_id,
                expected_state_version=invocation_state_version,
                allow_workflow_owned=True,
            )
        elif invocation_status in {"ready", "resuming"}:
            if active:
                raise HTTPException(
                    409,
                    "Result-ready Capability retained an active execution",
                )
            await db.rollback()
            await cancel_invocation(
                db,
                invocation_id=invocation_id,
                expected_state_version=invocation_state_version,
                allow_workflow_owned=True,
            )
        elif invocation_status in {"running", "waiting_user", "cancelling"}:
            expected_execution_status = {
                "running": "running",
                "waiting_user": "waiting_user",
                "cancelling": "cancelling",
            }[invocation_status]
            if (
                len(active) != 1
                or active[0].status != expected_execution_status
                or active[0].active_invocation_id != invocation.id
                or active[0].executor_kind != invocation.executor_kind
            ):
                raise HTTPException(
                    409,
                    "Active Capability execution identity cannot be proven",
                )
            definition = resolve_capability(invocation.capability_key)
            executor = definition.executor if definition is not None else None
            callback = getattr(executor, "cancel", None)
            if (
                definition is None
                or definition.executor_kind != invocation.executor_kind
                or not callable(callback)
            ):
                raise HTTPException(
                    409,
                    "Active Capability executor is unavailable or mismatched",
                )
            await db.rollback()
            await callback(db, invocation_id=invocation_id)
        elif invocation_status not in TERMINAL_INVOCATION_STATUSES:
            raise HTTPException(
                409,
                f"Capability is in unsupported status {invocation_status!r}",
            )
    except HTTPException:
        await db.rollback()
        raise
    except asyncio.CancelledError:
        await db.rollback()
        raise
    except CapabilityError as exc:
        await db.rollback()
        transition_error = (
            "Capability cancellation conflicted with another state transition",
            exc,
        )
    except Exception as exc:
        await db.rollback()
        transition_error = (
            "Capability executor cancellation could not be confirmed",
            exc,
        )

    # Re-read every durable row after the adapter returns.  Its callback may
    # commit several lower-level transitions; its return value is never proof.
    await db.rollback()
    db.expire_all()
    task = await db.get(Task, task_id, populate_existing=True)
    outbox = await db.get(
        CapabilityResumeOutbox,
        outbox_id,
        populate_existing=True,
    )
    invocation = await db.get(
        CapabilityInvocation,
        invocation_id,
        populate_existing=True,
    )
    executions = list(
        (
            await db.execute(
                select(CapabilityExecution)
                .where(CapabilityExecution.invocation_id == invocation_id)
                .order_by(CapabilityExecution.attempt, CapabilityExecution.id)
                .execution_options(populate_existing=True)
            )
        ).scalars()
    )
    final_pre_provider = bool(
        task is not None
        and outbox is not None
        and await _waiting_capability_outbox_is_pre_provider(db, task, outbox)
    )
    settled = not (
        task is None
        or task.status != "waiting_capability"
        or (
            task.incarnation_id,
            task.retry_count,
            task.turn_generation,
            task.session_id,
            outbox.from_turn_generation if outbox is not None else None,
            outbox.request_source_log_id if outbox is not None else None,
            outbox.request_output_log_id if outbox is not None else None,
            outbox.request_terminal_log_id if outbox is not None else None,
            outbox.request_native_turn_id if outbox is not None else None,
        )
        != expected_identity
        or outbox is None
        or outbox.task_id != task_id
        or outbox.invocation_id != invocation_id
        or outbox.status == "completed"
        or not final_pre_provider
        or invocation is None
        or invocation.task_id != task_id
        or invocation.source != "agent_request"
        or invocation.resume_policy != "resume_task"
        or invocation.request_task_incarnation_id != expected_identity[0]
        or invocation.request_task_retry_count != expected_identity[1]
        or invocation.request_task_turn_generation != expected_identity[4]
        or invocation.request_task_session_id != expected_identity[3]
        or invocation.request_source_log_id != expected_identity[5]
        or invocation.request_output_log_id != expected_identity[6]
        or invocation.request_terminal_log_id != expected_identity[7]
        or invocation.request_native_turn_id != expected_identity[8]
        or invocation.status not in TERMINAL_INVOCATION_STATUSES
        or invocation.active_task_id is not None
        or any(
            execution.status in ACTIVE_EXECUTION_STATUSES
            or execution.active_invocation_id is not None
            for execution in executions
        )
    )
    if not settled:
        await db.rollback()
        if transition_error is not None:
            message, cause = transition_error
            raise HTTPException(409, message) from cause
        raise HTTPException(
            409,
            "Capability cancellation did not reach one durable terminal state",
        )
    await db.rollback()
    return True


async def _stop_task_session_local_impl(
    task_id: int,
    db: AsyncSession,
    *,
    expected_identity: TestHarnessOwnerIdentity,
    worker_termination_operation_id: str | None = None,
    worker_supersede: bool = False,
    worker_termination_execution_token: str | None = None,
    worker_termination_state_version: int | None = None,
    task_control_effect: str | None = None,
) -> dict:
    """Keep message admission closed until the stopped generation is final."""

    from backend.main import dispatcher
    from backend.services.test_harness import test_harness_service

    async with test_harness_service.owner_stop_fence(
        task_id,
        reason="Owner Task session was stopped",
        expected_identity=expected_identity,
        locked_owner_validator=(
            _task_control_effect_gate_validator(
                expected_identity,
                task_control_effect,
            )
            if task_control_effect is not None
            else None
        ),
    ):
        async with dispatcher.task_queue_cancellation_lease(task_id):
            return await _stop_task_session_local_under_cancellation_lease(
                task_id,
                db,
                worker_termination_operation_id=(
                    worker_termination_operation_id
                ),
                worker_termination_execution_token=(
                    worker_termination_execution_token
                ),
                worker_termination_state_version=worker_termination_state_version,
                worker_supersede=worker_supersede,
            )


async def _stop_task_session_local_under_cancellation_lease(
    task_id: int,
    db: AsyncSession,
    *,
    worker_termination_operation_id: str | None = None,
    worker_supersede: bool = False,
    worker_termination_execution_token: str | None = None,
    worker_termination_state_version: int | None = None,
) -> dict:
    """Cancellation-safe local core for ``POST /stop-session``."""

    from backend.main import dispatcher, instance_manager, ralph_loop

    # Queue cancellation and auxiliary process stops are effects too.  Prove
    # the exact durable receipt owns them before touching either subsystem;
    # a mismatched operation id must not get as far as the later process CAS.
    await _require_local_termination_effect_authority(
        task_id,
        db,
        worker_termination_operation_id=worker_termination_operation_id,
        expected_operation=(
            "supersede" if worker_supersede else "stop_session"
        ),
        worker_termination_execution_token=(
            worker_termination_execution_token
        ),
        worker_termination_state_version=worker_termination_state_version,
    )
    termination_operation = (
        "supersede" if worker_supersede else "stop_session"
    )
    # A Codex turn may share its persistent account transport with unrelated
    # Tasks.  Shared peers are safe: the registry reserves and unloads only the
    # target root/descendant lineage.  Still reject an in-flight operation on
    # one of those exact target threads before cancelling capabilities,
    # clearing queued chat, or closing auxiliary producers.
    preflight_task = await db.get(Task, task_id, populate_existing=True)
    if preflight_task is not None and preflight_task.instance_id is not None:
        preflight_instance_id = await db.scalar(
            select(Instance.id).where(
                Instance.id == preflight_task.instance_id,
                Instance.current_task_id == task_id,
            )
        )
        from backend.services.codex_app_server import (
            CodexSharedTransportBusyError,
        )

        if preflight_instance_id is not None:
            try:
                await instance_manager.require_stop_session_preflight(
                    preflight_instance_id
                )
            except CodexSharedTransportBusyError as exc:
                await db.rollback()
                raise HTTPException(
                    409,
                    "Task session stop is temporarily blocked by an in-flight "
                    "Codex thread operation; "
                    "no queued messages or auxiliary producers were changed",
                ) from exc
    await db.rollback()
    await _cancel_waiting_task_capability_before_queue_abort(
        task_id,
        db,
    )
    await _require_local_termination_effect_authority(
        task_id,
        db,
        worker_termination_operation_id=worker_termination_operation_id,
        expected_operation=termination_operation,
        worker_termination_execution_token=(
            worker_termination_execution_token
        ),
        worker_termination_state_version=worker_termination_state_version,
    )
    await db.rollback()
    try:
        cleared = await dispatcher.abort_task_queue(
            task_id,
            cancel_durable=False,
            durable_db=db,
        )
    except Exception as exc:
        from backend.services.dispatcher import TaskQueueAbortTimeoutError

        if isinstance(exc, TaskQueueAbortTimeoutError):
            raise HTTPException(
                409,
                "Task queue worker could not be proven stopped; no terminal "
                "state was published",
            ) from exc
        raise

    await _require_local_termination_effect_authority(
        task_id,
        db,
        worker_termination_operation_id=worker_termination_operation_id,
        expected_operation=termination_operation,
        worker_termination_execution_token=worker_termination_execution_token,
        worker_termination_state_version=worker_termination_state_version,
    )

    # stop-session is a Task-wide execution stop, not merely a signal to the
    # current foreground process.  Monitors and CCM-owned sub-agents are
    # independent message producers; if they remain ``running`` they can post
    # a report immediately after the queue drain and resurrect the Task.  Close
    # those producers durably before resolving/stopping the main owner, then
    # drain once more to catch a report that was already in flight.
    from backend.models.monitor_session import MonitorSession

    (
        auxiliary_task,
        auxiliary_receipt,
    ) = await _lock_local_termination_effect_authority(task_id, db)
    auxiliary_rows = await db.execute(
        select(
            MonitorSession.id,
            MonitorSession.agent_type,
            MonitorSession.source,
        )
        .where(
            MonitorSession.task_id == task_id,
            MonitorSession.status.in_(("running", "cancelled")),
        )
        .with_for_update()
    )
    auxiliary_sessions = list(auxiliary_rows.all())
    auxiliary_lease_valid_at = datetime.utcnow()
    if not local_task_termination_effect_authority_matches(
        auxiliary_task,
        auxiliary_receipt,
        operation_id=worker_termination_operation_id,
        operation=(
            termination_operation
            if worker_termination_operation_id is not None
            else None
        ),
        execution_token=worker_termination_execution_token,
        state_version=worker_termination_state_version,
        lease_valid_at=auxiliary_lease_valid_at,
    ):
        await db.rollback()
        raise HTTPException(
            409,
            "Task termination receipt lease expired while auxiliary rows "
            "were being locked",
        )
    await db.execute(
        sa_update(MonitorSession)
        .where(
            MonitorSession.task_id == task_id,
            MonitorSession.status == "running",
        )
        .values(
            status="cancelled",
            completed_at=datetime.utcnow(),
            next_check_at=None,
            active_turn_generation=None,
            turn_started_at=None,
        )
    )
    await db.commit()

    for session_id, agent_type, source in auxiliary_sessions:
        if source != "ccm":
            continue
        await _require_local_termination_effect_authority(
            task_id,
            db,
            worker_termination_operation_id=worker_termination_operation_id,
            expected_operation=termination_operation,
            worker_termination_execution_token=(
                worker_termination_execution_token
            ),
            worker_termination_state_version=worker_termination_state_version,
        )
        try:
            if agent_type == "sub_agent":
                await dispatcher.stop_sub_agent_session_process(session_id)
            elif agent_type == "monitor":
                await dispatcher.stop_monitor_session_process(
                    session_id,
                    terminal=True,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise HTTPException(
                409,
                "Task message producers were closed, but auxiliary process "
                f"cleanup could not be confirmed for session {session_id}",
            ) from exc

    if auxiliary_sessions:
        await _require_local_termination_effect_authority(
            task_id,
            db,
            worker_termination_operation_id=worker_termination_operation_id,
            expected_operation=termination_operation,
            worker_termination_execution_token=(
                worker_termination_execution_token
            ),
            worker_termination_state_version=worker_termination_state_version,
        )
        try:
            cleared += await dispatcher.abort_task_queue(task_id)
        except Exception as exc:
            from backend.services.dispatcher import TaskQueueAbortTimeoutError

            if isinstance(exc, TaskQueueAbortTimeoutError):
                raise HTTPException(
                    409,
                    "Task auxiliary producers were stopped, but the queue "
                    "worker could not be proven stopped",
                ) from exc
            raise

    # Settle a launch reservation before deciding whether an exact process
    # owner exists. A no-owner Task is safe to terminalize only after this
    # barrier proves no spawned-but-uncommitted generation can appear.
    await db.rollback()
    db.expire_all()
    probe = await db.get(Task, task_id)
    if probe is None or probe.worker_id is not None or probe.shared_from_id is not None:
        await db.rollback()
        raise HTTPException(
            409,
            "Task execution location changed while stopping its session",
        )
    probe_instance_id = probe.instance_id
    probe_is_active_plan = probe.mode == "plan" and probe.status in {
        "in_progress",
        "executing",
    }
    await db.rollback()
    await _settle_task_launch_barrier(task_id, probe_instance_id)
    if probe_is_active_plan:
        await _require_local_termination_effect_authority(
            task_id,
            db,
            worker_termination_operation_id=worker_termination_operation_id,
            expected_operation=termination_operation,
            worker_termination_execution_token=(
                worker_termination_execution_token
            ),
            worker_termination_state_version=worker_termination_state_version,
        )
        try:
            stopped = await dispatcher.stop_plan_agent_lifecycle(
                task_id,
                probe_instance_id,
            )
            if not stopped:
                await _require_local_termination_effect_authority(
                    task_id,
                    db,
                    worker_termination_operation_id=(
                        worker_termination_operation_id
                    ),
                    expected_operation=termination_operation,
                    worker_termination_execution_token=(
                        worker_termination_execution_token
                    ),
                    worker_termination_state_version=(
                        worker_termination_state_version
                    ),
                )
                stopped = await ralph_loop.stop_plan_agent_lifecycle(task_id)
            if not stopped:
                raise RuntimeError(f"No exact Plan lifecycle owns Task {task_id}")
        except Exception as exc:
            raise HTTPException(
                409,
                "Plan Agent process cleanup could not be confirmed",
            ) from exc

    (
        authority_task,
        active_receipt,
    ) = await _lock_local_termination_effect_authority(task_id, db)
    active_task = (
        await db.execute(
            select(Task)
            .where(
                Task.id == task_id,
                Task.worker_id.is_(None),
                Task.shared_from_id.is_(None),
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if active_task is None:
        await db.rollback()
        raise HTTPException(
            409,
            "Task execution location changed while stopping its session",
        )
    owner_rows = await db.execute(
        select(
            Instance.id,
            Instance.pid,
            Instance.started_at,
        )
        .where(Instance.current_task_id == task_id)
        .with_for_update()
    )
    expected_generations = list(owner_rows.all())
    mutation_lease_valid_at = datetime.utcnow()
    if not local_task_termination_effect_authority_matches(
        authority_task,
        active_receipt,
        operation_id=worker_termination_operation_id,
        operation=(
            termination_operation
            if worker_termination_operation_id is not None
            else None
        ),
        execution_token=worker_termination_execution_token,
        state_version=worker_termination_state_version,
        lease_valid_at=mutation_lease_valid_at,
    ):
        await db.rollback()
        raise HTTPException(
            409,
            "Task termination receipt lease expired while owner rows were "
            "being locked",
        )

    stoppable_statuses = {
        "executing",
        "in_progress",
        "waiting_capability",
        "failed",
        "completed",
        "cancelled",
        "conflict",
    }
    if worker_supersede:
        stoppable_statuses.add("merging")
    if active_task.status not in stoppable_statuses:
        if active_task.status == "pending" and cleared:
            queue_only = await db.execute(
                sa_update(Task)
                .where(
                    *_task_generation_fence(task_id, active_task),
                    Task.pty_background_generation.is_(None),
                    worker_task_termination_authority_predicate(
                        operation_id=worker_termination_operation_id,
                        operation=(
                            termination_operation
                            if worker_termination_operation_id is not None
                            else None
                        ),
                        execution_token=worker_termination_execution_token,
                        state_version=worker_termination_state_version,
                        lease_valid_at=mutation_lease_valid_at,
                    ),
                )
                .values(status=active_task.status)
            )
            if not queue_only.rowcount:
                await db.rollback()
                raise HTTPException(
                    409,
                    "Task generation changed while queued messages were being cleared",
                )
            await db.commit()
            return {
                "ok": True,
                "stopped": False,
                "cleared_messages": cleared,
                "task_status": active_task.status,
                "background_active": False,
            }
        await db.rollback()
        raise HTTPException(400, "No running session found for this task")

    observed_status = active_task.status
    observed_retry_count = active_task.retry_count
    observed_turn_generation = active_task.turn_generation
    observed_instance_id = active_task.instance_id
    observed_started_at = active_task.started_at
    observed_session_id = active_task.session_id
    observed_completed_at = active_task.completed_at
    observed_background_generation = active_task.pty_background_generation
    transitioning_statuses = {
        "executing",
        "in_progress",
        "waiting_capability",
    }
    if worker_supersede:
        transitioning_statuses.add("merging")
    if expected_generations:
        # InstanceManager owns PTY terminal arbitration. Stop first while the
        # Task is still active; it then writes Task+Instance+marker atomically.
        # Publishing a terminal Task before this call lets on_exit discard its
        # exact Session and is the race this ordering prevents.
        await db.commit()
        stopped = await _stop_task_process(
            task_id,
            db,
            expected_generations=expected_generations,
            expected_task_turn_generation=observed_turn_generation,
            task_status="completed",
            worker_termination_operation_id=(
                worker_termination_operation_id
            ),
            **(
                {"allow_delivery_effect_stop": True}
                if worker_supersede
                else {}
            ),
            **(
                {
                    "worker_termination_operation": termination_operation,
                    "worker_termination_execution_token": (
                        worker_termination_execution_token
                    ),
                    "worker_termination_state_version": (
                        worker_termination_state_version
                    ),
                }
                if worker_termination_operation_id is not None
                else {}
            ),
        )
        remaining_generations = await _remaining_task_process_generations(
            task_id,
            db,
            expected_generations=expected_generations,
        )
        if remaining_generations:
            await db.rollback()
            raise HTTPException(
                409,
                "Task process cleanup could not be confirmed for instance(s): "
                + ", ".join(map(str, remaining_generations)),
            )
        await db.rollback()
        (
            post_stop_authority_task,
            post_stop_receipt,
        ) = await _lock_local_termination_effect_authority(task_id, db)
        current = (
            await db.execute(
                select(Task)
                .where(
                    Task.id == task_id,
                )
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
        expected_status = (
            "completed"
            if observed_status in transitioning_statuses
            else observed_status
        )
        if (
            current is None
            or current.worker_id is not None
            or current.shared_from_id is not None
            or current.retry_count != observed_retry_count
            or current.turn_generation != observed_turn_generation
            or current.instance_id != observed_instance_id
            or current.started_at != observed_started_at
        ):
            await db.rollback()
            raise HTTPException(
                409,
                "Task generation changed while its session was stopping",
            )
        replacement_owner = await db.scalar(
            select(Instance.id)
            .where(Instance.current_task_id == task_id)
            .with_for_update()
        )
        post_stop_lease_valid_at = datetime.utcnow()
        if not local_task_termination_effect_authority_matches(
            post_stop_authority_task,
            post_stop_receipt,
            operation_id=worker_termination_operation_id,
            operation=(
                termination_operation
                if worker_termination_operation_id is not None
                else None
            ),
            execution_token=worker_termination_execution_token,
            state_version=worker_termination_state_version,
            lease_valid_at=post_stop_lease_valid_at,
        ):
            await db.rollback()
            raise HTTPException(
                409,
                "Task termination receipt lease expired while stopped owner "
                "rows were being locked",
            )
        if replacement_owner is not None:
            await db.rollback()
            raise HTTPException(
                409,
                "Task acquired a newer process owner while its previous "
                "session was stopping",
            )
        if not stopped or current.status != expected_status:
            await db.rollback()
            raise HTTPException(
                409,
                "Task owner did not atomically publish its stopped state",
            )

        background_cleared_by_api = False
        if current.pty_background_generation is not None and (
            observed_background_generation is None
            or current.pty_background_generation != observed_background_generation
        ):
            await db.rollback()
            raise HTTPException(
                409,
                "Task entered a newer PTY background generation while its "
                "previous session was stopping",
            )
        if current.pty_background_generation is not None:
            background_clear = await db.execute(
                sa_update(Task)
                .where(
                    *_task_generation_fence(task_id, current),
                    Task.pty_background_generation
                    == current.pty_background_generation,
                    worker_task_termination_authority_predicate(
                        operation_id=worker_termination_operation_id,
                        operation=(
                            termination_operation
                            if worker_termination_operation_id is not None
                            else None
                        ),
                        execution_token=worker_termination_execution_token,
                        state_version=worker_termination_state_version,
                        lease_valid_at=post_stop_lease_valid_at,
                    ),
                )
                .values(pty_background_generation=None)
                .execution_options(synchronize_session=False)
            )
            if background_clear.rowcount != 1:
                await db.rollback()
                raise HTTPException(
                    409,
                    "Task termination receipt changed before background cleanup",
                )
            background_cleared_by_api = True
        publication_retry_count = current.retry_count
        publication_turn_generation = current.turn_generation
        publication_instance_id = current.instance_id
        publication_started_at = current.started_at
        publication_completed_at = await _read_persisted_task_completed_at(task_id, db)
        await db.commit()

        if background_cleared_by_api:
            publication_task = await _lock_task_generation(
                task_id,
                db,
                expected_status=expected_status,
                expected_retry_count=publication_retry_count,
                expected_turn_generation=publication_turn_generation,
                expected_instance_id=publication_instance_id,
                expected_started_at=publication_started_at,
                expected_completed_at=publication_completed_at,
                expected_pty_background_generation=None,
                allow_worker_termination_operation_id=(
                    worker_termination_operation_id
                ),
                worker_termination_operation=(
                    termination_operation
                    if worker_termination_operation_id is not None
                    else None
                ),
                worker_termination_execution_token=(
                    worker_termination_execution_token
                ),
                worker_termination_state_version=(
                    worker_termination_state_version
                ),
            )
            if (
                publication_task is None
                or publication_task.pty_background_generation is not None
            ):
                await db.rollback()
                raise HTTPException(
                    409,
                    "Task started a newer generation while its stopped status "
                    "was being published",
                )
            from backend.services.task_events import broadcast_status_change

            await broadcast_status_change(
                task_id,
                expected_status,
                background_active=False,
            )
            await db.commit()
        return {
            "ok": True,
            "stopped": True,
            "cleared_messages": cleared,
            "task_status": expected_status,
            "background_active": False,
        }

    if observed_background_generation is not None:
        # A truly late autonomous turn has no Instance owner. Stop the exact
        # Task/session state; never address a historical reusable slot.
        if observed_status != "completed" or not observed_session_id:
            await db.rollback()
            raise HTTPException(
                409,
                "Task has PTY background output without a safe detached owner",
            )
        guarded = await db.execute(
            sa_update(Task)
            .where(
                *_task_generation_fence(task_id, active_task),
                worker_task_termination_authority_predicate(
                    operation_id=worker_termination_operation_id,
                    operation=(
                        termination_operation
                        if worker_termination_operation_id is not None
                        else None
                    ),
                    execution_token=worker_termination_execution_token,
                    state_version=worker_termination_state_version,
                    lease_valid_at=mutation_lease_valid_at,
                ),
            )
            .values(status=active_task.status)
        )
        if not guarded.rowcount:
            await db.rollback()
            raise HTTPException(
                409,
                "Task generation changed while detached output was stopping",
            )
        await db.commit()
        detached_stopped = (
            await instance_manager.stop_detached_pty_background_generation(
                task_id,
                observed_session_id,
                observed_background_generation,
                expected_status=observed_status,
                expected_retry_count=observed_retry_count,
                expected_turn_generation=observed_turn_generation,
                expected_instance_id=observed_instance_id,
                expected_started_at=observed_started_at,
                expected_completed_at=observed_completed_at,
                yield_to_worker_task_termination=(
                    worker_termination_operation_id is None
                ),
                worker_termination_operation_id=(
                    worker_termination_operation_id
                ),
                worker_termination_operation=(
                    termination_operation
                    if worker_termination_operation_id is not None
                    else None
                ),
                worker_termination_execution_token=(
                    worker_termination_execution_token
                ),
                worker_termination_state_version=(
                    worker_termination_state_version
                ),
            )
        )
        if not detached_stopped:
            raise HTTPException(
                409,
                "Detached Claude PTY background session could not be proven stopped",
            )
        publication_task = await _lock_task_generation(
            task_id,
            db,
            expected_status=observed_status,
            expected_retry_count=observed_retry_count,
            expected_turn_generation=observed_turn_generation,
            expected_instance_id=observed_instance_id,
            expected_started_at=observed_started_at,
            expected_completed_at=observed_completed_at,
            expected_pty_background_generation=None,
            allow_worker_termination_operation_id=(
                worker_termination_operation_id
            ),
            worker_termination_operation=(
                termination_operation
                if worker_termination_operation_id is not None
                else None
            ),
            worker_termination_execution_token=(
                worker_termination_execution_token
            ),
            worker_termination_state_version=worker_termination_state_version,
        )
        if (
            publication_task is None
            or publication_task.pty_background_generation is not None
        ):
            await db.rollback()
            raise HTTPException(
                409,
                "Task started a newer background generation while its "
                "detached session stop was being published",
            )
        from backend.services.task_events import broadcast_status_change

        await broadcast_status_change(
            task_id,
            observed_status,
            background_active=False,
        )
        await db.commit()
        return {
            "ok": True,
            "stopped": True,
            "cleared_messages": cleared,
            "task_status": observed_status,
            "background_active": False,
        }

    transitioned = observed_status in transitioning_statuses
    if transitioned:
        completed_at = datetime.utcnow()
        completed = await db.execute(
            sa_update(Task)
            .where(
                *_task_generation_fence(task_id, active_task),
                Task.pty_background_generation.is_(None),
                worker_task_termination_authority_predicate(
                    operation_id=worker_termination_operation_id,
                    operation=(
                        termination_operation
                        if worker_termination_operation_id is not None
                        else None
                    ),
                    execution_token=worker_termination_execution_token,
                    state_version=worker_termination_state_version,
                    lease_valid_at=mutation_lease_valid_at,
                ),
            )
            .values(status="completed", completed_at=completed_at)
        )
        if not completed.rowcount:
            await db.rollback()
            raise HTTPException(
                409,
                "Task generation changed while stopping its session",
            )
        publication_completed_at = await _read_persisted_task_completed_at(task_id, db)
        await db.commit()
        publication_task = await _lock_task_generation(
            task_id,
            db,
            expected_status="completed",
            expected_retry_count=observed_retry_count,
            expected_turn_generation=observed_turn_generation,
            expected_instance_id=observed_instance_id,
            expected_started_at=observed_started_at,
            expected_completed_at=publication_completed_at,
            expected_pty_background_generation=None,
            allow_worker_termination_operation_id=(
                worker_termination_operation_id
            ),
            worker_termination_operation=(
                termination_operation
                if worker_termination_operation_id is not None
                else None
            ),
            worker_termination_execution_token=(
                worker_termination_execution_token
            ),
            worker_termination_state_version=worker_termination_state_version,
        )
        if (
            publication_task is None
            or publication_task.pty_background_generation is not None
        ):
            await db.rollback()
            raise HTTPException(
                409,
                "Task started a newer generation while its stopped status "
                "was being published",
            )
        from backend.services.task_events import broadcast_status_change

        await broadcast_status_change(
            task_id,
            "completed",
            background_active=False,
        )
        await db.commit()
        return {
            "ok": True,
            "stopped": False,
            "cleared_messages": cleared,
            "note": "No running process found, task marked as completed",
            "task_status": "completed",
            "background_active": False,
        }

    guarded = await db.execute(
        sa_update(Task)
        .where(
            *_task_generation_fence(task_id, active_task),
            worker_task_termination_authority_predicate(
                operation_id=worker_termination_operation_id,
                operation=(
                    termination_operation
                    if worker_termination_operation_id is not None
                    else None
                ),
                execution_token=worker_termination_execution_token,
                state_version=worker_termination_state_version,
                lease_valid_at=mutation_lease_valid_at,
            ),
        )
        .values(status=active_task.status)
    )
    if not guarded.rowcount:
        await db.rollback()
        raise HTTPException(
            409,
            "Task generation changed while stopping its session",
        )
    await db.commit()
    if cleared:
        return {
            "ok": True,
            "stopped": False,
            "cleared_messages": cleared,
            "task_status": active_task.status,
            "background_active": False,
        }
    raise HTTPException(400, "No running session found for this task")


async def _fresh_worker_terminal_task(
    task_id: int,
    request: Request,
    db: AsyncSession,
    *,
    action: str,
) -> tuple[Task, WorkerTaskGeneration]:
    """Re-authorize and observe one Worker Task inside its operation fence."""

    db.expire_all()
    current = await db.get(Task, task_id)
    if current is None:
        raise HTTPException(404, "Task not found")
    current = await lock_task_effect_access(
        request,
        current,
        db,
        allow_chat_share=False,
    )
    _require_not_delivery_owned_task(current, action=action)
    await _require_no_pr_review_publication(db, task_id)
    await _require_not_pr_review_task_mutation(db, task_id, action=action)
    if current.worker_id is None or current.shared_from_id is not None:
        raise HTTPException(
            409,
            "Task execution location changed before Worker termination",
        )
    if has_worker_execution_quarantine(current.metadata_):
        raise HTTPException(
            409,
            "Task Worker execution is quarantined pending explicit "
            "authoritative reconciliation",
        )
    observed = worker_task_generation(current)
    if observed is None:
        raise HTTPException(409, "Task Worker assignment changed")
    return current, observed


async def _worker_terminal_request_impl(
    task_id: int,
    request: Request,
    db: AsyncSession,
    *,
    operation: str,
    force_readback: bool,
) -> tuple[object, Task]:
    """Run the durable query-before-write protocol under one operation lock."""

    await db.rollback()
    async with get_task_operation_lock(task_id):
        current, observed = await _fresh_worker_terminal_task(
            task_id,
            request,
            db,
            action="cancelled" if operation == "cancel" else "stopped",
        )
        # ``operation_id`` and the immutable request digest are committed here,
        # before even the first remote GET.  A repeated public request resumes
        # this active receipt instead of allocating a new mutation identity.
        try:
            receipt = await create_or_resume_manager_receipt(
                db,
                current,
                operation=(
                    "stop_session" if operation == "stop-session" else operation
                ),
            )
        except DurableWorkerTerminationConflict as exc:
            raise HTTPException(409, str(exc)) from exc
        receipt_operation_id = receipt.operation_id

        try:
            outcome = await reconcile_manager_receipt(
                db,
                receipt_operation_id,
                proxy_request=_proxy,
            )
        except WorkerTaskTerminationPending as exc:
            # Once the terminal Worker result is committed on the Manager the
            # public operation succeeded even if the final ACK response was
            # lost.  Keep the active receipt for background ACK recovery while
            # returning the authoritative result to the caller.
            await db.rollback()
            db.expire_all()
            from backend.models.worker_task_termination import (
                WorkerTaskTerminationReceipt,
            )

            durable = await db.get(
                WorkerTaskTerminationReceipt, receipt_operation_id
            )
            if (
                durable is None
                or durable.status != "awaiting_ack"
                or not isinstance(durable.result_payload, dict)
            ):
                raise HTTPException(
                    503,
                    "Worker termination is durably pending reconciliation "
                    f"({receipt_operation_id})",
                ) from exc
            outcome_payload = durable.result_payload
            if outcome_payload.get("rejected") is True:
                raise HTTPException(
                    409,
                    str(
                        outcome_payload.get("error")
                        or "Worker rejected termination before side effects"
                    ),
                ) from exc
        except DurableWorkerTerminationConflict as exc:
            raise HTTPException(409, str(exc)) from exc
        else:
            if not isinstance(outcome.result_payload, dict):
                raise HTTPException(
                    503,
                    "Worker termination result is not yet available "
                    f"({receipt_operation_id})",
                )
            outcome_payload = outcome.result_payload

        db.expire_all()
        mirrored = await db.get(Task, task_id)
        if mirrored is None or mirrored.worker_id != observed.worker_id:
            raise HTTPException(
                409,
                "Task Worker assignment changed after termination",
            )
        response = outcome_payload.get("response")
        if not isinstance(response, dict):
            response = {"ok": True, "operation_id": receipt_operation_id}
        return response, mirrored


async def _stop_worker_task_for_destroy_impl(
    task_id: int,
    destroy_claim: WorkerDestroyLifecycleClaim,
    worker_proxy,
    db: AsyncSession,
) -> tuple[object, Task]:
    """Stop one active Task under an opaque exact Worker destroy claim."""

    if worker_proxy is None:
        raise HTTPException(503, "Worker 功能未启用")
    await db.rollback()
    async with get_task_operation_lock(task_id):
        # Validate the claim before reading Task authority, then again inside
        # every network request.  No public proxy path accepts ``destroying``.
        await worker_proxy._require_destroy_lifecycle_claim(destroy_claim)
        db.expire_all()
        current = await db.get(Task, task_id)
        if current is None:
            raise HTTPException(404, "Task not found")
        if (
            current.worker_id != destroy_claim.worker_id
            or current.shared_from_id is not None
        ):
            raise HTTPException(
                409,
                "Task moved away from the claimed destroying Worker",
            )
        if has_worker_execution_quarantine(current.metadata_):
            raise HTTPException(
                409,
                "Task Worker execution is quarantined pending explicit "
                "authoritative reconciliation",
            )
        active_termination = await active_worker_task_termination_receipt(
            db, task_id
        )
        if active_termination is not None and (
            active_termination.side != "manager"
            or active_termination.worker_id != destroy_claim.worker_id
            or active_termination.operation != "stop_session"
        ):
            raise HTTPException(
                409,
                "Task has a different active Worker termination receipt",
            )
        observed = worker_task_generation(
            current,
            expected_worker_id=destroy_claim.worker_id,
        )
        if observed is None:
            raise HTTPException(409, "Task Worker assignment changed")

        async def claimed_proxy(task, method, path, body=None, **options):
            return await worker_proxy._proxy_to_claimed_destroying_worker(
                task,
                method,
                path,
                body,
                destroy_claim=destroy_claim,
                **options,
            )
        try:
            receipt = await create_or_resume_manager_receipt(
                db,
                current,
                operation="stop_session",
                destroy_claim=destroy_claim,
            )
            receipt_operation_id = receipt.operation_id
            outcome = await reconcile_manager_receipt(
                db,
                receipt_operation_id,
                proxy_request=claimed_proxy,
            )
        except WorkerTaskTerminationPending as exc:
            await db.rollback()
            from backend.models.worker_task_termination import (
                WorkerTaskTerminationReceipt,
            )

            durable = await db.get(
                WorkerTaskTerminationReceipt, receipt_operation_id
            )
            if (
                durable is None
                or durable.status != "awaiting_ack"
                or not isinstance(durable.result_payload, dict)
            ):
                # The destroy lifecycle remains fail-closed.  A restart cannot
                # recreate its opaque destroying claim, but the active receipt
                # prevents lossy detach/migration and an operator retry resumes
                # the same operation id after the Worker is made reachable.
                raise HTTPException(
                    503,
                    "Destroy stop is durably pending termination receipt "
                    f"{receipt_operation_id}",
                ) from exc
            result_payload = durable.result_payload
            if result_payload.get("rejected") is True:
                raise HTTPException(
                    409,
                    str(
                        result_payload.get("error")
                        or "Destroy stop was rejected before side effects"
                    ),
                ) from exc
        except DurableWorkerTerminationConflict as exc:
            raise HTTPException(409, str(exc)) from exc
        else:
            if not isinstance(outcome.result_payload, dict):
                raise HTTPException(503, "Destroy termination result is pending")
            result_payload = outcome.result_payload

        db.expire_all()
        mirrored = await db.get(Task, task_id)
        if mirrored is None or mirrored.worker_id != destroy_claim.worker_id:
            raise HTTPException(
                409,
                "Task moved away from the claimed destroying Worker",
            )
        response = result_payload.get("response")
        return (
            response if isinstance(response, dict) else {"ok": True},
            mirrored,
        )


async def _stop_worker_task_for_destroy(
    task_id: int,
    destroy_claim: WorkerDestroyLifecycleClaim,
    worker_proxy,
    db: AsyncSession,
) -> tuple[object, Task]:
    """Cancellation-safe internal entrypoint for Worker destroy coordination."""

    return await _finish_task_operation(
        _stop_worker_task_for_destroy_impl(
            task_id,
            destroy_claim,
            worker_proxy,
            db,
        )
    )


@router.post("/{task_id}/stop-session")
async def stop_task_session(
    task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Stop the running Claude Code session for a task.

    Abort queued work, settle any launch reservation, and snapshot the exact
    reverse Instance owner. A proven owner is stopped before its terminal Task
    state is published; an ownerless generation is terminalized only after the
    launch barrier proves that no spawned-but-uncommitted process can appear.
    """

    task = await db.get(Task, task_id)
    if task:
        await require_task_control(request, task, db)
        _require_not_delivery_owned_task(task, action="stopped")
        await _require_not_isolated_browser_child(db, task, action="stopped")
        await _require_no_pr_review_publication(db, task_id)
        await _require_not_pr_review_task_mutation(
            db,
            task_id,
            action="stopped",
        )
    wt = await _worker_task_or_none(db, task_id)
    if wt is not None:
        result, _mirrored = await _finish_task_operation(
            _worker_terminal_request_impl(
                task_id,
                request,
                db,
                operation="stop-session",
                force_readback=True,
            )
        )
        return result

    # The public local path, unlike the receipt executor, must acquire the
    # shared mutation lock and re-read authority immediately before entering
    # its long queue/process cleanup. A Worker receipt that wins this lock is
    # then the only caller allowed to invoke the local core directly.
    await db.rollback()
    async with get_task_operation_lock(task_id):
        db.expire_all()
        current = await db.get(Task, task_id)
        if current is None:
            raise HTTPException(404, "Task not found")
        current = await lock_task_effect_access(
            request,
            current,
            db,
            allow_chat_share=False,
        )
        _require_not_delivery_owned_task(current, action="stopped")
        await _require_not_isolated_browser_child(
            db,
            current,
            action="stopped",
        )
        await _require_no_pr_review_publication(db, task_id)
        await _require_not_pr_review_task_mutation(
            db,
            task_id,
            action="stopped",
        )
        if current.worker_id is not None or current.shared_from_id is not None:
            raise HTTPException(
                409,
                "Task execution location changed while stopping its session",
            )
        if await active_worker_task_termination_receipt(db, task_id):
            raise HTTPException(
                409,
                "Task has an active Worker termination receipt",
            )
        expected_harness_owner = await _commit_task_control_effect_gate(
            request,
            db,
            current,
            effect="stop_session",
        )
        try:
            result = await _finish_task_operation(
                _stop_task_session_local_impl(
                    task_id,
                    db,
                    expected_identity=expected_harness_owner,
                    task_control_effect="stop_session",
                )
            )
        except HTTPException as exc:
            if (
                exc.status_code == 400
                and exc.detail == "No running session found for this task"
            ):
                await _settle_task_control_effect_gate(
                    db,
                    expected_harness_owner,
                    effect="stop_session",
                )
            raise
        await _settle_task_control_effect_gate(
            db,
            expected_harness_owner,
            effect="stop_session",
        )
        return result


async def _cancel_local_task_impl(
    task_id: int,
    db: AsyncSession,
    *,
    expected_identity: TestHarnessOwnerIdentity,
    worker_termination_operation_id: str | None = None,
    worker_termination_execution_token: str | None = None,
    worker_termination_state_version: int | None = None,
    task_control_effect: str | None = None,
) -> Task:
    """Keep message admission closed until cancellation is authoritative."""

    from backend.main import dispatcher
    from backend.services.test_harness import test_harness_service

    async with test_harness_service.owner_stop_fence(
        task_id,
        reason="Owner Task was cancelled",
        expected_identity=expected_identity,
        locked_owner_validator=(
            _task_control_effect_gate_validator(
                expected_identity,
                task_control_effect,
            )
            if task_control_effect is not None
            else None
        ),
    ):
        async with dispatcher.task_queue_cancellation_lease(task_id):
            return await _cancel_local_task_under_cancellation_lease(
                task_id,
                db,
                worker_termination_operation_id=(
                    worker_termination_operation_id
                ),
                worker_termination_execution_token=(
                    worker_termination_execution_token
                ),
                worker_termination_state_version=worker_termination_state_version,
            )


async def _cancel_local_task_under_cancellation_lease(
    task_id: int,
    db: AsyncSession,
    *,
    worker_termination_operation_id: str | None = None,
    worker_termination_execution_token: str | None = None,
    worker_termination_state_version: int | None = None,
) -> Task:
    """Cancellation-safe local core for ``POST /cancel``."""

    from backend.main import dispatcher, ralph_loop

    await _require_local_termination_effect_authority(
        task_id,
        db,
        worker_termination_operation_id=worker_termination_operation_id,
        expected_operation="cancel",
        worker_termination_execution_token=(
            worker_termination_execution_token
        ),
        worker_termination_state_version=worker_termination_state_version,
    )
    await _cancel_waiting_task_capability_before_queue_abort(
        task_id,
        db,
    )
    await _require_local_termination_effect_authority(
        task_id,
        db,
        worker_termination_operation_id=worker_termination_operation_id,
        expected_operation="cancel",
        worker_termination_execution_token=(
            worker_termination_execution_token
        ),
        worker_termination_state_version=worker_termination_state_version,
    )
    await db.rollback()
    try:
        await dispatcher.abort_task_queue(
            task_id,
            cancel_durable=False,
            durable_db=db,
        )
    except Exception as exc:
        from backend.services.dispatcher import TaskQueueAbortTimeoutError

        if isinstance(exc, TaskQueueAbortTimeoutError):
            raise HTTPException(
                409,
                "Task queue worker could not be proven stopped; cancellation "
                "was not published",
            ) from exc
        raise

    await _require_local_termination_effect_authority(
        task_id,
        db,
        worker_termination_operation_id=worker_termination_operation_id,
        expected_operation="cancel",
        worker_termination_execution_token=worker_termination_execution_token,
        worker_termination_state_version=worker_termination_state_version,
    )

    # Close the spawn-without-owner window before choosing the running-owner
    # path or the ownerless terminal CAS path.
    await db.rollback()
    db.expire_all()
    probe = await db.get(Task, task_id)
    if probe is None or probe.worker_id is not None or probe.shared_from_id is not None:
        await db.rollback()
        raise HTTPException(
            409,
            "Task execution location changed while cancellation was starting",
        )
    probe_instance_id = probe.instance_id
    probe_is_active_plan = probe.mode == "plan" and probe.status in {
        "in_progress",
        "executing",
    }
    await db.rollback()
    await _settle_task_launch_barrier(task_id, probe_instance_id)
    if probe_is_active_plan:
        await _require_local_termination_effect_authority(
            task_id,
            db,
            worker_termination_operation_id=worker_termination_operation_id,
            expected_operation="cancel",
            worker_termination_execution_token=(
                worker_termination_execution_token
            ),
            worker_termination_state_version=worker_termination_state_version,
        )
        stopped = await dispatcher.stop_plan_agent_lifecycle(
            task_id,
            probe_instance_id,
        )
        if not stopped:
            await _require_local_termination_effect_authority(
                task_id,
                db,
                worker_termination_operation_id=(
                    worker_termination_operation_id
                ),
                expected_operation="cancel",
                worker_termination_execution_token=(
                    worker_termination_execution_token
                ),
                worker_termination_state_version=(
                    worker_termination_state_version
                ),
            )
            stopped = await ralph_loop.stop_plan_agent_lifecycle(task_id)
        if not stopped:
            raise HTTPException(
                409,
                "Plan Agent process cleanup could not be confirmed",
            )

    (
        authority_task,
        active_receipt,
    ) = await _lock_local_termination_effect_authority(task_id, db)
    active_task = (
        await db.execute(
            select(Task)
            .where(
                Task.id == task_id,
                Task.worker_id.is_(None),
                Task.shared_from_id.is_(None),
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    active_statuses = (
        "pending_activation",
        "pending",
        "in_progress",
        "executing",
        "merging",
        "waiting_capability",
    )
    if active_task is None or active_task.status not in (
        *active_statuses,
        "cancelled",
    ):
        await db.rollback()
        raise HTTPException(400, "Cannot cancel task")

    observed_retry_count = active_task.retry_count
    observed_turn_generation = active_task.turn_generation
    observed_instance_id = active_task.instance_id
    observed_started_at = active_task.started_at
    observed_background_generation = active_task.pty_background_generation
    owner_rows = await db.execute(
        select(
            Instance.id,
            Instance.pid,
            Instance.started_at,
        )
        .where(Instance.current_task_id == task_id)
        .with_for_update()
    )
    expected_generations = list(owner_rows.all())
    mutation_lease_valid_at = datetime.utcnow()
    if not local_task_termination_effect_authority_matches(
        authority_task,
        active_receipt,
        operation_id=worker_termination_operation_id,
        operation=(
            "cancel" if worker_termination_operation_id is not None else None
        ),
        execution_token=worker_termination_execution_token,
        state_version=worker_termination_state_version,
        lease_valid_at=mutation_lease_valid_at,
    ):
        await db.rollback()
        raise HTTPException(
            409,
            "Task cancellation receipt lease expired while owner rows were "
            "being locked",
        )

    transitioned_by_api = False
    background_cleared_by_api = False
    if expected_generations:
        # Stop while the Task is still active. InstanceManager claims PTY
        # terminal ownership and commits Task+Instance+marker atomically.
        await db.commit()
        stopped = await _stop_task_process(
            task_id,
            db,
            expected_generations=expected_generations,
            expected_task_turn_generation=observed_turn_generation,
            task_status="cancelled",
            worker_termination_operation_id=(
                worker_termination_operation_id
            ),
            **(
                {
                    "worker_termination_operation": "cancel",
                    "worker_termination_execution_token": (
                        worker_termination_execution_token
                    ),
                    "worker_termination_state_version": (
                        worker_termination_state_version
                    ),
                }
                if worker_termination_operation_id is not None
                else {}
            ),
        )
        remaining_generations = await _remaining_task_process_generations(
            task_id,
            db,
            expected_generations=expected_generations,
        )
        if remaining_generations:
            await db.rollback()
            raise HTTPException(
                409,
                "Task process cleanup could not be confirmed for instance(s): "
                + ", ".join(map(str, remaining_generations)),
            )
        await db.rollback()
        (
            authority_task,
            active_receipt,
        ) = await _lock_local_termination_effect_authority(task_id, db)
        active_task = (
            await db.execute(
                select(Task)
                .where(
                    Task.id == task_id,
                )
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
        if (
            active_task is None
            or active_task.worker_id is not None
            or active_task.shared_from_id is not None
            or active_task.retry_count != observed_retry_count
            or active_task.turn_generation != observed_turn_generation
            or active_task.instance_id != observed_instance_id
            or active_task.started_at != observed_started_at
        ):
            await db.rollback()
            raise HTTPException(
                409,
                "Task generation changed while cancellation was stopping it",
            )
        replacement_owner = await db.scalar(
            select(Instance.id)
            .where(Instance.current_task_id == task_id)
            .with_for_update()
        )
        mutation_lease_valid_at = datetime.utcnow()
        if not local_task_termination_effect_authority_matches(
            authority_task,
            active_receipt,
            operation_id=worker_termination_operation_id,
            operation=(
                "cancel"
                if worker_termination_operation_id is not None
                else None
            ),
            execution_token=worker_termination_execution_token,
            state_version=worker_termination_state_version,
            lease_valid_at=mutation_lease_valid_at,
        ):
            await db.rollback()
            raise HTTPException(
                409,
                "Task cancellation receipt lease expired while stopped owner "
                "rows were being locked",
            )
        if replacement_owner is not None:
            await db.rollback()
            raise HTTPException(
                409,
                "Task acquired a newer process owner while cancellation was "
                "stopping its previous generation",
            )
        if not stopped or active_task.status != "cancelled":
            await db.rollback()
            raise HTTPException(
                409,
                "Task owner did not atomically publish its cancelled state",
            )
        if active_task.pty_background_generation is not None and (
            observed_background_generation is None
            or active_task.pty_background_generation != observed_background_generation
        ):
            await db.rollback()
            raise HTTPException(
                409,
                "Task entered a newer PTY background generation while "
                "cancellation was stopping its previous generation",
            )
        if active_task.pty_background_generation is not None:
            background_clear = await db.execute(
                sa_update(Task)
                .where(
                    *_task_generation_fence(task_id, active_task),
                    Task.pty_background_generation
                    == active_task.pty_background_generation,
                    worker_task_termination_authority_predicate(
                        operation_id=worker_termination_operation_id,
                        operation=(
                            "cancel"
                            if worker_termination_operation_id is not None
                            else None
                        ),
                        execution_token=worker_termination_execution_token,
                        state_version=worker_termination_state_version,
                        lease_valid_at=mutation_lease_valid_at,
                    ),
                )
                .values(pty_background_generation=None)
                .execution_options(synchronize_session=False)
            )
            if background_clear.rowcount != 1:
                await db.rollback()
                raise HTTPException(
                    409,
                    "Task cancellation receipt changed before background cleanup",
                )
            background_cleared_by_api = True
    else:
        if active_task.pty_background_generation is not None:
            await db.rollback()
            raise HTTPException(
                409,
                "Task still has active detached PTY output; use stop-session",
            )
        transitioned_by_api = active_task.status in active_statuses
        cancelled_values = (
            {
                "status": "cancelled",
                "completed_at": datetime.utcnow(),
            }
            if transitioned_by_api
            else {"status": "cancelled"}
        )
        cancelled = await db.execute(
            sa_update(Task)
            .where(
                *_task_generation_fence(task_id, active_task),
                Task.pty_background_generation.is_(None),
                worker_task_termination_authority_predicate(
                    operation_id=worker_termination_operation_id,
                    operation=(
                        "cancel"
                        if worker_termination_operation_id is not None
                        else None
                    ),
                    execution_token=worker_termination_execution_token,
                    state_version=worker_termination_state_version,
                    lease_valid_at=mutation_lease_valid_at,
                ),
            )
            .values(**cancelled_values)
        )
        if not cancelled.rowcount:
            await db.rollback()
            raise HTTPException(
                409,
                "Task generation changed while cancellation was starting",
            )
        active_task = await db.get(Task, task_id, populate_existing=True)

    from backend.models.monitor_session import MonitorSession

    committed_retry_count = active_task.retry_count
    committed_turn_generation = active_task.turn_generation
    committed_instance_id = active_task.instance_id
    committed_started_at = active_task.started_at
    committed_completed_at = await _read_persisted_task_completed_at(task_id, db)
    monitor_rows = await db.execute(
        select(
            MonitorSession.id,
            MonitorSession.agent_type,
            MonitorSession.source,
        )
        .where(
            MonitorSession.task_id == task_id,
            MonitorSession.status.in_(("running", "cancelled")),
        )
        .with_for_update()
    )
    auxiliary_sessions = list(monitor_rows.all())
    auxiliary_lease_valid_at = datetime.utcnow()
    if not local_task_termination_effect_authority_matches(
        active_task,
        active_receipt,
        operation_id=worker_termination_operation_id,
        operation=(
            "cancel" if worker_termination_operation_id is not None else None
        ),
        execution_token=worker_termination_execution_token,
        state_version=worker_termination_state_version,
        lease_valid_at=auxiliary_lease_valid_at,
    ):
        await db.rollback()
        raise HTTPException(
            409,
            "Task cancellation receipt lease expired while auxiliary rows "
            "were being locked",
        )
    await db.execute(
        sa_update(MonitorSession)
        .where(
            MonitorSession.task_id == task_id,
            MonitorSession.status == "running",
        )
        .values(
            status="cancelled",
            completed_at=datetime.utcnow(),
            next_check_at=None,
            active_turn_generation=None,
            turn_started_at=None,
        )
    )
    await db.commit()

    for session_id, agent_type, source in auxiliary_sessions:
        # Native agents are part of the main process tree. CCM-owned auxiliary
        # processes use their own exact registries and must be reaped explicitly.
        if source != "ccm":
            continue
        await _require_local_termination_effect_authority(
            task_id,
            db,
            worker_termination_operation_id=worker_termination_operation_id,
            expected_operation="cancel",
            worker_termination_execution_token=(
                worker_termination_execution_token
            ),
            worker_termination_state_version=worker_termination_state_version,
        )
        try:
            if agent_type == "sub_agent":
                await dispatcher.stop_sub_agent_session_process(session_id)
            elif agent_type == "monitor":
                await dispatcher.stop_monitor_session_process(
                    session_id,
                    terminal=True,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise HTTPException(
                409,
                "Task was cancelled, but auxiliary process cleanup could not "
                f"be confirmed for session {session_id}",
            ) from exc

    current_task = await _lock_task_generation(
        task_id,
        db,
        expected_status="cancelled",
        expected_retry_count=committed_retry_count,
        expected_turn_generation=committed_turn_generation,
        expected_instance_id=committed_instance_id,
        expected_started_at=committed_started_at,
        expected_completed_at=committed_completed_at,
        expected_pty_background_generation=None,
        allow_worker_termination_operation_id=(
            worker_termination_operation_id
        ),
        worker_termination_operation=(
            "cancel"
            if worker_termination_operation_id is not None
            else None
        ),
        worker_termination_execution_token=(
            worker_termination_execution_token
        ),
        worker_termination_state_version=worker_termination_state_version,
    )
    if current_task is None:
        raise HTTPException(
            409,
            "Task started a newer generation while cancellation was finishing",
        )

    if transitioned_by_api or background_cleared_by_api:
        from backend.services.task_events import broadcast_status_change

        await broadcast_status_change(
            task_id,
            "cancelled",
            background_active=False,
        )
    await db.commit()
    return current_task


@router.post("/{task_id}/cancel", response_model=TaskResponse)
async def cancel_task(
    task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    task = await db.get(Task, task_id)
    if task:
        await require_task_control(request, task, db)
        _require_not_delivery_owned_task(task, action="cancelled")
        await _require_not_isolated_browser_child(
            db,
            task,
            action="cancelled",
        )
        await _require_no_pr_review_publication(db, task_id)
        await _require_not_pr_review_task_mutation(
            db,
            task_id,
            action="cancelled",
        )
    wt = await _worker_task_or_none(db, task_id)
    if wt is not None:
        _result, mirrored = await _finish_task_operation(
            _worker_terminal_request_impl(
                task_id,
                request,
                db,
                operation="cancel",
                force_readback=False,
            )
        )
        return await task_response(request, mirrored, db)

    await db.rollback()
    async with get_task_operation_lock(task_id):
        db.expire_all()
        current = await db.get(Task, task_id)
        if current is None:
            raise HTTPException(404, "Task not found")
        current = await lock_task_effect_access(
            request,
            current,
            db,
            allow_chat_share=False,
        )
        _require_not_delivery_owned_task(current, action="cancelled")
        await _require_not_isolated_browser_child(
            db,
            current,
            action="cancelled",
        )
        await _require_no_pr_review_publication(db, task_id)
        await _require_not_pr_review_task_mutation(
            db,
            task_id,
            action="cancelled",
        )
        if current.worker_id is not None or current.shared_from_id is not None:
            raise HTTPException(
                409,
                "Task execution location changed while cancellation was starting",
            )
        if await active_worker_task_termination_receipt(db, task_id):
            raise HTTPException(
                409,
                "Task has an active Worker termination receipt",
            )
        expected_harness_owner = await _commit_task_control_effect_gate(
            request,
            db,
            current,
            effect="cancel",
        )
        try:
            await _finish_task_operation(
                _cancel_local_task_impl(
                    task_id,
                    db,
                    expected_identity=expected_harness_owner,
                    task_control_effect="cancel",
                )
            )
        except HTTPException as exc:
            if exc.status_code == 400 and exc.detail == "Cannot cancel task":
                await _settle_task_control_effect_gate(
                    db,
                    expected_harness_owner,
                    effect="cancel",
                )
            raise
        await _settle_task_control_effect_gate(
            db,
            expected_harness_owner,
            effect="cancel",
        )
        # ``_settle_task_control_effect_gate`` starts a fresh transaction with
        # rollback, which expires the ORM instance returned by the cancellation
        # core.  Load a complete response projection (including column_property
        # fields) inside the async context instead of letting Pydantic trigger
        # lazy IO after the endpoint returns.
        db.expire_all()
        settled_task = await db.scalar(
            select(Task)
            .where(Task.id == task_id)
            .execution_options(populate_existing=True)
        )
        if settled_task is None:
            raise HTTPException(409, "Task disappeared after cancellation")
        return await task_response(request, settled_task, db)


def _worker_manual_retry_response(task: Task, receipt: dict) -> dict:
    return {"task": task, "receipt": receipt}


@router.get(
    "/{task_id}/internal/worker-retry-receipts/{operation_id}",
    response_model=WorkerManualRetryResponse,
    include_in_schema=False,
)
async def get_worker_manual_retry_receipt(
    task_id: int,
    operation_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Return only the exact durable receipt stored on a Worker Task row."""

    require_internal_service(request)
    if (
        len(operation_id) != 32
        or any(char not in "0123456789abcdef" for char in operation_id)
    ):
        raise HTTPException(404, "Worker manual retry receipt not found")
    task = await require_worker_task_incarnation_header(
        request,
        task_id,
        db,
    )
    receipt = worker_manual_retry_receipt(task.metadata_ if task else None)
    if (
        task is None
        or receipt is None
        or receipt.get("side") != "worker"
        or receipt.get("operation_id") != operation_id
    ):
        raise HTTPException(404, "Worker manual retry receipt not found")
    return _worker_manual_retry_response(task, receipt)


@router.post(
    "/{task_id}/internal/worker-retry",
    response_model=WorkerManualRetryResponse,
    include_in_schema=False,
)
async def retry_worker_task_internal(
    task_id: int,
    body: WorkerManualRetryRequest,
    request: Request,
    queue: TaskQueue = Depends(_get_queue),
    db: AsyncSession = Depends(get_db),
):
    """Atomically commit one idempotent Manager-delegated Worker retry."""

    require_internal_service(request)
    if body.protocol_version != WORKER_MANUAL_RETRY_PROTOCOL or body.task_id != task_id:
        raise HTTPException(409, "Worker manual retry identity changed")
    payload = body.model_dump(mode="json")
    if worker_manual_retry_request_digest(payload) != body.request_digest:
        raise HTTPException(409, "Worker manual retry request digest mismatch")
    target_principal = canonical_delegated_principal_payload(payload)
    if (
        target_principal is None
        or target_principal
        != {
            field: payload[field]
            for field in _TASK_EXECUTION_PRINCIPAL_FIELDS
        }
        or worker_principal_digest(target_principal)
        != body.target_principal_digest
    ):
        raise HTTPException(409, "Worker manual retry target principal is invalid")

    await db.rollback()
    async with get_task_operation_lock(task_id):
        db.expire_all()
        task = await require_worker_task_incarnation_header(
            request,
            task_id,
            db,
            write_fence=True,
        )
        if task is None:
            raise HTTPException(404, "Task not found")
        existing = worker_manual_retry_receipt(task.metadata_)
        if existing is not None and existing.get("operation_id") == body.operation_id:
            if (
                existing.get("request_digest") != body.request_digest
                or existing.get("side") != "worker"
                or existing.get("state") != "committed"
            ):
                raise HTTPException(
                    409,
                    "Worker manual retry operation id was reused with another request",
                )
            return _worker_manual_retry_response(task, existing)

        source_principal = canonical_delegated_principal_payload(task)
        source_principal_digest = worker_principal_digest(task)
        source_generation = {
            "task_id": task.id,
            "worker_id": body.worker_id,
            "incarnation_id": task.incarnation_id,
            "status": task.status,
            "retry_count": task.retry_count,
            "turn_generation": task.turn_generation,
            "principal_digest": source_principal_digest,
        }
        expected_source = {
            "task_id": task_id,
            "worker_id": body.worker_id,
            "incarnation_id": body.source_incarnation_id,
            "status": body.expected_status,
            "retry_count": body.expected_retry_count,
            "turn_generation": body.expected_turn_generation,
            "principal_digest": body.source_principal_digest,
        }
        if (
            task.incarnation_id is None
            or source_principal is None
            or source_generation != expected_source
            or source_principal_digest != body.source_principal_digest
        ):
            raise HTTPException(
                409,
                "Worker Task incarnation, generation, or source principal changed",
            )
        result_generation = {
            "status": "pending",
            "retry_count": task.retry_count + 1,
            "turn_generation": task.turn_generation,
        }
        receipt = {
            "version": WORKER_MANUAL_RETRY_PROTOCOL,
            "side": "worker",
            "state": "committed",
            "operation_id": body.operation_id,
            "request_digest": body.request_digest,
            "source_generation": source_generation,
            "result_generation": result_generation,
            "source_principal_digest": body.source_principal_digest,
            "target_principal_digest": body.target_principal_digest,
            "target_principal": target_principal,
            "committed_at": datetime.utcnow().isoformat(timespec="microseconds"),
        }
        metadata = dict(task.metadata_ or {})
        metadata[WORKER_MANUAL_RETRY_RECEIPT_METADATA_KEY] = receipt
        metadata[WORKER_REMOTE_MATERIALIZED_METADATA_KEY] = True
        expected_harness_owner = test_harness_owner_identity(task)
        exact_source_principal = {
            "execution_user_id": task.execution_user_id,
            "execution_user_role": task.execution_user_role,
            "execution_mode": task.execution_mode,
            "execution_principal_kind": task.execution_principal_kind,
        }
        retried = await _retry_local_task_safely(
            task_id,
            queue,
            db,
            expected_identity=expected_harness_owner,
            task_updates={
                **target_principal,
                "metadata_": metadata,
            },
            expected_incarnation_id=body.source_incarnation_id,
            expected_principal=exact_source_principal,
        )
        if retried is None:
            raise HTTPException(409, "Worker Task changed while retrying")
        persisted = worker_manual_retry_receipt(retried.metadata_)
        if persisted != receipt:
            raise HTTPException(409, "Worker manual retry receipt was not committed")
        from backend.services.task_events import broadcast_status_change

        await broadcast_status_change(task_id, "pending")
        return _worker_manual_retry_response(retried, receipt)


async def _reconcile_prepared_worker_manual_retry(
    db: AsyncSession,
    task: Task,
) -> Task | None:
    """Read back and adopt one exact prepared retry after ACK loss/restart."""

    marker = worker_manual_retry_receipt(task.metadata_)
    observed = worker_task_generation(task)
    if (
        marker is None
        or observed is None
        or not worker_manual_retry_is_prepared(task.metadata_)
        or marker.get("worker_id") != observed.worker_id
        or not isinstance(marker.get("request"), dict)
        or marker.get("request_digest")
        != worker_manual_retry_request_digest(marker["request"])
        or marker.get("source_generation")
        != worker_manual_retry_source_generation(task, observed)
    ):
        return None
    operation_id = marker.get("operation_id")
    if not isinstance(operation_id, str) or len(operation_id) != 32:
        return None
    from backend.main import worker_proxy

    if worker_proxy is None:
        raise HTTPException(503, "Worker 功能未启用")
    worker = await worker_proxy.require_ready_worker(observed.worker_id)
    await worker_proxy.require_worker_delegated_principal_support(worker)
    await worker_proxy.require_worker_manual_retry_support(worker)
    try:
        result = await worker_proxy.proxy_to_worker(
            task,
            "GET",
            (
                f"/api/tasks/{task.id}/internal/"
                f"worker-retry-receipts/{operation_id}"
            ),
            require_json=True,
            operation_lock_held=True,
            surface_endpoint_not_found=True,
            require_task_incarnation_fence=True,
        )
    except WorkerEndpointNotFoundError as exc:
        raise HTTPException(
            409,
            "Worker retry is still quarantined; no exact receipt is available",
        ) from exc
    adopted = await apply_authoritative_worker_retry(
        db,
        observed,
        result,
        operation_id=operation_id,
        request_digest=marker["request_digest"],
        commit=False,
    )
    if adopted is None:
        await db.rollback()
        return None
    await _publish_worker_status_transition(
        db,
        observed_status=observed.status,
        resulting=adopted,
    )
    db.expire_all()
    return await db.get(Task, task.id)


@router.post("/{task_id}/retry", response_model=TaskResponse)
async def retry_task(
    task_id: int,
    request: Request,
    body: TaskActionRequest | None = None,
    queue: TaskQueue = Depends(_get_queue),
    db: AsyncSession = Depends(get_db),
):
    task = await db.get(Task, task_id)
    if task:
        await require_task_control(request, task, db)
    # The operation lock is shared with TaskMigrator.  Keep it through the
    # remote response CAS/local retry commit and status publication, otherwise
    # migration can copy an old generation while retry is still in flight.
    await db.rollback()
    async with get_task_operation_lock(task_id):
        db.expire_all()
        current = await db.get(Task, task_id)
        if current is None:
            raise HTTPException(404, "Task not found")
        current = await lock_task_effect_access(
            request,
            current,
            db,
            allow_chat_share=False,
        )
        _require_not_delivery_owned_task(current, action="retried")
        _require_not_waiting_capability(current, action="retried")
        await _require_not_isolated_browser_child(
            db,
            current,
            action="retried",
        )
        if worker_manual_retry_is_prepared(current.metadata_):
            reconciled = await _reconcile_prepared_worker_manual_retry(db, current)
            if reconciled is not None:
                return await task_response(request, reconciled, db)
            raise HTTPException(
                409,
                "Prepared Worker retry receipt is invalid; execution remains quarantined",
            )
        if has_worker_execution_quarantine(current.metadata_):
            raise HTTPException(
                409,
                "Task Worker execution is quarantined pending explicit "
                "authoritative reconciliation",
            )
        if await active_worker_task_termination_receipt(db, task_id):
            raise HTTPException(
                409,
                "Task has an active Worker termination receipt",
            )
        if current.mode == "plan":
            raise HTTPException(
                409,
                "Plan Tasks cannot be retried; revise the Plan or create an "
                "execution Task instead",
            )
        _require_expected_task_routing(
            current,
            body.expected_routing if body is not None else None,
            effective_model=current.model,
        )
        if current.status not in _MANUAL_RETRYABLE_STATUSES:
            raise HTTPException(
                409,
                f"Task status {current.status} is not retryable",
            )
        await _require_no_pr_review_publication(db, task_id)
        await _require_pr_review_retryable(db, task_id)
        if current.pty_background_generation is not None:
            raise HTTPException(
                409,
                "Task still has active Claude PTY background output",
            )
        if current.worker_turn_handoff_id is not None:
            raise HTTPException(
                409,
                "Task has a pending Worker follow-up turn handoff",
            )

        if current.worker_id is not None:
            expected_harness_owner = test_harness_owner_identity(current)
            await db.rollback()
            from backend.services.test_harness import test_harness_service
            from backend.main import worker_proxy

            if worker_proxy is None:
                raise HTTPException(503, "Worker 功能未启用")

            async with test_harness_service.owner_stop_fence(
                task_id,
                reason="Owner Task was retried",
                expected_identity=expected_harness_owner,
            ):
                db.expire_all()
                current = await db.get(Task, task_id)
                if current is None or current.worker_id is None:
                    raise HTTPException(409, "Task Worker assignment changed")
                await _ensure_worker_routing_ready(
                    current,
                    operation_lock_held=True,
                )
                observed = worker_task_generation(current)
                if observed is None:
                    raise HTTPException(409, "Task Worker assignment changed")
                if not worker_remote_task_is_materialized(current.metadata_):
                    source_principal = {
                        "execution_user_id": current.execution_user_id,
                        "execution_user_role": current.execution_user_role,
                        "execution_mode": current.execution_mode,
                        "execution_principal_kind": current.execution_principal_kind,
                    }
                    retried = await _retry_local_task_under_harness_owner_fence(
                        task_id,
                        queue,
                        db,
                        task_updates=task_execution_principal_from_request(
                            request,
                            force_sandbox=is_pr_sandbox_task(current),
                        ),
                        expected_incarnation_id=current.incarnation_id,
                        expected_principal=source_principal,
                        effect_request=request,
                    )
                    if retried is None:
                        raise HTTPException(
                            409,
                            "Task generation changed while retrying initial Worker forwarding",
                        )
                    from backend.services.task_events import broadcast_status_change

                    await broadcast_status_change(task_id, retried.status)
                    return await task_response(request, retried, db)
                worker = await worker_proxy.require_ready_worker(observed.worker_id)
                # Both gates are read-only and must precede Skill synchronization,
                # Manager outbox persistence, and the remote retry POST.
                await worker_proxy.require_worker_delegated_principal_support(worker)
                await worker_proxy.require_worker_manual_retry_support(worker)
                await _sync_worker_skill_selection_before_execution(current)
                await db.rollback()
                db.expire_all()
                current = await db.get(Task, task_id)
                if (
                    current is None
                    or worker_task_generation(
                        current,
                        expected_worker_id=observed.worker_id,
                    )
                    != observed
                ):
                    raise HTTPException(
                        409,
                        "Task Worker generation changed while Skill selection was "
                        "being synchronized",
                    )
                current = await lock_task_effect_access(
                    request,
                    current,
                    db,
                    allow_chat_share=False,
                )
                if (
                    worker_task_generation(
                        current,
                        expected_worker_id=observed.worker_id,
                    )
                    != observed
                ):
                    raise HTTPException(
                        409,
                        "Task Worker generation changed before retry admission",
                    )
                source_generation = worker_manual_retry_source_generation(
                    current,
                    observed,
                )
                target_native = task_execution_principal_from_request(
                    request,
                    force_sandbox=is_pr_sandbox_task(current),
                )
                if target_native["execution_principal_kind"] == "user":
                    from backend.models.user import User

                    # Authentication froze request.state before this remote
                    # admission transaction began.  Lock and revalidate the
                    # durable User row before publishing the retry outbox so
                    # a concurrent disable/demotion cannot cross the Task
                    # writer fence with stale unrestricted authority.
                    principal_gate = await db.execute(
                        sa_update(User)
                        .where(
                            User.id == target_native["execution_user_id"],
                            User.is_active.is_(True),
                            User.role
                            == target_native["execution_user_role"],
                        )
                        .values(role=User.role)
                    )
                    if principal_gate.rowcount != 1:
                        await db.rollback()
                        raise HTTPException(
                            409,
                            "Worker retry principal is no longer active or "
                            "its role changed",
                        )
                target_principal = delegated_task_execution_principal_values(
                    user_id=target_native["execution_user_id"],
                    role=target_native["execution_user_role"],
                    principal_kind=target_native["execution_principal_kind"],
                )
                target_principal_digest = worker_principal_digest(target_principal)
                if source_generation is None or target_principal_digest is None:
                    raise HTTPException(
                        409,
                        "Task Worker source principal or incarnation is invalid",
                    )
                operation_id = uuid.uuid4().hex
                retry_request = {
                    "protocol_version": WORKER_MANUAL_RETRY_PROTOCOL,
                    "operation_id": operation_id,
                    "task_id": task_id,
                    "worker_id": observed.worker_id,
                    "source_incarnation_id": source_generation["incarnation_id"],
                    "expected_status": source_generation["status"],
                    "expected_retry_count": source_generation["retry_count"],
                    "expected_turn_generation": source_generation["turn_generation"],
                    "source_principal_digest": source_generation["principal_digest"],
                    "target_principal_digest": target_principal_digest,
                    **target_principal,
                }
                retry_request["request_digest"] = (
                    worker_manual_retry_request_digest(retry_request)
                )
                prepared_marker = {
                    "version": WORKER_MANUAL_RETRY_PROTOCOL,
                    "side": "manager",
                    "state": "prepared",
                    "operation_id": operation_id,
                    "request_digest": retry_request["request_digest"],
                    "worker_id": observed.worker_id,
                    "source_generation": source_generation,
                    "target_principal": target_principal,
                    # Manager metadata keeps the native authority snapshot;
                    # the delegated form above is wire evidence only.
                    "target_manager_principal": target_native,
                    "target_principal_digest": target_principal_digest,
                    "request": retry_request,
                    "prepared_at": datetime.utcnow().isoformat(
                        timespec="microseconds"
                    ),
                }
                writer = await db.execute(
                    sa_update(Task)
                    .where(
                        *worker_task_generation_predicates(observed),
                        no_active_worker_task_termination_predicate(),
                        no_active_test_harness_owner_graph_predicate(),
                    )
                    .values(status=Task.status)
                )
                if writer.rowcount != 1:
                    await db.rollback()
                    raise HTTPException(
                        409,
                        "Task Worker generation changed before retry preparation",
                    )
                locked = (
                    await db.execute(
                        select(Task)
                        .where(*worker_task_generation_predicates(observed))
                        .with_for_update()
                        .execution_options(populate_existing=True)
                    )
                ).scalar_one_or_none()
                if locked is None:
                    await db.rollback()
                    raise HTTPException(
                        409,
                        "Task Worker generation changed before retry preparation",
                    )
                metadata = dict(locked.metadata_ or {})
                metadata[WORKER_MANUAL_RETRY_RECEIPT_METADATA_KEY] = prepared_marker
                prepared = await db.execute(
                    sa_update(Task)
                    .where(
                        *worker_task_generation_predicates(observed),
                        Task.incarnation_id == source_generation["incarnation_id"],
                        no_active_worker_task_termination_predicate(),
                        no_active_test_harness_owner_graph_predicate(),
                    )
                    .values(metadata_=metadata)
                )
                if prepared.rowcount != 1:
                    await db.rollback()
                    raise HTTPException(
                        409,
                        "Task Worker generation changed before retry preparation",
                    )
                await db.commit()

                try:
                    result = await worker_proxy.proxy_to_worker(
                        locked,
                        "POST",
                        f"/api/tasks/{task_id}/internal/worker-retry",
                        retry_request,
                        require_json=True,
                        operation_lock_held=True,
                        quarantine_on_transport_uncertainty=True,
                        require_task_incarnation_fence=True,
                    )
                except WorkerTaskMutationOutcomeUncertainError as remote_error:
                    try:
                        result = await worker_proxy.proxy_to_worker(
                            locked,
                            "GET",
                            (
                                f"/api/tasks/{task_id}/internal/"
                                f"worker-retry-receipts/{operation_id}"
                            ),
                            require_json=True,
                            operation_lock_held=True,
                            surface_endpoint_not_found=True,
                            require_task_incarnation_fence=True,
                        )
                    except Exception as reconciliation_error:
                        raise HTTPException(
                            409,
                            "Worker retry outcome is uncertain; the exact operation "
                            "remains quarantined for receipt reconciliation",
                        ) from reconciliation_error
                    if remote_error.cancellation is not None:
                        # Finish exact adoption below before preserving caller
                        # cancellation; otherwise a committed Worker retry would
                        # remain needlessly quarantined.
                        pending_cancellation = remote_error.cancellation
                    else:
                        pending_cancellation = None
                else:
                    pending_cancellation = None

                adopted = await apply_authoritative_worker_retry(
                    db,
                    observed,
                    result,
                    operation_id=operation_id,
                    request_digest=retry_request["request_digest"],
                    commit=False,
                )
                if adopted is None:
                    await db.rollback()
                    raise HTTPException(
                        409,
                        "Worker retry receipt did not match the prepared operation",
                    )
                await _publish_worker_status_transition(
                    db,
                    observed_status=observed.status,
                    resulting=adopted,
                )
                db.expire_all()
                resulting_task = await db.get(Task, task_id)
                if resulting_task is None:
                    raise HTTPException(409, "Task disappeared after Worker retry")
                if pending_cancellation is not None:
                    raise pending_cancellation
                return await task_response(request, resulting_task, db)

        _require_no_pending_worker_routing(current)
        expected_harness_owner = test_harness_owner_identity(current)
        retried = await _retry_local_task_safely(
            task_id,
            queue,
            db,
            expected_identity=expected_harness_owner,
            task_updates=task_execution_principal_from_request(
                request,
                force_sandbox=(
                    is_pr_sandbox_task(current)
                    or current.worker_id is not None
                    or current.shared_from_id is not None
                    or is_worker_managed_task_metadata(current.metadata_)
                ),
            ),
            effect_request=request,
        )
        if not retried:
            raise HTTPException(404, "Task not found")
        locked_task = await _lock_task_generation(
            task_id,
            db,
            expected_status=retried.status,
            expected_retry_count=retried.retry_count,
            expected_turn_generation=retried.turn_generation,
            expected_instance_id=retried.instance_id,
            expected_started_at=retried.started_at,
            expected_completed_at=retried.completed_at,
            expected_pty_background_generation=(retried.pty_background_generation),
        )
        if locked_task is None:
            raise HTTPException(
                409,
                "Task was claimed by a newer generation before retry publication",
            )
        from backend.services.task_events import broadcast_status_change

        await broadcast_status_change(task_id, retried.status)
        await db.commit()
        return await task_response(request, locked_task, db)


@router.post("/{task_id}/star", response_model=TaskResponse)
async def star_task(
    task_id: int,
    request: Request,
    queue: TaskQueue = Depends(_get_queue),
    db: AsyncSession = Depends(get_db),
):
    task = await db.get(Task, task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    task = await lock_task_effect_access(
        request,
        task,
        db,
        allow_chat_share=False,
    )
    task = await queue.star(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    return await task_response(request, task, db)


@router.post("/{task_id}/read", response_model=TaskResponse)
async def mark_task_read(
    task_id: int,
    request: Request,
    queue: TaskQueue = Depends(_get_queue),
    db: AsyncSession = Depends(get_db),
):
    task = await db.get(Task, task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    task = await lock_task_effect_access(
        request,
        task,
        db,
        allow_chat_share=False,
    )
    if await active_worker_task_termination_receipt(db, task_id):
        raise HTTPException(409, "Task has an active Worker termination receipt")
    task.has_unread = False
    await db.commit()
    await db.refresh(task)
    return await task_response(request, task, db)


@router.post("/{task_id}/unread", response_model=TaskResponse)
async def mark_task_unread(
    task_id: int,
    request: Request,
    queue: TaskQueue = Depends(_get_queue),
    db: AsyncSession = Depends(get_db),
):
    task = await db.get(Task, task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    task = await lock_task_effect_access(
        request,
        task,
        db,
        allow_chat_share=False,
    )
    if await active_worker_task_termination_receipt(db, task_id):
        raise HTTPException(409, "Task has an active Worker termination receipt")
    task.has_unread = True
    await db.commit()
    await db.refresh(task)
    return await task_response(request, task, db)


@router.post("/{task_id}/archive", response_model=TaskResponse)
async def archive_task(
    task_id: int,
    request: Request,
    queue: TaskQueue = Depends(_get_queue),
    db: AsyncSession = Depends(get_db),
):
    task = await db.get(Task, task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    task = await lock_task_effect_access(
        request,
        task,
        db,
        allow_chat_share=False,
    )
    _require_not_delivery_owned_task(task, action="archived")
    await _require_not_isolated_browser_child(
        db,
        task,
        action="archived",
    )
    task = await queue.archive(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    return await task_response(request, task, db)


@router.get("/queue/next", response_model=list[TaskResponse])
async def get_queue(
    request: Request,
    queue: TaskQueue = Depends(_get_queue),
    db: AsyncSession = Depends(get_db),
):
    user_id = get_current_user_id(request)
    if not is_admin(request) and user_id is None:
        return []
    tasks = await queue.list_tasks(
        status="pending",
        user_id=None if is_admin(request) else user_id,
    )
    return await task_list_response(request, tasks, db)


def _require_plan_review_operation(task: Task) -> None:
    if task.canonical_plan_id is not None:
        raise HTTPException(
            409,
            f"Legacy Plan Task has migrated to canonical Plan #{task.canonical_plan_id}",
        )
    if task.mode == "plan" and task.status == "superseded":
        successor_id = (task.metadata_ or {}).get("plan_superseded_by_task_id")
        detail = "Plan has been superseded"
        if isinstance(successor_id, int):
            detail += f" by Plan #{successor_id}"
        raise HTTPException(409, detail)
    if task.mode != "plan" or task.status != "plan_review":
        raise HTTPException(400, "Task is not in plan review state")


def _require_worker_plan_decision_receipt_route(task: Task) -> None:
    """Prevent the legacy public decision route from bypassing its receipt."""

    if (
        settings.ccm_node_role == "worker"
        and is_worker_managed_task_metadata(task.metadata_)
    ):
        raise HTTPException(
            409,
            "Worker Plan decisions require the durable internal receipt protocol",
        )


def _worker_plan_decision_response(task: Task, receipt: dict) -> dict:
    return {"task": task, "receipt": receipt}


def _worker_plan_decision_absent(task_id: int, operation_id: str) -> dict:
    return {
        "protocol_version": WORKER_PLAN_DECISION_PROTOCOL,
        "state": "absent",
        "operation_id": operation_id,
        "task_id": task_id,
    }


def _require_worker_plan_decision_operation_id(operation_id: str) -> None:
    if (
        len(operation_id) != 32
        or any(char not in "0123456789abcdef" for char in operation_id)
    ):
        raise HTTPException(404, "Worker Plan decision receipt not found")


@router.get(
    "/{task_id}/internal/worker-plan-decisions/{operation_id}",
    response_model=(
        WorkerPlanDecisionResponse | WorkerPlanDecisionAbsentResponse
    ),
    include_in_schema=False,
)
async def get_worker_plan_decision_receipt(
    task_id: int,
    operation_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Read one exact Worker receipt without replaying a terminal decision."""

    require_internal_service(request)
    _require_worker_plan_decision_operation_id(operation_id)
    task = await require_worker_task_incarnation_header(request, task_id, db)
    receipt = worker_plan_decision_worker_receipt(task.metadata_)
    if receipt is None:
        return _worker_plan_decision_absent(task_id, operation_id)
    if receipt.get("operation_id") != operation_id:
        raise HTTPException(
            409,
            "Worker Plan already has a different durable decision receipt",
        )
    return _worker_plan_decision_response(task, receipt)


def _validate_worker_plan_decision_source(
    task: Task,
    body: WorkerPlanDecisionRequest,
) -> None:
    expected_path = f"/api/tasks/{task.id}/plan/{body.action}"
    actual_routing = {
        "provider": (task.provider or "claude").lower(),
        "model": task.model,
        "codex_service_tier": task.codex_service_tier or "default",
    }
    if (
        body.protocol_version != WORKER_PLAN_DECISION_PROTOCOL
        or body.task_id != task.id
        or body.source_incarnation_id != task.incarnation_id
        or body.expected_status != task.status
        or body.expected_retry_count != task.retry_count
        or body.expected_turn_generation != task.turn_generation
        or body.decision_path != expected_path
        or body.routing.model_dump(mode="json") != actual_routing
        or body.plan_target_task_id != task.plan_target_task_id
        or task.worker_id is not None
        or task.shared_from_id is not None
        or not is_worker_managed_task_metadata(task.metadata_)
    ):
        raise HTTPException(
            409,
            "Worker Plan decision source generation or routing changed",
        )


async def _apply_worker_plan_decision(
    task_id: int,
    body: WorkerPlanDecisionRequest,
    db: AsyncSession,
) -> Task:
    """Commit the exact Worker receipt and terminal Plan state atomically."""

    from backend.services.plan_tasks import (
        PlanTerminalQuiescenceError,
        plan_staleness,
        run_plan_terminal_transition,
    )

    request_payload = body.model_dump(mode="json")
    if (
        worker_plan_decision_request_digest(request_payload)
        != body.request_digest
    ):
        raise HTTPException(409, "Worker Plan decision request digest mismatch")
    if body.action == "approve":
        try:
            approval = (
                PlanApprovalRequest.model_validate(body.decision_body)
                if body.decision_body is not None
                else None
            )
        except Exception as exc:
            raise HTTPException(
                409,
                "Worker Plan approval body is invalid",
            ) from exc
    else:
        if body.decision_body is not None:
            raise HTTPException(409, "Worker Plan rejection body must be empty")
        approval = None
    if (body.plan_target_task_id is None) != (
        body.plan_target_incarnation_id is None
    ):
        raise HTTPException(409, "Worker Plan target identity is incomplete")

    terminal_status = "completed" if body.action == "approve" else "cancelled"

    async def commit_decision() -> None:
        db.expire_all()
        exact = await db.get(Task, task_id)
        if exact is None:
            raise HTTPException(404, "Task not found")
        existing = worker_plan_decision_worker_receipt(exact.metadata_)
        if existing is not None:
            if not worker_plan_decision_worker_receipt_matches(
                existing,
                request_payload,
            ):
                raise HTTPException(
                    409,
                    "Worker Plan decision operation conflicts with its receipt",
                )
            return
        _validate_worker_plan_decision_source(exact, body)
        _require_plan_review_operation(exact)
        _require_expected_task_routing(
            exact,
            approval.expected_routing if approval is not None else None,
            effective_model=exact.model,
        )
        target = None
        if body.plan_target_task_id is not None:
            target = await db.get(Task, body.plan_target_task_id)
            if (
                target is None
                or target.incarnation_id != body.plan_target_incarnation_id
            ):
                raise HTTPException(
                    409,
                    "Worker Plan target incarnation changed",
                )
        if body.action == "approve":
            stale = await plan_staleness(db, exact, current_target=target)
            if stale["stale"] and not (approval and approval.confirm_stale):
                raise HTTPException(
                    409,
                    detail={
                        "message": "Plan context changed; confirm stale approval",
                        "staleness": stale,
                    },
                )

        applied_at = datetime.utcnow()
        result_generation = {
            "task_id": exact.id,
            "incarnation_id": exact.incarnation_id,
            "status": terminal_status,
            "retry_count": exact.retry_count,
            "turn_generation": exact.turn_generation,
        }
        receipt = {
            "protocol_version": WORKER_PLAN_DECISION_PROTOCOL,
            "side": "worker",
            "state": "applied",
            "action": body.action,
            "operation_id": body.operation_id,
            "request_digest": body.request_digest,
            "request": request_payload,
            "result_generation": result_generation,
            "applied_at": applied_at.isoformat(timespec="microseconds"),
        }
        metadata = dict(exact.metadata_ or {})
        metadata[WORKER_PLAN_DECISION_RECEIPT_METADATA_KEY] = receipt
        values = {
            "plan_approved": body.action == "approve",
            "status": terminal_status,
            "completed_at": applied_at,
            "metadata_": metadata,
        }
        if body.action == "approve":
            values["plan_approved_at"] = applied_at
            # The Worker credential identifies the Manager service, not the
            # human decision maker.  The Manager's prepared gate owns that
            # audit identity and restores it during exact receipt adoption.
            values["plan_approved_by"] = None
        changed = await db.execute(
            sa_update(Task)
            .where(*_task_generation_fence(task_id, exact))
            .values(**values)
        )
        if changed.rowcount != 1:
            raise HTTPException(
                409,
                "Worker Plan generation changed while applying the decision",
            )

    try:
        await run_plan_terminal_transition(
            db,
            task_id,
            terminal_status,
            commit_decision,
        )
    except PlanTerminalQuiescenceError as exc:
        raise HTTPException(409, str(exc)) from exc
    db.expire_all()
    applied = await db.get(Task, task_id)
    receipt = (
        worker_plan_decision_worker_receipt(applied.metadata_)
        if applied is not None
        else None
    )
    if (
        applied is None
        or receipt is None
        or not worker_plan_decision_worker_receipt_matches(
            receipt,
            request_payload,
        )
    ):
        raise HTTPException(409, "Worker Plan decision receipt was not committed")
    return applied


@router.put(
    "/{task_id}/internal/worker-plan-decisions/{operation_id}",
    response_model=WorkerPlanDecisionResponse,
    include_in_schema=False,
)
async def put_worker_plan_decision_receipt(
    task_id: int,
    operation_id: str,
    body: WorkerPlanDecisionRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Idempotently apply one Manager-authorized legacy Plan decision."""

    require_internal_service(request)
    _require_worker_plan_decision_operation_id(operation_id)
    if body.task_id != task_id or body.operation_id != operation_id:
        raise HTTPException(409, "Worker Plan decision identity changed")
    await db.rollback()
    async with get_task_operation_lock(task_id):
        db.expire_all()
        task = await require_worker_task_incarnation_header(
            request,
            task_id,
            db,
        )
        existing = worker_plan_decision_worker_receipt(task.metadata_)
        request_payload = body.model_dump(mode="json")
        if existing is not None:
            if not worker_plan_decision_worker_receipt_matches(
                existing,
                request_payload,
            ):
                raise HTTPException(
                    409,
                    "Worker Plan decision operation conflicts with its receipt",
                )
            await db.rollback()
            replayed = await db.get(Task, task_id)
            replayed_receipt = (
                worker_plan_decision_worker_receipt(replayed.metadata_)
                if replayed is not None
                else None
            )
            if replayed is None or replayed_receipt != existing:
                raise HTTPException(
                    409,
                    "Worker Plan decision receipt changed during replay",
                )
            return _worker_plan_decision_response(replayed, replayed_receipt)

        # This is a new terminal effect, not receipt reconciliation. Commit a
        # Worker-node admission before any queue/Plan mutation so a drain that
        # wins first rejects it, while an admitted non-terminal Plan remains a
        # visible blocker until this exact operation settles or is terminated.
        await db.rollback()
        await fence_worker_node_mutation(db)
        task = await require_worker_task_incarnation_header(
            request,
            task_id,
            db,
        )
        existing = worker_plan_decision_worker_receipt(task.metadata_)
        if existing is not None:
            if not worker_plan_decision_worker_receipt_matches(
                existing,
                request_payload,
            ):
                await db.rollback()
                raise HTTPException(
                    409,
                    "Worker Plan decision operation conflicts with its receipt",
                )
            await db.rollback()
            replayed = await require_worker_task_incarnation_header(
                request,
                task_id,
                db,
            )
            replayed_receipt = worker_plan_decision_worker_receipt(
                replayed.metadata_
            )
            if replayed_receipt != existing:
                raise HTTPException(
                    409,
                    "Worker Plan decision receipt changed during replay",
                )
            return _worker_plan_decision_response(replayed, replayed_receipt)
        await db.commit()
        applied = await _apply_worker_plan_decision(task_id, body, db)
        receipt = worker_plan_decision_worker_receipt(applied.metadata_)
        if receipt is None:
            raise HTTPException(409, "Worker Plan decision receipt is missing")
        return _worker_plan_decision_response(applied, receipt)


async def _reconcile_manager_worker_plan_decision(
    db: AsyncSession,
    task: Task,
    *,
    observed: WorkerTaskGeneration,
    identity: TestHarnessOwnerIdentity,
    effect: str,
    marker: dict,
) -> Task:
    """Read back/apply/adopt one exact Worker legacy Plan decision."""

    task_id = observed.task_id
    request_payload = marker.get("request")
    operation_id = marker.get("operation_id")
    request_digest = marker.get("request_digest")
    if not (
        isinstance(request_payload, dict)
        and isinstance(operation_id, str)
        and isinstance(request_digest, str)
        and worker_plan_decision_request_matches(
            request_payload,
            operation_id=operation_id,
            request_digest=request_digest,
        )
    ):
        raise HTTPException(409, "Manager Worker Plan decision outbox is invalid")
    receipt_path = (
        f"/api/tasks/{task_id}/internal/worker-plan-decisions/{operation_id}"
    )

    try:
        result = await _proxy(
            task,
            "GET",
            receipt_path,
            require_json=True,
            surface_endpoint_not_found=True,
            operation_lock_held=True,
            require_task_incarnation_fence=True,
        )
    except WorkerEndpointNotFoundError as exc:
        raise HTTPException(
            409,
            "Worker must be upgraded before a legacy Plan decision can be "
            "applied safely",
        ) from exc
    except Exception as exc:
        raise HTTPException(
            409,
            "Worker Plan decision receipt could not be read; the exact "
            "operation remains active",
        ) from exc

    pending_cancellation = None
    if worker_plan_decision_absent_response_matches(result, request_payload):
        try:
            result = await _proxy(
                task,
                "PUT",
                receipt_path,
                body=request_payload,
                require_json=True,
                operation_lock_held=True,
                quarantine_on_transport_uncertainty=True,
                require_task_incarnation_fence=True,
            )
        except WorkerTaskMutationOutcomeUncertainError as remote_error:
            if remote_error.cancellation is not None:
                pending_cancellation = remote_error.cancellation
            result = None
        # A syntactically valid but mismatched confirmation is still an ACK
        # loss boundary.  Read the durable receipt before declaring conflict;
        # never resend this non-repeatable operation in the same invocation.
        if not worker_plan_decision_response_matches(result, request_payload):
            try:
                result = await _proxy(
                    task,
                    "GET",
                    receipt_path,
                    require_json=True,
                    surface_endpoint_not_found=True,
                    operation_lock_held=True,
                    require_task_incarnation_fence=True,
                )
            except Exception as exc:
                raise HTTPException(
                    409,
                    "Worker Plan decision outcome is uncertain; the exact "
                    "operation remains active for receipt reconciliation",
                ) from exc
    elif not worker_plan_decision_response_matches(result, request_payload):
        # Malformed/contradictory readback is not permission to send a PUT.
        raise HTTPException(
            409,
            "Worker Plan decision readback is malformed or conflicts with the "
            "prepared operation",
        )

    if not worker_plan_decision_response_matches(result, request_payload):
        raise HTTPException(
            409,
            "Worker Plan decision has no exact applied receipt; the operation "
            "remains active",
        )
    remote_task = result["task"]
    worker_receipt = result["receipt"]

    await db.rollback()
    db.expire_all()
    current = await db.get(Task, task_id)
    if (
        current is None
        or worker_task_generation(
            current,
            expected_worker_id=observed.worker_id,
        )
        != observed
        or not _task_control_effect_gate_is_active(
            current,
            identity,
            effect,
        )
        or worker_plan_decision_gate_receipt(current.metadata_) != marker
    ):
        raise HTTPException(
            409,
            "Plan generation or decision outbox changed before receipt adoption",
        )
    settled_metadata = _settled_worker_plan_decision_metadata(
        current,
        identity,
        effect=effect,
        marker=marker,
        worker_receipt=worker_receipt,
    )
    resulting = await apply_authoritative_worker_task(
        db,
        observed,
        remote_task,
        metadata_updates={
            TEST_HARNESS_TERMINAL_GATE_KEY: settled_metadata[
                TEST_HARNESS_TERMINAL_GATE_KEY
            ]
        },
        worker_plan_decision_operation_id=operation_id,
        commit=False,
    )
    if resulting is None:
        await db.rollback()
        raise HTTPException(
            409,
            "Worker Plan receipt did not match the exact Manager generation",
        )
    adopted = await db.get(Task, task_id, populate_existing=True)
    if adopted is None:
        await db.rollback()
        raise HTTPException(409, "Plan disappeared during receipt adoption")
    if request_payload.get("action") == "approve":
        gate = settled_metadata[TEST_HARNESS_TERMINAL_GATE_KEY]
        authorized_user_id = gate.get("authorized_user_id")
        adopted.plan_approved_by = (
            authorized_user_id
            if type(authorized_user_id) is int and authorized_user_id > 0
            else None
        )
    await _publish_worker_status_transition(
        db,
        observed_status=observed.status,
        resulting=resulting,
    )
    db.expire_all()
    final = await db.get(Task, task_id)
    if final is None:
        raise HTTPException(409, "Plan disappeared after receipt settlement")
    if pending_cancellation is not None:
        raise pending_cancellation
    return final


@router.post("/{task_id}/plan/approve", response_model=TaskResponse)
async def approve_plan(
    task_id: int,
    request: Request,
    body: PlanApprovalRequest | None = None,
    queue: TaskQueue = Depends(_get_queue),
    db: AsyncSession = Depends(get_db),
):
    """Approve an independent Plan without starting an Agent turn."""
    task = await queue.get(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    await require_task_control(request, task, db)
    await db.rollback()
    async with get_task_operation_lock(task_id):
        db.expire_all()
        current = await db.get(Task, task_id)
        if current is None:
            raise HTTPException(404, "Task not found")
        await require_task_control(request, current, db)
        _require_expected_task_routing(
            current,
            body.expected_routing if body is not None else None,
            effective_model=current.model,
        )
        _require_plan_review_operation(current)
        _require_worker_plan_decision_receipt_route(current)

        target = None
        if current.plan_target_task_id is not None:
            target = await db.get(Task, current.plan_target_task_id)
            if target is None:
                raise HTTPException(409, "Plan target no longer exists")
            await require_task_control(request, target, db)

        expected_worker_id = current.worker_id
        if expected_worker_id is None:
            _require_no_pending_worker_routing(current)
        from backend.services.plan_tasks import plan_staleness

        if expected_worker_id is None:
            stale = await plan_staleness(db, current, current_target=target)
            if stale["stale"] and not (body and body.confirm_stale):
                raise HTTPException(
                    409,
                    detail={
                        "message": "Plan context changed; confirm stale approval",
                        "staleness": stale,
                    },
                )

        if target is None:
            current = await lock_task_effect_access(
                request,
                current,
                db,
                allow_chat_share=False,
            )
        else:
            locked_tasks = await lock_task_effect_accesses(
                request,
                [current, target],
                db,
                allow_chat_share=False,
            )
            locked_by_id = {locked.id: locked for locked in locked_tasks}
            current = locked_by_id[task_id]
            target = locked_by_id.get(current.plan_target_task_id)
            if target is None:
                raise HTTPException(409, "Plan target changed during approval")

        _require_expected_task_routing(
            current,
            body.expected_routing if body is not None else None,
            effective_model=current.model,
        )
        _require_plan_review_operation(current)
        if current.worker_id != expected_worker_id:
            raise HTTPException(409, "Task Worker assignment changed")
        if target is not None and current.plan_target_task_id != target.id:
            raise HTTPException(409, "Plan target changed during approval")
        if current.worker_id is None:
            _require_no_pending_worker_routing(current)
        expected_plan_identity = test_harness_owner_identity(current)
        expected_target_incarnation_id = (
            target.incarnation_id if target is not None else None
        )
        worker_plan_decision_request_base = None
        if expected_worker_id is not None:
            worker_plan_decision_request_base = {
                "protocol_version": WORKER_PLAN_DECISION_PROTOCOL,
                "action": "approve",
                "task_id": task_id,
                "manager_worker_id": expected_worker_id,
                "source_incarnation_id": (
                    expected_plan_identity.incarnation_id
                ),
                "expected_status": expected_plan_identity.status,
                "expected_retry_count": expected_plan_identity.retry_count,
                "expected_turn_generation": (
                    expected_plan_identity.turn_generation
                ),
                "decision_path": f"/api/tasks/{task_id}/plan/approve",
                "routing": {
                    "provider": (current.provider or "claude").lower(),
                    "model": current.model,
                    "codex_service_tier": (
                        current.codex_service_tier or "default"
                    ),
                },
                "decision_body": (
                    body.model_dump(mode="json")
                    if body is not None
                    else None
                ),
                "plan_target_task_id": current.plan_target_task_id,
                "plan_target_incarnation_id": (
                    expected_target_incarnation_id
                ),
            }
        expected_plan_identity = await _commit_task_control_effect_gate(
            request,
            db,
            current,
            effect="plan_approve",
            worker_plan_decision_request_base=(
                worker_plan_decision_request_base
            ),
        )

        if worker_plan_decision_request_base is not None:
            marker = await _prepare_worker_plan_decision_receipt(
                db,
                expected_plan_identity,
                effect="plan_approve",
                request_base=worker_plan_decision_request_base,
            )
            db.expire_all()
            current = await db.get(Task, task_id)
            if (
                current is None
                or not _task_control_effect_gate_is_active(
                    current,
                    expected_plan_identity,
                    "plan_approve",
                )
                or worker_plan_decision_gate_receipt(current.metadata_)
                != marker
            ):
                raise HTTPException(
                    409,
                    "Plan approval outbox changed before routing preflight",
                )
            await _ensure_worker_routing_ready(
                current,
                operation_lock_held=True,
            )
            db.expire_all()
            current = await db.get(Task, task_id)
            if (
                current is None
                or not _task_control_effect_gate_matches(
                    current,
                    expected_plan_identity,
                    "plan_approve",
                )
                or worker_plan_decision_gate_receipt(current.metadata_)
                != marker
            ):
                raise HTTPException(
                    409,
                    "Plan approval effect gate changed before Worker request",
                )
            observed = worker_task_generation(
                current,
                expected_worker_id=expected_worker_id,
            )
            if observed is None:
                raise HTTPException(409, "Task Worker assignment changed")
            approved = await _reconcile_manager_worker_plan_decision(
                db,
                current,
                observed=observed,
                identity=expected_plan_identity,
                effect="plan_approve",
                marker=marker,
            )
            return await task_response(request, approved, db)

        from backend.services.plan_tasks import (
            PlanTerminalQuiescenceError,
            run_plan_terminal_transition,
        )

        async def commit_approval() -> None:
            db.expire_all()
            exact = await db.get(Task, task_id)
            if exact is None:
                raise HTTPException(404, "Task not found")
            if not _task_control_effect_gate_matches(
                exact,
                expected_plan_identity,
                "plan_approve",
            ):
                raise HTTPException(
                    409,
                    "Plan approval effect gate changed before terminal commit",
                )
            _require_expected_task_routing(
                exact,
                body.expected_routing if body is not None else None,
                effective_model=exact.model,
            )
            _require_plan_review_operation(exact)
            if exact.worker_id is not None:
                raise HTTPException(409, "Task Worker assignment changed")
            _require_no_pending_worker_routing(exact)
            exact_target = None
            if exact.plan_target_task_id is not None:
                exact_target = await db.get(Task, exact.plan_target_task_id)
                if exact_target is None:
                    raise HTTPException(409, "Plan target no longer exists")
                if (
                    exact_target.incarnation_id
                    != expected_target_incarnation_id
                ):
                    raise HTTPException(
                        409,
                        "Plan target incarnation changed before approval",
                    )
            exact_stale = await plan_staleness(
                db,
                exact,
                current_target=exact_target,
            )
            if exact_stale["stale"] and not (body and body.confirm_stale):
                raise HTTPException(
                    409,
                    detail={
                        "message": "Plan context changed; confirm stale approval",
                        "staleness": exact_stale,
                    },
                )
            approved_at = datetime.utcnow()
            changed = await db.execute(
                sa_update(Task)
                .where(*_task_generation_fence(task_id, exact))
                .values(
                    plan_approved=True,
                    plan_approved_at=approved_at,
                    plan_approved_by=get_current_user_id(request),
                    status="completed",
                    completed_at=approved_at,
                )
            )
            if changed.rowcount != 1:
                raise HTTPException(
                    409,
                    "Task generation changed while approving the plan",
                )

        try:
            await run_plan_terminal_transition(
                db,
                task_id,
                "completed",
                commit_approval,
            )
        except PlanTerminalQuiescenceError as exc:
            raise HTTPException(409, str(exc)) from exc
        db.expire_all()
        approved = await db.get(Task, task_id)
        if approved is None:
            raise HTTPException(409, "Task disappeared while approving the plan")
        return await task_response(request, approved, db)


@router.post("/{task_id}/plan/reject", response_model=TaskResponse)
async def reject_plan(
    task_id: int,
    request: Request,
    queue: TaskQueue = Depends(_get_queue),
    db: AsyncSession = Depends(get_db),
):
    """Reject a plan-mode task's plan."""
    task = await queue.get(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    await require_task_control(request, task, db)
    await db.rollback()
    async with get_task_operation_lock(task_id):
        db.expire_all()
        current = await db.get(Task, task_id)
        if current is None:
            raise HTTPException(404, "Task not found")
        await require_task_control(request, current, db)
        _require_plan_review_operation(current)
        _require_worker_plan_decision_receipt_route(current)

        expected_worker_id = current.worker_id
        expected_target_task_id = current.plan_target_task_id
        expected_target_incarnation_id = None
        if expected_target_task_id is not None:
            target = await db.get(Task, expected_target_task_id)
            if target is None:
                raise HTTPException(409, "Plan target no longer exists")
            expected_target_incarnation_id = target.incarnation_id
        current = await lock_task_effect_access(
            request,
            current,
            db,
            allow_chat_share=False,
        )
        _require_plan_review_operation(current)
        if current.worker_id != expected_worker_id:
            raise HTTPException(409, "Task Worker assignment changed")
        if current.plan_target_task_id != expected_target_task_id:
            raise HTTPException(409, "Plan target changed during rejection")
        expected_plan_identity = test_harness_owner_identity(current)
        worker_plan_decision_request_base = None
        if expected_worker_id is not None:
            worker_plan_decision_request_base = {
                "protocol_version": WORKER_PLAN_DECISION_PROTOCOL,
                "action": "reject",
                "task_id": task_id,
                "manager_worker_id": expected_worker_id,
                "source_incarnation_id": (
                    expected_plan_identity.incarnation_id
                ),
                "expected_status": expected_plan_identity.status,
                "expected_retry_count": expected_plan_identity.retry_count,
                "expected_turn_generation": (
                    expected_plan_identity.turn_generation
                ),
                "decision_path": f"/api/tasks/{task_id}/plan/reject",
                "routing": {
                    "provider": (current.provider or "claude").lower(),
                    "model": current.model,
                    "codex_service_tier": (
                        current.codex_service_tier or "default"
                    ),
                },
                "decision_body": None,
                "plan_target_task_id": expected_target_task_id,
                "plan_target_incarnation_id": (
                    expected_target_incarnation_id
                ),
            }
        expected_plan_identity = await _commit_task_control_effect_gate(
            request,
            db,
            current,
            effect="plan_reject",
            worker_plan_decision_request_base=(
                worker_plan_decision_request_base
            ),
        )

        if worker_plan_decision_request_base is not None:
            marker = await _prepare_worker_plan_decision_receipt(
                db,
                expected_plan_identity,
                effect="plan_reject",
                request_base=worker_plan_decision_request_base,
            )
            db.expire_all()
            current = await db.get(Task, task_id)
            if (
                current is None
                or not _task_control_effect_gate_is_active(
                    current,
                    expected_plan_identity,
                    "plan_reject",
                )
                or worker_plan_decision_gate_receipt(current.metadata_)
                != marker
            ):
                raise HTTPException(
                    409,
                    "Plan rejection outbox changed before routing preflight",
                )
            await _ensure_worker_routing_ready(
                current,
                operation_lock_held=True,
            )
            db.expire_all()
            current = await db.get(Task, task_id)
            if (
                current is None
                or not _task_control_effect_gate_matches(
                    current,
                    expected_plan_identity,
                    "plan_reject",
                )
                or worker_plan_decision_gate_receipt(current.metadata_)
                != marker
            ):
                raise HTTPException(
                    409,
                    "Plan rejection effect gate changed before Worker request",
                )
            observed = worker_task_generation(
                current,
                expected_worker_id=expected_worker_id,
            )
            if observed is None:
                raise HTTPException(409, "Task Worker assignment changed")
            rejected = await _reconcile_manager_worker_plan_decision(
                db,
                current,
                observed=observed,
                identity=expected_plan_identity,
                effect="plan_reject",
                marker=marker,
            )
            return await task_response(request, rejected, db)

        from backend.services.plan_tasks import (
            PlanTerminalQuiescenceError,
            run_plan_terminal_transition,
        )

        async def commit_rejection() -> None:
            db.expire_all()
            exact = await db.get(Task, task_id)
            if exact is None:
                raise HTTPException(404, "Task not found")
            if not _task_control_effect_gate_matches(
                exact,
                expected_plan_identity,
                "plan_reject",
            ):
                raise HTTPException(
                    409,
                    "Plan rejection effect gate changed before terminal commit",
                )
            _require_plan_review_operation(exact)
            if exact.worker_id is not None:
                raise HTTPException(409, "Task Worker assignment changed")
            changed = await db.execute(
                sa_update(Task)
                .where(*_task_generation_fence(task_id, exact))
                .values(
                    plan_approved=False,
                    status="cancelled",
                    completed_at=datetime.utcnow(),
                )
            )
            if changed.rowcount != 1:
                raise HTTPException(
                    409,
                    "Task generation changed while rejecting the plan",
                )

        try:
            await run_plan_terminal_transition(
                db,
                task_id,
                "cancelled",
                commit_rejection,
            )
        except PlanTerminalQuiescenceError as exc:
            raise HTTPException(409, str(exc)) from exc
        db.expire_all()
        rejected = await db.get(Task, task_id)
        if rejected is None:
            raise HTTPException(409, "Task disappeared while rejecting the plan")
        return await task_response(request, rejected, db)
