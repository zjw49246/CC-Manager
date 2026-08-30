"""Authenticated, node-wide proof that a Worker may be destroyed safely.

The Manager's routing pointers are not enough to prove that the remote node is
quiescent.  A Worker can own locally-created Browser child Tasks, persisted
Harness cleanup, native Instance owners, and the Worker half of a termination
receipt.  This module gives the Worker one versioned, fail-closed snapshot of
all of those durable stores after every Manager mirror has been stopped.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.capability import (
    ACTIVE_EXECUTION_STATUSES,
    ACTIVE_INVOCATION_STATUSES,
    CapabilityExecution,
    CapabilityInvocation,
    CapabilityResumeOutbox,
)
from backend.models.instance import Instance
from backend.models.log_entry import LogEntry
from backend.models.plan_agent import PlanAgentRun, PlanAgentRuntimeReceipt
from backend.models.project import Project
from backend.models.sub_agent import SubAgentReport, SubAgentSession
from backend.models.task import Task
from backend.models.task_migration import TaskMigrationOperation
from backend.models.task_ssh_effect import TaskSSHEffectReceipt
from backend.models.task_id_allocator import (
    TASK_ID_WORKER_NAMESPACE_START,
)
from backend.models.test_harness import (
    BrowserReviewOperationReceipt,
    TestHarnessAttempt,
    TestHarnessChildBinding,
    TestHarnessEvent,
    TestHarnessEvidence,
    TestHarnessFinding,
    TestHarnessRun,
    TestHarnessSandboxLease,
)
from backend.models.worker_task_termination import WorkerTaskTerminationReceipt
from backend.models.worker_turn_handoff import WorkerTurnHandoffReceipt
from backend.models.workspace_review import WorkspaceReviewRun
from backend.services.test_harness_contracts import HARNESS_TERMINAL_STATUSES
from backend.services.test_harness_owner_fence import (
    TEST_HARNESS_TERMINAL_GATE_KEY,
    TestHarnessOwnerIdentity,
    test_harness_owner_terminal_gate_matches,
)
from backend.services.task_id_namespace import (
    TaskIdNamespaceError,
    fence_worker_task_insert,
)
from backend.services.task_events import (
    PTY_TERMINAL_PUBLICATION_EVENT_TYPE,
)
from backend.services.worker_node_control import (
    WORKER_NODE_DRAIN_PROTOCOL,
    begin_worker_node_runtime_seal,
    fence_worker_node_drain_claim,
)
from backend.services.project_materialization import (
    ACTIVE_PROJECT_MATERIALIZATION_STATUSES,
)


WORKER_NODE_DRAIN_PROOF_PROTOCOL = WORKER_NODE_DRAIN_PROTOCOL
_TASK_TERMINAL_STATUSES = frozenset(
    {"completed", "failed", "cancelled", "conflict"}
)
_WORKSPACE_TERMINAL_STATUSES = frozenset(
    {"completed", "failed", "cancelled"}
)
_BINDING_TERMINAL_STATES = frozenset({"stopped", "completed"})
_HANDOFF_SETTLED_STATUSES = frozenset({"cancelled", "completed"})
_MAX_BLOCKERS = 100


def _canonical_payload(payload: dict[str, Any]) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def worker_node_drain_proof_signature(
    payload: dict[str, Any],
    *,
    auth_token: str,
) -> str:
    """Bind one nonce-scoped proof to the Worker control credential."""

    if not isinstance(auth_token, str) or not auth_token:
        raise ValueError("Worker drain proof requires an authentication token")
    return hmac.new(
        auth_token.encode("utf-8"),
        _canonical_payload(payload),
        hashlib.sha256,
    ).hexdigest()


def verify_worker_node_drain_proof_signature(
    payload: dict[str, Any],
    *,
    auth_token: str,
    signature: object,
) -> bool:
    if not isinstance(signature, str) or len(signature) != 64:
        return False
    try:
        expected = worker_node_drain_proof_signature(
            payload,
            auth_token=auth_token,
        )
    except (TypeError, ValueError, UnicodeError):
        return False
    return hmac.compare_digest(expected, signature)


def _gate_from_task(task: Task) -> dict[str, Any] | None:
    metadata = task.metadata_ if isinstance(task.metadata_, dict) else {}
    gate = metadata.get(TEST_HARNESS_TERMINAL_GATE_KEY)
    return gate if isinstance(gate, dict) else None


async def exact_worker_task_terminal_cleanup_is_proven(
    db: AsyncSession,
    task: Task,
    *,
    source_status: str,
) -> bool:
    """Prove one exact Task generation is terminal, gated, and owner-free."""

    if (
        not task.incarnation_id
        or task.status not in _TASK_TERMINAL_STATUSES
        or task.pty_background_generation is not None
    ):
        return False
    identity = TestHarnessOwnerIdentity(
        task_id=task.id,
        incarnation_id=task.incarnation_id,
        retry_count=task.retry_count,
        turn_generation=task.turn_generation,
        status=source_status,
    )
    if not test_harness_owner_terminal_gate_matches(task, identity):
        return False
    owner = await db.scalar(
        select(Instance.id)
        .where(Instance.current_task_id == task.id)
        .limit(1)
    )
    if owner is not None:
        return False
    active_run = await db.scalar(
        select(TestHarnessRun.id)
        .where(
            TestHarnessRun.task_id == task.id,
            or_(
                TestHarnessRun.status.not_in(HARNESS_TERMINAL_STATUSES),
                TestHarnessRun.cleanup_status != "completed",
            ),
        )
        .limit(1)
    )
    if active_run is not None:
        return False
    active_workspace = await db.scalar(
        select(WorkspaceReviewRun.id)
        .where(
            WorkspaceReviewRun.task_id == task.id,
            or_(
                WorkspaceReviewRun.status.not_in(
                    _WORKSPACE_TERMINAL_STATUSES
                ),
                WorkspaceReviewRun.cleanup_status != "completed",
            ),
        )
        .limit(1)
    )
    if active_workspace is not None:
        return False
    active_binding = await db.scalar(
        select(TestHarnessChildBinding.id)
        .where(
            TestHarnessChildBinding.owner_task_id == task.id,
            TestHarnessChildBinding.state.not_in(
                _BINDING_TERMINAL_STATES
            ),
        )
        .limit(1)
    )
    if active_binding is not None:
        return False
    active_sub_agent = await db.scalar(
        select(SubAgentSession.id)
        .where(
            SubAgentSession.task_id == task.id,
            or_(
                SubAgentSession.status == "running",
                SubAgentSession.codex_cleanup_pending.is_(True),
                SubAgentSession.active_turn_generation.is_not(None),
            ),
        )
        .limit(1)
    )
    if active_sub_agent is not None:
        return False
    active_capability = await db.scalar(
        select(CapabilityInvocation.id)
        .where(
            CapabilityInvocation.task_id == task.id,
            CapabilityInvocation.status.in_(ACTIVE_INVOCATION_STATUSES),
        )
        .limit(1)
    )
    if active_capability is not None:
        return False
    active_execution = await db.scalar(
        select(CapabilityExecution.id)
        .join(
            CapabilityInvocation,
            CapabilityInvocation.id == CapabilityExecution.invocation_id,
        )
        .where(
            CapabilityInvocation.task_id == task.id,
            CapabilityExecution.status.in_(ACTIVE_EXECUTION_STATUSES),
        )
        .limit(1)
    )
    if active_execution is not None:
        return False
    active_resume = await db.scalar(
        select(CapabilityResumeOutbox.id)
        .where(
            CapabilityResumeOutbox.task_id == task.id,
            CapabilityResumeOutbox.status.not_in(
                ("completed", "cancelled", "failed")
            ),
        )
        .limit(1)
    )
    return active_resume is None


async def _worker_node_drain_snapshot(
    db: AsyncSession,
    *,
    nonce: str | None,
    drain_claim: str,
    install_runtime_seal: bool,
) -> dict[str, Any]:
    """Take the node-first snapshot used by seal and final proof.

    A fresh seal is written in this transaction *before* the scan, but is
    rolled back with any blocker.  The node-row UPDATE therefore waits behind
    every already-admitted callback transaction, while every callback arriving
    after it waits and is rejected only after the clean seal commits.  This is
    the cross-dialect writer drain; no timeout or process-local sleep is part
    of the correctness argument.
    """

    # Lock order is node-control -> Task allocator -> Tasks -> LogEntry. Task
    # creation takes the same first two fences and terminal publication takes
    # node-control -> Task -> LogEntry.  Phase one already closed new ownership;
    # phase two below serializes every exact-generation persistence callback.
    sealed_before = await fence_worker_node_drain_claim(
        db,
        claim=drain_claim,
    )
    if install_runtime_seal:
        await begin_worker_node_runtime_seal(db, claim=drain_claim)
    # Serialize with the high-range allocator.  On SQLite this is also the
    # database writer reservation; on PostgreSQL/MySQL it prevents a new local
    # Browser Task id from being allocated across this snapshot.
    try:
        await fence_worker_task_insert(db, bind_if_needed=False)
    except TaskIdNamespaceError as exc:
        await db.rollback()
        raise RuntimeError(
            "Worker drain proof requires a database durably bound as worker"
        ) from exc

    tasks = list(
        (
            await db.execute(select(Task).order_by(Task.id).with_for_update())
        ).scalars()
    )
    pending_task_publications = list(
        (
            await db.execute(
                select(
                    LogEntry.id,
                    LogEntry.task_id,
                    LogEntry.task_retry_count,
                    LogEntry.task_turn_generation,
                    LogEntry.native_turn_id,
                )
                .where(
                    LogEntry.event_type
                    == PTY_TERMINAL_PUBLICATION_EVENT_TYPE
                )
                .order_by(LogEntry.id)
                .with_for_update()
            )
        ).all()
    )
    blockers: list[dict[str, Any]] = []
    blocker_count = 0

    def add(kind: str, identity: object, detail: str) -> None:
        nonlocal blocker_count
        blocker_count += 1
        if len(blockers) < _MAX_BLOCKERS:
            blockers.append(
                {"kind": kind, "id": identity, "detail": detail[:500]}
            )

    if not install_runtime_seal and not sealed_before:
        add(
            "runtime_seal_missing",
            1,
            "exact drain claim has not completed the final runtime writer seal",
        )

    # Any row is an unresolved external publication effect. Deliberately do
    # not parse here: malformed/corrupt payloads must fail closed too, and the
    # recovery publisher is the only component allowed to validate and ACK
    # (delete) them under the matching Task lock.
    for (
        publication_id,
        task_id,
        retry_count,
        turn_generation,
        source_generation,
    ) in pending_task_publications:
        add(
            "task_event_publication_pending",
            publication_id,
            f"task_id={task_id}, retry_count={retry_count}, "
            f"turn_generation={turn_generation}, "
            f"source_background_generation={source_generation}",
        )

    for task in tasks:
        if task.status not in _TASK_TERMINAL_STATUSES:
            add("task_nonterminal", task.id, f"status={task.status}")
        if task.pty_background_generation is not None:
            add(
                "task_pty_background",
                task.id,
                f"generation={task.pty_background_generation}",
            )
        if task.id >= TASK_ID_WORKER_NAMESPACE_START:
            continue

        # Every low-range row is a Manager mirror on a bound Worker.  Its
        # acknowledged Worker receipt is the remote half of the Manager's
        # per-Task stop proof and identifies the status stored in the gate.
        receipts = list(
            (
                await db.execute(
                    select(WorkerTaskTerminationReceipt)
                    .where(
                        WorkerTaskTerminationReceipt.side == "worker",
                        WorkerTaskTerminationReceipt.task_id == task.id,
                        WorkerTaskTerminationReceipt.operation
                        == "stop_session",
                        WorkerTaskTerminationReceipt.status
                        == "acknowledged",
                        WorkerTaskTerminationReceipt.source_task_incarnation_id
                        == task.incarnation_id,
                        WorkerTaskTerminationReceipt.source_task_retry_count
                        == task.retry_count,
                        WorkerTaskTerminationReceipt.source_task_turn_generation
                        == task.turn_generation,
                    )
                    .order_by(
                        WorkerTaskTerminationReceipt.created_at.desc(),
                        WorkerTaskTerminationReceipt.operation_id.desc(),
                    )
                )
            ).scalars()
        )
        proven = False
        for receipt in receipts:
            if await exact_worker_task_terminal_cleanup_is_proven(
                db,
                task,
                source_status=receipt.source_task_status,
            ):
                proven = True
                break
        if not proven:
            gate = _gate_from_task(task)
            add(
                "manager_mirror_unproven",
                task.id,
                "missing exact acknowledged stop receipt, durable owner gate, "
                f"or cleanup proof (gate={bool(gate)})",
            )

    # Task migration operations deliberately have no foreign keys: deleting a
    # Task or Worker must not erase evidence that a remote prepare/commit may
    # still need reconciliation.  They are Manager-owned in the current
    # topology; Worker-side prepared receipts are the durable owner that makes
    # rollback-before-import safe. Terminal history is not executable
    # ownership and may be discarded only with the whole drained node.
    migrations = list(
        (
            await db.execute(
                select(
                    TaskMigrationOperation.operation_id,
                    TaskMigrationOperation.operation_sequence,
                    TaskMigrationOperation.side,
                    TaskMigrationOperation.task_id,
                    TaskMigrationOperation.phase,
                    TaskMigrationOperation.source_worker_id,
                    TaskMigrationOperation.target_worker_id,
                )
                .where(TaskMigrationOperation.active_task_id.isnot(None))
                .order_by(TaskMigrationOperation.operation_id)
            )
        ).all()
    )
    for (
        operation_id,
        operation_sequence,
        side,
        task_id,
        phase,
        source_worker_id,
        target_worker_id,
    ) in migrations:
        add(
            "task_migration_operation",
            operation_id,
            f"task_id={task_id}, sequence={operation_sequence}, "
            f"side={side}, phase={phase}, "
            f"source_worker_id={source_worker_id}, "
            f"target_worker_id={target_worker_id}",
        )

    projects = list(
        (
            await db.execute(
                select(Project.id, Project.name, Project.status)
                .where(
                    Project.status.in_(
                        ACTIVE_PROJECT_MATERIALIZATION_STATUSES
                    )
                )
                .order_by(Project.id)
            )
        ).all()
    )
    for project_id, project_name, project_status in projects:
        add(
            "project_materialization_active",
            project_id,
            f"name={project_name}, status={project_status}",
        )

    instance_owners = list(
        (
            await db.execute(
                select(
                    Instance.id,
                    Instance.current_task_id,
                    Instance.current_plan_run_id,
                ).where(
                    or_(
                        Instance.current_task_id.is_not(None),
                        Instance.current_plan_run_id.is_not(None),
                    )
                )
            )
        ).all()
    )
    for instance_id, task_id, plan_run_id in instance_owners:
        add(
            "instance_owner",
            instance_id,
            f"task_id={task_id}, plan_run_id={plan_run_id}",
        )

    runs = list(
        (
            await db.execute(
                select(
                    TestHarnessRun.id,
                    TestHarnessRun.status,
                    TestHarnessRun.cleanup_status,
                )
            )
        ).all()
    )
    for run_id, status, cleanup in runs:
        active = (
            status not in HARNESS_TERMINAL_STATUSES
            or cleanup != "completed"
        )
        add(
            "harness_run_active" if active else "unmigrated_harness_evidence",
            run_id,
            f"status={status}, cleanup={cleanup}; Worker-local reports and "
            "artifacts have no Manager import ACK",
        )

    workspaces = list(
        (
            await db.execute(
                select(
                    WorkspaceReviewRun.id,
                    WorkspaceReviewRun.status,
                    WorkspaceReviewRun.cleanup_status,
                )
            )
        ).all()
    )
    for run_id, status, cleanup in workspaces:
        active = (
            status not in _WORKSPACE_TERMINAL_STATUSES
            or cleanup != "completed"
        )
        add(
            "workspace_run_active"
            if active
            else "unmigrated_workspace_evidence",
            run_id,
            f"status={status}, cleanup={cleanup}; no Manager import ACK",
        )

    leases = list(
        (
            await db.execute(
                select(
                    TestHarnessSandboxLease.id,
                    TestHarnessSandboxLease.status,
                    TestHarnessSandboxLease.cleanup_status,
                )
            )
        ).all()
    )
    for lease_id, status, cleanup in leases:
        add(
            "sandbox_lease_active"
            if cleanup != "completed"
            else "unmigrated_sandbox_audit",
            lease_id,
            f"status={status}, cleanup={cleanup}",
        )

    bindings = list(
        (
            await db.execute(
                select(
                    TestHarnessChildBinding.id,
                    TestHarnessChildBinding.state,
                    TestHarnessChildBinding.child_task_id,
                )
            )
        ).all()
    )
    for binding_id, state, child_task_id in bindings:
        add(
            "child_binding_active"
            if state not in _BINDING_TERMINAL_STATES
            else "unmigrated_child_binding_audit",
            binding_id,
            f"state={state}, child_task_id={child_task_id}",
        )

    # These tables carry user-visible reports, screenshots, finding history,
    # and at-most-once browser action audit.  TaskMigrator/relay do not import
    # them or their private archive files today.  Even terminal rows therefore
    # block destruction until a future content-hash manifest import is ACKed.
    auxiliary_evidence_tables = (
        (TestHarnessAttempt, "unmigrated_harness_attempt"),
        (TestHarnessEvent, "unmigrated_harness_event"),
        (TestHarnessEvidence, "unmigrated_harness_artifact"),
        (TestHarnessFinding, "unmigrated_harness_finding"),
        (BrowserReviewOperationReceipt, "unmigrated_browser_operation"),
    )
    for model, kind in auxiliary_evidence_tables:
        first_id = await db.scalar(select(model.id).limit(1))
        if first_id is not None:
            add(kind, first_id, "Worker-local evidence has no Manager import ACK")

    ssh_effects = list(
        (
            await db.execute(
                select(
                    TaskSSHEffectReceipt.id,
                    TaskSSHEffectReceipt.task_id,
                    TaskSSHEffectReceipt.status,
                    TaskSSHEffectReceipt.operation,
                )
            )
        ).all()
    )
    for effect_id, task_id, status, operation in ssh_effects:
        add(
            (
                "task_ssh_effect_active"
                if status == "running"
                else "unmigrated_task_ssh_effect_audit"
            ),
            effect_id,
            f"task_id={task_id}, operation={operation}, status={status}; "
            "Worker-local remote-effect audit has no Manager import ACK",
        )

    plan_runs = list(
        (
            await db.execute(
                select(PlanAgentRun.id, PlanAgentRun.status).where(
                    PlanAgentRun.status.in_(
                        ("queued", "running", "waiting_user", "cancelling")
                    )
                )
            )
        ).all()
    )
    for run_id, status in plan_runs:
        add("plan_run_active", run_id, f"status={status}")
    runtime_receipts = list(
        (
            await db.execute(
                select(
                    PlanAgentRuntimeReceipt.id,
                    PlanAgentRuntimeReceipt.status,
                    PlanAgentRuntimeReceipt.run_id,
                ).where(PlanAgentRuntimeReceipt.status != "cleaned")
            )
        ).all()
    )
    for receipt_id, status, run_id in runtime_receipts:
        add(
            "plan_runtime_unclean",
            receipt_id,
            f"run_id={run_id}, status={status}",
        )

    sub_agents = list(
        (
            await db.execute(
                select(
                    SubAgentSession.id,
                    SubAgentSession.task_id,
                    SubAgentSession.status,
                    SubAgentSession.codex_cleanup_pending,
                    SubAgentSession.active_turn_generation,
                )
            )
        ).all()
    )
    for (
        session_id,
        task_id,
        status,
        cleanup_pending,
        active_turn_generation,
    ) in sub_agents:
        active = bool(
            status == "running"
            or cleanup_pending
            or active_turn_generation is not None
        )
        add(
            "sub_agent_active" if active else "unmigrated_sub_agent_audit",
            session_id,
            f"task_id={task_id}, status={status}, "
            f"codex_cleanup_pending={cleanup_pending}, "
            f"active_turn_generation={active_turn_generation}",
        )

    first_sub_agent_report_id = await db.scalar(
        select(SubAgentReport.id).limit(1)
    )
    if first_sub_agent_report_id is not None:
        add(
            "unmigrated_sub_agent_report",
            first_sub_agent_report_id,
            "Worker-local Monitor/Sub-Agent report has no Manager import ACK",
        )

    invocations = list(
        (
            await db.execute(
                select(
                    CapabilityInvocation.id,
                    CapabilityInvocation.task_id,
                    CapabilityInvocation.status,
                )
            )
        ).all()
    )
    for invocation_id, task_id, status in invocations:
        add(
            (
                "capability_invocation_active"
                if status in ACTIVE_INVOCATION_STATUSES
                else "unmigrated_capability_invocation"
            ),
            invocation_id,
            f"task_id={task_id}, status={status}",
        )
    executions = list(
        (
            await db.execute(
                select(
                    CapabilityExecution.id,
                    CapabilityExecution.invocation_id,
                    CapabilityExecution.status,
                )
            )
        ).all()
    )
    for execution_id, invocation_id, status in executions:
        add(
            (
                "capability_execution_active"
                if status in ACTIVE_EXECUTION_STATUSES
                else "unmigrated_capability_execution"
            ),
            execution_id,
            f"invocation_id={invocation_id}, status={status}",
        )
    resumes = list(
        (
            await db.execute(
                select(
                    CapabilityResumeOutbox.id,
                    CapabilityResumeOutbox.task_id,
                    CapabilityResumeOutbox.status,
                )
            )
        ).all()
    )
    for outbox_id, task_id, status in resumes:
        active = status not in ("completed", "cancelled", "failed")
        add(
            (
                "capability_resume_active"
                if active
                else "unmigrated_capability_resume"
            ),
            outbox_id,
            f"task_id={task_id}, status={status}",
        )

    # Handoff receipts intentionally survive the source Task's terminal state
    # and may still own a recoverable G+1 launch.  The node-control lock held by
    # this proof serializes both new chat admission and receipt transitions, so
    # every locally unsettled row observed here must block destruction even
    # when its Task is a terminal high-range Worker row.  Do not include the
    # payload: it can contain the user's full prompt and attachment paths.
    unsettled_handoffs = list(
        (
            await db.execute(
                select(
                    WorkerTurnHandoffReceipt.handoff_id,
                    WorkerTurnHandoffReceipt.task_id,
                    WorkerTurnHandoffReceipt.side,
                    WorkerTurnHandoffReceipt.status,
                    WorkerTurnHandoffReceipt.source_log_id,
                )
                .where(
                    WorkerTurnHandoffReceipt.status.not_in(
                        _HANDOFF_SETTLED_STATUSES
                    )
                )
                .order_by(WorkerTurnHandoffReceipt.handoff_id)
            )
        ).all()
    )
    for handoff_id, task_id, side, status, source_log_id in unsettled_handoffs:
        add(
            "worker_turn_handoff_unsettled",
            handoff_id,
            f"task_id={task_id}, side={side}, status={status}, "
            f"source_log_id={source_log_id}",
        )

    unsettled = list(
        (
            await db.execute(
                select(
                    WorkerTaskTerminationReceipt.operation_id,
                    WorkerTaskTerminationReceipt.task_id,
                    WorkerTaskTerminationReceipt.status,
                ).where(
                    WorkerTaskTerminationReceipt.side == "worker",
                    WorkerTaskTerminationReceipt.status != "acknowledged",
                )
            )
        ).all()
    )
    for operation_id, task_id, status in unsettled:
        add(
            "termination_receipt",
            operation_id,
            f"task_id={task_id}, status={status}",
        )

    safe = blocker_count == 0
    if install_runtime_seal and not safe:
        # If this was the first seal attempt, rollback reopens phase-one
        # continuation persistence so stop/recovery can resolve the blocker.
        # An already committed seal is irreversible and remains authoritative.
        await db.rollback()
        runtime_sealed = sealed_before
    else:
        await db.commit()
        runtime_sealed = bool(sealed_before or install_runtime_seal)
    payload = {
        "protocol_version": WORKER_NODE_DRAIN_PROOF_PROTOCOL,
        "node_role": "worker",
        "drain_claim": drain_claim,
        "runtime_sealed": runtime_sealed,
        "safe_to_destroy": bool(safe and runtime_sealed),
        "blockers": blockers,
        "blocker_count": blocker_count,
        "task_count": len(tasks),
    }
    if nonce is not None:
        payload["nonce"] = nonce
    return payload


async def seal_worker_node_runtime(
    db: AsyncSession,
    *,
    drain_claim: str,
) -> dict[str, Any]:
    """Atomically install phase two only after the node is fully quiescent."""

    snapshot = await _worker_node_drain_snapshot(
        db,
        nonce=None,
        drain_claim=drain_claim,
        install_runtime_seal=True,
    )
    return {
        **snapshot,
        "safe_to_seal": snapshot["safe_to_destroy"],
    }


async def build_worker_node_drain_proof(
    db: AsyncSession,
    *,
    nonce: str,
    drain_claim: str,
) -> dict[str, Any]:
    """Return a signed-proof payload only after the exact runtime seal."""

    return await _worker_node_drain_snapshot(
        db,
        nonce=nonce,
        drain_claim=drain_claim,
        install_runtime_seal=False,
    )
