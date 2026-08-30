"""Durable ownership and launch fencing for isolated Browser Agent Tasks.

The in-memory Browser Review job is only an execution handle.  This service
persists the authoritative owner -> child relationship before a child can be
claimed, and keeps cancellation/restart recovery independent from that
ephemeral handle.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import uuid
from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from backend.database import async_session
from backend.models.instance import Instance
from backend.models.task import Task
from backend.models.test_harness import (
    BrowserReviewOperationReceipt,
    TestHarnessChildBinding,
    TestHarnessRun,
)
from backend.models.workspace_review import WorkspaceReviewRun
from backend.services.task_creation import stage_task_record
from backend.services.cancellation import finish_awaitable
from backend.services.test_harness_owner_fence import (
    TEST_HARNESS_TERMINAL_GATE_KEY,
    TestHarnessOwnerIdentity,
    lock_test_harness_owner,
    test_harness_owner_fence,
    test_harness_owner_terminal_gate_matches,
)

logger = logging.getLogger(__name__)

CHILD_RESERVED = "reserved"
CHILD_READY = "ready"
CHILD_RUNNING = "running"
CHILD_STOPPING = "stopping"
CHILD_STOPPED = "stopped"
CHILD_COMPLETED = "completed"
CHILD_STOP_FAILED = "stop_failed"
BROWSER_LAUNCH_PROFILE_VERSION = 2

# Isolated Browser Tasks are implementation details of a durable Harness /
# Workspace owner.  Only these keys may influence their launch identity.  Pool
# routing may add one provider-account binding between dequeue and launch; it
# is deliberately runtime routing state, not permission or prompt input.
BROWSER_CHILD_IDENTITY_METADATA_KEYS = frozenset(
    {
        "browser_review_job_id",
        "test_harness_run_id",
        "workspace_review_run_id",
        "test_harness_parent_task_id",
        "workspace_review_parent_task_id",
        "isolated_browser_agent",
    }
)
BROWSER_CHILD_RUNTIME_METADATA_KEYS = frozenset(
    {"claude_account_id", "codex_account_id"}
)
BROWSER_CHILD_LIFECYCLE_METADATA_KEYS = frozenset(
    {TEST_HARNESS_TERMINAL_GATE_KEY}
)
BROWSER_CHILD_ALLOWED_METADATA_KEYS = (
    BROWSER_CHILD_IDENTITY_METADATA_KEYS
    | BROWSER_CHILD_RUNTIME_METADATA_KEYS
    | BROWSER_CHILD_LIFECYCLE_METADATA_KEYS
)

_BROWSER_CHILD_TERMINAL_GATE_KEYS = frozenset(
    {
        "incarnation_id",
        "retry_count",
        "turn_generation",
        "status",
        "reason",
        "cleanup_harness_run_ids",
        "cleanup_workspace_run_ids",
        "cleanup_browser_binding_ids",
    }
)
_BROWSER_CHILD_TERMINAL_GATE_GRAPH_KEYS = (
    "cleanup_harness_run_ids",
    "cleanup_workspace_run_ids",
    "cleanup_browser_binding_ids",
)

CHILD_TERMINAL_STATES = frozenset({CHILD_STOPPED, CHILD_COMPLETED})
TASK_TERMINAL_STATUSES = frozenset(
    {"completed", "failed", "cancelled", "conflict", "superseded"}
)
TASK_ACTIVE_STATUSES = frozenset(
    {"pending_activation", "pending", "in_progress", "executing", "merging"}
)


class TestHarnessChildError(RuntimeError):
    """A Browser Agent child could not be safely attached or stopped."""


class TestHarnessChildRecoveryError(TestHarnessChildError):
    """Startup could not prove that every interrupted child was stopped."""


async def mark_permitted_browser_operations_uncertain(
    db: AsyncSession,
    binding: TestHarnessChildBinding,
    child: Task,
    *,
    reason: str,
) -> int:
    """Quarantine unacknowledged browser effects before a reap receipt.

    A durable permit proves that the child was allowed to perform an external
    browser action.  Exact process reap cannot prove whether that action ran,
    so every missing ACK becomes ``uncertain`` in the same transaction that
    terminalizes the child binding.  Identity drift is corruption and fails
    closed instead of silently rewriting unrelated receipts.
    """

    receipts = list(
        (
            await db.execute(
                select(BrowserReviewOperationReceipt)
                .where(
                    BrowserReviewOperationReceipt.binding_id == binding.id,
                    BrowserReviewOperationReceipt.status == "permitted",
                )
                .order_by(BrowserReviewOperationReceipt.id)
                .with_for_update()
            )
        ).scalars()
    )
    for receipt in receipts:
        if (
            receipt.browser_review_job_id != binding.browser_review_job_id
            or receipt.harness_run_id != binding.harness_run_id
            or receipt.workspace_review_run_id
            != binding.workspace_review_run_id
            or receipt.owner_task_id != binding.owner_task_id
            or receipt.owner_task_incarnation_id
            != binding.owner_task_incarnation_id
            or receipt.owner_task_retry_count
            != binding.owner_task_retry_count
            or receipt.owner_task_turn_generation
            != binding.owner_task_turn_generation
            or receipt.owner_task_status != binding.owner_task_status
            or receipt.child_task_id != binding.child_task_id
            or receipt.child_task_incarnation_id
            != binding.child_task_incarnation_id
            or receipt.child_task_retry_count != binding.claimed_retry_count
            or receipt.child_task_retry_count != child.retry_count
            or receipt.child_task_turn_generation != child.turn_generation
            or receipt.child_task_status not in {"in_progress", "executing"}
        ):
            raise TestHarnessChildError(
                "Browser operation permit identity changed before child reap"
            )
    if not receipts:
        return 0
    now = datetime.utcnow()
    message = reason.strip()[:4000] or (
        "Browser child exited before the permitted operation was acknowledged"
    )
    for receipt in receipts:
        receipt.status = "uncertain"
        receipt.error = receipt.error or message
        receipt.acknowledged_at = receipt.acknowledged_at or now
    return len(receipts)


TaskStopper = Callable[[int], Awaitable[None]]


def browser_child_launch_digest(task: Task) -> str:
    """Hash every Task field that can change an isolated Browser launch."""

    payload = {
        "version": BROWSER_LAUNCH_PROFILE_VERSION,
        "description": task.description,
        "mode": task.mode,
        "provider": task.provider,
        "model": task.model,
        "reasoning_effort": task.effort_level,
        "codex_service_tier": task.codex_service_tier,
        "thinking_budget": task.thinking_budget,
        "system_prompt_mode": task.system_prompt_mode,
        "target_repo": task.target_repo,
        "target_branch": task.target_branch,
        "project_id": task.project_id,
        "worker_id": task.worker_id,
        "shared_from_id": task.shared_from_id,
        "delivery_run_id": task.delivery_run_id,
        "delivery_role": task.delivery_role,
        "execution_user_id": task.execution_user_id,
        "execution_user_role": task.execution_user_role,
        "execution_mode": task.execution_mode,
        "execution_principal_kind": task.execution_principal_kind,
        "tags": task.tags,
        "enable_workflows": task.enable_workflows,
        "enabled_skills": task.enabled_skills,
        "selected_user_skills": task.selected_user_skills,
        "timeout_hours": task.timeout_hours,
        "max_retries": task.max_retries,
        "capability_policy": task.capability_policy,
        # A Browser child always owns one fresh provider turn.  The provider
        # may persist its resulting session/cwd after launch, but those values
        # can never authorize a resume, retry, fork, or chat turn.
        "resume_policy": "fresh_only",
        "initial_session_id": None,
        "initial_last_cwd": None,
        "identity_metadata": {
            key: (task.metadata_ or {}).get(key)
            for key in sorted(BROWSER_CHILD_IDENTITY_METADATA_KEYS)
        },
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def browser_child_binding_error(
    binding: TestHarnessChildBinding,
    task: Task,
) -> str | None:
    """Return why a durable Browser binding no longer matches its Task."""

    metadata = task.metadata_ if isinstance(task.metadata_, dict) else {}
    if binding.owner_task_id >= binding.child_task_id:
        return "Browser child Task lock order is invalid"
    if binding.child_task_id != task.id:
        return "Browser child Task identity changed"
    if binding.child_task_incarnation_id != task.incarnation_id:
        return "Browser child Task incarnation changed"
    if binding.launch_profile_version != BROWSER_LAUNCH_PROFILE_VERSION:
        return "Browser child launch profile is missing or unsupported"
    if binding.provider != task.provider:
        return "Browser child provider drifted from its durable binding"
    if binding.model != task.model:
        return "Browser child model drifted from its durable binding"
    if binding.reasoning_effort != task.effort_level:
        return "Browser child effort drifted from its durable binding"
    if binding.codex_service_tier != task.codex_service_tier:
        return "Browser child service tier drifted from its durable binding"
    if binding.task_mode != task.mode:
        return "Browser child mode drifted from its durable binding"
    if task.worker_id is not None:
        return "Browser child cannot route through a remote Worker"
    if task.shared_from_id is not None:
        return "Browser child cannot be a shared shadow"
    if task.delivery_run_id is not None or task.delivery_role is not None:
        return "Browser child cannot belong to a Delivery Run"
    if task.tags not in (None, {}):
        return "Browser child cannot carry workflow classification tags"
    if (
        task.target_repo not in (None, "")
        or task.target_branch not in (None, "", "main")
        or task.project_id is not None
    ):
        return "Browser child cannot inherit a repository working directory"
    unknown_metadata = set(metadata) - BROWSER_CHILD_ALLOWED_METADATA_KEYS
    if unknown_metadata:
        return "Browser child contains metadata outside its immutable allowlist"
    terminal_gate = metadata.get(TEST_HARNESS_TERMINAL_GATE_KEY)
    if terminal_gate is not None:
        # The Dispatcher deliberately installs a durable gate while moving an
        # exact Task generation from ``in_progress`` to ``executing``.  Once
        # that status transition commits, the gate describes the immediately
        # preceding status and no longer closes admission for the current
        # generation.  It must remain in metadata for crash recovery, so an
        # isolated Browser child cannot reject the field merely because it is
        # present.  Conversely, a gate that still matches the current status
        # is active terminalization authority and must continue to fail
        # closed before the provider boundary.
        if (
            not isinstance(terminal_gate, dict)
            or set(terminal_gate) - _BROWSER_CHILD_TERMINAL_GATE_KEYS
            or terminal_gate.get("incarnation_id") != task.incarnation_id
            or terminal_gate.get("retry_count") != task.retry_count
            or terminal_gate.get("turn_generation") != task.turn_generation
            or terminal_gate.get("status") not in TASK_ACTIVE_STATUSES
            or any(
                terminal_gate.get(key, []) != []
                for key in _BROWSER_CHILD_TERMINAL_GATE_GRAPH_KEYS
            )
        ):
            return "Browser child terminal gate metadata drifted"
        current_identity = TestHarnessOwnerIdentity(
            task_id=task.id,
            incarnation_id=task.incarnation_id,
            retry_count=task.retry_count,
            turn_generation=task.turn_generation,
            status=task.status,
        )
        if test_harness_owner_terminal_gate_matches(task, current_identity):
            return "Browser child generation is already terminalizing"
    if binding.state not in CHILD_TERMINAL_STATES and (
        task.session_id is not None or task.last_cwd is not None
    ):
        return "Browser child must start a fresh provider session"
    if task.enabled_skills != {"browser-review": binding.browser_review_job_id}:
        return "Browser child lost its exact MCP-only skill binding"
    if metadata.get("isolated_browser_agent") is not True:
        return "Browser child lost its isolation marker"
    if metadata.get("browser_review_job_id") != binding.browser_review_job_id:
        return "Browser child job metadata drifted from its durable binding"
    if metadata.get("test_harness_run_id") != binding.harness_run_id:
        return "Browser child Harness Run metadata drifted"
    if metadata.get("workspace_review_run_id") != binding.workspace_review_run_id:
        return "Browser child Workspace Run metadata drifted"
    if metadata.get("test_harness_parent_task_id") != binding.owner_task_id:
        return "Browser child owner metadata drifted"
    if metadata.get("workspace_review_parent_task_id") != binding.owner_task_id:
        return "Browser child workspace owner metadata drifted"
    if binding.launch_config_digest != browser_child_launch_digest(task):
        return "Browser child launch configuration drifted from its durable binding"
    return None


def browser_child_public_mutation_error(
    task: Task,
    *,
    has_binding: bool,
) -> str | None:
    """Return a stable public-control-plane rejection for a Browser child."""

    metadata = task.metadata_ if isinstance(task.metadata_, dict) else {}
    if has_binding or metadata.get("isolated_browser_agent") is True:
        return (
            "Isolated Browser Agent Tasks are managed only by their durable "
            "Harness owner"
        )
    return None


def browser_child_ssh_grant_error(
    task: Task,
    *,
    has_ssh_grant: bool,
) -> str | None:
    """Integration hook for the scoped-SSH branch.

    PR #109 owns the durable TaskSSHGrant model.  Once that branch is merged,
    its grant admission and Browser activation paths must call this hook with
    the locked exact-incarnation grant result.  Keeping the policy here makes
    Browser+SSH fail closed without coupling this branch to an unavailable
    model.
    """

    metadata = task.metadata_ if isinstance(task.metadata_, dict) else {}
    if has_ssh_grant and metadata.get("isolated_browser_agent") is True:
        return "Isolated Browser Agent Tasks cannot receive SSH grants"
    return None


def browser_child_owner_error(
    binding: TestHarnessChildBinding,
    owner: Task | None,
) -> str | None:
    if owner is None:
        return "Browser child owner Task disappeared"
    if binding.owner_task_id != owner.id:
        return "Browser child owner Task identity changed"
    if binding.owner_task_incarnation_id != owner.incarnation_id:
        return "Browser child owner Task incarnation changed"
    if binding.owner_task_retry_count != owner.retry_count:
        return "Browser child owner retry generation changed"
    if binding.owner_task_turn_generation != owner.turn_generation:
        return "Browser child owner turn generation changed"
    if binding.owner_task_status != owner.status:
        return "Browser child owner status changed"
    return None


def browser_binding_owner_identity(
    binding: TestHarnessChildBinding,
) -> TestHarnessOwnerIdentity:
    if (
        binding.owner_task_incarnation_id is None
        or binding.owner_task_retry_count is None
        or binding.owner_task_turn_generation is None
        or binding.owner_task_status is None
    ):
        raise TestHarnessChildError(
            "Browser child binding has no durable owner generation"
        )
    return TestHarnessOwnerIdentity(
        task_id=binding.owner_task_id,
        incarnation_id=binding.owner_task_incarnation_id,
        retry_count=binding.owner_task_retry_count,
        turn_generation=binding.owner_task_turn_generation,
        status=binding.owner_task_status,
    )


def require_browser_child_binding(
    binding: TestHarnessChildBinding,
    task: Task,
) -> None:
    error = browser_child_binding_error(binding, task)
    if error is not None:
        raise TestHarnessChildError(error)


class TestHarnessChildService:
    """Own the durable lifecycle of isolated Browser Agent Tasks."""

    def __init__(
        self,
        *,
        db_factory: async_sessionmaker[AsyncSession] = async_session,
        task_stopper: TaskStopper | None = None,
    ) -> None:
        self.db_factory = db_factory
        self._task_stopper = task_stopper
        self._stop_locks: dict[str, asyncio.Lock] = {}
        self._locks_guard = asyncio.Lock()

    async def reserve_child(
        self,
        *,
        owner_task_id: int,
        browser_review_job_id: str,
        child_values: Mapping[str, Any],
        harness_run_id: str | None = None,
        workspace_review_run_id: str | None = None,
    ) -> tuple[Task, TestHarnessChildBinding]:
        """Atomically persist a non-runnable child and its durable owner."""

        async with test_harness_owner_fence(owner_task_id):
            return await self._reserve_child_under_owner_fence(
                owner_task_id=owner_task_id,
                browser_review_job_id=browser_review_job_id,
                child_values=child_values,
                harness_run_id=harness_run_id,
                workspace_review_run_id=workspace_review_run_id,
            )

    async def _reserve_child_under_owner_fence(
        self,
        *,
        owner_task_id: int,
        browser_review_job_id: str,
        child_values: Mapping[str, Any],
        harness_run_id: str | None = None,
        workspace_review_run_id: str | None = None,
    ) -> tuple[Task, TestHarnessChildBinding]:
        """Persist a child while the shared owner admission fence is held."""

        if not harness_run_id and not workspace_review_run_id:
            raise TestHarnessChildError("Browser child requires a durable run owner")
        if not browser_review_job_id:
            raise TestHarnessChildError("Browser child requires a Browser Review job")

        values = dict(child_values)
        metadata = dict(values.get("metadata_") or {})
        unknown_metadata = set(metadata) - BROWSER_CHILD_RUNTIME_METADATA_KEYS
        if unknown_metadata:
            raise TestHarnessChildError(
                "Browser child metadata must be supplied by its durable owner"
            )
        metadata.update(
            {
                "browser_review_job_id": browser_review_job_id,
                "test_harness_run_id": harness_run_id,
                "workspace_review_run_id": workspace_review_run_id,
                "test_harness_parent_task_id": owner_task_id,
                "workspace_review_parent_task_id": owner_task_id,
                "isolated_browser_agent": True,
            }
        )
        values.update(status="pending_activation", metadata_=metadata)
        if values.get("session_id") is not None or values.get("last_cwd") is not None:
            raise TestHarnessChildError(
                "Browser child must be reserved without a resumable session"
            )
        if (
            values.get("target_repo") is not None
            or values.get("target_branch") is not None
            or values.get("project_id") is not None
        ):
            raise TestHarnessChildError(
                "Browser child cannot inherit an untrusted repository cwd"
            )
        values.update(
            target_repo="",
            target_branch="main",
            project_id=None,
        )
        if values.get("capability_policy") is not None:
            raise TestHarnessChildError(
                "Browser child cannot request autonomous capabilities"
            )
        if values.get("worker_id") is not None:
            raise TestHarnessChildError("Browser child must execute locally")
        if values.get("shared_from_id") is not None:
            raise TestHarnessChildError("Browser child cannot be a shared shadow")
        if (
            values.get("delivery_run_id") is not None
            or values.get("delivery_role") is not None
        ):
            raise TestHarnessChildError("Browser child cannot belong to Delivery")
        if values.get("tags") not in (None, {}):
            raise TestHarnessChildError(
                "Browser child cannot carry workflow classification tags"
            )

        async with self.db_factory() as lookup:
            run_snapshot = (
                await lookup.get(TestHarnessRun, harness_run_id)
                if harness_run_id
                else None
            )
            workspace_snapshot = (
                await lookup.get(WorkspaceReviewRun, workspace_review_run_id)
                if workspace_review_run_id
                else None
            )
            if harness_run_id and (
                run_snapshot is None
                or run_snapshot.task_id != owner_task_id
                or run_snapshot.status in TASK_TERMINAL_STATUSES
            ):
                raise TestHarnessChildError(
                    "Harness run ended, disappeared, or changed owner while reserving child"
                )
            if workspace_review_run_id and (
                workspace_snapshot is None
                or workspace_snapshot.task_id != owner_task_id
                or workspace_snapshot.status in TASK_TERMINAL_STATUSES
            ):
                raise TestHarnessChildError(
                    "Workspace review ended, disappeared, or changed owner while reserving child"
                )
            identity_source = run_snapshot or workspace_snapshot
            if (
                identity_source is None
                or identity_source.owner_task_incarnation_id is None
                or identity_source.owner_task_retry_count is None
                or identity_source.owner_task_turn_generation is None
                or identity_source.owner_task_status is None
            ):
                raise TestHarnessChildError(
                    "Browser child Run has no durable owner generation"
                )
            owner_identity = TestHarnessOwnerIdentity(
                task_id=owner_task_id,
                incarnation_id=identity_source.owner_task_incarnation_id,
                retry_count=identity_source.owner_task_retry_count,
                turn_generation=identity_source.owner_task_turn_generation,
                status=identity_source.owner_task_status,
            )
            if run_snapshot is not None and workspace_snapshot is not None and (
                run_snapshot.owner_task_incarnation_id
                != workspace_snapshot.owner_task_incarnation_id
                or run_snapshot.owner_task_retry_count
                != workspace_snapshot.owner_task_retry_count
                or run_snapshot.owner_task_turn_generation
                != workspace_snapshot.owner_task_turn_generation
                or run_snapshot.owner_task_status
                != workspace_snapshot.owner_task_status
            ):
                raise TestHarnessChildError(
                    "Harness and Workspace Runs disagree on owner generation"
                )

        # Start a fresh transaction with the owner write-CAS as its first DB
        # statement. Reusing the preceding WAL read snapshot can otherwise
        # raise SQLITE_BUSY_SNAPSHOT after a concurrent delete commits.
        async with self.db_factory() as db:
            try:
                owner = await lock_test_harness_owner(db, owner_identity)
            except RuntimeError as exc:
                raise TestHarnessChildError(str(exc)) from exc
            # Owner Task is always created before its Browser child.  Keeping
            # the durable write order owner Task -> Run -> child Task prevents
            # dequeue/delete/activate from forming a cross-row deadlock cycle.
            run = (
                (
                    await db.execute(
                        select(TestHarnessRun)
                        .where(TestHarnessRun.id == harness_run_id)
                        .with_for_update()
                        .execution_options(populate_existing=True)
                    )
                ).scalar_one_or_none()
                if harness_run_id
                else None
            )
            workspace_run = (
                (
                    await db.execute(
                        select(WorkspaceReviewRun)
                        .where(WorkspaceReviewRun.id == workspace_review_run_id)
                        .with_for_update()
                        .execution_options(populate_existing=True)
                    )
                ).scalar_one_or_none()
                if workspace_review_run_id
                else None
            )
            if harness_run_id and not _run_matches_owner(
                run,
                owner_identity,
                terminal_statuses=TASK_TERMINAL_STATUSES,
            ):
                raise TestHarnessChildError(
                    "Harness run ended, disappeared, or changed owner while reserving child"
                )
            if workspace_review_run_id and not _run_matches_owner(
                workspace_run,
                owner_identity,
                terminal_statuses=TASK_TERMINAL_STATUSES,
            ):
                raise TestHarnessChildError(
                    "Workspace review ended, disappeared, or changed owner while reserving child"
                )
            from backend.services.task_creation import (
                system_task_execution_principal_values,
            )

            child = await stage_task_record(
                db,
                **values,
                **system_task_execution_principal_values(),
            )
            # SQLAlchemy client defaults substitute the ordinary Task values
            # (``""`` / ``"main"``) even when callers provide ``None``.
            # Clear them after INSERT and before freezing the launch digest so
            # Browser children cannot inherit repository instruction discovery.
            child.target_repo = ""
            child.target_branch = "main"
            child.project_id = None
            await db.flush()
            if child.id <= owner_task_id:
                raise TestHarnessChildError(
                    "Browser child Task does not follow its owner lock identity"
                )
            binding = TestHarnessChildBinding(
                id=uuid.uuid4().hex,
                harness_run_id=harness_run_id,
                workspace_review_run_id=workspace_review_run_id,
                owner_task_id=owner_task_id,
                owner_task_incarnation_id=owner.incarnation_id,
                owner_task_retry_count=owner.retry_count,
                owner_task_turn_generation=owner.turn_generation,
                owner_task_status=owner.status,
                child_task_id=child.id,
                child_task_incarnation_id=child.incarnation_id,
                browser_review_job_id=browser_review_job_id,
                launch_profile_version=BROWSER_LAUNCH_PROFILE_VERSION,
                provider=child.provider,
                model=child.model,
                reasoning_effort=child.effort_level,
                codex_service_tier=child.codex_service_tier,
                task_mode=child.mode,
                launch_config_digest=browser_child_launch_digest(child),
                state=CHILD_RESERVED,
            )
            db.add(binding)
            if run is not None:
                run.agent_task_id = child.id
                run.browser_review_job_id = browser_review_job_id
            if workspace_run is not None:
                workspace_run.agent_task_id = child.id
                workspace_run.browser_review_job_id = browser_review_job_id
            await db.commit()
            await db.refresh(child)
            await db.refresh(binding)
            return child, binding

    async def activate(self, binding_id: str) -> TestHarnessChildBinding:
        """Publish the child to TaskQueue only after its job is attached."""

        async with self.db_factory() as db:
            owner_task_id = await db.scalar(
                select(TestHarnessChildBinding.owner_task_id).where(
                    TestHarnessChildBinding.id == binding_id
                )
            )
        if owner_task_id is None:
            raise TestHarnessChildError("Browser child binding disappeared")
        async with test_harness_owner_fence(owner_task_id):
            return await self._activate_under_owner_fence(binding_id)

    async def _activate_under_owner_fence(
        self,
        binding_id: str,
    ) -> TestHarnessChildBinding:
        """Activate only while deletion cannot cross the owner boundary."""

        async with self.db_factory() as lookup:
            binding_snapshot = (
                await lookup.execute(
                    select(TestHarnessChildBinding)
                    .where(TestHarnessChildBinding.id == binding_id)
                )
            ).scalar_one_or_none()
            if binding_snapshot is None:
                raise TestHarnessChildError("Browser child binding disappeared")
            if binding_snapshot.state not in {CHILD_RESERVED, CHILD_READY}:
                raise TestHarnessChildError(
                    "Browser child cannot activate from state "
                    f"{binding_snapshot.state}"
                )
            if binding_snapshot.owner_task_id >= binding_snapshot.child_task_id:
                raise TestHarnessChildError("Browser child Task lock order is invalid")
            owner_identity = browser_binding_owner_identity(binding_snapshot)

        # See reserve_child: the durable Task writer fence must begin a fresh
        # transaction, not upgrade a potentially stale SQLite WAL snapshot.
        async with self.db_factory() as db:
            try:
                owner = await lock_test_harness_owner(
                    db,
                    owner_identity,
                )
            except RuntimeError as exc:
                raise TestHarnessChildError(str(exc)) from exc
            binding = (
                await db.execute(
                    select(TestHarnessChildBinding)
                    .where(TestHarnessChildBinding.id == binding_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            if binding is None:
                raise TestHarnessChildError("Browser child binding disappeared")
            if browser_binding_owner_identity(binding) != owner_identity:
                raise TestHarnessChildError(
                    "Browser child owner generation changed during activation"
                )
            if binding.state not in {CHILD_RESERVED, CHILD_READY}:
                raise TestHarnessChildError(
                    f"Browser child cannot activate from state {binding.state}"
                )
            owner_error = browser_child_owner_error(binding, owner)
            if owner_error is not None:
                raise TestHarnessChildError(owner_error)
            child = (
                await db.execute(
                    select(Task)
                    .where(Task.id == binding.child_task_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if child is None or child.status != "pending_activation":
                if not (
                    binding.state == CHILD_READY
                    and child is not None
                    and child.status == "pending"
                ):
                    raise TestHarnessChildError(
                        "Browser child disappeared or escaped its activation gate"
                    )
            assert child is not None
            require_browser_child_binding(binding, child)
            if binding.harness_run_id:
                run = await db.get(TestHarnessRun, binding.harness_run_id)
                if (
                    run is None
                    or run.task_id != binding.owner_task_id
                    or run.status in TASK_TERMINAL_STATUSES
                    or run.owner_task_incarnation_id
                    != binding.owner_task_incarnation_id
                    or run.owner_task_retry_count
                    != binding.owner_task_retry_count
                    or run.owner_task_turn_generation
                    != binding.owner_task_turn_generation
                    or run.owner_task_status != binding.owner_task_status
                ):
                    raise TestHarnessChildError(
                        "Harness run ended before Browser child activation"
                    )
            workspace_run = None
            if binding.workspace_review_run_id:
                workspace_run = await db.get(
                    WorkspaceReviewRun,
                    binding.workspace_review_run_id,
                )
                if (
                    workspace_run is None
                    or workspace_run.task_id != binding.owner_task_id
                    or workspace_run.status in TASK_TERMINAL_STATUSES
                    or workspace_run.owner_task_incarnation_id
                    != binding.owner_task_incarnation_id
                    or workspace_run.owner_task_retry_count
                    != binding.owner_task_retry_count
                    or workspace_run.owner_task_turn_generation
                    != binding.owner_task_turn_generation
                    or workspace_run.owner_task_status
                    != binding.owner_task_status
                ):
                    raise TestHarnessChildError(
                        "Workspace review ended before Browser child activation"
                    )
            if binding.state == CHILD_READY:
                return binding
            now = datetime.utcnow()
            child.status = "pending"
            binding.state = CHILD_READY
            binding.activated_at = now
            binding.error = None
            if workspace_run is not None:
                workspace_run.status = "reviewing"
                workspace_run.stage = "browser_agent_queued"
            await db.commit()
            await db.refresh(binding)
            return binding

    async def abort_reservation(self, binding_id: str, exc: BaseException) -> None:
        """Close a child that failed before its launch gate opened."""

        error = _safe_error(exc)
        async with self.db_factory() as db:
            binding = (
                await db.execute(
                    select(TestHarnessChildBinding)
                    .where(TestHarnessChildBinding.id == binding_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if binding is None:
                return
            if binding.state in CHILD_TERMINAL_STATES:
                return
            if binding.state != CHILD_RESERVED:
                raise TestHarnessChildError(
                    "Browser child reservation can only abort before activation"
                )
            child = await db.get(Task, binding.child_task_id)
            if child is None or child.status != "pending_activation":
                raise TestHarnessChildError(
                    "Browser child escaped its reservation before abort"
                )
            owner = await db.scalar(
                select(Instance.id).where(
                    Instance.current_task_id == binding.child_task_id
                )
            )
            if owner is not None:
                raise TestHarnessChildError(
                    "Reserved Browser child unexpectedly owns an Instance"
                )
            child.status = "cancelled"
            child.completed_at = datetime.utcnow()
            child.error_message = error
            binding.state = CHILD_STOPPED
            binding.stop_requested_at = binding.stop_requested_at or datetime.utcnow()
            binding.completed_at = datetime.utcnow()
            binding.error = error
            await db.commit()

    async def mark_terminal_by_child(
        self,
        child_task_id: int,
        *,
        task_status: str | None = None,
        error: str | None = None,
    ) -> bool:
        """Observe the durable reap receipt written by lifecycle cleanup.

        A terminal Task row is not process evidence.  The Browser watcher may
        therefore only consume an already-terminal binding; it must never
        manufacture that receipt from Task status alone.
        """

        async with self.db_factory() as db:
            binding = await db.scalar(
                select(TestHarnessChildBinding).where(
                    TestHarnessChildBinding.child_task_id == child_task_id
                )
            )
            return bool(
                binding is not None
                and binding.state in CHILD_TERMINAL_STATES
                and binding.completed_at is not None
            )

    async def stop_for_harness_run(self, run_id: str, *, reason: str) -> bool:
        binding_id = await self._binding_id(harness_run_id=run_id)
        if binding_id is None:
            return False
        await self.stop_binding(binding_id, reason=reason)
        return True

    async def stop_for_workspace_run(self, run_id: str, *, reason: str) -> bool:
        binding_id = await self._binding_id(workspace_review_run_id=run_id)
        if binding_id is None:
            return False
        await self.stop_binding(binding_id, reason=reason)
        return True

    async def stop_for_owner(self, task_id: int, *, reason: str) -> int:
        async with self.db_factory() as db:
            binding_ids = list(
                (
                    await db.execute(
                        select(TestHarnessChildBinding.id).where(
                            TestHarnessChildBinding.owner_task_id == task_id,
                            TestHarnessChildBinding.state.not_in(
                                CHILD_TERMINAL_STATES
                            ),
                        )
                    )
                ).scalars()
            )
        for binding_id in binding_ids:
            await self.stop_binding(binding_id, reason=reason)
        return len(binding_ids)

    async def stop_binding(self, binding_id: str, *, reason: str) -> None:
        """Stop and verify one exact child generation before returning."""

        lock = await self._stop_lock(binding_id)
        async with lock:
            await _finish_despite_cancellation(
                self._stop_binding_impl(binding_id, reason=reason)
            )

    async def _stop_binding_impl(self, binding_id: str, *, reason: str) -> None:
        child_task_id: int
        job_id: str
        child_incarnation_id: str
        expected_generation: Any
        async with self.db_factory() as db:
            # ``FOR UPDATE`` does not reserve a SQLite WAL writer.  Make the
            # binding the first portable writer so another Manager process
            # cannot publish a success/failure receipt from a stale snapshot.
            await db.execute(
                update(TestHarnessChildBinding)
                .where(TestHarnessChildBinding.id == binding_id)
                .values(state=TestHarnessChildBinding.state)
            )
            binding = (
                await db.execute(
                    select(TestHarnessChildBinding)
                    .where(TestHarnessChildBinding.id == binding_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if binding is None:
                raise TestHarnessChildError("Browser child binding disappeared")
            if binding.state in CHILD_TERMINAL_STATES:
                return
            child = (
                await db.execute(
                    select(Task)
                    .where(Task.id == binding.child_task_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if (
                child is None
                or not child.incarnation_id
                or child.incarnation_id != binding.child_task_incarnation_id
            ):
                raise TestHarnessChildError(
                    "Browser child Task incarnation disappeared before stop"
                )
            has_claim = (
                binding.claimed_retry_count is not None
                or binding.claimed_instance_id is not None
            )
            if binding.state == CHILD_RUNNING or (
                binding.state in {CHILD_STOPPING, CHILD_STOP_FAILED}
                and has_claim
            ):
                if (
                    binding.claimed_retry_count != child.retry_count
                    or binding.claimed_instance_id != child.instance_id
                    or binding.claimed_instance_id is None
                ):
                    raise TestHarnessChildError(
                        "Browser child claim changed before stop"
                    )
            elif binding.state in {CHILD_RESERVED, CHILD_READY} or (
                binding.state in {CHILD_STOPPING, CHILD_STOP_FAILED}
                and not has_claim
            ):
                if (
                    binding.claimed_retry_count is not None
                    or binding.claimed_instance_id is not None
                ):
                    raise TestHarnessChildError(
                        "Unlaunched Browser child contains a stale claim"
                    )
            else:
                raise TestHarnessChildError(
                    f"Browser child cannot stop from state {binding.state}"
                )
            from backend.services.task_termination import local_task_generation

            expected_generation = local_task_generation(child)
            binding.state = CHILD_STOPPING
            binding.stop_requested_at = binding.stop_requested_at or datetime.utcnow()
            binding.error = reason[:4000]
            child_task_id = binding.child_task_id
            child_incarnation_id = child.incarnation_id
            job_id = binding.browser_review_job_id
            await db.commit()

        try:
            from backend.services.browser_review_jobs import browser_review_job_manager

            await browser_review_job_manager.mark_cancelling(job_id)
            await self._stop_task(
                child_task_id,
                expected_generation=expected_generation,
            )
            await browser_review_job_manager.cancel(job_id)
            await self._verify_child_terminal(
                child_task_id,
                expected_generation=expected_generation,
                expected_incarnation_id=child_incarnation_id,
            )
        except BaseException as exc:
            async with self.db_factory() as db:
                # Failure is a one-way CAS from this operation's published
                # ``stopping`` state.  It can never overwrite a terminal
                # success committed by a concurrent cleanup executor.
                result = await db.execute(
                    update(TestHarnessChildBinding)
                    .where(
                        TestHarnessChildBinding.id == binding_id,
                        TestHarnessChildBinding.state == CHILD_STOPPING,
                    )
                    .values(
                        state=CHILD_STOP_FAILED,
                        error=_safe_error(exc),
                    )
                )
                await db.commit()
                if result.rowcount == 0:
                    binding = await db.get(TestHarnessChildBinding, binding_id)
                    if (
                        binding is not None
                        and binding.state in CHILD_TERMINAL_STATES
                    ):
                        return
            raise TestHarnessChildError(
                f"Browser child {child_task_id} cleanup could not be proven: "
                f"{_safe_error(exc)}"
            ) from exc

        async with self.db_factory() as db:
            await db.execute(
                update(TestHarnessChildBinding)
                .where(TestHarnessChildBinding.id == binding_id)
                .values(state=TestHarnessChildBinding.state)
            )
            binding = (
                await db.execute(
                    select(TestHarnessChildBinding)
                    .where(TestHarnessChildBinding.id == binding_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if binding is None:
                raise TestHarnessChildError("Browser child binding disappeared after stop")
            if binding.state in CHILD_TERMINAL_STATES:
                child = await db.get(Task, child_task_id)
                if child is None or child.incarnation_id != child_incarnation_id:
                    raise TestHarnessChildError(
                        "Browser child disappeared before operation receipt cleanup"
                    )
                await mark_permitted_browser_operations_uncertain(
                    db,
                    binding,
                    child,
                    reason=(
                        "Browser child was reaped before a permitted operation "
                        "was acknowledged"
                    ),
                )
                await db.commit()
                return
            if binding.state not in {CHILD_STOPPING, CHILD_STOP_FAILED}:
                raise TestHarnessChildError(
                    "Browser child stop receipt lost its recoverable state"
                )
            child = await db.get(Task, child_task_id)
            if child is None or child.incarnation_id != child_incarnation_id:
                raise TestHarnessChildError(
                    "Browser child incarnation changed before stop receipt"
                )
            from backend.services.task_termination import (
                local_task_generation,
                normalize_local_task_generation,
            )

            expected = normalize_local_task_generation(expected_generation)
            current = normalize_local_task_generation(local_task_generation(child))
            if (
                current.retry_count != expected.retry_count
                or current.turn_generation != expected.turn_generation
                or current.instance_id != expected.instance_id
                or current.started_at != expected.started_at
            ):
                raise TestHarnessChildError(
                    "Browser child generation changed before stop receipt"
                )
            owner = await db.scalar(
                select(Instance.id).where(Instance.current_task_id == child_task_id)
            )
            if owner is not None:
                raise TestHarnessChildError(
                    f"Browser child still owns Instance {owner} before stop receipt"
                )
            await mark_permitted_browser_operations_uncertain(
                db,
                binding,
                child,
                reason=(
                    "Browser child was stopped before a permitted operation "
                    "was acknowledged"
                ),
            )
            binding.state = (
                CHILD_COMPLETED
                if child is not None and child.status != "cancelled"
                else CHILD_STOPPED
            )
            binding.completed_at = datetime.utcnow()
            binding.error = None
            await db.commit()

    async def recover_interrupted(self) -> int:
        """Reap all nonterminal/legacy children before Dispatcher starts."""

        recovered = 0
        failures: list[str] = []
        await self._adopt_legacy_children(failures)
        async with self.db_factory() as db:
            bindings = list(
                (
                    await db.execute(
                        select(TestHarnessChildBinding).where(
                            TestHarnessChildBinding.state.not_in(
                                CHILD_TERMINAL_STATES
                            )
                        )
                    )
                ).scalars()
            )
            binding_ids = [binding.id for binding in bindings]
        for binding_id in binding_ids:
            try:
                await self.stop_binding(
                    binding_id,
                    reason="Manager restarted before Browser Agent cleanup completed",
                )
                recovered += 1
            except Exception as exc:
                logger.exception("Could not recover Browser child %s", binding_id)
                failures.append(f"{binding_id}: {_safe_error(exc)}")
        if failures:
            raise TestHarnessChildRecoveryError(
                "Interrupted Browser Agent cleanup failed: " + "; ".join(failures)
            )
        return recovered

    async def _adopt_legacy_children(self, failures: list[str]) -> None:
        """Fence active pre-migration isolated Tasks so they cannot launch."""

        async with self.db_factory() as db:
            tasks = list(
                (
                    await db.execute(
                        select(Task).where(Task.status.in_(TASK_ACTIVE_STATUSES))
                    )
                ).scalars()
            )
            for task in tasks:
                metadata = dict(task.metadata_ or {})
                if metadata.get("isolated_browser_agent") is not True:
                    continue
                existing = await db.scalar(
                    select(TestHarnessChildBinding.id).where(
                        TestHarnessChildBinding.child_task_id == task.id
                    )
                )
                if existing is not None:
                    continue
                harness_run_id = _metadata_id(metadata.get("test_harness_run_id"))
                workspace_run_id = _metadata_id(
                    metadata.get("workspace_review_run_id")
                )
                job_id = _metadata_id(metadata.get("browser_review_job_id"))
                owner_task_id = metadata.get("test_harness_parent_task_id") or metadata.get(
                    "workspace_review_parent_task_id"
                )
                if (
                    not (harness_run_id or workspace_run_id)
                    or not job_id
                    or type(owner_task_id) is not int
                ):
                    failures.append(
                        f"Task {task.id}: legacy Browser child identity is incomplete"
                    )
                    continue
                owner = await db.get(Task, owner_task_id)
                if owner is None or not owner.incarnation_id or not task.incarnation_id:
                    failures.append(
                        f"Task {task.id}: legacy Browser owner identity is missing"
                    )
                    continue
                task.archived = True
                db.add(
                    TestHarnessChildBinding(
                        id=uuid.uuid4().hex,
                        harness_run_id=harness_run_id,
                        workspace_review_run_id=workspace_run_id,
                        owner_task_id=owner_task_id,
                        owner_task_incarnation_id=owner.incarnation_id,
                        owner_task_retry_count=owner.retry_count,
                        owner_task_turn_generation=owner.turn_generation,
                        owner_task_status=owner.status,
                        child_task_id=task.id,
                        child_task_incarnation_id=task.incarnation_id,
                        browser_review_job_id=job_id,
                        state=CHILD_STOP_FAILED,
                        error="Adopted during startup recovery",
                    )
                )
            await db.commit()

    async def _binding_id(
        self,
        *,
        harness_run_id: str | None = None,
        workspace_review_run_id: str | None = None,
    ) -> str | None:
        async with self.db_factory() as db:
            predicates = []
            if harness_run_id:
                predicates.append(
                    TestHarnessChildBinding.harness_run_id == harness_run_id
                )
            if workspace_review_run_id:
                predicates.append(
                    TestHarnessChildBinding.workspace_review_run_id
                    == workspace_review_run_id
                )
            if not predicates:
                return None
            return await db.scalar(
                select(TestHarnessChildBinding.id).where(*predicates)
            )

    async def _stop_task(
        self,
        task_id: int,
        *,
        expected_generation: Any,
    ) -> None:
        if self._task_stopper is not None:
            await self._task_stopper(task_id)
            return
        from backend.services.task_termination import (
            local_task_generation,
            normalize_local_task_generation,
        )

        async with self.db_factory() as db:
            task = await db.get(Task, task_id)
            if task is None:
                raise TestHarnessChildError("Browser child Task disappeared")
            expected = normalize_local_task_generation(expected_generation)
            current = normalize_local_task_generation(local_task_generation(task))
            same_execution = (
                current.retry_count == expected.retry_count
                and current.turn_generation == expected.turn_generation
                and current.instance_id == expected.instance_id
                and current.started_at == expected.started_at
            )
            if not same_execution:
                raise TestHarnessChildError(
                    "Browser child execution generation changed before stop"
                )
            if task.status == "pending_activation":
                owner = await db.scalar(
                    select(Instance.id).where(Instance.current_task_id == task_id)
                )
                if owner is not None:
                    raise TestHarnessChildError(
                        "A gated Browser child unexpectedly owns an Instance"
                    )
                task.status = "cancelled"
                task.completed_at = datetime.utcnow()
                task.error_message = "Browser Agent stopped before activation"
                await db.commit()
                return
            terminal_status = task.status if task.status in TASK_TERMINAL_STATUSES else None
            if terminal_status is not None:
                owner_row = (
                    await db.execute(
                        select(
                            Instance.id,
                            Instance.pid,
                            Instance.started_at,
                        ).where(Instance.current_task_id == task_id)
                    )
                ).one_or_none()
                await db.rollback()
                if owner_row is None:
                    return
                if owner_row.id != expected.instance_id:
                    raise TestHarnessChildError(
                        "Browser child reverse Instance generation changed"
                    )
                from backend.main import instance_manager

                stop_status = (
                    terminal_status
                    if terminal_status in {"completed", "failed", "cancelled"}
                    else "cancelled"
                )
                await instance_manager.stop(
                    owner_row.id,
                    expected_task_id=task_id,
                    expected_task_turn_generation=expected.turn_generation,
                    expected_pid=owner_row.pid,
                    expected_started_at=owner_row.started_at,
                    task_status=stop_status,
                    terminal_consumer_timeout=30.0,
                    consumer_cancel_timeout=10.0,
                )
                return
        from backend.api.tasks import _cancel_local_task_under_cancellation_lease
        from backend.main import dispatcher

        async with self.db_factory() as db:
            async with dispatcher.task_queue_cancellation_lease(task_id):
                await _cancel_local_task_under_cancellation_lease(task_id, db)

    async def _verify_child_terminal(
        self,
        task_id: int,
        *,
        expected_generation: Any,
        expected_incarnation_id: str,
    ) -> None:
        from backend.services.task_termination import (
            local_task_generation,
            normalize_local_task_generation,
        )

        async with self.db_factory() as db:
            child = await db.get(Task, task_id)
            if child is None or child.incarnation_id != expected_incarnation_id:
                raise TestHarnessChildError(
                    "Browser child incarnation changed during cancellation"
                )
            expected = normalize_local_task_generation(expected_generation)
            current = normalize_local_task_generation(local_task_generation(child))
            if (
                current.retry_count != expected.retry_count
                or current.turn_generation != expected.turn_generation
                or current.instance_id != expected.instance_id
                or current.started_at != expected.started_at
            ):
                raise TestHarnessChildError(
                    "Browser child generation changed during cancellation"
                )
            if child.status not in TASK_TERMINAL_STATUSES:
                raise TestHarnessChildError(
                    f"Browser child remained {child.status} after cancellation"
                )
            owner = await db.scalar(
                select(Instance.id).where(Instance.current_task_id == task_id)
            )
            if owner is not None:
                raise TestHarnessChildError(
                    f"Browser child still owns Instance {owner} after cancellation"
                )

    async def _stop_lock(self, binding_id: str) -> asyncio.Lock:
        async with self._locks_guard:
            return self._stop_locks.setdefault(binding_id, asyncio.Lock())


async def finalize_reaped_browser_child_binding(
    db: AsyncSession,
    task: Task,
    *,
    instance_id: int,
) -> bool:
    """Write the Browser binding receipt after exact runtime reap proof.

    The caller must hold the exact Instance lifecycle lock and must already
    have observed ``InstanceManager.is_running(instance_id) is False``.  It
    either clears the exact reverse Instance owner in this transaction, or
    fences the pid-less unowned terminal snapshot previously committed by the
    exact output consumer. Persisting the binding terminal state here makes
    that proof recoverable after restart.
    """

    binding = (
        await db.execute(
            select(TestHarnessChildBinding)
            .where(TestHarnessChildBinding.child_task_id == task.id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if binding is None:
        return False
    if binding.state in CHILD_TERMINAL_STATES:
        await mark_permitted_browser_operations_uncertain(
            db,
            binding,
            task,
            reason=(
                "Browser child was reaped before a permitted operation "
                "was acknowledged"
            ),
        )
        return True
    if task.status not in TASK_TERMINAL_STATUSES:
        raise TestHarnessChildError(
            "Browser child reap receipt requires a terminal Task generation"
        )
    if (
        binding.state not in {CHILD_RUNNING, CHILD_STOPPING}
        or binding.child_task_incarnation_id != task.incarnation_id
        or binding.claimed_retry_count != task.retry_count
        or binding.claimed_instance_id != instance_id
        or task.instance_id != instance_id
    ):
        raise TestHarnessChildError(
            "Browser child claim changed before its exact reap receipt"
        )
    remaining_owner = await db.scalar(
        select(Instance.id).where(Instance.current_task_id == task.id)
    )
    if remaining_owner is not None:
        raise TestHarnessChildError(
            f"Browser child still owns Instance {remaining_owner}"
        )
    await mark_permitted_browser_operations_uncertain(
        db,
        binding,
        task,
        reason=(
            "Browser child exited before a permitted operation was acknowledged"
        ),
    )
    binding.state = (
        CHILD_STOPPED if task.status == "cancelled" else CHILD_COMPLETED
    )
    binding.completed_at = datetime.utcnow()
    binding.error = task.error_message
    return True


async def _finish_despite_cancellation(awaitable: Awaitable[None]) -> None:
    await finish_awaitable(awaitable)


def _metadata_id(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _run_matches_owner(
    run: TestHarnessRun | WorkspaceReviewRun | None,
    identity: TestHarnessOwnerIdentity,
    *,
    terminal_statuses: frozenset[str],
) -> bool:
    return bool(
        run is not None
        and run.task_id == identity.task_id
        and run.status not in terminal_statuses
        and run.owner_task_incarnation_id == identity.incarnation_id
        and run.owner_task_retry_count == identity.retry_count
        and run.owner_task_turn_generation == identity.turn_generation
        and run.owner_task_status == identity.status
    )


def _safe_error(exc: BaseException) -> str:
    text = str(exc).strip() or exc.__class__.__name__
    return text[:4000]


test_harness_child_service = TestHarnessChildService()
