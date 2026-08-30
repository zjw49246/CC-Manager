"""Durable, provider-neutral frontend test harness orchestration."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Awaitable, Callable

import httpx
from sqlalchemy import delete, or_, select, update

from backend.config import settings
from backend.database import async_session
from backend.models.instance import Instance
from backend.models.project import Project
from backend.models.task import Task
from backend.models.task_ssh_grant import TaskSSHGrant
from backend.models.test_harness import (
    TestHarnessAttempt,
    TestHarnessChildBinding,
    TestHarnessEvent,
    TestHarnessEvidence,
    TestHarnessFinding,
    TestHarnessRun,
    TestHarnessSandboxLease,
)
from backend.models.workspace_review import WorkspaceReviewRun
from backend.services.cancellation import finish_awaitable, settle_awaitable
from backend.services.test_harness_contracts import (
    DEFAULT_BROWSER_CHANNEL,
    HARNESS_TERMINAL_STATUSES,
    TestHarnessContractError,
    TestHarnessSpec,
    compile_test_plan,
    normalize_verdict,
    request_fingerprint,
)
from backend.services.test_harness_targets import (
    test_harness_target_manager,
    untrusted_git_target_capability,
)
from backend.services.test_harness_runtime import resolve_harness_runtime
from backend.services.test_harness_execution_context import (
    TestHarnessExecutionContextError,
    execution_context_from_runtime,
    freeze_harness_execution_context,
    frozen_git_project,
    public_harness_runtime,
    runtime_with_execution_context,
)
from backend.services.test_harness_artifacts import (
    OpenedHarnessArtifact,
    TestHarnessArtifactError,
    TestHarnessArtifactStore,
    test_harness_artifact_store,
)
from backend.services.test_harness_owner_fence import (
    TEST_HARNESS_TERMINAL_GATE_KEY,
    TestHarnessOwnerIdentity,
    install_test_harness_owner_terminal_gate,
    lock_test_harness_owner,
    test_harness_owner_fence,
    test_harness_owner_identity,
    test_harness_owner_locality_error,
)
from backend.services.worker_node_control import fence_worker_node_mutation


logger = logging.getLogger(__name__)

_WORKSPACE_TERMINAL = frozenset({"completed", "failed", "cancelled"})
_BROWSER_TERMINAL = frozenset({"completed", "failed", "cancelled"})
_PUBLIC_TERMINAL_OWNER_STATUSES = frozenset(
    {"completed", "failed", "cancelled", "conflict"}
)
_TERMINAL_GATE_RUN_IDS_KEY = "cleanup_harness_run_ids"
_TERMINAL_GATE_WORKSPACE_IDS_KEY = "cleanup_workspace_run_ids"
_TERMINAL_GATE_BINDING_IDS_KEY = "cleanup_browser_binding_ids"
ARCHIVE_STAGING = "staging"
ARCHIVE_ARCHIVING = "archiving"
ARCHIVE_COMPLETE = "complete"
ARCHIVE_RETRYABLE_ERROR = "retryable_error"
ARCHIVE_INCOMPLETE = "incomplete"
_ARCHIVE_RECOVERABLE_STATES = frozenset(
    {ARCHIVE_STAGING, ARCHIVE_ARCHIVING, ARCHIVE_RETRYABLE_ERROR}
)
_EVIDENCE_NAME_RE = re.compile(
    r"(?:initial\.png|final\.png|step-\d{2,3}\.png|report\.md|"
    r"telemetry\.json|response\.json|actions\.jsonl)"
)
_FRONTEND_PATH_SUFFIXES = {
    ".astro",
    ".css",
    ".gif",
    ".html",
    ".ico",
    ".jpeg",
    ".js",
    ".jpg",
    ".jsx",
    ".less",
    ".mjs",
    ".png",
    ".sass",
    ".scss",
    ".svelte",
    ".svg",
    ".ts",
    ".tsx",
    ".ttf",
    ".vue",
    ".webp",
    ".woff",
    ".woff2",
}


def _owner_has_in_process_runtime_evidence(task_id: int) -> bool:
    """Snapshot hidden launch/background evidence while admission has no await."""

    try:
        from backend.main import instance_manager
    except Exception:  # pragma: no cover - startup import failures are fatal elsewhere.
        return True
    if instance_manager is None:
        return False
    reservations = getattr(instance_manager, "_launch_reservations", {})
    if isinstance(reservations, dict) and any(
        getattr(reservation, "task_id", None) == task_id
        for reservation in reservations.values()
    ):
        return True
    active_background_ids = getattr(
        instance_manager,
        "active_pty_background_task_ids",
        None,
    )
    if callable(active_background_ids) and task_id in active_background_ids():
        return True
    return False


async def _require_terminal_owner_runtime_idle(
    db,
    owner: Task,
) -> None:
    """Do not start post-turn Browser work before the exact runtime is reaped."""

    if owner.status not in _PUBLIC_TERMINAL_OWNER_STATUSES:
        return
    if owner.instance_id is not None:
        raise TestHarnessBusyError(
            "Harness owner Task is terminal but its Instance claim has not "
            "settled"
        )
    if owner.pty_background_generation is not None:
        raise TestHarnessBusyError(
            "Harness owner Task is terminal but its PTY background generation "
            "has not settled"
        )
    reverse_instance = await db.scalar(
        select(Instance.id)
        .where(Instance.current_task_id == owner.id)
        .with_for_update()
        .limit(1)
    )
    if reverse_instance is not None:
        raise TestHarnessBusyError(
            "Harness owner Task is terminal but a reverse Instance owner has "
            "not settled"
        )
    if _owner_has_in_process_runtime_evidence(owner.id):
        raise TestHarnessBusyError(
            "Harness owner Task is terminal but an in-process launch or PTY "
            "runtime has not settled"
        )
_CONTENT_TYPES = {
    ".png": "image/png",
    ".md": "text/markdown; charset=utf-8",
    ".json": "application/json",
    ".jsonl": "application/x-ndjson",
}


def _looks_frontend_facing(path: str) -> bool:
    normalized = path.lower()
    parts = normalized.split("/")
    if any(part in {"frontend", "client", "web", "ui", "public"} for part in parts):
        return True
    name = parts[-1]
    if name in {
        "package.json",
        "vite.config.js",
        "vite.config.mjs",
        "vite.config.ts",
    }:
        return True
    return Path(name).suffix in _FRONTEND_PATH_SUFFIXES


def _git_browser_target_context(run: TestHarnessRun) -> dict[str, Any] | None:
    resolved = run.resolved_target
    if run.target_kind not in {"pull_request", "git_ref"} or not isinstance(
        resolved, dict
    ):
        return None
    changed_files: list[dict[str, Any]] = []
    raw_files = resolved.get("changed_files")
    if isinstance(raw_files, list):
        for item in raw_files:
            if not isinstance(item, dict) or not isinstance(item.get("path"), str):
                continue
            changed_files.append(
                {
                    key: item[key]
                    for key in (
                        "path",
                        "previous_path",
                        "status",
                        "additions",
                        "deletions",
                        "changes",
                    )
                    if key in item
                }
            )
    return {
        "kind": run.target_kind,
        "repository": resolved.get("repository"),
        "pr_number": resolved.get("pr_number"),
        "base_sha": resolved.get("base_sha"),
        "head_sha": resolved.get("head_sha"),
        "source_ref": resolved.get("source_ref"),
        "changed_files": changed_files,
        "frontend_changed_files": [
            item for item in changed_files if _looks_frontend_facing(str(item["path"]))
        ],
    }


class TestHarnessError(RuntimeError):
    """Safe user-visible harness failure."""


class TestHarnessBusyError(TestHarnessError):
    """The Task already owns a non-terminal harness run."""


class TestHarnessIdempotencyError(TestHarnessError):
    """An idempotency key was reused for different immutable input."""


class TestHarnessService:
    """Facade shared by Task chat, MCP, Goal, and future loops."""

    def __init__(
        self,
        *,
        db_factory=async_session,
        poll_interval: float = 0.5,
        artifact_store: TestHarnessArtifactStore | None = None,
        retention_interval: float | None = None,
        target_manager: Any | None = None,
        sandbox_manager: Any | None = None,
        child_service: Any | None = None,
    ) -> None:
        self.db_factory = db_factory
        self.poll_interval = poll_interval
        self.artifact_store = artifact_store or test_harness_artifact_store
        self.retention_interval = float(
            retention_interval
            if retention_interval is not None
            else settings.test_harness_artifact_cleanup_interval_seconds
        )
        self.target_manager = target_manager or test_harness_target_manager
        if sandbox_manager is None:
            from backend.services.test_harness_sandbox import (
                test_harness_sandbox_manager,
            )

            sandbox_manager = test_harness_sandbox_manager
        self.sandbox_manager = sandbox_manager
        if child_service is None:
            from backend.services.test_harness_children import (
                TestHarnessChildService,
            )

            child_service = TestHarnessChildService(db_factory=db_factory)
        self.child_service = child_service
        self._pipelines: dict[str, asyncio.Task[None]] = {}
        self._retention_task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()
        self._db_lock = asyncio.Lock()

    async def start_task_run(
        self,
        *,
        task_id: int,
        spec: TestHarnessSpec,
        owner_user_id: int | None = None,
        owner_identity: TestHarnessOwnerIdentity | None = None,
        preview_config_override: dict[str, Any] | None = None,
    ) -> TestHarnessRun:
        normalized = spec.normalized()
        async with test_harness_owner_fence(task_id):
            run, created, plan = await self._admit_task_run_under_owner_fence(
                task_id=task_id,
                spec=normalized,
                owner_user_id=owner_user_id,
                owner_identity=owner_identity,
                preview_config_override=preview_config_override,
            )
            if not created:
                return run

            # Fixed-URL and Git targets do not perform the Manager-hosted Git
            # snapshot used by current-workspace reviews. Preserve their
            # existing prepare/launch admission fence while letting the slow
            # current-workspace adapter run after this exact Run is durable.
            if normalized.target_kind != "current_workspace":
                return await self._activate_admitted_task_run(
                    run=run,
                    spec=normalized,
                    plan=plan,
                )

        # The durable Harness Run is now visible to owner terminalization.
        # WorkspaceReviewManager takes its own exact-generation writer fence
        # before linking a Workspace Run, so Git/file inspection must not keep
        # the process-local owner fence and make stop/cancel wait for it.
        return await self._activate_admitted_task_run(
            run=run,
            spec=normalized,
            plan=plan,
        )

    async def _admit_task_run_under_owner_fence(
        self,
        *,
        task_id: int,
        spec: TestHarnessSpec,
        owner_user_id: int | None = None,
        owner_identity: TestHarnessOwnerIdentity | None = None,
        preview_config_override: dict[str, Any] | None = None,
    ) -> tuple[TestHarnessRun, bool, dict[str, Any]]:
        async with self.db_factory() as db:
            task = await db.get(Task, task_id)
            if task is None:
                raise TestHarnessError("Task not found")
            browser_parent = await db.scalar(
                select(TestHarnessChildBinding.id).where(
                    TestHarnessChildBinding.child_task_id == task_id
                )
            )
            metadata = task.metadata_ if isinstance(task.metadata_, dict) else {}
            if (
                browser_parent is not None
                or metadata.get("isolated_browser_agent") is True
            ):
                raise TestHarnessError(
                    "Isolated Browser Agent Tasks cannot own nested Test Harness runs"
                )
            current_owner_identity = test_harness_owner_identity(task)
            if (
                owner_identity is not None
                and owner_identity != current_owner_identity
            ):
                raise TestHarnessError(
                    "Harness owner Task generation changed before admission"
                )
            owner_identity = owner_identity or current_owner_identity
            project = await db.get(Project, task.project_id) if task.project_id else None
            runtime = self._runtime_for_task(task, spec)
            plan = compile_test_plan(
                goal=spec.goal,
                profile=spec.profile,
                allow_actions=spec.allow_actions,
                viewport_width=spec.viewport_width,
                viewport_height=spec.viewport_height,
                max_steps=spec.max_steps or 20,
                max_actions=spec.max_actions or 0,
                supplied=spec.test_plan,
            )
            project_id = project.id if project is not None else None
            if spec.target_kind in {"pull_request", "git_ref"}:
                capability = await untrusted_git_target_capability(
                    project=project,
                )
                if not capability.available:
                    raise TestHarnessError(
                        capability.reason or "PR/ref sandbox target is unavailable"
                    )

        run, created = await self._create_run(
            task_id=task_id,
            project_id=project_id,
            owner_user_id=owner_user_id,
            spec=spec,
            plan=plan,
            runtime=runtime,
            owner_identity=owner_identity,
            preview_config_override=preview_config_override,
        )
        return run, created, plan

    async def _activate_admitted_task_run(
        self,
        *,
        run: TestHarnessRun,
        spec: TestHarnessSpec,
        plan: dict[str, Any],
    ) -> TestHarnessRun:
        if spec.target_kind == "current_workspace":
            try:
                await self._start_workspace_review(
                    run_id=run.id,
                    spec=spec,
                    test_plan=plan,
                )
            except BaseException as exc:
                cleanup_status, cleanup_error = (
                    await self._cancel_workspace_for_harness(run.id)
                )
                await self._fail_start(
                    run.id,
                    exc,
                    cleanup_status=cleanup_status,
                    cleanup_error=cleanup_error,
                )
                raise
        elif spec.target_kind == "fixed_url":
            # A fixed URL needs the caller to reserve either an inline browser
            # tool or a separate Task, then attach that exact job below.
            await self._update_run(
                run.id,
                values={"stage": "waiting_for_browser"},
                event_type="lifecycle",
                title="等待浏览器执行器",
                source_key="harness:waiting-for-browser",
            )
        elif spec.target_kind in {"pull_request", "git_ref"}:
            pipeline = asyncio.create_task(
                self._run_git_target_pipeline(run.id),
                name=f"test-harness-git-{run.id}",
            )
            self._register_pipeline(run.id, pipeline)
        else:  # pragma: no cover - normalized() is the authority.
            raise TestHarnessContractError("unsupported target kind")
        current = await self.get_run_model(run.id)
        assert current is not None
        return current

    async def _create_run(
        self,
        *,
        task_id: int | None,
        project_id: int | None,
        owner_user_id: int | None,
        spec: TestHarnessSpec,
        plan: dict[str, Any],
        runtime: dict[str, Any],
        owner_identity: TestHarnessOwnerIdentity | None = None,
        preview_config_override: dict[str, Any] | None = None,
    ) -> tuple[TestHarnessRun, bool]:
        scope = f"task:{task_id}" if task_id is not None else f"admin:{owner_user_id or 0}"
        async with self._lock:
            async with self.db_factory() as db:
                owner: Task | None = None
                # Global Worker writer order is node-control -> owner Task.
                # Holding this fence through Run/Event commit makes a run
                # that wins first visible to the later drain proof, while an
                # irreversible drain that wins first rejects materialization.
                await fence_worker_node_mutation(db)
                if task_id is not None:
                    if owner_identity is None or owner_identity.task_id != task_id:
                        raise TestHarnessError(
                            "Harness Run has no exact owner generation"
                        )
                    try:
                        # This is the first Task statement after the node
                        # mutation fence. It turns a stale
                        # preflight identity into a serialized writer CAS on
                        # SQLite WAL as well as a row lock on server databases.
                        owner = await lock_test_harness_owner(db, owner_identity)
                    except RuntimeError as exc:
                        raise TestHarnessError(str(exc)) from exc
                    locality_error = test_harness_owner_locality_error(owner)
                    if locality_error is not None:
                        raise TestHarnessError(locality_error)
                    if owner.project_id != project_id:
                        raise TestHarnessError(
                            "Harness owner Task project changed during admission"
                        )
                    # Runtime-bearing Task fields do not advance the logical
                    # turn generation. Freeze them from the fresh row after
                    # the owner writer barrier so a concurrent config update
                    # cannot leave a Run with stale routing evidence.
                    runtime = self._runtime_for_task(owner, spec)
                    project = None
                    if owner.project_id is not None:
                        # Project sharing and SSH admission use Project→Task
                        # locks. We already own Task, so taking a Project row
                        # lock here would invert that global order. A single
                        # MVCC row read is coherent; the canonical copy below
                        # makes any later Project mutation irrelevant to this
                        # Run and its idempotency fingerprint.
                        project = (
                            await db.execute(
                                select(Project)
                                .where(Project.id == owner.project_id)
                                .execution_options(populate_existing=True)
                            )
                        ).scalar_one_or_none()
                    try:
                        execution_context = freeze_harness_execution_context(
                            task=owner,
                            project=project,
                            target_kind=spec.target_kind,
                            target=spec.target,
                            preview_config_override=preview_config_override,
                        )
                    except TestHarnessExecutionContextError as exc:
                        raise TestHarnessError(str(exc)) from exc
                    runtime = runtime_with_execution_context(
                        runtime,
                        execution_context,
                    )
                    await _require_terminal_owner_runtime_idle(db, owner)
                    if not isinstance(settings.auth_token, str) or not settings.auth_token.strip():
                        raise TestHarnessError(
                            "Test Harness requires a configured AUTH_TOKEN"
                        )
                    ssh_grant = await db.scalar(
                        select(TaskSSHGrant.id).where(
                            TaskSSHGrant.task_id == owner.id
                        )
                    )
                    if ssh_grant is not None:
                        raise TestHarnessError(
                            "Tasks with managed SSH grants cannot start Test Harness runs"
                        )
                fingerprint = request_fingerprint(
                    target_kind=spec.target_kind,
                    target=spec.target,
                    test_plan=plan,
                    runtime=runtime,
                )
                if spec.idempotency_key:
                    existing = await db.scalar(
                        select(TestHarnessRun).where(
                            TestHarnessRun.idempotency_scope == scope,
                            TestHarnessRun.idempotency_key == spec.idempotency_key,
                        )
                    )
                    if existing is not None:
                        if existing.request_fingerprint != fingerprint:
                            raise TestHarnessIdempotencyError(
                                "idempotency key already belongs to different test input"
                            )
                        if task_id is not None and (
                            owner_identity is None
                            or existing.owner_task_incarnation_id
                            != owner_identity.incarnation_id
                            or existing.owner_task_retry_count
                            != owner_identity.retry_count
                            or existing.owner_task_turn_generation
                            != owner_identity.turn_generation
                            or existing.owner_task_status != owner_identity.status
                        ):
                            raise TestHarnessIdempotencyError(
                                "idempotency key belongs to another owner generation"
                            )
                        return existing, False
                if task_id is not None:
                    active = await db.scalar(
                        select(TestHarnessRun.id).where(
                            TestHarnessRun.task_id == task_id,
                            or_(
                                TestHarnessRun.status.not_in(
                                    HARNESS_TERMINAL_STATUSES
                                ),
                                TestHarnessRun.cleanup_status != "completed",
                            ),
                        )
                    )
                    if active is not None:
                        raise TestHarnessBusyError(
                            "This Task already has an active frontend test run"
                        )

                parent: TestHarnessRun | None = None
                if spec.parent_run_id:
                    parent = await db.get(TestHarnessRun, spec.parent_run_id)
                    if parent is None:
                        raise TestHarnessError("Parent test run not found")
                    if parent.task_id != task_id:
                        raise TestHarnessError("Parent test run belongs to another Task")
                    if parent.status not in HARNESS_TERMINAL_STATUSES:
                        raise TestHarnessError("Parent test run is not terminal")

                run_id = uuid.uuid4().hex
                run = TestHarnessRun(
                    id=run_id,
                    task_id=task_id,
                    owner_task_incarnation_id=(
                        owner.incarnation_id if owner is not None else None
                    ),
                    owner_task_retry_count=(
                        owner.retry_count if owner is not None else None
                    ),
                    owner_task_turn_generation=(
                        owner.turn_generation if owner is not None else None
                    ),
                    owner_task_status=(
                        owner.status if owner is not None else None
                    ),
                    project_id=project_id,
                    owner_user_id=owner_user_id,
                    target_kind=spec.target_kind,
                    target_spec={"kind": spec.target_kind, **spec.target},
                    test_plan=plan,
                    runtime_config=runtime,
                    request_fingerprint=fingerprint,
                    idempotency_scope=scope if spec.idempotency_key else None,
                    idempotency_key=spec.idempotency_key,
                    parent_run_id=parent.id if parent is not None else None,
                    root_run_id=parent.root_run_id if parent is not None else run_id,
                    attempt_number=(parent.attempt_number + 1) if parent is not None else 1,
                    status="queued",
                    stage="queued",
                    cleanup_status="pending",
                    event_sequence=1,
                )
                db.add(run)
                db.add(
                    TestHarnessEvent(
                        run_id=run_id,
                        sequence=1,
                        event_type="lifecycle",
                        stage="queued",
                        title="测试运行已创建",
                        detail="输入契约和测试计划已冻结。",
                        data={"target_kind": spec.target_kind},
                        source_key="harness:created",
                    )
                )
                await db.commit()
                await db.refresh(run)
                return run, True

    @staticmethod
    def _runtime_for_task(task: Task, spec: TestHarnessSpec) -> dict[str, Any]:
        selected = resolve_harness_runtime(
            task,
            provider=spec.provider,
            model=spec.model,
            reasoning_effort=spec.reasoning_effort,
            codex_service_tier=spec.codex_service_tier,
        )
        return {
            **selected,
            "profile": spec.profile,
            "allow_actions": spec.allow_actions,
            "browser_channel": spec.browser_channel,
            "viewport_width": spec.viewport_width,
            "viewport_height": spec.viewport_height,
            "max_steps": spec.max_steps,
            "max_actions": spec.max_actions,
            "terminal_owner": (
                "workspace"
                if spec.target_kind == "current_workspace"
                else "browser"
                if spec.target_kind == "fixed_url"
                else "sandbox"
            ),
            "context_policy": "isolated_black_box_v1",
        }

    async def _start_workspace_review(
        self,
        *,
        run_id: str,
        spec: TestHarnessSpec,
        test_plan: dict[str, Any],
        await_completion: bool = False,
    ) -> WorkspaceReviewRun:
        run = await self.get_run_model(run_id)
        if run is None or run.task_id is None:
            raise TestHarnessError("Harness run has no owning Task")
        try:
            execution_context = execution_context_from_runtime(
                run.runtime_config,
                target_kind="current_workspace",
            )
        except TestHarnessExecutionContextError as exc:
            raise TestHarnessError(str(exc)) from exc

        from backend.services.workspace_review import workspace_review_manager

        if (
            run.owner_task_incarnation_id is None
            or run.owner_task_retry_count is None
            or run.owner_task_turn_generation is None
            or run.owner_task_status is None
        ):
            raise TestHarnessError("Harness Run has no durable owner generation")

        workspace_run = await workspace_review_manager.start(
            task_id=run.task_id,
            goal=spec.goal,
            mode="review_only",
            profile=spec.profile,
            allow_actions=spec.allow_actions,
            browser_channel=spec.browser_channel,
            viewport_width=spec.viewport_width,
            viewport_height=spec.viewport_height,
            max_steps=spec.max_steps,
            max_actions=spec.max_actions,
            harness_run_id=run_id,
            workspace_override=Path(execution_context["workspace_path"]),
            preview_config_override=execution_context["preview_config"],
            test_plan=test_plan,
            runtime_config=public_harness_runtime(run.runtime_config),
            owner_identity=TestHarnessOwnerIdentity(
                task_id=run.task_id,
                incarnation_id=run.owner_task_incarnation_id,
                retry_count=run.owner_task_retry_count,
                turn_generation=run.owner_task_turn_generation,
                status=run.owner_task_status,
            ),
        )
        if await_completion:
            await self._watch_workspace_run(
                run_id=run_id,
                workspace_review_run_id=workspace_run.id,
            )
        else:
            watcher = asyncio.create_task(
                self._watch_workspace_run(
                    run_id=run_id,
                    workspace_review_run_id=workspace_run.id,
                ),
                name=f"test-harness-workspace-{run_id}",
            )
            self._register_pipeline(run_id, watcher)
        return workspace_run

    async def _watch_workspace_run(
        self,
        *,
        run_id: str,
        workspace_review_run_id: str,
    ) -> None:
        try:
            await self._watch_workspace_run_inner(
                run_id=run_id,
                workspace_review_run_id=workspace_review_run_id,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Test Harness workspace watcher failed run=%s", run_id)
            cleanup_status, cleanup_error = (
                await self._cancel_workspace_for_harness(run_id)
            )
            await self._fail_start(
                run_id,
                exc,
                cleanup_status=cleanup_status,
                cleanup_error=cleanup_error,
            )

    async def _cancel_workspace_for_harness(
        self,
        run_id: str,
    ) -> tuple[str, str | None]:
        """Cancel a linked Preview and project its exact cleanup proof."""

        from backend.services.workspace_review import workspace_review_manager

        async with self.db_factory() as db:
            workspace_run = await db.scalar(
                select(WorkspaceReviewRun).where(
                    WorkspaceReviewRun.harness_run_id == run_id
                )
            )
        if workspace_run is None:
            # Failure before WorkspaceReviewManager committed a Run created no
            # Preview handle and therefore has nothing external to reap.
            return "completed", None
        try:
            await workspace_review_manager.cancel(workspace_run.id)
        except BaseException as exc:
            logger.exception(
                "Could not cancel workspace review after Harness failure run=%s",
                run_id,
            )
            return "failed", _safe_error(exc)
        async with self.db_factory() as db:
            current = await db.get(WorkspaceReviewRun, workspace_run.id)
        if current is None:
            return "unconfirmed", "Workspace review cleanup record disappeared"
        await self._sync_workspace_run(run_id, current)
        if current.cleanup_status == "completed":
            return "completed", None
        if current.cleanup_status == "failed":
            return "failed", (
                current.cleanup_error or "Workspace review cleanup failed"
            )
        return "unconfirmed", (
            current.cleanup_error
            or "Workspace review cleanup did not reach a proven terminal state"
        )

    async def _watch_workspace_run_inner(
        self,
        *,
        run_id: str,
        workspace_review_run_id: str,
    ) -> None:
        while True:
            async with self.db_factory() as db:
                workspace_run = await db.get(
                    WorkspaceReviewRun,
                    workspace_review_run_id,
                )
            if workspace_run is None:
                raise TestHarnessError("Workspace review record disappeared")
            if workspace_run.browser_review_job_id:
                from backend.services.browser_review_jobs import browser_review_job_manager

                job = await browser_review_job_manager.get(
                    workspace_run.browser_review_job_id
                )
                if job is not None:
                    await self.sync_browser_job(job)
            await self._sync_workspace_run(run_id, workspace_run)
            if (
                workspace_run.status in _WORKSPACE_TERMINAL
                and workspace_run.cleanup_status != "pending"
            ):
                return
            await asyncio.sleep(self.poll_interval)

    async def _sync_workspace_run(
        self,
        run_id: str,
        workspace_run: WorkspaceReviewRun,
    ) -> None:
        if workspace_run.status == "queued":
            status = "queued"
        elif workspace_run.status == "preparing":
            status = "preparing_environment"
        elif workspace_run.status == "ready":
            status = "preview_ready"
        elif workspace_run.status in {"reviewing", "running"}:
            status = "running"
        elif workspace_run.status == "completed":
            status = "stale" if workspace_run.stale else "completed"
        else:
            status = workspace_run.status
        attempt = None
        if workspace_run.browser_review_job_id:
            async with self.db_factory() as db:
                attempt = await db.scalar(
                    select(TestHarnessAttempt).where(
                        TestHarnessAttempt.run_id == run_id,
                        TestHarnessAttempt.browser_review_job_id
                        == workspace_run.browser_review_job_id,
                    )
                )
        evidence_error: str | None = None
        if status == "completed" and (
            attempt is None or attempt.archive_state != ARCHIVE_COMPLETE
        ):
            status = "failed"
            verdict = "error"
            evidence_error = (
                (attempt.archive_error if attempt is not None else None)
                or "Browser evidence archive did not reach complete"
            )
        elif status == "completed":
            structured_verdict = (
                attempt.result_data.get("verdict")
                if attempt is not None and isinstance(attempt.result_data, dict)
                else None
            )
            verdict = normalize_verdict(structured_verdict, report=workspace_run.report)
        elif status == "stale":
            verdict = "stale"
        elif status == "cancelled":
            verdict = "cancelled"
        elif status == "failed":
            verdict = "error"
        else:
            verdict = None
        event_type = "lifecycle"
        title = _workspace_stage_title(workspace_run.stage)
        detail = evidence_error or workspace_run.error
        if evidence_error:
            event_type = "evidence"
            title = "测试证据归档未完成"
        elif workspace_run.cleanup_status == "failed":
            event_type = "cleanup"
            title = "隔离预览清理失败"
            detail = workspace_run.cleanup_error or workspace_run.error
        elif (
            workspace_run.cleanup_status == "completed"
            and status in HARNESS_TERMINAL_STATUSES
        ):
            event_type = "cleanup"
            title = "隔离预览已清理"
        elif (
            workspace_run.status in _WORKSPACE_TERMINAL
            and workspace_run.cleanup_status == "pending"
        ):
            title = "测试已结束，正在清理隔离环境"
        elif workspace_run.status == "reviewing" and workspace_run.stage == "completed":
            title = "浏览器报告已接收，正在收尾"
        await self._update_run(
            run_id,
            values={
                "workspace_review_run_id": workspace_run.id,
                "browser_review_job_id": workspace_run.browser_review_job_id,
                "agent_task_id": workspace_run.agent_task_id,
                "status": status,
                "stage": (
                    "evidence_incomplete" if evidence_error else workspace_run.stage
                ),
                "verdict": verdict,
                "source_git_head": workspace_run.git_head,
                "source_fingerprint": workspace_run.workspace_fingerprint,
                "stale": workspace_run.stale,
                "report": workspace_run.report,
                "error": (
                    f"Test evidence archive did not complete: {evidence_error}"
                    if evidence_error
                    else workspace_run.error
                ),
                "cleanup_status": workspace_run.cleanup_status,
                "cleanup_error": workspace_run.cleanup_error,
                "started_at": workspace_run.started_at,
                "completed_at": workspace_run.completed_at,
            },
            event_type=event_type,
            title=title,
            detail=detail,
            source_key=(
                f"workspace:{workspace_run.id}:{workspace_run.status}:"
                f"{workspace_run.stage}:{workspace_run.cleanup_status}"
            ),
        )

    async def _stop_agent_task(self, task_id: int) -> None:
        from backend.services.internal_api_endpoint import resolve_internal_api_base

        headers = (
            {"Authorization": f"Bearer {settings.auth_token}"}
            if settings.auth_token
            else {}
        )
        async with httpx.AsyncClient(timeout=30.0, trust_env=False) as client:
            response = await client.post(
                f"{resolve_internal_api_base(None)}/api/tasks/{task_id}/stop-session",
                headers=headers,
            )
            if response.status_code not in {200, 404}:
                raise TestHarnessError(
                    "Browser Agent stop was not confirmed: "
                    f"HTTP {response.status_code} {response.text[:500]}"
                )

    async def _stop_git_target_children(self, job: Any | None) -> None:
        if job is None:
            return
        harness_run_id = getattr(job, "harness_run_id", None)
        stopped_binding = False
        if isinstance(harness_run_id, str) and harness_run_id:
            stopped_binding = await self.child_service.stop_for_harness_run(
                harness_run_id,
                reason="Git target Harness pipeline stopped",
            )
        task_id = getattr(job, "task_id", None)
        if not stopped_binding and isinstance(task_id, int) and task_id > 0:
            await self._stop_agent_task(task_id)
        from backend.services.browser_review_jobs import browser_review_job_manager

        await browser_review_job_manager.cancel(job.id)

    async def _cleanup_git_target(self, run_id: str) -> str | None:
        async def cleanup_and_verify() -> str | None:
            cleanup_error: str | None = None
            try:
                await self.sandbox_manager.cleanup(run_id)
            except BaseException as exc:
                cleanup_error = _safe_error(exc)
            async with self.db_factory() as db:
                lease = await db.scalar(
                    select(TestHarnessSandboxLease).where(
                        TestHarnessSandboxLease.run_id == run_id
                    )
                )
            if cleanup_error is not None:
                return cleanup_error
            if lease is not None and lease.cleanup_status != "completed":
                return (
                    lease.cleanup_error
                    or "Sandbox cleanup returned without a durable completion receipt"
                )
            return None

        return await finish_awaitable(cleanup_and_verify())

    async def _run_git_target_pipeline(self, run_id: str) -> None:
        """Own target preparation, black-box execution and exact cleanup."""

        job: Any | None = None
        try:
            await self._update_run(
                run_id,
                values={
                    "status": "resolving_target",
                    "stage": "resolving_target",
                    "started_at": datetime.utcnow(),
                },
                event_type="lifecycle",
                title="正在解析精确 Git 目标",
                source_key="sandbox:resolving-target",
            )
            async with self.db_factory() as lookup:
                run_snapshot = await lookup.get(TestHarnessRun, run_id)
                if (
                    run_snapshot is None
                    or run_snapshot.task_id is None
                    or run_snapshot.owner_task_incarnation_id is None
                    or run_snapshot.owner_task_retry_count is None
                    or run_snapshot.owner_task_turn_generation is None
                    or run_snapshot.owner_task_status is None
                ):
                    raise TestHarnessError(
                        "Harness Run has no active durable owner generation"
                    )
                owner_identity = TestHarnessOwnerIdentity(
                    task_id=run_snapshot.task_id,
                    incarnation_id=run_snapshot.owner_task_incarnation_id,
                    retry_count=run_snapshot.owner_task_retry_count,
                    turn_generation=run_snapshot.owner_task_turn_generation,
                    status=run_snapshot.owner_task_status,
                )

            async with self.db_factory() as db:
                try:
                    task = await lock_test_harness_owner(db, owner_identity)
                except RuntimeError as exc:
                    raise TestHarnessError(str(exc)) from exc
                locality_error = test_harness_owner_locality_error(task)
                if locality_error is not None:
                    raise TestHarnessError(locality_error)
                run = (
                    await db.execute(
                        select(TestHarnessRun)
                        .where(
                            TestHarnessRun.id == run_id,
                            TestHarnessRun.task_id == owner_identity.task_id,
                            TestHarnessRun.owner_task_incarnation_id
                            == owner_identity.incarnation_id,
                            TestHarnessRun.owner_task_retry_count
                            == owner_identity.retry_count,
                            TestHarnessRun.owner_task_turn_generation
                            == owner_identity.turn_generation,
                            TestHarnessRun.owner_task_status
                            == owner_identity.status,
                            TestHarnessRun.status.not_in(
                                HARNESS_TERMINAL_STATUSES
                            ),
                        )
                        .with_for_update()
                        .execution_options(populate_existing=True)
                    )
                ).scalar_one_or_none()
                if run is None:
                    raise TestHarnessError(
                        "Harness Run ended or changed owner before Git target preparation"
                    )
                try:
                    execution_context = execution_context_from_runtime(
                        run.runtime_config,
                        target_kind=run.target_kind,
                    )
                except TestHarnessExecutionContextError as exc:
                    raise TestHarnessError(str(exc)) from exc
                if (
                    run.project_id is None
                    or task.project_id != run.project_id
                    or execution_context.get("project_id") != run.project_id
                ):
                    raise TestHarnessError(
                        "Harness owner Task Project changed before Git target preparation"
                    )
                project = frozen_git_project(execution_context)
                kind = run.target_kind
                target = {
                    key: value
                    for key, value in run.target_spec.items()
                    if key != "kind"
                }
                await db.commit()

            async def record_target_progress(
                stage: str,
                title: str,
                detail: str | None,
            ) -> None:
                await self._update_run(
                    run_id,
                    values={
                        "status": "preparing_environment",
                        "stage": stage,
                    },
                    event_type="lifecycle",
                    title=title,
                    detail=detail,
                    source_key=f"sandbox:{stage}",
                )

            prepared = await self.target_manager.prepare(
                run_id=run_id,
                task=task,
                project=project,
                kind=kind,
                target=target,
                on_progress=record_target_progress,
            )
            await self._update_run(
                run_id,
                values={
                    "status": "preview_ready",
                    "stage": "preview_ready",
                    "source_git_head": prepared.resolved.head_sha,
                    "source_fingerprint": prepared.resolved.fingerprint,
                },
                event_type="lifecycle",
                title="精确提交的隔离预览已就绪",
                detail=(
                    f"HEAD {prepared.resolved.head_sha[:12]}；"
                    "依赖出口已撤销，页面仅映射到 Manager loopback。"
                ),
                source_key="sandbox:preview-ready",
            )
            job = await self.start_managed_preview_browser(
                run_id=run_id,
                url=prepared.preview.url,
            )
            from backend.services.browser_review_jobs import browser_review_job_manager

            while True:
                current_job = await browser_review_job_manager.get(job.id)
                if current_job is None:
                    raise TestHarnessError("Sandbox Browser Review job disappeared")
                job = current_job
                await self.sync_browser_job(job)
                if job.status in _BROWSER_TERMINAL:
                    break
                await asyncio.sleep(self.poll_interval)

            cleanup_error = await self._cleanup_git_target(run_id)
            report = job._read_report()
            async with self.db_factory() as db:
                terminal_attempt = await db.scalar(
                    select(TestHarnessAttempt).where(
                        TestHarnessAttempt.run_id == run_id,
                        TestHarnessAttempt.browser_review_job_id == job.id,
                    )
                )
            if cleanup_error is not None:
                status = "failed"
                stage = "failed"
                verdict = "error"
                error = f"Sandbox cleanup could not be proven: {cleanup_error}"
                cleanup_status = "failed"
            elif (
                terminal_attempt is None
                or terminal_attempt.archive_state != ARCHIVE_COMPLETE
            ):
                status = "failed"
                stage = "evidence_incomplete"
                verdict = "error"
                error = (
                    "Test evidence archive did not complete"
                    + (
                        f": {terminal_attempt.archive_error}"
                        if terminal_attempt is not None
                        and terminal_attempt.archive_error
                        else ""
                    )
                )
                cleanup_status = "completed"
            elif job.status == "completed" and report:
                status = "completed"
                stage = "completed"
                verdict = normalize_verdict(job.verdict, report=report)
                error = None
                cleanup_status = "completed"
            elif job.status == "cancelled":
                status = "cancelled"
                stage = "cancelled"
                verdict = "cancelled"
                error = job.error
                cleanup_status = "completed"
            else:
                status = "failed"
                stage = "failed"
                verdict = "error"
                error = job.error or "Browser Agent did not return a report"
                cleanup_status = "completed"
            await self._update_run(
                run_id,
                values={
                    "status": status,
                    "stage": stage,
                    "verdict": verdict,
                    "report": report,
                    "error": error,
                    "cleanup_status": cleanup_status,
                    "cleanup_error": cleanup_error,
                    "completed_at": datetime.utcnow(),
                },
                event_type="cleanup" if cleanup_error is None else "error",
                title=(
                    "Sandbox 已按精确身份清理"
                    if cleanup_error is None
                    else "Sandbox 清理失败"
                ),
                detail=cleanup_error,
                source_key=f"sandbox:terminal:{status}:{cleanup_status}",
            )
        except asyncio.CancelledError:
            async def cancel_pipeline() -> None:
                await self._stop_git_target_children(job)
                cleanup_error = await self._cleanup_git_target(run_id)
                await self._update_run(
                    run_id,
                    values={
                        "status": "cancelled",
                        "stage": "cancelled",
                        "verdict": "cancelled",
                        "cleanup_status": (
                            "completed" if cleanup_error is None else "failed"
                        ),
                        "cleanup_error": cleanup_error,
                        "completed_at": datetime.utcnow(),
                    },
                    event_type="cleanup",
                    title="测试已取消并清理 Sandbox",
                    detail=cleanup_error,
                    source_key="sandbox:cancelled",
                )

            operation, _ = await settle_awaitable(cancel_pipeline())
            operation.result()
            raise
        except Exception as exc:
            logger.exception("Git target Harness pipeline failed run=%s", run_id)
            await self._stop_git_target_children(job)
            cleanup_error = await self._cleanup_git_target(run_id)
            await self._update_run(
                run_id,
                values={
                    "status": "failed",
                    "stage": "failed",
                    "verdict": "error",
                    "error": _safe_error(exc),
                    "cleanup_status": (
                        "completed" if cleanup_error is None else "failed"
                    ),
                    "cleanup_error": cleanup_error,
                    "completed_at": datetime.utcnow(),
                },
                event_type="error",
                title="PR/ref 隔离测试失败",
                detail=_safe_error(exc),
                source_key="sandbox:failed",
            )

    async def attach_browser_job(
        self,
        *,
        run_id: str,
        job: Any,
        watch_terminal: bool,
        browser_manager: Any | None = None,
    ) -> None:
        job.harness_run_id = run_id
        payload = job.as_dict()
        attempt_id = uuid.uuid4().hex
        async with self.db_factory() as lookup:
            run_snapshot = await lookup.get(TestHarnessRun, run_id)
            if run_snapshot is None:
                raise TestHarnessError("Harness run not found")
            if (
                run_snapshot.task_id is None
                or run_snapshot.owner_task_incarnation_id is None
                or run_snapshot.owner_task_retry_count is None
                or run_snapshot.owner_task_turn_generation is None
                or run_snapshot.owner_task_status is None
                or run_snapshot.status in HARNESS_TERMINAL_STATUSES
            ):
                raise TestHarnessError(
                    "Harness Run has no active durable owner generation"
                )
            owner_identity = TestHarnessOwnerIdentity(
                task_id=run_snapshot.task_id,
                incarnation_id=run_snapshot.owner_task_incarnation_id,
                retry_count=run_snapshot.owner_task_retry_count,
                turn_generation=run_snapshot.owner_task_turn_generation,
                status=run_snapshot.owner_task_status,
            )
        async with self._db_lock:
            async with self.db_factory() as db:
                try:
                    await lock_test_harness_owner(
                        db,
                        owner_identity,
                    )
                except RuntimeError as exc:
                    raise TestHarnessError(str(exc)) from exc
                run = (
                    await db.execute(
                        select(TestHarnessRun)
                        .where(
                            TestHarnessRun.id == run_id,
                            TestHarnessRun.task_id == owner_identity.task_id,
                            TestHarnessRun.owner_task_incarnation_id
                            == owner_identity.incarnation_id,
                            TestHarnessRun.owner_task_retry_count
                            == owner_identity.retry_count,
                            TestHarnessRun.owner_task_turn_generation
                            == owner_identity.turn_generation,
                            TestHarnessRun.owner_task_status
                            == owner_identity.status,
                            TestHarnessRun.status.not_in(
                                HARNESS_TERMINAL_STATUSES
                            ),
                        )
                        .with_for_update()
                        .execution_options(populate_existing=True)
                    )
                ).scalar_one_or_none()
                if run is None:
                    raise TestHarnessError(
                        "Harness Run ended or changed owner before Browser attach"
                    )
                existing = await db.scalar(
                    select(TestHarnessAttempt).where(
                        TestHarnessAttempt.run_id == run_id,
                        TestHarnessAttempt.ordinal == 1,
                    )
                )
                if existing is None:
                    staging_root = (
                        str(job.options.output_dir)
                        if job.options.output_dir is not None
                        else None
                    )
                    existing = TestHarnessAttempt(
                        id=attempt_id,
                        run_id=run_id,
                        ordinal=1,
                        status=job.status,
                        stage=job.stage,
                        provider=job.provider,
                        model=job.options.model,
                        reasoning_effort=job.options.reasoning_effort,
                        codex_service_tier=job.codex_service_tier,
                        agent_task_id=job.task_id,
                        browser_review_job_id=job.id,
                        artifact_root=staging_root,
                        artifact_staging_root=staging_root,
                        archive_state=ARCHIVE_STAGING,
                        archive_manifest={"version": 1, "expected": [], "archived": {}},
                        result_data=payload,
                    )
                    db.add(existing)
                run.browser_review_job_id = job.id
                run.agent_task_id = job.task_id
                run.status = "running" if job.status == "running" else run.status
                run.stage = job.stage
                await self._append_event(
                    db,
                    run,
                    event_type="lifecycle",
                    title="浏览器执行器已绑定",
                    stage=job.stage,
                    data={"browser_review_job_id": job.id},
                    source_key=f"browser:{job.id}:attached",
                )
                await db.commit()
        await self.sync_browser_job(job)
        if watch_terminal:
            watcher = asyncio.create_task(
                self._watch_browser_job(run_id, job.id, browser_manager),
                name=f"test-harness-browser-{run_id}",
            )
            self._register_pipeline(run_id, watcher)

    async def start_fixed_url_browser(
        self,
        *,
        run_id: str,
        inline: bool,
    ) -> Any:
        """Attach the fixed-URL adapter to an already persisted harness run."""

        run = await self.get_run_model(run_id)
        if run is None or run.task_id is None or run.target_kind != "fixed_url":
            raise TestHarnessError("Fixed URL harness run not found")
        async with test_harness_owner_fence(run.task_id):
            current = await self.get_run_model(run_id)
            if (
                current is None
                or current.task_id != run.task_id
                or current.status in HARNESS_TERMINAL_STATUSES
            ):
                raise TestHarnessError(
                    "Fixed URL harness owner ended before Browser admission"
                )
            return await self._start_browser_for_url(
                run=current,
                url=str(current.target_spec["url"]),
                network_policy="external_public",
                inline=inline,
                watch_terminal=True,
                fail_run_on_error=True,
            )

    async def start_managed_preview_browser(
        self,
        *,
        run_id: str,
        url: str,
    ) -> Any:
        run = await self.get_run_model(run_id)
        if (
            run is None
            or run.task_id is None
            or run.target_kind not in {"pull_request", "git_ref"}
        ):
            raise TestHarnessError("Git target Harness run not found")
        async with test_harness_owner_fence(run.task_id):
            current = await self.get_run_model(run_id)
            if (
                current is None
                or current.task_id != run.task_id
                or current.status in HARNESS_TERMINAL_STATUSES
            ):
                raise TestHarnessError(
                    "Git target Harness owner ended before Browser admission"
                )
            return await self._start_browser_for_url(
                run=current,
                url=url,
                network_policy="managed_preview",
                inline=False,
                watch_terminal=False,
                fail_run_on_error=False,
            )

    async def _start_browser_for_url(
        self,
        *,
        run: TestHarnessRun,
        url: str,
        network_policy: str,
        inline: bool,
        watch_terminal: bool,
        fail_run_on_error: bool,
    ) -> Any:
        run_id = run.id
        assert run.task_id is not None
        async with self.db_factory() as db:
            task = await db.get(Task, run.task_id)
            if task is None:
                raise TestHarnessError("Harness owner Task disappeared")
            created_by = task.created_by
        runtime = run.runtime_config
        from backend.services.browser_review import BrowserReviewOptions
        from backend.services.browser_review_jobs import browser_review_job_manager

        options = BrowserReviewOptions(
            url=url,
            network_policy=network_policy,
            goal=str(run.test_plan["objective"]),
            model=str(runtime["model"]),
            reasoning_effort=str(runtime["reasoning_effort"]),
            headless=True,
            allow_actions=bool(runtime["allow_actions"]),
            browser_channel=(
                "chrome" if runtime.get("browser_channel") == "chrome" else None
            ),
            viewport_width=int(runtime["viewport_width"]),
            viewport_height=int(runtime["viewport_height"]),
            max_steps=int(runtime["max_steps"]),
            max_actions=int(runtime["max_actions"]),
        )
        job = None
        child_binding_id: str | None = None
        try:
            if inline:
                job = await browser_review_job_manager.prepare_task_tool(
                    options,
                    task_id=task.id,
                    provider=str(runtime["provider"]),
                    codex_service_tier=str(runtime["codex_service_tier"]),
                    harness_run_id=run_id,
                )
            else:
                job = await browser_review_job_manager.prepare_agent(
                    options,
                    provider=str(runtime["provider"]),
                    codex_service_tier=str(runtime["codex_service_tier"]),
                    harness_run_id=run_id,
                )
                from backend.services.workspace_review import _browser_agent_prompt

                child, binding = await self.child_service.reserve_child(
                    owner_task_id=task.id,
                    browser_review_job_id=job.id,
                    harness_run_id=run_id,
                    child_values={
                        "title": f"Frontend Test Harness: Task {task.id}"[:200],
                        "description": _browser_agent_prompt(
                            job.id,
                            job.options,
                            profile=str(runtime.get("profile") or "standard"),
                            test_plan=run.test_plan,
                            target_context=_git_browser_target_context(run),
                        ),
                        "priority": 0,
                        "max_retries": 0,
                        "mode": "auto",
                        "provider": str(runtime["provider"]),
                        "model": str(runtime["model"]),
                        "codex_service_tier": str(runtime["codex_service_tier"]),
                        "effort_level": str(runtime["reasoning_effort"]),
                        "timeout_hours": 1.0,
                        "enabled_skills": {"browser-review": job.id},
                        "created_by": created_by,
                        "archived": True,
                    },
                )
                child_binding_id = binding.id
                await browser_review_job_manager.attach_task(
                    job.id,
                    child.id,
                    owner_task_id=task.id,
                )
            await self.attach_browser_job(
                run_id=run_id,
                job=job,
                watch_terminal=watch_terminal,
            )
            if child_binding_id is not None:
                await self.child_service.activate(child_binding_id)
                try:
                    from backend.main import dispatcher

                    if dispatcher is not None:
                        dispatcher.wake()
                except Exception:
                    logger.exception("Could not wake dispatcher for harness browser agent")
            return job
        except BaseException as exc:
            cleanup_error: str | None = None
            if job is not None:
                await browser_review_job_manager.fail_start(job.id, exc)
            if child_binding_id is not None:
                try:
                    await self.child_service.stop_binding(
                        child_binding_id,
                        reason=f"Browser Agent attach failed: {_safe_error(exc)}",
                    )
                except BaseException as cleanup_exc:
                    cleanup_error = _safe_error(cleanup_exc)
                    logger.exception(
                        "Could not roll back Browser child binding %s",
                        child_binding_id,
                    )
            if fail_run_on_error:
                await self._fail_start(
                    run_id,
                    exc,
                    cleanup_status=(
                        "completed" if cleanup_error is None else "failed"
                    ),
                    cleanup_error=cleanup_error,
                )
            raise

    async def _watch_browser_job(
        self,
        run_id: str,
        job_id: str,
        browser_manager: Any | None = None,
    ) -> None:
        if browser_manager is None:
            from backend.services.browser_review_jobs import browser_review_job_manager

            browser_manager = browser_review_job_manager

        try:
            while True:
                job = await browser_manager.get(job_id)
                if job is None:
                    raise TestHarnessError("Browser Review job disappeared")
                await self.sync_browser_job(job)
                if job.status in _BROWSER_TERMINAL:
                    return
                await asyncio.sleep(self.poll_interval)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Test Harness browser watcher failed run=%s", run_id)
            cleanup_error: str | None = None
            try:
                stopped = await self.child_service.stop_for_harness_run(
                    run_id,
                    reason="Harness Browser watcher lost its durable job",
                )
            except BaseException as cleanup_exc:
                stopped = False
                cleanup_error = _safe_error(cleanup_exc)
                logger.exception(
                    "Could not stop Browser child after watcher failure run=%s",
                    run_id,
                )
            await self._fail_start(
                run_id,
                exc,
                cleanup_status=(
                    "completed"
                    if stopped
                    else "failed"
                    if cleanup_error is not None
                    else "unconfirmed"
                ),
                cleanup_error=(
                    cleanup_error
                    or (
                        None
                        if stopped
                        else "Browser watcher lost the job before cleanup was proven"
                    )
                ),
            )

    async def sync_browser_job(self, job: Any) -> None:
        run_id = getattr(job, "harness_run_id", None)
        if not isinstance(run_id, str) or not run_id:
            return
        payload = job.as_dict()
        obsolete_storage_keys: list[str] = []
        async with self._db_lock:
            async with self.db_factory() as db:
                run = await db.get(TestHarnessRun, run_id)
                if run is None:
                    return
                attempt = await db.scalar(
                    select(TestHarnessAttempt).where(
                        TestHarnessAttempt.run_id == run_id,
                        TestHarnessAttempt.browser_review_job_id == job.id,
                    )
                )
                if attempt is None:
                    staging_root = (
                        str(job.options.output_dir)
                        if job.options.output_dir is not None
                        else None
                    )
                    attempt = TestHarnessAttempt(
                        id=uuid.uuid4().hex,
                        run_id=run_id,
                        ordinal=1,
                        provider=job.provider,
                        model=job.options.model,
                        reasoning_effort=job.options.reasoning_effort,
                        codex_service_tier=job.codex_service_tier,
                        browser_review_job_id=job.id,
                        artifact_root=staging_root,
                        artifact_staging_root=staging_root,
                        archive_state=ARCHIVE_STAGING,
                        archive_manifest={"version": 1, "expected": [], "archived": {}},
                    )
                    db.add(attempt)
                attempt.status = job.status
                attempt.stage = job.stage
                attempt.agent_task_id = job.task_id
                if attempt.archive_state != ARCHIVE_COMPLETE:
                    staging_root = (
                        str(job.options.output_dir)
                        if job.options.output_dir is not None
                        else attempt.artifact_staging_root
                    )
                    attempt.artifact_root = staging_root
                    attempt.artifact_staging_root = staging_root
                attempt.result_data = _json_copy(payload)
                attempt.error = job.error
                attempt.started_at = _parse_datetime(job.started_at)
                attempt.completed_at = _parse_datetime(job.completed_at)
                run.browser_review_job_id = job.id
                run.agent_task_id = job.task_id

                terminal_owner = run.runtime_config.get("terminal_owner")
                if terminal_owner == "browser":
                    if job.status == "completed":
                        run.status = "completed"
                        run.verdict = normalize_verdict(job.verdict, report=payload.get("report"))
                        run.report = payload.get("report")
                        run.completed_at = _parse_datetime(job.completed_at) or datetime.utcnow()
                        run.cleanup_status = "completed"
                    elif job.status == "failed":
                        run.status = "failed"
                        run.verdict = "error"
                        run.error = job.error
                        run.completed_at = _parse_datetime(job.completed_at) or datetime.utcnow()
                        run.cleanup_status = "completed"
                    elif job.status == "cancelled":
                        run.status = "cancelled"
                        run.verdict = "cancelled"
                        run.completed_at = _parse_datetime(job.completed_at) or datetime.utcnow()
                        run.cleanup_status = "completed"
                    else:
                        run.status = "running"
                    run.stage = job.stage
                    run.started_at = run.started_at or _parse_datetime(job.started_at)
                elif run.status not in HARNESS_TERMINAL_STATUSES:
                    run.stage = job.stage
                if terminal_owner == "workspace" and job.status == "completed":
                    # WorkspaceReviewRun owns preview cleanup/staleness, while
                    # the browser attempt owns the structured semantic result.
                    # Keep that exact verdict instead of falling back to a
                    # brittle Markdown keyword guess.
                    run.verdict = normalize_verdict(
                        job.verdict,
                        report=payload.get("report"),
                    )

                await self._append_event(
                    db,
                    run,
                    event_type="browser_state",
                    title=_browser_stage_title(job.stage),
                    stage=job.stage,
                    data={
                        "steps": job.steps,
                        "actions": job.actions,
                        "latest_screenshot": job.latest_screenshot,
                    },
                    source_key=(
                        f"browser:{job.id}:{job.status}:{job.stage}:"
                        f"{job.steps}:{job.actions}:{job.latest_screenshot or '-'}"
                    ),
                )
                for event in job.trace_events:
                    source_id = event.get("id")
                    await self._append_event(
                        db,
                        run,
                        event_type=str(event.get("kind") or "trace"),
                        title=str(event.get("title") or "模型操作轨迹")[:240],
                        detail=(
                            str(event["detail"])[:8000]
                            if event.get("detail") is not None
                            else None
                        ),
                        stage=job.stage,
                        data={
                            "tool_name": event.get("tool_name"),
                            "source_timestamp": event.get("timestamp"),
                        },
                        source_key=f"trace:{job.id}:{source_id}",
                    )
                archive_error: str | None = None
                try:
                    obsolete_storage_keys = await self._sync_evidence(
                        db,
                        run,
                        attempt,
                        job,
                        payload=payload,
                    )
                except (OSError, TestHarnessArtifactError) as exc:
                    archive_error = _safe_error(exc)
                    attempt.archive_state = ARCHIVE_RETRYABLE_ERROR
                    attempt.archive_error = archive_error
                    attempt.artifact_archive_prefix = None
                    attempt.archived_at = None
                    await self._append_event(
                        db,
                        run,
                        event_type="evidence",
                        title="测试证据归档未完成",
                        detail=archive_error,
                        stage=job.stage,
                        data={"archive_state": attempt.archive_state},
                        source_key=(
                            f"evidence:{attempt.id}:{attempt.archive_state}:"
                            f"{archive_error[:120]}"
                        ),
                    )
                else:
                    await self._append_event(
                        db,
                        run,
                        event_type="evidence",
                        title=(
                            "测试证据已完整归档"
                            if attempt.archive_state == ARCHIVE_COMPLETE
                            else "正在收集测试证据"
                        ),
                        stage=job.stage,
                        data={
                            "archive_state": attempt.archive_state,
                            "expected": len(
                                _archive_manifest(attempt.archive_manifest)["expected"]
                            ),
                        },
                        source_key=(
                            f"evidence:{attempt.id}:{attempt.archive_state}:"
                            f"{len(_archive_manifest(attempt.archive_manifest)['expected'])}"
                        ),
                    )
                if job.status in _BROWSER_TERMINAL and (
                    archive_error is not None
                    or attempt.archive_state != ARCHIVE_COMPLETE
                ):
                    run.status = "failed"
                    run.stage = "evidence_incomplete"
                    run.verdict = "error"
                    run.error = (
                        "Test evidence archive did not complete"
                        + (f": {archive_error}" if archive_error else "")
                    )
                    run.completed_at = (
                        _parse_datetime(job.completed_at) or datetime.utcnow()
                    )
                await self._sync_findings(db, run_id, job.findings)
                await db.commit()
        for storage_key in obsolete_storage_keys:
            if not self.artifact_store.remove(storage_key):
                logger.warning(
                    "Could not remove superseded Test Harness evidence %s",
                    storage_key,
                )

    async def _sync_evidence(
        self,
        db: Any,
        run: TestHarnessRun,
        attempt: TestHarnessAttempt,
        job: Any,
        *,
        payload: dict[str, Any],
    ) -> list[str]:
        if run.task_id is None:
            raise TestHarnessArtifactError(
                "Harness evidence has no owning Task identity"
            )
        manifest = _archive_manifest(attempt.archive_manifest)
        expected = set(manifest["expected"])
        payload_names = payload.get("artifacts")
        if isinstance(payload_names, list):
            expected.update(_valid_evidence_names(payload_names))
        latest_screenshot = payload.get("latest_screenshot")
        if isinstance(latest_screenshot, str):
            expected.update(_valid_evidence_names([latest_screenshot]))
        if isinstance(payload.get("report"), str) and payload["report"].strip():
            expected.add("report.md")
        manifest["expected"] = sorted(expected)
        manifest["terminal_status"] = (
            job.status if job.status in _BROWSER_TERMINAL else None
        )
        attempt.archive_manifest = _json_copy(manifest)

        if attempt.archive_state == ARCHIVE_COMPLETE:
            manifest["archived"] = await self._verify_archived_evidence(
                db,
                run=run,
                attempt=attempt,
                expected=manifest["expected"],
            )
            attempt.archive_manifest = _json_copy(manifest)
            return []

        root_value = attempt.artifact_staging_root or job.options.output_dir
        if root_value is None:
            raise TestHarnessArtifactError(
                "Browser Review staging directory was not recorded"
            )
        attempt.artifact_staging_root = str(root_value)
        attempt.artifact_root = str(root_value)
        attempt.archive_state = ARCHIVE_ARCHIVING
        attempt.archive_error = None
        if not self.artifact_store.is_managed_job_dir(root_value):
            raise TestHarnessArtifactError(
                "Browser Review staging directory is missing or unmanaged"
            )
        try:
            root = Path(root_value).resolve(strict=True)
            names = self.artifact_store.list_job_artifacts(root)
        except (OSError, TestHarnessArtifactError) as exc:
            raise TestHarnessArtifactError(
                "Browser Review staging directory is unavailable"
            ) from exc
        expected.update(names)
        manifest["expected"] = sorted(expected)
        attempt.archive_manifest = _json_copy(manifest)
        obsolete = await self._archive_evidence_files(
            db,
            run=run,
            attempt_id=attempt.id,
            root=root,
            names=names,
            browser_review_job_id=job.id,
        )
        # Completion is an explicit commit protocol: all descriptors must be
        # visible to the verification query before the staging pointer can be
        # cleared. Do not rely on ORM autoflush timing here.
        await db.flush()
        archived = await self._verify_archived_evidence(
            db,
            run=run,
            attempt=attempt,
            expected=manifest["expected"],
        )
        manifest["archived"] = archived
        attempt.archive_manifest = _json_copy(manifest)
        if job.status in _BROWSER_TERMINAL:
            prefix = self.artifact_store.run_prefix(
                task_id=run.task_id,
                run_id=run.id,
                attempt_id=attempt.id,
            )
            attempt.archive_state = ARCHIVE_COMPLETE
            attempt.artifact_archive_prefix = prefix
            attempt.artifact_staging_root = None
            attempt.artifact_root = prefix
            attempt.archive_error = None
            attempt.archived_at = datetime.utcnow()
        else:
            attempt.archive_state = ARCHIVE_STAGING
            attempt.artifact_archive_prefix = None
            attempt.archive_error = None
            attempt.archived_at = None
        return obsolete

    async def _verify_archived_evidence(
        self,
        db: Any,
        *,
        run: TestHarnessRun,
        attempt: TestHarnessAttempt,
        expected: list[str],
    ) -> dict[str, dict[str, Any]]:
        rows = list(
            (
                await db.execute(
                    select(TestHarnessEvidence).where(
                        TestHarnessEvidence.run_id == run.id,
                        TestHarnessEvidence.attempt_id == attempt.id,
                    )
                )
            ).scalars()
        )
        by_name = {row.name: row for row in rows}
        missing = [name for name in expected if name not in by_name]
        if missing:
            raise TestHarnessArtifactError(
                "Expected Test Harness evidence is missing: "
                + ", ".join(missing)
            )
        archived: dict[str, dict[str, Any]] = {}
        for name in expected:
            evidence = by_name[name]
            opened = self.artifact_store.open(
                evidence.storage_path,
                expected_sha256=evidence.sha256,
                expected_size=evidence.byte_size,
            )
            opened.close()
            archived[name] = {
                "storage_path": evidence.storage_path,
                "sha256": evidence.sha256,
                "byte_size": evidence.byte_size,
            }
        return archived

    async def _archive_evidence_files(
        self,
        db: Any,
        *,
        run: TestHarnessRun,
        attempt_id: str,
        root: Path,
        names: list[str],
        browser_review_job_id: str | None,
    ) -> list[str]:
        if run.task_id is None:
            return []
        obsolete: list[str] = []
        for name in names:
            candidate = root / name
            archived = self.artifact_store.archive(
                candidate,
                task_id=run.task_id,
                run_id=run.id,
                attempt_id=attempt_id,
                name=name,
            )
            evidence = await db.scalar(
                select(TestHarnessEvidence).where(
                    TestHarnessEvidence.run_id == run.id,
                    TestHarnessEvidence.name == name,
                )
            )
            kind = (
                "screenshot"
                if candidate.suffix == ".png"
                else "report"
                if candidate.suffix == ".md"
                else "telemetry"
            )
            if evidence is None:
                evidence = TestHarnessEvidence(
                    id=uuid.uuid4().hex,
                    run_id=run.id,
                    attempt_id=attempt_id,
                    kind=kind,
                    name=name,
                    content_type=_CONTENT_TYPES.get(
                        candidate.suffix, "application/octet-stream"
                    ),
                    storage_path=archived.storage_key,
                    sha256=archived.sha256,
                    byte_size=archived.byte_size,
                    metadata_={
                        "browser_review_job_id": browser_review_job_id,
                        "storage_version": 1,
                    },
                )
                db.add(evidence)
            else:
                old_storage_key = evidence.storage_path
                evidence.attempt_id = attempt_id
                evidence.storage_path = archived.storage_key
                evidence.sha256 = archived.sha256
                evidence.byte_size = archived.byte_size
                evidence.metadata_ = {
                    **(evidence.metadata_ or {}),
                    "browser_review_job_id": browser_review_job_id,
                    "storage_version": 1,
                }
                if old_storage_key != archived.storage_key:
                    obsolete.append(old_storage_key)
        return obsolete

    async def _sync_findings(
        self,
        db: Any,
        run_id: str,
        findings: list[dict[str, Any]],
    ) -> None:
        fingerprints = {item["fingerprint"] for item in findings}
        stale_query = delete(TestHarnessFinding).where(
            TestHarnessFinding.run_id == run_id
        )
        if fingerprints:
            stale_query = stale_query.where(
                TestHarnessFinding.fingerprint.not_in(fingerprints)
            )
        await db.execute(stale_query)
        existing = {
            item.fingerprint: item
            for item in (
                await db.execute(
                    select(TestHarnessFinding).where(
                        TestHarnessFinding.run_id == run_id
                    )
                )
            ).scalars()
        }
        for item in findings:
            row = existing.get(item["fingerprint"])
            if row is None:
                row = TestHarnessFinding(
                    id=uuid.uuid4().hex,
                    run_id=run_id,
                    ordinal=item["ordinal"],
                    fingerprint=item["fingerprint"],
                    scenario_id=item["scenario_id"],
                    severity=item["severity"],
                    category=item["category"],
                    title=item["title"],
                    route=item["route"],
                    locator=item["locator"],
                    expected=item["expected"],
                    actual=item["actual"],
                    reproduction=item["reproduction"],
                    evidence_names=item["evidence_names"],
                    confidence=item["confidence"],
                )
                db.add(row)
            else:
                for key in (
                    "ordinal",
                    "scenario_id",
                    "severity",
                    "category",
                    "title",
                    "route",
                    "locator",
                    "expected",
                    "actual",
                    "reproduction",
                    "evidence_names",
                    "confidence",
                ):
                    setattr(row, key, item[key])

    async def _update_run(
        self,
        run_id: str,
        *,
        values: dict[str, Any],
        event_type: str,
        title: str,
        detail: str | None = None,
        source_key: str | None = None,
    ) -> None:
        async with self._db_lock:
            async with self.db_factory() as db:
                # ``FOR UPDATE`` is ignored by SQLite.  The no-op UPDATE is
                # therefore the portable first-writer barrier for both the
                # lifecycle projection and its monotonically allocated event
                # sequence.  Separate Manager processes cannot otherwise both
                # read the same cleanup/event state and commit out of order.
                await db.execute(
                    update(TestHarnessRun)
                    .where(TestHarnessRun.id == run_id)
                    .values(event_sequence=TestHarnessRun.event_sequence)
                )
                run = (
                    await db.execute(
                        select(TestHarnessRun)
                        .where(TestHarnessRun.id == run_id)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if run is None:
                    return
                effective_values = dict(values)
                proposed_cleanup = effective_values.get("cleanup_status")
                if run.cleanup_status == "completed":
                    # External cleanup proof is an absorbing state.  A late
                    # executor may still publish an otherwise valid terminal
                    # lifecycle update, but it can never replace durable proof
                    # with failed/cleaning/unconfirmed or restore an error.
                    effective_values.pop("cleanup_status", None)
                    effective_values.pop("cleanup_error", None)
                elif proposed_cleanup == "completed":
                    effective_values["cleanup_error"] = None
                if not effective_values:
                    await db.rollback()
                    return
                proposed_status = effective_values.get("status")
                cleanup_only = set(effective_values).issubset(
                    {"cleanup_status", "cleanup_error"}
                )
                if run.status in HARNESS_TERMINAL_STATUSES:
                    if not cleanup_only and proposed_status != run.status:
                        await db.rollback()
                        return
                elif run.status == "cancelling":
                    if not cleanup_only and proposed_status != "cancelled":
                        await db.rollback()
                        return
                for key, value in effective_values.items():
                    setattr(run, key, value)
                await self._append_event(
                    db,
                    run,
                    event_type=event_type,
                    title=title,
                    detail=detail,
                    stage=str(effective_values.get("stage") or run.stage),
                    source_key=source_key,
                )
                await db.commit()

    async def _append_event(
        self,
        db: Any,
        run: TestHarnessRun,
        *,
        event_type: str,
        title: str,
        stage: str | None = None,
        detail: str | None = None,
        data: dict[str, Any] | None = None,
        source_key: str | None = None,
    ) -> bool:
        if source_key is not None:
            duplicate = await db.scalar(
                select(TestHarnessEvent.id).where(
                    TestHarnessEvent.run_id == run.id,
                    TestHarnessEvent.source_key == source_key,
                )
            )
            if duplicate is not None:
                return False
        run.event_sequence += 1
        db.add(
            TestHarnessEvent(
                run_id=run.id,
                sequence=run.event_sequence,
                event_type=event_type[:32],
                stage=stage[:48] if stage else None,
                title=title[:240],
                detail=detail[:8000] if detail else None,
                data=_json_copy(data or {}),
                source_key=source_key[:200] if source_key else None,
            )
        )
        return True

    async def _fail_start(
        self,
        run_id: str,
        exc: BaseException,
        *,
        cleanup_status: str,
        cleanup_error: str | None = None,
    ) -> None:
        await self._update_run(
            run_id,
            values={
                "status": "failed",
                "stage": "failed",
                "verdict": "error",
                "error": _safe_error(exc),
                "cleanup_status": cleanup_status,
                "cleanup_error": cleanup_error,
                "completed_at": datetime.utcnow(),
            },
            event_type="error",
            title="测试运行失败",
            detail=_safe_error(exc),
            source_key="harness:failed",
        )

    async def _mark_cancelled(
        self,
        run_id: str,
        *,
        cleanup_status: str,
        cleanup_error: str | None = None,
    ) -> None:
        await self._update_run(
            run_id,
            values={
                "status": "cancelled",
                "stage": "cancelled",
                "verdict": "cancelled",
                "cleanup_status": cleanup_status,
                "cleanup_error": cleanup_error,
                "completed_at": datetime.utcnow(),
            },
            event_type="lifecycle",
            title="测试运行已停止",
            source_key="harness:cancelled",
        )

    async def cancel(
        self,
        run_id: str,
        *,
        stop_agent_task: Callable[[int], Awaitable[None]] | None = None,
        expected_identity: TestHarnessOwnerIdentity | None = None,
    ) -> TestHarnessRun | None:
        run = await self.get_run_model(run_id)
        if run is None:
            return None
        if run.task_id is None:
            if expected_identity is not None:
                raise TestHarnessError(
                    "Standalone Harness Run cannot use a Task generation fence"
                )
            return await self._cancel_under_owner_fence(
                run_id,
                stop_agent_task=stop_agent_task,
            )
        if expected_identity is not None and (
            expected_identity.task_id != run.task_id
            or run.owner_task_incarnation_id != expected_identity.incarnation_id
            or run.owner_task_retry_count != expected_identity.retry_count
            or run.owner_task_turn_generation != expected_identity.turn_generation
            or run.owner_task_status != expected_identity.status
        ):
            raise TestHarnessError(
                "Harness Run belongs to a different owner generation"
            )
        async with test_harness_owner_fence(run.task_id):
            return await self._cancel_under_owner_fence(
                run_id,
                stop_agent_task=stop_agent_task,
                expected_identity=expected_identity,
            )

    async def _cancel_under_owner_fence(
        self,
        run_id: str,
        *,
        stop_agent_task: Callable[[int], Awaitable[None]] | None = None,
        expected_identity: TestHarnessOwnerIdentity | None = None,
    ) -> TestHarnessRun | None:
        if expected_identity is not None:
            async with self._db_lock:
                async with self.db_factory() as db:
                    try:
                        await lock_test_harness_owner(db, expected_identity)
                    except RuntimeError as exc:
                        raise TestHarnessError(str(exc)) from exc
                    run = (
                        await db.execute(
                            select(TestHarnessRun)
                            .where(
                                TestHarnessRun.id == run_id,
                                TestHarnessRun.task_id
                                == expected_identity.task_id,
                                TestHarnessRun.owner_task_incarnation_id
                                == expected_identity.incarnation_id,
                                TestHarnessRun.owner_task_retry_count
                                == expected_identity.retry_count,
                                TestHarnessRun.owner_task_turn_generation
                                == expected_identity.turn_generation,
                                TestHarnessRun.owner_task_status
                                == expected_identity.status,
                            )
                            .with_for_update()
                        )
                    ).scalar_one_or_none()
                    if run is None:
                        raise TestHarnessError(
                            "Harness Run owner generation changed before cancellation"
                        )
                    if run.status in HARNESS_TERMINAL_STATUSES:
                        await db.rollback()
                    else:
                        run.status = "cancelling"
                        run.stage = "cancelling"
                        await self._append_event(
                            db,
                            run,
                            event_type="lifecycle",
                            title="正在停止测试运行",
                            stage="cancelling",
                            source_key="harness:cancelling",
                        )
                        await db.commit()
            run = await self.get_run_model(run_id)
        else:
            run = await self.get_run_model(run_id)
        if run is None:
            return run
        if run.status in HARNESS_TERMINAL_STATUSES:
            if run.cleanup_status == "completed":
                return run
            if run.workspace_review_run_id:
                cleanup_status, cleanup_error = (
                    await self._cancel_workspace_for_harness(run_id)
                )
                await self._update_run(
                    run_id,
                    values={
                        "cleanup_status": cleanup_status,
                        "cleanup_error": cleanup_error,
                    },
                    event_type="cleanup",
                    title=(
                        "隔离预览已清理"
                        if cleanup_status == "completed"
                        else "隔离预览清理仍未完成"
                    ),
                    detail=cleanup_error,
                    source_key=(
                        f"harness:cleanup-retry:{cleanup_status}:"
                        f"{cleanup_error or 'ok'}"
                    )[:200],
                )
                return await self.get_run_model(run_id)
            pipeline = self._pipelines.get(run_id)
            if pipeline is not None and not pipeline.done():
                pipeline.cancel()
                await asyncio.gather(pipeline, return_exceptions=True)
            cleanup_status, cleanup_error = (
                await self._cancel_direct_run_resources(
                    run,
                    stop_agent_task=stop_agent_task,
                )
            )
            if run.target_kind in {"pull_request", "git_ref"}:
                sandbox_error = await self._cleanup_git_target(run_id)
                if sandbox_error is not None:
                    cleanup_status = "failed"
                    cleanup_error = sandbox_error
            await self._update_run(
                run_id,
                values={
                    "cleanup_status": cleanup_status,
                    "cleanup_error": cleanup_error,
                },
                event_type="cleanup",
                title=(
                    "测试运行资源已清理"
                    if cleanup_status == "completed"
                    else "测试运行资源清理仍未完成"
                ),
                detail=cleanup_error,
                source_key=(
                    f"harness:cleanup-retry:{cleanup_status}:"
                    f"{cleanup_error or 'ok'}"
                )[:200],
            )
            return await self.get_run_model(run_id)
        if expected_identity is None:
            await self._update_run(
                run_id,
                values={"status": "cancelling", "stage": "cancelling"},
                event_type="lifecycle",
                title="正在停止测试运行",
                source_key="harness:cancelling",
            )
        if run.workspace_review_run_id:
            cleanup_status, cleanup_error = (
                await self._cancel_workspace_for_harness(run_id)
            )
        else:
            pipeline = self._pipelines.get(run_id)
            if pipeline is not None and not pipeline.done():
                pipeline.cancel()
                await asyncio.gather(pipeline, return_exceptions=True)
            cleanup_status, cleanup_error = (
                await self._cancel_direct_run_resources(
                    run,
                    stop_agent_task=stop_agent_task,
                )
            )
        pipeline = self._pipelines.get(run_id)
        if pipeline is not None and not pipeline.done():
            pipeline.cancel()
            await asyncio.gather(pipeline, return_exceptions=True)
        if run.target_kind in {"pull_request", "git_ref"}:
            sandbox_error = await self._cleanup_git_target(run_id)
            if sandbox_error is not None:
                cleanup_status = "failed"
                cleanup_error = sandbox_error
        current = await self.get_run_model(run_id)
        if current is not None and current.status not in HARNESS_TERMINAL_STATUSES:
            await self._mark_cancelled(
                run_id,
                cleanup_status=cleanup_status,
                cleanup_error=cleanup_error,
            )
        elif current is not None and (
            current.cleanup_status != cleanup_status
            or current.cleanup_error != cleanup_error
        ):
            await self._update_run(
                run_id,
                values={
                    "cleanup_status": cleanup_status,
                    "cleanup_error": cleanup_error,
                },
                event_type="cleanup",
                title=(
                    "测试运行资源已清理"
                    if cleanup_status == "completed"
                    else "测试运行资源清理仍未完成"
                ),
                detail=cleanup_error,
                source_key=(
                    f"harness:cleanup-final:{cleanup_status}:"
                    f"{cleanup_error or 'ok'}"
                )[:200],
            )
        return await self.get_run_model(run_id)

    async def _cancel_direct_run_resources(
        self,
        run: TestHarnessRun,
        *,
        stop_agent_task: Callable[[int], Awaitable[None]] | None = None,
    ) -> tuple[str, str | None]:
        """Reap non-Workspace Browser resources and return durable proof state."""

        errors: list[str] = []
        stopped_binding = False
        stopped_agent = False
        try:
            stopped_binding = await self.child_service.stop_for_harness_run(
                run.id,
                reason="Harness run was cancelled",
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            errors.append(_safe_error(exc))
        if (
            not stopped_binding
            and run.agent_task_id is not None
            and run.agent_task_id != run.task_id
        ):
            try:
                if stop_agent_task is not None:
                    await stop_agent_task(run.agent_task_id)
                else:
                    await self._stop_agent_task(run.agent_task_id)
                stopped_agent = True
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                errors.append(_safe_error(exc))
        if run.browser_review_job_id:
            from backend.services.browser_review_jobs import browser_review_job_manager

            try:
                cancelled_job = await browser_review_job_manager.cancel(
                    run.browser_review_job_id
                )
                if cancelled_job is None:
                    if not stopped_binding and not stopped_agent:
                        errors.append(
                            "Browser Review job disappeared before cleanup was proven"
                        )
                else:
                    await self.sync_browser_job(cancelled_job)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                errors.append(_safe_error(exc))
        if errors:
            return "failed", "; ".join(dict.fromkeys(errors))[:4000]
        return "completed", None

    async def cancel_for_task(self, task_id: int, *, reason: str) -> int:
        """Cascade an explicit owner stop/delete to all active Harness runs."""

        async with test_harness_owner_fence(task_id):
            async with self._lock:
                return await self._cancel_for_task_unlocked(task_id, reason=reason)

    @staticmethod
    def _terminal_gate_graph_ids(
        gate: dict[str, Any],
        key: str,
    ) -> set[str]:
        raw = gate.get(key)
        if not isinstance(raw, list):
            return set()
        return {
            value
            for value in raw
            if isinstance(value, str)
            and len(value) == 32
            and value.isalnum()
        }

    async def _capture_terminal_cleanup_graph(
        self,
        db,
        owner: Task,
        identity: TestHarnessOwnerIdentity,
    ) -> tuple[set[str], set[str], set[str]]:
        """Persist the exact graph this terminal gate is responsible for."""

        metadata = dict(owner.metadata_ or {})
        raw_gate = metadata.get(TEST_HARNESS_TERMINAL_GATE_KEY)
        gate = dict(raw_gate) if isinstance(raw_gate, dict) else {}
        run_ids = self._terminal_gate_graph_ids(
            gate,
            _TERMINAL_GATE_RUN_IDS_KEY,
        )
        workspace_ids = self._terminal_gate_graph_ids(
            gate,
            _TERMINAL_GATE_WORKSPACE_IDS_KEY,
        )
        binding_ids = self._terminal_gate_graph_ids(
            gate,
            _TERMINAL_GATE_BINDING_IDS_KEY,
        )
        run_ids.update(
            (
                await db.execute(
                    select(TestHarnessRun.id).where(
                        TestHarnessRun.task_id == identity.task_id,
                        TestHarnessRun.owner_task_incarnation_id
                        == identity.incarnation_id,
                        TestHarnessRun.owner_task_retry_count
                        == identity.retry_count,
                        TestHarnessRun.owner_task_turn_generation
                        == identity.turn_generation,
                        TestHarnessRun.owner_task_status == identity.status,
                        or_(
                            TestHarnessRun.status.not_in(
                                HARNESS_TERMINAL_STATUSES
                            ),
                            TestHarnessRun.cleanup_status != "completed",
                        ),
                    )
                )
            ).scalars()
        )
        workspace_ids.update(
            (
                await db.execute(
                    select(WorkspaceReviewRun.id).where(
                        WorkspaceReviewRun.task_id == identity.task_id,
                        WorkspaceReviewRun.owner_task_incarnation_id
                        == identity.incarnation_id,
                        WorkspaceReviewRun.owner_task_retry_count
                        == identity.retry_count,
                        WorkspaceReviewRun.owner_task_turn_generation
                        == identity.turn_generation,
                        WorkspaceReviewRun.owner_task_status == identity.status,
                        or_(
                            WorkspaceReviewRun.status.not_in(
                                _WORKSPACE_TERMINAL
                            ),
                            WorkspaceReviewRun.cleanup_status != "completed",
                        ),
                    )
                )
            ).scalars()
        )
        binding_ids.update(
            (
                await db.execute(
                    select(TestHarnessChildBinding.id).where(
                        TestHarnessChildBinding.owner_task_id
                        == identity.task_id,
                        TestHarnessChildBinding.owner_task_incarnation_id
                        == identity.incarnation_id,
                        TestHarnessChildBinding.owner_task_retry_count
                        == identity.retry_count,
                        TestHarnessChildBinding.owner_task_turn_generation
                        == identity.turn_generation,
                        TestHarnessChildBinding.owner_task_status
                        == identity.status,
                        TestHarnessChildBinding.state.not_in(
                            ("stopped", "completed")
                        ),
                    )
                )
            ).scalars()
        )
        gate[_TERMINAL_GATE_RUN_IDS_KEY] = sorted(run_ids)
        gate[_TERMINAL_GATE_WORKSPACE_IDS_KEY] = sorted(workspace_ids)
        gate[_TERMINAL_GATE_BINDING_IDS_KEY] = sorted(binding_ids)
        metadata[TEST_HARNESS_TERMINAL_GATE_KEY] = gate
        owner.metadata_ = metadata
        return run_ids, workspace_ids, binding_ids

    async def _require_terminal_cleanup_proof(
        self,
        db,
        identity: TestHarnessOwnerIdentity,
        *,
        run_ids: set[str],
        workspace_ids: set[str],
        binding_ids: set[str],
    ) -> None:
        run_rows = []
        if run_ids:
            run_rows = list(
                (
                    await db.execute(
                        select(TestHarnessRun).where(
                            TestHarnessRun.id.in_(run_ids),
                            TestHarnessRun.task_id == identity.task_id,
                            TestHarnessRun.owner_task_incarnation_id
                            == identity.incarnation_id,
                            TestHarnessRun.owner_task_retry_count
                            == identity.retry_count,
                            TestHarnessRun.owner_task_turn_generation
                            == identity.turn_generation,
                            TestHarnessRun.owner_task_status == identity.status,
                        )
                    )
                ).scalars()
            )
        workspace_rows = []
        if workspace_ids:
            workspace_rows = list(
                (
                    await db.execute(
                        select(WorkspaceReviewRun).where(
                            WorkspaceReviewRun.id.in_(workspace_ids),
                            WorkspaceReviewRun.task_id == identity.task_id,
                            WorkspaceReviewRun.owner_task_incarnation_id
                            == identity.incarnation_id,
                            WorkspaceReviewRun.owner_task_retry_count
                            == identity.retry_count,
                            WorkspaceReviewRun.owner_task_turn_generation
                            == identity.turn_generation,
                            WorkspaceReviewRun.owner_task_status
                            == identity.status,
                        )
                    )
                ).scalars()
            )
        binding_rows = []
        if binding_ids:
            binding_rows = list(
                (
                    await db.execute(
                        select(TestHarnessChildBinding).where(
                            TestHarnessChildBinding.id.in_(binding_ids),
                            TestHarnessChildBinding.owner_task_id
                            == identity.task_id,
                            TestHarnessChildBinding.owner_task_incarnation_id
                            == identity.incarnation_id,
                            TestHarnessChildBinding.owner_task_retry_count
                            == identity.retry_count,
                            TestHarnessChildBinding.owner_task_turn_generation
                            == identity.turn_generation,
                            TestHarnessChildBinding.owner_task_status
                            == identity.status,
                        )
                    )
                ).scalars()
            )
        proven_runs = {
            row.id
            for row in run_rows
            if row.status in HARNESS_TERMINAL_STATUSES
            and row.cleanup_status == "completed"
        }
        proven_workspaces = {
            row.id
            for row in workspace_rows
            if row.status in _WORKSPACE_TERMINAL
            and row.cleanup_status == "completed"
        }
        proven_bindings = {
            row.id
            for row in binding_rows
            if row.state in {"stopped", "completed"}
        }
        if (
            proven_runs != run_ids
            or proven_workspaces != workspace_ids
            or proven_bindings != binding_ids
        ):
            raise TestHarnessError(
                "Harness owner graph did not reach a proven terminal and "
                "cleanup-complete state"
            )

    @asynccontextmanager
    async def owner_stop_fence(
        self,
        task_id: int,
        *,
        reason: str,
        expected_identity: TestHarnessOwnerIdentity | None = None,
        locked_owner_validator: (
            Callable[[Any], Awaitable[None]] | None
        ) = None,
    ):
        """Drain the owner graph and fence a terminal Task writer.

        ``expected_identity`` installs a durable, generation-scoped admission
        gate before cleanup.  That is required by natural Dispatcher terminal
        paths: a different Manager process may otherwise commit a late Run
        after this process has completed its stop scan.
        """

        if expected_identity is not None:
            # Once the exact durable gate is committed, every materializer
            # takes its own Task writer fence and rejects this generation.  Do
            # not retain the process-local lock across the caller's terminal
            # work: that work may need to cancel and join a Dispatcher worker
            # whose own finalizer re-enters this cleanup path.
            async with test_harness_owner_fence(task_id):
                async with self._lock:
                    await self._drain_owner_graph_for_stop(
                        task_id,
                        reason=reason,
                        expected_identity=expected_identity,
                        locked_owner_validator=locked_owner_validator,
                    )
            yield
            return

        # Legacy callers have no durable generation gate, so their terminal
        # mutation must remain inside the in-process admission fence.
        async with test_harness_owner_fence(task_id):
            async with self._lock:
                await self._drain_owner_graph_for_stop(
                    task_id,
                    reason=reason,
                    expected_identity=None,
                    locked_owner_validator=locked_owner_validator,
                )
                yield

    async def _drain_owner_graph_for_stop(
        self,
        task_id: int,
        *,
        reason: str,
        expected_identity: TestHarnessOwnerIdentity | None,
        locked_owner_validator: (
            Callable[[Any], Awaitable[None]] | None
        ) = None,
    ) -> None:
        """Install the exact gate, drain its graph, and verify cleanup."""

        run_ids: set[str] = set()
        workspace_ids: set[str] = set()
        binding_ids: set[str] = set()
        if expected_identity is not None:
            if expected_identity.task_id != task_id:
                raise TestHarnessError(
                    "Harness terminal owner identity does not match Task"
                )
            async with self.db_factory() as db:
                try:
                    owner = await install_test_harness_owner_terminal_gate(
                        db,
                        expected_identity,
                        reason=reason,
                        locked_owner_validator=locked_owner_validator,
                    )
                except RuntimeError as exc:
                    raise TestHarnessError(str(exc)) from exc
                (
                    run_ids,
                    workspace_ids,
                    binding_ids,
                ) = await self._capture_terminal_cleanup_graph(
                    db,
                    owner,
                    expected_identity,
                )
                # The gate must be independently durable before process I/O.
                # The graph IDs share its commit so a crash cannot forget a
                # terminal Run whose cleanup still needs a retry.
                await db.commit()
            for run_id in sorted(run_ids):
                # The durable gate itself is the cleanup authorization.  The
                # ordinary exact-owner admission lock intentionally rejects a
                # terminalizing generation, so cleanup consumes only these
                # frozen UUIDs instead of attempting admission again.
                await self.cancel(run_id)
            linked_workspace_ids: set[str] = set()
            if run_ids:
                async with self.db_factory() as db:
                    linked_workspace_ids = {
                        value
                        for value in (
                            await db.execute(
                                select(
                                    TestHarnessRun.workspace_review_run_id
                                ).where(TestHarnessRun.id.in_(run_ids))
                            )
                        ).scalars()
                        if isinstance(value, str)
                    }
            standalone_workspace_ids = workspace_ids - linked_workspace_ids
            if standalone_workspace_ids:
                from backend.services.workspace_review import (
                    workspace_review_manager,
                )

                for workspace_id in sorted(standalone_workspace_ids):
                    try:
                        await workspace_review_manager.cancel(workspace_id)
                    except Exception as exc:
                        raise TestHarnessError(str(exc)) from exc
            # A binding may have been committed before its parent Run linked
            # the child identity, or its parent cancellation may already have
            # become terminal.  Stop exactly the bindings frozen into this
            # gate; never broaden an exact-generation cleanup by owner Task.
            for binding_id in sorted(binding_ids):
                await self.child_service.stop_binding(
                    binding_id,
                    reason=reason,
                )
        else:
            # Legacy callers have no durable generation identity.  Preserve
            # their historical owner-wide cascade under the process fence.
            await self._cancel_for_task_unlocked(task_id, reason=reason)
            return
        async with self.db_factory() as db:
            try:
                owner = await install_test_harness_owner_terminal_gate(
                    db,
                    expected_identity,
                    reason=reason,
                    locked_owner_validator=locked_owner_validator,
                )
            except RuntimeError as exc:
                raise TestHarnessError(str(exc)) from exc
            (
                run_ids,
                workspace_ids,
                binding_ids,
            ) = await self._capture_terminal_cleanup_graph(
                db,
                owner,
                expected_identity,
            )
            await db.commit()
        async with self.db_factory() as db:
            await self._require_terminal_cleanup_proof(
                db,
                expected_identity,
                run_ids=run_ids,
                workspace_ids=workspace_ids,
                binding_ids=binding_ids,
            )
            await db.rollback()

    async def _cancel_for_task_unlocked(self, task_id: int, *, reason: str) -> int:
        """Cancel owner runs while the global run-admission lock is held."""

        async with self.db_factory() as db:
            run_ids = list(
                (
                    await db.execute(
                        select(TestHarnessRun.id)
                        .where(
                            TestHarnessRun.task_id == task_id,
                            TestHarnessRun.status.not_in(
                                HARNESS_TERMINAL_STATUSES
                            ),
                        )
                        .order_by(TestHarnessRun.created_at.desc())
                    )
                ).scalars()
            )
        for run_id in run_ids:
            await self.cancel(run_id)
        # A legacy/incomplete row may own a durable child before its Harness
        # status was persisted. Stop any remaining binding as a second fence.
        await self.child_service.stop_for_owner(task_id, reason=reason)
        return len(run_ids)

    async def get_run_model(self, run_id: str) -> TestHarnessRun | None:
        async with self.db_factory() as db:
            return await db.get(TestHarnessRun, run_id)

    async def get_run(self, run_id: str) -> dict[str, Any] | None:
        async with self.db_factory() as db:
            run = await db.get(TestHarnessRun, run_id)
            if run is None:
                return None
            return await self._serialize_run(db, run)

    async def list_for_task(self, task_id: int, *, limit: int = 50) -> list[dict[str, Any]]:
        async with self.db_factory() as db:
            runs = list(
                (
                    await db.execute(
                        select(TestHarnessRun)
                        .where(TestHarnessRun.task_id == task_id)
                        .order_by(TestHarnessRun.created_at.desc(), TestHarnessRun.id.desc())
                        .limit(min(100, max(1, limit)))
                    )
                ).scalars()
            )
            return [await self._serialize_run(db, run) for run in runs]

    async def refresh_task_staleness(self, task_id: int) -> None:
        """Project current-workspace freshness into durable Harness records."""

        from backend.services.workspace_review import refresh_workspace_review_staleness

        await refresh_workspace_review_staleness(
            task_id,
            db_factory=self.db_factory,
        )
        async with self.db_factory() as db:
            workspace_runs = list(
                (
                    await db.execute(
                        select(WorkspaceReviewRun).where(
                            WorkspaceReviewRun.task_id == task_id,
                            WorkspaceReviewRun.harness_run_id.is_not(None),
                        )
                    )
                ).scalars()
            )
        for workspace_run in workspace_runs:
            if workspace_run.harness_run_id:
                await self._sync_workspace_run(
                    workspace_run.harness_run_id,
                    workspace_run,
                )

    async def _serialize_run(self, db: Any, run: TestHarnessRun) -> dict[str, Any]:
        attempts = list(
            (
                await db.execute(
                    select(TestHarnessAttempt)
                    .where(TestHarnessAttempt.run_id == run.id)
                    .order_by(TestHarnessAttempt.ordinal.asc())
                )
            ).scalars()
        )
        events = list(
            (
                await db.execute(
                    select(TestHarnessEvent)
                    .where(TestHarnessEvent.run_id == run.id)
                    .order_by(TestHarnessEvent.sequence.asc())
                )
            ).scalars()
        )
        evidence = list(
            (
                await db.execute(
                    select(TestHarnessEvidence)
                    .where(TestHarnessEvidence.run_id == run.id)
                    .order_by(TestHarnessEvidence.created_at.asc())
                )
            ).scalars()
        )
        findings = list(
            (
                await db.execute(
                    select(TestHarnessFinding)
                    .where(TestHarnessFinding.run_id == run.id)
                    .order_by(TestHarnessFinding.ordinal.asc())
                )
            ).scalars()
        )
        workspace_payload = None
        private_preview_urls: list[str] = []
        if run.workspace_review_run_id:
            workspace = await db.get(WorkspaceReviewRun, run.workspace_review_run_id)
            if workspace is not None:
                from backend.services.workspace_review import workspace_review_run_dict

                workspace_payload = workspace_review_run_dict(workspace)
                if isinstance(workspace.preview_url, str) and workspace.preview_url:
                    private_preview_urls.append(workspace.preview_url)
        raw_browser_payload = attempts[-1].result_data if attempts else None
        if isinstance(raw_browser_payload, dict):
            from backend.services.browser_review_jobs import (
                public_browser_review_payload,
            )

            browser_payload = public_browser_review_payload(raw_browser_payload)
            if (
                raw_browser_payload.get("network_policy") == "managed_preview"
                and isinstance(raw_browser_payload.get("url"), str)
                and raw_browser_payload["url"]
            ):
                private_preview_urls.append(raw_browser_payload["url"])
        else:
            browser_payload = raw_browser_payload
        latest_attempt = attempts[-1] if attempts else None
        payload = {
            "id": run.id,
            "task_id": run.task_id,
            "project_id": run.project_id,
            "workspace_review_run_id": run.workspace_review_run_id,
            "browser_review_job_id": run.browser_review_job_id,
            "agent_task_id": run.agent_task_id,
            "target_kind": run.target_kind,
            "target": run.target_spec,
            "resolved_target": run.resolved_target,
            "test_plan": run.test_plan,
            "runtime": public_harness_runtime(run.runtime_config),
            "request_fingerprint": run.request_fingerprint,
            "parent_run_id": run.parent_run_id,
            "root_run_id": run.root_run_id,
            "attempt_number": run.attempt_number,
            "status": run.status,
            "stage": run.stage,
            "verdict": run.verdict,
            "source_git_head": run.source_git_head,
            "source_fingerprint": run.source_fingerprint,
            "stale": run.stale,
            "report": run.report,
            "error": run.error,
            "cleanup_status": run.cleanup_status,
            "cleanup_error": run.cleanup_error,
            "evidence_archive_state": (
                latest_attempt.archive_state if latest_attempt is not None else None
            ),
            "evidence_archive_error": (
                latest_attempt.archive_error if latest_attempt is not None else None
            ),
            "created_at": _iso(run.created_at),
            "started_at": _iso(run.started_at),
            "completed_at": _iso(run.completed_at),
            "attempts": [
                {
                    "id": item.id,
                    "ordinal": item.ordinal,
                    "status": item.status,
                    "stage": item.stage,
                    "provider": item.provider,
                    "model": item.model,
                    "reasoning_effort": item.reasoning_effort,
                    "codex_service_tier": item.codex_service_tier,
                    "agent_task_id": item.agent_task_id,
                    "browser_review_job_id": item.browser_review_job_id,
                    "archive_state": item.archive_state,
                    "archive_error": item.archive_error,
                    "archive_manifest": {
                        "version": _archive_manifest(item.archive_manifest)["version"],
                        "expected": _archive_manifest(item.archive_manifest)["expected"],
                        "archived": sorted(
                            _archive_manifest(item.archive_manifest)["archived"]
                        ),
                        "terminal_status": _archive_manifest(
                            item.archive_manifest
                        )["terminal_status"],
                    },
                    "archived_at": _iso(item.archived_at),
                    "error": item.error,
                    "created_at": _iso(item.created_at),
                    "started_at": _iso(item.started_at),
                    "completed_at": _iso(item.completed_at),
                }
                for item in attempts
            ],
            "events": [
                {
                    "id": item.id,
                    "sequence": item.sequence,
                    "event_type": item.event_type,
                    "stage": item.stage,
                    "title": item.title,
                    "detail": item.detail,
                    "data": item.data,
                    "created_at": _iso(item.created_at),
                }
                for item in events
            ],
            "evidence": [
                {
                    "id": item.id,
                    "kind": item.kind,
                    "name": item.name,
                    "content_type": item.content_type,
                    "sha256": item.sha256,
                    "byte_size": item.byte_size,
                    "metadata": item.metadata_,
                    "created_at": _iso(item.created_at),
                }
                for item in evidence
            ],
            "findings": [
                {
                    "id": item.id,
                    "fingerprint": item.fingerprint,
                    "scenario_id": item.scenario_id,
                    "severity": item.severity,
                    "category": item.category,
                    "title": item.title,
                    "route": item.route,
                    "locator": item.locator,
                    "expected": item.expected,
                    "actual": item.actual,
                    "reproduction": item.reproduction,
                    "evidence": item.evidence_names,
                    "confidence": item.confidence,
                }
                for item in findings
            ],
            "workspace_review": workspace_payload,
            "browser_review": browser_payload,
        }
        if private_preview_urls:
            from backend.services.browser_review_jobs import (
                redact_managed_preview_urls,
            )

            payload = redact_managed_preview_urls(payload, private_preview_urls)
            if isinstance(payload.get("browser_review"), dict):
                payload["browser_review"]["url"] = None
        return payload

    async def open_evidence(
        self,
        run_id: str,
        name: str,
    ) -> OpenedHarnessArtifact | None:
        if not name or len(name) > 255 or "/" in name or "\\" in name:
            return None
        async with self.db_factory() as db:
            evidence = await db.scalar(
                select(TestHarnessEvidence).where(
                    TestHarnessEvidence.run_id == run_id,
                    TestHarnessEvidence.name == name,
                )
            )
        if evidence is None:
            return None
        try:
            return self.artifact_store.open(
                evidence.storage_path,
                expected_sha256=evidence.sha256,
                expected_size=evidence.byte_size,
            )
        except TestHarnessArtifactError:
            return None

    async def resolve_evidence(self, run_id: str, name: str) -> Path | None:
        """Compatibility helper for tests; HTTP downloads use the opened FD."""

        opened = await self.open_evidence(run_id, name)
        if opened is None:
            return None
        opened.close()
        async with self.db_factory() as db:
            evidence = await db.scalar(
                select(TestHarnessEvidence).where(
                    TestHarnessEvidence.run_id == run_id,
                    TestHarnessEvidence.name == name,
                )
            )
        if evidence is None:
            return None
        try:
            return self.artifact_store.resolve_path(evidence.storage_path)
        except TestHarnessArtifactError:
            return None

    async def compare(self, base_run_id: str, candidate_run_id: str) -> dict[str, Any]:
        async with self.db_factory() as db:
            base = await db.get(TestHarnessRun, base_run_id)
            candidate = await db.get(TestHarnessRun, candidate_run_id)
            if base is None or candidate is None:
                raise TestHarnessError("Test run not found")
            if base.task_id != candidate.task_id:
                raise TestHarnessError("Test runs belong to different Tasks")
            base_findings = {
                item.fingerprint: item
                for item in (
                    await db.execute(
                        select(TestHarnessFinding).where(
                            TestHarnessFinding.run_id == base_run_id
                        )
                    )
                ).scalars()
            }
            candidate_findings = {
                item.fingerprint: item
                for item in (
                    await db.execute(
                        select(TestHarnessFinding).where(
                            TestHarnessFinding.run_id == candidate_run_id
                        )
                    )
                ).scalars()
            }
        return {
            "base_run_id": base_run_id,
            "candidate_run_id": candidate_run_id,
            "new": sorted(set(candidate_findings) - set(base_findings)),
            "persisting": sorted(set(candidate_findings) & set(base_findings)),
            "resolved": sorted(set(base_findings) - set(candidate_findings)),
            "base_verdict": base.verdict,
            "candidate_verdict": candidate.verdict,
        }

    async def repeat(
        self,
        run_id: str,
        *,
        owner_user_id: int | None = None,
        owner_identity: TestHarnessOwnerIdentity | None = None,
    ) -> TestHarnessRun:
        source = await self.get_run_model(run_id)
        if source is None or source.task_id is None:
            raise TestHarnessError("Test run not found")
        if source.status not in HARNESS_TERMINAL_STATUSES:
            raise TestHarnessError("Test run is not terminal")
        runtime = source.runtime_config
        spec = TestHarnessSpec(
            target_kind=source.target_kind,  # type: ignore[arg-type]
            target={
                key: value
                for key, value in source.target_spec.items()
                if key in {"url", "pr_number", "remote", "ref", "fetch"}
            },
            goal=str(source.test_plan.get("objective") or "Repeat frontend test"),
            profile=runtime.get("profile", "standard"),
            allow_actions=bool(runtime.get("allow_actions", True)),
            browser_channel=runtime.get(
                "browser_channel",
                DEFAULT_BROWSER_CHANNEL,
            ),
            viewport_width=int(runtime.get("viewport_width", 1440)),
            viewport_height=int(runtime.get("viewport_height", 900)),
            max_steps=int(runtime.get("max_steps", 20)),
            max_actions=int(runtime.get("max_actions", 0)),
            provider=runtime.get("provider"),
            model=runtime.get("model"),
            reasoning_effort=runtime.get("reasoning_effort"),
            codex_service_tier=runtime.get("codex_service_tier"),
            test_plan=source.test_plan,
            parent_run_id=source.id,
        )
        return await self.start_task_run(
            task_id=source.task_id,
            spec=spec,
            owner_user_id=owner_user_id,
            owner_identity=owner_identity,
        )

    async def cleanup_evidence(self, *, required_free_bytes: int = 0) -> int:
        """Apply TTL/quotas and reserve capacity without touching live evidence."""

        cutoff = datetime.utcnow() - timedelta(days=self.artifact_store.retention_days)
        required = max(0, int(required_free_bytes))
        retained_global_limit = max(
            0,
            self.artifact_store.max_total_bytes - required,
        )
        removed_keys: list[str] = []
        kept_global = 0
        kept_by_task: dict[int, int] = {}
        async with self._db_lock:
            async with self.db_factory() as db:
                rows = (
                    await db.execute(
                        select(
                            TestHarnessEvidence,
                            TestHarnessRun.task_id,
                            TestHarnessRun.status,
                        )
                        .join(TestHarnessRun, TestHarnessRun.id == TestHarnessEvidence.run_id)
                        .order_by(
                            TestHarnessEvidence.created_at.desc(),
                            TestHarnessEvidence.id.desc(),
                        )
                    )
                ).all()
                for evidence, task_id_value, run_status in rows:
                    task_id = int(task_id_value or 0)
                    task_total = kept_by_task.get(task_id, 0)
                    active = run_status not in HARNESS_TERMINAL_STATUSES
                    expired = evidence.created_at < cutoff
                    exceeds_task = (
                        task_total + evidence.byte_size
                        > self.artifact_store.max_task_bytes
                    )
                    exceeds_global = (
                        kept_global + evidence.byte_size
                        > retained_global_limit
                    )
                    if not active and (expired or exceeds_task or exceeds_global):
                        removed_keys.append(evidence.storage_path)
                        await db.delete(evidence)
                        continue
                    kept_by_task[task_id] = task_total + evidence.byte_size
                    kept_global += evidence.byte_size
                await db.commit()
                referenced = set(
                    (
                        await db.execute(select(TestHarnessEvidence.storage_path))
                    ).scalars()
                )
                protected_staging_job_ids = set(
                    (
                        await db.execute(
                            select(TestHarnessAttempt.browser_review_job_id).where(
                                TestHarnessAttempt.archive_state
                                != ARCHIVE_COMPLETE,
                                TestHarnessAttempt.browser_review_job_id.is_not(None),
                            )
                        )
                    ).scalars()
                )
        for storage_key in removed_keys:
            self.artifact_store.remove(storage_key)
        self.artifact_store.cleanup_orphan_archives(referenced)
        clean_job_dirs = True
        try:
            from backend.services.browser_review_jobs import browser_review_job_manager

            jobs = await browser_review_job_manager.list()
            # A nonterminal in-memory job may still write staging. Terminal
            # jobs are protected only when their durable archive is incomplete;
            # otherwise retaining them would make quota admission depend on
            # the in-memory history limit instead of archive durability.
            active_job_ids = {
                job.id for job in jobs if job.status not in _BROWSER_TERMINAL
            }
            active_job_ids.update(protected_staging_job_ids)
        except Exception:
            # Failure to prove which jobs are active must never turn into
            # deleting their staging evidence under quota pressure.
            clean_job_dirs = False
            active_job_ids = set(protected_staging_job_ids)
        if clean_job_dirs:
            self.artifact_store.cleanup_job_dirs(
                active_job_ids=active_job_ids,
                required_free_bytes=required,
            )
        return len(removed_keys)

    async def _retention_loop(self) -> None:
        while True:
            await asyncio.sleep(self.retention_interval)
            try:
                await self.cleanup_evidence()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Test Harness evidence retention failed")

    async def _reconcile_attempt_evidence(
        self,
        db: Any,
        *,
        run: TestHarnessRun,
        attempt: TestHarnessAttempt,
    ) -> list[str]:
        """Finish or verify one interrupted archive without trusting its pointer."""

        if run.task_id is None:
            raise TestHarnessArtifactError(
                "Interrupted Harness evidence has no owning Task"
            )
        manifest = _archive_manifest(attempt.archive_manifest)
        expected = set(manifest["expected"])
        payload = attempt.result_data if isinstance(attempt.result_data, dict) else {}
        payload_names = payload.get("artifacts")
        if isinstance(payload_names, list):
            expected.update(_valid_evidence_names(payload_names))
        latest_screenshot = payload.get("latest_screenshot")
        if isinstance(latest_screenshot, str):
            expected.update(_valid_evidence_names([latest_screenshot]))
        if isinstance(payload.get("report"), str) and payload["report"].strip():
            expected.add("report.md")

        existing_rows = list(
            (
                await db.execute(
                    select(TestHarnessEvidence).where(
                        TestHarnessEvidence.run_id == run.id,
                        TestHarnessEvidence.attempt_id == attempt.id,
                    )
                )
            ).scalars()
        )
        expected.update(row.name for row in existing_rows)
        manifest["expected"] = sorted(expected)
        manifest["terminal_status"] = (
            attempt.status if attempt.status in _BROWSER_TERMINAL else "failed"
        )
        attempt.archive_manifest = _json_copy(manifest)

        source_root = attempt.artifact_staging_root or attempt.artifact_root
        obsolete: list[str] = []
        if source_root and self.artifact_store.is_managed_job_dir(source_root):
            root = Path(source_root).resolve(strict=True)
            names = self.artifact_store.list_job_artifacts(root)
            expected.update(names)
            manifest["expected"] = sorted(expected)
            attempt.archive_manifest = _json_copy(manifest)
            attempt.archive_state = ARCHIVE_ARCHIVING
            obsolete = await self._archive_evidence_files(
                db,
                run=run,
                attempt_id=attempt.id,
                root=root,
                names=names,
                browser_review_job_id=attempt.browser_review_job_id,
            )
            await db.flush()
        elif attempt.archive_state != ARCHIVE_COMPLETE and not existing_rows:
            raise TestHarnessArtifactError(
                "Interrupted Browser Review staging directory is unavailable"
            )

        archived = await self._verify_archived_evidence(
            db,
            run=run,
            attempt=attempt,
            expected=manifest["expected"],
        )
        manifest["archived"] = archived
        prefix = self.artifact_store.run_prefix(
            task_id=run.task_id,
            run_id=run.id,
            attempt_id=attempt.id,
        )
        attempt.archive_manifest = _json_copy(manifest)
        attempt.archive_state = ARCHIVE_COMPLETE
        attempt.artifact_archive_prefix = prefix
        attempt.artifact_staging_root = None
        attempt.artifact_root = prefix
        attempt.archive_error = None
        attempt.archived_at = attempt.archived_at or datetime.utcnow()
        return obsolete

    async def recover_interrupted_runs(self) -> int:
        try:
            await self.sandbox_manager.recover_interrupted()
        except Exception:
            logger.exception("Could not fully recover interrupted Harness sandboxes")
        obsolete_storage_keys: list[str] = []
        async with self._db_lock:
            async with self.db_factory() as db:
                attempts_to_reconcile = list(
                    (
                        await db.execute(
                            select(TestHarnessAttempt).where(
                                TestHarnessAttempt.archive_state.in_(
                                    _ARCHIVE_RECOVERABLE_STATES
                                )
                            )
                        )
                    ).scalars()
                )
                reconciled = False
                for attempt in attempts_to_reconcile:
                    run_for_attempt = await db.get(TestHarnessRun, attempt.run_id)
                    if run_for_attempt is None or run_for_attempt.task_id is None:
                        continue
                    try:
                        obsolete_storage_keys.extend(
                            await self._reconcile_attempt_evidence(
                                db,
                                run=run_for_attempt,
                                attempt=attempt,
                            )
                        )
                    except (OSError, TestHarnessArtifactError) as exc:
                        logger.exception(
                            "Could not reconcile interrupted Harness evidence run=%s",
                            attempt.run_id,
                        )
                        attempt.archive_state = ARCHIVE_INCOMPLETE
                        attempt.archive_error = _safe_error(exc)
                        attempt.artifact_archive_prefix = None
                        attempt.archived_at = None
                        await self._append_event(
                            db,
                            run_for_attempt,
                            event_type="evidence",
                            title="测试证据恢复失败",
                            detail=attempt.archive_error,
                            stage="evidence_incomplete",
                            data={"archive_state": ARCHIVE_INCOMPLETE},
                            source_key=(
                                f"evidence:{attempt.id}:recovery-incomplete"
                            ),
                        )
                        if run_for_attempt.status == "completed":
                            run_for_attempt.status = "failed"
                            run_for_attempt.stage = "evidence_incomplete"
                            run_for_attempt.verdict = "error"
                            run_for_attempt.error = (
                                "Test evidence archive could not be recovered: "
                                f"{attempt.archive_error}"
                            )
                        reconciled = True
                        continue
                    reconciled = True

                runs = list(
                    (
                        await db.execute(
                            select(TestHarnessRun).where(
                                TestHarnessRun.status.not_in(HARNESS_TERMINAL_STATUSES)
                            )
                        )
                    ).scalars()
                )
                for run in runs:
                    sandbox_lease = await db.scalar(
                        select(TestHarnessSandboxLease).where(
                            TestHarnessSandboxLease.run_id == run.id
                        )
                    )
                    workspace_owner = await db.scalar(
                        select(WorkspaceReviewRun.id).where(
                            WorkspaceReviewRun.harness_run_id == run.id
                        )
                    )
                    child_binding = await db.scalar(
                        select(TestHarnessChildBinding.id).where(
                            TestHarnessChildBinding.harness_run_id == run.id
                        )
                    )
                    attempt = await db.scalar(
                        select(TestHarnessAttempt.id).where(
                            TestHarnessAttempt.run_id == run.id
                        )
                    )
                    sandbox_cleaned = (
                        sandbox_lease is not None
                        and sandbox_lease.cleanup_status == "completed"
                    )
                    # current_workspace admission now durably creates the
                    # Harness Run before its lock-free Git snapshot. If every
                    # downstream ownership pointer is still absent, the
                    # database transaction boundary proves that no Preview,
                    # Browser child, or Workspace Run was ever materialized.
                    pre_materialization_clean = bool(
                        run.target_kind == "current_workspace"
                        and run.workspace_review_run_id is None
                        and run.browser_review_job_id is None
                        and run.agent_task_id is None
                        and workspace_owner is None
                        and child_binding is None
                        and attempt is None
                        and sandbox_lease is None
                    )
                    cleanup_proven = sandbox_cleaned or pre_materialization_clean
                    run.status = "failed"
                    run.stage = "interrupted"
                    run.verdict = "error"
                    run.error = "Manager restarted before this test run reached a terminal state"
                    run.cleanup_status = (
                        "completed" if cleanup_proven else "unconfirmed"
                    )
                    run.cleanup_error = (
                        None
                        if cleanup_proven
                        else "The previous process could not prove browser, preview, or sandbox cleanup"
                    )
                    run.completed_at = datetime.utcnow()
                    await self._append_event(
                        db,
                        run,
                        event_type="error",
                        title="服务重启中断测试",
                        detail=run.cleanup_error,
                        stage="interrupted",
                        source_key="harness:interrupted",
                    )
                if runs:
                    attempt_rows = list(
                        (
                            await db.execute(
                                select(TestHarnessAttempt).where(
                                    TestHarnessAttempt.run_id.in_([run.id for run in runs]),
                                    TestHarnessAttempt.status.not_in(_BROWSER_TERMINAL),
                                )
                            )
                        ).scalars()
                    )
                    for attempt in attempt_rows:
                        attempt.status = "failed"
                        attempt.stage = "interrupted"
                        attempt.error = "Manager restarted before the browser attempt ended"
                        attempt.completed_at = datetime.utcnow()
                if runs or reconciled:
                    await db.commit()
                interrupted_count = len(runs)
        for storage_key in obsolete_storage_keys:
            self.artifact_store.remove(storage_key)
        await self.cleanup_evidence()
        if (
            self.retention_interval > 0
            and (self._retention_task is None or self._retention_task.done())
        ):
            self._retention_task = asyncio.create_task(
                self._retention_loop(),
                name="test-harness-evidence-retention",
            )
        return interrupted_count

    async def shutdown(self) -> None:
        retention_task = self._retention_task
        self._retention_task = None
        if retention_task is not None and not retention_task.done():
            retention_task.cancel()
        tasks = [task for task in self._pipelines.values() if not task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if retention_task is not None:
            await asyncio.gather(retention_task, return_exceptions=True)

    def _register_pipeline(self, run_id: str, task: asyncio.Task[None]) -> None:
        existing = self._pipelines.get(run_id)
        if existing is not None and not existing.done() and existing is not task:
            task.cancel()
            raise TestHarnessBusyError("Harness run already owns a pipeline")
        self._pipelines[run_id] = task

        def _done(done: asyncio.Task[None]) -> None:
            if self._pipelines.get(run_id) is done:
                self._pipelines.pop(run_id, None)

        task.add_done_callback(_done)


def _workspace_stage_title(stage: str) -> str:
    return {
        "queued": "测试已进入队列",
        "fingerprinted": "工作区指纹已记录",
        "starting_preview": "正在启动隔离预览",
        "preview_ready": "隔离预览已就绪",
        "browser_agent_queued": "黑盒浏览器 Agent 已排队",
        "waiting_for_agent": "等待黑盒浏览器 Agent",
        "agent_starting": "黑盒浏览器 Agent 正在启动",
        "browser_ready": "浏览器已打开页面",
        "executing_actions": "正在执行测试场景",
        "agent_reported": "黑盒浏览器 Agent 已提交报告",
        "completed": "测试运行已完成",
        "stale": "测试结果已过期",
        "failed": "测试运行失败",
        "cancelled": "测试运行已停止",
        "interrupted": "测试被服务重启中断",
    }.get(stage, stage.replace("_", " "))


def _browser_stage_title(stage: str) -> str:
    return {
        "queued": "浏览器任务等待执行",
        "waiting_for_agent": "等待黑盒 Agent",
        "waiting_for_browser": "等待浏览器启动",
        "agent_starting": "黑盒 Agent 正在启动",
        "browser_ready": "浏览器页面已打开",
        "executing_actions": "浏览器正在验证页面",
        "agent_reported": "Agent 已提交结构化结果",
        "completed": "浏览器测试已完成",
        "failed": "浏览器测试失败",
        "cancelled": "浏览器测试已停止",
    }.get(stage, stage.replace("_", " "))


def _safe_error(exc: BaseException) -> str:
    value = str(exc).strip() or exc.__class__.__name__
    return value[:4000]


def _valid_evidence_names(values: list[Any]) -> set[str]:
    return {
        value
        for value in values
        if isinstance(value, str) and _EVIDENCE_NAME_RE.fullmatch(value)
    }


def _archive_manifest(value: Any) -> dict[str, Any]:
    raw = value if isinstance(value, dict) else {}
    expected = raw.get("expected")
    archived = raw.get("archived")
    return {
        "version": 1,
        "expected": sorted(
            _valid_evidence_names(expected if isinstance(expected, list) else [])
        ),
        "archived": archived if isinstance(archived, dict) else {},
        "terminal_status": (
            raw.get("terminal_status")
            if raw.get("terminal_status") in _BROWSER_TERMINAL
            else None
        ),
    }


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _json_copy(value: Any) -> Any:
    return json.loads(json.dumps(value, ensure_ascii=False, default=str))


test_harness_service = TestHarnessService()
