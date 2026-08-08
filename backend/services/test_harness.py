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
from sqlalchemy import delete, select

from backend.config import settings
from backend.database import async_session
from backend.models.project import Project
from backend.models.task import Task
from backend.models.test_harness import (
    TestHarnessAttempt,
    TestHarnessEvent,
    TestHarnessEvidence,
    TestHarnessFinding,
    TestHarnessRun,
    TestHarnessSandboxLease,
)
from backend.models.workspace_review import WorkspaceReviewRun
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
from backend.services.test_harness_artifacts import (
    OpenedHarnessArtifact,
    TestHarnessArtifactError,
    TestHarnessArtifactStore,
    test_harness_artifact_store,
)


logger = logging.getLogger(__name__)

_WORKSPACE_TERMINAL = frozenset({"completed", "failed", "cancelled"})
_BROWSER_TERMINAL = frozenset({"completed", "failed", "cancelled"})
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
    ) -> TestHarnessRun:
        normalized = spec.normalized()
        async with self.db_factory() as db:
            task = await db.get(Task, task_id)
            if task is None:
                raise TestHarnessError("Task not found")
            project = await db.get(Project, task.project_id) if task.project_id else None
            runtime = self._runtime_for_task(task, normalized)
            plan = compile_test_plan(
                goal=normalized.goal,
                profile=normalized.profile,
                allow_actions=normalized.allow_actions,
                viewport_width=normalized.viewport_width,
                viewport_height=normalized.viewport_height,
                max_steps=normalized.max_steps or 20,
                max_actions=normalized.max_actions or 0,
                supplied=normalized.test_plan,
            )
            project_id = project.id if project is not None else None
            if normalized.target_kind in {"pull_request", "git_ref"}:
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
            spec=normalized,
            plan=plan,
            runtime=runtime,
        )
        if not created:
            return run

        if normalized.target_kind == "current_workspace":
            try:
                await self._start_workspace_review(
                    run_id=run.id,
                    spec=normalized,
                    test_plan=plan,
                )
            except BaseException as exc:
                await self._fail_start(run.id, exc)
                raise
        elif normalized.target_kind == "fixed_url":
            # A fixed URL needs the caller to reserve either an inline browser
            # tool or a separate Task, then attach that exact job below.
            await self._update_run(
                run.id,
                values={"stage": "waiting_for_browser"},
                event_type="lifecycle",
                title="等待浏览器执行器",
                source_key="harness:waiting-for-browser",
            )
        elif normalized.target_kind in {"pull_request", "git_ref"}:
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
    ) -> tuple[TestHarnessRun, bool]:
        scope = f"task:{task_id}" if task_id is not None else f"admin:{owner_user_id or 0}"
        fingerprint = request_fingerprint(
            target_kind=spec.target_kind,
            target=spec.target,
            test_plan=plan,
            runtime=runtime,
        )
        async with self._lock:
            async with self.db_factory() as db:
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
                        return existing, False
                if task_id is not None:
                    active = await db.scalar(
                        select(TestHarnessRun.id).where(
                            TestHarnessRun.task_id == task_id,
                            TestHarnessRun.status.not_in(HARNESS_TERMINAL_STATUSES),
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
        async with self.db_factory() as db:
            task = await db.get(Task, run.task_id)
            project = await db.get(Project, task.project_id) if task and task.project_id else None
            if task is None:
                raise TestHarnessError("Harness owner Task disappeared")
            preview_config = project.preview_config if project is not None else None
        if preview_config is None:
            raise TestHarnessError("Project has no confirmed Preview configuration")

        from backend.services.workspace_review import workspace_review_manager

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
            test_plan=test_plan,
            runtime_config=run.runtime_config,
        )
        await self._update_run(
            run_id,
            values={
                "workspace_review_run_id": workspace_run.id,
                "source_git_head": workspace_run.git_head,
                "source_fingerprint": workspace_run.workspace_fingerprint,
                "status": "preparing_environment",
                "stage": "fingerprinted",
                "started_at": workspace_run.started_at or datetime.utcnow(),
            },
            event_type="lifecycle",
            title="已锁定测试目标",
            detail=f"Commit {workspace_run.git_head[:12]}，工作区指纹 {workspace_run.workspace_fingerprint[:12]}。",
            source_key=f"workspace:{workspace_run.id}:linked",
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
            try:
                from backend.services.workspace_review import workspace_review_manager

                await workspace_review_manager.cancel(workspace_review_run_id)
            except Exception:
                logger.exception(
                    "Could not cancel workspace review after Harness watcher failure run=%s",
                    run_id,
                )
            await self._fail_start(run_id, exc)

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
        try:
            await asyncio.shield(self.sandbox_manager.cleanup(run_id))
            return None
        except BaseException as exc:
            return _safe_error(exc)

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
            async with self.db_factory() as db:
                run = await db.get(TestHarnessRun, run_id)
                if run is None or run.task_id is None:
                    raise TestHarnessError("Harness run has no owning Task")
                task = await db.get(Task, run.task_id)
                project = (
                    await db.get(Project, task.project_id)
                    if task is not None and task.project_id
                    else None
                )
                if task is None:
                    raise TestHarnessError("Harness owner Task disappeared")
                kind = run.target_kind
                target = {
                    key: value
                    for key, value in run.target_spec.items()
                    if key != "kind"
                }

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
            await asyncio.shield(self._stop_git_target_children(job))
            cleanup_error = await self._cleanup_git_target(run_id)
            await asyncio.shield(
                self._update_run(
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
            )
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
        async with self._db_lock:
            async with self.db_factory() as db:
                run = await db.get(TestHarnessRun, run_id)
                if run is None:
                    raise TestHarnessError("Harness run not found")
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
        return await self._start_browser_for_url(
            run=run,
            url=str(run.target_spec["url"]),
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
        return await self._start_browser_for_url(
            run=run,
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
            if job is not None:
                await browser_review_job_manager.fail_start(job.id, exc)
            if child_binding_id is not None:
                try:
                    await self.child_service.stop_binding(
                        child_binding_id,
                        reason=f"Browser Agent attach failed: {_safe_error(exc)}",
                    )
                except BaseException:
                    logger.exception(
                        "Could not roll back Browser child binding %s",
                        child_binding_id,
                    )
            if fail_run_on_error:
                await self._fail_start(run_id, exc)
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
            await self._fail_start(run_id, exc)

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
                run = await db.get(TestHarnessRun, run_id)
                if run is None:
                    return
                for key, value in values.items():
                    setattr(run, key, value)
                await self._append_event(
                    db,
                    run,
                    event_type=event_type,
                    title=title,
                    detail=detail,
                    stage=str(values.get("stage") or run.stage),
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

    async def _fail_start(self, run_id: str, exc: BaseException) -> None:
        await self._update_run(
            run_id,
            values={
                "status": "failed",
                "stage": "failed",
                "verdict": "error",
                "error": _safe_error(exc),
                "completed_at": datetime.utcnow(),
            },
            event_type="error",
            title="测试运行失败",
            detail=_safe_error(exc),
            source_key="harness:failed",
        )

    async def _mark_cancelled(self, run_id: str) -> None:
        await self._update_run(
            run_id,
            values={
                "status": "cancelled",
                "stage": "cancelled",
                "verdict": "cancelled",
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
    ) -> TestHarnessRun | None:
        run = await self.get_run_model(run_id)
        if run is None or run.status in HARNESS_TERMINAL_STATUSES:
            return run
        await self._update_run(
            run_id,
            values={"status": "cancelling", "stage": "cancelling"},
            event_type="lifecycle",
            title="正在停止测试运行",
            source_key="harness:cancelling",
        )
        if run.workspace_review_run_id:
            from backend.services.workspace_review import workspace_review_manager

            await workspace_review_manager.cancel(run.workspace_review_run_id)
        else:
            stopped_binding = await self.child_service.stop_for_harness_run(
                run_id,
                reason="Harness run was cancelled",
            )
            if (
                not stopped_binding
                and run.agent_task_id is not None
                and run.agent_task_id != run.task_id
            ):
                if stop_agent_task is not None:
                    await stop_agent_task(run.agent_task_id)
                else:
                    await self._stop_agent_task(run.agent_task_id)
            if run.browser_review_job_id:
                from backend.services.browser_review_jobs import browser_review_job_manager

                await browser_review_job_manager.cancel(run.browser_review_job_id)
        pipeline = self._pipelines.get(run_id)
        if pipeline is not None and not pipeline.done():
            pipeline.cancel()
            await asyncio.gather(pipeline, return_exceptions=True)
        current = await self.get_run_model(run_id)
        if current is not None and current.status not in HARNESS_TERMINAL_STATUSES:
            await self._mark_cancelled(run_id)
        return await self.get_run_model(run_id)

    async def cancel_for_task(self, task_id: int, *, reason: str) -> int:
        """Cascade an explicit owner stop/delete to all active Harness runs."""

        async with self._lock:
            return await self._cancel_for_task_unlocked(task_id, reason=reason)

    @asynccontextmanager
    async def owner_stop_fence(self, task_id: int, *, reason: str):
        """Prevent a new run from racing an explicit owner terminalization."""

        async with self._lock:
            await self._cancel_for_task_unlocked(task_id, reason=reason)
            yield

    async def _cancel_for_task_unlocked(self, task_id: int, *, reason: str) -> int:
        """Cancel owner runs while the global run-admission lock is held."""

        async with self.db_factory() as db:
            run_ids = list(
                (
                    await db.execute(
                        select(TestHarnessRun.id)
                        .where(
                            TestHarnessRun.task_id == task_id,
                            TestHarnessRun.status.not_in(HARNESS_TERMINAL_STATUSES),
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
        if run.workspace_review_run_id:
            workspace = await db.get(WorkspaceReviewRun, run.workspace_review_run_id)
            if workspace is not None:
                from backend.services.workspace_review import workspace_review_run_dict

                workspace_payload = workspace_review_run_dict(workspace)
        browser_payload = attempts[-1].result_data if attempts else None
        latest_attempt = attempts[-1] if attempts else None
        return {
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
            "runtime": run.runtime_config,
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

    async def repeat(self, run_id: str, *, owner_user_id: int | None = None) -> TestHarnessRun:
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
        )

    async def cleanup_evidence(self) -> int:
        """Apply TTL and task/global quotas without touching active runs."""

        cutoff = datetime.utcnow() - timedelta(days=self.artifact_store.retention_days)
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
                        > self.artifact_store.max_total_bytes
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
            # Every in-memory job may still be consumed by its owning pipeline
            # after the Task itself becomes terminal. Incomplete durable
            # attempts additionally retain staging across job-history pruning.
            active_job_ids = {job.id for job in jobs}
            active_job_ids.update(protected_staging_job_ids)
        except Exception:
            # Failure to prove which jobs are active must never turn into
            # deleting their staging evidence under quota pressure.
            clean_job_dirs = False
            active_job_ids = set(protected_staging_job_ids)
        if clean_job_dirs:
            self.artifact_store.cleanup_job_dirs(active_job_ids=active_job_ids)
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
                    sandbox_cleaned = (
                        sandbox_lease is not None
                        and sandbox_lease.cleanup_status == "completed"
                    )
                    run.status = "failed"
                    run.stage = "interrupted"
                    run.verdict = "error"
                    run.error = "Manager restarted before this test run reached a terminal state"
                    run.cleanup_status = (
                        "completed" if sandbox_cleaned else "unconfirmed"
                    )
                    run.cleanup_error = (
                        None
                        if sandbox_cleaned
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
