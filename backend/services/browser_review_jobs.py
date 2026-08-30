"""In-process orchestration for CCM Browser Review jobs."""

from __future__ import annotations

import asyncio
import base64
import binascii
import copy
import json
import logging
import re
import stat
import uuid
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable
from urllib.parse import urlsplit

from sqlalchemy import select

from backend.config import settings
from backend.services.browser_review import (
    BrowserReviewOptions,
    BrowserReviewResult,
    run_browser_review,
)
from backend.services.test_harness_contracts import (
    normalize_findings,
    normalize_verdict,
)
from backend.services.test_harness_artifacts import (
    TestHarnessArtifactStore,
    test_harness_artifact_store,
)


logger = logging.getLogger(__name__)


_ARTIFACT_NAME_RE = re.compile(
    r"(?:initial\.png|final\.png|step-\d{2,3}\.png|report\.md|"
    r"telemetry\.json|response\.json|actions\.jsonl)"
)
_TERMINAL_STATUSES = frozenset({"completed", "failed", "cancelled"})
_TASK_TERMINAL_STATUSES = frozenset(
    {"completed", "failed", "cancelled", "conflict"}
)
_MAX_SCREENSHOT_BYTES = 15 * 1024 * 1024
BrowserReviewRunner = Callable[..., Awaitable[BrowserReviewResult]]
BrowserReviewTaskReader = Callable[[int], Awaitable[dict[str, Any] | None]]
BrowserReviewAdmissionReclaimer = Callable[[int], Awaitable[int | None]]


class BrowserReviewBusyError(RuntimeError):
    """Raised when the single demo browser slot is already occupied."""


def _managed_preview_tokens(url: str) -> tuple[str, ...]:
    """Return exact private URL forms that may occur in public evidence."""

    candidates = {url, url.rstrip("/")}
    try:
        parsed = urlsplit(url)
        if parsed.scheme and parsed.netloc:
            candidates.add(f"{parsed.scheme}://{parsed.netloc}")
    except ValueError:
        pass
    return tuple(sorted((item for item in candidates if item), key=len, reverse=True))


def redact_managed_preview_urls(value: Any, urls: list[str] | tuple[str, ...]) -> Any:
    """Clone one public payload while removing exact managed Preview routes."""

    tokens = tuple(
        dict.fromkeys(
            token
            for url in urls
            if isinstance(url, str) and url
            for token in _managed_preview_tokens(url)
        )
    )

    def redact(item: Any) -> Any:
        if isinstance(item, str):
            for token in tokens:
                item = item.replace(token, "[managed-preview]")
            return item
        if isinstance(item, dict):
            return {key: redact(child) for key, child in item.items()}
        if isinstance(item, list):
            return [redact(child) for child in item]
        if isinstance(item, tuple):
            return tuple(redact(child) for child in item)
        return copy.deepcopy(item)

    return redact(value)


def public_browser_review_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Hide an ephemeral managed-preview origin from a browser result."""

    if payload.get("network_policy") != "managed_preview":
        return copy.deepcopy(payload)
    private_url = payload.get("url")
    projected = redact_managed_preview_urls(
        payload,
        [private_url] if isinstance(private_url, str) else [],
    )
    projected["url"] = None
    return projected


@dataclass
class BrowserReviewJob:
    id: str
    options: BrowserReviewOptions
    capture_only: bool
    created_at: str
    provider: str = "codex"
    codex_service_tier: str = "default"
    task_id: int | None = None
    # Optional owning Task when the browser is driven by a separate, hidden
    # agent Task.  ``task_id`` remains the exact agent lifecycle being watched.
    owner_task_id: int | None = None
    harness_run_id: str | None = None
    inline_tool: bool = False
    status: str = "queued"
    stage: str = "queued"
    started_at: str | None = None
    completed_at: str | None = None
    error: str | None = None
    response_id: str | None = None
    steps: int = 0
    actions: int = 0
    latest_screenshot: str | None = None
    telemetry: dict[str, Any] = field(default_factory=dict)
    action_batches: list[dict[str, Any]] = field(default_factory=list)
    trace_events: list[dict[str, Any]] = field(default_factory=list)
    verdict: str | None = None
    findings: list[dict[str, Any]] = field(default_factory=list)
    coverage: dict[str, Any] = field(default_factory=dict)
    _trace_log_ids: set[int] = field(default_factory=set, repr=False)
    task: asyncio.Task[None] | None = field(default=None, repr=False)

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "task_id": self.task_id,
            "owner_task_id": self.owner_task_id,
            "harness_run_id": self.harness_run_id,
            "inline_tool": self.inline_tool,
            "status": self.status,
            "stage": self.stage,
            "url": self.options.url,
            "network_policy": self.options.network_policy,
            "goal": self.options.goal,
            "provider": self.provider,
            "model": self.options.model,
            "reasoning_effort": self.options.reasoning_effort,
            "codex_service_tier": self.codex_service_tier,
            "allow_actions": self.options.allow_actions,
            "capture_only": self.capture_only,
            "browser_channel": self.options.browser_channel or "chromium",
            "viewport_width": self.options.viewport_width,
            "viewport_height": self.options.viewport_height,
            "max_steps": self.options.max_steps,
            "max_actions": self.options.max_actions,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "error": self.error,
            "response_id": self.response_id,
            "steps": self.steps,
            "actions": self.actions,
            "latest_screenshot": self.latest_screenshot,
            "telemetry": self.telemetry,
            "action_batches": list(self.action_batches),
            "trace": list(self.trace_events),
            "verdict": self.verdict,
            "findings": list(self.findings),
            "coverage": dict(self.coverage),
            "artifacts": self.artifact_names(),
            "report": self._read_report(),
        }

    def public_dict(self) -> dict[str, Any]:
        """Return the user-facing form without a managed loopback origin."""

        return public_browser_review_payload(self.as_dict())

    def artifact_names(self) -> list[str]:
        output_dir = self.options.output_dir
        if output_dir is None or not output_dir.is_dir():
            return []
        names: list[str] = []
        for candidate in output_dir.iterdir():
            if (
                _ARTIFACT_NAME_RE.fullmatch(candidate.name)
                and not candidate.is_symlink()
                and candidate.is_file()
            ):
                names.append(candidate.name)
        return sorted(names, key=_artifact_sort_key)

    def _read_report(self) -> str | None:
        output_dir = self.options.output_dir
        if output_dir is None:
            return None
        report = output_dir / "report.md"
        try:
            info = report.lstat()
            if not stat.S_ISREG(info.st_mode) or report.is_symlink():
                return None
            if info.st_size > 1_000_000:
                return None
            return report.read_text(encoding="utf-8")
        except (FileNotFoundError, OSError, UnicodeError):
            return None


class BrowserReviewJobManager:
    """Own one browser review slot and bridge agent Tasks to browser evidence."""

    def __init__(
        self,
        runner: BrowserReviewRunner = run_browser_review,
        *,
        task_reader: BrowserReviewTaskReader | None = None,
        poll_interval: float = 0.5,
        artifact_store: TestHarnessArtifactStore | None = None,
        history_limit: int | None = None,
        admission_reclaimer: BrowserReviewAdmissionReclaimer | None = None,
    ) -> None:
        self._runner = runner
        self._task_reader = task_reader or _read_task_snapshot
        self._poll_interval = poll_interval
        self._artifact_store = artifact_store or test_harness_artifact_store
        self._history_limit = max(
            1,
            int(
                settings.browser_review_job_history_limit
                if history_limit is None
                else history_limit
            ),
        )
        self._jobs: dict[str, BrowserReviewJob] = {}
        self._lock = asyncio.Lock()
        self._admission_reclaimer = admission_reclaimer

    async def start(
        self,
        options: BrowserReviewOptions,
        *,
        capture_only: bool,
        api_key: str | None,
    ) -> BrowserReviewJob:
        """Run the legacy capture/direct-Responses path used by the CLI demo."""

        job = await self._reserve(
            options,
            capture_only=capture_only,
            provider="capture" if capture_only else "openai-responses",
            codex_service_tier="default",
        )
        job.task = asyncio.create_task(
            self._run_capture(job, api_key=api_key),
            name=f"browser-review-capture-{job.id}",
        )
        return job

    async def prepare_agent(
        self,
        options: BrowserReviewOptions,
        *,
        provider: str,
        codex_service_tier: str,
        harness_run_id: str | None = None,
    ) -> BrowserReviewJob:
        """Reserve the browser slot before the caller persists its CCM Task."""

        return await self._reserve(
            options,
            capture_only=False,
            provider=provider,
            codex_service_tier=codex_service_tier,
            harness_run_id=harness_run_id,
        )

    async def prepare_task_tool(
        self,
        options: BrowserReviewOptions,
        *,
        task_id: int,
        provider: str,
        codex_service_tier: str,
        harness_run_id: str | None = None,
    ) -> BrowserReviewJob:
        """Reserve a browser run owned by an already-running ordinary Task."""

        job = await self._reserve(
            options,
            capture_only=False,
            provider=provider,
            codex_service_tier=codex_service_tier,
            harness_run_id=harness_run_id,
        )
        async with self._lock:
            job.task_id = task_id
            job.inline_tool = True
            job.stage = "waiting_for_browser"
            job.task = asyncio.create_task(
                self._watch_inline_task(job),
                name=f"browser-review-inline-task-{task_id}-{job.id}",
            )
        return job

    async def attach_task(
        self,
        job_id: str,
        task_id: int,
        *,
        owner_task_id: int | None = None,
    ) -> BrowserReviewJob:
        async with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(job_id)
            if job.task_id is not None:
                raise RuntimeError("Browser Review job already has a Task")
            job.task_id = task_id
            job.owner_task_id = owner_task_id
            job.stage = "waiting_for_agent"
            job.task = asyncio.create_task(
                self._watch_task(job),
                name=f"browser-review-task-{task_id}",
            )
            return job

    async def fail_start(self, job_id: str, exc: BaseException) -> None:
        async with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return
            job.status = "failed"
            job.stage = "failed"
            job.error = _safe_error(exc)
            job.completed_at = _now()

    async def get(self, job_id: str) -> BrowserReviewJob | None:
        async with self._lock:
            return self._jobs.get(job_id)

    async def list(self) -> list[BrowserReviewJob]:
        async with self._lock:
            return list(reversed(self._jobs.values()))

    async def list_for_task(self, task_id: int) -> list[BrowserReviewJob]:
        """Return this Task's runs and refresh their user-visible trace."""

        async with self._lock:
            jobs = [
                job
                for job in reversed(self._jobs.values())
                if (job.owner_task_id or job.task_id) == task_id
            ]
        # A workspace review is displayed on its owner Task but its trace is
        # produced by the isolated agent Task. Read the exact producer for
        # every run instead of accidentally merging the parent's conversation.
        for job in jobs:
            if job.task_id is not None:
                snapshot = await self._task_reader(job.task_id)
                if snapshot is not None:
                    self._merge_trace_events(job, snapshot.get("trace_events"))
        return jobs

    async def mark_cancelling(self, job_id: str) -> BrowserReviewJob | None:
        async with self._lock:
            job = self._jobs.get(job_id)
            if job is not None and job.status not in _TERMINAL_STATUSES:
                job.stage = "cancelling"
            return job

    async def cancel(self, job_id: str) -> BrowserReviewJob | None:
        """Cancel a capture runner. Agent Task cancellation is handled by its API."""

        async with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            task = job.task
            if job.status in _TERMINAL_STATUSES or task is None or task.done():
                return job
            job.stage = "cancelling"
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        if job.status not in _TERMINAL_STATUSES:
            job.status = "cancelled"
            job.stage = "cancelled"
            job.completed_at = _now()
        return job

    async def shutdown(self) -> None:
        async with self._lock:
            tasks = [
                job.task
                for job in self._jobs.values()
                if job.task is not None and not job.task.done()
            ]
            for task in tasks:
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        # Staging retention is owned by TestHarnessService, which can consult
        # durable archive state. A Browser manager shutdown cannot prove that
        # a terminal/in-memory job has been archived safely.

    async def context(self, job_id: str) -> dict[str, Any] | None:
        job = await self.get(job_id)
        if (
            job is None
            or job.capture_only
            or job.task_id is None
            or job.status in _TERMINAL_STATUSES
        ):
            return None
        return {
            "job_id": job.id,
            "task_id": job.task_id,
            "url": job.options.url,
            "network_policy": job.options.network_policy,
            "goal": job.options.goal,
            "provider": job.provider,
            "model": job.options.model,
            "reasoning_effort": job.options.reasoning_effort,
            "allow_actions": job.options.allow_actions,
            "browser_channel": job.options.browser_channel or "chromium",
            "viewport_width": job.options.viewport_width,
            "viewport_height": job.options.viewport_height,
            "max_steps": job.options.max_steps,
            "max_actions": job.options.max_actions,
        }

    async def record_event(self, job_id: str, event: dict[str, Any]) -> BrowserReviewJob:
        async with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(job_id)
            if job.capture_only or job.task_id is None:
                raise RuntimeError("Browser Review job is not attached to an agent Task")
            if job.status in _TERMINAL_STATUSES:
                raise RuntimeError("Browser Review job is already terminal")

            stage = str(event.get("stage") or job.stage)
            job.status = "running"
            job.stage = stage
            job.started_at = job.started_at or _now()
            job.steps = max(job.steps, int(event.get("steps") or 0))
            job.actions = max(job.actions, int(event.get("actions") or 0))
            telemetry = event.get("telemetry")
            if isinstance(telemetry, dict):
                job.telemetry = telemetry
                encoded_telemetry = json.dumps(
                    telemetry,
                    ensure_ascii=False,
                    indent=2,
                )
                self._write_artifact_text(job, "telemetry.json", encoded_telemetry)

            action_batch = event.get("action_batch")
            if isinstance(action_batch, list) and action_batch:
                batch = {"step": job.steps, "actions": action_batch}
                self._append_artifact_jsonl(job, "actions.jsonl", batch)
                job.action_batches.append(batch)
                if len(job.action_batches) > 250:
                    job.action_batches = job.action_batches[-250:]

            screenshot = _decode_screenshot(event.get("screenshot_base64"))
            if screenshot is not None:
                if stage == "browser_ready" and not (
                    job.options.output_dir / "initial.png"
                ).exists():
                    screenshot_name = "initial.png"
                elif stage in {"agent_reported", "browser_closed"}:
                    screenshot_name = "final.png"
                else:
                    screenshot_name = f"step-{max(1, job.steps):02d}.png"
                self._write_artifact_bytes(job, screenshot_name, screenshot)
                job.latest_screenshot = screenshot_name

            report = event.get("report")
            if isinstance(report, str) and report.strip():
                report_value = report.strip()
                self._write_artifact_text(job, "report.md", report_value)
            if report is not None or event.get("verdict") is not None:
                job.verdict = normalize_verdict(event.get("verdict"), report=report)
            if event.get("findings") is not None:
                job.findings = normalize_findings(event.get("findings"))
            coverage = event.get("coverage")
            if isinstance(coverage, dict):
                job.coverage = json.loads(json.dumps(coverage))
            if job.inline_tool:
                if stage == "agent_reported":
                    job.status = "completed"
                    job.stage = "completed"
                    job.completed_at = _now()
                elif stage == "cancelled":
                    job.status = "cancelled"
                    job.stage = "cancelled"
                    job.completed_at = _now()
                elif stage == "browser_closed":
                    job.status = "failed"
                    job.stage = "failed"
                    job.error = "Browser closed before the review report was submitted"
                    job.completed_at = _now()
            return job

    async def resolve_artifact(self, job_id: str, name: str) -> Path | None:
        if not _ARTIFACT_NAME_RE.fullmatch(name):
            return None
        job = await self.get(job_id)
        if job is None or job.options.output_dir is None:
            return None
        path = job.options.output_dir / name
        try:
            info = path.lstat()
        except OSError:
            return None
        if not stat.S_ISREG(info.st_mode) or path.is_symlink():
            return None
        return path

    async def _reserve(
        self,
        options: BrowserReviewOptions,
        *,
        capture_only: bool,
        provider: str,
        codex_service_tier: str,
        harness_run_id: str | None = None,
    ) -> BrowserReviewJob:
        options.validate()
        if self._artifact_store.total_bytes() >= self._artifact_store.max_total_bytes:
            if self._admission_reclaimer is not None:
                await self._admission_reclaimer(1)
            else:
                # Runtime import avoids a module cycle. The identity check is
                # important for CLI/tests that intentionally supply another
                # artifact store and therefore have no durable Harness DB.
                from backend.services.test_harness import test_harness_service

                if test_harness_service.artifact_store is self._artifact_store:
                    await test_harness_service.cleanup_evidence(
                        required_free_bytes=1
                    )
        async with self._lock:
            self._prune_terminal_locked()
            if any(
                job.status not in _TERMINAL_STATUSES for job in self._jobs.values()
            ):
                raise BrowserReviewBusyError(
                    "A browser review is already running; wait for it or cancel it"
                )
            protected_job_ids = {
                job.id
                for job in self._jobs.values()
                if job.status not in _TERMINAL_STATUSES
            }
            try:
                protected_job_ids.update(
                    await _read_incomplete_archive_job_ids()
                )
            except Exception:
                # Losing the durable archive view must fail closed: cleanup is
                # optional, while deleting the only retryable staging copy is
                # irreversible. create_job_dir will return a quota error if
                # the store is already full.
                logger.exception(
                    "Could not read protected Browser Review staging owners"
                )
            else:
                self._artifact_store.cleanup_job_dirs(
                    active_job_ids=protected_job_ids,
                )
            job_id = uuid.uuid4().hex
            output_dir = self._artifact_store.create_job_dir(job_id)
            job = BrowserReviewJob(
                id=job_id,
                options=replace(options, output_dir=output_dir),
                capture_only=capture_only,
                provider=provider,
                codex_service_tier=codex_service_tier,
                harness_run_id=harness_run_id,
                created_at=_now(),
            )
            self._jobs[job.id] = job
            return job

    async def _run_capture(self, job: BrowserReviewJob, *, api_key: str | None) -> None:
        job.status = "running"
        job.stage = "launching_browser"
        job.started_at = _now()

        def on_progress(event: dict[str, Any]) -> None:
            job.stage = str(event.get("stage") or job.stage)
            job.steps = int(event.get("steps") or 0)
            job.actions = int(event.get("actions") or 0)
            screenshot = event.get("latest_screenshot")
            if isinstance(screenshot, str):
                job.latest_screenshot = screenshot
            telemetry = event.get("telemetry")
            if isinstance(telemetry, dict):
                job.telemetry = telemetry
            action_batch = event.get("action_batch")
            if isinstance(action_batch, list):
                job.action_batches.append(
                    {"step": job.steps + 1, "actions": action_batch}
                )

        try:
            result = await self._runner(
                job.options,
                api_key=api_key,
                capture_only=job.capture_only,
                progress_callback=on_progress,
                artifact_store=self._artifact_store,
            )
        except asyncio.CancelledError:
            job.status = "cancelled"
            job.stage = "cancelled"
            job.completed_at = _now()
        except Exception as exc:
            job.status = "failed"
            job.stage = "failed"
            job.error = _safe_error(exc)
            job.completed_at = _now()
        else:
            job.status = "completed"
            job.stage = "completed"
            job.response_id = result.response_id
            job.steps = result.steps
            job.actions = result.actions
            job.latest_screenshot = result.screenshot_path.name
            job.completed_at = _now()

    async def _watch_task(self, job: BrowserReviewJob) -> None:
        assert job.task_id is not None
        try:
            while True:
                snapshot = await self._task_reader(job.task_id)
                if snapshot is None:
                    raise RuntimeError("Browser Review Task no longer exists")
                self._merge_trace_events(job, snapshot.get("trace_events"))
                status = str(snapshot.get("status") or "")
                if status in {"in_progress", "executing"}:
                    job.status = "running"
                    job.started_at = job.started_at or _now()
                    if job.stage == "waiting_for_agent":
                        job.stage = "agent_starting"
                if status in _TASK_TERMINAL_STATUSES:
                    assistant_report = snapshot.get("assistant_report")
                    if not job._read_report() and isinstance(assistant_report, str):
                        report_value = assistant_report.strip()
                        self._write_artifact_text(job, "report.md", report_value)
                    try:
                        from backend.services.test_harness_children import (
                            test_harness_child_service,
                        )

                        reaped = await test_harness_child_service.mark_terminal_by_child(
                            job.task_id,
                            task_status=status,
                            error=str(snapshot.get("error") or "") or None,
                        )
                    except Exception:
                        logger.exception(
                            "Could not read Browser child reap receipt for Task %s",
                            job.task_id,
                        )
                        reaped = False
                    if not reaped:
                        # Dispatcher finalization clears the exact reverse
                        # Instance owner only after the process group, output
                        # consumer and descendants are gone, then writes the
                        # durable binding receipt in that same transaction.
                        job.status = "running"
                        job.stage = "finalizing_agent_cleanup"
                        await asyncio.sleep(self._poll_interval)
                        continue
                    if status == "completed":
                        job.status = "completed"
                        job.stage = "completed"
                    elif status == "cancelled":
                        job.status = "cancelled"
                        job.stage = "cancelled"
                    else:
                        job.status = "failed"
                        job.stage = "failed"
                        job.error = str(
                            snapshot.get("error")
                            or f"Browser Review Task ended with status {status}"
                        )
                    job.completed_at = _now()
                    return
                await asyncio.sleep(self._poll_interval)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            job.status = "failed"
            job.stage = "failed"
            job.error = _safe_error(exc)
            job.completed_at = _now()

    async def _watch_inline_task(self, job: BrowserReviewJob) -> None:
        """Release an inline browser slot if its parent Task exits unexpectedly."""

        assert job.task_id is not None
        try:
            while True:
                if job.status in _TERMINAL_STATUSES:
                    return
                snapshot = await self._task_reader(job.task_id)
                if snapshot is None:
                    raise RuntimeError("Parent Task no longer exists")
                self._merge_trace_events(job, snapshot.get("trace_events"))
                task_status = str(snapshot.get("status") or "")
                if task_status in _TASK_TERMINAL_STATUSES:
                    if task_status == "cancelled":
                        job.status = "cancelled"
                        job.stage = "cancelled"
                    else:
                        job.status = "failed"
                        job.stage = "failed"
                        job.error = str(
                            snapshot.get("error")
                            or "Parent Task ended before finish_review submitted a report"
                        )
                    job.completed_at = _now()
                    return
                await asyncio.sleep(self._poll_interval)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if job.status not in _TERMINAL_STATUSES:
                job.status = "failed"
                job.stage = "failed"
                job.error = _safe_error(exc)
                job.completed_at = _now()

    @staticmethod
    def _job_output_dir(job: BrowserReviewJob) -> Path:
        output_dir = job.options.output_dir
        if output_dir is None:
            raise RuntimeError("Browser Review job has no evidence directory")
        return output_dir

    def _write_artifact_bytes(
        self,
        job: BrowserReviewJob,
        name: str,
        value: bytes,
    ) -> None:
        self._artifact_store.write_job_bytes(
            self._job_output_dir(job),
            name,
            value,
        )

    def _write_artifact_text(
        self,
        job: BrowserReviewJob,
        name: str,
        value: str,
    ) -> None:
        self._artifact_store.write_job_text(
            self._job_output_dir(job),
            name,
            value,
        )

    def _append_artifact_jsonl(
        self,
        job: BrowserReviewJob,
        name: str,
        value: dict[str, Any],
    ) -> None:
        self._artifact_store.append_job_jsonl(
            self._job_output_dir(job),
            name,
            value,
        )

    def _prune_terminal_locked(self) -> None:
        terminal_ids = [
            job_id
            for job_id, job in self._jobs.items()
            if job.status in _TERMINAL_STATUSES
        ]
        excess = max(0, len(terminal_ids) - self._history_limit + 1)
        for job_id in terminal_ids[:excess]:
            self._jobs.pop(job_id, None)
            # Durable Harness archive state decides when staging can be
            # reclaimed. Removing it here could destroy the only retry source
            # after an interrupted or failed archive transaction.

    @staticmethod
    def _merge_trace_events(job: BrowserReviewJob, events: Any) -> None:
        if not isinstance(events, list):
            return
        for event in events:
            if not isinstance(event, dict):
                continue
            timestamp = event.get("timestamp")
            if (
                job.inline_tool
                and
                isinstance(timestamp, str)
                and timestamp
                and timestamp < job.created_at
            ):
                continue
            log_id = event.get("id")
            if (
                not isinstance(log_id, int)
                or log_id in job._trace_log_ids
            ):
                continue
            job._trace_log_ids.add(log_id)
            job.trace_events.append(event)
        job.trace_events.sort(
            key=lambda event: (
                event.get("timestamp") or "",
                event.get("id") or 0,
            )
        )
        if len(job.trace_events) > 250:
            job.trace_events = job.trace_events[-250:]


async def _read_task_snapshot(task_id: int) -> dict[str, Any] | None:
    from backend.database import async_session
    from backend.models.log_entry import LogEntry
    from backend.models.task import Task

    async with async_session() as db:
        task = await db.get(Task, task_id)
        if task is None:
            return None
        assistant_report = (
            await db.execute(
                select(LogEntry.content)
                .where(
                    LogEntry.task_id == task_id,
                    LogEntry.role == "assistant",
                    LogEntry.event_type.in_(("message", "result")),
                    LogEntry.content.is_not(None),
                )
                .order_by(LogEntry.id.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        trace_rows = (
            await db.execute(
                select(
                    LogEntry.id,
                    LogEntry.event_type,
                    LogEntry.role,
                    LogEntry.content,
                    LogEntry.tool_name,
                    LogEntry.tool_input,
                    LogEntry.timestamp,
                )
                .where(
                    LogEntry.task_id == task_id,
                    (
                        (
                            LogEntry.event_type == "message"
                        )
                        & (LogEntry.role == "assistant")
                    )
                    | (
                        (LogEntry.event_type == "tool_use")
                        & (
                            LogEntry.tool_name.like("%browser_review%")
                            | LogEntry.tool_name.like("%frontend_review%")
                        )
                    ),
                )
                .order_by(LogEntry.id.desc())
                .limit(250)
            )
        ).all()
        chronological_rows = sorted(
            trace_rows,
            key=lambda row: (row.timestamp, row.id),
        )
        return {
            "status": task.status,
            "error": task.error_message,
            "assistant_report": assistant_report,
            "trace_events": [
                event
                for row in chronological_rows
                if not (
                    row.event_type == "message"
                    and row.content == assistant_report
                )
                if (event := _trace_event_from_log_row(row)) is not None
            ],
        }


_BROWSER_TOOL_TITLES = {
    "browser_open": "打开目标页面",
    "browser_observe": "观察当前页面",
    "browser_inspect": "检查页面结构",
    "browser_scroll": "滚动查看页面",
    "browser_wait": "等待页面稳定",
    "browser_move": "移动指针",
    "browser_click": "点击页面元素",
    "browser_double_click": "双击页面元素",
    "browser_type_text": "输入测试文本",
    "browser_keypress": "执行键盘操作",
    "browser_drag": "拖动页面元素",
    "finish_review": "提交审查报告",
    "start_review": "启动前端运行审查",
    "check_review": "检查审查进度",
    "stop_review": "停止前端运行审查",
}


def _trace_event_from_log_row(row: Any) -> dict[str, Any] | None:
    log_id, event_type, _role, content, tool_name, tool_input, timestamp = row
    timestamp_text = timestamp.isoformat() + "Z" if timestamp is not None else None
    if event_type == "message" and isinstance(content, str) and content.strip():
        text = content.strip()
        return {
            "id": log_id,
            "kind": "decision",
            "title": "模型观察与决策",
            "detail": _clip_trace_text(text),
            "timestamp": timestamp_text,
        }
    if event_type != "tool_use" or not isinstance(tool_name, str):
        return None
    short_name = tool_name.rsplit(".", 1)[-1]
    arguments = _safe_tool_arguments(short_name, tool_input)
    return {
        "id": log_id,
        "kind": "tool",
        "title": _BROWSER_TOOL_TITLES.get(short_name, short_name),
        "detail": arguments,
        "tool_name": short_name,
        "timestamp": timestamp_text,
    }


def _safe_tool_arguments(tool_name: str, raw: Any) -> str | None:
    try:
        payload = json.loads(raw) if isinstance(raw, str) and raw else {}
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    if tool_name == "finish_review":
        return None
    if tool_name == "browser_type_text" and isinstance(payload.get("text"), str):
        payload["text"] = f"<{len(payload['text'])} chars redacted>"
    if not payload:
        return None
    return _clip_trace_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ": ")),
        limit=800,
    )


def _clip_trace_text(value: str, limit: int = 1_600) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 14] + "...<truncated>"


def _decode_screenshot(value: Any) -> bytes | None:
    if value is None:
        return None
    if not isinstance(value, str) or len(value) > (_MAX_SCREENSHOT_BYTES * 4 // 3 + 8):
        raise ValueError("Browser Review screenshot is invalid or too large")
    try:
        screenshot = base64.b64decode(value, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise ValueError("Browser Review screenshot is not valid base64") from exc
    if len(screenshot) > _MAX_SCREENSHOT_BYTES or not screenshot.startswith(b"\x89PNG\r\n\x1a\n"):
        raise ValueError("Browser Review screenshot must be a PNG up to 15 MiB")
    return screenshot


def _artifact_sort_key(name: str) -> tuple[int, str]:
    if name == "initial.png":
        return (0, name)
    if name.startswith("step-"):
        return (1, name)
    if name == "final.png":
        return (2, name)
    return (3, name)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_error(exc: BaseException) -> str:
    text = str(exc).strip() or exc.__class__.__name__
    if len(text) > 2_000:
        return text[:1_986] + "...<truncated>"
    return text


async def _read_incomplete_archive_job_ids() -> set[str]:
    """Return staging owners whose durable evidence is not yet complete."""

    from backend.database import async_session
    from backend.models.test_harness import TestHarnessAttempt
    from backend.services.test_harness import ARCHIVE_COMPLETE

    async with async_session() as db:
        return set(
            (
                await db.execute(
                    select(TestHarnessAttempt.browser_review_job_id).where(
                        TestHarnessAttempt.archive_state != ARCHIVE_COMPLETE,
                        TestHarnessAttempt.browser_review_job_id.is_not(None),
                    )
                )
            ).scalars()
        )


browser_review_job_manager = BrowserReviewJobManager()
