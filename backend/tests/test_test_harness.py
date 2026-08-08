from __future__ import annotations

import asyncio
import os
import uuid
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from backend.models.project import Project
from backend.models.task import Task
from backend.models.test_harness import (
    TestHarnessAttempt as AttemptModel,
    TestHarnessChildBinding as ChildBindingModel,
    TestHarnessEvidence as EvidenceModel,
    TestHarnessRun as RunModel,
)
from backend.services.browser_review import BrowserReviewOptions
from backend.services.browser_review_jobs import BrowserReviewJob
from backend.services.test_harness import (
    _git_browser_target_context,
    TestHarnessError as HarnessError,
    TestHarnessIdempotencyError as HarnessIdempotencyError,
    TestHarnessService as HarnessService,
)
from backend.services.test_harness_contracts import (
    TestHarnessSpec as HarnessSpec,
    normalize_findings,
)
from backend.services.test_harness_artifacts import TestHarnessArtifactStore as ArtifactStore
from backend.services.test_harness_git_targets import ResolvedGitTarget
from backend.services.test_harness_sandbox import (
    SandboxPreviewSnapshot,
    SandboxSourceSnapshot,
)
from backend.services.test_harness_targets import PreparedGitTarget
from backend.services.test_harness_children import (
    CHILD_READY,
    CHILD_STOPPED,
    TestHarnessChildService as ChildService,
)
from backend.services import test_harness as harness_module


async def _task(db_factory) -> int:
    async with db_factory() as db:
        task = Task(
            title="Harness owner",
            status="completed",
            provider="codex",
            model="gpt-5.6-sol",
            codex_service_tier="default",
            effort_level="high",
        )
        db.add(task)
        await db.commit()
        return task.id


def test_git_browser_context_exposes_metadata_not_diff_content():
    run = SimpleNamespace(
        target_kind="pull_request",
        resolved_target={
            "repository": "acme/ui",
            "pr_number": 7,
            "base_sha": "a" * 40,
            "head_sha": "b" * 40,
            "source_ref": "feature",
            "clone_url": "https://github.com/acme/ui.git",
            "changed_files": [
                {
                    "path": "frontend/src/App.tsx",
                    "status": "modified",
                    "additions": 3,
                    "deletions": 1,
                    "patch": "untrusted source text must not be exposed",
                },
                {
                    "path": "backend/main.py",
                    "status": "modified",
                    "additions": 2,
                    "deletions": 0,
                },
                {
                    "path": "src/assets/logo.svg",
                    "status": "modified",
                    "additions": 1,
                    "deletions": 1,
                },
            ],
        },
    )

    context = _git_browser_target_context(run)

    assert context is not None
    assert context["repository"] == "acme/ui"
    assert [item["path"] for item in context["frontend_changed_files"]] == [
        "frontend/src/App.tsx",
        "src/assets/logo.svg",
    ]
    assert "patch" not in context["changed_files"][0]
    assert "clone_url" not in context


def _completed_job(
    tmp_path,
    *,
    title: str,
    severity: str = "high",
    artifact_store: ArtifactStore | None = None,
) -> BrowserReviewJob:
    job_id = uuid.uuid4().hex
    if artifact_store is not None:
        output = artifact_store.create_job_dir(job_id)
    else:
        output = tmp_path / job_id
        output.mkdir(mode=0o700)
    output.joinpath("initial.png").write_bytes(b"\x89PNG\r\n\x1a\ninitial image")
    output.joinpath("final.png").write_bytes(b"\x89PNG\r\n\x1a\nfinal image")
    output.joinpath("report.md").write_text("# Result\n\nVerdict: pass", encoding="utf-8")
    findings = normalize_findings(
        [
            {
                "scenario_id": "primary-flow",
                "severity": severity,
                "category": "functional",
                "title": title,
                "route": "/settings",
                "locator": "button.save",
                "expected": "Saved state is visible",
                "actual": "No confirmation is visible",
                "reproduction": ["Open settings", "Press Save"],
                "evidence": ["final.png"],
                "confidence": 0.9,
            }
        ]
    )
    now = datetime.utcnow().isoformat()
    return BrowserReviewJob(
        id=job_id,
        options=BrowserReviewOptions(
            url="http://127.0.0.1:5173",
            network_policy="managed_preview",
            goal="Verify settings",
            model="gpt-5.6-sol",
            reasoning_effort="high",
            output_dir=output,
        ),
        capture_only=False,
        provider="codex",
        codex_service_tier="default",
        status="completed",
        stage="completed",
        verdict="passed",
        findings=findings,
        coverage={"scenarios": ["primary-flow"]},
        latest_screenshot="final.png",
        steps=3,
        actions=1,
        created_at=now,
        started_at=now,
        completed_at=now,
    )


def _child_stopper(db_factory):
    async def stop(task_id: int) -> None:
        async with db_factory() as db:
            task = await db.get(Task, task_id)
            if task is not None and task.status not in {
                "completed",
                "failed",
                "cancelled",
                "conflict",
            }:
                task.status = "cancelled"
                task.completed_at = datetime.utcnow()
                await db.commit()

    return stop


@pytest.mark.asyncio
async def test_isolated_browser_attach_opens_launch_gate_only_after_job_binding(
    monkeypatch,
    db_factory,
    tmp_path,
):
    task_id = await _task(db_factory)
    child_service = ChildService(
        db_factory=db_factory,
        task_stopper=_child_stopper(db_factory),
    )
    service = HarnessService(
        db_factory=db_factory,
        child_service=child_service,
        artifact_store=ArtifactStore(tmp_path / "archive", retention_days=1),
    )
    run = await service.start_task_run(
        task_id=task_id,
        spec=HarnessSpec(
            target_kind="fixed_url",
            target={"url": "https://example.com"},
            goal="Verify the public page",
        ),
    )
    observed_status_at_attach: list[str] = []

    class BrowserManager:
        def __init__(self):
            self.job: BrowserReviewJob | None = None

        async def prepare_agent(self, options, **kwargs):
            self.job = BrowserReviewJob(
                id="job-durable-attach",
                options=options,
                capture_only=False,
                provider=kwargs["provider"],
                codex_service_tier=kwargs["codex_service_tier"],
                harness_run_id=kwargs["harness_run_id"],
                created_at=datetime.utcnow().isoformat(),
            )
            return self.job

        async def attach_task(self, job_id, child_id, *, owner_task_id=None):
            assert self.job is not None and job_id == self.job.id
            async with db_factory() as db:
                child = await db.get(Task, child_id)
                observed_status_at_attach.append(child.status)
            self.job.task_id = child_id
            self.job.owner_task_id = owner_task_id
            self.job.stage = "waiting_for_agent"
            return self.job

        async def fail_start(self, _job_id, exc):
            assert self.job is not None
            self.job.status = "failed"
            self.job.error = str(exc)

        async def mark_cancelling(self, _job_id):
            return self.job

        async def cancel(self, _job_id):
            assert self.job is not None
            self.job.status = "cancelled"
            return self.job

    manager = BrowserManager()
    from backend.services import browser_review_jobs

    monkeypatch.setattr(browser_review_jobs, "browser_review_job_manager", manager)

    job = await service._start_browser_for_url(
        run=run,
        url="https://example.com",
        network_policy="external_public",
        inline=False,
        watch_terminal=False,
        fail_run_on_error=True,
    )

    assert observed_status_at_attach == ["pending_activation"]
    async with db_factory() as db:
        child = await db.get(Task, job.task_id)
        binding = await db.scalar(
            select(ChildBindingModel).where(
                ChildBindingModel.harness_run_id == run.id
            )
        )
        assert child.status == "pending"
        assert binding is not None
        assert binding.state == CHILD_READY
        assert binding.child_task_id == child.id

    await service.cancel(run.id)
    async with db_factory() as db:
        binding = await db.scalar(
            select(ChildBindingModel).where(
                ChildBindingModel.harness_run_id == run.id
            )
        )
        child = await db.get(Task, binding.child_task_id)
        assert binding.state == CHILD_STOPPED
        assert child.status == "cancelled"


@pytest.mark.asyncio
async def test_isolated_browser_attach_failure_rolls_back_reserved_child(
    monkeypatch,
    db_factory,
    tmp_path,
):
    task_id = await _task(db_factory)
    child_service = ChildService(
        db_factory=db_factory,
        task_stopper=_child_stopper(db_factory),
    )
    service = HarnessService(
        db_factory=db_factory,
        child_service=child_service,
        artifact_store=ArtifactStore(tmp_path / "archive", retention_days=1),
    )
    run = await service.start_task_run(
        task_id=task_id,
        spec=HarnessSpec(
            target_kind="fixed_url",
            target={"url": "https://example.com"},
            goal="Verify attach rollback",
        ),
    )

    class BrowserManager:
        def __init__(self):
            self.job: BrowserReviewJob | None = None

        async def prepare_agent(self, options, **kwargs):
            self.job = BrowserReviewJob(
                id="job-attach-failure",
                options=options,
                capture_only=False,
                provider=kwargs["provider"],
                codex_service_tier=kwargs["codex_service_tier"],
                harness_run_id=kwargs["harness_run_id"],
                created_at=datetime.utcnow().isoformat(),
            )
            return self.job

        async def attach_task(self, *_args, **_kwargs):
            raise RuntimeError("watcher attach failed")

        async def fail_start(self, _job_id, exc):
            assert self.job is not None
            self.job.status = "failed"
            self.job.error = str(exc)

        async def mark_cancelling(self, _job_id):
            return self.job

        async def cancel(self, _job_id):
            return self.job

    manager = BrowserManager()
    from backend.services import browser_review_jobs

    monkeypatch.setattr(browser_review_jobs, "browser_review_job_manager", manager)

    with pytest.raises(RuntimeError, match="watcher attach failed"):
        await service._start_browser_for_url(
            run=run,
            url="https://example.com",
            network_policy="external_public",
            inline=False,
            watch_terminal=False,
            fail_run_on_error=True,
        )

    async with db_factory() as db:
        binding = await db.scalar(
            select(ChildBindingModel).where(
                ChildBindingModel.harness_run_id == run.id
            )
        )
        assert binding is not None
        child = await db.get(Task, binding.child_task_id)
        assert binding.state == CHILD_STOPPED
        assert child.status == "cancelled"


@pytest.mark.asyncio
async def test_fixed_url_run_is_idempotent_and_persists_structured_evidence(
    db_factory,
    tmp_path,
):
    task_id = await _task(db_factory)
    artifact_store = ArtifactStore(tmp_path / "archive", retention_days=1)
    service = HarnessService(
        db_factory=db_factory,
        poll_interval=0.01,
        artifact_store=artifact_store,
    )
    spec = HarnessSpec(
        target_kind="fixed_url",
        target={"url": "http://127.0.0.1:5173"},
        goal="Verify settings",
        idempotency_key="settings-v1",
    )
    run = await service.start_task_run(task_id=task_id, spec=spec)
    same = await service.start_task_run(task_id=task_id, spec=spec)
    assert same.id == run.id

    with pytest.raises(HarnessIdempotencyError):
        await service.start_task_run(
            task_id=task_id,
            spec=HarnessSpec(
                target_kind="fixed_url",
                target={"url": "http://127.0.0.1:5173"},
                goal="Different immutable input",
                idempotency_key="settings-v1",
            ),
        )

    job = _completed_job(
        tmp_path,
        title="Save confirmation is missing",
        artifact_store=artifact_store,
    )
    await service.attach_browser_job(run_id=run.id, job=job, watch_terminal=False)
    payload = await service.get_run(run.id)

    assert payload is not None
    assert payload["status"] == "completed"
    assert payload["verdict"] == "passed"
    assert payload["evidence_archive_state"] == "complete"
    assert payload["evidence_archive_error"] is None
    assert payload["attempts"][-1]["archive_manifest"]["expected"] == [
        "final.png",
        "initial.png",
        "report.md",
    ]
    assert payload["attempts"][-1]["archive_manifest"]["archived"] == [
        "final.png",
        "initial.png",
        "report.md",
    ]
    assert payload["browser_review"]["coverage"] == {"scenarios": ["primary-flow"]}
    assert payload["findings"][0]["title"] == "Save confirmation is missing"
    assert {item["name"] for item in payload["evidence"]} >= {
        "initial.png",
        "final.png",
        "report.md",
    }
    sequences = [event["sequence"] for event in payload["events"]]
    assert sequences == list(range(1, len(sequences) + 1))
    assert await service.resolve_evidence(run.id, "final.png") is not None

    async with db_factory() as db:
        evidence = await db.scalar(
            select(EvidenceModel).where(
                EvidenceModel.run_id == run.id,
                EvidenceModel.name == "final.png",
            )
        )
        assert evidence is not None
        assert not evidence.storage_path.startswith("/")
        attempt = await db.scalar(
            select(AttemptModel).where(AttemptModel.run_id == run.id)
        )
        assert attempt is not None
        assert attempt.archive_state == "complete"
        assert attempt.artifact_staging_root is None
        assert attempt.artifact_archive_prefix == artifact_store.run_prefix(
            task_id=task_id,
            run_id=run.id,
            attempt_id=attempt.id,
        )
        assert attempt.archived_at is not None

    assert job.options.output_dir is not None
    for source_file in job.options.output_dir.iterdir():
        source_file.unlink()
    job.options.output_dir.rmdir()
    restarted = HarnessService(
        db_factory=db_factory,
        artifact_store=artifact_store,
    )
    opened = await restarted.open_evidence(run.id, "final.png")
    assert opened is not None
    try:
        assert b"".join(opened.chunks()).startswith(b"\x89PNG")
    finally:
        opened.close()

    job.findings = []
    await service.sync_browser_job(job)
    cleared = await service.get_run(run.id)
    assert cleared is not None
    assert cleared["findings"] == []

    async with db_factory() as db:
        evidence_rows = list(
            (
                await db.execute(
                    select(EvidenceModel).where(
                        EvidenceModel.run_id == run.id
                    )
                )
            ).scalars()
        )
        for evidence in evidence_rows:
            evidence.created_at = datetime.utcnow() - timedelta(days=2)
        await db.commit()
    assert await restarted.cleanup_evidence() >= 3
    assert await restarted.open_evidence(run.id, "final.png") is None


@pytest.mark.asyncio
async def test_terminal_archive_missing_staging_fails_closed_and_keeps_pointer(
    db_factory,
    tmp_path,
):
    task_id = await _task(db_factory)
    store = ArtifactStore(tmp_path / "archive")
    service = HarnessService(db_factory=db_factory, artifact_store=store)
    run = await service.start_task_run(
        task_id=task_id,
        spec=HarnessSpec(
            target_kind="fixed_url",
            target={"url": "https://example.com"},
            goal="Require durable evidence",
        ),
    )
    job = _completed_job(
        tmp_path,
        title="Missing staging",
        artifact_store=store,
    )
    assert job.options.output_dir is not None
    staging_root = str(job.options.output_dir)
    for candidate in job.options.output_dir.iterdir():
        candidate.unlink()
    job.options.output_dir.rmdir()

    await service.attach_browser_job(
        run_id=run.id,
        job=job,
        watch_terminal=False,
    )

    payload = await service.get_run(run.id)
    assert payload is not None
    assert payload["status"] == "failed"
    assert payload["stage"] == "evidence_incomplete"
    assert payload["verdict"] == "error"
    assert payload["evidence_archive_state"] == "retryable_error"
    assert "staging directory is missing or unmanaged" in (
        payload["evidence_archive_error"] or ""
    )
    assert payload["evidence"] == []
    async with db_factory() as db:
        attempt = await db.scalar(
            select(AttemptModel).where(AttemptModel.run_id == run.id)
        )
        assert attempt is not None
        assert attempt.artifact_staging_root == staging_root
        assert attempt.artifact_archive_prefix is None
        assert attempt.archived_at is None


@pytest.mark.asyncio
async def test_partial_terminal_archive_retries_from_retained_staging(
    db_factory,
    tmp_path,
):
    task_id = await _task(db_factory)
    store = ArtifactStore(tmp_path / "archive")
    service = HarnessService(db_factory=db_factory, artifact_store=store)
    run = await service.start_task_run(
        task_id=task_id,
        spec=HarnessSpec(
            target_kind="fixed_url",
            target={"url": "https://example.com"},
            goal="Retry a partial evidence archive",
        ),
    )
    job_id = uuid.uuid4().hex
    output = store.create_job_dir(job_id)
    output.joinpath("report.md").write_text("# Passed", encoding="utf-8")
    now = datetime.utcnow().isoformat()
    job = BrowserReviewJob(
        id=job_id,
        options=BrowserReviewOptions(
            url="https://example.com",
            goal="Retry evidence",
            output_dir=output,
        ),
        capture_only=False,
        provider="codex",
        codex_service_tier="default",
        status="completed",
        stage="completed",
        verdict="passed",
        latest_screenshot="final.png",
        created_at=now,
        started_at=now,
        completed_at=now,
    )

    await service.attach_browser_job(
        run_id=run.id,
        job=job,
        watch_terminal=False,
    )
    partial = await service.get_run(run.id)
    assert partial is not None
    assert partial["status"] == "failed"
    assert partial["stage"] == "evidence_incomplete"
    assert partial["evidence_archive_state"] == "retryable_error"
    assert "final.png" in (partial["evidence_archive_error"] or "")
    assert [item["name"] for item in partial["evidence"]] == ["report.md"]
    assert output.exists()

    output.joinpath("final.png").write_bytes(b"\x89PNG\r\n\x1a\nrecovered")
    await service.sync_browser_job(job)
    recovered = await service.get_run(run.id)
    assert recovered is not None
    assert recovered["status"] == "completed"
    assert recovered["verdict"] == "passed"
    assert recovered["evidence_archive_state"] == "complete"
    assert {item["name"] for item in recovered["evidence"]} == {
        "final.png",
        "report.md",
    }


@pytest.mark.asyncio
async def test_hash_verification_failure_rearchives_from_retained_staging(
    db_factory,
    tmp_path,
):
    task_id = await _task(db_factory)
    store = ArtifactStore(tmp_path / "archive")
    service = HarnessService(db_factory=db_factory, artifact_store=store)
    run = await service.start_task_run(
        task_id=task_id,
        spec=HarnessSpec(
            target_kind="fixed_url",
            target={"url": "https://example.com"},
            goal="Repair corrupted persistent evidence",
        ),
    )
    job = _completed_job(
        tmp_path,
        title="Integrity evidence",
        artifact_store=store,
    )
    await service.attach_browser_job(
        run_id=run.id,
        job=job,
        watch_terminal=False,
    )
    async with db_factory() as db:
        evidence = await db.scalar(
            select(EvidenceModel).where(
                EvidenceModel.run_id == run.id,
                EvidenceModel.name == "final.png",
            )
        )
        assert evidence is not None
        archive_path = store.resolve_path(evidence.storage_path)
        original = archive_path.read_bytes()
    archive_path.write_bytes(original[:-1] + bytes([original[-1] ^ 0x01]))

    await service.sync_browser_job(job)
    rejected = await service.get_run(run.id)
    assert rejected is not None
    assert rejected["status"] == "failed"
    assert rejected["evidence_archive_state"] == "retryable_error"
    assert "integrity check failed" in (
        rejected["evidence_archive_error"] or ""
    )

    await service.sync_browser_job(job)
    repaired = await service.get_run(run.id)
    assert repaired is not None
    assert repaired["status"] == "completed"
    assert repaired["evidence_archive_state"] == "complete"
    opened = await service.open_evidence(run.id, "final.png")
    assert opened is not None
    try:
        assert b"".join(opened.chunks()) == original
    finally:
        opened.close()


@pytest.mark.asyncio
async def test_repeat_and_compare_use_stable_finding_fingerprints(db_factory, tmp_path):
    task_id = await _task(db_factory)
    artifact_store = ArtifactStore(tmp_path / "archive")
    service = HarnessService(
        db_factory=db_factory,
        poll_interval=0.01,
        artifact_store=artifact_store,
    )
    first = await service.start_task_run(
        task_id=task_id,
        spec=HarnessSpec(
            target_kind="fixed_url",
            target={"url": "http://127.0.0.1:5173"},
            goal="Verify settings",
        ),
    )
    await service.attach_browser_job(
        run_id=first.id,
        job=_completed_job(
            tmp_path,
            title="Save confirmation is missing",
            severity="high",
            artifact_store=artifact_store,
        ),
        watch_terminal=False,
    )

    repeated = await service.repeat(first.id)
    assert repeated.parent_run_id == first.id
    assert repeated.root_run_id == first.id
    assert repeated.attempt_number == 2
    await service.attach_browser_job(
        run_id=repeated.id,
        job=_completed_job(
            tmp_path,
            title="Save confirmation is missing",
            severity="medium",
            artifact_store=artifact_store,
        ),
        watch_terminal=False,
    )

    comparison = await service.compare(first.id, repeated.id)
    assert comparison["new"] == []
    assert len(comparison["persisting"]) == 1
    assert comparison["resolved"] == []


@pytest.mark.asyncio
async def test_sync_terminal_workspace_run_records_cleanup_event(db_factory):
    task_id = await _task(db_factory)
    service = HarnessService(db_factory=db_factory, poll_interval=0.01)
    run = await service.start_task_run(
        task_id=task_id,
        spec=HarnessSpec(
            target_kind="fixed_url",
            target={"url": "http://127.0.0.1:5173"},
            goal="Verify the finished workspace result",
        ),
    )
    now = datetime.utcnow()
    workspace_run = SimpleNamespace(
        id=uuid.uuid4().hex,
        status="completed",
        stage="completed",
        cleanup_status="completed",
        cleanup_error=None,
        browser_review_job_id="workspace-browser-job",
        agent_task_id=321,
        git_head="a" * 40,
        workspace_fingerprint="b" * 64,
        stale=False,
        report="# Result\n\nVerdict: pass",
        error=None,
        started_at=now,
        completed_at=now,
    )

    async with db_factory() as db:
        db.add(
            AttemptModel(
                id=uuid.uuid4().hex,
                run_id=run.id,
                ordinal=1,
                status="completed",
                stage="completed",
                provider="codex",
                model="gpt-5.6-sol",
                reasoning_effort="high",
                codex_service_tier="default",
                browser_review_job_id="workspace-browser-job",
                archive_state="complete",
                archive_manifest={
                    "version": 1,
                    "expected": [],
                    "archived": {},
                    "terminal_status": "completed",
                },
                result_data={"verdict": "passed"},
            )
        )
        await db.commit()

    await service._sync_workspace_run(run.id, workspace_run)

    payload = await service.get_run(run.id)
    assert payload is not None
    assert payload["status"] == "completed"
    assert payload["cleanup_status"] == "completed"
    assert payload["events"][-1]["event_type"] == "cleanup"
    assert payload["events"][-1]["title"] == "隔离预览已清理"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("target_kind", "target"),
    [
        ("pull_request", {"pr_number": 99, "remote": "origin"}),
        ("git_ref", {"ref": "feature", "remote": "origin", "fetch": True}),
    ],
)
async def test_untrusted_git_run_is_rejected_before_persistence(
    db_factory,
    target_kind,
    target,
):
    task_id = await _task(db_factory)
    service = HarnessService(db_factory=db_factory)

    with pytest.raises(HarnessError, match="isolated sandbox"):
        await service.start_task_run(
            task_id=task_id,
            spec=HarnessSpec(
                target_kind=target_kind,
                target=target,
                goal="Do not execute this branch",
            ),
        )

    async with db_factory() as db:
        assert await db.scalar(select(RunModel.id)) is None


@pytest.mark.asyncio
async def test_git_target_pipeline_runs_browser_then_proves_sandbox_cleanup(
    monkeypatch,
    db_factory,
    tmp_path,
):
    async with db_factory() as db:
        project = Project(
            name="git-harness-project",
            git_url="https://github.com/acme/ui.git",
            status="ready",
            preview_config={
                "sandbox": {"setup": [], "processes": [], "allowed_hosts": []}
            },
        )
        db.add(project)
        await db.flush()
        task = Task(
            title="Git Harness owner",
            status="completed",
            project_id=project.id,
            provider="codex",
            model="gpt-5.6-sol",
            effort_level="high",
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    resolved = ResolvedGitTarget(
        kind="pull_request",
        repository="acme/ui",
        clone_url="https://github.com/acme/ui.git",
        base_sha="a" * 40,
        head_sha="b" * 40,
        fetch_ref="refs/pull/7/head",
        source_repository="fork/ui",
        source_ref="feature",
        pr_number=7,
        changed_files=(),
        fingerprint="c" * 64,
    )
    prepared = PreparedGitTarget(
        resolved=resolved,
        source=SandboxSourceSnapshot(
            repository_path="/workspace/repo",
            head_sha=resolved.head_sha,
            internal_network_id="d" * 64,
            egress_network_id="e" * 64,
            proxy_container_id="f" * 64,
            allowed_hosts=("github.com",),
        ),
        preview=SandboxPreviewSnapshot(
            url="http://127.0.0.1:43123/",
            health_url="http://127.0.0.1:43123/health",
            host_port=43123,
            internal_port=4173,
            process_names=("web",),
            setup_logs=(),
        ),
    )

    class _TargetManager:
        async def prepare(self, **_kwargs):
            return prepared

    class _SandboxManager:
        def __init__(self):
            self.cleaned: list[str] = []

        async def cleanup(self, run_id):
            self.cleaned.append(run_id)

        async def recover_interrupted(self):
            return 0

    sandbox_manager = _SandboxManager()
    artifact_store = ArtifactStore(tmp_path / "git-archive")
    service = HarnessService(
        db_factory=db_factory,
        poll_interval=0.001,
        target_manager=_TargetManager(),
        sandbox_manager=sandbox_manager,
        artifact_store=artifact_store,
    )
    job = _completed_job(
        tmp_path,
        title="No issue",
        severity="info",
        artifact_store=artifact_store,
    )

    async def _start_browser(*, run_id, url):
        assert url == "http://127.0.0.1:43123/"
        assert artifact_store.list_job_artifacts(job.options.output_dir) == [
            "final.png",
            "initial.png",
            "report.md",
        ]
        job.harness_run_id = run_id
        await service.attach_browser_job(
            run_id=run_id,
            job=job,
            watch_terminal=False,
        )
        attached = await service.get_run(run_id)
        assert attached is not None
        assert attached["evidence_archive_state"] == "complete", (
            attached["evidence_archive_error"],
            [(item["name"], item["sha256"]) for item in attached["evidence"]],
        )
        return job

    service.start_managed_preview_browser = _start_browser

    class _BrowserManager:
        async def get(self, job_id):
            assert job_id == job.id
            return job

        async def cancel(self, _job_id):
            return job

    from backend.services import browser_review_jobs

    monkeypatch.setattr(
        browser_review_jobs,
        "browser_review_job_manager",
        _BrowserManager(),
    )

    async def _available(*_args, **_kwargs):
        return SimpleNamespace(available=True, reason=None)

    monkeypatch.setattr(
        harness_module,
        "untrusted_git_target_capability",
        _available,
    )

    run = await service.start_task_run(
        task_id=task_id,
        spec=HarnessSpec(
            target_kind="pull_request",
            target={"pr_number": 7, "remote": "origin"},
            goal="Verify PR 7",
        ),
    )
    pipeline = service._pipelines.get(run.id)
    assert pipeline is not None
    await asyncio.wait_for(asyncio.shield(pipeline), timeout=1)
    payload = await service.get_run(run.id)

    assert payload is not None
    assert payload["status"] == "completed", (
        payload["error"],
        payload["attempts"],
        payload["events"],
    )
    assert payload["source_git_head"] == "b" * 40
    assert payload["verdict"] == "passed"
    assert payload["cleanup_status"] == "completed"
    assert sandbox_manager.cleaned == [run.id]


@pytest.mark.asyncio
async def test_restart_reconciles_managed_job_files_before_marking_run_interrupted(
    db_factory,
    tmp_path,
):
    task_id = await _task(db_factory)
    store = ArtifactStore(tmp_path / "archive")
    service = HarnessService(
        db_factory=db_factory,
        artifact_store=store,
        retention_interval=0,
    )
    run = await service.start_task_run(
        task_id=task_id,
        spec=HarnessSpec(
            target_kind="fixed_url",
            target={"url": "https://example.com"},
            goal="Recover the last screenshot",
        ),
    )
    job_id = "c" * 32
    attempt_id = "d" * 32
    job_dir = store.create_job_dir(job_id)
    job_dir.joinpath("final.png").write_bytes(b"\x89PNG\r\n\x1a\nrecovered")
    async with db_factory() as db:
        db.add(
            AttemptModel(
                id=attempt_id,
                run_id=run.id,
                ordinal=1,
                status="running",
                stage="browser_ready",
                provider="codex",
                model="gpt-5.6-sol",
                reasoning_effort="medium",
                codex_service_tier="default",
                browser_review_job_id=job_id,
                artifact_root=str(job_dir),
                result_data={},
            )
        )
        await db.commit()

    assert await service.recover_interrupted_runs() == 1
    recovered = await service.get_run(run.id)
    assert recovered is not None
    assert recovered["status"] == "failed"
    assert {item["name"] for item in recovered["evidence"]} == {"final.png"}
    opened = await service.open_evidence(run.id, "final.png")
    assert opened is not None
    try:
        assert b"".join(opened.chunks()).endswith(b"recovered")
    finally:
        opened.close()
    async with db_factory() as db:
        attempt = await db.get(AttemptModel, attempt_id)
        assert attempt is not None
        assert attempt.archive_state == "complete"
        assert attempt.archive_error is None
        assert attempt.artifact_staging_root is None
        assert attempt.artifact_archive_prefix == store.run_prefix(
            task_id=task_id,
            run_id=run.id,
            attempt_id=attempt_id,
        )
        assert attempt.archived_at is not None
        assert attempt.artifact_root == store.run_prefix(
            task_id=task_id,
            run_id=run.id,
            attempt_id=attempt_id,
        )


@pytest.mark.asyncio
async def test_restart_marks_completed_run_failed_when_staging_cannot_be_recovered(
    db_factory,
    tmp_path,
):
    task_id = await _task(db_factory)
    store = ArtifactStore(tmp_path / "archive")
    service = HarnessService(
        db_factory=db_factory,
        artifact_store=store,
        retention_interval=0,
    )
    run = await service.start_task_run(
        task_id=task_id,
        spec=HarnessSpec(
            target_kind="fixed_url",
            target={"url": "https://example.com"},
            goal="Reject an unrecoverable archive",
        ),
    )
    attempt_id = uuid.uuid4().hex
    job_id = uuid.uuid4().hex
    missing_staging = store.jobs_root / job_id
    async with db_factory() as db:
        persisted_run = await db.get(RunModel, run.id)
        assert persisted_run is not None
        persisted_run.status = "completed"
        persisted_run.stage = "completed"
        persisted_run.verdict = "passed"
        persisted_run.report = "# Passed"
        persisted_run.cleanup_status = "completed"
        persisted_run.completed_at = datetime.utcnow()
        db.add(
            AttemptModel(
                id=attempt_id,
                run_id=run.id,
                ordinal=1,
                status="completed",
                stage="completed",
                provider="codex",
                model="gpt-5.6-sol",
                reasoning_effort="medium",
                codex_service_tier="default",
                browser_review_job_id=job_id,
                artifact_root=str(missing_staging),
                artifact_staging_root=str(missing_staging),
                archive_state="retryable_error",
                archive_manifest={
                    "version": 1,
                    "expected": ["final.png", "report.md"],
                    "archived": {},
                    "terminal_status": "completed",
                },
                result_data={
                    "artifacts": ["final.png", "report.md"],
                    "latest_screenshot": "final.png",
                    "report": "# Passed",
                },
            )
        )
        await db.commit()

    assert await service.recover_interrupted_runs() == 0
    recovered = await service.get_run(run.id)
    assert recovered is not None
    assert recovered["status"] == "failed"
    assert recovered["stage"] == "evidence_incomplete"
    assert recovered["verdict"] == "error"
    assert recovered["evidence_archive_state"] == "incomplete"
    assert "staging directory is unavailable" in (
        recovered["evidence_archive_error"] or ""
    )
    assert recovered["events"][-1]["title"] == "测试证据恢复失败"
    assert await service.recover_interrupted_runs() == 0
    recovered_again = await service.get_run(run.id)
    assert recovered_again is not None
    assert sum(
        event["title"] == "测试证据恢复失败"
        for event in recovered_again["events"]
    ) == 1
    async with db_factory() as db:
        attempt = await db.get(AttemptModel, attempt_id)
        assert attempt is not None
        assert attempt.artifact_staging_root == str(missing_staging)
        assert attempt.artifact_archive_prefix is None


@pytest.mark.asyncio
async def test_retention_keeps_incomplete_staging_until_archive_is_complete(
    db_factory,
    tmp_path,
):
    task_id = await _task(db_factory)
    store = ArtifactStore(tmp_path / "archive", retention_days=1)
    service = HarnessService(db_factory=db_factory, artifact_store=store)
    run = await service.start_task_run(
        task_id=task_id,
        spec=HarnessSpec(
            target_kind="fixed_url",
            target={"url": "https://example.com"},
            goal="Protect retry staging",
        ),
    )
    job_id = uuid.uuid4().hex
    attempt_id = uuid.uuid4().hex
    job_dir = store.create_job_dir(job_id)
    job_dir.joinpath("report.md").write_text("retry me", encoding="utf-8")
    old = (datetime.utcnow() - timedelta(days=2)).timestamp()
    os.utime(job_dir, (old, old))
    async with db_factory() as db:
        persisted_run = await db.get(RunModel, run.id)
        assert persisted_run is not None
        persisted_run.status = "failed"
        persisted_run.stage = "evidence_incomplete"
        persisted_run.verdict = "error"
        persisted_run.completed_at = datetime.utcnow()
        db.add(
            AttemptModel(
                id=attempt_id,
                run_id=run.id,
                ordinal=1,
                status="completed",
                stage="completed",
                provider="codex",
                model="gpt-5.6-sol",
                reasoning_effort="medium",
                codex_service_tier="default",
                browser_review_job_id=job_id,
                artifact_root=str(job_dir),
                artifact_staging_root=str(job_dir),
                archive_state="retryable_error",
                archive_manifest={"version": 1, "expected": ["report.md"]},
                result_data={"artifacts": ["report.md"]},
            )
        )
        await db.commit()

    await service.cleanup_evidence()
    assert job_dir.exists()

    async with db_factory() as db:
        attempt = await db.get(AttemptModel, attempt_id)
        assert attempt is not None
        attempt.archive_state = "complete"
        await db.commit()
    await service.cleanup_evidence()
    assert not job_dir.exists()
