from __future__ import annotations

import asyncio
import copy
import subprocess
from pathlib import Path

import pytest
from sqlalchemy import func, select, update

from backend.config import settings
from backend.models.project import Project
from backend.models.task import Task
from backend.models.test_harness import TestHarnessRun as HarnessRunModel
from backend.models.workspace_review import WorkspaceReviewRun
from backend.services import test_harness as test_harness_module
from backend.services import workspace_review as workspace_review_module
from backend.services.test_harness import (
    TestHarnessError as HarnessError,
    TestHarnessIdempotencyError as HarnessIdempotencyError,
    TestHarnessService as HarnessService,
)
from backend.services.test_harness_contracts import TestHarnessSpec as HarnessSpec
from backend.services.test_harness_execution_context import (
    HARNESS_EXECUTION_CONTEXT_KEY,
)
from backend.services.workspace_review import (
    WorkspaceReviewError,
    WorkspaceReviewManager,
    validate_preview_config,
)


def _git(workspace: Path, *args: str) -> None:
    subprocess.run(
        ["git", *args],
        cwd=workspace,
        check=True,
        capture_output=True,
        text=True,
    )


def _workspace(tmp_path: Path, name: str) -> Path:
    workspace = tmp_path / name
    workspace.mkdir()
    _git(workspace, "init")
    _git(workspace, "config", "user.email", "harness@example.invalid")
    _git(workspace, "config", "user.name", "Harness Test")
    (workspace / "index.html").write_text(
        f"<h1>{name}</h1>\n",
        encoding="utf-8",
    )
    _git(workspace, "add", "index.html")
    _git(workspace, "commit", "-m", "initial")
    return workspace.resolve()


def _preview_config(marker: str) -> dict:
    return {
        "version": 1,
        "name": f"Frozen preview {marker}",
        "setup": [],
        "processes": [
            {
                "name": "web",
                "command": [
                    "{python}",
                    "-m",
                    "http.server",
                    "{preview_port}",
                    "--bind",
                    "127.0.0.1",
                ],
                "cwd": ".",
                "env": {"HARNESS_ROUTE_MARKER": marker},
            }
        ],
        "url": "http://127.0.0.1:{preview_port}/",
        "health_url": "http://127.0.0.1:{preview_port}/",
        "startup_timeout_seconds": 10,
    }


def _git_preview_config(marker: str) -> dict:
    config = _preview_config(marker)
    config["sandbox"] = {
        "setup": [],
        "processes": [
            {
                "name": "web",
                "command": [
                    "python",
                    "-m",
                    "http.server",
                    "{preview_port}",
                    "--bind",
                    "0.0.0.0",
                ],
                "cwd": ".",
                "env": {"HARNESS_ROUTE_MARKER": marker},
            }
        ],
        "allowed_hosts": ["registry.npmjs.org"],
    }
    return config


async def _project_task(
    db_factory,
    *,
    project_name: str,
    workspace: Path | None = None,
    git_url: str | None = None,
    preview_config: dict | None = None,
) -> tuple[int, int]:
    async with db_factory() as db:
        project = Project(
            name=project_name,
            status="ready",
            local_path=str(workspace) if workspace is not None else None,
            git_url=git_url,
            preview_config=copy.deepcopy(preview_config),
        )
        db.add(project)
        await db.flush()
        task = Task(
            title=f"{project_name} owner",
            status="completed",
            provider="codex",
            model="gpt-5.6-sol",
            project_id=project.id,
            target_repo=str(workspace) if workspace is not None else "",
            last_cwd=str(workspace) if workspace is not None else None,
        )
        db.add(task)
        await db.commit()
        return task.id, project.id


@pytest.mark.asyncio
async def test_idempotency_key_conflicts_after_owner_project_changes(
    db_factory,
    monkeypatch,
):
    monkeypatch.setattr(settings, "auth_token", "test-harness-token")
    task_id, first_project_id = await _project_task(
        db_factory,
        project_name="Idempotency project A",
    )
    async with db_factory() as db:
        second = Project(name="Idempotency project B", status="ready")
        db.add(second)
        await db.commit()
        second_project_id = second.id

    service = HarnessService(db_factory=db_factory)
    spec = HarnessSpec(
        target_kind="fixed_url",
        target={"url": "https://example.com"},
        goal="Bind this idempotency key to the frozen owner project",
        idempotency_key="frozen-project-request",
    )
    first = await service.start_task_run(task_id=task_id, spec=spec)
    assert first.project_id == first_project_id
    assert HARNESS_EXECUTION_CONTEXT_KEY in first.runtime_config
    public_payload = await service.get_run(first.id)
    assert public_payload is not None
    assert HARNESS_EXECUTION_CONTEXT_KEY not in public_payload["runtime"]

    async with db_factory() as db:
        persisted = await db.get(HarnessRunModel, first.id)
        assert persisted is not None
        assert HARNESS_EXECUTION_CONTEXT_KEY in persisted.runtime_config
        persisted.status = "completed"
        persisted.stage = "completed"
        persisted.cleanup_status = "completed"
        owner = await db.get(Task, task_id)
        assert owner is not None
        owner.project_id = second_project_id
        await db.commit()

    with pytest.raises(
        HarnessIdempotencyError,
        match="idempotency key.*different test input",
    ):
        await service.start_task_run(task_id=task_id, spec=spec)

    async with db_factory() as db:
        assert await db.scalar(
            select(func.count())
            .select_from(HarnessRunModel)
            .where(HarnessRunModel.task_id == task_id)
        ) == 1


async def _pause_after_run_commit(service: HarnessService, monkeypatch):
    entered = asyncio.Event()
    proceed = asyncio.Event()
    original = service._start_workspace_review

    async def delayed_start_workspace_review(**kwargs):
        entered.set()
        await proceed.wait()
        return await original(**kwargs)

    monkeypatch.setattr(
        service,
        "_start_workspace_review",
        delayed_start_workspace_review,
    )
    return entered, proceed


async def _disable_workspace_background_pipeline(
    db_factory,
    service: HarnessService,
    monkeypatch,
) -> WorkspaceReviewManager:
    manager = WorkspaceReviewManager()

    async def no_pipeline(*_args, **_kwargs):
        return None

    async def no_watcher(**_kwargs):
        return None

    monkeypatch.setattr(workspace_review_module, "async_session", db_factory)
    monkeypatch.setattr(workspace_review_module, "workspace_review_manager", manager)
    monkeypatch.setattr(manager, "_run_pipeline", no_pipeline)
    monkeypatch.setattr(service, "_watch_workspace_run", no_watcher)
    return manager


@pytest.mark.asyncio
async def test_workspace_materialization_never_switches_post_commit_route(
    db_factory,
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(settings, "auth_token", "test-harness-token")
    workspace_a = _workspace(tmp_path, "workspace-a")
    workspace_b = _workspace(tmp_path, "workspace-b")
    config_a = _preview_config("A-original")
    expected_config_a = validate_preview_config(config_a, workspace_a)
    task_id, project_a_id = await _project_task(
        db_factory,
        project_name="Workspace project A",
        workspace=workspace_a,
        preview_config=config_a,
    )
    async with db_factory() as db:
        project_b = Project(
            name="Workspace project B",
            status="ready",
            local_path=str(workspace_b),
            preview_config=_preview_config("B-replacement"),
        )
        db.add(project_b)
        await db.commit()
        project_b_id = project_b.id

    service = HarnessService(db_factory=db_factory, poll_interval=0.01)
    await _disable_workspace_background_pipeline(db_factory, service, monkeypatch)
    entered, proceed = await _pause_after_run_commit(service, monkeypatch)
    start = asyncio.create_task(
        service.start_task_run(
            task_id=task_id,
            spec=HarnessSpec(
                target_kind="current_workspace",
                target={},
                goal="Keep the committed Workspace route immutable",
            ),
        )
    )
    await asyncio.wait_for(entered.wait(), timeout=2)

    async with db_factory() as db:
        owner = await db.get(Task, task_id)
        project_a = await db.get(Project, project_a_id)
        assert owner is not None and project_a is not None
        owner.project_id = project_b_id
        owner.target_repo = str(workspace_b)
        owner.last_cwd = str(workspace_b)
        project_a.preview_config = _preview_config("A-mutated-after-commit")
        await db.commit()

    proceed.set()
    rejected = False
    try:
        run = await start
    except (HarnessError, WorkspaceReviewError, ValueError):
        rejected = True
        async with db_factory() as db:
            run = await db.scalar(
                select(HarnessRunModel)
                .where(HarnessRunModel.task_id == task_id)
                .order_by(HarnessRunModel.created_at.desc())
            )
            assert run is not None

    async with db_factory() as db:
        workspace_runs = list(
            (
                await db.execute(
                    select(WorkspaceReviewRun).where(
                        WorkspaceReviewRun.harness_run_id == run.id
                    )
                )
            ).scalars()
        )
        if rejected:
            assert workspace_runs == []
            assert run.status == "failed"
        else:
            assert len(workspace_runs) == 1
            workspace_run = workspace_runs[0]
            assert workspace_run.project_id == project_a_id
            assert workspace_run.workspace_path == str(workspace_a)
            assert workspace_run.preview_config == expected_config_a
        persisted = await db.get(HarnessRunModel, run.id)
        assert persisted is not None
        context = persisted.runtime_config[HARNESS_EXECUTION_CONTEXT_KEY]
        assert context["project_id"] == project_a_id
        assert context["workspace_path"] == str(workspace_a)
        assert context["preview_config"] == expected_config_a

    await service.shutdown()


@pytest.mark.asyncio
async def test_workspace_materialization_uses_frozen_preview_config(
    db_factory,
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(settings, "auth_token", "test-harness-token")
    workspace = _workspace(tmp_path, "workspace-preview-config")
    original_config = _preview_config("original")
    expected_original = validate_preview_config(original_config, workspace)
    task_id, project_id = await _project_task(
        db_factory,
        project_name="Workspace preview config project",
        workspace=workspace,
        preview_config=original_config,
    )

    service = HarnessService(db_factory=db_factory, poll_interval=0.01)
    await _disable_workspace_background_pipeline(db_factory, service, monkeypatch)
    entered, proceed = await _pause_after_run_commit(service, monkeypatch)
    start = asyncio.create_task(
        service.start_task_run(
            task_id=task_id,
            spec=HarnessSpec(
                target_kind="current_workspace",
                target={},
                goal="Use the Preview configuration frozen at admission",
            ),
        )
    )
    await asyncio.wait_for(entered.wait(), timeout=2)

    async with db_factory() as db:
        project = await db.get(Project, project_id)
        assert project is not None
        project.preview_config = _preview_config("mutated-after-commit")
        await db.commit()

    proceed.set()
    run = await start
    async with db_factory() as db:
        workspace_run = await db.scalar(
            select(WorkspaceReviewRun).where(
                WorkspaceReviewRun.harness_run_id == run.id
            )
        )
        assert workspace_run is not None
        assert workspace_run.preview_config == expected_original
        assert workspace_run.project_id == project_id
        assert workspace_run.workspace_path == str(workspace)

    await service.shutdown()


class _CapturingTargetManager:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def prepare(self, *, task, project, **kwargs):
        self.calls.append(
            {
                "task_project_id": task.project_id,
                "project_id": project.id if project is not None else None,
                "git_url": project.git_url if project is not None else None,
                "preview_config": (
                    copy.deepcopy(project.preview_config)
                    if project is not None
                    else None
                ),
                "kind": kwargs["kind"],
                "target": copy.deepcopy(kwargs["target"]),
            }
        )
        raise RuntimeError("stop after frozen Git context capture")


class _NoopSandboxManager:
    async def cleanup(self, _run_id: str) -> None:
        return None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "switch_task_project",
    [False, True],
    ids=["project-row-mutated", "task-project-switched"],
)
async def test_git_pipeline_never_switches_post_commit_repository_or_config(
    db_factory,
    monkeypatch,
    switch_task_project,
):
    monkeypatch.setattr(settings, "auth_token", "test-harness-token")

    async def available_capability(*_args, **_kwargs):
        return type("Capability", (), {"available": True, "reason": None})()

    monkeypatch.setattr(
        test_harness_module,
        "untrusted_git_target_capability",
        available_capability,
    )
    original_config_a = _git_preview_config("A-original")
    task_id, project_a_id = await _project_task(
        db_factory,
        project_name="Git project A",
        git_url="https://github.com/acme/project-a.git",
        preview_config=original_config_a,
    )
    project_b_id = None
    if switch_task_project:
        async with db_factory() as db:
            project_b = Project(
                name="Git project B",
                status="ready",
                git_url="https://github.com/acme/project-b.git",
                preview_config=_git_preview_config("B-replacement"),
            )
            db.add(project_b)
            await db.commit()
            project_b_id = project_b.id

    target_manager = _CapturingTargetManager()
    service = HarnessService(
        db_factory=db_factory,
        poll_interval=0.01,
        target_manager=target_manager,
        sandbox_manager=_NoopSandboxManager(),
    )
    entered = asyncio.Event()
    proceed = asyncio.Event()
    original_pipeline = service._run_git_target_pipeline

    async def delayed_pipeline(run_id: str):
        entered.set()
        await proceed.wait()
        await original_pipeline(run_id)

    monkeypatch.setattr(service, "_run_git_target_pipeline", delayed_pipeline)
    run = await service.start_task_run(
        task_id=task_id,
        spec=HarnessSpec(
            target_kind="git_ref",
            target={"ref": "feature", "remote": "origin", "fetch": True},
            goal="Keep the committed Git repository and sandbox config immutable",
        ),
    )
    await asyncio.wait_for(entered.wait(), timeout=2)
    pipeline = service._pipelines[run.id]

    async with db_factory() as db:
        owner = await db.get(Task, task_id)
        project_a = await db.get(Project, project_a_id)
        assert owner is not None and project_a is not None
        if project_b_id is not None:
            owner.project_id = project_b_id
        project_a.git_url = "https://github.com/acme/project-a-mutated.git"
        project_a.preview_config = _git_preview_config("A-mutated-after-commit")
        await db.commit()

    proceed.set()
    await asyncio.wait_for(pipeline, timeout=2)

    if target_manager.calls:
        assert len(target_manager.calls) == 1
        call = target_manager.calls[0]
        assert call["project_id"] == project_a_id
        assert call["git_url"] == "https://github.com/acme/project-a.git"
        assert call["preview_config"] == original_config_a
        assert call["kind"] == "git_ref"
        assert call["target"] == {
            "remote": "origin",
            "ref": "feature",
            "fetch": True,
        }
    else:
        assert switch_task_project is True
        async with db_factory() as db:
            failed = await db.get(HarnessRunModel, run.id)
            assert failed is not None
            assert failed.status == "failed"
            assert failed.error

    async with db_factory() as db:
        persisted = await db.get(HarnessRunModel, run.id)
        assert persisted is not None
        context = persisted.runtime_config[HARNESS_EXECUTION_CONTEXT_KEY]
        assert context["project_id"] == project_a_id
        assert context["repository"] == "acme/project-a"
        assert context["git_url"] == "https://github.com/acme/project-a.git"
        assert context["preview_config"] == original_config_a

    await service.shutdown()
