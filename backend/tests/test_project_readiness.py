"""Tests for the Project readiness gate (clone-failure containment).

Covers the incident chain fix: the dispatch queue holds Tasks whose Project
is not ready; user-facing Task creation rejects clone-failed Projects; a
missing working directory raises the typed human-readable error instead of a
bare ``[Errno 2]``; clone failure/success annotates queued Tasks.
"""
import os

import pytest
import pytest_asyncio
from sqlalchemy import select

from backend.models.project import Project
from backend.models.task import Task
from backend.services.project_readiness import (
    ProjectNotDispatchableError,
    require_project_dispatchable,
)
from backend.services.task_queue import TaskQueue

# ``backend.api.projects`` (and every FastAPI app test) transitively imports
# ``deployment_start_guard`` whose module-level ``import fcntl`` is POSIX-only.
# Production and CI run on Linux; keep these cases runnable there.
requires_posix_backend = pytest.mark.skipif(
    os.name == "nt",
    reason="backend.api import chain requires POSIX fcntl",
)


@pytest_asyncio.fixture
async def queue(db_session):
    return TaskQueue(db_session)


async def _seed_project(
    db,
    *,
    status: str,
    name: str,
    error_message: str | None = None,
) -> Project:
    project = Project(
        name=name,
        git_url="https://github.com/example/readiness.git",
        has_remote=True,
        local_path=f"/tmp/{name}",
        default_branch="main",
        status=status,
        error_message=error_message,
    )
    db.add(project)
    await db.commit()
    await db.refresh(project)
    return project


# ── require_project_dispatchable ─────────────────────────────────────────────


def test_require_project_dispatchable_rejects_error_only():
    class _P:
        name = "p"
        error_message = "git clone failed"

    for status in ("pending", "cloning", "initializing", "ready"):
        _P.status = status
        require_project_dispatchable(_P)

    _P.status = "error"
    with pytest.raises(ProjectNotDispatchableError) as exc_info:
        require_project_dispatchable(_P)
    assert "git clone failed" in str(exc_info.value)
    require_project_dispatchable(None)


# ── Dispatch-queue readiness gate ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_dequeue_holds_task_while_project_is_cloning(queue):
    project = await _seed_project(
        queue.db, status="cloning", name="readiness-cloning"
    )
    project_id = project.id
    await queue.create(
        title="Waits for clone",
        description="d",
        project_id=project_id,
        target_repo=project.local_path,
    )

    assert await queue.dequeue() is None

    fresh_project = await queue.db.get(
        Project, project_id, populate_existing=True
    )
    fresh_project.status = "ready"
    await queue.db.commit()

    claimed = await queue.dequeue()
    assert claimed is not None
    assert claimed.title == "Waits for clone"
    assert claimed.status == "in_progress"


@pytest.mark.asyncio
async def test_dequeue_holds_task_of_error_project_until_reclone(queue):
    project = await _seed_project(
        queue.db,
        status="error",
        name="readiness-error",
        error_message="git clone failed: auth",
    )
    project_id = project.id
    task = await queue.create(
        title="Blocked by clone failure",
        description="d",
        project_id=project_id,
        target_repo=project.local_path,
    )
    task_id = task.id

    assert await queue.dequeue() is None
    refreshed = await queue.db.get(Task, task_id, populate_existing=True)
    assert refreshed.status == "pending"

    # Re-clone flips the Project back to ready; the Task resumes untouched.
    fresh_project = await queue.db.get(
        Project, project_id, populate_existing=True
    )
    fresh_project.status = "ready"
    await queue.db.commit()
    claimed = await queue.dequeue()
    assert claimed is not None
    assert claimed.id == task_id


@pytest.mark.asyncio
async def test_dequeue_unaffected_without_project_or_with_dangling_project(queue):
    await queue.create(
        title="Manual target repo",
        description="d",
        target_repo="/tmp/manual-repo",
    )
    first = await queue.dequeue()
    assert first is not None and first.title == "Manual target repo"

    project = await _seed_project(
        queue.db, status="ready", name="readiness-dangling"
    )
    await queue.create(
        title="Dangling project pointer",
        description="d",
        project_id=project.id,
        target_repo=project.local_path,
    )
    await queue.db.delete(project)
    await queue.db.commit()
    second = await queue.dequeue()
    assert second is not None and second.title == "Dangling project pointer"


@pytest.mark.asyncio
async def test_dequeue_holds_task_with_null_project_status():
    """A NULL status (legacy rows predating NOT NULL) must fail closed.

    ``NULL != 'ready'`` is UNKNOWN in SQL, so a naive predicate would treat
    such a Project as ready and dispatch a Task against a checkout that was
    never proven to exist. The current model forbids NULL, so this test
    recreates the legacy shape with a relaxed schema copy.
    """
    from sqlalchemy import MetaData
    from sqlalchemy.ext.asyncio import (
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )

    from backend.database import Base
    from backend.models.task_id_allocator import (
        TASK_ID_ALLOCATOR_SINGLETON_ID,
        TASK_ID_WORKER_NAMESPACE_START,
        TaskIdAllocator,
    )

    legacy_metadata = MetaData()
    for table in Base.metadata.tables.values():
        table.to_metadata(legacy_metadata)
    legacy_metadata.tables["projects"].columns["status"].nullable = True

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    try:
        async with engine.begin() as conn:
            await conn.run_sync(legacy_metadata.create_all)
            # ``to_metadata`` copies do not carry the original table's
            # after_create seed hook; insert the allocator singleton the same
            # way the canonical schema does.
            await conn.execute(
                legacy_metadata.tables[TaskIdAllocator.__tablename__]
                .insert()
                .values(
                    id=TASK_ID_ALLOCATOR_SINGLETON_ID,
                    node_role=None,
                    next_worker_task_id=TASK_ID_WORKER_NAMESPACE_START,
                )
            )
        factory = async_sessionmaker(
            engine, class_=AsyncSession, expire_on_commit=False
        )
        async with factory() as db:
            queue = TaskQueue(db)
            project = Project(
                name="legacy-null-status",
                git_url="https://github.com/example/legacy.git",
                has_remote=True,
                local_path="/tmp/legacy-null-status",
                default_branch="main",
            )
            db.add(project)
            await db.commit()
            await db.refresh(project)
            project_id = project.id

            # The ORM applies the column default when the attribute is None,
            # so the legacy NULL must be written through Core.
            from sqlalchemy import update as sa_update

            await db.execute(
                sa_update(Project)
                .where(Project.id == project_id)
                .values(status=None)
            )
            await db.commit()
            nulled = await db.get(Project, project_id, populate_existing=True)
            assert nulled.status is None

            task = await queue.create(
                title="Held behind NULL status",
                description="d",
                project_id=project_id,
                target_repo="/tmp/legacy-null-status",
            )
            task_id = task.id

            assert await queue.dequeue() is None
            held = await db.get(Task, task_id, populate_existing=True)
            assert held.status == "pending"

            fresh = await db.get(Project, project_id, populate_existing=True)
            fresh.status = "ready"
            await db.commit()
            claimed = await queue.dequeue()
            assert claimed is not None and claimed.id == task_id
    finally:
        await engine.dispose()


# ── API admission ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
@requires_posix_backend
async def test_create_task_rejects_error_project(client, session_factory):
    async with session_factory() as db:
        project = await _seed_project(
            db,
            status="error",
            name="api-error-project",
            error_message="git clone failed: could not read Username",
        )
        project_id = project.id

    resp = await client.post("/api/tasks", json={
        "title": "Should be refused",
        "description": "d",
        "project_id": project_id,
    })
    assert resp.status_code == 422
    assert "clone failed" in resp.json()["detail"]

    async with session_factory() as db:
        count = (
            await db.execute(
                select(Task).where(Task.project_id == project_id)
            )
        ).scalars().all()
        assert count == []


@pytest.mark.asyncio
@requires_posix_backend
async def test_create_task_allows_cloning_project(client, session_factory):
    async with session_factory() as db:
        project = await _seed_project(
            db, status="cloning", name="api-cloning-project"
        )
        project_id = project.id

    resp = await client.post("/api/tasks", json={
        "title": "Queued behind clone",
        "description": "d",
        "project_id": project_id,
    })
    assert resp.status_code == 201
    assert resp.json()["status"] == "pending"


@pytest.mark.asyncio
@requires_posix_backend
async def test_update_task_rejects_move_to_error_project(client, session_factory):
    async with session_factory() as db:
        source = await _seed_project(
            db, status="ready", name="api-move-source"
        )
        target = await _seed_project(
            db,
            status="error",
            name="api-move-target",
            error_message="git clone failed",
        )
        source_id, target_id = source.id, target.id

    created = await client.post("/api/tasks", json={
        "title": "Movable",
        "description": "d",
        "project_id": source_id,
    })
    assert created.status_code == 201
    task_id = created.json()["id"]

    moved = await client.put(f"/api/tasks/{task_id}", json={
        "project_id": target_id,
    })
    assert moved.status_code == 422
    assert "clone failed" in moved.json()["detail"]


@pytest.mark.asyncio
@requires_posix_backend
async def test_update_task_keeps_unchanged_error_project_editable(
    client, session_factory
):
    """A full-form PUT resubmitting the current project_id must stay allowed.

    Tasks deliberately wait in the queue for a re-clone; only a *new*
    association with an error Project is refused.
    """
    async with session_factory() as db:
        project = await _seed_project(
            db, status="ready", name="api-edit-keeps-project"
        )
        project_id = project.id

    created = await client.post("/api/tasks", json={
        "title": "Editable while waiting",
        "description": "d",
        "project_id": project_id,
    })
    assert created.status_code == 201
    task_id = created.json()["id"]

    async with session_factory() as db:
        broken = await db.get(Project, project_id)
        broken.status = "error"
        broken.error_message = "git clone failed"
        await db.commit()

    edited = await client.put(f"/api/tasks/{task_id}", json={
        "title": "Renamed while project waits for re-clone",
        "project_id": project_id,
    })
    assert edited.status_code == 200
    assert edited.json()["title"] == "Renamed while project waits for re-clone"


@pytest.mark.asyncio
@requires_posix_backend
async def test_todo_run_rejects_error_project(client, session_factory):
    async with session_factory() as db:
        project = await _seed_project(
            db,
            status="error",
            name="todo-error-project",
            error_message="git clone failed",
        )
        project_id = project.id

    todo = await client.post(f"/api/projects/{project_id}/todos", json={
        "title": "Todo on broken project",
        "prompt": "do it",
    })
    assert todo.status_code == 201
    todo_id = todo.json()["id"]

    run = await client.post(
        f"/api/projects/{project_id}/todos/{todo_id}/task",
        json={"title": "Todo on broken project", "prompt": "do it"},
    )
    assert run.status_code == 422
    assert "clone failed" in run.json()["detail"]


# ── Missing working directory ─────────────────────────────────────────────────


def test_prepare_task_working_directory_rejects_missing_explicit_path(tmp_path):
    from backend.services.task_agent_isolation import (
        TaskWorkingDirectoryMissingError,
        prepare_task_working_directory,
    )

    incarnation = "0123456789abcdef0123456789abcdef"
    missing = str(tmp_path / "gone" / "repo")
    with pytest.raises(TaskWorkingDirectoryMissingError) as exc_info:
        prepare_task_working_directory(
            1,
            incarnation,
            missing,
            has_explicit_workspace=True,
        )
    assert "does not exist" in str(exc_info.value)

    existing = tmp_path / "repo"
    existing.mkdir()
    resolved = prepare_task_working_directory(
        1,
        incarnation,
        str(existing),
        has_explicit_workspace=True,
    )
    assert os.path.isdir(resolved)


def test_require_existing_task_cwd(tmp_path):
    from backend.services.task_agent_isolation import (
        TaskWorkingDirectoryMissingError,
        require_existing_task_cwd,
    )

    assert require_existing_task_cwd(str(tmp_path)) == str(tmp_path)
    with pytest.raises(TaskWorkingDirectoryMissingError):
        require_existing_task_cwd(str(tmp_path / "missing"))


# ── Clone failure/side-channel notes ─────────────────────────────────────────


@requires_posix_backend
def test_describe_clone_failure_prefixes_auth_errors():
    from backend.api.projects import _describe_clone_failure

    raw = (
        "git clone failed: fatal: could not read Username for "
        "'https://github.com': No such device or address"
    )
    described = _describe_clone_failure(raw)
    assert described.startswith("git authentication failed")
    assert raw in described

    plain = "git clone failed: fatal: repository not found"
    assert _describe_clone_failure(plain) == plain


@pytest.mark.asyncio
@requires_posix_backend
async def test_clone_note_sync_annotates_and_clears(
    queue, db_factory, monkeypatch
):
    import backend.api.projects as projects_module

    monkeypatch.setattr(projects_module, "async_session", db_factory)

    project = await _seed_project(
        queue.db, status="cloning", name="note-sync-project"
    )
    task = await queue.create(
        title="Waiting task",
        description="d",
        project_id=project.id,
        target_repo=project.local_path,
    )

    await projects_module._sync_waiting_task_clone_notes(
        project.id, "git clone failed: auth"
    )
    refreshed = await queue.db.get(Task, task.id, populate_existing=True)
    assert refreshed.status == "pending"
    assert refreshed.error_message.startswith("Project clone failed: ")
    assert "git clone failed: auth" in refreshed.error_message

    await projects_module._sync_waiting_task_clone_notes(project.id, None)
    cleared = await queue.db.get(Task, task.id, populate_existing=True)
    assert cleared.error_message is None


@pytest.mark.asyncio
@requires_posix_backend
async def test_clone_note_clear_keeps_foreign_error_messages(
    queue, db_factory, monkeypatch
):
    import backend.api.projects as projects_module

    monkeypatch.setattr(projects_module, "async_session", db_factory)

    project = await _seed_project(
        queue.db, status="cloning", name="note-keep-project"
    )
    task = await queue.create(
        title="Task with unrelated error note",
        description="d",
        project_id=project.id,
        target_repo=project.local_path,
    )
    task.error_message = "some unrelated launch error"
    await queue.db.commit()

    await projects_module._sync_waiting_task_clone_notes(project.id, None)
    kept = await queue.db.get(Task, task.id, populate_existing=True)
    assert kept.error_message == "some unrelated launch error"


@pytest.mark.asyncio
@requires_posix_backend
async def test_clone_note_annotation_preserves_foreign_error_messages(
    queue, db_factory, monkeypatch
):
    """Failure annotation must never clobber an independent task diagnostic.

    Only tasks with an empty ``error_message`` or one previously written by
    the helper itself receive the clone note; a subsequent success clears
    only the generated note (PR #141 panel finding).
    """
    import backend.api.projects as projects_module

    monkeypatch.setattr(projects_module, "async_session", db_factory)

    project = await _seed_project(
        queue.db, status="cloning", name="note-preserve-project"
    )
    independent = await queue.create(
        title="Task with independent error",
        description="d",
        project_id=project.id,
        target_repo=project.local_path,
    )
    independent.error_message = "independent task error"
    empty = await queue.create(
        title="Task without error",
        description="d",
        project_id=project.id,
        target_repo=project.local_path,
    )
    noted = await queue.create(
        title="Task with a prior helper note",
        description="d",
        project_id=project.id,
        target_repo=project.local_path,
    )
    noted.error_message = (
        projects_module._CLONE_FAILURE_TASK_NOTE_PREFIX + "old clone failure"
    )
    await queue.db.commit()

    await projects_module._sync_waiting_task_clone_notes(
        project.id, "git clone failed: auth"
    )
    kept = await queue.db.get(Task, independent.id, populate_existing=True)
    assert kept.error_message == "independent task error"
    annotated = await queue.db.get(Task, empty.id, populate_existing=True)
    assert annotated.error_message.startswith("Project clone failed: ")
    assert "git clone failed: auth" in annotated.error_message
    renoted = await queue.db.get(Task, noted.id, populate_existing=True)
    assert renoted.error_message.startswith("Project clone failed: ")
    assert "git clone failed: auth" in renoted.error_message

    await projects_module._sync_waiting_task_clone_notes(project.id, None)
    still_kept = await queue.db.get(Task, independent.id, populate_existing=True)
    assert still_kept.error_message == "independent task error"
    cleared = await queue.db.get(Task, empty.id, populate_existing=True)
    assert cleared.error_message is None
    noted_cleared = await queue.db.get(Task, noted.id, populate_existing=True)
    assert noted_cleared.error_message is None


# ── Clone success is the final publication ────────────────────────────────────


@pytest.mark.asyncio
@requires_posix_backend
async def test_post_clone_setup_failure_cannot_reverse_ready(
    queue, db_factory, monkeypatch
):
    """``ready`` is authoritative: a failing post-clone step must not flip the
    Project back to ``error``, and the dispatcher wake happens only after the
    optional setup ran (so a claimed task never observes the reversal)."""
    from unittest.mock import AsyncMock, MagicMock, patch

    import backend.api.projects as projects_module
    import backend.services.delivery_setup as delivery_setup_module

    monkeypatch.setattr(projects_module, "async_session", db_factory)

    project = await _seed_project(
        queue.db, status="pending", name="ready-finality-project"
    )
    project_id = project.id
    task = await queue.create(
        title="Waits for final readiness",
        description="d",
        project_id=project_id,
        target_repo=project.local_path,
    )
    task_id = task.id

    events: list[str] = []

    async def failing_auto_configure(*args, **kwargs):
        events.append("auto_config")
        raise RuntimeError("post-clone GitHub setup exploded")

    monkeypatch.setattr(
        delivery_setup_module,
        "try_auto_configure_delivery_monitor",
        failing_auto_configure,
    )
    monkeypatch.setattr(
        projects_module,
        "_wake_dispatcher",
        lambda: events.append("wake"),
    )

    async def mock_subprocess(*args, **kwargs):
        proc = MagicMock()
        proc.communicate = AsyncMock(return_value=(b"", b""))
        proc.returncode = 0
        proc.wait = AsyncMock()
        return proc

    with patch("asyncio.create_subprocess_exec", side_effect=mock_subprocess), \
         patch("os.path.isdir", return_value=False), \
         patch("os.path.exists", return_value=True), \
         patch("os.makedirs"), \
         patch.object(projects_module, "_inject_agents_md", return_value=False), \
         patch.object(projects_module, "_scan_env_files", return_value=[]):
        await projects_module._clone_repo(
            project_id,
            "https://github.com/example/readiness.git",
            f"/tmp/ready-finality-{project_id}",
            "ready-finality-project",
            "main",
            None,
        )

    final_project = await queue.db.get(
        Project, project_id, populate_existing=True
    )
    assert final_project.status == "ready"
    assert final_project.error_message is None

    final_task = await queue.db.get(Task, task_id, populate_existing=True)
    assert final_task.status == "pending"
    assert final_task.error_message is None

    # The wake is the last step, after the isolated setup attempt.
    assert events == ["auto_config", "wake"]
