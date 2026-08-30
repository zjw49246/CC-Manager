"""Tests for Project API endpoints."""
from pathlib import Path
import subprocess

import pytest
from unittest.mock import patch, AsyncMock
from sqlalchemy import select

from backend.models.discussion import Discussion
from backend.models.pr_monitor import MonitoredRepo
from backend.models.project import Project
from backend.models.project_todo import ProjectTodo
from backend.models.task_share import ProjectShare
from backend.models.team_share import TeamProjectShare
from backend.models.worker import Worker


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ).stdout.strip()


@pytest.fixture
def mock_bg_tasks():
    """Patch background git tasks to prevent real git operations."""
    with patch("backend.api.projects._clone_repo", new_callable=AsyncMock) as mock_clone, \
         patch("backend.api.projects._init_local_repo", new_callable=AsyncMock) as mock_init:
        yield mock_clone, mock_init


@pytest.mark.asyncio
async def test_list_projects_empty(client):
    resp = await client.get("/api/projects")
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_create_project_with_git_url(client, mock_bg_tasks):
    mock_clone, mock_init = mock_bg_tasks
    resp = await client.post("/api/projects", json={
        "name": "my-remote-proj",
        "git_url": "https://github.com/user/repo.git",
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "my-remote-proj"
    assert data["has_remote"] is True
    assert data["git_url"] == "https://github.com/user/repo.git"
    assert data["status"] == "pending"


@pytest.mark.asyncio
async def test_create_project_local_no_git_url(client, mock_bg_tasks):
    mock_clone, mock_init = mock_bg_tasks
    resp = await client.post("/api/projects", json={"name": "local-proj"})
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "local-proj"
    assert data["has_remote"] is False
    assert data["git_url"] is None


@pytest.mark.asyncio
async def test_create_project_duplicate_name(client, mock_bg_tasks):
    await client.post("/api/projects", json={"name": "dup-proj"})
    resp = await client.post("/api/projects", json={"name": "dup-proj"})
    assert resp.status_code == 400
    assert "already exists" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_get_project(client, mock_bg_tasks):
    create_resp = await client.post("/api/projects", json={"name": "proj-get"})
    project_id = create_resp.json()["id"]
    resp = await client.get(f"/api/projects/{project_id}")
    assert resp.status_code == 200
    assert resp.json()["name"] == "proj-get"


@pytest.mark.asyncio
async def test_get_project_not_found(client):
    resp = await client.get("/api/projects/9999")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_update_project(client, mock_bg_tasks):
    create_resp = await client.post("/api/projects", json={"name": "proj-update"})
    project_id = create_resp.json()["id"]
    resp = await client.put(f"/api/projects/{project_id}", json={"name": "proj-renamed"})
    assert resp.status_code == 200
    assert resp.json()["name"] == "proj-renamed"


@pytest.mark.asyncio
async def test_update_project_git_url_sets_has_remote(client, mock_bg_tasks):
    """Setting git_url via update auto-sets has_remote=True."""
    create_resp = await client.post("/api/projects", json={"name": "local-2-remote"})
    project_id = create_resp.json()["id"]
    assert create_resp.json()["has_remote"] is False

    resp = await client.put(f"/api/projects/{project_id}", json={
        "git_url": "https://github.com/user/repo.git"
    })
    assert resp.status_code == 200
    assert resp.json()["has_remote"] is True


@pytest.mark.asyncio
async def test_update_project_not_found(client):
    resp = await client.put("/api/projects/9999", json={"name": "X"})
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_project(client, mock_bg_tasks):
    create_resp = await client.post("/api/projects", json={"name": "proj-del"})
    project_id = create_resp.json()["id"]
    resp = await client.delete(f"/api/projects/{project_id}")
    assert resp.status_code == 200
    assert resp.json()["ok"] is True

    resp = await client.get(f"/api/projects/{project_id}")
    assert resp.status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize("monitor_enabled", [True, False])
async def test_delete_project_requires_pr_monitor_to_be_deleted_first(
    client,
    mock_bg_tasks,
    session_factory,
    monitor_enabled,
):
    created = await client.post(
        "/api/projects",
        json={"name": f"project-with-monitor-{monitor_enabled}"},
    )
    project_id = created.json()["id"]

    async with session_factory() as db:
        monitor = MonitoredRepo(
            repo_full_name=f"owner/project-delete-{monitor_enabled}",
            project_id=project_id,
            enabled=monitor_enabled,
            webhook_secret="s" * 64,
        )
        todo = ProjectTodo(
            project_id=project_id,
            title="Preserve todo",
            prompt="This must survive a rejected Project deletion",
        )
        external_share = ProjectShare(
            project_id=project_id,
            shared_to_open_id=f"open-{monitor_enabled}",
            shared_to_name="Recipient",
            shared_to_ccm_url="https://recipient.example.com",
        )
        team_share = TeamProjectShare(
            project_id=project_id,
            target_type="user",
            target_id=100 + int(monitor_enabled),
            shared_by=1,
        )
        db.add_all([monitor, todo, external_share, team_share])
        await db.commit()
        monitor_id = monitor.id
        todo_id = todo.id
        external_share_id = external_share.id
        team_share_id = team_share.id

    response = await client.delete(f"/api/projects/{project_id}")

    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Delete PR Monitor {monitor_id} before deleting its Project"
    }
    async with session_factory() as db:
        assert await db.get(Project, project_id) is not None
        assert await db.get(MonitoredRepo, monitor_id) is not None
        assert await db.get(ProjectTodo, todo_id) is not None
        assert await db.get(ProjectShare, external_share_id) is not None
        assert await db.get(TeamProjectShare, team_share_id) is not None

    visible = await client.get("/api/pr-monitor/repos")
    assert visible.status_code == 200
    assert monitor_id in {item["id"] for item in visible.json()}


@pytest.mark.asyncio
async def test_delete_project_locks_pr_monitors_before_project_in_id_order(
    client,
    mock_bg_tasks,
    session_factory,
    monkeypatch,
):
    import backend.services.pr_review_actions as pr_review_actions
    import backend.services.project_share_admission as project_share_admission

    created = await client.post(
        "/api/projects",
        json={"name": "project-monitor-lock-order"},
    )
    project_id = created.json()["id"]
    async with session_factory() as db:
        monitors = [
            MonitoredRepo(
                repo_full_name=f"owner/project-lock-order-{index}",
                project_id=project_id,
                webhook_secret=str(index) * 64,
            )
            for index in (1, 2)
        ]
        db.add_all(monitors)
        await db.commit()
        monitor_ids = sorted(monitor.id for monitor in monitors)

    events = []
    original_monitor_lock = pr_review_actions.lock_pr_repo_action_boundary
    original_project_lock = project_share_admission.lock_project_share_authority

    async def traced_monitor_lock(db, monitor_id):
        events.append(("monitor", monitor_id))
        return await original_monitor_lock(db, monitor_id)

    async def traced_project_lock(db, locked_project_id):
        events.append(("project", locked_project_id))
        return await original_project_lock(db, locked_project_id)

    monkeypatch.setattr(
        pr_review_actions,
        "lock_pr_repo_action_boundary",
        traced_monitor_lock,
    )
    monkeypatch.setattr(
        project_share_admission,
        "lock_project_share_authority",
        traced_project_lock,
    )

    response = await client.delete(f"/api/projects/{project_id}")

    assert response.status_code == 409
    assert events == [
        ("monitor", monitor_id) for monitor_id in monitor_ids
    ] + [("project", project_id)]


@pytest.mark.asyncio
@pytest.mark.parametrize("race_kind", ["create", "move"])
async def test_delete_project_rechecks_monitor_created_or_moved_before_fence(
    client,
    mock_bg_tasks,
    session_factory,
    monkeypatch,
    race_kind,
):
    import backend.services.pr_review_actions as pr_review_actions
    import backend.services.project_share_admission as project_share_admission

    created = await client.post(
        "/api/projects",
        json={"name": f"project-monitor-{race_kind}-race-target"},
    )
    project_id = created.json()["id"]
    source_project_id = None
    monitor_id = None
    if race_kind == "move":
        source = await client.post(
            "/api/projects",
            json={"name": "project-monitor-move-race-source"},
        )
        source_project_id = source.json()["id"]
        async with session_factory() as db:
            monitor = MonitoredRepo(
                repo_full_name="owner/project-monitor-move-race",
                project_id=source_project_id,
                webhook_secret="m" * 64,
            )
            db.add(monitor)
            await db.commit()
            monitor_id = monitor.id

    original_monitor_lock = pr_review_actions.lock_pr_repo_action_boundary
    original_project_lock = project_share_admission.lock_project_share_authority
    raced = False

    async def race_then_lock_project(db, locked_project_id):
        nonlocal monitor_id, raced
        if not raced:
            raced = True
            async with session_factory() as race_db:
                if race_kind == "create":
                    await original_project_lock(race_db, project_id)
                    monitor = MonitoredRepo(
                        repo_full_name="owner/project-monitor-create-race",
                        project_id=project_id,
                        webhook_secret="c" * 64,
                    )
                    race_db.add(monitor)
                    await race_db.flush()
                    monitor_id = monitor.id
                else:
                    monitor = await original_monitor_lock(race_db, monitor_id)
                    for fenced_project_id in sorted(
                        {source_project_id, project_id}
                    ):
                        await original_project_lock(
                            race_db,
                            fenced_project_id,
                        )
                    monitor.project_id = project_id
                await race_db.commit()
        return await original_project_lock(db, locked_project_id)

    monkeypatch.setattr(
        project_share_admission,
        "lock_project_share_authority",
        race_then_lock_project,
    )

    response = await client.delete(f"/api/projects/{project_id}")

    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Delete PR Monitor {monitor_id} before deleting its Project"
    }
    async with session_factory() as db:
        assert await db.get(Project, project_id) is not None
        monitor = await db.get(MonitoredRepo, monitor_id)
        assert monitor is not None
        assert monitor.project_id == project_id


@pytest.mark.asyncio
async def test_delete_project_not_found(client):
    resp = await client.delete("/api/projects/9999")
    assert resp.status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize("discussion_status", ["active", "closing"])
async def test_delete_project_rejects_provider_capable_discussion_lease(
    client,
    mock_bg_tasks,
    session_factory,
    discussion_status,
):
    created = await client.post(
        "/api/projects",
        json={"name": f"project-delete-{discussion_status}-discussion"},
    )
    project_id = created.json()["id"]
    async with session_factory() as db:
        discussion = Discussion(
            title=f"{discussion_status} deletion lease",
            project_id=project_id,
            status=discussion_status,
        )
        db.add(discussion)
        await db.commit()

    response = await client.delete(f"/api/projects/{project_id}")

    assert response.status_code == 409
    assert "active or closing Discussion" in response.json()["detail"]
    async with session_factory() as db:
        assert await db.get(Project, project_id) is not None


@pytest.mark.asyncio
async def test_delete_project_requires_closed_discussion_to_be_deleted_first(
    client,
    mock_bg_tasks,
    session_factory,
):
    created = await client.post(
        "/api/projects",
        json={"name": "project-delete-closed-discussion"},
    )
    project_id = created.json()["id"]
    async with session_factory() as db:
        discussion = Discussion(
            title="closed deletion lease",
            project_id=project_id,
            status="closed",
        )
        db.add(discussion)
        await db.commit()
        discussion_id = discussion.id

    rejected = await client.delete(f"/api/projects/{project_id}")
    assert rejected.status_code == 409
    assert "Delete Discussion" in rejected.json()["detail"]
    async with session_factory() as db:
        assert await db.get(Project, project_id) is not None
        assert await db.get(Discussion, discussion_id) is not None

    cleaned = await client.delete(f"/api/discussions/{discussion_id}")
    assert cleaned.status_code == 200
    deleted = await client.delete(f"/api/projects/{project_id}")
    assert deleted.status_code == 200
    async with session_factory() as db:
        assert await db.get(Discussion, discussion_id) is None
        assert await db.get(Project, project_id) is None


@pytest.mark.asyncio
async def test_reclone_success(client, mock_bg_tasks, session_factory):
    """Reclone on a remote project resets status and triggers background clone."""
    mock_clone, mock_init = mock_bg_tasks
    create_resp = await client.post("/api/projects", json={
        "name": "proj-reclone",
        "git_url": "https://github.com/user/repo.git",
    })
    project_id = create_resp.json()["id"]

    resp = await client.post(f"/api/projects/{project_id}/reclone")
    assert resp.status_code == 200
    assert resp.json()["ok"] is True


@pytest.mark.asyncio
async def test_reclone_local_project_rejected(client, mock_bg_tasks):
    """Cannot reclone a local project (has_remote=False)."""
    create_resp = await client.post("/api/projects", json={"name": "proj-local-reclone"})
    project_id = create_resp.json()["id"]
    resp = await client.post(f"/api/projects/{project_id}/reclone")
    assert resp.status_code == 400
    assert "local project" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_reclone_worker_assigned_project_rejects_wrong_host_mutation(
    client,
    mock_bg_tasks,
    session_factory,
):
    mock_clone, _mock_init = mock_bg_tasks
    async with session_factory() as db:
        worker = Worker(name="remote-project-worker", status="ready")
        db.add(worker)
        await db.flush()
        project = Project(
            name="worker-routed-project",
            worker_id=worker.id,
            local_path="/workspace/worker-routed-project",
            git_url="https://github.com/example/worker-routed.git",
            has_remote=True,
            status="ready",
        )
        db.add(project)
        await db.commit()
        project_id = project.id

    response = await client.post(f"/api/projects/{project_id}/reclone")

    assert response.status_code == 409, response.text
    assert "cannot be re-cloned from the Manager" in response.json()["detail"]
    async with session_factory() as db:
        project = await db.get(Project, project_id)
        assert project is not None
        assert project.status == "ready"
    mock_clone.assert_not_awaited()


@pytest.mark.asyncio
async def test_worker_drain_rejects_project_create_and_headless_reclone(
    client,
    mock_bg_tasks,
    session_factory,
    monkeypatch,
):
    from backend.config import settings
    from backend.services.worker_node_control import begin_worker_node_drain

    mock_clone, _mock_init = mock_bg_tasks
    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(settings, "auth_token", "worker-project-test-token")
    client.headers["Authorization"] = "Bearer worker-project-test-token"

    async with session_factory() as db:
        existing = Project(
            name="existing-worker-project",
            local_path="/workspace/existing-worker-project",
            git_url="https://github.com/example/existing.git",
            has_remote=True,
            status="ready",
        )
        db.add(existing)
        await db.flush()
        project_id = existing.id
        await begin_worker_node_drain(db, claim="a" * 64)
        await db.commit()

    created = await client.post("/api/projects", json={
        "name": "must-not-cross-worker-drain",
        "git_url": "https://github.com/example/new.git",
    })
    assert created.status_code == 409, created.text
    recloned = await client.post(f"/api/projects/{project_id}/reclone")
    assert recloned.status_code == 403, recloned.text
    assert "outside the CCM Worker control-plane protocol" in recloned.json()["detail"]
    async with session_factory() as db:
        assert await db.get(Project, project_id) is not None
        assert (await db.get(Project, project_id)).status == "ready"
        assert await db.scalar(
            select(Project.id).where(
                Project.name == "must-not-cross-worker-drain"
            )
        ) is None
    mock_clone.assert_not_awaited()


@pytest.mark.asyncio
async def test_worker_restart_recovers_ownerless_project_materialization(
    session_factory,
    monkeypatch,
):
    from backend.config import settings
    from backend.services.project_materialization import (
        recover_interrupted_worker_project_materializations,
    )
    from backend.services.worker_node_control import begin_worker_node_drain

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    active_statuses = ("pending", "cloning", "initializing")
    async with session_factory() as db:
        db.add_all([
            Project(
                name=f"interrupted-{status}",
                local_path=f"/workspace/interrupted-{status}",
                status=status,
            )
            for status in active_statuses
        ])
        db.add(Project(
            name="already-ready",
            local_path="/workspace/already-ready",
            status="ready",
        ))
        await db.flush()
        await begin_worker_node_drain(db, claim="b" * 64)
        await db.commit()

    recovered = await recover_interrupted_worker_project_materializations(
        session_factory
    )
    assert recovered == len(active_statuses)
    async with session_factory() as db:
        projects = (
            await db.execute(select(Project).order_by(Project.name))
        ).scalars().all()
    by_name = {project.name: project for project in projects}
    for status in active_statuses:
        project = by_name[f"interrupted-{status}"]
        assert project.status == "error"
        assert "Worker restarted" in project.error_message
    assert by_name["already-ready"].status == "ready"


@pytest.mark.asyncio
async def test_worker_clone_skips_manager_delivery_monitor(
    db_factory,
    tmp_path,
    monkeypatch,
):
    from backend.api import projects as projects_mod
    from backend.config import settings
    from backend.services import delivery_setup

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(projects_mod, "async_session", db_factory)
    monkeypatch.setattr(
        projects_mod,
        "_prepare_existing_project_remote",
        AsyncMock(),
    )
    monkeypatch.setattr(projects_mod, "_apply_git_config", AsyncMock())
    monkeypatch.setattr(projects_mod, "_inject_agents_md", lambda _path: False)
    monkeypatch.setattr(
        projects_mod,
        "_commit_files",
        AsyncMock(return_value=((b"", b""), 0)),
    )
    monitor_setup = AsyncMock(return_value=None)
    monkeypatch.setattr(
        delivery_setup,
        "try_auto_configure_delivery_monitor",
        monitor_setup,
    )

    local = tmp_path / "worker-project-copy"
    local.mkdir()
    async with db_factory() as db:
        project = Project(
            name="worker-project-copy",
            local_path=str(local),
            git_url="https://github.com/example/repo.git",
            has_remote=True,
            default_branch="main",
            status="pending",
        )
        db.add(project)
        await db.commit()
        await db.refresh(project)
        project_id = project.id

    await projects_mod._clone_repo(
        project_id,
        "https://github.com/example/repo.git",
        str(local),
        "worker-project-copy",
        "main",
        {"git_author_name": "Worker"},
    )

    async with db_factory() as db:
        stored = await db.get(Project, project_id)
        assert stored.status == "ready"
    monitor_setup.assert_not_awaited()


# === AGENTS.md injection (Codex instruction file) ===


def test_inject_agents_md_creates_symlink(tmp_path):
    from backend.api.projects import _inject_agents_md
    (tmp_path / "CLAUDE.md").write_text("# guide\n")
    assert _inject_agents_md(str(tmp_path)) is True
    agents = tmp_path / "AGENTS.md"
    assert agents.exists()
    # Symlink (or fallback pointer file) must surface CLAUDE.md's guidance
    if agents.is_symlink():
        assert agents.read_text() == "# guide\n"
    else:
        assert "CLAUDE.md" in agents.read_text()


def test_inject_agents_md_noop_without_claude_md(tmp_path):
    from backend.api.projects import _inject_agents_md
    assert _inject_agents_md(str(tmp_path)) is False
    assert not (tmp_path / "AGENTS.md").exists()


def test_inject_agents_md_noop_when_exists(tmp_path):
    from backend.api.projects import _inject_agents_md
    (tmp_path / "CLAUDE.md").write_text("# guide\n")
    (tmp_path / "AGENTS.md").write_text("custom\n")
    assert _inject_agents_md(str(tmp_path)) is False
    assert (tmp_path / "AGENTS.md").read_text() == "custom\n"


@pytest.mark.asyncio
async def test_init_local_repo_preserves_existing_claude_md(db_factory, tmp_path, monkeypatch):
    """存量目录（有文件但未 git init）里已有的 CLAUDE.md 不被模板覆盖。"""
    from backend.api import projects as projects_mod
    monkeypatch.setattr(projects_mod, "async_session", db_factory)

    async with db_factory() as db:
        p = Project(name="pre", local_path=str(tmp_path), status="pending")
        db.add(p)
        await db.commit()
        await db.refresh(p)
        pid = p.id

    (tmp_path / "CLAUDE.md").write_text("# my existing guide\n")

    await projects_mod._init_local_repo(
        pid, str(tmp_path), "pre", "main",
        git_config={"git_user_name": "t", "git_user_email": "t@t.co"},
    )

    assert (tmp_path / "CLAUDE.md").read_text() == "# my existing guide\n"
    # AGENTS.md 补上了（指向未被覆盖的原 CLAUDE.md）
    assert (tmp_path / "AGENTS.md").exists()
    async with db_factory() as db:
        p2 = await db.get(Project, pid)
        assert p2.status == "ready"


@pytest.mark.asyncio
async def test_init_local_repo_preserves_both_existing_docs(db_factory, tmp_path, monkeypatch):
    """两个文件都已存在时全部原样保留，且不因无事可提交而报错。"""
    from backend.api import projects as projects_mod
    monkeypatch.setattr(projects_mod, "async_session", db_factory)

    async with db_factory() as db:
        p = Project(name="pre2", local_path=str(tmp_path), status="pending")
        db.add(p)
        await db.commit()
        await db.refresh(p)
        pid = p.id

    (tmp_path / "CLAUDE.md").write_text("# guide\n")
    (tmp_path / "AGENTS.md").write_text("# my own agents doc\n")

    await projects_mod._init_local_repo(
        pid, str(tmp_path), "pre2", "main",
        git_config={"git_user_name": "t", "git_user_email": "t@t.co"},
    )

    assert (tmp_path / "CLAUDE.md").read_text() == "# guide\n"
    assert (tmp_path / "AGENTS.md").read_text() == "# my own agents doc\n"
    async with db_factory() as db:
        p2 = await db.get(Project, pid)
        assert p2.status == "ready"


@pytest.mark.asyncio
async def test_existing_remote_project_adds_missing_origin_before_ready(
    db_factory,
    tmp_path,
    monkeypatch,
):
    from backend.api import projects as projects_mod
    from backend.services import delivery_setup

    remote = tmp_path / "remote.git"
    subprocess.run(
        ["git", "init", "--bare", str(remote)],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    local = tmp_path / "existing"
    local.mkdir()
    _git(local, "init", "-b", "main")
    _git(local, "config", "user.name", "CCM Test")
    _git(local, "config", "user.email", "ccm@example.invalid")
    (local / "seed.txt").write_text("seed\n", encoding="utf-8")
    _git(local, "add", "seed.txt")
    _git(local, "commit", "-m", "seed")

    monkeypatch.setattr(projects_mod, "async_session", db_factory)
    monitor_setup = AsyncMock(return_value=None)
    monkeypatch.setattr(
        delivery_setup,
        "try_auto_configure_delivery_monitor",
        monitor_setup,
    )
    async with db_factory() as db:
        project = Project(
            name="existing-remote",
            local_path=str(local),
            git_url=str(remote),
            has_remote=True,
            default_branch="main",
            status="pending",
        )
        db.add(project)
        await db.commit()
        await db.refresh(project)
        project_id = project.id

    await projects_mod._clone_repo(
        project_id,
        str(remote),
        str(local),
        "existing-remote",
        "main",
        {
            "git_author_name": "CCM Test",
            "git_author_email": "ccm@example.invalid",
        },
    )

    assert _git(local, "remote", "get-url", "origin") == str(remote)
    async with db_factory() as db:
        stored = await db.get(Project, project_id)
        assert stored is not None
        assert stored.status == "ready"
        assert stored.error_message is None
    monitor_setup.assert_awaited_once_with(project_id)


@pytest.mark.asyncio
async def test_existing_remote_project_rejects_ambiguous_origin(tmp_path):
    from backend.api import projects as projects_mod

    local = tmp_path / "ambiguous"
    local.mkdir()
    _git(local, "init", "-b", "main")
    first = "https://github.com/acme/first.git"
    second = "https://github.com/acme/second.git"
    _git(local, "remote", "add", "origin", first)
    _git(local, "config", "--add", "remote.origin.url", second)

    with pytest.raises(RuntimeError, match="at most one fetch"):
        await projects_mod._prepare_existing_project_remote(
            str(local),
            first,
            env=None,
        )


@pytest.mark.asyncio
async def test_existing_remote_project_repairs_push_only_origin(tmp_path):
    from backend.api import projects as projects_mod

    remote = tmp_path / "push-only-remote.git"
    subprocess.run(
        ["git", "init", "--bare", "--initial-branch=main", str(remote)],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    local = tmp_path / "push-only"
    local.mkdir()
    _git(local, "init", "-b", "main")
    _git(local, "config", "remote.origin.pushurl", str(remote))
    _git(local, "config", "user.name", "CCM Test")
    _git(local, "config", "user.email", "ccm@example.invalid")
    (local / "seed.txt").write_text("seed\n", encoding="utf-8")
    _git(local, "add", "seed.txt")
    _git(local, "commit", "-m", "seed")
    _git(local, "push", "origin", "main")

    await projects_mod._prepare_existing_project_remote(
        str(local),
        str(remote),
        env=None,
    )

    assert _git(local, "remote", "get-url", "origin") == str(remote)
    assert _git(local, "remote", "get-url", "--push", "origin") == str(remote)
