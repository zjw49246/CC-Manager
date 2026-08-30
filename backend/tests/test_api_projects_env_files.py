"""Tests for project env-files endpoints and helpers."""
import os
import pytest
from pathlib import Path
from unittest.mock import patch, AsyncMock, MagicMock

from backend.api.projects import _scan_env_files, _safe_resolve
from fastapi import HTTPException


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_bg_tasks():
    with patch("backend.api.projects._clone_repo", new_callable=AsyncMock) as mock_clone, \
         patch("backend.api.projects._init_local_repo", new_callable=AsyncMock) as mock_init:
        yield mock_clone, mock_init


@pytest.fixture
def project_dir(tmp_path):
    """Return a temp dir that acts as the project's local_path."""
    return tmp_path


async def _create_project(client, mock_bg_tasks, local_path: str) -> dict:
    """Helper: create a local project and patch its local_path in DB."""
    resp = await client.post("/api/projects", json={"name": "env-test-proj"})
    assert resp.status_code == 201
    project_id = resp.json()["id"]
    # Patch local_path directly in DB via update
    resp2 = await client.put(f"/api/projects/{project_id}", json={})
    # Use the session to set local_path directly
    return resp.json()


# ── _scan_env_files ───────────────────────────────────────────────────────────

def test_scan_finds_dot_env(tmp_path):
    (tmp_path / ".env").write_text("KEY=val")
    result = _scan_env_files(str(tmp_path))
    assert ".env" in result


def test_scan_finds_dot_env_local(tmp_path):
    (tmp_path / ".env.local").write_text("KEY=val")
    result = _scan_env_files(str(tmp_path))
    assert ".env.local" in result


def test_scan_finds_dot_env_production(tmp_path):
    (tmp_path / ".env.production").write_text("KEY=val")
    result = _scan_env_files(str(tmp_path))
    assert ".env.production" in result


def test_scan_finds_named_dot_env(tmp_path):
    (tmp_path / "backend.env").write_text("KEY=val")
    result = _scan_env_files(str(tmp_path))
    assert "backend.env" in result


def test_scan_skips_node_modules(tmp_path):
    node_modules = tmp_path / "node_modules"
    node_modules.mkdir()
    (node_modules / ".env").write_text("KEY=val")
    result = _scan_env_files(str(tmp_path))
    assert not any("node_modules" in p for p in result)


def test_scan_skips_git_dir(tmp_path):
    git_dir = tmp_path / ".git"
    git_dir.mkdir()
    (git_dir / ".env").write_text("KEY=val")
    result = _scan_env_files(str(tmp_path))
    assert not any(".git" in p for p in result)


def test_scan_skips_venv(tmp_path):
    venv = tmp_path / ".venv"
    venv.mkdir()
    (venv / ".env").write_text("KEY=val")
    result = _scan_env_files(str(tmp_path))
    assert not any(".venv" in p for p in result)


def test_scan_finds_nested_env(tmp_path):
    sub = tmp_path / "config"
    sub.mkdir()
    (sub / ".env").write_text("KEY=val")
    result = _scan_env_files(str(tmp_path))
    assert os.path.join("config", ".env") in result


def test_scan_result_is_sorted(tmp_path):
    (tmp_path / ".env").write_text("")
    (tmp_path / ".env.local").write_text("")
    (tmp_path / "app.env").write_text("")
    result = _scan_env_files(str(tmp_path))
    assert result == sorted(result)


def test_scan_ignores_non_env_files(tmp_path):
    (tmp_path / "app.py").write_text("print('hi')")
    (tmp_path / ".envrc").write_text("export KEY=val")  # Not matched
    result = _scan_env_files(str(tmp_path))
    assert result == []


def test_scan_empty_dir(tmp_path):
    result = _scan_env_files(str(tmp_path))
    assert result == []


# ── _safe_resolve ─────────────────────────────────────────────────────────────

def test_safe_resolve_valid(tmp_path):
    target = _safe_resolve(str(tmp_path), ".env")
    assert target == (tmp_path / ".env").resolve()


def test_safe_resolve_nested_valid(tmp_path):
    target = _safe_resolve(str(tmp_path), "config/.env")
    assert target == (tmp_path / "config" / ".env").resolve()


def test_safe_resolve_traversal_raises(tmp_path):
    with pytest.raises(HTTPException) as exc_info:
        _safe_resolve(str(tmp_path), "../../etc/passwd")
    assert exc_info.value.status_code == 400


def test_safe_resolve_absolute_path_raises(tmp_path):
    with pytest.raises(HTTPException):
        _safe_resolve(str(tmp_path), "/etc/passwd")


# ── API: list env files ───────────────────────────────────────────────────────

@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ("list", "get", "update", "scan"))
async def test_env_file_effects_reject_stale_cached_admin_role(
    session_factory,
    tmp_path,
    operation,
):
    """A concurrent demotion wins before any env secret filesystem effect."""

    import backend.api.projects as projects_api
    from backend.models.project import Project
    from backend.models.user import User

    env_path = tmp_path / ".env"
    env_path.write_text("SECRET=unchanged\n")
    async with session_factory() as db:
        user = User(
            email=f"stale-env-admin-{operation}@example.com",
            name="stale env admin",
            password_hash="test",
            role="member",
            is_active=True,
        )
        project = Project(
            name=f"stale env project {operation}",
            local_path=str(tmp_path),
            status="ready",
            env_files=[".env"],
        )
        db.add_all((user, project))
        await db.commit()
        await db.refresh(user)
        await db.refresh(project)
        user_id = user.id
        project_id = project.id

    request = MagicMock()
    request.state.auth_type = "jwt"
    request.state.user_id = user_id
    request.state.user_role = "admin"
    async with session_factory() as db:
        with pytest.raises(HTTPException) as rejected:
            if operation == "list":
                await projects_api.list_env_files(project_id, request, db)
            elif operation == "get":
                await projects_api.get_env_file(
                    project_id,
                    ".env",
                    request,
                    db,
                )
            elif operation == "update":
                await projects_api.update_env_file(
                    project_id,
                    ".env",
                    projects_api.EnvFileContent(content="SECRET=changed\n"),
                    request,
                    db,
                )
            else:
                await projects_api.scan_env_files(project_id, request, db)

    assert rejected.value.status_code == 409
    assert env_path.read_text() == "SECRET=unchanged\n"

@pytest.mark.asyncio
async def test_list_env_files_empty(client, mock_bg_tasks, session_factory, tmp_path):
    resp = await client.post("/api/projects", json={"name": "proj-envlist"})
    project_id = resp.json()["id"]

    # Set local_path via DB
    async with session_factory() as db:
        from backend.models.project import Project
        p = await db.get(Project, project_id)
        p.local_path = str(tmp_path)
        p.env_files = []
        await db.commit()

    resp = await client.get(f"/api/projects/{project_id}/env-files")
    assert resp.status_code == 200
    assert resp.json()["files"] == []


@pytest.mark.asyncio
async def test_list_env_files_with_existing_file(client, mock_bg_tasks, session_factory, tmp_path):
    (tmp_path / ".env").write_text("KEY=val")

    resp = await client.post("/api/projects", json={"name": "proj-envlist2"})
    project_id = resp.json()["id"]
    async with session_factory() as db:
        from backend.models.project import Project
        p = await db.get(Project, project_id)
        p.local_path = str(tmp_path)
        p.env_files = [".env"]
        await db.commit()

    resp = await client.get(f"/api/projects/{project_id}/env-files")
    assert resp.status_code == 200
    files = resp.json()["files"]
    assert len(files) == 1
    assert files[0]["path"] == ".env"
    assert files[0]["exists"] is True


@pytest.mark.asyncio
async def test_list_env_files_nonexistent_file(client, mock_bg_tasks, session_factory, tmp_path):
    resp = await client.post("/api/projects", json={"name": "proj-envlist3"})
    project_id = resp.json()["id"]
    async with session_factory() as db:
        from backend.models.project import Project
        p = await db.get(Project, project_id)
        p.local_path = str(tmp_path)
        p.env_files = [".env"]
        await db.commit()

    resp = await client.get(f"/api/projects/{project_id}/env-files")
    assert resp.status_code == 200
    files = resp.json()["files"]
    assert files[0]["exists"] is False


@pytest.mark.asyncio
async def test_list_env_files_project_not_found(client):
    resp = await client.get("/api/projects/9999/env-files")
    assert resp.status_code == 404


# ── API: get env file content ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_env_file_content(client, mock_bg_tasks, session_factory, tmp_path):
    (tmp_path / ".env").write_text("SECRET=abc123\nDEBUG=true")

    resp = await client.post("/api/projects", json={"name": "proj-envget"})
    project_id = resp.json()["id"]
    async with session_factory() as db:
        from backend.models.project import Project
        p = await db.get(Project, project_id)
        p.local_path = str(tmp_path)
        p.env_files = [".env"]
        await db.commit()

    resp = await client.get(f"/api/projects/{project_id}/env-files/.env")
    assert resp.status_code == 200
    assert resp.json()["content"] == "SECRET=abc123\nDEBUG=true"


@pytest.mark.asyncio
async def test_get_env_file_returns_empty_if_not_exists(client, mock_bg_tasks, session_factory, tmp_path):
    resp = await client.post("/api/projects", json={"name": "proj-envget2"})
    project_id = resp.json()["id"]
    async with session_factory() as db:
        from backend.models.project import Project
        p = await db.get(Project, project_id)
        p.local_path = str(tmp_path)
        p.env_files = [".env"]
        await db.commit()

    resp = await client.get(f"/api/projects/{project_id}/env-files/.env")
    assert resp.status_code == 200
    assert resp.json()["content"] == ""


@pytest.mark.asyncio
async def test_get_env_file_forbidden_if_not_tracked(client, mock_bg_tasks, session_factory, tmp_path):
    (tmp_path / ".env.secret").write_text("TOKEN=xyz")

    resp = await client.post("/api/projects", json={"name": "proj-envget3"})
    project_id = resp.json()["id"]
    async with session_factory() as db:
        from backend.models.project import Project
        p = await db.get(Project, project_id)
        p.local_path = str(tmp_path)
        p.env_files = []  # .env.secret not tracked
        await db.commit()

    resp = await client.get(f"/api/projects/{project_id}/env-files/.env.secret")
    assert resp.status_code == 403


# ── API: update env file content ──────────────────────────────────────────────

@pytest.mark.asyncio
async def test_update_env_file_creates_file(client, mock_bg_tasks, session_factory, tmp_path):
    resp = await client.post("/api/projects", json={"name": "proj-envput"})
    project_id = resp.json()["id"]
    async with session_factory() as db:
        from backend.models.project import Project
        p = await db.get(Project, project_id)
        p.local_path = str(tmp_path)
        p.env_files = [".env"]
        await db.commit()

    resp = await client.put(
        f"/api/projects/{project_id}/env-files/.env",
        json={"content": "API_KEY=test123\nDEBUG=false"},
    )
    assert resp.status_code == 200
    assert (tmp_path / ".env").read_text() == "API_KEY=test123\nDEBUG=false"


@pytest.mark.asyncio
async def test_update_env_file_creates_subdirs(client, mock_bg_tasks, session_factory, tmp_path):
    resp = await client.post("/api/projects", json={"name": "proj-envput2"})
    project_id = resp.json()["id"]
    async with session_factory() as db:
        from backend.models.project import Project
        p = await db.get(Project, project_id)
        p.local_path = str(tmp_path)
        p.env_files = ["config/.env.local"]
        await db.commit()

    resp = await client.put(
        f"/api/projects/{project_id}/env-files/config/.env.local",
        json={"content": "DB_HOST=localhost"},
    )
    assert resp.status_code == 200
    assert (tmp_path / "config" / ".env.local").read_text() == "DB_HOST=localhost"


@pytest.mark.asyncio
async def test_update_env_file_overwrite(client, mock_bg_tasks, session_factory, tmp_path):
    (tmp_path / ".env").write_text("OLD=value")

    resp = await client.post("/api/projects", json={"name": "proj-envput3"})
    project_id = resp.json()["id"]
    async with session_factory() as db:
        from backend.models.project import Project
        p = await db.get(Project, project_id)
        p.local_path = str(tmp_path)
        p.env_files = [".env"]
        await db.commit()

    resp = await client.put(
        f"/api/projects/{project_id}/env-files/.env",
        json={"content": "NEW=value"},
    )
    assert resp.status_code == 200
    assert (tmp_path / ".env").read_text() == "NEW=value"


@pytest.mark.asyncio
async def test_update_env_file_forbidden_if_not_tracked(client, mock_bg_tasks, session_factory, tmp_path):
    resp = await client.post("/api/projects", json={"name": "proj-envput4"})
    project_id = resp.json()["id"]
    async with session_factory() as db:
        from backend.models.project import Project
        p = await db.get(Project, project_id)
        p.local_path = str(tmp_path)
        p.env_files = []
        await db.commit()

    resp = await client.put(
        f"/api/projects/{project_id}/env-files/.env",
        json={"content": "KEY=val"},
    )
    assert resp.status_code == 403


# ── API: scan env files ───────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_scan_env_files_discovers_files(client, mock_bg_tasks, session_factory, tmp_path):
    (tmp_path / ".env").write_text("KEY=val")
    (tmp_path / ".env.local").write_text("LOCAL=1")

    resp = await client.post("/api/projects", json={"name": "proj-scan"})
    project_id = resp.json()["id"]
    async with session_factory() as db:
        from backend.models.project import Project
        p = await db.get(Project, project_id)
        p.local_path = str(tmp_path)
        p.env_files = []
        await db.commit()

    resp = await client.post(f"/api/projects/{project_id}/scan-env-files")
    assert resp.status_code == 200
    data = resp.json()
    assert ".env" in data["discovered"]
    assert ".env.local" in data["discovered"]
    assert data["tracked"] == []


@pytest.mark.asyncio
async def test_scan_separates_tracked_and_discovered(client, mock_bg_tasks, session_factory, tmp_path):
    (tmp_path / ".env").write_text("KEY=val")
    (tmp_path / ".env.local").write_text("LOCAL=1")

    resp = await client.post("/api/projects", json={"name": "proj-scan2"})
    project_id = resp.json()["id"]
    async with session_factory() as db:
        from backend.models.project import Project
        p = await db.get(Project, project_id)
        p.local_path = str(tmp_path)
        p.env_files = [".env"]  # .env already tracked
        await db.commit()

    resp = await client.post(f"/api/projects/{project_id}/scan-env-files")
    assert resp.status_code == 200
    data = resp.json()
    assert ".env" in data["tracked"]
    assert ".env" not in data["discovered"]
    assert ".env.local" in data["discovered"]


@pytest.mark.asyncio
async def test_scan_empty_repo(client, mock_bg_tasks, session_factory, tmp_path):
    resp = await client.post("/api/projects", json={"name": "proj-scan3"})
    project_id = resp.json()["id"]
    async with session_factory() as db:
        from backend.models.project import Project
        p = await db.get(Project, project_id)
        p.local_path = str(tmp_path)
        p.env_files = []
        await db.commit()

    resp = await client.post(f"/api/projects/{project_id}/scan-env-files")
    assert resp.status_code == 200
    data = resp.json()
    assert data["discovered"] == []
    assert data["tracked"] == []


@pytest.mark.asyncio
async def test_scan_project_not_found(client):
    resp = await client.post("/api/projects/9999/scan-env-files")
    assert resp.status_code == 404


# ── API: update env_files list via project update ─────────────────────────────

@pytest.mark.asyncio
async def test_update_project_env_files_list(client, mock_bg_tasks):
    resp = await client.post("/api/projects", json={"name": "proj-envfiles-list"})
    project_id = resp.json()["id"]
    assert resp.json()["env_files"] == []

    resp = await client.put(
        f"/api/projects/{project_id}",
        json={"env_files": [".env", ".env.local"]},
    )
    assert resp.status_code == 200
    assert resp.json()["env_files"] == [".env", ".env.local"]


@pytest.mark.asyncio
async def test_create_project_with_env_files(client, mock_bg_tasks):
    resp = await client.post("/api/projects", json={
        "name": "proj-envfiles-create",
        "env_files": [".env"],
    })
    assert resp.status_code == 201
    assert resp.json()["env_files"] == [".env"]


@pytest.mark.asyncio
async def test_env_files_default_empty(client, mock_bg_tasks):
    resp = await client.post("/api/projects", json={"name": "proj-default-envfiles"})
    assert resp.status_code == 201
    assert resp.json()["env_files"] == []
