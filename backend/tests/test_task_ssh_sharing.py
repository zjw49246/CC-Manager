import json
import tempfile
from pathlib import Path

import pytest
from sqlalchemy import select

from backend.models.project import Project
from backend.models.ssh_profile import SSHProfile
from backend.models.task import Task
from backend.models.task_share import ProjectShare, TaskShare
from backend.models.task_ssh_grant import TaskSSHGrant
from backend.models.team_share import TeamTaskShare
from backend.services import task_sharing
from backend.services import task_ssh_access
from backend.services.task_creation import purge_task_access_grants
from backend.services.task_agent_isolation import (
    generate_claude_task_isolation_settings,
)
from backend.services.task_ssh_access import (
    TaskSSHAccessError,
    replace_task_ssh_grants,
    resolve_task_ssh_profile,
    task_ssh_grant_snapshots,
    task_ssh_protected_paths,
    valid_task_ssh_capabilities,
)
from backend.config import settings


@pytest.fixture(autouse=True)
def _managed_ssh_auth_enabled(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "auth_token", "managed-ssh-test-token")
    monkeypatch.setattr(
        settings,
        "ssh_key_storage_dir",
        str(tmp_path / "managed-ssh-keys"),
    )


async def _seed_task_profile(db_factory, *, with_grant: bool = False):
    async with db_factory() as db:
        project = Project(name="ssh-sharing-project", local_path="/tmp")
        db.add(project)
        await db.flush()
        task = Task(
            title="ssh sharing task",
            description="test",
            status="pending",
            project_id=project.id,
        )
        profile = SSHProfile(
            name="ssh-sharing-profile",
            host="example.invalid",
            port=22,
            username="deploy",
            key_path=str(
                Path(settings.ssh_key_storage_dir) / "managed" / "profile-key"
            ),
            public_key_fingerprint="SHA256:client",
            host_key_type="ssh-ed25519",
            host_key_value="ssh-ed25519 AAAA",
            host_key_fingerprint="SHA256:host",
            enabled=True,
            task_access_enabled=True,
            task_capabilities=["read"],
            allowed_roots=["/srv/app"],
        )
        db.add_all([task, profile])
        await db.flush()
        if with_grant:
            db.add(TaskSSHGrant(
                task_id=task.id,
                ssh_profile_id=profile.id,
                profile_revision=profile.revision,
                capabilities=["read"],
            ))
        await db.commit()
        return project.id, task.id, profile.id


@pytest.mark.asyncio
async def test_stale_profile_key_is_collapsed_under_managed_root_for_claude(
    db_factory,
):
    """A missing Profile leaf stays protected without breaking every Bash."""

    _project_id, task_id, profile_id = await _seed_task_profile(db_factory)
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        profile = await db.get(SSHProfile, profile_id)
        protected = await task_ssh_protected_paths(db, task=task)

    managed_root = str(Path(settings.ssh_key_storage_dir))
    assert not Path(profile.key_path).exists()
    assert managed_root in protected
    assert profile.key_path in protected

    path = generate_claude_task_isolation_settings(task_id, protected)
    payload = json.loads(path.read_text(encoding="utf-8"))
    filesystem = payload["sandbox"]["filesystem"]
    credential_paths = {
        entry["path"]
        for entry in payload["sandbox"]["credentials"]["files"]
    }
    assert managed_root in filesystem["denyRead"]
    assert managed_root in filesystem["denyWrite"]
    assert managed_root in credential_paths
    assert profile.key_path not in filesystem["denyRead"]
    assert profile.key_path not in filesystem["denyWrite"]
    assert profile.key_path not in credential_paths


@pytest.mark.asyncio
async def test_local_team_share_keeps_exact_ssh_grant_valid(db_factory):
    _project_id, task_id, profile_id = await _seed_task_profile(
        db_factory,
        with_grant=True,
    )
    async with db_factory() as db:
        db.add(TeamTaskShare(
            task_id=task_id,
            target_type="user",
            target_id=42,
            permission="chat",
            shared_by=1,
        ))
        await db.commit()
        task = await db.get(Task, task_id)
        snapshots = await task_ssh_grant_snapshots(db, task)
        assert snapshots[0]["valid"] is True
        assert snapshots[0]["invalid_reason"] is None
        assert await valid_task_ssh_capabilities(db, task) == {"read"}
        resolved = await resolve_task_ssh_profile(
            db,
            task_id=task_id,
            profile_id=profile_id,
            required_capability="read",
        )
        assert resolved.id == profile_id


@pytest.mark.asyncio
@pytest.mark.parametrize("share_kind", ["task", "project"])
async def test_cannot_add_ssh_grant_to_outbound_shared_scope(
    db_factory,
    share_kind,
):
    project_id, task_id, profile_id = await _seed_task_profile(db_factory)
    async with db_factory() as db:
        if share_kind == "task":
            db.add(TaskShare(
                task_id=task_id,
                shared_to_open_id="ou_remote",
                shared_to_ccm_url="https://remote.invalid",
                share_token="token-task",
                status="active",
            ))
        else:
            db.add(ProjectShare(
                project_id=project_id,
                shared_to_open_id="ou_remote",
                shared_to_ccm_url="https://remote.invalid",
                status="active",
            ))
        await db.commit()
        task = await db.get(Task, task_id)
        with pytest.raises(TaskSSHAccessError, match="cannot be enabled"):
            await replace_task_ssh_grants(
                db,
                task,
                [{"profile_id": profile_id, "capabilities": ["read"]}],
                created_by=1,
            )


@pytest.mark.asyncio
async def test_grant_admission_persists_incarnation_for_upgraded_legacy_task(
    db_factory,
):
    _project_id, task_id, profile_id = await _seed_task_profile(db_factory)
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.incarnation_id = None
        await db.commit()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        snapshots = await replace_task_ssh_grants(
            db,
            task,
            [{"profile_id": profile_id, "capabilities": ["read"]}],
            created_by=1,
        )

    assert snapshots[0]["valid"] is True
    async with db_factory() as db:
        upgraded = await db.get(Task, task_id)
        assert upgraded.incarnation_id is not None
        assert len(upgraded.incarnation_id) == 32
        assert set(upgraded.incarnation_id) <= set("0123456789abcdef")
        assert await db.scalar(
            select(TaskSSHGrant.id).where(TaskSSHGrant.task_id == task_id)
        ) is not None


@pytest.mark.asyncio
async def test_runtime_capability_resolution_fails_closed_without_auth_token(
    db_factory,
    monkeypatch,
):
    _project_id, task_id, profile_id = await _seed_task_profile(
        db_factory,
        with_grant=True,
    )
    monkeypatch.setattr(settings, "auth_token", "")

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert await valid_task_ssh_capabilities(db, task) == set()
        with pytest.raises(TaskSSHAccessError) as exc:
            await resolve_task_ssh_profile(
                db,
                task_id=task_id,
                profile_id=profile_id,
                required_capability="read",
            )

    assert exc.value.status_code == 503
    assert "AUTH_TOKEN" in exc.value.detail


@pytest.mark.asyncio
async def test_protected_paths_cover_all_git_credential_sources(
    db_factory,
    monkeypatch,
    tmp_path,
):
    instance_key = tmp_path / "instance-key"
    global_key = tmp_path / "global-key"
    project_key = tmp_path / "project-key"
    project_root = tmp_path / "project"
    runtime_worktree = tmp_path / "runtime-worktree"
    project_credentials = project_root / ".git" / "credentials"
    project_credentials.parent.mkdir(parents=True)
    project_credentials.write_text("https://example.invalid\n")
    monkeypatch.setattr(
        settings,
        "git_ssh_key_path",
        str(instance_key),
    )
    xdg_config_home = tmp_path / "xdg-config"
    xdg_data_home = tmp_path / "xdg-data"
    configured_global = tmp_path / "gitconfig"
    configured_gh = tmp_path / "gh-config"
    configured_netrc = tmp_path / "netrc"
    configured_askpass = tmp_path / "askpass"
    configured_ssh_askpass = tmp_path / "ssh-askpass"
    configured_git_ssh = tmp_path / "git-ssh"
    monkeypatch.setenv("XDG_CONFIG_HOME", str(xdg_config_home))
    monkeypatch.setenv("XDG_DATA_HOME", str(xdg_data_home))
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(configured_global))
    monkeypatch.setenv("GH_CONFIG_DIR", str(configured_gh))
    monkeypatch.setenv("NETRC", str(configured_netrc))
    monkeypatch.setenv("GIT_ASKPASS", str(configured_askpass))
    monkeypatch.setenv("SSH_ASKPASS", str(configured_ssh_askpass))
    monkeypatch.setenv("GIT_SSH", str(configured_git_ssh))

    async with db_factory() as db:
        project = Project(
            name="git-credential-protection-project",
            local_path=str(project_root),
            git_ssh_key_path=str(project_key),
        )
        task = Task(
            title="protect git credentials",
            status="executing",
            project_id=None,
        )
        global_settings = await db.get(GlobalSettings, 1)
        if global_settings is None:
            global_settings = GlobalSettings(id=1)
            db.add(global_settings)
        global_settings.git_ssh_key_path = str(global_key)
        db.add_all([project, task])
        await db.flush()
        task.project_id = project.id
        await db.commit()

        ordinary_protected = set(
            await task_ssh_protected_paths(
                db,
                task=task,
                working_directory=runtime_worktree,
            )
        )
        protected = set(
            await task_ssh_protected_paths(
                db,
                task=task,
                working_directory=runtime_worktree,
                include_direct_git_credentials=True,
            )
        )
        selected_protected = set(
            await task_ssh_protected_paths(
                db,
                task=task,
                working_directory=runtime_worktree,
                allowed_credential_paths=(project_key,),
            )
        )

    # Every Task denies ambient credential sources. An ordinary Task removes
    # only the selected exact file from same-path denies; its provider profile
    # adds an exact allowRead while all parent/sibling roots remain denied.
    assert ordinary_protected == protected
    assert str(instance_key) in ordinary_protected
    assert str(project_key) in ordinary_protected
    assert str(project_credentials) in ordinary_protected
    assert str(configured_askpass) in ordinary_protected
    assert str(project_key) not in selected_protected
    assert str(instance_key) in selected_protected

    assert str(instance_key) in protected
    assert str(global_key) in protected
    assert str(project_key) in protected
    assert str(project_credentials) in protected
    assert str(runtime_worktree / ".git" / "credentials") not in protected
    assert str(
        Path(tempfile.gettempdir()) / "claude-manager-askpass"
    ) in protected
    assert str(Path.home() / ".git-credentials") in protected
    assert str(Path.home() / ".config" / "git") in protected
    assert str(Path.home() / ".config" / "gh") in protected
    assert str(Path.home() / ".netrc") in protected
    assert str(xdg_config_home / "git") in protected
    assert str(xdg_config_home / "gh") in protected
    assert str(xdg_data_home / "git") in protected
    assert str(xdg_data_home / "gh") in protected
    assert str(configured_global) in protected
    assert str(configured_gh) in protected
    assert str(configured_netrc) in protected
    assert str(configured_askpass) in protected
    assert str(configured_ssh_askpass) in protected
    assert str(configured_git_ssh) in protected


@pytest.mark.asyncio
async def test_outbound_task_and_project_share_reject_existing_grants(db_factory):
    project_id, task_id, _profile_id = await _seed_task_profile(
        db_factory,
        with_grant=True,
    )
    target = [{
        "open_id": "ou_remote",
        "name": "Remote",
        "ccm_url": "https://remote.invalid",
    }]
    async with db_factory() as db:
        with pytest.raises(ValueError, match="Remove this Task's SSH grants"):
            await task_sharing.share_task(db, task_id, target)
    async with db_factory() as db:
        with pytest.raises(ValueError, match="Remove SSH grants"):
            await task_sharing.share_project(db, project_id, target)


@pytest.mark.asyncio
async def test_outbound_task_share_locks_task_before_checking_ssh_grants(
    db_factory,
    monkeypatch,
):
    _project_id, task_id, _profile_id = await _seed_task_profile(db_factory)
    order = []

    async def lock_authority(_db, _task):
        order.append("task")
        return True

    async def find_grant(_db, _task_id):
        order.append("ssh-grant")
        return True

    monkeypatch.setattr(
        task_sharing,
        "lock_task_share_authority",
        lock_authority,
    )
    monkeypatch.setattr(
        task_ssh_access,
        "task_has_any_ssh_grants",
        find_grant,
    )
    async with db_factory() as db:
        with pytest.raises(ValueError, match="Remove this Task's SSH grants"):
            await task_sharing.share_task(db, task_id, [])

    assert order == ["task", "ssh-grant"]


@pytest.mark.asyncio
async def test_task_access_purge_removes_managed_ssh_grants(db_factory):
    _project_id, task_id, _profile_id = await _seed_task_profile(
        db_factory,
        with_grant=True,
    )

    async with db_factory() as db:
        await purge_task_access_grants(db, task_id)
        await db.commit()

    async with db_factory() as db:
        assert await db.scalar(
            select(TaskSSHGrant.id).where(TaskSSHGrant.task_id == task_id)
        ) is None
from backend.models.global_settings import GlobalSettings
