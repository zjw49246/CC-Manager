import pytest

from backend.models.project import Project
from backend.models.ssh_profile import SSHProfile
from backend.models.task import Task
from backend.models.task_share import ProjectShare, TaskShare
from backend.models.task_ssh_grant import TaskSSHGrant
from backend.models.team_share import TeamTaskShare
from backend.services import task_sharing
from backend.services.task_ssh_access import (
    TaskSSHAccessError,
    replace_task_ssh_grants,
    resolve_task_ssh_profile,
    task_ssh_grant_snapshots,
    valid_task_ssh_capabilities,
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
            key_path="/private/key",
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
async def test_existing_team_share_invalidates_legacy_ssh_grant(db_factory):
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
        assert snapshots[0]["valid"] is False
        assert snapshots[0]["invalid_reason"] == "team_task_shared"
        assert await valid_task_ssh_capabilities(db, task) == set()
        with pytest.raises(TaskSSHAccessError, match="team_task_shared"):
            await resolve_task_ssh_profile(
                db,
                task_id=task_id,
                profile_id=profile_id,
                required_capability="read",
            )


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
