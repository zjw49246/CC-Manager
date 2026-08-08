from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.models.ssh_profile import SSHProfile
from backend.models.task import Task
from backend.models.task_ssh_grant import TaskSSHGrant
from backend.schemas.task_ssh_grant import TaskSSHGrantInput
from backend.services.skill_context import is_worker_managed_task_metadata


class TaskSSHAccessError(ValueError):
    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


@dataclass(frozen=True)
class PreparedTaskSSHGrant:
    profile_id: int
    profile_revision: int
    capabilities: tuple[str, ...]


def task_ssh_policy_context(capabilities: Iterable[str]) -> str:
    """Return provider-neutral, model-visible instructions for managed SSH.

    The MCP allow-list is the enforcement boundary, but models also need an
    explicit routing rule.  Otherwise a request such as "check my SSH hosts"
    can make the general shell inspect ``~/.ssh/known_hosts`` and confuse
    historical host keys with the Profiles authorized for this Task.
    """

    selected = sorted(set(capabilities) & {"exec", "read", "write"})
    if not selected:
        return ""
    return (
        "## Managed Task SSH (authoritative)\n"
        "This Task has CCM-managed SSH access with capabilities: "
        f"{', '.join(selected)}. For every request involving SSH, a remote "
        "server, remote commands, or remote files, call "
        "`ccm_ssh.list_connections` first and use only the returned "
        "`valid=true` Profile ids with the `ccm_ssh` tools. Treat that "
        "result as the complete authorized connection list. Never inspect "
        "or use ambient `~/.ssh`, `known_hosts`, SSH agents, private-key "
        "paths, system `ssh`/`scp`/`sftp`, or another network client as a "
        "substitute. If a requested connection or capability is absent, "
        "report that it must be authorized in CCM; do not search the host "
        "for another credential."
    )


def _protected_path_variants(value: str | Path) -> set[str]:
    # Settings paths may use either ``~`` or environment variables. Expand
    # both before deciding whether the path can be protected; treating a
    # literal ``$HOME/...`` as relative would silently omit a credential root.
    raw = Path(os.path.expandvars(os.path.expanduser(os.fspath(value))))
    if not raw.is_absolute():
        return set()
    variants = {str(raw)}
    try:
        variants.add(str(raw.resolve(strict=False)))
    except OSError:
        pass
    return variants


async def task_ssh_protected_paths(
    db: AsyncSession,
    *,
    extra_paths: Iterable[str | Path] = (),
) -> tuple[str, ...]:
    """Return local credential paths that Task tools must never read.

    Include every Profile key, rather than only keys granted to the current
    Task: an ungranted key is exactly the credential an ambient shell must not
    discover.  The parent ``~/.ssh`` and CCM-managed storage roots also cover
    history/config files and keys that have not yet been imported as Profiles.
    """

    values: set[str] = set()
    values.update(_protected_path_variants(Path.home() / ".ssh"))
    values.update(_protected_path_variants(Path.home() / ".claude"))
    values.update(_protected_path_variants(Path.home() / ".codex"))
    values.update(_protected_path_variants(Path.home() / ".claude-pool"))
    values.update(_protected_path_variants(Path.home() / ".codex-pool"))
    values.update(_protected_path_variants(Path.home() / ".ccm"))
    values.update(_protected_path_variants(settings.pool_config_path))
    values.update(_protected_path_variants(settings.codex_pool_config_path))
    values.update(_protected_path_variants(settings.ssh_key_storage_dir))
    values.update(_protected_path_variants(settings.cloudrouter_accounts_dir))
    values.update(_protected_path_variants(settings.task_runtime_secret_dir))
    if settings.worker_ssh_key_path:
        values.update(_protected_path_variants(settings.worker_ssh_key_path))
    manager_env = Path(__file__).resolve().parent.parent.parent / ".env"
    values.update(_protected_path_variants(manager_env))
    try:
        from sqlalchemy.engine import make_url

        database_url = make_url(settings.database_url)
        database_path = database_url.database
        if (
            database_url.get_backend_name() == "sqlite"
            and database_path
            and database_path != ":memory:"
        ):
            sqlite_path = Path(database_path)
            if not sqlite_path.is_absolute():
                sqlite_path = Path.cwd() / sqlite_path
            for suffix in ("", "-journal", "-shm", "-wal"):
                values.update(
                    _protected_path_variants(f"{sqlite_path}{suffix}")
                )
    except (TypeError, ValueError):
        # A malformed database URL will fail application startup separately.
        # Do not let path discovery broaden a Task permission profile.
        pass
    key_paths = (await db.execute(select(SSHProfile.key_path))).scalars()
    for key_path in key_paths:
        if key_path:
            values.update(_protected_path_variants(key_path))
    for extra_path in extra_paths:
        values.update(_protected_path_variants(extra_path))
    return tuple(sorted(values))


async def task_ssh_sharing_invalid_reason(
    db: AsyncSession,
    *,
    task_id: int | None,
    project_id: int | None,
) -> str | None:
    """Return the first sharing boundary that makes Task SSH unsafe.

    Team shares can let another user steer the same local Task, while outbound
    shares expose a remote chat capability. Project shares are included because
    new/current Tasks can be shared through that broader scope.
    """

    from backend.models.task_share import ProjectShare, TaskShare
    from backend.models.team_share import TeamProjectShare, TeamTaskShare

    checks = []
    if task_id is not None:
        checks.extend((
            (
                "team_task_shared",
                select(TeamTaskShare.id)
                .where(TeamTaskShare.task_id == task_id)
                .limit(1),
            ),
            (
                "task_shared_outbound",
                select(TaskShare.id)
                .where(
                    TaskShare.task_id == task_id,
                    TaskShare.status == "active",
                )
                .limit(1),
            ),
        ))
    if project_id is not None:
        checks.extend((
            (
                "team_project_shared",
                select(TeamProjectShare.id)
                .where(TeamProjectShare.project_id == project_id)
                .limit(1),
            ),
            (
                "project_shared_outbound",
                select(ProjectShare.id)
                .where(
                    ProjectShare.project_id == project_id,
                    ProjectShare.status == "active",
                )
                .limit(1),
            ),
        ))
    for reason, query in checks:
        if await db.scalar(query) is not None:
            return reason
    return None


async def project_has_task_ssh_grants(
    db: AsyncSession,
    project_id: int,
) -> bool:
    """Check whether any Task in a Project has an SSH grant row."""

    return (
        await db.scalar(
            select(TaskSSHGrant.id)
            .join(Task, Task.id == TaskSSHGrant.task_id)
            .where(Task.project_id == project_id)
            .limit(1)
        )
        is not None
    )


async def task_has_any_ssh_grants(db: AsyncSession, task_id: int) -> bool:
    """Check all grant rows, including stale/disabled grants."""

    return (
        await db.scalar(
            select(TaskSSHGrant.id)
            .where(TaskSSHGrant.task_id == task_id)
            .limit(1)
        )
        is not None
    )


def task_ssh_scope_invalid_reason(
    *,
    worker_id: int | None,
    shared_from_id: int | None,
    metadata: Mapping[str, Any] | None,
) -> str | None:
    if worker_id is not None:
        return "task_on_worker"
    if shared_from_id is not None:
        return "task_shared"
    if is_worker_managed_task_metadata(metadata):
        return "task_worker_managed"
    return None


async def prepare_task_ssh_grants(
    db: AsyncSession,
    inputs: Iterable[TaskSSHGrantInput | dict],
    *,
    worker_id: int | None,
    shared_from_id: int | None = None,
    metadata: Mapping[str, Any] | None = None,
    task_id: int | None = None,
    project_id: int | None = None,
) -> list[PreparedTaskSSHGrant]:
    parsed = [
        value if isinstance(value, TaskSSHGrantInput) else TaskSSHGrantInput.model_validate(value)
        for value in inputs
    ]
    if not parsed:
        return []
    if project_id is not None:
        # Serialize Task creation-with-grants against both team and outbound
        # Project sharing. Existing Task grant replacement instead locks the
        # Task row, which Project sharing also locks before checking grants.
        from backend.models.project import Project

        locked_project_id = await db.scalar(
            select(Project.id)
            .where(Project.id == project_id)
            .with_for_update()
        )
        if locked_project_id is None:
            raise TaskSSHAccessError(404, "Project not found")
    if task_ssh_scope_invalid_reason(
        worker_id=worker_id,
        shared_from_id=shared_from_id,
        metadata=metadata,
    ) is not None:
        raise TaskSSHAccessError(
            409,
            "Managed SSH grants are available only to local, unshared Manager Tasks",
        )
    sharing_reason = await task_ssh_sharing_invalid_reason(
        db,
        task_id=task_id,
        project_id=project_id,
    )
    if sharing_reason is not None:
        raise TaskSSHAccessError(
            409,
            "Managed SSH grants cannot be enabled on a shared Task or Project "
            f"({sharing_reason})",
        )
    profile_ids = [value.profile_id for value in parsed]
    if len(profile_ids) != len(set(profile_ids)):
        raise TaskSSHAccessError(422, "Each SSH profile may be granted only once")
    profiles = list((await db.execute(
        select(SSHProfile).where(SSHProfile.id.in_(profile_ids))
    )).scalars().all())
    by_id = {profile.id: profile for profile in profiles}
    prepared: list[PreparedTaskSSHGrant] = []
    for value in parsed:
        profile = by_id.get(value.profile_id)
        if profile is None or profile.deleted_at is not None:
            raise TaskSSHAccessError(404, f"SSH profile {value.profile_id} not found")
        if not profile.enabled:
            raise TaskSSHAccessError(409, f"SSH profile {value.profile_id} is disabled")
        if not profile.task_access_enabled:
            raise TaskSSHAccessError(
                409,
                f"SSH profile {value.profile_id} is available only in Files",
            )
        disallowed = set(value.capabilities) - set(profile.task_capabilities or [])
        if disallowed:
            raise TaskSSHAccessError(
                422,
                f"SSH profile {value.profile_id} does not allow Task capabilities: "
                + ", ".join(sorted(disallowed)),
            )
        prepared.append(PreparedTaskSSHGrant(
            profile_id=profile.id,
            profile_revision=profile.revision,
            capabilities=tuple(value.capabilities),
        ))
    return prepared


def task_ssh_grant_rows(
    task_id: int,
    prepared: Iterable[PreparedTaskSSHGrant],
    *,
    created_by: int | None,
) -> list[TaskSSHGrant]:
    return [
        TaskSSHGrant(
            task_id=task_id,
            ssh_profile_id=value.profile_id,
            profile_revision=value.profile_revision,
            capabilities=list(value.capabilities),
            created_by=created_by,
        )
        for value in prepared
    ]


def _invalid_reason(
    task: Task,
    grant: TaskSSHGrant,
    profile: SSHProfile,
    *,
    sharing_reason: str | None = None,
) -> str | None:
    scope_reason = task_ssh_scope_invalid_reason(
        worker_id=task.worker_id,
        shared_from_id=task.shared_from_id,
        metadata=task.metadata_,
    )
    if scope_reason is not None:
        return scope_reason
    if sharing_reason is not None:
        return sharing_reason
    if profile.deleted_at is not None:
        return "profile_deleted"
    if not profile.enabled:
        return "profile_disabled"
    if not profile.task_access_enabled:
        return "profile_task_access_disabled"
    if not set(grant.capabilities or []).issubset(
        set(profile.task_capabilities or [])
    ):
        return "profile_task_capabilities_changed"
    if profile.revision != grant.profile_revision:
        return "profile_revision_changed"
    return None


async def task_ssh_grant_snapshots(
    db: AsyncSession,
    task: Task,
) -> list[dict]:
    rows = (await db.execute(
        select(TaskSSHGrant, SSHProfile)
        .join(SSHProfile, SSHProfile.id == TaskSSHGrant.ssh_profile_id)
        .where(TaskSSHGrant.task_id == task.id)
        .order_by(SSHProfile.name.asc(), TaskSSHGrant.id.asc())
    )).all()
    sharing_reason = await task_ssh_sharing_invalid_reason(
        db,
        task_id=task.id,
        project_id=task.project_id,
    )
    snapshots = []
    for grant, profile in rows:
        invalid_reason = _invalid_reason(
            task,
            grant,
            profile,
            sharing_reason=sharing_reason,
        )
        snapshots.append({
            "id": grant.id,
            "task_id": grant.task_id,
            "profile_id": profile.id,
            "profile_name": profile.name,
            "host": profile.host,
            "port": profile.port,
            "username": profile.username,
            "host_key_fingerprint": profile.host_key_fingerprint,
            "profile_revision": grant.profile_revision,
            "current_profile_revision": profile.revision,
            "capabilities": grant.capabilities,
            "profile_task_access_enabled": profile.task_access_enabled,
            "profile_task_capabilities": profile.task_capabilities,
            "profile_allowed_roots": profile.allowed_roots,
            "valid": invalid_reason is None,
            "invalid_reason": invalid_reason,
            "created_by": grant.created_by,
            "created_at": grant.created_at,
            "updated_at": grant.updated_at,
        })
    return snapshots


async def replace_task_ssh_grants(
    db: AsyncSession,
    task: Task,
    inputs: Iterable[TaskSSHGrantInput | dict],
    *,
    created_by: int | None,
) -> list[dict]:
    task_id = task.id
    # Project sharing takes Project -> Task locks. Preserve that global order
    # here so a grant replacement cannot deadlock against a concurrent share.
    if task.project_id is not None:
        from backend.models.project import Project

        locked_project_id = await db.scalar(
            select(Project.id)
            .where(Project.id == task.project_id)
            .with_for_update()
        )
        if locked_project_id is None:
            raise TaskSSHAccessError(404, "Project not found")
    locked_task = (
        await db.execute(
            select(Task).where(Task.id == task_id).with_for_update()
        )
    ).scalar_one_or_none()
    if locked_task is None:
        raise TaskSSHAccessError(404, "Task not found")
    if locked_task.project_id != task.project_id:
        raise TaskSSHAccessError(
            409,
            "Task Project changed while SSH grants were being updated; retry",
        )
    prepared = await prepare_task_ssh_grants(
        db,
        inputs,
        worker_id=locked_task.worker_id,
        shared_from_id=locked_task.shared_from_id,
        metadata=locked_task.metadata_,
        task_id=locked_task.id,
        project_id=locked_task.project_id,
    )
    await db.execute(
        delete(TaskSSHGrant).where(TaskSSHGrant.task_id == task_id)
    )
    db.add_all(task_ssh_grant_rows(
        task_id,
        prepared,
        created_by=created_by,
    ))
    await db.commit()
    db.expire_all()
    refreshed = await db.get(Task, task_id)
    if refreshed is None:
        raise TaskSSHAccessError(409, "Task disappeared while saving SSH grants")
    return await task_ssh_grant_snapshots(db, refreshed)


async def resolve_task_ssh_profile(
    db: AsyncSession,
    *,
    task_id: int,
    profile_id: int,
    required_capability: str,
) -> SSHProfile:
    task = await db.get(Task, task_id)
    if task is None:
        raise TaskSSHAccessError(404, "Task not found")
    scope_reason = task_ssh_scope_invalid_reason(
        worker_id=task.worker_id,
        shared_from_id=task.shared_from_id,
        metadata=task.metadata_,
    )
    if scope_reason is not None:
        raise TaskSSHAccessError(
            409,
            f"Task SSH access is not available in this scope: {scope_reason}",
        )
    sharing_reason = await task_ssh_sharing_invalid_reason(
        db,
        task_id=task.id,
        project_id=task.project_id,
    )
    if sharing_reason is not None:
        raise TaskSSHAccessError(
            409,
            f"Task SSH access is not available in this scope: {sharing_reason}",
        )
    row = (await db.execute(
        select(TaskSSHGrant, SSHProfile)
        .join(SSHProfile, SSHProfile.id == TaskSSHGrant.ssh_profile_id)
        .where(
            TaskSSHGrant.task_id == task_id,
            TaskSSHGrant.ssh_profile_id == profile_id,
        )
    )).one_or_none()
    if row is None:
        raise TaskSSHAccessError(403, "SSH profile is not granted to this Task")
    grant, profile = row
    if required_capability not in (grant.capabilities or []):
        raise TaskSSHAccessError(
            403,
            f"Task SSH grant does not allow {required_capability}",
        )
    invalid_reason = _invalid_reason(
        task,
        grant,
        profile,
        sharing_reason=sharing_reason,
    )
    if invalid_reason is not None:
        raise TaskSSHAccessError(
            409,
            f"Task SSH grant is no longer valid: {invalid_reason}",
        )
    return profile


async def valid_task_ssh_capabilities(
    db: AsyncSession,
    task: Task,
) -> set[str]:
    if task_ssh_scope_invalid_reason(
        worker_id=task.worker_id,
        shared_from_id=task.shared_from_id,
        metadata=task.metadata_,
    ) is not None:
        return set()
    sharing_reason = await task_ssh_sharing_invalid_reason(
        db,
        task_id=task.id,
        project_id=task.project_id,
    )
    if sharing_reason is not None:
        return set()
    rows = (await db.execute(
        select(TaskSSHGrant, SSHProfile)
        .join(SSHProfile, SSHProfile.id == TaskSSHGrant.ssh_profile_id)
        .where(
            TaskSSHGrant.task_id == task.id,
            TaskSSHGrant.profile_revision == SSHProfile.revision,
            SSHProfile.enabled.is_(True),
            SSHProfile.task_access_enabled.is_(True),
            SSHProfile.deleted_at.is_(None),
        )
    )).all()
    return {
        capability
        for grant, profile in rows
        if _invalid_reason(
            task,
            grant,
            profile,
            sharing_reason=sharing_reason,
        ) is None
        for capability in (grant.capabilities or [])
        if capability in {"exec", "read", "write"}
    }


async def task_has_valid_ssh_grants(
    db: AsyncSession,
    task: Task,
) -> bool:
    return bool(await valid_task_ssh_capabilities(db, task))
