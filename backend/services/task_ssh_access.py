from __future__ import annotations

import json
import os
import secrets
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.models.global_settings import GlobalSettings
from backend.models.project import Project
from backend.models.ssh_profile import SSHProfile
from backend.models.task import Task
from backend.models.task_ssh_grant import TaskSSHGrant
from backend.schemas.task_ssh_grant import TaskSSHGrantInput
from backend.services.skill_context import is_worker_managed_task_metadata
from backend.services.ssh_key_store import SSHManagedKeyStore, SSHManagedKeyStoreError


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


@dataclass(frozen=True)
class TaskSSHRuntimePolicy:
    """Launch-time SSH policy derived from every durable grant row.

    ``broker_only`` deliberately does not mean "has a currently valid grant".
    Once a Task has any durable grant row, a stale/disabled/shared Profile must
    not silently restore ambient Git credentials or direct networking. Valid
    capabilities control only which broker MCP tools can be exposed.
    """

    broker_only: bool
    capabilities: frozenset[str]


def _profile_uses_task_managed_key(profile: SSHProfile) -> bool:
    """Fail closed unless a Task-capable Profile key is in the managed root."""

    try:
        return SSHManagedKeyStore(
            settings.ssh_key_storage_dir,
        ).is_task_managed_path(profile.key_path)
    except (OSError, TypeError, ValueError, SSHManagedKeyStoreError):
        return False


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


def _configured_pool_home_roots(
    config_path: str | Path,
    *,
    home_field: str,
) -> set[str]:
    """Return every durable pool home, failing closed on malformed inventory.

    Disabled and retired accounts still contain provider credentials/session
    evidence, so their custom homes remain Manager-secret roots.  Read the
    entire authoritative config on every isolation snapshot: a bad record must
    never make later records disappear from the deny set or let a recovered
    Task fall back to ambient host reads.
    """

    config = Path(
        os.path.abspath(
            os.path.expandvars(os.path.expanduser(os.fspath(config_path)))
        )
    )
    values = _protected_path_variants(config.parent)
    try:
        info = config.lstat()
    except FileNotFoundError:
        return values
    except OSError as exc:
        raise TaskSSHAccessError(
            503,
            "Provider account inventory is unavailable for Task isolation",
        ) from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise TaskSSHAccessError(
            503,
            "Provider account inventory is unsafe for Task isolation",
        )
    try:
        payload = json.loads(config.read_text(encoding="utf-8"))
        accounts = payload.get("accounts") if isinstance(payload, dict) else None
        if not isinstance(accounts, list):
            raise ValueError("accounts must be a list")
        raw_homes: list[str] = []
        for account in accounts:
            if not isinstance(account, dict):
                raise ValueError("account must be an object")
            raw_home = account.get(home_field)
            if not isinstance(raw_home, str) or not raw_home.strip():
                raise ValueError(f"account {home_field} must be a path")
            expanded = os.path.expandvars(os.path.expanduser(raw_home.strip()))
            if not os.path.isabs(expanded):
                raise ValueError(f"account {home_field} must be absolute")
            raw_homes.append(expanded)
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
        raise TaskSSHAccessError(
            503,
            "Provider account inventory is invalid for Task isolation",
        ) from exc
    for home in raw_homes:
        values.update(_protected_path_variants(home))
    return values


def task_git_non_overridable_paths(
    *extra_paths: str | Path,
) -> tuple[str, ...]:
    """Return Manager-secret boundaries an exact Git allow may never reopen."""

    home = Path.home()
    values: set[str] = set()
    from backend.services.trusted_runtime import (
        trusted_runtime_protected_roots,
    )

    for value in (
        home / ".claude",
        home / ".codex",
        home / ".claude-pool",
        home / ".codex-pool",
        home / ".ccm",
        settings.ssh_key_storage_dir,
        settings.cloudrouter_accounts_dir,
        settings.task_runtime_secret_dir,
        Path(__file__).resolve().parent.parent.parent / ".env",
        *trusted_runtime_protected_roots(),
        *extra_paths,
    ):
        values.update(_protected_path_variants(value))
    values.update(_configured_pool_home_roots(
        settings.pool_config_path,
        home_field="config_dir",
    ))
    values.update(_configured_pool_home_roots(
        settings.codex_pool_config_path,
        home_field="codex_home",
    ))
    if settings.worker_ssh_key_path:
        values.update(_protected_path_variants(settings.worker_ssh_key_path))
    for backup_path in (
        settings.backup_temp_dir,
        settings.backup_destination_path,
    ):
        if backup_path:
            values.update(_protected_path_variants(backup_path))
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
        # Startup reports malformed database configuration separately. Keep
        # the allow-list narrow rather than guessing a path here.
        pass
    return tuple(sorted(values))


def manager_secret_protected_paths(
    *extra_paths: str | Path,
) -> tuple[str, ...]:
    """Provider-neutral alias for every stable Manager secret boundary."""

    return task_git_non_overridable_paths(*extra_paths)


async def task_ssh_protected_paths(
    db: AsyncSession,
    *,
    task: Task | None = None,
    working_directory: str | Path | None = None,
    extra_paths: Iterable[str | Path] = (),
    include_direct_git_credentials: bool = False,
    allowed_credential_paths: Iterable[str | Path] = (),
) -> tuple[str, ...]:
    """Return local credential paths that Task tools must never read.

    Include every Profile key, rather than only keys granted to the current
    Task: an ungranted key is exactly the credential an ambient shell must not
    discover.  The parent ``~/.ssh`` and CCM-managed storage roots also cover
    history/config files and keys that have not yet been imported as Profiles.
    """

    values: set[str] = set(task_git_non_overridable_paths(*extra_paths))
    home = Path.home()
    allowed_variants: set[str] = set()
    for allowed_path in allowed_credential_paths:
        allowed_variants.update(_protected_path_variants(allowed_path))
    key_paths = (await db.execute(select(SSHProfile.key_path))).scalars()
    for key_path in key_paths:
        if key_path:
            key_variants = _protected_path_variants(key_path)
            # Ordinary Tasks retain an explicitly selected Project/global Git
            # credential even when the same external file is also registered
            # as a Profile. This exemption is exact only: stable Manager roots
            # (notably SSH_KEY_STORAGE_DIR) remain denied below/above it.
            if key_variants and key_variants.issubset(allowed_variants):
                continue
            values.update(key_variants)

    # Every Task denies stable ambient credential roots. Ordinary Tasks may
    # re-allow only their exact, explicitly selected Git key/askpass file in
    # the provider permission profile; parent roots stay denied so keys or
    # helpers created after this snapshot cannot become visible.
    values.update(_protected_path_variants(home / ".ssh"))
    values.update(_protected_path_variants(home / ".git-credentials"))
    values.update(_protected_path_variants(home / ".gitconfig"))
    values.update(_protected_path_variants(home / ".netrc"))
    default_config_home = home / ".config"
    default_data_home = home / ".local" / "share"
    for root in (default_config_home, default_data_home):
        values.update(_protected_path_variants(root / "git"))
        values.update(_protected_path_variants(root / "gh"))
    for env_name in ("XDG_CONFIG_HOME", "XDG_DATA_HOME"):
        configured_root = os.environ.get(env_name)
        if configured_root:
            values.update(
                _protected_path_variants(Path(configured_root) / "git")
            )
            values.update(
                _protected_path_variants(Path(configured_root) / "gh")
            )
    for env_name in (
        "GIT_CONFIG_GLOBAL",
        "GH_CONFIG_DIR",
        "NETRC",
        "GIT_ASKPASS",
        "SSH_ASKPASS",
        "GIT_SSH",
    ):
        configured_path = os.environ.get(env_name)
        if configured_path and configured_path != os.devnull:
            values.update(_protected_path_variants(configured_path))
    values.update(
        _protected_path_variants(
            Path(tempfile.gettempdir()) / "claude-manager-askpass"
        )
    )
    if settings.git_ssh_key_path:
        values.update(_protected_path_variants(settings.git_ssh_key_path))

    global_settings = await db.get(GlobalSettings, 1)
    if global_settings and global_settings.git_ssh_key_path:
        values.update(
            _protected_path_variants(global_settings.git_ssh_key_path)
        )

    repository_paths: list[str | Path] = []
    if working_directory:
        repository_paths.append(working_directory)
    if task is not None and task.project_id is not None:
        project = await db.get(Project, task.project_id)
        if project is not None:
            if project.git_ssh_key_path:
                values.update(
                    _protected_path_variants(project.git_ssh_key_path)
                )
            if project.local_path:
                repository_paths.append(project.local_path)
    for repository_path in repository_paths:
        root = Path(
            os.path.expandvars(
                os.path.expanduser(os.fspath(repository_path))
            )
        )
        if not root.is_absolute():
            root = Path.cwd() / root
        # projects._apply_git_config writes the HTTPS token here and points
        # the local credential.helper at it. Protect an existing credential
        # leaf, but do not ask a provider sandbox to mount a missing leaf
        # beneath its already-denied .git parent: bubblewrap cannot create
        # that mount target inside a read-only parent.
        credential_path = root / ".git" / "credentials"
        if credential_path.exists():
            values.update(_protected_path_variants(credential_path))

    # Exact provider allowRead rules, not path omission, preserve the selected
    # ordinary Git credential beneath a denied parent. Removing only the exact
    # duplicate here avoids contradictory same-path deny/read entries; every
    # parent/root and all sibling credentials remain denied.
    values.difference_update(allowed_variants)
    return tuple(sorted(values))


async def task_ssh_sharing_invalid_reason(
    db: AsyncSession,
    *,
    task_id: int | None,
    project_id: int | None,
) -> str | None:
    """Return the first sharing boundary that makes Task SSH unsafe.

    Only legacy outbound federation shares cross the CCM trust boundary.
    Team shares are local ACL rows; the current caller role and exact Task SSH
    grant remain the authorization boundary for those turns.
    """

    from backend.models.task_share import ProjectShare, TaskShare

    checks = []
    if task_id is not None:
        checks.extend(((
                "task_shared_outbound",
                select(TaskShare.id)
                .where(
                    TaskShare.task_id == task_id,
                    TaskShare.status == "active",
                )
                .limit(1),
            ),))
    if project_id is not None:
        checks.extend(((
                "project_shared_outbound",
                select(ProjectShare.id)
                .where(
                    ProjectShare.project_id == project_id,
                    ProjectShare.status == "active",
                )
                .limit(1),
            ),))
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
    normalized_metadata = metadata if isinstance(metadata, Mapping) else {}
    if normalized_metadata.get("isolated_browser_agent") is True:
        return "task_isolated_browser_agent"
    if normalized_metadata.get("frontend_review") is not None:
        return "task_frontend_review"
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
    if not settings.auth_token:
        raise TaskSSHAccessError(
            503,
            "Managed SSH requires AUTH_TOKEN to be configured",
        )
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
        if not _profile_uses_task_managed_key(profile):
            raise TaskSSHAccessError(
                409,
                f"SSH profile {value.profile_id} must rotate its private key "
                "into CCM managed storage before Tasks may use it",
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
    if not _profile_uses_task_managed_key(profile):
        return "profile_key_not_managed"
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
    requested_inputs = list(inputs)
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
    # ``FOR UPDATE`` is ignored by SQLite.  This exact-row no-op UPDATE is a
    # portable writer fence shared with Monitor/Sub-Agent admission and Task
    # lifecycle writers: whichever transaction wins is visible to the loser
    # after it waits.  It also prevents delete/import of the same integer id
    # from crossing this replacement transaction.
    fenced = await db.execute(
        update(Task)
        .where(
            Task.id == task_id,
            Task.incarnation_id == task.incarnation_id,
        )
        .values(status=Task.status)
    )
    if fenced.rowcount != 1:
        raise TaskSSHAccessError(404, "Task not found")
    locked_task = await db.get(Task, task_id, populate_existing=True)
    if locked_task is None:
        raise TaskSSHAccessError(404, "Task not found")
    if locked_task.project_id != task.project_id:
        raise TaskSSHAccessError(
            409,
            "Task Project changed while SSH grants were being updated; retry",
        )
    if requested_inputs:
        active_statuses = {
            "in_progress",
            "executing",
            "merging",
            "migrating",
            "waiting_capability",
        }
        terminal_statuses = {
            "completed",
            "failed",
            "cancelled",
            "conflict",
            "superseded",
        }
        # Terminal Tasks retain their last ``instance_id`` for history after
        # the Instance releases ``current_task_id``.  Treat that stale link as
        # inactive; the reverse-owner check below remains authoritative and
        # still rejects any Instance that actually owns this Task.
        if locked_task.status in active_statuses or (
            locked_task.instance_id is not None
            and locked_task.status not in terminal_statuses
        ):
            raise TaskSSHAccessError(
                409,
                "Managed SSH grants cannot be added while the Task has an "
                "active execution generation",
            )

        from backend.models.instance import Instance

        reverse_owner_id = await db.scalar(
            select(Instance.id)
            .where(Instance.current_task_id == task_id)
            .limit(1)
        )
        if reverse_owner_id is not None:
            raise TaskSSHAccessError(
                409,
                "Managed SSH grants cannot be added while an Instance still "
                "owns the Task",
            )

        from backend.models.sub_agent import SubAgentSession

        running_child_id = await db.scalar(
            select(SubAgentSession.id)
            .where(
                SubAgentSession.task_id == task_id,
                SubAgentSession.status == "running",
            )
            .limit(1)
        )
        if running_child_id is not None:
            raise TaskSSHAccessError(
                409,
                "Managed SSH grants cannot be added while a Monitor or "
                "Sub-Agent is running",
            )

        # Harness admission takes the same exact Task writer fence before it
        # materializes a Run.  Whichever side wins is therefore visible to the
        # loser: a Task can never acquire Manager-held SSH credentials while
        # it owns a live Browser/Preview lifecycle.
        from backend.models.test_harness import (
            TestHarnessChildBinding,
            TestHarnessRun,
        )
        from backend.models.workspace_review import WorkspaceReviewRun
        from backend.services.test_harness_contracts import (
            HARNESS_TERMINAL_STATUSES,
        )

        active_harness_run_id = await db.scalar(
            select(TestHarnessRun.id)
            .where(
                TestHarnessRun.task_id == task_id,
                TestHarnessRun.status.not_in(HARNESS_TERMINAL_STATUSES),
            )
            .limit(1)
        )
        active_workspace_run_id = await db.scalar(
            select(WorkspaceReviewRun.id)
            .where(
                WorkspaceReviewRun.task_id == task_id,
                WorkspaceReviewRun.status.not_in(
                    {"completed", "failed", "cancelled"}
                ),
            )
            .limit(1)
        )
        active_browser_binding_id = await db.scalar(
            select(TestHarnessChildBinding.id)
            .where(
                TestHarnessChildBinding.owner_task_id == task_id,
                TestHarnessChildBinding.state.not_in(
                    {"stopped", "completed"}
                ),
            )
            .limit(1)
        )
        if any(
            value is not None
            for value in (
                active_harness_run_id,
                active_workspace_run_id,
                active_browser_binding_id,
            )
        ):
            raise TaskSSHAccessError(
                409,
                "Managed SSH grants cannot be combined with an active "
                "Browser Review or Test Harness run",
            )

    prepared = await prepare_task_ssh_grants(
        db,
        requested_inputs,
        worker_id=locked_task.worker_id,
        shared_from_id=locked_task.shared_from_id,
        metadata=locked_task.metadata_,
        task_id=locked_task.id,
        project_id=locked_task.project_id,
    )
    if prepared and locked_task.incarnation_id is None:
        # Delivery's incarnation migration deliberately leaves upgraded Task
        # rows nullable. Managed SSH credentials, however, are bound to the
        # exact incarnation and must remain invalid after Task-id reuse or a
        # Manager restart. Upgrade the legacy row while its Task lock is held
        # and commit the identity together with the grants.
        locked_task.incarnation_id = secrets.token_hex(16)
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
    if not settings.auth_token:
        raise TaskSSHAccessError(
            503,
            "Managed SSH requires AUTH_TOKEN to be configured",
        )
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
    return set((await task_ssh_runtime_policy(db, task)).capabilities)


async def task_ssh_runtime_policy(
    db: AsyncSession,
    task: Task,
) -> TaskSSHRuntimePolicy:
    """Return the fail-closed launch policy for one Task snapshot."""

    rows = (await db.execute(
        select(TaskSSHGrant, SSHProfile)
        .join(SSHProfile, SSHProfile.id == TaskSSHGrant.ssh_profile_id)
        .where(TaskSSHGrant.task_id == task.id)
    )).all()
    broker_only = bool(rows)
    # AUTH_TOKEN is also the root from which route-scoped broker credentials
    # are derived. Legacy open mode cannot authenticate that broker, so never
    # materialize a Manager-held private-key capability into an agent turn.
    if not settings.auth_token:
        return TaskSSHRuntimePolicy(broker_only, frozenset())
    if task_ssh_scope_invalid_reason(
        worker_id=task.worker_id,
        shared_from_id=task.shared_from_id,
        metadata=task.metadata_,
    ) is not None:
        return TaskSSHRuntimePolicy(broker_only, frozenset())
    sharing_reason = await task_ssh_sharing_invalid_reason(
        db,
        task_id=task.id,
        project_id=task.project_id,
    )
    if sharing_reason is not None:
        return TaskSSHRuntimePolicy(broker_only, frozenset())
    capabilities = {
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
    return TaskSSHRuntimePolicy(broker_only, frozenset(capabilities))


async def task_has_valid_ssh_grants(
    db: AsyncSession,
    task: Task,
) -> bool:
    return bool(await valid_task_ssh_capabilities(db, task))
