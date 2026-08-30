import asyncio
import fnmatch
import logging
import os
import pathlib
from urllib.parse import urlsplit, urlunsplit

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import delete, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.database import get_db, async_session
from backend.services.agent_docs import inject_agents_md
from backend.models.discussion import Discussion
from backend.models.project import Project
from backend.api.deps import (
    get_current_user_id,
    get_current_user_role,
    lock_request_user_authority,
    require_admin,
    require_project_access,
    require_worker_target_access,
)
from backend.models.project_todo import ProjectTodo
from backend.models.tag import Tag
from backend.models.global_settings import GlobalSettings
from backend.schemas.project import ProjectCreate, ProjectUpdate, ProjectResponse, ProjectReorderItem
from backend.services.git_config import merge_git_config, settings_to_dict
from backend.services.dispatcher import _build_git_env
from backend.services.worker_assignment import (
    WorkerAssignmentConflict,
    fence_ready_worker_assignment,
)
from backend.services.worker_node_control import fence_worker_node_mutation

router = APIRouter(prefix="/api/projects", tags=["projects"])
logger = logging.getLogger(__name__)


_DELIVERY_PROJECT_IDENTITY_FIELDS = frozenset(
    {"git_url", "has_remote", "default_branch", "preview_config"}
)


async def _delivery_run_reference(
    db: AsyncSession,
    *,
    project_id: int,
    active_only: bool,
) -> int | None:
    """Return one Delivery Run whose durable scope depends on this Project."""

    # Keep this import local so the ordinary Project API does not make the
    # Delivery model part of module-import initialization order.
    from backend.models.delivery import DeliveryRun

    statement = select(DeliveryRun.id).where(
        DeliveryRun.project_id == project_id,
    )
    if active_only:
        statement = statement.where(DeliveryRun.activity != "terminal")
    return (await db.execute(statement.limit(1).with_for_update())).scalar_one_or_none()


async def _project_pr_monitor_ids(
    db: AsyncSession,
    *,
    project_id: int,
) -> tuple[int, ...]:
    """Discover PR Monitor identities before taking deletion writer locks."""

    from backend.models.pr_monitor import MonitoredRepo

    return tuple(
        await db.scalars(
            select(MonitoredRepo.id)
            .where(MonitoredRepo.project_id == project_id)
            .order_by(MonitoredRepo.id)
        )
    )


async def _project_pr_monitor_reference(
    db: AsyncSession,
    *,
    project_id: int,
) -> int | None:
    """Return one PR Monitor attached to a Project deletion fence.

    The caller has already locked every initially observed MonitoredRepo in ID
    order, then acquired the Project writer fence.  This final plain read must
    not acquire another Repo lock after the Project lock.  It catches a monitor
    that was created or moved here before the Project fence was won; once that
    fence is held, create/rebind cannot write ``project_id`` until this
    transaction finishes.  Disabled monitors count too.
    """

    from backend.models.pr_monitor import MonitoredRepo

    return await db.scalar(
        select(MonitoredRepo.id)
        .where(MonitoredRepo.project_id == project_id)
        .order_by(MonitoredRepo.id)
        .limit(1)
    )


async def _reject_active_delivery_project_mutation(
    db: AsyncSession,
    *,
    project_id: int,
) -> None:
    run_id = await _delivery_run_reference(
        db,
        project_id=project_id,
        active_only=True,
    )
    if run_id is not None:
        raise HTTPException(
            409,
            f"Project is frozen while Delivery Run {run_id} is active",
        )

async def _require_project_access(request: Request, project_id: int, db: AsyncSession):
    """Backward-compatible local alias for the shared Project ACL."""
    await require_project_access(request, project_id, db)


def _redact_project_for_member(project: Project) -> dict:
    """Return the member-visible Project projection without credentials.

    Project membership authorizes repository-scoped Task work, not access to
    the Manager's credential material.  Keep the public repository metadata
    useful while stripping both dedicated credential fields and accidental
    URL userinfo.
    """

    data = ProjectResponse.model_validate(project).model_dump()
    data["git_ssh_key_path"] = None
    data["git_https_token"] = None
    # Preview profiles are administrator-owned execution configuration. Their
    # argv and custom environment can contain deployment-specific material;
    # Project membership grants use of the capability, not read access to its
    # stored configuration.
    data["preview_config"] = None
    git_url = data.get("git_url")
    if isinstance(git_url, str) and "://" in git_url:
        try:
            parsed = urlsplit(git_url)
            if (
                parsed.username is not None
                or parsed.password is not None
                or parsed.query
                or parsed.fragment
            ):
                if parsed.hostname is None:
                    data["git_url"] = None
                    return data
                host = parsed.hostname
                # urlsplit() removes IPv6 brackets from ``hostname``;
                # urlunsplit() requires them in the netloc.
                if ":" in host and not host.startswith("["):
                    host = f"[{host}]"
                if parsed.port is not None:
                    host = f"{host}:{parsed.port}"
                # Query parameters and fragments are not required repository
                # identity. Legacy remotes sometimes carry access tokens in
                # either component, so never expose them to Project members.
                data["git_url"] = urlunsplit(
                    (parsed.scheme, host, parsed.path, "", "")
                )
        except ValueError:
            # A malformed legacy URL is repository configuration, not member
            # input. Hide it rather than risk returning embedded credentials.
            data["git_url"] = None
    return data


def _project_response_for_request(request: Request, project: Project) -> dict:
    if get_current_user_role(request) in ("admin", "super_admin"):
        return ProjectResponse.model_validate(project).model_dump()
    return _redact_project_for_member(project)



@router.get("")
async def list_projects(request: Request, db: AsyncSession = Depends(get_db)):
    user_id = get_current_user_id(request)
    user_role = get_current_user_role(request)
    stmt = select(Project).order_by(Project.sort_order.asc(), Project.name.asc())
    if user_role not in ("admin", "super_admin") and user_id:
        from backend.models.team_share import TeamProjectShare
        from backend.models.user_group import UserGroupMember
        user_group_ids = select(UserGroupMember.group_id).where(UserGroupMember.user_id == user_id)
        shared_project_ids = select(TeamProjectShare.project_id).where(
            ((TeamProjectShare.target_type == "user") & (TeamProjectShare.target_id == user_id))
            | ((TeamProjectShare.target_type == "group") & TeamProjectShare.target_id.in_(user_group_ids))
        )
        stmt = stmt.where(Project.id.in_(shared_project_ids))
    elif user_role not in ("admin", "super_admin"):
        stmt = stmt.where(False)
    result = await db.execute(stmt)
    projects = list(result.scalars().all())
    if user_role not in ("admin", "super_admin"):
        from backend.models.project import project_is_internal

        # A stale TeamProjectShare from an older deployment is not authority
        # to enumerate Manager-owned grouping Projects.
        projects = [project for project in projects if not project_is_internal(project)]

    # Annotate each project with its location using project.worker_id
    from backend.models.worker import Worker as WorkerModel
    worker_name_map: dict[int, str] = {}
    worker_ids = {p.worker_id for p in projects if p.worker_id}
    if worker_ids:
        wr = await db.execute(select(WorkerModel.id, WorkerModel.name).where(WorkerModel.id.in_(worker_ids)))
        worker_name_map = {wid: wname for wid, wname in wr}

    out = []
    for p in projects:
        d = _project_response_for_request(request, p)
        d["location"] = worker_name_map.get(p.worker_id, "local") if p.worker_id else "local"
        out.append(d)
    return out


@router.get("/tags", response_model=list[str])
async def list_project_tags(request: Request, db: AsyncSession = Depends(get_db)):
    """Return unique tags from projects the user can see."""
    user_id = get_current_user_id(request)
    user_role = get_current_user_role(request)
    stmt = select(Project)
    if user_role not in ("admin", "super_admin") and user_id:
        from backend.models.team_share import TeamProjectShare
        from backend.models.user_group import UserGroupMember
        user_group_ids = select(UserGroupMember.group_id).where(UserGroupMember.user_id == user_id)
        shared_project_ids = select(TeamProjectShare.project_id).where(
            ((TeamProjectShare.target_type == "user") & (TeamProjectShare.target_id == user_id))
            | ((TeamProjectShare.target_type == "group") & TeamProjectShare.target_id.in_(user_group_ids))
        )
        stmt = stmt.where(Project.id.in_(shared_project_ids))
    elif user_role not in ("admin", "super_admin"):
        stmt = stmt.where(False)
    result = await db.execute(stmt)
    all_tags: set[str] = set()
    from backend.models.project import project_is_internal

    for project in result.scalars():
        if user_role not in ("admin", "super_admin") and project_is_internal(project):
            continue
        for tag in project.tags or []:
            # Internal identity markers are not reusable presentation tags.
            if isinstance(tag, str) and not tag.startswith("ccm:internal:"):
                all_tags.add(tag)
    return sorted(all_tags)


@router.put("/reorder", response_model=list[ProjectResponse])
async def reorder_projects(
    body: list[ProjectReorderItem],
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Bulk-update sort_order for a list of projects."""
    require_admin(request)
    requested = {item.id: item.sort_order for item in body}
    if len(requested) != len(body):
        raise HTTPException(400, "Project reorder contains duplicate ids")
    # Project rows are the resource fence; numeric order prevents reciprocal
    # reorder requests from taking the same rows in opposite order.
    for project_id in sorted(requested):
        locked = await db.execute(
            update(Project)
            .where(Project.id == project_id)
            .values(id=Project.id)
        )
        if locked.rowcount != 1:
            await db.rollback()
            raise HTTPException(404, "Project not found")
    await lock_request_user_authority(request, db)
    for project_id, sort_order in requested.items():
        await db.execute(
            update(Project)
            .where(Project.id == project_id)
            .values(sort_order=sort_order)
        )
    await db.commit()
    # Reuse the canonical visibility filter.  Returning the whole Project table
    # here used to disclose credentials even when the request body was empty.
    return await list_projects(request, db)


@router.post("", response_model=ProjectResponse, status_code=201)
async def create_project(request: Request, body: ProjectCreate, db: AsyncSession = Depends(get_db)):
    require_admin(request)
    await require_worker_target_access(request, body.worker_id, db)
    # Worker Project materialization is a durable local producer.  Hold the
    # node-control writer through the Project insert so drain-first rejects it
    # and create-first is visible to the later drain proof.
    await fence_worker_node_mutation(db)
    # Check duplicate name
    existing = await db.execute(select(Project).where(Project.name == body.name))
    if existing.scalar_one_or_none():
        raise HTTPException(400, f"Project '{body.name}' already exists")

    workspace = os.path.expanduser(settings.workspace_dir)
    local_path = os.path.join(workspace, body.name)
    has_remote = body.git_url is not None and body.git_url.strip() != ""

    project = Project(
        name=body.name,
        worker_id=body.worker_id,
        git_url=body.git_url if has_remote else None,
        has_remote=has_remote,
        default_branch=body.default_branch,
        local_path=local_path,
        status="pending",
        sort_order=body.sort_order,
        tags=body.tags,
        env_files=body.env_files,
        git_author_name=body.git_author_name,
        git_author_email=body.git_author_email,
        git_credential_type=body.git_credential_type,
        git_ssh_key_path=body.git_ssh_key_path,
        git_https_username=body.git_https_username,
        git_https_token=body.git_https_token,
    )
    try:
        await fence_ready_worker_assignment(db, body.worker_id)
    except WorkerAssignmentConflict as exc:
        await db.rollback()
        raise HTTPException(409, exc.detail) from exc
    await lock_request_user_authority(request, db)
    db.add(project)

    # Auto-create Tag records for any new tag names
    if body.tags:
        existing = await db.execute(select(Tag.name))
        existing_names = {row[0] for row in existing}
        for tag_name in body.tags:
            if tag_name not in existing_names:
                db.add(Tag(name=tag_name))
                existing_names.add(tag_name)

    # Resolve every background argument before commit, then spawn immediately
    # after it without an intervening await.  Startup recovery handles the
    # remaining hard-crash boundary between commit and in-process scheduling.
    git_config = None
    if body.worker_id is None:
        global_cfg = await db.get(GlobalSettings, 1)
        git_config = merge_git_config(_extract_git_config(project), settings_to_dict(global_cfg))
    else:
        # Manager-side records assigned to Workers are only routing metadata;
        # the Worker creates and materializes its own local Project copy.
        project.status = "ready"

    await db.commit()
    if body.worker_id is None:
        if has_remote:
            asyncio.create_task(_clone_repo(project.id, body.git_url, local_path, body.name, body.default_branch, git_config))
        else:
            asyncio.create_task(_init_local_repo(project.id, local_path, body.name, body.default_branch, git_config))

    return project


@router.get("/{project_id}", response_model=ProjectResponse)
async def get_project(project_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    await _require_project_access(request, project_id, db)
    project = await db.get(Project, project_id)
    if not project:
        raise HTTPException(404, "Project not found")
    return _project_response_for_request(request, project)


@router.put("/{project_id}", response_model=ProjectResponse)
async def update_project(
    project_id: int, body: ProjectUpdate, request: Request, db: AsyncSession = Depends(get_db)
):
    require_admin(request)
    project = await db.get(Project, project_id)
    if not project:
        raise HTTPException(404, "Project not found")
    await db.rollback()
    locked = await db.execute(
        update(Project)
        .where(Project.id == project_id)
        .values(id=Project.id)
    )
    if locked.rowcount != 1:
        raise HTTPException(404, "Project not found")
    await lock_request_user_authority(request, db)
    project = await db.get(Project, project_id, populate_existing=True)
    updates = body.model_dump(exclude_unset=True)
    if "preview_config" in updates and updates["preview_config"] is not None:
        if not project.local_path:
            raise HTTPException(422, "Project has no local workspace")
        try:
            from pathlib import Path

            from backend.services.workspace_review import (
                PreviewConfigurationError,
                validate_preview_profiles,
            )

            normalized = validate_preview_profiles(
                updates["preview_config"],
                Path(project.local_path).resolve(strict=True),
            )
        except (OSError, PreviewConfigurationError, ValueError) as exc:
            raise HTTPException(422, str(exc)) from exc
        updates["preview_config"] = {
            "version": 2,
            "default_profile": normalized["default_profile"],
            "profiles": normalized["profiles"],
        }
    # Auto-sync has_remote when git_url is set
    if "git_url" in updates and updates["git_url"] and "has_remote" not in updates:
        updates["has_remote"] = True
    changed_delivery_identity = {
        field
        for field in _DELIVERY_PROJECT_IDENTITY_FIELDS & updates.keys()
        if updates[field] != getattr(project, field)
    }
    if changed_delivery_identity:
        await _reject_active_delivery_project_mutation(
            db,
            project_id=project_id,
        )
    for key, value in updates.items():
        setattr(project, key, value)

    # Auto-create Tag records for any new tag names
    if "tags" in updates and updates["tags"]:
        existing = await db.execute(select(Tag.name))
        existing_names = {row[0] for row in existing}
        for tag_name in updates["tags"]:
            if tag_name not in existing_names:
                db.add(Tag(name=tag_name))
                existing_names.add(tag_name)

    await db.commit()
    await db.refresh(project)

    # Apply git config to local repo immediately if any git fields changed
    git_fields = {"git_author_name", "git_author_email", "git_credential_type",
                  "git_ssh_key_path", "git_https_username", "git_https_token"}
    if git_fields & updates.keys() and project.local_path and os.path.isdir(project.local_path):
        await _apply_git_config(project.local_path, _extract_git_config(project))

    return project


@router.delete("/{project_id}")
async def delete_project(project_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    from backend.api.deps import require_admin
    from backend.services.project_share_admission import (
        ProjectShareAdmissionError,
        lock_project_share_authority,
    )
    from backend.services.pr_review_actions import (
        FindingActionConflict,
        lock_pr_repo_action_boundary,
    )

    require_admin(request)
    project = await db.get(Project, project_id)
    if not project:
        raise HTTPException(404, "Project not found")
    monitor_ids = await _project_pr_monitor_ids(
        db,
        project_id=project_id,
    )
    await db.rollback()
    try:
        for monitor_id in monitor_ids:
            await lock_pr_repo_action_boundary(db, monitor_id)
    except FindingActionConflict as exc:
        await db.rollback()
        raise HTTPException(
            409,
            "Could not establish the PR Monitor deletion fence; retry",
        ) from exc
    try:
        project = await lock_project_share_authority(db, project_id)
    except ProjectShareAdmissionError as exc:
        await db.rollback()
        raise HTTPException(
            409,
            "Could not establish the Project deletion fence; retry",
        ) from exc
    except ValueError as exc:
        await db.rollback()
        raise HTTPException(404, "Project not found") from exc
    await lock_request_user_authority(request, db)

    discussion = (
        await db.execute(
            select(Discussion.id, Discussion.status)
            .where(Discussion.project_id == project_id)
            .order_by(Discussion.id)
            .limit(1)
            .with_for_update()
        )
    ).first()
    if discussion is not None:
        discussion_id, discussion_status = discussion
        if discussion_status in {"active", "closing"}:
            detail = (
                "Cannot delete a Project with an active or closing Discussion "
                f"{discussion_id}"
            )
        else:
            detail = (
                f"Delete Discussion {discussion_id} before deleting its Project"
            )
        raise HTTPException(
            409,
            detail,
        )
    run_id = await _delivery_run_reference(
        db,
        project_id=project_id,
        active_only=False,
    )
    if run_id is not None:
        raise HTTPException(
            409,
            f"Cannot delete a Project referenced by Delivery Run {run_id}",
        )
    monitor_id = await _project_pr_monitor_reference(
        db,
        project_id=project_id,
    )
    if monitor_id is not None:
        raise HTTPException(
            409,
            f"Delete PR Monitor {monitor_id} before deleting its Project",
        )
    # project_todos declares ON DELETE CASCADE, but SQLite does not enforce FKs
    # (no `PRAGMA foreign_keys=ON` in database.py), so the DB won't cascade.
    # Delete the todos explicitly so this works on SQLite too.
    # Sharing tables also need explicit cleanup: TeamProjectShare has no FK,
    # and relying on ProjectShare's FK would leave stale grants on SQLite.
    from backend.models.task_share import ProjectShare
    from backend.models.team_share import TeamProjectShare

    await db.execute(delete(ProjectTodo).where(ProjectTodo.project_id == project_id))
    await db.execute(delete(ProjectShare).where(ProjectShare.project_id == project_id))
    await db.execute(delete(TeamProjectShare).where(
        TeamProjectShare.project_id == project_id
    ))
    await db.delete(project)
    await db.commit()
    return {"ok": True}


@router.post("/{project_id}/reclone")
async def reclone_project(project_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    require_admin(request)
    project = await db.get(Project, project_id)
    if not project:
        raise HTTPException(404, "Project not found")
    await db.rollback()
    await fence_worker_node_mutation(db)
    project = (
        await db.execute(
            select(Project)
            .where(Project.id == project_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if project is None:
        raise HTTPException(404, "Project not found")
    await lock_request_user_authority(request, db)
    if project.worker_id is not None:
        # The Manager row is routing metadata only; the authoritative checkout
        # lives on the Worker.  There is no durable remote reclone protocol, so
        # running _clone_repo here would mutate the wrong host.
        raise HTTPException(
            409,
            "Worker-assigned Projects cannot be re-cloned from the Manager",
        )
    if not project.has_remote:
        raise HTTPException(400, "Cannot reclone a local project")
    await _reject_active_delivery_project_mutation(
        db,
        project_id=project_id,
    )
    global_cfg = await db.get(GlobalSettings, 1)
    git_config = merge_git_config(
        _extract_git_config(project),
        settings_to_dict(global_cfg),
    )
    clone_args = (
        project_id,
        project.git_url,
        project.local_path,
        project.name,
        project.default_branch,
        git_config,
    )
    project.status = "pending"
    project.error_message = None
    await db.commit()
    asyncio.create_task(_clone_repo(*clone_args))
    return {"ok": True}


def _extract_git_config(project) -> dict:
    """Extract git config fields from a Project instance into a plain dict."""
    return {
        "git_author_name": project.git_author_name,
        "git_author_email": project.git_author_email,
        "git_credential_type": project.git_credential_type,
        "git_ssh_key_path": project.git_ssh_key_path,
        "git_https_username": project.git_https_username,
        "git_https_token": project.git_https_token,
    }


async def _apply_git_config(local_path: str, git_config: dict):
    """Write per-repo git config after clone/init so commits use the correct identity."""
    async def _git_config(key: str, value: str):
        proc = await asyncio.create_subprocess_exec(
            "git", "config", key, value,
            cwd=local_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await proc.communicate()

    if git_config.get("git_author_name"):
        await _git_config("user.name", git_config["git_author_name"])
    if git_config.get("git_author_email"):
        await _git_config("user.email", git_config["git_author_email"])

    ctype = git_config.get("git_credential_type")
    if ctype == "ssh" and git_config.get("git_ssh_key_path"):
        key_path = git_config["git_ssh_key_path"]
        ssh_cmd = f"ssh -i {key_path} -o StrictHostKeyChecking=no"
        await _git_config("core.sshCommand", ssh_cmd)
    elif ctype == "https" and git_config.get("git_https_token"):
        # Store credentials in the repo's local credential store so git push/pull can auth.
        # We write a plaintext .git/credentials file and point credential.helper at it.
        import pathlib
        from urllib.parse import urlparse
        creds_path = pathlib.Path(local_path) / ".git" / "credentials"
        username = git_config.get("git_https_username") or "oauth2"
        token = git_config["git_https_token"]
        # Extract host from remote URL; fall back to wildcard if not available
        host = ""
        try:
            remote_proc = await asyncio.create_subprocess_exec(
                "git", "remote", "get-url", "origin",
                cwd=local_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout_data, _ = await remote_proc.communicate()
            remote_url = stdout_data.decode().strip()
            if remote_url.startswith("http"):
                parsed = urlparse(remote_url)
                host = parsed.hostname or ""
            elif ":" in remote_url and "@" in remote_url:
                # git@github.com:user/repo.git format
                host = remote_url.split("@")[1].split(":")[0]
        except Exception:
            pass
        if not host:
            host = "github.com"
        # Build credential lines for both https and http schemes
        creds_content = f"https://{username}:{token}@{host}\nhttp://{username}:{token}@{host}\n"
        creds_path.write_text(creds_content)
        # Reset credential helper chain first — an empty string clears all inherited
        # helpers (e.g. macOS osxkeychain) so they don't take priority over our store.
        await _git_config("credential.helper", "")
        await _git_config("credential.helper", f"store --file {creds_path}")


def _generate_claude_md(project_name: str, git_url: str | None, default_branch: str) -> str:
    """Generate a CLAUDE.md template for a new project."""
    remote_info = git_url if git_url else "无（纯本地项目）"
    return f"""# {project_name} — 项目指南

> **重要：Claude 必须自主维护本文件。** 架构或约定变化时更新，保持简洁。

## Git 信息

- Remote: {remote_info}
- 默认分支: {default_branch}

## 任务生命周期

你收到任务后，按以下 9 步流程自主完成：

1. **领取任务** — 你已被分配任务，阅读本文件和项目代码理解上下文
2. **创建工作区**:
   - `git fetch origin`（如有 remote）
   - `git worktree add -b task-<简短描述> .claude-manager/worktrees/task-<简短描述> origin/{default_branch}`
   - 进入 worktree 目录工作（后续所有操作在 worktree 中）
   - 如果 worktree 创建失败，直接在当前分支工作
3. **实现功能** — 编写代码，确保可运行
4. **提交代码** — `git add` + `git commit`，commit message 简洁描述改动
5. **Merge + 测试**:
   - `git fetch origin && git merge origin/{default_branch}`（集成最新代码，如有 remote）
   - 运行测试（如有测试命令）
6. **自动合并到 {default_branch}**（如有 remote）:
   - `git fetch origin {default_branch}`
   - `git rebase origin/{default_branch}`，如果冲突则自行 resolve
   - 如果成功：`git checkout {default_branch} && git merge <task-branch> && git push origin {default_branch}`
   - 如果这一步有任何失败，退回到步骤 5 重试
   - （纯本地项目跳过本步）
7. **标记完成** — 更新文档（必须在清理之前，防止进程被杀时状态丢失）
8. **清理** — 回到项目根目录:
   - `git worktree remove .claude-manager/worktrees/<worktree名>`
   - `git branch -D <task-branch>`
   - 如有 remote: `git push origin --delete <task-branch>`
9. **经验沉淀** — 在 PROGRESS.md 记录经验教训（可选）

### 冲突处理

rebase 发生冲突时：
1. 查看冲突文件: `git diff --name-only --diff-filter=U`
2. 逐个解决冲突
3. `git add <resolved-files> && git rebase --continue`
4. 如果无法解决: `git rebase --abort`，退回步骤 5

### 状态判断

- 通过 `git remote -v` 判断是否有 remote
- 有 remote → 必须完成步骤 6（merge + push）
- 无 remote → 跳过步骤 5 的 fetch、步骤 6 和步骤 8 的远程分支删除

## 文件维护规则

> **以下文件都由 Claude Code 自主维护，每次功能变更后必须同步更新。**

- **CLAUDE.md**（本文件）：架构、约定、关键路径变化时更新，只改变化的部分，保持简洁
- **AGENTS.md**（Codex 读取）：**与 CLAUDE.md 保持关键内容同步**——这是 CC/Codex coding 时的行为纪律：往其中一个写新内容时，把相同的意思也写进另一个（不要求逐字一致）。正常状态它是指向本文件的 symlink（改一处即同步），不要改成独立文件；若两者已是独立文件，不要用 symlink 覆盖已有内容，逐次同步意思即可
- **README.md**：面向用户的文档，功能、使用流程变化时同步更新，保持与实际代码一致
- **TEST.md**：测试指南，新增功能时同步添加测试用例和文档
- **PROGRESS.md**：见下方「经验教训沉淀」

## 测试规范

**开发时必须主动使用测试，不是事后补充！**

- **改代码前**：先跑测试，确认基线全绿
- **改代码后**：再跑一遍确认无回归
- **新增功能**：同步新增测试用例，更新 TEST.md
- **修 bug**：先写复现 bug 的测试（红），修复后确认变绿

## 经验教训沉淀

每次遇到问题或完成重要改动后，要在 PROGRESS.md 中记录：
- 遇到了什么问题
- 如何解决的
- 以后如何避免
- **必须附上 git commit ID**

**同样的问题不要犯两次！**

## 注意事项

- 在 worktree 中工作时，不要切换到其他分支
- 完成任务后确保代码可运行、测试通过
"""


# 实现移到 services/agent_docs.py（dispatcher 惰性补齐存量项目时复用）
_inject_agents_md = inject_agents_md


async def _commit_files(local_path: str, files: list[str], message: str):
    """git add + commit the given files; best-effort (repo may lack user config)."""
    proc = await asyncio.create_subprocess_exec(
        "git", "add", *files,
        cwd=local_path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    await proc.communicate()
    proc = await asyncio.create_subprocess_exec(
        "git", "commit", "-m", message,
        cwd=local_path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    return await proc.communicate(), proc.returncode


async def _project_git(
    local_path: str,
    *args: str,
    env: dict[str, str] | None = None,
    allowed_returncodes: frozenset[int] = frozenset({0}),
) -> bytes:
    """Run one bounded project-import Git command without a shell."""

    proc = await asyncio.create_subprocess_exec(
        "git",
        *args,
        cwd=local_path,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode not in allowed_returncodes:
        detail = stderr.decode("utf-8", errors="replace").strip()
        command = args[0] if args else "command"
        raise RuntimeError(
            f"git {command} failed" + (f": {detail}" if detail else "")
        )
    return stdout


async def _local_git_config_values(local_path: str, key: str) -> list[str]:
    raw = await _project_git(
        local_path,
        "config",
        "--no-includes",
        "--local",
        "--null",
        "--get-all",
        key,
        allowed_returncodes=frozenset({0, 1}),
    )
    try:
        return [
            value.decode("utf-8", errors="strict")
            for value in raw.split(b"\0")
            if value
        ]
    except UnicodeDecodeError as exc:
        raise RuntimeError("existing Git origin configuration is not UTF-8") from exc


def _same_project_remote(configured: str, existing: str) -> bool:
    """Compare equivalent GitHub spellings while keeping other URLs exact."""

    from backend.services.delivery_service import _github_repo_from_url

    configured_repo = _github_repo_from_url(configured)
    existing_repo = _github_repo_from_url(existing)
    if configured_repo is not None or existing_repo is not None:
        return (
            configured_repo is not None
            and existing_repo is not None
            and configured_repo.casefold() == existing_repo.casefold()
        )
    if os.path.isabs(configured) and os.path.isabs(existing):
        return os.path.realpath(configured) == os.path.realpath(existing)
    return configured == existing


async def _prepare_existing_project_remote(
    local_path: str,
    git_url: str,
    *,
    env: dict[str, str] | None,
) -> None:
    """Bind an existing exact worktree to one unambiguous configured origin."""

    if (
        not isinstance(git_url, str)
        or not git_url.strip()
        or git_url.startswith("-")
        or any(character in git_url for character in "\r\n\0")
    ):
        raise RuntimeError("configured Git URL is malformed")
    top_level = (
        await _project_git(local_path, "rev-parse", "--show-toplevel")
    ).decode("utf-8", errors="strict").strip()
    if os.path.realpath(top_level) != os.path.realpath(local_path):
        raise RuntimeError("existing project directory is not the Git worktree root")

    fetch_urls = await _local_git_config_values(
        local_path,
        "remote.origin.url",
    )
    push_urls = await _local_git_config_values(
        local_path,
        "remote.origin.pushurl",
    )
    if len(fetch_urls) > 1 or len(push_urls) > 1:
        raise RuntimeError(
            "existing origin must define at most one fetch and one push URL"
        )
    if push_urls and not _same_project_remote(git_url, push_urls[0]):
        raise RuntimeError(
            "existing origin push URL does not match the configured Project Git URL"
        )
    if not fetch_urls:
        remote_names = (
            await _project_git(local_path, "remote")
        ).decode("utf-8", errors="strict").splitlines()
        await _project_git(
            local_path,
            "remote",
            "set-url" if "origin" in remote_names else "add",
            "origin",
            git_url,
        )
        fetch_urls = [git_url]
    if not _same_project_remote(git_url, fetch_urls[0]):
        raise RuntimeError(
            "existing origin does not match the configured Project Git URL"
        )
    # Fetch only the configured origin. ``git fetch --all`` succeeds when no
    # remotes exist and previously allowed such projects to be marked ready.
    await _project_git(local_path, "fetch", "origin", env=env)


_GIT_AUTH_ERROR_MARKERS = (
    "could not read username",
    "could not read password",
    "terminal prompts disabled",
    "authentication failed",
    "permission denied (publickey",
    "host key verification failed",
)

_CLONE_FAILURE_TASK_NOTE_PREFIX = "Project clone failed: "


def _describe_clone_failure(raw: str) -> str:
    """Prefix credential failures with actionable guidance, keep the git tail."""
    lowered = raw.lower()
    if any(marker in lowered for marker in _GIT_AUTH_ERROR_MARKERS):
        return (
            "git authentication failed — configure a valid HTTPS token or SSH "
            "key in the Project Git settings, then re-clone. " + raw
        )
    return raw


def _wake_dispatcher() -> None:
    """Best-effort dispatcher nudge after a Project flips to ready.

    Swallow every failure: raising here would land in the clone coroutine's
    ``except`` block and mislabel an already-committed successful clone as an
    error. The dispatcher's 2s poll remains the fallback.
    """
    try:
        from backend.main import dispatcher

        if dispatcher is not None:
            dispatcher.wake()
    except Exception:
        pass


async def _sync_waiting_task_clone_notes(project_id: int, reason: str | None) -> None:
    """Annotate (or clear) queued Tasks held by the Project readiness gate.

    Only ``error_message`` is written — status stays ``pending`` so a later
    re-clone resumes the Tasks untouched (the dequeue claim also clears the
    note). Best-effort: the authoritative failure record is the Project row.
    """
    from backend.models.task import Task

    conditions = [
        Task.project_id == project_id,
        Task.status == "pending",
        Task.worker_id.is_(None),
        Task.shared_from_id.is_(None),
    ]
    if reason is None:
        # Only clear our own note so a real per-task error is never erased.
        conditions.append(
            Task.error_message.like(_CLONE_FAILURE_TASK_NOTE_PREFIX + "%")
        )
        values = {"error_message": None}
    else:
        # Symmetric guard on the write side: only annotate tasks that carry
        # no diagnostic yet or one previously generated by this helper. An
        # independent launch/validation error on a pending task must never be
        # replaced by the project-level clone note.
        conditions.append(
            or_(
                Task.error_message.is_(None),
                Task.error_message == "",
                Task.error_message.like(_CLONE_FAILURE_TASK_NOTE_PREFIX + "%"),
            )
        )
        note = (_CLONE_FAILURE_TASK_NOTE_PREFIX + reason)[:400]
        values = {
            "error_message": note
            + " — the task will start automatically after the project is re-cloned"
        }
    try:
        async with async_session() as db:
            await db.execute(update(Task).where(*conditions).values(**values))
            await db.commit()
    except Exception:
        pass


async def _clone_repo(project_id: int, git_url: str, local_path: str, project_name: str, default_branch: str, git_config: dict | None = None):
    """Clone a git repo in the background."""
    async with async_session() as db:
        await db.execute(
            update(Project).where(Project.id == project_id).values(status="cloning")
        )
        await db.commit()

    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        # Build env with git credentials so clone/fetch can authenticate
        git_env = _build_git_env(git_config or {})
        env = {**os.environ, **git_env}
        # Background clones have no TTY to answer a credential prompt: git
        # either hangs the dev shell or fails with the cryptic "could not read
        # Username ... No such device or address" under systemd. Fail fast
        # instead — GIT_ASKPASS (set when a token is configured) is consulted
        # before terminal prompts, so authenticated clones are unaffected.
        env["GIT_TERMINAL_PROMPT"] = "0"
        # SSH can prompt via /dev/tty regardless of stdin, so batch mode must
        # be present for every background clone: augment a configured command
        # or fall back to a plain batch-mode ssh (clone scope only — the
        # shared _build_git_env also feeds agent subprocesses and must keep
        # interactive semantics).
        if "GIT_SSH_COMMAND" in env:
            env["GIT_SSH_COMMAND"] += " -o BatchMode=yes"
        else:
            env["GIT_SSH_COMMAND"] = "ssh -o BatchMode=yes"

        if os.path.isdir(local_path):
            # Existing directories are common when a local project is later
            # connected to GitHub. Make the configured origin explicit before
            # declaring the import ready; a no-remote fetch is otherwise a
            # misleading successful no-op.
            await _prepare_existing_project_remote(
                local_path,
                git_url,
                env=env,
            )
        else:
            proc = await asyncio.create_subprocess_exec(
                "git", "clone", git_url, local_path,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
            )
            _, stderr = await proc.communicate()
            if proc.returncode != 0:
                raise RuntimeError(f"git clone failed: {stderr.decode()}")

        # Apply per-repo git config (author identity + credentials)
        if git_config:
            await _apply_git_config(local_path, git_config)

        # Generate agent docs if not exists: CLAUDE.md (claude) + AGENTS.md (codex)
        created_files = []
        claude_md_path = os.path.join(local_path, "CLAUDE.md")
        if not os.path.exists(claude_md_path):
            with open(claude_md_path, "w") as f:
                f.write(_generate_claude_md(project_name, git_url, default_branch))
            created_files.append("CLAUDE.md")
        if _inject_agents_md(local_path):
            created_files.append("AGENTS.md")

        if created_files:
            # Stage and commit so they're not left untracked
            # (don't fail on commit error — repo may have no user config yet)
            await _commit_files(
                local_path, created_files,
                f"Add {' + '.join(created_files)} for Claude Code Manager",
            )

        # Auto-scan for .env files after clone
        env_files = _scan_env_files(local_path)
        async with async_session() as db:
            await db.execute(
                update(Project).where(Project.id == project_id).values(
                    status="ready", env_files=env_files
                )
            )
            await db.commit()

    except Exception as e:
        reason = _describe_clone_failure(str(e))
        async with async_session() as db:
            await db.execute(
                update(Project)
                .where(Project.id == project_id)
                .values(status="error", error_message=reason[:1000])
            )
            await db.commit()
        await _sync_waiting_task_clone_notes(project_id, reason)
        return

    # ``ready`` above is the authoritative final publication of this clone.
    # Nothing below may route back to the failure handler and reverse it: a
    # task claimed after the wake must never observe the Project flipping to
    # error because an optional post-clone step failed.
    if settings.ccm_node_role == "manager":
        # PR Monitor is Manager-authoritative state.  Worker Project copies
        # are compute caches and must not perform GitHub setup or create a
        # second, invisible MonitoredRepo in the Worker database.
        try:
            from backend.services.delivery_setup import (
                try_auto_configure_delivery_monitor,
            )

            await try_auto_configure_delivery_monitor(project_id)
        except Exception:
            logger.exception(
                "Post-clone Delivery Monitor auto-configuration failed for "
                "project %s; the Project stays ready",
                project_id,
            )

    # Tasks created while the clone was still running are held back by the
    # dispatch-queue readiness gate; drop any stale failure note and nudge the
    # dispatcher last, after every post-publication step (the 2s poll is only
    # the fallback).
    await _sync_waiting_task_clone_notes(project_id, None)
    _wake_dispatcher()


async def _init_local_repo(project_id: int, local_path: str, project_name: str, default_branch: str, git_config: dict | None = None):
    """Initialize a local git repo (no remote)."""
    async with async_session() as db:
        await db.execute(
            update(Project).where(Project.id == project_id).values(status="initializing")
        )
        await db.commit()

    try:
        os.makedirs(local_path, exist_ok=True)

        if not os.path.isdir(os.path.join(local_path, ".git")):
            # git init
            proc = await asyncio.create_subprocess_exec(
                "git", "init", "-b", default_branch,
                cwd=local_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await proc.communicate()
            if proc.returncode != 0:
                raise RuntimeError(f"git init failed: {stderr.decode()}")

            # Apply per-repo git config before first commit so author is correct
            if git_config:
                await _apply_git_config(local_path, git_config)

            # Generate agent docs: CLAUDE.md (claude) + AGENTS.md (codex)。
            # 目录可能是「已有文件但尚未 git init」的存量目录——已有的
            # CLAUDE.md/AGENTS.md 一律不覆盖（AGENTS.md 由 inject 内部守卫）
            files = []
            claude_md_path = os.path.join(local_path, "CLAUDE.md")
            if not os.path.exists(claude_md_path):
                with open(claude_md_path, "w") as f:
                    f.write(_generate_claude_md(project_name, None, default_branch))
                files.append("CLAUDE.md")
            if _inject_agents_md(local_path):
                files.append("AGENTS.md")

            # Initial commit（只提交本次创建的文件；两者都已存在时无事可提）
            if files:
                (_, stderr), returncode = await _commit_files(
                    local_path, files, f"Initial commit with {' + '.join(files)}",
                )
                if returncode != 0:
                    raise RuntimeError(f"git commit failed: {stderr.decode()}")

        # Auto-scan for .env files after init
        env_files = _scan_env_files(local_path)
        async with async_session() as db:
            await db.execute(
                update(Project).where(Project.id == project_id).values(
                    status="ready", env_files=env_files
                )
            )
            await db.commit()

    except Exception as e:
        reason = str(e)
        async with async_session() as db:
            await db.execute(
                update(Project)
                .where(Project.id == project_id)
                .values(status="error", error_message=reason[:1000])
            )
            await db.commit()
        await _sync_waiting_task_clone_notes(project_id, reason)
        return

    # ``ready`` above is the authoritative final publication; note cleanup and
    # the dispatcher nudge stay outside the failure handler's reach.
    await _sync_waiting_task_clone_notes(project_id, None)
    _wake_dispatcher()


# ── Env files helpers ─────────────────────────────────────────────────────────

# Patterns to match when auto-scanning for .env files
_ENV_FILE_PATTERNS = [".env", ".env.*", "*.env"]
# Directories to skip during scan
_SCAN_SKIP_DIRS = {
    "node_modules", ".git", "__pycache__", ".venv", "venv",
    "dist", "build", ".next", ".nuxt", "target", "vendor",
    ".claude-manager",
}


def _scan_env_files(local_path: str) -> list[str]:
    """Walk project tree and return relative paths of .env-style files."""
    found: list[str] = []
    root = pathlib.Path(local_path)
    for dirpath, dirnames, filenames in os.walk(local_path):
        dirnames[:] = [d for d in dirnames if d not in _SCAN_SKIP_DIRS]
        for fname in filenames:
            if any(fnmatch.fnmatch(fname, pat) for pat in _ENV_FILE_PATTERNS):
                rel = str(pathlib.Path(dirpath, fname).relative_to(root))
                found.append(rel)
    return sorted(found)


def _safe_resolve(local_path: str, rel_path: str) -> pathlib.Path:
    """Resolve rel_path under local_path, raising 400 on path traversal."""
    root = pathlib.Path(local_path).resolve()
    target = (root / rel_path).resolve()
    if not str(target).startswith(str(root) + os.sep) and target != root:
        raise HTTPException(400, "Invalid path")
    return target


# ── Env files endpoints ───────────────────────────────────────────────────────

class EnvFileInfo(BaseModel):
    path: str
    exists: bool


class EnvFilesListResponse(BaseModel):
    files: list[EnvFileInfo]


class EnvFileContent(BaseModel):
    content: str


class ScanEnvFilesResponse(BaseModel):
    tracked: list[str]    # already in env_files
    discovered: list[str] # found in repo but not yet tracked


async def _lock_admin_env_file_effect(
    request: Request,
    db: AsyncSession,
    project_id: int,
) -> Project:
    """Fence the exact Project and mutable admin role through a file effect."""

    # End any preliminary read snapshot before taking the portable no-op
    # writer barrier.  The fixed order is Project -> User, matching other
    # Project effects and preventing a cached admin role from exposing or
    # scanning secrets after a concurrent demotion.
    await db.rollback()
    locked = await db.execute(
        update(Project)
        .where(Project.id == project_id)
        .values(id=Project.id)
        .execution_options(synchronize_session=False)
    )
    if locked.rowcount != 1:
        await db.rollback()
        raise HTTPException(404, "Project not found")
    await lock_request_user_authority(request, db)
    project = await db.get(Project, project_id, populate_existing=True)
    if project is None:  # Defensive: the writer fence already proved it exists.
        await db.rollback()
        raise HTTPException(404, "Project not found")
    return project


@router.get("/{project_id}/env-files", response_model=EnvFilesListResponse)
async def list_env_files(
    project_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """List all configured env file paths and whether each exists on disk."""
    require_admin(request)
    project = await _lock_admin_env_file_effect(request, db, project_id)
    if not project.local_path:
        raise HTTPException(400, "Project has no local path")
    files = []
    for rel in (project.env_files or []):
        target = _safe_resolve(project.local_path, rel)
        files.append(EnvFileInfo(path=rel, exists=target.exists()))
    return EnvFilesListResponse(files=files)


@router.get("/{project_id}/env-files/{filepath:path}", response_model=EnvFileContent)
async def get_env_file(
    project_id: int,
    filepath: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Read content of a configured env file. Returns empty string if not yet created."""
    require_admin(request)
    project = await _lock_admin_env_file_effect(request, db, project_id)
    if not project.local_path:
        raise HTTPException(400, "Project has no local path")
    if filepath not in (project.env_files or []):
        raise HTTPException(403, "Path not in project env_files list")
    target = _safe_resolve(project.local_path, filepath)
    if not target.exists():
        return EnvFileContent(content="")
    return EnvFileContent(content=target.read_text(encoding="utf-8"))


@router.put("/{project_id}/env-files/{filepath:path}", response_model=EnvFileContent)
async def update_env_file(
    project_id: int,
    filepath: str,
    body: EnvFileContent,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Write content to a configured env file. Creates the file (and dirs) if needed."""
    require_admin(request)
    project = await _lock_admin_env_file_effect(request, db, project_id)
    if not project.local_path:
        raise HTTPException(400, "Project has no local path")
    if filepath not in (project.env_files or []):
        raise HTTPException(403, "Path not in project env_files list")
    target = _safe_resolve(project.local_path, filepath)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(body.content, encoding="utf-8")
    return EnvFileContent(content=body.content)


@router.post("/{project_id}/scan-env-files", response_model=ScanEnvFilesResponse)
async def scan_env_files(
    project_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Scan the project repo for .env-style files and return discovered paths."""
    require_admin(request)
    project = await _lock_admin_env_file_effect(request, db, project_id)
    if not project.local_path or not os.path.isdir(project.local_path):
        raise HTTPException(400, "Project has no local path or directory does not exist")
    tracked = list(project.env_files or [])
    tracked_set = set(tracked)
    all_found = _scan_env_files(project.local_path)
    discovered = [p for p in all_found if p not in tracked_set]
    return ScanEnvFilesResponse(tracked=tracked, discovered=discovered)
