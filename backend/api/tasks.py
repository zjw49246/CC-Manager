import asyncio
import logging
import os
import shutil
import uuid
from contextlib import AsyncExitStack
from datetime import datetime
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import func, or_, select, update as sa_update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.database import get_db
from backend.models.task import Task
from backend.models.instance import Instance
from backend.schemas.task import (
    TaskActionRequest,
    PlanApprovalRequest,
    TaskCreate,
    TaskMigrationImport,
    TaskResponse,
    TaskRoutingExpectation,
    TaskTerminationRequest,
    TaskTerminationSnapshot,
    TaskUpdate,
    WorkerRoutingConfigRequest,
    WorkerRoutingConfigSnapshot,
)
from backend.services.task_queue import (
    TaskQueue,
    is_task_status_deletable,
    task_delete_fence,
)
from backend.services.task_creation import (
    prepare_task_create_values,
    stage_task_record,
    validate_task_service_tier_configuration,
)
from backend.services.pr_review_runtime import (
    is_pr_review_fix_task,
    is_pr_review_task,
    is_pr_sandbox_task,
)
from backend.services.task_skill_overrides import (
    clear_temporary_skills_marker,
)
from backend.services.task_termination import (
    TaskLaunchTerminationConflict,
    _finish_despite_cancellation as _finish_task_operation,
    lock_task_generation as _lock_task_generation,
    read_persisted_task_completed_at as _read_persisted_task_completed_at,
    remaining_task_process_generations as _remaining_task_process_generations,
    stop_task_process as _stop_task_process,
    task_generation_fence as _task_generation_fence,
)
from backend.services.worker_relay import (
    WorkerTaskGeneration,
    apply_authoritative_worker_task,
    worker_task_generation,
    worker_task_generation_predicates,
)
from backend.services.worker_proxy import (
    WorkerEndpointNotFoundError,
    get_task_operation_lock,
)
from backend.services.worker_routing_config import (
    InvalidWorkerRoutingMarker,
    WORKER_ROUTING_SAFE_STATUSES,
    WorkerRoutingPending,
    WorkerRoutingTuple,
    has_pending_worker_routing,
    read_pending_worker_routing,
    task_routing_tuple,
    with_pending_worker_routing,
    without_pending_worker_routing,
)
from backend.api.deps import (
    get_current_user_id,
    get_current_user_role,
    is_admin,
    require_admin,
    require_internal_service,
    require_project_access,
    require_task_access,
    require_task_control,
    require_worker_target_access,
)

router = APIRouter(prefix="/api/tasks", tags=["tasks"])
logger = logging.getLogger(__name__)
_MANUAL_RETRYABLE_STATUSES = frozenset({"failed", "cancelled", "conflict", "completed"})
_WORKER_ROUTING_CONFIG_FIELDS = frozenset({"provider", "model", "codex_service_tier"})
_WORKER_SKILL_CONFIG_FIELDS = frozenset(
    {"enabled_skills", "selected_user_skills", "metadata_"}
)
_WORKER_CONFIG_SYNC_UNSAFE_FIELDS = frozenset(
    {"worker_id", "project_id", "target_repo"}
)
_LOCAL_ROUTING_EDITABLE_STATUSES = WORKER_ROUTING_SAFE_STATUSES | {"pending"}
_WORKER_SKILL_EDITABLE_STATUSES = WORKER_ROUTING_SAFE_STATUSES | {"pending"}
_PR_REVIEW_CHAT_TERMINAL_STATUSES = frozenset(
    {"approved", "merged", "commented", "error"}
)


async def _require_no_pr_review_publication(
    db: AsyncSession,
    task_id: int,
) -> None:
    """Fence Task generation changes while its GitHub outbox is publishing."""

    from backend.models.pr_monitor import PRReview, PRReviewerRun

    publishing = await db.execute(
        select(PRReview.id).distinct()
        .outerjoin(
            PRReviewerRun,
            PRReviewerRun.pr_review_id == PRReview.id,
        )
        .where(
            or_(
                PRReview.task_id == task_id,
                PRReviewerRun.task_id == task_id,
            ),
            PRReview.status.in_(("publishing", "superseding")),
        )
    )
    if publishing.scalar_one_or_none() is not None:
        raise HTTPException(
            409,
            "PR review publication/synchronization is in progress; this Task "
            "generation is frozen",
        )


async def _require_not_pr_review_task_mutation(
    db: AsyncSession,
    task_id: int,
    *,
    action: str,
) -> None:
    """Keep automated review Tasks immutable outside their backend workflow."""

    from backend.models.pr_monitor import PRReview

    linked = await db.execute(
        select(PRReview.id)
        .where(PRReview.task_id == task_id)
        .limit(1)
    )
    task = await db.get(Task, task_id)
    if (
        linked.scalar_one_or_none() is not None
        or (task is not None and is_pr_sandbox_task(task))
    ):
        raise HTTPException(
            409,
            f"Automated PR workflow Tasks cannot be manually {action}; wait "
            "for the workflow outcome or a new PR snapshot",
        )


async def _require_pr_review_chat_allowed(
    db: AsyncSession,
    task_id: int,
    *,
    trusted_unlinked_terminal: bool = False,
) -> bool:
    """Allow discussion only after the automated review is durably terminal.

    ``reviewing`` must remain immutable until its exact completed Task
    generation has been claimed by the GitHub publication outbox.  A chat turn
    admitted in that window could otherwise replace the generation before the
    completion consumer verifies it.  Once publication is terminal, later
    turns cannot change the already-recorded GitHub action and are safe.

    Worker mirrors deliberately retain only the ``pr-review`` tag, not the
    Manager's PRReview row.  ``trusted_unlinked_terminal`` is therefore
    reserved for an internally authenticated Manager -> Worker request whose
    Manager-side terminal check ran while holding the Task operation lock.

    Returns ``True`` for a PR review Task and ``False`` for an ordinary Task.
    """

    from backend.models.pr_monitor import PRReview, PRReviewerRun

    task = await db.get(Task, task_id)
    if task is None:
        return False
    if is_pr_review_fix_task(task):
        raise HTTPException(
            409,
            "Automated PR fix Tasks cannot accept manual discussion or live "
            "injection; wait for the generated patch outcome",
        )
    metadata = task.metadata_ or {}
    tags = task.tags
    tag_marker = (
        isinstance(tags, (list, tuple, set, dict))
        and "pr-review" in tags
    )
    task_marker = is_pr_review_task(task)

    def allow_terminal_discussion() -> bool:
        if task.provider == "codex":
            # Automated Codex reviews run in a tool-free isolated thread.
            # That transport intentionally refuses native resume, so a
            # terminal follow-up would otherwise open a context-less thread
            # containing only the user's new message.
            raise HTTPException(
                409,
                "Terminal discussion is unavailable for isolated Codex PR "
                "review Tasks; start a separate Task with the review context",
            )
        return True

    linked = list((await db.execute(
        select(PRReview.status).distinct()
        .outerjoin(
            PRReviewerRun,
            PRReviewerRun.pr_review_id == PRReview.id,
        )
        .where(
            or_(
                PRReview.task_id == task_id,
                PRReviewerRun.task_id == task_id,
            )
        )
    )).scalars().all())
    if linked:
        # One Task belongs to exactly one immutable review snapshot.  Multiple
        # links or an unknown state indicate corrupt/partially migrated state
        # and must fail closed rather than guessing which review is current.
        if (
            len(linked) == 1
            and linked[0] in _PR_REVIEW_CHAT_TERMINAL_STATUSES
            and metadata.get("pr_review_superseded") is not True
        ):
            return allow_terminal_discussion()
        raise HTTPException(
            409,
            "Automated PR review discussion is available only after its "
            "GitHub review workflow is terminal",
        )

    if not task_marker:
        return False
    if (
        trusted_unlinked_terminal
        and tag_marker
        and metadata.get("pr_review_superseded") is not True
    ):
        return allow_terminal_discussion()
    raise HTTPException(
        409,
        "Automated PR review Task has no locally verified terminal review "
        "state",
    )


async def _require_pr_review_retryable(
    db: AsyncSession,
    task_id: int,
) -> None:
    """Do not run a Task generation whose linked review is already terminal."""

    from backend.models.pr_monitor import PRReview, PRReviewerRun

    result = await db.execute(
        select(PRReview.status).distinct()
        .outerjoin(
            PRReviewerRun,
            PRReviewerRun.pr_review_id == PRReview.id,
        )
        .where(
            or_(
                PRReview.task_id == task_id,
                PRReviewerRun.task_id == task_id,
            )
        )
        .limit(1)
    )
    review_status = result.scalar_one_or_none()
    task = await db.get(Task, task_id)
    task_marker = bool(task is not None and is_pr_sandbox_task(task))
    if review_status is not None:
        if review_status in {"pending", "waiting_ci", "reviewing"}:
            detail = (
                "Automated PR review Tasks cannot be manually retried; push a "
                "new PR snapshot instead"
            )
        else:
            detail = (
                "This PR review is already terminal; wait for a new PR snapshot "
                "instead of retrying its old Task"
            )
        raise HTTPException(
            409,
            detail,
        )
    if task_marker:
        raise HTTPException(
            409,
            "Automated PR workflow Tasks cannot be manually retried; wait for "
            "the workflow outcome or push a new PR snapshot instead",
        )


class _WorkerRoutingConfirmationUnavailable(HTTPException):
    """Worker ack/reconcile outcome could not be read after Manager commit."""

    def __init__(self):
        super().__init__(
            503,
            "Worker routing synchronization outcome could not be confirmed",
        )


def _require_expected_task_routing(
    task: Task,
    expected: TaskRoutingExpectation | None,
    *,
    effective_model: str | None,
) -> tuple[str, str | None, str]:
    """Reject a user action issued from a stale routing view."""

    actual = (
        (task.provider or "claude").lower(),
        effective_model,
        task.codex_service_tier or "default",
    )
    if expected is None:
        return actual
    requested = (
        expected.provider.lower(),
        expected.model,
        expected.codex_service_tier,
    )
    if requested != actual:
        raise HTTPException(
            409,
            "Task execution configuration changed since this page was "
            "loaded; refresh before starting another turn",
        )
    return actual


def _explicit_command_skills(message: str | None) -> dict[str, bool]:
    """Return the temporary Skills requested by one leading $command."""

    from backend.services.command_registry import parse_command

    command, _command_args = parse_command(message or "")
    return dict(command.required_skills or {}) if command else {}


async def _validate_skill_configuration(
    db: AsyncSession,
    *,
    provider: str | None,
    enabled_skills: dict | None,
    selected_user_skills: list[int] | None,
    user_skill_snapshots: list[dict] | None = None,
    worker_id: int | None = None,
    shared_from_id: int | None = None,
    metadata: dict | None = None,
) -> list[int] | None:
    """Validate and normalize task-scoped Skill selections."""

    from backend.config import settings as app_settings
    from backend.models.user_skill import UserSkill
    from backend.services.skill_context import (
        codex_monitor_supported_for_scope,
        normalize_user_skill_ids,
        skill_supported,
        user_skill_snapshot_from_mapping,
    )

    provider = (provider or "claude").lower()
    codex_monitor_enabled = codex_monitor_supported_for_scope(
        provider=provider,
        worker_id=worker_id,
        shared_from_id=shared_from_id,
        metadata=metadata,
        codex_main_mcp_enabled=app_settings.codex_main_mcp_enabled,
    )
    unsupported = sorted(
        name
        for name, enabled in (enabled_skills or {}).items()
        if enabled
        and not skill_supported(
            provider,
            name,
            codex_monitor_enabled=codex_monitor_enabled,
        )
    )
    if unsupported:
        raise HTTPException(
            400,
            "Provider "
            f"{(provider or 'claude').lower()} does not support Skills: "
            + ", ".join(unsupported),
        )

    normalized = normalize_user_skill_ids(selected_user_skills)
    unavailable_without_main_mcp = sorted(
        name
        for name, enabled in (enabled_skills or {}).items()
        if enabled and name != "sub-agent"
    )
    if (
        provider == "codex"
        and not app_settings.codex_main_mcp_enabled
        and (unavailable_without_main_mcp or normalized)
    ):
        raise HTTPException(
            400,
            "Codex main-task MCP is disabled; only Sub-Agent can be enabled",
        )
    if not normalized:
        return [] if selected_user_skills is not None else None
    found = set()
    for value in user_skill_snapshots or []:
        if not isinstance(value, dict):
            continue
        snapshot = user_skill_snapshot_from_mapping(value)
        if snapshot is not None:
            found.add(snapshot.id)
    if user_skill_snapshots is None:
        found.update(
            (await db.execute(select(UserSkill.id).where(UserSkill.id.in_(normalized))))
            .scalars()
            .all()
        )
    missing = [skill_id for skill_id in normalized if skill_id not in found]
    if missing:
        raise HTTPException(
            400,
            "Selected User Skills do not exist: "
            + ", ".join(str(skill_id) for skill_id in missing),
        )
    return normalized


def _find_session_jsonl(session_id: str, provider: str = "claude") -> Path | None:
    """Locate a provider session JSONL on disk.

    Codex stores rollouts under ``$CODEX_HOME/sessions/YYYY/MM/DD``.  This
    branch must run before the Claude pool lookup: treating a valid Codex
    rollout as a missing Claude session makes every follow-up abandon native
    history/cache and start a new thread.

    Pool deployments split sessions across multiple ~/.claude-account-N dirs,
    so a lookup that only checks ~/.claude / CLAUDE_CONFIG_DIR (and only the
    exact last_cwd-encoded project subdir) misses sessions created under a pool
    account and silently degrades recovery to a lossy summary (prod task #725).
    We reuse the pool's own locator (searches every account dir) and glob across
    all project subdirs so cwd-encoding differences don't hide the file either.
    """
    if (provider or "claude").lower() == "codex":
        homes_to_check: list[Path] = []

        # Pool account homes are the primary source of truth in multi-account
        # deployments.  Include disabled/cooling accounts too: their rollout
        # history remains valid even when the credentials cannot run a turn.
        try:
            from backend.main import codex_pool

            if codex_pool:
                for account in codex_pool.list_accounts():
                    codex_home = account.get("codex_home")
                    if codex_home:
                        homes_to_check.append(Path(codex_home).expanduser())
        except Exception:
            pass

        env_home = os.environ.get("CODEX_HOME")
        if env_home:
            homes_to_check.append(Path(env_home).expanduser())
        homes_to_check.append(Path.home() / ".codex")

        # Disk fallback covers removed pool entries and legacy account naming
        # such as ~/.codex-account-2.  A missing sessions/ child is harmless.
        try:
            homes_to_check.extend(
                path for path in sorted(Path.home().glob(".codex*")) if path.is_dir()
            )
        except OSError:
            pass

        seen: set[str] = set()
        for codex_home in homes_to_check:
            key = os.path.abspath(str(codex_home))
            if key in seen:
                continue
            seen.add(key)
            try:
                match = next(
                    (
                        path
                        for path in codex_home.glob(
                            f"sessions/*/*/*/rollout-*-{session_id}.jsonl"
                        )
                        if path.is_file()
                    ),
                    None,
                )
                if match:
                    return match
            except OSError:
                continue
        return None

    config_dir: str | None = None
    try:
        from backend.main import dispatcher

        if dispatcher and dispatcher.pool:
            config_dir = dispatcher.pool.locate_session_config_dir(session_id)
    except Exception:
        config_dir = None
    # Try pool locator result first, then env CLAUDE_CONFIG_DIR, then default
    dirs_to_check = []
    if config_dir:
        dirs_to_check.append(config_dir)
    env_dir = os.environ.get("CLAUDE_CONFIG_DIR")
    if env_dir and env_dir not in dirs_to_check:
        dirs_to_check.append(env_dir)
    default_dir = os.path.expanduser("~/.claude")
    if default_dir not in dirs_to_check:
        dirs_to_check.append(default_dir)
    for d in dirs_to_check:
        try:
            match = next(Path(d).glob(f"projects/*/{session_id}.jsonl"), None)
            if match:
                return match
        except OSError:
            pass
    # Fallback: scan all ~/.claude* dirs on disk. Covers accounts that were
    # removed from the pool but whose config dirs still exist on disk.
    home = Path.home()
    try:
        for d in sorted(home.iterdir()):
            if not d.name.startswith(".claude") or not d.is_dir():
                continue
            try:
                match = next(d.glob(f"projects/*/{session_id}.jsonl"), None)
                if match:
                    return match
            except OSError:
                continue
    except OSError:
        pass
    return None


async def _clone_session(source_task_id: int, db: AsyncSession) -> dict | None:
    """Clone a Claude Code session file from a source task, returning new session_id and last_cwd."""
    source = await db.get(Task, source_task_id)
    if not source or not source.session_id or not source.last_cwd:
        return None

    # A Codex rollout embeds its thread id in both the filename and session
    # metadata.  Copying it under a random filename does not create a valid new
    # thread, so keep this legacy clone operation Claude-only.
    if (source.provider or "claude").lower() != "claude":
        return None

    source_jsonl = _find_session_jsonl(source.session_id, provider="claude")
    if source_jsonl is None:
        return None

    new_session_id = str(uuid.uuid4())
    dest_jsonl = source_jsonl.parent / f"{new_session_id}.jsonl"
    shutil.copy2(source_jsonl, dest_jsonl)

    return {"session_id": new_session_id, "last_cwd": source.last_cwd}


def _get_queue(db: AsyncSession = Depends(get_db)) -> TaskQueue:
    return TaskQueue(db)


@router.get("/count")
async def count_tasks(
    request: Request,
    status: str | None = None,
    include_archived: bool = False,
    archived_only: bool = False,
    project_id: int | None = None,
    starred: bool | None = None,
    has_unread: bool | None = None,
    task_kind: Literal["standalone_plan", "related_plan", "main"] | None = None,
    queue: TaskQueue = Depends(_get_queue),
):
    user_id = get_current_user_id(request)
    user_role = get_current_user_role(request)
    total = await queue.count_tasks(
        status=status,
        include_archived=include_archived,
        archived_only=archived_only,
        project_id=project_id,
        starred=starred,
        has_unread=has_unread,
        task_kind=task_kind,
        user_id=user_id if user_role not in ("admin", "super_admin") else None,
    )
    return {"total": total}


@router.get("", response_model=list[TaskResponse])
async def list_tasks(
    request: Request,
    status: str | None = None,
    include_archived: bool = False,
    archived_only: bool = False,
    project_id: int | None = None,
    starred: bool | None = None,
    has_unread: bool | None = None,
    task_kind: Literal["standalone_plan", "related_plan", "main"] | None = None,
    limit: int = 50,
    offset: int = 0,
    queue: TaskQueue = Depends(_get_queue),
):
    user_id = get_current_user_id(request)
    user_role = get_current_user_role(request)
    return await queue.list_tasks(
        status=status,
        include_archived=include_archived,
        archived_only=archived_only,
        project_id=project_id,
        starred=starred,
        has_unread=has_unread,
        task_kind=task_kind,
        limit=limit,
        offset=offset,
        user_id=user_id if user_role not in ("admin", "super_admin") else None,
    )


@router.post("", response_model=TaskResponse, status_code=201)
async def create_task(
    request: Request,
    body: TaskCreate,
    queue: TaskQueue = Depends(_get_queue),
    db: AsyncSession = Depends(get_db),
):
    user_id = get_current_user_id(request)
    if body.mode == "plan":
        raise HTTPException(
            410,
            "Legacy mode=plan Task creation is closed; use POST /api/plans",
        )
    if body.secret_ids:
        require_admin(request)
    data = body.model_dump()
    data["created_by"] = user_id
    supersedes: Task | None = None
    if data.get("mode") == "plan":
        from backend.schemas.plan import resolve_plan_pipeline_config
        from backend.services.plan_pipeline_settings import (
            effective_plan_pipeline_config,
        )

        base_pipeline = await effective_plan_pipeline_config(db)

        pipeline = resolve_plan_pipeline_config(
            data.get("plan_pipeline_config"),
            base_config=base_pipeline,
            legacy_provider=(
                data.get("provider") if "provider" in body.model_fields_set else None
            ),
            legacy_model=(
                data.get("model") if "model" in body.model_fields_set else None
            ),
            legacy_effort=(
                data.get("effort_level")
                if "effort_level" in body.model_fields_set
                else None
            ),
        )
        data["plan_pipeline_config"] = pipeline.model_dump(mode="json")
        data["provider"] = pipeline.planner.primary.provider
        data["model"] = pipeline.planner.primary.model
        data["effort_level"] = pipeline.planner.primary.effort
    elif data.get("plan_pipeline_config") is not None:
        raise HTTPException(
            422,
            "plan_pipeline_config requires mode='plan'",
        )

    # Resolve the exact execution target before persisting anything.  A member
    # owning Worker A must not be able to name Worker B (or the Manager) merely
    # because some Worker exists in their account.
    project = None
    if body.project_id is not None:
        from backend.models.project import Project

        project = await db.get(Project, body.project_id)
        if project is None:
            raise HTTPException(404, "Project not found")
        await require_project_access(request, project.id, db)
        if body.worker_id is not None and body.worker_id != project.worker_id:
            raise HTTPException(
                400,
                "Task Worker must match the selected Project location",
            )
        data["worker_id"] = project.worker_id

    target_worker_id = data.get("worker_id")
    if project is None:
        await require_worker_target_access(request, target_worker_id, db)
    if body.frontend_review is not None and target_worker_id is not None:
        raise HTTPException(
            400,
            "Frontend Review Goal currently requires a Manager-local Project",
        )

    if data.get("id") is None:
        data.pop("id", None)  # 未指定 → 正常自增；指定 → 用 Manager 分配的全局 ID
    image_paths = data.pop("image_paths", None)
    file_paths = data.pop("file_paths", None)
    attachments = data.pop("attachments", None)
    secret_ids = data.pop("secret_ids", None)
    clone_from_task_id = data.pop("clone_from_task_id", None)
    frontend_review = data.pop("frontend_review", None)
    user_skill_snapshots = data.pop("user_skill_snapshots", None)
    if user_skill_snapshots is not None:
        require_admin(request)
    meta = data.get("metadata_") or {}
    all_paths = file_paths or image_paths
    if all_paths:
        meta["image_paths"] = all_paths
    if attachments:
        meta["attachments"] = attachments
    if secret_ids:
        meta["secret_ids"] = secret_ids
    if frontend_review is not None:
        from backend.services.frontend_review_goal import (
            FRONTEND_REVIEW_METADATA_KEY,
            build_frontend_review_goal_condition,
            frontend_review_goal_config,
        )

        normalized_frontend_review = frontend_review_goal_config({
            FRONTEND_REVIEW_METADATA_KEY: frontend_review,
        })
        if normalized_frontend_review is None:  # defensive: schema validates this
            raise HTTPException(422, "Invalid Frontend Review Goal configuration")
        meta[FRONTEND_REVIEW_METADATA_KEY] = normalized_frontend_review
        data["mode"] = "goal"
        data["goal_max_turns"] = normalized_frontend_review["max_iterations"]
        data["goal_condition"] = build_frontend_review_goal_condition(
            data.get("goal_condition")
        )
    if user_skill_snapshots is not None:
        from backend.services.skill_context import (
            USER_SKILL_SNAPSHOTS_METADATA_KEY,
            WORKER_MANAGED_TASK_METADATA_KEY,
        )

        meta[USER_SKILL_SNAPSHOTS_METADATA_KEY] = user_skill_snapshots
        meta[WORKER_MANAGED_TASK_METADATA_KEY] = True
    if meta:
        data["metadata_"] = meta

    if data.get("plan_target_task_id") is not None:
        if data.get("mode") != "plan":
            raise HTTPException(422, "plan_target_task_id requires mode='plan'")
        target = await db.get(Task, data["plan_target_task_id"])
        if target is None:
            raise HTTPException(404, "Plan target Task not found")
        await require_task_control(request, target, db)
        if not target.session_id:
            raise HTTPException(
                400,
                "Run the target Task before creating a session Plan",
            )
        if target.shared_from_id is not None:
            raise HTTPException(
                409,
                "Shared shadow tasks cannot own Plan Tasks",
            )
        if (
            data.get("worker_id") != target.worker_id
            or data.get("project_id") != target.project_id
        ):
            raise HTTPException(
                422,
                "Related Plan must use the target Task's Project and Worker",
            )
        from backend.services.plan_tasks import (
            ACTIVE_PLAN_STATUSES,
            MAX_ACTIVE_PLANS_PER_TASK,
        )

        active_count = await db.scalar(
            select(func.count(Task.id)).where(
                Task.plan_target_task_id == target.id,
                Task.mode == "plan",
                Task.status.in_(ACTIVE_PLAN_STATUSES),
            )
        )
        if int(active_count or 0) >= MAX_ACTIVE_PLANS_PER_TASK:
            raise HTTPException(
                429,
                f"Task already has {MAX_ACTIVE_PLANS_PER_TASK} active Plans",
            )
        supersedes_id = data.get("supersedes_plan_task_id")
        if supersedes_id is not None:
            supersedes = await db.get(Task, supersedes_id)
            if (
                supersedes is None
                or supersedes.mode != "plan"
                or supersedes.plan_target_task_id != target.id
            ):
                raise HTTPException(
                    400,
                    "Superseded Plan does not belong to this Task",
                )
            await require_task_control(request, supersedes, db)
            if supersedes.status != "plan_review":
                raise HTTPException(
                    409,
                    "Only a Plan awaiting review can be superseded",
                )
        # The execution node owns its LogEntry ids and repository. Re-capture
        # the same target boundary locally instead of trusting a Manager-side
        # watermark whose integer id has no cross-database meaning.
        from backend.services.plan_tasks import capture_task_context, latest_task_log_id

        local_context_log_id = await latest_task_log_id(db, target.id)
        data["plan_context_session_id"] = target.session_id
        data["plan_context_log_id"] = local_context_log_id
        data["plan_context_snapshot"] = await capture_task_context(
            db,
            target.id,
            through_log_id=local_context_log_id,
            max_chars=settings.plan_transcript_max_chars,
        )
        from backend.services.plan_tasks import capture_repo_revision

        data["plan_repo_revision"] = await capture_repo_revision(
            target.last_cwd or target.target_repo
        )
    elif data.get("mode") != "plan":
        for plan_only_field in (
            "plan_context_session_id",
            "plan_context_log_id",
            "plan_context_snapshot",
            "plan_repo_revision",
            "supersedes_plan_task_id",
        ):
            if data.get(plan_only_field) is not None:
                raise HTTPException(
                    422,
                    f"{plan_only_field} requires mode='plan'",
                )
    elif data.get("supersedes_plan_task_id") is not None:
        supersedes = await db.get(Task, data["supersedes_plan_task_id"])
        if (
            supersedes is None
            or supersedes.mode != "plan"
            or supersedes.plan_target_task_id is not None
        ):
            raise HTTPException(
                400,
                "Standalone Plan can only supersede another standalone Plan",
            )
        await require_task_control(request, supersedes, db)
        if supersedes.status != "plan_review":
            raise HTTPException(
                409,
                "Only a Plan awaiting review can be superseded",
            )

    if clone_from_task_id:
        source = await db.get(Task, clone_from_task_id)
        if source is None:
            raise HTTPException(404, "Clone source task not found")
        await require_task_control(request, source, db)
        if "attention_tag" not in body.model_fields_set:
            data["attention_tag"] = source.attention_tag
        cloned = await _clone_session(clone_from_task_id, db)
        if cloned:
            data["session_id"] = cloned["session_id"]
            data["last_cwd"] = cloned["last_cwd"]

    if data.get("mode") == "plan" and data.get("plan_repo_revision") is None:
        from backend.services.plan_tasks import capture_repo_revision

        if data.get("worker_id") is None:
            data["plan_repo_revision"] = await capture_repo_revision(
                data.get("last_cwd") or data.get("target_repo")
            )
    validation_skills = dict(data.get("enabled_skills") or {})
    validation_skills.update(_explicit_command_skills(data.get("description")))
    data["selected_user_skills"] = await _validate_skill_configuration(
        db,
        provider=data.get("provider"),
        enabled_skills=validation_skills,
        selected_user_skills=data.get("selected_user_skills"),
        user_skill_snapshots=user_skill_snapshots,
        worker_id=data.get("worker_id"),
        shared_from_id=data.get("shared_from_id"),
        metadata=data.get("metadata_"),
    )
    try:
        validate_task_service_tier_configuration(
            provider=data.get("provider"),
            model=data.get("model"),
            codex_service_tier=data.get("codex_service_tier"),
            mode=data.get("mode"),
            goal_evaluator_model=data.get("goal_evaluator_model"),
        )
        if data.get("mode") == "plan":
            from backend.schemas.plan import PlanPipelineConfig

            pipeline = PlanPipelineConfig.model_validate(data["plan_pipeline_config"])
            for route in (
                pipeline.planner.primary,
                pipeline.planner.fallback,
                pipeline.reviewer.primary,
                pipeline.reviewer.fallback,
            ):
                validate_task_service_tier_configuration(
                    provider=route.provider,
                    model=route.model,
                    codex_service_tier="default",
                    mode="plan",
                    goal_evaluator_model=None,
                )
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc

    if supersedes is None:
        task = await queue.create(**data)
    else:
        superseded_id = supersedes.id
        metadata = dict(data.get("metadata_") or {})
        metadata["revised_from_plan_task_id"] = superseded_id
        data["metadata_"] = metadata
        task = await stage_task_record(db, **data)
        from backend.services.plan_tasks import mark_plan_superseded

        if not await mark_plan_superseded(
            db,
            supersedes,
            successor_id=task.id,
        ):
            await db.rollback()
            raise HTTPException(
                409,
                "Plan changed while its revision was being created",
            )
        await db.commit()
        await db.refresh(task)
        from backend.services.task_events import broadcast_status_change

        await broadcast_status_change(superseded_id, "superseded")
    # Eliminate the dispatcher's historical 0-2s polling delay.  Importing
    # here avoids a module cycle during application construction.
    try:
        from backend.main import dispatcher

        if dispatcher:
            dispatcher.wake()
    except Exception:
        pass

    # Auto-share if project has active project-level shares
    if task.project_id:
        try:
            from backend.services.task_sharing import auto_share_new_task

            await auto_share_new_task(db, task.id, task.project_id)
        except Exception:
            pass  # best-effort

    return task


@router.post("/migration-import", response_model=TaskResponse, status_code=201)
async def import_migrated_task(
    request: Request,
    body: TaskMigrationImport,
    queue: TaskQueue = Depends(_get_queue),
    db: AsyncSession = Depends(get_db),
):
    """Create or refresh an inert task copied from a Manager.

    A normal task create commits ``pending`` and immediately wakes the local
    dispatcher.  Task migration used to call that endpoint and cancel in a
    second request, leaving a real window where the destination Worker could
    claim and execute the imported task.  This admin-only endpoint persists
    only a non-dispatchable source status in the same transaction and never
    wakes the dispatcher.

    Existing inactive copies are refreshed with a status CAS.  If a legacy
    copy has already become active, fail closed instead of cancelling work
    which may really be running.
    """
    require_admin(request)

    data = body.model_dump()
    source_status = data.pop("source_status")
    user_skill_snapshots = data.pop("user_skill_snapshots", None)
    frontend_review = data.pop("frontend_review", None)
    for transient_field in (
        "image_paths",
        "file_paths",
        "attachments",
        "secret_ids",
        "clone_from_task_id",
    ):
        data.pop(transient_field, None)
    from backend.services.skill_context import (
        USER_SKILL_SNAPSHOTS_METADATA_KEY,
        WORKER_MANAGED_TASK_METADATA_KEY,
    )

    migration_metadata = {
        WORKER_MANAGED_TASK_METADATA_KEY: True,
    }
    if user_skill_snapshots is not None:
        migration_metadata[USER_SKILL_SNAPSHOTS_METADATA_KEY] = user_skill_snapshots
    if frontend_review is not None:
        from backend.services.frontend_review_goal import (
            FRONTEND_REVIEW_METADATA_KEY,
            build_frontend_review_goal_condition,
            frontend_review_goal_config,
        )

        normalized_frontend_review = frontend_review_goal_config({
            FRONTEND_REVIEW_METADATA_KEY: frontend_review,
        })
        if normalized_frontend_review is not None:
            migration_metadata[FRONTEND_REVIEW_METADATA_KEY] = (
                normalized_frontend_review
            )
            data["mode"] = "goal"
            data["goal_max_turns"] = normalized_frontend_review["max_iterations"]
            data["goal_condition"] = build_frontend_review_goal_condition(
                data.get("goal_condition")
            )
    data["metadata_"] = migration_metadata
    data.update(
        worker_id=None,
        status=source_status,
        created_by=get_current_user_id(request),
    )

    data = prepare_task_create_values(data)
    validation_skills = dict(data.get("enabled_skills") or {})
    validation_skills.update(_explicit_command_skills(data.get("description")))
    data["selected_user_skills"] = await _validate_skill_configuration(
        db,
        provider=data.get("provider"),
        enabled_skills=validation_skills,
        selected_user_skills=data.get("selected_user_skills"),
        user_skill_snapshots=user_skill_snapshots,
        worker_id=None,
        shared_from_id=None,
        metadata=data.get("metadata_"),
    )
    try:
        validate_task_service_tier_configuration(
            provider=data.get("provider"),
            model=data.get("model"),
            codex_service_tier=data.get("codex_service_tier"),
            mode=data.get("mode"),
            goal_evaluator_model=data.get("goal_evaluator_model"),
        )
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc

    existing = await db.get(Task, body.id)
    if existing is None:
        # The first visible state is already inert.  In particular there is no
        # pending commit and no dispatcher.wake() between create and cancel.
        return await queue.create(**data)

    old_status = existing.status
    if old_status in ("in_progress", "executing", "migrating"):
        raise HTTPException(
            409,
            f"Destination task {body.id} is active ({old_status})",
        )

    values = {key: value for key, value in data.items() if key != "id"}
    result = await db.execute(
        sa_update(Task)
        .where(*_task_generation_fence(body.id, existing))
        .values(**values)
    )
    if result.rowcount != 1:
        await db.rollback()
        raise HTTPException(409, "Destination task changed during migration import")
    await db.commit()
    db.expire_all()
    task = await db.get(Task, body.id)
    if task is None:  # defensive: a concurrent delete must not look successful
        raise HTTPException(409, "Destination task disappeared during migration import")
    if old_status != source_status:
        from backend.services.task_events import broadcast_status_change

        await broadcast_status_change(task.id, source_status)
    return task


@router.get("/{task_id}", response_model=TaskResponse)
async def get_task(
    task_id: int,
    request: Request,
    queue: TaskQueue = Depends(_get_queue),
    db: AsyncSession = Depends(get_db),
):
    task = await queue.get(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    await require_task_access(request, task, db)
    return task


def _normalized_task_update_values(updates: dict) -> dict:
    """Mirror TaskQueue's explicit-NULL handling for one fenced UPDATE."""

    normalized = {}
    for key, value in updates.items():
        if value is None:
            mapped_attr = getattr(Task, key, None)
            columns = getattr(
                getattr(mapped_attr, "property", None),
                "columns",
                (),
            )
            if not columns or not columns[0].nullable:
                continue
        normalized[key] = value
    return normalized


def _routing_request_tuple(
    body: WorkerRoutingConfigRequest,
) -> WorkerRoutingTuple:
    return WorkerRoutingTuple(
        provider=body.provider,
        model=body.model,
        codex_service_tier=body.codex_service_tier,
    )


def _codex_account_binding(task: Task) -> object | None:
    metadata = task.metadata_
    if not isinstance(metadata, dict):
        return None
    return metadata.get("codex_account_id")


def _resolve_codex_thread_routing_home(task: Task) -> str:
    """Resolve one thread home without guessing between rollout copies."""

    from backend.main import codex_pool
    from backend.services.codex_app_server import normalize_codex_home

    binding = _codex_account_binding(task)
    if codex_pool is not None and binding is not None:
        bound_home = codex_pool.home_for_account(str(binding))
        if not bound_home:
            raise HTTPException(
                409,
                "Codex routing change was blocked because the Task's bound "
                "account home no longer exists",
            )
        return codex_pool.canonical_home(bound_home)

    if codex_pool is not None and task.session_id:
        try:
            matches = codex_pool.locate_session_homes(task.session_id)
        except Exception as exc:
            raise HTTPException(
                409,
                "Codex routing change was blocked because the native thread "
                "home could not be resolved",
            ) from exc
        if len(matches) > 1:
            raise HTTPException(
                409,
                "Codex routing change was blocked because the native thread "
                "exists in multiple account homes without an authoritative "
                "Task binding",
            )
        if len(matches) == 1:
            return codex_pool.canonical_home(matches[0])
        if getattr(codex_pool, "enabled", False):
            raise HTTPException(
                409,
                "Codex routing change was blocked because the native thread "
                "has no authoritative account home",
            )

    return normalize_codex_home(None)


async def _hold_codex_thread_routing_quiescence(
    stack: AsyncExitStack,
    task: Task,
    candidate: WorkerRoutingTuple,
) -> None:
    """Reserve one idle native thread through the caller's routing commit."""

    if (
        not task.session_id
        or (task.provider or "").lower() != "codex"
        or task_routing_tuple(task) == candidate
    ):
        return
    codex_home = _resolve_codex_thread_routing_home(task)
    from backend.main import instance_manager

    try:
        await stack.enter_async_context(
            instance_manager.codex_thread_routing_guard(
                codex_home,
                task.session_id,
            )
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.warning(
            "Codex routing change could not prove thread quiescence: "
            "task=%s session=%s home=%s error=%s",
            task.id,
            task.session_id,
            codex_home,
            type(exc).__name__,
        )
        raise HTTPException(
            409,
            "Codex routing change was blocked because its native thread or "
            "Goal could not be proven idle",
        ) from exc


def _routing_guard_generation_changed(current: Task, observed: Task) -> bool:
    return (
        current.session_id != observed.session_id
        or task_routing_tuple(current) != task_routing_tuple(observed)
        or _codex_account_binding(current) != _codex_account_binding(observed)
    )


def _worker_routing_snapshot(task: Task) -> dict:
    try:
        pending = read_pending_worker_routing(task)
    except InvalidWorkerRoutingMarker as exc:
        raise HTTPException(
            409,
            "Worker Task has an invalid routing synchronization marker",
        ) from exc
    return {
        "id": task.id,
        "status": task.status,
        "worker_id": task.worker_id,
        "shared_from_id": task.shared_from_id,
        **task_routing_tuple(task).as_dict(),
        "pending": pending.as_dict() if pending is not None else None,
    }


async def _lock_worker_local_routing_task(
    task_id: int,
    request: Request,
    db: AsyncSession,
    *,
    safe_status_required: bool,
    allowed_statuses: frozenset[str] | None = None,
) -> Task:
    """Acquire the portable Task write barrier and return its strict snapshot."""

    predicates = [
        Task.id == task_id,
        Task.worker_id.is_(None),
        Task.shared_from_id.is_(None),
        Task.pty_background_generation.is_(None),
    ]
    if allowed_statuses is not None:
        predicates.append(Task.status.in_(allowed_statuses))
    elif safe_status_required:
        predicates.append(Task.status.in_(WORKER_ROUTING_SAFE_STATUSES))
    guarded = await db.execute(
        sa_update(Task).where(*predicates).values(status=Task.status)
    )
    if guarded.rowcount != 1:
        await db.rollback()
        db.expire_all()
        current = await db.get(Task, task_id)
        if current is None:
            raise HTTPException(404, "Task not found")
        await require_task_control(request, current, db)
        if current.worker_id is not None or current.shared_from_id is not None:
            raise HTTPException(
                409,
                "Routing synchronization endpoints only accept Worker-local Tasks",
            )
        if allowed_statuses is not None:
            detail = (
                "Task routing config cannot change after an execution claim "
                "became active"
            )
        else:
            detail = (
                "Worker Task routing config cannot change while it is pending or active"
            )
        raise HTTPException(409, detail)
    current = await db.get(Task, task_id, populate_existing=True)
    if current is None:
        await db.rollback()
        raise HTTPException(404, "Task not found")
    await require_task_control(request, current, db)
    reverse_owner = (
        await db.execute(
            select(Instance.id)
            .where(Instance.current_task_id == task_id)
            .with_for_update()
            .limit(1)
        )
    ).scalar_one_or_none()
    if reverse_owner is not None:
        await db.rollback()
        raise HTTPException(
            409,
            "Task still has an active or unconfirmed Instance generation; "
            "routing configuration cannot change until process cleanup is "
            "complete",
        )
    return current


async def _running_routing_sub_agent_id(
    db: AsyncSession,
    task_id: int,
) -> int | None:
    """Return a child generation that can still emit with the old route."""

    from backend.models.sub_agent import SubAgentSession

    return (
        await db.execute(
            select(SubAgentSession.id)
            .where(
                SubAgentSession.task_id == task_id,
                SubAgentSession.status == "running",
                (
                    (SubAgentSession.agent_type == "sub_agent")
                    & (SubAgentSession.source == "ccm")
                )
                | (SubAgentSession.source == "native"),
            )
            .limit(1)
        )
    ).scalar_one_or_none()


@router.post(
    "/{task_id}/routing-config/stage",
    response_model=WorkerRoutingConfigSnapshot,
    include_in_schema=False,
)
async def stage_worker_routing_config(
    task_id: int,
    body: WorkerRoutingConfigRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Durably block launches and record a candidate without changing live config."""

    require_admin(request)
    task = await db.get(Task, task_id)
    if task is not None:
        await require_task_control(request, task, db)
    await db.rollback()
    candidate = _routing_request_tuple(body)

    async with get_task_operation_lock(task_id):
        # A terminal status may be published before a pre-owner launch or an
        # existing process generation is fully reaped.  Settle that hidden
        # reservation before taking the durable Task→Instance barrier below.
        db.expire_all()
        observed = await db.get(Task, task_id)
        if observed is None:
            raise HTTPException(404, "Task not found")
        await require_task_control(request, observed, db)
        try:
            validate_task_service_tier_configuration(
                provider=candidate.provider,
                model=candidate.model,
                codex_service_tier=candidate.codex_service_tier,
                mode=observed.mode,
                goal_evaluator_model=observed.goal_evaluator_model,
            )
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        if observed.worker_id is not None or observed.shared_from_id is not None:
            raise HTTPException(
                409,
                "Routing synchronization endpoints only accept Worker-local Tasks",
            )
        if observed.status not in WORKER_ROUTING_SAFE_STATUSES:
            raise HTTPException(
                409,
                "Worker Task routing config cannot change while it is pending "
                "or active",
            )
        if candidate.provider != observed.provider and observed.session_id is not None:
            raise HTTPException(
                409,
                "Task provider cannot change while an existing native session "
                "may still emit output; start a new Task instead",
            )
        observed_instance_id = observed.instance_id
        db.expunge(observed)
        await db.rollback()
        await _settle_task_launch_barrier(task_id, observed_instance_id)

        async with AsyncExitStack() as routing_stack:
            await _hold_codex_thread_routing_quiescence(
                routing_stack,
                observed,
                candidate,
            )
            db.expire_all()
            current = await _lock_worker_local_routing_task(
                task_id,
                request,
                db,
                safe_status_required=True,
            )
            if _routing_guard_generation_changed(current, observed):
                await db.rollback()
                raise HTTPException(
                    409,
                    "Worker Task native session or routing generation changed "
                    "while quiescence was being verified",
                )
            try:
                pending = read_pending_worker_routing(current)
            except InvalidWorkerRoutingMarker as exc:
                await db.rollback()
                raise HTTPException(
                    409,
                    "Worker Task has an invalid routing synchronization marker",
                ) from exc

            # A Codex CCM sub-agent can still be between account resolution and
            # start_turn after its parent task became terminal.  Keep stage
            # behind that exact running child generation; its final launch gate
            # provides the opposite ordering when stage wins first.
            running_sub_agent = await _running_routing_sub_agent_id(
                db,
                task_id,
            )
            if running_sub_agent is not None:
                await db.rollback()
                raise HTTPException(
                    409,
                    "Worker Task routing config cannot be staged while a CCM "
                    "sub-agent is running",
                )

            if (
                candidate.provider != current.provider
                and current.session_id is not None
            ):
                await db.rollback()
                raise HTTPException(
                    409,
                    "Task provider cannot change while an existing native "
                    "session may still emit output; start a new Task instead",
                )

            requested = WorkerRoutingPending(body.op_id, candidate)
            if pending is not None:
                if pending != requested:
                    await db.rollback()
                    raise HTTPException(
                        409,
                        "Worker Task already has a different routing "
                        "synchronization operation pending",
                    )
                snapshot = _worker_routing_snapshot(current)
                await db.rollback()
                return snapshot

            current.metadata_ = with_pending_worker_routing(
                current.metadata_,
                requested,
            )
            await db.commit()
            snapshot = _worker_routing_snapshot(current)
            await db.rollback()
            return snapshot


@router.get(
    "/{task_id}/routing-config/status",
    response_model=WorkerRoutingConfigSnapshot,
    include_in_schema=False,
)
async def read_worker_routing_config(
    task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Return the live tuple and durable pending candidate for convergence."""

    require_admin(request)
    async with get_task_operation_lock(task_id):
        task = await db.get(Task, task_id)
        if task is None:
            raise HTTPException(404, "Task not found")
        await require_task_control(request, task, db)
        if task.worker_id is not None or task.shared_from_id is not None:
            raise HTTPException(
                409,
                "Routing synchronization endpoints only accept Worker-local Tasks",
            )
        return _worker_routing_snapshot(task)


@router.post(
    "/{task_id}/routing-config/ack",
    response_model=WorkerRoutingConfigSnapshot,
    include_in_schema=False,
)
async def ack_worker_routing_config(
    task_id: int,
    body: WorkerRoutingConfigRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Atomically promote the staged candidate and clear its launch fence."""

    require_admin(request)
    task = await db.get(Task, task_id)
    if task is not None:
        await require_task_control(request, task, db)
    await db.rollback()
    candidate = _routing_request_tuple(body)
    async with get_task_operation_lock(task_id):
        db.expire_all()
        current = await _lock_worker_local_routing_task(
            task_id,
            request,
            db,
            safe_status_required=True,
        )
        try:
            pending = read_pending_worker_routing(current)
        except InvalidWorkerRoutingMarker as exc:
            await db.rollback()
            raise HTTPException(
                409,
                "Worker Task has an invalid routing synchronization marker",
            ) from exc
        requested = WorkerRoutingPending(body.op_id, candidate)
        if pending is None:
            if task_routing_tuple(current) != candidate:
                await db.rollback()
                raise HTTPException(
                    409,
                    "Worker routing ack has no matching pending or applied tuple",
                )
            snapshot = _worker_routing_snapshot(current)
            await db.rollback()
            return snapshot
        if pending != requested:
            await db.rollback()
            raise HTTPException(
                409,
                "Worker routing ack does not match the pending operation",
            )

        current.provider = candidate.provider
        current.model = candidate.model
        current.codex_service_tier = candidate.codex_service_tier
        current.metadata_ = without_pending_worker_routing(current.metadata_)
        await db.commit()
        return _worker_routing_snapshot(current)


@router.post(
    "/{task_id}/routing-config/reconcile",
    response_model=WorkerRoutingConfigSnapshot,
    include_in_schema=False,
)
async def reconcile_worker_routing_config(
    task_id: int,
    body: WorkerRoutingConfigRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Abort an orphan stage by restoring the Manager-authoritative live tuple."""

    require_admin(request)
    task = await db.get(Task, task_id)
    if task is not None:
        await require_task_control(request, task, db)
    await db.rollback()
    authoritative = _routing_request_tuple(body)

    async with get_task_operation_lock(task_id):
        db.expire_all()
        current = await _lock_worker_local_routing_task(
            task_id,
            request,
            db,
            safe_status_required=True,
        )
        try:
            validate_task_service_tier_configuration(
                provider=authoritative.provider,
                model=authoritative.model,
                codex_service_tier=authoritative.codex_service_tier,
                mode=current.mode,
                goal_evaluator_model=current.goal_evaluator_model,
            )
        except ValueError as exc:
            await db.rollback()
            raise HTTPException(422, str(exc)) from exc
        try:
            pending = read_pending_worker_routing(current)
        except InvalidWorkerRoutingMarker as exc:
            await db.rollback()
            raise HTTPException(
                409,
                "Worker Task has an invalid routing synchronization marker",
            ) from exc
        if pending is None:
            if task_routing_tuple(current) != authoritative:
                await db.rollback()
                raise HTTPException(
                    409,
                    "Worker routing differs from Manager without a pending operation",
                )
            snapshot = _worker_routing_snapshot(current)
            await db.rollback()
            return snapshot
        if pending.op_id != body.op_id:
            await db.rollback()
            raise HTTPException(
                409,
                "Worker routing reconcile does not match the pending operation",
            )

        current.provider = authoritative.provider
        current.model = authoritative.model
        current.codex_service_tier = authoritative.codex_service_tier
        current.metadata_ = without_pending_worker_routing(current.metadata_)
        await db.commit()
        return _worker_routing_snapshot(current)


def _validate_remote_worker_routing_snapshot(
    value,
    *,
    task_id: int,
) -> WorkerRoutingConfigSnapshot:
    try:
        snapshot = WorkerRoutingConfigSnapshot.model_validate(value)
    except Exception as exc:
        raise HTTPException(
            502,
            "Worker returned an invalid routing synchronization snapshot",
        ) from exc
    if snapshot.id != task_id:
        raise HTTPException(
            502,
            "Worker returned a routing snapshot for a different Task",
        )
    return snapshot


def _snapshot_routing_tuple(
    snapshot: WorkerRoutingConfigSnapshot,
) -> WorkerRoutingTuple:
    return WorkerRoutingTuple(
        provider=snapshot.provider,
        model=snapshot.model,
        codex_service_tier=snapshot.codex_service_tier,
    )


async def _read_remote_worker_routing(
    task: Task,
    *,
    operation_lock_held: bool,
    surface_endpoint_not_found: bool = False,
) -> WorkerRoutingConfigSnapshot:
    result = await _proxy(
        task,
        "GET",
        f"/api/tasks/{task.id}/routing-config/status",
        require_json=True,
        surface_endpoint_not_found=surface_endpoint_not_found,
        operation_lock_held=operation_lock_held,
    )
    return _validate_remote_worker_routing_snapshot(result, task_id=task.id)


def _validate_legacy_worker_routing_snapshot(
    value,
    *,
    task: Task,
) -> WorkerRoutingConfigSnapshot:
    """Validate the ordinary Task response used by a pre-routing-protocol Worker."""

    if not isinstance(value, dict):
        raise HTTPException(
            502,
            "Legacy Worker returned an invalid Task routing confirmation",
        )
    required = {"id", "status", "provider", "model"}
    if not required.issubset(value):
        raise HTTPException(
            502,
            "Legacy Worker Task response omitted required routing fields",
        )
    if value["id"] != task.id:
        raise HTTPException(
            502,
            "Legacy Worker returned routing for a different Task",
        )
    if not isinstance(value["status"], str) or not value["status"]:
        raise HTTPException(
            502,
            "Legacy Worker returned an invalid Task status",
        )

    authoritative = task_routing_tuple(task)
    if authoritative.codex_service_tier != "default":
        raise HTTPException(
            409,
            "Legacy Worker cannot confirm Codex Fast routing; execution was blocked",
        )
    remote = WorkerRoutingTuple(
        provider=value["provider"],
        model=value["model"],
        # Workers predating the routing protocol also predate service tiers.
        codex_service_tier=value.get("codex_service_tier", "default"),
    )
    if remote != authoritative:
        raise HTTPException(
            409,
            "Legacy Worker Task routing does not exactly match the Manager; "
            "execution was blocked",
        )
    return WorkerRoutingConfigSnapshot(
        id=task.id,
        status=value["status"],
        worker_id=None,
        shared_from_id=None,
        provider=remote.provider,
        model=remote.model,
        codex_service_tier=remote.codex_service_tier,
        pending=None,
    )


async def _read_legacy_worker_routing(
    task: Task,
    *,
    operation_lock_held: bool,
) -> WorkerRoutingConfigSnapshot:
    result = await _proxy(
        task,
        "GET",
        f"/api/tasks/{task.id}",
        require_json=True,
        operation_lock_held=operation_lock_held,
    )
    return _validate_legacy_worker_routing_snapshot(result, task=task)


async def _confirm_worker_routing_mutation(
    task: Task,
    *,
    path: str,
    payload: dict,
    expected: WorkerRoutingTuple,
    operation_lock_held: bool,
) -> WorkerRoutingConfigSnapshot:
    """Confirm ack/reconcile, recovering only a lost success response."""

    try:
        result = await _proxy(
            task,
            "POST",
            path,
            payload,
            require_json=True,
            operation_lock_held=operation_lock_held,
        )
        snapshot = _validate_remote_worker_routing_snapshot(
            result,
            task_id=task.id,
        )
    except Exception as mutation_error:
        try:
            snapshot = await _read_remote_worker_routing(
                task,
                operation_lock_held=operation_lock_held,
            )
        except Exception:
            raise _WorkerRoutingConfirmationUnavailable() from mutation_error
    if snapshot.pending is not None or _snapshot_routing_tuple(snapshot) != expected:
        raise HTTPException(
            502,
            "Worker routing synchronization remains pending or divergent",
        )
    return snapshot


async def _ensure_worker_routing_ready(
    task: Task,
    *,
    operation_lock_held: bool,
    allow_legacy_standard: bool = True,
) -> WorkerRoutingConfigSnapshot:
    """Converge an orphan stage, then prove Worker live config equals Manager."""

    authoritative = task_routing_tuple(task)
    try:
        snapshot = await _read_remote_worker_routing(
            task,
            operation_lock_held=operation_lock_held,
            surface_endpoint_not_found=allow_legacy_standard,
        )
    except WorkerEndpointNotFoundError:
        snapshot = await _read_legacy_worker_routing(
            task,
            operation_lock_held=operation_lock_held,
        )
    pending = snapshot.pending
    if pending is not None:
        pending_tuple = WorkerRoutingTuple(
            provider=pending.provider,
            model=pending.model,
            codex_service_tier=pending.codex_service_tier,
        )
        payload = {
            "op_id": pending.op_id,
            **authoritative.as_dict(),
        }
        action = "ack" if pending_tuple == authoritative else "reconcile"
        snapshot = await _confirm_worker_routing_mutation(
            task,
            path=f"/api/tasks/{task.id}/routing-config/{action}",
            payload=payload,
            expected=authoritative,
            operation_lock_held=operation_lock_held,
        )
    if (
        snapshot.pending is not None
        or _snapshot_routing_tuple(snapshot) != authoritative
    ):
        raise HTTPException(
            409,
            "Worker routing config does not exactly match the Manager; execution "
            "was blocked",
        )
    return snapshot


def _require_no_pending_worker_routing(task: Task) -> None:
    if has_pending_worker_routing(task):
        raise HTTPException(
            409,
            "Task routing configuration synchronization is pending; execution "
            "is blocked until Manager and Worker converge",
        )


async def _update_local_task_with_routing_config(
    task_id: int,
    updates: dict,
    request: Request,
    queue: TaskQueue,
) -> Task:
    """Atomically save a local route only while no generation can use the old one."""

    mixed = set(updates).difference(_WORKER_ROUTING_CONFIG_FIELDS)
    if mixed:
        raise HTTPException(
            409,
            "Task routing changes may only contain provider, model, and Codex "
            "service tier; save other fields separately",
        )

    await queue.db.rollback()
    async with get_task_operation_lock(task_id):
        queue.db.expire_all()
        observed = await queue.db.get(Task, task_id)
        if observed is None:
            raise HTTPException(404, "Task not found")
        await require_task_control(request, observed, queue.db)
        if observed.worker_id is not None or observed.shared_from_id is not None:
            raise HTTPException(
                409,
                "Task execution authority changed before routing update",
            )
        if observed.status not in _LOCAL_ROUTING_EDITABLE_STATUSES:
            raise HTTPException(
                409,
                "Task routing config cannot change after an execution claim "
                "became active; wait for the current turn to finish",
            )
        normalized = _normalized_task_update_values(updates)
        candidate = WorkerRoutingTuple(
            provider=normalized.get("provider", observed.provider),
            model=(normalized["model"] if "model" in normalized else observed.model),
            codex_service_tier=normalized.get(
                "codex_service_tier",
                observed.codex_service_tier,
            ),
        )
        try:
            validate_task_service_tier_configuration(
                provider=candidate.provider,
                model=candidate.model,
                codex_service_tier=candidate.codex_service_tier,
                mode=observed.mode,
                goal_evaluator_model=observed.goal_evaluator_model,
            )
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        if candidate.provider != observed.provider and observed.session_id is not None:
            raise HTTPException(
                409,
                "Task provider cannot change while an existing native session "
                "may still emit output; start a new Task instead",
            )
        observed_instance_id = observed.instance_id
        queue.db.expunge(observed)
        await queue.db.rollback()
        await _settle_task_launch_barrier(task_id, observed_instance_id)

        async with AsyncExitStack() as routing_stack:
            await _hold_codex_thread_routing_quiescence(
                routing_stack,
                observed,
                candidate,
            )
            queue.db.expire_all()
            current = await _lock_worker_local_routing_task(
                task_id,
                request,
                queue.db,
                safe_status_required=False,
                allowed_statuses=_LOCAL_ROUTING_EDITABLE_STATUSES,
            )
            if _routing_guard_generation_changed(current, observed):
                await queue.db.rollback()
                raise HTTPException(
                    409,
                    "Task native session or routing generation changed while "
                    "quiescence was being verified",
                )
            _require_no_pending_worker_routing(current)
            if await _running_routing_sub_agent_id(queue.db, task_id) is not None:
                await queue.db.rollback()
                raise HTTPException(
                    409,
                    "Task routing config cannot change while a sub-agent is running",
                )

            current.provider = candidate.provider
            current.model = candidate.model
            current.codex_service_tier = candidate.codex_service_tier
            await queue.db.commit()
            await queue.db.refresh(current)
            return current


async def _update_worker_task_with_routing_config(
    task_id: int,
    updates: dict,
    request: Request,
    queue: TaskQueue,
    *,
    expected_worker_id: int,
) -> Task:
    """Run stage → exact Manager CAS → Worker ack under cancellation shielding."""

    unsafe = _WORKER_CONFIG_SYNC_UNSAFE_FIELDS.intersection(updates)
    if unsafe:
        raise HTTPException(
            409,
            "Worker location/project changes must be saved separately from "
            "provider, model, or Codex Fast changes",
        )
    mixed = set(updates).difference(_WORKER_ROUTING_CONFIG_FIELDS)
    if mixed:
        raise HTTPException(
            409,
            "Worker routing changes may only contain provider, model, and "
            "Codex service tier; save other fields separately",
        )

    await queue.db.rollback()
    async with get_task_operation_lock(task_id):
        queue.db.expire_all()
        current = await queue.db.get(Task, task_id)
        if current is None:
            raise HTTPException(404, "Task not found")
        await require_task_control(request, current, queue.db)
        if current.worker_id != expected_worker_id:
            raise HTTPException(
                409,
                "Task Worker assignment changed before config synchronization",
            )
        if current.status not in WORKER_ROUTING_SAFE_STATUSES:
            raise HTTPException(
                409,
                "Worker Task config cannot change while it is pending or active; "
                "wait for the current Worker turn to finish",
            )

        normalized = _normalized_task_update_values(updates)
        candidate = WorkerRoutingTuple(
            provider=normalized.get("provider", current.provider),
            model=(normalized["model"] if "model" in normalized else current.model),
            codex_service_tier=normalized.get(
                "codex_service_tier",
                current.codex_service_tier,
            ),
        )
        try:
            validate_task_service_tier_configuration(
                provider=candidate.provider,
                model=candidate.model,
                codex_service_tier=candidate.codex_service_tier,
                mode=current.mode,
                goal_evaluator_model=current.goal_evaluator_model,
            )
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        normalized.update(candidate.as_dict())

        observed = worker_task_generation(
            current,
            expected_worker_id=expected_worker_id,
        )
        if observed is None:
            raise HTTPException(409, "Task Worker assignment changed")
        previous = task_routing_tuple(current)
        op_id = uuid.uuid4().hex
        payload = {"op_id": op_id, **candidate.as_dict()}
        # Never retain the Manager DB read transaction over Worker network
        # calls.  Relay is then free to advance status, and the one exact CAS
        # below will detect that generation change instead of re-fencing it.
        queue.db.expunge(current)
        await queue.db.rollback()

        # Resolve any durable stage left by a prior crashed request before
        # starting a different operation.  A marker with our current tuple is
        # a lost ack; a different candidate is safely aborted to our tuple.
        await _ensure_worker_routing_ready(
            current,
            operation_lock_held=True,
            allow_legacy_standard=False,
        )

        # A timeout here is intentionally not read back into success.  The
        # Worker may have staged the marker, but Manager has not committed, so
        # it must remain blocked for the next explicit convergence attempt.
        staged_result = await _proxy(
            current,
            "POST",
            f"/api/tasks/{task_id}/routing-config/stage",
            payload,
            require_json=True,
            operation_lock_held=True,
        )
        staged = _validate_remote_worker_routing_snapshot(
            staged_result,
            task_id=task_id,
        )
        if (
            staged.status not in WORKER_ROUTING_SAFE_STATUSES
            or _snapshot_routing_tuple(staged) != previous
            or staged.pending is None
            or staged.pending.op_id != op_id
            or WorkerRoutingTuple(
                provider=staged.pending.provider,
                model=staged.pending.model,
                codex_service_tier=staged.pending.codex_service_tier,
            )
            != candidate
        ):
            raise HTTPException(
                502,
                "Worker did not strictly confirm the staged routing candidate",
            )

        predicates = [
            *worker_task_generation_predicates(observed),
            Task.provider == previous.provider,
            (
                Task.model.is_(None)
                if previous.model is None
                else Task.model == previous.model
            ),
            Task.codex_service_tier == previous.codex_service_tier,
        ]
        changed = await queue.db.execute(
            sa_update(Task).where(*predicates).values(**normalized)
        )
        if changed.rowcount != 1:
            await queue.db.rollback()
            raise HTTPException(
                409,
                "Task Worker generation changed while routing config was "
                "staged; Worker remains safely blocked",
            )
        await queue.db.commit()
        queue.db.expire_all()
        updated = await queue.db.get(Task, task_id)
        if updated is None:
            raise HTTPException(
                409,
                "Task disappeared after routing config commit; Worker remains "
                "safely blocked",
            )

        try:
            await _confirm_worker_routing_mutation(
                updated,
                path=f"/api/tasks/{task_id}/routing-config/ack",
                payload=payload,
                expected=candidate,
                operation_lock_held=True,
            )
        except _WorkerRoutingConfirmationUnavailable:
            # Manager commit is the configuration commit point.  Returning an
            # error here would leave the UI displaying its old Fast/Standard
            # badge even though every subsequent execution is governed by the
            # new authoritative tuple.  The Worker either applied it already
            # or still has the durable stage marker, which blocks execution
            # until the next retry/chat preflight converges it.
            logger.warning(
                "Worker routing ack could not be confirmed after Manager "
                "commit; task=%s worker=%s op=%s remains execution-fenced",
                task_id,
                expected_worker_id,
                op_id,
            )
        return updated


async def _update_worker_task_with_skill_configuration(
    task_id: int,
    updates: dict,
    request: Request,
    queue: TaskQueue,
    *,
    expected_worker_id: int,
) -> Task:
    """Serialize Manager-authoritative Skill saves with Worker execution."""

    await queue.db.rollback()
    async with get_task_operation_lock(task_id):
        queue.db.expire_all()
        current = await queue.db.get(Task, task_id)
        if current is None:
            raise HTTPException(404, "Task not found")
        await require_task_control(request, current, queue.db)
        if current.worker_id != expected_worker_id:
            raise HTTPException(
                409,
                "Task Worker assignment changed before Skill configuration "
                "could be saved",
            )
        if current.status not in _WORKER_SKILL_EDITABLE_STATUSES:
            raise HTTPException(
                409,
                "Worker Task Skill configuration cannot change after an "
                "execution claim became active; wait for the current Worker "
                "turn to finish",
            )
        updated = await queue.update_task(task_id, **updates)
        if updated is None:
            raise HTTPException(404, "Task not found")
        return updated


@router.put("/{task_id}", response_model=TaskResponse)
async def update_task(
    task_id: int,
    body: TaskUpdate,
    request: Request,
    queue: TaskQueue = Depends(_get_queue),
):
    task = await queue.get(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    await require_task_control(request, task, queue.db)
    await _require_not_pr_review_task_mutation(
        queue.db,
        task_id,
        action="edited",
    )
    updates = body.model_dump(exclude_unset=True)
    user_skill_snapshots = updates.pop("user_skill_snapshots", None)
    if user_skill_snapshots is not None:
        require_admin(request)
    try:
        validate_task_service_tier_configuration(
            provider=updates.get("provider", task.provider),
            model=updates.get("model", task.model),
            codex_service_tier=updates.get(
                "codex_service_tier",
                task.codex_service_tier,
            ),
            mode=updates.get("mode", task.mode),
            goal_evaluator_model=updates.get(
                "goal_evaluator_model",
                task.goal_evaluator_model,
            ),
        )
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    if "enabled_skills" in updates:
        # An explicit save is authoritative even when its JSON happens to equal
        # a currently active one-turn override. Clearing the generation marker
        # lets lifecycle cleanup distinguish that user write from its own
        # temporary value.
        updates["metadata_"] = clear_temporary_skills_marker(task.metadata_)
    if user_skill_snapshots is not None:
        from backend.services.skill_context import (
            USER_SKILL_SNAPSHOTS_METADATA_KEY,
            WORKER_MANAGED_TASK_METADATA_KEY,
        )

        metadata = dict(updates.get("metadata_") or task.metadata_ or {})
        metadata[USER_SKILL_SNAPSHOTS_METADATA_KEY] = user_skill_snapshots
        metadata[WORKER_MANAGED_TASK_METADATA_KEY] = True
        updates["metadata_"] = metadata

    # "off" sentinel → explicit NULL. Do this before the Worker branch so both
    # mirrors receive the same normalized value in a combined config update.
    if updates.get("system_prompt_mode") == "off":
        updates["system_prompt_mode"] = None

    # Validate the effective Skill configuration before routing
    # synchronization or Worker migration can create externally visible state.
    effective_provider = updates.get("provider", task.provider)
    effective_description = updates.get("description", task.description)
    effective_worker_id = task.worker_id
    if "worker_id" in updates:
        requested_worker_id = updates["worker_id"]
        effective_worker_id = None if requested_worker_id == -1 else requested_worker_id
    effective_metadata = updates.get("metadata_", task.metadata_)
    command_skills = _explicit_command_skills(effective_description)
    skill_configuration_changed = (
        bool(
            {
                "provider",
                "enabled_skills",
                "selected_user_skills",
                "worker_id",
            }
            & updates.keys()
        )
        or user_skill_snapshots is not None
        or bool(command_skills and "description" in updates)
    )
    if skill_configuration_changed:
        from backend.services.skill_context import (
            USER_SKILL_SNAPSHOTS_METADATA_KEY,
        )

        effective_skills = dict(
            updates.get(
                "enabled_skills",
                task.enabled_skills,
            )
            or {}
        )
        effective_skills.update(command_skills)
        effective_user_skills = updates.get(
            "selected_user_skills",
            task.selected_user_skills,
        )
        normalized_user_skills = await _validate_skill_configuration(
            queue.db,
            provider=effective_provider,
            enabled_skills=effective_skills,
            selected_user_skills=effective_user_skills,
            user_skill_snapshots=(
                user_skill_snapshots
                if user_skill_snapshots is not None
                else (task.metadata_ or {}).get(USER_SKILL_SNAPSHOTS_METADATA_KEY)
            ),
            worker_id=effective_worker_id,
            shared_from_id=task.shared_from_id,
            metadata=effective_metadata,
        )
        if "selected_user_skills" in updates:
            updates["selected_user_skills"] = normalized_user_skills

    worker_id_supplied = "worker_id" in updates
    target_project = None
    target_project_id = updates.get("project_id", task.project_id)
    if target_project_id is not None:
        from backend.models.project import Project

        target_project = await queue.db.get(Project, target_project_id)
        if target_project is None:
            raise HTTPException(404, "Project not found")
        await require_project_access(request, target_project_id, queue.db)
        if (
            "project_id" in updates
            and not worker_id_supplied
            and task.worker_id != target_project.worker_id
        ):
            raise HTTPException(
                400,
                "Task Worker must match the selected Project location",
            )
    elif "project_id" in updates and not worker_id_supplied:
        await require_worker_target_access(request, task.worker_id, queue.db)

    # 执行位置切换走 TaskMigrator（同 mode/model 一样在 task 详情改，
    # 但语义是迁移而非改字段）。-1 = 切回本机
    if "worker_id" in updates:
        target = updates.pop("worker_id")
        if target == -1:
            target = None
        if target_project is not None and target != target_project.worker_id:
            raise HTTPException(
                400,
                "Task Worker must match the selected Project location",
            )
        if target_project is None:
            await require_worker_target_access(request, target, queue.db)
        if task.worker_id != target:
            from backend.main import task_migrator

            if task_migrator is None:
                raise HTTPException(503, "Worker 功能未启用")
            from backend.services.task_migrator import MigrationError

            try:
                # 同步执行：迁移结束后才返回，前端拿到的就是最终状态。
                # 大工作目录会久——前端按钮置灰 + migrating 状态广播兜底
                if updates:
                    await task_migrator.migrate(
                        task_id,
                        target,
                        task_updates=updates,
                    )
                else:
                    await task_migrator.migrate(task_id, target)
            except MigrationError as e:
                raise HTTPException(409, str(e))
            # migrate 在独立 session 写库；当前 DI session 的 identity map
            # 还缓存着旧 worker_id，必须 expire 否则响应返回迁移前的值
            queue.db.expire_all()
            migrated = await queue.get(task_id)
            if not migrated:
                raise HTTPException(404, "Task not found")
            return migrated

    # An already-forwarded Worker owns the executable Task row. Synchronize
    # its complete routing tuple before making the Manager mirror visible.
    if task.worker_id is not None and _WORKER_ROUTING_CONFIG_FIELDS.intersection(
        updates
    ):
        return await _finish_task_operation(
            _update_worker_task_with_routing_config(
                task_id,
                updates,
                request,
                queue,
                expected_worker_id=task.worker_id,
            )
        )
    if task.worker_id is None and _WORKER_ROUTING_CONFIG_FIELDS.intersection(updates):
        return await _finish_task_operation(
            _update_local_task_with_routing_config(
                task_id,
                updates,
                request,
                queue,
            )
        )

    # Skill-only edits remain Manager-authoritative until the next turn, but
    # they must commit under the same lock as retry/chat/plan approval.  This
    # gives every execution admission one unambiguous final tuple to sync.
    if task.worker_id is not None and _WORKER_SKILL_CONFIG_FIELDS.intersection(updates):
        return await _finish_task_operation(
            _update_worker_task_with_skill_configuration(
                task_id,
                updates,
                request,
                queue,
                expected_worker_id=task.worker_id,
            )
        )

    if not updates:
        task = await queue.get(task_id)
        if not task:
            raise HTTPException(404, "Task not found")
        return task
    task = await queue.update_task(task_id, **updates)
    if not task:
        raise HTTPException(404, "Task not found")
    return task


async def _settle_task_launch_barrier(
    task_id: int,
    instance_id: int | None,
) -> None:
    """Prove a pre-owner launch aborted after the Task became terminal."""

    from backend.services.task_termination import settle_task_launch_barrier

    try:
        await settle_task_launch_barrier(task_id, instance_id)
    except TaskLaunchTerminationConflict as exc:
        raise HTTPException(
            409,
            str(exc),
        ) from exc


async def _retry_local_task_safely(
    task_id: int,
    queue: TaskQueue,
    db: AsyncSession,
    *,
    task_updates: dict | None = None,
    commit: bool = True,
) -> Task | None:
    """Retry without discarding evidence of a possibly-live orphan process.

    Startup recovery intentionally retains ``Task.instance_id`` plus the
    Instance PID/current owner when it cannot prove that an unmanaged process
    died.  The retry endpoint is the only normal path that releases that
    terminal claim, so it must reconcile under InstanceManager's exact
    lifecycle lock before ``TaskQueue.retry`` clears the task-side owner.
    """

    from backend.main import instance_manager

    db.expire_all()
    task = await db.get(Task, task_id)
    if task is None:
        return None

    observed_status = task.status
    if observed_status not in _MANUAL_RETRYABLE_STATUSES:
        raise HTTPException(
            status_code=409,
            detail=f"Task status {observed_status} is not retryable",
        )
    observed_generation = (
        task.retry_count,
        task.instance_id,
        task.started_at,
        task.completed_at,
        task.pty_background_generation,
    )
    reverse_owner_ids = set(
        (
            await db.execute(
                select(Instance.id).where(Instance.current_task_id == task_id)
            )
        )
        .scalars()
        .all()
    )
    candidate_ids = set(reverse_owner_ids)
    if task.instance_id is not None:
        candidate_ids.add(task.instance_id)

    # Release the discovery snapshot before waiting for lifecycle locks. A
    # launch holder may need to commit Task/Instance ownership before releasing
    # that lock, and MySQL RR would otherwise keep all lock-internal reads on
    # the stale generation.
    await db.rollback()

    # Take every relevant lifecycle lock in stable order. This covers the
    # one-sided recovery state where Task.instance_id is NULL but an Instance
    # still names the task, and avoids deadlocks between two malformed rows.
    async with AsyncExitStack() as stack:
        for instance_id in sorted(candidate_ids):
            await stack.enter_async_context(
                instance_manager._instance_lifecycle_lock(instance_id)
            )

        db.expire_all()
        current_task = await db.get(Task, task_id)
        if current_task is None:
            return None
        current_generation = (
            current_task.retry_count,
            current_task.instance_id,
            current_task.started_at,
            current_task.completed_at,
            current_task.pty_background_generation,
        )
        if (
            current_task.status != observed_status
            or current_generation != observed_generation
        ):
            raise HTTPException(
                status_code=409,
                detail=(
                    "Task ownership changed or generation changed while retrying; "
                    "refresh and try again"
                ),
            )

        # Take the Task row/current-generation lock before any Instance row.
        # cancel/delete use the same Task -> Instance order; without this
        # guard retry could hold Instance while cancellation waits for it and
        # then block on cancellation's Task lock.
        guarded_task = await db.execute(
            sa_update(Task)
            .where(*_task_generation_fence(task_id, current_task))
            .values(status=current_task.status)
        )
        if not guarded_task.rowcount:
            await db.rollback()
            raise HTTPException(
                status_code=409,
                detail=(
                    "Task ownership changed or generation changed while retrying; "
                    "refresh and try again"
                ),
            )

        owner_result = await db.execute(
            select(Instance)
            .where(Instance.current_task_id == task_id)
            .with_for_update()
        )
        reverse_owners = list(owner_result.scalars().all())
        current_candidate_ids = {instance.id for instance in reverse_owners}
        if current_task.instance_id is not None:
            current_candidate_ids.add(current_task.instance_id)
        if not current_candidate_ids.issubset(candidate_ids):
            raise HTTPException(
                status_code=409,
                detail=("Task ownership changed while retrying; refresh and try again"),
            )

        # A task-side link without a reverse owner can still point at a
        # pre-commit managed generation. Treat it as uncertain unless the slot
        # now explicitly belongs to another task.
        if current_task.instance_id is not None:
            task_side_instance = (
                await db.execute(
                    select(Instance)
                    .where(Instance.id == current_task.instance_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if (
                task_side_instance is not None
                and task_side_instance.current_task_id in (None, task_id)
                and instance_manager.is_running(current_task.instance_id)
            ):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Instance {current_task.instance_id} still has a live "
                        "managed generation; stop it before retrying"
                    ),
                )

        for instance in reverse_owners:
            if instance_manager.is_running(instance.id):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Instance {instance.id} still has a live managed "
                        "generation; stop it before retrying"
                    ),
                )

            pid = instance.pid
            if pid is not None:
                try:
                    os.kill(pid, 0)
                except ProcessLookupError:
                    pass
                except OSError:
                    # Permission errors and platform-specific failures do not
                    # prove death. Keep all ownership evidence fail-closed.
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"Unmanaged process PID {pid} may still be alive; "
                            "stop or reconcile it before retrying"
                        ),
                    )
                else:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"Unmanaged process PID {pid} is still alive; "
                            "stop it before retrying"
                        ),
                    )
            elif instance.status == "running":
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Instance {instance.id} still has an uncertain running "
                        "owner; stop or reconcile it before retrying"
                    ),
                )

            instance_predicates = [
                Instance.id == instance.id,
                Instance.current_task_id == task_id,
                Instance.status == instance.status,
                (Instance.pid.is_(None) if pid is None else Instance.pid == pid),
                (
                    Instance.started_at.is_(None)
                    if instance.started_at is None
                    else Instance.started_at == instance.started_at
                ),
            ]
            cleared = await db.execute(
                sa_update(Instance)
                .where(*instance_predicates)
                .values(status="error", current_task_id=None, pid=None)
            )
            if not cleared.rowcount:
                await db.rollback()
                raise HTTPException(
                    status_code=409,
                    detail="Instance ownership changed while retrying; try again",
                )

        retried = await queue.retry(
            task_id,
            expected_statuses=(observed_status,),
            generation_fence=observed_generation,
            rollback_on_miss=True,
            task_updates=task_updates,
            commit=commit,
        )
        if retried is None:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Task ownership changed or generation changed while retrying; "
                    "refresh and try again"
                ),
            )
        return retried


@router.delete("/{task_id}")
async def delete_task(
    task_id: int,
    request: Request,
    queue: TaskQueue = Depends(_get_queue),
    db: AsyncSession = Depends(get_db),
):
    task = await db.get(Task, task_id)
    if task:
        await require_task_control(request, task, db)
    from backend.main import instance_manager, task_migrator, worker_proxy

    if task is None:
        raise HTTPException(404, "Task not found")
    await _require_no_pr_review_publication(db, task_id)
    await _require_not_pr_review_task_mutation(
        db,
        task_id,
        action="deleted",
    )
    if (
        task.worker_id is None
        and task.pty_background_generation is not None
    ):
        raise HTTPException(
            409,
            "Task still has active Claude PTY background output",
        )

    if task is not None and task.worker_id is not None:
        # A Worker task has two durable copies, but only the remote copy owns
        # its process lifecycle.  Serialize against migration so an A→B→A ABA
        # cannot rebuild the task after the remote delete and still satisfy the
        # Manager mirror fence.
        await db.rollback()
        migration_lock = None
        if task_migrator is not None:
            migration_lock = task_migrator._locks.setdefault(
                task_id,
                asyncio.Lock(),
            )
            if migration_lock.locked():
                raise HTTPException(
                    409,
                    "Task is being migrated; retry deletion after migration",
                )
        if worker_proxy is None:
            raise HTTPException(503, "Worker 功能未启用")
        worker_operation_lock = worker_proxy.task_operation_lock(task_id)

        async with AsyncExitStack() as stack:
            if migration_lock is not None:
                await stack.enter_async_context(migration_lock)
            await stack.enter_async_context(worker_operation_lock)

            db.expire_all()
            worker_task = await db.get(Task, task_id)
            if worker_task is None:
                raise HTTPException(404, "Task not found")
            await require_task_control(request, worker_task, db)
            if worker_task.worker_id is None:
                raise HTTPException(
                    409,
                    "Task moved back to this Manager; refresh before deleting",
                )
            if not is_task_status_deletable(
                mode=worker_task.mode,
                status=worker_task.status,
            ):
                raise HTTPException(
                    400,
                    "Cannot delete task (not in deletable state)",
                )

            worker_id = worker_task.worker_id
            delete_fence = task_delete_fence(worker_task)
            remote_result = await _proxy(
                worker_task,
                "DELETE",
                f"/api/tasks/{task_id}",
                require_json=True,
                allow_task_absent=True,
                operation_lock_held=True,
            )
            if (
                not isinstance(remote_result, dict)
                or remote_result.get("ok") is not True
            ):
                await db.rollback()
                raise HTTPException(
                    502,
                    "Worker did not explicitly confirm task deletion; "
                    "Manager mirror was preserved",
                )

            # Drop the pre-proxy read snapshot. TaskQueue.delete starts with an
            # exact current-write CAS over the original Worker generation, so
            # a concurrent relay/retry cannot make us erase a newer mirror.
            await db.rollback()
            ok = await queue.delete(
                task_id,
                expected_fence=delete_fence,
                remote_worker_deleted=True,
            )
            if not ok:
                # Relay/status updates can legitimately land after the Worker
                # has committed deletion. Remote mutation/forwarding and task
                # migration are still fenced by the two locks above, so a
                # current mirror on the same Worker is only stale state, not a
                # rebuilt remote generation. Lock and delete that exact current
                # row to make the cross-CCM delete converge.
                await db.rollback()
                current_worker_task = (
                    await db.execute(
                        select(Task).where(Task.id == task_id).with_for_update()
                    )
                ).scalar_one_or_none()
                if current_worker_task is None:
                    ok = True
                elif current_worker_task.worker_id != worker_id:
                    await db.rollback()
                    worker_proxy.relay.unsubscribe_task(worker_id, task_id)
                    raise HTTPException(
                        409,
                        "Worker deleted the old task, but the Manager mirror "
                        "moved to another execution location and was preserved",
                    )
                else:
                    current_fence = task_delete_fence(current_worker_task)
                    ok = await queue.delete(
                        task_id,
                        expected_fence=current_fence,
                        remote_worker_deleted=True,
                    )
                if not ok:
                    raise HTTPException(
                        409,
                        "Worker deleted the task, but local runtime ownership "
                        "could not be safely reconciled; the mirror was preserved",
                    )
            worker_proxy.relay.unsubscribe_task(worker_id, task_id)
        return {"ok": True}

    if not is_task_status_deletable(mode=task.mode, status=task.status):
        raise HTTPException(
            400, "Cannot delete task (not found or not in deletable state)"
        )
    from backend.services.test_harness import test_harness_service

    async with test_harness_service.owner_stop_fence(
        task_id,
        reason="Owner Task was deleted",
    ):
        lifecycle_ids = set(
            (
                await db.execute(
                    select(Instance.id).where(Instance.current_task_id == task_id)
                )
            )
            .scalars()
            .all()
        )
        if task is not None and task.instance_id is not None:
            task_side_instance = await db.get(Instance, task.instance_id)
            if task_side_instance is not None and task_side_instance.current_task_id in (
                None,
                task_id,
            ):
                lifecycle_ids.add(task.instance_id)
        # Do not wait on a lifecycle lock while retaining a read transaction:
        # launch holds that lock while committing Task/Instance metadata.
        await db.rollback()

        # Serialize deletion with the complete launch/spawn/persist window. A
        # terminal Task can otherwise disappear just before a child is registered;
        # the launch would eventually abort, but shutdown in that gap would have no
        # durable Task evidence.
        async with AsyncExitStack() as stack:
            for instance_id in sorted(lifecycle_ids):
                await stack.enter_async_context(
                    instance_manager._instance_lifecycle_lock(instance_id)
                )
            ok = await queue.delete(task_id)
    if not ok:
        raise HTTPException(
            400, "Cannot delete task (not found or not in deletable state)"
        )
    return {"ok": True}


async def _worker_task_or_none(db: AsyncSession, task_id: int) -> Task | None:
    """task 在 Worker 上则返回之（代理路径），本机返回 None。"""
    task = await db.get(Task, task_id)
    return task if (task and task.worker_id is not None) else None


async def _proxy(
    task: Task,
    method: str,
    path: str,
    body=None,
    *,
    require_json: bool = False,
    allow_task_absent: bool = False,
    surface_endpoint_not_found: bool = False,
    operation_lock_held: bool = False,
):
    from backend.main import worker_proxy

    if worker_proxy is None:
        raise HTTPException(503, "Worker 功能未启用")
    if (
        require_json
        or allow_task_absent
        or surface_endpoint_not_found
        or operation_lock_held
    ):
        proxy_options = {
            "require_json": require_json,
            "allow_task_absent": allow_task_absent,
            "operation_lock_held": operation_lock_held,
        }
        if surface_endpoint_not_found:
            proxy_options["surface_endpoint_not_found"] = True
        return await worker_proxy.proxy_to_worker(
            task,
            method,
            path,
            body,
            **proxy_options,
        )
    return await worker_proxy.proxy_to_worker(task, method, path, body)


async def _sync_worker_skill_selection_before_execution(task: Task) -> None:
    """Confirm Manager Skills on the Worker before an executable transition."""

    from backend.main import worker_proxy

    if worker_proxy is None:
        raise HTTPException(503, "Worker 功能未启用")
    worker = await worker_proxy.require_ready_worker(task.worker_id)
    await worker_proxy.sync_task_skill_selection(worker, task)


async def _sync_task_from_worker_response(
    db: AsyncSession,
    task: Task,
    result,
    *,
    observed: WorkerTaskGeneration,
):
    """代理响应是 worker 的 task JSON 时，同步关键字段（status 等 relay 也会同步，
    这里立即写一份让 API 响应不滞后）。

    ``observed`` 必须在代理网络请求前捕获。响应回来后只允许 CAS 那个
    Worker assignment/generation，不能重新读取当前 Task 后把旧响应套到新代次。
    """

    task_id = observed.task_id
    resulting = await apply_authoritative_worker_task(db, observed, result)
    if resulting is None:
        await db.rollback()
        raise HTTPException(
            409,
            "Task Worker assignment or generation changed while the request "
            "was in flight",
        )
    status_changed = resulting.status != observed.status
    if status_changed:
        # relay 断连窗口内 Worker 侧广播镜像不过来，这里本地补一次。
        # Hold an exact-result no-op UPDATE across publication so a rapid retry
        # cannot let this old status event cross the replacement generation.
        guarded = await db.execute(
            sa_update(Task)
            .where(*worker_task_generation_predicates(resulting))
            .values(status=resulting.status)
        )
        if guarded.rowcount == 1:
            from backend.services.task_events import broadcast_status_change

            await broadcast_status_change(task_id, resulting.status)
            await db.commit()
        else:
            await db.rollback()
            raise HTTPException(
                409,
                "Task Worker assignment or generation changed before status "
                "publication",
            )

    db.expire_all()
    current = await db.get(Task, task_id)
    if current is None:
        raise HTTPException(
            409,
            "Task disappeared while the Worker request was in flight",
        )
    return current


async def _internal_pr_review_termination_task(
    task_id: int,
    request: Request,
    db: AsyncSession,
) -> Task:
    """Authorize one hidden Manager→Worker termination protocol request."""

    require_internal_service(request)
    task = await db.get(Task, task_id)
    if task:
        await require_task_control(request, task, db)
        await _require_no_pr_review_publication(db, task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    if not is_pr_sandbox_task(task):
        raise HTTPException(
            400,
            "Exact-generation termination is restricted to PR workflow tasks",
        )
    return task


@router.get(
    "/{task_id}/terminate-generation",
    response_model=TaskTerminationSnapshot,
    include_in_schema=False,
)
async def get_task_termination_generation(
    task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Return the Worker's exact opaque generation to its Manager only."""

    return await _internal_pr_review_termination_task(task_id, request, db)


@router.post(
    "/{task_id}/terminate-generation",
    response_model=TaskResponse,
    include_in_schema=False,
)
async def terminate_task_generation(
    task_id: int,
    body: TaskTerminationRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Internal Manager→Worker exact-generation termination endpoint.

    The complete local lifecycle is cancellation-shielded. The resulting Task
    row remains locked through response serialization so a remote retry cannot
    overtake the authoritative terminal snapshot returned to the Manager.
    """

    await _internal_pr_review_termination_task(task_id, request, db)

    from backend.services.task_termination import (
        LocalTaskGeneration,
        TaskTerminationConflict,
        lock_task_generation,
        terminate_local_task_generation,
    )

    try:
        terminated = await terminate_local_task_generation(
            task_id,
            db,
            reason="Superseded by new PR push",
            expected_generation=LocalTaskGeneration(
                status=body.expected_status,
                retry_count=body.expected_retry_count,
                instance_id=body.expected_instance_id,
                started_at=body.expected_started_at,
                completed_at=body.expected_completed_at,
                pty_background_generation=(body.expected_pty_background_generation),
            ),
        )
    except TaskTerminationConflict as exc:
        await db.rollback()
        raise HTTPException(
            409,
            "Task generation cleanup could not be confirmed",
        ) from exc

    locked_task = await lock_task_generation(
        task_id,
        db,
        expected_status=terminated.terminal_status,
        expected_retry_count=terminated.retry_count,
        expected_instance_id=terminated.instance_id,
        expected_started_at=terminated.started_at,
        expected_completed_at=terminated.completed_at,
        expected_pty_background_generation=(terminated.pty_background_generation),
    )
    if locked_task is None:
        raise HTTPException(
            409,
            "Task started a newer generation after termination",
        )
    return locked_task


async def _stop_task_session_local_impl(
    task_id: int,
    db: AsyncSession,
) -> dict:
    """Keep message admission closed until the stopped generation is final."""

    from backend.main import dispatcher
    from backend.services.test_harness import test_harness_service

    async with test_harness_service.owner_stop_fence(
        task_id,
        reason="Owner Task session was stopped",
    ):
        async with dispatcher.task_queue_cancellation_lease(task_id):
            return await _stop_task_session_local_under_cancellation_lease(
                task_id,
                db,
            )


async def _stop_task_session_local_under_cancellation_lease(
    task_id: int,
    db: AsyncSession,
) -> dict:
    """Cancellation-safe local core for ``POST /stop-session``."""

    from backend.main import dispatcher, instance_manager, ralph_loop

    await db.rollback()
    try:
        cleared = await dispatcher.abort_task_queue(
            task_id,
            cancel_durable=False,
            durable_db=db,
        )
    except Exception as exc:
        from backend.services.dispatcher import TaskQueueAbortTimeoutError

        if isinstance(exc, TaskQueueAbortTimeoutError):
            raise HTTPException(
                409,
                "Task queue worker could not be proven stopped; no terminal "
                "state was published",
            ) from exc
        raise

    # stop-session is a Task-wide execution stop, not merely a signal to the
    # current foreground process.  Monitors and CCM-owned sub-agents are
    # independent message producers; if they remain ``running`` they can post
    # a report immediately after the queue drain and resurrect the Task.  Close
    # those producers durably before resolving/stopping the main owner, then
    # drain once more to catch a report that was already in flight.
    from backend.models.monitor_session import MonitorSession

    auxiliary_rows = await db.execute(
        select(
            MonitorSession.id,
            MonitorSession.agent_type,
            MonitorSession.source,
        )
        .where(
            MonitorSession.task_id == task_id,
            MonitorSession.status.in_(("running", "cancelled")),
        )
        .with_for_update()
    )
    auxiliary_sessions = list(auxiliary_rows.all())
    await db.execute(
        sa_update(MonitorSession)
        .where(
            MonitorSession.task_id == task_id,
            MonitorSession.status == "running",
        )
        .values(
            status="cancelled",
            completed_at=datetime.utcnow(),
            next_check_at=None,
            active_turn_generation=None,
            turn_started_at=None,
        )
    )
    await db.commit()

    for session_id, agent_type, source in auxiliary_sessions:
        if source != "ccm":
            continue
        try:
            if agent_type == "sub_agent":
                await dispatcher.stop_sub_agent_session_process(session_id)
            elif agent_type == "monitor":
                await dispatcher.stop_monitor_session_process(
                    session_id,
                    terminal=True,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise HTTPException(
                409,
                "Task message producers were closed, but auxiliary process "
                f"cleanup could not be confirmed for session {session_id}",
            ) from exc

    if auxiliary_sessions:
        try:
            cleared += await dispatcher.abort_task_queue(task_id)
        except Exception as exc:
            from backend.services.dispatcher import TaskQueueAbortTimeoutError

            if isinstance(exc, TaskQueueAbortTimeoutError):
                raise HTTPException(
                    409,
                    "Task auxiliary producers were stopped, but the queue "
                    "worker could not be proven stopped",
                ) from exc
            raise

    # Settle a launch reservation before deciding whether an exact process
    # owner exists. A no-owner Task is safe to terminalize only after this
    # barrier proves no spawned-but-uncommitted generation can appear.
    await db.rollback()
    db.expire_all()
    probe = await db.get(Task, task_id)
    if probe is None or probe.worker_id is not None or probe.shared_from_id is not None:
        await db.rollback()
        raise HTTPException(
            409,
            "Task execution location changed while stopping its session",
        )
    probe_instance_id = probe.instance_id
    probe_is_active_plan = probe.mode == "plan" and probe.status in {
        "in_progress",
        "executing",
    }
    await db.rollback()
    await _settle_task_launch_barrier(task_id, probe_instance_id)
    if probe_is_active_plan:
        try:
            stopped = await dispatcher.stop_plan_agent_lifecycle(
                task_id,
                probe_instance_id,
            )
            if not stopped:
                stopped = await ralph_loop.stop_plan_agent_lifecycle(task_id)
            if not stopped:
                raise RuntimeError(f"No exact Plan lifecycle owns Task {task_id}")
        except Exception as exc:
            raise HTTPException(
                409,
                "Plan Agent process cleanup could not be confirmed",
            ) from exc

    db.expire_all()
    active_task = (
        await db.execute(
            select(Task)
            .where(
                Task.id == task_id,
                Task.worker_id.is_(None),
                Task.shared_from_id.is_(None),
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if active_task is None:
        await db.rollback()
        raise HTTPException(
            409,
            "Task execution location changed while stopping its session",
        )

    stoppable_statuses = {
        "executing",
        "in_progress",
        "failed",
        "completed",
        "cancelled",
        "conflict",
    }
    if active_task.status not in stoppable_statuses:
        if active_task.status == "pending" and cleared:
            queue_only = await db.execute(
                sa_update(Task)
                .where(
                    *_task_generation_fence(task_id, active_task),
                    Task.pty_background_generation.is_(None),
                )
                .values(status=active_task.status)
            )
            if not queue_only.rowcount:
                await db.rollback()
                raise HTTPException(
                    409,
                    "Task generation changed while queued messages were being cleared",
                )
            await db.commit()
            return {
                "ok": True,
                "stopped": False,
                "cleared_messages": cleared,
                "task_status": active_task.status,
                "background_active": False,
            }
        await db.rollback()
        raise HTTPException(400, "No running session found for this task")

    observed_status = active_task.status
    observed_retry_count = active_task.retry_count
    observed_instance_id = active_task.instance_id
    observed_started_at = active_task.started_at
    observed_session_id = active_task.session_id
    observed_completed_at = active_task.completed_at
    observed_background_generation = active_task.pty_background_generation
    owner_rows = await db.execute(
        select(
            Instance.id,
            Instance.pid,
            Instance.started_at,
        )
        .where(Instance.current_task_id == task_id)
        .with_for_update()
    )
    expected_generations = list(owner_rows.all())

    if expected_generations:
        # InstanceManager owns PTY terminal arbitration. Stop first while the
        # Task is still active; it then writes Task+Instance+marker atomically.
        # Publishing a terminal Task before this call lets on_exit discard its
        # exact Session and is the race this ordering prevents.
        await db.commit()
        stopped = await _stop_task_process(
            task_id,
            db,
            expected_generations=expected_generations,
            task_status="completed",
        )
        remaining_generations = await _remaining_task_process_generations(
            task_id,
            db,
            expected_generations=expected_generations,
        )
        if remaining_generations:
            await db.rollback()
            raise HTTPException(
                409,
                "Task process cleanup could not be confirmed for instance(s): "
                + ", ".join(map(str, remaining_generations)),
            )
        await db.rollback()
        db.expire_all()
        current = (
            await db.execute(select(Task).where(Task.id == task_id).with_for_update())
        ).scalar_one_or_none()
        expected_status = (
            "completed"
            if observed_status in {"executing", "in_progress"}
            else observed_status
        )
        if (
            current is None
            or current.worker_id is not None
            or current.shared_from_id is not None
            or current.retry_count != observed_retry_count
            or current.instance_id != observed_instance_id
            or current.started_at != observed_started_at
        ):
            await db.rollback()
            raise HTTPException(
                409,
                "Task generation changed while its session was stopping",
            )
        replacement_owner = await db.scalar(
            select(Instance.id)
            .where(Instance.current_task_id == task_id)
            .with_for_update()
        )
        if replacement_owner is not None:
            await db.rollback()
            raise HTTPException(
                409,
                "Task acquired a newer process owner while its previous "
                "session was stopping",
            )
        if not stopped or current.status != expected_status:
            await db.rollback()
            raise HTTPException(
                409,
                "Task owner did not atomically publish its stopped state",
            )

        background_cleared_by_api = False
        if current.pty_background_generation is not None and (
            observed_background_generation is None
            or current.pty_background_generation != observed_background_generation
        ):
            await db.rollback()
            raise HTTPException(
                409,
                "Task entered a newer PTY background generation while its "
                "previous session was stopping",
            )
        if current.pty_background_generation is not None:
            current.pty_background_generation = None
            background_cleared_by_api = True
        publication_retry_count = current.retry_count
        publication_instance_id = current.instance_id
        publication_started_at = current.started_at
        publication_completed_at = await _read_persisted_task_completed_at(task_id, db)
        await db.commit()

        if background_cleared_by_api:
            publication_task = await _lock_task_generation(
                task_id,
                db,
                expected_status=expected_status,
                expected_retry_count=publication_retry_count,
                expected_instance_id=publication_instance_id,
                expected_started_at=publication_started_at,
                expected_completed_at=publication_completed_at,
                expected_pty_background_generation=None,
            )
            if (
                publication_task is None
                or publication_task.pty_background_generation is not None
            ):
                await db.rollback()
                raise HTTPException(
                    409,
                    "Task started a newer generation while its stopped status "
                    "was being published",
                )
            from backend.services.task_events import broadcast_status_change

            await broadcast_status_change(
                task_id,
                expected_status,
                background_active=False,
            )
            await db.commit()
        return {
            "ok": True,
            "stopped": True,
            "cleared_messages": cleared,
            "task_status": expected_status,
            "background_active": False,
        }

    if observed_background_generation is not None:
        # A truly late autonomous turn has no Instance owner. Stop the exact
        # Task/session state; never address a historical reusable slot.
        if observed_status != "completed" or not observed_session_id:
            await db.rollback()
            raise HTTPException(
                409,
                "Task has PTY background output without a safe detached owner",
            )
        guarded = await db.execute(
            sa_update(Task)
            .where(*_task_generation_fence(task_id, active_task))
            .values(status=active_task.status)
        )
        if not guarded.rowcount:
            await db.rollback()
            raise HTTPException(
                409,
                "Task generation changed while detached output was stopping",
            )
        await db.commit()
        detached_stopped = (
            await instance_manager.stop_detached_pty_background_generation(
                task_id,
                observed_session_id,
                observed_background_generation,
                expected_status=observed_status,
                expected_retry_count=observed_retry_count,
                expected_instance_id=observed_instance_id,
                expected_started_at=observed_started_at,
                expected_completed_at=observed_completed_at,
            )
        )
        if not detached_stopped:
            raise HTTPException(
                409,
                "Detached Claude PTY background session could not be proven stopped",
            )
        publication_task = await _lock_task_generation(
            task_id,
            db,
            expected_status=observed_status,
            expected_retry_count=observed_retry_count,
            expected_instance_id=observed_instance_id,
            expected_started_at=observed_started_at,
            expected_completed_at=observed_completed_at,
            expected_pty_background_generation=None,
        )
        if (
            publication_task is None
            or publication_task.pty_background_generation is not None
        ):
            await db.rollback()
            raise HTTPException(
                409,
                "Task started a newer background generation while its "
                "detached session stop was being published",
            )
        from backend.services.task_events import broadcast_status_change

        await broadcast_status_change(
            task_id,
            observed_status,
            background_active=False,
        )
        await db.commit()
        return {
            "ok": True,
            "stopped": True,
            "cleared_messages": cleared,
            "task_status": observed_status,
            "background_active": False,
        }

    transitioned = observed_status in {"executing", "in_progress"}
    if transitioned:
        completed_at = datetime.utcnow()
        completed = await db.execute(
            sa_update(Task)
            .where(
                *_task_generation_fence(task_id, active_task),
                Task.pty_background_generation.is_(None),
            )
            .values(status="completed", completed_at=completed_at)
        )
        if not completed.rowcount:
            await db.rollback()
            raise HTTPException(
                409,
                "Task generation changed while stopping its session",
            )
        publication_completed_at = await _read_persisted_task_completed_at(task_id, db)
        await db.commit()
        publication_task = await _lock_task_generation(
            task_id,
            db,
            expected_status="completed",
            expected_retry_count=observed_retry_count,
            expected_instance_id=observed_instance_id,
            expected_started_at=observed_started_at,
            expected_completed_at=publication_completed_at,
            expected_pty_background_generation=None,
        )
        if (
            publication_task is None
            or publication_task.pty_background_generation is not None
        ):
            await db.rollback()
            raise HTTPException(
                409,
                "Task started a newer generation while its stopped status "
                "was being published",
            )
        from backend.services.task_events import broadcast_status_change

        await broadcast_status_change(
            task_id,
            "completed",
            background_active=False,
        )
        await db.commit()
        return {
            "ok": True,
            "stopped": False,
            "cleared_messages": cleared,
            "note": "No running process found, task marked as completed",
            "task_status": "completed",
            "background_active": False,
        }

    guarded = await db.execute(
        sa_update(Task)
        .where(*_task_generation_fence(task_id, active_task))
        .values(status=active_task.status)
    )
    if not guarded.rowcount:
        await db.rollback()
        raise HTTPException(
            409,
            "Task generation changed while stopping its session",
        )
    await db.commit()
    return {
        "ok": True,
        "stopped": False,
        "cleared_messages": cleared,
        "note": (
            f"Task is already {active_task.status}; no running process found"
        ),
        "task_status": active_task.status,
        "background_active": False,
    }


@router.post("/{task_id}/stop-session")
async def stop_task_session(
    task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Stop the running Claude Code session for a task.

    Abort queued work, settle any launch reservation, and snapshot the exact
    reverse Instance owner. A proven owner is stopped before its terminal Task
    state is published; an ownerless generation is terminalized only after the
    launch barrier proves that no spawned-but-uncommitted process can appear.
    """

    task = await db.get(Task, task_id)
    if task:
        await require_task_control(request, task, db)
        await _require_no_pr_review_publication(db, task_id)
        await _require_not_pr_review_task_mutation(
            db,
            task_id,
            action="stopped",
        )
    wt = await _worker_task_or_none(db, task_id)
    if wt is not None:
        return await _proxy(wt, "POST", f"/api/tasks/{task_id}/stop-session")

    return await _finish_task_operation(_stop_task_session_local_impl(task_id, db))


async def _cancel_local_task_impl(
    task_id: int,
    db: AsyncSession,
) -> Task:
    """Keep message admission closed until cancellation is authoritative."""

    from backend.main import dispatcher
    from backend.services.test_harness import test_harness_service

    async with test_harness_service.owner_stop_fence(
        task_id,
        reason="Owner Task was cancelled",
    ):
        async with dispatcher.task_queue_cancellation_lease(task_id):
            return await _cancel_local_task_under_cancellation_lease(task_id, db)


async def _cancel_local_task_under_cancellation_lease(
    task_id: int,
    db: AsyncSession,
) -> Task:
    """Cancellation-safe local core for ``POST /cancel``."""

    from backend.main import dispatcher, ralph_loop

    await db.rollback()
    try:
        await dispatcher.abort_task_queue(
            task_id,
            cancel_durable=False,
            durable_db=db,
        )
    except Exception as exc:
        from backend.services.dispatcher import TaskQueueAbortTimeoutError

        if isinstance(exc, TaskQueueAbortTimeoutError):
            raise HTTPException(
                409,
                "Task queue worker could not be proven stopped; cancellation "
                "was not published",
            ) from exc
        raise

    # Close the spawn-without-owner window before choosing the running-owner
    # path or the ownerless terminal CAS path.
    await db.rollback()
    db.expire_all()
    probe = await db.get(Task, task_id)
    if probe is None or probe.worker_id is not None or probe.shared_from_id is not None:
        await db.rollback()
        raise HTTPException(
            409,
            "Task execution location changed while cancellation was starting",
        )
    probe_instance_id = probe.instance_id
    probe_is_active_plan = probe.mode == "plan" and probe.status in {
        "in_progress",
        "executing",
    }
    await db.rollback()
    await _settle_task_launch_barrier(task_id, probe_instance_id)
    if probe_is_active_plan:
        stopped = await dispatcher.stop_plan_agent_lifecycle(
            task_id,
            probe_instance_id,
        )
        if not stopped:
            stopped = await ralph_loop.stop_plan_agent_lifecycle(task_id)
        if not stopped:
            raise HTTPException(
                409,
                "Plan Agent process cleanup could not be confirmed",
            )

    db.expire_all()
    active_task = (
        await db.execute(
            select(Task)
            .where(
                Task.id == task_id,
                Task.worker_id.is_(None),
                Task.shared_from_id.is_(None),
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    active_statuses = (
        "pending_activation",
        "pending",
        "in_progress",
        "executing",
        "merging",
    )
    if active_task is None or active_task.status not in (
        *active_statuses,
        "cancelled",
    ):
        await db.rollback()
        raise HTTPException(400, "Cannot cancel task")

    observed_retry_count = active_task.retry_count
    observed_instance_id = active_task.instance_id
    observed_started_at = active_task.started_at
    observed_background_generation = active_task.pty_background_generation
    owner_rows = await db.execute(
        select(
            Instance.id,
            Instance.pid,
            Instance.started_at,
        )
        .where(Instance.current_task_id == task_id)
        .with_for_update()
    )
    expected_generations = list(owner_rows.all())

    transitioned_by_api = False
    background_cleared_by_api = False
    if expected_generations:
        # Stop while the Task is still active. InstanceManager claims PTY
        # terminal ownership and commits Task+Instance+marker atomically.
        await db.commit()
        stopped = await _stop_task_process(
            task_id,
            db,
            expected_generations=expected_generations,
            task_status="cancelled",
        )
        remaining_generations = await _remaining_task_process_generations(
            task_id,
            db,
            expected_generations=expected_generations,
        )
        if remaining_generations:
            await db.rollback()
            raise HTTPException(
                409,
                "Task process cleanup could not be confirmed for instance(s): "
                + ", ".join(map(str, remaining_generations)),
            )
        await db.rollback()
        db.expire_all()
        active_task = (
            await db.execute(select(Task).where(Task.id == task_id).with_for_update())
        ).scalar_one_or_none()
        if (
            active_task is None
            or active_task.worker_id is not None
            or active_task.shared_from_id is not None
            or active_task.retry_count != observed_retry_count
            or active_task.instance_id != observed_instance_id
            or active_task.started_at != observed_started_at
        ):
            await db.rollback()
            raise HTTPException(
                409,
                "Task generation changed while cancellation was stopping it",
            )
        replacement_owner = await db.scalar(
            select(Instance.id)
            .where(Instance.current_task_id == task_id)
            .with_for_update()
        )
        if replacement_owner is not None:
            await db.rollback()
            raise HTTPException(
                409,
                "Task acquired a newer process owner while cancellation was "
                "stopping its previous generation",
            )
        if not stopped or active_task.status != "cancelled":
            await db.rollback()
            raise HTTPException(
                409,
                "Task owner did not atomically publish its cancelled state",
            )
        if active_task.pty_background_generation is not None and (
            observed_background_generation is None
            or active_task.pty_background_generation != observed_background_generation
        ):
            await db.rollback()
            raise HTTPException(
                409,
                "Task entered a newer PTY background generation while "
                "cancellation was stopping its previous generation",
            )
        if active_task.pty_background_generation is not None:
            active_task.pty_background_generation = None
            background_cleared_by_api = True
    else:
        if active_task.pty_background_generation is not None:
            await db.rollback()
            raise HTTPException(
                409,
                "Task still has active detached PTY output; use stop-session",
            )
        transitioned_by_api = active_task.status in active_statuses
        cancelled_values = (
            {
                "status": "cancelled",
                "completed_at": datetime.utcnow(),
            }
            if transitioned_by_api
            else {"status": "cancelled"}
        )
        cancelled = await db.execute(
            sa_update(Task)
            .where(
                *_task_generation_fence(task_id, active_task),
                Task.pty_background_generation.is_(None),
            )
            .values(**cancelled_values)
        )
        if not cancelled.rowcount:
            await db.rollback()
            raise HTTPException(
                409,
                "Task generation changed while cancellation was starting",
            )
        active_task = await db.get(Task, task_id, populate_existing=True)

    from backend.models.monitor_session import MonitorSession

    monitor_rows = await db.execute(
        select(
            MonitorSession.id,
            MonitorSession.agent_type,
            MonitorSession.source,
        )
        .where(
            MonitorSession.task_id == task_id,
            MonitorSession.status.in_(("running", "cancelled")),
        )
        .with_for_update()
    )
    auxiliary_sessions = list(monitor_rows.all())
    await db.execute(
        sa_update(MonitorSession)
        .where(
            MonitorSession.task_id == task_id,
            MonitorSession.status == "running",
        )
        .values(
            status="cancelled",
            completed_at=datetime.utcnow(),
            next_check_at=None,
            active_turn_generation=None,
            turn_started_at=None,
        )
    )
    committed_retry_count = active_task.retry_count
    committed_instance_id = active_task.instance_id
    committed_started_at = active_task.started_at
    committed_completed_at = await _read_persisted_task_completed_at(task_id, db)
    await db.commit()

    for session_id, agent_type, source in auxiliary_sessions:
        # Native agents are part of the main process tree. CCM-owned auxiliary
        # processes use their own exact registries and must be reaped explicitly.
        if source != "ccm":
            continue
        try:
            if agent_type == "sub_agent":
                await dispatcher.stop_sub_agent_session_process(session_id)
            elif agent_type == "monitor":
                await dispatcher.stop_monitor_session_process(
                    session_id,
                    terminal=True,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise HTTPException(
                409,
                "Task was cancelled, but auxiliary process cleanup could not "
                f"be confirmed for session {session_id}",
            ) from exc

    current_task = await _lock_task_generation(
        task_id,
        db,
        expected_status="cancelled",
        expected_retry_count=committed_retry_count,
        expected_instance_id=committed_instance_id,
        expected_started_at=committed_started_at,
        expected_completed_at=committed_completed_at,
        expected_pty_background_generation=None,
    )
    if current_task is None:
        raise HTTPException(
            409,
            "Task started a newer generation while cancellation was finishing",
        )

    if transitioned_by_api or background_cleared_by_api:
        from backend.services.task_events import broadcast_status_change

        await broadcast_status_change(
            task_id,
            "cancelled",
            background_active=False,
        )
    await db.commit()
    return current_task


@router.post("/{task_id}/cancel", response_model=TaskResponse)
async def cancel_task(
    task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    task = await db.get(Task, task_id)
    if task:
        await require_task_control(request, task, db)
        await _require_no_pr_review_publication(db, task_id)
        await _require_not_pr_review_task_mutation(
            db,
            task_id,
            action="cancelled",
        )
    wt = await _worker_task_or_none(db, task_id)
    if wt is not None:
        observed = worker_task_generation(wt)
        if observed is None:
            raise HTTPException(409, "Task Worker assignment changed")
        result = await _proxy(wt, "POST", f"/api/tasks/{task_id}/cancel")
        return await _sync_task_from_worker_response(
            db,
            wt,
            result,
            observed=observed,
        )

    return await _finish_task_operation(_cancel_local_task_impl(task_id, db))


@router.post("/{task_id}/retry", response_model=TaskResponse)
async def retry_task(
    task_id: int,
    request: Request,
    body: TaskActionRequest | None = None,
    queue: TaskQueue = Depends(_get_queue),
    db: AsyncSession = Depends(get_db),
):
    task = await db.get(Task, task_id)
    if task:
        await require_task_control(request, task, db)
    # The operation lock is shared with TaskMigrator.  Keep it through the
    # remote response CAS/local retry commit and status publication, otherwise
    # migration can copy an old generation while retry is still in flight.
    await db.rollback()
    async with get_task_operation_lock(task_id):
        db.expire_all()
        current = await db.get(Task, task_id)
        if current is None:
            raise HTTPException(404, "Task not found")
        await require_task_control(request, current, db)
        _require_expected_task_routing(
            current,
            body.expected_routing if body is not None else None,
            effective_model=current.model,
        )
        if current.status not in _MANUAL_RETRYABLE_STATUSES:
            raise HTTPException(
                409,
                f"Task status {current.status} is not retryable",
            )
        await _require_no_pr_review_publication(db, task_id)
        await _require_pr_review_retryable(db, task_id)
        if current.pty_background_generation is not None:
            raise HTTPException(
                409,
                "Task still has active Claude PTY background output",
            )

        if current.worker_id is not None:
            await _ensure_worker_routing_ready(
                current,
                operation_lock_held=True,
            )
            observed = worker_task_generation(current)
            if observed is None:
                raise HTTPException(409, "Task Worker assignment changed")
            await _sync_worker_skill_selection_before_execution(current)
            await db.rollback()
            db.expire_all()
            current = await db.get(Task, task_id)
            if (
                current is None
                or worker_task_generation(
                    current,
                    expected_worker_id=observed.worker_id,
                )
                != observed
            ):
                raise HTTPException(
                    409,
                    "Task Worker generation changed while Skill selection was "
                    "being synchronized",
                )
            result = await _proxy(
                current,
                "POST",
                f"/api/tasks/{task_id}/retry",
                body=body.model_dump(mode="json") if body is not None else None,
                operation_lock_held=True,
            )
            return await _sync_task_from_worker_response(
                db,
                current,
                result,
                observed=observed,
            )

        _require_no_pending_worker_routing(current)
        retried = await _retry_local_task_safely(task_id, queue, db)
        if not retried:
            raise HTTPException(404, "Task not found")
        locked_task = await _lock_task_generation(
            task_id,
            db,
            expected_status=retried.status,
            expected_retry_count=retried.retry_count,
            expected_instance_id=retried.instance_id,
            expected_started_at=retried.started_at,
            expected_completed_at=retried.completed_at,
            expected_pty_background_generation=(retried.pty_background_generation),
        )
        if locked_task is None:
            raise HTTPException(
                409,
                "Task was claimed by a newer generation before retry publication",
            )
        from backend.services.task_events import broadcast_status_change

        await broadcast_status_change(task_id, retried.status)
        await db.commit()
        return locked_task


@router.post("/{task_id}/star", response_model=TaskResponse)
async def star_task(
    task_id: int,
    request: Request,
    queue: TaskQueue = Depends(_get_queue),
    db: AsyncSession = Depends(get_db),
):
    task = await db.get(Task, task_id)
    if task:
        await require_task_control(request, task, db)
    task = await queue.star(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    return task


@router.post("/{task_id}/read", response_model=TaskResponse)
async def mark_task_read(
    task_id: int,
    request: Request,
    queue: TaskQueue = Depends(_get_queue),
    db: AsyncSession = Depends(get_db),
):
    task = await db.get(Task, task_id)
    if task:
        await require_task_control(request, task, db)
    task = await queue.update_task(task_id, has_unread=False)
    if not task:
        raise HTTPException(404, "Task not found")
    return task


@router.post("/{task_id}/unread", response_model=TaskResponse)
async def mark_task_unread(
    task_id: int,
    request: Request,
    queue: TaskQueue = Depends(_get_queue),
    db: AsyncSession = Depends(get_db),
):
    task = await db.get(Task, task_id)
    if task:
        await require_task_control(request, task, db)
    task = await queue.update_task(task_id, has_unread=True)
    if not task:
        raise HTTPException(404, "Task not found")
    return task


@router.post("/{task_id}/archive", response_model=TaskResponse)
async def archive_task(
    task_id: int,
    request: Request,
    queue: TaskQueue = Depends(_get_queue),
    db: AsyncSession = Depends(get_db),
):
    task = await db.get(Task, task_id)
    if task:
        await require_task_control(request, task, db)
    task = await queue.archive(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    return task


@router.get("/queue/next", response_model=list[TaskResponse])
async def get_queue(
    request: Request,
    queue: TaskQueue = Depends(_get_queue),
):
    user_id = get_current_user_id(request)
    return await queue.list_tasks(
        status="pending",
        user_id=None if is_admin(request) else user_id,
    )


def _require_plan_review_operation(task: Task) -> None:
    if task.canonical_plan_id is not None:
        raise HTTPException(
            409,
            f"Legacy Plan Task has migrated to canonical Plan #{task.canonical_plan_id}",
        )
    if task.mode == "plan" and task.status == "superseded":
        successor_id = (task.metadata_ or {}).get("plan_superseded_by_task_id")
        detail = "Plan has been superseded"
        if isinstance(successor_id, int):
            detail += f" by Plan #{successor_id}"
        raise HTTPException(409, detail)
    if task.mode != "plan" or task.status != "plan_review":
        raise HTTPException(400, "Task is not in plan review state")


@router.post("/{task_id}/plan/approve", response_model=TaskResponse)
async def approve_plan(
    task_id: int,
    request: Request,
    body: PlanApprovalRequest | None = None,
    queue: TaskQueue = Depends(_get_queue),
    db: AsyncSession = Depends(get_db),
):
    """Approve an independent Plan without starting an Agent turn."""
    task = await queue.get(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    await require_task_control(request, task, db)
    await db.rollback()
    async with get_task_operation_lock(task_id):
        db.expire_all()
        current = await db.get(Task, task_id)
        if current is None:
            raise HTTPException(404, "Task not found")
        await require_task_control(request, current, db)
        _require_expected_task_routing(
            current,
            body.expected_routing if body is not None else None,
            effective_model=current.model,
        )
        _require_plan_review_operation(current)

        target = None
        if current.plan_target_task_id is not None:
            target = await db.get(Task, current.plan_target_task_id)
            if target is None:
                raise HTTPException(409, "Plan target no longer exists")
            await require_task_control(request, target, db)

        if current.worker_id is not None:
            approving_user_id = get_current_user_id(request)
            await _ensure_worker_routing_ready(
                current,
                operation_lock_held=True,
            )
            observed = worker_task_generation(current)
            if observed is None:
                raise HTTPException(409, "Task Worker assignment changed")
            result = await _proxy(
                current,
                "POST",
                f"/api/tasks/{task_id}/plan/approve",
                body=body.model_dump(mode="json") if body is not None else None,
                operation_lock_held=True,
            )
            approved = await _sync_task_from_worker_response(
                db,
                current,
                result,
                observed=observed,
            )
            # Worker authentication identifies the Manager service, not the
            # human who made this decision. Keep this Manager-local audit field
            # authoritative and never mirror a Worker-local user id.
            approved.plan_approved_by = approving_user_id
            await db.commit()
            await db.refresh(approved)
            return approved

        _require_no_pending_worker_routing(current)
        from backend.services.plan_tasks import plan_staleness

        stale = await plan_staleness(db, current, current_target=target)
        if stale["stale"] and not (body and body.confirm_stale):
            raise HTTPException(
                409,
                detail={
                    "message": "Plan context changed; confirm stale approval",
                    "staleness": stale,
                },
            )
        approved_at = datetime.utcnow()
        changed = await db.execute(
            sa_update(Task)
            .where(*_task_generation_fence(task_id, current))
            .values(
                plan_approved=True,
                plan_approved_at=approved_at,
                plan_approved_by=get_current_user_id(request),
                status="completed",
                completed_at=approved_at,
            )
        )
        if changed.rowcount != 1:
            await db.rollback()
            raise HTTPException(
                409,
                "Task generation changed while approving the plan",
            )
        await db.commit()
        db.expire_all()
        approved = await db.get(Task, task_id)
        if approved is None:
            raise HTTPException(409, "Task disappeared while approving the plan")
        from backend.services.task_events import broadcast_status_change

        await broadcast_status_change(task_id, "completed")
        return approved


@router.post("/{task_id}/plan/reject", response_model=TaskResponse)
async def reject_plan(
    task_id: int,
    request: Request,
    queue: TaskQueue = Depends(_get_queue),
    db: AsyncSession = Depends(get_db),
):
    """Reject a plan-mode task's plan."""
    task = await queue.get(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    await require_task_control(request, task, db)
    await db.rollback()
    async with get_task_operation_lock(task_id):
        db.expire_all()
        current = await db.get(Task, task_id)
        if current is None:
            raise HTTPException(404, "Task not found")
        await require_task_control(request, current, db)
        _require_plan_review_operation(current)

        if current.worker_id is not None:
            observed = worker_task_generation(current)
            if observed is None:
                raise HTTPException(409, "Task Worker assignment changed")
            result = await _proxy(
                current,
                "POST",
                f"/api/tasks/{task_id}/plan/reject",
                operation_lock_held=True,
            )
            return await _sync_task_from_worker_response(
                queue.db,
                current,
                result,
                observed=observed,
            )

        changed = await db.execute(
            sa_update(Task)
            .where(*_task_generation_fence(task_id, current))
            .values(
                plan_approved=False,
                status="cancelled",
                completed_at=datetime.utcnow(),
            )
        )
        if changed.rowcount != 1:
            await db.rollback()
            raise HTTPException(
                409,
                "Task generation changed while rejecting the plan",
            )
        await db.commit()
        db.expire_all()
        rejected = await db.get(Task, task_id)
        if rejected is None:
            raise HTTPException(409, "Task disappeared while rejecting the plan")
        from backend.services.task_events import broadcast_status_change

        await broadcast_status_change(task_id, "cancelled")
        return rejected
