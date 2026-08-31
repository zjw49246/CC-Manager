import asyncio
import hashlib
import hmac
import json
import logging
import re
import secrets
from datetime import datetime, timedelta
from types import SimpleNamespace
from weakref import WeakKeyDictionary

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from sqlalchemy import (
    and_,
    case,
    delete as sa_delete,
    desc,
    func,
    literal,
    or_,
    select,
    update as sa_update,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import aliased, load_only
from starlette.requests import ClientDisconnect

from backend.config import settings
from backend.database import get_db
from backend.models.delivery import DeliveryRun
from backend.models.pr_monitor import (
    MonitoredRepo,
    PRFinding,
    PRFindingAction,
    PRFindingRebuttal,
    PRMergeQueueAction,
    PRMonitorTaskTombstone,
    PRReview,
    PRReviewerRun,
    PRMonitorRun,
    PRRepairWake,
    pr_merge_queue_action_has_ambiguous_remote_effect,
    pr_merge_queue_action_ambiguous_remote_effect_predicate,
    pr_monitor_run_has_terminal_intent,
)
from backend.models.task import Task
from backend.pr_review_evidence import (
    PR_REVIEW_INPUT_ERROR_CATEGORY,
    pr_review_input_error_detail,
    valid_pr_review_input_error_evidence,
)
from backend.services.delivery_pr_policy import legacy_pr_effect_is_forbidden
from backend.services.pr_review_actions import (
    FindingActionConflict,
    lock_pr_repo_action_boundary,
)
from backend.api.deps import (
    get_current_user_id,
    is_admin,
    lock_project_effect_access,
    lock_project_worker_effect_access,
    lock_request_user_authority,
    lock_worker_effect_access,
    require_admin,
    require_project_access,
    require_worker_target_access,
    require_task_control,
)
from backend.schemas.pr_monitor import (
    MonitoredRepoCreate,
    MonitoredRepoUpdate,
    MonitoredRepoResponse,
    MonitoredRepoSecretResponse,
    PRReviewResponse,
    PRReviewDetailResponse,
    PRReviewRerunRequest,
    PRReviewRerunResponse,
    PRResultFeedItem,
    GitHubPublisherIdentityResponse,
    PRReviewerRunResponse,
    PRFindingResponse,
    PRFindingActionResponse,
    FindingActionRequest,
    HumanAdviceRequest,
    ConfirmFixRequest,
    PRFindingRebuttalCreate,
    PRFindingRebuttalResponse,
    PRMonitorBindRequest,
    PRMonitorBranchUpdateRequest,
    PRMonitorBranchUpdateResponse,
    PRMonitorRunResponse,
    PRMonitorReviewAttemptResponse,
    PRRepairWakeResponse,
    PRMergeActionResponse,
    required_checks_support_direct_auto_merge,
)

logger = logging.getLogger(__name__)

_GIT_COMMIT_SHA_RE = re.compile(r"[0-9a-fA-F]{40}\Z")
_PR_REVIEW_LIST_SUMMARY_MAX_BYTES = 2000
_MAX_GITHUB_REPO_FULL_NAME_CHARS = 200
# GitHub documents a hard 25 MB webhook payload ceiling.  Enforce the same
# boundary before JSON parsing, HMAC work, or a repository lookup so the
# unauthenticated endpoint never buffers an unbounded request.  The streaming
# check remains authoritative when Content-Length is absent or dishonest.
_MAX_GITHUB_WEBHOOK_BODY_BYTES = 25 * 1024 * 1024
_PR_SYNCHRONIZE_LOCKS: WeakKeyDictionary[
    asyncio.AbstractEventLoop,
    dict[int, asyncio.Lock],
] = WeakKeyDictionary()
_DELIVERY_REPO_FROZEN_FIELDS = frozenset(
    {
        "project_id",
        "enabled",
        "auto_merge",
        "provider",
        "review_model",
        "review_effort",
        "review_mode",
        "wait_for_ci",
        "required_checks",
        "auto_repair",
        "max_repair_attempts",
        "merge_queue_mode",
        "default_branch",
        "allowed_authors",
    }
)


def _pr_repo_write_lock(repo_id: int) -> asyncio.Lock:
    """Serialize one monitor's webhook/delete barrier in this process."""

    loop = asyncio.get_running_loop()
    locks = _PR_SYNCHRONIZE_LOCKS.setdefault(loop, {})
    return locks.setdefault(repo_id, asyncio.Lock())


def _lowercase_hex_sql_remainder(column):
    """Return SQL text left after removing only lowercase hexadecimal."""

    remainder = column
    for character in "0123456789abcdef":
        remainder = func.replace(remainder, character, "")
    return remainder


async def _record_pr_monitor_task_tombstones(
    db: AsyncSession,
    task_ids,
) -> None:
    """Idempotently preserve internal Task identities in this transaction."""

    canonical_ids = sorted({
        task_id
        for task_id in task_ids
        if isinstance(task_id, int) and not isinstance(task_id, bool)
    })
    if not canonical_ids:
        return
    values = [{"task_id": task_id} for task_id in canonical_ids]
    dialect = db.get_bind().dialect.name
    if dialect == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as dialect_insert

        statement = dialect_insert(PRMonitorTaskTombstone).values(values)
        await db.execute(statement.on_conflict_do_nothing(
            index_elements=[PRMonitorTaskTombstone.task_id]
        ))
        return
    if dialect == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as dialect_insert

        statement = dialect_insert(PRMonitorTaskTombstone).values(values)
        await db.execute(statement.on_conflict_do_nothing(
            index_elements=[PRMonitorTaskTombstone.task_id]
        ))
        return
    if dialect in {"mysql", "mariadb"}:
        from sqlalchemy.dialects.mysql import insert as dialect_insert

        statement = dialect_insert(PRMonitorTaskTombstone).values(values)
        await db.execute(statement.on_duplicate_key_update(
            task_id=statement.inserted.task_id
        ))
        return

    # Keep an explicit fallback for supported SQLAlchemy test dialects.  The
    # production dialects above use atomic conflict handling.
    existing_ids = set((await db.execute(
        select(PRMonitorTaskTombstone.task_id).where(
            PRMonitorTaskTombstone.task_id.in_(canonical_ids)
        )
    )).scalars())
    db.add_all(
        PRMonitorTaskTombstone(task_id=task_id)
        for task_id in canonical_ids
        if task_id not in existing_ids
    )
    await db.flush()


async def _delivery_repo_run_reference(
    db: AsyncSession,
    *,
    repo_id: int,
    active_only: bool,
) -> int | None:
    """Return a Run whose durable Delivery scope owns this monitor."""

    from backend.models.delivery import DeliveryRun

    statement = select(DeliveryRun.id).where(
        DeliveryRun.monitored_repo_id == repo_id,
    )
    if active_only:
        statement = statement.where(DeliveryRun.activity != "terminal")
    return (await db.execute(statement.limit(1).with_for_update())).scalar_one_or_none()


async def _require_legacy_pr_effect_allowed(
    db: AsyncSession,
    *,
    action: str,
    review: PRReview | None = None,
    monitor_run: PRMonitorRun | None = None,
    task: Task | None = None,
    allow_terminal_intent: bool = False,
) -> None:
    """Reject legacy mutation of a Delivery-owned or terminalizing lifecycle."""

    if monitor_run is None and review is not None and review.monitor_run_id is not None:
        monitor_run = await db.get(PRMonitorRun, review.monitor_run_id)
    if not allow_terminal_intent and pr_monitor_run_has_terminal_intent(monitor_run):
        raise HTTPException(
            409,
            f"PR terminal lifecycle is pending; it cannot be {action}",
        )

    if await legacy_pr_effect_is_forbidden(
        db,
        review=review,
        monitor_run=monitor_run,
        task=task,
    ):
        raise HTTPException(
            409,
            f"Delivery-owned PR state cannot be {action} through legacy "
            "PR Monitor controls",
        )


def _action_response_payload(action: PRFindingAction) -> dict:
    """Expose repair metadata without leaking the stored patch or nonce."""

    payload = PRFindingActionResponse.model_validate(action).model_dump()
    raw_result = dict(action.result or {})
    payload["result"] = {
        key: value
        for key, value in raw_result.items()
        if key not in {
            "patch",
            "confirmation_token",
            "action_nonce",
            "push_owner_token",
        }
    } or None
    if (
        action.status == "awaiting_confirmation"
        and action.confirmed_at is None
        and action.task_id is not None
    ):
        payload["diff_download_url"] = f"/api/pr-monitor/actions/{action.id}/diff"
    return payload


def _parse_commit_sha(value: object, field_name: str) -> str:
    """Return a canonical webhook commit SHA or reject the signed payload."""
    if not isinstance(value, str) or _GIT_COMMIT_SHA_RE.fullmatch(value) is None:
        raise HTTPException(
            400,
            f"pull_request.{field_name}.sha must be exactly 40 hexadecimal characters",
        )
    return value.lower()


def _require_current_webhook_signature(
    repo: MonitoredRepo,
    *,
    body: bytes | bytearray,
    signature_header: str,
) -> None:
    """Verify the delivery against the exact locked monitor generation."""

    if not signature_header.startswith("sha256="):
        raise HTTPException(403, "Missing or invalid signature")
    expected_sig = "sha256=" + hmac.new(
        repo.webhook_secret.encode(),
        body,
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(signature_header, expected_sig):
        raise HTTPException(403, "Invalid signature")


def _webhook_policy_rejection(
    repo: MonitoredRepo,
    *,
    base_branch: str,
    pr_author: str,
    publisher_actor: str | None,
) -> str | None:
    """Return why a signed PR is outside one captured monitor policy.

    This check is deliberately synchronous.  Callers prefetch the bounded
    service actor only after releasing their DB transaction, then reuse that
    scalar during locked rechecks so no network/subprocess await can hold a
    connection or repository row lock.
    """

    if base_branch != repo.default_branch:
        return f"target branch: {base_branch}"
    allowed = repo.allowed_authors or []
    canonical_author = pr_author.casefold()
    canonical_allowed = {
        item.casefold() for item in allowed if isinstance(item, str)
    }
    if allowed and canonical_author not in canonical_allowed:
        return f"author not allowed: {pr_author}"
    own_login = publisher_actor or ""
    if (
        own_login
        and canonical_author == own_login.casefold()
        and canonical_author not in canonical_allowed
    ):
        return f"self PR (gh login: {own_login})"
    return None


def _bounded_github_publisher_actor(status: object) -> str | None:
    """Project identity status to the only scalar webhook policy consumes."""

    actor = status.get("actor") if isinstance(status, dict) else None
    if (
        not isinstance(actor, str)
        or not actor
        or len(actor) > 200
        or any(character in actor for character in ("\x00", "\n", "\r"))
    ):
        return None
    return actor


def _webhook_repo_snapshot(repo: MonitoredRepo) -> SimpleNamespace:
    """Copy fields needed across transaction-free webhook external I/O."""

    return SimpleNamespace(
        id=repo.id,
        repo_full_name=repo.repo_full_name,
        enabled=repo.enabled,
        default_branch=repo.default_branch,
        allowed_authors=list(repo.allowed_authors or []),
        auto_merge=bool(repo.auto_merge),
        provider=repo.provider,
        review_mode=repo.review_mode,
        wait_for_ci=bool(repo.wait_for_ci),
        required_checks=list(repo.required_checks or []),
    )


async def _capture_webhook_ci_evidence(
    repo,
    *,
    head_sha: str,
) -> dict | None:
    """Fetch CI only when the captured policy requires a panel gate."""

    if (repo.review_mode or "single") != "panel" or not repo.wait_for_ci:
        return None
    from backend.services.pr_review_panel import capture_exact_head_ci_evidence

    return await capture_exact_head_ci_evidence(
        repo.repo_full_name,
        head_sha,
        repo.required_checks,
    )


def _require_locked_ci_evidence(
    repo: MonitoredRepo,
    *,
    head_sha: str,
    evidence: dict | None,
) -> dict | None:
    """Bind lock-free CI data to the latest locked admission policy."""

    if (repo.review_mode or "single") != "panel" or not repo.wait_for_ci:
        return None
    from backend.services.pr_review_panel import validated_exact_head_ci_evidence

    try:
        validated_exact_head_ci_evidence(
            evidence,
            repo_name=repo.repo_full_name,
            head_sha=head_sha,
            required_checks=repo.required_checks,
        )
    except ValueError as exc:
        raise HTTPException(
            409,
            "PR Monitor CI policy changed during remote verification; retry "
            "the exact-head admission",
        ) from exc
    return evidence


def _pr_monitor_run_generation_value(run: PRMonitorRun | None) -> tuple | None:
    if run is None:
        return None
    return (
        run.id,
        run.state_version,
        run.status,
        run.current_review_id,
        run.current_base_sha,
        run.current_head_sha,
        run.completed_at,
        run.terminal_intent_status,
        run.terminal_intent_base_ref,
        run.terminal_intent_head_sha,
        run.terminal_intent_delivery_id,
        run.terminal_intent_observed_at,
        run.terminal_intent_checked_at,
        run.legacy_terminal_recovery_pending,
    )


async def _pr_monitor_run_generation(
    db: AsyncSession,
    *,
    repo_id: int,
    pr_number: int,
    lock: bool = False,
) -> tuple | None:
    """Read the exact local lifecycle generation around lock-free remote I/O."""

    statement = select(PRMonitorRun).where(
        PRMonitorRun.repo_id == repo_id,
        PRMonitorRun.pr_number == pr_number,
    )
    if lock:
        statement = statement.with_for_update()
    run = (await db.execute(statement)).scalar_one_or_none()
    return _pr_monitor_run_generation_value(run)


router = APIRouter(prefix="/api/pr-monitor", tags=["pr-monitor"])
webhook_router = APIRouter(prefix="/api/github", tags=["pr-monitor"])


async def _find_processed_review(
    db: AsyncSession,
    repo_id: int,
    pr_number: int,
    base_ref: str,
    base_sha: str,
    head_sha: str,
    delivery_id: str | None,
) -> PRReview | None:
    """Find an existing review for this snapshot or exact webhook delivery."""
    duplicate_keys = [
        and_(
            PRReview.repo_id == repo_id,
            PRReview.pr_number == pr_number,
            PRReview.base_ref == base_ref,
            PRReview.base_sha == base_sha,
            PRReview.head_sha == head_sha,
        )
    ]
    if delivery_id:
        duplicate_keys.append(
            and_(
                PRReview.repo_id == repo_id,
                PRReview.delivery_id == delivery_id,
            )
        )

    result = await db.execute(
        select(PRReview)
        .where(or_(*duplicate_keys))
        .order_by(desc(PRReview.id))
        .limit(1)
    )
    return result.scalar_one_or_none()


def _duplicate_review_response(
    review: PRReview,
    delivery_id: str | None,
) -> dict:
    same_delivery = bool(delivery_id and review.delivery_id == delivery_id)
    return {
        "status": "ignored",
        "reason": (
            "webhook delivery already processed"
            if same_delivery
            else "PR snapshot already reviewed"
        ),
        "review_id": review.id,
    }


async def _prepare_pr_review_context_or_422(
    db: AsyncSession,
    repo: MonitoredRepo,
    pr_data: dict,
    *,
    base_ref: str | None = None,
) -> dict:
    """Prepare immutable review input with a stable public size rejection."""

    from backend.services.pr_review_service import (
        PRReviewInputTooLarge,
        prepare_pr_review_context,
    )

    try:
        if base_ref is None:
            return await prepare_pr_review_context(repo, pr_data)
        return await prepare_pr_review_context(repo, pr_data, base_ref=base_ref)
    except PRReviewInputTooLarge as exc:
        await db.rollback()
        raise HTTPException(status_code=422, detail=exc.public_detail) from exc


async def _capture_pr_review_context_rejection(
    repo: MonitoredRepo,
    pr_data: dict,
    *,
    base_ref: str | None = None,
) -> tuple[dict | None, object | None]:
    """Return immutable input or one structured deterministic size failure."""

    from backend.services.pr_review_service import (
        PRReviewInputTooLarge,
        prepare_pr_review_context,
    )

    try:
        if base_ref is None:
            return await prepare_pr_review_context(repo, pr_data), None
        return await prepare_pr_review_context(repo, pr_data, base_ref=base_ref), None
    except PRReviewInputTooLarge as exc:
        context = getattr(exc, "prepared_context", None)
        return context if isinstance(context, dict) else None, exc


async def _create_pr_review_task_or_422(
    db: AsyncSession,
    repo: MonitoredRepo,
    pr_data: dict,
    *,
    prepared_context: dict,
    prepared_ci_evidence: dict | None = None,
    allow_remote_ci: bool = True,
    allow_terminal_reactivation: bool = False,
):
    """Create a review while keeping deterministic input limits client-visible."""

    from backend.services.pr_review_service import (
        PRReviewInputTooLarge,
        PRReviewLifecycleConflict,
        create_pr_review_task,
    )

    try:
        return await create_pr_review_task(
            db,
            repo,
            pr_data,
            prepared_context=prepared_context,
            prepared_ci_evidence=prepared_ci_evidence,
            allow_remote_ci=allow_remote_ci,
            allow_terminal_reactivation=allow_terminal_reactivation,
        )
    except PRReviewInputTooLarge as exc:
        await db.rollback()
        raise HTTPException(status_code=422, detail=exc.public_detail) from exc
    except PRReviewLifecycleConflict as exc:
        await db.rollback()
        raise HTTPException(
            status_code=(503 if allow_terminal_reactivation else 409),
            detail=str(exc),
        ) from exc


async def _preflight_pr_review_prompts_or_422(
    db: AsyncSession,
    repo: MonitoredRepo,
    pr_data: dict,
    *,
    prepared_context: dict,
) -> None:
    """Revalidate prompt budgets against repository policy under its lock."""

    from backend.services.pr_review_service import (
        PRReviewInputTooLarge,
        preflight_pr_review_prompts,
    )

    try:
        preflight_pr_review_prompts(
            repo,
            pr_data,
            prepared_context=prepared_context,
        )
    except PRReviewInputTooLarge as exc:
        await db.rollback()
        raise HTTPException(status_code=422, detail=exc.public_detail) from exc


def _capture_pr_review_preflight_rejection(
    repo: MonitoredRepo,
    pr_data: dict,
    *,
    prepared_context: dict,
) -> object | None:
    """Return a structured locked-policy rejection without releasing locks."""

    from backend.services.pr_review_service import (
        PRReviewInputTooLarge,
        preflight_pr_review_prompts,
    )

    try:
        preflight_pr_review_prompts(
            repo,
            pr_data,
            prepared_context=prepared_context,
        )
    except PRReviewInputTooLarge as exc:
        return exc
    return None


def _is_pr_review_input_rejection(review: PRReview) -> bool:
    """Validate the complete structured shape before exposing a 422 receipt."""

    evidence = _public_input_error_evidence(review)
    if evidence["error_category"] != PR_REVIEW_INPUT_ERROR_CATEGORY:
        return False
    canonical_detail = pr_review_input_error_detail(
        measured=evidence["error_measured"],
        limit=evidence["error_limit"],
        unit=evidence["error_unit"],
    )
    return bool(
        review.status == "error"
        and review.action_taken == "error"
        and review.code_verdict is None
        and review.publication_state == "not_applicable"
        and review.failure_stage == "reviewer"
        and review.review_summary == canonical_detail
    )


def _pr_review_input_rejection_response(review: PRReview) -> JSONResponse:
    """Return a stable webhook receipt for first delivery and redelivery."""

    if not _is_pr_review_input_rejection(review):
        raise HTTPException(500, "PR review input rejection evidence is incomplete")
    evidence = _public_input_error_evidence(review)
    detail = pr_review_input_error_detail(
        measured=evidence["error_measured"],
        limit=evidence["error_limit"],
        unit=evidence["error_unit"],
    )
    return JSONResponse(
        status_code=422,
        content={
            "detail": detail,
            "error_category": evidence["error_category"],
            "measured": evidence["error_measured"],
            "limit": evidence["error_limit"],
            "unit": evidence["error_unit"],
            "review_id": review.id,
        },
    )


@router.get("/webhook-info")
async def webhook_info():
    """Return the public webhook URL (from PUBLIC_BASE_URL), or null if unset."""
    base = settings.public_base_url.strip().rstrip("/")
    return {"webhook_url": f"{base}/api/github/webhook" if base else None}


async def _cached_github_publisher_identity(*, force: bool = False) -> dict:
    """Use the service-wide actor source shared with publication/self policy."""

    from backend.services.pr_review_service import (
        _github_publisher_identity_status,
    )

    return await _github_publisher_identity_status(force=force)


def _reset_github_identity_status_cache() -> None:
    """Clear process-local identity state (used by tests and auth rotation)."""

    from backend.services.pr_review_service import (
        _reset_github_publisher_identity_cache,
    )

    _reset_github_publisher_identity_cache()


@router.get(
    "/github-identity",
    response_model=GitHubPublisherIdentityResponse,
)
async def github_publisher_identity(
    request: Request,
    repo_id: int = Query(..., gt=0),
    refresh: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    repo = await db.get(MonitoredRepo, repo_id)
    if repo is None:
        raise HTTPException(404, "Repository not found")
    await _require_pr_monitor_access(request, db, repo)
    if refresh and not is_admin(request):
        raise HTTPException(
            403,
            "Only administrators may force a GitHub identity refresh",
        )
    # Identity lookup can spend up to the bounded gh subprocess timeout. ACL
    # reads are complete, so release the request transaction/connection before
    # waiting on external I/O; otherwise concurrent status cards could exhaust
    # the application DB pool and even starve stop/control requests.
    await db.rollback()
    status = await _cached_github_publisher_identity(force=refresh)
    return {
        **status,
        "available": status["actor"] is not None,
    }


@router.get("/repos", response_model=list[MonitoredRepoResponse])
async def list_repos(request: Request, db: AsyncSession = Depends(get_db)):
    stmt = select(MonitoredRepo).order_by(desc(MonitoredRepo.created_at))
    if not is_admin(request):
        user_id = get_current_user_id(request)
        if user_id is None:
            stmt = stmt.where(False)
        else:
            # A Worker owner administers compute capacity only. Repository
            # visibility follows the Project ACL, and projectless monitors
            # remain administrator-only because they have no durable owner.
            project_ids = await _member_visible_pr_project_ids(db, user_id)
            stmt = stmt.where(
                MonitoredRepo.project_id.in_(project_ids)
                if project_ids else False
            )
    result = await db.execute(stmt)
    return result.scalars().all()


async def _member_visible_pr_project_ids(
    db: AsyncSession,
    user_id: int,
) -> list[int]:
    """Resolve share ACLs while excluding Manager-owned internal Projects."""

    from backend.models.project import Project, project_is_internal
    from backend.models.team_share import TeamProjectShare
    from backend.models.user_group import UserGroupMember

    group_ids = select(UserGroupMember.group_id).where(
        UserGroupMember.user_id == user_id
    )
    shared_ids = list((await db.execute(
        select(TeamProjectShare.project_id)
        .where(
            (
                (TeamProjectShare.target_type == "user")
                & (TeamProjectShare.target_id == user_id)
            )
            | (
                (TeamProjectShare.target_type == "group")
                & TeamProjectShare.target_id.in_(group_ids)
            )
        )
        .distinct()
    )).scalars())
    if not shared_ids:
        return []
    projects = list((await db.execute(
        select(Project).where(Project.id.in_(shared_ids))
    )).scalars())
    return sorted(
        project.id for project in projects if not project_is_internal(project)
    )


async def _require_pr_monitor_access(
    request: Request,
    db: AsyncSession,
    repo: MonitoredRepo,
) -> None:
    """Authorize monitor data independently from its compute location."""

    if is_admin(request):
        return
    if repo.project_id is None:
        raise HTTPException(403, "No access to this PR monitor")
    await require_project_access(request, repo.project_id, db)


async def _reauthorize_pr_effect(
    request: Request,
    db: AsyncSession,
    repo: MonitoredRepo,
) -> None:
    """Revalidate monitor ACL under the Project share writer fence.

    Finding actions first serialize on their MonitoredRepo.  Project-derived
    member authority can still be revoked independently, so the effect must
    also take the Project row boundary used by TeamProjectShare add/remove and
    re-check access after any wait.  Projectless monitors remain admin-only;
    after the repository boundary they also fence the mutable JWT User row.
    """

    if repo.project_id is not None:
        try:
            await lock_project_effect_access(request, repo.project_id, db)
        except HTTPException as exc:
            if exc.status_code == 404:
                raise HTTPException(
                    409,
                    "PR monitor Project is no longer available",
                ) from exc
            raise
        return
    await _require_pr_monitor_access(request, db, repo)
    await lock_request_user_authority(request, db)


async def _reauthorize_pr_topology_effect(
    request: Request,
    db: AsyncSession,
    repo: MonitoredRepo,
    *,
    target_project_id: int | None,
) -> None:
    """Fence an administrator-only monitor Project move deterministically.

    The repository row is already locked.  A move can involve both the old
    and new Projects, so lock every Project in numeric order before the User
    row.  Calling the single-Project helper twice would instead produce
    ``old Project -> User -> new Project`` and let two opposite moves deadlock.
    """

    require_admin(request)
    from backend.services.project_share_admission import (
        ProjectShareAdmissionError,
        lock_project_share_authority,
    )

    project_ids = sorted(
        {
            project_id
            for project_id in (repo.project_id, target_project_id)
            if project_id is not None
        }
    )
    for project_id in project_ids:
        try:
            await lock_project_share_authority(db, project_id)
        except ProjectShareAdmissionError as exc:
            raise HTTPException(409, str(exc)) from exc
        except ValueError as exc:
            if project_id == target_project_id:
                raise HTTPException(404, "Project not found") from exc
            raise HTTPException(
                409,
                "PR monitor Project is no longer available",
            ) from exc
    await lock_request_user_authority(request, db)


def _pr_effect_authorizer(request: Request):
    async def authorize(db: AsyncSession, repo: MonitoredRepo) -> None:
        await _reauthorize_pr_effect(request, db, repo)

    return authorize


_ACTIVE_REVIEW_STATUSES = (
    "pending",
    "waiting_ci",
    "reviewing",
    "publishing",
    "superseding",
)
_ACTIVE_ADJUDICATION_STATUSES = ("pending", "adjudicating", "accepted")
_ACTIVE_FINDING_ACTION_STATUSES = (
    "pending",
    "running",
    "awaiting_confirmation",
    "cancelling",
)
_STARTED_REPAIR_STATUSES = (
    "delivering",
    "accepted",
    "awaiting_push",
    "running",  # compatibility with rows written by older deployments
)
# Includes an armed direct action before its first lease claim. The historical
# name remains for compatibility with Queue recovery predicates.
_STARTED_MERGE_QUEUE_STATUSES = ("pending", "enqueuing", "queued", "checking")
_EXTERNALLY_BUSY_RUN_STATUSES = ("resolving_fixed_threads", "repair_migrating")


async def _active_finding_action_for_repo(
    db: AsyncSession,
    repo_id: int,
) -> int | None:
    """Lock and return any action that still owns a Finding side effect."""

    return (await db.execute(
        select(PRFindingAction.id)
        .join(PRFinding, PRFinding.id == PRFindingAction.finding_id)
        .join(PRReview, PRReview.id == PRFinding.pr_review_id)
        .where(
            PRReview.repo_id == repo_id,
            or_(
                PRFindingAction.active_fix_finding_id.is_not(None),
                PRFindingAction.status.in_(_ACTIVE_FINDING_ACTION_STATUSES),
            ),
        )
        .order_by(PRFindingAction.id.asc())
        .limit(1)
        .with_for_update()
    )).scalar_one_or_none()


async def _rerun_blocked_run_ids(
    db: AsyncSession,
    monitor_runs: list[PRMonitorRun],
) -> set[int]:
    """Batch-project side-effect ownership into an honest rerun affordance."""

    if not monitor_runs:
        return set()
    run_ids = {item.id for item in monitor_runs}
    repo_ids = {item.repo_id for item in monitor_runs}
    blocked = {
        item.id
        for item in monitor_runs
        if (
            item.status in _EXTERNALLY_BUSY_RUN_STATUSES
            or pr_monitor_run_has_terminal_intent(item)
        )
    }

    active_fix_repos = set((await db.execute(
        select(PRReview.repo_id)
        .select_from(PRFindingAction)
        .join(PRFinding, PRFinding.id == PRFindingAction.finding_id)
        .join(PRReview, PRReview.id == PRFinding.pr_review_id)
        .where(
            PRReview.repo_id.in_(repo_ids),
            or_(
                PRFindingAction.active_fix_finding_id.is_not(None),
                PRFindingAction.status.in_(_ACTIVE_FINDING_ACTION_STATUSES),
            ),
        )
        .distinct()
    )).scalars())
    blocked.update(
        item.id for item in monitor_runs if item.repo_id in active_fix_repos
    )

    # Delivery-owned reviewer generations are visible only through Delivery
    # and can never be re-admitted by the ordinary PR Monitor rerun API. Use
    # exact durable ownership edges; repo/PR alone is intentionally reusable.
    blocked.update((await db.execute(
        select(DeliveryRun.pr_monitor_run_id)
        .where(DeliveryRun.pr_monitor_run_id.in_(run_ids))
    )).scalars())
    blocked.update((await db.execute(
        select(PRReview.monitor_run_id)
        .outerjoin(Task, Task.id == PRReview.task_id)
        .where(
            PRReview.monitor_run_id.in_(run_ids),
            or_(
                PRReview.delivery_id.like("delivery:%"),
                Task.mode == "delivery_loop",
                Task.delivery_run_id.is_not(None),
            ),
        )
        .distinct()
    )).scalars())

    for model, statuses in (
        (PRFindingRebuttal, _ACTIVE_ADJUDICATION_STATUSES),
        (PRRepairWake, _STARTED_REPAIR_STATUSES),
    ):
        blocked.update((await db.execute(
            select(model.monitor_run_id)
            .where(
                model.monitor_run_id.in_(run_ids),
                model.status.in_(statuses),
            )
            .distinct()
        )).scalars())

    blocked.update((await db.execute(
        select(PRMergeQueueAction.monitor_run_id)
        .where(
            PRMergeQueueAction.monitor_run_id.in_(run_ids),
            or_(
                PRMergeQueueAction.status.in_(
                    _STARTED_MERGE_QUEUE_STATUSES
                ),
                pr_merge_queue_action_ambiguous_remote_effect_predicate(),
            ),
        )
        .distinct()
    )).scalars())

    # Resolution claims are durable on the Finding itself.  The Run status is
    # normally updated in the same protocol, but checking the lease prevents
    # a crash between those writes from presenting a misleading rerun button.
    blocked.update((await db.execute(
        select(PRReview.monitor_run_id)
        .select_from(PRFinding)
        .join(PRReview, PRReview.id == PRFinding.pr_review_id)
        .where(
            PRReview.monitor_run_id.in_(run_ids),
            PRFinding.resolution_lease_token.is_not(None),
        )
        .distinct()
    )).scalars())
    return {int(item) for item in blocked if item is not None}


async def _quiesce_monitor_runs(
    db: AsyncSession,
    *,
    repo_id: int,
    run_id: int | None = None,
    reason: str,
    terminal_reconciliation: bool = False,
) -> None:
    """Atomically stop undispatched effects or reject an in-flight effect.

    The caller must hold the repository row lock.  Pending Repair and Merge
    Queue rows have not crossed an external-effect boundary and can therefore
    be withdrawn.  Once a Task/publication/queue operation has started we
    fail closed instead of presenting a pause/disable control that did not
    actually stop work.
    """

    run_filter = (
        PRMonitorRun.id == run_id
        if run_id is not None
        else PRMonitorRun.repo_id == repo_id
    )
    runs = list((await db.execute(
        select(PRMonitorRun).where(run_filter).with_for_update()
    )).scalars())
    if run_id is not None and not runs:
        raise HTTPException(404, "PR Monitor Run not found")
    if run_id is not None and (
        runs[0].status in {"merged", "closed"} or runs[0].completed_at is not None
    ):
        raise HTTPException(409, "Cannot pause a terminal PR Monitor Run")
    if run_id is not None:
        await _require_legacy_pr_effect_allowed(
            db,
            action="paused",
            monitor_run=runs[0],
            allow_terminal_intent=terminal_reconciliation,
        )
    run_ids = [item.id for item in runs]

    review_filter = PRReview.repo_id == repo_id
    if run_id is not None:
        current_review_ids = [
            item.current_review_id for item in runs if item.current_review_id is not None
        ]
        review_filter = or_(
            PRReview.monitor_run_id == run_id,
            PRReview.id.in_(current_review_ids) if current_review_ids else False,
        )
    if terminal_reconciliation:
        undispatched = list((await db.execute(
            select(PRReview)
            .where(
                review_filter,
                PRReview.status.in_(("pending", "waiting_ci")),
            )
            .with_for_update()
        )).scalars())
        if undispatched:
            review_ids = [item.id for item in undispatched]
            dispatched_review_ids = set((await db.execute(
                select(PRReviewerRun.pr_review_id)
                .where(
                    PRReviewerRun.pr_review_id.in_(review_ids),
                    PRReviewerRun.task_id.is_not(None),
                )
                .distinct()
            )).scalars())
            now = datetime.utcnow()
            for item in undispatched:
                if item.task_id is not None or item.id in dispatched_review_ids:
                    continue
                item.status = "cancelled"
                item.completed_at = now
                item.publication_state = "not_applicable"
                item.failure_stage = "lifecycle"
                item.review_summary = (
                    "PR reached a terminal lifecycle before review dispatch"
                )
    active_review = (await db.execute(
        select(PRReview.id)
        .where(review_filter, PRReview.status.in_(_ACTIVE_REVIEW_STATUSES))
        .limit(1)
        .with_for_update()
    )).scalar_one_or_none()

    if terminal_reconciliation and run_ids:
        # A signed, remote-verified merged intent is immutable. If its exact
        # direct outbox no longer has a live lease, consume that evidence here
        # so the terminal webhook can atomically close both projections. Queue
        # actions, closed intents, mismatched heads, and live publishers remain
        # behind the active-effect fence below.
        from backend.services.pr_direct_merge import (
            _database_now,
            _direct_merge_has_exact_merged_intent,
        )

        current_review_ids = [
            item.current_review_id
            for item in runs
            if item.current_review_id is not None
        ]
        current_reviews = (
            list((await db.execute(
                select(PRReview)
                .where(PRReview.id.in_(current_review_ids))
                .with_for_update()
            )).scalars())
            if current_review_ids
            else []
        )
        reviews_by_id = {item.id: item for item in current_reviews}
        runs_by_id = {item.id: item for item in runs}
        direct_actions = list((await db.execute(
            select(PRMergeQueueAction)
            .where(
                PRMergeQueueAction.monitor_run_id.in_(run_ids),
                PRMergeQueueAction.effect_kind == "direct",
                PRMergeQueueAction.status.in_(("pending", "enqueuing")),
            )
            .with_for_update()
        )).scalars())
        db_now = await _database_now(db)
        for action in direct_actions:
            run = runs_by_id.get(action.monitor_run_id)
            review = reviews_by_id.get(action.review_id)
            live_lease = bool(
                action.lease_token is not None
                and (
                    action.lease_expires_at is None
                    or action.lease_expires_at > db_now
                )
            )
            if (
                run is None
                or review is None
                or live_lease
                or not _direct_merge_has_exact_merged_intent(
                    run=run,
                    review=review,
                    action=action,
                )
            ):
                continue
            action.status = "merged"
            action.last_error = None
            action.completed_at = db_now
            action.lease_token = None
            action.lease_expires_at = None

    active_fix_action = await _active_finding_action_for_repo(db, repo_id)
    active_adjudication = active_repair = active_merge = active_resolution = None
    if run_ids:
        active_adjudication = (await db.execute(
            select(PRFindingRebuttal.id)
            .where(
                PRFindingRebuttal.monitor_run_id.in_(run_ids),
                PRFindingRebuttal.status.in_(_ACTIVE_ADJUDICATION_STATUSES),
            )
            .limit(1)
            .with_for_update()
        )).scalar_one_or_none()
        active_repair = (await db.execute(
            select(PRRepairWake.id)
            .where(
                PRRepairWake.monitor_run_id.in_(run_ids),
                PRRepairWake.status.in_(_STARTED_REPAIR_STATUSES),
            )
            .limit(1)
            .with_for_update()
        )).scalar_one_or_none()
        active_merge = (await db.execute(
            select(PRMergeQueueAction.id)
            .where(
                PRMergeQueueAction.monitor_run_id.in_(run_ids),
                or_(
                    PRMergeQueueAction.status.in_(
                        _STARTED_MERGE_QUEUE_STATUSES
                    ),
                    pr_merge_queue_action_ambiguous_remote_effect_predicate(),
                ),
            )
            .limit(1)
            .with_for_update()
        )).scalar_one_or_none()
        active_resolution = (await db.execute(
            select(PRFinding.id)
            .join(PRReview, PRReview.id == PRFinding.pr_review_id)
            .where(
                PRReview.monitor_run_id.in_(run_ids),
                PRFinding.resolution_lease_token.is_not(None),
            )
            .limit(1)
            .with_for_update()
        )).scalar_one_or_none()

    # ``repair_migrating`` is the only durable marker while TaskMigrator owns
    # execution state outside this transaction.  It must remain fail-closed
    # even for a signed terminal webhook; the migration owner will observe the
    # intent, supersede its Wake, and move the Run out of this state after the
    # external operation settles.  ``resolving_fixed_threads`` is different:
    # once no Finding lease or active rebuttal exists, the Run lock plus the
    # terminal-intent fence on every new claim make it safe to quiesce here.
    busy_run = next(
        (
            item
            for item in runs
            if item.status == "repair_migrating"
            or (
                not terminal_reconciliation
                and item.status == "resolving_fixed_threads"
            )
        ),
        None,
    )
    if any((
        active_review,
        active_fix_action,
        active_adjudication,
        active_repair,
        active_merge,
        active_resolution,
        busy_run,
    )):
        raise HTTPException(
            409,
            "Cannot pause PR Monitor while review, Finding repair, "
            "adjudication, Repair, thread resolution, or merge work is active",
        )

    if run_ids:
        await db.execute(
            sa_update(PRRepairWake)
            .where(
                PRRepairWake.monitor_run_id.in_(run_ids),
                PRRepairWake.status == "pending",
            )
            .values(status="shadow", last_error=reason)
        )
        await db.execute(
            sa_update(PRMergeQueueAction)
            .where(
                PRMergeQueueAction.monitor_run_id.in_(run_ids),
                PRMergeQueueAction.status == "pending",
            )
            .values(status="paused", last_error=reason)
        )
    for run in runs:
        if run.status not in {"merged", "closed"}:
            run.status = "paused"
            run.pause_reason = reason
            run.state_version += 1


async def _withdraw_pending_repairs(db: AsyncSession, *, repo_id: int) -> None:
    """Withdraw automatic Repair work before disabling that policy."""

    runs = list((await db.execute(
        select(PRMonitorRun)
        .where(PRMonitorRun.repo_id == repo_id)
        .with_for_update()
    )).scalars())
    run_ids = [item.id for item in runs]
    if not run_ids:
        return
    active = (await db.execute(
        select(PRRepairWake.id)
        .where(
            PRRepairWake.monitor_run_id.in_(run_ids),
            PRRepairWake.status.in_(_STARTED_REPAIR_STATUSES),
        )
        .limit(1)
        .with_for_update()
    )).scalar_one_or_none()
    if active is not None or any(
        item.status == "repair_migrating" for item in runs
    ):
        raise HTTPException(409, "Cannot disable automatic Repair while Repair work is active")
    await db.execute(
        sa_update(PRRepairWake)
        .where(
            PRRepairWake.monitor_run_id.in_(run_ids),
            PRRepairWake.status == "pending",
        )
        .values(status="shadow", last_error="auto_repair_disabled")
    )
    for run in runs:
        if run.status == "repair_pending":
            run.status = "waiting_for_fix"
            run.state_version += 1


async def _withdraw_pending_merge_actions(db: AsyncSession, *, repo_id: int) -> None:
    """Withdraw automatic queue admission before changing queue policy."""

    runs = list((await db.execute(
        select(PRMonitorRun)
        .where(PRMonitorRun.repo_id == repo_id)
        .with_for_update()
    )).scalars())
    run_ids = [item.id for item in runs]
    if not run_ids:
        return
    active = (await db.execute(
        select(PRMergeQueueAction.id)
        .where(
            PRMergeQueueAction.monitor_run_id.in_(run_ids),
            or_(
                PRMergeQueueAction.status.in_(_STARTED_MERGE_QUEUE_STATUSES),
                pr_merge_queue_action_ambiguous_remote_effect_predicate(),
            ),
        )
        .limit(1)
        .with_for_update()
    )).scalar_one_or_none()
    if active is not None:
        raise HTTPException(409, "Cannot change Merge Queue policy while queue work is active")
    await db.execute(
        sa_update(PRMergeQueueAction)
        .where(
            PRMergeQueueAction.monitor_run_id.in_(run_ids),
            PRMergeQueueAction.status == "pending",
        )
        .values(status="shadow", last_error="merge_queue_policy_changed")
    )
    for run in runs:
        if run.status == "merge_queue_pending":
            run.status = "ready_to_merge"
            run.state_version += 1


@router.post("/repos", response_model=MonitoredRepoSecretResponse)
async def create_repo(request: Request, body: MonitoredRepoCreate, db: AsyncSession = Depends(get_db)):
    if body.review_mode == "single" and body.wait_for_ci:
        raise HTTPException(400, "wait_for_ci requires review_mode=panel")
    if body.review_mode == "single" and body.auto_repair:
        raise HTTPException(400, "auto_repair requires review_mode=panel")
    if body.wait_for_ci and not body.required_checks:
        raise HTTPException(400, "wait_for_ci requires at least one required check")
    if body.auto_merge and not required_checks_support_direct_auto_merge(
        body.required_checks
    ):
        raise HTTPException(
            400,
            "auto_merge requires app-bound check_run required checks",
        )
    if body.merge_queue_mode != "manual":
        raise HTTPException(400, "Merge Queue is retired; use manual direct merge")
    worker_id = body.worker_id
    if body.project_id is not None:
        from backend.models.project import Project

        project = await db.get(Project, body.project_id)
        if project is None:
            raise HTTPException(404, "Project not found")
        await require_project_access(request, project.id, db)
        if project.worker_id != worker_id:
            raise HTTPException(
                400,
                "PR monitor Worker must match the selected Project location",
            )
    else:
        # MonitoredRepo has no creator/owner column, so a projectless monitor
        # cannot be safely delegated to a member. Administrators may still
        # create the legacy standalone form and choose an exact target node.
        require_admin(request)
        await require_worker_target_access(request, worker_id, db)

    # Authorize the exact target first so the global uniqueness check cannot
    # be used by another Worker owner to enumerate monitored repositories.
    existing = await db.execute(
        select(MonitoredRepo).where(MonitoredRepo.repo_full_name == body.repo_full_name)
    )
    if existing.scalar_one_or_none():
        raise HTTPException(409, f"Repository '{body.repo_full_name}' already monitored")

    # The checks above are deliberately optimistic.  Start a fresh mutation
    # transaction, establish Project -> Worker -> User authority, then insert.
    # This closes the gap
    # where a Project share is revoked or an administrator is disabled after
    # HTTP authentication but before the durable monitor is created.
    await db.rollback()
    if body.project_id is not None:
        # Project is the shared topology boundary.  Fence its current Worker
        # before group/User authority so Task, Plan, and Monitor creation all
        # use Project -> Worker -> membership -> User.
        project = await lock_project_worker_effect_access(
            request,
            body.project_id,
            db,
        )
        if project.worker_id != worker_id:
            raise HTTPException(
                400,
                "PR monitor Worker must match the selected Project location",
            )
    else:
        require_admin(request)
        await lock_worker_effect_access(request, worker_id, db)

    existing = await db.execute(
        select(MonitoredRepo).where(
            MonitoredRepo.repo_full_name == body.repo_full_name
        )
    )
    if existing.scalar_one_or_none():
        raise HTTPException(
            409,
            f"Repository '{body.repo_full_name}' already monitored",
        )

    repo = MonitoredRepo(
        repo_full_name=body.repo_full_name,
        project_id=body.project_id,
        worker_id=worker_id,
        auto_merge=body.auto_merge,
        provider=body.provider,
        review_model=body.review_model,
        review_effort=body.review_effort,
        review_mode=body.review_mode,
        wait_for_ci=body.wait_for_ci,
        required_checks=[item.model_dump() for item in body.required_checks],
        auto_repair=body.auto_repair,
        max_repair_attempts=body.max_repair_attempts,
        merge_queue_mode=body.merge_queue_mode,
        default_branch=body.default_branch,
        allowed_authors=body.allowed_authors,
        webhook_secret=secrets.token_hex(32),
    )
    db.add(repo)
    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise HTTPException(
            409,
            f"Repository '{body.repo_full_name}' already monitored",
        ) from exc
    await db.refresh(repo)
    return repo


@router.get("/repos/{repo_id}", response_model=MonitoredRepoResponse)
async def get_repo(
    repo_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    repo = await db.get(MonitoredRepo, repo_id)
    if not repo:
        raise HTTPException(404, "Repository not found")
    await _require_pr_monitor_access(request, db, repo)
    return repo


@router.put("/repos/{repo_id}", response_model=MonitoredRepoResponse)
async def update_repo(
    repo_id: int,
    body: MonitoredRepoUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    repo = await db.get(MonitoredRepo, repo_id)
    if not repo:
        raise HTTPException(404, "Repository not found")
    await _require_pr_monitor_access(request, db, repo)

    update_data = body.model_dump(exclude_unset=True)
    if "required_checks" in update_data:
        update_data["required_checks"] = [
            item.model_dump() if hasattr(item, "model_dump") else item
            for item in update_data["required_checks"]
        ]
    optimistic_project_move = (
        "project_id" in update_data
        and update_data["project_id"] != repo.project_id
    )
    if optimistic_project_move:
        # Project binding is administrator-owned topology. Reject before
        # inspecting active workflow state so a member cannot distinguish a
        # busy monitor from an idle one through a forbidden move.
        require_admin(request)
    await db.rollback()
    async with _pr_repo_write_lock(repo_id):
        try:
            repo = await lock_pr_repo_action_boundary(db, repo_id)
        except FindingActionConflict as exc:
            raise HTTPException(404, "Repository not found")
        # Recompute against the fenced row.  A request that looked like a
        # no-op before waiting may become a real topology move if another
        # administrator changed the Project first; the stale optimistic
        # snapshot must never let that move bypass the admin gate or the
        # deterministic old/new Project lock order.
        actual_project_move = (
            "project_id" in update_data
            and update_data["project_id"] != repo.project_id
        )
        if actual_project_move:
            await _reauthorize_pr_topology_effect(
                request,
                db,
                repo,
                target_project_id=update_data["project_id"],
            )
        else:
            await _reauthorize_pr_effect(request, db, repo)

        changed_delivery_policy = {
            key
            for key in _DELIVERY_REPO_FROZEN_FIELDS & update_data.keys()
            if update_data[key] != getattr(repo, key)
        }
        if changed_delivery_policy:
            active_delivery_run = await _delivery_repo_run_reference(
                db,
                repo_id=repo_id,
                active_only=True,
            )
            if active_delivery_run is not None:
                raise HTTPException(
                    409,
                    "PR Monitor policy is frozen while Delivery Run "
                    f"{active_delivery_run} is active",
                )

        effective_mode = update_data.get("review_mode", repo.review_mode)
        effective_wait = update_data.get("wait_for_ci", repo.wait_for_ci)
        effective_auto_repair = update_data.get("auto_repair", repo.auto_repair)
        effective_checks = update_data.get("required_checks", repo.required_checks or [])
        if effective_mode == "single" and effective_wait:
            raise HTTPException(400, "wait_for_ci requires review_mode=panel")
        if effective_mode == "single" and effective_auto_repair:
            raise HTTPException(400, "auto_repair requires review_mode=panel")
        if effective_wait and not effective_checks:
            raise HTTPException(400, "wait_for_ci requires at least one required check")
        effective_auto_merge = update_data.get("auto_merge", repo.auto_merge)
        effective_merge_queue = update_data.get("merge_queue_mode", repo.merge_queue_mode)
        if effective_auto_merge and not required_checks_support_direct_auto_merge(
            effective_checks
        ):
            raise HTTPException(
                400,
                "auto_merge requires app-bound check_run required checks",
            )
        if effective_merge_queue != "manual":
            raise HTTPException(
                400,
                "Merge Queue is retired; use manual direct merge",
            )

        frozen_review_policy = {
            "review_mode",
            "wait_for_ci",
            "required_checks",
            "provider",
            "review_model",
            "review_effort",
            "auto_merge",
            "default_branch",
            "project_id",
        }
        changed_review_policy = {
            key
            for key in frozen_review_policy & update_data.keys()
            if update_data[key] != getattr(repo, key)
        }
        if changed_review_policy:
            active_review = (await db.execute(
                select(PRReview.id)
                .where(
                    PRReview.repo_id == repo_id,
                    PRReview.status.in_(_ACTIVE_REVIEW_STATUSES),
                )
                .limit(1)
                .with_for_update()
            )).scalar_one_or_none()
            active_run = (await db.execute(
                select(PRMonitorRun.id)
                .where(
                    PRMonitorRun.repo_id == repo_id,
                    PRMonitorRun.status.not_in(("merged", "closed")),
                )
                .limit(1)
                .with_for_update()
            )).scalar_one_or_none()
            if active_review is not None or active_run is not None:
                raise HTTPException(
                    409,
                    "Review policy is frozen for the lifetime of an active PR Monitor Run",
                )

        disabling = update_data.get("enabled") is False and repo.enabled
        if disabling:
            await _quiesce_monitor_runs(
                db,
                repo_id=repo_id,
                reason="repo_disabled",
            )
        else:
            if update_data.get("auto_repair") is False and repo.auto_repair:
                await _withdraw_pending_repairs(db, repo_id=repo_id)
            if (
                "merge_queue_mode" in update_data
                and effective_merge_queue != repo.merge_queue_mode
            ) or "required_checks" in changed_review_policy:
                await _withdraw_pending_merge_actions(db, repo_id=repo_id)

        if "project_id" in update_data and update_data["project_id"] is None:
            # Removing the Project would also remove the only durable member
            # ACL. Keep projectless monitors administrator-owned.
            require_admin(request)
        project_id = update_data.get("project_id")
        if project_id is not None:
            from backend.models.project import Project

            project = await db.get(Project, project_id)
            if project is None:
                raise HTTPException(404, "Project not found")
            await require_project_access(request, project_id, db)
            if project.worker_id != repo.worker_id:
                raise HTTPException(
                    400,
                    "PR monitor Worker must match the selected Project location",
                )
        for key, value in update_data.items():
            setattr(repo, key, value)

        await db.commit()
        await db.refresh(repo)
        return repo


@router.delete("/repos/{repo_id}")
async def delete_repo(repo_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    repo = await db.get(MonitoredRepo, repo_id)
    if not repo:
        raise HTTPException(404, "Repository not found")
    await _require_pr_monitor_access(request, db, repo)

    await db.rollback()
    async with _pr_repo_write_lock(repo_id):
        try:
            locked_repo = await lock_pr_repo_action_boundary(db, repo_id)
        except FindingActionConflict as exc:
            raise HTTPException(404, "Repository not found")
        await _reauthorize_pr_effect(request, db, locked_repo)
        delivery_run_id = await _delivery_repo_run_reference(
            db,
            repo_id=repo_id,
            active_only=False,
        )
        if delivery_run_id is not None:
            await db.rollback()
            raise HTTPException(
                409,
                "Cannot delete a PR monitor referenced by Delivery Run "
                f"{delivery_run_id}",
            )
        active_fix_action = await _active_finding_action_for_repo(db, repo_id)
        if active_fix_action is not None:
            await db.rollback()
            raise HTTPException(
                409,
                "Cannot delete a PR monitor while a Finding repair action is active",
            )
        reviews = (await db.execute(
            select(PRReview).where(PRReview.repo_id == repo_id)
        )).scalars().all()
        active = [
            review
            for review in reviews
            if review.status in {
                "pending",
                "waiting_ci",
                "reviewing",
                "publishing",
                "superseding",
            }
        ]
        if active:
            await db.rollback()
            raise HTTPException(
                409,
                "Cannot delete a PR monitor while review Tasks, publication, "
                "or synchronize recovery are active",
            )
        review_ids = [review.id for review in reviews]
        monitor_run_ids = list((await db.execute(
            select(PRMonitorRun.id).where(PRMonitorRun.repo_id == repo_id)
        )).scalars())
        if monitor_run_ids:
            active_wake = (await db.execute(select(PRRepairWake.id).where(
                PRRepairWake.monitor_run_id.in_(monitor_run_ids),
                PRRepairWake.status.in_(("pending", *_STARTED_REPAIR_STATUSES)),
            ).limit(1))).scalar_one_or_none()
            active_merge = (await db.execute(select(PRMergeQueueAction.id).where(
                PRMergeQueueAction.monitor_run_id.in_(monitor_run_ids),
                or_(
                    PRMergeQueueAction.status.in_((
                        "pending", *_STARTED_MERGE_QUEUE_STATUSES
                    )),
                    pr_merge_queue_action_ambiguous_remote_effect_predicate(),
                ),
            ).limit(1))).scalar_one_or_none()
            active_rebuttal = (await db.execute(select(PRFindingRebuttal.id).where(
                PRFindingRebuttal.monitor_run_id.in_(monitor_run_ids),
                PRFindingRebuttal.status.in_(_ACTIVE_ADJUDICATION_STATUSES),
            ).limit(1))).scalar_one_or_none()
            resolving_run = (await db.execute(select(PRMonitorRun.id).where(
                PRMonitorRun.id.in_(monitor_run_ids),
                PRMonitorRun.status == "resolving_fixed_threads",
            ).limit(1))).scalar_one_or_none()
            unresolved_thread = None
            if review_ids:
                unresolved_thread = (await db.execute(select(PRFinding.id).where(
                    PRFinding.pr_review_id.in_(review_ids),
                    PRFinding.thread_status.in_((
                        "published_inline",
                        "published_fallback",
                    )),
                ).limit(1))).scalar_one_or_none()
            if (
                active_wake
                or active_merge
                or active_rebuttal
                or resolving_run
                or unresolved_thread
            ):
                await db.rollback()
                raise HTTPException(
                    409,
                    "Cannot delete a PR monitor while Repair, adjudication, "
                    "Finding resolution, or Merge Queue work is active",
                )
        if monitor_run_ids:
            await db.execute(
                sa_delete(PRMergeQueueAction).where(
                    PRMergeQueueAction.monitor_run_id.in_(monitor_run_ids)
                )
            )
            await db.execute(
                sa_delete(PRRepairWake).where(
                    PRRepairWake.monitor_run_id.in_(monitor_run_ids)
                )
            )
        if review_ids:
            reviewer_task_ids = (await db.execute(
                select(PRReviewerRun.task_id).where(
                    PRReviewerRun.pr_review_id.in_(review_ids)
                )
            )).scalars()
            finding_ids = list((await db.execute(
                select(PRFinding.id).where(
                    PRFinding.pr_review_id.in_(review_ids)
                )
            )).scalars())
            owned_task_ids = {
                review.task_id
                for review in reviews
                if review.task_id is not None
            }
            display_task_ids = (await db.execute(
                select(PRMonitorRun.display_task_id).where(
                    PRMonitorRun.id.in_(monitor_run_ids),
                    PRMonitorRun.display_task_id.is_not(None),
                )
            )).scalars()
            owned_task_ids.update(
                task_id
                for task_id in display_task_ids
                if task_id is not None
            )
            owned_task_ids.update(
                task_id
                for task_id in reviewer_task_ids
                if task_id is not None
            )
            rebuttal_owner_predicate = (
                PRFindingRebuttal.pr_review_id.in_(review_ids)
            )
            if finding_ids:
                rebuttal_owner_predicate = or_(
                    rebuttal_owner_predicate,
                    PRFindingRebuttal.finding_id.in_(finding_ids),
                )
            rebuttal_task_ids = (await db.execute(
                select(PRFindingRebuttal.task_id).where(
                    rebuttal_owner_predicate,
                    PRFindingRebuttal.task_id.is_not(None),
                )
            )).scalars()
            owned_task_ids.update(rebuttal_task_ids)
            if finding_ids:
                action_task_ids = (await db.execute(
                    select(PRFindingAction.task_id).where(
                        PRFindingAction.finding_id.in_(finding_ids),
                        PRFindingAction.task_id.is_not(None),
                    )
                )).scalars()
                owned_task_ids.update(action_task_ids)
            # Preserve all four owner identities before deleting any owner
            # row.  The final commit makes tombstones and owner cleanup one
            # atomic repository-delete transition.
            await _record_pr_monitor_task_tombstones(db, owned_task_ids)
            # Legacy rows can disagree in either direction: the direct review
            # or the Finding can belong to this repository.  Classify and
            # delete the union before either owner graph is removed, using the
            # exact same predicate for Task tombstones and cleanup.
            await db.execute(
                sa_delete(PRFindingRebuttal).where(
                    rebuttal_owner_predicate
                )
            )
            if finding_ids:
                # Do not rely on database-level cascades here.  SQLite
                # deployments may predate foreign_keys=ON, and orphaned
                # terminal actions would retain globally unique idempotency
                # keys after their monitor is deleted.
                await db.execute(
                    sa_delete(PRFindingAction).where(
                        PRFindingAction.finding_id.in_(finding_ids)
                    )
                )
                await db.execute(
                    sa_delete(PRFinding).where(
                        PRFinding.id.in_(finding_ids)
                    )
                )
            await db.execute(
                sa_delete(PRReviewerRun).where(
                    PRReviewerRun.pr_review_id.in_(review_ids)
                )
            )
        for review in reviews:
            await db.delete(review)
        if monitor_run_ids:
            await db.execute(
                sa_delete(PRMonitorRun).where(PRMonitorRun.id.in_(monitor_run_ids))
            )

        await db.delete(locked_repo)
        await db.commit()
    return {"ok": True}


@router.post("/repos/{repo_id}/toggle", response_model=MonitoredRepoResponse)
async def toggle_repo(repo_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    repo = await db.get(MonitoredRepo, repo_id)
    if not repo:
        raise HTTPException(404, "Repository not found")
    await _require_pr_monitor_access(request, db, repo)
    await db.rollback()
    async with _pr_repo_write_lock(repo_id):
        try:
            repo = await lock_pr_repo_action_boundary(db, repo_id)
        except FindingActionConflict as exc:
            raise HTTPException(404, "Repository not found")
        await _reauthorize_pr_effect(request, db, repo)
        if repo.enabled:
            active_delivery_run = await _delivery_repo_run_reference(
                db,
                repo_id=repo_id,
                active_only=True,
            )
            if active_delivery_run is not None:
                raise HTTPException(
                    409,
                    "Cannot disable a PR monitor while Delivery Run "
                    f"{active_delivery_run} is active",
                )
            await _quiesce_monitor_runs(
                db,
                repo_id=repo_id,
                reason="repo_disabled",
            )
        repo.enabled = not repo.enabled
        await db.commit()
        await db.refresh(repo)
        return repo


@router.post(
    "/repos/{repo_id}/regenerate-secret",
    response_model=MonitoredRepoSecretResponse,
)
async def regenerate_secret(repo_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    repo = await db.get(MonitoredRepo, repo_id)
    if not repo:
        raise HTTPException(404, "Repository not found")
    await _require_pr_monitor_access(request, db, repo)
    await db.rollback()
    async with _pr_repo_write_lock(repo_id):
        try:
            repo = await lock_pr_repo_action_boundary(db, repo_id)
        except FindingActionConflict as exc:
            raise HTTPException(404, "Repository not found")
        await _reauthorize_pr_effect(request, db, repo)
        active_delivery_run = await _delivery_repo_run_reference(
            db,
            repo_id=repo_id,
            active_only=True,
        )
        if active_delivery_run is not None:
            raise HTTPException(
                409,
                "Cannot rotate the webhook secret while Delivery Run "
                f"{active_delivery_run} is active",
            )
        if await _active_finding_action_for_repo(db, repo_id) is not None:
            raise HTTPException(
                409,
                "Cannot rotate the webhook secret while a Finding repair action is active",
            )
        repo.webhook_secret = secrets.token_hex(32)
        await db.commit()
        await db.refresh(repo)
        return repo


_REVIEW_DISPLAY_STATUSES = {
    "pending": "Pending",
    "waiting_ci": "Waiting for CI",
    "reviewing": "Reviewing",
    "publishing": "Publishing result",
    "superseding": "Updating to a newer commit",
    "approved": "Passed",
    "commented": "Changes required",
    "merged": "Merged",
    "superseded": "Superseded",
    "cancelled": "Cancelled",
}
_PUBLICATION_STATES = frozenset({
    "not_started",
    "publishing",
    "reconciling",
    "published",
    "failed",
    "not_applicable",
})
_FAILURE_STAGES = frozenset({
    "reviewer",
    "ci",
    "github_identity",
    "publication",
    "merge",
    "recovery",
    "lifecycle",
})
_PANEL_REVIEWER_ROLES = frozenset({
    "principal_engineer",
    "senior_engineer",
    "qa_engineer",
})
_LEGACY_TECHNICAL_REVIEW_SUMMARY_RE = re.compile(
    r"\AAgent recommendation: "
    r"(?P<recommendation>approved_merged|lgtm_comment|review_comments); "
    r"backend action: "
    r"(?P<action>approved_merged|lgtm_comment|review_comments); "
    r"durable nonce evidence verified\Z"
)
_LEGACY_HUMAN_REVIEW_SUMMARIES = {
    "approved_merged": "Review passed and the reviewed commit was merged.",
    "lgtm_comment": "Review passed with no blocking findings.",
    "review_comments": "Review found changes that need to be addressed.",
}
_LEGACY_LIFECYCLE_RECOVERY_MARKERS = (
    "pr became draft",
    "pr snapshot changed",
    "pr changed without matching merge evidence",
    "pr was closed",
    "pr was merged",
)
_ARMED_ACTION_UNSET = object()


def _human_facing_review_summary(review: PRReview) -> str | None:
    """Translate only the exact technical summary emitted by older CCMs."""

    value = review.review_summary
    if not isinstance(value, str):
        return value
    match = _LEGACY_TECHNICAL_REVIEW_SUMMARY_RE.fullmatch(value)
    if match is None:
        return value
    recommendation = match.group("recommendation")
    action = match.group("action")
    if recommendation != action or review.action_taken != action:
        return value
    return _LEGACY_HUMAN_REVIEW_SUMMARIES[action]


def _normalized_reviewer_verdict(run) -> str | None:
    verdict = getattr(run, "verdict", None)
    if verdict in {"pass", "changes_required"}:
        return verdict
    status = getattr(run, "status", None)
    if status == "passed":
        return "pass"
    if status == "changes_required":
        return "changes_required"
    return None


def _reviewer_outcome_kind(run) -> str:
    if getattr(run, "status", None) == "error":
        return "infrastructure_error"
    if _normalized_reviewer_verdict(run) is not None:
        return "review_result"
    if getattr(run, "status", None) in {"cancelled", "superseded"}:
        return "lifecycle"
    return "in_progress"


def _armed_single_review_action(review: PRReview) -> str | None:
    # A single-reviewer verdict is frozen before the publication outbox is
    # armed.  If the subsequent exact-head GitHub write terminalizes as an
    # error, retain that code verdict only when the complete durable outbox
    # generation is still present; a stray pending_action alone is not proof.
    return (
        str(review.pending_action)
        if (
            review.status in {"publishing", "error"}
            and review.pending_action in {
                "lgtm_comment", "approved_merged", "review_comments"
            }
            and isinstance(review.action_nonce, str)
            and re.fullmatch(r"[0-9a-f]{48}", review.action_nonce) is not None
            and review.task_id is not None
            and isinstance(review.pending_review_body, str)
            and bool(review.pending_review_body)
            and isinstance(review.publishing_actor, str)
            and review.publishing_actor
            and type(review.publishing_retry_count) is int
            and review.publishing_retry_count >= 0
            and isinstance(review.publishing_task_started_at, datetime)
            and isinstance(review.publishing_started_at, datetime)
        )
        else None
    )


def _aggregate_review_verdict(
    review: PRReview,
    runs: list,
    *,
    armed_action_override: object = _ARMED_ACTION_UNSET,
) -> str | None:
    # New single-review generations freeze the exact code result before any
    # GitHub capability or identity lookup.  Prefer that immutable fact over
    # mutable publication/lifecycle status.  Invalid database values fail
    # closed instead of being reflected to the public Literal schema.
    code_verdict = getattr(review, "code_verdict", None)
    if code_verdict is not None:
        return (
            code_verdict
            if code_verdict in {"pass", "changes_required"}
            else None
        )
    if runs:
        # Reviewer rows are the durable Panel protocol, not an arbitrary vote
        # collection.  A partial/corrupt role set must fail closed: otherwise
        # one or two surviving rows could be presented as a completed panel.
        roles = [getattr(run, "role", None) for run in runs]
        if (
            len(runs) != len(_PANEL_REVIEWER_ROLES)
            or len(set(roles)) != len(roles)
            or set(roles) != _PANEL_REVIEWER_ROLES
        ):
            return None
        verdicts = [_normalized_reviewer_verdict(run) for run in runs]
        if (
            all(verdict is not None for verdict in verdicts)
            and all(_reviewer_outcome_kind(run) == "review_result" for run in runs)
        ):
            return (
                "changes_required"
                if "changes_required" in verdicts
                else "pass"
            )
        return None
    armed_action = (
        _armed_single_review_action(review)
        if armed_action_override is _ARMED_ACTION_UNSET
        else armed_action_override
    )
    if review.status in {"approved", "merged"} or review.action_taken in {
        "lgtm_comment",
        "approved_merged",
    } or armed_action in {"lgtm_comment", "approved_merged"}:
        return "pass"
    if (
        review.status == "commented"
        or review.action_taken == "review_comments"
        or armed_action == "review_comments"
    ):
        return "changes_required"
    return None


def _publication_failure_is_lifecycle(review: PRReview) -> bool:
    # New rows have a durable failure axis.  Reviewer prose can legitimately
    # discuss phrases such as "PR was closed" and must never override it.
    explicit_stage = getattr(review, "failure_stage", None)
    if explicit_stage is not None:
        return explicit_stage == "lifecycle"

    # Compatibility is intentionally restricted to the complete pre-migration
    # publication-outbox error shape with no immutable GitHub evidence.
    if (
        review.status != "error"
        or review.action_taken != "error"
        or review.pending_action not in {
            "lgtm_comment",
            "review_comments",
            "approved_merged",
        }
        or any((
            review.published_actor,
            review.published_at,
            review.github_review_id,
            review.github_review_url,
            review.github_review_state,
        ))
        or not isinstance(review.review_summary, str)
    ):
        return False
    value = review.review_summary.lower()
    return any(marker in value for marker in _LEGACY_LIFECYCLE_RECOVERY_MARKERS)


def _is_strict_legacy_lifecycle_recovery_candidate(
    review: PRReview,
    monitor_run: PRMonitorRun,
) -> bool:
    """Recognize only pre-migration publication/lifecycle races.

    These rows were written after the code verdict and publication outbox
    were frozen, but before CCM retained a signed terminal intent.  Recovery
    still requires a fresh authoritative GitHub terminal snapshot; this
    predicate merely admits the narrow historical row shape.
    """

    summary = review.review_summary
    return bool(
        review.id == monitor_run.current_review_id
        and review.status == "error"
        and review.action_taken == "error"
        and review.pending_action in {
            "lgtm_comment",
            "review_comments",
            "approved_merged",
        }
        and review.publication_state == "not_applicable"
        and review.failure_stage == "lifecycle"
        and isinstance(summary, str)
        and any(
            marker in summary.lower()
            for marker in _LEGACY_LIFECYCLE_RECOVERY_MARKERS
        )
        and monitor_run.status not in {"merged", "closed"}
        and monitor_run.completed_at is None
        and monitor_run.legacy_terminal_recovery_pending is True
        and monitor_run.terminal_intent_status is None
        and monitor_run.terminal_intent_base_ref is None
        and monitor_run.terminal_intent_head_sha is None
        and monitor_run.terminal_intent_delivery_id is None
        and monitor_run.terminal_intent_observed_at is None
    )


def _monitor_run_exactly_binds_review(
    monitor_run: PRMonitorRun | None,
    review: PRReview,
) -> bool:
    """Require the complete immutable Run/Review subject relationship.

    ``current_review_id`` is not itself a foreign key. Never borrow lifecycle
    state or Project visibility from a partially or cross-repository bound Run.
    """

    return bool(
        monitor_run is not None
        and review.monitor_run_id == monitor_run.id
        and monitor_run.current_review_id == review.id
        and review.repo_id == monitor_run.repo_id
        and review.pr_number == monitor_run.pr_number
        and review.base_sha == monitor_run.current_base_sha
        and review.head_sha == monitor_run.current_head_sha
    )


def _public_review_states(
    review: PRReview,
    runs: list,
    *,
    monitor_run: PRMonitorRun | None = None,
) -> dict[str, str | None]:
    """Project verdict, publication, and lifecycle as independent axes."""

    aggregate_verdict = _aggregate_review_verdict(review, runs)
    if aggregate_verdict is not None:
        verdict_state = "complete"
    elif review.status in _ACTIVE_REVIEW_STATUSES:
        verdict_state = "pending"
    else:
        verdict_state = "unavailable"

    publication_state = review.publication_state
    if publication_state not in _PUBLICATION_STATES:
        publication_state = "not_started"
    has_complete_publication_evidence = bool(
        type(review.github_review_id) is int
        and review.github_review_id > 0
        and isinstance(review.github_review_url, str)
        and review.github_review_url
        and isinstance(review.github_review_state, str)
        and review.github_review_state
        and isinstance(review.published_actor, str)
        and review.published_actor
        and isinstance(review.published_at, datetime)
    )
    if publication_state == "published" and not has_complete_publication_evidence:
        publication_state = "reconciling"
    if publication_state == "failed" and _publication_failure_is_lifecycle(review):
        publication_state = "not_applicable"
    # Backward-compatible projection for rows created before publication
    # evidence became a first-class state machine.
    if publication_state == "not_started":
        if review.status == "publishing":
            publication_state = "publishing"
        elif review.status in {"approved", "commented", "merged"} or review.action_taken in {
            "lgtm_comment",
            "review_comments",
            "approved_merged",
        }:
            # Legacy rows prove that CCM reached its local verified terminal,
            # but older binaries cleared actor/time and never retained the
            # GitHub Review id.  Do not fabricate immutable evidence.
            publication_state = "reconciling"
        elif review.status in {"superseded", "cancelled"}:
            publication_state = "not_applicable"
        elif aggregate_verdict is not None and review.status == "error":
            publication_state = (
                "not_applicable"
                if _publication_failure_is_lifecycle(review)
                else "failed"
            )

    if monitor_run is None:
        lifecycle_state = "unknown"
    elif (
        monitor_run is not None
        and monitor_run.terminal_intent_status in {"merged", "closed"}
    ):
        lifecycle_state = monitor_run.terminal_intent_status
    elif (
        monitor_run is not None
        and pr_monitor_run_has_terminal_intent(monitor_run)
    ):
        lifecycle_state = "unknown"
    elif monitor_run is not None and monitor_run.status in {"merged", "closed"}:
        lifecycle_state = monitor_run.status
    elif review.status == "superseding":
        lifecycle_state = "superseding"
    elif review.status == "superseded":
        lifecycle_state = "superseded"
    elif review.status == "cancelled":
        lifecycle_state = "cancelled"
    elif review.status == "merged":
        lifecycle_state = "merged"
    else:
        # Reviewer, CI, identity, publication, and merge failures are not a
        # GitHub PR lifecycle transition.  Keep the lifecycle axis open unless
        # a signed terminal/supersede/cancel fact above proves otherwise.
        lifecycle_state = "reviewing"

    failure_stage = review.failure_stage
    if failure_stage not in _FAILURE_STAGES:
        failure_stage = None
    if failure_stage is None:
        failed_run = next(
            (run for run in runs if _reviewer_outcome_kind(run) == "infrastructure_error"),
            None,
        )
        if review.ci_status in {"failed", "missing"} and aggregate_verdict is None:
            failure_stage = "ci"
        elif failed_run is not None or (
            review.status == "error" and aggregate_verdict is None
        ):
            failure_stage = "reviewer"
        elif publication_state == "not_applicable" and review.status == "error":
            failure_stage = "lifecycle"
        elif publication_state == "failed":
            failure_stage = "publication"
        elif (
            publication_state == "reconciling"
            and review.status != "publishing"
            and aggregate_verdict is None
        ):
            failure_stage = "recovery"
        elif (
            lifecycle_state == "unknown"
            and aggregate_verdict is None
            and publication_state in {"failed", "reconciling"}
        ):
            failure_stage = "recovery"

    return {
        "verdict_state": verdict_state,
        "aggregate_verdict": aggregate_verdict,
        "publication_state": publication_state,
        "lifecycle_state": lifecycle_state,
        "failure_stage": failure_stage,
    }


def _public_publication_evidence(review) -> dict[str, object | None]:
    """Expose publication identity only when the immutable receipt is whole.

    A crash can leave a legacy or corrupt row with just one of the actor,
    timestamp, Review id, URL, or state fields.  Such a row is projected as
    ``reconciling`` above; leaking the surviving URL would nevertheless make
    clients render it as a confirmed publication.  Keep partial evidence for
    internal recovery, but make the public receipt all-or-nothing.
    """

    complete = bool(
        type(getattr(review, "github_review_id", None)) is int
        and review.github_review_id > 0
        and isinstance(getattr(review, "github_review_url", None), str)
        and review.github_review_url
        and isinstance(getattr(review, "github_review_state", None), str)
        and review.github_review_state
        and isinstance(getattr(review, "published_actor", None), str)
        and review.published_actor
        and isinstance(getattr(review, "published_at", None), datetime)
    )
    if not complete:
        return {
            "published_actor": None,
            "published_at": None,
            "github_review_id": None,
            "github_review_url": None,
            "github_state": None,
            "github_event": None,
        }
    return {
        "published_actor": review.published_actor,
        "published_at": review.published_at,
        "github_review_id": review.github_review_id,
        "github_review_url": review.github_review_url,
        "github_state": review.github_review_state,
        "github_event": (
            "COMMENT" if review.github_review_state == "COMMENTED" else None
        ),
    }


def _public_result_feed_states(
    review: PRReview,
    runs: list,
    *,
    monitor_run: PRMonitorRun | None,
    armed_action: str | None,
) -> dict[str, str | None]:
    """Project feed axes using only explicitly selected, non-secret fields."""

    aggregate_verdict = _aggregate_review_verdict(
        review,
        runs,
        armed_action_override=armed_action,
    )
    if aggregate_verdict is not None:
        verdict_state = "complete"
    elif review.status in _ACTIVE_REVIEW_STATUSES:
        verdict_state = "pending"
    else:
        verdict_state = "unavailable"

    publication_state = (
        review.publication_state
        if review.publication_state in _PUBLICATION_STATES
        else "not_started"
    )
    complete_evidence = bool(
        type(review.github_review_id) is int
        and review.github_review_id > 0
        and isinstance(review.github_review_url, str)
        and review.github_review_url
        and isinstance(review.github_review_state, str)
        and review.github_review_state
        and isinstance(review.published_actor, str)
        and review.published_actor
        and isinstance(review.published_at, datetime)
    )
    if publication_state == "published" and not complete_evidence:
        publication_state = "reconciling"
    if (
        publication_state == "failed"
        and bool(getattr(review, "publication_failure_is_lifecycle", False))
    ):
        publication_state = "not_applicable"
    if publication_state == "not_started":
        if review.status == "publishing":
            publication_state = "publishing"
        elif review.status in {"approved", "commented", "merged"} or review.action_taken in {
            "lgtm_comment",
            "review_comments",
            "approved_merged",
        }:
            publication_state = "reconciling"
        elif review.status in {"superseded", "cancelled"}:
            publication_state = "not_applicable"
        elif aggregate_verdict is not None and review.status == "error":
            publication_state = "failed"

    if monitor_run is None:
        lifecycle_state = "unknown"
    elif monitor_run.terminal_intent_status in {"merged", "closed"}:
        lifecycle_state = monitor_run.terminal_intent_status
    elif pr_monitor_run_has_terminal_intent(monitor_run):
        lifecycle_state = "unknown"
    elif monitor_run.status in {"merged", "closed"}:
        lifecycle_state = monitor_run.status
    elif review.status == "superseding":
        lifecycle_state = "superseding"
    elif review.status == "superseded":
        lifecycle_state = "superseded"
    elif review.status == "cancelled":
        lifecycle_state = "cancelled"
    elif review.status == "merged":
        lifecycle_state = "merged"
    else:
        lifecycle_state = "reviewing"

    failure_stage = (
        review.failure_stage
        if review.failure_stage in _FAILURE_STAGES
        else None
    )
    if failure_stage is None:
        failed_run = next(
            (item for item in runs if _reviewer_outcome_kind(item) == "infrastructure_error"),
            None,
        )
        if review.ci_status in {"failed", "missing"} and aggregate_verdict is None:
            failure_stage = "ci"
        elif failed_run is not None or (
            review.status == "error" and aggregate_verdict is None
        ):
            failure_stage = "reviewer"
        elif publication_state == "not_applicable" and review.status == "error":
            failure_stage = "lifecycle"
        elif publication_state == "failed":
            failure_stage = "publication"
        elif (
            publication_state == "reconciling"
            and review.status != "publishing"
            and aggregate_verdict is None
        ):
            failure_stage = "recovery"
        elif (
            lifecycle_state == "unknown"
            and aggregate_verdict is None
            and publication_state in {"failed", "reconciling"}
        ):
            failure_stage = "recovery"
    return {
        "verdict_state": verdict_state,
        "aggregate_verdict": aggregate_verdict,
        "publication_state": publication_state,
        "lifecycle_state": lifecycle_state,
        "failure_stage": failure_stage,
    }


def _bounded_review_list_summary(value: str | None) -> str | None:
    """Keep collection responses small without changing detail content."""

    if value is None:
        return value
    encoded = value.encode("utf-8")
    if len(encoded) <= _PR_REVIEW_LIST_SUMMARY_MAX_BYTES:
        return value
    suffix = "…"
    prefix = encoded[
        : _PR_REVIEW_LIST_SUMMARY_MAX_BYTES - len(suffix.encode("utf-8"))
    ].decode("utf-8", errors="ignore")
    return prefix.rstrip() + suffix


def _result_feed_display_summary(states: dict[str, str | None]) -> str:
    """Return an enum-derived feed summary with no internal diagnostics.

    The repository Review detail remains the authenticated diagnostic view.
    The ordinary Tasks-style feed is intentionally safe for every Project
    member and therefore never includes reviewer errors, paths, Task/session
    identifiers, prompts, command output, or publication nonces.
    """

    verdict = states.get("aggregate_verdict")
    publication = states.get("publication_state")
    lifecycle = states.get("lifecycle_state")
    failure = states.get("failure_stage")
    if verdict == "changes_required":
        summary = "Code review found changes that need to be addressed."
    elif verdict == "pass":
        summary = "Code review completed with no blocking findings."
    elif failure == "ci":
        return "Code review could not complete because required CI did not pass."
    elif failure == "github_identity":
        return "CCM could not verify its GitHub publishing identity."
    elif failure == "reviewer":
        return "A reviewer could not complete; no code verdict is available."
    elif failure == "lifecycle" or lifecycle in {"merged", "closed", "failed"}:
        if lifecycle in {"merged", "closed"}:
            return f"The PR was {lifecycle} before a code verdict completed."
        return "The review ended because the PR lifecycle changed."
    elif failure in {"publication", "recovery"}:
        return "The GitHub Review could not be confirmed; check the Review detail."
    elif failure == "merge":
        return "The code verdict completed, but the requested merge did not."
    elif lifecycle == "superseding":
        return "A newer PR commit is replacing this review."
    elif lifecycle in {"superseded", "cancelled"}:
        return "This review generation is no longer active."
    elif lifecycle == "unknown":
        return (
            "Historical review result is unavailable; the PR lifecycle "
            "was not retained."
        )
    else:
        return "Code review is still in progress."

    if publication in {"failed", "reconciling"}:
        summary += " GitHub publication is not confirmed."
    elif publication == "not_applicable" and lifecycle in {"merged", "closed"}:
        summary += f" The PR was {lifecycle} before publication completed."
    elif publication == "published":
        summary += " The GitHub Review was published."
    if lifecycle == "unknown":
        summary += " Historical PR lifecycle was not retained."
    return summary


def _public_input_error_evidence(review) -> dict[str, object | None]:
    """Whitelist only a complete, bounded deterministic input-size receipt."""

    if valid_pr_review_input_error_evidence(
        category=getattr(review, "error_category", None),
        measured=getattr(review, "error_measured", None),
        limit=getattr(review, "error_limit", None),
        unit=getattr(review, "error_unit", None),
    ):
        return {
            "error_category": PR_REVIEW_INPUT_ERROR_CATEGORY,
            "error_measured": review.error_measured,
            "error_limit": review.error_limit,
            "error_unit": review.error_unit,
        }
    return {
        "error_category": None,
        "error_measured": None,
        "error_limit": None,
        "error_unit": None,
    }


def _review_response_payload(
    review: PRReview,
    runs: list,
    *,
    include_full_summary: bool,
    monitor_run: PRMonitorRun | None = None,
    monitor_enabled: bool = False,
    rerun_effects_clear: bool = False,
) -> dict:
    """Build the public list or detail projection for one review.

    Panel rows keep ``task_id`` for old clients, but ``task_ids`` is the only
    complete Task identity.  The list endpoint deliberately selects no role
    body or machine result JSON and bounds human summaries; those are complete
    only on the detail endpoint.
    """

    if not _monitor_run_exactly_binds_review(monitor_run, review):
        monitor_run = None
        rerun_effects_clear = False

    status_counts: dict[str, int] = {}
    verdict_counts: dict[str, int] = {}
    task_ids: list[int] = []
    for run in runs:
        status = getattr(run, "status", None) or "unknown"
        status_counts[status] = status_counts.get(status, 0) + 1
        verdict = _normalized_reviewer_verdict(run)
        if verdict is not None:
            verdict_counts[verdict] = verdict_counts.get(verdict, 0) + 1
        task_id = getattr(run, "task_id", None)
        if task_id is not None and task_id not in task_ids:
            task_ids.append(task_id)
    if not runs and review.task_id is not None:
        task_ids.append(review.task_id)

    public_states = _public_review_states(
        review,
        runs,
        monitor_run=monitor_run,
    )
    aggregate_verdict = public_states["aggregate_verdict"]
    input_error = _public_input_error_evidence(review)
    failed_run = next(
        (run for run in runs if _reviewer_outcome_kind(run) == "infrastructure_error"),
        None,
    )
    # A complete code verdict remains authoritative even when GitHub
    # publication later becomes stale or fails.  Publication errors are a
    # separate axis and must never erase the reviewer result.
    if aggregate_verdict is not None:
        outcome_kind = "review_result"
        display_status = (
            "Changes required"
            if aggregate_verdict == "changes_required"
            else "Passed"
        )
    elif review.status == "error" or failed_run is not None:
        outcome_kind = "infrastructure_error"
        display_status = (
            "Review input too large"
            if input_error["error_category"] == PR_REVIEW_INPUT_ERROR_CATEGORY
            else "Infrastructure error"
        )
    elif review.status in _ACTIVE_REVIEW_STATUSES:
        outcome_kind = "in_progress"
        display_status = _REVIEW_DISPLAY_STATUSES.get(review.status, review.status)
    else:
        outcome_kind = "lifecycle"
        display_status = _REVIEW_DISPLAY_STATUSES.get(
            review.status,
            review.status.replace("_", " ").title(),
        )

    response_summary = _human_facing_review_summary(review)
    if (
        aggregate_verdict is not None
        and not runs
        and _armed_single_review_action(review) is not None
    ):
        # The strict result was marker-stripped before the outbox was armed.
        # Surface that human code review without ever exposing the field name,
        # nonce, or other transient publication machinery.
        response_summary = review.pending_review_body
    display_summary = response_summary
    if failed_run is not None and getattr(failed_run, "error_message", None):
        role_error = (
            f"{getattr(failed_run, 'role', 'reviewer')}: "
            f"{failed_run.error_message}"
        )
        if not display_summary:
            display_summary = role_error
        elif failed_run.error_message not in display_summary:
            display_summary = f"{display_summary} ({role_error})"

    if not include_full_summary:
        response_summary = _bounded_review_list_summary(response_summary)
        display_summary = _bounded_review_list_summary(display_summary)

    # Never validate the ORM object directly: legacy/manual rows can contain a
    # partial or malformed evidence quartet, and Pydantic would turn that into
    # a 500 before this endpoint can project the safe all-null representation.
    base_payload = {
        field_name: getattr(review, field_name)
        for field_name in PRReviewResponse.model_fields
        if hasattr(review, field_name)
    }
    base_payload.update(input_error)
    payload = PRReviewResponse.model_validate(base_payload).model_dump()
    payload.update({
        "review_summary": response_summary,
        "task_ids": task_ids,
        "display_task_id": (
            monitor_run.display_task_id
            if monitor_run is not None
            else None
        ),
        "display_status": display_status,
        "display_summary": display_summary,
        "outcome_kind": outcome_kind,
        "aggregate_verdict": aggregate_verdict,
        **public_states,
        "publication_error": review.publication_error,
        **_public_publication_evidence(review),
        "can_rerun": bool(
            monitor_enabled
            and rerun_effects_clear
            and monitor_run is not None
            and monitor_run.current_review_id == review.id
            and monitor_run.current_head_sha == review.head_sha
            and monitor_run.status not in {"merged", "closed"}
            and monitor_run.completed_at is None
            and review.status not in _ACTIVE_REVIEW_STATUSES
        ),
        "reviewer_count": len(runs),
        "reviewer_status_counts": dict(sorted(status_counts.items())),
        "reviewer_verdict_counts": dict(sorted(verdict_counts.items())),
    })
    return payload


@router.get("/repos/{repo_id}/reviews", response_model=list[PRReviewResponse])
async def list_reviews(
    repo_id: int,
    request: Request,
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    repo = await db.get(MonitoredRepo, repo_id)
    if not repo:
        raise HTTPException(404, "Repository not found")
    await _require_pr_monitor_access(request, db, repo)

    offset = (page - 1) * size
    # ``delivery:`` is the immutable namespace reserved by the Delivery
    # publisher. Do not infer presentation ownership from mutable repository,
    # PR-number, or Monitor relationships: those identities can be reused by
    # ordinary PR Monitor workflows after a Delivery completes.
    ordinary_review = or_(
        PRReview.delivery_id.is_(None),
        ~PRReview.delivery_id.like("delivery:%"),
    )
    result = await db.execute(
        select(PRReview)
        .where(PRReview.repo_id == repo_id, ordinary_review)
        .order_by(desc(PRReview.created_at))
        .offset(offset)
        .limit(size)
    )
    reviews = list(result.scalars())
    if not reviews:
        return []
    reviewer_rows = (await db.execute(
        select(
            PRReviewerRun.pr_review_id,
            PRReviewerRun.role,
            PRReviewerRun.task_id,
            PRReviewerRun.status,
            PRReviewerRun.verdict,
            PRReviewerRun.error_message,
        )
        .where(PRReviewerRun.pr_review_id.in_([review.id for review in reviews]))
        .order_by(PRReviewerRun.pr_review_id, PRReviewerRun.id)
    )).all()
    runs_by_review: dict[int, list] = {}
    for run in reviewer_rows:
        runs_by_review.setdefault(run.pr_review_id, []).append(run)
    monitor_rows = list((await db.execute(
        select(PRMonitorRun).where(
            PRMonitorRun.current_review_id.in_([review.id for review in reviews])
        )
    )).scalars())
    reviews_by_id = {review.id: review for review in reviews}
    monitor_by_review = {
        item.current_review_id: item
        for item in monitor_rows
        if (
            item.current_review_id is not None
            and (bound_review := reviews_by_id.get(item.current_review_id))
            is not None
            and _monitor_run_exactly_binds_review(item, bound_review)
        )
    }
    rerun_blocked = await _rerun_blocked_run_ids(
        db,
        list(monitor_by_review.values()),
    )
    return [
        _review_response_payload(
            review,
            runs_by_review.get(review.id, []),
            include_full_summary=False,
            monitor_run=monitor_by_review.get(review.id),
            monitor_enabled=repo.enabled,
            rerun_effects_clear=bool(
                monitor_by_review.get(review.id) is not None
                and monitor_by_review[review.id].id not in rerun_blocked
            ),
        )
        for review in reviews
    ]


@router.get("/results", response_model=list[PRResultFeedItem])
async def list_pr_review_results(
    request: Request,
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    """Return one safe, read-only work item per visible PR lifecycle.

    This is intentionally not a Task projection.  Internal Reviewer Tasks,
    prompts, sessions, outbox nonces, and pending GitHub bodies never enter
    the selected columns or response model.
    """

    # REPLACE is case-sensitive on every supported database, including MySQL
    # under a case-insensitive column collation.  Strip only the allowed
    # lowercase alphabet so uppercase/non-hex input necessarily remains.
    nonce_remainder = _lowercase_hex_sql_remainder(PRReview.action_nonce)
    armed_action = case(
        (
            and_(
                PRReview.status.in_(("publishing", "error")),
                PRReview.pending_action.in_((
                    "lgtm_comment",
                    "approved_merged",
                    "review_comments",
                )),
                PRReview.action_nonce.is_not(None),
                func.length(PRReview.action_nonce) == 48,
                func.length(nonce_remainder) == 0,
                PRReview.task_id.is_not(None),
                PRReview.pending_review_body.is_not(None),
                func.length(PRReview.pending_review_body) > 0,
                PRReview.publishing_actor.is_not(None),
                func.length(PRReview.publishing_actor) > 0,
                PRReview.publishing_retry_count >= 0,
                PRReview.publishing_task_started_at.is_not(None),
                PRReview.publishing_started_at.is_not(None),
            ),
            PRReview.pending_action,
        ),
        else_=None,
    ).label("armed_action")
    legacy_lifecycle_publication_failure = case(
        (PRReview.failure_stage == "lifecycle", True),
        (PRReview.failure_stage.is_not(None), False),
        (
            and_(
                PRReview.status == "error",
                PRReview.action_taken == "error",
                PRReview.pending_action.in_((
                    "lgtm_comment",
                    "review_comments",
                    "approved_merged",
                )),
                PRReview.published_actor.is_(None),
                PRReview.published_at.is_(None),
                PRReview.github_review_id.is_(None),
                PRReview.github_review_url.is_(None),
                PRReview.github_review_state.is_(None),
                PRReview.review_summary.is_not(None),
                or_(*(
                    func.lower(PRReview.review_summary).like(f"%{marker}%")
                    for marker in _LEGACY_LIFECYCLE_RECOVERY_MARKERS
                )),
            ),
            True,
        ),
        else_=False,
    ).label("publication_failure_is_lifecycle")
    review_columns = (
        PRReview.id.label("result_review_id"),
        PRReview.pr_number.label("result_review_pr_number"),
        PRReview.base_ref.label("result_review_base_ref"),
        PRReview.base_sha.label("result_review_base_sha"),
        PRReview.head_sha.label("result_review_head_sha"),
        PRReview.pr_title.label("result_review_pr_title"),
        PRReview.pr_url.label("result_review_pr_url"),
        PRReview.status.label("result_review_status"),
        PRReview.code_verdict.label("result_review_code_verdict"),
        PRReview.action_taken.label("result_review_action_taken"),
        PRReview.ci_status.label("result_review_ci_status"),
        PRReview.publication_state.label("result_review_publication_state"),
        PRReview.failure_stage.label("result_review_failure_stage"),
        PRReview.error_category.label("result_review_error_category"),
        PRReview.error_measured.label("result_review_error_measured"),
        PRReview.error_limit.label("result_review_error_limit"),
        PRReview.error_unit.label("result_review_error_unit"),
        PRReview.published_actor.label("result_review_published_actor"),
        PRReview.published_at.label("result_review_published_at"),
        PRReview.github_review_id.label("result_review_github_review_id"),
        PRReview.github_review_url.label("result_review_github_review_url"),
        PRReview.github_review_state.label("result_review_github_review_state"),
        armed_action,
        legacy_lifecycle_publication_failure,
    )
    terminal_intent_present = case(
        (
            or_(
                PRMonitorRun.legacy_terminal_recovery_pending.is_(True),
                PRMonitorRun.terminal_intent_status.is_not(None),
                PRMonitorRun.terminal_intent_base_ref.is_not(None),
                PRMonitorRun.terminal_intent_head_sha.is_not(None),
                PRMonitorRun.terminal_intent_delivery_id.is_not(None),
                PRMonitorRun.terminal_intent_observed_at.is_not(None),
            ),
            True,
        ),
        else_=False,
    ).label("result_run_terminal_intent_present")
    run_stmt = (
        select(
            PRMonitorRun.id.label("result_run_id"),
            PRMonitorRun.repo_id.label("result_run_repo_id"),
            PRMonitorRun.status.label("result_run_status"),
            PRMonitorRun.current_head_sha.label("result_run_current_head_sha"),
            PRMonitorRun.current_review_id.label("result_run_current_review_id"),
            PRMonitorRun.display_task_id.label("result_run_display_task_id"),
            PRMonitorRun.terminal_intent_status.label(
                "result_run_terminal_intent_status"
            ),
            terminal_intent_present,
            PRMonitorRun.created_at.label("result_run_created_at"),
            PRMonitorRun.updated_at.label("result_run_updated_at"),
            PRMonitorRun.completed_at.label("result_run_completed_at"),
            MonitoredRepo.id.label("result_repo_id"),
            MonitoredRepo.repo_full_name.label("result_repo_full_name"),
            MonitoredRepo.enabled.label("result_repo_enabled"),
            *review_columns,
        )
        .join(MonitoredRepo, MonitoredRepo.id == PRMonitorRun.repo_id)
        .join(
            PRReview,
            and_(
                PRReview.id == PRMonitorRun.current_review_id,
                PRReview.monitor_run_id == PRMonitorRun.id,
                PRReview.repo_id == PRMonitorRun.repo_id,
                PRReview.pr_number == PRMonitorRun.pr_number,
                PRReview.base_sha == PRMonitorRun.current_base_sha,
                PRReview.head_sha == PRMonitorRun.current_head_sha,
            ),
        )
        .outerjoin(Task, Task.id == PRReview.task_id)
        .where(
            or_(
                PRReview.delivery_id.is_(None),
                ~PRReview.delivery_id.like("delivery:%"),
            ),
            or_(
                Task.id.is_(None),
                and_(
                    Task.mode != "delivery_loop",
                    Task.delivery_run_id.is_(None),
                ),
            ),
            ~select(DeliveryRun.id).where(
                DeliveryRun.pr_monitor_run_id == PRMonitorRun.id,
            ).correlate(PRMonitorRun).exists(),
        )
    )

    # Pre-Run Single reviews are retained as a narrow read-only compatibility
    # projection.  Only the latest orphan for one repo/PR is eligible, and an
    # existing lifecycle Run always wins.  Delivery has three independent
    # ownership signals; exclude all of them redundantly so Delivery internals
    # can never leak into the ordinary Tasks-style result feed.
    newer_orphan = aliased(PRReview, name="newer_orphan_review")
    newer_orphan_task = aliased(Task, name="newer_orphan_task")
    orphan_updated_at = func.coalesce(
        PRReview.completed_at,
        PRReview.created_at,
    )
    orphan_stmt = (
        select(
            literal(None).label("result_run_id"),
            PRReview.repo_id.label("result_run_repo_id"),
            literal(None).label("result_run_status"),
            PRReview.head_sha.label("result_run_current_head_sha"),
            PRReview.id.label("result_run_current_review_id"),
            literal(None).label("result_run_display_task_id"),
            literal(None).label("result_run_terminal_intent_status"),
            literal(False).label("result_run_terminal_intent_present"),
            PRReview.created_at.label("result_run_created_at"),
            orphan_updated_at.label("result_run_updated_at"),
            PRReview.completed_at.label("result_run_completed_at"),
            MonitoredRepo.id.label("result_repo_id"),
            MonitoredRepo.repo_full_name.label("result_repo_full_name"),
            MonitoredRepo.enabled.label("result_repo_enabled"),
            *review_columns,
        )
        .select_from(PRReview)
        .join(MonitoredRepo, MonitoredRepo.id == PRReview.repo_id)
        .outerjoin(Task, Task.id == PRReview.task_id)
        .where(
            PRReview.monitor_run_id.is_(None),
            ~select(PRMonitorRun.id).where(
                PRMonitorRun.repo_id == PRReview.repo_id,
                PRMonitorRun.pr_number == PRReview.pr_number,
            ).correlate(PRReview).exists(),
            ~select(newer_orphan.id)
            .select_from(newer_orphan)
            .outerjoin(
                newer_orphan_task,
                newer_orphan_task.id == newer_orphan.task_id,
            )
            .where(
                newer_orphan.repo_id == PRReview.repo_id,
                newer_orphan.pr_number == PRReview.pr_number,
                newer_orphan.monitor_run_id.is_(None),
                newer_orphan.id > PRReview.id,
                or_(
                    newer_orphan.delivery_id.is_(None),
                    ~newer_orphan.delivery_id.like("delivery:%"),
                ),
                or_(
                    newer_orphan_task.id.is_(None),
                    and_(
                        newer_orphan_task.mode != "delivery_loop",
                        newer_orphan_task.delivery_run_id.is_(None),
                    ),
                ),
            )
            .correlate(PRReview)
            .exists(),
            or_(
                PRReview.delivery_id.is_(None),
                ~PRReview.delivery_id.like("delivery:%"),
            ),
            or_(
                Task.id.is_(None),
                and_(
                    Task.mode != "delivery_loop",
                    Task.delivery_run_id.is_(None),
                ),
            ),
            ~select(DeliveryRun.id).where(
                DeliveryRun.monitored_repo_id == PRReview.repo_id,
                DeliveryRun.pr_number == PRReview.pr_number,
            ).correlate(PRReview).exists(),
        )
    )

    if not is_admin(request):
        user_id = get_current_user_id(request)
        if user_id is None:
            run_stmt = run_stmt.where(False)
            orphan_stmt = orphan_stmt.where(False)
        else:
            # Projectless monitors have no share authority and remain
            # administrator-only, matching GET /repos.
            project_ids = await _member_visible_pr_project_ids(db, user_id)
            visibility = (
                MonitoredRepo.project_id.in_(project_ids)
                if project_ids else False
            )
            run_stmt = run_stmt.where(visibility)
            orphan_stmt = orphan_stmt.where(visibility)

    # UNION first, then apply one global stable order and one pagination
    # window.  Per-branch offsets would duplicate or omit rows when a newer
    # orphan interleaves with Run-backed results between requests.
    candidates = run_stmt.union_all(orphan_stmt).subquery(
        "pr_result_candidates"
    )
    selected_rows = list((await db.execute(
        select(candidates)
        .order_by(
            desc(candidates.c.result_run_updated_at),
            desc(candidates.c.result_review_id),
        )
        .offset((page - 1) * size)
        .limit(size)
    )).all())
    if not selected_rows:
        return []

    # Convert the explicit scalar projection into the same tiny attribute
    # shapes used by the pure enum projectors below.  No ORM entity is loaded,
    # so webhook secrets, prompts, pending bodies, nonces, leases, reviewer
    # errors, and task/session identities cannot be reached by later code.
    rows = []
    for item in selected_rows:
        monitor_run = (
            SimpleNamespace(
                id=item.result_run_id,
                repo_id=item.result_run_repo_id,
                status=item.result_run_status,
                current_head_sha=item.result_run_current_head_sha,
                current_review_id=item.result_run_current_review_id,
                display_task_id=item.result_run_display_task_id,
                terminal_intent_status=(
                    item.result_run_terminal_intent_status
                ),
                terminal_intent_present=(
                    item.result_run_terminal_intent_present
                ),
                created_at=item.result_run_created_at,
                updated_at=item.result_run_updated_at,
                completed_at=item.result_run_completed_at,
            )
            if item.result_run_id is not None
            else None
        )
        repo = SimpleNamespace(
            id=item.result_repo_id,
            repo_full_name=item.result_repo_full_name,
            enabled=item.result_repo_enabled,
        )
        review = SimpleNamespace(
            id=item.result_review_id,
            pr_number=item.result_review_pr_number,
            base_ref=item.result_review_base_ref,
            base_sha=item.result_review_base_sha,
            head_sha=item.result_review_head_sha,
            pr_title=item.result_review_pr_title,
            pr_url=item.result_review_pr_url,
            status=item.result_review_status,
            code_verdict=item.result_review_code_verdict,
            action_taken=item.result_review_action_taken,
            ci_status=item.result_review_ci_status,
            publication_state=item.result_review_publication_state,
            failure_stage=item.result_review_failure_stage,
            error_category=item.result_review_error_category,
            error_measured=item.result_review_error_measured,
            error_limit=item.result_review_error_limit,
            error_unit=item.result_review_error_unit,
            published_actor=item.result_review_published_actor,
            published_at=item.result_review_published_at,
            github_review_id=item.result_review_github_review_id,
            github_review_url=item.result_review_github_review_url,
            github_review_state=item.result_review_github_review_state,
            publication_failure_is_lifecycle=(
                item.publication_failure_is_lifecycle
            ),
            created_at=item.result_run_created_at,
            completed_at=item.result_run_completed_at,
        )
        rows.append((monitor_run, repo, review, item.armed_action))

    review_ids = [review.id for _run, _repo, review, _armed in rows]
    reviewer_rows = list((await db.execute(
        select(
            PRReviewerRun.pr_review_id,
            PRReviewerRun.role,
            PRReviewerRun.status,
            PRReviewerRun.verdict,
        )
        .where(PRReviewerRun.pr_review_id.in_(review_ids))
        .order_by(PRReviewerRun.pr_review_id, PRReviewerRun.id)
    )).all())
    runs_by_review: dict[int, list] = {}
    for reviewer in reviewer_rows:
        runs_by_review.setdefault(reviewer.pr_review_id, []).append(reviewer)

    rerun_blocked = await _rerun_blocked_run_ids(
        db,
        [
            monitor_run
            for monitor_run, _repo, _review, _armed in rows
            if monitor_run is not None
        ],
    )
    result: list[dict] = []
    for monitor_run, repo, review, armed_action_value in rows:
        reviewer_runs = runs_by_review.get(review.id, [])
        states = _public_result_feed_states(
            review,
            reviewer_runs,
            monitor_run=monitor_run,
            armed_action=armed_action_value,
        )
        aggregate = states["aggregate_verdict"]
        input_error = _public_input_error_evidence(review)
        display_status = (
            "Changes required"
            if aggregate == "changes_required"
            else "Passed"
            if aggregate == "pass"
            else "Review input too large"
            if input_error["error_category"] == "unsupported_input_size"
            else "Infrastructure error"
            if states["failure_stage"] in {
                "reviewer",
                "ci",
                "github_identity",
                "publication",
                "merge",
                "recovery",
            }
            else _REVIEW_DISPLAY_STATUSES.get(
                review.status,
                review.status.replace("_", " ").title(),
            )
        )
        result.append({
            "result_key": (
                f"run:{monitor_run.id}"
                if monitor_run is not None
                else f"review:{review.id}"
            ),
            "run_id": monitor_run.id if monitor_run is not None else None,
            "display_task_id": (
                monitor_run.display_task_id
                if monitor_run is not None
                else None
            ),
            "repo_id": repo.id,
            "repo_full_name": repo.repo_full_name,
            "pr_number": review.pr_number,
            "pr_title": review.pr_title,
            "pr_url": review.pr_url,
            "review_id": review.id,
            "base_ref": review.base_ref,
            "base_sha": review.base_sha,
            "head_sha": review.head_sha,
            **states,
            "display_status": display_status,
            "display_summary": (
                "The exact PR review input exceeded the configured safe model "
                "limit, so no Reviewer Task was created."
                if input_error["error_category"] == "unsupported_input_size"
                else _result_feed_display_summary(states)
            ),
            **input_error,
            **_public_publication_evidence(review),
            "created_at": (
                monitor_run.created_at
                if monitor_run is not None
                else review.created_at
            ),
            "updated_at": (
                monitor_run.updated_at
                if monitor_run is not None
                else (review.completed_at or review.created_at)
            ),
            "completed_at": (
                monitor_run.completed_at
                if monitor_run is not None
                else review.completed_at
            ),
            "can_rerun": bool(
                monitor_run is not None
                and repo.enabled
                and monitor_run.id not in rerun_blocked
                and monitor_run.status not in {"merged", "closed"}
                and monitor_run.completed_at is None
                and review.status not in _ACTIVE_REVIEW_STATUSES
                and review.id == monitor_run.current_review_id
                and review.head_sha == monitor_run.current_head_sha
            ),
        })
    return result


@router.get("/reviews/{review_id}", response_model=PRReviewDetailResponse)
async def get_review(
    review_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    review = await db.get(PRReview, review_id)
    if not review:
        raise HTTPException(404, "Review not found")
    repo = await db.get(MonitoredRepo, review.repo_id)
    if repo is None:
        raise HTTPException(404, "Repository not found")
    await _require_pr_monitor_access(request, db, repo)
    runs = list((await db.execute(
        select(PRReviewerRun)
        .where(PRReviewerRun.pr_review_id == review.id)
        .order_by(PRReviewerRun.id)
        .options(load_only(
            PRReviewerRun.id,
            PRReviewerRun.pr_review_id,
            PRReviewerRun.role,
            PRReviewerRun.task_id,
            PRReviewerRun.provider,
            PRReviewerRun.model,
            PRReviewerRun.effort,
            PRReviewerRun.status,
            PRReviewerRun.verdict,
            PRReviewerRun.result_body,
            PRReviewerRun.error_message,
            PRReviewerRun.created_at,
            PRReviewerRun.completed_at,
        ))
    )).scalars())
    findings = list((await db.execute(
        select(PRFinding)
        .where(PRFinding.pr_review_id == review.id)
        .order_by(PRFinding.id)
    )).scalars())
    rebuttals = list((await db.execute(
        select(PRFindingRebuttal)
        .where(PRFindingRebuttal.pr_review_id == review.id)
        .order_by(PRFindingRebuttal.id)
    )).scalars())
    actions = list((await db.execute(
        select(PRFindingAction)
        .where(PRFindingAction.finding_id.in_([item.id for item in findings]))
        .order_by(PRFindingAction.id)
    )).scalars()) if findings else []
    by_run: dict[int, list[PRFinding]] = {}
    by_finding: dict[int, list[PRFindingRebuttal]] = {}
    latest_action: dict[int, PRFindingAction] = {}
    for action in actions:
        latest_action[action.finding_id] = action
    for rebuttal in rebuttals:
        by_finding.setdefault(rebuttal.finding_id, []).append(rebuttal)
    for finding in findings:
        by_run.setdefault(finding.reviewer_run_id, []).append(finding)
    monitor_run = (
        await db.get(PRMonitorRun, review.monitor_run_id)
        if review.monitor_run_id is not None
        else None
    )
    if not _monitor_run_exactly_binds_review(monitor_run, review):
        monitor_run = None
    rerun_blocked = await _rerun_blocked_run_ids(
        db,
        [monitor_run] if monitor_run is not None else [],
    )
    payload = _review_response_payload(
        review,
        runs,
        include_full_summary=True,
        monitor_run=monitor_run,
        monitor_enabled=repo.enabled,
        rerun_effects_clear=bool(
            monitor_run is not None and monitor_run.id not in rerun_blocked
        ),
    )
    from backend.services.pr_review_actions import is_current_review_snapshot

    payload["is_current_snapshot"] = await is_current_review_snapshot(db, review)
    payload["reviewer_runs"] = [
        PRReviewerRunResponse.model_validate(run).model_copy(
            update={
                "outcome_kind": _reviewer_outcome_kind(run),
                "findings": [
                PRFindingResponse.model_validate(finding).model_copy(update={
                    "rebuttals": [
                        PRFindingRebuttalResponse.model_validate(item)
                        for item in by_finding.get(finding.id, [])
                    ],
                    "latest_action": (
                        PRFindingActionResponse.model_validate(
                            _action_response_payload(latest_action[finding.id])
                        )
                        if finding.id in latest_action else None
                    ),
                })
                for finding in by_run.get(run.id, [])
                ],
            }
        )
        for run in runs
    ]
    return payload


def _rerun_response_payload(review: PRReview) -> dict:
    """Return the public admission receipt, never internal Task metadata."""

    if (
        review.rerun_of_review_id is None
        or review.monitor_run_id is None
        or review.head_sha is None
    ):
        raise HTTPException(500, "Rerun admission receipt is incomplete")
    return {
        "id": review.id,
        "attempt": review.attempt,
        "rerun_of_review_id": review.rerun_of_review_id,
        "monitor_run_id": review.monitor_run_id,
        "status": review.status,
        "head_sha": review.head_sha,
    }


def _rerun_winner_exactly_binds_source(
    winner: PRReview,
    source: PRReview,
    run: PRMonitorRun | None,
) -> bool:
    """Validate immutable rerun lineage before exposing its narrow receipt.

    An idempotency replay is allowed after the lifecycle Run has advanced.
    Requiring the winner to remain ``current_review_id`` would turn a
    successful request into a later 409 after a synchronize/close webhook,
    which violates the key's replay contract.  The receipt does not authorize
    a new effect, so prove the immutable source/winner/Run ownership instead
    of re-proving current admission state.
    """

    return bool(
        run is not None
        and winner.id != source.id
        and winner.rerun_of_review_id == source.id
        and winner.attempt == source.attempt + 1
        and (
            source.delivery_id is None
            or not source.delivery_id.startswith("delivery:")
        )
        and winner.delivery_id is None
        and source.monitor_run_id == run.id
        and winner.monitor_run_id == run.id
        and run.repo_id == source.repo_id
        and run.pr_number == source.pr_number
        and winner.repo_id == source.repo_id
        and winner.pr_number == source.pr_number
        and winner.base_ref == source.base_ref
        and winner.base_sha == source.base_sha
        and winner.head_sha == source.head_sha
        and winner.pr_title == source.pr_title
        and winner.pr_author == source.pr_author
        and winner.pr_url == source.pr_url
    )


async def _require_exact_rerun_winner(
    db: AsyncSession,
    *,
    winner: PRReview,
    source: PRReview,
) -> PRReview:
    run = (
        await db.get(PRMonitorRun, winner.monitor_run_id)
        if winner.monitor_run_id is not None
        else None
    )
    if not _rerun_winner_exactly_binds_source(winner, source, run):
        raise HTTPException(
            409,
            "Idempotent rerun result does not match the selected review",
        )
    return winner


@router.post("/reviews/{review_id}/rerun", response_model=PRReviewRerunResponse)
async def rerun_pr_review(
    review_id: int,
    body: PRReviewRerunRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Create one immutable, idempotent review attempt for the exact head."""

    source = await db.get(PRReview, review_id)
    if source is None:
        raise HTTPException(404, "Review not found")
    repo = await db.get(MonitoredRepo, source.repo_id)
    if repo is None:
        raise HTTPException(404, "Repository not found")
    await _require_pr_monitor_access(request, db, repo)
    if source.head_sha != body.expected_head_sha:
        raise HTTPException(409, "Expected head does not match the selected review")
    existing = await db.scalar(
        select(PRReview).where(
            PRReview.rerun_of_review_id == source.id,
            PRReview.rerun_idempotency_key == body.idempotency_key,
        )
    )
    if existing is not None:
        await _require_exact_rerun_winner(
            db,
            winner=existing,
            source=source,
        )
        return _rerun_response_payload(existing)
    if source.status in _ACTIVE_REVIEW_STATUSES:
        raise HTTPException(409, "The selected review is still active")
    run = (
        await db.get(PRMonitorRun, source.monitor_run_id)
        if source.monitor_run_id is not None
        else None
    )
    if (
        not _monitor_run_exactly_binds_review(run, source)
        or run.current_head_sha != body.expected_head_sha
    ):
        raise HTTPException(409, "Only the current exact-head review can be rerun")
    if run.status in {"merged", "closed"} or run.completed_at is not None:
        raise HTTPException(409, "A terminal PR lifecycle cannot be rerun")
    await _require_legacy_pr_effect_allowed(
        db,
        action="rerun",
        review=source,
        monitor_run=run,
    )
    review_data = {
        "number": source.pr_number,
        "base_ref": source.base_ref,
        "base_sha": source.base_sha,
        "head_sha": source.head_sha,
        "delivery_id": None,
        "title": source.pr_title,
        "author": source.pr_author,
        "url": source.pr_url,
        "head_repo_full_name": run.head_repo_full_name,
        "head_branch": run.head_branch,
    }
    repo_id = repo.id
    repo = _webhook_repo_snapshot(repo)
    observed_source_generation = (
        source.id,
        source.status,
        source.attempt,
        source.monitor_run_id,
        source.base_ref,
        source.base_sha,
        source.head_sha,
        source.completed_at,
    )
    observed_run_generation = (
        run.id,
        run.state_version,
        run.status,
        run.current_review_id,
        run.current_base_sha,
        run.current_head_sha,
        run.completed_at,
        run.terminal_intent_status,
    )
    await db.rollback()

    prepared_context, preliminary_rejection = (
        await _capture_pr_review_context_rejection(repo, review_data)
    )
    if prepared_context is None:
        raise HTTPException(
            503,
            "PR review input could not be revalidated against the current "
            "repository policy",
        )
    from backend.services.pr_review_service import (
        verify_pr_review_snapshot_current,
    )

    await verify_pr_review_snapshot_current(
        repo,
        review_data,
        base_ref=review_data["base_ref"],
    )
    prepared_ci_evidence = (
        await _capture_webhook_ci_evidence(
            repo,
            head_sha=review_data["head_sha"],
        )
        if preliminary_rejection is None
        else None
    )

    async with _pr_repo_write_lock(repo_id):
        try:
            locked_repo = await lock_pr_repo_action_boundary(db, repo_id)
        except FindingActionConflict as exc:
            raise HTTPException(404, "Repository not found") from exc
        await _reauthorize_pr_effect(request, db, locked_repo)
        # Preserve the controller-wide PRMonitorRun -> PRReview row-lock
        # order.  This discovery read does not lock either row; both identities
        # are revalidated after the ordered locks are acquired.
        source_monitor_run_id = await db.scalar(
            select(PRReview.monitor_run_id).where(
                PRReview.id == review_id,
                PRReview.repo_id == repo_id,
            )
        )
        run = (await db.execute(
            select(PRMonitorRun)
            .where(
                PRMonitorRun.id == source_monitor_run_id,
                PRMonitorRun.repo_id == repo_id,
            )
            .with_for_update()
        )).scalar_one_or_none()
        source = (await db.execute(
            select(PRReview)
            .where(PRReview.id == review_id, PRReview.repo_id == repo_id)
            .with_for_update()
        )).scalar_one_or_none()
        if source is None:
            raise HTTPException(404, "Review not found")
        if source.head_sha != body.expected_head_sha:
            raise HTTPException(409, "Expected head does not match the selected review")
        existing = await db.scalar(
            select(PRReview).where(
                PRReview.rerun_of_review_id == source.id,
                PRReview.rerun_idempotency_key == body.idempotency_key,
            )
        )
        if existing is not None:
            existing_id = existing.id
            await db.rollback()
            existing = await db.get(PRReview, existing_id)
            source = await db.get(PRReview, review_id)
            if existing is None or source is None:
                raise HTTPException(
                    409,
                    "Idempotent rerun result changed during validation",
                )
            await _require_exact_rerun_winner(
                db,
                winner=existing,
                source=source,
            )
            return _rerun_response_payload(existing)
        if not locked_repo.enabled:
            raise HTTPException(409, "Enable the PR monitor before rerunning")
        if (
            (
                source.id,
                source.status,
                source.attempt,
                source.monitor_run_id,
                source.base_ref,
                source.base_sha,
                source.head_sha,
                source.completed_at,
            )
            != observed_source_generation
            or (
                run.id,
                run.state_version,
                run.status,
                run.current_review_id,
                run.current_base_sha,
                run.current_head_sha,
                run.completed_at,
                run.terminal_intent_status,
            )
            != observed_run_generation
            or
            not _monitor_run_exactly_binds_review(run, source)
            or source.head_sha != body.expected_head_sha
            or run.current_head_sha != body.expected_head_sha
            or source.status in _ACTIVE_REVIEW_STATUSES
            or run.status in {"merged", "closed"}
            or run.completed_at is not None
        ):
            raise HTTPException(409, "PR review subject changed before rerun")
        await _require_legacy_pr_effect_allowed(
            db,
            action="rerun",
            review=source,
            monitor_run=run,
        )
        active_fix = await _active_finding_action_for_repo(db, repo_id)
        active_rebuttal = await db.scalar(
            select(PRFindingRebuttal.id)
            .where(
                PRFindingRebuttal.monitor_run_id == run.id,
                PRFindingRebuttal.status.in_(_ACTIVE_ADJUDICATION_STATUSES),
            )
            .limit(1)
            .with_for_update()
        )
        active_repair = await db.scalar(
            select(PRRepairWake.id)
            .where(
                PRRepairWake.monitor_run_id == run.id,
                PRRepairWake.status.in_(_STARTED_REPAIR_STATUSES),
            )
            .limit(1)
            .with_for_update()
        )
        active_merge = await db.scalar(
            select(PRMergeQueueAction.id)
            .where(
                PRMergeQueueAction.monitor_run_id == run.id,
                or_(
                    PRMergeQueueAction.status.in_(
                        _STARTED_MERGE_QUEUE_STATUSES
                    ),
                    pr_merge_queue_action_ambiguous_remote_effect_predicate(),
                ),
            )
            .limit(1)
            .with_for_update()
        )
        active_thread_resolution = await db.scalar(
            select(PRFinding.id)
            .join(PRReview, PRReview.id == PRFinding.pr_review_id)
            .where(
                PRReview.monitor_run_id == run.id,
                PRFinding.resolution_lease_token.is_not(None),
            )
            .limit(1)
            .with_for_update()
        )
        if any((
            active_fix,
            active_rebuttal,
            active_repair,
            active_merge,
            active_thread_resolution,
            run.status in _EXTERNALLY_BUSY_RUN_STATUSES,
        )):
            raise HTTPException(
                409,
                "Cannot rerun while a Finding fix, rebuttal, Repair, thread "
                "resolution, or Merge Queue effect is active",
            )
        locked_input_rejection = _capture_pr_review_preflight_rejection(
            locked_repo,
            review_data,
            prepared_context=prepared_context,
        )
        if locked_input_rejection is not None:
            await db.rollback()
            raise HTTPException(
                status_code=422,
                detail=locked_input_rejection.public_detail,
            )
        locked_ci_evidence = _require_locked_ci_evidence(
            locked_repo,
            head_sha=source.head_sha,
            evidence=prepared_ci_evidence,
        )
        latest_attempt = await db.scalar(
            select(func.max(PRReview.attempt)).where(
                PRReview.repo_id == repo_id,
                PRReview.pr_number == source.pr_number,
                PRReview.base_ref == source.base_ref,
                PRReview.base_sha == source.base_sha,
                PRReview.head_sha == source.head_sha,
            )
        )
        review_data.update({
            "_review_attempt": int(latest_attempt or 0) + 1,
            "_rerun_of_review_id": source.id,
            "_rerun_idempotency_key": body.idempotency_key,
        })
        try:
            created = await _create_pr_review_task_or_422(
                db,
                locked_repo,
                review_data,
                prepared_context=prepared_context,
                prepared_ci_evidence=locked_ci_evidence,
                allow_remote_ci=False,
            )
        except IntegrityError:
            await db.rollback()
            winner = await db.scalar(
                select(PRReview).where(
                    PRReview.rerun_of_review_id == review_id,
                    PRReview.rerun_idempotency_key == body.idempotency_key,
                )
            )
            if winner is None:
                raise HTTPException(409, "A concurrent rerun changed this PR subject")
            source = await db.get(PRReview, review_id)
            if source is None:
                raise HTTPException(409, "A concurrent rerun changed this PR subject")
            await _require_exact_rerun_winner(
                db,
                winner=winner,
                source=source,
            )
            created = winner
    return _rerun_response_payload(created)


async def _load_authorized_finding(
    request: Request,
    db: AsyncSession,
    finding_id: int,
) -> tuple[PRFinding, PRReview, MonitoredRepo]:
    finding = await db.get(PRFinding, finding_id)
    review = (
        await db.get(PRReview, finding.pr_review_id)
        if finding is not None else None
    )
    repo = (
        await db.get(MonitoredRepo, review.repo_id)
        if review is not None else None
    )
    if finding is None or review is None or repo is None:
        raise HTTPException(404, "Finding not found")
    await _require_pr_monitor_access(request, db, repo)
    return finding, review, repo


async def _perform_immediate_finding_action(
    *,
    request: Request,
    db: AsyncSession,
    finding_id: int,
    action_type: str,
    idempotency_key: str,
    human_advice: str | None = None,
) -> dict:
    finding, review, repo = await _load_authorized_finding(
        request, db, finding_id
    )
    await _require_legacy_pr_effect_allowed(
        db,
        action=(
            "ignored" if action_type == "ignore" else "given legacy advice"
        ),
        review=review,
    )
    from backend.services.pr_review_actions import (
        FindingActionConflict,
        create_immediate_finding_action,
    )

    finding_row_id = finding.id
    review_row_id = review.id
    repo_id = repo.id
    actor_user_id = get_current_user_id(request)
    await db.rollback()
    async with _pr_repo_write_lock(repo_id):
        try:
            action = await create_immediate_finding_action(
                db,
                finding_id=finding_row_id,
                review_id=review_row_id,
                action_type=action_type,
                idempotency_key=idempotency_key,
                actor_user_id=actor_user_id,
                human_advice=human_advice,
                effect_authorizer=_pr_effect_authorizer(request),
            )
        except FindingActionConflict as exc:
            raise HTTPException(409, str(exc)) from exc
    return _action_response_payload(action)


@router.post(
    "/findings/{finding_id}/ignore",
    response_model=PRFindingActionResponse,
)
async def ignore_review_finding(
    finding_id: int,
    body: FindingActionRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    return await _perform_immediate_finding_action(
        request=request,
        db=db,
        finding_id=finding_id,
        action_type="ignore",
        idempotency_key=body.idempotency_key,
    )


@router.post(
    "/findings/{finding_id}/advice",
    response_model=PRFindingActionResponse,
)
async def save_review_finding_advice(
    finding_id: int,
    body: HumanAdviceRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    return await _perform_immediate_finding_action(
        request=request,
        db=db,
        finding_id=finding_id,
        action_type="human_advice",
        idempotency_key=body.idempotency_key,
        human_advice=body.advice,
    )


@router.post(
    "/findings/{finding_id}/fix",
    response_model=PRFindingActionResponse,
)
async def create_review_finding_fix(
    finding_id: int,
    body: FindingActionRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    finding, review, repo = await _load_authorized_finding(
        request, db, finding_id
    )
    await _require_legacy_pr_effect_allowed(
        db,
        action="repaired",
        review=review,
    )
    from backend.services.pr_review_actions import FindingActionConflict
    from backend.services.pr_review_fix import FixConfirmationError, create_fix_task
    from backend.services.pr_review_service import GhError

    finding_row_id = finding.id
    review_row_id = review.id
    repo_id = repo.id
    actor_user_id = get_current_user_id(request)
    await db.rollback()
    async with _pr_repo_write_lock(repo_id):
        try:
            action = await create_fix_task(
                db,
                finding_id=finding_row_id,
                review_id=review_row_id,
                repo_id=repo_id,
                idempotency_key=body.idempotency_key,
                actor_user_id=actor_user_id,
                effect_authorizer=_pr_effect_authorizer(request),
            )
        except (FindingActionConflict, FixConfirmationError) as exc:
            raise HTTPException(409, str(exc)) from exc
        except GhError as exc:
            raise HTTPException(409, f"PR repair input is no longer available: {exc}") from exc
    return _action_response_payload(action)


async def _load_authorized_finding_action(
    request: Request,
    db: AsyncSession,
    action_id: int,
) -> tuple[PRFindingAction, PRFinding, PRReview, MonitoredRepo]:
    action = await db.get(PRFindingAction, action_id)
    if action is None:
        raise HTTPException(404, "Finding action not found")
    finding, review, repo = await _load_authorized_finding(
        request, db, action.finding_id
    )
    return action, finding, review, repo


@router.get(
    "/actions/{action_id}",
    response_model=PRFindingActionResponse,
)
async def get_review_finding_action(
    action_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    action, _, _, repo = await _load_authorized_finding_action(
        request, db, action_id
    )
    await db.rollback()
    try:
        from backend.database import async_session
        from backend.main import worker_relay
        from backend.services.pr_review_fix import reconcile_finding_action

        await reconcile_finding_action(
            async_session,
            action_id,
            worker_relay=worker_relay,
        )
    except Exception:
        # The periodic reconciler remains authoritative; a transient Worker or
        # GitHub outage must not turn an otherwise readable audit row into 500.
        logger.exception(
            "On-read PR finding action recovery failed for action %s",
            action_id,
        )
    db.expire_all()
    action, _, _, _ = await _load_authorized_finding_action(
        request,
        db,
        action_id,
    )
    return _action_response_payload(action)


@router.get("/actions/{action_id}/diff", response_class=PlainTextResponse)
async def download_review_finding_diff(
    action_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    action, _, _, repo = await _load_authorized_finding_action(
        request, db, action_id
    )
    patch_text = (action.result or {}).get("patch")
    if (
        action.status != "awaiting_confirmation"
        or action.confirmed_at is not None
        or not isinstance(patch_text, str)
        or action.patch_sha256
        != hashlib.sha256(patch_text.encode("utf-8")).hexdigest()
    ):
        raise HTTPException(409, "Validated PR fix diff is not available")
    repo_id = repo.id
    actor_user_id = get_current_user_id(request)
    await db.rollback()
    async with _pr_repo_write_lock(repo_id):
        from backend.services.pr_review_actions import (
            FindingActionConflict,
            lock_pr_repo_action_boundary,
        )

        try:
            locked_repo = await lock_pr_repo_action_boundary(db, repo_id)
        except FindingActionConflict as exc:
            raise HTTPException(409, str(exc)) from exc
        await _reauthorize_pr_effect(request, db, locked_repo)
        if not locked_repo.enabled:
            raise HTTPException(409, "PR monitor is disabled")
        action = (
            await db.execute(
                select(PRFindingAction)
                .where(PRFindingAction.id == action_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
        patch_text = (action.result or {}).get("patch") if action else None
        if (
            action is None
            or action.status != "awaiting_confirmation"
            or action.confirmed_at is not None
            or not isinstance(patch_text, str)
            or action.patch_sha256
            != hashlib.sha256(patch_text.encode("utf-8")).hexdigest()
        ):
            raise HTTPException(409, "Validated PR fix diff is not available")
        confirmation_token = (action.result or {}).get("confirmation_token")
        if not isinstance(confirmation_token, str):
            raise HTTPException(409, "Validated PR fix confirmation is unavailable")
        receipt = secrets.token_urlsafe(32)
        action.download_receipt_hash = hashlib.sha256(
            receipt.encode("utf-8")
        ).hexdigest()
        action.downloaded_by_user_id = actor_user_id
        from backend.services.pr_review_service import _database_now

        action.downloaded_at = await _database_now(db)
        await db.commit()
        return PlainTextResponse(
            patch_text,
            media_type="text/x-diff; charset=utf-8",
            headers={
                "Content-Disposition": (
                    f'inline; filename="pr-fix-{action_id}.diff"'
                ),
                "Cache-Control": "no-store",
                "X-CCM-PR-Fix-Receipt": receipt,
                "X-CCM-PR-Fix-Token": confirmation_token,
            },
        )


@router.post(
    "/actions/{action_id}/confirm",
    response_model=PRFindingActionResponse,
)
async def confirm_review_finding_fix(
    action_id: int,
    body: ConfirmFixRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    action, _, review, repo = await _load_authorized_finding_action(
        request, db, action_id
    )
    await _require_legacy_pr_effect_allowed(
        db,
        action="confirmed for repair",
        review=review,
    )
    from backend.services.pr_review_fix import FixConfirmationError, confirm_fix

    repo_id = repo.id
    actor_user_id = get_current_user_id(request)
    await db.rollback()
    async with _pr_repo_write_lock(repo_id):
        try:
            completed = await confirm_fix(
                db,
                action_id=action_id,
                confirmation_token=body.confirmation_token,
                patch_sha256=body.patch_sha256,
                download_receipt=body.download_receipt,
                confirmed_by_user_id=actor_user_id,
                effect_authorizer=_pr_effect_authorizer(request),
            )
        except FixConfirmationError as exc:
            raise HTTPException(409, str(exc)) from exc
    return _action_response_payload(completed)


@router.post(
    "/actions/{action_id}/cancel",
    response_model=PRFindingActionResponse,
)
async def cancel_review_finding_fix(
    action_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    action, _, _, repo = await _load_authorized_finding_action(
        request,
        db,
        action_id,
    )
    from backend.services.pr_review_fix import (
        FixConfirmationError,
        cancel_fix_action,
    )

    repo_id = repo.id
    actor_user_id = get_current_user_id(request)
    await db.rollback()
    async with _pr_repo_write_lock(repo_id):
        try:
            cancelled = await cancel_fix_action(
                db,
                action_id=action_id,
                cancelled_by_user_id=actor_user_id,
                effect_authorizer=_pr_effect_authorizer(request),
            )
        except FixConfirmationError as exc:
            raise HTTPException(409, str(exc)) from exc
    return _action_response_payload(cancelled)


@router.post(
    "/findings/{finding_id}/rebut",
    response_model=PRFindingRebuttalResponse,
)
async def submit_finding_rebuttal(
    finding_id: int,
    body: PRFindingRebuttalCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Submit exact-subject evidence to an isolated adjudicator."""
    from backend.models.task import Task
    from backend.services.pr_review_adjudication import create_rebuttal_task
    from backend.services.pr_review_service import (
        _gh_pr_view,
        _validated_pr_snapshot,
    )

    finding = await db.get(PRFinding, finding_id)
    if finding is None:
        raise HTTPException(404, "Finding not found")
    review = await db.get(PRReview, finding.pr_review_id)
    run = await db.get(PRMonitorRun, review.monitor_run_id) if review else None
    repo = await db.get(MonitoredRepo, review.repo_id) if review else None
    if review is None or run is None or repo is None:
        raise HTTPException(409, "Finding lifecycle is incomplete")
    await _require_pr_monitor_access(request, db, repo)
    await _require_legacy_pr_effect_allowed(
        db,
        action="rebutted",
        review=review,
        monitor_run=run,
    )
    if (
        review.status not in {"commented", "approved"}
        or run.status not in {"waiting_for_fix", "paused"}
        or run.current_review_id != review.id
        or run.current_head_sha != finding.head_sha
    ):
        raise HTTPException(409, "Finding belongs to a superseded PR head")
    if finding.severity not in {"critical", "high", "medium"} or finding.status != "open":
        raise HTTPException(409, "Only an open blocking Finding can be rebutted")
    if run.developer_task_id is None:
        raise HTTPException(409, "Bind the original Developer Task before rebuttal")
    developer = await db.get(Task, run.developer_task_id)
    if developer is None:
        raise HTTPException(409, "Bound Developer Task no longer exists")
    await require_task_control(request, developer, db)
    repo_id = repo.id
    review_id = review.id
    run_id = run.id
    developer_id = developer.id
    repo_snapshot = _webhook_repo_snapshot(repo)
    review_data = {
        "number": review.pr_number,
        "base_ref": review.base_ref,
        "base_sha": review.base_sha,
        "head_sha": review.head_sha,
        "title": review.pr_title,
        "author": review.pr_author,
        "url": review.pr_url,
    }
    observed_run_generation = (
        run.state_version,
        run.status,
        run.current_review_id,
        run.current_base_sha,
        run.current_head_sha,
        run.developer_task_id,
        run.completed_at,
        run.terminal_intent_status,
    )
    observed_review_generation = (
        review.status,
        review.monitor_run_id,
        review.base_ref,
        review.base_sha,
        review.head_sha,
        review.completed_at,
    )
    observed_finding_generation = (
        finding.pr_review_id,
        finding.status,
        finding.severity,
        finding.base_sha,
        finding.head_sha,
    )
    observed_developer_generation = (
        developer.status,
        developer.retry_count,
        developer.turn_generation,
        developer.session_id,
        developer.last_cwd,
        developer.project_id,
        developer.result_branch,
        developer.completed_at,
    )
    # Authorization and lifecycle reads above are intentionally short.  Release
    # their connection before the two GitHub subject guards and immutable
    # context capture, then exact-CAS every participating row under the writer
    # fence below.
    await db.rollback()
    snapshot = _validated_pr_snapshot(
        await _gh_pr_view(review_data["number"], repo_snapshot.repo_full_name)
    )
    if (
        snapshot.get("state") != "OPEN"
        or snapshot.get("merged_at") is not None
        or snapshot.get("is_draft") is not False
        or snapshot.get("base_ref") != review_data["base_ref"]
        or snapshot.get("base_sha") != review_data["base_sha"]
        or snapshot.get("head_sha") != review_data["head_sha"]
    ):
        raise HTTPException(409, "GitHub PR subject changed before adjudication")
    context, input_rejection = await _capture_pr_review_context_rejection(
        repo_snapshot,
        review_data,
        base_ref=review_data["base_ref"],
    )
    if input_rejection is not None:
        raise HTTPException(422, input_rejection.public_detail)
    if context is None:
        raise HTTPException(503, "PR review input capture is incomplete")
    # Context preparation may perform remote reads.  Refresh the GitHub
    # subject once more before entering the portable database writer fence;
    # holding SQLite's global writer slot across network I/O would block
    # unrelated writes without actually locking the remote PR.
    snapshot = _validated_pr_snapshot(
        await _gh_pr_view(review_data["number"], repo_snapshot.repo_full_name)
    )
    if (
        snapshot.get("state") != "OPEN"
        or snapshot.get("merged_at") is not None
        or snapshot.get("is_draft") is not False
        or snapshot.get("base_ref") != review_data["base_ref"]
        or snapshot.get("base_sha") != review_data["base_sha"]
        or snapshot.get("head_sha") != review_data["head_sha"]
    ):
        raise HTTPException(409, "GitHub PR subject changed before adjudication")
    async with _pr_repo_write_lock(repo_id):
        try:
            repo = await lock_pr_repo_action_boundary(db, repo_id)
        except FindingActionConflict as exc:
            raise HTTPException(
                409,
                "Finding lifecycle changed before adjudication",
            ) from exc
        await _reauthorize_pr_effect(request, db, repo)
        run = (await db.execute(
            select(PRMonitorRun)
            .where(PRMonitorRun.id == run_id, PRMonitorRun.repo_id == repo_id)
            .with_for_update()
        )).scalar_one_or_none()
        review = (await db.execute(
            select(PRReview)
            .where(PRReview.id == review_id, PRReview.repo_id == repo_id)
            .with_for_update()
        )).scalar_one_or_none()
        finding = (await db.execute(
            select(PRFinding)
            .where(PRFinding.id == finding_id, PRFinding.pr_review_id == review_id)
            .with_for_update()
        )).scalar_one_or_none()
        developer = (await db.execute(
            select(Task).where(Task.id == developer_id).with_for_update()
        )).scalar_one_or_none()
        if repo is None or run is None or review is None or finding is None:
            raise HTTPException(409, "Finding lifecycle changed before adjudication")
        await _require_legacy_pr_effect_allowed(
            db,
            action="rebutted",
            review=review,
            monitor_run=run,
            task=developer,
        )
        if developer is None:
            raise HTTPException(409, "Bound Developer Task no longer exists")
        await require_task_control(request, developer, db)
        if not repo.enabled:
            raise HTTPException(409, "PR monitor is disabled")
        if (
            (
                run.state_version,
                run.status,
                run.current_review_id,
                run.current_base_sha,
                run.current_head_sha,
                run.developer_task_id,
                run.completed_at,
                run.terminal_intent_status,
            )
            != observed_run_generation
            or (
                review.status,
                review.monitor_run_id,
                review.base_ref,
                review.base_sha,
                review.head_sha,
                review.completed_at,
            )
            != observed_review_generation
            or (
                finding.pr_review_id,
                finding.status,
                finding.severity,
                finding.base_sha,
                finding.head_sha,
            )
            != observed_finding_generation
            or (
                developer.status,
                developer.retry_count,
                developer.turn_generation,
                developer.session_id,
                developer.last_cwd,
                developer.project_id,
                developer.result_branch,
                developer.completed_at,
            )
            != observed_developer_generation
            or review.status not in {"commented", "approved"}
            or run.status not in {"waiting_for_fix", "paused"}
            or run.current_review_id != review.id
            or run.current_base_sha != review.base_sha
            or run.current_head_sha != review.head_sha
            or review.base_ref != context.get("base_ref")
            or finding.base_sha != review.base_sha
            or finding.head_sha != review.head_sha
            or run.developer_task_id != developer.id
        ):
            raise HTTPException(409, "Finding belongs to a superseded PR subject")
        if finding.severity not in {"critical", "high", "medium"} or finding.status != "open":
            raise HTTPException(409, "Only an open blocking Finding can be rebutted")
        active_fix = (await db.execute(
            select(PRFindingAction.id)
            .where(
                PRFindingAction.finding_id == finding.id,
                or_(
                    PRFindingAction.active_fix_finding_id == finding.id,
                    PRFindingAction.status.in_(_ACTIVE_FINDING_ACTION_STATUSES),
                ),
            )
            .limit(1)
            .with_for_update()
        )).scalar_one_or_none()
        if active_fix is not None:
            raise HTTPException(
                409,
                "This Finding already has an active AI repair",
            )
        active = (await db.execute(
            select(PRFindingRebuttal.id)
            .where(
                PRFindingRebuttal.finding_id == finding.id,
                PRFindingRebuttal.status.in_(_ACTIVE_ADJUDICATION_STATUSES),
            )
            .limit(1)
            .with_for_update()
        )).scalar_one_or_none()
        if active is not None:
            raise HTTPException(409, "This Finding already has an active adjudication")
        return await create_rebuttal_task(
            db,
            repo=repo,
            run=run,
            review=review,
            finding=finding,
            developer_task=developer,
            evidence=body.evidence,
            material=context["material"],
        )


@router.get("/runs/{run_id}", response_model=PRMonitorRunResponse)
async def get_monitor_run(
    run_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    run = await db.get(PRMonitorRun, run_id)
    if run is None:
        raise HTTPException(404, "PR Monitor Run not found")
    repo = await db.get(MonitoredRepo, run.repo_id)
    if repo is None:
        raise HTTPException(404, "Repository not found")
    await _require_pr_monitor_access(request, db, repo)
    wakes = list((await db.execute(
        select(PRRepairWake)
        .where(PRRepairWake.monitor_run_id == run.id)
        .order_by(desc(PRRepairWake.id))
    )).scalars())
    payload = PRMonitorRunResponse.model_validate(run).model_dump()
    payload["wakes"] = [PRRepairWakeResponse.model_validate(item) for item in wakes]
    merge_actions = list((await db.execute(
        select(PRMergeQueueAction)
        .where(PRMergeQueueAction.monitor_run_id == run.id)
        .order_by(desc(PRMergeQueueAction.id))
    )).scalars())
    payload["merge_actions"] = [
        PRMergeActionResponse.model_validate(item) for item in merge_actions
    ]
    reviews = list((await db.execute(
        select(PRReview)
        .where(PRReview.monitor_run_id == run.id)
        .order_by(PRReview.created_at, PRReview.id)
    )).scalars())
    reviewer_rows = list((await db.execute(
        select(PRReviewerRun)
        .where(PRReviewerRun.pr_review_id.in_([item.id for item in reviews]))
        .options(load_only(
            PRReviewerRun.pr_review_id,
            PRReviewerRun.role,
            PRReviewerRun.status,
            PRReviewerRun.verdict,
        ))
    )).scalars()) if reviews else []
    reviewer_runs_by_review: dict[int, list[PRReviewerRun]] = {}
    for reviewer in reviewer_rows:
        reviewer_runs_by_review.setdefault(reviewer.pr_review_id, []).append(reviewer)
    review_history = []
    for review in reviews:
        reviewer_runs = reviewer_runs_by_review.get(review.id, [])
        public_states = _public_review_states(review, reviewer_runs)
        publication_evidence = _public_publication_evidence(review)
        review_history.append(PRMonitorReviewAttemptResponse.model_validate({
            "id": review.id,
            "attempt": review.attempt,
            "head_sha": review.head_sha,
            "status": review.status,
            "aggregate_verdict": public_states["aggregate_verdict"],
            "publication_state": public_states["publication_state"],
            "github_review_id": publication_evidence["github_review_id"],
            "github_review_url": publication_evidence["github_review_url"],
            "created_at": review.created_at,
            "completed_at": review.completed_at,
        }))
    payload["review_history"] = review_history
    return payload


@router.post("/runs/{run_id}/bind-developer", response_model=PRMonitorRunResponse)
async def bind_monitor_developer(
    run_id: int,
    body: PRMonitorBindRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    from backend.models.task import Task
    from backend.services.pr_review_service import _gh_pr_view, _validated_pr_snapshot
    from backend.services.worker_proxy import get_task_operation_lock

    run = await db.get(PRMonitorRun, run_id)
    if run is None:
        raise HTTPException(404, "PR Monitor Run not found")
    repo = await db.get(MonitoredRepo, run.repo_id)
    task = await db.get(Task, body.task_id)
    if repo is None or task is None:
        raise HTTPException(404, "Repository or Developer Task not found")
    await _require_pr_monitor_access(request, db, repo)
    await require_task_control(request, task, db)
    review = (
        await db.get(PRReview, run.current_review_id)
        if run.current_review_id is not None
        else None
    )
    if (
        review is None
        or review.repo_id != repo.id
        or review.monitor_run_id != run.id
    ):
        raise HTTPException(409, "PR Monitor Gate subject is incomplete")
    # Cheap fail-fast before the lock-free GitHub subject read.  The
    # authoritative check is repeated under Repo -> Run -> Review -> Task
    # writer fences below, so a concurrent Delivery adoption also fails
    # closed without making this preliminary read the mutation authority.
    await _require_legacy_pr_effect_allowed(
        db,
        action="bound to a Developer",
        review=review,
        monitor_run=run,
        task=task,
    )
    repo_id = repo.id
    task_id = task.id
    repo_full_name = repo.repo_full_name
    pr_number = run.pr_number
    bound_base_ref = review.base_ref
    bound_base_sha = run.current_base_sha
    bound_head_sha = run.current_head_sha
    observed_run_generation = (
        run.state_version,
        run.status,
        run.current_review_id,
        run.current_base_sha,
        run.current_head_sha,
        run.developer_task_id,
        run.completed_at,
        run.terminal_intent_status,
    )
    observed_review_generation = (
        review.status,
        review.monitor_run_id,
        review.base_ref,
        review.base_sha,
        review.head_sha,
        review.completed_at,
    )
    observed_task_generation = (
        task.status,
        task.retry_count,
        task.turn_generation,
        task.session_id,
        task.last_cwd,
        task.project_id,
        task.result_branch,
        task.completed_at,
    )
    await db.rollback()
    snapshot = _validated_pr_snapshot(
        await _gh_pr_view(pr_number, repo_full_name)
    )
    if (
        snapshot.get("state") != "OPEN"
        or snapshot.get("merged_at") is not None
        or snapshot.get("is_draft") is not False
        or snapshot.get("base_ref") != bound_base_ref
        or snapshot.get("base_sha") != bound_base_sha
        or snapshot.get("head_sha") != bound_head_sha
    ):
        raise HTTPException(
            409,
            "GitHub PR subject changed while binding; wait for synchronize",
        )
    async with _pr_repo_write_lock(repo_id):
        async with get_task_operation_lock(task_id):
            try:
                repo = await lock_pr_repo_action_boundary(db, repo_id)
            except FindingActionConflict as exc:
                raise HTTPException(
                    404,
                    "Repository not found",
                ) from exc
            await _reauthorize_pr_effect(request, db, repo)
            run = (await db.execute(
                select(PRMonitorRun)
                .where(PRMonitorRun.id == run_id, PRMonitorRun.repo_id == repo_id)
                .with_for_update()
            )).scalar_one_or_none()
            review = (
                (await db.execute(
                    select(PRReview)
                    .where(
                        PRReview.id == run.current_review_id,
                        PRReview.repo_id == repo_id,
                        PRReview.monitor_run_id == run.id,
                    )
                    .with_for_update()
                )).scalar_one_or_none()
                if run is not None and run.current_review_id is not None
                else None
            )
            task = (await db.execute(
                select(Task).where(Task.id == task_id).with_for_update()
            )).scalar_one_or_none()
            if repo is None or run is None or task is None:
                raise HTTPException(404, "Repository, Run, or Developer Task not found")
            if (
                review is None
                or review.monitor_run_id != run.id
                or review.base_sha != run.current_base_sha
                or review.head_sha != run.current_head_sha
            ):
                raise HTTPException(409, "PR Monitor Gate subject is incomplete")
            await require_task_control(request, task, db)
            await _require_legacy_pr_effect_allowed(
                db,
                action="bound to a Developer",
                monitor_run=run,
                task=task,
            )
            if not repo.enabled:
                raise HTTPException(409, "Cannot bind a Developer while the monitor is disabled")
            if run.status in {"merged", "closed"} or run.completed_at is not None:
                raise HTTPException(409, "Cannot bind a Developer to a terminal PR Monitor Run")
            if run.status in _EXTERNALLY_BUSY_RUN_STATUSES:
                raise HTTPException(409, "Cannot bind a Developer while monitor effects are active")
            active_effect = (await db.execute(
                select(PRRepairWake.id)
                .where(
                    PRRepairWake.monitor_run_id == run.id,
                    PRRepairWake.status.in_(_STARTED_REPAIR_STATUSES),
                )
                .limit(1)
                .with_for_update()
            )).scalar_one_or_none()
            active_adjudication = (await db.execute(
                select(PRFindingRebuttal.id)
                .where(
                    PRFindingRebuttal.monitor_run_id == run.id,
                    PRFindingRebuttal.status.in_(_ACTIVE_ADJUDICATION_STATUSES),
                )
                .limit(1)
                .with_for_update()
            )).scalar_one_or_none()
            if active_effect is not None or active_adjudication is not None:
                raise HTTPException(409, "Cannot bind a Developer while monitor effects are active")
            if "pr-review" in (task.tags or []):
                raise HTTPException(400, "A Reviewer Task cannot be bound as the Developer")
            if repo.project_id is None or task.project_id != repo.project_id:
                raise HTTPException(400, "Developer Task must belong to the monitored Project")
            if not task.session_id or not task.last_cwd:
                raise HTTPException(409, "Developer Task has no resumable session/cwd yet")
            if not run.head_branch or task.result_branch != run.head_branch:
                raise HTTPException(409, "Developer Task branch does not match the PR head branch")
            if repo.auto_repair and (
                not run.head_repo_full_name
                or run.head_repo_full_name.lower() != repo.repo_full_name.lower()
            ):
                raise HTTPException(
                    409,
                    "Automatic Repair requires a proven same-repository PR head",
                )
            if (
                (
                    run.state_version,
                    run.status,
                    run.current_review_id,
                    run.current_base_sha,
                    run.current_head_sha,
                    run.developer_task_id,
                    run.completed_at,
                    run.terminal_intent_status,
                )
                != observed_run_generation
                or (
                    review.status,
                    review.monitor_run_id,
                    review.base_ref,
                    review.base_sha,
                    review.head_sha,
                    review.completed_at,
                )
                != observed_review_generation
                or (
                    task.status,
                    task.retry_count,
                    task.turn_generation,
                    task.session_id,
                    task.last_cwd,
                    task.project_id,
                    task.result_branch,
                    task.completed_at,
                )
                != observed_task_generation
            ):
                raise HTTPException(
                    409,
                    "PR Monitor or Developer generation changed while binding",
                )
            conflict = (await db.execute(
                select(PRMonitorRun.id)
                .where(
                    PRMonitorRun.developer_task_id == task.id,
                    PRMonitorRun.id != run.id,
                    PRMonitorRun.status.not_in(("merged", "closed")),
                )
                .limit(1)
                .with_for_update()
            )).scalar_one_or_none()
            if conflict is not None:
                raise HTTPException(409, "Developer Task is already bound to another active PR")
            run.developer_task_id = task.id
            run.binding_verified_at = datetime.utcnow()
            run.state_version += 1
            shadows = list((await db.execute(select(PRRepairWake).where(
                PRRepairWake.monitor_run_id == run.id,
                PRRepairWake.review_id == review.id,
                PRRepairWake.trigger_head_sha == run.current_head_sha,
                PRRepairWake.status == "shadow",
            ).with_for_update())).scalars())
            for wake in shadows:
                wake.developer_task_id = task.id
                if repo.auto_repair and run.repair_attempts < run.max_repair_attempts:
                    wake.status = "pending"
                    wake.last_error = None
            if repo.auto_repair and shadows and run.repair_attempts < run.max_repair_attempts:
                run.status = "repair_pending"
            await db.commit()
    return await get_monitor_run(run_id, request, db)


@router.post("/runs/{run_id}/pause", response_model=PRMonitorRunResponse)
async def pause_monitor_run(run_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    run = await db.get(PRMonitorRun, run_id)
    if run is None:
        raise HTTPException(404, "PR Monitor Run not found")
    repo = await db.get(MonitoredRepo, run.repo_id)
    if repo is None:
        raise HTTPException(404, "Repository not found")
    await _require_pr_monitor_access(request, db, repo)
    repo_id = repo.id
    await db.rollback()
    async with _pr_repo_write_lock(repo_id):
        try:
            locked_repo = await lock_pr_repo_action_boundary(db, repo_id)
        except FindingActionConflict:
            raise HTTPException(404, "Repository not found")
        await _reauthorize_pr_effect(request, db, locked_repo)
        await _quiesce_monitor_runs(
            db,
            repo_id=repo_id,
            run_id=run_id,
            reason="manual",
        )
        await db.commit()
    return await get_monitor_run(run_id, request, db)


@router.post("/runs/{run_id}/unbind-developer", response_model=PRMonitorRunResponse)
async def unbind_monitor_developer(run_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    from backend.services.worker_proxy import get_task_operation_lock

    run = await db.get(PRMonitorRun, run_id)
    if run is None:
        raise HTTPException(404, "PR Monitor Run not found")
    repo = await db.get(MonitoredRepo, run.repo_id)
    if repo is None:
        raise HTTPException(404, "Repository not found")
    await _require_pr_monitor_access(request, db, repo)
    if run.developer_task_id is None:
        raise HTTPException(409, "PR Monitor Run has no bound Developer")
    repo_id = repo.id
    task_id = run.developer_task_id
    await db.rollback()
    async with _pr_repo_write_lock(repo_id):
        async with get_task_operation_lock(task_id):
            try:
                repo = await lock_pr_repo_action_boundary(db, repo_id)
            except FindingActionConflict as exc:
                raise HTTPException(
                    404,
                    "Repository not found",
                ) from exc
            await _reauthorize_pr_effect(request, db, repo)
            run = (await db.execute(
                select(PRMonitorRun)
                .where(PRMonitorRun.id == run_id, PRMonitorRun.repo_id == repo_id)
                .with_for_update()
            )).scalar_one_or_none()
            if repo is None or run is None:
                raise HTTPException(404, "Repository or PR Monitor Run not found")
            if run.developer_task_id != task_id:
                raise HTTPException(409, "Developer binding changed concurrently")
            await _require_legacy_pr_effect_allowed(
                db,
                action="unbound from its Developer",
                monitor_run=run,
            )
            if run.status in {"merged", "closed"} or run.completed_at is not None:
                raise HTTPException(409, "Cannot unbind a terminal PR Monitor Run")
            active_repair = (await db.execute(select(PRRepairWake.id).where(
                PRRepairWake.monitor_run_id == run.id,
                PRRepairWake.status.in_(_STARTED_REPAIR_STATUSES),
            ).limit(1).with_for_update())).scalar_one_or_none()
            active_adjudication = (await db.execute(
                select(PRFindingRebuttal.id).where(
                    PRFindingRebuttal.monitor_run_id == run.id,
                    PRFindingRebuttal.status.in_(_ACTIVE_ADJUDICATION_STATUSES),
                ).limit(1).with_for_update()
            )).scalar_one_or_none()
            active_merge = (await db.execute(
                select(PRMergeQueueAction.id).where(
                    PRMergeQueueAction.monitor_run_id == run.id,
                    or_(
                        PRMergeQueueAction.status.in_(
                            _STARTED_MERGE_QUEUE_STATUSES
                        ),
                        pr_merge_queue_action_ambiguous_remote_effect_predicate(),
                    ),
                ).limit(1).with_for_update()
            )).scalar_one_or_none()
            active_publication = (await db.execute(
                select(PRReview.id).where(
                    PRReview.monitor_run_id == run.id,
                    PRReview.status.in_(("publishing", "superseding")),
                ).limit(1).with_for_update()
            )).scalar_one_or_none()
            if any((active_repair, active_adjudication, active_merge, active_publication)):
                raise HTTPException(409, "Cannot unbind while monitor effects are active")
            wakes = list((await db.execute(select(PRRepairWake).where(
                PRRepairWake.monitor_run_id == run.id,
                PRRepairWake.status.in_(("pending", "shadow")),
            ).with_for_update())).scalars())
            for wake in wakes:
                wake.developer_task_id = None
                wake.status = "shadow"
                if wake.last_error is None:
                    wake.last_error = "developer_unbound"
            run.developer_task_id = None
            run.binding_verified_at = None
            if run.status == "repair_pending":
                run.status = "waiting_for_fix"
            run.state_version += 1
            await db.commit()
    return await get_monitor_run(run_id, request, db)


@router.post("/runs/{run_id}/resume", response_model=PRMonitorRunResponse)
async def resume_monitor_run(run_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    from backend.models.task import Task
    from backend.services.pr_merge_queue import (
        _read_merge_group_ref,
        _read_queue_entry,
    )
    from backend.services.pr_review_service import _gh_pr_view, _validated_pr_snapshot

    run = await db.get(PRMonitorRun, run_id)
    if run is None:
        raise HTTPException(404, "PR Monitor Run not found")
    repo = await db.get(MonitoredRepo, run.repo_id)
    if repo is None:
        raise HTTPException(404, "Repository not found")
    await _require_pr_monitor_access(request, db, repo)
    await _require_legacy_pr_effect_allowed(
        db,
        action="resumed",
        monitor_run=run,
    )
    if not repo.enabled:
        raise HTTPException(409, "Enable the PR monitor before resuming a Run")
    if run.status != "paused":
        raise HTTPException(409, "Only a paused PR Monitor Run can be resumed")
    if run.pause_reason in {
        "direct_merge_base_update_required",
        "direct_merge_base_update_requested",
    }:
        raise HTTPException(
            409,
            "Update the PR branch and wait for CCM to review the new head",
        )
    review = (
        await db.get(PRReview, run.current_review_id)
        if run.current_review_id is not None
        else None
    )
    if (
        review is None
        or review.repo_id != repo.id
        or review.monitor_run_id != run.id
        or review.base_sha != run.current_base_sha
        or review.head_sha != run.current_head_sha
    ):
        raise HTTPException(409, "PR Monitor Gate subject is incomplete")

    current_action = (await db.execute(select(PRMergeQueueAction).where(
        PRMergeQueueAction.monitor_run_id == run.id,
        PRMergeQueueAction.review_id == review.id,
        PRMergeQueueAction.trigger_base_sha == run.current_base_sha,
        PRMergeQueueAction.trigger_head_sha == run.current_head_sha,
        PRMergeQueueAction.status == "paused",
    ).order_by(desc(PRMergeQueueAction.id)))).scalars().first()
    current_wake = (await db.execute(select(PRRepairWake).where(
        PRRepairWake.monitor_run_id == run.id,
        PRRepairWake.review_id == review.id,
        PRRepairWake.trigger_base_sha == run.current_base_sha,
        PRRepairWake.trigger_head_sha == run.current_head_sha,
        PRRepairWake.status.in_(("shadow", "failed")),
    ).order_by(desc(PRRepairWake.id)))).scalars().first()
    task = None
    if (
        current_action is None
        and current_wake is not None
        and repo.auto_repair
        and run.developer_task_id is not None
    ):
        task = await db.get(Task, run.developer_task_id)
        if task is None or not task.session_id or not task.last_cwd:
            raise HTTPException(409, "Bound Developer Task is not resumable")
        if run.repair_attempts >= run.max_repair_attempts:
            raise HTTPException(409, "Automatic repair budget is exhausted")

    repo_id = repo.id
    repo_full_name = repo.repo_full_name
    pr_number = run.pr_number
    remote_base_ref = review.base_ref
    remote_trigger_base_sha = (
        current_action.trigger_base_sha
        if current_action is not None
        else (
            current_wake.trigger_base_sha
            if current_wake is not None
            else run.current_base_sha
        )
    )
    remote_trigger_head_sha = (
        current_action.trigger_head_sha
        if current_action is not None
        else (
            current_wake.trigger_head_sha
            if current_wake is not None
            else run.current_head_sha
        )
    )
    observed_run_generation = (
        run.state_version,
        run.status,
        run.current_review_id,
        run.current_base_sha,
        run.current_head_sha,
        run.developer_task_id,
        run.pause_reason,
        run.completed_at,
        run.terminal_intent_status,
    )
    observed_review_generation = (
        review.status,
        review.monitor_run_id,
        review.base_ref,
        review.base_sha,
        review.head_sha,
        review.completed_at,
    )
    observed_action_generation = (
        (
            current_action.id,
            current_action.status,
            current_action.review_id,
            current_action.trigger_base_sha,
            current_action.trigger_head_sha,
            current_action.action_nonce,
            current_action.github_queue_entry_id,
            current_action.merge_group_sha,
            current_action.merge_group_ref,
            current_action.ci_status,
            current_action.last_error,
        )
        if current_action is not None
        else None
    )
    observed_wake_generation = (
        (
            current_wake.id,
            current_wake.status,
            current_wake.review_id,
            current_wake.trigger_base_sha,
            current_wake.trigger_head_sha,
            current_wake.developer_task_id,
            current_wake.delivery_token,
            current_wake.last_error,
        )
        if current_wake is not None
        else None
    )
    observed_task_generation = (
        (
            task.id,
            task.status,
            task.retry_count,
            task.turn_generation,
            task.session_id,
            task.last_cwd,
            task.completed_at,
        )
        if task is not None
        else None
    )
    await db.rollback()

    remote_entry = None
    remote_merge_group = None
    if observed_action_generation is not None:
        try:
            snapshot = _validated_pr_snapshot(
                await _gh_pr_view(pr_number, repo_full_name)
            )
        except Exception as exc:
            raise HTTPException(
                409,
                "GitHub PR state could not be confirmed while resuming Merge Queue",
            ) from exc
        if (
            snapshot.get("state") != "OPEN"
            or snapshot.get("merged_at") is not None
            or snapshot.get("is_draft") is not False
            or snapshot.get("base_ref") != remote_base_ref
            or snapshot.get("base_sha") != remote_trigger_base_sha
            or snapshot.get("head_sha") != remote_trigger_head_sha
        ):
            raise HTTPException(409, "GitHub PR subject changed while resuming Merge Queue")
        try:
            remote_entry = await _read_queue_entry(repo_full_name, pr_number)
        except Exception as exc:
            raise HTTPException(409, "Remote Merge Queue state could not be confirmed") from exc
        if remote_entry is not None and (
            remote_entry.base_ref != remote_base_ref
            or remote_entry.base_sha != remote_trigger_base_sha
            or remote_entry.head_sha != remote_trigger_head_sha
        ):
            raise HTTPException(409, "Remote Merge Queue entry is for another subject")
        if remote_entry is not None and remote_entry.state in {"UNMERGEABLE", "LOCKED"}:
            raise HTTPException(409, f"Remote Merge Queue entry is {remote_entry.state.lower()}")
        if remote_entry is not None:
            try:
                remote_merge_group = await _read_merge_group_ref(
                    repo_full_name,
                    default_branch=remote_base_ref,
                    pr_number=pr_number,
                )
            except Exception as exc:
                raise HTTPException(
                    409,
                    "Remote Merge Queue merge-group state could not be confirmed",
                ) from exc
    elif observed_task_generation is not None:
        snapshot = _validated_pr_snapshot(
            await _gh_pr_view(pr_number, repo_full_name)
        )
        if (
            snapshot.get("state") != "OPEN"
            or snapshot.get("merged_at") is not None
            or snapshot.get("is_draft") is not False
            or snapshot.get("base_ref") != remote_base_ref
            or snapshot.get("base_sha") != remote_trigger_base_sha
            or snapshot.get("head_sha") != remote_trigger_head_sha
        ):
            raise HTTPException(409, "GitHub PR subject changed while resuming Repair")

    async with _pr_repo_write_lock(repo_id):
        try:
            repo = await lock_pr_repo_action_boundary(db, repo_id)
        except FindingActionConflict as exc:
            raise HTTPException(404, "Repository not found") from exc
        await _reauthorize_pr_effect(request, db, repo)
        run = (await db.execute(
            select(PRMonitorRun)
            .where(PRMonitorRun.id == run_id, PRMonitorRun.repo_id == repo_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )).scalar_one_or_none()
        review = (
            (await db.execute(
                select(PRReview)
                .where(
                    PRReview.id == run.current_review_id,
                    PRReview.repo_id == repo_id,
                    PRReview.monitor_run_id == run.id,
                )
                .with_for_update()
                .execution_options(populate_existing=True)
            )).scalar_one_or_none()
            if run is not None and run.current_review_id is not None
            else None
        )
        if repo is None or run is None or review is None:
            raise HTTPException(409, "PR Monitor Gate subject changed while resuming")
        await _require_legacy_pr_effect_allowed(
            db,
            action="resumed",
            monitor_run=run,
        )
        if not repo.enabled or repo.repo_full_name != repo_full_name:
            raise HTTPException(409, "PR monitor policy changed while resuming")
        if (
            (
                run.state_version,
                run.status,
                run.current_review_id,
                run.current_base_sha,
                run.current_head_sha,
                run.developer_task_id,
                run.pause_reason,
                run.completed_at,
                run.terminal_intent_status,
            )
            != observed_run_generation
            or (
                review.status,
                review.monitor_run_id,
                review.base_ref,
                review.base_sha,
                review.head_sha,
                review.completed_at,
            )
            != observed_review_generation
        ):
            raise HTTPException(409, "PR Monitor generation changed while resuming")

        current_action = (await db.execute(select(PRMergeQueueAction).where(
            PRMergeQueueAction.monitor_run_id == run.id,
            PRMergeQueueAction.review_id == review.id,
            PRMergeQueueAction.trigger_base_sha == run.current_base_sha,
            PRMergeQueueAction.trigger_head_sha == run.current_head_sha,
            PRMergeQueueAction.status == "paused",
        ).order_by(desc(PRMergeQueueAction.id)).with_for_update())).scalars().first()
        current_wake = (await db.execute(select(PRRepairWake).where(
            PRRepairWake.monitor_run_id == run.id,
            PRRepairWake.review_id == review.id,
            PRRepairWake.trigger_base_sha == run.current_base_sha,
            PRRepairWake.trigger_head_sha == run.current_head_sha,
            PRRepairWake.status.in_(("shadow", "failed")),
        ).order_by(desc(PRRepairWake.id)).with_for_update())).scalars().first()
        current_action_generation = (
            (
                current_action.id,
                current_action.status,
                current_action.review_id,
                current_action.trigger_base_sha,
                current_action.trigger_head_sha,
                current_action.action_nonce,
                current_action.github_queue_entry_id,
                current_action.merge_group_sha,
                current_action.merge_group_ref,
                current_action.ci_status,
                current_action.last_error,
            )
            if current_action is not None
            else None
        )
        current_wake_generation = (
            (
                current_wake.id,
                current_wake.status,
                current_wake.review_id,
                current_wake.trigger_base_sha,
                current_wake.trigger_head_sha,
                current_wake.developer_task_id,
                current_wake.delivery_token,
                current_wake.last_error,
            )
            if current_wake is not None
            else None
        )
        if (
            current_action_generation != observed_action_generation
            or current_wake_generation != observed_wake_generation
        ):
            raise HTTPException(409, "PR Monitor effect changed while resuming")

        if current_action is not None:
            if remote_entry is None:
                current_action.status = "pending"
                current_action.github_queue_entry_id = None
                current_action.merge_group_sha = None
                current_action.merge_group_ref = None
                current_action.ci_status = None
                current_action.ci_details = None
                run.status = "merge_queue_pending"
            elif remote_merge_group is None:
                current_action.status = "queued"
                current_action.github_queue_entry_id = remote_entry.id
                current_action.merge_group_sha = None
                current_action.merge_group_ref = None
                current_action.ci_status = None
                current_action.ci_details = None
                run.status = "merge_queued"
            else:
                current_action.status = "checking"
                current_action.github_queue_entry_id = remote_entry.id
                current_action.merge_group_sha = remote_merge_group[0]
                current_action.merge_group_ref = remote_merge_group[1]
                current_action.ci_status = "pending"
                current_action.ci_details = None
                run.status = "merge_group_checking"
            current_action.last_error = None
        elif (
            current_wake is not None
            and repo.auto_repair
            and run.developer_task_id is not None
            and observed_task_generation is not None
        ):
            task = (await db.execute(
                select(Task)
                .where(Task.id == run.developer_task_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )).scalar_one_or_none()
            if task is None or (
                task.id,
                task.status,
                task.retry_count,
                task.turn_generation,
                task.session_id,
                task.last_cwd,
                task.completed_at,
            ) != observed_task_generation:
                raise HTTPException(409, "Developer Task changed while resuming Repair")
            if not task.session_id or not task.last_cwd:
                raise HTTPException(409, "Bound Developer Task is not resumable")
            if run.repair_attempts >= run.max_repair_attempts:
                raise HTTPException(409, "Automatic repair budget is exhausted")
            current_wake.status = "pending"
            current_wake.delivery_token = secrets.token_hex(24)
            current_wake.last_error = None
            current_wake.developer_task_id = task.id
            run.status = "repair_pending"
        else:
            if current_wake is not None:
                current_wake.status = "shadow"
            blockers = list((await db.execute(select(PRFinding.id).where(
                PRFinding.pr_review_id == review.id,
                PRFinding.severity.in_(("critical", "high", "medium")),
                or_(
                    PRFinding.status == "open",
                    PRFinding.thread_status != "resolved",
                ),
            ))).scalars())
            if review.status == "waiting_ci":
                run.status = "waiting_ci"
            elif review.status in {"pending", "reviewing"}:
                run.status = "reviewing"
            elif review.status in {"approved", "commented"} and not blockers:
                run.status = "ready_to_merge"
            else:
                run.status = "waiting_for_fix"
        run.pause_reason = None
        run.state_version += 1
        await db.commit()
    return await get_monitor_run(run_id, request, db)


@router.post("/runs/{run_id}/merge", response_model=PRMonitorRunResponse)
@router.post("/runs/{run_id}/enqueue-merge", response_model=PRMonitorRunResponse)
async def merge_monitor_run(
    run_id: int, request: Request, db: AsyncSession = Depends(get_db)
):
    run = await db.get(PRMonitorRun, run_id)
    if run is None:
        raise HTTPException(404, "PR Monitor Run not found")
    repo = await db.get(MonitoredRepo, run.repo_id)
    if repo is None:
        raise HTTPException(404, "Repository not found")
    await _require_pr_monitor_access(request, db, repo)
    # Delivery-owned and terminalizing subjects must be rejected before any
    # GitHub capability probe. Besides avoiding unnecessary remote I/O, this
    # keeps the legacy PR Monitor controls from leaking a transport error for
    # a lifecycle that is owned by Delivery.
    await _require_legacy_pr_effect_allowed(
        db,
        action="merged",
        monitor_run=run,
    )
    if not repo.enabled:
        raise HTTPException(409, "Enable the PR monitor before merging")
    preflight_review = (
        await db.get(PRReview, run.current_review_id)
        if run.current_review_id is not None
        else None
    )
    if (
        preflight_review is None
        or preflight_review.repo_id != repo.id
        or preflight_review.monitor_run_id != run.id
        or run.status != "ready_to_merge"
        or run.completed_at is not None
        or run.current_base_sha != preflight_review.base_sha
        or run.current_head_sha != preflight_review.head_sha
        or preflight_review.status not in {"approved", "commented"}
        or preflight_review.action_taken != "lgtm_comment"
        or preflight_review.publication_state != "published"
    ):
        raise HTTPException(409, "The exact reviewed PR head is not ready to merge")
    preflight_blocker = await db.scalar(
        select(PRFinding.id)
        .join(PRReview, PRReview.id == PRFinding.pr_review_id)
        .where(
            PRReview.monitor_run_id == run.id,
            PRFinding.severity.in_(("critical", "high", "medium")),
            or_(
                PRFinding.status == "open",
                PRFinding.thread_status != "resolved",
            ),
        )
        .limit(1)
    )
    if preflight_blocker is not None:
        raise HTTPException(409, "The exact reviewed PR head no longer passes Gate")
    repo_id = repo.id
    repo_name = repo.repo_full_name
    frozen_wait_for_ci = bool(repo.wait_for_ci)
    frozen_required_checks = json.loads(json.dumps(repo.required_checks or []))
    db_factory = async_sessionmaker(
        bind=db.bind,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    await db.rollback()

    # Resolve network-backed capability and identity before taking any database
    # writer boundary. The locked section below revalidates every frozen value
    # before arming the durable exact-head effect.
    from backend.services.pr_review_service import (
        GhError,
        GhRepositoryCapabilityError,
        _freeze_safe_merge_method,
        _gh_authenticated_login,
    )

    try:
        publishing_actor = await _gh_authenticated_login()
        merge_method = await _freeze_safe_merge_method(repo_name)
    except GhRepositoryCapabilityError as exc:
        raise HTTPException(
            409,
            f"GitHub repository cannot be merged directly: {exc}",
        ) from exc
    except GhError as exc:
        logger.warning("Unable to freeze direct merge capability: %s", exc)
        raise HTTPException(
            503,
            "GitHub merge capability could not be verified; retry shortly",
        ) from exc

    action_id: int
    async with _pr_repo_write_lock(repo_id):
        try:
            repo = await lock_pr_repo_action_boundary(db, repo_id)
        except FindingActionConflict as exc:
            raise HTTPException(404, "Repository not found") from exc
        await _reauthorize_pr_effect(request, db, repo)
        if not repo.enabled:
            raise HTTPException(409, "Enable the PR monitor before merging")
        if (
            repo.repo_full_name != repo_name
            or bool(repo.wait_for_ci) != frozen_wait_for_ci
            or (repo.required_checks or []) != frozen_required_checks
        ):
            raise HTTPException(409, "PR Monitor merge policy changed; retry")
        run = (
            await db.execute(
                select(PRMonitorRun)
                .where(
                    PRMonitorRun.id == run_id,
                    PRMonitorRun.repo_id == repo_id,
                )
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
        review = None
        if run is not None and run.current_review_id is not None:
            review = (
                await db.execute(
                    select(PRReview)
                    .where(
                        PRReview.id == run.current_review_id,
                        PRReview.repo_id == repo_id,
                    )
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
        if run is None or review is None:
            raise HTTPException(409, "PR Monitor Gate subject is incomplete")
        await _require_legacy_pr_effect_allowed(
            db,
            action="merged",
            review=review,
            monitor_run=run,
        )
        if run.status != "ready_to_merge":
            raise HTTPException(
                409,
                "The exact reviewed PR head is not ready to merge",
            )
        blockers = await db.scalar(
            select(PRFinding.id)
            .join(PRReview, PRReview.id == PRFinding.pr_review_id)
            .where(
                PRReview.monitor_run_id == run.id,
                PRFinding.severity.in_(("critical", "high", "medium")),
                or_(
                    PRFinding.status == "open",
                    PRFinding.thread_status != "resolved",
                ),
            )
            .limit(1)
            .with_for_update()
        )
        if (
            run.completed_at is not None
            or pr_monitor_run_has_terminal_intent(run)
            or run.current_base_sha != review.base_sha
            or run.current_head_sha != review.head_sha
            or review.monitor_run_id != run.id
            or review.status not in {"approved", "commented"}
            or review.action_taken != "lgtm_comment"
            or review.publication_state != "published"
            or blockers is not None
        ):
            raise HTTPException(409, "The exact reviewed PR head no longer passes Gate")
        action = (
            await db.execute(
                select(PRMergeQueueAction)
                .where(
                    PRMergeQueueAction.monitor_run_id == run.id,
                    PRMergeQueueAction.review_id == review.id,
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        if action is None:
            action = PRMergeQueueAction(
                monitor_run_id=run.id,
                review_id=review.id,
                trigger_base_sha=run.current_base_sha,
                trigger_head_sha=run.current_head_sha,
                status="pending",
                effect_kind="direct",
                trigger_kind="manual",
                action_nonce=secrets.token_hex(24),
                publishing_actor=publishing_actor,
                publishing_started_at=datetime.utcnow(),
                merge_method=merge_method,
                wait_for_ci=frozen_wait_for_ci,
                required_checks=frozen_required_checks,
            )
            db.add(action)
            await db.flush()
        elif action.effect_kind == "queue":
            if (
                action.status not in {"shadow", "failed", "paused", "superseded"}
                or pr_merge_queue_action_has_ambiguous_remote_effect(action)
            ):
                raise HTTPException(
                    409,
                    "A legacy Merge Queue action is still being reconciled; "
                    "retry Merge PR after it returns to ready",
                )
            # Reuse the unique outbox row only after legacy reconciliation has
            # proved it owns no possible remote Queue effect. This conversion is
            # always caused by a fresh human click, never by recovery.
            action.status = "pending"
            action.effect_kind = "direct"
            action.trigger_kind = "manual"
            action.trigger_base_sha = run.current_base_sha
            action.trigger_head_sha = run.current_head_sha
            action.action_nonce = secrets.token_hex(24)
            action.publishing_actor = publishing_actor
            action.publishing_started_at = datetime.utcnow()
            action.merge_method = merge_method
            action.wait_for_ci = frozen_wait_for_ci
            action.required_checks = frozen_required_checks
            action.github_pr_node_id = None
            action.github_queue_entry_id = None
            action.merge_group_sha = None
            action.merge_group_ref = None
            action.ci_status = None
            action.ci_details = None
            action.attempt_count = 0
            action.lease_token = None
            action.lease_expires_at = None
            action.last_error = None
            action.completed_at = None
        elif (
            action.effect_kind == "direct"
            and action.status in {"failed", "paused"}
            and not pr_merge_queue_action_has_ambiguous_remote_effect(action)
        ):
            action.status = "pending"
            action.trigger_kind = "manual"
            action.action_nonce = secrets.token_hex(24)
            action.publishing_actor = publishing_actor
            action.publishing_started_at = datetime.utcnow()
            action.merge_method = merge_method
            action.wait_for_ci = frozen_wait_for_ci
            action.required_checks = frozen_required_checks
            action.attempt_count = 0
            action.lease_token = None
            action.lease_expires_at = None
            action.last_error = None
            action.completed_at = None
        else:
            raise HTTPException(
                409,
                f"Merge action is already {action.status}",
            )
        action_id = action.id
        run.status = "merge_pending"
        run.pause_reason = None
        run.state_version += 1
        await db.commit()

    from backend.services.pr_direct_merge import reconcile_direct_merge_action

    try:
        await reconcile_direct_merge_action(db_factory, action_id)
    except Exception:
        # The durable outbox remains recoverable by the periodic reconciler. A
        # response still exposes its persisted state instead of making a retry
        # create an untracked second GitHub effect.
        logger.exception("Immediate direct PR merge reconciliation failed")
    return await get_monitor_run(run_id, request, db)


@router.post(
    "/runs/{run_id}/update-branch",
    response_model=PRMonitorBranchUpdateResponse,
)
async def update_monitor_pr_branch(
    run_id: int,
    body: PRMonitorBranchUpdateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Merge the current base branch into a paused PR through GitHub's API."""

    run = await db.get(PRMonitorRun, run_id)
    if run is None:
        raise HTTPException(404, "PR Monitor Run not found")
    repo = await db.get(MonitoredRepo, run.repo_id)
    if repo is None:
        raise HTTPException(404, "Repository not found")
    await _require_pr_monitor_access(request, db, repo)
    await _require_legacy_pr_effect_allowed(
        db,
        action="update the PR branch",
        monitor_run=run,
    )
    if not repo.enabled:
        raise HTTPException(409, "Enable the PR monitor before updating the PR branch")

    expected_head_sha = body.expected_head_sha.lower()
    if (
        run.status != "paused"
        or run.pause_reason != "direct_merge_base_update_required"
        or run.current_head_sha != expected_head_sha
        or run.current_review_id is None
    ):
        raise HTTPException(
            409,
            "The PR branch update is no longer valid for this exact reviewed head",
        )

    review = await db.get(PRReview, run.current_review_id)
    if (
        review is None
        or review.monitor_run_id != run.id
        or review.repo_id != repo.id
        or review.pr_number != run.pr_number
        or review.head_sha != expected_head_sha
        or review.status not in {"approved", "commented"}
        or review.code_verdict != "pass"
        or review.action_taken != "lgtm_comment"
        or review.publication_state != "published"
    ):
        raise HTTPException(
            409,
            "The PR branch update requires a published passing review",
        )

    repo_id = repo.id
    repo_name = repo.repo_full_name
    pr_number = run.pr_number
    base_ref = review.base_ref
    expected_base_sha = run.current_base_sha
    review_id = review.id

    # Mark the operation before remote I/O so a crash after GitHub accepts the
    # request remains visible and cannot be mistaken for an untouched pause.
    # Do not re-admit a requested marker: an unknown outcome must wait for the
    # synchronize webhook or explicit reconciliation.
    await db.rollback()
    async with _pr_repo_write_lock(repo_id):
        locked_repo = await lock_pr_repo_action_boundary(db, repo_id)
        await _reauthorize_pr_effect(request, db, locked_repo)
        if not locked_repo.enabled or locked_repo.repo_full_name != repo_name:
            raise HTTPException(409, "PR Monitor repository policy changed; retry")
        locked_run = (
            await db.execute(
                select(PRMonitorRun)
                .where(
                    PRMonitorRun.id == run_id,
                    PRMonitorRun.repo_id == repo_id,
                )
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
        locked_review = (
            await db.get(PRReview, locked_run.current_review_id)
            if locked_run is not None and locked_run.current_review_id is not None
            else None
        )
        if (
            locked_run is None
            or locked_review is None
            or locked_run.status != "paused"
            or locked_run.pause_reason != "direct_merge_base_update_required"
            or locked_run.current_head_sha != expected_head_sha
            or locked_review.id != review_id
            or locked_review.head_sha != expected_head_sha
            or locked_review.base_ref != base_ref
            or locked_run.current_base_sha != expected_base_sha
        ):
            await db.rollback()
            raise HTTPException(
                409,
                "The PR branch update is no longer valid for this exact reviewed head",
            )
        await _require_legacy_pr_effect_allowed(
            db,
            action="update the PR branch",
            monitor_run=locked_run,
            review=locked_review,
        )
        locked_run.pause_reason = "direct_merge_base_update_requested"
        locked_run.state_version += 1
        await db.commit()

    from backend.services.pr_review_service import (
        GhError,
        GhRepositoryCapabilityError,
        PRBranchUpdateConflict,
        _freeze_safe_merge_method,
        update_pr_branch,
    )

    try:
        await _freeze_safe_merge_method(repo_name)
    except GhRepositoryCapabilityError as exc:
        async with _pr_repo_write_lock(repo_id):
            locked_repo = await lock_pr_repo_action_boundary(db, repo_id)
            await _reauthorize_pr_effect(request, db, locked_repo)
            locked_run = await db.get(PRMonitorRun, run_id, populate_existing=True)
            if (
                locked_run is not None
                and locked_run.status == "paused"
                and locked_run.current_head_sha == expected_head_sha
                and locked_run.pause_reason == "direct_merge_base_update_requested"
            ):
                locked_run.pause_reason = "direct_merge_base_update_required"
                locked_run.state_version += 1
                await db.commit()
            else:
                await db.rollback()
        raise HTTPException(409, f"GitHub cannot update this PR branch: {exc}") from exc
    except GhError as exc:
        raise HTTPException(503, "GitHub branch update capability could not be verified; retry shortly") from exc

    try:
        await update_pr_branch(
            repo_name=repo_name,
            pr_number=pr_number,
            base_ref=base_ref,
            expected_base_sha=expected_base_sha,
            expected_head_sha=expected_head_sha,
        )
    except PRBranchUpdateConflict as exc:
        logger.info("PR branch update became stale for run %s: %s", run_id, exc)
        async with _pr_repo_write_lock(repo_id):
            locked_repo = await lock_pr_repo_action_boundary(db, repo_id)
            await _reauthorize_pr_effect(request, db, locked_repo)
            locked_run = await db.get(PRMonitorRun, run_id, populate_existing=True)
            if (
                locked_run is not None
                and locked_run.status == "paused"
                and locked_run.current_head_sha == expected_head_sha
                and locked_run.pause_reason == "direct_merge_base_update_requested"
            ):
                locked_run.pause_reason = "direct_merge_base_update_required"
                locked_run.state_version += 1
                await db.commit()
            else:
                await db.rollback()
        raise HTTPException(409, str(exc)) from exc
    except GhError as exc:
        logger.warning("GitHub PR branch update failed for run %s: %s", run_id, exc)
        raise HTTPException(
            503,
            "GitHub branch update outcome is uncertain; wait for the synchronize webhook before retrying",
        ) from exc

    return PRMonitorBranchUpdateResponse(
        status="accepted",
        expected_head_sha=expected_head_sha,
        message="GitHub accepted the branch update; CCM will review the new head after synchronize",
    )


# --- Webhook endpoint ---


async def _read_github_webhook_body(request: Request) -> bytearray:
    """Read one exact, bounded webhook body with strict framing checks.

    ASGI exposes an already de-chunked byte stream.  ``Content-Length`` is an
    early rejection hint only; the accumulated stream length is the security
    boundary and is compared with the declaration after EOF.  Returning the
    original byte sequence in one bytearray lets JSON and HMAC consume the
    same data without a second request read or an extra 25 MB copy.
    """

    raw_headers = request.scope.get("headers") or ()
    content_lengths = [
        value.strip()
        for name, value in raw_headers
        if bytes(name).lower() == b"content-length"
    ]
    transfer_encodings = [
        value.strip()
        for name, value in raw_headers
        if bytes(name).lower() == b"transfer-encoding" and value.strip()
    ]
    if len(content_lengths) > 1:
        raise HTTPException(400, "Multiple Content-Length headers are invalid")
    if content_lengths and transfer_encodings:
        raise HTTPException(
            400,
            "Content-Length and Transfer-Encoding cannot both be supplied",
        )

    declared_length: int | None = None
    if content_lengths:
        raw_length = content_lengths[0]
        if not raw_length or re.fullmatch(rb"[0-9]+", raw_length) is None:
            raise HTTPException(400, "Invalid Content-Length header")
        # Avoid feeding an attacker-controlled, arbitrarily long decimal into
        # ``int``.  Any 20-digit positive decimal is already far beyond 25 MB.
        if len(raw_length) > 19:
            raise HTTPException(413, "GitHub webhook payload is too large")
        declared_length = int(raw_length)
        if declared_length > _MAX_GITHUB_WEBHOOK_BODY_BYTES:
            raise HTTPException(413, "GitHub webhook payload is too large")

    body = bytearray()
    try:
        async for chunk in request.stream():
            if len(chunk) > _MAX_GITHUB_WEBHOOK_BODY_BYTES - len(body):
                raise HTTPException(
                    413,
                    "GitHub webhook payload is too large",
                )
            body.extend(chunk)
    except ClientDisconnect as exc:
        raise HTTPException(
            400,
            "GitHub webhook request body was interrupted",
        ) from exc

    if declared_length is not None and len(body) != declared_length:
        raise HTTPException(
            400,
            "GitHub webhook body length does not match Content-Length",
        )
    return body


async def _terminalize_pull_request_run(
    db: AsyncSession,
    *,
    repo_id: int,
    pr_number: int,
    base_ref: str,
    base_sha: str,
    head_sha: str,
    merged: bool | None,
    body: bytes | bytearray | None = None,
    signature_header: str | None = None,
    trusted_recovery: bool = False,
    legacy_recovery: bool = False,
    delivery_id: str | None = None,
) -> dict:
    """Apply a signed closed event only after a fresh GitHub state fence."""

    from backend.services.pr_review_service import (
        _gh_pr_view,
        _validated_pr_snapshot,
    )

    if legacy_recovery and not trusted_recovery:
        raise RuntimeError("legacy lifecycle recovery must be trusted")

    def repo_generation(repo: MonitoredRepo) -> tuple:
        return (
            repo.id,
            repo.repo_full_name,
            bool(repo.enabled),
            repo.webhook_secret,
        )

    def run_generation(run: PRMonitorRun) -> tuple:
        # ``state_version`` is the normal lifecycle CAS.  Include the complete
        # terminal-intent subject as well because recovery updates its harmless
        # checked-at throttle without incrementing that version, and a signed
        # reopen clears the intent as one atomic subject replacement.
        return (
            run.id,
            run.state_version,
            run.status,
            run.current_review_id,
            run.current_base_sha,
            run.current_head_sha,
            run.completed_at,
            run.terminal_intent_status,
            run.terminal_intent_base_ref,
            run.terminal_intent_head_sha,
            run.terminal_intent_delivery_id,
            run.terminal_intent_observed_at,
            run.terminal_intent_checked_at,
            run.legacy_terminal_recovery_pending,
        )

    def legacy_review_generation(review: PRReview) -> tuple:
        return (
            review.id,
            review.monitor_run_id,
            review.repo_id,
            review.pr_number,
            review.status,
            review.action_taken,
            review.pending_action,
            review.publication_state,
            review.failure_stage,
            review.review_summary,
            review.completed_at,
        )

    def terminal_snapshot_state(snapshot: dict) -> tuple[bool, bool]:
        return (
            bool(
                snapshot.get("state") == "MERGED"
                and snapshot.get("merged_at") is not None
            ),
            bool(
                snapshot.get("state") == "CLOSED"
                and snapshot.get("merged_at") is None
            ),
        )

    async def changed_during_remote_verification(reason: str) -> dict:
        await db.rollback()
        if trusted_recovery:
            return {"status": "ignored", "reason": reason}
        # A signed lifecycle delivery must not be acknowledged when another
        # Manager changed its exact subject while GitHub was being read.  A
        # retry will either persist the current intent or prove it stale.
        raise HTTPException(
            503,
            "PR terminal lifecycle changed during remote verification; retry "
            "the signed delivery",
        )

    await db.rollback()
    async with _pr_repo_write_lock(repo_id):
        # Phase 1: authorize and freeze the exact local generation in a short
        # transaction.  GitHub must never be awaited while this Repo -> Run ->
        # Review lock chain (or even a pooled DB connection) remains held.
        repo = (await db.execute(
            select(MonitoredRepo)
            .where(MonitoredRepo.id == repo_id)
            .with_for_update()
        )).scalar_one_or_none()
        if repo is None or not repo.enabled:
            await db.rollback()
            return {"status": "ignored", "reason": "repository not monitored or disabled"}
        if not trusted_recovery:
            if body is None or signature_header is None:
                await db.rollback()
                raise RuntimeError("signed terminal webhook evidence is missing")
            try:
                _require_current_webhook_signature(
                    repo,
                    body=body,
                    signature_header=signature_header,
                )
            except Exception:
                await db.rollback()
                raise
        observed_repo_generation = repo_generation(repo)
        repo_name = repo.repo_full_name
        run = (await db.execute(
            select(PRMonitorRun)
            .where(
                PRMonitorRun.repo_id == repo_id,
                PRMonitorRun.pr_number == pr_number,
            )
            .with_for_update()
        )).scalar_one_or_none()
        if run is None:
            await db.rollback()
            return {"status": "ignored", "reason": "PR lifecycle not found"}
        observed_run_generation = run_generation(run)
        observed_legacy_review_generation = None
        if trusted_recovery and legacy_recovery:
            review = (
                (await db.execute(
                    select(PRReview)
                    .where(PRReview.id == run.current_review_id)
                    .with_for_update()
                )).scalar_one_or_none()
                if run.current_review_id is not None
                else None
            )
            if (
                review is None
                or not _is_strict_legacy_lifecycle_recovery_candidate(
                    review,
                    run,
                )
            ):
                await db.rollback()
                return {
                    "status": "ignored",
                    "reason": "legacy lifecycle candidate changed",
                }
            observed_legacy_review_generation = legacy_review_generation(review)
        elif trusted_recovery and (
            run.terminal_intent_status not in {"merged", "closed"}
            or run.terminal_intent_base_ref != base_ref
            or run.terminal_intent_head_sha != head_sha
        ):
            await db.rollback()
            return {"status": "ignored", "reason": "no matching terminal intent"}
        await db.rollback()

        # Phase 2: authoritative remote fence with no SQLAlchemy transaction,
        # connection checkout, or row lock retained from the authorization
        # snapshot above.
        snapshot = _validated_pr_snapshot(
            await _gh_pr_view(pr_number, repo_name)
        )
        remote_is_merged, remote_is_closed = terminal_snapshot_state(snapshot)
        remote_terminal_matches = bool(
            snapshot.get("base_ref") == base_ref
            and snapshot.get("head_sha") == head_sha
            and (remote_is_merged or remote_is_closed)
            and (merged is None or merged is remote_is_merged)
        )

        # Phase 3: re-lock Repo -> Run -> Review and require the exact frozen
        # generation before consuming the remote result.  This is the
        # cross-process counterpart to the process-local repository lock.
        db.expire_all()
        repo = (await db.execute(
            select(MonitoredRepo)
            .where(MonitoredRepo.id == repo_id)
            .with_for_update()
        )).scalar_one_or_none()
        if repo is None or not repo.enabled:
            await db.rollback()
            return {"status": "ignored", "reason": "repository not monitored or disabled"}
        if repo_generation(repo) != observed_repo_generation:
            return await changed_during_remote_verification(
                "repository changed during terminal verification"
            )
        if not trusted_recovery:
            assert body is not None and signature_header is not None
            try:
                _require_current_webhook_signature(
                    repo,
                    body=body,
                    signature_header=signature_header,
                )
            except Exception:
                await db.rollback()
                raise
        run = (await db.execute(
            select(PRMonitorRun)
            .where(
                PRMonitorRun.repo_id == repo_id,
                PRMonitorRun.pr_number == pr_number,
            )
            .with_for_update()
        )).scalar_one_or_none()
        if run is None or run_generation(run) != observed_run_generation:
            return await changed_during_remote_verification(
                "PR lifecycle generation changed during terminal verification"
            )
        review = None
        if trusted_recovery and legacy_recovery:
            review = (
                (await db.execute(
                    select(PRReview)
                    .where(PRReview.id == run.current_review_id)
                    .with_for_update()
                )).scalar_one_or_none()
                if run.current_review_id is not None
                else None
            )
            if (
                review is None
                or legacy_review_generation(review)
                != observed_legacy_review_generation
                or not _is_strict_legacy_lifecycle_recovery_candidate(
                    review,
                    run,
                )
            ):
                await db.rollback()
                return {
                    "status": "ignored",
                    "reason": "legacy lifecycle candidate changed",
                }

        if not remote_terminal_matches:
            if trusted_recovery and not legacy_recovery:
                # A fresh OPEN read can mean that GitHub reopened the PR after
                # the signed close. Background recovery is not authorized to
                # erase that durable fence; only a signed reopened/ready
                # webhook may admit a new immutable attempt and clear it after
                # all old effect owners settle.
                run.terminal_intent_checked_at = datetime.utcnow()
                await db.commit()
            elif trusted_recovery and legacy_recovery:
                assert review is not None
                run.legacy_terminal_recovery_pending = False
                run.terminal_intent_checked_at = datetime.utcnow()
                review.publication_state = "failed"
                review.failure_stage = "recovery"
                review.publication_error = (
                    "Historical GitHub publication evidence is unavailable; "
                    "the legacy terminal marker was disproved"
                )
                await db.commit()
            else:
                await db.rollback()
            return {
                "status": "ignored",
                "reason": "stale pull_request.closed delivery",
            }

        terminal_status = "merged" if remote_is_merged else "closed"
        persist_new_intent = not trusted_recovery
        if trusted_recovery and legacy_recovery:
            persist_new_intent = True
        elif trusted_recovery:
            if (
                run.terminal_intent_status != terminal_status
                or run.terminal_intent_base_ref != base_ref
                or run.terminal_intent_head_sha != head_sha
            ):
                await db.rollback()
                return {"status": "ignored", "reason": "no matching terminal intent"}

        if persist_new_intent:
            # Persist the signed (or narrowly admitted historical) and
            # remote-verified intent before active work is quiesced. Recovery
            # scans only this small durable set.
            run.terminal_intent_status = terminal_status
            run.terminal_intent_base_ref = base_ref
            run.terminal_intent_head_sha = head_sha
            run.terminal_intent_delivery_id = delivery_id
            run.terminal_intent_observed_at = datetime.utcnow()
            run.terminal_intent_checked_at = None
            run.legacy_terminal_recovery_pending = False
            # Read the persisted representation before freezing the CAS tuple.
            # MySQL deployments may normalize DateTime precision on write; a
            # tuple built only from the pre-flush Python value would then reject
            # its own committed intent at the final barrier.
            await db.flush()
            await db.refresh(run)
            committed_intent_generation = run_generation(run)
            repeated_repo_generation = repo_generation(repo)
            repeated_repo_name = repo.repo_full_name
            await db.commit()

            # The intent commit is the durable effect fence.  It survives any
            # failure, mismatch, or rollback after this point.  Repeat GitHub's
            # terminal read only after commit returned the connection to the
            # pool, then re-lock and CAS the exact committed intent.
            repeated = _validated_pr_snapshot(
                await _gh_pr_view(pr_number, repeated_repo_name)
            )
            repeated_matches = bool(
                repeated.get("base_ref") == base_ref
                and repeated.get("head_sha") == head_sha
                and (
                    (
                        terminal_status == "merged"
                        and repeated.get("state") == "MERGED"
                        and repeated.get("merged_at") is not None
                    )
                    or (
                        terminal_status == "closed"
                        and repeated.get("state") == "CLOSED"
                        and repeated.get("merged_at") is None
                    )
                )
            )
            if not repeated_matches:
                return {"status": "ignored", "reason": "terminal intent awaits reconciliation"}

            db.expire_all()
            repo = (await db.execute(
                select(MonitoredRepo)
                .where(MonitoredRepo.id == repo_id)
                .with_for_update()
            )).scalar_one_or_none()
            if repo is None or not repo.enabled:
                await db.rollback()
                return {"status": "ignored", "reason": "repository not monitored or disabled"}
            if repo_generation(repo) != repeated_repo_generation:
                await db.rollback()
                return {
                    "status": "ignored",
                    "reason": "terminal intent awaits reconciliation",
                }
            run = (await db.execute(
                select(PRMonitorRun)
                .where(
                    PRMonitorRun.repo_id == repo_id,
                    PRMonitorRun.pr_number == pr_number,
                    PRMonitorRun.terminal_intent_status == terminal_status,
                    PRMonitorRun.terminal_intent_base_ref == base_ref,
                    PRMonitorRun.terminal_intent_head_sha == head_sha,
                )
                .with_for_update()
            )).scalar_one_or_none()
            if (
                run is None
                or run_generation(run) != committed_intent_generation
            ):
                await db.rollback()
                return {"status": "ignored", "reason": "terminal intent changed"}
        if run.status == terminal_status and run.completed_at is not None:
            await db.rollback()
            return {
                "status": "accepted",
                "run_id": run.id,
                "lifecycle": terminal_status,
            }
        try:
            await _quiesce_monitor_runs(
                db,
                repo_id=repo_id,
                run_id=run.id,
                reason=f"pr_{terminal_status}",
                terminal_reconciliation=True,
            )
        except HTTPException as exc:
            if exc.status_code == 409:
                await db.rollback()
                # A non-2xx response asks GitHub to retry.  Never claim the
                # terminal lifecycle while an exact reviewer/publication,
                # Finding fix, rebuttal, Repair, thread-resolution, or Merge
                # Queue generation may still perform an effect.
                raise HTTPException(
                    503,
                    "PR terminal event is pending active-effect quiescence",
                ) from exc
            raise
        run = await db.get(PRMonitorRun, run.id, populate_existing=True)
        assert run is not None
        review = (
            (await db.execute(
                select(PRReview)
                .where(PRReview.id == run.current_review_id)
                .with_for_update()
            )).scalar_one_or_none()
            if run.current_review_id is not None
            else None
        )
        now = datetime.utcnow()
        if review is not None and review.status in {
            "pending", "waiting_ci", "reviewing", "superseding"
        }:
            review.status = "cancelled"
            review.completed_at = now
            review.publication_state = "not_applicable"
            review.failure_stage = "lifecycle"
            if review.code_verdict is None:
                review.review_summary = (
                    f"PR was {terminal_status} before review publication completed"
                )
            await db.execute(
                sa_update(PRReviewerRun)
                .where(
                    PRReviewerRun.pr_review_id == review.id,
                    PRReviewerRun.status.in_(("pending", "reviewing", "finalizing")),
                )
                .values(
                    status="cancelled",
                    error_message=f"PR was {terminal_status}",
                    completed_at=now,
                )
            )
        await db.execute(
            sa_update(PRRepairWake)
            .where(
                PRRepairWake.monitor_run_id == run.id,
                PRRepairWake.status.in_(("shadow", "pending")),
            )
            .values(
                status="superseded",
                last_error=f"pr_{terminal_status}",
                completed_at=now,
            )
        )
        await db.execute(
            sa_update(PRMergeQueueAction)
            .where(
                PRMergeQueueAction.monitor_run_id == run.id,
                or_(
                    PRMergeQueueAction.status.in_(("shadow", "pending")),
                    and_(
                        PRMergeQueueAction.status == "paused",
                        ~pr_merge_queue_action_ambiguous_remote_effect_predicate(),
                    ),
                ),
            )
            .values(
                status="superseded",
                last_error=f"pr_{terminal_status}",
                completed_at=now,
            )
        )
        run.status = terminal_status
        run.pause_reason = None
        run.completed_at = now
        run.terminal_intent_checked_at = now
        run.state_version += 1
        await db.commit()
        return {"status": "accepted", "run_id": run.id, "lifecycle": terminal_status}


async def reconcile_remote_pr_lifecycles(db_factory, *, limit: int = 10) -> int:
    """Recover durable intents plus one narrowly-classified legacy row.

    Signed intents use their frozen event subject, not the potentially older
    current Review. One slot per sweep is reserved for a pre-migration
    lifecycle/publication race; that path still requires two matching remote
    terminal reads before it can create an intent.
    """

    if limit <= 0:
        return 0
    async with db_factory() as db:
        from backend.services.pr_review_service import _database_now

        db_now = await _database_now(db)
        cutoff = db_now - timedelta(minutes=5)
        legacy_summary = func.lower(func.coalesce(PRReview.review_summary, ""))
        legacy_candidates = list((await db.execute(
            select(
                PRMonitorRun.id,
                PRMonitorRun.repo_id,
                PRMonitorRun.pr_number,
                PRMonitorRun.terminal_intent_checked_at,
            )
            .join(MonitoredRepo, MonitoredRepo.id == PRMonitorRun.repo_id)
            .join(PRReview, PRReview.id == PRMonitorRun.current_review_id)
            .where(
                MonitoredRepo.enabled.is_(True),
                PRMonitorRun.status.not_in(("merged", "closed")),
                PRMonitorRun.completed_at.is_(None),
                PRMonitorRun.terminal_intent_status.is_(None),
                PRMonitorRun.terminal_intent_base_ref.is_(None),
                PRMonitorRun.terminal_intent_head_sha.is_(None),
                PRMonitorRun.terminal_intent_delivery_id.is_(None),
                PRMonitorRun.terminal_intent_observed_at.is_(None),
                PRMonitorRun.legacy_terminal_recovery_pending.is_(True),
                PRReview.status == "error",
                PRReview.action_taken == "error",
                PRReview.pending_action.in_((
                    "lgtm_comment",
                    "review_comments",
                    "approved_merged",
                )),
                PRReview.publication_state == "not_applicable",
                PRReview.failure_stage == "lifecycle",
                or_(*(
                    legacy_summary.contains(marker)
                    for marker in _LEGACY_LIFECYCLE_RECOVERY_MARKERS
                )),
                or_(
                    PRMonitorRun.terminal_intent_checked_at.is_(None),
                    PRMonitorRun.terminal_intent_checked_at <= cutoff,
                ),
            )
            .order_by(
                PRMonitorRun.terminal_intent_checked_at.asc(),
                PRMonitorRun.id.asc(),
            )
            .limit(1)
        )).all())
        signed_limit = max(0, limit - len(legacy_candidates))
        signed_candidates = list((await db.execute(
            select(
                PRMonitorRun.id,
                PRMonitorRun.repo_id,
                PRMonitorRun.pr_number,
                PRMonitorRun.terminal_intent_base_ref,
                PRMonitorRun.current_base_sha,
                PRMonitorRun.terminal_intent_head_sha,
                PRMonitorRun.terminal_intent_checked_at,
            )
            .join(MonitoredRepo, MonitoredRepo.id == PRMonitorRun.repo_id)
            .where(
                MonitoredRepo.enabled.is_(True),
                PRMonitorRun.status.not_in(("merged", "closed")),
                PRMonitorRun.terminal_intent_status.in_(("merged", "closed")),
                PRMonitorRun.terminal_intent_base_ref.is_not(None),
                PRMonitorRun.terminal_intent_head_sha.is_not(None),
                or_(
                    PRMonitorRun.terminal_intent_checked_at.is_(None),
                    PRMonitorRun.terminal_intent_checked_at <= cutoff,
                ),
            )
            .order_by(
                PRMonitorRun.terminal_intent_checked_at.asc(),
                PRMonitorRun.id.asc(),
            )
            .limit(signed_limit)
        )).all()) if signed_limit else []
        legacy_rows = []
        for row in legacy_candidates:
            checked_match = (
                PRMonitorRun.terminal_intent_checked_at.is_(None)
                if row.terminal_intent_checked_at is None
                else PRMonitorRun.terminal_intent_checked_at
                == row.terminal_intent_checked_at
            )
            claimed = await db.execute(
                sa_update(PRMonitorRun)
                .where(
                    PRMonitorRun.id == row.id,
                    PRMonitorRun.status.not_in(("merged", "closed")),
                    PRMonitorRun.completed_at.is_(None),
                    PRMonitorRun.terminal_intent_status.is_(None),
                    PRMonitorRun.terminal_intent_base_ref.is_(None),
                    PRMonitorRun.terminal_intent_head_sha.is_(None),
                    PRMonitorRun.terminal_intent_delivery_id.is_(None),
                    PRMonitorRun.terminal_intent_observed_at.is_(None),
                    PRMonitorRun.legacy_terminal_recovery_pending.is_(True),
                    checked_match,
                )
                .values(terminal_intent_checked_at=db_now)
            )
            if claimed.rowcount == 1:
                legacy_rows.append(row)
        signed_rows = []
        for row in signed_candidates:
            checked_match = (
                PRMonitorRun.terminal_intent_checked_at.is_(None)
                if row.terminal_intent_checked_at is None
                else PRMonitorRun.terminal_intent_checked_at
                == row.terminal_intent_checked_at
            )
            claimed = await db.execute(
                sa_update(PRMonitorRun)
                .where(
                    PRMonitorRun.id == row.id,
                    PRMonitorRun.status.not_in(("merged", "closed")),
                    PRMonitorRun.terminal_intent_status.in_(("merged", "closed")),
                    PRMonitorRun.terminal_intent_base_ref
                    == row.terminal_intent_base_ref,
                    PRMonitorRun.terminal_intent_head_sha
                    == row.terminal_intent_head_sha,
                    checked_match,
                )
                .values(terminal_intent_checked_at=db_now)
            )
            if claimed.rowcount == 1:
                signed_rows.append(row)
        if legacy_rows or signed_rows:
            await db.commit()

    reconciled = 0
    for (
        _run_id,
        repo_id,
        pr_number,
        base_ref,
        base_sha,
        head_sha,
        _checked_at,
    ) in signed_rows:
        if not isinstance(base_ref, str) or not isinstance(head_sha, str):
            continue
        async with db_factory() as db:
            try:
                result = await _terminalize_pull_request_run(
                    db,
                    repo_id=repo_id,
                    pr_number=pr_number,
                    base_ref=base_ref,
                    base_sha=base_sha,
                    head_sha=head_sha,
                    merged=None,
                    trusted_recovery=True,
                )
            except HTTPException as exc:
                if exc.status_code == 503:
                    continue
                logger.warning(
                    "Remote PR lifecycle recovery rejected %s#%s: %s",
                    repo_id,
                    pr_number,
                    exc.detail,
                )
            except Exception:
                logger.debug(
                    "Remote PR lifecycle is not terminal for %s#%s",
                    repo_id,
                    pr_number,
                    exc_info=True,
                )
            else:
                if result.get("status") == "accepted":
                    reconciled += 1

    if legacy_rows:
        from backend.services.pr_review_service import (
            _gh_pr_view,
            _validated_pr_snapshot,
        )

    for _run_id, repo_id, pr_number, _checked_at in legacy_rows:
        async with db_factory() as db:
            try:
                repo_name = await db.scalar(
                    select(MonitoredRepo.repo_full_name).where(
                        MonitoredRepo.id == repo_id,
                        MonitoredRepo.enabled.is_(True),
                    )
                )
                await db.rollback()
                if not isinstance(repo_name, str):
                    continue
                snapshot = _validated_pr_snapshot(
                    await _gh_pr_view(pr_number, repo_name)
                )
                if (
                    snapshot.get("state") not in {"MERGED", "CLOSED"}
                    or (
                        snapshot.get("state") == "MERGED"
                        and snapshot.get("merged_at") is None
                    )
                    or (
                        snapshot.get("state") == "CLOSED"
                        and snapshot.get("merged_at") is not None
                    )
                ):
                    # Re-lock Repo -> Run -> Review before disproving the
                    # migration candidate. Restore the publication axis to a
                    # recovery failure; leaving lifecycle/not_applicable would
                    # falsely claim that GitHub proved a close or merge.
                    await db.rollback()
                    locked_repo = (await db.execute(
                        select(MonitoredRepo)
                        .where(
                            MonitoredRepo.id == repo_id,
                            MonitoredRepo.enabled.is_(True),
                        )
                        .with_for_update()
                    )).scalar_one_or_none()
                    legacy_run = (await db.execute(
                        select(PRMonitorRun)
                        .where(
                            PRMonitorRun.id == _run_id,
                            PRMonitorRun.repo_id == repo_id,
                            PRMonitorRun.terminal_intent_status.is_(None),
                            PRMonitorRun.terminal_intent_base_ref.is_(None),
                            PRMonitorRun.terminal_intent_head_sha.is_(None),
                            PRMonitorRun.terminal_intent_delivery_id.is_(None),
                            PRMonitorRun.terminal_intent_observed_at.is_(None),
                            PRMonitorRun.legacy_terminal_recovery_pending.is_(True),
                        )
                        .with_for_update()
                    )).scalar_one_or_none()
                    legacy_review = (
                        (await db.execute(
                            select(PRReview)
                            .where(PRReview.id == legacy_run.current_review_id)
                            .with_for_update()
                        )).scalar_one_or_none()
                        if legacy_run is not None
                        and legacy_run.current_review_id is not None
                        else None
                    )
                    if (
                        locked_repo is not None
                        and legacy_run is not None
                        and legacy_review is not None
                        and _is_strict_legacy_lifecycle_recovery_candidate(
                            legacy_review,
                            legacy_run,
                        )
                    ):
                        legacy_run.legacy_terminal_recovery_pending = False
                        legacy_run.terminal_intent_checked_at = datetime.utcnow()
                        legacy_review.publication_state = "failed"
                        legacy_review.failure_stage = "recovery"
                        legacy_review.publication_error = (
                            "Historical GitHub publication evidence is unavailable; "
                            "the legacy terminal marker was disproved"
                        )
                        await db.commit()
                    else:
                        await db.rollback()
                    continue
                result = await _terminalize_pull_request_run(
                    db,
                    repo_id=repo_id,
                    pr_number=pr_number,
                    base_ref=str(snapshot["base_ref"]),
                    base_sha=str(snapshot["base_sha"]),
                    head_sha=str(snapshot["head_sha"]),
                    merged=snapshot.get("state") == "MERGED",
                    trusted_recovery=True,
                    legacy_recovery=True,
                )
            except HTTPException as exc:
                if exc.status_code == 503:
                    continue
                logger.warning(
                    "Legacy PR lifecycle recovery rejected %s#%s: %s",
                    repo_id,
                    pr_number,
                    exc.detail,
                )
            except Exception:
                logger.debug(
                    "Legacy PR lifecycle is not terminal for %s#%s",
                    repo_id,
                    pr_number,
                    exc_info=True,
                )
            else:
                if result.get("status") == "accepted":
                    reconciled += 1
    return reconciled


@webhook_router.post("/webhook")
async def github_webhook(request: Request, db: AsyncSession = Depends(get_db)):
    body = await _read_github_webhook_body(request)

    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError, RecursionError):
        raise HTTPException(400, "Invalid JSON payload")
    if not isinstance(payload, dict):
        raise HTTPException(400, "GitHub webhook payload must be an object")

    if "repository" not in payload:
        return {"status": "ignored", "reason": "no repository info"}
    repository = payload["repository"]
    if not isinstance(repository, dict):
        raise HTTPException(400, "repository must be an object")
    if "full_name" not in repository or repository["full_name"] == "":
        return {"status": "ignored", "reason": "no repository info"}
    repo_full_name = repository["full_name"]
    if not isinstance(repo_full_name, str):
        raise HTTPException(400, "repository.full_name must be a string")
    if len(repo_full_name) > _MAX_GITHUB_REPO_FULL_NAME_CHARS:
        raise HTTPException(400, "repository.full_name is too long")

    result = await db.execute(
        select(MonitoredRepo).where(MonitoredRepo.repo_full_name == repo_full_name)
    )
    repo = result.scalar_one_or_none()
    if not repo or not repo.enabled:
        return {"status": "ignored", "reason": "repository not monitored or disabled"}

    signature_header = request.headers.get("X-Hub-Signature-256", "")
    _require_current_webhook_signature(
        repo,
        body=body,
        signature_header=signature_header,
    )

    event_type = request.headers.get("X-GitHub-Event", "")
    if event_type == "merge_group":
        if payload.get("action") != "checks_requested":
            return {"status": "ignored", "reason": f"merge_group action: {payload.get('action', '')}"}
        merge_group = payload.get("merge_group")
        if not isinstance(merge_group, dict):
            raise HTTPException(400, "merge_group must be an object")
        merge_sha = _parse_commit_sha(merge_group.get("head_sha"), "merge_group")
        merge_ref = merge_group.get("head_ref")
        if not isinstance(merge_ref, str) or not merge_ref or len(merge_ref) > 500:
            raise HTTPException(400, "merge_group.head_ref is invalid")
        from backend.services.pr_merge_queue import bind_merge_group

        repo_id = repo.id
        await db.rollback()
        async with _pr_repo_write_lock(repo_id):
            locked_repo = (await db.execute(
                select(MonitoredRepo)
                .where(MonitoredRepo.id == repo_id)
                .with_for_update()
            )).scalar_one_or_none()
            if locked_repo is None or not locked_repo.enabled:
                await db.rollback()
                return {
                    "status": "ignored",
                    "reason": "repository not monitored or disabled",
                }
            _require_current_webhook_signature(
                locked_repo,
                body=body,
                signature_header=signature_header,
            )
            bound = await bind_merge_group(
                db,
                repo=locked_repo,
                head_sha=merge_sha,
                head_ref=merge_ref,
            )
        return {
            "status": "accepted" if bound else "ignored",
            "reason": None if bound else "no unique queued PR for merge group",
        }

    # Only handle pull_request events beyond this point.
    if event_type != "pull_request":
        return {"status": "ignored", "reason": f"event type: {event_type}"}

    action = payload.get("action", "")
    reopened_delivery = action in {"reopened", "ready_for_review"}
    if action in {"reopened", "ready_for_review"}:
        # Both events admit an exact open subject.  A reopened event is a new
        # attempt on the stable PR lifecycle; ready_for_review commonly has no
        # earlier non-draft review and follows normal opened idempotency.
        action = "opened"
    if action == "edited":
        changes = payload.get("changes")
        base_change = changes.get("base") if isinstance(changes, dict) else None
        ref_change = (
            base_change.get("ref") if isinstance(base_change, dict) else None
        )
        previous_ref = (
            ref_change.get("from") if isinstance(ref_change, dict) else None
        )
        if not isinstance(previous_ref, str) or not previous_ref:
            return {
                "status": "ignored",
                "reason": "edited event did not retarget the PR base",
            }
        # A base retarget is a new immutable review subject even when GitHub
        # reports the same base/head commit OIDs. Reuse synchronize's durable
        # supersede/termination protocol.
        action = "synchronize"
    if action not in ("opened", "synchronize", "closed"):
        return {"status": "ignored", "reason": f"action: {action}"}

    pr = payload.get("pull_request")
    if not isinstance(pr, dict):
        raise HTTPException(400, "pull_request must be an object")

    base = pr.get("base")
    head = pr.get("head")
    base_sha = _parse_commit_sha(
        base.get("sha") if isinstance(base, dict) else None,
        "base",
    )
    head_sha = _parse_commit_sha(
        head.get("sha") if isinstance(head, dict) else None,
        "head",
    )

    # Skip draft PRs
    if action != "closed" and pr.get("draft", False):
        return {"status": "ignored", "reason": "draft PR"}

    base_branch = base.get("ref", "") if isinstance(base, dict) else ""
    pr_author = pr.get("user", {}).get("login", "")

    pr_number = pr.get("number")
    delivery_id = (request.headers.get("X-GitHub-Delivery", "") or "").strip() or None
    reopened_idempotency_key = None
    if reopened_delivery:
        raw_reopen_key = delivery_id or hashlib.sha256(body).hexdigest()
        reopened_idempotency_key = (
            f"wh:{raw_reopen_key}"
            if len(raw_reopen_key) <= 61
            else f"wh:{hashlib.sha256(raw_reopen_key.encode()).hexdigest()[:61]}"
        )
    repo_id = repo.id
    repo_name = repo.repo_full_name

    if (
        not isinstance(pr_number, int)
        or isinstance(pr_number, bool)
        or pr_number <= 0
    ):
        raise HTTPException(400, "pull_request.number must be a positive integer")

    pr_title = pr.get("title", "")
    pr_url = pr.get("html_url", "")
    head_repo = head.get("repo") if isinstance(head, dict) else None
    head_repo_full_name = (
        head_repo.get("full_name") if isinstance(head_repo, dict) else None
    )
    head_branch = head.get("ref") if isinstance(head, dict) else None
    if head_repo_full_name is not None and (
        not isinstance(head_repo_full_name, str)
        or re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", head_repo_full_name) is None
    ):
        raise HTTPException(400, "pull_request.head.repo.full_name is invalid")
    if head_branch is not None and (
        not isinstance(head_branch, str)
        or not head_branch
        or len(head_branch) > 200
        or "\x00" in head_branch
    ):
        raise HTTPException(400, "pull_request.head.ref is invalid")

    # Signature verification and all payload validation are complete.  Copy
    # the bounded policy fields, release the request transaction, and only then
    # consult the service's GitHub identity.  The same actor scalar is reused
    # under repository row locks below; those locked checks are network-free.
    repo = _webhook_repo_snapshot(repo)
    await db.rollback()
    publisher_actor = None
    if action != "closed":
        publisher_actor = _bounded_github_publisher_actor(
            await _cached_github_publisher_identity()
        )
        policy_rejection = _webhook_policy_rejection(
            repo,
            base_branch=base_branch,
            pr_author=pr_author,
            publisher_actor=publisher_actor,
        )
        if policy_rejection is not None:
            return {"status": "ignored", "reason": policy_rejection}

    if action == "closed":
        merged = pr.get("merged")
        if not isinstance(merged, bool):
            raise HTTPException(400, "pull_request.merged must be a boolean")
        return await _terminalize_pull_request_run(
            db,
            repo_id=repo_id,
            pr_number=pr_number,
            base_ref=base_branch,
            base_sha=base_sha,
            head_sha=head_sha,
            merged=merged,
            body=body,
            signature_header=signature_header,
            delivery_id=delivery_id,
        )

    # Fast-path idempotency check. The database uniqueness constraints below
    # are still required because two deliveries can race between this SELECT
    # and the INSERT performed by create_pr_review_task.
    processed_review = await _find_processed_review(
        db,
        repo_id,
        pr_number,
        base_branch,
        base_sha,
        head_sha,
        delivery_id,
    )
    if processed_review and (
        not reopened_delivery
        or (delivery_id and processed_review.delivery_id == delivery_id)
        or processed_review.rerun_idempotency_key == reopened_idempotency_key
    ):
        logger.info(
            "Ignored duplicate PR webhook for %s#%d at %s...%s (review %d)",
            repo_name,
            pr_number,
            base_sha,
            head_sha,
            processed_review.id,
        )
        if _is_pr_review_input_rejection(processed_review):
            return _pr_review_input_rejection_response(processed_review)
        return _duplicate_review_response(processed_review, delivery_id)

    if action == "synchronize":
        from backend.services.pr_review_service import (
            verify_pr_review_snapshot_current,
        )

        replacement_data = {
            "number": pr_number,
            "base_ref": base_branch,
            "base_sha": base_sha,
            "head_sha": head_sha,
            "delivery_id": delivery_id,
            "title": pr_title,
            "author": pr_author,
            "url": pr_url,
            "head_repo_full_name": head_repo_full_name,
            "head_branch": head_branch,
        }
        observed_run_generation = await _pr_monitor_run_generation(
            db,
            repo_id=repo_id,
            pr_number=pr_number,
        )
        # Fetch and validate every model-visible byte before terminating the
        # old generation. A transient GitHub/context failure therefore leaves
        # the still-running review untouched and lets GitHub retry delivery.
        # The duplicate lookup opened a short read transaction.  Immutable
        # GitHub capture may take seconds, so release it before external I/O.
        await db.rollback()
        prepared_context, input_rejection = await _capture_pr_review_context_rejection(
            repo,
            replacement_data,
        )
        # Remote freshness checks are intentionally outside the repository
        # row-lock transaction.  The locked section below rechecks all local
        # policy, signature, idempotency, and exact-generation fences, while
        # publication retains its own fresh remote actor/snapshot fence.
        await verify_pr_review_snapshot_current(repo, replacement_data)
        prepared_ci_evidence = (
            await _capture_webhook_ci_evidence(
                repo,
                head_sha=head_sha,
            )
            if input_rejection is None
            else None
        )

        async with _pr_repo_write_lock(repo_id):
            db.expire_all()
            # This row lock is the cross-process write barrier. The lightweight
            # GitHub guard runs inside it, after context preparation, so a slow
            # older webhook cannot overwrite a newer durable intent.
            locked_repo = (
                await db.execute(
                    select(MonitoredRepo)
                    .where(MonitoredRepo.id == repo_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if locked_repo is None or not locked_repo.enabled:
                await db.rollback()
                return {
                    "status": "ignored",
                    "reason": "repository not monitored or disabled",
                }
            # Context capture may take long enough for an administrator to
            # rotate the secret or edit admission policy.  The repository row
            # lock is the delivery's linearization point, so repeat both checks
            # here before persisting a supersede intent or stopping old work.
            _require_current_webhook_signature(
                locked_repo,
                body=body,
                signature_header=signature_header,
            )
            policy_rejection = _webhook_policy_rejection(
                locked_repo,
                base_branch=base_branch,
                pr_author=pr_author,
                publisher_actor=publisher_actor,
            )
            if policy_rejection is not None:
                await db.rollback()
                return {"status": "ignored", "reason": policy_rejection}
            if prepared_context is None:
                await db.rollback()
                raise HTTPException(
                    503,
                    "PR review input could not be revalidated against the "
                    "current repository policy",
                )
            # The repository policy observed during slow GitHub capture is not
            # authoritative.  Always replace that preliminary outcome with a
            # fresh preflight against the row locked above; a provider/mode
            # change may make the same immutable context either admissible or
            # too large.
            input_rejection = _capture_pr_review_preflight_rejection(
                locked_repo,
                replacement_data,
                prepared_context=prepared_context,
            )
            locked_ci_evidence = (
                _require_locked_ci_evidence(
                    locked_repo,
                    head_sha=head_sha,
                    evidence=prepared_ci_evidence,
                )
                if input_rejection is None
                else None
            )

            # Re-run idempotency at the actual write barrier. This prevents a
            # duplicate same-snapshot synchronize from claiming its own newly
            # created review as an older generation.
            processed_review = await _find_processed_review(
                db,
                repo_id,
                pr_number,
                base_branch,
                base_sha,
                head_sha,
                delivery_id,
            )
            if processed_review is not None:
                duplicate_response = (
                    _pr_review_input_rejection_response(processed_review)
                    if _is_pr_review_input_rejection(processed_review)
                    else _duplicate_review_response(processed_review, delivery_id)
                )
                await db.rollback()
                return duplicate_response

            locked_lifecycle = (await db.execute(
                select(PRMonitorRun)
                .where(
                    PRMonitorRun.repo_id == repo_id,
                    PRMonitorRun.pr_number == pr_number,
                )
                .with_for_update()
            )).scalar_one_or_none()
            locked_run_generation = _pr_monitor_run_generation_value(
                locked_lifecycle
            )
            if locked_run_generation != observed_run_generation:
                await db.rollback()
                raise HTTPException(
                    409,
                    "A newer PR lifecycle generation changed while this "
                    "delivery performed remote verification; retry the "
                    "synchronize event",
                )
            if locked_lifecycle is not None:
                # The signed GitHub delivery is still a legacy PR Monitor
                # writer.  An exact DeliveryRun edge (including a terminal
                # one) permanently reserves this lifecycle for the Delivery
                # controller; never supersede its Review with an opaque
                # webhook generation.
                await _require_legacy_pr_effect_allowed(
                    db,
                    action="synchronized",
                    monitor_run=locked_lifecycle,
                    allow_terminal_intent=True,
                )

            active_merge_effect = await db.scalar(
                select(PRMergeQueueAction.id)
                .join(
                    PRMonitorRun,
                    PRMonitorRun.id == PRMergeQueueAction.monitor_run_id,
                )
                .where(
                    PRMonitorRun.repo_id == repo_id,
                    PRMonitorRun.pr_number == pr_number,
                    or_(
                        PRMergeQueueAction.status.in_(
                            _STARTED_MERGE_QUEUE_STATUSES
                        ),
                        pr_merge_queue_action_ambiguous_remote_effect_predicate(),
                    ),
                )
                .order_by(PRMergeQueueAction.id)
                .limit(1)
                .with_for_update()
            )
            if active_merge_effect is not None:
                await db.rollback()
                raise HTTPException(
                    503,
                    "PR synchronize is waiting for Merge Queue ownership "
                    "to reconcile",
                )

            active_repair = (await db.execute(
                select(PRRepairWake)
                .join(PRMonitorRun, PRMonitorRun.id == PRRepairWake.monitor_run_id)
                .where(
                    PRMonitorRun.repo_id == repo_id,
                    PRMonitorRun.pr_number == pr_number,
                    PRRepairWake.status == "accepted",
                )
                .order_by(desc(PRRepairWake.id))
            )).scalars().first()
            active_adjudication = (await db.execute(
                select(PRFindingRebuttal)
                .join(PRReview, PRReview.id == PRFindingRebuttal.pr_review_id)
                .where(
                    PRReview.repo_id == repo_id,
                    PRReview.pr_number == pr_number,
                    PRFindingRebuttal.status == "adjudicating",
                )
                .order_by(desc(PRFindingRebuttal.id))
            )).scalars().first()
            active_review_predicate = PRReview.status.in_(
                ("pending", "waiting_ci", "reviewing", "superseding")
            )
            if active_repair is not None and active_repair.review_id is not None:
                active_review_predicate = or_(
                    active_review_predicate,
                    PRReview.id == active_repair.review_id,
                )
            if active_adjudication is not None:
                active_review_predicate = or_(
                    active_review_predicate,
                    PRReview.id == active_adjudication.pr_review_id,
                )
            active_result = await db.execute(
                select(PRReview).where(
                    PRReview.repo_id == repo_id,
                    PRReview.pr_number == pr_number,
                    active_review_predicate,
                )
            )
            observed_reviews = list(active_result.scalars().all())

            # A publishing row is a durable external-action outbox. Never
            # supersede it while a GitHub write may be in flight; it remains
            # pinned to the old head and reconciles independently.
            if not observed_reviews:
                try:
                    if input_rejection is not None:
                        from backend.services.pr_review_service import (
                            create_pr_review_input_rejection,
                        )

                        review = await create_pr_review_input_rejection(
                            db,
                            locked_repo,
                            replacement_data,
                            error=input_rejection,
                        )
                    else:
                        if prepared_context is None:
                            raise HTTPException(
                                500,
                                "PR review input capture is incomplete",
                            )
                        review = await _create_pr_review_task_or_422(
                            db,
                            locked_repo,
                            replacement_data,
                            prepared_context=prepared_context,
                            prepared_ci_evidence=locked_ci_evidence,
                            allow_remote_ci=False,
                        )
                except IntegrityError:
                    await db.rollback()
                    winner = await _find_processed_review(
                        db,
                        repo_id,
                        pr_number,
                        base_branch,
                        base_sha,
                        head_sha,
                        delivery_id,
                    )
                    if winner is not None:
                        if _is_pr_review_input_rejection(winner):
                            return _pr_review_input_rejection_response(winner)
                        return _duplicate_review_response(
                            winner,
                            delivery_id,
                        )
                    raise
                if input_rejection is not None:
                    return _pr_review_input_rejection_response(review)
                return {"status": "accepted", "review_id": review.id}

            # Persist the immutable replacement intent before touching any old
            # Task. Each row is exact-CASed from the state observed under the
            # repository barrier. A stale webhook cannot overwrite a newer
            # superseding token, and a partial claim is rolled back atomically.
            superseding_token = secrets.token_hex(24)
            superseding_started_at = datetime.utcnow()
            # Version 4 always retains the exact immutable context.  A size
            # rejection is only an admission hint from this write barrier; a
            # crash recovery or the final post-termination barrier must apply
            # the then-current locked provider/mode before deciding whether to
            # create a Task or a durable rejection result.  Legacy v2/v3
            # snapshots remain readable by recovery.
            superseding_snapshot = {
                "version": 4,
                "pr_data": replacement_data,
                "prepared_context": prepared_context,
                "input_rejection": (
                    {
                        "category": input_rejection.category,
                        "measured": input_rejection.measured,
                        "limit": input_rejection.limit,
                        "unit": input_rejection.unit,
                    }
                    if input_rejection is not None
                    else None
                ),
            }
            active_review_generations = []
            for old in observed_reviews:
                predicates = [
                    PRReview.id == old.id,
                    PRReview.repo_id == repo_id,
                    PRReview.pr_number == pr_number,
                    PRReview.status == old.status,
                    (
                        PRReview.task_id.is_(None)
                        if old.task_id is None
                        else PRReview.task_id == old.task_id
                    ),
                ]
                if old.status == "superseding":
                    predicates.extend(
                        (
                            (
                                PRReview.superseding_token.is_(None)
                                if old.superseding_token is None
                                else PRReview.superseding_token
                                == old.superseding_token
                            ),
                            (
                                PRReview.superseding_started_at.is_(None)
                                if old.superseding_started_at is None
                                else PRReview.superseding_started_at
                                == old.superseding_started_at
                            ),
                        )
                    )
                claimed = await db.execute(
                    sa_update(PRReview)
                    .where(*predicates)
                    .values(
                        status="superseding",
                        superseding_snapshot=superseding_snapshot,
                        superseding_token=superseding_token,
                        superseding_started_at=superseding_started_at,
                    )
                )
                if claimed.rowcount != 1:
                    await db.rollback()
                    raise HTTPException(
                        409,
                        "A newer PR synchronize intent won the write barrier; "
                        "this stale delivery was not applied",
                    )
                active_review_generations.append(
                    (old.id, old.task_id, old.status)
                )
            await db.commit()

            claimed_rows = await db.execute(
                select(PRReview.id).where(
                    PRReview.id.in_(
                        review_id
                        for review_id, _task_id, _status
                        in active_review_generations
                    ),
                    PRReview.status == "superseding",
                    PRReview.superseding_token == superseding_token,
                )
            )
            claimed_ids = set(claimed_rows.scalars().all())
            expected_ids = {
                review_id
                for review_id, _task_id, _status
                in active_review_generations
            }
            if claimed_ids != expected_ids:
                await db.rollback()
                raise HTTPException(
                    409,
                    "A newer PR synchronize intent replaced this delivery; "
                    "durable recovery will finish the newer snapshot",
                )

            if active_repair is not None:
                from backend.services.pr_monitor_loop import (
                    record_repair_push_observed,
                )

                active_repair_id = active_repair.id
                active_repair_head_sha = active_repair.trigger_head_sha
                await record_repair_push_observed(
                    db,
                    wake_id=active_repair_id,
                    previous_head_sha=active_repair_head_sha,
                    new_head_sha=head_sha,
                )
                active_repair = await db.get(
                    PRRepairWake,
                    active_repair_id,
                    populate_existing=True,
                )

            repair_developer_task_id = (
                active_repair.developer_task_id
                if active_repair is not None
                else None
            )
            repair_retry_count = (
                active_repair.accepted_task_retry_count
                if active_repair is not None
                else None
            )
            repair_session_id = (
                active_repair.accepted_session_id
                if active_repair is not None
                else None
            )
            completed_repair_developer_task_id = (
                repair_developer_task_id
                if active_repair is not None and active_repair.status == "completed"
                else None
            )

            from backend.services.task_termination import (
                TaskTerminationResult,
                TaskTerminationConflict,
                lock_task_generation,
                lock_worker_task_generation,
                task_termination_operation_locks,
                terminate_authoritative_task_generation,
            )

            task_ids = {
                task_id
                for _review_id, task_id, _status in active_review_generations
                if task_id is not None
            }
            panel_task_ids = (await db.execute(
                select(PRReviewerRun.task_id).where(
                    PRReviewerRun.pr_review_id.in_(expected_ids),
                    PRReviewerRun.task_id.is_not(None),
                )
            )).scalars().all()
            task_ids.update(panel_task_ids)
            if repair_developer_task_id is not None:
                repair_task = await db.get(Task, repair_developer_task_id)
                if (
                    repair_task is not None
                    and repair_task.status in ("in_progress", "executing")
                    and repair_task.retry_count == repair_retry_count
                    and repair_task.session_id == repair_session_id
                ):
                    task_ids.add(repair_task.id)
            if active_adjudication is not None and active_adjudication.task_id is not None:
                adjudicator_task = await db.get(Task, active_adjudication.task_id)
                if adjudicator_task is not None and adjudicator_task.status in (
                    "pending", "in_progress", "executing", "completed"
                ):
                    task_ids.add(adjudicator_task.id)
            # Worker migration and remote task mutations must remain excluded
            # until the replacement review commit releases the exact Task row
            # locks below. Otherwise a remote retry can run before its delayed
            # Manager mirror update is blocked by our database transaction.
            async with task_termination_operation_locks(task_ids):
                termination_results = {}
                for old_task_id in sorted(task_ids):
                    try:
                        termination_results[old_task_id] = (
                            await terminate_authoritative_task_generation(
                                old_task_id,
                                db,
                                reason="Superseded by new push",
                                operation_locks_held=True,
                                allow_delivery_effect_stop=True,
                            )
                        )
                    except TaskTerminationConflict as exc:
                        await db.rollback()
                        logger.warning(
                            "Refused to supersede PR review panel: task %d cleanup "
                            "was not confirmed: %s",
                            old_task_id,
                            exc,
                        )
                        raise HTTPException(
                            409,
                            "Previous PR review task cleanup could not be "
                            "confirmed; durable replacement recovery will retry",
                        ) from exc

                # Reacquire every exact resulting generation in stable order
                # and retain the row + operation locks through replacement
                # creation. A retry in the post-cleanup window then fails this
                # webhook rather than reviving the old review alongside its
                # replacement.
                for old_task_id in sorted(termination_results):
                    terminated = termination_results[old_task_id]
                    if isinstance(terminated, TaskTerminationResult):
                        locked_task = await lock_task_generation(
                            old_task_id,
                            db,
                            expected_status=terminated.terminal_status,
                            expected_retry_count=terminated.retry_count,
                            expected_turn_generation=terminated.turn_generation,
                            expected_instance_id=terminated.instance_id,
                            expected_started_at=terminated.started_at,
                            expected_completed_at=terminated.completed_at,
                            expected_pty_background_generation=(
                                terminated.pty_background_generation
                            ),
                        )
                    else:
                        locked_task = await lock_worker_task_generation(
                            db,
                            terminated.resulting,
                        )
                    if locked_task is None:
                        await db.rollback()
                        raise HTTPException(
                            409,
                            "Previous PR review task started a newer generation; "
                            "durable replacement recovery will retry",
                        )
                    if (
                        old_task_id == completed_repair_developer_task_id
                    ):
                        from backend.services.pr_monitor_loop import (
                            restore_repair_developer_task,
                        )

                        restore_repair_developer_task(locked_task)

                # The first repository row lock was released by the durable
                # intent commit. Reacquire it before the final review updates
                # and replacement INSERT so a newer webhook on another Manager
                # cannot observe the old token, wait here, then lose its intent
                # after this transaction commits.
                current_repo = (
                    await db.execute(
                        select(MonitoredRepo)
                        .where(MonitoredRepo.id == repo_id)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if current_repo is None or not current_repo.enabled:
                    await db.rollback()
                    raise HTTPException(
                        409,
                        "PR monitor changed during synchronize; durable "
                        "replacement recovery will retry",
                    )
                # Old-Task termination may take long enough for an
                # administrator to change provider, panel mode, or the CI
                # policy after the first durable-intent barrier.  Recompute
                # admission from the immutable context under this final
                # repository lock.  Never let stale size evidence win, and do
                # not perform a remote CI call while Task/DB locks are held.
                input_rejection = _capture_pr_review_preflight_rejection(
                    current_repo,
                    replacement_data,
                    prepared_context=prepared_context,
                )
                locked_ci_evidence = (
                    _require_locked_ci_evidence(
                        current_repo,
                        head_sha=head_sha,
                        evidence=prepared_ci_evidence,
                    )
                    if input_rejection is None
                    else None
                )

                for (
                    review_id,
                    old_task_id,
                    _old_status,
                ) in active_review_generations:
                    review_predicates = [
                        PRReview.id == review_id,
                        PRReview.status == "superseding",
                        PRReview.superseding_token == superseding_token,
                        (
                            PRReview.task_id.is_(None)
                            if old_task_id is None
                            else PRReview.task_id == old_task_id
                        ),
                    ]
                    superseded = await db.execute(
                        sa_update(PRReview)
                        .where(*review_predicates)
                        .values(
                            status="superseded",
                            completed_at=datetime.utcnow(),
                            superseding_snapshot=None,
                            superseding_token=None,
                            superseding_started_at=None,
                        )
                    )
                    if not superseded.rowcount:
                        await db.rollback()
                        raise HTTPException(
                            409,
                            "Previous PR review changed while it was being "
                            "stopped; durable replacement recovery will retry",
                        )
                    if old_task_id is not None:
                        logger.info(
                            "Safely stopped task %d (superseded PR review)",
                            old_task_id,
                        )

                await db.execute(
                    sa_update(PRReviewerRun)
                    .where(
                        PRReviewerRun.pr_review_id.in_(expected_ids),
                        PRReviewerRun.status.in_(
                            ("pending", "reviewing", "passed", "changes_required")
                        ),
                    )
                    .values(
                        status="superseded",
                        completed_at=datetime.utcnow(),
                    )
                )

                # Termination commits/expirations invalidate the repo ORM
                # identity. Keep supersede writes uncommitted and let
                # replacement creation commit both review generations.
                try:
                    if input_rejection is not None:
                        from backend.services.pr_review_service import (
                            create_pr_review_input_rejection,
                        )

                        review = await create_pr_review_input_rejection(
                            db,
                            current_repo,
                            replacement_data,
                            error=input_rejection,
                        )
                    else:
                        if prepared_context is None:
                            raise HTTPException(
                                500,
                                "PR review input capture is incomplete",
                            )
                        review = await _create_pr_review_task_or_422(
                            db,
                            current_repo,
                            replacement_data,
                            prepared_context=prepared_context,
                            prepared_ci_evidence=locked_ci_evidence,
                            allow_remote_ci=False,
                        )
                except IntegrityError as exc:
                    await db.rollback()
                    raise HTTPException(
                        409,
                        "Another synchronize created the replacement snapshot; "
                        "durable recovery will reconcile the old generation",
                    ) from exc
                if input_rejection is not None:
                    return _pr_review_input_rejection_response(review)
                return {"status": "accepted", "review_id": review.id}

    # Opened deliveries do not replace another live generation.
    active_result = await db.execute(
        select(PRReview).where(
            PRReview.repo_id == repo.id,
            PRReview.pr_number == pr_number,
            PRReview.status.in_(
                ["pending", "waiting_ci", "reviewing", "publishing", "superseding"]
            ),
        )
    )
    active_reviews = active_result.scalars().all()
    if active_reviews:
        if reopened_delivery:
            raise HTTPException(
                503,
                "PR reopen/ready event is waiting for the prior review generation",
            )
        return {"status": "ignored", "reason": "review already in progress"}
    if action == "opened" and not reopened_delivery:
        # Also skip if a completed review already exists for this PR
        completed_result = await db.execute(
            select(func.count()).select_from(PRReview).where(
                PRReview.repo_id == repo.id,
                PRReview.pr_number == pr_number,
                PRReview.status.in_(["approved", "merged", "commented"]),
            )
        )
        if completed_result.scalar():
            return {"status": "ignored", "reason": "PR already reviewed"}

    # Import and call service
    from backend.services.pr_review_service import (
        verify_pr_review_snapshot_current,
    )

    review_data = {
        "number": pr_number,
        "base_ref": base_branch,
        "base_sha": base_sha,
        "head_sha": head_sha,
        "delivery_id": delivery_id,
        "title": pr_title,
        "author": pr_author,
        "url": pr_url,
        "head_repo_full_name": head_repo_full_name,
        "head_branch": head_branch,
    }
    observed_reopen_run_generation = (
        await _pr_monitor_run_generation(
            db,
            repo_id=repo_id,
            pr_number=pr_number,
        )
        if reopened_delivery
        else None
    )
    # Active/completed checks above are short DB reads; do not retain their
    # connection while immutable GitHub input is fetched.
    await db.rollback()
    prepared_context, input_rejection = await _capture_pr_review_context_rejection(
        repo,
        review_data,
    )
    await verify_pr_review_snapshot_current(repo, review_data)
    prepared_ci_evidence = (
        await _capture_webhook_ci_evidence(
            repo,
            head_sha=head_sha,
        )
        if input_rejection is None
        else None
    )
    async with _pr_repo_write_lock(repo_id):
        db.expire_all()
        locked_repo = (
            await db.execute(
                select(MonitoredRepo)
                .where(MonitoredRepo.id == repo_id)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if locked_repo is None or not locked_repo.enabled:
            await db.rollback()
            return {
                "status": "ignored",
                "reason": "repository not monitored or disabled",
            }
        _require_current_webhook_signature(
            locked_repo,
            body=body,
            signature_header=signature_header,
        )
        policy_rejection = _webhook_policy_rejection(
            locked_repo,
            base_branch=base_branch,
            pr_author=pr_author,
            publisher_actor=publisher_actor,
        )
        if policy_rejection is not None:
            await db.rollback()
            return {"status": "ignored", "reason": policy_rejection}
        if prepared_context is None:
            await db.rollback()
            raise HTTPException(
                503,
                "PR review input could not be revalidated against the current "
                "repository policy",
            )
        input_rejection = _capture_pr_review_preflight_rejection(
            locked_repo,
            review_data,
            prepared_context=prepared_context,
        )
        locked_ci_evidence = (
            _require_locked_ci_evidence(
                locked_repo,
                head_sha=head_sha,
                evidence=prepared_ci_evidence,
            )
            if input_rejection is None
            else None
        )
        processed_review = await _find_processed_review(
            db,
            repo_id,
            pr_number,
            base_branch,
            base_sha,
            head_sha,
            delivery_id,
        )
        if processed_review is not None and (
            not reopened_delivery
            or (delivery_id and processed_review.delivery_id == delivery_id)
            or processed_review.rerun_idempotency_key == reopened_idempotency_key
        ):
            duplicate_response = (
                _pr_review_input_rejection_response(processed_review)
                if _is_pr_review_input_rejection(processed_review)
                else _duplicate_review_response(processed_review, delivery_id)
            )
            await db.rollback()
            return duplicate_response
        active_now = await db.execute(
            select(PRReview.id)
            .where(
                PRReview.repo_id == repo_id,
                PRReview.pr_number == pr_number,
                PRReview.status.in_(
                    ("pending", "waiting_ci", "reviewing", "publishing", "superseding")
                ),
            )
            .limit(1)
        )
        if active_now.scalar_one_or_none() is not None:
            await db.rollback()
            if reopened_delivery:
                raise HTTPException(
                    503,
                    "PR reopen/ready event is waiting for the prior review generation",
                )
            return {
                "status": "ignored",
                "reason": "review already in progress",
            }
        completed_now = await db.execute(
            select(PRReview.id)
            .where(
                PRReview.repo_id == repo_id,
                PRReview.pr_number == pr_number,
                PRReview.status.in_(("approved", "merged", "commented")),
            )
            .limit(1)
        )
        if completed_now.scalar_one_or_none() is not None and not reopened_delivery:
            await db.rollback()
            return {"status": "ignored", "reason": "PR already reviewed"}
        if reopened_delivery:
            lifecycle = (await db.execute(
                select(PRMonitorRun)
                .where(
                    PRMonitorRun.repo_id == repo_id,
                    PRMonitorRun.pr_number == pr_number,
                )
                .with_for_update()
            )).scalar_one_or_none()
            if (
                _pr_monitor_run_generation_value(lifecycle)
                != observed_reopen_run_generation
            ):
                await db.rollback()
                raise HTTPException(
                    409,
                    "A newer PR lifecycle generation changed while this "
                    "reopen event performed remote verification; retry the "
                    "signed delivery",
                )
            if lifecycle is not None:
                # Reopen readiness must be proven before a Reviewer Task is
                # staged.  attach_review_to_run repeats the same check as a
                # defense for direct/internal callers, but discovering an
                # active effect only after Task creation makes the webhook's
                # transaction much harder to reason about and recover.
                await _require_legacy_pr_effect_allowed(
                    db,
                    action="reactivated",
                    monitor_run=lifecycle,
                    allow_terminal_intent=True,
                )
                from backend.services.pr_monitor_loop import (
                    assert_terminal_reactivation_ready,
                )
                from backend.services.pr_review_service import (
                    PRReviewLifecycleConflict,
                )

                try:
                    await assert_terminal_reactivation_ready(
                        db,
                        run=lifecycle,
                    )
                except PRReviewLifecycleConflict as exc:
                    await db.rollback()
                    raise HTTPException(503, str(exc)) from exc
            prior_review = (
                await db.get(PRReview, lifecycle.current_review_id)
                if lifecycle is not None and lifecycle.current_review_id is not None
                else processed_review
            )
            latest_attempt = await db.scalar(
                select(func.max(PRReview.attempt)).where(
                    PRReview.repo_id == repo_id,
                    PRReview.pr_number == pr_number,
                    PRReview.base_ref == base_branch,
                    PRReview.base_sha == base_sha,
                    PRReview.head_sha == head_sha,
                )
            )
            review_data.update({
                "_review_attempt": int(latest_attempt or 0) + 1,
                "_rerun_of_review_id": (
                    prior_review.id if prior_review is not None else None
                ),
                "_rerun_idempotency_key": reopened_idempotency_key,
            })
        try:
            if input_rejection is not None:
                from backend.services.pr_review_service import (
                    create_pr_review_input_rejection,
                )

                review = await create_pr_review_input_rejection(
                    db,
                    locked_repo,
                    review_data,
                    error=input_rejection,
                    allow_terminal_reactivation=reopened_delivery,
                )
            else:
                if prepared_context is None:
                    raise HTTPException(500, "PR review input capture is incomplete")
                review = await _create_pr_review_task_or_422(
                    db,
                    locked_repo,
                    review_data,
                    prepared_context=prepared_context,
                    prepared_ci_evidence=locked_ci_evidence,
                    allow_remote_ci=False,
                    allow_terminal_reactivation=reopened_delivery,
                )
        except IntegrityError:
            # A concurrent Manager may have won the same database uniqueness
            # key despite the process-local companion lock.
            await db.rollback()
            processed_review = await _find_processed_review(
                db,
                repo_id,
                pr_number,
                base_branch,
                base_sha,
                head_sha,
                delivery_id,
            )
            if processed_review:
                logger.info(
                    "Ignored concurrently duplicated PR webhook for %s#%d at "
                    "%s...%s (review %d)",
                    repo_name,
                    pr_number,
                    base_sha,
                    head_sha,
                    processed_review.id,
                )
                if _is_pr_review_input_rejection(processed_review):
                    return _pr_review_input_rejection_response(processed_review)
                return _duplicate_review_response(processed_review, delivery_id)
            raise

    if input_rejection is not None:
        return _pr_review_input_rejection_response(review)
    return {"status": "accepted", "review_id": review.id}
