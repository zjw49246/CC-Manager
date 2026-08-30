"""Transactional state boundary for autonomous Delivery Runs.

The controller owns orchestration, but every durable state mutation funnels
through this module.  Network, Git and agent calls are deliberately excluded
from these transactions.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
import secrets
from typing import Any
from urllib.parse import urlsplit

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.models.delivery import (
    DeliveryCycle,
    DeliveryRun,
    DeliveryTransition,
)
from backend.models.pr_monitor import MonitoredRepo
from backend.models.project import Project
from backend.models.project_todo import ProjectTodo
from backend.schemas.pr_monitor import required_checks_support_direct_auto_merge
from backend.services.delivery_reducer import (
    DeliveryReducerEvent,
    DeliveryState,
    reduce_delivery_state,
)
from backend.services.task_creation import (
    prepare_task_create_values,
    resolve_task_runtime_defaults,
    stage_task_record,
    validate_task_service_tier_configuration,
)


class DeliveryError(RuntimeError):
    """Stable service error base class."""


class DeliveryNotFoundError(DeliveryError):
    pass


class DeliveryConflictError(DeliveryError):
    pass


class DeliveryValidationError(DeliveryError):
    pass


class DeliveryUnsupportedScopeError(DeliveryError):
    pass


class DeliveryUnavailableError(DeliveryError):
    pass


_BRANCH_COMPONENT_RE = re.compile(r"[^a-z0-9._-]+")
_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
_GITHUB_REPO_RE = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+\Z")
_DELIVERY_PROVIDERS = frozenset({"claude", "codex"})
_SCP_GITHUB_RE = re.compile(
    r"(?:[^/@:]+@)?github\.com:(?P<repo>[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+?)(?:\.git)?/?\Z",
    re.IGNORECASE,
)


def _configured_delivery_providers() -> frozenset[str]:
    """Return the Delivery routes enabled for this deployment."""

    configured = {
        item.strip().lower()
        for item in (settings.provider_options or "").split(",")
        if item.strip().lower() in _DELIVERY_PROVIDERS
    }
    # Preserve the historical dual-provider behavior for empty/invalid legacy
    # values, matching Plan execution and the provider catalog.
    return frozenset(configured or _DELIVERY_PROVIDERS)


def _github_repo_from_url(value: object) -> str | None:
    """Resolve only unambiguous GitHub remotes used by Delivery admission."""

    if not isinstance(value, str) or not value or any(ch in value for ch in "\r\n\x00"):
        return None
    scp_match = _SCP_GITHUB_RE.fullmatch(value)
    if scp_match is not None:
        return scp_match.group("repo")
    try:
        parsed = urlsplit(value)
    except ValueError:
        return None
    if parsed.scheme.lower() not in {"https", "http", "ssh"}:
        return None
    if (parsed.hostname or "").lower() != "github.com":
        return None
    path = parsed.path.strip("/")
    if path.endswith(".git"):
        path = path[:-4]
    return path if _GITHUB_REPO_RE.fullmatch(path) else None


def canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise DeliveryValidationError("Delivery payload must be finite JSON") from exc


def value_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def text_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _non_empty_text(value: object, *, field: str, limit: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeliveryValidationError(f"{field} is required")
    normalized = value.strip()
    if len(normalized) > limit:
        raise DeliveryValidationError(f"{field} exceeds {limit} characters")
    return normalized


def _branch_component(title: str) -> str:
    component = _BRANCH_COMPONENT_RE.sub("-", title.lower()).strip("-._")
    return (component[:48].rstrip("-._") or "task")


@dataclass(frozen=True, slots=True)
class DeliveryCreateSpec:
    idempotency_key: str
    project_id: int
    monitored_repo_id: int
    title: str
    requirements: str
    created_by: int | None = None
    source_todo_id: int | None = None
    base_branch: str | None = None
    provider: str = "codex"
    model: str | None = None
    codex_service_tier: str = "default"
    effort_level: str | None = None
    timeout_hours: float | None = None
    max_cycles: int = 10
    max_no_progress: int = 3
    # ``None`` preserves the legacy explicit-Monitor API behavior. The
    # one-message entry point always sends a concrete per-Run choice.
    auto_merge: bool | None = None
    strict_branch_protection: bool = False
    # Direct service callers predate the Browser gate and retain the legacy
    # flow unless they opt in. Public API schemas explicitly default to auto.
    frontend_review: str = "off"


def _admission_scope(created_by: int | None) -> str:
    if created_by is None:
        return "system"
    if isinstance(created_by, bool) or created_by <= 0:
        raise DeliveryValidationError("created_by must be a positive user id")
    return f"user:{created_by}"


def _optional_text(value: object, *, field: str, limit: int) -> str | None:
    if value is None:
        return None
    return _non_empty_text(value, field=field, limit=limit)


def _admission_request(
    spec: DeliveryCreateSpec,
    *,
    title: str,
    requirements: str,
    provider: str,
    model: str | None,
    effort_level: str | None,
) -> dict[str, object]:
    """Canonical caller intent used only for admission replay detection.

    Runtime defaults are deliberately *not* substituted here.  Retrying the
    same request after an administrator changes a deployment default must
    still return the already-frozen Run rather than become a false conflict.
    The resolved runtime tuple is stored separately in ``policy_snapshot``.
    """

    request = {
        "schema_version": 2,
        "project_id": spec.project_id,
        "monitored_repo_id": spec.monitored_repo_id,
        "title": title,
        "requirements": requirements,
        "source_todo_id": spec.source_todo_id,
        "base_branch": (
            spec.base_branch.strip()
            if isinstance(spec.base_branch, str)
            else spec.base_branch
        ),
        "provider": provider,
        "model": model,
        "codex_service_tier": spec.codex_service_tier,
        "effort_level": effort_level,
        "timeout_hours": spec.timeout_hours,
        "max_cycles": spec.max_cycles,
        "max_no_progress": spec.max_no_progress,
        "strict_branch_protection": spec.strict_branch_protection,
    }
    if spec.auto_merge is not None:
        request["auto_merge"] = spec.auto_merge
    request["frontend_review"] = spec.frontend_review
    return request


async def _idempotent_admission(
    db: AsyncSession,
    *,
    admission_scope: str,
    project_id: int,
    idempotency_key: str,
    request_hash: str,
    pre_strict_request_hash: str,
    legacy_request_hash: str,
) -> DeliveryRun | None:
    existing = (
        await db.execute(
            select(DeliveryRun)
            .where(
                DeliveryRun.admission_scope == admission_scope,
                DeliveryRun.project_id == project_id,
                DeliveryRun.idempotency_key == idempotency_key,
            )
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if existing is None:
        return None
    policy = (
        existing.policy_snapshot
        if isinstance(existing.policy_snapshot, dict)
        else {}
    )
    compatible_replay = (
        "strict_branch_protection" not in policy
        and existing.request_hash
        in {pre_strict_request_hash, legacy_request_hash}
    )
    if existing.request_hash != request_hash and not compatible_replay:
        raise DeliveryConflictError(
            "Idempotency key is already bound to a different Delivery request"
        )
    return existing


def state_from_run(run: DeliveryRun) -> DeliveryState:
    return DeliveryState(
        phase=run.phase,
        activity=run.activity,
        outcome=run.outcome,
        wait_reason=run.wait_reason,
        paused_from_activity=run.paused_from_activity,
        pause_reason=run.pause_reason,
        error_code=run.error_code,
        error_message=run.error_message,
        state_version=run.state_version,
    )


def _assign_state(run: DeliveryRun, state: DeliveryState) -> None:
    run.phase = state.phase
    run.activity = state.activity
    run.outcome = state.outcome
    run.wait_reason = state.wait_reason
    run.paused_from_activity = state.paused_from_activity
    run.pause_reason = state.pause_reason
    run.error_code = state.error_code
    run.error_message = state.error_message
    run.state_version = state.state_version
    run.updated_at = datetime.utcnow()
    run.completed_at = (
        datetime.utcnow() if state.activity == "terminal" else None
    )


async def lock_run(db: AsyncSession, run_id: int) -> DeliveryRun:
    """Freshly lock one Run after a portable write barrier."""

    guarded = await db.execute(
        update(DeliveryRun)
        .where(DeliveryRun.id == run_id)
        .values(state_version=DeliveryRun.state_version)
    )
    if guarded.rowcount != 1:
        raise DeliveryNotFoundError("Delivery Run not found")
    run = (
        await db.execute(
            select(DeliveryRun)
            .where(DeliveryRun.id == run_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if run is None:
        raise DeliveryNotFoundError("Delivery Run not found")
    return run


async def lock_current_cycle(
    db: AsyncSession,
    run: DeliveryRun,
) -> DeliveryCycle:
    if run.current_cycle_id is None:
        raise DeliveryConflictError("Delivery Run has no current cycle")
    cycle = (
        await db.execute(
            select(DeliveryCycle)
            .where(
                DeliveryCycle.id == run.current_cycle_id,
                DeliveryCycle.run_id == run.id,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if cycle is None:
        raise DeliveryConflictError("Delivery Run current cycle is missing")
    return cycle


async def apply_run_event(
    db: AsyncSession,
    *,
    run: DeliveryRun,
    event: DeliveryReducerEvent,
    actor_kind: str,
    actor_id: str | None = None,
    event_id: int | None = None,
    metadata: dict | None = None,
) -> tuple[DeliveryState, tuple[str, ...]]:
    """Reduce and stage one append-only Run transition without committing."""

    before = state_from_run(run)
    reduction = reduce_delivery_state(
        before,
        event,
        expected_version=run.state_version,
    )
    _assign_state(run, reduction.state)
    db.add(
        DeliveryTransition(
            run_id=run.id,
            state_version=reduction.state.state_version,
            event_id=event_id,
            cause=event.kind,
            actor_kind=actor_kind,
            actor_id=actor_id,
            before_state=asdict(before),
            after_state=asdict(reduction.state),
            metadata_=metadata,
        )
    )
    await db.flush()
    return reduction.state, reduction.effects


def complete_cycle(cycle: DeliveryCycle, *, status: str = "completed") -> None:
    if status not in {"completed", "failed", "cancelled", "superseded"}:
        raise DeliveryValidationError("Invalid terminal Delivery cycle status")
    cycle.status = status
    cycle.active_run_id = None
    cycle.state_version += 1
    cycle.updated_at = datetime.utcnow()
    cycle.completed_at = datetime.utcnow()


async def start_next_cycle(
    db: AsyncSession,
    *,
    run: DeliveryRun,
    trigger_kind: str,
    trigger_payload: dict,
    trigger_pr_review_id: int | None = None,
    trigger_pr_repair_wake_id: int | None = None,
) -> DeliveryCycle:
    if run.cycle_count >= run.max_cycles:
        raise DeliveryConflictError("Delivery Run cycle budget is exhausted")
    existing_active = await db.scalar(
        select(DeliveryCycle.id)
        .where(DeliveryCycle.active_run_id == run.id)
        .limit(1)
    )
    if existing_active is not None:
        raise DeliveryConflictError(
            f"Delivery Run already has active cycle {existing_active}"
        )
    frozen_trigger = json.loads(canonical_json(trigger_payload))
    cycle = DeliveryCycle(
        run_id=run.id,
        cycle_number=run.cycle_count + 1,
        active_run_id=run.id,
        status="planning",
        state_version=1,
        trigger_kind=_non_empty_text(
            trigger_kind,
            field="trigger_kind",
            limit=64,
        ),
        trigger_payload=frozen_trigger,
        trigger_hash=value_hash(frozen_trigger),
        trigger_pr_review_id=trigger_pr_review_id,
        trigger_pr_repair_wake_id=trigger_pr_repair_wake_id,
        base_sha=run.base_sha,
        start_head_sha=run.head_sha,
    )
    db.add(cycle)
    await db.flush()
    run.current_cycle_id = cycle.id
    run.cycle_count = cycle.cycle_number
    run.updated_at = datetime.utcnow()
    return cycle


async def create_delivery_run(
    db: AsyncSession,
    spec: DeliveryCreateSpec,
    *,
    admission_disabled_reason: str | None = None,
) -> DeliveryRun:
    """Atomically create Run, resting Developer Task and first cycle."""

    idempotency_key = _non_empty_text(
        spec.idempotency_key,
        field="idempotency_key",
        limit=128,
    )
    title = _non_empty_text(spec.title, field="title", limit=200)
    requirements = _non_empty_text(
        spec.requirements,
        field="requirements",
        limit=200_000,
    )
    requested_provider = _non_empty_text(
        spec.provider,
        field="provider",
        limit=20,
    ).lower()
    requested_model = _optional_text(spec.model, field="model", limit=100)
    requested_effort = _optional_text(
        spec.effort_level,
        field="effort_level",
        limit=20,
    )
    admission_scope = _admission_scope(spec.created_by)
    admission_request = _admission_request(
        spec,
        title=title,
        requirements=requirements,
        provider=requested_provider,
        model=requested_model,
        effort_level=requested_effort,
    )
    request_hash = value_hash(admission_request)
    pre_strict_request = dict(admission_request)
    pre_strict_request.pop("strict_branch_protection", None)
    pre_strict_request_hash = value_hash(pre_strict_request)
    legacy_request = dict(pre_strict_request)
    legacy_request["schema_version"] = 1
    legacy_request.pop("auto_merge", None)
    legacy_request.pop("frontend_review", None)
    legacy_request_hash = value_hash(legacy_request)

    # Global topology lock order is MonitoredRepo -> Project.  PR Monitor
    # mutation paths already hold the repository row before reauthorizing
    # through its Project, so Delivery admission must take the same order or
    # PostgreSQL/MySQL can deadlock on concurrent create/update operations.
    # The no-op writes are portable writer fences because SQLite ignores
    # SELECT ... FOR UPDATE.
    guarded_repo = await db.execute(
        update(MonitoredRepo)
        .where(MonitoredRepo.id == spec.monitored_repo_id)
        .values(updated_at=MonitoredRepo.updated_at)
        .execution_options(synchronize_session=False)
    )
    if guarded_repo.rowcount != 1:
        raise DeliveryNotFoundError("Monitored repository not found")
    repo = (
        await db.execute(
            select(MonitoredRepo)
            .where(MonitoredRepo.id == spec.monitored_repo_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if repo is None:
        raise DeliveryNotFoundError("Monitored repository not found")

    await db.execute(
        update(Project)
        .where(Project.id == spec.project_id)
        .values(id=Project.id)
    )
    project = (
        await db.execute(
            select(Project)
            .where(Project.id == spec.project_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if project is None:
        raise DeliveryNotFoundError("Project not found")

    existing = await _idempotent_admission(
        db,
        admission_scope=admission_scope,
        project_id=project.id,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        pre_strict_request_hash=pre_strict_request_hash,
        legacy_request_hash=legacy_request_hash,
    )
    if existing is not None:
        await db.commit()
        return existing

    if admission_disabled_reason is not None:
        raise DeliveryUnavailableError(admission_disabled_reason)

    if requested_provider not in _DELIVERY_PROVIDERS:
        raise DeliveryUnsupportedScopeError(
            "Delivery Loop supports the Claude and Codex providers only"
        )
    configured_providers = _configured_delivery_providers()
    if requested_provider not in configured_providers:
        raise DeliveryValidationError(
            f"Delivery provider '{requested_provider}' is not enabled by "
            "provider_options"
        )
    try:
        resolved_provider, resolved_model, resolved_effort = (
            resolve_task_runtime_defaults(
                provider=requested_provider,
                model=(
                    None if requested_model in {None, "default"} else requested_model
                ),
                effort_level=requested_effort,
            )
        )
        validate_task_service_tier_configuration(
            provider=resolved_provider,
            model=resolved_model,
            codex_service_tier=spec.codex_service_tier,
            mode="delivery_loop",
            goal_evaluator_model=None,
        )
    except ValueError as exc:
        raise DeliveryValidationError(str(exc)) from exc

    if isinstance(spec.max_cycles, bool) or not 1 <= spec.max_cycles <= 100:
        raise DeliveryValidationError("max_cycles must be between 1 and 100")
    if (
        isinstance(spec.max_no_progress, bool)
        or not 1 <= spec.max_no_progress <= 20
    ):
        raise DeliveryValidationError(
            "max_no_progress must be between 1 and 20"
        )
    if type(spec.strict_branch_protection) is not bool:
        raise DeliveryValidationError(
            "strict_branch_protection must be a boolean"
        )

    # Admission now holds both topology rows in the same order as PR Monitor
    # mutation paths. Either the mutation wins and this validation observes
    # its new state, or this Run commits first and the mutation returns 409.
    repo_provider = (repo.provider or "").strip().lower()
    if repo_provider not in _DELIVERY_PROVIDERS:
        raise DeliveryValidationError(
            "PR Monitor provider must be 'claude' or 'codex'"
        )
    if repo_provider not in configured_providers:
        raise DeliveryValidationError(
            f"PR Monitor provider '{repo_provider}' is not enabled by "
            "provider_options"
        )
    if project.worker_id is not None or repo.worker_id is not None:
        raise DeliveryUnsupportedScopeError(
            "Delivery Loop V1 supports local projects only"
        )
    if not project.local_path or not project.has_remote:
        raise DeliveryValidationError(
            "Delivery Loop requires a local project with a Git remote"
        )
    if not repo.enabled or repo.project_id != project.id:
        raise DeliveryValidationError(
            "Monitored repository must be enabled and bound to the project"
        )
    configured_repo = _github_repo_from_url(project.git_url)
    if (
        configured_repo is None
        or _GITHUB_REPO_RE.fullmatch(repo.repo_full_name or "") is None
        or configured_repo.lower() != repo.repo_full_name.lower()
    ):
        raise DeliveryValidationError(
            "Project GitHub remote must exactly match the monitored repository"
        )
    if (repo.merge_queue_mode or "manual") != "manual":
        raise DeliveryValidationError(
            "Delivery Loop requires Merge Queue disabled"
        )
    if (repo.review_mode or "single") != "panel":
        raise DeliveryValidationError(
            "Delivery Loop requires PR Monitor panel review"
        )
    if bool(repo.wait_for_ci) != bool(repo.required_checks):
        raise DeliveryValidationError(
            "PR Monitor exact-head CI policy is incomplete"
        )
    if spec.auto_merge is not None and type(spec.auto_merge) is not bool:
        raise DeliveryValidationError("auto_merge must be a boolean")
    auto_merge = (
        bool(repo.auto_merge)
        if spec.auto_merge is None
        else spec.auto_merge
    )
    if auto_merge and (
        not repo.wait_for_ci
        or not repo.required_checks
        or not required_checks_support_direct_auto_merge(repo.required_checks)
    ):
        raise DeliveryValidationError(
            "Delivery auto-merge requires app-bound check_run required CI "
            "policies discovered from branch protection"
        )
    frontend_review = _non_empty_text(
        spec.frontend_review,
        field="frontend_review",
        limit=16,
    ).lower()
    if frontend_review not in {"auto", "required", "off"}:
        raise DeliveryValidationError(
            "frontend_review must be 'auto', 'required', or 'off'"
        )
    if frontend_review == "required":
        if not isinstance(settings.auth_token, str) or not settings.auth_token.strip():
            raise DeliveryUnavailableError(
                "Required frontend review needs a configured AUTH_TOKEN"
            )
        if project.preview_config is None:
            raise DeliveryValidationError(
                "Required frontend review needs a confirmed Project Preview configuration"
            )
        try:
            from backend.services.workspace_review import (
                WorkspaceReviewError,
                validate_preview_profiles,
            )

            preview_workspace = Path(project.local_path).resolve(strict=True)
            if not (preview_workspace / ".git").exists():
                raise WorkspaceReviewError(
                    "Project path is not a Git checkout"
                )
            validate_preview_profiles(project.preview_config, preview_workspace)
        except (OSError, TypeError, ValueError, WorkspaceReviewError) as exc:
            raise DeliveryValidationError(
                f"Required frontend review Preview configuration is invalid: {exc}"
            ) from exc
    source_todo: ProjectTodo | None = None
    if spec.source_todo_id is not None:
        todo = (
            await db.execute(
                select(ProjectTodo)
                .where(ProjectTodo.id == spec.source_todo_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
        if todo is None or todo.project_id != project.id:
            raise DeliveryValidationError(
                "source_todo_id does not belong to the project"
            )
        source_todo = todo
        existing_todo_owner = await db.scalar(
            select(DeliveryRun.id)
            .where(DeliveryRun.source_todo_id == todo.id)
            .limit(1)
        )
        if existing_todo_owner is not None:
            raise DeliveryConflictError(
                f"Source Todo is already owned by Delivery Run "
                f"{existing_todo_owner}"
            )

    base_branch = _non_empty_text(
        spec.base_branch or project.default_branch or repo.default_branch,
        field="base_branch",
        limit=200,
    )
    if base_branch != repo.default_branch:
        raise DeliveryValidationError(
            "Delivery base branch must match the PR Monitor default branch"
        )
    policy = {
        "schema_version": 2,
        "terminal": "merged" if auto_merge else "ready_to_merge",
        "auto_merge": auto_merge,
        "strict_branch_protection": spec.strict_branch_protection,
        "max_cycles": spec.max_cycles,
        "max_no_progress": spec.max_no_progress,
        "provider": resolved_provider,
        "model": resolved_model,
        "codex_service_tier": spec.codex_service_tier,
        "effort_level": resolved_effort,
        "timeout_hours": spec.timeout_hours,
        "frontend_review": {
            "mode": frontend_review,
            "profile": "standard",
            "allow_actions": True,
        },
        "pr_monitor": {
            "repo_id": repo.id,
            "repo_full_name": repo.repo_full_name,
            "review_mode": repo.review_mode,
            "wait_for_ci": bool(repo.wait_for_ci),
            "required_checks": repo.required_checks,
        },
    }
    frozen_policy = json.loads(canonical_json(policy))
    provisional = f"ccm/delivery/pending-{secrets.token_hex(8)}"
    run = DeliveryRun(
        created_by=spec.created_by,
        admission_scope=admission_scope,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        project_id=project.id,
        monitored_repo_id=repo.id,
        source_todo_id=spec.source_todo_id,
        title=title,
        requirements=requirements,
        requirements_hash=text_hash(requirements),
        policy_snapshot=frozen_policy,
        policy_hash=value_hash(frozen_policy),
        base_branch=base_branch,
        delivery_branch=provisional,
        phase="planning",
        activity="ready",
        state_version=1,
        max_cycles=spec.max_cycles,
        max_no_progress=spec.max_no_progress,
        next_reconcile_at=datetime.utcnow(),
    )
    db.add(run)
    await db.flush()
    run.delivery_branch = (
        f"ccm/delivery/{run.id}-{_branch_component(title)}"
    )

    task_values = prepare_task_create_values(
        {
            "title": title,
            "description": requirements,
            "status": "delivery_waiting",
            "priority": 0,
            "project_id": project.id,
            "target_repo": project.local_path,
            "target_branch": base_branch,
            "result_branch": run.delivery_branch,
            "mode": "delivery_loop",
            "delivery_run_id": run.id,
            "delivery_role": "developer",
            "worker_id": None,
            "created_by": spec.created_by,
            "execution_user_id": None,
            "execution_user_role": "member",
            "execution_mode": "sandbox",
            "execution_principal_kind": "system",
            "provider": resolved_provider,
            "model": resolved_model,
            "codex_service_tier": spec.codex_service_tier,
            "effort_level": resolved_effort,
            "timeout_hours": spec.timeout_hours,
            "metadata_": {
                "delivery_policy_hash": run.policy_hash,
                "delivery_requirements_hash": run.requirements_hash,
            },
        }
    )
    try:
        task = await stage_task_record(db, **task_values)
    except ValueError as exc:
        raise DeliveryValidationError(str(exc)) from exc
    run.developer_task_id = task.id
    if source_todo is not None:
        # Provenance and completion are part of the same commit as the Run and
        # its resting Developer Task.  A UI crash after POST therefore cannot
        # leave a spawned Delivery task detached from its source Todo.
        # Claim with one conditional write rather than overwriting the loaded
        # object.  Two concurrent Run requests for the same Todo must not both
        # commit and leave ``created_task_id`` pointing at only the later Task.
        claimed = await db.execute(
            update(ProjectTodo)
            .where(
                ProjectTodo.id == source_todo.id,
                ProjectTodo.project_id == project.id,
                ProjectTodo.status == "open",
                ProjectTodo.created_task_id.is_(None),
                ProjectTodo.task_request_hash.is_(None),
            )
            .values(
                status="done",
                created_task_id=task.id,
                updated_at=datetime.utcnow(),
            )
        )
        if claimed.rowcount != 1:
            raise DeliveryConflictError(
                "Source Todo is not open or has already been claimed by a Task"
            )
    await start_next_cycle(
        db,
        run=run,
        trigger_kind="initial_request",
        trigger_payload={
            "requirements_hash": run.requirements_hash,
            "policy_hash": run.policy_hash,
        },
    )
    db.add(
        DeliveryTransition(
            run_id=run.id,
            state_version=1,
            cause="created",
            actor_kind="user" if spec.created_by is not None else "system",
            actor_id=str(spec.created_by) if spec.created_by is not None else None,
            before_state={},
            after_state=asdict(state_from_run(run)),
            metadata_={"developer_task_id": task.id},
        )
    )
    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise DeliveryConflictError(
            "A Delivery Run already owns this branch, Task, or active cycle"
        ) from exc
    await db.refresh(run)
    return run


async def get_delivery_run(db: AsyncSession, run_id: int) -> DeliveryRun:
    run = await db.get(DeliveryRun, run_id, populate_existing=True)
    if run is None:
        raise DeliveryNotFoundError("Delivery Run not found")
    return run


async def list_delivery_runs(
    db: AsyncSession,
    *,
    project_id: int | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[DeliveryRun]:
    statement = select(DeliveryRun).order_by(
        DeliveryRun.created_at.desc(),
        DeliveryRun.id.desc(),
    )
    if project_id is not None:
        statement = statement.where(DeliveryRun.project_id == project_id)
    return list(
        (
            await db.execute(statement.limit(min(max(limit, 1), 200)).offset(max(offset, 0)))
        ).scalars()
    )


def validate_sha256(value: str, *, field: str) -> str:
    normalized = value.strip().lower()
    if _HASH_RE.fullmatch(normalized) is None:
        raise DeliveryValidationError(f"{field} must be a SHA-256 digest")
    return normalized
