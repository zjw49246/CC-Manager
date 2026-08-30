"""Safe, idempotent PR Monitor bootstrap for Delivery projects.

The first-party Delivery entry point should not require operators to duplicate
Project identity in PR Monitor.  This module derives the immutable GitHub
identity from the Project and creates the conservative Monitor policy required
by Delivery admission.  Panel review is always enabled.  Exact CI identities
are added only when GitHub declares them as required; unresolved declared
requirements fail closed instead of asking the user to fill Monitor fields.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
import logging
import secrets
from urllib.parse import quote

from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.models.pr_monitor import MonitoredRepo
from backend.models.project import Project
from backend.services.delivery_service import (
    _configured_delivery_providers,
    _github_repo_from_url,
)
from backend.services.pr_review_actions import (
    FindingActionConflict,
    lock_pr_repo_action_boundary,
)
from backend.services.pr_review_service import GhError, _gh_api_json


logger = logging.getLogger(__name__)


TRUSTED_OBSERVED_CI_POLICY = {
    "kind": "check_run",
    "name": "Triggered checks on exact PR head",
    "app_slug": "ccm-trusted",
}


class DeliverySetupError(RuntimeError):
    """Stable project-to-Monitor bootstrap error."""


class DeliverySetupNotFoundError(DeliverySetupError):
    pass


class DeliverySetupValidationError(DeliverySetupError):
    pass


class DeliverySetupConflictError(DeliverySetupError):
    pass


class DeliverySetupPermissionError(DeliverySetupError):
    pass


class DeliverySetupUnavailableError(DeliverySetupError):
    def __init__(
        self,
        message: str,
        *,
        repo_full_name: str | None = None,
        candidates: list[dict[str, str]] | None = None,
        code: str = "delivery_setup_unavailable",
    ) -> None:
        super().__init__(message)
        self.repo_full_name = repo_full_name
        self.candidates = candidates or []
        self.code = code


@dataclass(frozen=True, slots=True)
class DeliveryMonitorSetup:
    repo: MonitoredRepo
    created: bool
    discovery_source: str | None


@dataclass(frozen=True, slots=True)
class _ProjectIdentity:
    project_id: int
    repo_full_name: str
    default_branch: str
    git_url: str


def _public_policy(candidate: dict[str, object]) -> dict[str, str]:
    return {
        "kind": str(candidate["kind"]),
        "name": str(candidate["name"]),
        "app_slug": str(candidate["app_slug"]),
    }


def _default_provider() -> str:
    providers = _configured_delivery_providers()
    preferred = (settings.default_provider or "").strip().lower()
    if preferred in providers:
        return preferred
    if "codex" in providers:
        return "codex"
    return sorted(providers)[0]


def _project_identity(project: Project | None) -> _ProjectIdentity:
    if project is None:
        raise DeliverySetupNotFoundError("Project not found")
    if project.worker_id is not None:
        raise DeliverySetupValidationError(
            "Delivery Loop V1 only supports projects on this Manager"
        )
    if project.status != "ready":
        raise DeliverySetupValidationError(
            "Project import is not ready yet; wait for clone to complete"
        )
    if not project.has_remote or not project.local_path:
        raise DeliverySetupValidationError(
            "Delivery Loop requires a local project with a GitHub remote"
        )
    repo_full_name = _github_repo_from_url(project.git_url)
    if repo_full_name is None:
        raise DeliverySetupValidationError(
            "Delivery Loop automatic setup requires an unambiguous github.com remote"
        )
    if not isinstance(project.default_branch, str) or not project.default_branch:
        raise DeliverySetupValidationError("Project default branch is invalid")
    return _ProjectIdentity(
        project_id=project.id,
        # GitHub repository identity is case-insensitive. Canonicalizing new
        # rows also lets the database unique key serialize concurrent setup
        # attempts made through differently-cased remotes.
        repo_full_name=repo_full_name.lower(),
        default_branch=project.default_branch,
        git_url=project.git_url,
    )


def _monitor_problem(
    repo: MonitoredRepo,
    identity: _ProjectIdentity,
) -> str | None:
    provider = (repo.provider or "").strip().lower()
    if repo.project_id != identity.project_id or repo.worker_id is not None:
        return "the GitHub repository is already monitored by another Project"
    if repo.repo_full_name.lower() != identity.repo_full_name.lower():
        return "the PR Monitor repository identity does not match the Project"
    if not repo.enabled or repo.status != "active":
        return "the existing PR Monitor is disabled or inactive"
    if provider not in _configured_delivery_providers():
        return "the existing PR Monitor provider is not enabled"
    if repo.review_mode != "panel":
        return "the existing PR Monitor must use panel review"
    if bool(repo.wait_for_ci) != bool(repo.required_checks):
        return "the existing PR Monitor has an incomplete exact-head CI policy"
    if repo.merge_queue_mode != "manual":
        return "the existing PR Monitor must keep Merge Queue disabled"
    if repo.default_branch != identity.default_branch:
        return "the existing PR Monitor default branch does not match the Project"
    return None


async def _existing_monitor(
    db: AsyncSession,
    identity: _ProjectIdentity,
) -> MonitoredRepo | None:
    """Read a candidate without taking the repository topology lock.

    The post-discovery caller already holds Project while resolving a
    concurrent creator. Taking MonitoredRepo there would invert the global
    MonitoredRepo -> Project order used by PR Monitor mutations. A plain MVCC
    read is sufficient: monitor creation is serialized by the Project fence,
    and Quick Start subsequently reacquires both rows in canonical order.
    """

    statement = select(MonitoredRepo).where(
        func.lower(MonitoredRepo.repo_full_name) == identity.repo_full_name.lower()
    )
    rows = list((await db.execute(statement)).scalars())
    if len(rows) > 1:
        raise DeliverySetupConflictError(
            "Multiple PR Monitor rows differ only by GitHub repository casing"
        )
    return rows[0] if rows else None


async def _lock_project_identity(
    db: AsyncSession,
    project_id: int,
) -> _ProjectIdentity:
    """Fence Project identity after any required MonitoredRepo writer lock."""

    await db.execute(
        update(Project).where(Project.id == project_id).values(id=Project.id)
    )
    return _project_identity(
        await db.get(Project, project_id, populate_existing=True)
    )


async def _reconcile_existing_monitor(
    db: AsyncSession,
    *,
    repo_id: int,
    identity: _ProjectIdentity,
    policies: list[dict[str, str]],
    source: str,
    create_authorizer: Callable[[AsyncSession], Awaitable[None]] | None,
) -> DeliveryMonitorSetup:
    """Update an existing Monitor under the canonical topology lock order."""

    # A plain discovery read may already have started a transaction. Re-enter
    # on a clean transaction so the writer order is always MonitoredRepo ->
    # Project -> membership/User authority. Merely assigning ORM attributes
    # while Project is locked is not safe: flush would acquire the repository
    # row lock in the reverse order.
    await db.rollback()
    try:
        existing = await lock_pr_repo_action_boundary(db, repo_id)
    except FindingActionConflict as exc:
        await db.rollback()
        raise DeliverySetupConflictError(
            "The existing PR Monitor changed during Delivery setup; retry"
        ) from exc

    current = await _lock_project_identity(db, identity.project_id)
    if current != identity:
        await db.rollback()
        raise DeliverySetupConflictError(
            "Project GitHub identity changed during PR Monitor setup; retry"
        )
    problem = _monitor_problem(existing, current)
    if problem:
        await db.rollback()
        raise DeliverySetupConflictError(
            f"Cannot configure Delivery automatically because {problem}. "
            "An administrator must resolve the conflicting legacy binding."
        )

    # User-driven Quick Start can spend substantial time on GitHub discovery.
    # Revalidate its cached principal after both topology rows are fenced.
    if create_authorizer is not None:
        await create_authorizer(db)
    if policies:
        existing.wait_for_ci = True
        existing.required_checks = policies
    await db.commit()
    await db.refresh(existing)
    return DeliveryMonitorSetup(existing, False, source)


def _parse_ci_candidates(
    check_payload: object,
    status_payload: object,
) -> list[dict[str, object]]:
    if not isinstance(check_payload, dict) or not isinstance(status_payload, dict):
        raise DeliverySetupUnavailableError("GitHub CI discovery response is malformed")
    check_runs = check_payload.get("check_runs")
    total_count = check_payload.get("total_count")
    statuses = status_payload.get("statuses")
    if (
        not isinstance(check_runs, list)
        or type(total_count) is not int
        or total_count != len(check_runs)
        or total_count > 100
        or not isinstance(statuses, list)
        or len(statuses) >= 100
    ):
        raise DeliverySetupUnavailableError(
            "GitHub returned too many or malformed CI checks to configure safely"
        )

    newest: dict[tuple[str, str, str], dict[str, object]] = {}
    for item in check_runs:
        app = item.get("app") if isinstance(item, dict) else None
        name = item.get("name") if isinstance(item, dict) else None
        slug = app.get("slug") if isinstance(app, dict) else None
        app_id = app.get("id") if isinstance(app, dict) else None
        check_id = item.get("id") if isinstance(item, dict) else None
        if (
            not isinstance(name, str)
            or not name.strip()
            or not isinstance(slug, str)
            or not slug.strip()
            or type(app_id) is not int
            or app_id <= 0
            or type(check_id) is not int
        ):
            raise DeliverySetupUnavailableError(
                "GitHub check-run identity is malformed"
            )
        candidate = {
            "kind": "check_run",
            "name": name.strip(),
            "app_slug": slug.strip().lower(),
            "app_id": app_id,
            "github_id": check_id,
        }
        key = ("check_run", candidate["name"], candidate["app_slug"])
        previous = newest.get(key)
        if previous is None or check_id > int(previous["github_id"]):
            newest[key] = candidate

    for item in statuses:
        creator = item.get("creator") if isinstance(item, dict) else None
        name = item.get("context") if isinstance(item, dict) else None
        login = creator.get("login") if isinstance(creator, dict) else None
        status_id = item.get("id") if isinstance(item, dict) else None
        if (
            not isinstance(name, str)
            or not name.strip()
            or not isinstance(login, str)
            or not login.strip()
            or type(status_id) is not int
        ):
            raise DeliverySetupUnavailableError(
                "GitHub commit-status identity is malformed"
            )
        candidate = {
            "kind": "status",
            "name": name.strip(),
            "app_slug": login.strip().lower(),
            "app_id": None,
            "github_id": status_id,
        }
        key = ("status", candidate["name"], candidate["app_slug"])
        previous = newest.get(key)
        if previous is None or status_id > int(previous["github_id"]):
            newest[key] = candidate

    return sorted(
        newest.values(),
        key=lambda item: (
            str(item["name"]).lower(),
            str(item["kind"]),
            str(item["app_slug"]),
        ),
    )


def _protected_policies(
    protection: object,
    candidates: list[dict[str, object]],
) -> list[dict[str, str]] | None:
    if not isinstance(protection, dict):
        return None
    required = protection.get("required_status_checks")
    if not isinstance(required, dict):
        return None
    checks = required.get("checks")
    contexts = required.get("contexts")
    if checks is not None and not isinstance(checks, list):
        return None
    if contexts is not None and not isinstance(contexts, list):
        return None

    requirements: list[tuple[str, int | None]] = []
    seen_names: set[str] = set()
    for item in checks or []:
        name = item.get("context") if isinstance(item, dict) else None
        app_id = item.get("app_id") if isinstance(item, dict) else None
        if (
            not isinstance(name, str)
            or not name.strip()
            or (app_id is not None and (type(app_id) is not int or app_id < -1))
        ):
            return None
        normalized = name.strip()
        requirements.append((normalized, app_id if app_id and app_id > 0 else None))
        seen_names.add(normalized)
    for item in contexts or []:
        if not isinstance(item, str) or not item.strip():
            return None
        normalized = item.strip()
        if normalized not in seen_names:
            requirements.append((normalized, None))
            seen_names.add(normalized)
    if not requirements:
        return None

    policies: list[dict[str, str]] = []
    identities: set[tuple[str, str, str]] = set()
    for name, app_id in requirements:
        matches = [
            candidate
            for candidate in candidates
            if candidate["name"] == name
            and (
                app_id is None
                or (candidate["kind"] == "check_run" and candidate["app_id"] == app_id)
            )
        ]
        if len(matches) != 1:
            return None
        policy = _public_policy(matches[0])
        identity = (policy["kind"], policy["name"], policy["app_slug"])
        if identity not in identities:
            identities.add(identity)
            policies.append(policy)
    return policies or None


def _protection_declares_required_checks(protection: object) -> bool:
    if not isinstance(protection, dict):
        return False
    required = protection.get("required_status_checks")
    if not isinstance(required, dict):
        return False
    checks = required.get("checks")
    contexts = required.get("contexts")
    return bool(
        (isinstance(checks, list) and checks)
        or (isinstance(contexts, list) and contexts)
    )


def _is_unprotected_branch_error(exc: GhError) -> bool:
    """Recognize only GitHub's explicit no-classic-protection response."""

    detail = str(exc).strip().lower()
    return (
        "http 404" in detail
        or "status code 404" in detail
        or "branch not protected" in detail
    )


def _is_branch_protection_plan_error(exc: GhError) -> bool:
    """Recognize GitHub plan gating, not generic authorization failures."""

    detail = str(exc).strip().lower()
    return (
        "upgrade to github pro" in detail
        or "upgrade to github team" in detail
        or ("make this repository public" in detail and "enable this feature" in detail)
    )


async def discover_delivery_required_checks(
    repo_full_name: str,
    default_branch: str,
    *,
    strict_branch_protection: bool = False,
) -> tuple[list[dict[str, str]], str]:
    """Resolve GitHub-declared exact-head CI policies, or return no CI gate."""

    branch = quote(default_branch, safe="")
    try:
        checks = await _gh_api_json(
            f"repos/{repo_full_name}/commits/{branch}/check-runs?per_page=100"
        )
        statuses = await _gh_api_json(
            f"repos/{repo_full_name}/commits/{branch}/status?per_page=100"
        )
    except GhError as exc:
        raise DeliverySetupUnavailableError(
            "Could not read GitHub CI with the server's gh login",
            repo_full_name=repo_full_name,
        ) from exc
    candidates = _parse_ci_candidates(checks, statuses)

    protection: object = None
    try:
        protection = await _gh_api_json(
            f"repos/{repo_full_name}/branches/{branch}/protection"
        )
    except GhError as exc:
        # Reading branch protection often needs repository administration
        # metadata. An explicit 404 means no classic protection; auth,
        # transport and malformed responses are not proof that no required
        # checks exist and must therefore fail closed.
        if _is_unprotected_branch_error(exc):
            protection = None
        elif not strict_branch_protection and _is_branch_protection_plan_error(exc):
            # GitHub Free private repositories cannot expose/enforce this
            # policy. Trusted Delivery falls back to the exact producer
            # identities observed on the current default-branch commit; the
            # merge gate will require those identities again on the PR head.
            protection = None
        else:
            raise DeliverySetupUnavailableError(
                "Could not prove the GitHub branch-protection policy with "
                "the server's gh login",
                repo_full_name=repo_full_name,
                candidates=[_public_policy(item) for item in candidates],
                code="branch_protection_unavailable",
            ) from exc

    protected = _protected_policies(protection, candidates)
    if protected:
        return protected, "branch_protection"
    if _protection_declares_required_checks(protection):
        raise DeliverySetupUnavailableError(
            "GitHub branch protection declares required checks, but their "
            "exact producer identities could not be resolved automatically. "
            "Run the protected CI on the default branch and retry.",
            repo_full_name=repo_full_name,
            candidates=[_public_policy(item) for item in candidates],
            code="required_checks_unresolved",
        )
    if not strict_branch_protection and candidates:
        # Trusted mode deliberately does not freeze the checks observed on the
        # default branch. Workflow path filters and job-level conditions mean
        # that set is commonly much larger than the checks triggered by a
        # particular PR. Store one explicit policy marker; the exact-head Gate
        # expands it to the checks GitHub actually created for that PR head.
        return [dict(TRUSTED_OBSERVED_CI_POLICY)], "trusted_observed_checks"
    # A repository with no observed CI still receives Panel review, but direct
    # auto-merge remains disabled because there is no exact check identity to
    # prove on the PR head.
    return [], "no_declared_required_checks"


async def ensure_default_delivery_monitor(
    db: AsyncSession,
    project_id: int,
    *,
    allow_create: bool = True,
    strict_branch_protection: bool = False,
    create_authorizer: Callable[[AsyncSession], Awaitable[None]] | None = None,
) -> DeliveryMonitorSetup:
    """Return a compatible Monitor or create its conservative default."""

    identity = _project_identity(await db.get(Project, project_id))
    existing = await _existing_monitor(db, identity)
    existing_id = existing.id if existing is not None else None
    if existing is not None:
        problem = _monitor_problem(existing, identity)
        if problem:
            raise DeliverySetupConflictError(
                f"Cannot configure Delivery automatically because {problem}. "
                "An administrator must resolve the conflicting legacy binding."
            )
        if not allow_create:
            if strict_branch_protection:
                await db.rollback()
                raise DeliverySetupPermissionError(
                    "A CCM administrator must refresh this Project's strict "
                    "PR Monitor CI policy"
                )
            await db.commit()
            return DeliveryMonitorSetup(existing, False, None)
        if (
            not strict_branch_protection
            and existing.required_checks == [TRUSTED_OBSERVED_CI_POLICY]
        ):
            await db.commit()
            return DeliveryMonitorSetup(existing, False, None)

    if existing is None and not allow_create:
        await db.rollback()
        raise DeliverySetupPermissionError(
            "A CCM administrator must configure this Project's PR Monitor once"
        )

    # Do not hold a database transaction while invoking GitHub.
    await db.rollback()
    if strict_branch_protection:
        policies, source = await discover_delivery_required_checks(
            identity.repo_full_name,
            identity.default_branch,
            strict_branch_protection=True,
        )
    else:
        # Preserve the historical two-argument extension seam for tests and
        # deployments that replace the discovery helper.
        policies, source = await discover_delivery_required_checks(
            identity.repo_full_name,
            identity.default_branch,
        )

    if existing_id is not None:
        return await _reconcile_existing_monitor(
            db,
            repo_id=existing_id,
            identity=identity,
            policies=policies,
            source=source,
            create_authorizer=create_authorizer,
        )

    # No Monitor existed before discovery. Serialize creation against Project
    # identity edits and re-prove the exact snapshot used for GitHub discovery.
    current = await _lock_project_identity(db, project_id)
    if current != identity:
        await db.rollback()
        raise DeliverySetupConflictError(
            "Project GitHub identity changed during PR Monitor setup; retry"
        )

    existing = await _existing_monitor(db, current)
    if existing is not None:
        # A creator may have committed while GitHub discovery was in flight.
        # Release Project before taking that existing repository writer lock,
        # then re-enter both rows in canonical order and revalidate everything.
        existing_id = existing.id
        await db.rollback()
        return await _reconcile_existing_monitor(
            db,
            repo_id=existing_id,
            identity=identity,
            policies=policies,
            source=source,
            create_authorizer=create_authorizer,
        )

    # Background post-clone setup is a system producer and intentionally has
    # no authorizer. Quick Start revalidates the cached HTTP principal inside
    # the same Project-fenced transaction that inserts the Monitor.
    if create_authorizer is not None:
        await create_authorizer(db)

    repo = MonitoredRepo(
        repo_full_name=current.repo_full_name,
        project_id=current.project_id,
        worker_id=None,
        enabled=True,
        auto_merge=False,
        provider=_default_provider(),
        review_model=None,
        review_effort=None,
        review_mode="panel",
        wait_for_ci=bool(policies),
        required_checks=policies,
        auto_repair=True,
        max_repair_attempts=3,
        merge_queue_mode="manual",
        default_branch=current.default_branch,
        allowed_authors=[],
        webhook_secret=secrets.token_hex(32),
        status="active",
    )
    db.add(repo)
    try:
        await db.commit()
    except IntegrityError as exc:
        # A concurrent quick-start may have won the global repository unique
        # key.  Re-read and accept only the exact compatible result.
        await db.rollback()
        winner = await _existing_monitor(db, current)
        if winner is None or _monitor_problem(winner, current):
            raise DeliverySetupConflictError(
                "Another PR Monitor setup won concurrently with incompatible settings"
            ) from exc
        await db.commit()
        return DeliveryMonitorSetup(winner, False, None)
    await db.refresh(repo)
    return DeliveryMonitorSetup(repo, True, source)


async def try_auto_configure_delivery_monitor(project_id: int) -> None:
    """Best-effort post-clone bootstrap; quick-start remains the retry path."""

    from backend.database import async_session

    async with async_session() as db:
        try:
            setup = await ensure_default_delivery_monitor(db, project_id)
        except DeliverySetupError as exc:
            logger.info(
                "Delivery PR Monitor auto-setup deferred for Project %s: %s",
                project_id,
                exc,
            )
            return
        except Exception:
            logger.warning(
                "Delivery PR Monitor auto-setup failed for Project %s",
                project_id,
                exc_info=True,
            )
            return
        if setup.created:
            logger.info(
                "Created default Delivery PR Monitor %s for Project %s via %s",
                setup.repo.id,
                project_id,
                setup.discovery_source,
            )
