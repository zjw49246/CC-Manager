"""Project import → PR Monitor bootstrap → one-message Delivery coverage."""

from __future__ import annotations

import pytest
from sqlalchemy import func, select

from backend.api import delivery_runs as delivery_api
from backend.config import settings
from backend.models.delivery import DeliveryRun
from backend.models.pr_monitor import MonitoredRepo
from backend.models.project import Project
from backend.services import delivery_setup
from backend.services.delivery_setup import (
    DeliverySetupConflictError,
    DeliverySetupPermissionError,
    DeliverySetupUnavailableError,
    discover_delivery_required_checks,
    ensure_default_delivery_monitor,
    TRUSTED_OBSERVED_CI_POLICY,
)


def _github_responses(endpoint: str) -> dict:
    if "/check-runs" in endpoint:
        return {
            "total_count": 2,
            "check_runs": [
                {
                    "id": 21,
                    "name": "tests",
                    "app": {"id": 15368, "slug": "github-actions"},
                },
                {
                    "id": 22,
                    "name": "lint",
                    "app": {"id": 15368, "slug": "github-actions"},
                },
            ],
        }
    if "/status" in endpoint:
        return {"statuses": []}
    if "/protection" in endpoint:
        return {
            "required_status_checks": {
                "strict": True,
                "contexts": ["tests"],
                "checks": [{"context": "tests", "app_id": 15368}],
            }
        }
    raise AssertionError(endpoint)


@pytest.mark.asyncio
async def test_ci_discovery_prefers_exact_branch_protection(monkeypatch):
    async def fake_gh(endpoint: str):
        return _github_responses(endpoint)

    monkeypatch.setattr(delivery_setup, "_gh_api_json", fake_gh)

    policies, source = await discover_delivery_required_checks(
        "acme/widgets",
        "main",
    )

    assert source == "branch_protection"
    assert policies == [
        {
            "kind": "check_run",
            "name": "tests",
            "app_slug": "github-actions",
        }
    ]


@pytest.mark.asyncio
async def test_ci_discovery_uses_observed_checks_in_trusted_mode(
    monkeypatch,
):
    async def fake_gh(endpoint: str):
        if "/protection" in endpoint:
            raise delivery_setup.GhError("HTTP 404: Not Found")
        return _github_responses(endpoint)

    monkeypatch.setattr(delivery_setup, "_gh_api_json", fake_gh)

    policies, source = await discover_delivery_required_checks(
        "acme/widgets",
        "main",
    )

    assert policies == [TRUSTED_OBSERVED_CI_POLICY]
    assert source == "trusted_observed_checks"


@pytest.mark.asyncio
async def test_ci_discovery_uses_single_observed_check_in_trusted_mode(monkeypatch):
    async def fake_gh(endpoint: str):
        if "/protection" in endpoint:
            raise delivery_setup.GhError("HTTP 404: Not Found")
        value = _github_responses(endpoint)
        if "/check-runs" in endpoint:
            value = {
                "total_count": 1,
                "check_runs": [value["check_runs"][0]],
            }
        return value

    monkeypatch.setattr(delivery_setup, "_gh_api_json", fake_gh)

    policies, source = await discover_delivery_required_checks(
        "acme/widgets",
        "main",
    )

    assert policies == [TRUSTED_OBSERVED_CI_POLICY]
    assert source == "trusted_observed_checks"


@pytest.mark.asyncio
async def test_ci_discovery_fails_closed_when_protection_cannot_be_read(
    monkeypatch,
):
    async def fake_gh(endpoint: str):
        if "/protection" in endpoint:
            raise delivery_setup.GhError(
                "HTTP 403: Resource not accessible by integration"
            )
        return _github_responses(endpoint)

    monkeypatch.setattr(delivery_setup, "_gh_api_json", fake_gh)

    with pytest.raises(
        DeliverySetupUnavailableError,
        match="Could not prove the GitHub branch-protection policy",
    ) as caught:
        await discover_delivery_required_checks("acme/widgets", "main")

    assert caught.value.code == "branch_protection_unavailable"


@pytest.mark.asyncio
async def test_ci_discovery_allows_plan_gate_in_trusted_mode(monkeypatch):
    async def fake_gh(endpoint: str):
        if "/protection" in endpoint:
            raise delivery_setup.GhError(
                "HTTP 403: Upgrade to GitHub Pro or make this repository "
                "public to enable this feature"
            )
        return _github_responses(endpoint)

    monkeypatch.setattr(delivery_setup, "_gh_api_json", fake_gh)

    policies, source = await discover_delivery_required_checks(
        "acme/widgets",
        "main",
        strict_branch_protection=False,
    )

    assert policies == [TRUSTED_OBSERVED_CI_POLICY]
    assert source == "trusted_observed_checks"


@pytest.mark.asyncio
async def test_ci_discovery_keeps_plan_gate_strict_when_requested(monkeypatch):
    async def fake_gh(endpoint: str):
        if "/protection" in endpoint:
            raise delivery_setup.GhError(
                "HTTP 403: Upgrade to GitHub Pro or make this repository "
                "public to enable this feature"
            )
        return _github_responses(endpoint)

    monkeypatch.setattr(delivery_setup, "_gh_api_json", fake_gh)

    with pytest.raises(
        DeliverySetupUnavailableError,
        match="Could not prove the GitHub branch-protection policy",
    ):
        await discover_delivery_required_checks(
            "acme/widgets",
            "main",
            strict_branch_protection=True,
        )


@pytest.mark.asyncio
async def test_ci_discovery_fails_when_declared_identity_cannot_be_resolved(
    monkeypatch,
):
    async def fake_gh(endpoint: str):
        value = _github_responses(endpoint)
        if "/protection" in endpoint:
            value["required_status_checks"] = {
                "strict": True,
                "contexts": ["release"],
                "checks": [{"context": "release", "app_id": 999}],
            }
        return value

    monkeypatch.setattr(delivery_setup, "_gh_api_json", fake_gh)

    with pytest.raises(
        DeliverySetupUnavailableError,
        match="could not be resolved automatically",
    ) as caught:
        await discover_delivery_required_checks("acme/widgets", "main")

    assert caught.value.code == "required_checks_unresolved"


async def _project(session_factory, *, suffix: str) -> Project:
    async with session_factory() as db:
        project = Project(
            name=f"delivery-setup-{suffix}",
            local_path=f"/srv/delivery-setup-{suffix}",
            git_url=f"https://github.com/acme/delivery-setup-{suffix}.git",
            has_remote=True,
            default_branch="main",
            status="ready",
        )
        db.add(project)
        await db.commit()
        await db.refresh(project)
        return project


@pytest.mark.asyncio
async def test_existing_monitor_lookup_never_takes_repo_lock():
    statements = []

    class EmptyResult:
        @staticmethod
        def scalars():
            return ()

    class RecordingSession:
        async def execute(self, statement):
            statements.append(statement)
            return EmptyResult()

    identity = delivery_setup._ProjectIdentity(
        project_id=1,
        repo_full_name="acme/lock-order",
        default_branch="main",
        git_url="https://github.com/acme/lock-order.git",
    )

    assert await delivery_setup._existing_monitor(
        RecordingSession(),
        identity,
    ) is None
    assert len(statements) == 1
    assert getattr(statements[0], "_for_update_arg", None) is None


@pytest.mark.asyncio
async def test_monitor_bootstrap_accepts_concurrent_compatible_winner(
    session_factory,
    monkeypatch,
):
    project = await _project(session_factory, suffix="concurrent-winner")

    async def fake_discovery(repo: str, branch: str):
        assert repo == "acme/delivery-setup-concurrent-winner"
        assert branch == "main"
        async with session_factory() as winner_db:
            winner_db.add(MonitoredRepo(
                repo_full_name=repo,
                project_id=project.id,
                webhook_secret="winner-secret",
                enabled=True,
                auto_merge=False,
                provider="codex",
                review_mode="panel",
                wait_for_ci=False,
                required_checks=[],
                auto_repair=True,
                max_repair_attempts=3,
                merge_queue_mode="manual",
                default_branch=branch,
                allowed_authors=[],
                status="active",
            ))
            await winner_db.commit()
        return [], "no_declared_required_checks"

    monkeypatch.setattr(
        delivery_setup,
        "discover_delivery_required_checks",
        fake_discovery,
    )

    async with session_factory() as db:
        setup = await ensure_default_delivery_monitor(db, project.id)

    assert setup.created is False
    assert setup.repo.webhook_secret == "winner-secret"
    async with session_factory() as db:
        assert await db.scalar(select(func.count(MonitoredRepo.id))) == 1


@pytest.mark.asyncio
async def test_monitor_bootstrap_is_conservative_and_idempotent(
    session_factory,
    monkeypatch,
):
    project = await _project(session_factory, suffix="idempotent")

    async def fake_discovery(repo: str, branch: str):
        assert repo == "acme/delivery-setup-idempotent"
        assert branch == "main"
        return (
            [
                {
                    "kind": "check_run",
                    "name": "tests",
                    "app_slug": "github-actions",
                }
            ],
            "branch_protection",
        )

    monkeypatch.setattr(
        delivery_setup,
        "discover_delivery_required_checks",
        fake_discovery,
    )

    async with session_factory() as db:
        first = await ensure_default_delivery_monitor(db, project.id)
    async with session_factory() as db:
        second = await ensure_default_delivery_monitor(db, project.id)

    assert first.created is True
    assert second.created is False
    assert second.repo.id == first.repo.id
    assert first.repo.review_mode == "panel"
    assert first.repo.wait_for_ci is True
    assert first.repo.auto_repair is True
    assert first.repo.auto_merge is False
    assert first.repo.merge_queue_mode == "manual"
    async with session_factory() as db:
        assert await db.scalar(select(func.count(MonitoredRepo.id))) == 1


@pytest.mark.asyncio
async def test_monitor_bootstrap_adds_trusted_ci_to_existing_empty_monitor(
    session_factory,
    monkeypatch,
):
    project = await _project(session_factory, suffix="existing-empty")
    lock_order: list[str] = []
    real_repo_lock = delivery_setup.lock_pr_repo_action_boundary
    real_project_lock = delivery_setup._lock_project_identity

    async def tracked_repo_lock(db, repo_id):
        lock_order.append("repo")
        return await real_repo_lock(db, repo_id)

    async def tracked_project_lock(db, project_id):
        lock_order.append("project")
        return await real_project_lock(db, project_id)

    async def authorize(_db):
        lock_order.append("authority")

    monkeypatch.setattr(
        delivery_setup,
        "lock_pr_repo_action_boundary",
        tracked_repo_lock,
    )
    monkeypatch.setattr(
        delivery_setup,
        "_lock_project_identity",
        tracked_project_lock,
    )
    async with session_factory() as db:
        db.add(
            MonitoredRepo(
                repo_full_name="acme/delivery-setup-existing-empty",
                project_id=project.id,
                webhook_secret="existing",
                review_mode="panel",
                wait_for_ci=False,
                required_checks=[],
                merge_queue_mode="manual",
                default_branch="main",
            )
        )
        await db.commit()

    async def fake_discovery(repo: str, branch: str):
        assert repo == "acme/delivery-setup-existing-empty"
        assert branch == "main"
        return (
            [
                {
                    "kind": "check_run",
                    "name": "tests",
                    "app_slug": "github-actions",
                }
            ],
            "trusted_observed_checks",
        )

    monkeypatch.setattr(
        delivery_setup,
        "discover_delivery_required_checks",
        fake_discovery,
    )

    async with session_factory() as db:
        setup = await ensure_default_delivery_monitor(
            db,
            project.id,
            create_authorizer=authorize,
        )

    assert setup.created is False
    assert lock_order == ["repo", "project", "authority"]
    assert setup.repo.wait_for_ci is True
    assert setup.repo.required_checks == [
        {
            "kind": "check_run",
            "name": "tests",
            "app_slug": "github-actions",
        }
    ]


@pytest.mark.asyncio
async def test_member_strict_bootstrap_cannot_mutate_existing_monitor(
    session_factory,
    monkeypatch,
):
    project = await _project(session_factory, suffix="member-strict")
    async with session_factory() as db:
        repo = MonitoredRepo(
            repo_full_name="acme/delivery-setup-member-strict",
            project_id=project.id,
            webhook_secret="existing",
            review_mode="panel",
            wait_for_ci=False,
            required_checks=[],
            merge_queue_mode="manual",
            default_branch="main",
        )
        db.add(repo)
        await db.commit()
        repo_id = repo.id

    async def unexpected_discovery(*_args, **_kwargs):
        raise AssertionError("member strict setup must not discover or mutate CI")

    monkeypatch.setattr(
        delivery_setup,
        "discover_delivery_required_checks",
        unexpected_discovery,
    )

    async with session_factory() as db:
        with pytest.raises(
            DeliverySetupPermissionError,
            match="administrator must refresh",
        ):
            await ensure_default_delivery_monitor(
                db,
                project.id,
                allow_create=False,
                strict_branch_protection=True,
            )

    async with session_factory() as db:
        unchanged = await db.get(MonitoredRepo, repo_id)
        assert unchanged.wait_for_ci is False
        assert unchanged.required_checks == []


@pytest.mark.asyncio
async def test_monitor_bootstrap_does_not_hijack_existing_repo(
    session_factory,
):
    project = await _project(session_factory, suffix="conflict")
    async with session_factory() as db:
        db.add(
            MonitoredRepo(
                repo_full_name="acme/delivery-setup-conflict",
                project_id=None,
                webhook_secret="existing",
                review_mode="panel",
                wait_for_ci=True,
                required_checks=[
                    {
                        "kind": "check_run",
                        "name": "tests",
                        "app_slug": "github-actions",
                    }
                ],
                merge_queue_mode="manual",
                default_branch="main",
            )
        )
        await db.commit()

    async with session_factory() as db:
        with pytest.raises(DeliverySetupConflictError, match="another Project"):
            await ensure_default_delivery_monitor(db, project.id)


@pytest.mark.asyncio
async def test_quick_start_creates_monitor_and_delivery_from_one_message(
    client,
    session_factory,
    monkeypatch,
):
    project = await _project(session_factory, suffix="quick")
    monkeypatch.setattr(settings, "delivery_loop_enabled", True)
    monkeypatch.setattr(settings, "capability_core_enabled", True)
    monkeypatch.setattr(delivery_api, "_wake_controller", lambda: None)
    topology_calls: list[tuple[int, int]] = []
    real_topology_lock = delivery_api._lock_delivery_admission_topology

    async def observed_topology_lock(request, db, *, project_id, monitored_repo_id):
        topology_calls.append((project_id, monitored_repo_id))
        await real_topology_lock(
            request,
            db,
            project_id=project_id,
            monitored_repo_id=monitored_repo_id,
        )

    monkeypatch.setattr(
        delivery_api,
        "_lock_delivery_admission_topology",
        observed_topology_lock,
    )

    async def fake_discovery(repo: str, branch: str):
        return (
            [
                {
                    "kind": "check_run",
                    "name": "tests",
                    "app_slug": "github-actions",
                }
            ],
            "branch_protection",
        )

    monkeypatch.setattr(
        delivery_setup,
        "discover_delivery_required_checks",
        fake_discovery,
    )

    request = {
        "idempotency_key": "quick-start-one-message",
        "project_id": project.id,
        "requirements": "Fix login redirects\n\nAdd regression coverage.",
        "auto_merge": True,
    }
    response = await client.post("/api/delivery-runs/quick-start", json=request)
    replay = await client.post("/api/delivery-runs/quick-start", json=request)

    assert response.status_code == 201, response.text
    assert replay.status_code == 201, replay.text
    assert len(topology_calls) == 2
    assert {project_id for project_id, _repo_id in topology_calls} == {
        project.id
    }
    assert len({repo_id for _project_id, repo_id in topology_calls}) == 1
    body = response.json()
    assert replay.json()["id"] == body["id"]
    assert body["title"] == "Fix login redirects"
    assert body["phase"] == "planning"
    assert body["terminal"] == "merged"
    async with session_factory() as db:
        repo = await db.scalar(
            select(MonitoredRepo).where(MonitoredRepo.project_id == project.id)
        )
        run = await db.get(DeliveryRun, body["id"])
        assert repo is not None
        assert run is not None
        assert run.monitored_repo_id == repo.id
        assert await db.scalar(select(func.count(DeliveryRun.id))) == 1
        assert repo.required_checks == [
            {
                "kind": "check_run",
                "name": "tests",
                "app_slug": "github-actions",
            }
        ]


@pytest.mark.asyncio
async def test_quick_start_keeps_panel_without_inventing_ci_configuration(
    client,
    session_factory,
    monkeypatch,
):
    project = await _project(session_factory, suffix="panel-only")
    monkeypatch.setattr(settings, "delivery_loop_enabled", True)
    monkeypatch.setattr(settings, "capability_core_enabled", True)
    monkeypatch.setattr(delivery_api, "_wake_controller", lambda: None)

    async def fake_discovery(repo: str, branch: str):
        return [], "no_declared_required_checks"

    monkeypatch.setattr(
        delivery_setup,
        "discover_delivery_required_checks",
        fake_discovery,
    )

    response = await client.post(
        "/api/delivery-runs/quick-start",
        json={
            "idempotency_key": "quick-start-panel-only",
            "project_id": project.id,
            "requirements": "Create the PR and run the mandatory Panel.",
        },
    )

    assert response.status_code == 201, response.text
    assert response.json()["terminal"] == "ready_to_merge"
    async with session_factory() as db:
        repo = await db.scalar(
            select(MonitoredRepo).where(MonitoredRepo.project_id == project.id)
        )
        assert repo is not None
        assert repo.review_mode == "panel"
        assert repo.wait_for_ci is False
        assert repo.required_checks == []


@pytest.mark.asyncio
async def test_quick_start_rejects_auto_merge_without_exact_required_ci(
    client,
    session_factory,
    monkeypatch,
):
    project = await _project(session_factory, suffix="unsafe-auto-merge")
    monkeypatch.setattr(settings, "delivery_loop_enabled", True)
    monkeypatch.setattr(settings, "capability_core_enabled", True)

    async def fake_discovery(repo: str, branch: str):
        return [], "no_declared_required_checks"

    monkeypatch.setattr(
        delivery_setup,
        "discover_delivery_required_checks",
        fake_discovery,
    )

    response = await client.post(
        "/api/delivery-runs/quick-start",
        json={
            "idempotency_key": "quick-start-no-ci-auto-merge",
            "project_id": project.id,
            "requirements": "Do not merge without a protected CI identity.",
            "auto_merge": True,
        },
    )

    assert response.status_code == 400, response.text
    assert "auto-merge requires" in response.text
    async with session_factory() as db:
        assert await db.scalar(select(func.count(DeliveryRun.id))) == 0
