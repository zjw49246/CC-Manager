"""HTTP contracts for Delivery Loop admission and operator controls."""

from __future__ import annotations

import json
from datetime import datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import func, select, update

from backend.api import delivery_runs as delivery_api
from backend.config import settings
from backend.models.capability import CapabilityExecution, CapabilityInvocation
from backend.models.delivery import (
    DeliveryAction,
    DeliveryCycle,
    DeliveryRun,
    DeliveryTransition,
    DeliveryTurn,
)
from backend.models.pr_monitor import MonitoredRepo
from backend.models.project import Project
from backend.models.project_todo import ProjectTodo
from backend.models.plan import Plan, PlanInputRequest, PlanVersion
from backend.models.plan_agent import PlanAgentRun
from backend.models.task import Task
from backend.models.team_share import TeamProjectShare, TeamTaskShare
from backend.models.user import User
from backend.services import delivery_service
from backend.services import delivery_setup
from backend.services.delivery_service import (
    DeliveryCreateSpec,
    DeliveryValidationError,
    create_delivery_run,
)
from backend.services.delivery_reducer import DeliveryReducerEvent
from backend.schemas.plan import default_plan_pipeline_config
from backend.tests.test_auth_ws_security import (
    _create_user,
    secured_client as secured_client,
)
from backend.tests.group_acl_test_helpers import (
    grant_group_project_access,
    revoke_group_membership_at_effect_fence,
)


def _payload(project: Project, repo: MonitoredRepo, **overrides: object) -> dict:
    payload: dict[str, object] = {
        "idempotency_key": f"delivery-api-{uuid4()}",
        "project_id": project.id,
        "monitored_repo_id": repo.id,
        "title": "Fix the delivery race",
        "requirements": "Fix the race and add focused regression coverage.",
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "effort_level": "high",
        "max_cycles": 7,
        "max_no_progress": 2,
    }
    payload.update(overrides)
    return payload


async def _scope(
    session_factory,
    *,
    suffix: str,
    auto_merge: bool = False,
) -> tuple[Project, MonitoredRepo]:
    async with session_factory() as db:
        project = Project(
            name=f"delivery-api-{suffix}",
            local_path=f"/srv/repos/delivery-api-{suffix}",
            git_url=f"git@github.com:acme/delivery-api-{suffix}.git",
            has_remote=True,
            default_branch="main",
            status="ready",
        )
        db.add(project)
        await db.flush()
        repo = MonitoredRepo(
            repo_full_name=f"acme/delivery-api-{suffix}",
            project_id=project.id,
            webhook_secret="test-secret",
            enabled=True,
            auto_merge=auto_merge,
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
        db.add(repo)
        await db.commit()
        await db.refresh(project)
        await db.refresh(repo)
        return project, repo


@pytest.mark.asyncio
async def test_delivery_api_topology_helper_locks_repo_before_project(
    monkeypatch,
):
    calls: list[str] = []
    repo = SimpleNamespace(project_id=11)

    class FakeSession:
        async def get(self, _model, _identity):
            calls.append("optimistic_repo")
            return repo

        async def rollback(self):
            calls.append("rollback")

    async def authorize_project(_request, _project_id, _db):
        calls.append("optimistic_project_acl")

    async def lock_repo(_db, _repo_id):
        calls.append("repo")
        return repo

    async def lock_project(_request, _project_id, _db):
        calls.append("project")

    monkeypatch.setattr(
        delivery_api,
        "require_project_access",
        authorize_project,
    )
    monkeypatch.setattr(
        delivery_api,
        "lock_pr_repo_action_boundary",
        lock_repo,
    )
    monkeypatch.setattr(
        delivery_api,
        "lock_project_effect_access",
        lock_project,
    )

    await delivery_api._lock_delivery_admission_topology(
        object(),
        FakeSession(),
        project_id=11,
        monitored_repo_id=22,
    )

    assert calls == [
        "optimistic_project_acl",
        "optimistic_repo",
        "rollback",
        "repo",
        "project",
    ]


@pytest.fixture
def delivery_enabled(monkeypatch):
    monkeypatch.setattr(settings, "delivery_loop_enabled", True)
    monkeypatch.setattr(settings, "capability_core_enabled", True)
    # API tests verify the durable wake boundary, not a background controller
    # racing the assertions against the in-memory database.
    monkeypatch.setattr(delivery_api, "_wake_controller", lambda: None)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("delivery_flag", "capability_flag", "detail"),
    [
        (False, True, "Delivery Loop mode is disabled"),
        (True, False, "requires Capability Core"),
    ],
)
async def test_create_is_fail_closed_by_both_feature_flags(
    client,
    session_factory,
    monkeypatch,
    delivery_flag,
    capability_flag,
    detail,
):
    project, repo = await _scope(session_factory, suffix=detail[:4])
    monkeypatch.setattr(settings, "delivery_loop_enabled", delivery_flag)
    monkeypatch.setattr(settings, "capability_core_enabled", capability_flag)
    monkeypatch.setattr(delivery_api, "_wake_controller", lambda: None)

    response = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )

    assert response.status_code == 503
    assert detail in response.json()["detail"]
    async with session_factory() as db:
        assert await db.scalar(select(func.count(DeliveryRun.id))) == 0
        assert await db.scalar(select(func.count(Task.id))) == 0


@pytest.mark.asyncio
async def test_quick_start_monitor_create_revalidates_cached_admin_authority(
    secured_client,
    delivery_enabled,
    monkeypatch,
):
    """Admin revocation during GitHub discovery must veto Monitor creation."""

    client, session_factory = secured_client
    admin_id, admin_token = await _create_user(
        session_factory,
        email="delivery-quick-start-disabled-admin@example.com",
        role="admin",
    )
    async with session_factory() as db:
        project = Project(
            name="delivery-quick-start-admin-race",
            local_path="/srv/repos/delivery-quick-start-admin-race",
            git_url="git@github.com:acme/delivery-quick-start-admin-race.git",
            has_remote=True,
            default_branch="main",
            status="ready",
        )
        db.add(project)
        await db.commit()
        project_id = project.id

    async def no_required_checks(_repo_full_name, _default_branch):
        return [], "no_declared_required_checks"

    monkeypatch.setattr(
        delivery_setup,
        "discover_delivery_required_checks",
        no_required_checks,
    )
    original = delivery_api.lock_request_user_authority
    fence = {"calls": 0, "disabled": 0}

    async def disable_then_lock(request, db):
        fence["calls"] += 1
        async with session_factory() as competing_db:
            changed = await competing_db.execute(
                update(User).where(User.id == admin_id).values(is_active=False)
            )
            fence["disabled"] += changed.rowcount
            await competing_db.commit()
        await original(request, db)

    monkeypatch.setattr(
        delivery_api,
        "lock_request_user_authority",
        disable_then_lock,
    )

    response = await client.post(
        "/api/delivery-runs/quick-start",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={
            "idempotency_key": "quick-start-admin-revoked",
            "project_id": project_id,
            "requirements": "must not create a Monitor after revocation",
        },
    )

    assert response.status_code == 409, response.text
    assert "disabled or changed role" in response.json()["detail"]
    assert fence == {"calls": 1, "disabled": 1}
    async with session_factory() as db:
        assert await db.scalar(select(func.count(MonitoredRepo.id))) == 0
        assert await db.scalar(select(func.count(DeliveryRun.id))) == 0
        assert await db.scalar(select(func.count(Task.id))) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "effect",
    ["create", "quick_start", "resume", "retry"],
)
async def test_delivery_effect_rejects_group_revoked_at_final_project_fence(
    secured_client,
    delivery_enabled,
    monkeypatch,
    effect,
):
    """All Delivery admissions and restarts share one Project ACL boundary."""

    client, session_factory = secured_client
    member_id, member_token = await _create_user(
        session_factory,
        email=f"delivery-{effect}-effect@example.com",
        role="member",
    )
    project, repo = await _scope(
        session_factory,
        suffix=f"{effect}-effect",
    )
    await grant_group_project_access(
        session_factory,
        project_id=project.id,
        user_id=member_id,
    )
    admin_headers = {"Authorization": "Bearer security-service-token"}
    member_headers = {"Authorization": f"Bearer {member_token}"}
    run_id = None
    before_state_version = None
    before_cycle_count = None

    if effect in {"resume", "retry"}:
        created = await client.post(
            "/api/delivery-runs",
            headers=admin_headers,
            json=_payload(
                project,
                repo,
                idempotency_key=f"seed-{effect}-effect",
            ),
        )
        assert created.status_code == 201, created.text
        run_id = created.json()["id"]
        if effect == "resume":
            paused = await client.post(
                f"/api/delivery-runs/{run_id}/pause",
                headers=admin_headers,
                json={"reason": "seed paused state"},
            )
            assert paused.status_code == 200, paused.text
            before_state_version = paused.json()["state_version"]
            before_cycle_count = paused.json()["cycle_count"]
        else:
            async with session_factory() as db:
                run = await db.get(DeliveryRun, run_id)
                cycle = await db.get(DeliveryCycle, run.current_cycle_id)
                task = await db.get(Task, run.developer_task_id)
                delivery_service.complete_cycle(cycle, status="failed")
                await delivery_service.apply_run_event(
                    db,
                    run=run,
                    event=DeliveryReducerEvent(
                        "fail",
                        {
                            "error_code": "plan_run_failed",
                            "error_message": "seed retry state",
                        },
                    ),
                    actor_kind="controller",
                )
                task.status = "failed"
                task.completed_at = datetime.utcnow()
                task.error_message = run.error_message
                await db.commit()
                before_state_version = run.state_version
                before_cycle_count = run.cycle_count

    fence = revoke_group_membership_at_effect_fence(monkeypatch)
    if effect == "create":
        response = await client.post(
            "/api/delivery-runs",
            headers=member_headers,
            json=_payload(
                project,
                repo,
                idempotency_key="denied-create-effect",
            ),
        )
    elif effect == "quick_start":
        response = await client.post(
            "/api/delivery-runs/quick-start",
            headers=member_headers,
            json={
                "idempotency_key": "denied-quick-start-effect",
                "project_id": project.id,
                "requirements": "must not start Delivery",
            },
        )
    elif effect == "resume":
        response = await client.post(
            f"/api/delivery-runs/{run_id}/resume",
            headers=member_headers,
            json={"reason": "must stay paused"},
        )
    else:
        response = await client.post(
            f"/api/delivery-runs/{run_id}/retry",
            headers=member_headers,
            json={
                "expected_state_version": before_state_version,
                "reason": "must stay failed",
            },
        )

    assert response.status_code == 403, response.text
    assert fence == {"calls": 1, "revoked": True}
    async with session_factory() as db:
        runs = list((await db.execute(select(DeliveryRun))).scalars())
        tasks = list((await db.execute(select(Task))).scalars())
        if effect in {"create", "quick_start"}:
            assert runs == []
            assert tasks == []
        else:
            assert len(runs) == 1
            run = runs[0]
            task = await db.get(Task, run.developer_task_id)
            assert run.state_version == before_state_version
            assert run.cycle_count == before_cycle_count
            if effect == "resume":
                assert run.activity == "paused"
                assert (
                    await db.scalar(
                        select(func.count(DeliveryTransition.id)).where(
                            DeliveryTransition.run_id == run.id,
                            DeliveryTransition.cause == "resume",
                        )
                    )
                    == 0
                )
            else:
                assert (run.activity, run.outcome) == ("terminal", "failed")
                assert task.status == "failed"
                assert (
                    await db.scalar(
                        select(func.count(DeliveryTransition.id)).where(
                            DeliveryTransition.run_id == run.id,
                            DeliveryTransition.cause == "retry",
                        )
                    )
                    == 0
                )


@pytest.mark.asyncio
async def test_delivery_admission_contract_accepts_claude_provider(
    client,
    session_factory,
    delivery_enabled,
    monkeypatch,
):
    project, repo = await _scope(session_factory, suffix="runtime-validation")
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

    response = await client.post(
        "/api/delivery-runs",
        json=_payload(
            project,
            repo,
            provider="claude",
            model="claude-opus-4-6",
        ),
    )

    assert response.status_code == 201, response.text
    assert topology_calls == [(project.id, repo.id)]
    body = response.json()
    async with session_factory() as db:
        run = await db.get(DeliveryRun, body["id"])
        task = await db.get(Task, body["developer_task_id"])
        assert run is not None
        assert task is not None
        assert run.policy_snapshot["provider"] == "claude"
        assert run.policy_snapshot["model"] == "claude-opus-4-6"
        assert task.provider == "claude"
        assert task.model == "claude-opus-4-6"


@pytest.mark.asyncio
async def test_delivery_admission_rejects_codex_fast_for_claude_provider(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="claude-fast")

    response = await client.post(
        "/api/delivery-runs",
        json=_payload(
            project,
            repo,
            provider="claude",
            model="claude-opus-4-6",
            codex_service_tier="priority",
        ),
    )

    assert response.status_code == 400, response.text
    assert "only available" in response.text
    async with session_factory() as db:
        assert await db.scalar(select(func.count(DeliveryRun.id))) == 0
        assert await db.scalar(select(func.count(Task.id))) == 0


@pytest.mark.asyncio
async def test_delivery_admission_requires_caller_idempotency_key(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="missing-idempotency")
    payload = _payload(project, repo)
    payload.pop("idempotency_key")

    response = await client.post("/api/delivery-runs", json=payload)

    assert response.status_code == 422, response.text
    async with session_factory() as db:
        assert await db.scalar(select(func.count(DeliveryRun.id))) == 0
        assert await db.scalar(select(func.count(Task.id))) == 0


@pytest.mark.asyncio
async def test_delivery_admission_replays_same_request_and_conflicts_on_rebind(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="idempotent-replay")
    payload = _payload(
        project,
        repo,
        idempotency_key="api-stable-admission-key",
    )

    first = await client.post("/api/delivery-runs", json=payload)
    replay = await client.post("/api/delivery-runs", json=payload)
    conflict = await client.post(
        "/api/delivery-runs",
        json={**payload, "requirements": "A different request."},
    )

    assert first.status_code == 201, first.text
    assert replay.status_code == 201, replay.text
    assert replay.json()["id"] == first.json()["id"]
    assert replay.json()["developer_task_id"] == first.json()["developer_task_id"]
    assert conflict.status_code == 409, conflict.text
    assert "different Delivery request" in conflict.text
    async with session_factory() as db:
        assert await db.scalar(select(func.count(DeliveryRun.id))) == 1
        assert await db.scalar(select(func.count(Task.id))) == 1
        run = await db.get(DeliveryRun, first.json()["id"])
        assert run is not None
        assert run.admission_scope == "system"
        assert run.idempotency_key == "api-stable-admission-key"
        assert len(run.request_hash) == 64


@pytest.mark.asyncio
async def test_feature_flags_stop_admission_but_keep_existing_run_controls(
    client,
    session_factory,
    delivery_enabled,
    monkeypatch,
):
    project, repo = await _scope(session_factory, suffix="flag-recovery")
    original_payload = _payload(project, repo)
    created = await client.post(
        "/api/delivery-runs",
        json=original_payload,
    )
    assert created.status_code == 201, created.text
    run_id = created.json()["id"]

    monkeypatch.setattr(settings, "delivery_loop_enabled", False)
    monkeypatch.setattr(settings, "capability_core_enabled", False)
    replay = await client.post(
        "/api/delivery-runs",
        json=original_payload,
    )
    rejected_new = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo, title="Not admitted"),
    )
    listed = await client.get("/api/delivery-runs")
    readback = await client.get(f"/api/delivery-runs/{run_id}")
    cancelled = await client.post(
        f"/api/delivery-runs/{run_id}/cancel",
        json={"reason": "dark-launch rollback"},
    )

    assert rejected_new.status_code == 503
    assert replay.status_code == 201, replay.text
    assert replay.json()["id"] == run_id
    assert [item["id"] for item in listed.json()] == [run_id]
    assert readback.status_code == 200
    assert cancelled.status_code == 200, cancelled.text
    assert cancelled.json()["outcome"] == "cancelled"


@pytest.mark.asyncio
async def test_create_is_atomic_and_detail_exposes_public_evidence(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="atomic")

    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )

    assert created.status_code == 201, created.text
    body = created.json()
    assert body["phase"] == "planning"
    assert body["activity"] == "ready"
    assert body["terminal"] == "ready_to_merge"
    assert body["allowed_actions"] == ["pause", "cancel"]
    assert body["delivery_branch"] == (
        f"ccm/delivery/{body['id']}-fix-the-delivery-race"
    )

    detail = await client.get(f"/api/delivery-runs/{body['id']}")
    assert detail.status_code == 200, detail.text
    detail_body = detail.json()
    assert "policy_snapshot" not in detail_body
    assert detail_body["terminal"] == "ready_to_merge"
    assert len(detail_body["cycles"]) == 1
    assert detail_body["cycles"][0]["trigger_kind"] == "initial_request"
    assert detail_body["turns"] == []
    assert [item["cause"] for item in detail_body["transitions"]] == ["created"]

    task_response = await client.get(f"/api/tasks/{body['developer_task_id']}")
    assert task_response.status_code == 200, task_response.text
    task_body = task_response.json()
    assert task_body["mode"] == "delivery_loop"
    assert task_body["status"] == "delivery_waiting"
    assert task_body["delivery_run_id"] == body["id"]
    assert task_body["delivery_role"] == "developer"
    assert task_body["delivery_phase"] == "planning"
    assert task_body["delivery_activity"] == "ready"
    assert task_body["delivery_outcome"] is None
    assert task_body["delivery_terminal"] == "ready_to_merge"

    async with session_factory() as db:
        assert await db.scalar(select(func.count(DeliveryRun.id))) == 1
        assert await db.scalar(select(func.count(DeliveryCycle.id))) == 1
        assert await db.scalar(select(func.count(DeliveryTransition.id))) == 1
        assert await db.scalar(select(func.count(Task.id))) == 1


@pytest.mark.asyncio
async def test_progress_projection_and_attention_count_are_api_authoritative(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="progress")
    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )
    assert created.status_code == 201, created.text
    run_id = created.json()["id"]

    async with session_factory() as db:
        run = await db.get(DeliveryRun, run_id)
        # Controller lease renewals update this timestamp but are not public
        # progress and must not make a stuck Run look busy.
        run.updated_at = datetime(2099, 1, 1)
        await db.commit()

    progress = await client.get(f"/api/delivery-runs/{run_id}/progress")
    count = await client.get("/api/delivery-runs/attention-count")

    assert progress.status_code == 200, progress.text
    body = progress.json()
    assert body["headline"] == "Preparing the implementation plan"
    assert body["attention_required"] is False
    assert body["frontend_review"]["policy"] == "auto"
    assert [stage["key"] for stage in body["stages"]] == [
        "planning",
        "coding",
        "pre_review",
        "frontend_review",
        "publishing",
        "monitoring",
    ]
    assert body["events"][0]["title"] == "Delivery created"
    assert not body["last_activity_at"].startswith("2099-")
    assert count.status_code == 200
    assert count.json() == {"total": 0}

    paused = await client.post(
        f"/api/delivery-runs/{run_id}/pause",
        json={"reason": "Choose a rollout strategy"},
    )
    assert paused.status_code == 200, paused.text
    attention = await client.get("/api/delivery-runs/attention-count")
    paused_progress = await client.get(f"/api/delivery-runs/{run_id}/progress")
    assert attention.json() == {"total": 1}
    assert paused_progress.json()["attention_required"] is True
    assert paused_progress.json()["attention_kind"] == "paused"

    async with session_factory() as db:
        run = await db.get(DeliveryRun, run_id)
        run.phase = "done"
        run.activity = "terminal"
        run.outcome = "failed"
        run.error_code = "test_failure"
        run.error_message = "Inspect this failure in Run detail"
        run.completed_at = datetime.utcnow()
        await db.commit()

    terminal_count = await client.get("/api/delivery-runs/attention-count")
    terminal_progress = await client.get(f"/api/delivery-runs/{run_id}/progress")
    assert terminal_count.json() == {"total": 0}
    assert terminal_progress.json()["attention_required"] is True
    assert terminal_progress.json()["attention_kind"] == "terminal_error"


@pytest.mark.asyncio
async def test_failed_prepublication_run_retries_in_place_with_a_new_cycle(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="operator-retry")
    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )
    assert created.status_code == 201, created.text
    run_id = created.json()["id"]

    async with session_factory() as db:
        run = await db.get(DeliveryRun, run_id)
        cycle = await db.get(DeliveryCycle, run.current_cycle_id)
        task = await db.get(Task, run.developer_task_id)
        delivery_service.complete_cycle(cycle, status="failed")
        await delivery_service.apply_run_event(
            db,
            run=run,
            event=DeliveryReducerEvent(
                "fail",
                {
                    "error_code": "plan_run_failed",
                    "error_message": "Both reviewer routes were unavailable",
                },
            ),
            actor_kind="controller",
        )
        task.status = "failed"
        task.completed_at = datetime.utcnow()
        task.error_message = run.error_message
        await db.commit()
        failed_version = run.state_version
        first_cycle_id = cycle.id

    failed = await client.get(f"/api/delivery-runs/{run_id}")
    assert failed.status_code == 200, failed.text
    assert failed.json()["allowed_actions"] == ["retry"]

    retried = await client.post(
        f"/api/delivery-runs/{run_id}/retry",
        json={
            "expected_state_version": failed_version,
            "reason": "Provider routes recovered",
        },
    )
    assert retried.status_code == 200, retried.text
    body = retried.json()
    assert (body["phase"], body["activity"], body["outcome"]) == (
        "planning",
        "ready",
        None,
    )
    assert body["cycle_count"] == 2
    assert body["allowed_actions"] == ["pause", "cancel"]
    assert body["error_code"] is None
    assert body["completed_at"] is None

    async with session_factory() as db:
        run = await db.get(DeliveryRun, run_id)
        first_cycle = await db.get(DeliveryCycle, first_cycle_id)
        next_cycle = await db.get(DeliveryCycle, run.current_cycle_id)
        task = await db.get(Task, run.developer_task_id)
        transition = await db.scalar(
            select(DeliveryTransition).where(
                DeliveryTransition.run_id == run_id,
                DeliveryTransition.cause == "retry",
            )
        )
        assert first_cycle.status == "failed"
        assert next_cycle.status == "planning"
        assert next_cycle.trigger_kind == "operator_retry"
        assert next_cycle.trigger_payload["previous_cycle_id"] == first_cycle_id
        assert next_cycle.trigger_payload["previous_error_code"] == "plan_run_failed"
        assert transition.metadata_["reason"] == "Provider routes recovered"
        assert task.status == "delivery_waiting"
        assert task.completed_at is None
        assert task.error_message is None

    stale_retry = await client.post(
        f"/api/delivery-runs/{run_id}/retry",
        json={"expected_state_version": failed_version},
    )
    assert stale_retry.status_code == 409
    assert "changed before retry" in stale_retry.text


@pytest.mark.asyncio
async def test_failed_development_retries_from_development_with_approved_plan(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="development-retry")
    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )
    assert created.status_code == 201, created.text
    run_id = created.json()["id"]

    async with session_factory() as db:
        run = await db.get(DeliveryRun, run_id)
        cycle = await db.get(DeliveryCycle, run.current_cycle_id)
        task = await db.get(Task, run.developer_task_id)
        plan = Plan(
            title="Approved Delivery Plan",
            initial_request="Implement the Delivery",
            project_id=project.id,
            pipeline_config=default_plan_pipeline_config().model_dump(),
        )
        db.add(plan)
        await db.flush()
        version = PlanVersion(
            plan_id=plan.id,
            version_number=1,
            content="# Approved Delivery Plan",
        )
        db.add(version)
        await db.flush()
        plan.current_version_id = version.id
        cycle.plan_version_id = version.id
        cycle.status = "coding"
        await delivery_service.apply_run_event(
            db,
            run=run,
            event=DeliveryReducerEvent("plan_requested"),
            actor_kind="controller",
        )
        await delivery_service.apply_run_event(
            db,
            run=run,
            event=DeliveryReducerEvent("plan_ready"),
            actor_kind="capability",
        )
        delivery_service.complete_cycle(cycle, status="failed")
        await delivery_service.apply_run_event(
            db,
            run=run,
            event=DeliveryReducerEvent(
                "fail",
                {
                    "error_code": "developer_turn_failed",
                    "error_message": "Developer transport was interrupted",
                },
            ),
            actor_kind="controller",
        )
        task.status = "failed"
        task.completed_at = datetime.utcnow()
        task.error_message = run.error_message
        await db.commit()
        failed_version = run.state_version
        failed_cycle_id = cycle.id
        plan_version_id = version.id

    retried = await client.post(
        f"/api/delivery-runs/{run_id}/retry",
        json={"expected_state_version": failed_version},
    )

    assert retried.status_code == 200, retried.text
    body = retried.json()
    assert (body["phase"], body["activity"]) == ("coding", "ready")
    assert body["cycle_count"] == 2

    async with session_factory() as db:
        run = await db.get(DeliveryRun, run_id)
        next_cycle = await db.get(DeliveryCycle, run.current_cycle_id)
        assert next_cycle.id != failed_cycle_id
        assert next_cycle.status == "coding"
        assert next_cycle.plan_version_id == plan_version_id
        assert next_cycle.plan_invocation_id is None
        assert next_cycle.trigger_payload["resume_phase"] == "coding"


@pytest.mark.asyncio
async def test_retry_is_hidden_when_cycle_budget_is_exhausted(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="retry-budget")
    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo, max_cycles=1),
    )
    run_id = created.json()["id"]

    async with session_factory() as db:
        run = await db.get(DeliveryRun, run_id)
        cycle = await db.get(DeliveryCycle, run.current_cycle_id)
        task = await db.get(Task, run.developer_task_id)
        delivery_service.complete_cycle(cycle, status="failed")
        await delivery_service.apply_run_event(
            db,
            run=run,
            event=DeliveryReducerEvent(
                "fail",
                {
                    "error_code": "plan_run_failed",
                    "error_message": "Temporary provider failure",
                },
            ),
            actor_kind="controller",
        )
        task.status = "failed"
        task.completed_at = datetime.utcnow()
        await db.commit()
        failed_version = run.state_version

    failed = await client.get(f"/api/delivery-runs/{run_id}")
    assert failed.json()["allowed_actions"] == []
    response = await client.post(
        f"/api/delivery-runs/{run_id}/retry",
        json={"expected_state_version": failed_version},
    )
    assert response.status_code == 409
    assert "remaining cycle budget" in response.text


@pytest.mark.asyncio
async def test_progress_projects_open_plan_input_into_the_delivery_run(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="plan-input")
    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )
    assert created.status_code == 201, created.text
    run_id = created.json()["id"]
    task_id = created.json()["developer_task_id"]

    async with session_factory() as db:
        plan = Plan(
            title="Delivery clarification",
            initial_request="Choose the rollout scope",
            target_task_id=task_id,
            project_id=project.id,
            pipeline_config=default_plan_pipeline_config().model_dump(),
        )
        db.add(plan)
        await db.flush()
        invocation = CapabilityInvocation(
            task_id=task_id,
            capability_key="plan",
            source="delivery_controller",
            purpose="required_gate",
            status="waiting_user",
            state_version=1,
            idempotency_key=f"delivery-plan-input-{run_id}",
            input_payload={"prompt": plan.initial_request},
            input_hash="a" * 64,
            subject_kind="task_generation",
            subject_ref={"task_id": task_id},
            subject_hash="b" * 64,
            executor_kind="plan_agent",
            executor_config={},
            executor_config_hash="c" * 64,
            policy_snapshot={},
            policy_hash="d" * 64,
            resume_policy="controller",
            max_attempts=1,
            active_task_id=task_id,
        )
        db.add(invocation)
        await db.flush()
        execution = CapabilityExecution(
            invocation_id=invocation.id,
            attempt=1,
            status="waiting_user",
            state_version=1,
            active_invocation_id=invocation.id,
            idempotency_key=f"delivery-plan-execution-{run_id}",
            executor_kind="plan_agent",
            input_hash=invocation.input_hash,
        )
        db.add(execution)
        await db.flush()
        plan_run = PlanAgentRun(
            plan_id=plan.id,
            run_type="capability",
            capability_execution_id=execution.id,
            request_text=plan.initial_request,
            pipeline_config=plan.pipeline_config,
            status="waiting_user",
            current_stage="planner",
            generation=1,
            max_interactions=3,
        )
        db.add(plan_run)
        await db.flush()
        input_request = PlanInputRequest(
            plan_id=plan.id,
            run_id=plan_run.id,
            source_step_id=1,
            requested_by="planner",
            reason="The rollout boundary changes the implementation.",
            questions=[
                {
                    "id": "scope",
                    "header": "Scope",
                    "question": "Which rollout scope should be used?",
                    "response_type": "single_choice",
                    "options": [
                        {"label": "One project", "value": "one"},
                        {"label": "All projects", "value": "all"},
                    ],
                    "required": True,
                }
            ],
            status="open",
            idempotency_key=f"delivery-plan-input-request-{run_id}",
            opened_at=datetime.utcnow(),
        )
        db.add(input_request)
        await db.flush()
        plan.active_run_id = plan_run.id
        plan_run.open_input_request_id = input_request.id
        execution.handle_kind = "plan_agent_run"
        execution.handle_id = str(plan_run.id)
        execution.handle_generation = plan_run.generation
        cycle = await db.scalar(
            select(DeliveryCycle).where(DeliveryCycle.run_id == run_id)
        )
        cycle.plan_invocation_id = invocation.id
        await db.commit()

    response = await client.get(f"/api/delivery-runs/{run_id}/progress")
    plan_response = await client.get(f"/api/plans/{plan.id}")
    attention = await client.get("/api/delivery-runs/attention-count")

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["attention_required"] is True
    assert body["attention_kind"] == "plan_input"
    assert body["headline"] == "Plan needs your decision"
    assert body["plan_id"] == plan.id
    assert body["plan_input"]["plan_id"] == plan.id
    assert body["plan_input"]["request"]["questions"][0]["id"] == "scope"
    assert body["plan_input"]["run"] == {
        "id": plan_run.id,
        "generation": 1,
        "status": "waiting_user",
        "current_stage": "planner",
    }
    assert "answered_by" not in body["plan_input"]["request"]
    assert plan_response.status_code == 200, plan_response.text
    assert plan_response.json()["delivery_run_id"] == run_id
    assert attention.json() == {"total": 1}

    # A historical Plan may remain waiting while a newer cycle is current.
    # Attention must follow the current cycle's exact invocation, not any
    # waiting Plan ever attached to the reused Developer Task.
    async with session_factory() as db:
        stored = await db.get(DeliveryRun, run_id)
        old_cycle = await db.get(DeliveryCycle, stored.current_cycle_id)
        old_cycle.status = "completed"
        old_cycle.active_run_id = None
        old_cycle.completed_at = datetime.utcnow()
        await db.flush()
        current_cycle = DeliveryCycle(
            run_id=stored.id,
            cycle_number=2,
            active_run_id=stored.id,
            status="planning",
            state_version=1,
            trigger_kind="test_new_cycle",
            trigger_payload={},
            trigger_hash="e" * 64,
        )
        db.add(current_cycle)
        await db.flush()
        stored.current_cycle_id = current_cycle.id
        stored.cycle_count = 2
        await db.commit()

    historical_attention = await client.get("/api/delivery-runs/attention-count")
    assert historical_attention.json() == {"total": 0}


@pytest.mark.asyncio
async def test_delivery_plan_projection_uses_cycle_version_relationship(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="plan-projection")
    created = await client.post("/api/delivery-runs", json=_payload(project, repo))
    assert created.status_code == 201, created.text
    run_id = created.json()["id"]

    async with session_factory() as db:
        delivery = await db.get(DeliveryRun, run_id)
        assert delivery is not None
        developer_task_id = delivery.developer_task_id
        assert developer_task_id is not None
        plan = Plan(
            title="Delivery Plan",
            initial_request="Plan the Delivery",
            project_id=project.id,
            pipeline_config=default_plan_pipeline_config().model_dump(),
        )
        db.add(plan)
        await db.flush()
        version = PlanVersion(
            plan_id=plan.id,
            version_number=1,
            content="# Delivery Plan",
        )
        db.add(version)
        await db.flush()
        plan.current_version_id = version.id
        cycle = await db.scalar(
            select(DeliveryCycle).where(DeliveryCycle.run_id == run_id)
        )
        cycle.plan_version_id = version.id
        await db.commit()
        plan_id = plan.id

    response = await client.get(f"/api/plans/{plan_id}")
    catalog = await client.get("/api/plans", params={"q": "Delivery Plan"})
    catalog_count = await client.get(
        "/api/plans/count", params={"q": "Delivery Plan"}
    )
    tasks = await client.get("/api/tasks", params={"limit": 1000})

    assert response.status_code == 200, response.text
    assert response.json()["delivery_run_id"] == run_id
    assert catalog.status_code == 200, catalog.text
    assert all(item["id"] != plan_id for item in catalog.json())
    assert catalog_count.status_code == 200, catalog_count.text
    assert catalog_count.json() == {"total": 0}
    assert tasks.status_code == 200, tasks.text
    assert all(item["id"] != developer_task_id for item in tasks.json())


@pytest.mark.asyncio
async def test_create_projects_frozen_auto_merge_terminal(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(
        session_factory,
        suffix="auto-terminal",
        auto_merge=True,
    )

    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )

    assert created.status_code == 201, created.text
    body = created.json()
    assert body["terminal"] == "merged"
    task_response = await client.get(f"/api/tasks/{body['developer_task_id']}")
    assert task_response.status_code == 200, task_response.text
    assert task_response.json()["delivery_terminal"] == "merged"


@pytest.mark.asyncio
async def test_active_run_freezes_project_identity_and_destructive_actions(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="project-freeze")
    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )
    assert created.status_code == 201, created.text

    responses = [
        await client.put(
            f"/api/projects/{project.id}",
            json={"git_url": "git@github.com:acme/other.git"},
        ),
        await client.put(
            f"/api/projects/{project.id}",
            json={"default_branch": "develop"},
        ),
        await client.put(
            f"/api/projects/{project.id}",
            json={"has_remote": False},
        ),
        await client.post(f"/api/projects/{project.id}/reclone"),
        await client.delete(f"/api/projects/{project.id}"),
    ]

    assert [response.status_code for response in responses] == [409] * 5
    assert all("Delivery Run" in response.text for response in responses)
    cosmetic = await client.put(
        f"/api/projects/{project.id}",
        json={"badge_color": "blue"},
    )
    assert cosmetic.status_code == 200, cosmetic.text
    async with session_factory() as db:
        persisted = await db.get(Project, project.id)
        assert persisted is not None
        assert persisted.git_url == project.git_url
        assert persisted.default_branch == "main"
        assert persisted.has_remote is True


@pytest.mark.asyncio
async def test_active_run_freezes_monitor_policy_disable_secret_and_delete(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="monitor-freeze")
    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )
    assert created.status_code == 201, created.text

    responses = [
        await client.put(
            f"/api/pr-monitor/repos/{repo.id}",
            json={"project_id": None},
        ),
        await client.put(
            f"/api/pr-monitor/repos/{repo.id}",
            json={"default_branch": "develop"},
        ),
        await client.put(
            f"/api/pr-monitor/repos/{repo.id}",
            json={"merge_queue_mode": "auto"},
        ),
        await client.put(
            f"/api/pr-monitor/repos/{repo.id}",
            json={"auto_merge": True},
        ),
        await client.put(
            f"/api/pr-monitor/repos/{repo.id}",
            json={"enabled": False},
        ),
        await client.post(f"/api/pr-monitor/repos/{repo.id}/toggle"),
        await client.post(f"/api/pr-monitor/repos/{repo.id}/regenerate-secret"),
        await client.delete(f"/api/pr-monitor/repos/{repo.id}"),
    ]

    assert [response.status_code for response in responses] == [409] * 8
    assert all("Delivery Run" in response.text for response in responses)
    no_op = await client.put(
        f"/api/pr-monitor/repos/{repo.id}",
        json={"default_branch": "main"},
    )
    assert no_op.status_code == 200, no_op.text
    async with session_factory() as db:
        persisted = await db.get(MonitoredRepo, repo.id)
        assert persisted is not None
        assert persisted.project_id == project.id
        assert persisted.enabled is True
        assert persisted.auto_merge is False
        assert persisted.merge_queue_mode == "manual"
        assert persisted.default_branch == "main"


@pytest.mark.asyncio
async def test_terminal_run_allows_identity_updates_but_preserves_scope_history(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="terminal-scope")
    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )
    cancelled = await client.post(
        f"/api/delivery-runs/{created.json()['id']}/cancel",
        json={"reason": "scope guard test"},
    )
    assert cancelled.status_code == 200, cancelled.text

    project_update = await client.put(
        f"/api/projects/{project.id}",
        json={"default_branch": "develop"},
    )
    repo_update = await client.put(
        f"/api/pr-monitor/repos/{repo.id}",
        json={"default_branch": "develop"},
    )
    repo_delete = await client.delete(f"/api/pr-monitor/repos/{repo.id}")
    project_delete = await client.delete(f"/api/projects/{project.id}")

    assert project_update.status_code == 200, project_update.text
    assert repo_update.status_code == 200, repo_update.text
    assert repo_delete.status_code == 409
    assert project_delete.status_code == 409
    assert "referenced by Delivery Run" in repo_delete.text
    assert "referenced by Delivery Run" in project_delete.text


@pytest.mark.asyncio
async def test_create_rolls_back_run_task_cycle_and_todo_on_late_failure(
    client,
    session_factory,
    delivery_enabled,
    monkeypatch,
):
    project, repo = await _scope(session_factory, suffix="rollback")
    async with session_factory() as db:
        todo = ProjectTodo(
            project_id=project.id,
            title="Atomic source",
            prompt="Keep this open if admission fails.",
            status="open",
        )
        db.add(todo)
        await db.commit()
        await db.refresh(todo)
        todo_id = todo.id

    async def fail_after_task_stage(*args, **kwargs):
        raise DeliveryValidationError("injected cycle failure")

    monkeypatch.setattr(
        delivery_service,
        "start_next_cycle",
        fail_after_task_stage,
    )
    response = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo, source_todo_id=todo_id),
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "injected cycle failure"
    async with session_factory() as db:
        assert await db.scalar(select(func.count(DeliveryRun.id))) == 0
        assert await db.scalar(select(func.count(DeliveryCycle.id))) == 0
        assert await db.scalar(select(func.count(Task.id))) == 0
        todo = await db.get(ProjectTodo, todo_id)
        assert todo is not None
        assert todo.status == "open"
        assert todo.created_task_id is None


@pytest.mark.asyncio
async def test_source_todo_provenance_is_atomic_and_project_scoped(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="todo")
    other_project, _ = await _scope(session_factory, suffix="todo-other")
    async with session_factory() as db:
        source = ProjectTodo(
            project_id=project.id,
            title="Todo source",
            prompt="Implement it through Delivery Loop.",
            status="open",
        )
        foreign = ProjectTodo(
            project_id=other_project.id,
            title="Foreign source",
            prompt="Must not be attached to another project.",
            status="open",
        )
        db.add_all([source, foreign])
        await db.commit()
        await db.refresh(source)
        await db.refresh(foreign)
        source_id = source.id
        foreign_id = foreign.id

    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo, source_todo_id=source_id),
    )
    rejected = await client.post(
        "/api/delivery-runs",
        json=_payload(
            project,
            repo,
            source_todo_id=foreign_id,
            title="Do not create",
        ),
    )

    assert created.status_code == 201, created.text
    assert rejected.status_code == 400
    assert "does not belong" in rejected.json()["detail"]
    async with session_factory() as db:
        source = await db.get(ProjectTodo, source_id)
        foreign = await db.get(ProjectTodo, foreign_id)
        assert source is not None
        assert source.status == "done"
        assert source.created_task_id == created.json()["developer_task_id"]
        assert foreign is not None
        assert foreign.status == "open"
        assert foreign.created_task_id is None
        assert await db.scalar(select(func.count(DeliveryRun.id))) == 1


@pytest.mark.asyncio
async def test_source_todo_conditional_claim_rejects_duplicate_run(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="todo-claim")
    async with session_factory() as db:
        todo = ProjectTodo(
            project_id=project.id,
            title="Claim exactly once",
            prompt="A retry must recover the first result, not fork provenance.",
            status="open",
        )
        db.add(todo)
        await db.commit()
        await db.refresh(todo)
        todo_id = todo.id

    first = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo, source_todo_id=todo_id),
    )
    duplicate = await client.post(
        "/api/delivery-runs",
        json=_payload(
            project,
            repo,
            source_todo_id=todo_id,
            title="Duplicate request",
        ),
    )

    assert first.status_code == 201, first.text
    assert duplicate.status_code == 409, duplicate.text
    assert "already owned by Delivery Run" in duplicate.json()["detail"]
    async with session_factory() as db:
        todo = await db.get(ProjectTodo, todo_id)
        assert todo is not None
        assert todo.status == "done"
        assert todo.created_task_id == first.json()["developer_task_id"]
        assert await db.scalar(select(func.count(DeliveryRun.id))) == 1
        assert await db.scalar(select(func.count(Task.id))) == 1
        assert await db.scalar(select(func.count(DeliveryCycle.id))) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "forged",
    [
        {"mode": "delivery_loop"},
        {"delivery_run_id": 1},
        {"delivery_run_id": 0},
        {"delivery_role": "developer"},
    ],
)
async def test_public_task_create_rejects_delivery_controller_forgery(
    client,
    session_factory,
    forged,
):
    response = await client.post(
        "/api/tasks",
        json={
            "title": "Forged Delivery Task",
            "description": "Do not admit through the ordinary Task API.",
            **forged,
        },
    )

    assert response.status_code == 422, response.text
    async with session_factory() as db:
        assert await db.scalar(select(func.count(Task.id))) == 0


@pytest.mark.asyncio
async def test_public_task_create_tolerates_null_delivery_readback_fields(
    client,
    session_factory,
):
    response = await client.post(
        "/api/tasks",
        json={
            "title": "Ordinary Task",
            "description": "Null readback fields carry no ownership claim.",
            "delivery_run_id": None,
            "delivery_role": None,
        },
    )

    assert response.status_code == 201, response.text
    async with session_factory() as db:
        task = await db.get(Task, response.json()["id"])
        assert task is not None
        assert task.mode == "auto"
        assert task.delivery_run_id is None
        assert task.delivery_role is None


@pytest.mark.asyncio
async def test_pause_resume_cancel_state_contract(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="commands")
    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )
    run_id = created.json()["id"]

    paused = await client.post(
        f"/api/delivery-runs/{run_id}/pause",
        json={"reason": "operator maintenance"},
    )
    duplicate_pause = await client.post(
        f"/api/delivery-runs/{run_id}/pause",
        json={"reason": "again"},
    )
    paused_cancel = await client.post(
        f"/api/delivery-runs/{run_id}/cancel",
        json={"reason": "paused cancellation is unsafe"},
    )
    resumed = await client.post(
        f"/api/delivery-runs/{run_id}/resume",
        json={"reason": "maintenance complete"},
    )
    duplicate_resume = await client.post(
        f"/api/delivery-runs/{run_id}/resume",
        json={},
    )
    cancelled = await client.post(
        f"/api/delivery-runs/{run_id}/cancel",
        json={"reason": "request withdrawn"},
    )
    terminal_cancel = await client.post(
        f"/api/delivery-runs/{run_id}/cancel",
        json={"reason": "again"},
    )

    assert paused.status_code == 200, paused.text
    assert paused.json()["activity"] == "paused"
    assert paused.json()["pause_reason"] == "operator maintenance"
    assert paused.json()["allowed_actions"] == ["resume"]
    assert duplicate_pause.status_code == 409
    assert paused_cancel.status_code == 409
    assert "only be resumed" in paused_cancel.text
    assert resumed.status_code == 200, resumed.text
    assert resumed.json()["activity"] == "ready"
    assert resumed.json()["allowed_actions"] == ["pause", "cancel"]
    assert duplicate_resume.status_code == 409
    assert cancelled.status_code == 200, cancelled.text
    assert cancelled.json()["phase"] == "done"
    assert cancelled.json()["activity"] == "terminal"
    assert cancelled.json()["outcome"] == "cancelled"
    assert cancelled.json()["allowed_actions"] == []
    assert terminal_cancel.status_code == 409

    detail = await client.get(f"/api/delivery-runs/{run_id}")
    transitions = detail.json()["transitions"]
    assert [item["cause"] for item in transitions] == [
        "created",
        "pause",
        "resume",
        "cancel",
    ]
    assert all("metadata" not in item for item in transitions)
    assert all("actor_id" not in item for item in transitions)
    async with session_factory() as db:
        run = await db.get(DeliveryRun, run_id)
        cycle = await db.get(DeliveryCycle, run.current_cycle_id)
        task = await db.get(Task, run.developer_task_id)
        assert cycle.status == "cancelled"
        assert cycle.active_run_id is None
        assert task.status == "cancelled"
        assert task.completed_at is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("phase", "activity", "wait_reason"),
    [
        ("coding", "running", None),
        ("planning", "waiting", "plan_capability"),
        ("pre_review", "waiting", "code_review_capability"),
    ],
)
async def test_commands_reject_active_exact_generation_effects(
    client,
    session_factory,
    delivery_enabled,
    phase,
    activity,
    wait_reason,
):
    project, repo = await _scope(
        session_factory,
        suffix=f"active-{phase}-{activity}",
    )
    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )
    run_id = created.json()["id"]
    async with session_factory() as db:
        run = await db.get(DeliveryRun, run_id)
        assert run is not None
        run.phase = phase
        run.activity = activity
        run.wait_reason = wait_reason
        await db.commit()

    responses = [
        await client.post(
            f"/api/delivery-runs/{run_id}/pause",
            json={"reason": "unsafe"},
        ),
        await client.post(
            f"/api/delivery-runs/{run_id}/cancel",
            json={"reason": "unsafe"},
        ),
    ]

    assert [response.status_code for response in responses] == [409, 409]
    assert all("exact-generation" in response.text for response in responses)
    readback = await client.get(f"/api/delivery-runs/{run_id}")
    assert readback.json()["allowed_actions"] == []
    assert readback.json()["phase"] == phase
    assert readback.json()["activity"] == activity


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("phase", "paused_from_activity"),
    [
        ("coding", "running"),
        ("publishing", "running"),
        ("planning", "waiting"),
        ("pre_review", "waiting"),
    ],
)
async def test_cancel_rejects_paused_exact_generation_effects(
    client,
    session_factory,
    delivery_enabled,
    phase,
    paused_from_activity,
):
    project, repo = await _scope(
        session_factory,
        suffix=f"paused-{phase}-{paused_from_activity}",
    )
    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )
    run_id = created.json()["id"]
    async with session_factory() as db:
        run = await db.get(DeliveryRun, run_id)
        assert run is not None
        run.phase = phase
        run.activity = "paused"
        run.wait_reason = None
        run.paused_from_activity = paused_from_activity
        run.pause_reason = "controller preserved an exact active generation"
        await db.commit()

    before = await client.get(f"/api/delivery-runs/{run_id}")
    cancelled = await client.post(
        f"/api/delivery-runs/{run_id}/cancel",
        json={"reason": "do not orphan the active generation"},
    )

    assert before.status_code == 200, before.text
    assert before.json()["allowed_actions"] == ["resume"]
    assert cancelled.status_code == 409, cancelled.text
    assert "exact-generation" in cancelled.text
    readback = await client.get(f"/api/delivery-runs/{run_id}")
    assert readback.json()["phase"] == phase
    assert readback.json()["activity"] == "paused"
    assert readback.json()["outcome"] is None
    assert readback.json()["allowed_actions"] == ["resume"]

    async with session_factory() as db:
        causes = list(
            (
                await db.execute(
                    select(DeliveryTransition.cause).where(
                        DeliveryTransition.run_id == run_id
                    )
                )
            ).scalars()
        )
        assert causes == ["created"]


@pytest.mark.asyncio
async def test_monitor_wait_rejects_pause_and_cancel_without_effect_fence(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="monitor-wait")
    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )
    run_id = created.json()["id"]
    async with session_factory() as db:
        run = await db.get(DeliveryRun, run_id)
        assert run is not None
        run.phase = "monitoring"
        run.activity = "waiting"
        run.wait_reason = "pr_monitor"
        await db.commit()

    before = await client.get(f"/api/delivery-runs/{run_id}")
    paused = await client.post(
        f"/api/delivery-runs/{run_id}/pause",
        json={"reason": "hold publication result"},
    )
    cancelled = await client.post(
        f"/api/delivery-runs/{run_id}/cancel",
        json={"reason": "no longer needed"},
    )

    assert before.json()["allowed_actions"] == []
    assert paused.status_code == 409, paused.text
    assert cancelled.status_code == 409, cancelled.text
    assert "exact-generation" in paused.text
    assert "exact-generation" in cancelled.text
    readback = await client.get(f"/api/delivery-runs/{run_id}")
    assert readback.json()["phase"] == "monitoring"
    assert readback.json()["activity"] == "waiting"
    assert readback.json()["wait_reason"] == "pr_monitor"
    assert readback.json()["outcome"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("fence_kind", ["pr_number", "active_action"])
async def test_ready_run_rejects_commands_after_publication_side_effect(
    client,
    session_factory,
    delivery_enabled,
    fence_kind,
):
    project, repo = await _scope(
        session_factory,
        suffix=f"publication-fence-{fence_kind}",
    )
    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )
    run_id = created.json()["id"]
    async with session_factory() as db:
        run = await db.get(DeliveryRun, run_id)
        assert run is not None
        if fence_kind == "pr_number":
            run.pr_number = 73
        else:
            db.add(
                DeliveryAction(
                    run_id=run.id,
                    cycle_id=run.current_cycle_id,
                    active_run_id=run.id,
                    action_type="publish_pr",
                    idempotency_key=f"api-publication-fence-{run.id}",
                    desired_version=run.state_version,
                    payload={},
                    payload_hash="e" * 64,
                    status="pending",
                )
            )
        await db.commit()

    before = await client.get(f"/api/delivery-runs/{run_id}")
    paused = await client.post(
        f"/api/delivery-runs/{run_id}/pause",
        json={"reason": "unsafe after publication"},
    )
    cancelled = await client.post(
        f"/api/delivery-runs/{run_id}/cancel",
        json={"reason": "unsafe after publication"},
    )

    assert before.json()["allowed_actions"] == []
    assert paused.status_code == 409, paused.text
    assert cancelled.status_code == 409, cancelled.text
    expected = (
        "side-effect fence" if fence_kind == "pr_number" else "publication action"
    )
    assert expected in paused.text
    assert expected in cancelled.text


@pytest.mark.asyncio
async def test_locked_state_is_rechecked_after_acl_snapshot(
    client,
    session_factory,
    delivery_enabled,
    monkeypatch,
):
    project, repo = await _scope(session_factory, suffix="stale-command")
    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )
    run_id = created.json()["id"]
    real_lock_run = delivery_api.lock_run

    async def controller_won_race(db, requested_run_id):
        run = await real_lock_run(db, requested_run_id)
        # Emulate the controller transition becoming visible only at the
        # locked read.  The command must recheck this fresh generation before
        # applying pause/cancel.
        run.phase = "coding"
        run.activity = "running"
        run.wait_reason = None
        return run

    monkeypatch.setattr(delivery_api, "lock_run", controller_won_race)
    response = await client.post(
        f"/api/delivery-runs/{run_id}/pause",
        json={"reason": "stale operator click"},
    )

    assert response.status_code == 409
    assert "exact-generation" in response.text
    async with session_factory() as db:
        # The emulated concurrent state was in the rejected command's
        # transaction and is rolled back; importantly, no pause transition was
        # committed from the stale ready snapshot.
        run = await db.get(DeliveryRun, run_id)
        assert run is not None
        assert run.activity == "ready"
        causes = list(
            (
                await db.execute(
                    select(DeliveryTransition.cause).where(
                        DeliveryTransition.run_id == run_id
                    )
                )
            ).scalars()
        )
        assert causes == ["created"]


@pytest.mark.asyncio
async def test_commands_cannot_cross_active_controller_lease(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="active-lease")
    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )
    run_id = created.json()["id"]
    async with session_factory() as db:
        run = await db.get(DeliveryRun, run_id)
        assert run is not None
        run.lease_owner = "controller-between-effect-and-state"
        run.controller_generation += 1
        await db.commit()

    before = await client.get(f"/api/delivery-runs/{run_id}")
    responses = [
        await client.post(
            f"/api/delivery-runs/{run_id}/{command}",
            json={"reason": "must serialize with controller"},
        )
        for command in ("pause", "cancel")
    ]

    assert before.status_code == 200, before.text
    assert before.json()["allowed_actions"] == []
    assert [response.status_code for response in responses] == [409, 409]
    assert all("lease" in response.text for response in responses)
    async with session_factory() as db:
        run = await db.get(DeliveryRun, run_id)
        assert run is not None
        assert (run.phase, run.activity, run.outcome) == (
            "planning",
            "ready",
            None,
        )
        causes = list(
            (
                await db.execute(
                    select(DeliveryTransition.cause).where(
                        DeliveryTransition.run_id == run_id
                    )
                )
            ).scalars()
        )
        assert causes == ["created"]


@pytest.mark.asyncio
async def test_delivery_acl_and_deep_filtered_pagination(
    secured_client,
    monkeypatch,
):
    client, session_factory = secured_client
    alice_id, alice_token = await _create_user(
        session_factory,
        email="delivery-alice@example.com",
        role="member",
    )
    bob_id, _ = await _create_user(
        session_factory,
        email="delivery-bob@example.com",
        role="member",
    )
    visible_project, visible_repo = await _scope(
        session_factory,
        suffix="acl-visible",
    )
    hidden_project, hidden_repo = await _scope(
        session_factory,
        suffix="acl-hidden",
    )
    async with session_factory() as db:
        db.add(
            TeamProjectShare(
                project_id=visible_project.id,
                target_type="user",
                target_id=alice_id,
                shared_by=bob_id,
            )
        )
        await db.commit()

    monkeypatch.setattr(settings, "delivery_loop_enabled", True)
    monkeypatch.setattr(settings, "capability_core_enabled", True)
    monkeypatch.setattr(delivery_api, "_wake_controller", lambda: None)
    headers = {"Authorization": f"Bearer {alice_token}"}

    unsigned = await client.get("/api/delivery-runs")
    visible = await client.post(
        "/api/delivery-runs",
        headers=headers,
        json=_payload(visible_project, visible_repo, title="Visible older run"),
    )
    assert visible.status_code == 201, visible.text
    visible_id = visible.json()["id"]

    # More than the old ``limit * 4`` candidate window are newer but hidden.
    # The ACL-filtered list must keep scanning and still return the visible Run.
    async with session_factory() as db:
        hidden_ids = []
        for index in range(5):
            run = await create_delivery_run(
                db,
                DeliveryCreateSpec(
                    idempotency_key=f"hidden-listing-{index}",
                    project_id=hidden_project.id,
                    monitored_repo_id=hidden_repo.id,
                    title=f"Hidden newer run {index}",
                    requirements="Private Delivery evidence.",
                    created_by=bob_id,
                ),
            )
            hidden_ids.append(run.id)

    all_visible = await client.get(
        "/api/delivery-runs?limit=1",
        headers=headers,
    )
    visible_offset = await client.get(
        "/api/delivery-runs?limit=1&offset=1",
        headers=headers,
    )
    visible_project_list = await client.get(
        f"/api/delivery-runs?project_id={visible_project.id}",
        headers=headers,
    )
    forbidden_project_list = await client.get(
        f"/api/delivery-runs?project_id={hidden_project.id}",
        headers=headers,
    )
    forbidden_detail = await client.get(
        f"/api/delivery-runs/{hidden_ids[-1]}",
        headers=headers,
    )
    allowed_detail = await client.get(
        f"/api/delivery-runs/{visible_id}",
        headers=headers,
    )

    assert unsigned.status_code == 401
    assert all_visible.status_code == 200, all_visible.text
    assert [item["id"] for item in all_visible.json()] == [visible_id]
    assert visible_offset.status_code == 200
    assert visible_offset.json() == []
    assert [item["id"] for item in visible_project_list.json()] == [visible_id]
    assert forbidden_project_list.status_code == 403
    assert forbidden_detail.status_code == 403
    assert allowed_detail.status_code == 200
    assert "created_by" not in allowed_detail.json()


@pytest.mark.asyncio
async def test_delivery_human_projection_omits_machine_identity_and_host_paths(
    client,
    session_factory,
    delivery_enabled,
):
    project, repo = await _scope(session_factory, suffix="public-projection")
    created = await client.post(
        "/api/delivery-runs",
        json=_payload(project, repo),
    )
    assert created.status_code == 201, created.text
    run_id = created.json()["id"]
    private_root = "/srv/private/delivery-public-projection"
    private_workspace = f"{private_root}/.claude-manager/worktrees/delivery-{run_id}"

    async with session_factory() as db:
        run = await db.get(DeliveryRun, run_id)
        cycle = await db.get(DeliveryCycle, run.current_cycle_id)
        transition = await db.scalar(
            select(DeliveryTransition).where(DeliveryTransition.run_id == run_id)
        )
        run.workspace_path = private_workspace
        run.head_tree_sha = "1" * 40
        run.patch_sha256 = "2" * 64
        run.wait_reason = f"waiting under {private_workspace}/controller.sock"
        run.error_message = f"failed under {private_workspace}/secret.txt"
        run.policy_snapshot = {
            **run.policy_snapshot,
            "internal_controller_config": "must-not-cross-human-api",
        }
        cycle.trigger_payload = {
            "summary": f"reviewed {private_workspace}/src/app.py",
            "review_result_id": 771,
            "test_harness_run_id": "f" * 32,
            "turn_id": 772,
            "findings": [
                {
                    "title": "Visible finding",
                    "path": f"{private_workspace}/src/app.py",
                    "severity": "high",
                    "internal_receipt": "must-not-cross-human-api",
                }
            ],
        }
        cycle.result_head_tree_sha = "3" * 40
        cycle.result_patch_sha256 = "4" * 64
        cycle.error_message = f"cycle error at {private_root}/internal.log"
        transition.actor_id = "controller-owner-secret"
        transition.metadata_ = {
            "reason": f"controller inspected {private_workspace}/internal.log",
            "capability_invocation_id": 776,
        }
        transition.before_state = {
            **transition.before_state,
            "machine_path": private_workspace,
        }
        db.add(
            DeliveryTurn(
                run_id=run.id,
                cycle_id=cycle.id,
                generation=1,
                correlation_id="internal-correlation-secret",
                active_run_id=None,
                purpose="code",
                trigger_kind="plan_ready",
                trigger_payload={"capability_invocation_id": 777},
                prompt_payload={"private_path": private_workspace},
                prompt_hash="5" * 64,
                status="completed",
                task_id=run.developer_task_id,
                task_retry_count=7,
                task_instance_id=778,
                task_started_at=datetime.utcnow(),
                task_session_id="internal-session-secret",
                source_log_id=779,
                checkpoint={
                    "previous_session_id": "previous-session-secret",
                    "workspace": private_workspace,
                },
                checkpoint_status="admitted",
                attempts=1,
                last_error=f"turn failed at {private_workspace}/turn.log",
                completed_at=datetime.utcnow(),
            )
        )
        await db.commit()

    detail = await client.get(f"/api/delivery-runs/{run_id}")
    progress = await client.get(f"/api/delivery-runs/{run_id}/progress")

    assert detail.status_code == 200, detail.text
    assert progress.status_code == 200, progress.text
    body = detail.json()
    for field in (
        "created_by",
        "worktree_id",
        "workspace_path",
        "requirements_hash",
        "policy_hash",
        "policy_snapshot",
        "head_tree_sha",
        "patch_sha256",
        "head_generation",
        "next_reconcile_at",
    ):
        assert field not in body
    assert body["wait_reason"] == ("waiting under [delivery-workspace]/controller.sock")
    assert body["error_message"] == "failed under [delivery-workspace]/secret.txt"

    cycle_body = body["cycles"][0]
    for field in (
        "plan_invocation_id",
        "review_invocation_id",
        "review_result_id",
        "result_head_tree_sha",
        "result_patch_sha256",
    ):
        assert field not in cycle_body
    assert cycle_body["trigger_payload"] == {
        "summary": "reviewed [delivery-workspace]/src/app.py",
        "findings": [
            {
                "severity": "high",
                "title": "Visible finding",
                "path": "[delivery-workspace]/src/app.py",
            }
        ],
    }

    turn_body = body["turns"][0]
    for field in (
        "correlation_id",
        "trigger_payload",
        "task_retry_count",
        "task_instance_id",
        "task_session_id",
        "checkpoint",
        "checkpoint_status",
    ):
        assert field not in turn_body
    assert turn_body["last_error"].startswith("turn failed at [delivery-workspace]/")

    transition_body = body["transitions"][0]
    for field in ("actor_id", "before_state", "after_state", "metadata"):
        assert field not in transition_body

    serialized = json.dumps(
        {"detail": body, "progress": progress.json()},
        sort_keys=True,
    )
    for secret in (
        private_root,
        "controller-owner-secret",
        "internal-correlation-secret",
        "internal-session-secret",
        "previous-session-secret",
        "must-not-cross-human-api",
    ):
        assert secret not in serialized
    assert progress.json()["events"][0]["id"] == "event:1"


@pytest.mark.asyncio
async def test_chat_only_task_share_cannot_read_or_control_delivery_run(
    secured_client,
    delivery_enabled,
):
    client, session_factory = secured_client
    member_id, member_token = await _create_user(
        session_factory,
        email="delivery-chat-only@example.com",
        role="member",
    )
    sharer_id, _ = await _create_user(
        session_factory,
        email="delivery-chat-sharer@example.com",
        role="admin",
    )
    project, repo = await _scope(session_factory, suffix="chat-only")
    admin_headers = {"Authorization": "Bearer security-service-token"}
    member_headers = {"Authorization": f"Bearer {member_token}"}
    created = await client.post(
        "/api/delivery-runs",
        headers=admin_headers,
        json=_payload(project, repo),
    )
    assert created.status_code == 201, created.text
    run_id = created.json()["id"]
    task_id = created.json()["developer_task_id"]
    state_version = created.json()["state_version"]
    async with session_factory() as db:
        db.add(
            TeamTaskShare(
                task_id=task_id,
                target_type="user",
                target_id=member_id,
                permission="chat",
                shared_by=sharer_id,
            )
        )
        await db.commit()

    shared_task = await client.get(
        f"/api/tasks/{task_id}",
        headers=member_headers,
    )
    assert shared_task.status_code == 200, shared_task.text
    assert shared_task.json()["access_scope"] == "chat"
    assert shared_task.json()["delivery_run_id"] == run_id

    listing = await client.get("/api/delivery-runs", headers=member_headers)
    detail = await client.get(
        f"/api/delivery-runs/{run_id}",
        headers=member_headers,
    )
    progress = await client.get(
        f"/api/delivery-runs/{run_id}/progress",
        headers=member_headers,
    )
    denied_create = await client.post(
        "/api/delivery-runs",
        headers=member_headers,
        json=_payload(
            project,
            repo,
            idempotency_key="chat-share-must-not-create",
        ),
    )
    denied_quick_start = await client.post(
        "/api/delivery-runs/quick-start",
        headers=member_headers,
        json={
            "idempotency_key": "chat-share-must-not-quick-start",
            "project_id": project.id,
            "requirements": "must not start",
        },
    )
    command_responses = [
        await client.post(
            f"/api/delivery-runs/{run_id}/pause",
            headers=member_headers,
            json={"reason": "must not pause"},
        ),
        await client.post(
            f"/api/delivery-runs/{run_id}/resume",
            headers=member_headers,
            json={},
        ),
        await client.post(
            f"/api/delivery-runs/{run_id}/cancel",
            headers=member_headers,
            json={"reason": "must not cancel"},
        ),
        await client.post(
            f"/api/delivery-runs/{run_id}/retry",
            headers=member_headers,
            json={"expected_state_version": state_version},
        ),
    ]

    assert listing.status_code == 200
    assert listing.json() == []
    assert detail.status_code == 403
    assert progress.status_code == 403
    assert denied_create.status_code == 403
    assert denied_quick_start.status_code == 403
    assert [response.status_code for response in command_responses] == [
        403,
        403,
        403,
        403,
    ]
    async with session_factory() as db:
        run = await db.get(DeliveryRun, run_id)
        assert run.state_version == state_version
        assert (run.phase, run.activity) == ("planning", "ready")
        assert (
            await db.scalar(
                select(func.count(DeliveryTransition.id)).where(
                    DeliveryTransition.run_id == run_id
                )
            )
            == 1
        )
