"""Tests for System API endpoints."""
from unittest.mock import AsyncMock, MagicMock

import pytest

from backend.models.instance import Instance
from backend.models.pr_monitor import MonitoredRepo, PRReview, PRReviewerRun
from backend.models.task import Task


def test_interactive_frontend_is_manager_only():
    from backend.main import _serve_interactive_frontend

    assert _serve_interactive_frontend("manager") is True
    assert _serve_interactive_frontend("worker") is False


@pytest.mark.asyncio
async def test_health(client):
    resp = await client.get("/api/system/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert "commit" in body  # Manager/Worker 版本锁定校验用


@pytest.mark.asyncio
async def test_cors_exposes_pr_fix_download_credentials(client):
    resp = await client.get(
        "/api/system/health",
        headers={"Origin": "https://ccm.example.test"},
    )

    assert resp.status_code == 200
    exposed = {
        item.strip().lower()
        for item in resp.headers["access-control-expose-headers"].split(",")
    }
    assert "x-ccm-pr-fix-receipt" in exposed
    assert "x-ccm-pr-fix-token" in exposed


@pytest.mark.asyncio
async def test_stats_empty(client):
    resp = await client.get("/api/system/stats")
    assert resp.status_code == 200
    data = resp.json()
    assert data["tasks"]["pending"] == 0
    assert data["tasks"]["completed"] == 0
    assert data["running_instances"] == 0


@pytest.mark.asyncio
async def test_stats_with_tasks(client, session_factory):
    # Create tasks in various statuses
    await client.post("/api/tasks", json={"title": "A", "description": "d", "target_repo": "/tmp"})
    await client.post("/api/tasks", json={"title": "B", "description": "d", "target_repo": "/tmp"})
    create3 = await client.post("/api/tasks", json={"title": "C", "description": "d", "target_repo": "/tmp"})
    # Cancel one to change its status
    await client.post(f"/api/tasks/{create3.json()['id']}/cancel")

    resp = await client.get("/api/system/stats")
    data = resp.json()
    assert data["tasks"]["pending"] == 2


@pytest.mark.asyncio
async def test_stats_excludes_internal_pr_reviewers_without_changing_task_scope(
    client,
    session_factory,
):
    async with session_factory() as db:
        ordinary = Task(
            title="ordinary",
            description="visible active work",
            status="pending",
        )
        ordinary_archived = Task(
            title="ordinary archived",
            description="visible history",
            status="completed",
            archived=True,
        )
        db.add_all((ordinary, ordinary_archived))
        await db.flush()
        shared_mirror = Task(
            title="shared mirror",
            description="still part of execution statistics",
            status="pending",
            shared_from_id=ordinary.id,
        )
        legacy_single_task = Task(
            title="legacy single reviewer",
            description="internal review protocol",
            status="pending",
        )
        panel_first_task = Task(
            title="panel principal reviewer",
            description="internal review protocol",
            status="pending",
            archived=True,
        )
        panel_second_task = Task(
            title="panel senior reviewer",
            description="internal review protocol",
            status="completed",
        )
        db.add_all((
            shared_mirror,
            legacy_single_task,
            panel_first_task,
            panel_second_task,
        ))
        await db.flush()

        repo = MonitoredRepo(
            repo_full_name="example/system-stats-reviewers",
            webhook_secret="stats-secret",
        )
        db.add(repo)
        await db.flush()
        legacy_review = PRReview(
            repo_id=repo.id,
            pr_number=10,
            base_ref="main",
            base_sha="a" * 40,
            head_sha="b" * 40,
            pr_title="Legacy single",
            pr_author="alice",
            pr_url="https://github.com/example/system-stats-reviewers/pull/10",
            task_id=legacy_single_task.id,
            status="reviewing",
        )
        panel_review = PRReview(
            repo_id=repo.id,
            pr_number=11,
            base_ref="main",
            base_sha="c" * 40,
            head_sha="d" * 40,
            pr_title="Reviewer panel",
            pr_author="bob",
            pr_url="https://github.com/example/system-stats-reviewers/pull/11",
            # The first panel Task deliberately has both legacy and panel
            # links.  It must still count as one internal reviewer, not leak
            # through either classification branch.
            task_id=panel_first_task.id,
            status="reviewing",
        )
        db.add_all((legacy_review, panel_review))
        await db.flush()
        db.add_all((
            PRReviewerRun(
                pr_review_id=panel_review.id,
                role="principal_engineer",
                task_id=panel_first_task.id,
                provider="claude",
                status="reviewing",
                prompt_policy_hash="e" * 64,
                guide_pack_hash="f" * 64,
            ),
            PRReviewerRun(
                pr_review_id=panel_review.id,
                role="senior_engineer",
                task_id=panel_second_task.id,
                provider="claude",
                status="passed",
                prompt_policy_hash="1" * 64,
                guide_pack_hash="2" * 64,
            ),
        ))
        await db.commit()

    response = await client.get("/api/system/stats")

    assert response.status_code == 200
    counts = response.json()["tasks"]
    assert counts == {
        "pending": 2,
        "in_progress": 0,
        "executing": 0,
        "completed": 1,
        "failed": 0,
    }


@pytest.mark.asyncio
async def test_stats_running_instances(client, session_factory):
    # Create an instance with status="running"
    async with session_factory() as db:
        inst = Instance(name="worker-test", status="running")
        db.add(inst)
        await db.commit()

    resp = await client.get("/api/system/stats")
    data = resp.json()
    assert data["running_instances"] >= 1


# === /api/system/update tests ===


@pytest.mark.asyncio
async def test_update_dry_run_forwards_force_and_branch(client, monkeypatch):
    service = MagicMock()
    service.dry_run = AsyncMock(return_value={"has_updates": False})
    monkeypatch.setattr("backend.main.update_service", service)

    resp = await client.post(
        "/api/system/update",
        json={"dry_run": True, "force": True, "branch": "release/test"},
    )

    assert resp.status_code == 200
    assert resp.json() == {"has_updates": False}
    service.dry_run.assert_awaited_once_with(
        branch="release/test",
        force=True,
        channel=None,
    )


@pytest.mark.asyncio
async def test_update_dry_run_forwards_explicit_channel(client, monkeypatch):
    service = MagicMock()
    service.dry_run = AsyncMock(return_value={"has_updates": False})
    monkeypatch.setattr("backend.main.update_service", service)

    resp = await client.post(
        "/api/system/update",
        json={"dry_run": True, "force": True, "channel": "stable"},
    )

    assert resp.status_code == 200
    service.dry_run.assert_awaited_once_with(
        branch=None,
        force=True,
        channel="stable",
    )


@pytest.mark.asyncio
async def test_update_returns_conflict_when_active_tasks_block_start(client, monkeypatch):
    service = MagicMock()
    service.start_update = AsyncMock(return_value={
        "error": "当前有 1 个任务正在运行，请等待任务完成后再更新",
        "update_blocked": True,
    })
    monkeypatch.setattr("backend.main.update_service", service)

    resp = await client.post(
        "/api/system/update",
        json={"force": True, "branch": "main"},
    )

    assert resp.status_code == 409
    assert "当前有 1 个任务正在运行" in resp.json()["detail"]
    service.start_update.assert_awaited_once_with(
        skip_frontend_build=False,
        force=True,
        branch="main",
        channel=None,
    )


@pytest.mark.asyncio
async def test_restart_and_repair_endpoints_delegate(client, monkeypatch):
    service = MagicMock()
    service.restart = AsyncMock(return_value={"status": "started"})
    service.start_repair = AsyncMock(return_value={"status": "started"})
    service.reconcile_blockers = AsyncMock(
        return_value={
            "reconciled": True,
            "update_blocked": False,
            "active_task_count": 0,
            "active_tasks": [],
        }
    )
    monkeypatch.setattr("backend.main.update_service", service)

    restart = await client.post("/api/system/restart")
    repair = await client.post("/api/system/update/repair", json={})
    reconcile = await client.post("/api/system/update/reconcile")

    assert restart.status_code == 200
    assert repair.status_code == 200
    assert reconcile.status_code == 200
    assert reconcile.json()["reconciled"] is True
    service.restart.assert_awaited_once()
    service.start_repair.assert_awaited_once_with(skip_frontend_build=False)
    service.reconcile_blockers.assert_awaited_once()


@pytest.mark.asyncio
async def test_reconcile_endpoint_returns_structured_conflict(
    client, monkeypatch,
):
    service = MagicMock()
    service.reconcile_blockers = AsyncMock(
        return_value={
            "error": "无法安全核对",
            "reconciled": False,
            "update_blocked": True,
            "active_task_count": 0,
            "active_tasks": [],
        }
    )
    monkeypatch.setattr("backend.main.update_service", service)

    response = await client.post("/api/system/update/reconcile")

    assert response.status_code == 409
    assert response.json()["detail"] == {
        "error": "无法安全核对",
        "reconciled": False,
        "update_blocked": True,
        "active_task_count": 0,
        "active_tasks": [],
    }


@pytest.mark.asyncio
async def test_rollback_confirmation_is_structured_conflict(client, monkeypatch):
    service = MagicMock()
    service.rollback = AsyncMock(
        return_value={
            "error": "database restore confirmation required",
            "confirmation_required": True,
            "database_restore_required": True,
        }
    )
    monkeypatch.setattr("backend.main.update_service", service)

    response = await client.post("/api/system/update/rollback", json={})

    assert response.status_code == 409
    assert response.json()["detail"]["confirmation_required"] is True
    service.rollback.assert_awaited_once_with(
        confirm_database_restore=False
    )


# === /api/system/config tests ===


@pytest.mark.asyncio
async def test_config_returns_default_model(client):
    resp = await client.get("/api/system/config")
    assert resp.status_code == 200
    data = resp.json()
    assert "default_model" in data
    assert isinstance(data["default_model"], str)
    assert len(data["default_model"]) > 0


@pytest.mark.asyncio
async def test_config_advertises_exact_task_id_namespace(client, monkeypatch):
    from backend.config import settings

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(settings, "auth_token", "worker-config-test-token")
    resp = await client.get(
        "/api/system/config",
        headers={"Authorization": "Bearer worker-config-test-token"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["task_id_namespace_protocol"] == 1
    assert data["task_id_namespace_boundary"] == 1_000_000_000
    assert data["ccm_node_role"] == "worker"
    assert data["worker_delegated_launch_admission_protocol"] == 2


@pytest.mark.asyncio
async def test_config_ships_codex_sol_as_default(client):
    resp = await client.get("/api/system/config")
    assert resp.status_code == 200
    data = resp.json()
    assert data["default_provider"] == "codex"
    assert data["default_codex_model"] == "gpt-5.6-sol"
    assert "gpt-5.6-sol" in data["codex_model_options"]


@pytest.mark.asyncio
async def test_config_returns_two_stage_plan_pipeline_defaults(client):
    resp = await client.get("/api/system/config")
    assert resp.status_code == 200
    pipeline = resp.json()["plan_pipeline_defaults"]

    assert pipeline == {
        "version": 1,
        "planner": {
            "primary": {
                "provider": "claude",
                "model": "claude-fable-5",
                "effort": "high",
            },
            "fallback": {
                "provider": "codex",
                "model": "gpt-5.6-terra",
                "effort": "xhigh",
            },
        },
        "reviewer": {
            "enabled": True,
            "primary": {
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "effort": "xhigh",
            },
            "fallback": {
                "provider": "claude",
                "model": "claude-sonnet-5",
                "effort": "high",
            },
        },
        "max_revision_cycles": 3,
        "max_interactions": 3,
    }


@pytest.mark.asyncio
async def test_config_returns_codex_service_tier_capabilities(client):
    resp = await client.get("/api/system/config")
    assert resp.status_code == 200
    data = resp.json()

    assert data["default_codex_service_tier"] == "default"
    assert data["codex_service_tier_options"] == ["default", "priority"]
    tiers = data["codex_model_service_tiers"]
    assert tiers["gpt-5.6-sol"] == ["default", "priority"]
    assert tiers["gpt-5.4"] == ["default", "priority"]
    assert tiers["gpt-5.4-mini"] == ["default"]
    assert tiers["gpt-5.3-codex-spark"] == ["default"]


@pytest.mark.asyncio
async def test_config_declares_pr_review_snapshot_context_capability(client):
    resp = await client.get("/api/system/config")
    assert resp.status_code == 200
    assert resp.json()["pr_review_snapshot_context_version"] == 3
    assert resp.json()["pr_review_terminal_chat_version"] == 1
    assert resp.json()["task_artifact_scope_version"] == 1


@pytest.mark.asyncio
async def test_config_returns_model_options_list(client):
    resp = await client.get("/api/system/config")
    assert resp.status_code == 200
    data = resp.json()
    assert "model_options" in data
    assert isinstance(data["model_options"], list)
    assert len(data["model_options"]) > 0


@pytest.mark.asyncio
async def test_config_model_options_no_empty_strings(client):
    """model_options should not contain empty strings."""
    resp = await client.get("/api/system/config")
    for opt in resp.json()["model_options"]:
        assert opt.strip() != ""


@pytest.mark.asyncio
async def test_config_default_model_options_include_1m_variants(client):
    """The shipped default model_options should include 1m variants."""
    resp = await client.get("/api/system/config")
    options = resp.json()["model_options"]
    assert "claude-opus-4-6[1m]" in options
    assert "claude-sonnet-4-6[1m]" in options


@pytest.mark.asyncio
async def test_config_includes_opus5_capabilities(client):
    resp = await client.get("/api/system/config")
    data = resp.json()

    assert "claude-opus-5" in data["model_options"]
    assert "claude-opus-5[1m]" not in data["model_options"]
    assert data["claude_model_context_windows"]["claude-opus-5"] == 1_000_000
    assert data["claude_model_efforts"]["claude-opus-5"] == [
        "low",
        "medium",
        "high",
        "xhigh",
        "max",
    ]


@pytest.mark.asyncio
async def test_config_reflects_settings(client):
    from unittest.mock import patch
    from backend.config import settings

    with patch.object(settings, "default_model", "haiku"), \
         patch.object(settings, "model_options", "haiku,sonnet"):
        resp = await client.get("/api/system/config")
    data = resp.json()
    assert data["default_model"] == "haiku"
    assert data["model_options"] == ["haiku", "sonnet"]


@pytest.mark.asyncio
async def test_auto_capability_switch_is_independent_and_fail_closed(client):
    from unittest.mock import patch
    from backend.config import settings

    response = await client.get("/api/system/config")
    assert response.status_code == 200
    assert response.json()["capability_core_enabled"] is True
    assert response.json()["auto_capability_enabled"] is True
    assert response.json()["delivery_loop_enabled"] is True

    with patch.object(settings, "auto_capability_enabled", True), \
         patch.object(settings, "capability_core_enabled", False):
        response = await client.get("/api/system/config")
    assert response.json()["auto_capability_enabled"] is False
    assert response.json()["delivery_loop_enabled"] is False

    with patch.object(settings, "auto_capability_enabled", True), \
         patch.object(settings, "capability_core_enabled", True):
        response = await client.get("/api/system/config")
    assert response.json()["auto_capability_enabled"] is True


# === Effort config tests ===


@pytest.mark.asyncio
async def test_config_returns_effort_fields(client):
    """Config endpoint returns default_effort and effort_options."""
    resp = await client.get("/api/system/config")
    assert resp.status_code == 200
    data = resp.json()
    assert "default_effort" in data
    assert data["default_effort"] == "medium"
    assert "effort_options" in data
    assert isinstance(data["effort_options"], list)
    assert "low" in data["effort_options"]
    assert "high" in data["effort_options"]
    assert "max" in data["effort_options"]


@pytest.mark.asyncio
async def test_config_effort_reflects_settings(client):
    """Effort config reflects overridden settings."""
    from unittest.mock import patch
    from backend.config import settings

    with patch.object(settings, "default_effort", "high"), \
         patch.object(settings, "effort_options", "low,high"):
        resp = await client.get("/api/system/config")
    data = resp.json()
    assert data["default_effort"] == "high"
    assert data["effort_options"] == ["low", "high"]
