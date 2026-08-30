"""Tests for /api/settings/runtime — frontend PTY mode toggle."""
import asyncio
from types import SimpleNamespace

import pytest

from backend.config import settings
from backend.services.task_creation import (
    delegated_task_execution_principal_values,
    task_execution_principal_values,
)


@pytest.mark.asyncio
async def test_get_runtime_settings(client):
    resp = await client.get("/api/settings/runtime")
    assert resp.status_code == 200
    data = resp.json()
    assert "use_pty_mode" in data
    assert "pty_available" in data
    assert "codex_app_server_enabled" in data
    assert "codex_main_mcp_enabled" in data
    assert "codex_monitor_enabled" in data
    assert "agent_sandbox_unrestricted_enabled" not in data


@pytest.mark.asyncio
async def test_update_channel_defaults_to_stable_and_round_trips(client):
    response = await client.get("/api/settings/update-channel")
    assert response.status_code == 200
    assert response.json() == {"update_channel": "stable"}

    response = await client.put(
        "/api/settings/update-channel", json={"update_channel": "main"}
    )
    assert response.status_code == 200
    assert response.json() == {"update_channel": "main"}

    response = await client.put(
        "/api/settings/update-channel", json={"update_channel": "invalid"}
    )
    assert response.status_code == 422


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [True, False])
async def test_runtime_settings_reports_effective_codex_main_mcp_capability(
    client, monkeypatch, enabled,
):
    monkeypatch.setattr(settings, "codex_main_mcp_enabled", enabled)

    get_resp = await client.get("/api/settings/runtime")
    assert get_resp.status_code == 200
    assert get_resp.json()["codex_main_mcp_enabled"] is enabled
    assert get_resp.json()["codex_monitor_enabled"] is enabled

    put_resp = await client.put(
        "/api/settings/runtime",
        json={"auto_sort_on_access": True},
    )
    assert put_resp.status_code == 200
    assert put_resp.json()["codex_main_mcp_enabled"] is enabled
    assert put_resp.json()["codex_monitor_enabled"] is enabled


@pytest.mark.asyncio
async def test_removed_agent_unrestricted_setting_is_rejected(client):
    response = await client.put(
        "/api/settings/runtime",
        json={"agent_sandbox_unrestricted_enabled": True},
    )

    assert response.status_code == 422


def test_task_principal_builder_rejects_mixed_admin_system_identity():
    with pytest.raises(ValueError, match="system Task principal must be member"):
        task_execution_principal_values(
            user_id=None,
            role="admin",
            principal_kind="system",
        )


def test_task_principal_builder_maps_roles_without_an_override_switch():
    assert task_execution_principal_values(
        user_id=17,
        role="admin",
        principal_kind="user",
    )["execution_mode"] == "unrestricted"
    assert task_execution_principal_values(
        user_id=18,
        role="member",
        principal_kind="user",
    )["execution_mode"] == "sandbox"


@pytest.mark.parametrize(
    ("origin_kind", "expected_kind", "user_id", "role", "mode"),
    [
        ("user", "delegated_user", 17, "admin", "unrestricted"),
        ("user", "delegated_user", 18, "member", "sandbox"),
        (
            "deployment_token",
            "delegated_deployment_token",
            None,
            "super_admin",
            "unrestricted",
        ),
        ("system", "system", None, "member", "sandbox"),
    ],
)
def test_task_principal_builder_maps_worker_delegation(
    origin_kind,
    expected_kind,
    user_id,
    role,
    mode,
):
    assert delegated_task_execution_principal_values(
        user_id=user_id,
        role=role,
        principal_kind=origin_kind,
    ) == {
        "execution_user_id": user_id,
        "execution_user_role": role,
        "execution_mode": mode,
        "execution_principal_kind": expected_kind,
    }


@pytest.mark.asyncio
async def test_toggle_pty_mode_roundtrip(client):
    from backend.main import instance_manager

    try:
        resp = await client.put(
            "/api/settings/runtime", json={"use_pty_mode": True}
        )
        assert resp.status_code == 200
        body = resp.json()
        # claude_pty installed in dev venv -> enable succeeds
        assert body["pty_available"] is True
        assert body["use_pty_mode"] is True
        assert instance_manager.pty_mode_enabled is True

        resp = await client.put(
            "/api/settings/runtime", json={"use_pty_mode": False}
        )
        assert resp.json()["use_pty_mode"] is False
        assert instance_manager.pty_mode_enabled is False

        # GET reflects current state
        resp = await client.get("/api/settings/runtime")
        assert resp.json()["use_pty_mode"] is False
    finally:
        instance_manager.set_pty_mode(False)


@pytest.mark.asyncio
async def test_toggle_off_drains_idle_sessions(client):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from backend.main import instance_manager

    class FakeBackend:
        _pool = SimpleNamespace(_sessions={})
        drain_idle_sessions = AsyncMock(return_value=2)

    old_backend = instance_manager._pty_backend
    old_enabled = instance_manager._pty_enabled
    try:
        instance_manager._pty_backend = FakeBackend()
        instance_manager._pty_enabled = True

        resp = await client.put(
            "/api/settings/runtime", json={"use_pty_mode": False}
        )
        assert resp.status_code == 200
        assert resp.json()["use_pty_mode"] is False
        FakeBackend.drain_idle_sessions.assert_awaited_once()
    finally:
        instance_manager._pty_backend = old_backend
        instance_manager._pty_enabled = old_enabled


@pytest.mark.asyncio
async def test_first_runtime_settings_create_holds_worker_drain_fence(
    tmp_path,
    monkeypatch,
):
    """The singleton bootstrap commit must happen before the drain fence."""

    from sqlalchemy.ext.asyncio import (
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )

    from backend.api.settings import update_runtime_settings
    from backend.database import Base
    from backend.main import instance_manager
    from backend.schemas.global_settings import RuntimeSettingsUpdate
    from backend.services.worker_node_control import begin_worker_node_drain

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'worker-runtime-fence.db'}",
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    mutation_paused = asyncio.Event()
    release_mutation = asyncio.Event()

    async def pause_after_fence():
        mutation_paused.set()
        await release_mutation.wait()
        return 0

    async def claim_drain():
        async with factory() as drain_db:
            await begin_worker_node_drain(drain_db, claim="c" * 64)
            await drain_db.commit()

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(
        instance_manager,
        "drain_idle_pty_sessions",
        pause_after_fence,
    )
    old_enabled = instance_manager._pty_enabled
    instance_manager._pty_enabled = True
    update_task = None
    drain_task = None
    try:
        async with factory() as update_db:
            request = SimpleNamespace(
                state=SimpleNamespace(user_role="admin"),
            )
            update_task = asyncio.create_task(update_runtime_settings(
                request,
                RuntimeSettingsUpdate(use_pty_mode=False),
                update_db,
            ))
            await asyncio.wait_for(mutation_paused.wait(), timeout=2)
            drain_task = asyncio.create_task(claim_drain())
            await asyncio.sleep(0.05)
            assert not drain_task.done()
            release_mutation.set()
            response = await asyncio.wait_for(update_task, timeout=2)
            assert response.use_pty_mode is False
            await asyncio.wait_for(drain_task, timeout=2)
    finally:
        release_mutation.set()
        if update_task is not None and not update_task.done():
            await update_task
        if drain_task is not None and not drain_task.done():
            await drain_task
        instance_manager._pty_enabled = old_enabled
        await engine.dispose()


@pytest.mark.asyncio
async def test_context_compact_threshold_default_and_update(client):
    from backend.config import settings

    # Default: no DB override -> env default
    resp = await client.get("/api/settings/runtime")
    assert resp.status_code == 200
    assert resp.json()["context_compact_threshold"] == pytest.approx(
        settings.context_compact_threshold
    )

    # Update -> persisted and returned as effective value
    resp = await client.put(
        "/api/settings/runtime", json={"context_compact_threshold": 0.7}
    )
    assert resp.status_code == 200
    assert resp.json()["context_compact_threshold"] == pytest.approx(0.7)

    resp = await client.get("/api/settings/runtime")
    assert resp.json()["context_compact_threshold"] == pytest.approx(0.7)

    # Updating other fields must not clobber the stored threshold
    resp = await client.put(
        "/api/settings/runtime", json={"auto_sort_on_access": True}
    )
    assert resp.json()["context_compact_threshold"] == pytest.approx(0.7)


@pytest.mark.asyncio
async def test_context_compact_threshold_rejects_out_of_range(client):
    for bad in (0.1, 0.99, 2):
        resp = await client.put(
            "/api/settings/runtime", json={"context_compact_threshold": bad}
        )
        assert resp.status_code == 422, f"{bad} should be rejected"


@pytest.mark.asyncio
async def test_plan_pipeline_settings_are_persisted_and_returned(client):
    current = (await client.get("/api/settings/plan-pipeline")).json()
    current["planner"]["primary"] = {
        "provider": "codex",
        "model": "gpt-5.6-luna",
        "effort": "max",
    }
    current["max_revision_cycles"] = 1

    saved = await client.put(
        "/api/settings/plan-pipeline",
        json=current,
    )

    assert saved.status_code == 200, saved.text
    assert saved.json() == current
    assert (await client.get("/api/settings/plan-pipeline")).json() == current
    system = await client.get("/api/system/config")
    assert system.json()["plan_pipeline_defaults"] == current
