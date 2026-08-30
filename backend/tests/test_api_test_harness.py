from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI, HTTPException, Request
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from backend.api import test_harness as test_harness_api
from backend.config import settings
from backend.database import get_db
from backend.models.task import Task
from backend.models.user import User
from backend.models.test_harness import (
    TestHarnessEvidence as EvidenceModel,
    TestHarnessRun as RunModel,
)
from backend.services.test_harness import TestHarnessService as HarnessService
from backend.services.test_harness_artifacts import (
    TestHarnessArtifactStore as ArtifactStore,
)
from backend.services.test_harness_contracts import (
    TestHarnessSpec as HarnessSpec,
    compile_test_plan,
)
from backend.services.internal_service_auth import InternalServiceClaims


@pytest.mark.asyncio
async def test_list_test_runs_releases_route_connection_before_service_sessions(
    monkeypatch,
    tmp_path,
):
    """A one-connection pool must not deadlock on the route's nested service reads."""

    from sqlalchemy.ext.asyncio import (
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )

    from backend.database import Base

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'single-connection.db'}",
        pool_size=1,
        max_overflow=0,
        pool_timeout=0.05,
    )
    factory = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        async with factory() as db:
            task = Task(title="Single connection list", status="executing")
            db.add(task)
            await db.commit()
            task_id = task.id

        service = HarnessService(db_factory=factory, poll_interval=0.01)
        monkeypatch.setattr(test_harness_api, "test_harness_service", service)

        app = FastAPI()

        @app.middleware("http")
        async def _admin(request: Request, call_next):
            request.state.user_role = "admin"
            request.state.auth_type = "token"
            return await call_next(request)

        async def _get_db():
            async with factory() as db:
                yield db

        app.include_router(test_harness_api.router)
        app.dependency_overrides[get_db] = _get_db
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await asyncio.wait_for(
                client.get(f"/api/tasks/{task_id}/test-runs"),
                timeout=1,
            )
            from backend.services.workspace_review import WorkspaceReviewError

            refresh = AsyncMock(
                side_effect=WorkspaceReviewError("Git snapshot unavailable")
            )
            monkeypatch.setattr(service, "refresh_task_staleness", refresh)
            degraded = await client.get(f"/api/tasks/{task_id}/test-runs")

        assert response.status_code == 200, response.text
        assert response.json() == []
        assert degraded.status_code == 200, degraded.text
        assert degraded.json() == []
        refresh.assert_awaited_once_with(task_id)
    finally:
        await engine.dispose()


def _request_state(
    *,
    auth_type: str,
    role: str,
    user_id: int | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        state=SimpleNamespace(
            auth_type=auth_type,
            user_role=role,
            user_id=user_id,
        ),
        headers={},
    )


@pytest.mark.asyncio
async def test_task_test_run_api_persists_lists_and_cancels_fixed_url(
    monkeypatch,
    db_factory,
):
    async with db_factory() as db:
        task = Task(
            title="API Harness",
            status="completed",
            provider="claude",
            model="claude-opus-4-6",
            effort_level="high",
        )
        db.add(task)
        other_task = Task(
            title="Other API Harness",
            status="completed",
            provider="codex",
            model="gpt-5.6-sol",
        )
        db.add(other_task)
        await db.commit()
        task_id = task.id
        other_task_id = other_task.id

    service = HarnessService(db_factory=db_factory, poll_interval=0.01)
    async def _attach_marker(*, run_id: str, inline: bool):
        assert inline is False
        await service._update_run(
            run_id,
            values={"browser_review_job_id": "b" * 32},
            event_type="lifecycle",
            title="Browser reserved",
            source_key="test:browser-reserved",
        )
        return object()

    start_browser = AsyncMock(side_effect=_attach_marker)
    monkeypatch.setattr(service, "start_fixed_url_browser", start_browser)
    cancel_resources = AsyncMock(return_value=("completed", None))
    monkeypatch.setattr(
        service,
        "_cancel_direct_run_resources",
        cancel_resources,
    )
    monkeypatch.setattr(test_harness_api, "test_harness_service", service)

    app = FastAPI()

    @app.middleware("http")
    async def _admin(request: Request, call_next):
        request.state.user_role = "admin"
        request.state.auth_type = "token"
        return await call_next(request)

    async def _get_db():
        async with db_factory() as db:
            yield db

    app.include_router(test_harness_api.router)
    app.dependency_overrides[get_db] = _get_db

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        capabilities = await client.get(
            f"/api/tasks/{task_id}/test-runs/capabilities"
        )
        assert capabilities.status_code == 200
        assert capabilities.json()["targets"]["pull_request"] is False
        assert capabilities.json()["targets"]["git_ref"] is False
        assert "sandbox" in capabilities.json()["target_reasons"]["pull_request"]

        started = await client.post(
            f"/api/tasks/{task_id}/test-runs",
            json={
                "target_kind": "fixed_url",
                "target": {"url": "http://127.0.0.1:5173"},
                "goal": "Verify the settings screen",
                "allow_actions": False,
                "max_actions": 0,
                "idempotency_key": "api-fixed-url-v1",
            },
        )
        assert started.status_code == 202, started.text
        payload = started.json()
        run_id = payload["id"]
        assert payload["target_kind"] == "fixed_url"
        assert payload["runtime"]["provider"] == "claude"
        assert payload["runtime"]["context_policy"] == "isolated_black_box_v1"
        assert payload["runtime"]["browser_channel"] == "chromium"
        assert payload["stage"] == "waiting_for_browser"
        start_browser.assert_awaited_once_with(run_id=run_id, inline=False)

        listed = await client.get(f"/api/tasks/{task_id}/test-runs")
        assert listed.status_code == 200
        assert [item["id"] for item in listed.json()] == [run_id]

        duplicate = await client.post(
            f"/api/tasks/{task_id}/test-runs",
            json={
                "target_kind": "fixed_url",
                "target": {"url": "http://127.0.0.1:5173"},
                "goal": "Verify the settings screen",
                "allow_actions": False,
                "max_actions": 0,
                "idempotency_key": "api-fixed-url-v1",
            },
        )
        assert duplicate.status_code == 202
        assert duplicate.json()["id"] == run_id
        assert start_browser.await_count == 1

        cancelled = await client.post(
            f"/api/tasks/{task_id}/test-runs/{run_id}/cancel"
        )
        assert cancelled.status_code == 200, cancelled.text
        assert cancelled.json()["status"] == "cancelled"
        cancel_resources.assert_awaited_once()

        repeated = await client.post(
            f"/api/tasks/{task_id}/test-runs/{run_id}/repeat"
        )
        assert repeated.status_code == 202, repeated.text
        repeated_payload = repeated.json()
        assert repeated_payload["id"] != run_id
        assert repeated_payload["parent_run_id"] == run_id
        assert repeated_payload["runtime"]["browser_channel"] == "chromium"
        assert repeated_payload["browser_review_job_id"] == "b" * 32
        assert start_browser.await_count == 2
        assert start_browser.await_args.kwargs == {
            "run_id": repeated_payload["id"],
            "inline": False,
        }

        foreign = await service.start_task_run(
            task_id=other_task_id,
            spec=HarnessSpec(
                target_kind="fixed_url",
                target={"url": "http://127.0.0.1:5174"},
                goal="Foreign Task evidence must remain private",
            ),
        )
        compared = await client.get(
            f"/api/tasks/{task_id}/test-runs/{run_id}/compare/{foreign.id}"
        )
        assert compared.status_code == 404


@pytest.mark.asyncio
async def test_public_test_run_waits_for_parent_task_terminal(db_factory):
    async with db_factory() as db:
        task = Task(title="Running", status="executing")
        db.add(task)
        await db.commit()
        task_id = task.id

    app = FastAPI()

    @app.middleware("http")
    async def _admin(request: Request, call_next):
        request.state.user_role = "admin"
        request.state.auth_type = "token"
        return await call_next(request)

    async def _get_db():
        async with db_factory() as db:
            yield db

    app.include_router(test_harness_api.router)
    app.dependency_overrides[get_db] = _get_db
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post(
            f"/api/tasks/{task_id}/test-runs",
            json={
                "target_kind": "fixed_url",
                "target": {"url": "http://127.0.0.1:5173"},
                "goal": "Do not race the active turn",
            },
        )

        assert response.status_code == 409
        assert "Agent 可直接调用测试工具" in response.json()["detail"]


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["start", "repeat"])
async def test_public_test_run_effect_rejects_cached_jwt_role_change(
    monkeypatch,
    db_factory,
    operation,
):
    async with db_factory() as db:
        user = User(
            email=f"harness-role-{operation}@example.invalid",
            name="Harness role race",
            password_hash="not-used",
            role="member",
            is_active=True,
        )
        db.add(user)
        await db.flush()
        task = Task(
            title="Harness cached role admission",
            status="completed",
            created_by=user.id,
        )
        db.add(task)
        await db.commit()
        task_id = task.id
        user_id = user.id

    service = SimpleNamespace(
        start_task_run=AsyncMock(),
        start_fixed_url_browser=AsyncMock(),
        get_run=AsyncMock(return_value={"task_id": task_id}),
        repeat=AsyncMock(),
    )
    monkeypatch.setattr(test_harness_api, "test_harness_service", service)
    # Simulate an HTTP request authenticated while the User was still admin;
    # the durable row has already been demoted before effect admission.
    request = _request_state(
        auth_type="jwt",
        role="admin",
        user_id=user_id,
    )

    async with db_factory() as db:
        with pytest.raises(HTTPException) as caught:
            if operation == "start":
                await test_harness_api.start_test_harness_run(
                    task_id,
                    test_harness_api.TestHarnessRunStart(
                        target_kind="fixed_url",
                        target={"url": "https://example.com"},
                        goal="Do not use stale admin authority",
                    ),
                    request,
                    db,
                )
            else:
                await test_harness_api.repeat_test_harness_run(
                    task_id,
                    "a" * 32,
                    request,
                    db,
                )

    assert caught.value.status_code == 409
    assert "changed role" in caught.value.detail
    service.start_task_run.assert_not_awaited()
    service.get_run.assert_not_awaited()
    service.repeat.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("owner_fields", "message"),
    [
        ({"worker_id": 71}, "Worker-authoritative"),
        ({"shared_from_id": 72}, "Shared shadow"),
    ],
)
@pytest.mark.parametrize("operation", ["start", "repeat"])
async def test_public_test_run_never_materializes_manager_mirror(
    monkeypatch,
    db_factory,
    owner_fields,
    message,
    operation,
):
    async with db_factory() as db:
        task = Task(
            title="Remote Harness mirror",
            status="completed",
            **owner_fields,
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    service = SimpleNamespace(
        start_task_run=AsyncMock(),
        start_fixed_url_browser=AsyncMock(),
        get_run=AsyncMock(return_value={"task_id": task_id}),
        repeat=AsyncMock(),
    )
    monkeypatch.setattr(test_harness_api, "test_harness_service", service)
    request = _request_state(auth_type="token", role="super_admin")

    async with db_factory() as db:
        with pytest.raises(HTTPException) as caught:
            if operation == "start":
                await test_harness_api.start_test_harness_run(
                    task_id,
                    test_harness_api.TestHarnessRunStart(
                        target_kind="fixed_url",
                        target={"url": "https://example.com"},
                        goal="Do not materialize on the Manager mirror",
                    ),
                    request,
                    db,
                )
            else:
                await test_harness_api.repeat_test_harness_run(
                    task_id,
                    "b" * 32,
                    request,
                    db,
                )

    assert caught.value.status_code == 409
    assert message in caught.value.detail
    service.start_task_run.assert_not_awaited()
    service.get_run.assert_not_awaited()
    service.repeat.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("owner_fields", "message"),
    [
        ({"worker_id": 61}, "Worker-authoritative"),
        ({"shared_from_id": 62}, "Shared shadow"),
    ],
)
async def test_capabilities_disable_manager_targets_for_remote_authority(
    monkeypatch,
    db_factory,
    owner_fields,
    message,
):
    async with db_factory() as db:
        task = Task(
            title="Remote-authoritative Harness capabilities",
            status="completed",
            provider="codex",
            model="gpt-5.6-sol",
            **owner_fields,
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    async def allow_access(*_args):
        return None

    async def sandbox_capability(*, project):
        assert project is None
        return SimpleNamespace(
            available=True,
            reason=None,
            sandbox=SimpleNamespace(as_dict=lambda: {"available": True}),
        )

    monkeypatch.setattr(test_harness_api, "require_task_control", allow_access)
    monkeypatch.setattr(
        test_harness_api,
        "untrusted_git_target_capability",
        sandbox_capability,
    )
    async with db_factory() as db:
        payload = await test_harness_api.get_test_harness_capabilities(
            task_id,
            SimpleNamespace(),
            db,
        )
    assert payload["available"] is False
    assert set(payload["targets"].values()) == {False}
    for target in ("current_workspace", "fixed_url", "pull_request", "git_ref"):
        assert message in payload["target_reasons"][target]


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["start", "repeat"])
async def test_public_start_and_repeat_reject_terminal_owner_with_live_instance(
    monkeypatch,
    db_factory,
    operation,
):
    from backend.models.instance import Instance

    monkeypatch.setattr(settings, "auth_token", "public-harness-secret")
    async with db_factory() as db:
        task = Task(
            title="Terminal owner awaiting runtime reap",
            status="completed",
            provider="codex",
            model="gpt-5.6-sol",
        )
        db.add(task)
        await db.flush()
        db.add(
            Instance(
                name="late terminal owner",
                status="running",
                current_task_id=task.id,
            )
        )
        run_id = "8" * 32
        db.add(
            RunModel(
                id=run_id,
                task_id=task.id,
                owner_task_incarnation_id=task.incarnation_id,
                owner_task_retry_count=task.retry_count,
                owner_task_turn_generation=task.turn_generation,
                owner_task_status=task.status,
                target_kind="fixed_url",
                target_spec={"url": "https://example.com"},
                test_plan=compile_test_plan(
                    goal="repeat after runtime reap",
                    profile="standard",
                    allow_actions=False,
                    viewport_width=1440,
                    viewport_height=900,
                    max_steps=20,
                    max_actions=0,
                ),
                runtime_config={
                    "provider": "codex",
                    "model": "gpt-5.6-sol",
                    "reasoning_effort": "high",
                    "codex_service_tier": "default",
                    "profile": "standard",
                    "allow_actions": False,
                    "browser_channel": "chromium",
                    "max_steps": 20,
                    "max_actions": 0,
                },
                request_fingerprint="f" * 64,
                root_run_id=run_id,
                status="completed",
                stage="completed",
            )
        )
        await db.commit()
        task_id = task.id

    service = HarnessService(db_factory=db_factory, poll_interval=0.01)
    service.start_fixed_url_browser = AsyncMock(return_value=object())
    monkeypatch.setattr(test_harness_api, "test_harness_service", service)
    app = FastAPI()

    @app.middleware("http")
    async def _admin(request: Request, call_next):
        request.state.user_role = "admin"
        request.state.auth_type = "token"
        return await call_next(request)

    async def _get_db():
        async with db_factory() as db:
            yield db

    app.include_router(test_harness_api.router)
    app.dependency_overrides[get_db] = _get_db
    path = (
        f"/api/tasks/{task_id}/test-runs"
        if operation == "start"
        else f"/api/tasks/{task_id}/test-runs/{run_id}/repeat"
    )
    body = (
        {
            "target_kind": "fixed_url",
            "target": {"url": "https://example.com"},
            "goal": "wait for runtime reap",
        }
        if operation == "start"
        else None
    )
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post(path, json=body)

    assert response.status_code == 409, response.text
    assert "reverse Instance owner" in response.text
    service.start_fixed_url_browser.assert_not_awaited()
    async with db_factory() as db:
        runs = list(
            (
                await db.execute(
                    select(RunModel.id).where(RunModel.task_id == task_id)
                )
            ).scalars()
        )
        assert runs == [run_id]


@pytest.mark.asyncio
async def test_internal_test_run_routes_revalidate_exact_parent_generation(
    monkeypatch,
    db_factory,
):
    monkeypatch.setattr(settings, "auth_token", "internal-harness-secret")
    async with db_factory() as db:
        task = Task(
            title="Internal Harness owner",
            status="executing",
            provider="codex",
            model="gpt-5.6-sol",
        )
        db.add(task)
        await db.commit()
        task_id = task.id
        claims = InternalServiceClaims(
            audience="ccm_frontend_review",
            token_id="internal-harness-token",
            expires_at=4_000_000_000,
            task_id=task.id,
            task_incarnation_id=task.incarnation_id,
            task_retry_count=task.retry_count,
            task_turn_generation=task.turn_generation,
            task_status=task.status,
            owner_kind="task",
            owner_id=task.id,
        )
    request = SimpleNamespace(
        state=SimpleNamespace(
            auth_type="internal_service",
            internal_service_claims=claims,
        )
    )
    service = HarnessService(db_factory=db_factory, poll_interval=0.01)
    service.start_fixed_url_browser = AsyncMock(return_value=object())
    monkeypatch.setattr(test_harness_api, "test_harness_service", service)
    body = test_harness_api.TestHarnessRunStart(
        target_kind="fixed_url",
        target={"url": "https://example.com"},
        goal="Review exact generation",
        allow_actions=False,
        max_actions=0,
    )
    async with db_factory() as db:
        started = await test_harness_api.start_test_harness_run_internal(
            task_id,
            body,
            request,
            db,
        )
    run_id = started["id"]
    async with db_factory() as db:
        durable_run = await db.get(RunModel, run_id)
        assert durable_run.owner_task_retry_count == claims.task_retry_count
        assert (
            durable_run.owner_task_turn_generation
            == claims.task_turn_generation
        )

    async with db_factory() as db:
        status_payload = await test_harness_api.get_test_harness_run_internal(
            task_id,
            run_id,
            request,
            db,
        )
    assert status_payload["id"] == run_id

    async with db_factory() as db:
        stopped = await test_harness_api.cancel_test_harness_run_internal(
            task_id,
            run_id,
            request,
            db,
        )
    assert stopped["status"] == "cancelled"

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.turn_generation += 1
        await db.commit()
    async with db_factory() as db:
        with pytest.raises(HTTPException) as caught:
            await test_harness_api.get_test_harness_run_internal(
                task_id,
                run_id,
                request,
                db,
            )
    assert getattr(caught.value, "status_code", None) == 403


@pytest.mark.asyncio
async def test_evidence_download_streams_the_integrity_checked_open_file(
    monkeypatch,
    db_factory,
    tmp_path,
):
    async with db_factory() as db:
        task = Task(title="Evidence owner", status="completed")
        db.add(task)
        await db.commit()
        task_id = task.id

    store = ArtifactStore(tmp_path / "archive")
    service = HarnessService(
        db_factory=db_factory,
        artifact_store=store,
        retention_interval=0,
    )
    run = await service.start_task_run(
        task_id=task_id,
        spec=HarnessSpec(
            target_kind="fixed_url",
            target={"url": "https://example.com"},
            goal="Verify durable evidence download",
        ),
    )
    source = tmp_path / "final.png"
    payload = b"\x89PNG\r\n\x1a\nverified-image"
    source.write_bytes(payload)
    archived = store.archive(
        source,
        task_id=task_id,
        run_id=run.id,
        attempt_id="a" * 32,
        name="final.png",
    )
    async with db_factory() as db:
        db.add(
            EvidenceModel(
                id="e" * 32,
                run_id=run.id,
                attempt_id=None,
                kind="screenshot",
                name="final.png",
                content_type="image/png",
                storage_path=archived.storage_key,
                sha256=archived.sha256,
                byte_size=archived.byte_size,
                metadata_={"storage_version": 1},
            )
        )
        await db.commit()
    monkeypatch.setattr(test_harness_api, "test_harness_service", service)

    app = FastAPI()

    @app.middleware("http")
    async def _admin(request: Request, call_next):
        request.state.user_role = "admin"
        request.state.auth_type = "token"
        return await call_next(request)

    async def _get_db():
        async with db_factory() as db:
            yield db

    app.include_router(test_harness_api.router)
    app.dependency_overrides[get_db] = _get_db
    url = f"/api/tasks/{task_id}/test-runs/{run.id}/evidence/final.png"
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get(url)
        assert response.status_code == 200
        assert response.headers["content-length"] == str(len(payload))
        assert response.content == payload

        archived.path.write_bytes(payload + b"tampered")
        rejected = await client.get(url)
        assert rejected.status_code == 404


@pytest.mark.asyncio
async def test_task_can_save_and_use_browser_runtime_independent_from_parent(
    monkeypatch,
    db_factory,
):
    async with db_factory() as db:
        task = Task(
            title="Codex parent with Claude browser",
            status="completed",
            provider="codex",
            model="gpt-5.6-sol",
            effort_level="high",
            codex_service_tier="priority",
            metadata_={"keep": "account-binding"},
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    service = HarnessService(db_factory=db_factory, poll_interval=0.01)
    start_browser = AsyncMock(return_value=object())
    monkeypatch.setattr(service, "start_fixed_url_browser", start_browser)
    monkeypatch.setattr(test_harness_api, "test_harness_service", service)

    app = FastAPI()

    @app.middleware("http")
    async def _admin(request: Request, call_next):
        request.state.user_role = "admin"
        request.state.auth_type = "token"
        return await call_next(request)

    async def _get_db():
        async with db_factory() as db:
            yield db

    app.include_router(test_harness_api.router)
    app.dependency_overrides[get_db] = _get_db

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        inherited = await client.get(f"/api/tasks/{task_id}/test-runs/config")
        assert inherited.status_code == 200, inherited.text
        assert inherited.json()["inherit_task"] is True
        assert inherited.json()["provider"] == "codex"
        assert inherited.json()["model"] == "gpt-5.6-sol"

        saved = await client.put(
            f"/api/tasks/{task_id}/test-runs/config",
            json={
                "inherit_task": False,
                "provider": "claude",
                "model": "claude-opus-5",
                "reasoning_effort": "max",
                "codex_service_tier": "default",
            },
        )
        assert saved.status_code == 200, saved.text
        assert saved.json()["source"] == "browser_review_config"
        assert saved.json()["provider"] == "claude"
        assert saved.json()["model"] == "claude-opus-5"
        assert saved.json()["reasoning_effort"] == "max"
        assert saved.json()["task_runtime"] == {
            "provider": "codex",
            "model": "gpt-5.6-sol",
            "reasoning_effort": "high",
            "codex_service_tier": "priority",
        }

        started = await client.post(
            f"/api/tasks/{task_id}/test-runs",
            json={
                "target_kind": "fixed_url",
                "target": {"url": "http://127.0.0.1:5173"},
                "goal": "Use the independently configured Browser Agent",
            },
        )
        assert started.status_code == 202, started.text
        assert started.json()["runtime"]["provider"] == "claude"
        assert started.json()["runtime"]["model"] == "claude-opus-5"
        assert started.json()["runtime"]["reasoning_effort"] == "max"
        assert started.json()["runtime"]["selection_source"] == "browser_review_config"

        invalid = await client.put(
            f"/api/tasks/{task_id}/test-runs/config",
            json={
                "inherit_task": False,
                "provider": "claude",
                "model": "claude-opus-4-6",
                "reasoning_effort": "ultra",
            },
        )
        assert invalid.status_code == 422

    async with db_factory() as db:
        persisted = await db.get(Task, task_id)
        assert persisted is not None
        assert persisted.metadata_["keep"] == "account-binding"
        assert persisted.metadata_["test_harness_runtime"] == {
            "version": 1,
            "inherit_task": False,
            "provider": "claude",
            "model": "claude-opus-5",
            "reasoning_effort": "max",
            "codex_service_tier": "default",
        }
    start_browser.assert_awaited_once()


@pytest.mark.asyncio
async def test_runtime_config_merges_fresh_metadata_after_terminal_gate_commit(
    monkeypatch,
    db_factory,
):
    async with db_factory() as db:
        task = Task(
            title="Browser config terminal-gate race",
            status="completed",
            provider="codex",
            model="gpt-5.6-sol",
            effort_level="high",
            metadata_={"keep": "account-binding"},
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    gate_installed = False

    async def install_gate_after_optimistic_read(_request, _task, _db):
        nonlocal gate_installed
        assert not gate_installed
        gate_installed = True
        async with db_factory() as writer:
            current = await writer.get(Task, task_id)
            assert current is not None
            current.metadata_ = {
                **(current.metadata_ or {}),
                "test_harness_terminal_generation": {
                    "incarnation_id": current.incarnation_id,
                    "retry_count": current.retry_count,
                    "turn_generation": current.turn_generation,
                    "status": current.status,
                    "reason": "terminal writer won after route read",
                },
            }
            await writer.commit()

    monkeypatch.setattr(
        test_harness_api,
        "require_task_control",
        install_gate_after_optimistic_read,
    )
    body = test_harness_api.TestHarnessRuntimeConfigUpdate(
        inherit_task=False,
        provider="claude",
        model="claude-opus-5",
        reasoning_effort="max",
        codex_service_tier="default",
    )
    async with db_factory() as route_db:
        payload = await test_harness_api.update_test_harness_runtime_config(
            task_id,
            body,
            SimpleNamespace(),
            route_db,
        )
    assert payload["provider"] == "claude"
    assert gate_installed is True

    async with db_factory() as db:
        persisted = await db.get(Task, task_id)
        assert persisted is not None
        assert persisted.metadata_["keep"] == "account-binding"
        assert persisted.metadata_["test_harness_terminal_generation"][
            "reason"
        ] == "terminal writer won after route read"
        assert persisted.metadata_["test_harness_runtime"]["model"] == (
            "claude-opus-5"
        )
