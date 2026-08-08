from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI, Request
from httpx import ASGITransport, AsyncClient

from backend.api import test_harness as test_harness_api
from backend.database import get_db
from backend.models.task import Task
from backend.models.test_harness import TestHarnessEvidence as EvidenceModel
from backend.services.test_harness import TestHarnessService as HarnessService
from backend.services.test_harness_artifacts import (
    TestHarnessArtifactStore as ArtifactStore,
)
from backend.services.test_harness_contracts import TestHarnessSpec as HarnessSpec


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
