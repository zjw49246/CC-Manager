from __future__ import annotations

import asyncio
import base64
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from backend.api import browser_reviews
from backend.config import settings
from backend.services.browser_review_jobs import BrowserReviewJobManager
from backend.services.browser_review import BrowserReviewOptions
from backend.services.test_harness_artifacts import TestHarnessArtifactStore as ArtifactStore


@pytest.mark.asyncio
async def test_browser_review_api_creates_ccm_task_and_records_evidence(
    monkeypatch, tmp_path
):
    task_state = {
        "status": "in_progress",
        "error": None,
        "assistant_report": None,
    }

    async def read_task(_task_id: int):
        return dict(task_state)

    created_task: dict = {}

    class FakeTaskQueue:
        def __init__(self, _db):
            pass

        async def create(self, **kwargs):
            created_task.update(kwargs)
            return SimpleNamespace(
                id=91,
                metadata_=dict(kwargs.get("metadata_") or {}),
                description=kwargs["description"],
            )

    class FakeDb:
        async def commit(self):
            return None

    class FakeHarnessService:
        async def start_task_run(self, *, task_id, spec, **_kwargs):
            assert task_id == 91
            self.spec = spec
            return SimpleNamespace(
                id="h" * 32,
                test_plan={
                    "version": 1,
                    "objective": spec.goal,
                    "scenarios": [],
                },
            )

        async def attach_browser_job(self, *, run_id, job, **_kwargs):
            job.harness_run_id = run_id

        async def sync_browser_job(self, _job):
            return None

    monkeypatch.setattr(browser_reviews, "TaskQueue", FakeTaskQueue)
    monkeypatch.setattr(browser_reviews, "test_harness_service", FakeHarnessService())
    monkeypatch.setattr(settings, "auth_token", "")
    manager = BrowserReviewJobManager(
        task_reader=read_task,
        poll_interval=0.01,
        artifact_store=ArtifactStore(tmp_path / "artifacts"),
    )
    app = FastAPI()
    app.include_router(browser_reviews.router)
    app.dependency_overrides[browser_reviews.require_admin] = lambda: None
    app.dependency_overrides[browser_reviews.get_db] = lambda: FakeDb()
    app.dependency_overrides[
        browser_reviews.get_browser_review_job_manager
    ] = lambda: manager

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        config = await client.get("/api/browser-reviews/config")
        assert config.status_code == 200
        assert config.json()["execution"] == "ccm_task_account_pool"
        assert config.json()["browser_channels"][0] == "chromium"
        assert {"claude", "codex"}.intersection(config.json()["providers"])

        created = await client.post(
            "/api/browser-reviews",
            json={
                "url": "https://example.com",
                "provider": "codex",
                "model": settings.default_codex_model,
                "reasoning_effort": "medium",
            },
        )
        assert created.status_code == 202, created.text
        job_id = created.json()["id"]
        assert created.json()["task_id"] == 91
        assert created.json()["browser_channel"] == "chromium"
        assert created_task["provider"] == "codex"
        assert created_task["enabled_skills"] == {"browser-review": job_id}
        assert "finish_review" in created_task["description"]

        context = await client.get(
            f"/api/browser-reviews/{job_id}/internal/context"
        )
        assert context.status_code == 200
        assert context.json()["url"] == "https://example.com"
        assert context.json()["network_policy"] == "external_public"

        png = b"\x89PNG\r\n\x1a\nfinal"
        evidence = await client.post(
            f"/api/browser-reviews/{job_id}/internal/events",
            json={
                "stage": "agent_reported",
                "steps": 1,
                "actions": 1,
                "screenshot_base64": base64.b64encode(png).decode(),
                "telemetry": {"page_errors": [{"message": "render exploded"}]},
                "action_batch": [{"type": "scroll", "scroll_y": 500}],
                "report": "# Browser result",
                "verdict": "failed",
                "findings": [
                    {
                        "scenario_id": "runtime-health",
                        "severity": "high",
                        "category": "runtime",
                        "title": "The page crashed",
                        "route": "/settings",
                        "locator": "main",
                        "expected": "The page remains usable",
                        "actual": "A page error was reported",
                        "reproduction": ["Open settings"],
                        "evidence": ["final.png"],
                        "confidence": 0.95,
                    }
                ],
            },
        )
        assert evidence.status_code == 200, evidence.text
        task_state["status"] = "completed"
        job = await manager.get(job_id)
        assert job is not None and job.task is not None
        await asyncio.wait_for(job.task, timeout=1)

        polled = await client.get(f"/api/browser-reviews/{job_id}")
        assert polled.status_code == 200
        assert polled.json()["status"] == "completed"
        assert polled.json()["report"] == "# Browser result"
        assert polled.json()["verdict"] == "failed"
        assert polled.json()["findings"][0]["title"] == "The page crashed"

        artifact = await client.get(
            f"/api/browser-reviews/{job_id}/artifacts/final.png"
        )
        assert artifact.status_code == 200
        assert artifact.headers["content-type"] == "image/png"
        assert artifact.content == png


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("url", "message"),
    [
        ("file:///tmp/index.html", "http or https"),
        ("http://127.0.0.1:8000/admin", "public IP"),
        ("http://169.254.169.254/latest/meta-data", "public IP"),
    ],
)
async def test_browser_review_api_rejects_unsafe_url(monkeypatch, url, message):
    manager = BrowserReviewJobManager()
    app = FastAPI()
    app.include_router(browser_reviews.router)
    app.dependency_overrides[browser_reviews.require_admin] = lambda: None
    app.dependency_overrides[browser_reviews.get_db] = lambda: object()
    app.dependency_overrides[
        browser_reviews.get_browser_review_job_manager
    ] = lambda: manager

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/api/browser-reviews",
            json={
                "url": url,
                "provider": "codex",
                "model": settings.default_codex_model,
                "reasoning_effort": "medium",
            },
        )

    assert response.status_code == 422
    assert message in response.json()["detail"]


@pytest.mark.asyncio
async def test_ordinary_task_can_start_and_list_isolated_browser_review(
    monkeypatch,
    tmp_path,
):
    task = SimpleNamespace(
        id=73,
        status="in_progress",
        provider="claude",
        model="claude-opus-4-6",
        effort_level="high",
        codex_service_tier="default",
    )

    class FakeDb:
        async def get(self, _model, task_id):
            return task if task_id == task.id else None

    async def allow_task_access(*_args):
        return None

    task_state = {"status": "in_progress", "trace_events": []}

    async def read_task(_task_id: int):
        return dict(task_state)

    monkeypatch.setattr(settings, "auth_token", "")
    monkeypatch.setattr(browser_reviews, "require_task_access", allow_task_access)
    manager = BrowserReviewJobManager(
        task_reader=read_task,
        artifact_store=ArtifactStore(tmp_path / "artifacts"),
    )

    class FakeHarnessService:
        async def start_task_run(self, *, task_id, spec, **_kwargs):
            assert task_id == task.id
            self.spec = spec
            return SimpleNamespace(id="i" * 32)

        async def start_fixed_url_browser(self, *, run_id, inline):
            assert run_id == "i" * 32
            assert inline is False
            job = await manager.prepare_agent(
                BrowserReviewOptions(
                    url=self.spec.target["url"],
                    goal=self.spec.goal,
                    model=task.model,
                    reasoning_effort=task.effort_level,
                    allow_actions=self.spec.allow_actions,
                    browser_channel=(
                        "chrome" if self.spec.browser_channel == "chrome" else None
                    ),
                    viewport_width=self.spec.viewport_width,
                    viewport_height=self.spec.viewport_height,
                    max_steps=self.spec.max_steps,
                    max_actions=self.spec.max_actions,
                ),
                provider=task.provider,
                codex_service_tier=task.codex_service_tier,
                harness_run_id=run_id,
            )
            await manager.attach_task(job.id, task.id, owner_task_id=task.id)
            return job

        async def sync_browser_job(self, _job):
            return None

    monkeypatch.setattr(browser_reviews, "test_harness_service", FakeHarnessService())
    app = FastAPI()
    app.include_router(browser_reviews.router)
    app.include_router(browser_reviews.task_router)
    app.dependency_overrides[browser_reviews.require_admin] = lambda: None
    app.dependency_overrides[browser_reviews.get_db] = lambda: FakeDb()
    app.dependency_overrides[
        browser_reviews.get_browser_review_job_manager
    ] = lambda: manager

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        started = await client.post(
            "/api/tasks/73/browser-reviews/internal/start",
            json={
                "url": "https://example.com",
                "goal": "Check the task UI",
                "viewport_width": 390,
                "viewport_height": 844,
                "max_actions": 0,
            },
        )
        assert started.status_code == 201, started.text
        payload = started.json()
        assert payload["task_id"] == 73
        assert payload["inline_tool"] is False
        assert payload["provider"] == "claude"
        assert payload["model"] == "claude-opus-4-6"
        assert payload["browser_channel"] == "chromium"
        assert payload["viewport_width"] == 390
        assert payload["viewport_height"] == 844
        assert payload["max_actions"] == 0
        job_id = payload["id"]

        completed = await client.post(
            f"/api/browser-reviews/{job_id}/internal/events",
            json={
                "stage": "agent_reported",
                "steps": 1,
                "actions": 0,
                "report": "# Task report",
            },
        )
        assert completed.status_code == 200, completed.text
        task_state["status"] = "completed"
        job = await manager.get(job_id)
        assert job is not None and job.task is not None
        await asyncio.wait_for(job.task, timeout=1)

        status_response = await client.get(
            f"/api/tasks/73/browser-reviews/{job_id}/internal/status"
        )
        assert status_response.status_code == 200
        assert status_response.json()["status"] == "completed"

        listed = await client.get("/api/tasks/73/browser-reviews")
        assert listed.status_code == 200, listed.text
        assert listed.json()[0]["report"] == "# Task report"
    await manager.shutdown()
