from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import FastAPI, HTTPException
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from backend.api import browser_reviews
from backend.config import settings
from backend.models.task import Task
from backend.models.test_harness import (
    BrowserReviewOperationReceipt,
    TestHarnessChildBinding,
    TestHarnessRun,
)
from backend.models.workspace_review import WorkspaceReviewRun
from backend.services.internal_service_auth import InternalServiceClaims
from backend.services.browser_review_jobs import (
    BrowserReviewJob,
    BrowserReviewJobManager,
)
from backend.services.browser_review import BrowserReviewOptions
from backend.services.test_harness_artifacts import TestHarnessArtifactStore as ArtifactStore


async def _browser_operation_graph(db_session):
    job_id = "1" * 32
    run_id = "2" * 32
    owner = Task(
        title="Browser owner",
        description="Own one browser run",
        status="executing",
        provider="codex",
        model="gpt-5.6-sol",
    )
    child = Task(
        title="Browser child",
        description="Use only browser tools",
        status="in_progress",
        provider="codex",
        model="gpt-5.6-sol",
        enabled_skills={"browser-review": job_id},
        metadata_={"isolated_browser_agent": True},
        retry_count=0,
        turn_generation=1,
        instance_id=44,
    )
    db_session.add(owner)
    await db_session.flush()
    db_session.add(child)
    await db_session.flush()
    run = TestHarnessRun(
        id=run_id,
        task_id=owner.id,
        owner_task_incarnation_id=owner.incarnation_id,
        owner_task_retry_count=owner.retry_count,
        owner_task_turn_generation=owner.turn_generation,
        owner_task_status=owner.status,
        browser_review_job_id=job_id,
        agent_task_id=child.id,
        target_kind="fixed_url",
        target_spec={"kind": "fixed_url", "url": "https://example.com"},
        test_plan={"objective": "Review"},
        runtime_config={"allow_actions": True, "max_actions": 2},
        request_fingerprint="3" * 64,
        root_run_id=run_id,
        status="running",
        stage="browser_ready",
    )
    binding = TestHarnessChildBinding(
        id="4" * 32,
        harness_run_id=run_id,
        owner_task_id=owner.id,
        owner_task_incarnation_id=owner.incarnation_id,
        owner_task_retry_count=owner.retry_count,
        owner_task_turn_generation=owner.turn_generation,
        owner_task_status=owner.status,
        child_task_id=child.id,
        child_task_incarnation_id=child.incarnation_id,
        browser_review_job_id=job_id,
        state="running",
        claimed_retry_count=child.retry_count,
        claimed_instance_id=child.instance_id,
    )
    db_session.add_all([run, binding])
    await db_session.commit()
    claims = InternalServiceClaims(
        audience="ccm_browser_review",
        token_id="browser-token",
        expires_at=4_000_000_000,
        task_id=child.id,
        task_incarnation_id=child.incarnation_id,
        task_retry_count=child.retry_count,
        task_turn_generation=child.turn_generation,
        task_status=child.status,
        owner_kind="browser-review-job",
        owner_id=job_id,
    )
    request = SimpleNamespace(
        state=SimpleNamespace(
            auth_type="internal_service",
            internal_service_claims=claims,
        )
    )
    return job_id, owner.id, child.id, request


@pytest.mark.asyncio
async def test_browser_interaction_permit_and_ack_are_durable_and_replay_safe(
    db_session,
    monkeypatch,
):
    monkeypatch.setattr(settings, "auth_token", "browser-operation-secret")
    job_id, _owner_id, _child_id, request = await _browser_operation_graph(
        db_session
    )
    operation_id = "5" * 32
    permit = browser_reviews.BrowserOperationPermit(
        action_kind="click",
        request_digest="6" * 64,
        execution_nonce="7" * 32,
    )

    first = await browser_reviews.permit_browser_review_operation(
        job_id,
        operation_id,
        permit,
        request,
        db_session,
    )
    replayed_permit = await browser_reviews.permit_browser_review_operation(
        job_id,
        operation_id,
        permit,
        request,
        db_session,
    )
    assert first == {"state": "permitted", "replayed": False}
    assert replayed_permit == {"state": "permitted", "replayed": True}

    with pytest.raises(HTTPException, match="outcome is uncertain"):
        await browser_reviews.permit_browser_review_operation(
            job_id,
            operation_id,
            permit.model_copy(update={"execution_nonce": "8" * 32}),
            request,
            db_session,
        )

    ack = browser_reviews.BrowserOperationAck(
        request_digest=permit.request_digest,
        execution_nonce=permit.execution_nonce,
        status="completed",
        ack_digest="9" * 64,
        result={"steps": 3, "actions": 1},
    )
    completed = await browser_reviews.acknowledge_browser_review_operation(
        job_id,
        operation_id,
        ack,
        request,
        db_session,
    )
    replayed_ack = await browser_reviews.acknowledge_browser_review_operation(
        job_id,
        operation_id,
        ack,
        request,
        db_session,
    )
    completed_permit = await browser_reviews.permit_browser_review_operation(
        job_id,
        operation_id,
        permit.model_copy(update={"execution_nonce": "a" * 32}),
        request,
        db_session,
    )
    assert completed["state"] == "completed"
    assert completed["replayed"] is False
    assert replayed_ack["replayed"] is True
    assert completed_permit == {
        "state": "completed",
        "replayed": True,
        "result": {"steps": 3, "actions": 1},
    }
    receipt = await db_session.scalar(
        select(BrowserReviewOperationReceipt).where(
            BrowserReviewOperationReceipt.browser_review_job_id == job_id,
            BrowserReviewOperationReceipt.operation_id == operation_id,
        )
    )
    assert receipt is not None
    assert receipt.status == "completed"


@pytest.mark.asyncio
async def test_browser_operation_callback_rejects_stale_owner_generation(
    db_session,
    monkeypatch,
):
    monkeypatch.setattr(settings, "auth_token", "browser-operation-secret")
    job_id, owner_id, _child_id, request = await _browser_operation_graph(
        db_session
    )
    owner = await db_session.get(Task, owner_id)
    owner.turn_generation += 1
    await db_session.commit()

    with pytest.raises(HTTPException, match="owner generation changed"):
        await browser_reviews.permit_browser_review_operation(
            job_id,
            "b" * 32,
            browser_reviews.BrowserOperationPermit(
                action_kind="type",
                request_digest="c" * 64,
                execution_nonce="d" * 32,
            ),
            request,
            db_session,
        )
    assert (
        await db_session.scalar(select(BrowserReviewOperationReceipt.id))
        is None
    )


@pytest.mark.asyncio
async def test_browser_operation_callback_rejects_terminalizing_owner_gate(
    db_session,
    monkeypatch,
):
    monkeypatch.setattr(settings, "auth_token", "browser-operation-secret")
    job_id, owner_id, _child_id, request = await _browser_operation_graph(
        db_session
    )
    owner = await db_session.get(Task, owner_id)
    owner.metadata_ = {
        **(owner.metadata_ or {}),
        "test_harness_terminal_generation": {
            "incarnation_id": owner.incarnation_id,
            "retry_count": owner.retry_count,
            "turn_generation": owner.turn_generation,
            "status": owner.status,
            "reason": "owner terminalizing",
        },
    }
    await db_session.commit()

    with pytest.raises(HTTPException, match="terminalizing"):
        await browser_reviews.permit_browser_review_operation(
            job_id,
            "e" * 32,
            browser_reviews.BrowserOperationPermit(
                action_kind="click",
                request_digest="f" * 64,
                execution_nonce="0" * 32,
            ),
            request,
            db_session,
        )
    assert await db_session.scalar(
        select(BrowserReviewOperationReceipt.id)
    ) is None


@pytest.mark.asyncio
async def test_browser_operation_callback_rejects_cancelling_workspace(
    db_session,
    monkeypatch,
):
    monkeypatch.setattr(settings, "auth_token", "browser-operation-secret")
    job_id, owner_id, _child_id, request = await _browser_operation_graph(
        db_session
    )
    run = await db_session.get(TestHarnessRun, "2" * 32)
    binding = await db_session.get(TestHarnessChildBinding, "4" * 32)
    owner = await db_session.get(Task, owner_id)
    assert run is not None and binding is not None and owner is not None
    workspace_id = "a" * 32
    run.workspace_review_run_id = workspace_id
    binding.workspace_review_run_id = workspace_id
    db_session.add(
        WorkspaceReviewRun(
            id=workspace_id,
            task_id=owner.id,
            owner_task_incarnation_id=owner.incarnation_id,
            owner_task_retry_count=owner.retry_count,
            owner_task_turn_generation=owner.turn_generation,
            owner_task_status=owner.status,
            harness_run_id=run.id,
            agent_task_id=binding.child_task_id,
            browser_review_job_id=job_id,
            mode="review_only",
            profile="standard",
            goal="Reject actions once cancellation begins",
            status="cancelling",
            stage="cancelling",
            workspace_path="/isolated/workspace",
            git_head="b" * 40,
            workspace_fingerprint="c" * 64,
            preview_config={"version": 1},
            cleanup_status="pending",
        )
    )
    await db_session.commit()

    with pytest.raises(HTTPException, match="Workspace generation changed"):
        await browser_reviews.permit_browser_review_operation(
            job_id,
            "d" * 32,
            browser_reviews.BrowserOperationPermit(
                action_kind="click",
                request_digest="e" * 64,
                execution_nonce="f" * 32,
            ),
            request,
            db_session,
        )
    assert await db_session.scalar(
        select(BrowserReviewOperationReceipt.id)
    ) is None


@pytest.mark.asyncio
async def test_legacy_browser_cancel_uses_durable_graph_and_retries_cleanup(
    db_session,
    monkeypatch,
):
    job_id, owner_id, child_id, _request = await _browser_operation_graph(
        db_session
    )
    owner = await db_session.get(Task, owner_id)
    assert owner is not None
    expected_owner = (
        owner.incarnation_id,
        owner.retry_count,
        owner.turn_generation,
        owner.status,
    )
    job = BrowserReviewJob(
        id=job_id,
        options=BrowserReviewOptions(
            url="https://example.com",
            goal="Cancel through the Harness graph",
            model="gpt-5.6-sol",
            reasoning_effort="high",
        ),
        capture_only=False,
        created_at="2026-08-09T00:00:00Z",
        provider="codex",
        task_id=child_id,
        owner_task_id=owner_id,
        harness_run_id="2" * 32,
        status="running",
        stage="browser_ready",
    )
    manager = BrowserReviewJobManager()
    manager._jobs[job_id] = job
    calls: list[tuple[str, object]] = []

    class FakeHarnessService:
        async def cancel(self, run_id, *, expected_identity):
            calls.append((run_id, expected_identity))
            if len(calls) == 1:
                return SimpleNamespace(
                    id=run_id,
                    status="cancelled",
                    cleanup_status="failed",
                    cleanup_error="first exact child reap failed",
                )
            job.status = "cancelled"
            job.stage = "cancelled"
            return SimpleNamespace(
                id=run_id,
                status="cancelled",
                cleanup_status="completed",
                cleanup_error=None,
            )

    monkeypatch.setattr(
        browser_reviews,
        "test_harness_service",
        FakeHarnessService(),
    )
    request = SimpleNamespace()
    with pytest.raises(HTTPException, match="first exact child reap failed") as caught:
        await browser_reviews.cancel_browser_review(
            job_id,
            request,
            db_session,
            manager,
        )
    assert caught.value.status_code == 409

    result = await browser_reviews.cancel_browser_review(
        job_id,
        request,
        db_session,
        manager,
    )
    assert result["status"] == "cancelled"
    assert [call[0] for call in calls] == ["2" * 32, "2" * 32]
    for _run_id, identity in calls:
        assert identity.task_id == owner_id
        assert identity.incarnation_id == expected_owner[0]
        assert identity.retry_count == expected_owner[1]
        assert identity.turn_generation == expected_owner[2]
        assert identity.status == expected_owner[3]


@pytest.mark.asyncio
@pytest.mark.parametrize("configured_auth", ["", "   "])
async def test_browser_review_api_requires_auth_before_any_materialization(
    monkeypatch, tmp_path, configured_auth
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

        async def rollback(self):
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

        async def start_fixed_url_browser(self, *, run_id, inline):
            assert run_id == "h" * 32
            assert inline is False
            job = await manager.prepare_agent(
                BrowserReviewOptions(
                    url=self.spec.target["url"],
                    goal=self.spec.goal,
                    model=settings.default_codex_model,
                    reasoning_effort="medium",
                ),
                provider="codex",
                codex_service_tier="default",
                harness_run_id=run_id,
            )
            await manager.attach_task(job.id, 92, owner_task_id=91)
            return job

        async def sync_browser_job(self, _job):
            return None

    monkeypatch.setattr(browser_reviews, "TaskQueue", FakeTaskQueue)
    monkeypatch.setattr(browser_reviews, "test_harness_service", FakeHarnessService())
    monkeypatch.setattr(settings, "auth_token", configured_auth)
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
        assert created.status_code == 503, created.text
        assert "AUTH_TOKEN" in created.json()["detail"]
        assert created_task == {}
        assert await manager.list() == []
    await manager.shutdown()


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
async def test_ordinary_task_browser_review_requires_auth_before_materialization(
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

        async def commit(self):
            return None

    task_state = {"status": "in_progress", "trace_events": []}

    async def read_task(_task_id: int):
        return dict(task_state)

    monkeypatch.setattr(settings, "auth_token", "")
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
        assert started.status_code == 503, started.text
        assert "AUTH_TOKEN" in started.json()["detail"]
        assert await manager.list() == []
    await manager.shutdown()


@pytest.mark.asyncio
async def test_task_browser_routes_revalidate_exact_parent_generation(
    monkeypatch,
    db_factory,
):
    monkeypatch.setattr(settings, "auth_token", "frontend-review-secret")
    async with db_factory() as db:
        owner = Task(
            title="Frontend Review owner",
            status="executing",
            provider="codex",
            model="gpt-5.6-sol",
            effort_level="high",
        )
        db.add(owner)
        await db.commit()
        owner_id = owner.id
        claims = InternalServiceClaims(
            audience="ccm_frontend_review",
            token_id="frontend-review-token",
            expires_at=4_000_000_000,
            task_id=owner.id,
            task_incarnation_id=owner.incarnation_id,
            task_retry_count=owner.retry_count,
            task_turn_generation=owner.turn_generation,
            task_status=owner.status,
            owner_kind="task",
            owner_id=owner.id,
        )
    request = SimpleNamespace(
        state=SimpleNamespace(
            auth_type="internal_service",
            internal_service_claims=claims,
        )
    )
    job_id = "7" * 32
    run_id = "8" * 32
    job_state: dict[str, object] = {
        "id": job_id,
        "task_id": None,
        "owner_task_id": owner_id,
        "status": "queued",
    }

    class FakeJob:
        id = job_id

        @property
        def task_id(self):
            return job_state["task_id"]

        @property
        def owner_task_id(self):
            return job_state["owner_task_id"]

        def as_dict(self):
            return dict(job_state)

    job = FakeJob()
    captured: dict[str, object] = {}

    class FakeHarnessService:
        async def start_task_run(self, *, task_id, spec, owner_identity, **_kwargs):
            captured["identity"] = owner_identity
            async with db_factory() as db:
                db.add(
                    TestHarnessRun(
                        id=run_id,
                        task_id=task_id,
                        owner_task_incarnation_id=owner_identity.incarnation_id,
                        owner_task_retry_count=owner_identity.retry_count,
                        owner_task_turn_generation=owner_identity.turn_generation,
                        owner_task_status=owner_identity.status,
                        target_kind="fixed_url",
                        target_spec={"url": spec.target["url"]},
                        test_plan={"objective": spec.goal},
                        runtime_config={"allow_actions": spec.allow_actions},
                        request_fingerprint="9" * 64,
                        root_run_id=run_id,
                        status="running",
                        stage="waiting_for_browser",
                    )
                )
                await db.commit()
            return SimpleNamespace(id=run_id)

        async def start_fixed_url_browser(self, *, run_id, inline):
            assert run_id == "8" * 32 and inline is False
            async with db_factory() as db:
                child = Task(
                    title="Isolated Browser child",
                    status="pending",
                    provider="codex",
                    model="gpt-5.6-sol",
                )
                db.add(child)
                await db.flush()
                owner = await db.get(Task, owner_id)
                db.add(
                    TestHarnessChildBinding(
                        id="a" * 32,
                        harness_run_id=run_id,
                        owner_task_id=owner.id,
                        owner_task_incarnation_id=owner.incarnation_id,
                        owner_task_retry_count=owner.retry_count,
                        owner_task_turn_generation=owner.turn_generation,
                        owner_task_status=owner.status,
                        child_task_id=child.id,
                        child_task_incarnation_id=child.incarnation_id,
                        browser_review_job_id=job_id,
                        state="ready",
                    )
                )
                await db.commit()
                job_state["task_id"] = child.id
            return job

    class FakeManager:
        async def get(self, requested_job_id):
            return job if requested_job_id == job_id else None

    monkeypatch.setattr(
        browser_reviews,
        "test_harness_service",
        FakeHarnessService(),
    )
    manager = FakeManager()
    body = browser_reviews.TaskBrowserReviewStart(
        url="https://example.com",
        goal="Review exact generation",
        allow_actions=False,
        max_actions=0,
    )
    async with db_factory() as db:
        started = await browser_reviews.start_task_browser_review_internal(
            owner_id,
            body,
            request,
            db,
            manager,
        )
    assert started["id"] == job_id
    assert captured["identity"].incarnation_id == claims.task_incarnation_id

    async with db_factory() as db:
        status_payload = (
            await browser_reviews.get_task_browser_review_internal_status(
                owner_id,
                job_id,
                request,
                db,
                manager,
            )
        )
    assert status_payload["id"] == job_id

    async with db_factory() as db:
        owner = await db.get(Task, owner_id)
        owner.turn_generation += 1
        await db.commit()
    async with db_factory() as db:
        with pytest.raises(HTTPException) as caught:
            await browser_reviews.get_task_browser_review_internal_status(
                owner_id,
                job_id,
                request,
                db,
                manager,
            )
    assert caught.value.status_code == 403
