"""Worker nodes expose execution protocols, never a second human CCM UI."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from backend.api.auth import create_jwt
from backend.config import Settings, settings
from backend.models.task import Task


def test_worker_settings_require_non_empty_deployment_token():
    with pytest.raises(
        ValidationError,
        match="CCM_NODE_ROLE=worker requires a non-empty AUTH_TOKEN",
    ):
        Settings(
            _env_file=None,
            ccm_node_role="worker",
            auth_token="   ",
        )

    configured = Settings(
        _env_file=None,
        ccm_node_role="worker",
        auth_token="worker-secret",
    )
    assert configured.auth_token == "worker-secret"


@pytest.mark.asyncio
async def test_worker_http_is_health_plus_authenticated_protocol_only(
    client,
    monkeypatch,
):
    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(settings, "auth_token", "worker-secret")

    assert (await client.get("/api/system/health")).status_code == 200
    assert (await client.get("/api/tasks/count")).status_code == 401
    headless = await client.get("/")
    assert headless.status_code == 404
    assert "no human-facing HTTP UI" in headless.json()["detail"]

    fake_user = SimpleNamespace(id=7, email="admin@example.com", role="admin")
    jwt_token = create_jwt(fake_user)
    rejected_jwt = await client.get(
        "/api/tasks/count",
        headers={"Authorization": f"Bearer {jwt_token}"},
    )
    assert rejected_jwt.status_code == 401
    assert "deployment" in rejected_jwt.json()["detail"]

    headers = {"Authorization": "Bearer worker-secret"}
    assert (
        await client.get("/api/system/stats", headers=headers)
    ).status_code == 200
    # Collection browsing is a Manager UI concern, not a Worker protocol.
    assert (
        await client.get("/api/tasks/count", headers=headers)
    ).status_code == 403
    assert (
        await client.get("/api/projects", headers=headers)
    ).status_code == 200

    manager_only = await client.get("/api/workers", headers=headers)
    assert manager_only.status_code == 503
    assert "CCM_NODE_ROLE=manager" in manager_only.json()["detail"]
    assert (
        await client.get("/api/instances", headers=headers)
    ).status_code == 403


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("POST", "/api/plans"),
        ("PATCH", "/api/plans/1"),
        ("POST", "/api/plans/1/runs"),
        ("POST", "/api/plans/1/fork"),
        ("POST", "/api/plan-runs/1/cancel"),
        ("POST", "/api/plan-versions/1/approve"),
        ("POST", "/api/plan-versions/1/reject"),
        ("POST", "/api/plan-versions/1/create-execution-task"),
        ("POST", "/api/tasks/1/plan/approve"),
        ("POST", "/api/tasks/1/plan/reject"),
        ("POST", "/api/tasks/1/plan/revise"),
        ("POST", "/api/tasks/1/plan/create-execution-task"),
        ("POST", "/api/tasks/1/retry"),
        ("POST", "/api/tasks/1/archive"),
    ],
)
async def test_worker_deployment_token_rejects_human_mutation_routes(
    client,
    monkeypatch,
    method,
    path,
):
    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(settings, "auth_token", "worker-secret")

    response = await client.request(
        method,
        path,
        headers={"Authorization": "Bearer worker-secret"},
    )

    assert response.status_code == 403
    assert "outside the CCM Worker control-plane" in response.json()["detail"]


@pytest.mark.asyncio
async def test_worker_deployment_token_chat_requires_exact_handoff(
    client,
    monkeypatch,
):
    """A node credential alone is never authority for a model turn."""

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(settings, "auth_token", "worker-secret")

    response = await client.post(
        "/api/tasks/42/chat",
        headers={"Authorization": "Bearer worker-secret"},
        json={"message": "do not synthesize a system turn"},
    )

    assert response.status_code == 403
    assert "exact turn handoff" in response.json()["detail"]


async def _worker_plain_task_request(
    client,
    *,
    method: str,
    task_id: int,
    incarnation_id: str | None,
):
    headers = {"Authorization": "Bearer worker-secret"}
    if incarnation_id is not None:
        headers["X-CCM-Task-Incarnation"] = incarnation_id
    return await client.request(
        method,
        f"/api/tasks/{task_id}",
        headers=headers,
        json={"title": "worker-updated"} if method == "PUT" else None,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["GET", "PUT", "DELETE"])
@pytest.mark.parametrize(
    "incarnation_header",
    [None, "not-an-incarnation", "b" * 32],
    ids=["missing", "malformed", "wrong"],
)
async def test_worker_plain_task_routes_require_exact_incarnation_header(
    client,
    session_factory,
    monkeypatch,
    method,
    incarnation_header,
):
    """The deployment credential alone cannot select a logical Task."""

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(settings, "auth_token", "worker-secret")
    async with session_factory() as db:
        task = Task(
            title="worker-original",
            description="plain route identity",
            status="completed",
            incarnation_id="a" * 32,
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    response = await _worker_plain_task_request(
        client,
        method=method,
        task_id=task_id,
        incarnation_id=incarnation_header,
    )

    assert response.status_code == 409, response.text
    async with session_factory() as db:
        current = await db.get(Task, task_id)
    assert current is not None
    assert current.incarnation_id == "a" * 32
    assert current.title == "worker-original"


@pytest.mark.asyncio
async def test_worker_plain_put_rejects_missing_header_before_body_parsing(
    client,
    monkeypatch,
):
    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(settings, "auth_token", "worker-secret")

    response = await client.put(
        "/api/tasks/42",
        headers={
            "Authorization": "Bearer worker-secret",
            "Content-Type": "application/json",
        },
        content=b"{not valid JSON",
    )

    assert response.status_code == 409, response.text
    assert "incarnation header" in response.json()["detail"]


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["GET", "PUT", "DELETE"])
async def test_worker_plain_task_routes_accept_exact_incarnation_header(
    client,
    session_factory,
    monkeypatch,
    method,
):
    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(settings, "auth_token", "worker-secret")
    incarnation_id = "c" * 32
    async with session_factory() as db:
        task = Task(
            title="worker-original",
            description="plain route identity",
            status="completed",
            incarnation_id=incarnation_id,
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    response = await _worker_plain_task_request(
        client,
        method=method,
        task_id=task_id,
        incarnation_id=incarnation_id,
    )

    assert response.status_code == 200, response.text
    async with session_factory() as db:
        current = await db.get(Task, task_id)
    if method == "DELETE":
        assert current is None
    else:
        assert current is not None
        assert current.incarnation_id == incarnation_id
        assert current.title == (
            "worker-updated" if method == "PUT" else "worker-original"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["GET", "PUT", "DELETE"])
async def test_worker_plain_task_routes_reject_stale_incarnation_after_id_aba(
    client,
    session_factory,
    monkeypatch,
    method,
):
    """A deleted/recreated integer id must not alias the former Task."""

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(settings, "auth_token", "worker-secret")
    stale_incarnation = "d" * 32
    replacement_incarnation = "e" * 32
    async with session_factory() as db:
        old = Task(
            title="old incarnation",
            description="will be replaced",
            status="completed",
            incarnation_id=stale_incarnation,
        )
        db.add(old)
        await db.commit()
        await db.refresh(old)
        task_id = old.id
        await db.delete(old)
        await db.commit()
        db.add(Task(
            id=task_id,
            title="replacement incarnation",
            description="must survive stale request",
            status="completed",
            incarnation_id=replacement_incarnation,
        ))
        await db.commit()

    response = await _worker_plain_task_request(
        client,
        method=method,
        task_id=task_id,
        incarnation_id=stale_incarnation,
    )

    assert response.status_code == 409, response.text
    async with session_factory() as db:
        replacement = await db.get(Task, task_id)
    assert replacement is not None
    assert replacement.incarnation_id == replacement_incarnation
    assert replacement.title == "replacement incarnation"


def test_worker_plan_protocol_allowlist_is_exact():
    from backend.middleware.auth import TokenAuthMiddleware

    allowed = {
        ("POST", "/api/plans/worker-repo-revision"),
        ("POST", "/api/plans/worker-import"),
        ("POST", "/api/plans/worker-materialize-version"),
        ("GET", "/api/plans/worker-application-receipts/receipt-1"),
        ("POST", "/api/plans/worker-application-receipts/receipt-1/resolve"),
        ("GET", "/api/plans/42/versions"),
        ("GET", "/api/plan-runs/43"),
        ("GET", "/api/plan-runs/43/worker-import-audit"),
        ("POST", "/api/plan-runs/43/worker-import-cancel"),
        ("POST", "/api/plan-runs/43/input-requests/44/answer"),
    }
    denied = {
        ("POST", "/api/plans/42/runs"),
        ("POST", "/api/plan-runs/43/cancel"),
        ("POST", "/api/plan-versions/45/approve"),
        ("GET", "/api/plans/count"),
    }

    assert all(
        TokenAuthMiddleware._worker_control_plane_path_allowed(method, path)
        for method, path in allowed
    )
    assert not any(
        TokenAuthMiddleware._worker_control_plane_path_allowed(method, path)
        for method, path in denied
    )


def test_worker_task_project_and_pool_protocol_allowlists_are_exact():
    """Deployment auth follows actual Manager calls, not human API prefixes."""

    from backend.middleware.auth import TokenAuthMiddleware

    allowed = {
        ("POST", "/api/tasks"),
        ("GET", "/api/tasks/42"),
        ("PUT", "/api/tasks/42"),
        ("DELETE", "/api/tasks/42"),
        ("POST", "/api/tasks/migration-import"),
        ("POST", "/api/tasks/migration-import/commit"),
        ("POST", "/api/tasks/migration-import/rollback"),
        ("POST", "/api/tasks/42/chat"),
        ("GET", "/api/tasks/42/chat/history"),
        ("GET", "/api/tasks/42/plan/staleness"),
        ("GET", "/api/tasks/42/plan/runs"),
        ("GET", "/api/tasks/42/artifacts/download"),
        ("POST", "/api/tasks/42/monitor-sessions"),
        ("DELETE", "/api/tasks/42/monitor-sessions/7"),
        ("POST", "/api/tasks/42/sub-agent-sessions"),
        ("DELETE", "/api/tasks/42/sub-agent-sessions/8"),
        ("GET", "/api/tasks/42/worker-turn-handoffs/handoff"),
        ("POST", "/api/tasks/42/worker-turn-handoffs/handoff/resume"),
        ("GET", "/api/tasks/42/routing-config/status"),
        ("POST", "/api/tasks/42/routing-config/stage"),
        ("GET", "/api/tasks/42/internal/worker-retry-receipts/op"),
        ("POST", "/api/tasks/42/internal/worker-retry"),
        ("GET", "/api/tasks/42/internal/worker-plan-decisions/op"),
        ("PUT", "/api/tasks/42/internal/worker-plan-decisions/op"),
        ("GET", "/api/tasks/42/termination-receipts/op"),
        ("PUT", "/api/tasks/42/termination-receipts/op"),
        ("POST", "/api/tasks/42/termination-receipts/op/ack"),
        ("GET", "/api/projects"),
        ("POST", "/api/projects"),
        ("GET", "/api/projects/3"),
        ("GET", "/api/pool/status"),
        ("GET", "/api/pool/usage"),
        ("POST", "/api/pool/accounts/default/relogin"),
        ("DELETE", "/api/pool/accounts/default"),
        ("GET", "/api/codex-pool/status"),
        ("GET", "/api/codex-pool/usage"),
        ("POST", "/api/codex-pool/add"),
        ("GET", "/api/codex-pool/add/user@example.com"),
        ("GET", "/api/codex-pool/accounts/codex-1/verify"),
        ("GET", "/api/codex-pool/accounts/codex-1/relogin"),
        ("POST", "/api/codex-pool/accounts/codex-1/relogin"),
        ("DELETE", "/api/codex-pool/accounts/codex-1"),
        ("GET", "/api/codex-pool/login-attempts/attempt"),
        ("POST", "/api/codex-pool/login-attempts/attempt/otp"),
        ("DELETE", "/api/codex-pool/login-attempts/attempt"),
    }
    denied = {
        ("GET", "/api/tasks"),
        ("GET", "/api/tasks/count"),
        ("POST", "/api/tasks/42/share"),
        ("DELETE", "/api/tasks/42/share/member"),
        ("GET", "/api/tasks/42/shares"),
        ("PUT", "/api/tasks/42/ssh-grants"),
        ("POST", "/api/tasks/42/test-runs"),
        ("PUT", "/api/tasks/42/test-runs/config"),
        ("PUT", "/api/projects/3"),
        ("DELETE", "/api/projects/3"),
        ("POST", "/api/projects/3/reclone"),
        ("GET", "/api/projects/3/env-files/.env"),
        ("PUT", "/api/projects/3/env-files/.env"),
        ("POST", "/api/tasks/42/capability-invocations"),
        ("GET", "/api/capability-invocations/invocation"),
        ("POST", "/api/capability-invocations/invocation/cancel"),
        ("POST", "/api/pool/reload"),
        ("POST", "/api/codex-pool/maintenance"),
    }

    assert all(
        TokenAuthMiddleware._worker_control_plane_path_allowed(method, path)
        for method, path in allowed
    )
    assert not any(
        TokenAuthMiddleware._worker_control_plane_path_allowed(method, path)
        for method, path in denied
    )


@pytest.mark.asyncio
async def test_worker_deployment_token_can_begin_node_drain(
    client,
    monkeypatch,
):
    """The exact irreversible destroy route is part of the control plane."""

    from backend.api import system as system_api
    from backend.services.worker_drain_proof import (
        WORKER_NODE_DRAIN_PROOF_PROTOCOL,
    )

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(settings, "auth_token", "worker-secret")
    begin = AsyncMock()
    monkeypatch.setattr(system_api, "begin_worker_node_drain", begin)
    claim = "b" * 64

    response = await client.post(
        "/api/system/worker-drain/begin",
        headers={"Authorization": "Bearer worker-secret"},
        json={
            "protocol_version": WORKER_NODE_DRAIN_PROOF_PROTOCOL,
            "drain_claim": claim,
        },
    )

    assert response.status_code == 200, response.text
    assert response.json()["drain_claim"] == claim
    assert response.json()["draining"] is True
    assert len(response.json()["signature"]) == 64
    begin.assert_awaited_once()


@pytest.mark.asyncio
async def test_worker_deployment_token_can_seal_node_runtime(
    client,
    monkeypatch,
):
    """Phase two is reachable and drains callbacks before the durable seal."""

    from backend import main as backend_main
    from backend.api import system as system_api
    from backend.services.worker_drain_proof import (
        WORKER_NODE_DRAIN_PROOF_PROTOCOL,
    )

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(settings, "auth_token", "worker-secret")
    claim = "b" * 64
    drain_permission_callbacks = AsyncMock(return_value=0)
    recover_publications = AsyncMock(return_value=(0, 0))
    seal = AsyncMock(
        return_value={
            "protocol_version": WORKER_NODE_DRAIN_PROOF_PROTOCOL,
            "node_role": "worker",
            "drain_claim": claim,
            "runtime_sealed": True,
            "safe_to_destroy": True,
            "safe_to_seal": True,
            "blockers": [],
            "blocker_count": 0,
            "task_count": 0,
        }
    )
    monkeypatch.setattr(
        backend_main.instance_manager,
        "drain_pty_permission_callbacks",
        drain_permission_callbacks,
    )
    monkeypatch.setattr(
        backend_main.instance_manager,
        "recover_pty_terminal_publications",
        recover_publications,
    )
    monkeypatch.setattr(system_api, "seal_worker_node_runtime", seal)

    response = await client.post(
        "/api/system/worker-drain/seal",
        headers={"Authorization": "Bearer worker-secret"},
        json={
            "protocol_version": WORKER_NODE_DRAIN_PROOF_PROTOCOL,
            "drain_claim": claim,
        },
    )

    assert response.status_code == 200, response.text
    assert response.json()["runtime_sealed"] is True
    assert response.json()["safe_to_seal"] is True
    assert len(response.json()["signature"]) == 64
    drain_permission_callbacks.assert_awaited_once_with()
    recover_publications.assert_awaited_once_with()
    seal.assert_awaited_once()


@pytest.mark.asyncio
async def test_worker_deployment_token_can_request_node_drain_proof(
    client,
    monkeypatch,
):
    """The Manager destroy protocol must reach the exact Worker route."""

    from backend.api import system as system_api
    from backend.services.worker_drain_proof import (
        WORKER_NODE_DRAIN_PROOF_PROTOCOL,
    )

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(settings, "auth_token", "worker-secret")
    proof = {
        "protocol_version": WORKER_NODE_DRAIN_PROOF_PROTOCOL,
        "nonce": "a" * 32,
        "node_role": "worker",
        "drain_claim": "b" * 64,
        "safe_to_destroy": True,
        "blocker_count": 0,
        "blockers": [],
    }
    build_proof = AsyncMock(return_value=proof)
    monkeypatch.setattr(
        system_api,
        "build_worker_node_drain_proof",
        build_proof,
    )

    response = await client.post(
        "/api/system/worker-drain-proof",
        headers={"Authorization": "Bearer worker-secret"},
        json={
            "protocol_version": WORKER_NODE_DRAIN_PROOF_PROTOCOL,
            "nonce": "a" * 32,
            "drain_claim": "b" * 64,
        },
    )

    assert response.status_code == 200, response.text
    assert response.json()["safe_to_destroy"] is True
    assert len(response.json()["signature"]) == 64
    build_proof.assert_awaited_once()


@pytest.mark.asyncio
async def test_worker_deployment_token_cannot_create_untracked_local_task(
    client,
    monkeypatch,
):
    """Only explicit Manager mirrors may enter through the Worker Task API."""

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(settings, "auth_token", "worker-secret")

    response = await client.post(
        "/api/tasks",
        headers={"Authorization": "Bearer worker-secret"},
        json={"title": "hidden local task", "description": "must not exist"},
    )

    assert response.status_code == 403, response.text
    assert "Manager-mirrored Task identity" in response.json()["detail"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("POST", "/api/auth/login"),
        ("POST", "/api/auth/register"),
        ("POST", "/api/auth/send-code"),
        ("POST", "/api/github/webhook"),
        ("POST", "/api/feishu/callback"),
        ("POST", "/api/shared/receive"),
        ("POST", "/api/shared/revoke"),
        ("GET", "/api/shared-access/1/history"),
    ],
)
async def test_worker_explicitly_rejects_human_and_share_endpoints(
    client,
    monkeypatch,
    method,
    path,
):
    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(settings, "auth_token", "worker-secret")

    response = await client.request(
        method,
        path,
        headers={"Authorization": "Bearer worker-secret"},
    )

    assert response.status_code == 403
    assert "headless CCM Worker" in response.json()["detail"]


@pytest.mark.asyncio
async def test_worker_request_boundary_stays_closed_if_token_is_cleared(
    client,
    monkeypatch,
):
    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(settings, "auth_token", "")

    assert (await client.get("/api/system/health")).status_code == 200
    blocked = await client.get("/api/tasks/count")
    assert blocked.status_code == 503
    assert "requires a configured" in blocked.json()["detail"]


@pytest.mark.asyncio
async def test_worker_accepts_generation_scoped_harness_callback(
    client,
    session_factory,
    monkeypatch,
):
    from backend import database
    from backend.services.internal_service_auth import (
        issue_internal_service_token,
    )

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(settings, "auth_token", "worker-secret")
    monkeypatch.setattr(database, "async_session", session_factory)

    async with session_factory() as db:
        task = Task(
            title="worker harness owner",
            description="callback",
            status="executing",
            incarnation_id="a" * 32,
            retry_count=2,
            turn_generation=5,
            provider="codex",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    token = issue_internal_service_token(
        audience="ccm_frontend_review",
        task_id=task_id,
        task_incarnation_id="a" * 32,
        task_retry_count=2,
        task_turn_generation=5,
        task_status="executing",
        owner_kind="worker-harness-test",
        owner_id="owner-1",
    )
    response = await client.get(
        f"/api/tasks/{task_id}/test-runs/capabilities",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200, response.text
    assert response.json()["contract_version"] == 1


def test_worker_websocket_accepts_only_deployment_token(monkeypatch):
    from backend.api.ws import _ws_identity

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(settings, "auth_token", "worker-secret")
    deployment_ws = SimpleNamespace(
        headers={"authorization": "Bearer worker-secret"},
        query_params={},
    )
    identity = _ws_identity(deployment_ws)
    assert identity == {
        "user_id": None,
        "role": "super_admin",
        "auth_type": "worker_control_plane",
    }

    fake_user = SimpleNamespace(id=9, email="member@example.com", role="member")
    jwt_ws = SimpleNamespace(
        headers={},
        query_params={"token": create_jwt(fake_user)},
    )
    assert _ws_identity(jwt_ws) is None

    monkeypatch.setattr(settings, "auth_token", "")
    assert _ws_identity(deployment_ws) is None


@pytest.mark.asyncio
async def test_worker_does_not_start_delivery_control_plane(monkeypatch):
    import backend.main as main

    monkeypatch.setattr(main.settings, "ccm_node_role", "worker")
    monkeypatch.setattr(main.settings, "auto_start_dispatcher", True)
    monkeypatch.setattr(
        main,
        "start_dispatcher_runtime",
        AsyncMock(),
    )
    monkeypatch.setattr(main, "worker_relay", None)
    monkeypatch.setattr(
        main.worker_task_termination_coordinator,
        "recover_once",
        AsyncMock(),
    )
    monkeypatch.setattr(
        main.worker_task_termination_coordinator,
        "start",
        AsyncMock(),
    )
    monkeypatch.setattr(main.capability_coordinator, "start", AsyncMock())
    monkeypatch.setattr(main.delivery_controller, "start", AsyncMock())

    await main._start_execution_runtimes()

    main.start_dispatcher_runtime.assert_awaited_once_with()
    main.worker_task_termination_coordinator.recover_once.assert_awaited_once_with(
        include_manager=False
    )
    main.worker_task_termination_coordinator.start.assert_awaited_once_with()
    main.capability_coordinator.start.assert_awaited_once_with()
    main.delivery_controller.start.assert_not_awaited()
