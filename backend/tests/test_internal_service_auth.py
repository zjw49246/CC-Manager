"""Security tests for Task-scoped CCM child-process credentials."""

from fastapi import FastAPI, Request
from httpx import ASGITransport, AsyncClient
import pytest

from backend.config import settings
from backend.api.deps import require_task_access
from backend.middleware.auth import TokenAuthMiddleware
from backend.models.task import Task
from backend.services import internal_service_auth as auth


TASK_INCARNATION = "a" * 32
TASK_RETRY_COUNT = 2
TASK_TURN_GENERATION = 7
TASK_STATUS = "executing"


@pytest.fixture(autouse=True)
def _internal_auth_state(monkeypatch):
    monkeypatch.setattr(settings, "auth_token", "deployment-secret")
    with auth._revocation_lock:
        auth._owner_tokens.clear()
        auth._owner_token_cache.clear()
        auth._revoked_tokens.clear()
    yield
    with auth._revocation_lock:
        auth._owner_tokens.clear()
        auth._owner_token_cache.clear()
        auth._revoked_tokens.clear()


def _issue(audience: str, **claims) -> str:
    if claims.get("task_id") is not None:
        claims.setdefault("task_incarnation_id", TASK_INCARNATION)
    if audience in {
        "ccm_ssh",
        "ccm_skills",
        "ccm_ask_user",
        "ccm_frontend_review",
        "ccm_workspace_review",
        "ccm_browser_review",
    }:
        claims.setdefault("task_retry_count", TASK_RETRY_COUNT)
        claims.setdefault("task_turn_generation", TASK_TURN_GENERATION)
        claims.setdefault("task_status", TASK_STATUS)
    return auth.issue_internal_service_token(
        audience=audience,
        owner_kind="test",
        owner_id=f"{audience}-owner",
        **claims,
    )


@pytest.mark.parametrize("configured", ["", "   ", "\t\n", None])
def test_internal_service_tokens_require_nonblank_deployment_secret(
    monkeypatch,
    configured,
):
    monkeypatch.setattr(settings, "auth_token", configured)
    token = auth.issue_internal_service_token(
        audience="ccm_skills",
        task_id=42,
        task_incarnation_id=TASK_INCARNATION,
        task_retry_count=TASK_RETRY_COUNT,
        task_turn_generation=TASK_TURN_GENERATION,
        task_status=TASK_STATUS,
        owner_kind="test",
        owner_id="blank-secret",
    )
    assert token == ""


def test_ssh_credential_is_bound_to_task_method_and_route():
    token = _issue("ccm_ssh", task_id=42)

    claims = auth.authenticate_internal_service_token(
        token,
        method="GET",
        path="/api/tasks/42/ssh-access",
    )
    assert claims.task_id == 42
    assert claims.task_retry_count == TASK_RETRY_COUNT
    assert claims.task_turn_generation == TASK_TURN_GENERATION
    assert claims.task_status == TASK_STATUS
    auth.authenticate_internal_service_token(
        token,
        method="POST",
        path="/api/tasks/42/ssh-access/7/read",
    )

    for method, path in (
        ("GET", "/api/tasks/43/ssh-access"),
        ("POST", "/api/tasks/42/ssh-access/7/delete"),
        ("DELETE", "/api/tasks/42/ssh-access/7/read"),
        ("GET", "/api/ssh/profiles"),
        ("GET", "/api/instances"),
    ):
        with pytest.raises(auth.InternalServiceTokenError) as exc:
            auth.authenticate_internal_service_token(
                token,
                method=method,
                path=path,
            )
        assert exc.value.status_code == 403


def test_skills_credential_cannot_use_the_general_task_update_route():
    token = _issue("ccm_skills", task_id=42)

    auth.authenticate_internal_service_token(
        token,
        method="PUT",
        path="/api/tasks/42/internal/enabled-skills",
    )
    with pytest.raises(auth.InternalServiceTokenError) as exc:
        auth.authenticate_internal_service_token(
            token,
            method="PUT",
            path="/api/tasks/42",
        )
    assert exc.value.status_code == 403


def test_browser_child_credential_is_bound_to_exact_job_routes():
    job_id = "b" * 32
    token = auth.issue_internal_service_token(
        audience="ccm_browser_review",
        task_id=42,
        task_incarnation_id=TASK_INCARNATION,
        task_retry_count=TASK_RETRY_COUNT,
        task_turn_generation=TASK_TURN_GENERATION,
        task_status=TASK_STATUS,
        owner_kind="browser-review-job",
        owner_id=job_id,
    )
    for method, path in (
        ("GET", f"/api/browser-reviews/{job_id}/internal/context"),
        ("POST", f"/api/browser-reviews/{job_id}/internal/events"),
        (
            "POST",
            f"/api/browser-reviews/{job_id}/internal/operations/{'c' * 32}/permit",
        ),
        (
            "POST",
            f"/api/browser-reviews/{job_id}/internal/operations/{'c' * 32}/ack",
        ),
    ):
        claims = auth.authenticate_internal_service_token(
            token,
            method=method,
            path=path,
        )
        assert claims.owner_id == job_id

    for method, path in (
        ("GET", f"/api/browser-reviews/{'d' * 32}/internal/context"),
        ("POST", f"/api/browser-reviews/{job_id}/cancel"),
        ("GET", "/api/instances"),
    ):
        with pytest.raises(auth.InternalServiceTokenError) as exc:
            auth.authenticate_internal_service_token(
                token,
                method=method,
                path=path,
            )
        assert exc.value.status_code == 403


@pytest.mark.parametrize(
    "audience",
    ("ccm_frontend_review", "ccm_workspace_review"),
)
def test_parent_browser_credentials_are_task_and_generation_scoped(audience):
    token = _issue(audience, task_id=42)
    auth.authenticate_internal_service_token(
        token,
        method="POST",
        path="/api/tasks/42/test-runs/internal/start",
    )
    auth.authenticate_internal_service_token(
        token,
        method="GET",
        path=f"/api/tasks/42/test-runs/{'e' * 32}/internal/status",
    )
    with pytest.raises(auth.InternalServiceTokenError):
        auth.authenticate_internal_service_token(
            token,
            method="POST",
            path="/api/tasks/43/test-runs/internal/start",
        )


@pytest.mark.parametrize(
    ("audience", "claims", "method", "path"),
    [
        (
            "ccm_skills",
            {"task_id": 42},
            "POST",
            "/api/tasks/42/sub-agent-sessions",
        ),
        (
            "ccm_monitor_agent",
            {"task_id": 42, "monitor_session_id": 8},
            "POST",
            "/api/tasks/42/monitor-sessions/8/checks",
        ),
        (
            "ccm_sub_agent",
            {"task_id": 42, "sub_agent_session_id": 9},
            "POST",
            "/api/tasks/42/sub-agent-sessions/9/result",
        ),
        (
            "ccm_ask_user",
            {"task_id": 42},
            "POST",
            "/api/ask-user/wait",
        ),
    ],
)
def test_each_mcp_audience_accepts_only_its_callback_surface(
    audience,
    claims,
    method,
    path,
):
    token = _issue(audience, **claims)
    auth.authenticate_internal_service_token(token, method=method, path=path)

    with pytest.raises(auth.InternalServiceTokenError) as exc:
        auth.authenticate_internal_service_token(
            token,
            method="GET",
            path="/api/tasks/42/ssh-access",
        )
    assert exc.value.status_code == 403


def test_tampered_expired_and_revoked_credentials_are_rejected(monkeypatch):
    monkeypatch.setattr(auth.time, "time", lambda: 1_000)
    token = auth.issue_internal_service_token(
        audience="ccm_ssh",
        task_id=42,
        task_incarnation_id=TASK_INCARNATION,
        task_retry_count=TASK_RETRY_COUNT,
        task_turn_generation=TASK_TURN_GENERATION,
        task_status=TASK_STATUS,
        owner_kind="task-turn",
        owner_id=42,
        ttl_seconds=10,
    )
    prefix, payload, signature = token.split(".")
    tampered = f"{prefix}.{payload[:-1]}A.{signature}"

    with pytest.raises(auth.InternalServiceTokenError):
        auth.authenticate_internal_service_token(
            tampered,
            method="GET",
            path="/api/tasks/42/ssh-access",
        )

    auth.revoke_internal_service_owner("task-turn", 42)
    with pytest.raises(auth.InternalServiceTokenError, match="revoked"):
        auth.authenticate_internal_service_token(
            token,
            method="GET",
            path="/api/tasks/42/ssh-access",
        )

    fresh = auth.issue_internal_service_token(
        audience="ccm_ssh",
        task_id=42,
        task_incarnation_id=TASK_INCARNATION,
        task_retry_count=TASK_RETRY_COUNT,
        task_turn_generation=TASK_TURN_GENERATION,
        task_status=TASK_STATUS,
        owner_kind="task-turn",
        owner_id=42,
        ttl_seconds=10,
    )
    monkeypatch.setattr(auth.time, "time", lambda: 1_011)
    with pytest.raises(auth.InternalServiceTokenError, match="expired"):
        auth.authenticate_internal_service_token(
            fresh,
            method="GET",
            path="/api/tasks/42/ssh-access",
        )


def test_identical_owner_scope_reuses_token_until_revoked(monkeypatch):
    monkeypatch.setattr(auth.time, "time", lambda: 1_000)
    first = auth.issue_internal_service_token(
        audience="ccm_ssh",
        task_id=42,
        task_incarnation_id=TASK_INCARNATION,
        task_retry_count=TASK_RETRY_COUNT,
        task_turn_generation=TASK_TURN_GENERATION,
        task_status=TASK_STATUS,
        owner_kind="task-turn",
        owner_id=42,
        ttl_seconds=600,
    )
    second = auth.issue_internal_service_token(
        audience="ccm_ssh",
        task_id=42,
        task_incarnation_id=TASK_INCARNATION,
        task_retry_count=TASK_RETRY_COUNT,
        task_turn_generation=TASK_TURN_GENERATION,
        task_status=TASK_STATUS,
        owner_kind="task-turn",
        owner_id=42,
        ttl_seconds=600,
    )
    assert second == first

    auth.revoke_internal_service_owner("task-turn", 42)
    replacement = auth.issue_internal_service_token(
        audience="ccm_ssh",
        task_id=42,
        task_incarnation_id=TASK_INCARNATION,
        task_retry_count=TASK_RETRY_COUNT,
        task_turn_generation=TASK_TURN_GENERATION,
        task_status=TASK_STATUS,
        owner_kind="task-turn",
        owner_id=42,
        ttl_seconds=600,
    )
    assert replacement != first


def test_task_scoped_credential_requires_exact_incarnation():
    with pytest.raises(
        ValueError,
        match="Task-scoped credential requires a Task incarnation",
    ):
        auth.issue_internal_service_token(
            audience="ccm_skills",
            task_id=42,
            owner_kind="task-turn",
            owner_id=42,
        )


@pytest.mark.parametrize(
    "audience",
    (
        "ccm_ssh",
        "ccm_skills",
        "ccm_ask_user",
        "ccm_frontend_review",
        "ccm_workspace_review",
        "ccm_browser_review",
    ),
)
def test_main_task_credentials_require_exact_active_generation(audience):
    with pytest.raises(ValueError, match="exact active generation"):
        auth.issue_internal_service_token(
            audience=audience,
            task_id=42,
            task_incarnation_id=TASK_INCARNATION,
            owner_kind="task-turn",
            owner_id=42,
        )
    with pytest.raises(ValueError, match="active Task status"):
        auth.issue_internal_service_token(
            audience=audience,
            task_id=42,
            task_incarnation_id=TASK_INCARNATION,
            task_retry_count=0,
            task_turn_generation=0,
            task_status="completed",
            owner_kind="task-turn",
            owner_id=42,
        )


@pytest.mark.asyncio
async def test_middleware_requires_bearer_and_never_elevates_scoped_token(
    monkeypatch,
):
    current_task = {
        "id": 42,
        "incarnation_id": "a" * 32,
        "retry_count": TASK_RETRY_COUNT,
        "turn_generation": TASK_TURN_GENERATION,
        "status": TASK_STATUS,
    }

    class AuthoritativeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def scalar(self, statement):
            values = set(statement.compile().params.values())
            required = {
                current_task["id"],
                current_task["incarnation_id"],
            }
            if "tasks.retry_count" in str(statement):
                required.update({
                    current_task["retry_count"],
                    current_task["turn_generation"],
                    current_task["status"],
                })
            if required.issubset(values):
                return current_task["id"]
            return None

    monkeypatch.setattr(
        "backend.database.async_session",
        lambda: AuthoritativeSession(),
    )
    app = FastAPI()
    app.add_middleware(TokenAuthMiddleware)

    @app.get("/api/tasks/42/ssh-access")
    async def task_ssh_access(request: Request):
        return {
            "auth_type": request.state.auth_type,
            "role": request.state.user_role,
            "task_id": request.state.internal_service_claims.task_id,
        }

    @app.get("/api/instances")
    async def instances():
        return {"ok": True}

    @app.post("/api/tasks/42/sub-agent-sessions")
    async def create_sub_agent():
        return {"ok": True}

    @app.post("/api/tasks/42/monitor-sessions/8/checks")
    async def monitor_check():
        return {"ok": True}

    @app.post("/api/tasks/42/sub-agent-sessions/9/result")
    async def sub_agent_result():
        return {"ok": True}

    @app.post("/api/ask-user/wait")
    async def ask_user_wait():
        return {"ok": True}

    @app.put("/api/tasks/42/internal/enabled-skills")
    async def task_replaced_after_middleware(request: Request):
        # Middleware authenticated incarnation A. Simulate delete/import/id
        # reuse before the route loads its authoritative Task row.
        replacement = Task(
            id=42,
            incarnation_id="b" * 32,
            title="replacement",
            status="pending",
        )
        await require_task_access(request, replacement, None)
        return {"ok": True}

    token = _issue(
        "ccm_ssh",
        task_id=42,
        task_incarnation_id=current_task["incarnation_id"],
    )
    stale_scoped_credentials = (
        (
            "GET",
            "/api/tasks/42/ssh-access",
            token,
        ),
        (
            "POST",
            "/api/tasks/42/sub-agent-sessions",
            _issue("ccm_skills", task_id=42),
        ),
        (
            "POST",
            "/api/tasks/42/monitor-sessions/8/checks",
            _issue(
                "ccm_monitor_agent",
                task_id=42,
                monitor_session_id=8,
            ),
        ),
        (
            "POST",
            "/api/tasks/42/sub-agent-sessions/9/result",
            _issue(
                "ccm_sub_agent",
                task_id=42,
                sub_agent_session_id=9,
            ),
        ),
        (
            "POST",
            "/api/ask-user/wait",
            _issue("ccm_ask_user", task_id=42),
        ),
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        allowed = await client.get(
            "/api/tasks/42/ssh-access",
            headers={"Authorization": f"Bearer {token}"},
        )
        query_rejected = await client.get(
            "/api/tasks/42/ssh-access",
            params={"token": token},
        )
        admin_rejected = await client.get(
            "/api/instances",
            headers={"Authorization": f"Bearer {token}"},
        )
        route_toctou_rejected = await client.put(
            "/api/tasks/42/internal/enabled-skills",
            headers={
                "Authorization": (
                    "Bearer "
                    f"{_issue('ccm_skills', task_id=42)}"
                ),
            },
        )
        stale_generations = []
        for field, stale_value in (
            ("retry_count", TASK_RETRY_COUNT + 1),
            ("turn_generation", TASK_TURN_GENERATION + 1),
            ("status", "in_progress"),
        ):
            original = current_task[field]
            current_task[field] = stale_value
            stale_generations.append(
                await client.get(
                    "/api/tasks/42/ssh-access",
                    headers={"Authorization": f"Bearer {token}"},
                )
            )
            current_task[field] = original
        # Simulate a Manager restart followed by integer Task-id reuse. The
        # in-memory issue/revocation caches are gone, so only the durable
        # incarnation lookup can reject the old process credential.
        with auth._revocation_lock:
            auth._owner_tokens.clear()
            auth._owner_token_cache.clear()
            auth._revoked_tokens.clear()
        current_task["incarnation_id"] = "b" * 32
        stale_after_restart = []
        for method, path, scoped_token in stale_scoped_credentials:
            stale_after_restart.append(
                await client.request(
                    method,
                    path,
                    headers={
                        "Authorization": f"Bearer {scoped_token}",
                    },
                )
            )

    assert allowed.status_code == 200
    assert allowed.json() == {
        "auth_type": "internal_service",
        "role": "internal_service",
        "task_id": 42,
    }
    assert query_rejected.status_code == 401
    assert admin_rejected.status_code == 403
    assert route_toctou_rejected.status_code == 403
    assert [response.status_code for response in stale_generations] == [
        401,
        401,
        401,
    ]
    assert {
        response.json()["detail"] for response in stale_generations
    } == {"Internal service SSH Task generation is stale"}
    assert [response.status_code for response in stale_after_restart] == [
        401,
        401,
        401,
        401,
        401,
    ]
    assert {
        response.json()["detail"] for response in stale_after_restart
    } == {
        "Internal service SSH Task generation is stale",
        "Internal service Task generation is stale",
        "Internal service Task incarnation is stale",
    }


@pytest.mark.asyncio
async def test_skills_and_ask_tokens_recheck_generation_after_restart(
    monkeypatch,
):
    current = {
        "id": 42,
        "incarnation": TASK_INCARNATION,
        "retry": TASK_RETRY_COUNT,
        "turn": TASK_TURN_GENERATION,
        "status": TASK_STATUS,
    }

    class AuthoritativeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def scalar(self, statement):
            values = set(statement.compile().params.values())
            required = {
                current["id"],
                current["incarnation"],
                current["retry"],
                current["turn"],
                current["status"],
            }
            return current["id"] if required.issubset(values) else None

    monkeypatch.setattr(
        "backend.database.async_session",
        lambda: AuthoritativeSession(),
    )
    app = FastAPI()
    app.add_middleware(TokenAuthMiddleware)

    @app.post("/api/tasks/42/internal/skill-tools")
    async def skill_tools():
        return {"ok": True}

    @app.post("/api/ask-user/wait")
    async def ask_user_wait():
        return {"ok": True}

    tokens = {
        "skills": _issue("ccm_skills", task_id=42),
        "ask": _issue("ccm_ask_user", task_id=42),
    }
    routes = {
        "skills": "/api/tasks/42/internal/skill-tools",
        "ask": "/api/ask-user/wait",
    }
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        for name, token in tokens.items():
            allowed = await client.post(
                routes[name],
                headers={"Authorization": f"Bearer {token}"},
            )
            assert allowed.status_code == 200

        # Revocation/caches disappear on restart. Durable Task state remains
        # the sole authority for old child credentials.
        with auth._revocation_lock:
            auth._owner_tokens.clear()
            auth._owner_token_cache.clear()
            auth._revoked_tokens.clear()

        rejected = []
        for field, stale_value in (
            ("retry", TASK_RETRY_COUNT + 1),
            ("turn", TASK_TURN_GENERATION + 1),
            ("status", "completed"),
        ):
            original = current[field]
            current[field] = stale_value
            for name, token in tokens.items():
                rejected.append(await client.post(
                    routes[name],
                    headers={"Authorization": f"Bearer {token}"},
                ))
            current[field] = original

    assert [response.status_code for response in rejected] == [401] * 6
    assert {
        response.json()["detail"] for response in rejected
    } == {"Internal service Task generation is stale"}
