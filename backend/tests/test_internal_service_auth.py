"""Security tests for Task-scoped CCM child-process credentials."""

from fastapi import FastAPI, Request
from httpx import ASGITransport, AsyncClient
import pytest

from backend.config import settings
from backend.middleware.auth import TokenAuthMiddleware
from backend.services import internal_service_auth as auth


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
    return auth.issue_internal_service_token(
        audience=audience,
        owner_kind="test",
        owner_id=f"{audience}-owner",
        **claims,
    )


def test_ssh_credential_is_bound_to_task_method_and_route():
    token = _issue("ccm_ssh", task_id=42)

    claims = auth.authenticate_internal_service_token(
        token,
        method="GET",
        path="/api/tasks/42/ssh-access",
    )
    assert claims.task_id == 42
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
        owner_kind="task-turn",
        owner_id=42,
        ttl_seconds=600,
    )
    second = auth.issue_internal_service_token(
        audience="ccm_ssh",
        task_id=42,
        owner_kind="task-turn",
        owner_id=42,
        ttl_seconds=600,
    )
    assert second == first

    auth.revoke_internal_service_owner("task-turn", 42)
    replacement = auth.issue_internal_service_token(
        audience="ccm_ssh",
        task_id=42,
        owner_kind="task-turn",
        owner_id=42,
        ttl_seconds=600,
    )
    assert replacement != first


@pytest.mark.asyncio
async def test_middleware_requires_bearer_and_never_elevates_scoped_token():
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

    token = _issue("ccm_ssh", task_id=42)
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

    assert allowed.status_code == 200
    assert allowed.json() == {
        "auth_type": "internal_service",
        "role": "internal_service",
        "task_id": 42,
    }
    assert query_rejected.status_code == 401
    assert admin_rejected.status_code == 403
