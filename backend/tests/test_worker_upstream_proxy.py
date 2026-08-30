"""Manager -> Worker HTTP proxy boundary contracts.

No request in this module leaves the test process.  These checks keep Worker
credentials and failures separate from the browser's Manager authentication.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import HTTPException

import backend.api.workers as workers_api
from backend.models.user import User
from backend.models.worker import Worker
from backend.schemas.global_settings import RuntimeSettingsUpdate


pytestmark = pytest.mark.usefixtures("worker_control_plane_auth")


async def _insert_ready_worker(
    session_factory,
    *,
    owner_user_id: int | None = None,
    bootstrap_step: str | None = None,
) -> Worker:
    async with session_factory() as db:
        worker = Worker(
            name="proxy-worker",
            status="ready",
            private_ip="10.0.0.9",
            ccm_port=8123,
            ssh_user="ubuntu",
            ssh_key_path="/tmp/test-worker-key",
            auth_token="internal-worker-token",
            accounts=[],
            owner_user_id=owner_user_id,
            bootstrap_step=bootstrap_step,
        )
        db.add(worker)
        await db.commit()
        await db.refresh(worker)
        return worker


class _FakeResponse:
    def __init__(self, status_code: int, body: object):
        self.status_code = status_code
        self._body = body
        self.text = (
            body
            if isinstance(body, str)
            else "<invalid-json>"
            if isinstance(body, _InvalidJSON)
            else json.dumps(body)
        )

    def json(self):
        if isinstance(self._body, _InvalidJSON):
            raise ValueError("invalid JSON")
        return self._body


class _InvalidJSON:
    pass


def _install_worker_transport(monkeypatch, outcomes):
    pending = list(outcomes)
    requests: list[tuple[str, str, dict]] = []

    class FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            self.timeout = kwargs.get("timeout")

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def _send(self, method: str, url: str, **kwargs):
            requests.append((method, url, kwargs))
            outcome = pending.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome

        async def get(self, url: str, **kwargs):
            return await self._send("GET", url, **kwargs)

        async def put(self, url: str, **kwargs):
            return await self._send("PUT", url, **kwargs)

        async def delete(self, url: str, **kwargs):
            return await self._send("DELETE", url, **kwargs)

    monkeypatch.setattr(workers_api.httpx, "AsyncClient", FakeAsyncClient)
    return requests, pending


def _request(*, user_id: int, role: str):
    return SimpleNamespace(
        state=SimpleNamespace(
            user_id=user_id,
            user_role=role,
            auth_type="jwt",
        )
    )


async def test_worker_owner_can_update_non_privileged_runtime_setting(
    session_factory,
    monkeypatch,
):
    async with session_factory() as db:
        owner = User(
            email="runtime-setting-owner@example.com",
            name="runtime setting owner",
            password_hash="test",
            role="member",
            is_active=True,
        )
        db.add(owner)
        await db.commit()
        await db.refresh(owner)
        owner_id = owner.id

    worker = await _insert_ready_worker(
        session_factory,
        owner_user_id=owner_id,
    )
    response_body = {"use_pty_mode": False}
    requests, pending = _install_worker_transport(
        monkeypatch,
        [_FakeResponse(200, response_body)],
    )

    async with session_factory() as db:
        response = await workers_api.update_worker_runtime_settings(
            worker.id,
            _request(user_id=owner_id, role="member"),
            RuntimeSettingsUpdate(use_pty_mode=False),
            db,
        )

    assert response == response_body
    assert requests[0][2]["json"] == {"use_pty_mode": False}
    assert not pending


async def test_worker_runtime_update_rejects_stale_member_ownership(
    session_factory,
    monkeypatch,
):
    """The final Worker CAS, not the preliminary read, owns admission."""

    import backend.api.deps as deps

    async with session_factory() as db:
        former_owner = User(
            email="former-runtime-owner@example.com",
            name="former runtime owner",
            password_hash="test",
            role="member",
            is_active=True,
        )
        current_owner = User(
            email="current-runtime-owner@example.com",
            name="current runtime owner",
            password_hash="test",
            role="member",
            is_active=True,
        )
        db.add_all((former_owner, current_owner))
        await db.commit()
        await db.refresh(former_owner)
        await db.refresh(current_owner)
        former_owner_id = former_owner.id
        current_owner_id = current_owner.id

    worker = await _insert_ready_worker(
        session_factory,
        owner_user_id=current_owner_id,
    )
    requests, pending = _install_worker_transport(
        monkeypatch,
        [_FakeResponse(200, {"use_pty_mode": False})],
    )
    # Model the TOCTOU point where the preliminary read saw the old owner.
    monkeypatch.setattr(
        deps,
        "require_worker_access",
        AsyncMock(return_value=None),
    )

    async with session_factory() as db:
        with pytest.raises(HTTPException) as rejected:
            await workers_api.update_worker_runtime_settings(
                worker.id,
                _request(user_id=former_owner_id, role="member"),
                RuntimeSettingsUpdate(use_pty_mode=False),
                db,
            )

    assert rejected.value.status_code == 409
    assert requests == []
    assert len(pending) == 1


async def test_worker_runtime_update_rejects_stale_admin_role(
    session_factory,
    monkeypatch,
):
    """A demotion committed before the final fence prevents remote writes."""

    async with session_factory() as db:
        stale_admin = User(
            email="stale-runtime-admin@example.com",
            name="stale runtime admin",
            password_hash="test",
            role="member",
            is_active=True,
        )
        db.add(stale_admin)
        await db.commit()
        await db.refresh(stale_admin)
        stale_admin_id = stale_admin.id

    worker = await _insert_ready_worker(session_factory)
    requests, pending = _install_worker_transport(
        monkeypatch,
        [_FakeResponse(200, {"use_pty_mode": False})],
    )

    async with session_factory() as db:
        with pytest.raises(HTTPException) as rejected:
            await workers_api.update_worker_runtime_settings(
                worker.id,
                _request(user_id=stale_admin_id, role="admin"),
                RuntimeSettingsUpdate(use_pty_mode=False),
                db,
            )

    assert rejected.value.status_code == 409
    assert "changed role" in str(rejected.value.detail)
    assert requests == []
    assert len(pending) == 1


@pytest.mark.parametrize(
    ("method", "path", "body"),
    [
        (
            "post",
            "/pool/add",
            {
                "email": "blocked@example.com",
                "provider": "claude",
                "token": "mail-token",
            },
        ),
        ("delete", "/pool/codex-1?provider=codex", None),
        ("put", "/settings/runtime", {"use_pty_mode": False}),
    ],
)
async def test_ready_destroy_recovery_worker_rejects_new_mutations(
    client,
    session_factory,
    monkeypatch,
    method,
    path,
    body,
):
    worker = await _insert_ready_worker(
        session_factory,
        bootstrap_step="destroy",
    )
    requests, _pending = _install_worker_transport(monkeypatch, [])

    kwargs = {"json": body} if body is not None else {}
    response = await getattr(client, method)(
        f"/api/workers/{worker.id}{path}",
        **kwargs,
    )

    assert response.status_code == 409
    assert "destroy" in response.json()["detail"]
    assert requests == []


@pytest.mark.parametrize("remote_status", [401, 403])
@pytest.mark.parametrize(
    ("method", "path", "body"),
    [
        ("get", "/pool?provider=codex", None),
        ("get", "/pool/usage?provider=codex", None),
        ("delete", "/pool/codex-1?provider=codex", None),
        ("get", "/settings/runtime", None),
        ("put", "/settings/runtime", {"use_pty_mode": False}),
    ],
)
async def test_worker_auth_failures_are_never_forwarded_as_manager_auth_errors(
    client,
    session_factory,
    monkeypatch,
    remote_status,
    method,
    path,
    body,
):
    worker = await _insert_ready_worker(session_factory)
    requests, pending = _install_worker_transport(
        monkeypatch,
        [_FakeResponse(remote_status, {"detail": "remote credential rejected"})],
    )

    request = getattr(client, method)
    kwargs = {"json": body} if body is not None else {}
    response = await request(f"/api/workers/{worker.id}{path}", **kwargs)

    assert response.status_code == 502
    assert "Worker 认证失败" in response.json()["detail"]
    assert str(remote_status) in response.json()["detail"]
    assert "remote credential rejected" not in response.json()["detail"]
    assert len(requests) == 1
    assert requests[0][2]["headers"] == {
        "Authorization": "Bearer internal-worker-token"
    }
    assert not pending


async def test_ready_worker_rejects_unrecoverable_dynamic_claude_account_add(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _insert_ready_worker(session_factory)
    requests, _pending = _install_worker_transport(monkeypatch, [])

    response = await client.post(
        f"/api/workers/{worker.id}/pool/add",
        json={
            "email": "claude@example.com",
            "provider": "claude",
            "token": "mail-token",
        },
    )

    assert response.status_code == 409
    assert "暂不可用" in response.json()["detail"]
    assert requests == []


async def test_worker_connection_error_is_a_gateway_error(
    client, session_factory, monkeypatch,
):
    worker = await _insert_ready_worker(session_factory)
    requests, _ = _install_worker_transport(
        monkeypatch,
        [httpx.ConnectError("private address unreachable")],
    )

    response = await client.get(
        f"/api/workers/{worker.id}/settings/runtime"
    )

    assert response.status_code == 502
    assert "Worker 网关连接失败" in response.json()["detail"]
    assert len(requests) == 1


@pytest.mark.parametrize("remote_status", [429, 500, 503])
async def test_worker_usage_does_not_fallback_on_quota_or_upstream_failure(
    client, session_factory, monkeypatch, remote_status,
):
    worker = await _insert_ready_worker(session_factory)
    requests, pending = _install_worker_transport(
        monkeypatch,
        [_FakeResponse(remote_status, {"detail": "usage unavailable"})],
    )

    response = await client.get(
        f"/api/workers/{worker.id}/pool/usage?provider=codex"
    )

    assert response.status_code == 502
    assert f"HTTP {remote_status}" in response.json()["detail"]
    # No status request may disguise this as an empty/healthy pool.
    assert len(requests) == 1
    assert not pending


async def test_worker_usage_falls_back_to_status_only_for_legacy_404(
    client, session_factory, monkeypatch,
):
    worker = await _insert_ready_worker(session_factory)
    status = {
        "enabled": True,
        "total": 1,
        "available": 1,
        "accounts": [{"id": "codex-1", "available": True}],
    }
    requests, pending = _install_worker_transport(
        monkeypatch,
        [
            _FakeResponse(404, {"detail": "legacy worker has no usage route"}),
            _FakeResponse(200, status),
        ],
    )

    response = await client.get(
        f"/api/workers/{worker.id}/pool/usage?provider=codex"
    )

    assert response.status_code == 200
    assert response.json() == status
    assert [url for _method, url, _kwargs in requests] == [
        "http://10.0.0.9:8123/api/codex-pool/usage?force=true",
        "http://10.0.0.9:8123/api/codex-pool/status",
    ]
    assert not pending


async def test_worker_usage_reports_explicit_disabled_pool_after_two_404s(
    client, session_factory, monkeypatch,
):
    worker = await _insert_ready_worker(session_factory)
    requests, pending = _install_worker_transport(
        monkeypatch,
        [
            _FakeResponse(404, {"detail": "legacy usage route missing"}),
            _FakeResponse(404, {"detail": "pool not enabled"}),
        ],
    )

    response = await client.get(
        f"/api/workers/{worker.id}/pool/usage?provider=claude"
    )

    assert response.status_code == 200
    assert response.json() == {
        "enabled": False,
        "total": 0,
        "available": 0,
        "accounts": [],
    }
    assert len(requests) == 2
    assert not pending


async def test_worker_malformed_json_is_an_upstream_gateway_error(
    client, session_factory, monkeypatch,
):
    worker = await _insert_ready_worker(session_factory)
    _install_worker_transport(
        monkeypatch,
        [_FakeResponse(200, _InvalidJSON())],
    )

    response = await client.get(
        f"/api/workers/{worker.id}/settings/runtime"
    )

    assert response.status_code == 502
    assert "无效 JSON" in response.json()["detail"]
