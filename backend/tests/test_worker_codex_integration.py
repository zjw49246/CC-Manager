"""Integration contracts for provider-aware Worker account management.

These tests keep all traffic local and mocked.  They specifically lock down the
boundary where the Manager sends Codex credentials through SSH stdin to the
Worker-local CCM API, rather than duplicating Codex login state machinery.
"""

from __future__ import annotations

import asyncio
import base64
import json
import secrets
import shlex
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, call

import httpx
import pytest
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

import backend.api.deps as api_deps
import backend.api.workers as workers_api
import backend.main as main_module
import backend.services.worker_provisioner as worker_provisioner_module
from backend.models.worker import Worker
from backend.services.worker_provisioner import (
    CLAUDE_LOGIN_IDENTITY_KEY,
    WorkerProvisioner,
    worker_claude_login_identity,
    worker_create_client_token,
    worker_create_client_token_digest,
)
from backend.services.worker_drain_proof import (
    worker_node_drain_proof_signature,
)
from backend.services.worker_proxy import (
    build_worker_destroy_termination_receipt,
    capture_worker_destroy_lifecycle_claim,
    worker_destroy_provision_spec_digest,
)


pytestmark = pytest.mark.usefixtures("worker_control_plane_auth")


TEST_CLOUD_SCOPE = {
    "provider": "aws",
    "partition": "aws",
    "account_id": "123456789012",
    "region": "us-east-1",
}


async def _insert_worker(session_factory, **fields) -> Worker:
    fields.setdefault("status", "ready")
    fields.setdefault("private_ip", "10.0.0.9")
    fields.setdefault("ssh_user", "ubuntu")
    fields.setdefault("ssh_key_path", "/tmp/worker-key")
    fields.setdefault("auth_token", "worker-auth-token")
    async with session_factory() as db:
        worker = Worker(name="codex-worker", **fields)
        db.add(worker)
        await db.commit()
        await db.refresh(worker)
        return worker


async def _drain_worker_background_tasks() -> None:
    while workers_api._background_tasks:
        await asyncio.gather(*tuple(workers_api._background_tasks))


async def _authorize_direct_destroy(session_factory, worker_id: int):
    """Install the same final cloud-effect outbox required in production."""

    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
        worker.status = "destroying"
        worker.destroy_lifecycle_nonce = secrets.token_hex(16)
        await db.flush()
        claim = capture_worker_destroy_lifecycle_claim(worker)
        worker.provision_spec = {
            "version": 1,
            "name": worker.name,
            "has_fixed_overrides": False,
            "overrides": {},
            "cloud_scope": TEST_CLOUD_SCOPE,
            "client_token_digest": worker_create_client_token_digest(
                worker.id,
                worker.auth_token,
            ),
        }
        proof = {
            "protocol_version": 3,
            "nonce": "f" * 32,
            "node_role": "worker",
            "drain_claim": claim.node_drain_claim,
            "runtime_sealed": True,
            "safe_to_destroy": True,
            "blockers": [],
            "blocker_count": 0,
            "task_count": 0,
        }
        proof["signature"] = worker_node_drain_proof_signature(
            proof,
            auth_token=worker.auth_token,
        )
        worker.destroy_termination_receipt = (
            build_worker_destroy_termination_receipt(
                claim,
                proof,
                cloud_scope=TEST_CLOUD_SCOPE,
                provision_spec_digest=worker_destroy_provision_spec_digest(
                    worker.provision_spec
                ),
                client_token_digest=worker_create_client_token_digest(
                    worker.id,
                    worker.auth_token,
                ),
            )
        )
        await db.commit()
        return claim


async def test_login_codex_account_posts_credentials_and_polls_to_success(
    db_factory, monkeypatch,
):
    provisioner = WorkerProvisioner(db_factory, cloud=object())
    worker = Worker(
        name="remote-worker",
        private_ip="10.0.0.9",
        ssh_user="ubuntu",
        auth_token="worker-token",
        ccm_port=8000,
    )
    local_api = AsyncMock(side_effect=[
        {"status": "running", "account_id": "codex-7"},
        {"status": "finalizing"},
        {"status": "success"},
    ])
    provisioner.worker_local_api = local_api
    sleep = AsyncMock()
    monkeypatch.setattr(worker_provisioner_module.asyncio, "sleep", sleep)

    account_id = await provisioner.login_codex_account(
        worker,
        {
            "email": "codex+worker@example.com",
            "token": "  mailbox-token  ",
            "password": "  exact OpenAI password  ",
            "login_method": "mailcatcher",
        },
    )

    assert account_id == "codex-7"
    assert local_api.await_args_list == [
        call(
            worker,
            "POST",
            "/api/codex-pool/add",
            payload={
                "email": "codex+worker@example.com",
                "token": "mailbox-token",
                "password": "  exact OpenAI password  ",
                "login_method": "mailcatcher",
            },
            timeout=45,
        ),
        call(
            worker,
            "GET",
            "/api/codex-pool/add/codex%2Bworker%40example.com",
            timeout=30,
        ),
        call(
            worker,
            "GET",
            "/api/codex-pool/add/codex%2Bworker%40example.com",
            timeout=30,
        ),
    ]
    assert sleep.await_count == 2


async def test_ensure_codex_account_reuses_existing_slot_without_add(db_factory):
    provisioner = WorkerProvisioner(db_factory, cloud=object())
    worker = Worker(
        name="remote-worker",
        private_ip="10.0.0.9",
        ssh_user="ubuntu",
        auth_token="worker-token",
        ccm_port=8000,
    )
    provisioner.worker_local_api = AsyncMock(side_effect=[
        {
            "accounts": [{
                "id": "codex-1",
                "email": "codex@example.com",
                "enabled": True,
            }],
        },
        {"logged_in": True, "email": "codex@example.com"},
    ])
    provisioner.login_codex_account = AsyncMock()

    account_id = await provisioner.ensure_codex_account(worker, {
        "account_id": "codex-1",
        "email": "codex@example.com",
        "token": "mail-token",
        "password": "password",
    })

    assert account_id == "codex-1"
    provisioner.login_codex_account.assert_not_awaited()
    assert provisioner.worker_local_api.await_args_list == [
        call(worker, "GET", "/api/codex-pool/status", timeout=30),
        call(
            worker,
            "GET",
            "/api/codex-pool/accounts/codex-1/verify?live=true",
            timeout=30,
        ),
    ]


async def test_ensure_codex_account_relogs_existing_broken_slot(
    db_factory, monkeypatch,
):
    provisioner = WorkerProvisioner(db_factory, cloud=object())
    worker = Worker(
        name="remote-worker",
        private_ip="10.0.0.9",
        ssh_user="ubuntu",
        auth_token="worker-token",
        ccm_port=8000,
    )
    provisioner.worker_local_api = AsyncMock(side_effect=[
        {"accounts": [{"id": "codex-1", "email": "codex@example.com"}]},
        {"logged_in": False, "detail": "auth.json missing"},
        {"status": "running", "attempt_id": "retry-attempt"},
        {"status": "success"},
    ])
    sleep = AsyncMock()
    monkeypatch.setattr(worker_provisioner_module.asyncio, "sleep", sleep)

    account_id = await provisioner.ensure_codex_account(worker, {
        "account_id": "codex-1",
        "email": "codex@example.com",
        "token": "mail-token",
        "password": "password",
    })

    assert account_id == "codex-1"
    assert provisioner.worker_local_api.await_args_list[-2:] == [
        call(
            worker,
            "POST",
            "/api/codex-pool/accounts/codex-1/relogin",
            timeout=45,
        ),
        call(
            worker,
            "GET",
            "/api/codex-pool/accounts/codex-1/relogin",
            timeout=30,
        ),
    ]


@pytest.mark.parametrize(
    ("response", "message"),
    [
        (
            {
                "status": "awaiting_otp",
                "attempt_id": "attempt-1",
                "challenge_id": "challenge-1",
            },
            "人工输入邮箱验证码",
        ),
        ({"status": "failed", "detail": "OAuth callback failed"}, "OAuth callback failed"),
    ],
)
async def test_login_codex_account_surfaces_otp_and_terminal_failure(
    db_factory, response, message,
):
    provisioner = WorkerProvisioner(db_factory, cloud=object())
    worker = Worker(
        name="remote-worker",
        private_ip="10.0.0.9",
        ssh_user="ubuntu",
        auth_token="worker-token",
        ccm_port=8000,
    )
    provisioner.worker_local_api = AsyncMock(return_value=response)

    with pytest.raises(RuntimeError, match=message):
        await provisioner.login_codex_account(
            worker,
            {
                "email": "codex@example.com",
                "token": "mailbox-token",
                "password": "",
                "login_method": "171mail",
            },
        )

    if response["status"] == "awaiting_otp":
        assert provisioner.worker_local_api.await_count == 2
        assert provisioner.worker_local_api.await_args_list[-1] == call(
            worker,
            "DELETE",
            "/api/codex-pool/login-attempts/attempt-1",
            timeout=45,
        )
    else:
        provisioner.worker_local_api.assert_awaited_once()


async def test_step_account_login_keeps_codex_and_historical_claude_slots_independent(
    db_factory, session_factory,
):
    worker = await _insert_worker(
        session_factory,
        status="creating",
        accounts=[],
        cloud_instance_id="i-provider-slots",
    )
    async with session_factory() as db:
        current = await db.get(Worker, worker.id)
        current.provision_spec = {
            "version": 1,
            "name": current.name,
            "has_fixed_overrides": False,
            "overrides": {},
            "cloud_scope": TEST_CLOUD_SCOPE,
            "client_token_digest": worker_create_client_token_digest(
                current.id,
                current.auth_token,
            ),
        }
        await db.commit()
        await db.refresh(current)
        claude_login_identity = worker_claude_login_identity(current)
    provisioner = WorkerProvisioner(db_factory, cloud=object())
    provisioner._log = AsyncMock()
    provisioner.ensure_codex_account = AsyncMock(return_value="codex-4")
    ssh = AsyncMock()
    ssh.run.side_effect = [(0, "uploaded"), (0, "claude login ok")]
    codex_password = "  opaque password  "
    codex_token = "codex-mailbox-token"

    await provisioner._step_account_login(
        ssh,
        worker.id,
        [
            {
                "email": "codex@example.com",
                "provider": "codex",
                "token": codex_token,
                "password": codex_password,
                "login_method": "mailcatcher",
            },
            {
                # Missing provider is a historical Claude-only record. Codex
                # must not consume or renumber its independent Claude slot.
                "email": "legacy-claude@example.com",
                "token": "claude-mail-token",
                "login_method": "onet",
            },
        ],
    )

    codex_call = provisioner.ensure_codex_account.await_args
    assert codex_call.args[0].id == worker.id
    assert codex_call.args[1] == {
        "email": "codex@example.com",
        "provider": "codex",
        "token": codex_token,
        "password": codex_password,
        "login_method": "mailcatcher",
    }

    assert ssh.run.await_count == 2
    upload_command = ssh.run.await_args_list[0].args[0]
    upload_argv = shlex.split(upload_command)
    encoded_script = upload_argv[upload_argv.index("%s") + 1]
    login_script = base64.b64decode(encoded_script).decode()
    login_argv = shlex.split(login_script[login_script.index("uv run "):])
    assert login_argv[login_argv.index("--add-to-pool") + 1] == "default"

    async with session_factory() as db:
        persisted = await db.get(Worker, worker.id)
    assert persisted.accounts == [
        {
            "email": "codex@example.com",
            "token": codex_token,
            "password": codex_password,
            "provider": "codex",
            "login_method": "mailcatcher",
            "status": "logged_in",
            "account_id": "codex-4",
        },
        {
            "email": "legacy-claude@example.com",
            "token": "claude-mail-token",
            "password": "",
            "provider": "claude",
            "login_method": "onet",
            "status": "logged_in",
            "account_id": "default",
            CLAUDE_LOGIN_IDENTITY_KEY: claude_login_identity,
        },
    ]


async def test_worker_local_api_sends_bearer_and_payload_only_through_ssh_stdin(
    db_factory,
):
    provisioner = WorkerProvisioner(db_factory, cloud=object())
    worker = Worker(
        name="remote-worker",
        private_ip="10.0.0.9",
        ssh_user="ubuntu",
        ssh_key_path="/tmp/worker-key",
        auth_token="worker-auth-token",
        ccm_port=8123,
    )
    ssh = AsyncMock()
    ssh.run_with_input.return_value = (0, '{"ok": true, "status": "running"}')
    provisioner._ssh = lambda _worker: ssh
    payload = {
        "email": "stdin-only@example.com",
        "token": "stdin-only-mailbox-token",
        "password": "stdin-only-openai-password",
        "login_method": "171mail",
    }

    result = await provisioner.worker_local_api(
        worker,
        "POST",
        "/api/codex-pool/add",
        payload=payload,
        timeout=41,
    )

    assert result == {"ok": True, "status": "running"}
    ssh.run.assert_not_awaited()
    ssh.run_with_input.assert_awaited_once()
    command, input_data = ssh.run_with_input.await_args.args
    envelope = json.loads(input_data)
    assert envelope == {
        "url": "http://127.0.0.1:8123/api/codex-pool/add",
        "method": "POST",
        "timeout": 41,
        "auth_token": "worker-auth-token",
        "has_payload": True,
        "payload": payload,
    }
    assert all(str(value) not in command for value in payload.values())
    assert "worker-auth-token" not in command
    assert "/api/codex-pool/add" not in command
    assert command.startswith("python3 -c ")
    assert "ProxyHandler({})" in command
    assert ssh.run_with_input.await_args.kwargs == {
        "timeout": 46,
        "sensitive": True,
    }


class _JSONResponse:
    def __init__(self, body: dict, status_code: int = 200):
        self._body = body
        self.status_code = status_code
        self.text = json.dumps(body)

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise httpx.HTTPStatusError(
                "remote failure",
                request=httpx.Request("GET", "http://worker"),
                response=httpx.Response(self.status_code),
            )


async def test_worker_codex_pool_status_usage_and_delete_use_codex_paths(
    client, session_factory, monkeypatch,
):
    worker = await _insert_worker(session_factory, accounts=[{
        "email": "deleted@example.com",
        "provider": "codex",
        "token": "mail-token-that-must-be-erased",
        "password": "password-that-must-be-erased",
        "login_method": "mailcatcher",
        "status": "logged_in",
        "account_id": "codex+1",
    }])
    requests: list[tuple[str, str, dict]] = []

    class FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            self.timeout = kwargs.get("timeout")

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def get(self, url, **kwargs):
            requests.append(("GET", url, kwargs))
            kind = "usage" if "/usage" in url else "status"
            return _JSONResponse({"kind": kind, "accounts": []})

        async def delete(self, url, **kwargs):
            requests.append(("DELETE", url, kwargs))
            return _JSONResponse({"ok": True})

    monkeypatch.setattr(httpx, "AsyncClient", FakeAsyncClient)

    status = await client.get(f"/api/workers/{worker.id}/pool?provider=codex")
    usage = await client.get(f"/api/workers/{worker.id}/pool/usage?provider=codex")
    deleted = await client.delete(
        f"/api/workers/{worker.id}/pool/codex%2B1?provider=codex"
    )

    assert status.status_code == 200
    assert status.json()["kind"] == "status"
    assert usage.status_code == 200
    assert usage.json()["kind"] == "usage"
    assert deleted.status_code == 200
    assert deleted.json() == {"ok": True}
    assert [(method, url) for method, url, _ in requests] == [
        ("GET", "http://10.0.0.9:8000/api/codex-pool/status"),
        ("GET", "http://10.0.0.9:8000/api/codex-pool/usage?force=true"),
        ("DELETE", "http://10.0.0.9:8000/api/codex-pool/accounts/codex%2B1"),
    ]
    for _method, _url, kwargs in requests:
        assert kwargs["headers"] == {
            "Authorization": "Bearer worker-auth-token"
        }

    async with session_factory() as db:
        persisted = await db.get(Worker, worker.id)
        assert persisted.accounts == []
        persisted.status = "error"
        await db.commit()

    # A later bootstrap retry must not resurrect the remotely deleted account
    # or retain its token/password in Manager DB.
    provisioner = AsyncMock()
    monkeypatch.setattr(main_module, "worker_provisioner", provisioner)
    retried = await client.post(f"/api/workers/{worker.id}/retry")
    assert retried.status_code == 200
    await worker_provisioner_module.asyncio.sleep(0)
    provisioner.create_worker.assert_awaited_once_with(worker.id, accounts=[])


async def test_dynamic_codex_add_coalesces_case_insensitive_active_email(
    client, session_factory, monkeypatch,
):
    worker = await _insert_worker(session_factory, accounts=[])
    provisioner = WorkerProvisioner(session_factory, cloud=object())
    login_entered = asyncio.Event()
    release_login = asyncio.Event()

    async def blocked_login(_worker, _account, **_kwargs):
        login_entered.set()
        await release_login.wait()
        return "codex-1"

    provisioner.ensure_codex_account = AsyncMock(side_effect=blocked_login)
    monkeypatch.setattr(main_module, "worker_provisioner", provisioner)
    state_key = f"{worker.id}:codex:same.email@example.com"
    workers_api._worker_login_state.pop(state_key, None)

    first = await client.post(
        f"/api/workers/{worker.id}/pool/add",
        json={
            "email": "Same.Email@Example.com",
            "provider": "codex",
            "token": "first-mail-token",
        },
    )
    await asyncio.wait_for(login_entered.wait(), timeout=1)
    second = await client.post(
        f"/api/workers/{worker.id}/pool/add",
        json={
            "email": "same.email@example.com",
            "provider": "codex",
            "token": "must-not-overwrite-first-token",
        },
    )

    try:
        assert first.status_code == 200, first.text
        assert second.status_code == 200, second.text
        assert second.json()["status"] == "running"
        provisioner.ensure_codex_account.assert_awaited_once()
    finally:
        release_login.set()
        await _drain_worker_background_tasks()

    async with session_factory() as db:
        persisted = await db.get(Worker, worker.id)
    assert persisted.accounts == [{
        "email": "Same.Email@Example.com",
        "provider": "codex",
        "token": "first-mail-token",
        "password": "",
        "login_method": "",
        "account_id": "codex-1",
        "status": "logged_in",
    }]
    workers_api._worker_login_state.pop(state_key, None)


@pytest.mark.parametrize("login_status", ["running", "cancelling"])
async def test_worker_account_delete_rejects_active_codex_login(
    client, session_factory, monkeypatch, login_status,
):
    account = {
        "email": "active-delete@example.com",
        "provider": "codex",
        "token": "mail-token",
        "password": "",
        "login_method": "",
        "account_id": "codex-8",
        "status": "pending",
    }
    worker = await _insert_worker(session_factory, accounts=[account])
    state_key = f"{worker.id}:codex:active-delete@example.com"
    workers_api._worker_login_state[state_key] = {
        "status": login_status,
        "provider": "codex",
        "attempt_id": "active-delete-attempt",
    }
    remote_delete = AsyncMock()
    monkeypatch.setattr(workers_api, "_worker_http_request", remote_delete)

    try:
        response = await client.delete(
            f"/api/workers/{worker.id}/pool/codex-8?provider=codex"
        )
    finally:
        workers_api._worker_login_state.pop(state_key, None)

    assert response.status_code == 409
    assert "登录仍在进行中" in response.json()["detail"]
    remote_delete.assert_not_awaited()
    async with session_factory() as db:
        assert (await db.get(Worker, worker.id)).accounts == [account]


async def test_remote_terminal_login_stays_active_until_failure_is_persisted(
    client, session_factory, monkeypatch,
):
    worker = await _insert_worker(session_factory, accounts=[])
    provisioner = WorkerProvisioner(session_factory, cloud=object())
    remote_terminal_published = asyncio.Event()
    release_failure = asyncio.Event()

    async def terminal_then_block(_worker, _account, **kwargs):
        await kwargs["on_status"]({
            "status": "failed",
            "attempt_id": "terminal-race-attempt",
            "detail": "remote browser failed",
        })
        remote_terminal_published.set()
        await release_failure.wait()
        raise RuntimeError("remote browser failed")

    provisioner.ensure_codex_account = AsyncMock(side_effect=terminal_then_block)
    remote_delete = AsyncMock()
    monkeypatch.setattr(main_module, "worker_provisioner", provisioner)
    monkeypatch.setattr(workers_api, "_worker_http_request", remote_delete)
    state_key = f"{worker.id}:codex:terminal-race@example.com"
    workers_api._worker_login_state.pop(state_key, None)

    added = await client.post(
        f"/api/workers/{worker.id}/pool/add",
        json={
            "email": "terminal-race@example.com",
            "provider": "codex",
            "token": "terminal-race-mail-token",
        },
    )
    await asyncio.wait_for(remote_terminal_published.wait(), timeout=1)
    blocked_delete = await client.delete(
        f"/api/workers/{worker.id}/pool/codex-1?provider=codex"
    )

    try:
        assert added.status_code == 200, added.text
        assert workers_api._worker_login_state[state_key]["status"] == "finalizing"
        assert blocked_delete.status_code == 409
        remote_delete.assert_not_awaited()
    finally:
        release_failure.set()
        await _drain_worker_background_tasks()

    assert workers_api._worker_login_state[state_key]["status"] == "failed"
    async with session_factory() as db:
        persisted = await db.get(Worker, worker.id)
    assert persisted.accounts == [{
        "email": "terminal-race@example.com",
        "provider": "codex",
        "token": "terminal-race-mail-token",
        "password": "",
        "login_method": "",
        "status": "failed",
    }]
    workers_api._worker_login_state.pop(state_key, None)


async def test_cancelled_dynamic_codex_login_blocks_immediate_second_ensure(
    client, session_factory, monkeypatch,
):
    worker = await _insert_worker(session_factory, accounts=[])
    provisioner = WorkerProvisioner(session_factory, cloud=object())
    login_entered = asyncio.Event()
    release_login = asyncio.Event()

    async def awaiting_cancel(_worker, _account, **kwargs):
        await kwargs["on_status"]({
            "status": "awaiting_otp",
            "attempt_id": "cancel-attempt",
            "challenge_id": "cancel-challenge",
        })
        login_entered.set()
        await release_login.wait()
        raise RuntimeError("remote login cancelled")

    provisioner.ensure_codex_account = AsyncMock(side_effect=awaiting_cancel)
    provisioner.worker_local_api = AsyncMock(return_value={
        "ok": True,
        "status": "cancelled",
    })
    monkeypatch.setattr(main_module, "worker_provisioner", provisioner)
    state_key = f"{worker.id}:codex:cancel-retry@example.com"
    workers_api._worker_login_state.pop(state_key, None)

    first = await client.post(
        f"/api/workers/{worker.id}/pool/add",
        json={
            "email": "cancel-retry@example.com",
            "provider": "codex",
            "token": "first-mail-token",
        },
    )
    await asyncio.wait_for(login_entered.wait(), timeout=1)
    cancelled = await client.delete(
        f"/api/workers/{worker.id}/pool/login-attempts/cancel-attempt"
    )
    immediate_retry = await client.post(
        f"/api/workers/{worker.id}/pool/add",
        json={
            "email": "CANCEL-RETRY@example.com",
            "provider": "codex",
            "token": "must-not-start-a-second-login",
        },
    )

    try:
        assert first.status_code == 200, first.text
        assert cancelled.status_code == 200, cancelled.text
        assert immediate_retry.status_code == 200, immediate_retry.text
        assert immediate_retry.json()["status"] == "cancelling"
        provisioner.ensure_codex_account.assert_awaited_once()
    finally:
        release_login.set()
        await _drain_worker_background_tasks()

    assert workers_api._worker_login_state[state_key]["status"] == "failed"
    async with session_factory() as db:
        persisted = await db.get(Worker, worker.id)
    assert persisted.accounts == [{
        "email": "cancel-retry@example.com",
        "provider": "codex",
        "token": "first-mail-token",
        "password": "",
        "login_method": "",
        "status": "failed",
    }]
    workers_api._worker_login_state.pop(state_key, None)


async def test_manager_restart_recovers_worker_otp_route_from_remote_state(
    client,
    session_factory,
    monkeypatch,
):
    email = "restart-otp@example.com"
    attempt_id = "restart-attempt"
    challenge_id = "restart-challenge"
    worker = await _insert_worker(
        session_factory,
        accounts=[{
            "email": email,
            "provider": "codex",
            "token": "saved-mail-token",
            "password": "",
            "login_method": "",
            "status": "pending",
        }],
    )
    provisioner = WorkerProvisioner(session_factory, cloud=object())
    remote_state = {
        "status": "awaiting_otp",
        "attempt_id": attempt_id,
        "challenge_id": challenge_id,
        "expires_at": 9_999_999_999,
        "account_id": "codex-1",
    }
    provisioner.worker_local_api = AsyncMock(
        side_effect=[remote_state, remote_state, {
            "ok": True,
            "status": "verifying_otp",
        }]
    )
    monkeypatch.setattr(main_module, "worker_provisioner", provisioner)
    state_key = f"{worker.id}:codex:{email}"
    workers_api._worker_login_state.pop(state_key, None)

    status = await client.get(
        f"/api/workers/{worker.id}/pool/add/{email}?provider=codex"
    )
    assert status.status_code == 200, status.text
    assert status.json()["challenge_id"] == challenge_id

    # Model another Manager process/restart between status polling and submit.
    workers_api._worker_login_state.pop(state_key, None)
    submitted = await client.post(
        f"/api/workers/{worker.id}/pool/login-attempts/{attempt_id}/otp",
        json={"challenge_id": challenge_id, "code": "123456"},
    )
    assert submitted.status_code == 200, submitted.text
    assert submitted.json()["status"] == "verifying_otp"
    assert provisioner.worker_local_api.await_args_list[-1].args[1:] == (
        "POST",
        f"/api/codex-pool/login-attempts/{attempt_id}/otp",
    )
    assert provisioner.worker_local_api.await_args_list[-1].kwargs == {
        "payload": {"challenge_id": challenge_id, "code": "123456"},
        "timeout": 30,
    }
    workers_api._worker_login_state.pop(state_key, None)


@pytest.mark.parametrize(
    ("remote_status", "expected_status"),
    (("success", "logged_in"), ("failed", "failed")),
)
async def test_manager_restart_durably_settles_remote_terminal_codex_login(
    client,
    session_factory,
    monkeypatch,
    remote_status,
    expected_status,
):
    email = f"restart-{remote_status}@example.com"
    worker = await _insert_worker(
        session_factory,
        accounts=[{
            "email": email,
            "provider": "codex",
            "token": "saved-mail-token",
            "password": "saved-openai-password",
            "login_method": "mailcatcher",
            "status": "pending",
        }],
    )
    terminal = {
        "status": remote_status,
        "attempt_id": f"attempt-{remote_status}",
        "account_id": "codex-9",
        **({"detail": "browser failed"} if remote_status == "failed" else {}),
    }
    provisioner = WorkerProvisioner(session_factory, cloud=object())
    responses = [terminal]
    if remote_status == "success":
        responses.append({
            "accounts": [{"id": "codex-9", "email": email}],
        })
    provisioner.worker_local_api = AsyncMock(side_effect=responses)
    monkeypatch.setattr(main_module, "worker_provisioner", provisioner)
    state_key = f"{worker.id}:codex:{email}"
    workers_api._worker_login_state.pop(state_key, None)

    status = await client.get(
        f"/api/workers/{worker.id}/pool/add/{email}?provider=codex"
    )
    assert status.status_code == 200, status.text
    assert status.json()["status"] == remote_status
    async with session_factory() as db:
        persisted = await db.get(Worker, worker.id)
    assert persisted.accounts == [{
        "email": email,
        "provider": "codex",
        "token": "saved-mail-token",
        "password": "saved-openai-password",
        "login_method": "mailcatcher",
        "status": expected_status,
        "account_id": "codex-9",
    }]
    workers_api._worker_login_state.pop(state_key, None)
    remote_call_count = provisioner.worker_local_api.await_count
    durable = await client.get(
        f"/api/workers/{worker.id}/pool/add/{email}?provider=codex"
    )
    assert durable.status_code == 200, durable.text
    assert durable.json()["status"] == remote_status
    assert provisioner.worker_local_api.await_count == remote_call_count


async def test_destroyed_worker_rejects_late_dynamic_codex_persistence(
    client, session_factory, monkeypatch,
):
    worker = await _insert_worker(
        session_factory,
        accounts=[],
        cloud_instance_id="i-active-login",
    )
    cloud = AsyncMock()
    cloud.termination_scope.return_value = dict(TEST_CLOUD_SCOPE)
    provisioner = WorkerProvisioner(session_factory, cloud=cloud)
    login_entered = asyncio.Event()
    release_login = asyncio.Event()

    async def late_remote_success(_worker, _account, **_kwargs):
        login_entered.set()
        await release_login.wait()
        return "codex-late"

    provisioner.ensure_codex_account = AsyncMock(side_effect=late_remote_success)
    monkeypatch.setattr(main_module, "worker_provisioner", provisioner)
    state_key = f"{worker.id}:codex:destroy-race@example.com"
    workers_api._worker_login_state.pop(state_key, None)

    response = await client.post(
        f"/api/workers/{worker.id}/pool/add",
        json={
            "email": "destroy-race@example.com",
            "provider": "codex",
            "token": "mail-token-must-not-return",
            "password": "openai-password-must-not-return",
        },
    )
    await asyncio.wait_for(login_entered.wait(), timeout=1)
    destroy_claim = await _authorize_direct_destroy(
        session_factory,
        worker.id,
    )
    await provisioner.destroy_worker(
        worker.id,
        destroy_claim=destroy_claim,
    )

    async with session_factory() as db:
        after_destroy = await db.get(Worker, worker.id)
    try:
        assert response.status_code == 200, response.text
        assert after_destroy.status == "terminated"
        assert after_destroy.auth_token is None
        assert after_destroy.accounts == [{
            "email": "destroy-race@example.com",
            "provider": "codex",
            "status": "pending",
        }]
    finally:
        release_login.set()
        await _drain_worker_background_tasks()

    async with session_factory() as db:
        after_late_callback = await db.get(Worker, worker.id)
    assert after_late_callback.status == "terminated"
    assert after_late_callback.auth_token is None
    assert after_late_callback.accounts == after_destroy.accounts
    serialized_accounts = json.dumps(after_late_callback.accounts)
    assert "mail-token-must-not-return" not in serialized_accounts
    assert "openai-password-must-not-return" not in serialized_accounts
    assert workers_api._worker_login_state[state_key]["status"] == "failed"
    assert "persistence rejected while terminated" in (
        workers_api._worker_login_state[state_key]["detail"]
    )
    workers_api._worker_login_state.pop(state_key, None)


async def test_destroy_winning_after_account_snapshot_rejects_stale_secret_write(
    session_factory, monkeypatch,
):
    worker = await _insert_worker(
        session_factory,
        accounts=[],
        cloud_instance_id="i-account-write-race",
    )
    cloud = AsyncMock()
    cloud.termination_scope.return_value = dict(TEST_CLOUD_SCOPE)
    provisioner = WorkerProvisioner(session_factory, cloud=cloud)
    snapshot_released = asyncio.Event()
    release_stale_write = asyncio.Event()
    original_rollback = AsyncSession.rollback

    async def rollback_with_barrier(session):
        await original_rollback(session)
        task = asyncio.current_task()
        if task is not None and task.get_name() == "stale-account-persist":
            snapshot_released.set()
            await release_stale_write.wait()

    monkeypatch.setattr(AsyncSession, "rollback", rollback_with_barrier)
    account = {
        "email": "destroy-write-race@example.com",
        "provider": "codex",
        "token": "mail-token-must-stay-erased",
        "password": "password-must-stay-erased",
        "login_method": "",
    }
    persist_task = asyncio.create_task(
        workers_api._persist_worker_account_state(
            provisioner,
            worker.id,
            account,
            status="failed",
        ),
        name="stale-account-persist",
    )
    await asyncio.wait_for(snapshot_released.wait(), timeout=1)
    destroy_claim = await _authorize_direct_destroy(
        session_factory,
        worker.id,
    )
    await provisioner.destroy_worker(
        worker.id,
        destroy_claim=destroy_claim,
    )
    release_stale_write.set()

    with pytest.raises(RuntimeError, match="rejected while terminated"):
        await persist_task

    async with session_factory() as db:
        persisted = await db.get(Worker, worker.id)
    assert persisted.status == "terminated"
    assert persisted.accounts == []
    serialized = json.dumps(persisted.accounts)
    assert "mail-token-must-stay-erased" not in serialized
    assert "password-must-stay-erased" not in serialized


async def test_delete_tombstone_winning_after_account_snapshot_rejects_stale_secret_write(
    session_factory, monkeypatch,
):
    account = {
        "email": "delete-persist-race@example.com",
        "provider": "codex",
        "token": "mail-token-must-stay-erased",
        "password": "password-must-stay-erased",
        "login_method": "",
        "account_id": "codex-delete-race",
        "status": "logged_in",
    }
    worker = await _insert_worker(
        session_factory,
        accounts=[account],
        cloud_instance_id="i-delete-persist-race",
    )
    provisioner = WorkerProvisioner(session_factory, cloud=object())
    snapshot_released = asyncio.Event()
    release_stale_write = asyncio.Event()
    original_rollback = AsyncSession.rollback

    async def rollback_with_barrier(session):
        await original_rollback(session)
        task = asyncio.current_task()
        if task is not None and task.get_name() == "stale-delete-persist":
            snapshot_released.set()
            await release_stale_write.wait()

    monkeypatch.setattr(AsyncSession, "rollback", rollback_with_barrier)
    stale_account = {
        **account,
        "token": "late-mail-token-must-not-return",
        "password": "late-password-must-not-return",
    }
    persist_task = asyncio.create_task(
        workers_api._persist_worker_account_state(
            provisioner,
            worker.id,
            stale_account,
            status="logged_in",
            account_id="codex-delete-race",
        ),
        name="stale-delete-persist",
    )
    await asyncio.wait_for(snapshot_released.wait(), timeout=1)

    delete_request = SimpleNamespace(
        state=SimpleNamespace(
            auth_type="token",
            user_id=None,
            user_role="super_admin",
        )
    )
    async with session_factory() as db:
        current = await db.get(Worker, worker.id)
        current.accounts, receipt = (
            workers_api._prepare_persisted_worker_account_delete(
                current.accounts,
                current,
                delete_request,
                provider="codex",
                account_id="codex-delete-race",
            )
        )
        await db.commit()
    release_stale_write.set()

    with pytest.raises(RuntimeError, match="deletion is awaiting"):
        await persist_task

    async with session_factory() as db:
        persisted = await db.get(Worker, worker.id)
    assert persisted.accounts == [{
        "email": account["email"],
        "provider": "codex",
        "account_id": "codex-delete-race",
        "status": "deleting",
        workers_api._WORKER_ACCOUNT_DELETE_RECEIPT_KEY: receipt,
    }]
    serialized = json.dumps(persisted.accounts)
    assert account["token"] not in serialized
    assert account["password"] not in serialized
    assert stale_account["token"] not in serialized
    assert stale_account["password"] not in serialized


async def test_destroy_winning_after_delete_snapshot_rejects_stale_account_write(
    session_factory, monkeypatch,
):
    accounts = [
        {
            "email": "delete-me@example.com",
            "provider": "codex",
            "token": "deleted-mail-token",
            "password": "deleted-password",
            "login_method": "",
            "account_id": "codex-1",
            "status": "logged_in",
        },
        {
            "email": "survivor@example.com",
            "provider": "codex",
            "token": "survivor-mail-token-must-stay-erased",
            "password": "survivor-password-must-stay-erased",
            "login_method": "",
            "account_id": "codex-2",
            "status": "logged_in",
        },
    ]
    worker = await _insert_worker(
        session_factory,
        accounts=accounts,
        cloud_instance_id="i-delete-write-race",
    )
    cloud = AsyncMock()
    cloud.termination_scope.return_value = dict(TEST_CLOUD_SCOPE)
    provisioner = WorkerProvisioner(session_factory, cloud=cloud)
    snapshot_released = asyncio.Event()
    release_stale_delete = asyncio.Event()
    original_rollback = AsyncSession.rollback
    barrier_used = False

    async def rollback_with_barrier(session):
        nonlocal barrier_used
        await original_rollback(session)
        task = asyncio.current_task()
        if (
            not barrier_used
            and task is not None
            and task.get_name() == "stale-account-delete"
        ):
            barrier_used = True
            snapshot_released.set()
            await release_stale_delete.wait()

    monkeypatch.setattr(AsyncSession, "rollback", rollback_with_barrier)
    remote_delete = AsyncMock()
    monkeypatch.setattr(workers_api, "_worker_http_request", remote_delete)
    request = SimpleNamespace(
        state=SimpleNamespace(user_id=None, user_role="super_admin")
    )
    async with session_factory() as route_db:
        delete_task = asyncio.create_task(
            workers_api.delete_worker_account(
                worker.id,
                request,
                "codex-1",
                provider="codex",
                db=route_db,
            ),
            name="stale-account-delete",
        )
        await asyncio.wait_for(snapshot_released.wait(), timeout=1)
        try:
            destroy_claim = await _authorize_direct_destroy(
                session_factory,
                worker.id,
            )
            await provisioner.destroy_worker(
                worker.id,
                destroy_claim=destroy_claim,
            )
        finally:
            release_stale_delete.set()
        with pytest.raises(HTTPException) as rejected:
            await delete_task

    assert rejected.value.status_code == 409
    remote_delete.assert_not_awaited()
    async with session_factory() as db:
        persisted = await db.get(Worker, worker.id)
    assert persisted.status == "terminated"
    serialized = json.dumps(persisted.accounts)
    assert "survivor-mail-token-must-stay-erased" not in serialized
    assert "survivor-password-must-stay-erased" not in serialized
    assert persisted.accounts == [
        {
            "email": "delete-me@example.com",
            "provider": "codex",
            "status": "logged_in",
            "account_id": "codex-1",
        },
        {
            "email": "survivor@example.com",
            "provider": "codex",
            "status": "logged_in",
            "account_id": "codex-2",
        },
    ]


async def test_delete_holds_admission_until_remote_slot_is_gone(
    client, session_factory, monkeypatch,
):
    worker = await _insert_worker(
        session_factory,
        accounts=[{
            "email": "replace-after-delete@example.com",
            "provider": "codex",
            "token": "old-mail-token",
            "password": "old-password",
            "login_method": "",
            "account_id": "codex-1",
            "status": "logged_in",
        }],
    )
    provisioner = WorkerProvisioner(session_factory, cloud=object())
    provisioner.ensure_codex_account = AsyncMock(return_value="codex-1")
    monkeypatch.setattr(main_module, "worker_provisioner", provisioner)
    remote_delete_entered = asyncio.Event()
    release_remote_delete = asyncio.Event()

    async def blocked_remote_delete(*_args, **_kwargs):
        remote_delete_entered.set()
        await release_remote_delete.wait()
        return _JSONResponse({"ok": True})

    remote_delete = AsyncMock(side_effect=blocked_remote_delete)
    monkeypatch.setattr(workers_api, "_worker_http_request", remote_delete)
    state_key = f"{worker.id}:codex:replace-after-delete@example.com"
    workers_api._worker_login_state.pop(state_key, None)

    delete_task = asyncio.create_task(client.delete(
        f"/api/workers/{worker.id}/pool/codex-1?provider=codex"
    ))
    await asyncio.wait_for(remote_delete_entered.wait(), timeout=1)
    add_task = asyncio.create_task(client.post(
        f"/api/workers/{worker.id}/pool/add",
        json={
            "email": "replace-after-delete@example.com",
            "provider": "codex",
            "token": "new-mail-token",
            "password": "new-password",
        },
    ))
    await asyncio.sleep(0)

    try:
        assert not add_task.done()
        provisioner.ensure_codex_account.assert_not_awaited()
    finally:
        release_remote_delete.set()

    deleted, added = await asyncio.gather(delete_task, add_task)
    await _drain_worker_background_tasks()
    assert deleted.status_code == 200, deleted.text
    assert added.status_code == 200, added.text
    provisioner.ensure_codex_account.assert_awaited_once()
    async with session_factory() as db:
        persisted = await db.get(Worker, worker.id)
    assert persisted.accounts == [{
        "email": "replace-after-delete@example.com",
        "provider": "codex",
        "token": "new-mail-token",
        "password": "new-password",
        "login_method": "",
        "account_id": "codex-1",
        "status": "logged_in",
    }]
    workers_api._worker_login_state.pop(state_key, None)


async def test_dynamic_codex_add_resumes_persisted_pending_credentials(
    client, session_factory, monkeypatch,
):
    persisted_account = {
        "email": "pending@example.com",
        "provider": "codex",
        "token": "known-good-mail-token",
        "password": "known-good-openai-password",
        "login_method": "mailcatcher",
        "account_id": "codex-4",
        "status": "pending",
    }
    worker = await _insert_worker(
        session_factory,
        accounts=[persisted_account],
    )
    provisioner = WorkerProvisioner(session_factory, cloud=object())
    provisioner.ensure_codex_account = AsyncMock(return_value="codex-4")
    monkeypatch.setattr(main_module, "worker_provisioner", provisioner)
    state_key = f"{worker.id}:codex:pending@example.com"
    workers_api._worker_login_state.pop(state_key, None)

    response = await client.post(
        f"/api/workers/{worker.id}/pool/add",
        json={
            "email": "PENDING@example.com",
            "provider": "codex",
            "token": "replacement-form-token",
            "password": "replacement-form-password",
        },
    )
    await _drain_worker_background_tasks()

    assert response.status_code == 200, response.text
    submitted_account = provisioner.ensure_codex_account.await_args.args[1]
    assert submitted_account["token"] == "known-good-mail-token"
    assert submitted_account["password"] == "known-good-openai-password"
    assert submitted_account["account_id"] == "codex-4"
    async with session_factory() as db:
        persisted = await db.get(Worker, worker.id)
    assert persisted.accounts == [{
        **persisted_account,
        "status": "logged_in",
    }]
    workers_api._worker_login_state.pop(state_key, None)


async def test_dynamic_codex_add_rejects_already_logged_in_email(
    client, session_factory, monkeypatch,
):
    account = {
        "email": "existing@example.com",
        "provider": "codex",
        "token": "existing-mail-token",
        "password": "existing-openai-password",
        "login_method": "mailcatcher",
        "account_id": "codex-2",
        "status": "logged_in",
    }
    worker = await _insert_worker(session_factory, accounts=[account])
    provisioner = WorkerProvisioner(session_factory, cloud=object())
    provisioner.ensure_codex_account = AsyncMock()
    monkeypatch.setattr(main_module, "worker_provisioner", provisioner)
    state_key = f"{worker.id}:codex:existing@example.com"
    workers_api._worker_login_state.pop(state_key, None)

    response = await client.post(
        f"/api/workers/{worker.id}/pool/add",
        json={
            "email": "EXISTING@example.com",
            "provider": "codex",
            "token": "new-token",
        },
    )

    assert response.status_code == 409
    assert "已在 Worker 号池" in response.json()["detail"]
    provisioner.ensure_codex_account.assert_not_awaited()
    assert state_key not in workers_api._worker_login_state
    async with session_factory() as db:
        assert (await db.get(Worker, worker.id)).accounts == [account]


async def test_dynamic_codex_remote_success_is_failed_when_local_commit_fails(
    client, session_factory, monkeypatch,
):
    worker = await _insert_worker(session_factory, accounts=[])
    provisioner = WorkerProvisioner(session_factory, cloud=object())

    async def remote_success(_worker, _account, **kwargs):
        await kwargs["on_status"]({
            "status": "success",
            "account_id": "codex-9",
        })
        return "codex-9"

    provisioner.ensure_codex_account = AsyncMock(side_effect=remote_success)
    monkeypatch.setattr(main_module, "worker_provisioner", provisioner)
    original_persist = workers_api._persist_worker_account_state

    async def fail_final_commit(*args, **kwargs):
        if kwargs.get("status") == "logged_in":
            raise RuntimeError("manager account commit failed")
        return await original_persist(*args, **kwargs)

    monkeypatch.setattr(
        workers_api,
        "_persist_worker_account_state",
        fail_final_commit,
    )
    state_key = f"{worker.id}:codex:commit-failure@example.com"
    workers_api._worker_login_state.pop(state_key, None)

    response = await client.post(
        f"/api/workers/{worker.id}/pool/add",
        json={
            "email": "commit-failure@example.com",
            "provider": "codex",
            "token": "mail-token",
        },
    )
    await _drain_worker_background_tasks()
    status = await client.get(
        f"/api/workers/{worker.id}/pool/add/commit-failure%40example.com"
        "?provider=codex"
    )

    assert response.status_code == 200, response.text
    assert status.status_code == 200
    assert status.json()["status"] == "failed"
    assert "manager account commit failed" in status.json()["detail"]
    async with session_factory() as db:
        persisted = await db.get(Worker, worker.id)
    assert persisted.accounts == [{
        "email": "commit-failure@example.com",
        "provider": "codex",
        "token": "mail-token",
        "password": "",
        "login_method": "",
        "account_id": "codex-9",
        "status": "failed",
    }]
    workers_api._worker_login_state.pop(state_key, None)


async def test_worker_account_delete_remote_404_still_clears_local_credentials(
    client, session_factory, monkeypatch,
):
    worker = await _insert_worker(session_factory, accounts=[{
        "email": "already-absent@example.com",
        "provider": "codex",
        "token": "mail-token-that-must-be-erased",
        "password": "password-that-must-be-erased",
        "login_method": "mailcatcher",
        "status": "logged_in",
        "account_id": "codex-3",
    }])

    class MissingAccountClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def delete(self, _url, **_kwargs):
            return _JSONResponse({"detail": "not found"}, status_code=404)

    monkeypatch.setattr(httpx, "AsyncClient", MissingAccountClient)

    response = await client.delete(
        f"/api/workers/{worker.id}/pool/codex-3?provider=codex"
    )

    assert response.status_code == 200, response.text
    assert response.json() == {"ok": True, "already_absent": True}
    async with session_factory() as db:
        assert (await db.get(Worker, worker.id)).accounts == []


async def test_worker_account_delete_ack_loss_replays_exact_tombstone(
    client,
    session_factory,
    monkeypatch,
):
    account = {
        "email": "ack-loss@example.com",
        "provider": "codex",
        "token": "mail-token-that-must-be-erased",
        "password": "password-that-must-be-erased",
        "login_method": "mailcatcher",
        "status": "logged_in",
        "account_id": "codex-ack",
    }
    worker = await _insert_worker(session_factory, accounts=[account])
    provisioner = WorkerProvisioner(session_factory, cloud=object())
    provisioner.ensure_codex_account = AsyncMock(
        side_effect=AssertionError("deleting slot must not be revived")
    )
    provisioner.create_worker = AsyncMock()
    monkeypatch.setattr(main_module, "worker_provisioner", provisioner)
    observed_operations: list[str] = []

    async def ack_loss_then_absent(
        _worker,
        method,
        path,
        **_kwargs,
    ):
        assert method == "DELETE"
        assert path == "/api/codex-pool/accounts/codex-ack"
        async with session_factory() as db:
            persisted = await db.get(Worker, worker.id)
            tombstone = persisted.accounts[0]
            receipt = tombstone[
                workers_api._WORKER_ACCOUNT_DELETE_RECEIPT_KEY
            ]
            observed_operations.append(receipt["operation_id"])
            assert tombstone["status"] == "deleting"
            assert "token" not in tombstone
            assert "password" not in tombstone
            assert "login_method" not in tombstone
        if len(observed_operations) == 1:
            raise HTTPException(502, "remote DELETE ACK was lost")
        return _JSONResponse({"detail": "not found"}, status_code=404)

    remote_delete = AsyncMock(side_effect=ack_loss_then_absent)
    monkeypatch.setattr(workers_api, "_worker_http_request", remote_delete)

    first = await client.delete(
        f"/api/workers/{worker.id}/pool/codex-ack?provider=codex"
    )
    assert first.status_code == 502
    async with session_factory() as db:
        persisted = await db.get(Worker, worker.id)
        tombstone = persisted.accounts[0]
        receipt = tombstone[workers_api._WORKER_ACCOUNT_DELETE_RECEIPT_KEY]
    assert tombstone == {
        "email": account["email"],
        "provider": "codex",
        "account_id": "codex-ack",
        "status": "deleting",
        workers_api._WORKER_ACCOUNT_DELETE_RECEIPT_KEY: receipt,
    }
    assert receipt["state"] == "prepared"
    assert receipt["worker_id"] == worker.id
    assert receipt["worker_owner_user_id"] is None
    assert receipt["provider"] == "codex"
    assert receipt["account_id"] == "codex-ack"
    serialized = json.dumps(tombstone)
    assert account["token"] not in serialized
    assert account["password"] not in serialized

    from backend.models.user import User

    async with session_factory() as db:
        second_admin = User(
            email="second-delete-admin@example.com",
            name="second delete admin",
            password_hash="test",
            role="admin",
            is_active=True,
        )
        db.add(second_admin)
        await db.commit()
        await db.refresh(second_admin)
        second_admin_id = second_admin.id
    second_actor_request = SimpleNamespace(
        state=SimpleNamespace(
            auth_type="jwt",
            user_id=second_admin_id,
            user_role="admin",
        )
    )
    async with session_factory() as db:
        with pytest.raises(HTTPException) as second_actor:
            await workers_api.delete_worker_account(
                worker.id,
                second_actor_request,
                "codex-ack",
                provider="codex",
                db=db,
            )
    assert second_actor.value.status_code == 409
    assert "另一授权主体" in str(second_actor.value.detail)
    assert remote_delete.await_count == 1

    assignment_request = SimpleNamespace(
        state=SimpleNamespace(
            auth_type="token",
            user_id=None,
            user_role="super_admin",
        )
    )
    async with session_factory() as db:
        with pytest.raises(HTTPException) as blocked_assignment:
            await workers_api.assign_worker(
                worker.id,
                workers_api.AssignWorkerBody(owner_user_id=999_999),
                assignment_request,
                db,
            )
    assert blocked_assignment.value.status_code == 409
    assert "账号删除" in str(blocked_assignment.value.detail)
    async with session_factory() as db:
        assert (await db.get(Worker, worker.id)).owner_user_id is None

    blocked_add = await client.post(
        f"/api/workers/{worker.id}/pool/add",
        json={
            "email": account["email"],
            "provider": "codex",
            "token": "replacement-token-must-not-persist",
        },
    )
    assert blocked_add.status_code == 409
    provisioner.ensure_codex_account.assert_not_awaited()

    with pytest.raises(RuntimeError, match="deletion is awaiting"):
        await workers_api._persist_worker_account_state(
            provisioner,
            worker.id,
            {
                "email": account["email"],
                "provider": "codex",
                "token": "late-token-must-not-persist",
                "password": "late-password-must-not-persist",
                "login_method": "",
            },
            status="logged_in",
            account_id="codex-ack",
        )

    async with session_factory() as db:
        persisted = await db.get(Worker, worker.id)
        persisted.status = "error"
        await db.commit()
    blocked_bootstrap = await client.post(f"/api/workers/{worker.id}/retry")
    assert blocked_bootstrap.status_code == 409
    provisioner.create_worker.assert_not_awaited()

    # ``error`` is a health-state drift, not a new lifecycle identity. The
    # same actor may replay only this exact ready-time receipt; settlement must
    # not falsely mark the Worker healthy again.
    retried = await client.delete(
        f"/api/workers/{worker.id}/pool/codex-ack?provider=codex"
    )
    assert retried.status_code == 200, retried.text
    assert retried.json() == {"ok": True, "already_absent": True}
    assert observed_operations == [
        receipt["operation_id"],
        receipt["operation_id"],
    ]
    async with session_factory() as db:
        settled = await db.get(Worker, worker.id)
    assert settled.status == "error"
    assert settled.accounts == []


async def test_worker_account_delete_error_without_tombstone_is_replay_only(
    client,
    session_factory,
    monkeypatch,
):
    account = {
        "email": "error-without-delete@example.com",
        "provider": "codex",
        "token": "credential-must-remain",
        "password": "password-must-remain",
        "account_id": "codex-error-without-delete",
        "status": "logged_in",
    }
    worker = await _insert_worker(
        session_factory,
        status="error",
        accounts=[account],
    )
    remote_delete = AsyncMock()
    monkeypatch.setattr(workers_api, "_worker_http_request", remote_delete)

    rejected = await client.delete(
        f"/api/workers/{worker.id}/pool/codex-error-without-delete"
        "?provider=codex"
    )

    assert rejected.status_code == 409
    assert "只允许重放" in rejected.json()["detail"]
    remote_delete.assert_not_awaited()
    async with session_factory() as db:
        persisted = await db.get(Worker, worker.id)
    assert persisted.status == "error"
    assert persisted.accounts == [account]


@pytest.mark.parametrize(
    "drift_field",
    (
        "owner_user_id",
        "destroy_lifecycle_nonce",
        "private_ip",
        "ccm_port",
        "auth_token",
    ),
)
async def test_worker_account_delete_error_replay_rejects_identity_drift(
    client,
    session_factory,
    monkeypatch,
    drift_field,
):
    from backend.models.user import User

    worker = await _insert_worker(session_factory, accounts=[{
        "email": f"error-drift-{drift_field}@example.com",
        "provider": "codex",
        "token": "drift-token-must-be-erased",
        "password": "drift-password-must-be-erased",
        "account_id": f"codex-error-drift-{drift_field}",
        "status": "logged_in",
    }])
    remote_delete = AsyncMock(
        side_effect=HTTPException(502, "remote DELETE ACK was lost")
    )
    monkeypatch.setattr(workers_api, "_worker_http_request", remote_delete)
    path = (
        f"/api/workers/{worker.id}/pool/codex-error-drift-{drift_field}"
        "?provider=codex"
    )

    first = await client.delete(path)
    assert first.status_code == 502
    async with session_factory() as db:
        persisted = await db.get(Worker, worker.id)
        persisted.status = "error"
        if drift_field == "owner_user_id":
            new_owner = User(
                email="delete-drift-owner@example.com",
                name="delete drift owner",
                password_hash="test",
                role="member",
                is_active=True,
            )
            db.add(new_owner)
            await db.flush()
            persisted.owner_user_id = new_owner.id
        elif drift_field == "destroy_lifecycle_nonce":
            persisted.destroy_lifecycle_nonce = "d" * 32
        elif drift_field == "private_ip":
            persisted.private_ip = "10.0.0.250"
        elif drift_field == "ccm_port":
            persisted.ccm_port = (persisted.ccm_port or 8002) + 1
        elif drift_field == "auth_token":
            persisted.auth_token = "rotated-worker-control-token"
        await db.commit()

    rejected = await client.delete(path)

    assert rejected.status_code == 409
    assert remote_delete.await_count == 1
    async with session_factory() as db:
        retained = await db.get(Worker, worker.id)
    assert retained.status == "error"
    assert workers_api._has_worker_account_delete_outbox(retained.accounts)


async def test_worker_account_delete_malformed_top_level_accounts_fail_closed(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _insert_worker(
        session_factory,
        accounts={
            "status": "deleting",
            workers_api._WORKER_ACCOUNT_DELETE_RECEIPT_KEY: {
                "operation_id": "malformed",
            },
        },
    )
    remote_delete = AsyncMock()
    monkeypatch.setattr(workers_api, "_worker_http_request", remote_delete)

    rejected = await client.delete(
        f"/api/workers/{worker.id}/pool/codex-malformed?provider=codex"
    )

    assert rejected.status_code == 409
    assert "账号列表格式无效" in rejected.json()["detail"]
    remote_delete.assert_not_awaited()
    lifecycle_request = SimpleNamespace(
        state=SimpleNamespace(
            auth_type="token",
            user_id=None,
            user_role="super_admin",
        )
    )
    async with session_factory() as db:
        with pytest.raises(HTTPException) as blocked_lifecycle:
            await workers_api._transition_worker_status(
                db,
                lifecycle_request,
                worker.id,
                allowed_statuses=("ready",),
                target_status="stopping",
            )
        # Even an internal caller which catches the rejection and commits the
        # same session cannot publish the provisional lifecycle CAS.
        await db.commit()
    assert blocked_lifecycle.value.status_code == 409
    async with session_factory() as db:
        retained = await db.get(Worker, worker.id)
    assert retained.status == "ready"
    assert retained.accounts["status"] == "deleting"


async def test_worker_lifecycle_authority_rejection_rolls_back_before_guard_release(
    session_factory,
):
    from backend.models.user import User

    async with session_factory() as db:
        stale_admin = User(
            email="lifecycle-stale-admin@example.com",
            name="lifecycle stale admin",
            password_hash="test",
            role="member",
            is_active=True,
        )
        worker = Worker(
            name="authority rollback worker",
            status="ready",
            private_ip="10.0.0.71",
            auth_token="authority-rollback-token",
            accounts=[],
        )
        db.add_all((stale_admin, worker))
        await db.commit()
        stale_admin_id = stale_admin.id
        worker_id = worker.id

    stale_request = SimpleNamespace(
        state=SimpleNamespace(
            auth_type="jwt",
            user_id=stale_admin_id,
            user_role="admin",
        )
    )
    async with session_factory() as db:
        with pytest.raises(HTTPException):
            await workers_api._transition_worker_status(
                db,
                stale_request,
                worker_id,
                allowed_statuses=("ready",),
                target_status="stopping",
            )
        await db.commit()

    async with session_factory() as db:
        retained = await db.get(Worker, worker_id)
    assert retained.status == "ready"
    assert retained.destroy_lifecycle_nonce is None


async def test_worker_assignment_recipient_rejection_rolls_back_before_guard_release(
    session_factory,
):
    from backend.models.user import User

    async with session_factory() as db:
        disabled_owner = User(
            email="disabled-worker-owner@example.com",
            name="disabled worker owner",
            password_hash="test",
            role="member",
            is_active=False,
        )
        worker = Worker(
            name="assignment rollback worker",
            status="ready",
            private_ip="10.0.0.72",
            auth_token="assignment-rollback-token",
            accounts=[],
        )
        db.add_all((disabled_owner, worker))
        await db.commit()
        disabled_owner_id = disabled_owner.id
        worker_id = worker.id

    admin_request = SimpleNamespace(
        state=SimpleNamespace(
            auth_type="token",
            user_id=None,
            user_role="super_admin",
        )
    )
    async with session_factory() as db:
        with pytest.raises(HTTPException) as rejected:
            await workers_api.assign_worker(
                worker_id,
                workers_api.AssignWorkerBody(owner_user_id=disabled_owner_id),
                admin_request,
                db,
            )
        await db.commit()
    assert rejected.value.status_code == 400

    async with session_factory() as db:
        retained = await db.get(Worker, worker_id)
    assert retained.owner_user_id is None


async def test_worker_account_delete_rechecks_role_and_owner_before_tombstone(
    session_factory,
    monkeypatch,
):
    from backend.models.user import User

    async with session_factory() as db:
        stale_admin = User(
            email="stale-delete-admin@example.com",
            name="stale delete admin",
            password_hash="test",
            role="member",
            is_active=True,
        )
        old_owner = User(
            email="old-delete-owner@example.com",
            name="old delete owner",
            password_hash="test",
            role="member",
            is_active=True,
        )
        new_owner = User(
            email="new-delete-owner@example.com",
            name="new delete owner",
            password_hash="test",
            role="member",
            is_active=True,
        )
        db.add_all((stale_admin, old_owner, new_owner))
        await db.flush()
        role_worker = Worker(
            name="role-fenced account delete",
            status="ready",
            private_ip="10.0.0.41",
            auth_token="role-worker-token",
            accounts=[{
                "email": "role-delete@example.com",
                "provider": "codex",
                "token": "role-secret",
                "account_id": "codex-role",
                "status": "logged_in",
            }],
        )
        owner_worker = Worker(
            name="owner-fenced account delete",
            status="ready",
            private_ip="10.0.0.42",
            auth_token="owner-worker-token",
            owner_user_id=new_owner.id,
            accounts=[{
                "email": "owner-delete@example.com",
                "provider": "codex",
                "token": "owner-secret",
                "account_id": "codex-owner",
                "status": "logged_in",
            }],
        )
        db.add_all((role_worker, owner_worker))
        await db.commit()
        stale_admin_id = stale_admin.id
        old_owner_id = old_owner.id
        role_worker_id = role_worker.id
        owner_worker_id = owner_worker.id

    monkeypatch.setattr(
        api_deps,
        "require_worker_access",
        AsyncMock(return_value=None),
    )
    remote = AsyncMock()
    monkeypatch.setattr(workers_api, "_worker_http_request", remote)
    stale_role_request = SimpleNamespace(
        state=SimpleNamespace(
            auth_type="jwt",
            user_id=stale_admin_id,
            user_role="admin",
        )
    )
    stale_owner_request = SimpleNamespace(
        state=SimpleNamespace(
            auth_type="jwt",
            user_id=old_owner_id,
            user_role="member",
        )
    )

    async with session_factory() as db:
        with pytest.raises(HTTPException) as stale_role:
            await workers_api.delete_worker_account(
                role_worker_id,
                stale_role_request,
                "codex-role",
                provider="codex",
                db=db,
            )
    assert stale_role.value.status_code == 409
    async with session_factory() as db:
        with pytest.raises(HTTPException) as stale_owner:
            await workers_api.delete_worker_account(
                owner_worker_id,
                stale_owner_request,
                "codex-owner",
                provider="codex",
                db=db,
            )
    assert stale_owner.value.status_code == 409
    remote.assert_not_awaited()
    async with session_factory() as db:
        role_accounts = (await db.get(Worker, role_worker_id)).accounts
        owner_accounts = (await db.get(Worker, owner_worker_id)).accounts
    assert role_accounts[0]["token"] == "role-secret"
    assert owner_accounts[0]["token"] == "owner-secret"
    assert not workers_api._has_worker_account_delete_outbox(role_accounts)
    assert not workers_api._has_worker_account_delete_outbox(owner_accounts)


async def test_worker_account_delete_tombstone_blocks_destroy_race(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _insert_worker(session_factory, accounts=[{
        "email": "destroy-delete-race@example.com",
        "provider": "codex",
        "token": "destroy-race-token",
        "password": "destroy-race-password",
        "account_id": "codex-destroy-race",
        "status": "logged_in",
    }])
    remote_entered = asyncio.Event()
    release_remote = asyncio.Event()

    async def blocked_remote(*_args, **_kwargs):
        remote_entered.set()
        await release_remote.wait()
        return _JSONResponse({"ok": True})

    monkeypatch.setattr(
        workers_api,
        "_worker_http_request",
        AsyncMock(side_effect=blocked_remote),
    )
    deleting = asyncio.create_task(client.delete(
        f"/api/workers/{worker.id}/pool/codex-destroy-race?provider=codex"
    ))
    await asyncio.wait_for(remote_entered.wait(), timeout=1)

    lifecycle_request = SimpleNamespace(
        state=SimpleNamespace(
            auth_type="token",
            user_id=None,
            user_role="super_admin",
        )
    )
    try:
        async with session_factory() as db:
            with pytest.raises(HTTPException) as blocked_destroy:
                await workers_api._transition_worker_status(
                    db,
                    lifecycle_request,
                    worker.id,
                    allowed_statuses=("ready",),
                    target_status="destroying",
                    block_active_task_terminations=True,
                    destroy_lifecycle_nonce=secrets.token_hex(16),
                )
        assert blocked_destroy.value.status_code == 409
        assert "账号删除" in str(blocked_destroy.value.detail)
    finally:
        release_remote.set()

    deleted = await deleting
    assert deleted.status_code == 200, deleted.text
    async with session_factory() as db:
        persisted = await db.get(Worker, worker.id)
    assert persisted.status == "ready"
    assert persisted.destroy_lifecycle_nonce is None
    assert persisted.accounts == []


async def test_worker_account_delete_prepare_serializes_lifecycle_rollback(
    client,
    session_factory,
    monkeypatch,
):
    worker = await _insert_worker(session_factory, accounts=[{
        "email": "uncommitted-delete@example.com",
        "provider": "codex",
        "token": "uncommitted-token-must-be-erased",
        "password": "uncommitted-password-must-be-erased",
        "account_id": "codex-uncommitted-delete",
        "status": "logged_in",
    }])
    prepare_flushed = asyncio.Event()
    release_prepare_commit = asyncio.Event()
    lifecycle_requested = asyncio.Event()
    lifecycle_body_entered = asyncio.Event()
    original_commit = AsyncSession.commit
    original_lifecycle_guard = workers_api._worker_lifecycle_transaction_lock
    original_transition_locked = workers_api._transition_worker_status_locked
    prepare_commit_seen = False

    async def commit_with_prepare_barrier(session):
        nonlocal prepare_commit_seen
        has_delete_tombstone = any(
            isinstance(obj, Worker)
            and workers_api._has_worker_account_delete_outbox(obj.accounts)
            for obj in session.dirty
        )
        if not prepare_commit_seen and has_delete_tombstone:
            prepare_commit_seen = True
            # Materialize the JSON UPDATE on StaticPool's one connection, then
            # hold it uncommitted. A competing session rollback used to erase
            # this outbox while the first session later reported commit success.
            await session.flush()
            prepare_flushed.set()
            await release_prepare_commit.wait()
        await original_commit(session)

    @asynccontextmanager
    async def observed_lifecycle_guard(worker_id):
        task = asyncio.current_task()
        if task is not None and task.get_name() == "competing-destroy":
            lifecycle_requested.set()
        async with original_lifecycle_guard(worker_id):
            yield

    async def observed_transition_locked(*args, **kwargs):
        task = asyncio.current_task()
        if task is not None and task.get_name() == "competing-destroy":
            lifecycle_body_entered.set()
        return await original_transition_locked(*args, **kwargs)

    monkeypatch.setattr(AsyncSession, "commit", commit_with_prepare_barrier)
    monkeypatch.setattr(
        workers_api,
        "_worker_lifecycle_transaction_lock",
        observed_lifecycle_guard,
    )
    monkeypatch.setattr(
        workers_api,
        "_transition_worker_status_locked",
        observed_transition_locked,
    )
    remote_delete = AsyncMock(
        side_effect=HTTPException(502, "remote DELETE ACK was lost")
    )
    monkeypatch.setattr(workers_api, "_worker_http_request", remote_delete)

    deleting = asyncio.create_task(
        client.delete(
            f"/api/workers/{worker.id}/pool/codex-uncommitted-delete"
            "?provider=codex"
        ),
        name="uncommitted-account-delete",
    )
    await asyncio.wait_for(prepare_flushed.wait(), timeout=1)

    lifecycle_request = SimpleNamespace(
        state=SimpleNamespace(
            auth_type="token",
            user_id=None,
            user_role="super_admin",
        )
    )

    async def competing_destroy():
        async with session_factory() as db:
            try:
                await workers_api._transition_worker_status(
                    db,
                    lifecycle_request,
                    worker.id,
                    allowed_statuses=("ready",),
                    target_status="destroying",
                    block_active_task_terminations=True,
                    destroy_lifecycle_nonce=secrets.token_hex(16),
                )
            except HTTPException as exc:
                return exc
        raise AssertionError("destroy unexpectedly crossed account delete")

    destroying = asyncio.create_task(
        competing_destroy(),
        name="competing-destroy",
    )
    await asyncio.wait_for(lifecycle_requested.wait(), timeout=1)
    assert not lifecycle_body_entered.is_set()
    release_prepare_commit.set()
    await asyncio.wait_for(lifecycle_body_entered.wait(), timeout=1)

    deleted = await deleting
    blocked_destroy = await destroying
    assert deleted.status_code == 502
    assert blocked_destroy.status_code == 409
    assert "账号删除" in str(blocked_destroy.detail)
    remote_delete.assert_awaited_once()
    async with session_factory() as db:
        persisted = await db.get(Worker, worker.id)
    assert persisted.status == "ready"
    assert persisted.destroy_lifecycle_nonce is None
    assert workers_api._has_worker_account_delete_outbox(persisted.accounts)
    serialized = json.dumps(persisted.accounts)
    assert "uncommitted-token-must-be-erased" not in serialized
    assert "uncommitted-password-must-be-erased" not in serialized


async def test_worker_add_status_requires_worker_access(
    client, session_factory, monkeypatch,
):
    worker = await _insert_worker(session_factory, owner_user_id=42)
    state_key = f"{worker.id}:codex:private@example.com"
    workers_api._worker_login_state[state_key] = {
        "status": "failed",
        "detail": "private login detail",
    }
    access_guard = AsyncMock(
        side_effect=HTTPException(status_code=403, detail="No access to this worker")
    )
    monkeypatch.setattr(api_deps, "require_worker_access", access_guard)
    try:
        response = await client.get(
            f"/api/workers/{worker.id}/pool/add/private%40example.com"
            "?provider=codex"
        )
    finally:
        workers_api._worker_login_state.pop(state_key, None)

    assert response.status_code == 403
    assert response.json()["detail"] == "No access to this worker"
    access_guard.assert_awaited_once()
