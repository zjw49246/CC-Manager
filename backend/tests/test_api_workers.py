"""Tests for Worker management API + provisioner state machine."""
import asyncio
import base64
import copy
import logging
import shlex
import socket
from unittest.mock import AsyncMock, Mock

import pytest

import backend.main as main_module
from backend.config import settings
from backend.api.workers import (
    _build_add_account_command,
    _persist_worker_account_state,
    _remove_persisted_worker_account,
)
from backend.models.worker import Worker
from backend.services.worker_provisioner import (
    BootstrapError,
    WorkerProvisioner,
    _build_account_login_script,
    _build_script_upload_command,
)
from backend.services.ssh_executor import SSHExecutor, SSHKeyMaterial
from backend.services.ssh_executor import SSHKeyPreflightError


# === Fixtures ===


@pytest.fixture
def fake_provisioner(monkeypatch):
    prov = AsyncMock()
    prov.cloud = AsyncMock()
    prov.cloud.self_describe.return_value = {"name": "test-manager"}
    prov.preflight_ssh_key = Mock(return_value=None)
    monkeypatch.setattr(main_module, "worker_provisioner", prov)
    return prov


async def _insert_worker(session_factory, **fields) -> int:
    fields.setdefault("status", "ready")
    async with session_factory() as db:
        worker = Worker(name="test-worker", **fields)
        db.add(worker)
        await db.commit()
        await db.refresh(worker)
        return worker.id


class FakeCloud:
    """最小 CloudProvider 替身。"""

    def __init__(self):
        self.calls = []
        self.last_overrides = None

    async def self_describe(self):
        return {"instance_type": "t3.large"}

    async def describe_instance(self, iid):
        self.calls.append(("describe", iid))
        return {"instance_id": iid, "state": "stopped", "private_ip": "10.0.0.9",
                "public_ip": None, "name": "x"}

    async def create_instance(self, name, overrides=None):
        self.calls.append(("create", name))
        self.last_overrides = overrides
        return "i-new123"

    async def wait_until_running(self, iid, timeout=300):
        self.calls.append(("wait", iid))
        return "10.0.0.9"

    async def stop_instance(self, iid):
        self.calls.append(("stop", iid))

    async def start_instance(self, iid):
        self.calls.append(("start", iid))

    async def terminate_instance(self, iid):
        self.calls.append(("terminate", iid))


# === API tests ===


async def test_list_workers_empty(client):
    resp = await client.get("/api/workers")
    assert resp.status_code == 200
    assert resp.json() == []


async def test_startup_recovery_makes_interrupted_worker_lifecycles_retryable(
    session_factory, monkeypatch,
):
    statuses = ["creating", "bootstrapping", "starting", "stopping", "destroying"]
    worker_ids = []
    async with session_factory() as db:
        for status in statuses:
            worker = Worker(
                name=f"stale-{status}",
                status=status,
                cloud_instance_id=f"i-{status}",
                auth_token="worker-secret",
                accounts=[{
                    "email": "codex@example.com",
                    "provider": "codex",
                    "token": "mail-token",
                    "password": "openai-password",
                }],
            )
            db.add(worker)
            await db.flush()
            worker_ids.append(worker.id)
        await db.commit()
    monkeypatch.setattr(main_module, "async_session", session_factory)

    await main_module._recover_stale_worker_lifecycles()

    async with session_factory() as db:
        recovered = [await db.get(Worker, worker_id) for worker_id in worker_ids]
    assert [worker.status for worker in recovered] == ["error"] * len(statuses)
    for previous, worker in zip(statuses, recovered):
        assert worker.cloud_instance_id == f"i-{previous}"
        assert worker.auth_token == "worker-secret"
        assert worker.accounts[0]["token"] == "mail-token"
        assert worker.bootstrap_step == (
            "destroy" if previous == "destroying" else "startup-recovery"
        )


async def test_interrupted_destroy_cannot_be_retried_as_bootstrap(
    client, session_factory, fake_provisioner,
):
    worker_id = await _insert_worker(
        session_factory,
        status="error",
        bootstrap_step="destroy",
        bootstrap_error="Manager restarted during destroy",
        cloud_instance_id="i-destroying",
    )

    response = await client.post(f"/api/workers/{worker_id}/retry")

    assert response.status_code == 409
    assert "只能重试销毁" in response.json()["detail"]
    fake_provisioner.create_worker.assert_not_awaited()


async def test_create_worker_disabled_returns_503(client, monkeypatch):
    monkeypatch.setattr(main_module, "worker_provisioner", None)
    resp = await client.post("/api/workers", json={"accounts": []})
    assert resp.status_code == 503


async def test_create_worker_rejects_bad_ssh_key_before_db_or_cloud(
    client, fake_provisioner,
):
    fake_provisioner.preflight_ssh_key.side_effect = SSHKeyPreflightError(
        "key_permissions", "SSH private key permissions are too broad",
    )

    resp = await client.post("/api/workers", json={"name": "unsafe-worker"})

    assert resp.status_code == 503
    assert "key_permissions" in resp.json()["detail"]
    assert (await client.get("/api/workers")).json() == []
    fake_provisioner.create_worker.assert_not_called()


async def test_create_worker_auto_name_and_background_task(
    client, fake_provisioner, session_factory,
):
    resp = await client.post(
        "/api/workers",
        json={
            "name": "test-w",
            "accounts": [{
                "email": "a@x.com",
                "token": "tok123",
                "login_method": "onet",
            }],
        },
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["name"] == "test-w"
    assert data["status"] == "creating"
    assert data["accounts"] == [{
        "email": "a@x.com",
        "provider": "codex",
        "status": "pending",
    }]
    await asyncio.sleep(0)  # 让 create_task 跑起来
    fake_provisioner.create_worker.assert_called_once()
    kwargs = fake_provisioner.create_worker.call_args.kwargs
    assert kwargs["accounts"] == [{
        "email": "a@x.com",
        "provider": "codex",
        "token": "tok123",
        "password": "",
        "login_method": "onet",
    }]
    async with session_factory() as db:
        worker = await db.get(Worker, data["id"])
    assert worker.accounts == [{
        "email": "a@x.com",
        "provider": "codex",
        "token": "tok123",
        "password": "",
        "login_method": "onet",
        "status": "pending",
    }]


@pytest.mark.parametrize("token", [None, "", " \n "])
async def test_create_worker_rejects_empty_account_token(
    client, fake_provisioner, token,
):
    resp = await client.post(
        "/api/workers",
        json={"name": "test-w", "accounts": [{"email": "a@x.com", "token": token}]},
    )
    assert resp.status_code == 400
    assert "token" in resp.json()["detail"]
    fake_provisioner.create_worker.assert_not_called()


async def test_create_worker_accepts_unattended_codex_without_leaking_secrets(
    client, fake_provisioner, session_factory,
):
    password = "  openai-password-with-spaces  "
    resp = await client.post(
        "/api/workers",
        json={
            "name": "codex-worker",
            "accounts": [{
                "email": "codex@example.com",
                "token": "mailbox-token",
                "password": password,
                "login_method": "mailcatcher",
            }],
        },
    )

    assert resp.status_code == 200, resp.text
    assert resp.json()["accounts"] == [{
        "email": "codex@example.com",
        "provider": "codex",
        "status": "pending",
    }]
    assert password not in resp.text
    await asyncio.sleep(0)
    expected = {
        "email": "codex@example.com",
        "provider": "codex",
        "token": "mailbox-token",
        "password": password,
        "login_method": "mailcatcher",
    }
    fake_provisioner.create_worker.assert_awaited_once_with(
        resp.json()["id"], accounts=[expected]
    )
    async with session_factory() as db:
        worker = await db.get(Worker, resp.json()["id"])
    assert worker.accounts == [{**expected, "status": "pending"}]


async def test_create_worker_requires_token_for_explicit_claude_account(
    client, fake_provisioner,
):
    resp = await client.post(
        "/api/workers",
        json={
            "name": "claude-worker",
            "accounts": [{
                "email": "claude@example.com",
                "provider": "claude",
                "password": "not-a-claude-login-credential",
            }],
        },
    )

    assert resp.status_code == 400
    assert "Claude" in resp.json()["detail"]
    assert "token" in resp.json()["detail"]
    fake_provisioner.create_worker.assert_not_called()


async def test_create_worker_rejects_unknown_account_provider(
    client, fake_provisioner,
):
    resp = await client.post(
        "/api/workers",
        json={
            "name": "bad-worker",
            "accounts": [{
                "email": "a@example.com",
                "provider": "other",
                "token": "token",
            }],
        },
    )

    assert resp.status_code == 400
    assert "provider" in resp.json()["detail"]
    fake_provisioner.create_worker.assert_not_called()


async def test_create_worker_rejects_case_insensitive_duplicate_identity_before_start(
    client, fake_provisioner,
):
    response = await client.post(
        "/api/workers",
        json={
            "name": "duplicate-worker",
            "accounts": [
                {
                    "email": "Duplicate@Example.com",
                    "provider": "codex",
                    "token": "first-mail-token",
                },
                {
                    "email": "duplicate@example.com",
                    "provider": "CODEX",
                    "token": "second-mail-token",
                },
            ],
        },
    )

    assert response.status_code == 400
    assert "重复的 Worker 账号" in response.json()["detail"]
    assert (await client.get("/api/workers")).json() == []
    fake_provisioner.create_worker.assert_not_awaited()


async def test_stop_requires_ready(client, session_factory, fake_provisioner):
    wid = await _insert_worker(session_factory, status="stopped")
    resp = await client.post(f"/api/workers/{wid}/stop")
    assert resp.status_code == 409


async def test_stop_start_destroy_flow(client, session_factory, fake_provisioner, monkeypatch, db_factory):
    import backend.api.workers as workers_api
    # destroy 链路走 _migrate_back_then_destroy（Phase 3），mock 掉让它直接调 prov.destroy_worker
    async def _simple_destroy(prov, worker_id, db_factory_arg=None):
        await prov.destroy_worker(worker_id)
    monkeypatch.setattr(workers_api, "_migrate_back_then_destroy", _simple_destroy)
    wid = await _insert_worker(session_factory, status="ready")
    assert (await client.post(f"/api/workers/{wid}/stop")).status_code == 200
    await asyncio.sleep(0)
    fake_provisioner.stop_worker.assert_called_once_with(wid)

    async with session_factory() as db:
        (await db.get(Worker, wid)).status = "stopped"
        await db.commit()
    assert (await client.post(f"/api/workers/{wid}/start")).status_code == 200
    await asyncio.sleep(0)
    fake_provisioner.start_worker.assert_called_once_with(wid)

    async with session_factory() as db:
        (await db.get(Worker, wid)).status = "ready"
        await db.commit()
    assert (await client.post(f"/api/workers/{wid}/destroy")).status_code == 200
    # destroy 现在走 _migrate_back_then_destroy → prov.destroy_worker，
    # 需要更多 event loop ticks 让后台任务完成
    for _ in range(50):
        await asyncio.sleep(0)
    fake_provisioner.destroy_worker.assert_called_once_with(wid)


@pytest.mark.parametrize(
    "status",
    ["creating", "bootstrapping", "starting", "stopping", "destroying"],
)
async def test_destroy_rejects_worker_lifecycle_busy_states(
    client, session_factory, fake_provisioner, status,
):
    wid = await _insert_worker(session_factory, status=status)

    response = await client.post(f"/api/workers/{wid}/destroy")

    assert response.status_code == 409
    assert status in response.json()["detail"]
    async with session_factory() as db:
        assert (await db.get(Worker, wid)).status == status
    fake_provisioner.destroy_worker.assert_not_called()


async def test_retry_only_from_error(client, session_factory, fake_provisioner):
    wid = await _insert_worker(session_factory, status="ready")
    assert (await client.post(f"/api/workers/{wid}/retry")).status_code == 409

    async with session_factory() as db:
        (await db.get(Worker, wid)).status = "error"
        await db.commit()
    assert (await client.post(f"/api/workers/{wid}/retry")).status_code == 200


async def test_retry_preserves_account_token_and_login_method(
    client, session_factory, fake_provisioner,
):
    wid = await _insert_worker(
        session_factory,
        status="error",
        accounts=[{
            "email": "onet@example.com",
            "token": "mailcatcher-token",
            "login_method": "onet",
            "status": "failed",
        }],
    )
    resp = await client.post(f"/api/workers/{wid}/retry")
    assert resp.status_code == 200, resp.text
    assert resp.json()["accounts"] == [{
        "email": "onet@example.com",
        "provider": "claude",
        "status": "failed",
    }]
    await asyncio.sleep(0)
    fake_provisioner.create_worker.assert_awaited_once_with(
        wid,
        accounts=[{
            "email": "onet@example.com",
            "provider": "claude",
            "token": "mailcatcher-token",
            "password": "",
            "login_method": "onet",
        }],
    )


async def test_retry_restores_codex_token_and_opaque_password_without_trimming(
    client, session_factory, fake_provisioner,
):
    password = "  exact-password  "
    wid = await _insert_worker(
        session_factory,
        status="error",
        accounts=[{
            "email": "codex@example.com",
            "provider": "codex",
            "token": "mailbox-token",
            "password": password,
            "login_method": "mailcatcher",
            "status": "failed",
            "account_id": "codex-1",
        }],
    )

    resp = await client.post(f"/api/workers/{wid}/retry")

    assert resp.status_code == 200, resp.text
    assert resp.json()["accounts"] == [{
        "email": "codex@example.com",
        "provider": "codex",
        "status": "failed",
    }]
    assert password not in resp.text
    await asyncio.sleep(0)
    fake_provisioner.create_worker.assert_awaited_once_with(
        wid,
        accounts=[{
            "email": "codex@example.com",
            "provider": "codex",
            "token": "mailbox-token",
            "password": password,
            "login_method": "mailcatcher",
            "account_id": "codex-1",
        }],
    )


async def test_retry_missing_historical_token_fails_without_status_change(
    client, session_factory, fake_provisioner,
):
    wid = await _insert_worker(
        session_factory,
        status="error",
        accounts=[{"email": "legacy@example.com", "status": "failed"}],
    )
    resp = await client.post(f"/api/workers/{wid}/retry")
    assert resp.status_code == 409
    assert "缺少 token" in resp.json()["detail"]
    async with session_factory() as db:
        worker = await db.get(Worker, wid)
    assert worker.status == "error"
    fake_provisioner.create_worker.assert_not_called()


async def test_retry_rejects_case_insensitive_duplicate_identity_before_start(
    client, session_factory, fake_provisioner,
):
    accounts = [
        {
            "email": "Duplicate@Example.com",
            "provider": "codex",
            "token": "first-mail-token",
            "password": "",
            "login_method": "mailcatcher",
            "status": "failed",
        },
        {
            "email": "duplicate@example.com",
            "provider": "CODEX",
            "token": "second-mail-token",
            "password": "",
            "login_method": "mailcatcher",
            "status": "failed",
        },
    ]
    worker_id = await _insert_worker(
        session_factory,
        status="error",
        accounts=accounts,
    )

    response = await client.post(f"/api/workers/{worker_id}/retry")

    assert response.status_code == 409
    assert "重复账号" in response.json()["detail"]
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert worker.status == "error"
    assert worker.accounts == accounts
    fake_provisioner.create_worker.assert_not_awaited()


def test_historical_claude_slots_stay_stable_across_sequential_deletes():
    accounts = [
        {"email": "first@example.com", "token": "first-secret"},
        {"email": "second@example.com", "token": "second-secret"},
    ]

    accounts, removed_default = _remove_persisted_worker_account(
        accounts, provider="claude", account_id="default",
    )
    assert removed_default is True
    assert accounts == [{
        "email": "second@example.com",
        "token": "second-secret",
        "account_id": "account-2",
    }]

    accounts, removed_second = _remove_persisted_worker_account(
        accounts, provider="claude", account_id="account-2",
    )
    assert removed_second is True
    assert accounts == []


async def test_persist_worker_account_state_keeps_concurrent_codex_updates(
    db_factory, session_factory,
):
    worker_id = await _insert_worker(
        session_factory,
        status="ready",
        accounts=[],
    )
    provisioner = WorkerProvisioner(db_factory, cloud=object())
    first = {
        "email": "first-codex@example.com",
        "provider": "codex",
        "token": "first-mail-token",
        "password": "first-openai-password",
        "login_method": "mailcatcher",
    }
    second = {
        "email": "second-codex@example.com",
        "provider": "codex",
        "token": "second-mail-token",
        "password": "second-openai-password",
        "login_method": "mailcatcher",
    }

    await asyncio.gather(
        _persist_worker_account_state(
            provisioner,
            worker_id,
            first,
            status="logged_in",
            account_id="codex-1",
        ),
        _persist_worker_account_state(
            provisioner,
            worker_id,
            second,
            status="logged_in",
            account_id="codex-2",
        ),
    )

    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert sorted(worker.accounts, key=lambda item: item["email"]) == [
        {
            **first,
            "account_id": "codex-1",
            "status": "logged_in",
        },
        {
            **second,
            "account_id": "codex-2",
            "status": "logged_in",
        },
    ]


async def test_get_worker_and_logs(client, session_factory, fake_provisioner):
    wid = await _insert_worker(session_factory, bootstrap_log="[00:00:00] hi\n")
    resp = await client.get(f"/api/workers/{wid}")
    assert resp.status_code == 200
    resp = await client.get(f"/api/workers/{wid}/logs")
    assert resp.json()["bootstrap_log"] == "[00:00:00] hi\n"
    assert (await client.get("/api/workers/999")).status_code == 404


async def test_rename_worker_with_known_active_instance_updates_db_and_tag(
    client, session_factory, monkeypatch,
):
    import backend.services.cloud_provider as cloud_provider_module

    worker_id = await _insert_worker(
        session_factory,
        status="ready",
        cloud_instance_id="i-known-active",
    )
    cloud = AsyncMock()
    cloud_factory = Mock(return_value=cloud)
    monkeypatch.setattr(cloud_provider_module, "AWSProvider", cloud_factory)
    monkeypatch.setattr(main_module, "broadcaster", None)

    response = await client.patch(
        f"/api/workers/{worker_id}/rename",
        json={"name": "renamed-known-worker"},
    )

    assert response.status_code == 200, response.text
    assert response.json()["name"] == "renamed-known-worker"
    cloud_factory.assert_called_once_with()
    cloud.update_instance_tags.assert_awaited_once_with(
        "i-known-active",
        {"Name": "renamed-known-worker"},
    )
    async with session_factory() as db:
        assert (await db.get(Worker, worker_id)).name == "renamed-known-worker"


async def test_rename_worker_rejects_pending_destroy_retry(
    client, session_factory, monkeypatch,
):
    import backend.services.cloud_provider as cloud_provider_module

    worker_id = await _insert_worker(
        session_factory,
        status="error",
        cloud_instance_id="i-destroy-retry",
        bootstrap_step="destroy",
    )
    cloud = AsyncMock()
    monkeypatch.setattr(cloud_provider_module, "AWSProvider", Mock(return_value=cloud))

    response = await client.patch(
        f"/api/workers/{worker_id}/rename",
        json={"name": "must-not-rename"},
    )

    assert response.status_code == 409
    cloud.update_instance_tags.assert_not_awaited()
    async with session_factory() as db:
        assert (await db.get(Worker, worker_id)).name != "must-not-rename"


async def test_terminated_workers_hidden_from_list(client, session_factory):
    await _insert_worker(session_factory, status="terminated")
    wid = await _insert_worker(session_factory, status="ready")
    resp = await client.get("/api/workers")
    assert [w["id"] for w in resp.json()] == [wid]


# === Provisioner state machine tests（cloud/bootstrap 全替身，不碰网络） ===


def _flag_value(argv: list[str], flag: str) -> str:
    return argv[argv.index(flag) + 1]


def test_worker_login_commands_quote_untrusted_values():
    email = "mail+'; touch /tmp/pwn; #@example.com"
    token = "line-one\n$(touch /tmp/pwn) ' \" end"
    remote_dir = "/srv/ccm dir; touch /tmp/pwn"
    script = _build_account_login_script(
        remote_dir,
        email=email,
        token=token,
        slot="account-2",
        login_method="gazeta",
    )
    assert shlex.split(script.splitlines()[3]) == ["cd", remote_dir]
    login_argv = shlex.split(script[script.index("uv run "):])
    assert _flag_value(login_argv, "--email") == email
    assert _flag_value(login_argv, "--token") == token
    assert _flag_value(login_argv, "--add-to-pool") == "account-2"
    assert _flag_value(login_argv, "--login-method") == "gazeta"
    assert _flag_value(login_argv, "--config-dir") == "$CONFIG_DIR"

    upload = _build_script_upload_command(script, "/tmp/a script.sh")
    upload_argv = shlex.split(upload)
    encoded = upload_argv[upload_argv.index("%s") + 1]
    assert base64.b64decode(encoded).decode() == script
    assert email not in upload
    assert token not in upload
    assert "<<" not in upload

    add_command = _build_add_account_command(
        remote_dir,
        email=email,
        token=token,
        slot="default",
        login_method="onet",
    )
    pieces = add_command.split(" && ")
    assert shlex.split(pieces[0]) == ["cd", remote_dir]
    add_argv = shlex.split(pieces[-1])
    assert _flag_value(add_argv, "--email") == email
    assert _flag_value(add_argv, "--token") == token
    assert _flag_value(add_argv, "--login-method") == "onet"


async def test_sensitive_ssh_command_is_redacted_from_debug_log(monkeypatch, caplog):
    ssh = SSHExecutor(host="worker.internal", user="ubuntu", key_path="/tmp/test-key")
    monkeypatch.setattr(ssh, "_run_sync", lambda _command, _timeout: (0, "ok"))

    with caplog.at_level(logging.DEBUG, logger="backend.services.ssh_executor"):
        result = await ssh.run("login --token super-secret-token", sensitive=True)

    assert result == (0, "ok")
    assert "super-secret-token" not in caplog.text
    assert "sensitive command redacted" in caplog.text


@pytest.mark.parametrize("main_mcp_enabled", [True, False])
async def test_provisioner_ccm_config_uses_private_stdin_atomic_write(
    db_factory, session_factory, monkeypatch, main_mcp_enabled,
):
    monkeypatch.setattr(settings, "codex_main_mcp_enabled", main_mcp_enabled)
    wid = await _insert_worker(
        session_factory,
        status="creating",
        auth_token="worker-super-secret-token",
    )
    prov = WorkerProvisioner(db_factory=db_factory, cloud=FakeCloud(), broadcaster=None)
    ssh = AsyncMock()
    ssh.run_with_input.return_value = (0, "ok")

    await prov._step_ccm_config(ssh, wid)

    ssh.run.assert_not_awaited()
    command, env = ssh.run_with_input.await_args.args
    assert "worker-super-secret-token" not in command
    assert "CODEX_POOL_ENABLED=true" in env
    assert "DEFAULT_PROVIDER=codex" in env
    assert (
        f"CODEX_MAIN_MCP_ENABLED={'true' if main_mcp_enabled else 'false'}"
        in env
    )
    assert "WORKER_ENABLED=false" in env
    assert "AUTH_TOKEN=worker-super-secret-token" in env
    assert "umask 077" in command
    assert "chmod 600" in command
    assert ".env.ccm-tmp" in command
    assert ssh.run_with_input.await_args.kwargs["sensitive"] is True


async def test_provisioner_system_init_installs_codex_and_login_runtime(
    db_factory,
):
    prov = WorkerProvisioner(db_factory=db_factory, cloud=FakeCloud(), broadcaster=None)
    ssh = AsyncMock()
    ssh.run.return_value = (
        0,
        "node=v22 uv=uv 0.1 claude=claude 1 codex=codex 1 chrome=Chrome 149 docker=Docker 1",
    )
    prov._log = AsyncMock()

    await prov._step_system_init(ssh, 1)

    script = ssh.run.await_args.args[0]
    assert "setup_22.x" in script
    assert 'CODEX_CLI_VERSION="0.144.6"' in script
    assert '@openai/codex@$CODEX_CLI_VERSION' in script
    assert '"codex-cli $CODEX_CLI_VERSION"' in script
    assert "xvfb xauth xdotool" in script
    assert "149.0.7827.53-1" in script
    assert "google-chrome-stable_current" not in script


async def test_provisioner_login_persists_credentials_and_onet_method(
    db_factory, session_factory,
):
    wid = await _insert_worker(session_factory, status="creating")
    prov = WorkerProvisioner(db_factory=db_factory, cloud=FakeCloud(), broadcaster=None)
    ssh = AsyncMock()
    ssh.run.side_effect = [(0, "uploaded"), (0, "login ok")]
    account = {
        "email": "user+'quote@example.com",
        "token": "secret\n'; echo injected",
        "login_method": "onet",
    }

    await prov._step_account_login(ssh, wid, [account])

    async with session_factory() as db:
        worker = await db.get(Worker, wid)
    assert worker.accounts == [{
        **account,
        "provider": "claude",
        "password": "",
        "status": "logged_in",
        "account_id": "default",
    }]
    upload_command = ssh.run.await_args_list[0].args[0]
    assert ssh.run.await_args_list[0].kwargs["sensitive"] is True
    assert account["email"] not in upload_command
    assert account["token"] not in upload_command
    uploaded_argv = shlex.split(upload_command)
    uploaded_script = base64.b64decode(
        uploaded_argv[uploaded_argv.index("%s") + 1]
    ).decode()
    login_argv = shlex.split(uploaded_script[uploaded_script.index("uv run "):])
    assert _flag_value(login_argv, "--email") == account["email"]
    assert _flag_value(login_argv, "--token") == account["token"]
    assert _flag_value(login_argv, "--login-method") == "onet"


async def test_provisioner_login_rejects_empty_token_before_ssh(
    db_factory, session_factory,
):
    wid = await _insert_worker(session_factory, status="creating")
    prov = WorkerProvisioner(db_factory=db_factory, cloud=FakeCloud(), broadcaster=None)
    ssh = AsyncMock()
    with pytest.raises(BootstrapError, match="缺少 token"):
        await prov._step_account_login(
            ssh,
            wid,
            [{"email": "legacy@example.com", "token": "", "login_method": "onet"}],
        )
    ssh.run.assert_not_awaited()


async def test_provisioner_login_rejects_case_insensitive_duplicate_identity(
    db_factory, session_factory,
):
    worker_id = await _insert_worker(
        session_factory,
        status="creating",
        accounts=[],
    )
    provisioner = WorkerProvisioner(db_factory, cloud=FakeCloud())
    provisioner.ensure_codex_account = AsyncMock()
    ssh = AsyncMock()

    with pytest.raises(BootstrapError, match="重复的 Worker 账号"):
        await provisioner._step_account_login(
            ssh,
            worker_id,
            [
                {
                    "email": "Duplicate@Example.com",
                    "provider": "codex",
                    "token": "first-mail-token",
                },
                {
                    "email": "duplicate@example.com",
                    "provider": "CODEX",
                    "token": "second-mail-token",
                },
            ],
        )

    provisioner.ensure_codex_account.assert_not_awaited()
    ssh.run.assert_not_awaited()
    async with session_factory() as db:
        assert (await db.get(Worker, worker_id)).accounts == []


async def test_provisioner_login_rejects_codex_without_mailbox_token(
    db_factory, session_factory,
):
    worker_id = await _insert_worker(
        session_factory,
        status="creating",
        accounts=[],
    )
    provisioner = WorkerProvisioner(db_factory, cloud=FakeCloud())
    provisioner.ensure_codex_account = AsyncMock()
    ssh = AsyncMock()

    with pytest.raises(BootstrapError, match="缺少邮箱 token"):
        await provisioner._step_account_login(
            ssh,
            worker_id,
            [{
                "email": "password-only@example.com",
                "provider": "codex",
                "token": "",
                "password": "openai-password",
                "login_method": "mailcatcher",
            }],
        )

    provisioner.ensure_codex_account.assert_not_awaited()
    ssh.run.assert_not_awaited()
    async with session_factory() as db:
        assert (await db.get(Worker, worker_id)).accounts == []


async def test_bootstrap_starts_worker_service_before_codex_login(
    db_factory, session_factory,
):
    wid = await _insert_worker(
        session_factory,
        status="creating",
        private_ip="10.0.0.9",
        auth_token="worker-token",
    )
    prov = WorkerProvisioner(db_factory=db_factory, cloud=FakeCloud(), broadcaster=None)
    ssh = AsyncMock()
    prov._ssh = Mock(return_value=ssh)
    order = []

    def step(name):
        async def run(*_args, **_kwargs):
            order.append(name)
        return AsyncMock(side_effect=run)

    prov._step_ssh_wait = step("ssh-wait")
    prov._step_system_init = step("system-init")
    prov._step_ccm_deploy = step("ccm-deploy")
    prov._step_ccm_config = step("ccm-config")
    prov._step_docker_sandbox = step("docker-sandbox")
    prov._step_ccm_service = step("ccm-service")
    prov._step_health_check = step("health-check")
    prov._step_account_login = step("account-login")
    prov._step_claude_warmup = step("claude-warmup")

    await prov._bootstrap(wid, [{
        "provider": "codex",
        "email": "codex@example.com",
        "password": "secret",
    }])

    assert order.index("ccm-service") < order.index("health-check")
    assert order.index("health-check") < order.index("account-login")
    assert "claude-warmup" not in order


async def test_provisioner_create_happy_path(db_factory, session_factory):
    wid = await _insert_worker(session_factory, status="creating")
    cloud = FakeCloud()
    prov = WorkerProvisioner(db_factory=db_factory, cloud=cloud, broadcaster=None)
    prov.preflight_ssh_key = Mock(return_value=SSHKeyMaterial(
        private_key_path="/tmp/test-worker-key",
        openssh_public_key="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAITestWorkerKeyMaterial",
    ))
    prov._bootstrap = AsyncMock()

    await prov.create_worker(wid, accounts=[])

    async with session_factory() as db:
        w = await db.get(Worker, wid)
    assert w.status == "ready"
    assert w.cloud_instance_id == "i-new123"
    assert w.private_ip == "10.0.0.9"
    assert w.last_heartbeat is not None
    assert cloud.last_overrides["ssh_user"] == w.ssh_user
    assert cloud.last_overrides["ccm_port"] == w.ccm_port
    assert cloud.last_overrides["ssh_public_key"].startswith("ssh-ed25519 ")
    assert cloud.last_overrides["client_token"].startswith("ccm-")


async def test_provisioner_reuses_ec2_client_token_after_lost_create_response(
    db_factory, session_factory,
):
    class LostResponseCloud(FakeCloud):
        def __init__(self):
            super().__init__()
            self.tokens = []

        async def create_instance(self, name, overrides=None):
            self.tokens.append(overrides["client_token"])
            if len(self.tokens) == 1:
                raise TimeoutError("run_instances response lost")
            return await super().create_instance(name, overrides)

    wid = await _insert_worker(session_factory, status="creating")
    cloud = LostResponseCloud()
    provisioner = WorkerProvisioner(db_factory, cloud=cloud)
    provisioner.preflight_ssh_key = Mock(return_value=SSHKeyMaterial(
        private_key_path="/tmp/test-worker-key",
        openssh_public_key="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAITestWorkerKeyMaterial",
    ))
    provisioner._bootstrap = AsyncMock()

    await provisioner.create_worker(wid, accounts=[])
    await provisioner.create_worker(wid, accounts=[])

    assert len(cloud.tokens) == 2
    assert cloud.tokens[0] == cloud.tokens[1]
    async with session_factory() as db:
        worker = await db.get(Worker, wid)
    assert worker.status == "ready"


async def test_lost_create_response_freezes_spec_and_blocks_rename_until_retry_claims_instance(
    client, db_factory, session_factory, monkeypatch,
):
    from backend.config import settings

    class FrozenRequestCloud(FakeCloud):
        def __init__(self):
            super().__init__()
            self.create_requests = []

        async def create_instance(self, name, overrides=None):
            self.create_requests.append((name, copy.deepcopy(overrides)))
            if len(self.create_requests) == 1:
                raise TimeoutError("run_instances response lost")
            return "i-frozen-request"

    monkeypatch.setattr(settings, "worker_instance_type", "m7i.large")
    monkeypatch.setattr(settings, "worker_image_id", "ami-frozen")
    monkeypatch.setattr(settings, "worker_subnet_id", "subnet-frozen")
    monkeypatch.setattr(
        settings,
        "worker_security_group_ids",
        "sg-frozen-a,sg-frozen-b",
    )
    monkeypatch.setattr(settings, "worker_key_name", "key-frozen")
    worker_id = await _insert_worker(session_factory, status="creating")
    cloud = FrozenRequestCloud()
    provisioner = WorkerProvisioner(db_factory, cloud=cloud)
    provisioner.preflight_ssh_key = Mock(return_value=SSHKeyMaterial(
        private_key_path="/tmp/test-worker-key",
        openssh_public_key="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAITestWorkerKeyMaterial",
    ))
    provisioner._bootstrap = AsyncMock()

    await provisioner.create_worker(worker_id, accounts=[])

    async with session_factory() as db:
        after_lost_response = await db.get(Worker, worker_id)
        frozen_spec = copy.deepcopy(after_lost_response.provision_spec)
    assert after_lost_response.status == "error"
    assert after_lost_response.cloud_instance_id is None
    assert frozen_spec == {
        "version": 1,
        "name": "test-worker",
        "has_fixed_overrides": True,
        "overrides": {
            "instance_type": "m7i.large",
            "image_id": "ami-frozen",
            "subnet_id": "subnet-frozen",
            "security_group_ids": ["sg-frozen-a", "sg-frozen-b"],
            "key_name": "key-frozen",
            "ssh_public_key": "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAITestWorkerKeyMaterial",
            "ssh_user": after_lost_response.ssh_user,
            "ccm_port": after_lost_response.ccm_port,
        },
    }

    rename = await client.patch(
        f"/api/workers/{worker_id}/rename",
        json={"name": "api-rename-must-be-blocked"},
    )
    assert rename.status_code == 409

    # Simulate out-of-band DB/config drift between the lost response and retry.
    # The retry must still send the exact journaled semantic request.
    async with session_factory() as db:
        current = await db.get(Worker, worker_id)
        current.name = "out-of-band-name"
        await db.commit()
    monkeypatch.setattr(settings, "worker_instance_type", "c7g.4xlarge")
    monkeypatch.setattr(settings, "worker_image_id", "ami-changed")
    monkeypatch.setattr(settings, "worker_subnet_id", "subnet-changed")
    monkeypatch.setattr(settings, "worker_security_group_ids", "sg-changed")
    monkeypatch.setattr(settings, "worker_key_name", "key-changed")

    await provisioner.create_worker(worker_id, accounts=[])

    assert len(cloud.create_requests) == 2
    assert cloud.create_requests[0] == cloud.create_requests[1]
    frozen_name, frozen_overrides = cloud.create_requests[0]
    assert frozen_name == "test-worker"
    assert frozen_overrides["client_token"].startswith("ccm-")
    async with session_factory() as db:
        retried = await db.get(Worker, worker_id)
    assert retried.status == "ready"
    assert retried.cloud_instance_id == "i-frozen-request"
    assert retried.name == "out-of-band-name"
    assert retried.provision_spec == frozen_spec


async def test_provisioner_rotates_replacement_client_token_then_reuses_it_after_lost_response(
    db_factory, session_factory,
):
    class ReplacementLostResponseCloud(FakeCloud):
        def __init__(self):
            super().__init__()
            self.tokens = []
            self.initial_describes = 0

        async def create_instance(self, name, overrides=None):
            self.tokens.append(overrides["client_token"])
            if len(self.tokens) == 1:
                return "i-initial"
            if len(self.tokens) == 2:
                raise TimeoutError("replacement run_instances response lost")
            return "i-replacement"

        async def describe_instance(self, iid):
            if iid == "i-initial":
                self.initial_describes += 1
                state = "running" if self.initial_describes == 1 else "terminated"
                return {
                    "instance_id": iid,
                    "state": state,
                    "private_ip": "10.0.0.9",
                    "public_ip": None,
                    "name": "initial",
                }
            return {
                "instance_id": iid,
                "state": "running",
                "private_ip": "10.0.0.10",
                "public_ip": None,
                "name": "replacement",
            }

    worker_id = await _insert_worker(session_factory, status="creating")
    cloud = ReplacementLostResponseCloud()
    provisioner = WorkerProvisioner(db_factory, cloud=cloud)
    provisioner.preflight_ssh_key = Mock(return_value=SSHKeyMaterial(
        private_key_path="/tmp/test-worker-key",
        openssh_public_key="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAITestWorkerKeyMaterial",
    ))
    provisioner._bootstrap = AsyncMock()

    await provisioner.create_worker(worker_id, accounts=[])
    await provisioner.create_worker(worker_id, accounts=[])
    async with session_factory() as db:
        after_lost_response = await db.get(Worker, worker_id)
        assert after_lost_response.status == "error"
        assert after_lost_response.cloud_instance_id is None

    await provisioner.create_worker(worker_id, accounts=[])

    assert len(cloud.tokens) == 3
    assert cloud.tokens[0] != cloud.tokens[1]
    assert cloud.tokens[1] == cloud.tokens[2]
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert worker.status == "ready"
    assert worker.cloud_instance_id == "i-replacement"


async def test_provisioner_bad_key_fails_before_cloud_create(
    db_factory, session_factory,
):
    wid = await _insert_worker(session_factory, status="creating")
    cloud = FakeCloud()
    prov = WorkerProvisioner(db_factory=db_factory, cloud=cloud, broadcaster=None)
    prov.preflight_ssh_key = Mock(side_effect=SSHKeyPreflightError(
        "key_not_found", "SSH private key file does not exist",
    ))

    await prov.create_worker(wid, accounts=[])

    async with session_factory() as db:
        worker = await db.get(Worker, wid)
    assert worker.status == "error"
    assert worker.bootstrap_step == "provision-config"
    assert "key_not_found" in worker.bootstrap_error
    assert not any(call[0] == "create" for call in cloud.calls)


async def test_provisioner_bootstrap_failure_records_step(db_factory, session_factory):
    wid = await _insert_worker(session_factory, status="creating")
    prov = WorkerProvisioner(db_factory=db_factory, cloud=FakeCloud(), broadcaster=None)
    prov.preflight_ssh_key = Mock(return_value=SSHKeyMaterial(
        private_key_path="/tmp/test-worker-key",
        openssh_public_key="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAITestWorkerKeyMaterial",
    ))
    prov._bootstrap = AsyncMock(side_effect=BootstrapError("ccm-deploy", "rsync failed"))

    await prov.create_worker(wid, accounts=[])

    async with session_factory() as db:
        w = await db.get(Worker, wid)
    assert w.status == "error"
    assert w.bootstrap_step == "ccm-deploy"
    assert "rsync failed" in w.bootstrap_error
    assert "FAILED" in w.bootstrap_log


async def test_provisioner_stop_and_start(db_factory, session_factory):
    wid = await _insert_worker(
        session_factory, status="ready", cloud_instance_id="i-x", private_ip="10.0.0.9"
    )
    cloud = FakeCloud()
    prov = WorkerProvisioner(db_factory=db_factory, cloud=cloud, broadcaster=None)
    prov._ssh = lambda w: AsyncMock()  # 跳过真实 SSH

    await prov.stop_worker(wid)
    async with session_factory() as db:
        assert (await db.get(Worker, wid)).status == "stopped"
    assert ("stop", "i-x") in cloud.calls

    prov._step_ssh_wait = AsyncMock()
    prov._step_health_check = AsyncMock()
    await prov.start_worker(wid)
    async with session_factory() as db:
        w = await db.get(Worker, wid)
    assert w.status == "ready"
    assert ("start", "i-x") in cloud.calls


async def test_stop_worker_relay_failure_still_stops_cloud_instance(
    db_factory, session_factory,
):
    worker_id = await _insert_worker(
        session_factory,
        status="ready",
        cloud_instance_id="i-relay-failure",
        private_ip="10.0.0.9",
    )
    cloud = FakeCloud()
    relay = AsyncMock()
    relay.stop_worker.side_effect = RuntimeError("relay unavailable")
    provisioner = WorkerProvisioner(
        db_factory,
        cloud=cloud,
        relay=relay,
    )
    ssh = AsyncMock()
    provisioner._ssh = lambda _worker: ssh

    await provisioner.stop_worker(worker_id)

    relay.stop_worker.assert_awaited_once_with(worker_id)
    assert ("stop", "i-relay-failure") in cloud.calls
    async with session_factory() as db:
        assert (await db.get(Worker, worker_id)).status == "stopped"


async def test_start_worker_stays_starting_until_account_check_finishes(
    db_factory, session_factory,
):
    worker_id = await _insert_worker(
        session_factory,
        status="stopped",
        cloud_instance_id="i-start-gate",
        private_ip="10.0.0.9",
    )
    provisioner = WorkerProvisioner(db_factory, cloud=FakeCloud())
    provisioner._ssh = lambda _worker: AsyncMock()
    provisioner._step_ssh_wait = AsyncMock()
    provisioner._step_health_check = AsyncMock()
    account_check_entered = asyncio.Event()
    release_account_check = asyncio.Event()

    async def blocked_account_check(_worker):
        account_check_entered.set()
        await release_account_check.wait()

    provisioner._check_pool_accounts = AsyncMock(side_effect=blocked_account_check)
    start_task = asyncio.create_task(provisioner.start_worker(worker_id))
    try:
        await asyncio.wait_for(account_check_entered.wait(), timeout=1)
        async with session_factory() as db:
            assert (await db.get(Worker, worker_id)).status == "starting"
    finally:
        release_account_check.set()
        await start_task

    async with session_factory() as db:
        assert (await db.get(Worker, worker_id)).status == "ready"
    provisioner._check_pool_accounts.assert_awaited_once()


async def test_start_worker_codex_auth_failure_stays_nonrecoverable_error(
    db_factory, session_factory,
):
    worker_id = await _insert_worker(
        session_factory,
        status="stopped",
        cloud_instance_id="i-codex",
        private_ip="10.0.0.9",
        accounts=[{
            "email": "codex@example.com",
            "provider": "codex",
            "token": "mail-token",
            "password": "openai-password",
            "account_id": "codex-1",
            "status": "logged_in",
        }],
    )
    provisioner = WorkerProvisioner(db_factory, cloud=FakeCloud())
    provisioner._ssh = lambda _worker: AsyncMock()
    provisioner._step_ssh_wait = AsyncMock()
    provisioner._step_health_check = AsyncMock()
    provisioner.ensure_codex_account = AsyncMock(
        side_effect=RuntimeError("refresh token revoked")
    )

    await provisioner.start_worker(worker_id)

    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert worker.status == "error"
    assert worker.bootstrap_step == "account-login"
    provisioner._probe_health = AsyncMock(return_value={"commit": "abc"})
    await provisioner._health_check_worker(worker, {}, AsyncMock())
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert worker.status == "error"
    assert worker.bootstrap_step == "account-login"


async def test_provisioner_destroy_created_terminates_and_scrubs_credentials(
    client, db_factory, session_factory,
):
    saved_accounts = [{
        "email": "codex@example.com",
        "provider": "codex",
        "status": "logged_in",
        "account_id": "codex-1",
        "token": "email-secret",
        "password": "openai-secret",
        "future_secret": "must-not-survive",
    }]
    wid = await _insert_worker(
        session_factory,
        status="ready",
        cloud_instance_id="i-x",
        auth_token="worker-auth-secret",
        accounts=saved_accounts,
    )
    cloud = FakeCloud()
    prov = WorkerProvisioner(db_factory=db_factory, cloud=cloud, broadcaster=None)
    await prov.destroy_worker(wid)

    assert ("terminate", "i-x") in cloud.calls
    async with session_factory() as db:
        worker = await db.get(Worker, wid)
    assert worker.status == "terminated"
    assert worker.auth_token is None
    assert worker.bootstrap_step is None
    assert worker.bootstrap_error is None
    assert worker.accounts == [{
        "email": "codex@example.com",
        "provider": "codex",
        "status": "logged_in",
        "account_id": "codex-1",
    }]

    # Direct audit lookup remains safely serializable, while normal listing
    # hides terminated rows.  auth_token is never part of WorkerResponse.
    response = await client.get(f"/api/workers/{wid}")
    assert response.status_code == 200
    assert "auth_token" not in response.json()
    assert response.json()["accounts"] == [{
        "email": "codex@example.com",
        "provider": "codex",
        "status": "logged_in",
    }]
    assert all(item["id"] != wid for item in (await client.get("/api/workers")).json())


async def test_provisioner_destroy_failure_stays_visible_and_retryable(
    client, db_factory, session_factory,
):
    accounts = [{
        "email": "codex@example.com",
        "provider": "codex",
        "token": "email-secret",
        "password": "openai-secret",
        "status": "logged_in",
    }]
    wid = await _insert_worker(
        session_factory,
        status="ready",
        cloud_instance_id="i-x",
        auth_token="worker-auth-secret",
        accounts=accounts,
    )
    cloud = FakeCloud()
    cloud.terminate_instance = AsyncMock(side_effect=RuntimeError("AWS unavailable"))
    prov = WorkerProvisioner(db_factory=db_factory, cloud=cloud, broadcaster=None)

    await prov.destroy_worker(wid)

    async with session_factory() as db:
        worker = await db.get(Worker, wid)
    assert worker.status == "error"
    assert worker.bootstrap_step == "destroy"
    assert "AWS unavailable" in worker.bootstrap_error
    assert worker.auth_token == "worker-auth-secret"
    assert worker.accounts == accounts
    assert wid in [item["id"] for item in (await client.get("/api/workers")).json()]


async def test_health_check_marks_error_and_recovers(db_factory, session_factory, monkeypatch):
    wid = await _insert_worker(
        session_factory, status="ready", private_ip="10.0.0.9", auth_token="t"
    )
    prov = WorkerProvisioner(db_factory=db_factory, cloud=FakeCloud(), broadcaster=None)

    class FailClient:
        def __init__(self, *a, **k): ...
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def get(self, *a, **k): raise ConnectionError("down")

    import backend.services.worker_provisioner as wp
    monkeypatch.setattr(wp.httpx, "AsyncClient", FailClient)
    fail_counts: dict[int, int] = {}
    for _ in range(3):
        await prov._health_check_once(fail_counts)
    async with session_factory() as db:
        w = await db.get(Worker, wid)
    assert w.status == "error"

    class OkResp:
        status_code = 200
        def raise_for_status(self): ...
        def json(self): return {"status": "ok", "commit": "abc123"}

    class OkClient(FailClient):
        async def get(self, *a, **k): return OkResp()

    monkeypatch.setattr(wp.httpx, "AsyncClient", OkClient)
    await prov._health_check_once(fail_counts)
    async with session_factory() as db:
        w = await db.get(Worker, wid)
    assert w.status == "ready"
    assert w.ccm_commit == "abc123"
    assert w.bootstrap_error is None


async def test_stale_error_health_success_does_not_overwrite_starting(
    db_factory, session_factory,
):
    worker_id = await _insert_worker(
        session_factory,
        status="error",
        private_ip="10.0.0.9",
        auth_token="worker-token",
        bootstrap_step=None,
        bootstrap_error="temporarily unhealthy",
    )
    async with session_factory() as db:
        stale_error_snapshot = await db.get(Worker, worker_id)
    async with session_factory() as db:
        current = await db.get(Worker, worker_id)
        current.status = "starting"
        await db.commit()

    provisioner = WorkerProvisioner(db_factory, cloud=FakeCloud())
    provisioner._probe_health = AsyncMock(return_value={"commit": "stale-commit"})
    provisioner._broadcast = AsyncMock()

    await provisioner._health_check_worker(
        stale_error_snapshot,
        {},
        AsyncMock(),
    )

    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert worker.status == "starting"
    assert worker.bootstrap_error == "temporarily unhealthy"
    assert worker.ccm_commit is None
    provisioner._broadcast.assert_not_awaited()


@pytest.mark.parametrize("transition_status", ["stopping", "destroying"])
async def test_stale_ready_health_failure_does_not_degrade_lifecycle_transition(
    db_factory, session_factory, transition_status,
):
    worker_id = await _insert_worker(
        session_factory,
        status="ready",
        private_ip="10.0.0.9",
        auth_token="worker-token",
    )
    async with session_factory() as db:
        stale_ready_snapshot = await db.get(Worker, worker_id)
    async with session_factory() as db:
        current = await db.get(Worker, worker_id)
        current.status = transition_status
        await db.commit()

    provisioner = WorkerProvisioner(db_factory, cloud=FakeCloud())
    provisioner._probe_health = AsyncMock(side_effect=ConnectionError("stale probe failed"))
    provisioner._broadcast = AsyncMock()
    fail_counts = {worker_id: 2}

    await provisioner._health_check_worker(
        stale_ready_snapshot,
        fail_counts,
        AsyncMock(),
    )

    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert worker.status == transition_status
    assert worker.bootstrap_error is None
    assert worker_id not in fail_counts
    provisioner._broadcast.assert_not_awaited()


async def test_stop_worker_without_instance_goes_stopped(db_factory, session_factory):
    """bootstrap 在开机前失败的 worker：stop 不应卡死在 stopping。"""
    wid = await _insert_worker(session_factory, status="error", cloud_instance_id=None)
    prov = WorkerProvisioner(db_factory=db_factory, cloud=FakeCloud(), broadcaster=None)
    await prov.stop_worker(wid)
    async with session_factory() as db:
        assert (await db.get(Worker, wid)).status == "stopped"


async def test_stop_worker_failure_goes_error_not_stuck(db_factory, session_factory):
    wid = await _insert_worker(session_factory, status="ready", cloud_instance_id="i-x")
    cloud = FakeCloud()

    async def boom(iid):
        raise RuntimeError("ec2 down")

    cloud.stop_instance = boom
    prov = WorkerProvisioner(db_factory=db_factory, cloud=cloud, broadcaster=None)
    prov._ssh = lambda w: AsyncMock()
    await prov.stop_worker(wid)
    async with session_factory() as db:
        w = await db.get(Worker, wid)
    assert w.status == "error"
    assert "关机失败" in w.bootstrap_error


async def test_health_check_does_not_whitewash_bootstrap_error(db_factory, session_factory, monkeypatch):
    """bootstrap 失败（step 非 None）的 error 不能因服务恰好活着被自动洗白。"""
    wid = await _insert_worker(
        session_factory, status="error", private_ip="10.0.0.9", auth_token="t",
        bootstrap_step="account-login", bootstrap_error="全部账号登录失败",
    )
    prov = WorkerProvisioner(db_factory=db_factory, cloud=FakeCloud(), broadcaster=None)

    class OkResp:
        status_code = 200
        def raise_for_status(self): ...
        def json(self): return {"status": "ok", "commit": "abc"}

    class OkClient:
        def __init__(self, *a, **k): ...
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def get(self, *a, **k): return OkResp()

    import backend.services.worker_provisioner as wp
    monkeypatch.setattr(wp.httpx, "AsyncClient", OkClient)
    await prov._health_check_once({})
    async with session_factory() as db:
        w = await db.get(Worker, wid)
    assert w.status == "error"  # 不自动恢复
    assert w.bootstrap_error == "全部账号登录失败"


async def test_stop_endpoint_sets_transitional_status_sync(client, session_factory, fake_provisioner):
    """双击防护：第一发同步置 stopping，第二发 409。"""
    wid = await _insert_worker(session_factory, status="ready")
    r1 = await client.post(f"/api/workers/{wid}/stop")
    assert r1.status_code == 200
    assert r1.json()["status"] == "stopping"
    r2 = await client.post(f"/api/workers/{wid}/stop")
    assert r2.status_code == 409


@pytest.mark.parametrize(
    ("initial_status", "action", "provisioner_method"),
    [
        ("ready", "stop", "stop_worker"),
        ("stopped", "start", "start_worker"),
        ("ready", "destroy", "destroy_worker"),
        ("error", "retry", "create_worker"),
    ],
)
async def test_worker_lifecycle_transition_compare_and_set_spawns_once(
    client,
    session_factory,
    fake_provisioner,
    monkeypatch,
    initial_status,
    action,
    provisioner_method,
):
    import backend.api.workers as workers_api

    if action == "destroy":
        async def _simple_destroy(prov, worker_id, db_factory_arg=None):
            await prov.destroy_worker(worker_id)

        monkeypatch.setattr(
            workers_api, "_migrate_back_then_destroy", _simple_destroy,
        )

    wid = await _insert_worker(session_factory, status=initial_status, accounts=[])

    responses = await asyncio.gather(
        client.post(f"/api/workers/{wid}/{action}"),
        client.post(f"/api/workers/{wid}/{action}"),
    )

    assert sorted(response.status_code for response in responses) == [200, 409]
    for _ in range(20):
        await asyncio.sleep(0)
    method = getattr(fake_provisioner, provisioner_method)
    if action == "retry":
        method.assert_awaited_once_with(wid, accounts=[])
    else:
        method.assert_awaited_once_with(wid)


async def test_git_head_commit_deploy_file_fallback(tmp_path):
    """rsync 部署不带 .git：git_head_commit 回退读 .deploy_commit。"""
    from backend.services.git_info import git_head_commit
    (tmp_path / ".deploy_commit").write_text("abc123def\n")
    assert git_head_commit(str(tmp_path)) == "abc123def"
    # 既无 git 也无文件 → ""
    assert git_head_commit(str(tmp_path / "nonexistent")) == ""
