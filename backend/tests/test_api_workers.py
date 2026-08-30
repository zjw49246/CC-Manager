"""Tests for Worker management API + provisioner state machine."""
import asyncio
import base64
import copy
import logging
import secrets
import shlex
import socket
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, call

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

import backend.main as main_module
from backend.config import settings
from backend.api.workers import (
    _build_add_account_command,
    _persist_worker_account_state,
    _remove_persisted_worker_account,
)
from backend.models.worker import Worker
from backend.models.task import Task
from backend.models.plan import Plan, PlanVersion
from backend.models.plan_agent import (
    PlanAgentRun,
    PlanAgentStep,
    PlanAgentWorkerDispatchReceipt,
)
from backend.schemas.plan import default_plan_pipeline_config
from backend.services.plan_runtime_receipt import new_prepared_runtime_receipt
from backend.services.worker_provisioner import (
    BootstrapError,
    CLAUDE_LOGIN_IDENTITY_KEY,
    WorkerProvisioner,
    _build_account_login_script,
    _build_script_upload_command,
    build_worker_rename_tag_outbox,
    worker_claude_login_identity,
    worker_create_client_token,
    worker_create_client_token_digest,
)
from backend.services.worker_drain_proof import (
    worker_node_drain_proof_signature,
)
from backend.services.worker_relay import (
    LEGACY_PLAN_CARRIER_CONFLICT_METADATA_KEY,
    WORKER_TERMINATION_UNCERTAINTY_METADATA_KEY,
)
from backend.services.worker_proxy import (
    WorkerProxy,
    build_worker_destroy_termination_receipt,
    capture_worker_destroy_lifecycle_claim,
    worker_destroy_provision_spec_digest,
)
from backend.services.ssh_executor import SSHExecutor, SSHKeyMaterial
from backend.services.ssh_executor import SSHKeyPreflightError


pytestmark = pytest.mark.usefixtures("worker_control_plane_auth")


FAKE_CLOUD_SCOPE = {
    "provider": "aws",
    "partition": "aws",
    "account_id": "123456789012",
    "region": "us-east-1",
}


# === Fixtures ===


@pytest.fixture(scope="module", autouse=True)
def assert_worker_api_auth_isolation():
    """Worker API fixture teardown must preserve the entering auth state."""

    original_token = settings.auth_token
    yield
    assert settings.auth_token == original_token


@pytest.fixture
def fake_provisioner(monkeypatch, session_factory):
    prov = AsyncMock()
    prov.cloud = AsyncMock()
    prov.cloud.self_describe.return_value = {"name": "test-manager"}
    prov.cloud.termination_scope.return_value = dict(FAKE_CLOUD_SCOPE)
    prov.preflight_ssh_key = Mock(return_value=None)
    prov._current_cloud_scope.return_value = dict(FAKE_CLOUD_SCOPE)

    async def require_cloud_identity(worker, *, verify_private_ip=False, **_kwargs):
        if not worker.cloud_instance_id or not worker.private_ip or not worker.auth_token:
            raise RuntimeError("test Worker lacks exact cloud identity")
        spec = worker.provision_spec or {
            "version": 1,
            "name": worker.name,
            "has_fixed_overrides": False,
            "overrides": {},
            "cloud_scope": FAKE_CLOUD_SCOPE,
            "client_token_digest": worker_create_client_token_digest(
                worker.id,
                worker.auth_token,
            ),
        }
        async with session_factory() as db:
            current = await db.get(Worker, worker.id)
            current.provision_spec = copy.deepcopy(spec)
            await db.commit()
            await db.refresh(current)
            worker = current
        return {
            "worker": worker,
            "cloud_scope": dict(FAKE_CLOUD_SCOPE),
            "client_token": worker_create_client_token(
                worker.id,
                worker.auth_token,
            ),
            "client_token_digest": worker_create_client_token_digest(
                worker.id,
                worker.auth_token,
            ),
            "provision_spec_digest": worker_destroy_provision_spec_digest(
                worker.provision_spec
            ),
            "instance_info": {
                "instance_id": worker.cloud_instance_id,
                "private_ip": worker.private_ip,
            } if verify_private_ip else None,
        }

    prov.require_worker_cloud_identity.side_effect = require_cloud_identity

    async def reconcile_rename(worker_id, *, expected_operation_id=None):
        async with session_factory() as db:
            current = await db.get(Worker, worker_id)
            receipt = current.rename_tag_outbox
            if receipt is None:
                return current
            if (
                expected_operation_id is not None
                and receipt.get("operation_id") != expected_operation_id
            ):
                raise RuntimeError("rename operation changed")
            await prov.cloud.update_instance_tags(
                current.cloud_instance_id,
                {"Name": receipt["desired_name"]},
            )
            current.rename_tag_outbox = None
            await db.commit()
            await db.refresh(current)
            db.expunge(current)
            return current

    prov.reconcile_worker_rename_tag_outbox.side_effect = reconcile_rename
    monkeypatch.setattr(main_module, "worker_provisioner", prov)
    return prov


@pytest.fixture(autouse=True)
def clean_remote_worker_drain_proof(monkeypatch):
    """Existing Manager destroy tests model a fully drained remote Worker."""

    async def _clean(_self, _claim):
        payload = {
            "protocol_version": 3,
            "nonce": "0" * 32,
            "node_role": "worker",
            "drain_claim": _claim.node_drain_claim,
            "runtime_sealed": True,
            "safe_to_destroy": True,
            "blockers": [],
            "blocker_count": 0,
            "task_count": 0,
        }
        return {
            **payload,
            "signature": worker_node_drain_proof_signature(
                payload,
                auth_token=_claim.auth_token,
            ),
        }

    async def _begin(_self, claim):
        return {
            "protocol_version": 3,
            "node_role": "worker",
            "drain_claim": claim.node_drain_claim,
            "draining": True,
        }

    async def _seal(_self, claim):
        return {
            "protocol_version": 3,
            "node_role": "worker",
            "drain_claim": claim.node_drain_claim,
            "runtime_sealed": True,
            "safe_to_seal": True,
            "blockers": [],
            "blocker_count": 0,
        }

    monkeypatch.setattr(
        WorkerProxy,
        "require_claimed_destroy_drain_proof",
        _clean,
    )
    monkeypatch.setattr(
        WorkerProxy,
        "begin_claimed_destroy_drain",
        _begin,
    )
    monkeypatch.setattr(
        WorkerProxy,
        "seal_claimed_destroy_runtime",
        _seal,
    )

    async def _complete_log_backfill(_self, _claim, _task_ids):
        return None

    monkeypatch.setattr(
        WorkerProxy,
        "require_claimed_destroy_log_backfill",
        _complete_log_backfill,
    )


async def _insert_worker(session_factory, **fields) -> int:
    fields.setdefault("status", "ready")
    if (
        "destroy_lifecycle_nonce" not in fields
        and (
            fields.get("status") == "destroying"
            or fields.get("bootstrap_step") == "destroy"
        )
    ):
        fields["destroy_lifecycle_nonce"] = secrets.token_hex(16)
    async with session_factory() as db:
        worker = Worker(name="test-worker", **fields)
        db.add(worker)
        await db.commit()
        await db.refresh(worker)
        return worker.id


async def _set_worker_cloud_login_generation(
    session_factory,
    worker_id: int,
    *,
    instance_id: str,
    auth_token: str,
) -> dict:
    """Install one exact non-secret Worker identity and return its binding."""

    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
        worker.cloud_instance_id = instance_id
        worker.auth_token = auth_token
        worker.provision_spec = {
            "version": 1,
            "name": worker.name,
            "has_fixed_overrides": False,
            "overrides": {},
            "cloud_scope": FAKE_CLOUD_SCOPE,
            "client_token_digest": worker_create_client_token_digest(
                worker.id,
                auth_token,
            ),
        }
        await db.commit()
        await db.refresh(worker)
        return worker_claude_login_identity(worker)


async def _authorize_worker_cloud_termination(
    session_factory,
    worker_id: int,
) -> object:
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
        claim = capture_worker_destroy_lifecycle_claim(worker)
        if worker.provision_spec is None:
            worker.provision_spec = {
                "version": 1,
                "name": worker.name,
                "has_fixed_overrides": False,
                "overrides": {},
                "cloud_scope": FAKE_CLOUD_SCOPE,
                "client_token_digest": worker_create_client_token_digest(
                    worker.id,
                    worker.auth_token,
                ),
            }
        worker.destroy_termination_receipt = (
            build_worker_destroy_termination_receipt(
                claim,
                _clean_destroy_proof(claim),
                cloud_scope=FAKE_CLOUD_SCOPE,
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


def _clean_destroy_proof(claim, *, nonce: str = "f" * 32) -> dict:
    payload = {
        "protocol_version": 3,
        "nonce": nonce,
        "node_role": "worker",
        "drain_claim": claim.node_drain_claim,
        "runtime_sealed": True,
        "safe_to_destroy": True,
        "blockers": [],
        "blocker_count": 0,
        "task_count": 0,
    }
    return {
        **payload,
        "signature": worker_node_drain_proof_signature(
            payload,
            auth_token=claim.auth_token,
        ),
    }


class FakeCloud:
    """最小 CloudProvider 替身。"""

    def __init__(self, existing_instance_id: str | None = None):
        self.calls = []
        self.last_overrides = None
        self.existing_instance_id = existing_instance_id

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

    async def termination_scope(self):
        return dict(FAKE_CLOUD_SCOPE)

    async def find_instance_by_create_token(
        self,
        _client_token,
        *,
        include_terminated=False,
    ):
        return self.existing_instance_id

    async def wait_until_running(self, iid, timeout=300):
        self.calls.append(("wait", iid))
        return "10.0.0.9"

    async def stop_instance(self, iid):
        self.calls.append(("stop", iid))

    async def start_instance(self, iid):
        self.calls.append(("start", iid))

    async def terminate_instance(self, iid, *, allow_not_found=False):
        self.calls.append(("terminate", iid))


# === API tests ===


@pytest.mark.parametrize(
    ("method", "path", "payload"),
    [
        ("GET", "/api/workers", None),
        ("POST", "/api/workers", {"name": "blocked"}),
        ("GET", "/api/workers/1", None),
        ("GET", "/api/workers/1/logs", None),
        ("POST", "/api/workers/1/stop", None),
        ("POST", "/api/workers/1/start", None),
        ("POST", "/api/workers/1/destroy", None),
        ("POST", "/api/workers/1/retry", None),
        ("GET", "/api/workers/1/pool", None),
        ("POST", "/api/workers/1/pool/add", {}),
        ("GET", "/api/workers/1/pool/add/user%40example.com", None),
        ("POST", "/api/workers/1/pool/login-attempts/a/otp", {}),
        ("DELETE", "/api/workers/1/pool/login-attempts/a", None),
        ("DELETE", "/api/workers/1/pool/codex-1", None),
        ("GET", "/api/workers/1/pool/usage", None),
        ("GET", "/api/workers/1/settings/runtime", None),
        ("PUT", "/api/workers/1/settings/runtime", {}),
        ("PATCH", "/api/workers/1/rename", {"name": "blocked"}),
        ("PUT", "/api/workers/1/assign", {"owner_user_id": None}),
    ],
)
async def test_worker_control_plane_routes_fail_closed_without_auth_token(
    client,
    session_factory,
    fake_provisioner,
    monkeypatch,
    method,
    path,
    payload,
):
    monkeypatch.setattr(settings, "auth_token", "")

    response = await client.request(method, path, json=payload)

    assert response.status_code == 503, response.text
    assert response.json() == {
        "detail": (
            "Worker control plane requires CCM_NODE_ROLE=manager and a "
            "non-empty AUTH_TOKEN"
        )
    }
    async with session_factory() as db:
        assert (await db.execute(text("SELECT COUNT(*) FROM workers"))).scalar() == 0
    fake_provisioner.create_worker.assert_not_called()
    fake_provisioner.cloud.assert_not_called()


async def test_worker_control_plane_routes_fail_closed_on_worker_node(
    client,
    session_factory,
    fake_provisioner,
    monkeypatch,
):
    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    assert settings.auth_token

    response = await client.get("/api/workers")

    assert response.status_code == 503, response.text
    async with session_factory() as db:
        assert (await db.execute(text("SELECT COUNT(*) FROM workers"))).scalar() == 0
    fake_provisioner.create_worker.assert_not_called()
    fake_provisioner.cloud.assert_not_called()


async def test_worker_background_paths_fail_closed_without_auth_token(
    monkeypatch,
):
    monkeypatch.setattr(settings, "auth_token", "")
    db_factory = Mock(side_effect=AssertionError("must not open Worker DB"))
    cloud = AsyncMock()
    relay = AsyncMock()
    provisioner = WorkerProvisioner(db_factory, cloud=cloud, relay=relay)
    monkeypatch.setattr(main_module, "async_session", db_factory)
    monkeypatch.setattr(main_module, "worker_relay", relay)

    await main_module._recover_stale_worker_lifecycles()
    await main_module._recover_worker_relays()
    await provisioner.health_check_loop(interval=0)
    with pytest.raises(RuntimeError, match="AUTH_TOKEN"):
        await provisioner.create_worker(1)

    db_factory.assert_not_called()
    relay.assert_not_awaited()
    cloud.assert_not_awaited()


async def test_list_workers_empty(client):
    resp = await client.get("/api/workers")
    assert resp.status_code == 200
    assert resp.json() == []


async def test_startup_recovery_makes_interrupted_worker_lifecycles_retryable(
    session_factory, monkeypatch,
):
    cases = [
        ("creating", "provision", "provision"),
        ("bootstrapping", "account-login", "account-login"),
        ("starting", "health-check", "startup-recovery"),
        ("stopping", "ccm-service", "startup-recovery"),
        ("destroying", "cloud-terminate", "destroy"),
    ]
    worker_ids = []
    async with session_factory() as db:
        for status, bootstrap_step, _expected_step in cases:
            worker = Worker(
                name=f"stale-{status}",
                status=status,
                bootstrap_step=bootstrap_step,
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
    assert [worker.status for worker in recovered] == ["error"] * len(cases)
    for (previous_status, _previous_step, expected_step), worker in zip(
        cases,
        recovered,
    ):
        assert worker.cloud_instance_id == f"i-{previous_status}"
        assert worker.auth_token == "worker-secret"
        assert worker.accounts[0]["token"] == "mail-token"
        assert worker.bootstrap_step == expected_step


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


async def test_fresh_worker_lifecycle_clears_old_destroy_authority(
    session_factory,
):
    """A non-destroy retry cannot inherit an older cloud-effect outbox."""

    import backend.api.workers as workers_api

    worker_id = await _insert_worker(
        session_factory,
        status="destroying",
        cloud_instance_id="i-old-destroy-authority",
        private_ip="10.0.0.31",
        auth_token="old-destroy-secret",
    )
    await _authorize_worker_cloud_termination(session_factory, worker_id)
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
        worker.status = "error"
        worker.bootstrap_step = "startup-recovery"
        await db.commit()

    async with session_factory() as db:
        lifecycle_request = MagicMock()
        lifecycle_request.state.auth_type = "token"
        lifecycle_request.state.user_role = "super_admin"
        lifecycle_request.state.user_id = None
        await workers_api._transition_worker_status(
            db,
            lifecycle_request,
            worker_id,
            allowed_statuses=("error",),
            target_status="creating",
        )

    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert worker.status == "creating"
    assert worker.destroy_lifecycle_nonce is None
    assert worker.destroy_termination_receipt is None


async def test_worker_lifecycle_rejects_stale_cached_admin_role(
    session_factory,
):
    """A demotion that wins before the lifecycle CAS blocks cloud work."""

    from fastapi import HTTPException

    import backend.api.workers as workers_api
    from backend.models.user import User

    async with session_factory() as db:
        user = User(
            email="stale-worker-admin@example.com",
            name="stale worker admin",
            password_hash="test",
            role="member",
            is_active=True,
        )
        worker = Worker(name="role-fenced worker", status="ready")
        db.add_all((user, worker))
        await db.commit()
        await db.refresh(user)
        await db.refresh(worker)
        user_id = user.id
        worker_id = worker.id

    request = MagicMock()
    request.state.auth_type = "jwt"
    request.state.user_id = user_id
    request.state.user_role = "admin"
    async with session_factory() as db:
        with pytest.raises(HTTPException) as rejected:
            await workers_api._transition_worker_status(
                db,
                request,
                worker_id,
                allowed_statuses=("ready",),
                target_status="stopping",
            )
    assert rejected.value.status_code == 409
    assert "changed role" in str(rejected.value.detail)
    async with session_factory() as db:
        assert (await db.get(Worker, worker_id)).status == "ready"


async def test_worker_lifecycle_rejects_stale_member_ownership(
    session_factory,
):
    """A Worker ownership transfer is part of the lifecycle row CAS."""

    from fastapi import HTTPException

    import backend.api.workers as workers_api
    from backend.models.user import User

    async with session_factory() as db:
        old_owner = User(
            email="old-worker-owner@example.com",
            name="old worker owner",
            password_hash="test",
            role="member",
            is_active=True,
        )
        new_owner = User(
            email="new-worker-owner@example.com",
            name="new worker owner",
            password_hash="test",
            role="member",
            is_active=True,
        )
        db.add_all((old_owner, new_owner))
        await db.flush()
        worker = Worker(
            name="ownership-fenced worker",
            status="ready",
            owner_user_id=new_owner.id,
        )
        db.add(worker)
        await db.commit()
        await db.refresh(worker)
        old_owner_id = old_owner.id
        worker_id = worker.id

    request = MagicMock()
    request.state.auth_type = "jwt"
    request.state.user_id = old_owner_id
    request.state.user_role = "member"
    async with session_factory() as db:
        with pytest.raises(HTTPException) as rejected:
            await workers_api._transition_worker_status(
                db,
                request,
                worker_id,
                allowed_statuses=("ready",),
                target_status="stopping",
            )
    assert rejected.value.status_code == 409
    async with session_factory() as db:
        assert (await db.get(Worker, worker_id)).status == "ready"


async def test_worker_lifecycle_cas_rejects_bootstrap_failure_start(
    session_factory,
):
    """A stale route snapshot cannot start an error/account-login Worker."""

    from fastapi import HTTPException

    import backend.api.workers as workers_api

    worker_id = await _insert_worker(
        session_factory,
        status="error",
        bootstrap_step="account-login",
    )
    request = MagicMock()
    request.state.auth_type = "token"
    request.state.user_role = "super_admin"
    request.state.user_id = None

    async with session_factory() as db:
        with pytest.raises(HTTPException) as rejected:
            await workers_api._transition_worker_status(
                db,
                request,
                worker_id,
                allowed_statuses=("stopped", "error"),
                target_status="starting",
                require_bootstrap_step_none=True,
            )

    assert rejected.value.status_code == 409
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert worker.status == "error"
    assert worker.bootstrap_step == "account-login"


async def test_create_worker_rejects_stale_cached_admin_role(
    session_factory,
    fake_provisioner,
):
    """No durable provisioning job survives a concurrent admin demotion."""

    from fastapi import HTTPException
    from sqlalchemy import func, select

    import backend.api.workers as workers_api
    from backend.models.user import User
    from backend.schemas.worker import WorkerCreate

    async with session_factory() as db:
        user = User(
            email="stale-worker-creator@example.com",
            name="stale worker creator",
            password_hash="test",
            role="member",
            is_active=True,
        )
        db.add(user)
        await db.commit()
        await db.refresh(user)
        user_id = user.id

    request = MagicMock()
    request.state.auth_type = "jwt"
    request.state.user_id = user_id
    request.state.user_role = "admin"
    async with session_factory() as db:
        with pytest.raises(HTTPException) as rejected:
            await workers_api.create_worker(
                WorkerCreate(name="must not provision"),
                request,
                db,
            )
    assert rejected.value.status_code == 409
    async with session_factory() as db:
        assert await db.scalar(select(func.count(Worker.id))) == 0
    fake_provisioner.create_worker.assert_not_awaited()


async def test_real_destroying_restart_recovery_can_retry_destroy(
    client,
    session_factory,
    fake_provisioner,
    monkeypatch,
):
    """Exercise the durable pre-crash state through the public retry path."""

    import backend.api.workers as workers_api

    worker_id = await _insert_worker(
        session_factory,
        status="destroying",
        bootstrap_step=None,
        cloud_instance_id="i-real-interrupted-destroy",
        auth_token="destroy-recovery-secret",
    )
    monkeypatch.setattr(main_module, "async_session", session_factory)

    await main_module._recover_stale_worker_lifecycles()

    async with session_factory() as db:
        recovered = await db.get(Worker, worker_id)
    assert recovered.status == "error"
    assert recovered.bootstrap_step == "destroy"
    assert "Manager restarted while Worker was destroying" in (
        recovered.bootstrap_error or ""
    )

    coordinator = AsyncMock()
    scheduled = []

    def capture_spawn(coro):
        scheduled.append(coro)
        return Mock()

    monkeypatch.setattr(workers_api, "_migrate_back_then_destroy", coordinator)
    monkeypatch.setattr(workers_api, "_spawn", capture_spawn)

    response = await client.post(f"/api/workers/{worker_id}/destroy")

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "destroying"
    assert len(scheduled) == 1
    await scheduled[0]
    coordinator.assert_awaited_once()
    provisioner, claimed_worker_id, destroy_claim = coordinator.await_args.args
    assert provisioner is fake_provisioner
    assert claimed_worker_id == worker_id
    assert destroy_claim.worker_id == worker_id
    async with session_factory() as db:
        current = await db.get(Worker, worker_id)
    assert current.status == "destroying"


async def test_authorized_destroy_restart_skips_dead_worker_protocol(
    client,
    session_factory,
    fake_provisioner,
    monkeypatch,
):
    """A committed final-proof outbox resumes only the idempotent cloud call."""

    import backend.api.workers as workers_api

    worker_id = await _insert_worker(
        session_factory,
        status="destroying",
        cloud_instance_id="i-authorized-before-crash",
        private_ip="10.0.0.32",
        auth_token="authorized-recovery-secret",
    )
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
        pre_crash_claim = capture_worker_destroy_lifecycle_claim(worker)
    committed_receipt = (
            await workers_api._persist_worker_destroy_termination_authorization(
                session_factory,
                provisioner=fake_provisioner,
                destroy_claim=pre_crash_claim,
            proof=_clean_destroy_proof(pre_crash_claim),
        )
    )
    assert committed_receipt["destroy_lifecycle_nonce"] == (
        pre_crash_claim.destroy_lifecycle_nonce
    )
    monkeypatch.setattr(main_module, "async_session", session_factory)
    await main_module._recover_stale_worker_lifecycles()

    remote_calls = {
        name: AsyncMock(
            side_effect=AssertionError(
                f"authorized recovery must not call dead Worker {name}"
            )
        )
        for name in (
            "begin_claimed_destroy_drain",
            "seal_claimed_destroy_runtime",
            "require_claimed_destroy_log_backfill",
            "require_claimed_destroy_drain_proof",
        )
    }
    for name, method in remote_calls.items():
        monkeypatch.setattr(WorkerProxy, name, method)

    original_destroy = workers_api._migrate_back_then_destroy

    async def destroy_with_test_db(provisioner, destroying_worker_id, claim):
        await original_destroy(
            provisioner,
            destroying_worker_id,
            claim,
            db_factory=session_factory,
        )

    monkeypatch.setattr(
        workers_api,
        "_migrate_back_then_destroy",
        destroy_with_test_db,
    )

    scheduled = []

    def capture_spawn(coro):
        scheduled.append(coro)
        return Mock()

    monkeypatch.setattr(workers_api, "_spawn", capture_spawn)
    response = await client.post(f"/api/workers/{worker_id}/destroy")
    assert response.status_code == 200, response.text
    assert len(scheduled) == 1
    await scheduled[0]

    fake_provisioner.destroy_worker.assert_awaited_once()
    assert fake_provisioner.destroy_worker.await_args.args == (worker_id,)
    assert (
        fake_provisioner.destroy_worker.await_args.kwargs["destroy_claim"].worker_id
        == worker_id
    )
    for method in remote_calls.values():
        method.assert_not_awaited()
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert worker.destroy_termination_receipt is not None


async def test_malformed_destroy_authority_fails_closed_without_remote_or_cloud(
    client,
    session_factory,
    fake_provisioner,
    monkeypatch,
):
    """Corrupt restart authority must never authorize an irreversible call."""

    import backend.api.workers as workers_api

    worker_id = await _insert_worker(
        session_factory,
        status="error",
        bootstrap_step="destroy",
        cloud_instance_id="i-corrupt-destroy-authority",
        auth_token="corrupt-destroy-secret",
        destroy_termination_receipt={"version": 1},
    )
    remote_calls = {
        name: AsyncMock(
            side_effect=AssertionError(
                f"malformed authority must not call Worker {name}"
            )
        )
        for name in (
            "begin_claimed_destroy_drain",
            "seal_claimed_destroy_runtime",
            "require_claimed_destroy_log_backfill",
            "require_claimed_destroy_drain_proof",
        )
    }
    for name, method in remote_calls.items():
        monkeypatch.setattr(WorkerProxy, name, method)

    original_destroy = workers_api._migrate_back_then_destroy

    async def destroy_with_test_db(provisioner, destroying_worker_id, claim):
        await original_destroy(
            provisioner,
            destroying_worker_id,
            claim,
            db_factory=session_factory,
        )

    monkeypatch.setattr(
        workers_api,
        "_migrate_back_then_destroy",
        destroy_with_test_db,
    )
    scheduled = []

    def capture_spawn(coro):
        scheduled.append(coro)
        return Mock()

    monkeypatch.setattr(workers_api, "_spawn", capture_spawn)
    response = await client.post(f"/api/workers/{worker_id}/destroy")
    assert response.status_code == 200, response.text
    assert len(scheduled) == 1
    await scheduled[0]

    fake_provisioner.destroy_worker.assert_not_awaited()
    for method in remote_calls.values():
        method.assert_not_awaited()
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert worker.status == "ready"
    assert worker.bootstrap_step == "destroy"
    assert "authority" in (worker.bootstrap_error or "")
    assert worker.destroy_termination_receipt == {"version": 1}


async def _insert_worker_plan_graph(
    session_factory,
    *,
    worker_id: int,
    run_status: str = "completed",
    archived: bool,
    active: bool = False,
    dirty_runtime: bool = False,
    clean_runtime: bool = False,
    dirty_worker_dispatch: bool = False,
    worker_mirror_runtime: bool = False,
    historical_dispatch_reason: str | None = None,
) -> tuple[int, int]:
    async with session_factory() as db:
        run_generation = (
            1
            if historical_dispatch_reason is not None or run_status == "cancelling"
            else 0
        )
        plan = Plan(
            title="Worker lifecycle Plan",
            initial_request="Keep exact Worker ownership",
            worker_id=worker_id,
            priority=0,
            archived_at=datetime.utcnow() if archived else None,
            pipeline_config=default_plan_pipeline_config().model_dump(mode="json"),
        )
        db.add(plan)
        await db.flush()
        run = PlanAgentRun(
            plan_id=plan.id,
            worker_id=worker_id,
            run_type="initial",
            status=run_status,
            current_stage="complete" if run_status == "completed" else "planner",
            generation=run_generation,
            cancellation_target_generation=(0 if run_status == "cancelling" else None),
            pipeline_config=plan.pipeline_config,
            finished_at=(
                datetime.utcnow()
                if run_status in {"completed", "failed", "cancelled"}
                else None
            ),
        )
        db.add(run)
        await db.flush()
        if active:
            plan.active_run_id = run.id
        if dirty_runtime or clean_runtime:
            step = PlanAgentStep(
                run_id=run.id,
                plan_id=plan.id,
                step_type="planner",
                round=1,
                generation=run.generation,
                provider="claude",
                status="failed",
            )
            db.add(step)
            await db.flush()
            receipt = new_prepared_runtime_receipt(step, attempt_index=1)
            if dirty_runtime:
                receipt.status = "cleanup_failed"
                receipt.cleanup_error = "still owns a process"
            else:
                receipt.status = "cleaned"
                receipt.cleaned_at = datetime.utcnow()
            db.add(receipt)
        if worker_mirror_runtime:
            step = PlanAgentStep(
                run_id=run.id,
                plan_id=plan.id,
                worker_id=worker_id,
                worker_step_id=901,
                step_type="planner",
                round=1,
                generation=run.generation,
                provider="claude",
                status=run_status,
                finished_at=datetime.utcnow(),
            )
            db.add(step)
            await db.flush()
            if run_status == "completed":
                version = PlanVersion(
                    plan_id=plan.id,
                    worker_id=worker_id,
                    worker_version_id=902,
                    version_number=1,
                    produced_by_run_id=run.id,
                    produced_by_step_id=step.id,
                    content="complete Worker Plan result",
                )
                db.add(version)
                await db.flush()
                step.plan_version_id = version.id
                run.result_version_id = version.id
                run.draft_step_id = step.id
                run.draft_content = version.content
                plan.current_version_id = version.id
            db.add(
                PlanAgentWorkerDispatchReceipt(
                    plan_id=plan.id,
                    run_id=run.id,
                    target_task_id=plan.target_task_id,
                    worker_id=worker_id,
                    run_generation=run.generation,
                    protocol=1,
                    status="settled",
                    payload_digest="b" * 64,
                    remote_status=run_status,
                    settlement_reason="remote_pause",
                    settled_at=datetime.utcnow(),
                )
            )
        if historical_dispatch_reason is not None:
            db.add(
                PlanAgentWorkerDispatchReceipt(
                    plan_id=plan.id,
                    run_id=run.id,
                    target_task_id=plan.target_task_id,
                    worker_id=worker_id,
                    run_generation=run.generation - 1,
                    protocol=1,
                    status="settled",
                    payload_digest=(
                        "c" * 64
                        if historical_dispatch_reason == "remote_absent"
                        else None
                    ),
                    settlement_reason=historical_dispatch_reason,
                    settled_at=datetime.utcnow(),
                )
            )
        if dirty_worker_dispatch:
            db.add(PlanAgentWorkerDispatchReceipt(
                plan_id=plan.id,
                run_id=run.id,
                target_task_id=plan.target_task_id,
                worker_id=worker_id,
                run_generation=run.generation,
                protocol=1,
                status="remote_possible",
                payload_digest="a" * 64,
            ))
        await db.commit()
        return plan.id, run.id


@pytest.mark.asyncio
async def test_destroy_locks_user_before_runtime_and_receipt_aggregates(
    session_factory,
    monkeypatch,
):
    import backend.api.deps as deps
    import backend.api.workers as workers_api
    import backend.services.worker_task_termination as termination

    async with session_factory() as db:
        worker = Worker(name="destroy-lock-order", status="ready")
        db.add(worker)
        await db.flush()
        db.add(
            Task(
                title="destroy-lock-order-task",
                status="pending",
                worker_id=worker.id,
            )
        )
        await db.commit()
        worker_id = worker.id

    order: list[str] = []

    async def lock_user(request, db):
        order.append("user")

    async def plan_blockers(db, checked_worker_id):
        assert checked_worker_id == worker_id
        order.append("plan_aggregate")
        return [], []

    async def pr_blockers(db, checked_worker_id):
        assert checked_worker_id == worker_id
        order.append("pr_aggregate")
        return []

    async def active_receipt(db, task_id, *, for_update=False):
        assert for_update is True
        order.append("receipt")
        return None

    monkeypatch.setattr(deps, "lock_request_user_authority", lock_user)
    monkeypatch.setattr(
        workers_api,
        "_worker_plan_runtime_blockers",
        plan_blockers,
    )
    monkeypatch.setattr(
        workers_api,
        "_worker_pr_monitor_runtime_blockers",
        pr_blockers,
    )
    monkeypatch.setattr(
        termination,
        "active_worker_task_termination_receipt",
        active_receipt,
    )

    request = SimpleNamespace(
        state=SimpleNamespace(
            auth_type="deployment_token",
            user_role="super_admin",
            user_id=None,
        )
    )
    async with session_factory() as db:
        worker = await workers_api._transition_worker_status_locked(
            db,
            request,
            worker_id,
            allowed_statuses=("ready",),
            target_status="destroying",
            block_active_task_terminations=True,
            destroy_lifecycle_nonce="a" * 32,
        )

    assert worker.status == "destroying"
    assert order == ["user", "plan_aggregate", "pr_aggregate", "receipt"]


@pytest.mark.asyncio
async def test_destroy_allows_inactive_unarchived_clean_worker_plan(
    client,
    session_factory,
    fake_provisioner,
    monkeypatch,
):
    import backend.api.workers as workers_api

    worker_id = await _insert_worker(
        session_factory,
        status="ready",
        cloud_instance_id="i-plan-history",
    )
    await _insert_worker_plan_graph(
        session_factory,
        worker_id=worker_id,
        archived=False,
        worker_mirror_runtime=True,
    )
    coordinator = AsyncMock()
    scheduled = []

    def capture_spawn(coro):
        scheduled.append(coro)
        return Mock()

    monkeypatch.setattr(workers_api, "_migrate_back_then_destroy", coordinator)
    monkeypatch.setattr(workers_api, "_spawn", capture_spawn)

    response = await client.post(f"/api/workers/{worker_id}/destroy")

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "destroying"
    assert len(scheduled) == 1
    await scheduled[0]
    coordinator.assert_awaited_once()
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert worker.status == "destroying"


@pytest.mark.parametrize(
    "run_status",
    ["queued", "running", "waiting_user", "cancelling"],
)
async def test_destroy_rejects_active_worker_plan_runtime(
    client,
    session_factory,
    fake_provisioner,
    run_status,
):
    worker_id = await _insert_worker(
        session_factory,
        status="ready",
        cloud_instance_id=f"i-plan-{run_status}",
    )
    await _insert_worker_plan_graph(
        session_factory,
        worker_id=worker_id,
        run_status=run_status,
        archived=True,
        active=True,
    )

    response = await client.post(f"/api/workers/{worker_id}/destroy")

    assert response.status_code == 409
    assert run_status in response.json()["detail"]
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert worker.status == "ready"


async def test_destroy_rejects_dirty_terminal_worker_plan_runtime(
    client,
    session_factory,
    fake_provisioner,
):
    worker_id = await _insert_worker(
        session_factory,
        status="ready",
        cloud_instance_id="i-plan-dirty-terminal",
    )
    await _insert_worker_plan_graph(
        session_factory,
        worker_id=worker_id,
        archived=True,
        dirty_runtime=True,
    )

    response = await client.post(f"/api/workers/{worker_id}/destroy")

    assert response.status_code == 409
    assert "completed" in response.json()["detail"]
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert worker.status == "ready"


async def test_destroy_rejects_uncertain_worker_plan_dispatch_receipt(
    client,
    session_factory,
    fake_provisioner,
):
    worker_id = await _insert_worker(
        session_factory,
        status="ready",
        cloud_instance_id="i-plan-remote-possible",
    )
    await _insert_worker_plan_graph(
        session_factory,
        worker_id=worker_id,
        archived=True,
        dirty_worker_dispatch=True,
    )

    response = await client.post(f"/api/workers/{worker_id}/destroy")

    assert response.status_code == 409
    assert "completed" in response.json()["detail"]
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert worker.status == "ready"


@pytest.mark.parametrize("drift", ["rebound", "detached", "missing"])
async def test_destroy_finds_dispatch_receipt_by_frozen_worker_identity(
    client,
    session_factory,
    fake_provisioner,
    drift,
):
    worker_id = await _insert_worker(
        session_factory,
        status="ready",
        cloud_instance_id=f"i-plan-dispatch-{drift}",
    )
    replacement_worker_id = await _insert_worker(
        session_factory,
        status="ready",
        cloud_instance_id=f"i-plan-dispatch-replacement-{drift}",
    )
    plan_id, run_id = await _insert_worker_plan_graph(
        session_factory,
        worker_id=worker_id,
        archived=True,
        dirty_worker_dispatch=True,
    )
    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        run = await db.get(PlanAgentRun, run_id)
        assert plan is not None and run is not None
        if drift == "rebound":
            plan.worker_id = replacement_worker_id
            run.worker_id = replacement_worker_id
        elif drift == "detached":
            plan.worker_id = None
            run.worker_id = None
        else:
            plan.worker_id = None
            await db.delete(run)
        await db.commit()

    response = await client.post(f"/api/workers/{worker_id}/destroy")

    assert response.status_code == 409
    assert "dispatch:remote_possible" in response.json()["detail"]
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert worker is not None and worker.status == "ready"
    fake_provisioner.destroy_worker.assert_not_awaited()


async def test_destroy_rejects_forged_cleaned_worker_plan_runtime(
    client,
    session_factory,
    fake_provisioner,
):
    worker_id = await _insert_worker(
        session_factory,
        status="ready",
        cloud_instance_id="i-plan-forged-cleaned",
    )
    _plan_id, run_id = await _insert_worker_plan_graph(
        session_factory,
        worker_id=worker_id,
        archived=True,
        clean_runtime=True,
    )
    async with session_factory() as db:
        await db.execute(text("PRAGMA ignore_check_constraints = ON"))
        await db.execute(
            text(
                "UPDATE plan_agent_runtime_receipts SET cleaned_at = NULL "
                "WHERE run_id = :run_id"
            ),
            {"run_id": run_id},
        )
        await db.commit()
        await db.execute(text("PRAGMA ignore_check_constraints = OFF"))

    response = await client.post(f"/api/workers/{worker_id}/destroy")

    assert response.status_code == 409
    assert "completed" in response.json()["detail"]
    fake_provisioner.destroy_worker.assert_not_awaited()


async def test_destroy_rejects_cleaned_runtime_with_wrong_generation(
    client,
    session_factory,
    fake_provisioner,
):
    worker_id = await _insert_worker(
        session_factory,
        status="ready",
        cloud_instance_id="i-plan-cleaned-wrong-generation",
    )
    _plan_id, run_id = await _insert_worker_plan_graph(
        session_factory,
        worker_id=worker_id,
        archived=True,
        clean_runtime=True,
    )
    async with session_factory() as db:
        await db.execute(
            text(
                "UPDATE plan_agent_runtime_receipts "
                "SET run_generation = run_generation + 1 WHERE run_id = :run_id"
            ),
            {"run_id": run_id},
        )
        await db.commit()

    response = await client.post(f"/api/workers/{worker_id}/destroy")

    assert response.status_code == 409
    assert "completed" in response.json()["detail"]
    fake_provisioner.destroy_worker.assert_not_awaited()


async def test_destroy_allows_archived_clean_terminal_worker_plan_history(
    client,
    session_factory,
    fake_provisioner,
    monkeypatch,
):
    import backend.api.workers as workers_api

    worker_id = await _insert_worker(
        session_factory,
        status="ready",
        cloud_instance_id="i-plan-archived",
    )
    await _insert_worker_plan_graph(
        session_factory,
        worker_id=worker_id,
        archived=True,
        worker_mirror_runtime=True,
    )
    coordinator = AsyncMock()
    scheduled = []

    def capture_spawn(coro):
        scheduled.append(coro)
        return Mock()

    monkeypatch.setattr(workers_api, "_migrate_back_then_destroy", coordinator)
    monkeypatch.setattr(workers_api, "_spawn", capture_spawn)

    response = await client.post(f"/api/workers/{worker_id}/destroy")

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "destroying"
    assert len(scheduled) == 1
    await scheduled[0]
    coordinator.assert_awaited_once()


async def test_destroy_allows_remote_absent_history_before_clean_worker_outcome(
    client,
    session_factory,
    fake_provisioner,
    monkeypatch,
):
    import backend.api.workers as workers_api

    worker_id = await _insert_worker(
        session_factory,
        status="ready",
        cloud_instance_id="i-plan-remote-absent-history",
    )
    await _insert_worker_plan_graph(
        session_factory,
        worker_id=worker_id,
        archived=True,
        worker_mirror_runtime=True,
        historical_dispatch_reason="remote_absent",
    )
    coordinator = AsyncMock()
    scheduled = []

    def capture_spawn(coro):
        scheduled.append(coro)
        return Mock()

    monkeypatch.setattr(workers_api, "_migrate_back_then_destroy", coordinator)
    monkeypatch.setattr(workers_api, "_spawn", capture_spawn)

    response = await client.post(f"/api/workers/{worker_id}/destroy")

    assert response.status_code == 200, response.text
    assert len(scheduled) == 1
    await scheduled[0]
    coordinator.assert_awaited_once()


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
    async def _simple_destroy(
        prov,
        worker_id,
        _destroy_claim,
        db_factory_arg=None,
    ):
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


async def test_destroy_exactly_stops_active_task_before_migration(
    client,
    session_factory,
    fake_provisioner,
    monkeypatch,
):
    """The destroy-only proxy must retain terminal readback while ready is closed."""

    import backend.api.workers as workers_api
    from backend.models.task import Task
    from backend.services.worker_proxy import WorkerProxy
    from backend.services.worker_task_termination import (
        canonical_json_digest,
        receipt_not_found_payload,
    )

    worker_id = await _insert_worker(
        session_factory,
        status="ready",
        cloud_instance_id="i-active-destroy",
        private_ip="10.0.0.41",
        auth_token="destroy-secret",
    )
    async with session_factory() as db:
        task = Task(
            title="active Worker task",
            description="stop exactly before migration",
            status="executing",
            worker_id=worker_id,
            retry_count=3,
            turn_generation=8,
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    remote_calls = []
    remote_receipt = None

    async def remote_terminal_protocol(
        _proxy,
        worker,
        remote_task,
        method,
        path,
        body=None,
        **_options,
    ):
        nonlocal remote_receipt
        assert worker.id == worker_id
        assert worker.status == "destroying"
        assert worker.cloud_instance_id == "i-active-destroy"
        assert worker.private_ip == "10.0.0.41"
        assert worker.auth_token == "destroy-secret"
        assert remote_task.id == task_id
        remote_calls.append((method, path))
        operation_path = path.removesuffix("/ack")
        operation_id = operation_path.rsplit("/", 1)[-1]
        if method == "GET":
            assert remote_receipt is None
            return receipt_not_found_payload(task_id, operation_id)
        if method == "PUT":
            request_payload = body["request_payload"]
            request_digest = body["request_digest"]
            result_payload = {
                "version": 2,
                "operation_id": operation_id,
                "task_id": task_id,
                "operation": "stop_session",
                "request_digest": request_digest,
                "task": {
                    "id": task_id,
                    "status": "completed",
                    "retry_count": 3,
                    "turn_generation": 8,
                    "instance_id": None,
                    "started_at": None,
                    "completed_at": "2026-01-02T03:04:06.000000",
                    "session_id": None,
                    "error_message": None,
                    "background_active": False,
                },
                "response": {
                    "ok": True,
                    "stopped": True,
                    "cleared_messages": 0,
                },
            }
            remote_receipt = {
                "version": 2,
                "operation_id": operation_id,
                "task_id": task_id,
                "side": "worker",
                "worker_id": None,
                "operation": "stop_session",
                "status": "succeeded",
                "state_version": 3,
                "source": {
                    "incarnation_id": "1" * 32,
                    "status": "executing",
                    "retry_count": 3,
                    "turn_generation": 8,
                    "source_log_id": None,
                    "instance_id": None,
                    "started_at": None,
                    "completed_at": None,
                    "session_id": None,
                    "pty_background_generation": None,
                },
                "request_payload": request_payload,
                "request_digest": request_digest,
                "result_payload": result_payload,
                "result_digest": canonical_json_digest(result_payload),
                "attempt_count": 1,
                "reconcile_count": 0,
                "last_error": None,
                "accepted_at": "2026-01-02T03:04:05.000000",
                "completed_at": "2026-01-02T03:04:06.000000",
                "ack_intent_at": None,
                "acknowledged_at": None,
                "created_at": "2026-01-02T03:04:05.000000",
                "updated_at": "2026-01-02T03:04:06.000000",
            }
            return remote_receipt
        assert method == "POST" and path.endswith("/ack")
        acknowledged = copy.deepcopy(remote_receipt)
        acknowledged["status"] = "acknowledged"
        acknowledged["state_version"] += 1
        acknowledged["acknowledged_at"] = "2026-01-02T03:04:07.000000"
        acknowledged["updated_at"] = "2026-01-02T03:04:07.000000"
        return acknowledged

    ready_gate = AsyncMock(
        side_effect=AssertionError(
            "destroy terminal protocol must not widen the public ready proxy"
        )
    )
    monkeypatch.setattr(
        WorkerProxy,
        "_proxy_to_authorized_worker_locked",
        remote_terminal_protocol,
    )
    monkeypatch.setattr(WorkerProxy, "require_ready_worker", ready_gate)

    migrator = AsyncMock()

    async def migrate_after_terminal_readback(migrating_task_id, target):
        assert migrating_task_id == task_id
        assert target is None
        async with session_factory() as db:
            current = await db.get(Task, task_id)
            assert current.status == "completed"
            assert current.retry_count == 3
            assert current.turn_generation == 8
            current.worker_id = None
            await db.commit()

    migrator.migrate.side_effect = migrate_after_terminal_readback
    relay = AsyncMock()
    monkeypatch.setattr(main_module, "task_migrator", migrator)
    monkeypatch.setattr(main_module, "worker_relay", relay)
    original_destroy = workers_api._migrate_back_then_destroy

    async def destroy_with_test_db(provisioner, destroying_worker_id, claim):
        await original_destroy(
            provisioner,
            destroying_worker_id,
            claim,
            db_factory=session_factory,
        )

    monkeypatch.setattr(
        workers_api,
        "_migrate_back_then_destroy",
        destroy_with_test_db,
    )

    response = await client.post(f"/api/workers/{worker_id}/destroy")
    assert response.status_code == 200, response.text
    pending = list(workers_api._background_tasks)
    if pending:
        await asyncio.gather(*pending)

    assert [method for method, _path in remote_calls] == ["GET", "PUT", "POST"]
    receipt_path = remote_calls[0][1]
    assert receipt_path.startswith(
        f"/api/tasks/{task_id}/termination-receipts/"
    )
    assert remote_calls[1][1] == receipt_path
    assert remote_calls[2][1] == receipt_path + "/ack"
    ready_gate.assert_not_awaited()
    migrator.migrate.assert_awaited_once_with(task_id, None)
    relay.stop_worker.assert_awaited_once_with(worker_id)
    fake_provisioner.destroy_worker.assert_awaited_once()
    assert fake_provisioner.destroy_worker.await_args.args == (worker_id,)
    assert (
        fake_provisioner.destroy_worker.await_args.kwargs["destroy_claim"].worker_id
        == worker_id
    )
    async with session_factory() as db:
        current = await db.get(Task, task_id)
    assert current.worker_id is None
    assert current.status == "completed"


async def test_destroy_claim_rejects_changed_endpoint_without_widening_ready(
    session_factory,
):
    from fastapi import HTTPException
    from backend.models.task import Task
    from backend.services.worker_proxy import WorkerProxy

    worker_id = await _insert_worker(
        session_factory,
        status="destroying",
        cloud_instance_id="i-claimed",
        private_ip="10.0.0.51",
        auth_token="claimed-secret",
    )
    async with session_factory() as db:
        task = Task(
            title="claimed destroy",
            description="endpoint identity must stay exact",
            status="executing",
            worker_id=worker_id,
        )
        db.add(task)
        worker = await db.get(Worker, worker_id)
        claim = capture_worker_destroy_lifecycle_claim(worker)
        assert "claimed-secret" not in repr(claim)
        worker.private_ip = "10.0.0.99"
        await db.commit()

    proxy = WorkerProxy(session_factory, AsyncMock())
    remote = AsyncMock()
    proxy._proxy_to_authorized_worker_locked = remote

    with pytest.raises(ValueError, match="only exact termination receipt"):
        await proxy._proxy_to_claimed_destroying_worker(
            task,
            "POST",
            f"/api/tasks/{task.id}/cancel",
            destroy_claim=claim,
            require_json=True,
            operation_lock_held=True,
            quarantine_on_transport_uncertainty=True,
        )

    with pytest.raises(HTTPException) as rejected:
        await proxy._proxy_to_claimed_destroying_worker(
            task,
            "GET",
            f"/api/tasks/{task.id}/termination-receipts/{'a' * 32}",
            destroy_claim=claim,
            require_json=True,
            operation_lock_held=True,
        )
    assert rejected.value.status_code == 409
    remote.assert_not_awaited()

    with pytest.raises(HTTPException) as public_rejected:
        await proxy.require_ready_worker(worker_id)
    assert public_rejected.value.status_code == 503


async def test_stale_destroy_coordinator_cannot_restore_new_lifecycle(
    session_factory,
):
    import backend.api.workers as workers_api

    worker_id = await _insert_worker(
        session_factory,
        status="destroying",
        cloud_instance_id="i-destroy-aba",
        private_ip="10.0.0.61",
        auth_token="destroy-aba-secret",
    )
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
        stale_claim = capture_worker_destroy_lifecycle_claim(worker)
        worker.destroy_lifecycle_nonce = secrets.token_hex(16)
        worker.bootstrap_error = "new destroy lifecycle"
        await db.commit()

    await workers_api._mark_worker_destroy_blocked(
        session_factory,
        destroy_claim=stale_claim,
        detail="stale coordinator must not restore ready",
    )

    async with session_factory() as db:
        current = await db.get(Worker, worker_id)
    assert current.status == "destroying"
    assert current.bootstrap_error == "new destroy lifecycle"


async def test_destroy_claim_survives_trailing_metadata_writer(
    session_factory,
):
    """Bootstrap logs/metadata must not revoke the dedicated lifecycle nonce."""

    import backend.api.workers as workers_api

    worker_id = await _insert_worker(
        session_factory,
        status="destroying",
        cloud_instance_id="i-destroy-metadata",
        private_ip="10.0.0.62",
        auth_token="destroy-metadata-secret",
    )
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
        claim = capture_worker_destroy_lifecycle_claim(worker)
        original_nonce = worker.destroy_lifecycle_nonce
        worker.bootstrap_log = "late worker ready log\n"
        worker.bootstrap_error = "harmless trailing metadata"
        await db.commit()

    await workers_api._mark_worker_destroy_blocked(
        session_factory,
        destroy_claim=claim,
        detail="reconciliation remains owned by the same destroy",
    )

    async with session_factory() as db:
        current = await db.get(Worker, worker_id)
    assert current.status == "ready"
    assert current.bootstrap_step == "destroy"
    assert current.destroy_lifecycle_nonce == original_nonce
    assert current.bootstrap_error == (
        "reconciliation remains owned by the same destroy"
    )


async def test_destroy_fails_closed_for_pending_worker_turn_handoff(
    client,
    session_factory,
    fake_provisioner,
    monkeypatch,
):
    """Recovery stays live, then a second destroy succeeds after settlement."""
    import backend.api.tasks as tasks_api
    import backend.api.workers as workers_api
    from backend.models.log_entry import LogEntry
    from backend.models.task import Task
    from backend.models.worker_turn_handoff import WorkerTurnHandoffReceipt
    from backend.services.task_migrator import MigrationError

    worker_id = await _insert_worker(
        session_factory,
        status="ready",
        cloud_instance_id="i-handoff",
        private_ip="10.0.0.52",
        auth_token="handoff-worker-token",
    )
    handoff_id = "a" * 32
    async with session_factory() as db:
        task = Task(
            title="pending Worker follow-up",
            description="keep its exact route",
            status="completed",
            worker_id=worker_id,
            retry_count=2,
            turn_generation=7,
        )
        db.add(task)
        await db.flush()
        source_log = LogEntry(
            task_id=task.id,
            task_retry_count=2,
            task_turn_generation=7,
            event_type="user_message",
            role="user",
            content="continue",
        )
        db.add(source_log)
        await db.flush()
        request_payload = {
            "message": "continue",
            "worker_turn_handoff_id": handoff_id,
            "worker_turn_handoff_retry_count": 2,
            "worker_turn_handoff_from_generation": 7,
        }
        db.add(
            WorkerTurnHandoffReceipt(
                handoff_id=handoff_id,
                task_id=task.id,
                source_log_id=source_log.id,
                side="manager",
                worker_id=worker_id,
                retry_count=2,
                from_generation=7,
                status="prepared",
                request_payload=request_payload,
                request_digest="b" * 64,
            )
        )
        task.worker_turn_handoff_id = handoff_id
        task.worker_turn_handoff_worker_id = worker_id
        task.worker_turn_handoff_retry_count = 2
        task.worker_turn_handoff_from_generation = 7
        task.worker_turn_handoff_source_log_id = source_log.id
        task.worker_turn_handoff_acknowledged = False
        await db.commit()
        task_id = task.id
        source_log_id = source_log.id

    migrator = AsyncMock()

    async def migrate_after_exact_settlement(migrating_task_id, target):
        assert target is None
        async with session_factory() as db:
            current = await db.get(Task, migrating_task_id)
            if current.worker_turn_handoff_id is not None:
                raise MigrationError(
                    "Worker follow-up turn handoff is still pending"
                )
            current.worker_id = None
            await db.commit()

    migrator.migrate.side_effect = migrate_after_exact_settlement
    relay = AsyncMock()
    monkeypatch.setattr(main_module, "task_migrator", migrator)
    monkeypatch.setattr(main_module, "worker_relay", relay)
    # This test isolates the handoff barrier. Exact remote-stop receipt
    # convergence is covered separately; bypass it here so migration reaches
    # the intentionally pending handoff on both destroy attempts.
    monkeypatch.setattr(
        tasks_api,
        "_stop_worker_task_for_destroy",
        AsyncMock(),
    )
    original_destroy = workers_api._migrate_back_then_destroy

    async def destroy_with_test_db(
        provisioner,
        destroying_worker_id,
        destroy_claim,
        _db=None,
    ):
        await original_destroy(
            provisioner,
            destroying_worker_id,
            destroy_claim,
            db_factory=session_factory,
        )

    monkeypatch.setattr(
        workers_api,
        "_migrate_back_then_destroy",
        destroy_with_test_db,
    )

    first = await client.post(f"/api/workers/{worker_id}/destroy")
    assert first.status_code == 200
    pending = list(workers_api._background_tasks)
    if pending:
        await asyncio.gather(*pending)

    async with session_factory() as db:
        current_task = await db.get(Task, task_id)
        receipt = await db.get(WorkerTurnHandoffReceipt, handoff_id)
        worker = await db.get(Worker, worker_id)
    assert current_task.worker_id == worker_id
    assert current_task.worker_turn_handoff_id == handoff_id
    assert current_task.worker_turn_handoff_source_log_id == source_log_id
    assert receipt.status == "prepared"
    assert receipt.worker_id == worker_id
    # WorkerRelay's durable handoff recovery loop only runs in ready state.
    assert worker.status == "ready"
    # The destroy marker keeps relay recovery live but every new durable
    # assignment/public route requires bootstrap_step IS NULL.
    assert worker.bootstrap_step == "destroy"
    assert "handoff" in worker.bootstrap_error
    migrator.migrate.assert_awaited_once_with(task_id, None)
    relay.stop_worker.assert_not_awaited()
    fake_provisioner.destroy_worker.assert_not_awaited()

    # Simulate WorkerRelay's exact cancellation transaction: settle the
    # matching Manager receipt and clear the matching Task marker together.
    async with session_factory() as db:
        current_task = await db.get(Task, task_id)
        receipt = await db.get(WorkerTurnHandoffReceipt, handoff_id)
        receipt.status = "cancelled"
        receipt.cancel_reason = "exact Worker receipt cancelled before launch"
        current_task.worker_turn_handoff_id = None
        current_task.worker_turn_handoff_worker_id = None
        current_task.worker_turn_handoff_retry_count = None
        current_task.worker_turn_handoff_from_generation = None
        current_task.worker_turn_handoff_source_log_id = None
        current_task.worker_turn_handoff_acknowledged = None
        await db.commit()

    second = await client.post(f"/api/workers/{worker_id}/destroy")
    assert second.status_code == 200
    pending = list(workers_api._background_tasks)
    if pending:
        await asyncio.gather(*pending)

    async with session_factory() as db:
        migrated = await db.get(Task, task_id)
        settled = await db.get(WorkerTurnHandoffReceipt, handoff_id)
    assert migrated.worker_id is None
    assert migrated.worker_turn_handoff_id is None
    assert settled.status == "cancelled"
    assert migrator.migrate.await_count == 2
    relay.stop_worker.assert_awaited_once_with(worker_id)
    fake_provisioner.destroy_worker.assert_awaited_once()
    assert fake_provisioner.destroy_worker.await_args.args == (worker_id,)
    assert (
        fake_provisioner.destroy_worker.await_args.kwargs["destroy_claim"].worker_id
        == worker_id
    )


@pytest.mark.parametrize("status", ["pending", "merging"])
async def test_destroy_keeps_non_inert_task_on_live_worker(
    session_factory,
    monkeypatch,
    status,
):
    """A stale successful migration return is never cloud-destroy authority."""

    import backend.api.workers as workers_api
    from backend.models.task import Task

    worker_id = await _insert_worker(
        session_factory,
        status="destroying",
        cloud_instance_id=f"i-{status}",
    )
    async with session_factory() as db:
        task = Task(
            title=f"{status} Worker task",
            description="retain exact remote evidence",
            status=status,
            worker_id=worker_id,
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    migrator = AsyncMock()
    relay = AsyncMock()
    provisioner = AsyncMock()
    monkeypatch.setattr(main_module, "task_migrator", migrator)
    monkeypatch.setattr(main_module, "worker_relay", relay)

    async with session_factory() as db:
        destroy_claim = capture_worker_destroy_lifecycle_claim(
            await db.get(Worker, worker_id)
        )

    await workers_api._migrate_back_then_destroy(
        provisioner,
        worker_id,
        destroy_claim,
        db_factory=session_factory,
    )

    async with session_factory() as db:
        current_task = await db.get(Task, task_id)
        current_worker = await db.get(Worker, worker_id)
    assert current_task.worker_id == worker_id
    assert current_task.status == status
    assert current_worker.status == "ready"
    assert status in current_worker.bootstrap_error
    # Destroy requires a durable remote stop receipt for every Manager mirror.
    # Without a reachable Worker receipt endpoint it stops before migration,
    # never using the migrator as a lossy cleanup fallback.
    migrator.migrate.assert_not_awaited()
    relay.stop_worker.assert_not_awaited()
    provisioner.destroy_worker.assert_not_awaited()


@pytest.mark.parametrize(
    "marker_name",
    [
        WORKER_TERMINATION_UNCERTAINTY_METADATA_KEY,
        LEGACY_PLAN_CARRIER_CONFLICT_METADATA_KEY,
    ],
)
async def test_destroy_keeps_quarantined_inert_task_on_live_worker(
    session_factory,
    monkeypatch,
    marker_name,
):
    import backend.api.workers as workers_api
    from backend.models.task import Task

    worker_id = await _insert_worker(
        session_factory,
        status="destroying",
        cloud_instance_id="i-quarantined",
    )
    async with session_factory() as db:
        task = Task(
            title="quarantined Worker task",
            description="retain exact remote evidence",
            status="completed",
            worker_id=worker_id,
            # Marker presence is the safety boundary, even when malformed.
            metadata_={marker_name: "malformed"},
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    migrator = AsyncMock()
    migrator.migrate.side_effect = RuntimeError("migration is quarantined")
    relay = AsyncMock()
    provisioner = AsyncMock()
    monkeypatch.setattr(main_module, "task_migrator", migrator)
    monkeypatch.setattr(main_module, "worker_relay", relay)

    async with session_factory() as db:
        destroy_claim = capture_worker_destroy_lifecycle_claim(
            await db.get(Worker, worker_id)
        )

    await workers_api._migrate_back_then_destroy(
        provisioner,
        worker_id,
        destroy_claim,
        db_factory=session_factory,
    )

    async with session_factory() as db:
        current_task = await db.get(Task, task_id)
        current_worker = await db.get(Worker, worker_id)
    assert current_task.worker_id == worker_id
    assert current_task.status == "completed"
    assert marker_name in current_task.metadata_
    assert current_worker.status == "ready"
    assert "quarantine" in current_worker.bootstrap_error
    relay.stop_worker.assert_not_awaited()
    provisioner.destroy_worker.assert_not_awaited()


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
        bootstrap_step="account-login",
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
            "status": "failed",
        }],
    )


async def test_retry_preserves_exact_bound_logged_in_claude_account(
    client,
    session_factory,
    fake_provisioner,
):
    worker_id = await _insert_worker(
        session_factory,
        status="error",
        bootstrap_step="account-login",
    )
    identity = await _set_worker_cloud_login_generation(
        session_factory,
        worker_id,
        instance_id="i-bound-login",
        auth_token="bound-login-token",
    )
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
        worker.accounts = [{
            "email": "bound@example.com",
            "token": "mail-token",
            "password": "",
            "provider": "claude",
            "login_method": "onet",
            "status": "logged_in",
            "account_id": "default",
            CLAUDE_LOGIN_IDENTITY_KEY: identity,
        }]
        await db.commit()

    response = await client.post(f"/api/workers/{worker_id}/retry")

    assert response.status_code == 200, response.text
    await asyncio.sleep(0)
    fake_provisioner.create_worker.assert_awaited_once_with(
        worker_id,
        accounts=[{
            "email": "bound@example.com",
            "token": "mail-token",
            "password": "",
            "provider": "claude",
            "login_method": "onet",
            "account_id": "default",
            "status": "logged_in",
            CLAUDE_LOGIN_IDENTITY_KEY: identity,
        }],
    )


async def test_retry_downgrades_stale_claude_login_binding(
    client,
    session_factory,
    fake_provisioner,
):
    worker_id = await _insert_worker(
        session_factory,
        status="error",
        bootstrap_step="account-login",
    )
    stale_identity = await _set_worker_cloud_login_generation(
        session_factory,
        worker_id,
        instance_id="i-old-login",
        auth_token="old-login-token",
    )
    await _set_worker_cloud_login_generation(
        session_factory,
        worker_id,
        instance_id="i-replacement-login",
        auth_token="replacement-login-token",
    )
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
        worker.accounts = [{
            "email": "stale@example.com",
            "token": "mail-token",
            "provider": "claude",
            "login_method": "onet",
            "status": "logged_in",
            CLAUDE_LOGIN_IDENTITY_KEY: stale_identity,
        }]
        await db.commit()

    response = await client.post(f"/api/workers/{worker_id}/retry")

    assert response.status_code == 200, response.text
    await asyncio.sleep(0)
    fake_provisioner.create_worker.assert_awaited_once_with(
        worker_id,
        accounts=[{
            "email": "stale@example.com",
            "token": "mail-token",
            "password": "",
            "provider": "claude",
            "login_method": "onet",
            "status": "failed",
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


@pytest.mark.parametrize(
    "status_fields",
    [
        {"status": "pending"},
        {},
        {"status": 7},
        {"status": "logging_in"},
    ],
    ids=("pending", "missing", "malformed", "nonterminal"),
)
async def test_retry_rejects_uncertain_interrupted_claude_login(
    client,
    session_factory,
    fake_provisioner,
    status_fields,
):
    account = {
        "email": "uncertain@example.com",
        "token": "mail-token",
        "login_method": "onet",
        **status_fields,
    }
    worker_id = await _insert_worker(
        session_factory,
        status="error",
        bootstrap_step="account-login",
        accounts=[account],
    )

    response = await client.post(f"/api/workers/{worker_id}/retry")

    assert response.status_code == 409
    assert "登录结果不确定" in response.json()["detail"]
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert worker.status == "error"
    assert worker.bootstrap_step == "account-login"
    assert worker.accounts == [account]
    fake_provisioner.create_worker.assert_not_awaited()


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


async def test_persist_worker_account_state_rejects_destroy_recovery_marker(
    db_factory,
    session_factory,
):
    original = [{
        "email": "existing@example.com",
        "provider": "codex",
        "status": "logged_in",
        "account_id": "existing",
    }]
    worker_id = await _insert_worker(
        session_factory,
        status="ready",
        bootstrap_step="destroy",
        accounts=original,
    )
    provisioner = WorkerProvisioner(db_factory, cloud=object())
    account = {
        "email": "late@example.com",
        "provider": "codex",
        "token": "mail-token",
        "password": "password",
        "login_method": "mailcatcher",
    }

    with pytest.raises(RuntimeError, match="ready/destroy"):
        await _persist_worker_account_state(
            provisioner,
            worker_id,
            account,
            status="logged_in",
            account_id="late",
        )

    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert worker.accounts == original


async def test_get_worker_and_logs(client, session_factory, fake_provisioner):
    wid = await _insert_worker(session_factory, bootstrap_log="[00:00:00] hi\n")
    resp = await client.get(f"/api/workers/{wid}")
    assert resp.status_code == 200
    resp = await client.get(f"/api/workers/{wid}/logs")
    assert resp.json()["bootstrap_log"] == "[00:00:00] hi\n"
    assert (await client.get("/api/workers/999")).status_code == 404


async def test_rename_worker_with_known_active_instance_updates_db_and_tag(
    client, session_factory, fake_provisioner, monkeypatch,
):
    import backend.services.cloud_provider as cloud_provider_module

    worker_id = await _insert_worker(
        session_factory,
        status="ready",
        cloud_instance_id="i-known-active",
        private_ip="10.0.0.9",
        auth_token="worker-token",
    )
    cloud_factory = Mock(side_effect=AssertionError("must reuse provisioner cloud"))
    monkeypatch.setattr(cloud_provider_module, "AWSProvider", cloud_factory)
    monkeypatch.setattr(main_module, "broadcaster", None)

    response = await client.patch(
        f"/api/workers/{worker_id}/rename",
        json={"name": "renamed-known-worker"},
    )

    assert response.status_code == 200, response.text
    assert response.json()["name"] == "renamed-known-worker"
    cloud_factory.assert_not_called()
    fake_provisioner.require_worker_cloud_identity.assert_awaited_once()
    exact_worker = (
        fake_provisioner.require_worker_cloud_identity.await_args.args[0]
    )
    assert exact_worker.id == worker_id
    assert exact_worker.name == "test-worker"
    fake_provisioner.cloud.update_instance_tags.assert_awaited_once_with(
        "i-known-active",
        {"Name": "renamed-known-worker"},
    )
    async with session_factory() as db:
        persisted = await db.get(Worker, worker_id)
        assert persisted.name == "renamed-known-worker"
        assert persisted.rename_generation == 1
        assert persisted.rename_tag_outbox is None


@pytest.mark.parametrize(
    "identity_error",
    [
        RuntimeError("cloud scope drift"),
        RuntimeError("cloud instance drift"),
    ],
    ids=("scope-drift", "instance-drift"),
)
async def test_rename_worker_rejects_when_cloud_identity_drifts(
    client,
    session_factory,
    fake_provisioner,
    monkeypatch,
    identity_error,
):
    import backend.services.cloud_provider as cloud_provider_module

    worker_id = await _insert_worker(
        session_factory,
        status="ready",
        cloud_instance_id="i-drifted",
        private_ip="10.0.0.9",
        auth_token="worker-token",
    )
    fake_provisioner.require_worker_cloud_identity.side_effect = identity_error
    cloud_factory = Mock(side_effect=AssertionError("must reuse provisioner cloud"))
    monkeypatch.setattr(cloud_provider_module, "AWSProvider", cloud_factory)
    monkeypatch.setattr(main_module, "broadcaster", None)

    response = await client.patch(
        f"/api/workers/{worker_id}/rename",
        json={"name": "locally-renamed"},
    )

    assert response.status_code == 409, response.text
    fake_provisioner.require_worker_cloud_identity.assert_awaited_once()
    fake_provisioner.cloud.update_instance_tags.assert_not_awaited()
    cloud_factory.assert_not_called()
    async with session_factory() as db:
        persisted = await db.get(Worker, worker_id)
        assert persisted.name == "test-worker"
        assert persisted.rename_tag_outbox is None


async def test_rename_worker_keeps_db_name_when_cloud_tag_update_fails(
    client,
    session_factory,
    fake_provisioner,
    monkeypatch,
):
    worker_id = await _insert_worker(
        session_factory,
        status="ready",
        cloud_instance_id="i-tag-failure",
        private_ip="10.0.0.9",
        auth_token="worker-token",
    )
    fake_provisioner.cloud.update_instance_tags.side_effect = RuntimeError(
        "tag update failed"
    )
    monkeypatch.setattr(main_module, "broadcaster", None)

    response = await client.patch(
        f"/api/workers/{worker_id}/rename",
        json={"name": "rename-survives-tag-failure"},
    )

    assert response.status_code == 200, response.text
    assert response.json()["name"] == "rename-survives-tag-failure"
    fake_provisioner.require_worker_cloud_identity.assert_awaited_once()
    fake_provisioner.cloud.update_instance_tags.assert_awaited_once_with(
        "i-tag-failure",
        {"Name": "rename-survives-tag-failure"},
    )
    async with session_factory() as db:
        persisted = await db.get(Worker, worker_id)
        assert persisted.name == "rename-survives-tag-failure"
        assert persisted.rename_generation == 1
        assert persisted.rename_tag_outbox["desired_name"] == (
            "rename-survives-tag-failure"
        )


async def test_worker_rename_outbox_replays_ack_loss_and_clears_exact_generation(
    session_factory,
):
    worker_id = await _insert_worker(
        session_factory,
        status="ready",
        cloud_instance_id="i-rename-replay",
        private_ip="10.0.0.20",
        auth_token="rename-replay-token",
        provision_spec={
            "version": 1,
            "name": "test-worker",
            "has_fixed_overrides": False,
            "overrides": {},
            "cloud_scope": FAKE_CLOUD_SCOPE,
            "client_token_digest": worker_create_client_token_digest(
                1,
                "rename-replay-token",
            ),
        },
    )
    # IDs are deterministic in the isolated test database today, but bind the
    # digest to the actual row in case another fixture inserts first.
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
        worker.provision_spec = {
            **worker.provision_spec,
            "client_token_digest": worker_create_client_token_digest(
                worker.id,
                worker.auth_token,
            ),
        }
        receipt = build_worker_rename_tag_outbox(
            worker,
            desired_name="durable-name",
            generation=1,
            cloud_scope=FAKE_CLOUD_SCOPE,
            client_token_digest=worker.provision_spec["client_token_digest"],
        )
        worker.name = "durable-name"
        worker.rename_generation = 1
        worker.rename_tag_outbox = receipt
        await db.commit()

    cloud = AsyncMock()
    cloud.termination_scope.return_value = dict(FAKE_CLOUD_SCOPE)
    cloud.find_instance_by_create_token.return_value = "i-rename-replay"
    cloud.update_instance_tags.side_effect = [
        RuntimeError("provider accepted but response was lost"),
        None,
    ]
    provisioner = WorkerProvisioner(session_factory, cloud=cloud)

    with pytest.raises(RuntimeError, match="response was lost"):
        await provisioner.reconcile_worker_rename_tag_outbox(
            worker_id,
            expected_operation_id=receipt["operation_id"],
        )
    async with session_factory() as db:
        uncertain = await db.get(Worker, worker_id)
        assert uncertain.rename_tag_outbox == receipt
        assert uncertain.rename_generation == 1

    settled = await provisioner.reconcile_worker_rename_tag_outbox(
        worker_id,
        expected_operation_id=receipt["operation_id"],
    )
    assert settled.rename_tag_outbox is None
    assert settled.rename_generation == 1
    assert cloud.update_instance_tags.await_args_list == [
        call("i-rename-replay", {"Name": "durable-name"}),
        call("i-rename-replay", {"Name": "durable-name"}),
    ]

    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocked_tag(*_args, **_kwargs):
        entered.set()
        await release.wait()

    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
        second = build_worker_rename_tag_outbox(
            worker,
            desired_name="cancel-safe-name",
            generation=2,
            cloud_scope=FAKE_CLOUD_SCOPE,
            client_token_digest=worker_create_client_token_digest(
                worker.id,
                worker.auth_token,
            ),
        )
        worker.name = "cancel-safe-name"
        worker.rename_generation = 2
        worker.rename_tag_outbox = second
        await db.commit()
    cloud.update_instance_tags.side_effect = blocked_tag
    replay = asyncio.create_task(
        provisioner.reconcile_worker_rename_tag_outbox(
            worker_id,
            expected_operation_id=second["operation_id"],
        )
    )
    await asyncio.wait_for(entered.wait(), timeout=1)
    replay.cancel()
    await asyncio.sleep(0)
    assert not replay.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await replay
    async with session_factory() as db:
        cancelled_after_ack = await db.get(Worker, worker_id)
        assert cancelled_after_ack.rename_generation == 2
        assert cancelled_after_ack.rename_tag_outbox is None


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


async def test_rename_worker_final_cas_rejects_stale_member_ownership(
    session_factory,
    monkeypatch,
):
    """A former owner cannot rename after an ownership transfer wins."""

    from fastapi import HTTPException

    import backend.api.deps as deps
    import backend.api.workers as workers_api
    from backend.models.user import User

    async with session_factory() as db:
        former_owner = User(
            email="former-rename-owner@example.com",
            name="former rename owner",
            password_hash="test",
            role="member",
            is_active=True,
        )
        current_owner = User(
            email="current-rename-owner@example.com",
            name="current rename owner",
            password_hash="test",
            role="member",
            is_active=True,
        )
        db.add_all((former_owner, current_owner))
        await db.flush()
        worker = Worker(
            name="ownership-before-rename",
            status="ready",
            cloud_instance_id="i-owner-transfer",
            owner_user_id=current_owner.id,
        )
        db.add(worker)
        await db.commit()
        former_owner_id = former_owner.id
        worker_id = worker.id

    # Model the exact TOCTOU point: preliminary authorization observed the
    # old ownership, but the durable Worker row already carries the transfer.
    monkeypatch.setattr(
        deps,
        "require_worker_access",
        AsyncMock(return_value=None),
    )
    request = MagicMock()
    request.state.auth_type = "jwt"
    request.state.user_id = former_owner_id
    request.state.user_role = "member"
    async with session_factory() as db:
        with pytest.raises(HTTPException) as rejected:
            await workers_api.rename_worker(
                worker_id,
                workers_api.RenameWorkerBody(name="stale-owner-rename"),
                request,
                db,
            )
    assert rejected.value.status_code == 409
    async with session_factory() as db:
        assert (await db.get(Worker, worker_id)).name == "ownership-before-rename"


async def test_rename_worker_rejects_stale_cached_admin_role(
    session_factory,
    monkeypatch,
):
    """An admin demotion rolls back the rename before its cloud effect."""

    from fastapi import HTTPException

    import backend.api.workers as workers_api
    import backend.services.cloud_provider as cloud_provider_module
    from backend.models.user import User

    async with session_factory() as db:
        user = User(
            email="stale-rename-admin@example.com",
            name="stale rename admin",
            password_hash="test",
            role="member",
            is_active=True,
        )
        worker = Worker(
            name="role-before-rename",
            status="ready",
            cloud_instance_id="i-role-demotion",
        )
        db.add_all((user, worker))
        await db.commit()
        user_id = user.id
        worker_id = worker.id

    cloud = AsyncMock()
    monkeypatch.setattr(
        cloud_provider_module,
        "AWSProvider",
        Mock(return_value=cloud),
    )
    request = MagicMock()
    request.state.auth_type = "jwt"
    request.state.user_id = user_id
    request.state.user_role = "admin"
    async with session_factory() as db:
        with pytest.raises(HTTPException) as rejected:
            await workers_api.rename_worker(
                worker_id,
                workers_api.RenameWorkerBody(name="stale-admin-rename"),
                request,
                db,
            )
    assert rejected.value.status_code == 409
    assert "changed role" in str(rejected.value.detail)
    cloud.update_instance_tags.assert_not_awaited()
    async with session_factory() as db:
        assert (await db.get(Worker, worker_id)).name == "role-before-rename"


async def test_assign_worker_rejects_stale_cached_admin_role(
    session_factory,
):
    """An admin demotion rolls the Worker assignment CAS back."""

    from fastapi import HTTPException

    import backend.api.workers as workers_api
    from backend.models.user import User

    async with session_factory() as db:
        stale_admin = User(
            email="stale-assign-admin@example.com",
            name="stale assign admin",
            password_hash="test",
            role="member",
            is_active=True,
        )
        owner = User(
            email="preserved-worker-owner@example.com",
            name="preserved worker owner",
            password_hash="test",
            role="member",
            is_active=True,
        )
        db.add_all((stale_admin, owner))
        await db.flush()
        worker = Worker(
            name="role-fenced assignment",
            status="ready",
            owner_user_id=owner.id,
        )
        db.add(worker)
        await db.commit()
        stale_admin_id = stale_admin.id
        owner_id = owner.id
        worker_id = worker.id

    request = MagicMock()
    request.state.auth_type = "jwt"
    request.state.user_id = stale_admin_id
    request.state.user_role = "admin"
    async with session_factory() as db:
        with pytest.raises(HTTPException) as rejected:
            await workers_api.assign_worker(
                worker_id,
                workers_api.AssignWorkerBody(owner_user_id=None),
                request,
                db,
            )
    assert rejected.value.status_code == 409
    async with session_factory() as db:
        assert (await db.get(Worker, worker_id)).owner_user_id == owner_id


@pytest.mark.parametrize("recipient_kind", ["missing", "inactive"])
async def test_assign_worker_requires_active_existing_recipient(
    session_factory,
    recipient_kind,
):
    """Assignment never commits an orphan or disabled owner reference."""

    from fastapi import HTTPException

    import backend.api.workers as workers_api
    from backend.models.user import User

    async with session_factory() as db:
        worker = Worker(name="recipient-fenced assignment", status="ready")
        db.add(worker)
        if recipient_kind == "inactive":
            recipient = User(
                email="inactive-worker-owner@example.com",
                name="inactive worker owner",
                password_hash="test",
                role="member",
                is_active=False,
            )
            db.add(recipient)
            await db.flush()
            recipient_id = recipient.id
        else:
            recipient_id = 2_000_000_000
        await db.commit()
        worker_id = worker.id

    request = MagicMock()
    request.state.auth_type = "token"
    request.state.user_id = None
    request.state.user_role = "super_admin"
    async with session_factory() as db:
        with pytest.raises(HTTPException) as rejected:
            await workers_api.assign_worker(
                worker_id,
                workers_api.AssignWorkerBody(owner_user_id=recipient_id),
                request,
                db,
            )
    assert rejected.value.status_code == 400
    async with session_factory() as db:
        assert (await db.get(Worker, worker_id)).owner_user_id is None


async def test_assign_worker_locks_actor_and_recipient_in_user_id_order(
    session_factory,
    monkeypatch,
):
    """Crossed admin assignments cannot acquire User rows in reverse order."""

    import backend.api.workers as workers_api
    from backend.models.user import User

    async with session_factory() as db:
        actor = User(
            email="ordered-worker-actor@example.com",
            name="ordered worker actor",
            password_hash="test",
            role="admin",
            is_active=True,
        )
        recipient = User(
            email="ordered-worker-recipient@example.com",
            name="ordered worker recipient",
            password_hash="test",
            role="member",
            is_active=True,
        )
        worker = Worker(name="ordered assignment", status="ready")
        db.add_all((actor, recipient, worker))
        await db.commit()
        await db.refresh(actor)
        await db.refresh(recipient)
        await db.refresh(worker)
        actor_id = actor.id
        recipient_id = recipient.id
        worker_id = worker.id
    assert actor_id < recipient_id

    original_execute = AsyncSession.execute
    user_updates: list[str] = []

    async def execute_spy(session, statement, *args, **kwargs):
        table = getattr(statement, "table", None)
        if (
            getattr(statement, "is_update", False)
            and getattr(table, "name", None) == "users"
        ):
            user_updates.append(
                str(statement.compile(compile_kwargs={"literal_binds": True}))
            )
        return await original_execute(session, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "execute", execute_spy)
    monkeypatch.setattr(
        "backend.services.feishu_notify.notify_worker_assigned",
        AsyncMock(return_value=None),
    )
    request = MagicMock()
    request.state.auth_type = "jwt"
    request.state.user_id = actor_id
    request.state.user_role = "admin"

    async with session_factory() as db:
        assigned = await workers_api.assign_worker(
            worker_id,
            workers_api.AssignWorkerBody(owner_user_id=recipient_id),
            request,
            db,
        )

    assert assigned.owner_user_id == recipient_id
    assert len(user_updates) == 2
    assert f"users.id = {actor_id}" in user_updates[0]
    assert f"users.id = {recipient_id}" in user_updates[1]


async def test_worker_pool_and_runtime_routes_recheck_member_ownership_before_effect(
    session_factory,
    monkeypatch,
):
    """A transfer that wins after the route read blocks every remote call."""

    from fastapi import HTTPException

    import backend.api.deps as deps
    import backend.api.workers as workers_api
    from backend.models.user import User
    from backend.schemas.global_settings import RuntimeSettingsUpdate

    async with session_factory() as db:
        former_owner = User(
            email="former-pool-owner@example.com",
            name="former pool owner",
            password_hash="test",
            role="member",
            is_active=True,
        )
        current_owner = User(
            email="current-pool-owner@example.com",
            name="current pool owner",
            password_hash="test",
            role="member",
            is_active=True,
        )
        db.add_all((former_owner, current_owner))
        await db.flush()
        worker = Worker(
            name="transferred pool worker",
            status="ready",
            private_ip="10.0.0.88",
            owner_user_id=current_owner.id,
        )
        db.add(worker)
        await db.commit()
        former_owner_id = former_owner.id
        worker_id = worker.id

    # Model a transfer committed after each endpoint's preliminary read.
    monkeypatch.setattr(
        deps,
        "require_worker_access",
        AsyncMock(return_value=None),
    )
    remote = AsyncMock()
    monkeypatch.setattr(workers_api, "_worker_http_request", remote)
    request = MagicMock()
    request.state.auth_type = "jwt"
    request.state.user_id = former_owner_id
    request.state.user_role = "member"
    calls = (
        lambda db: workers_api.get_worker_pool(
            worker_id, request, "codex", db
        ),
        lambda db: workers_api.worker_add_status(
            worker_id, "owner@example.com", request, "codex", db
        ),
        lambda db: workers_api.get_worker_pool_usage(
            worker_id, request, "codex", db
        ),
        lambda db: workers_api.get_worker_runtime_settings(
            worker_id, request, db
        ),
        lambda db: workers_api.update_worker_runtime_settings(
            worker_id,
            request,
            RuntimeSettingsUpdate(auto_sort_on_access=True),
            db,
        ),
    )
    for call in calls:
        async with session_factory() as db:
            with pytest.raises(HTTPException) as rejected:
                await call(db)
        assert rejected.value.status_code == 409
    remote.assert_not_awaited()


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
    assert "CCM_NODE_ROLE=worker" in env
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
    assert 'CODEX_CLI_VERSION="0.147.0"' in script
    assert '@openai/codex@$CODEX_CLI_VERSION' in script
    assert '"codex-cli $CODEX_CLI_VERSION"' in script
    assert "xvfb xauth xdotool" in script
    assert "bubblewrap socat" in script
    assert "149.0.7827.53-1" in script
    assert "google-chrome-stable_current" not in script


async def test_provisioner_service_survives_child_oom(db_factory):
    prov = WorkerProvisioner(
        db_factory=db_factory,
        cloud=FakeCloud(),
        broadcaster=None,
    )
    ssh = AsyncMock()
    ssh.run.return_value = (0, "ok")
    worker = Mock(ssh_user="ubuntu", ccm_port=8000)

    await prov._step_ccm_service(ssh, worker)

    unit_script = ssh.run.await_args.args[0]
    assert "OOMPolicy=continue" in unit_script


async def test_provisioner_login_persists_credentials_and_onet_method(
    db_factory, session_factory,
):
    wid = await _insert_worker(session_factory, status="creating")
    login_identity = await _set_worker_cloud_login_generation(
        session_factory,
        wid,
        instance_id="i-onet-login",
        auth_token="onet-login-token",
    )
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
        CLAUDE_LOGIN_IDENTITY_KEY: login_identity,
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


async def test_provisioner_login_skips_logged_in_claude_and_retries_failed(
    db_factory,
    session_factory,
):
    worker_id = await _insert_worker(session_factory, status="creating")
    login_identity = await _set_worker_cloud_login_generation(
        session_factory,
        worker_id,
        instance_id="i-mixed-login",
        auth_token="mixed-login-token",
    )
    provisioner = WorkerProvisioner(
        db_factory=db_factory,
        cloud=FakeCloud(),
        broadcaster=None,
    )
    provisioner._log = AsyncMock()
    ssh = AsyncMock()
    ssh.run.side_effect = [(0, "uploaded"), (0, "login ok")]
    accounts = [
        {
            "email": "already@example.com",
            "token": "already-token",
            "password": "",
            "provider": "claude",
            "login_method": "onet",
            "status": "logged_in",
            "account_id": "default",
            CLAUDE_LOGIN_IDENTITY_KEY: login_identity,
        },
        {
            "email": "retry@example.com",
            "token": "retry-token",
            "password": "",
            "provider": "claude",
            "login_method": "onet",
            "status": "failed",
            "account_id": "account-2",
        },
    ]

    await provisioner._step_account_login(ssh, worker_id, accounts)

    assert ssh.run.await_count == 2
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert worker.accounts == [
        accounts[0],
        {
            **accounts[1],
            "status": "logged_in",
            CLAUDE_LOGIN_IDENTITY_KEY: login_identity,
        },
    ]
    assert any(
        "skipping remote login" in call.args[1]
        for call in provisioner._log.await_args_list
    )


async def test_failed_claude_retry_journals_pending_before_ambiguous_effect(
    client,
    db_factory,
    session_factory,
    fake_provisioner,
):
    account = {
        "email": "ambiguous@example.com",
        "token": "mail-token",
        "password": "",
        "provider": "claude",
        "login_method": "onet",
        "status": "failed",
    }
    worker_id = await _insert_worker(
        session_factory,
        status="creating",
        accounts=[account],
    )
    await _set_worker_cloud_login_generation(
        session_factory,
        worker_id,
        instance_id="i-ambiguous-login",
        auth_token="ambiguous-login-token",
    )
    provisioner = WorkerProvisioner(db_factory, cloud=FakeCloud())
    provisioner._log = AsyncMock()
    ssh = AsyncMock()
    observed_accounts = []
    remote_call_count = 0

    async def lose_second_ssh_result(*_args, **_kwargs):
        nonlocal remote_call_count
        remote_call_count += 1
        async with session_factory() as db:
            worker = await db.get(Worker, worker_id)
            observed_accounts.append(worker.accounts)
        if remote_call_count == 1:
            return 0, "uploaded"
        raise RuntimeError("SSH result lost after remote login may have completed")

    ssh.run.side_effect = lose_second_ssh_result

    with pytest.raises(RuntimeError, match="SSH result lost"):
        await provisioner._step_account_login(ssh, worker_id, [account])

    expected_pending = [{
        "email": "ambiguous@example.com",
        "token": "mail-token",
        "password": "",
        "provider": "claude",
        "login_method": "onet",
        "status": "pending",
        "account_id": "default",
    }]
    assert observed_accounts == [expected_pending, expected_pending]
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
        assert worker.accounts == expected_pending
        worker.status = "error"
        worker.bootstrap_step = "account-login"
        await db.commit()

    response = await client.post(f"/api/workers/{worker_id}/retry")

    assert response.status_code == 409, response.text
    assert "登录结果不确定" in response.json()["detail"]
    fake_provisioner.create_worker.assert_not_awaited()


async def test_provisioner_relogs_claude_after_worker_generation_changes(
    db_factory,
    session_factory,
):
    worker_id = await _insert_worker(session_factory, status="creating")
    stale_identity = await _set_worker_cloud_login_generation(
        session_factory,
        worker_id,
        instance_id="i-old-generation",
        auth_token="old-generation-token",
    )
    current_identity = await _set_worker_cloud_login_generation(
        session_factory,
        worker_id,
        instance_id="i-new-generation",
        auth_token="new-generation-token",
    )
    provisioner = WorkerProvisioner(db_factory, cloud=FakeCloud())
    provisioner._log = AsyncMock()
    ssh = AsyncMock()
    ssh.run.side_effect = [(0, "uploaded"), (0, "login ok")]

    await provisioner._step_account_login(
        ssh,
        worker_id,
        [{
            "email": "replacement@example.com",
            "token": "mail-token",
            "password": "",
            "provider": "claude",
            "login_method": "onet",
            "status": "logged_in",
            "account_id": "default",
            CLAUDE_LOGIN_IDENTITY_KEY: stale_identity,
        }],
    )

    assert ssh.run.await_count == 2
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert worker.accounts == [{
        "email": "replacement@example.com",
        "token": "mail-token",
        "password": "",
        "provider": "claude",
        "login_method": "onet",
        "status": "logged_in",
        "account_id": "default",
        CLAUDE_LOGIN_IDENTITY_KEY: current_identity,
    }]
    assert any(
        "older Worker generation" in call.args[1]
        for call in provisioner._log.await_args_list
    )


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
    prov._step_ccm_quiesce = step("ccm-quiesce")
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
        "cloud_scope": FAKE_CLOUD_SCOPE,
        "client_token_digest": worker_create_client_token_digest(
            after_lost_response.id,
            after_lost_response.auth_token,
        ),
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

        async def find_instance_by_create_token(
            self,
            _client_token,
            *,
            include_terminated=False,
        ):
            return "i-initial"

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
        session_factory,
        status="ready",
        cloud_instance_id="i-x",
        private_ip="10.0.0.9",
        auth_token="worker-lifecycle-token",
    )
    cloud = FakeCloud(existing_instance_id="i-x")
    prov = WorkerProvisioner(db_factory=db_factory, cloud=cloud, broadcaster=None)
    prov._ssh = lambda w: AsyncMock()  # 跳过真实 SSH
    prov._step_ccm_config = AsyncMock()
    prov._step_ccm_service = AsyncMock()

    await prov.stop_worker(wid)
    async with session_factory() as db:
        assert (await db.get(Worker, wid)).status == "stopped"
    assert ("stop", "i-x") in cloud.calls

    prov._step_ssh_wait = AsyncMock()
    prov._step_ccm_quiesce = AsyncMock()
    prov._step_health_check = AsyncMock()
    prov._check_pool_accounts = AsyncMock()
    await prov.start_worker(wid)
    async with session_factory() as db:
        w = await db.get(Worker, wid)
    assert w.status == "ready"
    assert ("start", "i-x") in cloud.calls


async def test_stop_worker_requires_cloud_stopped_proof(
    db_factory, session_factory, monkeypatch,
):
    """A stop timeout/unknown outcome must never be published as stopped."""
    worker_id = await _insert_worker(
        session_factory,
        status="ready",
        cloud_instance_id="i-still-stopping",
        private_ip="10.0.0.9",
        auth_token="worker-stop-proof-token",
    )
    cloud = FakeCloud(existing_instance_id="i-still-stopping")
    cloud.describe_instance = AsyncMock(return_value={
        "instance_id": "i-still-stopping",
        "state": "stopping",
        "private_ip": "10.0.0.9",
        "public_ip": None,
        "name": "x",
    })
    provisioner = WorkerProvisioner(db_factory, cloud=cloud)
    provisioner._ssh = lambda _worker: AsyncMock()
    provisioner._step_ccm_config = AsyncMock()
    provisioner._step_ccm_service = AsyncMock()
    sleep = AsyncMock()
    monkeypatch.setattr(
        "backend.services.worker_provisioner.asyncio.sleep",
        sleep,
    )

    await provisioner.stop_worker(worker_id)

    assert cloud.describe_instance.await_count == 61
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert worker.status == "error"
    assert worker.bootstrap_step is None
    assert "未确认" in worker.bootstrap_error
    assert "stopping" in worker.bootstrap_error


async def test_stop_worker_relay_failure_still_stops_cloud_instance(
    db_factory, session_factory,
):
    worker_id = await _insert_worker(
        session_factory,
        status="ready",
        cloud_instance_id="i-relay-failure",
        private_ip="10.0.0.9",
        auth_token="worker-relay-stop-token",
    )
    cloud = FakeCloud(existing_instance_id="i-relay-failure")
    relay = AsyncMock()
    relay.stop_worker.side_effect = RuntimeError("relay unavailable")
    provisioner = WorkerProvisioner(
        db_factory,
        cloud=cloud,
        relay=relay,
    )
    ssh = AsyncMock()
    provisioner._ssh = lambda _worker: ssh
    provisioner._step_ccm_config = AsyncMock()
    provisioner._step_ccm_service = AsyncMock()

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
        auth_token="worker-start-gate-token",
    )
    provisioner = WorkerProvisioner(
        db_factory,
        cloud=FakeCloud(existing_instance_id="i-start-gate"),
    )
    provisioner._ssh = lambda _worker: AsyncMock()
    provisioner._step_ssh_wait = AsyncMock()
    provisioner._step_ccm_quiesce = AsyncMock()
    provisioner._step_ccm_config = AsyncMock()
    provisioner._step_ccm_service = AsyncMock()
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
        auth_token="worker-codex-start-token",
        accounts=[{
            "email": "codex@example.com",
            "provider": "codex",
            "token": "mail-token",
            "password": "openai-password",
            "account_id": "codex-1",
            "status": "logged_in",
        }],
        ccm_commit="abc",
    )
    provisioner = WorkerProvisioner(
        db_factory,
        cloud=FakeCloud(existing_instance_id="i-codex"),
    )
    provisioner._ssh = lambda _worker: AsyncMock()
    provisioner._step_ssh_wait = AsyncMock()
    provisioner._step_ccm_quiesce = AsyncMock()
    provisioner._step_ccm_config = AsyncMock()
    provisioner._step_ccm_service = AsyncMock()
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
    provisioner._probe_auth = AsyncMock(return_value={})
    await provisioner._health_check_worker(worker, {}, AsyncMock())
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert worker.status == "error"
    assert worker.bootstrap_step == "account-login"


async def test_start_worker_commit_mismatch_requires_redeploy_before_accounts(
    db_factory, session_factory,
):
    worker_id = await _insert_worker(
        session_factory,
        status="stopped",
        cloud_instance_id="i-stale-code",
        private_ip="10.0.0.9",
        auth_token="worker-stale-code-token",
        ccm_commit="expected-commit",
    )
    provisioner = WorkerProvisioner(
        db_factory,
        cloud=FakeCloud(existing_instance_id="i-stale-code"),
    )
    provisioner._ssh = lambda _worker: AsyncMock()
    provisioner._step_ssh_wait = AsyncMock()
    provisioner._step_ccm_quiesce = AsyncMock()
    provisioner._step_ccm_config = AsyncMock()
    provisioner._step_ccm_service = AsyncMock()
    provisioner._probe_health = AsyncMock(return_value={
        "status": "ok",
        "commit": "unexpected-remote-commit",
    })
    provisioner._probe_auth = AsyncMock(return_value={
        "ccm_node_role": "worker",
    })
    provisioner._check_pool_accounts = AsyncMock()

    await provisioner.start_worker(worker_id)

    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert worker.status == "error"
    assert worker.bootstrap_step == "health-check"
    assert "expected-commit" in worker.bootstrap_error
    assert "unexpected-remote-commit" in worker.bootstrap_error
    assert "重新部署" in worker.bootstrap_error
    assert worker.ccm_commit == "expected-commit"
    provisioner._check_pool_accounts.assert_not_awaited()


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
        status="destroying",
        cloud_instance_id="i-x",
        private_ip="10.0.0.9",
        auth_token="worker-auth-secret",
        accounts=saved_accounts,
    )
    destroy_claim = await _authorize_worker_cloud_termination(
        session_factory,
        wid,
    )
    cloud = FakeCloud(existing_instance_id="i-x")
    prov = WorkerProvisioner(db_factory=db_factory, cloud=cloud, broadcaster=None)
    await prov.destroy_worker(wid, destroy_claim=destroy_claim)

    assert ("terminate", "i-x") in cloud.calls
    async with session_factory() as db:
        worker = await db.get(Worker, wid)
    assert worker.status == "terminated"
    assert worker.auth_token is None
    assert worker.bootstrap_step is None
    assert worker.bootstrap_error is None
    assert worker.destroy_lifecycle_nonce is None
    assert worker.destroy_termination_receipt is None
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
        status="destroying",
        cloud_instance_id="i-x",
        private_ip="10.0.0.9",
        auth_token="worker-auth-secret",
        accounts=accounts,
    )
    destroy_claim = await _authorize_worker_cloud_termination(
        session_factory,
        wid,
    )
    cloud = FakeCloud(existing_instance_id="i-x")
    cloud.terminate_instance = AsyncMock(
        side_effect=[RuntimeError("AWS unavailable"), None]
    )
    prov = WorkerProvisioner(db_factory=db_factory, cloud=cloud, broadcaster=None)

    await prov.destroy_worker(wid, destroy_claim=destroy_claim)

    async with session_factory() as db:
        worker = await db.get(Worker, wid)
    assert worker.status == "error"
    assert worker.bootstrap_step == "destroy"
    assert "AWS unavailable" in worker.bootstrap_error
    assert worker.auth_token == "worker-auth-secret"
    assert worker.accounts == accounts
    assert worker.destroy_termination_receipt is not None
    assert wid in [item["id"] for item in (await client.get("/api/workers")).json()]

    # The same durable authority retries the idempotent cloud operation; it
    # does not need a live Worker or a second drain proof after an ambiguous
    # provider response.
    async with session_factory() as db:
        retrying = await db.get(Worker, wid)
        retrying.status = "destroying"
        await db.commit()
    await prov.destroy_worker(wid, destroy_claim=destroy_claim)
    assert cloud.terminate_instance.await_count == 2
    async with session_factory() as db:
        worker = await db.get(Worker, wid)
    assert worker.status == "terminated"
    assert worker.destroy_lifecycle_nonce is None
    assert worker.destroy_termination_receipt is None


async def test_provisioner_destroy_cancellation_preserves_retry_authority(
    db_factory,
    session_factory,
):
    """Cancellation may hide a cloud response, so the outbox must survive."""

    worker_id = await _insert_worker(
        session_factory,
        status="destroying",
        cloud_instance_id="i-cancelled-cloud-response",
        private_ip="10.0.0.9",
        auth_token="cancelled-destroy-secret",
    )
    destroy_claim = await _authorize_worker_cloud_termination(
        session_factory,
        worker_id,
    )
    cloud = FakeCloud(
        existing_instance_id="i-cancelled-cloud-response"
    )
    cloud.terminate_instance = AsyncMock(
        side_effect=[asyncio.CancelledError(), None]
    )
    provisioner = WorkerProvisioner(
        db_factory=db_factory,
        cloud=cloud,
        broadcaster=None,
    )

    with pytest.raises(asyncio.CancelledError):
        await provisioner.destroy_worker(
            worker_id,
            destroy_claim=destroy_claim,
        )

    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert worker.status == "destroying"
    assert worker.auth_token == "cancelled-destroy-secret"
    assert worker.destroy_lifecycle_nonce is not None
    assert worker.destroy_termination_receipt is not None

    await provisioner.destroy_worker(
        worker_id,
        destroy_claim=destroy_claim,
    )
    assert cloud.terminate_instance.await_count == 2
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert worker.status == "terminated"
    assert worker.auth_token is None
    assert worker.destroy_lifecycle_nonce is None
    assert worker.destroy_termination_receipt is None


async def test_destroy_wrong_cloud_scope_never_calls_provider(
    db_factory,
    session_factory,
):
    worker_id = await _insert_worker(
        session_factory,
        status="destroying",
        cloud_instance_id="i-wrong-scope",
        private_ip="10.0.0.9",
        auth_token="wrong-scope-worker-token",
    )
    destroy_claim = await _authorize_worker_cloud_termination(
        session_factory,
        worker_id,
    )
    cloud = FakeCloud(existing_instance_id="i-wrong-scope")
    cloud.termination_scope = AsyncMock(return_value={
        **FAKE_CLOUD_SCOPE,
        "account_id": "999999999999",
    })
    cloud.terminate_instance = AsyncMock()
    provisioner = WorkerProvisioner(db_factory, cloud=cloud)

    await provisioner.destroy_worker(
        worker_id,
        destroy_claim=destroy_claim,
    )

    cloud.terminate_instance.assert_not_awaited()
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert worker.status == "error"
    assert worker.bootstrap_step == "destroy"
    assert "authority" in worker.bootstrap_error
    assert worker.destroy_termination_receipt is not None
    assert worker.auth_token == "wrong-scope-worker-token"


async def test_destroy_authorization_rejects_client_token_instance_mismatch(
    session_factory,
):
    import backend.api.workers as workers_api

    worker_id = await _insert_worker(
        session_factory,
        status="destroying",
        cloud_instance_id="i-row-instance",
        private_ip="10.0.0.9",
        auth_token="mismatched-client-token-worker",
    )
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
        destroy_claim = capture_worker_destroy_lifecycle_claim(worker)
    cloud = FakeCloud(existing_instance_id="i-different-instance")
    provisioner = WorkerProvisioner(session_factory, cloud=cloud)

    with pytest.raises(RuntimeError, match="ClientToken"):
        await workers_api._persist_worker_destroy_termination_authorization(
            session_factory,
            provisioner=provisioner,
            destroy_claim=destroy_claim,
            proof=_clean_destroy_proof(destroy_claim),
        )

    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert worker.destroy_termination_receipt is None
    assert worker.provision_spec is None
    assert worker.status == "destroying"


async def test_legacy_worker_cloud_scope_backfill_requires_exact_client_token(
    db_factory,
    session_factory,
):
    worker_id = await _insert_worker(
        session_factory,
        status="ready",
        cloud_instance_id="i-legacy-exact",
        private_ip="10.0.0.9",
        auth_token="legacy-cloud-worker-token",
        provision_spec=None,
    )
    cloud = FakeCloud(existing_instance_id="i-legacy-exact")
    provisioner = WorkerProvisioner(db_factory, cloud=cloud)
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)

    identity = await provisioner.require_worker_cloud_identity(
        worker,
        verify_private_ip=True,
    )

    assert identity["cloud_scope"] == FAKE_CLOUD_SCOPE
    async with session_factory() as db:
        reconciled = await db.get(Worker, worker_id)
    assert reconciled.provision_spec["cloud_scope"] == FAKE_CLOUD_SCOPE
    assert reconciled.provision_spec["client_token_digest"] == (
        worker_create_client_token_digest(
            reconciled.id,
            reconciled.auth_token,
        )
    )
    assert reconciled.provision_spec["identity_reconciliation"] == {
        "version": 1,
        "method": "client_token",
        "instance_id": "i-legacy-exact",
    }


async def test_late_destroy_failure_cannot_resurrect_terminal_worker(
    db_factory,
    session_factory,
):
    worker_id = await _insert_worker(
        session_factory,
        status="destroying",
        cloud_instance_id="i-late-failure",
        private_ip="10.0.0.9",
        auth_token="late-failure-worker-token",
    )
    destroy_claim = await _authorize_worker_cloud_termination(
        session_factory,
        worker_id,
    )
    first_effect_entered = asyncio.Event()
    release_first_effect = asyncio.Event()

    class RacingCloud(FakeCloud):
        def __init__(self):
            super().__init__(existing_instance_id="i-late-failure")
            self.effect_count = 0

        async def terminate_instance(
            self,
            iid,
            *,
            allow_not_found=False,
        ):
            assert iid == "i-late-failure"
            assert allow_not_found is True
            self.effect_count += 1
            if self.effect_count == 1:
                first_effect_entered.set()
                await release_first_effect.wait()
                raise RuntimeError("late coordinator failure")

    cloud = RacingCloud()
    first = WorkerProvisioner(db_factory, cloud=cloud)
    second = WorkerProvisioner(db_factory, cloud=cloud)
    stale_coordinator = asyncio.create_task(
        first.destroy_worker(worker_id, destroy_claim=destroy_claim)
    )
    await asyncio.wait_for(first_effect_entered.wait(), timeout=1)

    await second.destroy_worker(worker_id, destroy_claim=destroy_claim)
    release_first_effect.set()
    await stale_coordinator

    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert cloud.effect_count == 2
    assert worker.status == "terminated"
    assert worker.bootstrap_step is None
    assert worker.destroy_lifecycle_nonce is None
    assert worker.destroy_termination_receipt is None
    assert worker.auth_token is None


async def test_late_destroy_success_converges_same_receipt_error_to_terminal(
    db_factory,
    session_factory,
):
    worker_id = await _insert_worker(
        session_factory,
        status="destroying",
        cloud_instance_id="i-late-success",
        private_ip="10.0.0.9",
        auth_token="late-success-worker-token",
    )
    destroy_claim = await _authorize_worker_cloud_termination(
        session_factory,
        worker_id,
    )
    first_effect_entered = asyncio.Event()
    release_first_effect = asyncio.Event()

    class RacingCloud(FakeCloud):
        def __init__(self):
            super().__init__(existing_instance_id="i-late-success")
            self.effect_count = 0

        async def terminate_instance(
            self,
            iid,
            *,
            allow_not_found=False,
        ):
            assert iid == "i-late-success"
            assert allow_not_found is True
            self.effect_count += 1
            if self.effect_count == 1:
                first_effect_entered.set()
                await release_first_effect.wait()
                return
            raise RuntimeError("competing coordinator failed")

    cloud = RacingCloud()
    first = WorkerProvisioner(db_factory, cloud=cloud)
    second = WorkerProvisioner(db_factory, cloud=cloud)
    successful_coordinator = asyncio.create_task(
        first.destroy_worker(worker_id, destroy_claim=destroy_claim)
    )
    await asyncio.wait_for(first_effect_entered.wait(), timeout=1)

    await second.destroy_worker(worker_id, destroy_claim=destroy_claim)
    async with session_factory() as db:
        retryable = await db.get(Worker, worker_id)
    assert retryable.status == "error"
    assert retryable.bootstrap_step == "destroy"
    assert retryable.destroy_termination_receipt is not None

    release_first_effect.set()
    await successful_coordinator
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert worker.status == "terminated"
    assert worker.destroy_lifecycle_nonce is None
    assert worker.destroy_termination_receipt is None


@pytest.mark.parametrize(
    ("status", "bootstrap_step", "expected_detail"),
    [
        ("stopped", None, "先启动"),
        ("error", None, "ClientToken"),
    ],
)
async def test_fresh_destroy_rejects_unreachable_or_unreconciled_worker(
    client,
    session_factory,
    fake_provisioner,
    status,
    bootstrap_step,
    expected_detail,
):
    worker_id = await _insert_worker(
        session_factory,
        status=status,
        bootstrap_step=bootstrap_step,
        cloud_instance_id="i-not-ready-for-destroy",
        private_ip="10.0.0.9",
        auth_token="not-ready-worker-token",
    )

    response = await client.post(f"/api/workers/{worker_id}/destroy")

    assert response.status_code == 409
    assert expected_detail in response.json()["detail"]
    fake_provisioner.destroy_worker.assert_not_awaited()
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert worker.status == status
    assert worker.destroy_lifecycle_nonce is None
    assert worker.destroy_termination_receipt is None


async def test_health_check_marks_error_and_recovers(db_factory, session_factory, monkeypatch):
    wid = await _insert_worker(
        session_factory, status="ready", private_ip="10.0.0.9", auth_token="t",
        ccm_commit="abc123",
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
    # Node-role/namespace authentication is covered independently; this test
    # isolates health degradation/recovery and exact commit matching.
    prov._probe_auth = AsyncMock(return_value={})
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
        ccm_commit="expected-commit",
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
    provisioner._probe_health = AsyncMock(return_value={"commit": "expected-commit"})
    provisioner._probe_auth = AsyncMock(return_value={})
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
    assert worker.ccm_commit == "expected-commit"
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
    prov._step_ccm_config = AsyncMock()
    prov._step_ccm_service = AsyncMock()
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
    ("action", "provisioner_method"),
    (("start", "start_worker"), ("stop", "stop_worker")),
)
async def test_bootstrap_failure_rejects_non_retry_lifecycle_actions(
    client,
    session_factory,
    fake_provisioner,
    action,
    provisioner_method,
):
    worker_id = await _insert_worker(
        session_factory,
        status="error",
        bootstrap_step="account-login",
    )

    response = await client.post(f"/api/workers/{worker_id}/{action}")

    assert response.status_code == 409
    assert "只能使用 retry" in response.json()["detail"]
    getattr(fake_provisioner, provisioner_method).assert_not_awaited()
    async with session_factory() as db:
        worker = await db.get(Worker, worker_id)
    assert worker.status == "error"
    assert worker.bootstrap_step == "account-login"


async def test_health_degraded_error_can_still_start(
    client,
    session_factory,
    fake_provisioner,
):
    worker_id = await _insert_worker(
        session_factory,
        status="error",
        bootstrap_step=None,
    )

    response = await client.post(f"/api/workers/{worker_id}/start")

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "starting"
    await asyncio.sleep(0)
    fake_provisioner.start_worker.assert_awaited_once_with(worker_id)


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
        async def _simple_destroy(
            prov,
            worker_id,
            _destroy_claim,
            db_factory_arg=None,
        ):
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
