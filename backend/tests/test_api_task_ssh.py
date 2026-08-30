import asyncio
from pathlib import Path

import pytest
import pytest_asyncio
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from backend.models.task_ssh_grant import TaskSSHGrant
from backend.models.task_ssh_effect import TaskSSHEffectReceipt
from backend.models.test_harness import TestHarnessRun
from backend.models.ssh_profile import SSHProfile
from backend.services.ssh_executor import (
    SSHCommandResult,
    derive_openssh_public_key,
)
from backend.services.task_ssh_access import (
    TaskSSHAccessError,
    prepare_task_ssh_grants,
    task_ssh_runtime_policy,
)
from backend.services.ssh_profiles import validated_profile_material
from backend.schemas.task_ssh_grant import TaskSSHExecuteRequest
from backend.models.task import Task
from backend.config import settings


def _effect_id(value: int) -> str:
    return f"{value:032x}"


@pytest_asyncio.fixture(autouse=True)
async def _authenticated_managed_ssh_api(client, monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "auth_token", "managed-ssh-test-token")
    monkeypatch.setattr(
        settings,
        "ssh_key_storage_dir",
        str(tmp_path / "task-ssh-key-store"),
    )
    client.headers["Authorization"] = "Bearer managed-ssh-test-token"
    yield


def _private_key_file(tmp_path: Path) -> Path:
    private_key = ed25519.Ed25519PrivateKey.generate()
    managed = Path(settings.ssh_key_storage_dir) / "managed"
    managed.mkdir(mode=0o700, parents=True, exist_ok=True)
    path = managed / "task-ssh-key"
    path.write_bytes(private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.OpenSSH,
        encryption_algorithm=serialization.NoEncryption(),
    ))
    path.chmod(0o600)
    return path


def _external_private_key_file(tmp_path: Path) -> Path:
    private_key = ed25519.Ed25519PrivateKey.generate()
    path = tmp_path / "external-task-ssh-key"
    path.write_bytes(private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.OpenSSH,
        encryption_algorithm=serialization.NoEncryption(),
    ))
    path.chmod(0o600)
    return path


async def _upload_private_key(client, source: Path) -> str:
    response = await client.post(
        "/api/ssh-profiles/upload-key",
        files={
            "file": (
                source.name,
                source.read_bytes(),
                "application/octet-stream",
            )
        },
    )
    assert response.status_code == 200, response.text
    return response.json()["upload_token"]


async def _create_profile(client, tmp_path: Path) -> tuple[int, Path]:
    source_key = _private_key_file(tmp_path)
    host_key = derive_openssh_public_key(source_key)
    upload_token = await _upload_private_key(client, source_key)
    response = await client.post("/api/ssh-profiles", json={
        "name": "task-target",
        "host": "ssh.task.internal",
        "username": "deploy",
        "key_upload_token": upload_token,
        "host_key_value": host_key,
        "task_access_enabled": True,
        "task_capabilities": ["exec", "read", "write"],
    })
    assert response.status_code == 201, response.text
    managed_key = (
        Path(settings.ssh_key_storage_dir) / "managed" / upload_token
    )
    assert managed_key.is_file()
    return response.json()["id"], managed_key


@pytest.mark.asyncio
async def test_external_profile_cannot_be_granted_or_used_after_dirty_enable(
    client,
    session_factory,
    tmp_path,
):
    key_path = _external_private_key_file(tmp_path)
    host_key = derive_openssh_public_key(key_path)
    material = validated_profile_material(
        key_path=str(key_path),
        host_key_value=host_key,
    )
    async with session_factory() as db:
        profile = SSHProfile(
            name="grandfathered-dirty",
            host="ssh.legacy.internal",
            username="deploy",
            task_access_enabled=True,
            task_capabilities=["read"],
            allowed_roots=["/"],
            **material,
        )
        db.add(profile)
        await db.commit()
        profile_id = profile.id

    rejected = await client.post("/api/tasks", json={
        "description": "must not grant an external Profile key",
        "ssh_grants": [{"profile_id": profile_id, "capabilities": ["read"]}],
    })
    assert rejected.status_code == 409
    assert "rotate its private key into CCM managed storage" in rejected.text

    created = await client.post("/api/tasks", json={
        "description": "dirty database grant must remain broker-only",
    })
    assert created.status_code == 201, created.text
    task_id = created.json()["id"]
    async with session_factory() as db:
        db.add(TaskSSHGrant(
            task_id=task_id,
            ssh_profile_id=profile_id,
            profile_revision=1,
            capabilities=["read"],
        ))
        await db.commit()

    snapshot = await client.get(f"/api/tasks/{task_id}/ssh-grants")
    assert snapshot.status_code == 200, snapshot.text
    assert snapshot.json()[0]["valid"] is False
    assert snapshot.json()[0]["invalid_reason"] == "profile_key_not_managed"

    denied = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/read",
        json={"path": "/etc/hostname"},
    )
    assert denied.status_code == 409
    assert "profile_key_not_managed" in denied.text

    async with session_factory() as db:
        task = await db.get(Task, task_id)
        policy = await task_ssh_runtime_policy(db, task)
    assert policy.broker_only is True
    assert policy.capabilities == frozenset()


@pytest.mark.asyncio
async def test_managed_ssh_http_surfaces_fail_closed_without_auth_token(
    client,
    monkeypatch,
):
    monkeypatch.setattr(settings, "auth_token", "")
    client.headers.pop("Authorization", None)

    responses = [
        await client.get("/api/ssh-profiles"),
        await client.post(
            "/api/ssh-profiles/upload-key",
            files={"file": ("id_ed25519", b"not-a-key")},
        ),
        await client.post(
            "/api/tasks",
            json={
                "description": "must not receive a Manager SSH key",
                "ssh_grants": [{
                    "profile_id": 1,
                    "capabilities": ["read"],
                }],
            },
        ),
        await client.put(
            "/api/tasks/1/ssh-grants",
            json={"grants": []},
        ),
        await client.get("/api/tasks/1/ssh-access"),
        await client.post(
            "/api/files/ssh/1/list",
            json={"path": "/"},
        ),
    ]

    assert {response.status_code for response in responses} == {503}
    assert {
        response.json()["detail"] for response in responses
    } == {"Managed SSH requires AUTH_TOKEN to be configured"}
    ordinary = await client.get("/api/tasks/count")
    assert ordinary.status_code == 200


@pytest.mark.asyncio
async def test_files_only_profile_cannot_be_granted_to_task(client, tmp_path):
    key_path = _private_key_file(tmp_path)
    host_key = derive_openssh_public_key(key_path)
    upload_token = await _upload_private_key(client, key_path)
    profile = await client.post("/api/ssh-profiles", json={
        "name": "files-only",
        "host": "ssh.files.internal",
        "username": "reader",
        "key_upload_token": upload_token,
        "host_key_value": host_key,
    })
    assert profile.status_code == 201, profile.text
    assert profile.json()["task_access_enabled"] is False
    assert profile.json()["task_capabilities"] == []

    eligible = await client.get(
        "/api/ssh-profiles?task_eligible_only=true"
    )
    assert eligible.status_code == 200
    assert eligible.json() == []

    task = await client.post("/api/tasks", json={
        "description": "Try to use a Files-only connection",
        "ssh_grants": [{
            "profile_id": profile.json()["id"],
            "capabilities": ["read"],
        }],
    })
    assert task.status_code == 409
    assert "available only in Files" in task.text


@pytest.mark.asyncio
async def test_task_grant_cannot_exceed_profile_policy(client, tmp_path):
    key_path = _private_key_file(tmp_path)
    host_key = derive_openssh_public_key(key_path)
    upload_token = await _upload_private_key(client, key_path)
    profile = await client.post("/api/ssh-profiles", json={
        "name": "read-only-tasks",
        "host": "ssh.read.internal",
        "username": "reader",
        "key_upload_token": upload_token,
        "host_key_value": host_key,
        "task_access_enabled": True,
        "task_capabilities": ["read"],
    })
    assert profile.status_code == 201, profile.text

    denied = await client.post("/api/tasks", json={
        "description": "Run a command",
        "ssh_grants": [{
            "profile_id": profile.json()["id"],
            "capabilities": ["exec"],
        }],
    })
    assert denied.status_code == 422
    assert "does not allow Task capabilities: exec" in denied.text

    allowed = await client.post("/api/tasks", json={
        "description": "Read a file",
        "ssh_grants": [{
            "profile_id": profile.json()["id"],
            "capabilities": ["read"],
        }],
    })
    assert allowed.status_code == 201, allowed.text
    snapshot = await client.get(
        f"/api/tasks/{allowed.json()['id']}/ssh-grants"
    )
    assert snapshot.json()[0]["profile_task_access_enabled"] is True
    assert snapshot.json()[0]["profile_task_capabilities"] == ["read"]


@pytest.mark.asyncio
async def test_profile_policy_change_invalidates_existing_grant(client, tmp_path):
    profile_id, _ = await _create_profile(client, tmp_path)
    created = await client.post("/api/tasks", json={
        "description": "Inspect remote files",
        "ssh_grants": [{"profile_id": profile_id, "capabilities": ["read"]}],
    })
    assert created.status_code == 201, created.text

    changed = await client.put(f"/api/ssh-profiles/{profile_id}", json={
        "expected_revision": 1,
        "task_access_enabled": False,
        "task_capabilities": [],
    })
    assert changed.status_code == 200, changed.text
    assert changed.json()["revision"] == 2

    snapshot = await client.get(
        f"/api/tasks/{created.json()['id']}/ssh-grants"
    )
    assert snapshot.json()[0]["valid"] is False
    assert snapshot.json()[0]["invalid_reason"] == "profile_task_access_disabled"


@pytest.mark.asyncio
async def test_second_profile_security_change_invalidates_reauthorized_grant(
    client,
    tmp_path,
):
    profile_id, _ = await _create_profile(client, tmp_path)
    created = await client.post("/api/tasks", json={
        "description": "Read within the current approved root",
        "ssh_grants": [{"profile_id": profile_id, "capabilities": ["read"]}],
    })
    task_id = created.json()["id"]

    first_change = await client.put(
        f"/api/ssh-profiles/{profile_id}",
        json={"expected_revision": 1, "allowed_roots": ["/srv/one"]},
    )
    assert first_change.json()["revision"] == 2
    reauthorized = await client.put(f"/api/tasks/{task_id}/ssh-grants", json={
        "grants": [{"profile_id": profile_id, "capabilities": ["read"]}],
    })
    assert reauthorized.json()[0]["profile_revision"] == 2
    assert reauthorized.json()[0]["valid"] is True

    second_change = await client.put(
        f"/api/ssh-profiles/{profile_id}",
        json={"expected_revision": 2, "allowed_roots": ["/srv/two"]},
    )
    assert second_change.json()["revision"] == 3
    snapshot = await client.get(f"/api/tasks/{task_id}/ssh-grants")
    assert snapshot.json()[0]["profile_revision"] == 2
    assert snapshot.json()[0]["current_profile_revision"] == 3
    assert snapshot.json()[0]["valid"] is False
    assert snapshot.json()[0]["invalid_reason"] == "profile_revision_changed"


@pytest.mark.asyncio
async def test_task_create_atomically_snapshots_ssh_grant(client, tmp_path):
    profile_id, _ = await _create_profile(client, tmp_path)

    created = await client.post("/api/tasks", json={
        "title": "remote check",
        "description": "Inspect the remote service",
        "ssh_grants": [{
            "profile_id": profile_id,
            "capabilities": ["exec", "read", "exec"],
        }],
    })

    assert created.status_code == 201, created.text
    task_id = created.json()["id"]
    listed = await client.get(f"/api/tasks/{task_id}/ssh-grants")
    assert listed.status_code == 200
    grant = listed.json()[0]
    assert {
        "task_id": task_id,
        "profile_id": profile_id,
        "profile_name": "task-target",
        "host": "ssh.task.internal",
        "port": 22,
        "username": "deploy",
        "host_key_fingerprint": listed.json()[0]["host_key_fingerprint"],
        "profile_revision": 1,
        "current_profile_revision": 1,
        "capabilities": ["exec", "read"],
        "valid": True,
        "invalid_reason": None,
        "created_by": None,
    }.items() <= grant.items()
    serialized = listed.text
    assert "key_path" not in serialized
    assert "host_key_value" not in serialized


@pytest.mark.asyncio
async def test_task_delete_removes_ssh_grants(
    client,
    session_factory,
    tmp_path,
):
    profile_id, _ = await _create_profile(client, tmp_path)
    created = await client.post("/api/tasks", json={
        "description": "Temporary remote inspection",
        "ssh_grants": [{"profile_id": profile_id, "capabilities": ["read"]}],
    })
    assert created.status_code == 201, created.text
    task_id = created.json()["id"]

    deleted = await client.delete(f"/api/tasks/{task_id}")
    assert deleted.status_code == 200, deleted.text

    async with session_factory() as db:
        remaining = list((await db.execute(
            select(TaskSSHGrant).where(TaskSSHGrant.task_id == task_id)
        )).scalars())
    assert remaining == []

    recreated = await client.post("/api/tasks", json={
        "description": "Another remote inspection",
        "ssh_grants": [{"profile_id": profile_id, "capabilities": ["read"]}],
    })
    assert recreated.status_code == 201, recreated.text


@pytest.mark.asyncio
async def test_task_ssh_execute_fails_closed_after_profile_revision_change(
    client,
    tmp_path,
    monkeypatch,
):
    profile_id, _ = await _create_profile(client, tmp_path)
    created = await client.post("/api/tasks", json={
        "description": "Run a remote health check",
        "ssh_grants": [{"profile_id": profile_id, "capabilities": ["exec"]}],
    })
    task_id = created.json()["id"]
    observed = []

    class FakeExecutor:
        async def run_result(self, command, **kwargs):
            observed.append((command, kwargs))
            return SSHCommandResult(
                exit_code=0,
                stdout="healthy\n",
                stderr="",
                truncated=False,
                duration_ms=12,
            )

    monkeypatch.setattr(
        "backend.api.task_ssh.executor_for_profile",
        lambda _profile: FakeExecutor(),
    )
    first = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/execute",
        json={
            "effect_id": _effect_id(1),
            "command": "systemctl is-active app",
            "timeout_seconds": 20,
        },
    )
    assert first.status_code == 200
    assert first.json()["stdout"] == "healthy\n"
    assert observed == [(
        "systemctl is-active app",
        {
            "timeout": 20,
            "max_output_bytes": 64 * 1024,
            "sensitive": True,
        },
    )]

    changed = await client.put(
        f"/api/ssh-profiles/{profile_id}",
        json={"expected_revision": 1, "username": "release"},
    )
    assert changed.status_code == 200
    assert changed.json()["revision"] == 2

    stale = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/execute",
        json={"effect_id": _effect_id(2), "command": "hostname"},
    )
    assert stale.status_code == 409
    assert "profile_revision_changed" in stale.text
    access = await client.get(f"/api/tasks/{task_id}/ssh-access")
    assert access.json()[0]["valid"] is False
    assert access.json()[0]["invalid_reason"] == "profile_revision_changed"

    refreshed = await client.put(f"/api/tasks/{task_id}/ssh-grants", json={
        "grants": [{"profile_id": profile_id, "capabilities": ["exec"]}],
    })
    assert refreshed.status_code == 200
    assert refreshed.json()[0]["profile_revision"] == 2
    assert refreshed.json()[0]["valid"] is True


@pytest.mark.asyncio
async def test_task_ssh_execute_requires_explicit_exec_capability(
    client,
    tmp_path,
    monkeypatch,
):
    profile_id, _ = await _create_profile(client, tmp_path)
    created = await client.post("/api/tasks", json={
        "description": "Read remote configuration",
        "ssh_grants": [{"profile_id": profile_id, "capabilities": ["read"]}],
    })
    task_id = created.json()["id"]
    monkeypatch.setattr(
        "backend.api.task_ssh.executor_for_profile",
        lambda _profile: (_ for _ in ()).throw(AssertionError("must not connect")),
    )

    response = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/execute",
        json={
            "effect_id": _effect_id(1),
            "command": "cat /etc/app.conf",
        },
    )

    assert response.status_code == 403
    assert "does not allow exec" in response.text


@pytest.mark.asyncio
async def test_duplicate_task_ssh_grants_reject_before_task_creation(
    client,
    tmp_path,
):
    profile_id, _ = await _create_profile(client, tmp_path)
    before = (await client.get("/api/tasks/count")).json()["total"]

    response = await client.post("/api/tasks", json={
        "description": "Invalid duplicate authorization",
        "ssh_grants": [
            {"profile_id": profile_id, "capabilities": ["exec"]},
            {"profile_id": profile_id, "capabilities": ["read"]},
        ],
    })

    assert response.status_code == 422
    after = (await client.get("/api/tasks/count")).json()["total"]
    assert after == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("worker_id", "shared_from_id", "metadata"),
    [
        (9, None, None),
        (None, 73, None),
        (None, None, {"ccm_worker_managed_task": True}),
        (None, None, {"ccm_user_skill_snapshots": []}),
        (None, None, {"isolated_browser_agent": True}),
        (None, None, {"frontend_review": {"enabled": True}}),
    ],
)
async def test_nonlocal_task_scope_cannot_receive_manager_local_ssh_grant(
    db_session,
    worker_id,
    shared_from_id,
    metadata,
):
    with pytest.raises(TaskSSHAccessError, match="local, unshared Manager Tasks"):
        await prepare_task_ssh_grants(
            db_session,
            [{"profile_id": 1, "capabilities": ["exec"]}],
            worker_id=worker_id,
            shared_from_id=shared_from_id,
            metadata=metadata,
        )


@pytest.mark.parametrize(
    ("scope_field", "scope_value", "invalid_reason"),
    [
        ("shared_from_id", 73, "task_shared"),
        ("metadata_", {"ccm_worker_managed_task": True}, "task_worker_managed"),
        (
            "metadata_",
            {"isolated_browser_agent": True},
            "task_isolated_browser_agent",
        ),
        (
            "metadata_",
            {"frontend_review": {"enabled": True}},
            "task_frontend_review",
        ),
    ],
)
@pytest.mark.asyncio
async def test_existing_grant_fails_closed_in_nonlocal_task_scope(
    client,
    db_session,
    tmp_path,
    monkeypatch,
    scope_field,
    scope_value,
    invalid_reason,
):
    profile_id, _ = await _create_profile(client, tmp_path)
    created = await client.post("/api/tasks", json={
        "description": "Inspect a remote service",
        "ssh_grants": [{"profile_id": profile_id, "capabilities": ["exec"]}],
    })
    task_id = created.json()["id"]
    task = await db_session.get(Task, task_id)
    setattr(task, scope_field, scope_value)
    await db_session.commit()
    monkeypatch.setattr(
        "backend.api.task_ssh.executor_for_profile",
        lambda _profile: (_ for _ in ()).throw(AssertionError("must not connect")),
    )

    snapshot = await client.get(f"/api/tasks/{task_id}/ssh-access")
    executed = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/execute",
        json={"effect_id": _effect_id(1), "command": "hostname"},
    )
    replaced = await client.put(f"/api/tasks/{task_id}/ssh-grants", json={
        "grants": [{"profile_id": profile_id, "capabilities": ["exec"]}],
    })

    assert snapshot.status_code == 200
    assert snapshot.json()[0]["valid"] is False
    assert snapshot.json()[0]["invalid_reason"] == invalid_reason
    assert executed.status_code == 409
    assert invalid_reason in executed.text
    assert replaced.status_code == 409


@pytest.mark.asyncio
async def test_active_browser_harness_blocks_late_managed_ssh_grant(
    client,
    db_session,
    tmp_path,
):
    profile_id, _ = await _create_profile(client, tmp_path)
    created = await client.post(
        "/api/tasks",
        json={"description": "Browser lifecycle owns this Task"},
    )
    assert created.status_code == 201, created.text
    task_id = created.json()["id"]
    task = await db_session.get(Task, task_id)
    assert task is not None
    run_id = "a" * 32
    db_session.add(
        TestHarnessRun(
            id=run_id,
            task_id=task_id,
            owner_task_incarnation_id=task.incarnation_id,
            owner_task_retry_count=task.retry_count,
            owner_task_turn_generation=task.turn_generation,
            owner_task_status=task.status,
            target_kind="fixed_url",
            target_spec={"kind": "fixed_url", "url": "https://example.com"},
            test_plan={"objective": "Inspect"},
            runtime_config={},
            request_fingerprint="b" * 64,
            root_run_id=run_id,
            attempt_number=1,
            status="running",
            stage="browser_ready",
            cleanup_status="pending",
        )
    )
    await db_session.commit()

    response = await client.put(
        f"/api/tasks/{task_id}/ssh-grants",
        json={
            "grants": [
                {"profile_id": profile_id, "capabilities": ["exec"]}
            ]
        },
    )

    assert response.status_code == 409
    assert "active Browser Review or Test Harness" in response.text
    assert (
        await db_session.scalar(
            select(TaskSSHGrant.id).where(TaskSSHGrant.task_id == task_id)
        )
        is None
    )


@pytest.mark.asyncio
async def test_task_ssh_fails_closed_when_profile_key_file_is_replaced(
    client,
    tmp_path,
):
    profile_id, key_path = await _create_profile(client, tmp_path)
    created = await client.post("/api/tasks", json={
        "description": "Inspect a remote service",
        "ssh_grants": [{"profile_id": profile_id, "capabilities": ["exec"]}],
    })
    task_id = created.json()["id"]
    replacement = ed25519.Ed25519PrivateKey.generate()
    key_path.write_bytes(replacement.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.OpenSSH,
        encryption_algorithm=serialization.NoEncryption(),
    ))
    key_path.chmod(0o600)

    executed = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/execute",
        json={"effect_id": _effect_id(1), "command": "hostname"},
    )

    assert executed.status_code == 409
    assert "private key is no longer usable" in executed.text
    assert str(key_path) not in executed.text

@pytest.mark.asyncio
async def test_task_ssh_read_and_write_operations_enforce_capabilities(
    client,
    tmp_path,
    monkeypatch,
):
    profile_id, _ = await _create_profile(client, tmp_path)
    created = await client.post("/api/tasks", json={
        "description": "Update a remote configuration file",
        "ssh_grants": [{
            "profile_id": profile_id,
            "capabilities": ["read", "write"],
        }],
    })
    task_id = created.json()["id"]
    monkeypatch.setattr(
        "backend.api.task_ssh._list_directory_sync",
        lambda _profile, path: (path, [{
            "name": "app.conf",
            "path": f"{path}/app.conf",
            "is_dir": False,
            "size": 18,
        }], False),
    )
    monkeypatch.setattr(
        "backend.api.task_ssh._read_file_sync",
        lambda _profile, path, max_bytes: (
            path,
            "PORT=8000\n",
            10,
            max_bytes < 10,
        ),
    )
    observed_write = []
    monkeypatch.setattr(
        "backend.api.task_ssh._write_file_sync",
        lambda _profile, path, content, overwrite: observed_write.append(
            (path, content, overwrite)
        ) or (path, len(content.encode())),
    )

    listed = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/list",
        json={"path": "/etc/app"},
    )
    read = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/read",
        json={"path": "/etc/app/app.conf", "max_bytes": 1024},
    )
    written = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/write",
        json={
            "effect_id": _effect_id(1),
            "path": "/etc/app/app.conf",
            "content": "PORT=9000\n",
            "overwrite": True,
        },
    )

    assert listed.status_code == 200
    assert listed.json()["entries"][0]["name"] == "app.conf"
    assert read.status_code == 200
    assert read.json()["content"] == "PORT=8000\n"
    assert written.status_code == 200
    assert written.json()["bytes_written"] == 10
    assert observed_write == [("/etc/app/app.conf", "PORT=9000\n", True)]

    exec_denied = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/execute",
        json={"effect_id": _effect_id(2), "command": "hostname"},
    )
    assert exec_denied.status_code == 403


@pytest.mark.asyncio
async def test_task_ssh_completed_effect_replays_after_lost_ack_and_survives_delete(
    client,
    session_factory,
    tmp_path,
    monkeypatch,
):
    profile_id, _ = await _create_profile(client, tmp_path)
    created = await client.post("/api/tasks", json={
        "description": "Run exactly one remote deployment check",
        "ssh_grants": [{"profile_id": profile_id, "capabilities": ["exec"]}],
    })
    task_id = created.json()["id"]
    calls = 0

    class FakeExecutor:
        async def run_result(self, _command, **_kwargs):
            nonlocal calls
            calls += 1
            return SSHCommandResult(
                exit_code=0,
                stdout="ok\n",
                stderr="",
                truncated=False,
                duration_ms=7,
            )

    monkeypatch.setattr(
        "backend.api.task_ssh.executor_for_profile",
        lambda _profile: FakeExecutor(),
    )
    payload = {
        "effect_id": _effect_id(100),
        "command": "deploy --once",
    }

    # Treat the first successful HTTP response as an ACK that was lost by the
    # caller, then submit the exact same stable effect id again.
    first = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/execute",
        json=payload,
    )
    replay = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/execute",
        json=payload,
    )

    assert first.status_code == replay.status_code == 200
    assert first.json()["replayed"] is False
    assert replay.json()["replayed"] is True
    assert replay.json()["effect_id"] == payload["effect_id"]
    assert replay.json()["stdout"] == "ok\n"
    assert calls == 1

    deleted = await client.delete(f"/api/tasks/{task_id}")
    assert deleted.status_code == 200, deleted.text
    deleted_profile = await client.delete(
        f"/api/ssh-profiles/{profile_id}",
        params={"expected_revision": 1},
    )
    assert deleted_profile.status_code == 200, deleted_profile.text
    async with session_factory() as db:
        receipt = await db.scalar(select(TaskSSHEffectReceipt).where(
            TaskSSHEffectReceipt.task_id == task_id,
            TaskSSHEffectReceipt.effect_id == payload["effect_id"],
        ))
    assert receipt is not None
    assert receipt.status == "completed"
    assert receipt.result_payload["stdout"] == "ok\n"
    assert not hasattr(receipt, "command")


@pytest.mark.asyncio
async def test_task_ssh_same_effect_id_conflicting_digest_is_rejected(
    client,
    tmp_path,
    monkeypatch,
):
    profile_id, _ = await _create_profile(client, tmp_path)
    created = await client.post("/api/tasks", json={
        "description": "Bind one SSH effect id",
        "ssh_grants": [{"profile_id": profile_id, "capabilities": ["exec"]}],
    })
    task_id = created.json()["id"]
    calls = 0

    class FakeExecutor:
        async def run_result(self, _command, **_kwargs):
            nonlocal calls
            calls += 1
            return SSHCommandResult(0, "", "", False, 1)

    monkeypatch.setattr(
        "backend.api.task_ssh.executor_for_profile",
        lambda _profile: FakeExecutor(),
    )
    effect_id = _effect_id(101)
    first = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/execute",
        json={"effect_id": effect_id, "command": "first-command"},
    )
    conflict = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/execute",
        json={"effect_id": effect_id, "command": "different-command"},
    )

    assert first.status_code == 200
    assert conflict.status_code == 409
    assert conflict.json()["detail"]["effect_id"] == effect_id
    assert "different request" in conflict.text
    assert calls == 1


@pytest.mark.asyncio
async def test_task_ssh_concurrent_same_effect_executes_once_and_replays(
    client,
    tmp_path,
    monkeypatch,
):
    profile_id, _ = await _create_profile(client, tmp_path)
    created = await client.post("/api/tasks", json={
        "description": "Coalesce a concurrent SSH mutation",
        "ssh_grants": [{"profile_id": profile_id, "capabilities": ["exec"]}],
    })
    task_id = created.json()["id"]
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    class FakeExecutor:
        async def run_result(self, _command, **_kwargs):
            nonlocal calls
            calls += 1
            started.set()
            await release.wait()
            return SSHCommandResult(0, "once\n", "", False, 2)

    monkeypatch.setattr(
        "backend.api.task_ssh.executor_for_profile",
        lambda _profile: FakeExecutor(),
    )
    payload = {"effect_id": _effect_id(102), "command": "one-effect"}
    first = asyncio.create_task(client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/execute",
        json=payload,
    ))
    await asyncio.wait_for(started.wait(), timeout=2)
    duplicate = asyncio.create_task(client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/execute",
        json=payload,
    ))
    await asyncio.sleep(0.05)
    release.set()
    first_response, duplicate_response = await asyncio.gather(first, duplicate)
    settled_replay = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/execute",
        json=payload,
    )

    assert first_response.status_code == 200
    # SQLite's in-memory StaticPool lets the duplicate observe ``running``
    # immediately; separate-connection databases wait on the writer fence and
    # then return a completed replay. Both outcomes forbid a second effect.
    assert duplicate_response.status_code in {200, 409}
    if duplicate_response.status_code == 200:
        assert duplicate_response.json()["replayed"] is True
    else:
        assert duplicate_response.json()["detail"]["effect_status"] == "running"
    assert settled_replay.status_code == 200
    assert settled_replay.json()["replayed"] is True
    assert calls == 1


@pytest.mark.asyncio
async def test_task_ssh_exception_is_ambiguous_and_new_id_cannot_bypass(
    client,
    session_factory,
    tmp_path,
    monkeypatch,
):
    profile_id, _ = await _create_profile(client, tmp_path)
    created = await client.post("/api/tasks", json={
        "description": "Do not replay an uncertain command",
        "ssh_grants": [{"profile_id": profile_id, "capabilities": ["exec"]}],
    })
    task_id = created.json()["id"]
    calls = 0
    secret_marker = "command-secret-and-private-key-path"

    class FakeExecutor:
        async def run_result(self, _command, **_kwargs):
            nonlocal calls
            calls += 1
            raise RuntimeError(secret_marker)

    monkeypatch.setattr(
        "backend.api.task_ssh.executor_for_profile",
        lambda _profile: FakeExecutor(),
    )
    original_id = _effect_id(103)
    request_body = {"effect_id": original_id, "command": "unknown-effect"}
    failed = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/execute",
        json=request_body,
    )
    same_id = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/execute",
        json=request_body,
    )
    replacement_id = _effect_id(104)
    replacement = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/execute",
        json={
            **request_body,
            "effect_id": replacement_id,
            "timeout_seconds": 180,
            "max_output_bytes": 4096,
        },
    )

    assert failed.status_code == 400
    assert failed.json()["detail"]["effect_status"] == "ambiguous"
    assert secret_marker not in failed.text
    assert same_id.status_code == 409
    assert same_id.json()["detail"]["effect_status"] == "ambiguous"
    assert replacement.status_code == 409
    assert replacement.json()["detail"]["existing_effect_id"] == original_id
    assert calls == 1
    async with session_factory() as db:
        receipt = await db.scalar(select(TaskSSHEffectReceipt).where(
            TaskSSHEffectReceipt.effect_id == original_id,
        ))
    assert receipt.status == "ambiguous"
    assert receipt.outcome_code == "remote_outcome_unknown"
    assert receipt.result_payload is None
    assert secret_marker not in repr(receipt.__dict__)


@pytest.mark.asyncio
async def test_task_ssh_cancellation_persists_ambiguous_receipt(
    client,
    session_factory,
    tmp_path,
    monkeypatch,
):
    profile_id, _ = await _create_profile(client, tmp_path)
    created = await client.post("/api/tasks", json={
        "description": "Cancel an in-flight SSH request safely",
        "ssh_grants": [{"profile_id": profile_id, "capabilities": ["exec"]}],
    })
    task_id = created.json()["id"]
    started = asyncio.Event()

    class FakeExecutor:
        async def run_result(self, _command, **_kwargs):
            started.set()
            await asyncio.Event().wait()

    monkeypatch.setattr(
        "backend.api.task_ssh.executor_for_profile",
        lambda _profile: FakeExecutor(),
    )
    effect_id = _effect_id(105)
    operation = asyncio.create_task(client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/execute",
        json={"effect_id": effect_id, "command": "possibly-started"},
    ))
    await asyncio.wait_for(started.wait(), timeout=2)
    operation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await operation

    async with session_factory() as db:
        receipt = await db.scalar(select(TaskSSHEffectReceipt).where(
            TaskSSHEffectReceipt.effect_id == effect_id,
        ))
    assert receipt.status == "ambiguous"
    assert receipt.outcome_code == "remote_outcome_unknown"

    retry = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/execute",
        json={"effect_id": effect_id, "command": "possibly-started"},
    )
    assert retry.status_code == 409
    assert retry.json()["detail"]["effect_status"] == "ambiguous"


@pytest.mark.asyncio
async def test_task_ssh_sqlite_permit_blocks_generation_drift_before_effect(
    client,
    session_factory,
    tmp_path,
    monkeypatch,
):
    profile_id, _ = await _create_profile(client, tmp_path)
    created = await client.post("/api/tasks", json={
        "description": "Fence an SSH effect to one Task generation",
        "ssh_grants": [{"profile_id": profile_id, "capabilities": ["exec"]}],
    })
    task_id = created.json()["id"]
    from backend.api import task_ssh as task_ssh_api

    original_prepare = task_ssh_api._prepare_admitted_effect
    drift_blocked = False
    remote_calls = 0

    async def drift_generation(db, admission, **kwargs):
        nonlocal drift_blocked
        async with session_factory() as other_db:
            task = await other_db.get(Task, task_id)
            task.retry_count += 1
            try:
                await other_db.commit()
            except IntegrityError:
                drift_blocked = True
                await other_db.rollback()
        return await original_prepare(db, admission, **kwargs)

    class FakeExecutor:
        async def run_result(self, _command, **_kwargs):
            nonlocal remote_calls
            remote_calls += 1
            return SSHCommandResult(0, "same-generation\n", "", False, 1)

    monkeypatch.setattr(
        task_ssh_api,
        "_prepare_admitted_effect",
        drift_generation,
    )
    monkeypatch.setattr(
        task_ssh_api,
        "executor_for_profile",
        lambda _profile: FakeExecutor(),
    )
    effect_id = _effect_id(106)
    response = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/execute",
        json={"effect_id": effect_id, "command": "must-not-run"},
    )

    assert response.status_code == 200
    assert drift_blocked is True
    assert remote_calls == 1
    async with session_factory() as db:
        receipt = await db.scalar(select(TaskSSHEffectReceipt).where(
            TaskSSHEffectReceipt.effect_id == effect_id,
        ))
        task = await db.get(Task, task_id)
    assert receipt.status == "completed"
    assert task.retry_count == receipt.task_retry_count == 0


@pytest.mark.asyncio
async def test_task_ssh_write_receipt_replays_without_retaining_content(
    client,
    session_factory,
    tmp_path,
    monkeypatch,
):
    profile_id, _ = await _create_profile(client, tmp_path)
    created = await client.post("/api/tasks", json={
        "description": "Write exactly once",
        "ssh_grants": [{"profile_id": profile_id, "capabilities": ["write"]}],
    })
    task_id = created.json()["id"]
    calls = 0
    secret_content = "private-write-content-marker"

    def fake_write(_profile, path, content, _overwrite):
        nonlocal calls
        calls += 1
        assert content == secret_content
        return path, len(content.encode())

    monkeypatch.setattr(
        "backend.api.task_ssh._write_file_sync",
        fake_write,
    )
    body = {
        "effect_id": _effect_id(107),
        "path": "/etc/app/config",
        "content": secret_content,
        "overwrite": True,
    }
    first = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/write",
        json=body,
    )
    replay = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/write",
        json=body,
    )

    assert first.status_code == replay.status_code == 200
    assert first.json()["replayed"] is False
    assert replay.json()["replayed"] is True
    assert calls == 1
    async with session_factory() as db:
        receipt = await db.scalar(select(TaskSSHEffectReceipt).where(
            TaskSSHEffectReceipt.effect_id == body["effect_id"],
        ))
    assert receipt.status == "completed"
    assert secret_content not in repr(receipt.__dict__)


@pytest.mark.asyncio
async def test_task_ssh_effect_admission_has_no_count_quota(
    client,
    tmp_path,
    monkeypatch,
):
    profile_id, _ = await _create_profile(client, tmp_path)
    created = await client.post("/api/tasks", json={
        "description": "Bound permanent SSH effect evidence",
        "ssh_grants": [{"profile_id": profile_id, "capabilities": ["exec"]}],
    })
    task_id = created.json()["id"]
    calls = 0

    class FakeExecutor:
        async def run_result(self, _command, **_kwargs):
            nonlocal calls
            calls += 1
            return SSHCommandResult(0, "ok\n", "", False, 1)

    monkeypatch.setattr(
        "backend.api.task_ssh.executor_for_profile",
        lambda _profile: FakeExecutor(),
    )
    # This exceeds both former hard limits (64 per generation and 256 per
    # incarnation) without changing the Task execution identity.
    for index in range(257):
        response = await client.post(
            f"/api/tasks/{task_id}/ssh-access/{profile_id}/execute",
            json={
                "effect_id": f"{index + 1:032x}",
                "command": f"command-{index}",
            },
        )
        assert response.status_code == 200, response.text

    assert calls == 257


@pytest.mark.asyncio
async def test_task_ssh_large_result_is_bounded_durable_and_replayed(
    client,
    session_factory,
    tmp_path,
    monkeypatch,
):
    profile_id, _ = await _create_profile(client, tmp_path)
    created = await client.post("/api/tasks", json={
        "description": "Compact a large remote result",
        "ssh_grants": [{"profile_id": profile_id, "capabilities": ["exec"]}],
    })
    task_id = created.json()["id"]
    calls = 0
    large_output = "x" * (80 * 1024)

    class FakeExecutor:
        async def run_result(self, _command, **_kwargs):
            nonlocal calls
            calls += 1
            return SSHCommandResult(0, large_output, "", False, 1)

    monkeypatch.setattr(
        "backend.api.task_ssh.executor_for_profile",
        lambda _profile: FakeExecutor(),
    )
    body = {"effect_id": _effect_id(110), "command": "large-output"}

    first = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/execute",
        json=body,
    )
    replay = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/execute",
        json=body,
    )

    assert first.status_code == 200
    assert len(first.json()["stdout"].encode()) == 64 * 1024
    assert first.json()["truncated"] is True
    assert replay.status_code == 200
    assert replay.json() == {
        **first.json(),
        "replayed": True,
    }
    assert calls == 1
    async with session_factory() as db:
        receipt = await db.scalar(select(TaskSSHEffectReceipt).where(
            TaskSSHEffectReceipt.effect_id == body["effect_id"],
    ))
    assert receipt.status == "completed"
    assert receipt.result_compacted is False
    assert receipt.result_payload == {
        key: value
        for key, value in first.json().items()
        if key not in {"effect_id", "replayed"}
    }
    assert isinstance(receipt.result_digest, str)
    assert len(receipt.result_digest) == 64


@pytest.mark.asyncio
async def test_task_ssh_rejects_output_limit_above_durable_replay_cap(
    client,
    tmp_path,
):
    profile_id, _ = await _create_profile(client, tmp_path)
    created = await client.post("/api/tasks", json={
        "description": "Reject an unreplayable output request",
        "ssh_grants": [{"profile_id": profile_id, "capabilities": ["exec"]}],
    })

    response = await client.post(
        f"/api/tasks/{created.json()['id']}/ssh-access/{profile_id}/execute",
        json={
            "effect_id": _effect_id(114),
            "command": "large-output",
            "max_output_bytes": 64 * 1024 + 1,
        },
    )

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_task_ssh_startup_recovery_marks_running_ambiguous_and_releases_permit(
    client,
    session_factory,
    tmp_path,
    monkeypatch,
):
    profile_id, _ = await _create_profile(client, tmp_path)
    created = await client.post("/api/tasks", json={
        "description": "Recover a crash-interrupted remote effect",
        "ssh_grants": [{"profile_id": profile_id, "capabilities": ["exec"]}],
    })
    task_id = created.json()["id"]
    effect_id = _effect_id(113)
    command = "possibly-ran-before-restart"
    from backend.api import task_ssh as task_ssh_api

    async with session_factory() as db:
        task = await db.get(Task, task_id)
        profile = await db.get(SSHProfile, profile_id)
        db.add(TaskSSHEffectReceipt(
            effect_id=effect_id,
            task_id=task.id,
            task_incarnation_id=task.incarnation_id,
            task_retry_count=task.retry_count,
            task_turn_generation=task.turn_generation,
            task_status=task.status,
            profile_id=profile.id,
            profile_revision=profile.revision,
            operation="execute",
            request_digest=task_ssh_api._effect_request_digest(
                "execute",
                profile_id,
                TaskSSHExecuteRequest(
                    effect_id=effect_id,
                    command=command,
                ),
            ),
            status="running",
        ))
        await db.commit()

    async with session_factory() as db:
        task = await db.get(Task, task_id)
        task.retry_count += 1
        with pytest.raises(IntegrityError):
            await db.commit()
        await db.rollback()

    from backend.services.task_ssh_effect_recovery import (
        recover_interrupted_task_ssh_effects,
    )

    assert await recover_interrupted_task_ssh_effects(session_factory) == 1
    assert await recover_interrupted_task_ssh_effects(session_factory) == 0

    async with session_factory() as db:
        receipt = await db.scalar(select(TaskSSHEffectReceipt).where(
            TaskSSHEffectReceipt.effect_id == effect_id,
        ))
        assert receipt.status == "ambiguous"
        assert receipt.outcome_code == "manager_restart_unknown"
        task = await db.get(Task, task_id)
        task.retry_count += 1
        await db.commit()

    monkeypatch.setattr(
        task_ssh_api,
        "executor_for_profile",
        lambda _profile: pytest.fail("ambiguous effect must not reexecute"),
    )
    retry = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/execute",
        json={"effect_id": effect_id, "command": command},
    )
    assert retry.status_code == 409
    assert retry.json()["detail"]["effect_status"] == "ambiguous"


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["retry_count", "turn_generation", "status"])
async def test_task_ssh_completed_replay_rejects_task_generation_drift(
    client,
    session_factory,
    tmp_path,
    monkeypatch,
    drift,
):
    profile_id, _ = await _create_profile(client, tmp_path)
    created = await client.post("/api/tasks", json={
        "description": "Bind replay to an exact Task generation",
        "ssh_grants": [{"profile_id": profile_id, "capabilities": ["exec"]}],
    })
    task_id = created.json()["id"]
    calls = 0

    class FakeExecutor:
        async def run_result(self, _command, **_kwargs):
            nonlocal calls
            calls += 1
            return SSHCommandResult(0, "once\n", "", False, 1)

    monkeypatch.setattr(
        "backend.api.task_ssh.executor_for_profile",
        lambda _profile: FakeExecutor(),
    )
    body = {"effect_id": _effect_id(111), "command": "generation-bound"}
    first = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/execute",
        json=body,
    )
    assert first.status_code == 200

    async with session_factory() as db:
        task = await db.get(Task, task_id)
        if drift == "retry_count":
            task.retry_count += 1
        elif drift == "turn_generation":
            task.turn_generation += 1
        else:
            task.status = "running"
        await db.commit()

    replay = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/execute",
        json=body,
    )
    assert replay.status_code == 409
    assert "different Task execution generation" in replay.text
    assert calls == 1


@pytest.mark.asyncio
async def test_task_ssh_completed_replay_rejects_profile_revision_after_regrant(
    client,
    session_factory,
    tmp_path,
    monkeypatch,
):
    profile_id, _ = await _create_profile(client, tmp_path)
    created = await client.post("/api/tasks", json={
        "description": "Bind replay to one SSH profile revision",
        "ssh_grants": [{"profile_id": profile_id, "capabilities": ["exec"]}],
    })
    task_id = created.json()["id"]
    calls = 0

    class FakeExecutor:
        async def run_result(self, _command, **_kwargs):
            nonlocal calls
            calls += 1
            return SSHCommandResult(0, "once\n", "", False, 1)

    monkeypatch.setattr(
        "backend.api.task_ssh.executor_for_profile",
        lambda _profile: FakeExecutor(),
    )
    body = {"effect_id": _effect_id(112), "command": "revision-bound"}
    first = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/execute",
        json=body,
    )
    assert first.status_code == 200

    async with session_factory() as db:
        profile = await db.get(SSHProfile, profile_id)
        grant = await db.scalar(select(TaskSSHGrant).where(
            TaskSSHGrant.task_id == task_id,
            TaskSSHGrant.ssh_profile_id == profile_id,
        ))
        profile.revision += 1
        grant.profile_revision = profile.revision
        await db.commit()

    replay = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/execute",
        json=body,
    )
    assert replay.status_code == 409
    assert "different SSH profile revision" in replay.text
    assert calls == 1


def test_task_ssh_list_directory_uses_one_sftp_channel(monkeypatch):
    observed = {"opened": 0, "closed": 0}

    class SFTP:
        def get_channel(self):
            return type("Channel", (), {
                "settimeout": lambda self, timeout: None,
            })()

        def normalize(self, path):
            return path

        def listdir_iter(self, _path, read_aheads):
            assert read_aheads == 10
            return iter(())

        def close(self):
            observed["closed"] += 1

    class Client:
        def open_sftp(self):
            observed["opened"] += 1
            return SFTP()

        def close(self):
            return None

    monkeypatch.setattr(
        "backend.api.task_ssh.executor_for_profile",
        lambda _profile: type("Executor", (), {
            "connect": lambda self, timeout: Client(),
        })(),
    )
    from backend.api.task_ssh import _list_directory_sync

    path, entries, truncated = _list_directory_sync(
        type("Profile", (), {"allowed_roots": ["/"]})(),
        "/var/log",
    )

    assert (path, entries, truncated) == ("/var/log", [], False)
    assert observed == {"opened": 1, "closed": 1}


def test_task_ssh_non_overwrite_write_uses_remote_exclusive_create(monkeypatch):
    observed = {}

    class RemoteFile:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def write(self, payload):
            observed["payload"] = payload

    class SFTP:
        def get_channel(self):
            return type("Channel", (), {"settimeout": lambda self, timeout: None})()

        def normalize(self, path):
            return path

        def lstat(self, _path):
            raise FileNotFoundError

        def open(self, path, mode):
            observed["open"] = (path, mode)
            return RemoteFile()

        def close(self):
            return None

    class Client:
        def open_sftp(self):
            return SFTP()

        def close(self):
            return None

    monkeypatch.setattr(
        "backend.api.task_ssh.executor_for_profile",
        lambda _profile: type("Executor", (), {
            "connect": lambda self, timeout: Client(),
        })(),
    )
    from backend.api.task_ssh import _write_file_sync

    path, count = _write_file_sync(
        type("Profile", (), {"allowed_roots": ["/"]})(),
        "/tmp/new.txt",
        "hello",
        False,
    )

    assert path == "/tmp/new.txt"
    assert count == 5
    assert observed == {
        "open": ("/tmp/new.txt", "wx"),
        "payload": b"hello",
    }
