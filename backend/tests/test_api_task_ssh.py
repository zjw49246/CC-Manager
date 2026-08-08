from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519
from sqlalchemy import select

from backend.models.task_ssh_grant import TaskSSHGrant
from backend.services.ssh_executor import (
    SSHCommandResult,
    derive_openssh_public_key,
)
from backend.services.task_ssh_access import (
    TaskSSHAccessError,
    prepare_task_ssh_grants,
)
from backend.models.task import Task


def _private_key_file(tmp_path: Path) -> Path:
    private_key = ed25519.Ed25519PrivateKey.generate()
    path = tmp_path / "task-ssh-key"
    path.write_bytes(private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.OpenSSH,
        encryption_algorithm=serialization.NoEncryption(),
    ))
    path.chmod(0o600)
    return path


async def _create_profile(client, tmp_path: Path) -> tuple[int, Path]:
    key_path = _private_key_file(tmp_path)
    host_key = derive_openssh_public_key(key_path)
    response = await client.post("/api/ssh-profiles", json={
        "name": "task-target",
        "host": "ssh.task.internal",
        "username": "deploy",
        "key_path": str(key_path),
        "host_key_value": host_key,
        "task_access_enabled": True,
        "task_capabilities": ["exec", "read", "write"],
    })
    assert response.status_code == 201, response.text
    return response.json()["id"], key_path


@pytest.mark.asyncio
async def test_files_only_profile_cannot_be_granted_to_task(client, tmp_path):
    key_path = _private_key_file(tmp_path)
    host_key = derive_openssh_public_key(key_path)
    profile = await client.post("/api/ssh-profiles", json={
        "name": "files-only",
        "host": "ssh.files.internal",
        "username": "reader",
        "key_path": str(key_path),
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
    profile = await client.post("/api/ssh-profiles", json={
        "name": "read-only-tasks",
        "host": "ssh.read.internal",
        "username": "reader",
        "key_path": str(key_path),
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
        json={"command": "systemctl is-active app", "timeout_seconds": 20},
    )
    assert first.status_code == 200
    assert first.json()["stdout"] == "healthy\n"
    assert observed == [(
        "systemctl is-active app",
        {
            "timeout": 20,
            "max_output_bytes": 1024 * 1024,
            "sensitive": True,
        },
    )]

    changed = await client.put(
        f"/api/ssh-profiles/{profile_id}",
        json={"username": "release"},
    )
    assert changed.status_code == 200
    assert changed.json()["revision"] == 2

    stale = await client.post(
        f"/api/tasks/{task_id}/ssh-access/{profile_id}/execute",
        json={"command": "hostname"},
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
        json={"command": "cat /etc/app.conf"},
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
        json={"command": "hostname"},
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
        json={"command": "hostname"},
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
        json={"command": "hostname"},
    )
    assert exec_denied.status_code == 403


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
