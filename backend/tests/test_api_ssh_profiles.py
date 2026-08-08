import os
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from backend.services.ssh_executor import (
    SSHProbeResult,
    derive_openssh_public_key,
)
from backend.config import settings


def _private_key_file(tmp_path: Path) -> Path:
    private_key = ed25519.Ed25519PrivateKey.generate()
    path = tmp_path / "managed-ssh-key"
    path.write_bytes(private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.OpenSSH,
        encryption_algorithm=serialization.NoEncryption(),
    ))
    path.chmod(0o600)
    return path


@pytest.mark.asyncio
async def test_managed_profile_crud_masks_key_and_revisions_identity(
    client, tmp_path, monkeypatch,
):
    key_path = _private_key_file(tmp_path)
    host_key = derive_openssh_public_key(key_path)

    create = await client.post("/api/ssh-profiles", json={
        "name": "staging",
        "host": "ssh.staging.internal",
        "port": 2222,
        "username": "deploy",
        "key_path": str(key_path),
        "host_key_value": host_key,
    })
    assert create.status_code == 201, create.text
    profile = create.json()
    assert profile["revision"] == 1
    assert profile["task_access_enabled"] is False
    assert profile["task_capabilities"] == []
    assert profile["allowed_roots"] == ["/"]
    assert profile["key_path_hint"] == "…/managed-ssh-key"
    assert "key_path" not in profile
    assert profile["public_key_fingerprint"].startswith("SHA256:")
    original_key_fingerprint = profile["public_key_fingerprint"]
    assert profile["host_key_fingerprint"].startswith("SHA256:")

    task_policy = await client.put(
        f"/api/ssh-profiles/{profile['id']}",
        json={
            "task_access_enabled": True,
            "task_capabilities": ["read", "read", "exec"],
        },
    )
    assert task_policy.status_code == 200, task_policy.text
    assert task_policy.json()["revision"] == 2
    assert task_policy.json()["task_capabilities"] == ["read", "exec"]

    rename = await client.put(
        f"/api/ssh-profiles/{profile['id']}", json={"name": "staging-a"},
    )
    assert rename.status_code == 200
    assert rename.json()["revision"] == 2

    identity = await client.put(
        f"/api/ssh-profiles/{profile['id']}", json={"username": "release"},
    )
    assert identity.status_code == 200
    assert identity.json()["revision"] == 3
    assert identity.json()["last_test_ok"] is None

    replacement = ed25519.Ed25519PrivateKey.generate()
    key_path.write_bytes(replacement.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.OpenSSH,
        encryption_algorithm=serialization.NoEncryption(),
    ))
    key_path.chmod(0o600)
    rotated = await client.put(
        f"/api/ssh-profiles/{profile['id']}",
        json={"key_path": str(key_path)},
    )
    assert rotated.status_code == 200
    assert rotated.json()["revision"] == 4
    assert rotated.json()["public_key_fingerprint"] != original_key_fingerprint

    monkeypatch.setattr(
        "backend.services.ssh_profiles.SSHExecutor.probe",
        lambda *_args, **_kwargs: _async_result(SSHProbeResult(True)),
    )
    tested = await client.post(f"/api/ssh-profiles/{profile['id']}/test")
    assert tested.status_code == 200
    assert tested.json() == {"ok": True, "error_code": None, "detail": None}

    listing = await client.get("/api/ssh-profiles")
    assert listing.status_code == 200
    assert [item["name"] for item in listing.json()] == ["staging-a"]

    deleted = await client.delete(f"/api/ssh-profiles/{profile['id']}")
    assert deleted.status_code == 200
    assert (await client.get("/api/ssh-profiles")).json() == []
    assert (await client.get(f"/api/ssh-profiles/{profile['id']}")).status_code == 404


async def _async_result(value):
    return value


@pytest.mark.asyncio
async def test_profile_endpoint_change_requires_new_host_key(client, tmp_path):
    key_path = _private_key_file(tmp_path)
    host_key = derive_openssh_public_key(key_path)
    created = await client.post("/api/ssh-profiles", json={
        "name": "production",
        "host": "old.example.internal",
        "username": "deploy",
        "key_path": str(key_path),
        "host_key_value": host_key,
    })
    profile_id = created.json()["id"]

    rejected = await client.put(
        f"/api/ssh-profiles/{profile_id}",
        json={"host": "new.example.internal"},
    )

    assert rejected.status_code == 400
    assert "newly confirmed host key" in rejected.text


@pytest.mark.asyncio
async def test_profile_allowed_roots_are_normalized_and_revisioned(client, tmp_path):
    key_path = _private_key_file(tmp_path)
    host_key = derive_openssh_public_key(key_path)
    created = await client.post("/api/ssh-profiles", json={
        "name": "root-policy",
        "host": "roots.example.internal",
        "username": "deploy",
        "key_path": str(key_path),
        "host_key_value": host_key,
        "allowed_roots": ["/srv/app/", "/srv/app/logs", "/var//data"],
    })
    assert created.status_code == 201, created.text
    profile = created.json()
    assert profile["allowed_roots"] == ["/srv/app", "/var/data"]

    updated = await client.put(
        f"/api/ssh-profiles/{profile['id']}",
        json={"allowed_roots": ["/srv/app"]},
    )
    assert updated.status_code == 200, updated.text
    assert updated.json()["revision"] == 2

    for invalid in ([], ["relative/path"], None):
        rejected = await client.put(
            f"/api/ssh-profiles/{profile['id']}",
            json={"allowed_roots": invalid},
        )
        assert rejected.status_code == 422


@pytest.mark.asyncio
async def test_profile_rejects_inconsistent_task_policy(client, tmp_path):
    key_path = _private_key_file(tmp_path)
    host_key = derive_openssh_public_key(key_path)
    base = {
        "name": "policy",
        "host": "ssh.policy.internal",
        "username": "deploy",
        "key_path": str(key_path),
        "host_key_value": host_key,
    }

    capabilities_without_switch = await client.post(
        "/api/ssh-profiles",
        json={**base, "task_capabilities": ["read"]},
    )
    assert capabilities_without_switch.status_code == 422

    switch_without_capabilities = await client.post(
        "/api/ssh-profiles",
        json={
            **base,
            "name": "empty-policy",
            "task_access_enabled": True,
        },
    )
    assert switch_without_capabilities.status_code == 422

    created = await client.post("/api/ssh-profiles", json=base)
    inconsistent_update = await client.put(
        f"/api/ssh-profiles/{created.json()['id']}",
        json={"task_access_enabled": True},
    )
    assert inconsistent_update.status_code == 422

    null_policy = await client.put(
        f"/api/ssh-profiles/{created.json()['id']}",
        json={"task_capabilities": None},
    )
    assert null_policy.status_code == 422


@pytest.mark.asyncio
async def test_profile_rejects_unsafe_key_and_duplicate_name(client, tmp_path):
    key_path = _private_key_file(tmp_path)
    host_key = derive_openssh_public_key(key_path)
    payload = {
        "name": "duplicate",
        "host": "ssh.example.internal",
        "username": "deploy",
        "key_path": str(key_path),
        "host_key_value": host_key,
    }
    assert (await client.post("/api/ssh-profiles", json=payload)).status_code == 201
    assert (await client.post("/api/ssh-profiles", json=payload)).status_code == 409

    key_path.chmod(0o644)
    unsafe = await client.post("/api/ssh-profiles", json={**payload, "name": "unsafe"})
    assert unsafe.status_code == 400
    assert unsafe.json()["detail"]["code"] == "key_permissions"


@pytest.mark.asyncio
async def test_probe_host_key_returns_confirmable_identity(client, monkeypatch):
    monkeypatch.setattr(
        "backend.api.ssh_profiles.probe_ssh_host_key",
        lambda host, *, port, timeout: type("HostKey", (), {
            "key_type": "ssh-ed25519",
            "openssh_public_key": "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIJNQCBTQso2itH2uBoMKDWX3zZZS0tI4WZJ1bnFmM8oQ",
            "sha256_fingerprint": "SHA256:test",
        })(),
    )

    response = await client.post("/api/ssh-profiles/probe-host-key", json={
        "host": "ssh.example.internal",
        "port": 2200,
    })

    assert response.status_code == 200
    assert response.json()["key_type"] == "ssh-ed25519"
    assert response.json()["fingerprint"] == "SHA256:test"


@pytest.mark.asyncio
async def test_upload_private_key_creates_and_deletes_managed_profile_key(
    client, tmp_path, monkeypatch,
):
    store_root = tmp_path / "ssh-key-store"
    monkeypatch.setattr(settings, "ssh_key_storage_dir", str(store_root))
    source = _private_key_file(tmp_path)
    host_key = derive_openssh_public_key(source)

    uploaded = await client.post(
        "/api/ssh-profiles/upload-key",
        files={"file": ("production.pem", source.read_bytes(), "application/x-pem-file")},
    )

    assert uploaded.status_code == 200, uploaded.text
    upload = uploaded.json()
    assert upload["filename"] == "production.pem"
    assert upload["public_key_fingerprint"].startswith("SHA256:")
    assert "path" not in upload
    token = upload["upload_token"]
    pending = store_root / "pending" / token
    assert pending.is_file()
    assert pending.stat().st_mode & 0o777 == 0o600

    created = await client.post("/api/ssh-profiles", json={
        "name": "uploaded-production",
        "host": "ssh.example.internal",
        "username": "deploy",
        "key_upload_token": token,
        "host_key_value": host_key,
    })

    assert created.status_code == 201, created.text
    assert "key_path" not in created.json()
    managed = store_root / "managed" / token
    assert managed.is_file()
    assert not pending.exists()

    reused = await client.post("/api/ssh-profiles", json={
        "name": "token-reuse",
        "host": "ssh.example.internal",
        "username": "deploy",
        "key_upload_token": token,
        "host_key_value": host_key,
    })
    assert reused.status_code == 400
    assert reused.json()["detail"]["code"] == "upload_token_invalid"

    replacement = ed25519.Ed25519PrivateKey.generate().private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.OpenSSH,
        encryption_algorithm=serialization.NoEncryption(),
    )
    replacement_upload = await client.post(
        "/api/ssh-profiles/upload-key",
        files={"file": ("replacement.pem", replacement, "application/x-pem-file")},
    )
    replacement_token = replacement_upload.json()["upload_token"]
    rotated = await client.put(
        f"/api/ssh-profiles/{created.json()['id']}",
        json={"key_upload_token": replacement_token},
    )
    assert rotated.status_code == 200, rotated.text
    assert rotated.json()["revision"] == 2
    assert not managed.exists()
    managed = store_root / "managed" / replacement_token
    assert managed.is_file()

    deleted = await client.delete(f"/api/ssh-profiles/{created.json()['id']}")
    assert deleted.status_code == 200
    assert not managed.exists()


@pytest.mark.asyncio
async def test_invalid_or_cancelled_private_key_upload_is_not_claimable(
    client, tmp_path, monkeypatch,
):
    store_root = tmp_path / "ssh-key-store"
    monkeypatch.setattr(settings, "ssh_key_storage_dir", str(store_root))

    invalid = await client.post(
        "/api/ssh-profiles/upload-key",
        files={"file": ("not-a-key.pem", b"not a private key", "application/octet-stream")},
    )
    assert invalid.status_code == 400
    assert invalid.json()["detail"]["code"] == "key_invalid"
    assert not list((store_root / "pending").iterdir())

    source = _private_key_file(tmp_path)
    host_key = derive_openssh_public_key(source)
    expired_upload = await client.post(
        "/api/ssh-profiles/upload-key",
        files={"file": ("expired.pem", source.read_bytes(), "application/octet-stream")},
    )
    expired_token = expired_upload.json()["upload_token"]
    expired_path = store_root / "pending" / expired_token
    old_time = expired_path.stat().st_mtime - 25 * 60 * 60
    os.utime(expired_path, (old_time, old_time))
    expired = await client.post("/api/ssh-profiles", json={
        "name": "expired-upload",
        "host": "ssh.example.internal",
        "username": "deploy",
        "key_upload_token": expired_token,
        "host_key_value": host_key,
    })
    assert expired.status_code == 400
    assert expired.json()["detail"]["code"] == "upload_token_invalid"
    assert not expired_path.exists()

    uploaded = await client.post(
        "/api/ssh-profiles/upload-key",
        files={"file": ("id_ed25519", source.read_bytes(), "application/octet-stream")},
    )
    token = uploaded.json()["upload_token"]
    cancelled = await client.delete(f"/api/ssh-profiles/upload-key/{token}")
    assert cancelled.status_code == 200

    rejected = await client.post("/api/ssh-profiles", json={
        "name": "cancelled-upload",
        "host": "ssh.example.internal",
        "username": "deploy",
        "key_upload_token": token,
        "host_key_value": host_key,
    })
    assert rejected.status_code == 400
    assert rejected.json()["detail"]["code"] == "upload_token_invalid"


@pytest.mark.asyncio
async def test_failed_profile_save_keeps_upload_token_retryable(
    client, tmp_path, monkeypatch,
):
    store_root = tmp_path / "ssh-key-store"
    monkeypatch.setattr(settings, "ssh_key_storage_dir", str(store_root))
    source = _private_key_file(tmp_path)
    host_key = derive_openssh_public_key(source)
    base = {
        "name": "duplicate-upload",
        "host": "ssh.example.internal",
        "username": "deploy",
        "key_path": str(source),
        "host_key_value": host_key,
    }
    assert (await client.post("/api/ssh-profiles", json=base)).status_code == 201

    uploaded = await client.post(
        "/api/ssh-profiles/upload-key",
        files={"file": ("retry.pem", source.read_bytes(), "application/octet-stream")},
    )
    token = uploaded.json()["upload_token"]
    failed = await client.post("/api/ssh-profiles", json={
        **{key: value for key, value in base.items() if key != "key_path"},
        "key_upload_token": token,
    })
    assert failed.status_code == 409
    assert (store_root / "pending" / token).is_file()
    assert not (store_root / "managed" / token).exists()

    retried = await client.post("/api/ssh-profiles", json={
        **{key: value for key, value in base.items() if key not in {"name", "key_path"}},
        "name": "retry-succeeded",
        "key_upload_token": token,
    })
    assert retried.status_code == 201, retried.text
    assert not (store_root / "pending" / token).exists()
    assert (store_root / "managed" / token).is_file()
