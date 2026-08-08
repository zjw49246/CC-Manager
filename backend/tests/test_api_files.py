"""Regression tests for temporary files created by the file API."""

import tempfile
from pathlib import Path
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient

import backend.api.files as files_module


def _managed_files_app(profile):
    class FakeSession:
        async def get(self, model, profile_id):
            assert model is files_module.SSHProfile
            return profile if profile_id == profile.id else None

    async def override_db():
        yield FakeSession()

    app = FastAPI()
    app.include_router(files_module.router)
    app.dependency_overrides[files_module.require_admin] = lambda: None
    app.dependency_overrides[files_module.get_db] = override_db
    return app


async def test_managed_ssh_files_use_profile_id_without_browser_credentials(
    monkeypatch,
):
    profile = SimpleNamespace(
        id=7,
        enabled=True,
        deleted_at=None,
        task_access_enabled=False,
        task_capabilities=[],
        allowed_roots=["/srv"],
    )
    observed = []

    def fake_list(resolved_profile, path):
        observed.append((resolved_profile, path))
        return "/srv", [{
            "name": "report.txt",
            "path": "/srv/report.txt",
            "is_dir": False,
            "size": 12,
        }], False

    def fake_read(resolved_profile, path):
        observed.append((resolved_profile, path))
        return "/srv/report.txt", "managed content", 15

    monkeypatch.setattr(files_module, "_managed_ssh_list_sync", fake_list)
    monkeypatch.setattr(files_module, "_managed_ssh_read_sync", fake_read)

    transport = ASGITransport(app=_managed_files_app(profile))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        listed = await client.post(
            "/api/files/ssh/7/list",
            json={"path": "/srv"},
        )
        read = await client.post(
            "/api/files/ssh/7/read",
            json={"path": "/srv/report.txt"},
        )

    assert listed.status_code == 200
    assert listed.json()["entries"][0]["name"] == "report.txt"
    assert read.status_code == 200
    assert read.json() == {
        "path": "/srv/report.txt",
        "content": "managed content",
        "size": 15,
    }
    assert observed == [
        (profile, "/srv"),
        (profile, "/srv/report.txt"),
    ]


async def test_managed_ssh_files_reject_disabled_profile(monkeypatch):
    profile = SimpleNamespace(id=3, enabled=False, deleted_at=None)
    monkeypatch.setattr(
        files_module,
        "_managed_ssh_list_sync",
        lambda *_args: (_ for _ in ()).throw(AssertionError("must not connect")),
    )

    transport = ASGITransport(app=_managed_files_app(profile))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/files/ssh/3/list",
            json={"path": "/"},
        )

    assert response.status_code == 409
    assert response.json()["detail"] == "SSH profile is disabled"


async def test_managed_ssh_files_require_absolute_remote_path():
    profile = SimpleNamespace(id=3, enabled=True, deleted_at=None)
    transport = ASGITransport(app=_managed_files_app(profile))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/files/ssh/3/read",
            json={"path": "relative/file.txt"},
        )

    assert response.status_code == 422


def test_managed_ssh_download_enforces_stream_limit_and_cleans_partial_file(
    tmp_path,
    monkeypatch,
):
    created_paths: list[Path] = []
    real_named_temporary_file = tempfile.NamedTemporaryFile

    def isolated_named_temporary_file(*args, **kwargs):
        kwargs["dir"] = tmp_path
        temporary_file = real_named_temporary_file(*args, **kwargs)
        created_paths.append(Path(temporary_file.name))
        return temporary_file

    class FakeRemoteFile:
        chunks = iter((b"12345", b""))

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self, _size):
            return next(self.chunks)

    class FakeSFTP:
        def get_channel(self):
            return SimpleNamespace(settimeout=lambda _timeout: None)

        def normalize(self, path):
            return path

        def stat(self, _path):
            return SimpleNamespace(st_size=3)

        def open(self, _path, _mode):
            return FakeRemoteFile()

        def close(self):
            return None

    class FakeClient:
        def open_sftp(self):
            return FakeSFTP()

        def close(self):
            return None

    monkeypatch.setattr(files_module, "MAX_DOWNLOAD_SIZE", 4)
    monkeypatch.setattr(
        files_module.tempfile,
        "NamedTemporaryFile",
        isolated_named_temporary_file,
    )
    monkeypatch.setattr(
        files_module,
        "executor_for_profile",
        lambda _profile: SimpleNamespace(connect=lambda timeout: FakeClient()),
    )

    try:
        files_module._managed_ssh_download_sync(
            SimpleNamespace(allowed_roots=["/"]),
            "/remote/growing.bin",
        )
    except HTTPException as exc:
        assert exc.status_code == 413
    else:
        raise AssertionError("growing remote file should exceed the stream limit")

    assert len(created_paths) == 1
    assert not created_paths[0].exists()


async def test_ssh_download_removes_temporary_file_after_response(
    tmp_path,
    monkeypatch,
):
    payload = b"downloaded over ssh"
    created_paths: list[Path] = []
    real_named_temporary_file = tempfile.NamedTemporaryFile

    def isolated_named_temporary_file(*args, **kwargs):
        kwargs["dir"] = tmp_path
        temporary_file = real_named_temporary_file(*args, **kwargs)
        created_paths.append(Path(temporary_file.name))
        return temporary_file

    class FakeSFTP:
        closed = False

        def stat(self, remote_path):
            assert remote_path == "/remote/report.txt"
            return SimpleNamespace(st_size=len(payload))

        def getfo(self, remote_path, destination):
            assert remote_path == "/remote/report.txt"
            destination.write(payload)

        def close(self):
            self.closed = True

    class FakeSSHClient:
        closed = False

        def __init__(self):
            self.sftp = FakeSFTP()

        def open_sftp(self):
            return self.sftp

        def close(self):
            self.closed = True

    ssh_client = FakeSSHClient()
    monkeypatch.setattr(
        files_module.tempfile,
        "NamedTemporaryFile",
        isolated_named_temporary_file,
    )
    monkeypatch.setattr(
        files_module,
        "_make_ssh_client",
        lambda _credentials: ssh_client,
    )

    app = FastAPI()
    app.include_router(files_module.router)
    app.dependency_overrides[files_module.require_admin] = lambda: None
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/files/ssh/download",
            json={
                "host": "worker.internal",
                "username": "ubuntu",
                "path": "/remote/report.txt",
            },
        )

    assert response.status_code == 200
    assert response.content == payload
    assert len(created_paths) == 1
    assert created_paths[0].name.startswith("ccm-ssh-download-")
    assert not created_paths[0].exists()
    assert ssh_client.sftp.closed is True
    assert ssh_client.closed is True
