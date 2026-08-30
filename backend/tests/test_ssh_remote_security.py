import asyncio
import threading

import pytest

from backend.config import settings
from backend.schemas.ssh_profile import normalize_allowed_roots
from backend.services.ssh_remote_paths import (
    SSHRemotePathDenied,
    resolve_existing_remote_path,
    resolve_remote_write_path,
)
from backend.services.ssh_sftp import (
    SSHSFTPBusyError,
    SSHSFTPOperationTimeout,
    run_bounded_sftp,
)


class FakeSFTP:
    def __init__(self, normalized: dict[str, str] | None = None):
        self.normalized = normalized or {}

    def normalize(self, path: str) -> str:
        return self.normalized.get(path, path)

    def lstat(self, _path: str):
        raise FileNotFoundError


def test_allowed_roots_are_absolute_normalized_and_collapsed():
    assert normalize_allowed_roots([
        "/srv/app/",
        "/srv/app/logs",
        "/var//log/../data",
        "/srv/app",
    ]) == ["/srv/app", "/var/data"]
    with pytest.raises(ValueError, match="absolute POSIX"):
        normalize_allowed_roots(["relative"])
    with pytest.raises(ValueError, match="at least one"):
        normalize_allowed_roots([])


def test_existing_remote_symlink_cannot_escape_allowed_root():
    sftp = FakeSFTP({
        "/srv/app": "/real/app",
        "/srv/app/link": "/etc",
    })
    with pytest.raises(SSHRemotePathDenied, match="outside"):
        resolve_existing_remote_path(sftp, "/srv/app/link", ["/srv/app"])


def test_empty_persisted_allowed_roots_fail_closed():
    with pytest.raises(SSHRemotePathDenied, match="no usable allowed roots"):
        resolve_existing_remote_path(FakeSFTP(), "/srv/app", [])


def test_new_remote_write_uses_canonical_allowed_parent():
    sftp = FakeSFTP({
        "/srv/app": "/real/app",
        "/srv/app/releases": "/real/app/releases",
    })
    assert resolve_remote_write_path(
        sftp,
        "/srv/app/releases/new.txt",
        ["/srv/app"],
    ) == "/real/app/releases/new.txt"


@pytest.mark.asyncio
async def test_timed_out_sftp_worker_keeps_global_slot_until_thread_exits(
    monkeypatch,
):
    monkeypatch.setattr(settings, "ssh_sftp_max_concurrency", 1)
    monkeypatch.setattr(settings, "ssh_sftp_queue_timeout_seconds", 0.03)
    gate = threading.Event()
    cleaned: list[str] = []

    def stalled() -> str:
        gate.wait(timeout=2)
        return "late-result"

    with pytest.raises(SSHSFTPOperationTimeout):
        await run_bounded_sftp(
            stalled,
            operation_timeout=0.02,
            abandoned_result_cleanup=cleaned.append,
        )
    with pytest.raises(SSHSFTPBusyError):
        await run_bounded_sftp(lambda: "must-not-start", operation_timeout=1)

    gate.set()
    for _ in range(50):
        if cleaned:
            break
        await asyncio.sleep(0.01)
    assert cleaned == ["late-result"]
    assert await run_bounded_sftp(lambda: "available", operation_timeout=1) == "available"
