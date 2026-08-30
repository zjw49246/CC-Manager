from __future__ import annotations

import json
import os
import socket
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import backend.api.codex_pool as codex_pool_api
import backend.api.pool as claude_pool_api
from backend.services import login_runtime


def _configure_runtime(monkeypatch, tmp_path: Path, *, display: str = ":199") -> None:
    monkeypatch.setenv("CCM_XVFB_DISPLAY", display)
    monkeypatch.setenv("CCM_LOGIN_CDP_PORT", "9322")
    monkeypatch.setenv("CCM_LOGIN_RUNTIME_DIR", str(tmp_path / "runtime"))
    monkeypatch.setenv("CCM_LOGIN_TMPDIR", str(tmp_path / "login-tmp"))
    monkeypatch.setenv("CCM_LOGIN_MIN_AVAILABLE_MB", "0")
    monkeypatch.setenv("CCM_LOGIN_MIN_TEMP_FREE_MB", "0")


def _legacy_path_identity(
    manager: login_runtime.XvfbManager,
    path: Path,
) -> dict[str, int]:
    identity = manager._path_identity(path)
    assert identity is not None
    identity.pop("ctime_ns")
    return identity


def test_claude_and_codex_pool_share_one_login_lock():
    assert claude_pool_api._login_lock is login_runtime.login_lock
    assert codex_pool_api._login_lock is login_runtime.login_lock


def test_login_child_environment_uses_configured_isolated_runtime(
    monkeypatch,
    tmp_path,
):
    _configure_runtime(monkeypatch, tmp_path, display=":101")

    env = login_runtime.login_child_environment(extra={"EXTRA": "yes"})

    assert env["DISPLAY"] == ":101"
    assert "CCM_LOGIN_CDP_PORT" not in env
    assert env["TMPDIR"] == str(tmp_path / "login-tmp")
    assert env["XAUTHORITY"].endswith("display-101.auth")
    assert env["EXTRA"] == "yes"


def test_resource_guard_rejects_low_available_memory(monkeypatch, tmp_path):
    _configure_runtime(monkeypatch, tmp_path)
    monkeypatch.setenv("CCM_LOGIN_MIN_AVAILABLE_MB", "512")

    with pytest.raises(login_runtime.LoginResourceError, match="available memory 77 MiB"):
        login_runtime.ensure_login_capacity(
            temp_dir=login_runtime.login_temp_directory(),
            mem_available_bytes=77 * 1024 * 1024,
        )


def test_xauthority_cookie_is_private_and_not_exposed_in_argv(
    monkeypatch,
    tmp_path,
):
    auth_path = tmp_path / "display.auth"
    run = Mock(return_value=SimpleNamespace(returncode=0))
    monkeypatch.setattr(login_runtime.subprocess, "run", run)
    monkeypatch.setattr(login_runtime.secrets, "token_hex", lambda _size: "cookie")

    login_runtime.XvfbManager._write_xauthority(":199", auth_path)

    assert auth_path.stat().st_mode & 0o777 == 0o600
    assert run.call_args.args[0] == ["xauth", "-f", str(auth_path)]
    assert "cookie" not in run.call_args.args[0]
    assert "MIT-MAGIC-COOKIE-1 cookie" in run.call_args.kwargs["input"]


def test_cached_xvfb_is_polled_before_reuse(monkeypatch, tmp_path):
    _configure_runtime(monkeypatch, tmp_path)
    manager = login_runtime.XvfbManager()
    proc = SimpleNamespace(returncode=None, poll=Mock(), pid=123)
    manager._proc = proc
    monkeypatch.setattr(manager, "_display_ready", lambda *_args: True)

    runtime = manager._ensure_sync()

    proc.poll.assert_called_once()
    assert runtime.display == ":199"


def test_ready_xvfb_from_sibling_process_is_reused_without_popen(
    monkeypatch,
    tmp_path,
):
    _configure_runtime(monkeypatch, tmp_path)
    manager = login_runtime.XvfbManager()
    monkeypatch.setattr(manager, "_display_ready", lambda *_args: True)
    popen = Mock(side_effect=AssertionError("must not start another Xvfb"))
    monkeypatch.setattr(login_runtime.subprocess, "Popen", popen)

    runtime = manager._ensure_sync()

    assert runtime.display == ":199"
    popen.assert_not_called()
    lock_path, *_ = manager._paths(199)
    assert not manager._owner_path(lock_path).exists()


def test_authenticated_live_legacy_owner_is_reused_without_upgrade(
    monkeypatch,
    tmp_path,
):
    _configure_runtime(monkeypatch, tmp_path)
    manager = login_runtime.XvfbManager()
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    lock_path = runtime_dir / "display.lock"
    auth_path = runtime_dir / "display.auth"
    stderr_path = runtime_dir / "stderr.log"
    socket_path = tmp_path / "X199"
    x_lock_path = tmp_path / ".X199-lock"
    monkeypatch.setattr(
        manager,
        "_paths",
        lambda _number: (lock_path, auth_path, stderr_path, socket_path),
    )

    xvfb_socket = socket.socket(socket.AF_UNIX)
    xvfb_socket.bind(str(socket_path))
    xvfb_socket.close()
    x_lock_path.write_text("111\n", encoding="utf-8")
    owner_path = manager._owner_path(lock_path)
    legacy_record = {
        "version": 1,
        "display": ":199",
        "pid": 111,
        "start_time_ticks": 100,
        "socket": _legacy_path_identity(manager, socket_path),
        "x_lock": _legacy_path_identity(manager, x_lock_path),
    }
    manager._write_owner_record(owner_path, legacy_record)
    # A replacement may immediately reuse all four v1 identity fields.  Even
    # an authenticated display therefore cannot prove that it is the socket
    # described by a legacy record.
    socket_path.unlink()
    replacement = socket.socket(socket.AF_UNIX)
    replacement.bind(str(socket_path))
    replacement.close()
    monkeypatch.setattr(manager, "_display_ready", lambda *_args: True)
    monkeypatch.setattr(
        manager,
        "_read_process_identity",
        lambda pid: ("S", 100) if pid == 111 else None,
    )
    popen = Mock(side_effect=AssertionError("must reuse authenticated Xvfb"))
    monkeypatch.setattr(login_runtime.subprocess, "Popen", popen)

    runtime = manager._ensure_sync()

    assert runtime.display == ":199"
    assert socket_path.exists()
    assert json.loads(owner_path.read_text(encoding="utf-8")) == legacy_record
    popen.assert_not_called()


def test_stale_legacy_owner_cannot_be_auto_recovered(
    monkeypatch,
    tmp_path,
):
    _configure_runtime(monkeypatch, tmp_path)
    manager = login_runtime.XvfbManager()
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    lock_path = runtime_dir / "display.lock"
    auth_path = runtime_dir / "display.auth"
    stderr_path = runtime_dir / "stderr.log"
    socket_path = tmp_path / "X199"
    monkeypatch.setattr(
        manager,
        "_paths",
        lambda _number: (lock_path, auth_path, stderr_path, socket_path),
    )

    stale_socket = socket.socket(socket.AF_UNIX)
    stale_socket.bind(str(socket_path))
    stale_socket.close()
    owner_path = manager._owner_path(lock_path)
    legacy_record = {
        "version": 1,
        "display": ":199",
        "pid": 111,
        "start_time_ticks": 100,
        "socket": _legacy_path_identity(manager, socket_path),
        "x_lock": None,
    }
    manager._write_owner_record(owner_path, legacy_record)
    monkeypatch.setattr(manager, "_display_ready", lambda *_args: False)
    monkeypatch.setattr(manager, "_read_process_identity", lambda _pid: None)
    popen = Mock(side_effect=AssertionError("must not replace legacy Xvfb"))
    monkeypatch.setattr(login_runtime.subprocess, "Popen", popen)

    with pytest.raises(
        login_runtime.LoginRuntimeError,
        match="legacy v1.*cannot be safely auto-recovered",
    ):
        manager._ensure_sync()

    assert socket_path.exists()
    assert json.loads(owner_path.read_text(encoding="utf-8")) == legacy_record
    popen.assert_not_called()


def test_empty_dead_legacy_owner_is_retained_until_v2_start_succeeds(
    monkeypatch,
    tmp_path,
):
    _configure_runtime(monkeypatch, tmp_path)
    manager = login_runtime.XvfbManager()
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    lock_path = runtime_dir / "display.lock"
    auth_path = runtime_dir / "display.auth"
    stderr_path = runtime_dir / "stderr.log"
    socket_path = tmp_path / "X199"
    monkeypatch.setattr(
        manager,
        "_paths",
        lambda _number: (lock_path, auth_path, stderr_path, socket_path),
    )
    old_socket = socket.socket(socket.AF_UNIX)
    old_socket.bind(str(socket_path))
    old_socket.close()
    legacy_socket_identity = _legacy_path_identity(manager, socket_path)
    socket_path.unlink()
    owner_path = manager._owner_path(lock_path)
    legacy_record = {
        "version": 1,
        "display": ":199",
        "pid": 111,
        "start_time_ticks": 100,
        "socket": legacy_socket_identity,
        "x_lock": None,
    }
    manager._write_owner_record(owner_path, legacy_record)
    new_socket: socket.socket | None = None

    class FakeProcess:
        pid = 222
        returncode = None

        def poll(self):
            return self.returncode

    def fake_popen(*_args, **_kwargs):
        nonlocal new_socket
        assert json.loads(owner_path.read_text(encoding="utf-8")) == legacy_record
        new_socket = socket.socket(socket.AF_UNIX)
        new_socket.bind(str(socket_path))
        return FakeProcess()

    monkeypatch.setattr(
        manager,
        "_read_process_identity",
        lambda pid: None if pid == 111 else ("S", 200),
    )
    monkeypatch.setattr(
        manager,
        "_display_ready",
        lambda *_args: manager._proc is not None,
    )
    monkeypatch.setattr(
        manager,
        "_write_xauthority",
        lambda _display, path: path.touch(),
    )
    monkeypatch.setattr(login_runtime.subprocess, "Popen", fake_popen)

    runtime = manager._ensure_sync()

    assert runtime.display == ":199"
    persisted = json.loads(owner_path.read_text(encoding="utf-8"))
    assert persisted["version"] == 2
    assert persisted["pid"] == 222
    assert persisted["socket"] == manager._path_identity(socket_path)
    assert new_socket is not None
    new_socket.close()


def test_foreign_x_socket_is_not_killed_or_replaced(monkeypatch, tmp_path):
    _configure_runtime(monkeypatch, tmp_path)
    manager = login_runtime.XvfbManager()
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    socket_path = tmp_path / "foreign-X199"
    socket_path.touch()
    monkeypatch.setattr(
        manager,
        "_paths",
        lambda _number: (
            runtime_dir / "display.lock",
            runtime_dir / "display.auth",
            runtime_dir / "stderr.log",
            socket_path,
        ),
    )
    monkeypatch.setattr(manager, "_display_ready", lambda *_args: False)
    popen = Mock(side_effect=AssertionError("must not replace a foreign X server"))
    monkeypatch.setattr(login_runtime.subprocess, "Popen", popen)

    with pytest.raises(login_runtime.LoginRuntimeError, match="cannot be authenticated"):
        manager._ensure_sync()

    popen.assert_not_called()


def test_xvfb_start_waits_for_real_display_readiness(monkeypatch, tmp_path):
    _configure_runtime(monkeypatch, tmp_path)
    manager = login_runtime.XvfbManager()
    ready = iter([False, False, True])
    monkeypatch.setattr(manager, "_display_ready", lambda *_args: next(ready))
    monkeypatch.setattr(manager, "_write_xauthority", lambda _display, path: path.touch())
    monkeypatch.setattr(login_runtime.time, "sleep", lambda _seconds: None)

    class FakeProcess:
        pid = 321
        returncode = None

        def poll(self):
            return self.returncode

    popen = Mock(return_value=FakeProcess())
    monkeypatch.setattr(login_runtime.subprocess, "Popen", popen)
    monkeypatch.setattr(manager, "_record_owned_display", lambda **_kwargs: None)

    runtime = manager._ensure_sync()

    assert runtime.display == ":199"
    command = popen.call_args.args[0]
    assert command[:2] == ["Xvfb", ":199"]
    assert "-ac" not in command
    assert "-auth" in command


def test_sigkilled_owned_xvfb_stale_socket_is_recovered_before_restart(
    monkeypatch,
    tmp_path,
):
    _configure_runtime(monkeypatch, tmp_path)
    manager = login_runtime.XvfbManager()
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    lock_path = runtime_dir / "display.lock"
    auth_path = runtime_dir / "display.auth"
    stderr_path = runtime_dir / "stderr.log"
    socket_path = tmp_path / "X199"
    monkeypatch.setattr(
        manager,
        "_paths",
        lambda _number: (lock_path, auth_path, stderr_path, socket_path),
    )

    stale_socket = socket.socket(socket.AF_UNIX)
    stale_socket.bind(str(socket_path))
    stale_socket.close()
    stale_identity = manager._path_identity(socket_path)
    owner_path = manager._owner_path(lock_path)
    stale_owner_record = {
        "version": 2,
        "display": ":199",
        "pid": 111,
        "start_time_ticks": 100,
        "socket": stale_identity,
        "x_lock": None,
    }
    manager._write_owner_record(owner_path, stale_owner_record)

    new_socket: socket.socket | None = None

    class FakeProcess:
        pid = 222
        returncode = None

        def poll(self):
            return self.returncode

    def fake_popen(*_args, **_kwargs):
        nonlocal new_socket
        assert not socket_path.exists()
        assert json.loads(owner_path.read_text(encoding="utf-8")) == (
            stale_owner_record
        )
        new_socket = socket.socket(socket.AF_UNIX)
        new_socket.bind(str(socket_path))
        return FakeProcess()

    monkeypatch.setattr(
        manager,
        "_read_process_identity",
        lambda pid: None if pid == 111 else ("S", 200),
    )
    monkeypatch.setattr(
        manager,
        "_display_ready",
        lambda *_args: manager._proc is not None,
    )
    monkeypatch.setattr(
        manager,
        "_write_xauthority",
        lambda _display, path: path.touch(),
    )
    monkeypatch.setattr(login_runtime.subprocess, "Popen", fake_popen)

    runtime = manager._ensure_sync()

    assert runtime.display == ":199"
    assert manager._path_identity(socket_path) != stale_identity
    persisted = json.loads(owner_path.read_text(encoding="utf-8"))
    assert persisted["version"] == 2
    assert persisted["pid"] == 222
    assert persisted["start_time_ticks"] == 200
    assert persisted["socket"] == manager._path_identity(socket_path)
    assert new_socket is not None
    new_socket.close()


def test_stale_owner_record_does_not_authorize_replaced_socket(
    monkeypatch,
    tmp_path,
):
    _configure_runtime(monkeypatch, tmp_path)
    manager = login_runtime.XvfbManager()
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    lock_path = runtime_dir / "display.lock"
    auth_path = runtime_dir / "display.auth"
    stderr_path = runtime_dir / "stderr.log"
    socket_path = tmp_path / "X199"
    monkeypatch.setattr(
        manager,
        "_paths",
        lambda _number: (lock_path, auth_path, stderr_path, socket_path),
    )

    original = socket.socket(socket.AF_UNIX)
    original.bind(str(socket_path))
    original.close()
    owner_path = manager._owner_path(lock_path)
    manager._write_owner_record(
        owner_path,
        {
            "version": 2,
            "display": ":199",
            "pid": 111,
            "start_time_ticks": 100,
            "socket": manager._path_identity(socket_path),
            "x_lock": None,
        },
    )
    socket_path.unlink()
    replacement = socket.socket(socket.AF_UNIX)
    replacement.bind(str(socket_path))
    replacement.close()
    monkeypatch.setattr(manager, "_read_process_identity", lambda _pid: None)
    monkeypatch.setattr(manager, "_display_ready", lambda *_args: False)

    with pytest.raises(
        login_runtime.LoginRuntimeError,
        match="socket no longer matches",
    ):
        manager._ensure_sync()

    assert socket_path.exists()


def test_recovery_does_not_unlink_socket_replaced_during_process_check(
    monkeypatch,
    tmp_path,
):
    _configure_runtime(monkeypatch, tmp_path)
    manager = login_runtime.XvfbManager()
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    lock_path = runtime_dir / "display.lock"
    auth_path = runtime_dir / "display.auth"
    stderr_path = runtime_dir / "stderr.log"
    socket_path = tmp_path / "X199"
    monkeypatch.setattr(
        manager,
        "_paths",
        lambda _number: (lock_path, auth_path, stderr_path, socket_path),
    )

    original = socket.socket(socket.AF_UNIX)
    original.bind(str(socket_path))
    original.close()
    owner_path = manager._owner_path(lock_path)
    owner_record = {
        "version": 2,
        "display": ":199",
        "pid": 111,
        "start_time_ticks": 100,
        "socket": manager._path_identity(socket_path),
        "x_lock": None,
    }
    manager._write_owner_record(owner_path, owner_record)
    replacement_socket: socket.socket | None = None
    replacement_identity: dict[str, int] | None = None

    def replace_socket_while_pinned(_pid):
        nonlocal replacement_socket, replacement_identity
        socket_path.unlink()
        replacement_socket = socket.socket(socket.AF_UNIX)
        replacement_socket.bind(str(socket_path))
        replacement_identity = manager._path_identity(socket_path)
        return None

    monkeypatch.setattr(
        manager,
        "_read_process_identity",
        replace_socket_while_pinned,
    )
    monkeypatch.setattr(manager, "_display_ready", lambda *_args: False)
    popen = Mock(side_effect=AssertionError("must not replace a foreign X server"))
    monkeypatch.setattr(login_runtime.subprocess, "Popen", popen)

    with pytest.raises(
        login_runtime.LoginRuntimeError,
        match="changed while being quarantined",
    ):
        manager._ensure_sync()

    assert replacement_identity is not None
    restored_identity = manager._path_identity(socket_path)
    assert restored_identity is not None
    for field in ("device", "inode", "uid", "file_type"):
        assert restored_identity[field] == replacement_identity[field]
    assert json.loads(owner_path.read_text(encoding="utf-8")) == owner_record
    assert not list(tmp_path.glob(".ccm-xvfb-recovery-*"))
    popen.assert_not_called()
    assert replacement_socket is not None
    replacement_socket.close()


def test_float_v2_owner_version_is_rejected_without_removing_socket(
    monkeypatch,
    tmp_path,
):
    _configure_runtime(monkeypatch, tmp_path)
    manager = login_runtime.XvfbManager()
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    lock_path = runtime_dir / "display.lock"
    auth_path = runtime_dir / "display.auth"
    stderr_path = runtime_dir / "stderr.log"
    socket_path = tmp_path / "X199"
    monkeypatch.setattr(
        manager,
        "_paths",
        lambda _number: (lock_path, auth_path, stderr_path, socket_path),
    )

    owned_socket = socket.socket(socket.AF_UNIX)
    owned_socket.bind(str(socket_path))
    owned_socket.close()
    owner_path = manager._owner_path(lock_path)
    manager._write_owner_record(
        owner_path,
        {
            "version": 2.0,
            "display": ":199",
            "pid": 111,
            "start_time_ticks": 100,
            "socket": manager._path_identity(socket_path),
            "x_lock": None,
        },
    )
    monkeypatch.setattr(manager, "_display_ready", lambda *_args: False)

    with pytest.raises(login_runtime.LoginRuntimeError, match="Invalid Xvfb owner"):
        manager._ensure_sync()

    assert socket_path.exists()


def test_failed_socket_hardlink_restore_preserves_quarantine(
    monkeypatch,
    tmp_path,
):
    manager = login_runtime.XvfbManager()
    socket_path = tmp_path / "X199"
    quarantine_dir = tmp_path / ".ccm-xvfb-recovery-X199-test"
    quarantine_dir.mkdir(mode=0o700)
    quarantine_path = quarantine_dir / socket_path.name
    quarantined_socket = socket.socket(socket.AF_UNIX)
    quarantined_socket.bind(str(socket_path))
    quarantined_socket.close()
    socket_path.rename(quarantine_path)
    quarantine_identity = manager._path_identity(quarantine_path)

    monkeypatch.setattr(
        login_runtime.os,
        "link",
        Mock(side_effect=OSError("hard links unavailable")),
    )

    assert not manager._restore_quarantined_artifact(
        socket_path,
        quarantine_dir,
        quarantine_path,
    )
    assert not socket_path.exists()
    assert manager._path_identity(quarantine_path) == quarantine_identity
