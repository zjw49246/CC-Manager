#!/usr/bin/env python3
"""Keep an accidental systemd restart from killing active CCM work.

systemd normally sends SIGTERM to the whole service cgroup.  This entrypoint
is the service's main process and must be paired with ``KillMode=mixed`` and
``SendSIGKILL=no`` so it can decide whether to forward that signal without a
later timeout killing the guarded child.  A controlled UpdateService handoff
is allowed by the durable deployment lease; an ordinary stop/restart is
allowed only after the local blocker endpoint proves the service is idle.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import signal
import stat
import subprocess
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen


def _safe_json(path: Path) -> dict | None:
    try:
        metadata = path.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or metadata.st_nlink != 1
            or metadata.st_mode & 0o022
        ):
            return None
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
        )
    except (FileNotFoundError, OSError):
        return None
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or opened.st_nlink != 1
            or opened.st_mode & 0o022
        ):
            return None
        with os.fdopen(descriptor, encoding="utf-8") as stream:
            descriptor = -1
            payload = json.load(stream)
    except (OSError, ValueError, TypeError):
        return None
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    return payload if isinstance(payload, dict) else None


def _controlled_handoff(project: Path, port: int) -> bool:
    lease = _safe_json(project / "backups" / "deployment-lease.json")
    if not lease:
        return False
    if str(lease.get("port") or "") != str(port):
        return False
    if (
        lease.get("status") != "restarting"
        or not lease.get("handoff")
        or not str(lease.get("owner_token") or "")
    ):
        return False
    if not lease.get("handoff_provisional"):
        return True
    try:
        deadline = datetime.fromisoformat(
            str(lease.get("handoff_ack_deadline") or "")
        )
        if deadline.tzinfo is None:
            deadline = deadline.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return False
    return datetime.now(timezone.utc) <= deadline


def _service_is_idle(port: int, token: str) -> bool:
    if not token:
        return False
    request = Request(
        f"http://127.0.0.1:{port}/api/system/update/blockers",
        headers={"Authorization": f"Bearer {token}"},
        method="GET",
    )
    try:
        with urlopen(request, timeout=3.0) as response:
            payload = json.load(response)
    except (OSError, URLError, ValueError, TypeError):
        return False
    if not isinstance(payload, dict):
        return False
    count = payload.get("active_task_count")
    active_tasks = payload.get("active_tasks")
    if isinstance(count, bool) or not isinstance(count, int):
        return False
    if not isinstance(active_tasks, list):
        return False
    return count == 0 and not active_tasks and payload.get("update_blocked") is False


@contextmanager
def _deployment_lock(project: Path):
    lock_path = project / "backups" / "deployment-lease.lock"
    try:
        parent_metadata = lock_path.parent.lstat()
    except FileNotFoundError:
        lock_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        parent_metadata = lock_path.parent.lstat()
    if not stat.S_ISDIR(parent_metadata.st_mode) or parent_metadata.st_mode & 0o022:
        raise RuntimeError("deployment lock directory is not safe")
    descriptor = os.open(
        lock_path,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or metadata.st_nlink != 1
            or metadata.st_mode & 0o022
        ):
            raise RuntimeError("deployment lock is not a safe regular file")
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _shutdown_allowed(project: Path, port: int) -> bool:
    if _controlled_handoff(project, port):
        return True
    return _service_is_idle(port, os.environ.get("AUTH_TOKEN", "").strip())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = list(args.command)
    if command and command[0] == "--":
        command.pop(0)
    if not command:
        parser.error("a service command is required after --")
    project = Path(args.project).resolve()
    child = subprocess.Popen(command, cwd=project, start_new_session=True)
    forwarded = False

    def handle_signal(signum, _frame) -> None:
        nonlocal forwarded
        if forwarded or child.poll() is not None:
            return
        try:
            with _deployment_lock(project):
                allowed = _shutdown_allowed(project, args.port)
        except Exception as exc:
            print(
                f"guarded service shutdown check failed: {exc}",
                file=sys.stderr,
                flush=True,
            )
            allowed = False
        if not allowed:
            print(
                "guarded service refused SIGTERM while CCM has active or "
                "unverifiable work; use the CCM update/restart operation",
                file=sys.stderr,
                flush=True,
            )
            return
        forwarded = True
        try:
            os.killpg(child.pid, signum)
        except ProcessLookupError:
            pass

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    while True:
        try:
            return child.wait()
        except InterruptedError:
            continue


if __name__ == "__main__":
    raise SystemExit(main())
