from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest


SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "guarded_service_entrypoint.py"
)


class _BlockerHandler(BaseHTTPRequestHandler):
    payload: dict = {}

    def do_GET(self):  # noqa: N802 - stdlib handler API
        body = json.dumps(self.payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        return


@pytest.fixture
def blocker_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _BlockerHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server, _BlockerHandler
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _project(tmp_path: Path) -> Path:
    project = tmp_path / "project"
    (project / "backups").mkdir(parents=True)
    (project / "backups").chmod(0o700)
    (project / "backups" / "deployment-lease.lock").touch()
    (project / "backups" / "deployment-lease.lock").chmod(0o600)
    return project


def _start(project: Path, port: int, env: dict[str, str]) -> subprocess.Popen:
    child_pid_file = project / "child.pid"
    return subprocess.Popen(
        [
            sys.executable,
            str(SCRIPT),
            "--project",
            str(project),
            "--port",
            str(port),
            "--",
            sys.executable,
            "-c",
            "import pathlib, sys, time; pathlib.Path(sys.argv[1]).write_text(str(__import__('os').getpid())); time.sleep(30)",
            str(child_pid_file),
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _kill_group(process: subprocess.Popen) -> None:
    if process.poll() is None:
        process.kill()
    process.wait(timeout=5)
    try:
        child_pid = int((Path(process.args[3]) / "child.pid").read_text())
    except (IndexError, OSError, TypeError, ValueError):
        return
    try:
        os.kill(child_pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def test_active_blocker_refuses_external_sigterm(tmp_path, blocker_server):
    server, handler = blocker_server
    handler.payload = {
        "update_blocked": True,
        "active_task_count": 1,
        "active_tasks": [{"id": 7}],
    }
    process = _start(
        _project(tmp_path),
        server.server_port,
        {**os.environ, "AUTH_TOKEN": "test-token"},
    )
    try:
        time.sleep(0.2)
        process.send_signal(signal.SIGTERM)
        time.sleep(0.4)
        assert process.poll() is None
    finally:
        _kill_group(process)


def test_idle_service_forwards_sigterm(tmp_path, blocker_server):
    server, handler = blocker_server
    handler.payload = {
        "update_blocked": False,
        "active_task_count": 0,
        "active_tasks": [],
    }
    process = _start(
        _project(tmp_path),
        server.server_port,
        {**os.environ, "AUTH_TOKEN": "test-token"},
    )
    try:
        time.sleep(0.2)
        process.send_signal(signal.SIGTERM)
        process.wait(timeout=5)
        assert process.returncode != 0
    finally:
        _kill_group(process) if process.poll() is None else None


def test_controlled_handoff_forwards_without_api(tmp_path):
    project = _project(tmp_path)
    lease = project / "backups" / "deployment-lease.json"
    lease.write_text(
        json.dumps(
            {
                "status": "restarting",
                "port": 8999,
                "handoff": True,
                "handoff_provisional": False,
                "owner_token": "deployment-token",
            }
        )
    )
    lease.chmod(0o600)
    process = _start(project, 8999, {**os.environ, "AUTH_TOKEN": ""})
    try:
        time.sleep(0.2)
        process.send_signal(signal.SIGTERM)
        process.wait(timeout=5)
        assert process.returncode != 0
    finally:
        _kill_group(process) if process.poll() is None else None
