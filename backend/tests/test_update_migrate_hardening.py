"""Fault-injection tests for the external deployment worker.

All service and migration commands are local stubs.  Nothing in this module
talks to the host's real systemd unit or runs CCM's real Alembic migrations.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import socket
import sqlite3
import stat
import subprocess
import sys
import time
from pathlib import Path

import pytest


SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "update_migrate.sh"


def _write_secure(path: Path, body: str) -> None:
    path.parent.chmod(0o700)
    path.write_text(body)
    path.chmod(0o600)


def _write_sqlite(path: Path, value: str) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS marker (value TEXT NOT NULL)"
        )
        connection.execute("DELETE FROM marker")
        connection.execute("INSERT INTO marker VALUES (?)", (value,))
        connection.commit()
    finally:
        connection.close()


def _sqlite_value(path: Path) -> str:
    connection = sqlite3.connect(path)
    try:
        row = connection.execute("SELECT value FROM marker").fetchone()
    finally:
        connection.close()
    assert row is not None
    return row[0]


def _git_project(tmp_path: Path, *, frontend: bool = False) -> tuple[Path, str, str]:
    project = tmp_path / "project"
    project.mkdir()
    git_env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "CCM test",
        "GIT_AUTHOR_EMAIL": "ccm@example.invalid",
        "GIT_COMMITTER_NAME": "CCM test",
        "GIT_COMMITTER_EMAIL": "ccm@example.invalid",
    }
    subprocess.run(["git", "init", "-q"], cwd=project, env=git_env, check=True)
    (project / "version.txt").write_text("old")
    if frontend:
        frontend_dir = project / "frontend"
        frontend_dir.mkdir()
        (frontend_dir / "package.json").write_text("{}")
    subprocess.run(["git", "add", "."], cwd=project, env=git_env, check=True)
    subprocess.run(
        ["git", "commit", "-qm", "old"], cwd=project, env=git_env, check=True
    )
    old_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=project,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    (project / "version.txt").write_text("new")
    subprocess.run(
        ["git", "commit", "-am", "new", "-q"],
        cwd=project,
        env=git_env,
        check=True,
    )
    new_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=project,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return project, old_commit, new_commit


def _fake_tools(tmp_path: Path) -> tuple[dict[str, str], Path, Path, Path]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    service_log = tmp_path / "service.log"
    uv_log = tmp_path / "uv.log"
    active_state = tmp_path / "active"
    active_state.write_text("active")

    systemctl = bin_dir / "systemctl"
    systemctl.write_text(
        "#!/bin/bash\n"
        'echo "$@" >> "$FAKE_SERVICE_LOG"\n'
        'if [ "${1:-}" = "--user" ]; then shift; fi\n'
        'case "${1:-}" in\n'
        " stop)\n"
        '   if [ "${FAIL_STOP:-0}" = "1" ]; then\n'
        '     [ "${STOP_LEAVES_ACTIVE:-1}" = "1" ] || '
        'echo inactive > "$FAKE_ACTIVE_STATE"\n'
        "     exit 1\n"
        "   fi\n"
        '   echo inactive > "$FAKE_ACTIVE_STATE"; exit 0;;\n'
        " start)\n"
        '   [ "${FAIL_START:-0}" = "1" ] && exit 1\n'
        '   echo active > "$FAKE_ACTIVE_STATE"; exit 0;;\n'
        " is-active)\n"
        '   grep -qx active "$FAKE_ACTIVE_STATE"; exit $?;;\n'
        "esac\n"
        "exit 0\n"
    )
    sudo = bin_dir / "sudo"
    sudo.write_text(
        "#!/bin/bash\n"
        'if [ "${1:-}" = "-n" ]; then shift; fi\n'
        'exec "$@"\n'
    )
    real_python = shlex.quote(sys.executable)
    uv = bin_dir / "uv"
    uv.write_text(
        "#!/bin/bash\n"
        'echo "$@" >> "$FAKE_UV_LOG"\n'
        'if [[ "$*" == *"alembic upgrade head"* ]]; then\n'
        '  if [ "${UV_SLEEP:-0}" != "0" ]; then sleep "$UV_SLEEP"; fi\n'
        '  if [ -n "${UV_DATABASE:-}" ]; then\n'
        f"    {real_python} -c 'import sqlite3,sys; "
        "c=sqlite3.connect(sys.argv[1]); "
        'c.execute(\"UPDATE marker SET value=?\",(sys.argv[2],)); '
        "c.commit(); c.close()' "
        '"$UV_DATABASE" "${UV_VALUE:-migrated}"\n'
        "  fi\n"
        '  exit "${UV_MIGRATE_RC:-0}"\n'
        "fi\n"
        'if [ "${1:-}" = "sync" ]; then exit "${UV_SYNC_RC:-0}"; fi\n'
        "exit 0\n"
    )
    # The script invokes `python - PORT EXPECTED` only for health checks.
    # Everything else is delegated to real Python.
    python_wrapper = bin_dir / "python-wrapper"
    python_wrapper.write_text(
        "#!/bin/bash\n"
        'if [ "${1:-}" = "-" ] && [[ "${2:-}" =~ ^[0-9]+$ ]]; then\n'
        "  /bin/cat >/dev/null\n"
        '  if [ -n "${FAKE_HEALTH_COUNTER:-}" ]; then\n'
        '    count=0; [ ! -f "$FAKE_HEALTH_COUNTER" ] || '
        'count="$(cat "$FAKE_HEALTH_COUNTER")"\n'
        '    count=$((count + 1)); echo "$count" > "$FAKE_HEALTH_COUNTER"\n'
        '    [ "$count" -gt "${FAKE_HEALTH_FAILS:-0}" ] || exit 1\n'
        '    [ "$count" -le "${FAKE_HEALTH_SUCCESSES:-999}" ] || exit 1\n'
        "  fi\n"
        '  [ "${3:-}" = "${FAKE_RUNNING_COMMIT:-}" ]\n'
        "  exit $?\n"
        "fi\n"
        f"exec {real_python} \"$@\"\n"
    )
    bwrap = bin_dir / "bwrap"
    bwrap.write_text("#!/bin/sh\nexit 0\n")
    socat = bin_dir / "socat"
    socat.write_text("#!/bin/sh\nexit 0\n")
    npm_root = tmp_path / "npm-root"
    apply_seccomp = (
        npm_root
        / "@anthropic-ai/sandbox-runtime/vendor/seccomp/x64/apply-seccomp"
    )
    apply_seccomp.parent.mkdir(parents=True)
    apply_seccomp.write_text("#!/bin/sh\nexit 0\n")
    apply_seccomp.chmod(apply_seccomp.stat().st_mode | stat.S_IEXEC)
    npm = bin_dir / "npm"
    npm.write_text('#!/bin/sh\nprintf "%s\\n" "$FAKE_NPM_ROOT"\n')
    for executable in (
        systemctl,
        sudo,
        uv,
        python_wrapper,
        bwrap,
        socat,
        npm,
    ):
        executable.chmod(executable.stat().st_mode | stat.S_IEXEC)

    env = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "CCM_ESCAPED": "1",
        "CCM_UPDATE_HEALTHCHECK_MODE": "required",
        "CCM_UPDATE_PIDCHECK_MODE": "skip",
        "CCM_UPDATE_START_ATTEMPTS": "1",
        "CCM_UPDATE_HEALTH_ATTEMPTS": "1",
        "CCM_UPDATE_STABILITY_CHECKS": "1",
        "CCM_UPDATE_MIGRATION_TIMEOUT": "2",
        "FAKE_SERVICE_LOG": str(service_log),
        "FAKE_ACTIVE_STATE": str(active_state),
        "FAKE_UV_LOG": str(uv_log),
        "FAKE_NPM_ROOT": str(npm_root),
    }
    return env, service_log, uv_log, python_wrapper


@pytest.fixture
def update_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    paths = [
        Path(f"/tmp/ccm-update-status-{port}.json"),
        Path(f"/tmp/ccm-update-migrate-{port}.log"),
    ]
    for path in paths:
        path.unlink(missing_ok=True)
    yield port
    for path in paths:
        path.unlink(missing_ok=True)


def _run(
    *,
    project: Path,
    old_commit: str,
    backup: Path,
    port: int,
    database: Path,
    mode: str,
    env: dict[str, str],
    python_wrapper: Path,
    frontend_backup: Path | None = None,
    lease: Path | None = None,
    token: str = "",
    policy: str = "",
    operation: str = "",
    run_copy_dir: Path | None = None,
    script: Path = SCRIPT,
    timeout: float = 30,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "bash",
            str(script),
            str(project),
            old_commit,
            str(backup),
            str(port),
            str(database),
            "ccm-test.service",
            mode,
            "",
            str(python_wrapper),
            "user",
            str(frontend_backup) if frontend_backup else "",
            "false" if mode in {"rollback_code", "restart"} else "true",
            "null" if mode == "rollback" else "false",
            str(lease) if lease else "",
            token,
            policy,
            operation or mode,
            str(run_copy_dir) if run_copy_dir else "",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _status(port: int) -> dict:
    return json.loads(Path(f"/tmp/ccm-update-status-{port}.json").read_text())


def test_protocol_v2_and_shell_syntax() -> None:
    content = SCRIPT.read_text()
    assert "CCM_UPDATE_PROTOCOL_VERSION=2" in content.splitlines()[:8]
    subprocess.run(["bash", "-n", str(SCRIPT)], check=True)


def test_systemd_user_manager_deserialize_is_narrowly_allowed() -> None:
    content = SCRIPT.read_text()
    start = content.index("def fixed_systemd_user_manager")
    end = content.index("\n\nunsafe_uninspectable", start)
    namespace = {"Path": Path, "re": re}
    exec(content[start:end], namespace)
    classifier = namespace["fixed_systemd_user_manager"]

    assert classifier("/usr/lib/systemd/systemd --user")
    assert classifier("/usr/lib/systemd/systemd --user --deserialize=19")
    assert not classifier("/usr/lib/systemd/systemd --user --deserialize=x")
    assert not classifier("/usr/lib/systemd/systemd --user --deserialize=1 extra")
    assert not classifier("/tmp/systemd --system")
    assert not classifier("python systemd --user")


def test_server_side_sshd_session_allowlist_requires_root_parent() -> None:
    content = SCRIPT.read_text()
    start = content.index("def fixed_server_side_sshd_session")
    end = content.index("\n\nunsafe_uninspectable", start)
    namespace = {"re": re}
    exec(content[start:end], namespace)
    classifier = namespace["fixed_server_side_sshd_session"]
    valid = {
        "process_name": "sshd-session",
        "command": "sshd-session: ubuntu@pts/0",
        "cgroup": "0::/user.slice/user-1000.slice/session-799.scope",
        "parent_name": "sshd-session",
        "parent_uids": (0, 0, 0, 0),
        "parent_command": "sshd-session: ubuntu [priv]",
        "parent_cgroup": "0::/user.slice/user-1000.slice/session-799.scope",
        "own_uid": 1000,
        "account_name": "ubuntu",
    }

    assert classifier(**valid)
    assert classifier(**{**valid, "command": "sshd-session: ubuntu@notty"})
    assert not classifier(**{**valid, "process_name": "python"})
    assert not classifier(**{**valid, "command": "sshd-session: ubuntu@pts/0 extra"})
    assert not classifier(**{**valid, "cgroup": "0::/user.slice/user-1000.slice/app.slice"})
    assert not classifier(**{**valid, "parent_name": "python"})
    assert not classifier(**{**valid, "parent_uids": (1000, 1000, 1000, 1000)})
    assert not classifier(**{**valid, "parent_command": "python [priv]"})
    assert not classifier(
        **{
            **valid,
            "parent_cgroup": "0::/user.slice/user-1000.slice/session-800.scope",
        }
    )


def test_root_bootstrap_uses_isolated_system_python_before_setuid() -> None:
    content = SCRIPT.read_text()
    bootstrap_start = content.index('if [ "$(id -u)" = "0" ]; then')
    bootstrap_end = content.index("\nfi\n", bootstrap_start)
    bootstrap = content[bootstrap_start:bootstrap_end]
    assert "/usr/bin/env -i" in bootstrap
    assert "/usr/bin/python3 -I -S -" in bootstrap
    assert 'exec "$PYTHON_BIN"' not in bootstrap
    assert "os.fchown(fd, uid, gid)" in bootstrap
    assert "os.chown(" not in bootstrap
    assert "metadata.st_nlink != 1" in bootstrap
    assert "os.setgid(gid)" in bootstrap
    assert "os.setuid(uid)" in bootstrap


def test_linux_prerequisite_gate_fails_before_service_stop(
    tmp_path: Path,
    update_port: int,
) -> None:
    project, old_commit, _ = _git_project(tmp_path)
    database = tmp_path / "live.db"
    backup = tmp_path / "backup.db"
    _write_sqlite(database, "unchanged")
    _write_sqlite(backup, "unchanged")
    env, service_log, uv_log, python_wrapper = _fake_tools(tmp_path)
    (Path(env["PATH"].split(":", 1)[0]) / "socat").unlink()
    lookup_mask = tmp_path / "mask-socat-command.bash"
    lookup_mask.write_text(
        "command() {\n"
        "  if [ \"${1:-}\" = \"-v\" ] && "
        "[ \"${2:-}\" = \"socat\" ]; then return 1; fi\n"
        "  builtin command \"$@\"\n"
        "}\n"
    )
    # Keep the prerequisite probe hermetic even when the test host itself has
    # /usr/bin/socat installed later in PATH.
    env["BASH_ENV"] = str(lookup_mask)

    result = _run(
        project=project,
        old_commit=old_commit,
        backup=backup,
        port=update_port,
        database=database,
        mode="migrate",
        env=env,
        python_wrapper=python_wrapper,
    )

    assert result.returncode != 0
    status = _status(update_port)
    assert status["status"] == "failed"
    assert status["step"] == "deployment_prerequisites"
    assert "bubblewrap/socat" in status["message"]
    assert "apt-get install -y bubblewrap socat" in Path(
        status["log_file"]
    ).read_text()
    assert not service_log.exists() or "stop" not in service_log.read_text()
    assert not uv_log.exists() or not uv_log.read_text()


def test_linux_prerequisite_gate_requires_matching_apply_seccomp(
    tmp_path: Path,
    update_port: int,
) -> None:
    project, old_commit, _ = _git_project(tmp_path)
    database = tmp_path / "live.db"
    backup = tmp_path / "backup.db"
    _write_sqlite(database, "unchanged")
    _write_sqlite(backup, "unchanged")
    env, service_log, uv_log, python_wrapper = _fake_tools(tmp_path)
    helper = (
        Path(env["FAKE_NPM_ROOT"])
        / "@anthropic-ai/sandbox-runtime/vendor/seccomp/x64/apply-seccomp"
    )
    helper.unlink()

    result = _run(
        project=project,
        old_commit=old_commit,
        backup=backup,
        port=update_port,
        database=database,
        mode="migrate",
        env=env,
        python_wrapper=python_wrapper,
    )

    assert result.returncode != 0
    status = _status(update_port)
    assert status["status"] == "failed"
    assert status["step"] == "deployment_prerequisites"
    assert "apply-seccomp" in status["message"]
    log = Path(status["log_file"]).read_text()
    assert "matching apply-seccomp" in log
    assert "@anthropic-ai/sandbox-runtime@0.0.71" in log
    assert not service_log.exists() or "stop" not in service_log.read_text()
    assert not uv_log.exists() or not uv_log.read_text()


def test_success_refreshes_stopped_snapshot_and_requires_target_commit(
    tmp_path: Path, update_port: int
) -> None:
    project, old_commit, new_commit = _git_project(tmp_path)
    database = tmp_path / "live.db"
    backup = tmp_path / "reserved-final.db"
    _write_sqlite(database, "latest-before-stop")
    env, service_log, uv_log, python_wrapper = _fake_tools(tmp_path)
    env.update(
        {
            "UV_DATABASE": str(database),
            "FAKE_RUNNING_COMMIT": new_commit,
        }
    )

    result = _run(
        project=project,
        old_commit=old_commit,
        backup=backup,
        port=update_port,
        database=database,
        mode="migrate",
        env=env,
        python_wrapper=python_wrapper,
    )

    assert result.returncode == 0, result.stderr
    assert _sqlite_value(backup) == "latest-before-stop"
    assert _sqlite_value(database) == "migrated"
    status = _status(update_port)
    assert status["status"] == "completed"
    assert status["expected_commit"] == new_commit
    assert status["database_migration_applied"] is True
    assert "alembic upgrade head" in uv_log.read_text()
    calls = service_log.read_text()
    assert "stop ccm-test.service" in calls
    assert "start ccm-test.service" in calls
    assert "is-active --quiet ccm-test.service" in calls


def test_migration_failure_restores_final_snapshot_and_old_code(
    tmp_path: Path, update_port: int
) -> None:
    project, old_commit, _ = _git_project(tmp_path)
    database = tmp_path / "live.db"
    backup = tmp_path / "reserved-final.db"
    _write_sqlite(database, "latest-before-stop")
    env, _, _, python_wrapper = _fake_tools(tmp_path)
    env.update(
        {
            "UV_DATABASE": str(database),
            "UV_VALUE": "partially-migrated",
            "UV_MIGRATE_RC": "23",
            "FAKE_RUNNING_COMMIT": old_commit,
        }
    )

    result = _run(
        project=project,
        old_commit=old_commit,
        backup=backup,
        port=update_port,
        database=database,
        mode="migrate",
        env=env,
        python_wrapper=python_wrapper,
    )

    assert result.returncode == 0, result.stderr
    assert _sqlite_value(database) == "latest-before-stop"
    assert (project / "version.txt").read_text() == "old"
    status = _status(update_port)
    assert status["status"] == "rolled_back"
    assert status["database_migration_applied"] is False
    assert status["expected_commit"] == old_commit


@pytest.mark.parametrize("operation", ["repair", "update"])
def test_same_commit_migration_failure_keeps_maintenance_fence(
    tmp_path: Path, update_port: int, operation: str
) -> None:
    project, _, current_commit = _git_project(tmp_path)
    database = tmp_path / "live.db"
    backup = tmp_path / "online.db"
    _write_sqlite(database, "before-repair")
    _write_sqlite(backup, "stale")
    env, _, _, python_wrapper = _fake_tools(tmp_path)
    env.update(
        {
            "UV_DATABASE": str(database),
            "UV_VALUE": "partial-repair",
            "UV_MIGRATE_RC": "7",
            "FAKE_RUNNING_COMMIT": current_commit,
        }
    )

    result = _run(
        project=project,
        old_commit=current_commit,
        backup=backup,
        port=update_port,
        database=database,
        mode="migrate",
        env=env,
        python_wrapper=python_wrapper,
        operation=operation,
    )

    assert result.returncode == 0, result.stderr
    status = _status(update_port)
    assert status["status"] == "rolled_back"
    assert status["rollback_incomplete"] is False
    assert status["deployment_incomplete"] is True
    lease = json.loads(
        (project / "backups" / "deployment-lease.json").read_text()
    )
    assert lease["status"] == "rolled_back"
    assert lease["deployment_incomplete"] is True
    assert lease["handoff"] is False
    assert _sqlite_value(database) == "before-repair"


def test_invalid_final_snapshot_never_restores_stale_online_backup(
    tmp_path: Path, update_port: int
) -> None:
    project, old_commit, _ = _git_project(tmp_path)
    database = tmp_path / "not-sqlite.db"
    database.write_bytes(b"latest bytes that must survive")
    backup = tmp_path / "stale.db"
    _write_sqlite(backup, "stale")
    env, _, uv_log, python_wrapper = _fake_tools(tmp_path)
    env["FAKE_RUNNING_COMMIT"] = old_commit

    result = _run(
        project=project,
        old_commit=old_commit,
        backup=backup,
        port=update_port,
        database=database,
        mode="migrate",
        env=env,
        python_wrapper=python_wrapper,
    )

    assert result.returncode == 0, result.stderr
    assert database.read_bytes() == b"latest bytes that must survive"
    assert _sqlite_value(backup) == "stale"
    assert (project / "version.txt").read_text() == "old"
    assert _status(update_port)["status"] == "rolled_back"
    assert "alembic" not in (uv_log.read_text() if uv_log.exists() else "")


def test_code_only_rollback_never_touches_database(
    tmp_path: Path, update_port: int
) -> None:
    project, old_commit, _ = _git_project(tmp_path)
    database = tmp_path / "live.db"
    backup = tmp_path / "stale.db"
    _write_sqlite(database, "latest")
    _write_sqlite(backup, "stale")
    env, _, uv_log, python_wrapper = _fake_tools(tmp_path)
    env["FAKE_RUNNING_COMMIT"] = old_commit

    result = _run(
        project=project,
        old_commit=old_commit,
        backup=backup,
        port=update_port,
        database=database,
        mode="rollback_code",
        env=env,
        python_wrapper=python_wrapper,
    )

    assert result.returncode == 0, result.stderr
    assert _sqlite_value(database) == "latest"
    assert _sqlite_value(backup) == "stale"
    assert (project / "version.txt").read_text() == "old"
    assert "alembic" not in uv_log.read_text()


def test_corrupt_manual_restore_is_terminal_rollback_failed(
    tmp_path: Path, update_port: int
) -> None:
    project, old_commit, _ = _git_project(tmp_path)
    database = tmp_path / "live.db"
    backup = tmp_path / "corrupt.db"
    _write_sqlite(database, "must-survive")
    backup.write_bytes(b"not SQLite")
    env, service_log, _, python_wrapper = _fake_tools(tmp_path)
    env["FAKE_RUNNING_COMMIT"] = old_commit

    result = _run(
        project=project,
        old_commit=old_commit,
        backup=backup,
        port=update_port,
        database=database,
        mode="rollback",
        env=env,
        python_wrapper=python_wrapper,
    )

    assert result.returncode != 0
    assert _sqlite_value(database) == "must-survive"
    status = _status(update_port)
    assert status["status"] == "rollback_failed"
    assert status["step"] == "restore_database"
    assert status["rollback_incomplete"] is True
    calls = service_log.read_text()
    assert "stop ccm-test.service" in calls
    assert "start ccm-test.service" not in calls


def test_rollback_restores_previously_absent_frontend_dist(
    tmp_path: Path, update_port: int
) -> None:
    project, old_commit, _ = _git_project(tmp_path, frontend=True)
    database = tmp_path / "live.db"
    backup = tmp_path / "unused.db"
    _write_sqlite(database, "unchanged")
    _write_sqlite(backup, "unused")
    dist = project / "frontend" / "dist"
    dist.mkdir()
    (dist / "index.html").write_text("new build")
    snapshot = tmp_path / "dist-snapshot"
    snapshot.mkdir()
    (snapshot / ".ccm-dist-absent").write_text("")
    env, _, _, python_wrapper = _fake_tools(tmp_path)
    env["FAKE_RUNNING_COMMIT"] = old_commit

    result = _run(
        project=project,
        old_commit=old_commit,
        backup=backup,
        port=update_port,
        database=database,
        mode="rollback_code",
        env=env,
        python_wrapper=python_wrapper,
        frontend_backup=snapshot,
    )

    assert result.returncode == 0, result.stderr
    assert not dist.exists()
    assert _sqlite_value(database) == "unchanged"


def test_wrong_health_commit_fails_restart_without_mutations(
    tmp_path: Path, update_port: int
) -> None:
    project, _, new_commit = _git_project(tmp_path)
    database = tmp_path / "live.db"
    backup = tmp_path / "unused.db"
    _write_sqlite(database, "unchanged")
    _write_sqlite(backup, "unused")
    env, service_log, uv_log, python_wrapper = _fake_tools(tmp_path)
    env["FAKE_RUNNING_COMMIT"] = "different-commit"

    result = _run(
        project=project,
        old_commit=new_commit,
        backup=backup,
        port=update_port,
        database=database,
        mode="restart",
        env=env,
        python_wrapper=python_wrapper,
        policy="retry",
        operation="restart",
    )

    assert result.returncode != 0
    assert _sqlite_value(database) == "unchanged"
    assert (project / "version.txt").read_text() == "new"
    assert not uv_log.exists() or not uv_log.read_text()
    status = _status(update_port)
    assert status["status"] == "failed"
    assert status["expected_commit"] == new_commit
    assert service_log.read_text().count("start ccm-test.service") == 1


def test_one_transient_health_success_cannot_publish_completed(
    tmp_path: Path, update_port: int
) -> None:
    project, _, new_commit = _git_project(tmp_path)
    database = tmp_path / "live.db"
    backup = tmp_path / "unused.db"
    _write_sqlite(database, "unchanged")
    _write_sqlite(backup, "unused")
    health_counter = tmp_path / "health-counter"
    env, _, _, python_wrapper = _fake_tools(tmp_path)
    env.update(
        {
            "FAKE_RUNNING_COMMIT": new_commit,
            "FAKE_HEALTH_COUNTER": str(health_counter),
            "FAKE_HEALTH_SUCCESSES": "1",
            "CCM_UPDATE_HEALTH_ATTEMPTS": "2",
            "CCM_UPDATE_STABILITY_CHECKS": "2",
        }
    )

    result = _run(
        project=project,
        old_commit=new_commit,
        backup=backup,
        port=update_port,
        database=database,
        mode="restart",
        env=env,
        python_wrapper=python_wrapper,
        policy="retry",
        operation="restart",
    )

    assert result.returncode != 0
    assert int(health_counter.read_text()) == 2
    assert _status(update_port)["status"] == "failed"
    assert _sqlite_value(database) == "unchanged"


def test_delayed_health_within_startup_window_completes_after_stability(
    tmp_path: Path, update_port: int
) -> None:
    project, _, new_commit = _git_project(tmp_path)
    database = tmp_path / "live.db"
    backup = tmp_path / "unused.db"
    _write_sqlite(database, "unchanged")
    _write_sqlite(backup, "unused")
    health_counter = tmp_path / "health-counter"
    env, _, _, python_wrapper = _fake_tools(tmp_path)
    env.update(
        {
            "FAKE_RUNNING_COMMIT": new_commit,
            "FAKE_HEALTH_COUNTER": str(health_counter),
            "FAKE_HEALTH_FAILS": "2",
            "CCM_UPDATE_HEALTH_ATTEMPTS": "4",
            "CCM_UPDATE_STABILITY_CHECKS": "2",
        }
    )

    result = _run(
        project=project,
        old_commit=new_commit,
        backup=backup,
        port=update_port,
        database=database,
        mode="restart",
        env=env,
        python_wrapper=python_wrapper,
        policy="retry",
        operation="restart",
    )

    assert result.returncode == 0, result.stderr
    assert int(health_counter.read_text()) == 4
    assert _status(update_port)["status"] == "completed"
    assert "HEALTH_ATTEMPTS=\"${CCM_UPDATE_HEALTH_ATTEMPTS:-180}\"" in (
        SCRIPT.read_text()
    )


def test_matching_lease_records_active_handoff_then_terminal(
    tmp_path: Path, update_port: int
) -> None:
    project, _, new_commit = _git_project(tmp_path)
    database = tmp_path / "live.db"
    backup = tmp_path / "unused.db"
    _write_sqlite(database, "unchanged")
    _write_sqlite(backup, "unused")
    lease = tmp_path / "backups" / "deployment-lease.json"
    lease.parent.mkdir()
    token = "owner-token"
    _write_secure(
        lease,
        json.dumps(
            {
                "owner_token": token,
                "status": "claimed",
                "handoff": True,
                "operation": "restart",
            }
        )
    )
    env, _, _, python_wrapper = _fake_tools(tmp_path)
    env["FAKE_RUNNING_COMMIT"] = new_commit

    result = _run(
        project=project,
        old_commit=new_commit,
        backup=backup,
        port=update_port,
        database=database,
        mode="restart",
        env=env,
        python_wrapper=python_wrapper,
        lease=lease,
        token=token,
        policy="retry",
        operation="restart",
    )

    assert result.returncode == 0, result.stderr
    terminal = json.loads(lease.read_text())
    assert terminal["status"] == "completed"
    assert terminal["handoff"] is False
    assert terminal["handoff_pid"] == 0
    assert terminal["owner_token"] == token
    assert terminal["expected_commit"] == new_commit
    assert terminal["terminal_intent"] == "completed"


@pytest.mark.parametrize("unsafe_target", ["lease", "lock"])
def test_writable_lease_or_lock_is_rejected_before_service_stop(
    tmp_path: Path, update_port: int, unsafe_target: str
) -> None:
    project, _, new_commit = _git_project(tmp_path)
    database = tmp_path / "live.db"
    backup = tmp_path / "unused.db"
    _write_sqlite(database, "unchanged")
    _write_sqlite(backup, "unused")
    lease = tmp_path / "backups" / "deployment-lease.json"
    lock = tmp_path / "backups" / "deployment-lease.lock"
    lease.parent.mkdir()
    token = "secure-owner"
    original = {
        "owner_token": token,
        "status": "claimed",
        "handoff": True,
        "operation": "restart",
        "port": update_port,
    }
    _write_secure(lease, json.dumps(original))
    _write_secure(lock, "")
    (lease if unsafe_target == "lease" else lock).chmod(0o666)
    env, service_log, _, python_wrapper = _fake_tools(tmp_path)
    env["FAKE_RUNNING_COMMIT"] = new_commit

    result = _run(
        project=project,
        old_commit=new_commit,
        backup=backup,
        port=update_port,
        database=database,
        mode="restart",
        env=env,
        python_wrapper=python_wrapper,
        lease=lease,
        token=token,
        policy="retry",
        operation="restart",
    )

    assert result.returncode != 0
    assert json.loads(lease.read_text()) == original
    assert not service_log.exists() or not service_log.read_text()


def test_mirror_rejects_replaced_status_token_before_stop(
    tmp_path: Path, update_port: int
) -> None:
    project, _, new_commit = _git_project(tmp_path)
    database = tmp_path / "live.db"
    backup = tmp_path / "unused.db"
    _write_sqlite(database, "unchanged")
    _write_sqlite(backup, "unused")
    lease = tmp_path / "backups" / "deployment-lease.json"
    lease.parent.mkdir()
    token = "status-owner"
    original = {
        "owner_token": token,
        "status": "claimed",
        "handoff": True,
        "operation": "restart",
        "port": update_port,
    }
    _write_secure(lease, json.dumps(original))
    env, service_log, _, python_wrapper = _fake_tools(tmp_path)
    env["FAKE_RUNNING_COMMIT"] = new_commit
    status_path = Path(f"/tmp/ccm-update-status-{update_port}.json")
    tampering_wrapper = tmp_path / "bin" / "tampering-python"
    tampering_wrapper.write_text(
        "#!/bin/bash\n"
        f'if [ "${{1:-}}" = "-" ] && [ "${{2:-}}" = "{status_path}" ] '
        '&& [ "${3:-}" = "stopping" ]; then\n'
        f'  "{python_wrapper}" "$@" || exit $?\n'
        f'  "{sys.executable}" -c '
        "'import json,sys; p=sys.argv[1]; d=json.load(open(p)); "
        'd["deployment_owner_token"]="attacker"; '
        "open(p,\"w\").write(json.dumps(d))' "
        f'"{status_path}"\n'
        f'  chmod 0600 "{status_path}"\n'
        "  exit 0\n"
        "fi\n"
        f'exec "{python_wrapper}" "$@"\n'
    )
    tampering_wrapper.chmod(0o700)

    result = _run(
        project=project,
        old_commit=new_commit,
        backup=backup,
        port=update_port,
        database=database,
        mode="restart",
        env=env,
        python_wrapper=tampering_wrapper,
        lease=lease,
        token=token,
        policy="retry",
        operation="restart",
    )

    assert result.returncode != 0
    assert json.loads(lease.read_text()) == original
    assert not service_log.exists() or not service_log.read_text()


def test_lease_token_mismatch_refuses_to_stop_or_overwrite(
    tmp_path: Path, update_port: int
) -> None:
    project, _, new_commit = _git_project(tmp_path)
    database = tmp_path / "live.db"
    backup = tmp_path / "unused.db"
    _write_sqlite(database, "unchanged")
    _write_sqlite(backup, "unused")
    lease = tmp_path / "backups" / "deployment-lease.json"
    lease.parent.mkdir()
    original = {
        "owner_token": "real-owner",
        "status": "claimed",
        "handoff": True,
        "operation": "restart",
    }
    _write_secure(lease, json.dumps(original))
    env, service_log, _, python_wrapper = _fake_tools(tmp_path)
    env["FAKE_RUNNING_COMMIT"] = new_commit

    result = _run(
        project=project,
        old_commit=new_commit,
        backup=backup,
        port=update_port,
        database=database,
        mode="restart",
        env=env,
        python_wrapper=python_wrapper,
        lease=lease,
        token="wrong-owner",
        policy="retry",
        operation="restart",
    )

    assert result.returncode != 0
    assert json.loads(lease.read_text()) == original
    assert not service_log.exists() or not service_log.read_text()
    assert _sqlite_value(database) == "unchanged"


def test_late_worker_with_same_token_cannot_cross_terminal_lease(
    tmp_path: Path, update_port: int
) -> None:
    project, _, new_commit = _git_project(tmp_path)
    database = tmp_path / "live.db"
    backup = tmp_path / "unused.db"
    _write_sqlite(database, "unchanged")
    _write_sqlite(backup, "unused")
    lease = tmp_path / "backups" / "deployment-lease.json"
    lease.parent.mkdir()
    token = "expired-handoff-owner"
    terminal = {
        "owner_token": token,
        "status": "failed",
        "handoff": False,
        "deployment_incomplete": True,
        "operation": "restart",
    }
    _write_secure(lease, json.dumps(terminal))
    env, service_log, _, python_wrapper = _fake_tools(tmp_path)
    env["FAKE_RUNNING_COMMIT"] = new_commit

    result = _run(
        project=project,
        old_commit=new_commit,
        backup=backup,
        port=update_port,
        database=database,
        mode="restart",
        env=env,
        python_wrapper=python_wrapper,
        lease=lease,
        token=token,
        policy="retry",
        operation="restart",
    )

    assert result.returncode != 0
    assert json.loads(lease.read_text()) == terminal
    assert not service_log.exists() or not service_log.read_text()
    assert _sqlite_value(database) == "unchanged"


def test_legacy_ten_arg_worker_self_claims_clean_terminal_repo_lease(
    tmp_path: Path, update_port: int
) -> None:
    project, _, new_commit = _git_project(tmp_path)
    database = tmp_path / "live.db"
    backup = tmp_path / "unused.db"
    _write_sqlite(database, "unchanged")
    _write_sqlite(backup, "unused")
    lease = project / "backups" / "deployment-lease.json"
    lease.parent.mkdir()
    _write_secure(
        lease,
        json.dumps(
            {
                "owner_token": "previous-owner",
                "status": "completed",
                "handoff": False,
                "deployment_incomplete": False,
            }
        )
    )
    env, _, _, python_wrapper = _fake_tools(tmp_path)
    env["FAKE_RUNNING_COMMIT"] = new_commit

    # No lease/token arguments: this models an old in-memory backend executing
    # the newly pulled protocol-v2 script.
    result = _run(
        project=project,
        old_commit=new_commit,
        backup=backup,
        port=update_port,
        database=database,
        mode="restart",
        env=env,
        python_wrapper=python_wrapper,
        policy="retry",
        operation="restart",
    )

    assert result.returncode == 0, result.stderr
    terminal = json.loads(lease.read_text())
    assert terminal["status"] == "completed"
    assert terminal["owner_token"].startswith("legacy-")
    assert terminal["owner_token"] != "previous-owner"
    assert terminal["handoff"] is False


def test_legacy_worker_refuses_active_repo_lease_before_stop(
    tmp_path: Path, update_port: int
) -> None:
    project, _, new_commit = _git_project(tmp_path)
    database = tmp_path / "live.db"
    backup = tmp_path / "unused.db"
    _write_sqlite(database, "unchanged")
    _write_sqlite(backup, "unused")
    lease = project / "backups" / "deployment-lease.json"
    lease.parent.mkdir()
    active = {
        "owner_token": "other-owner",
        "status": "starting",
        "handoff": True,
        "deployment_incomplete": True,
    }
    _write_secure(lease, json.dumps(active))
    env, service_log, _, python_wrapper = _fake_tools(tmp_path)
    env["FAKE_RUNNING_COMMIT"] = new_commit

    result = _run(
        project=project,
        old_commit=new_commit,
        backup=backup,
        port=update_port,
        database=database,
        mode="restart",
        env=env,
        python_wrapper=python_wrapper,
        policy="retry",
        operation="restart",
    )

    assert result.returncode != 0
    assert json.loads(lease.read_text()) == active
    assert not service_log.exists() or not service_log.read_text()


def test_open_sqlite_handle_blocks_migration_and_preserves_db(
    tmp_path: Path, update_port: int
) -> None:
    project, old_commit, _ = _git_project(tmp_path)
    database = tmp_path / "live.db"
    backup = tmp_path / "stale.db"
    ready = tmp_path / "ready"
    _write_sqlite(database, "latest")
    _write_sqlite(backup, "stale")
    env, _, uv_log, python_wrapper = _fake_tools(tmp_path)
    env["FAKE_RUNNING_COMMIT"] = old_commit
    holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "import pathlib,sqlite3,sys,time;"
                "c=sqlite3.connect(sys.argv[1]);"
                "pathlib.Path(sys.argv[2]).write_text('ready');"
                "time.sleep(30)"
            ),
            str(database),
            str(ready),
        ]
    )
    try:
        deadline = time.time() + 5
        while time.time() < deadline and not ready.exists():
            time.sleep(0.02)
        assert ready.exists()
        result = _run(
            project=project,
            old_commit=old_commit,
            backup=backup,
            port=update_port,
            database=database,
            mode="migrate",
            env=env,
            python_wrapper=python_wrapper,
        )
    finally:
        holder.terminate()
        holder.wait(timeout=5)

    assert result.returncode == 0, result.stderr
    assert _sqlite_value(database) == "latest"
    assert _sqlite_value(backup) == "stale"
    assert "alembic" not in (uv_log.read_text() if uv_log.exists() else "")
    log = Path(f"/tmp/ccm-update-migrate-{update_port}.log").read_text()
    assert f"pid={holder.pid}" in log


def test_uninspectable_same_user_fd_table_fails_closed(
    tmp_path: Path, update_port: int
) -> None:
    project, old_commit, _ = _git_project(tmp_path)
    database = tmp_path / "live.db"
    backup = tmp_path / "stale.db"
    ready = tmp_path / "ready"
    _write_sqlite(database, "latest")
    _write_sqlite(backup, "stale")
    env, _, uv_log, python_wrapper = _fake_tools(tmp_path)
    env["FAKE_RUNNING_COMMIT"] = old_commit
    holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "import ctypes,pathlib,sqlite3,sys,time;"
                "c=sqlite3.connect(sys.argv[1]);"
                "ctypes.CDLL(None).prctl(4,0,0,0,0);"
                "pathlib.Path(sys.argv[2]).write_text('ready');"
                "time.sleep(30)"
            ),
            str(database),
            str(ready),
        ]
    )
    try:
        deadline = time.time() + 5
        while time.time() < deadline and not ready.exists():
            time.sleep(0.02)
        assert ready.exists()
        result = _run(
            project=project,
            old_commit=old_commit,
            backup=backup,
            port=update_port,
            database=database,
            mode="migrate",
            env=env,
            python_wrapper=python_wrapper,
        )
    finally:
        holder.terminate()
        holder.wait(timeout=5)

    assert result.returncode == 0, result.stderr
    assert _sqlite_value(database) == "latest"
    assert _sqlite_value(backup) == "stale"
    assert "alembic" not in (uv_log.read_text() if uv_log.exists() else "")
    assert _status(update_port)["step"] == "backup_database"


def test_migration_timeout_rolls_back_and_run_copy_is_removed(
    tmp_path: Path, update_port: int
) -> None:
    project, old_commit, _ = _git_project(tmp_path)
    database = tmp_path / "live.db"
    backup = tmp_path / "stale.db"
    _write_sqlite(database, "latest")
    _write_sqlite(backup, "stale")
    run_copy = Path("/tmp") / f"ccm-update-run-test-{update_port}"
    run_copy.mkdir()
    copied_script = run_copy / "update_migrate.sh"
    copied_script.write_bytes(SCRIPT.read_bytes())
    copied_script.chmod(0o700)
    env, _, _, python_wrapper = _fake_tools(tmp_path)
    env.update({"UV_SLEEP": "10", "FAKE_RUNNING_COMMIT": old_commit})

    result = _run(
        project=project,
        old_commit=old_commit,
        backup=backup,
        port=update_port,
        database=database,
        mode="migrate",
        env=env,
        python_wrapper=python_wrapper,
        run_copy_dir=run_copy,
        script=copied_script,
        timeout=20,
    )

    assert result.returncode == 0, result.stderr
    assert not run_copy.exists()
    assert _sqlite_value(database) == "latest"
    assert (project / "version.txt").read_text() == "old"
    assert _status(update_port)["status"] == "rolled_back"


def test_fixed_log_symlink_is_rejected_before_service_stop(
    tmp_path: Path, update_port: int
) -> None:
    project, _, new_commit = _git_project(tmp_path)
    database = tmp_path / "live.db"
    backup = tmp_path / "unused.db"
    victim = tmp_path / "victim"
    victim.write_text("must survive")
    log_path = Path(f"/tmp/ccm-update-migrate-{update_port}.log")
    log_path.symlink_to(victim)
    _write_sqlite(database, "unchanged")
    _write_sqlite(backup, "unused")
    env, service_log, _, python_wrapper = _fake_tools(tmp_path)
    env["FAKE_RUNNING_COMMIT"] = new_commit

    result = _run(
        project=project,
        old_commit=new_commit,
        backup=backup,
        port=update_port,
        database=database,
        mode="restart",
        env=env,
        python_wrapper=python_wrapper,
        policy="retry",
    )

    assert result.returncode != 0
    assert victim.read_text() == "must survive"
    assert not service_log.exists()


def test_status_helper_failure_is_not_masked_by_old_status_or_mirror(
    tmp_path: Path, update_port: int
) -> None:
    project, _, new_commit = _git_project(tmp_path)
    database = tmp_path / "live.db"
    backup = tmp_path / "unused.db"
    _write_sqlite(database, "unchanged")
    _write_sqlite(backup, "unused")
    victim = tmp_path / "status-victim"
    victim.write_text('{"status":"completed"}')
    status_path = Path(f"/tmp/ccm-update-status-{update_port}.json")
    status_path.symlink_to(victim)
    env, service_log, _, python_wrapper = _fake_tools(tmp_path)
    env["FAKE_RUNNING_COMMIT"] = new_commit

    result = _run(
        project=project,
        old_commit=new_commit,
        backup=backup,
        port=update_port,
        database=database,
        mode="restart",
        env=env,
        python_wrapper=python_wrapper,
        policy="retry",
    )

    assert result.returncode != 0
    assert victim.read_text() == '{"status":"completed"}'
    assert not service_log.exists() or not service_log.read_text()
