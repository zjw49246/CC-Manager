#!/bin/bash
# CCM's external deployment worker.  Keep this protocol backwards compatible:
# an old in-memory backend may execute the newly pulled script with only the
# first ten arguments.
CCM_UPDATE_PROTOCOL_VERSION=2
set -uo pipefail

PROJECT_DIR="${1:?project directory is required}"
OLD_COMMIT="${2:?old commit is required}"
BACKUP_FILE="${3:-}"
PORT="${4:?port is required}"
DB_FILE="${5:-claude_manager.db}"
SERVICE_NAME="${6:-ccm.service}"
MODE="${7:-migrate}"                 # migrate | rollback | rollback_code | restart
SERVER_PID="${8:-}"                  # bare uvicorn PID, or live service PID
PYTHON_BIN="${9:-python3}"
SYSTEMD_SCOPE="${10:-auto}"          # auto | user | system
FRONTEND_BACKUP_DIR="${11:-}"
DATABASE_MIGRATION_REQUIRED="${12:-}"
DATABASE_MIGRATION_APPLIED="${13:-false}"  # true | false | null
DEPLOYMENT_LEASE_FILE="${14:-}"
DEPLOYMENT_OWNER_TOKEN="${15:-}"
RESTART_FAILURE_POLICY="${16:-}"     # retry | rollback
DEPLOYMENT_OPERATION="${17:-}"
RUN_COPY_DIR="${18:-}"
LEGACY_LEASE_BOOTSTRAP=0

STATUS_FILE="/tmp/ccm-update-status-${PORT}.json"
LOG_FILE="/tmp/ccm-update-migrate-${PORT}.log"
HEALTHCHECK_MODE="${CCM_UPDATE_HEALTHCHECK_MODE:-required}"
PIDCHECK_MODE="${CCM_UPDATE_PIDCHECK_MODE:-required}"
START_ATTEMPTS="${CCM_UPDATE_START_ATTEMPTS:-3}"
HEALTH_ATTEMPTS="${CCM_UPDATE_HEALTH_ATTEMPTS:-180}"
STABILITY_CHECKS="${CCM_UPDATE_STABILITY_CHECKS:-3}"
MIGRATION_TIMEOUT="${CCM_UPDATE_MIGRATION_TIMEOUT:-120}"
SELF="$(readlink -f "$0")"

case "$PORT" in
    ''|*[!0-9]*) echo "invalid port: $PORT" >&2; exit 2 ;;
esac
case "$MODE" in
    migrate|rollback|rollback_code|restart) ;;
    *) echo "invalid mode: $MODE" >&2; exit 2 ;;
esac
for value in "$START_ATTEMPTS" "$HEALTH_ATTEMPTS" "$STABILITY_CHECKS" \
    "$MIGRATION_TIMEOUT"; do
    case "$value" in
        ''|*[!0-9]*|0) echo "invalid positive numeric setting: $value" >&2; exit 2 ;;
    esac
done
case "$HEALTHCHECK_MODE" in
    required|skip) ;;
    *) echo "invalid healthcheck mode: $HEALTHCHECK_MODE" >&2; exit 2 ;;
esac
case "$PIDCHECK_MODE" in
    required|skip) ;;
    *) echo "invalid pidcheck mode: $PIDCHECK_MODE" >&2; exit 2 ;;
esac
if [ "$STABILITY_CHECKS" -gt "$HEALTH_ATTEMPTS" ]; then
    echo "stability checks may not exceed health attempts" >&2
    exit 2
fi

# Old backends do not pass the migration decision.  Infer exactly the behavior
# of their two modes while retaining the new tri-state status fields.
if [ -z "$DATABASE_MIGRATION_REQUIRED" ]; then
    case "$MODE" in
        migrate|rollback) DATABASE_MIGRATION_REQUIRED="true" ;;
        rollback_code|restart) DATABASE_MIGRATION_REQUIRED="false" ;;
    esac
fi
case "$DATABASE_MIGRATION_REQUIRED" in
    true|false) ;;
    *) echo "invalid database_migration_required" >&2; exit 2 ;;
esac
case "$DATABASE_MIGRATION_APPLIED" in
    true|false|null) ;;
    *) echo "invalid database_migration_applied" >&2; exit 2 ;;
esac
if { [ -n "$DEPLOYMENT_LEASE_FILE" ] && [ -z "$DEPLOYMENT_OWNER_TOKEN" ]; } || \
   { [ -z "$DEPLOYMENT_LEASE_FILE" ] && [ -n "$DEPLOYMENT_OWNER_TOKEN" ]; }; then
    echo "deployment lease file and owner token must be supplied together" >&2
    exit 2
fi
if [ -z "$DEPLOYMENT_LEASE_FILE" ] && [ -z "$DEPLOYMENT_OWNER_TOKEN" ]; then
    LEGACY_LEASE_BOOTSTRAP=1
fi
if [ "$MODE" = "restart" ]; then
    case "$RESTART_FAILURE_POLICY" in
        ""|retry|rollback) ;;
        *) echo "invalid restart failure policy" >&2; exit 2 ;;
    esac
fi
if [ -z "$DEPLOYMENT_OPERATION" ]; then
    case "$MODE" in
        migrate) DEPLOYMENT_OPERATION="update" ;;
        restart) DEPLOYMENT_OPERATION="restart" ;;
        rollback|rollback_code) DEPLOYMENT_OPERATION="rollback" ;;
    esac
fi

# A system-scope transient unit is normally created by root.  New backends pass
# --uid/--gid to systemd-run, but this bootstrap is needed for the first update
# from an old backend.  Drop to the live service identity before writing the
# repo, lease, status, or log.
if [ "$(id -u)" = "0" ]; then
    case "$SERVER_PID" in
        ''|*[!0-9]*)
            echo "root launcher cannot verify service identity" >&2
            exit 1
            ;;
    esac
    [ -r "/proc/${SERVER_PID}/status" ] || {
        echo "root launcher cannot read service identity" >&2
        exit 1
    }
    # Never execute the caller-supplied venv interpreter with uid 0: its
    # sitecustomize/.pth/native modules may be writable by the service user.
    # Isolated, no-site system Python performs only the privilege drop.
    exec /usr/bin/env -i \
        PATH=/usr/sbin:/usr/bin:/sbin:/bin \
        /usr/bin/python3 -I -S - \
        "$SERVER_PID" "$SELF" "$LOG_FILE" "$STATUS_FILE" "$@" <<'PY'
import os
import pwd
import stat
import sys
from pathlib import Path

pid = int(sys.argv[1])
script = sys.argv[2]
fixed_paths = [Path(sys.argv[3]), Path(sys.argv[4])]
script_args = sys.argv[5:]
fields = {}
for line in Path(f"/proc/{pid}/status").read_text().splitlines():
    if line.startswith("Uid:"):
        fields["uid"] = int(line.split()[1])
    elif line.startswith("Gid:"):
        fields["gid"] = int(line.split()[1])
uid, gid = fields.get("uid"), fields.get("gid")
if uid is None or gid is None or uid == 0:
    raise SystemExit("refusing invalid/root service identity")
account = pwd.getpwuid(uid)
service_environment = {}
try:
    raw_environment = Path(f"/proc/{pid}/environ").read_bytes()
except OSError:
    raw_environment = b""
for entry in raw_environment.split(b"\0"):
    if not entry or b"=" not in entry:
        continue
    key_bytes, value_bytes = entry.split(b"=", 1)
    key = key_bytes.decode(errors="surrogateescape")
    value = value_bytes.decode(errors="surrogateescape")
    # These affect an interpreter or dynamic loader before application code.
    # The selected venv Python is used only after setuid, but deployment
    # behavior must not inherit ambient injection settings either.
    if key.startswith(("PYTHON", "LD_")) or key in {
        "BASH_ENV", "ENV", "CDPATH", "GLOBIGNORE", "SHELLOPTS",
    }:
        continue
    service_environment[key] = value
for path in fixed_paths:
    if path.parent != Path("/tmp") or not path.exists():
        continue
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        metadata = os.fstat(fd)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_uid not in {0, uid}
            or metadata.st_mode & 0o022
        ):
            raise SystemExit(f"unsafe bootstrap path: {path}")
        os.fchown(fd, uid, gid)
    finally:
        os.close(fd)
os.initgroups(account.pw_name, gid)
os.setgid(gid)
os.setuid(uid)
service_environment.update({
    "HOME": account.pw_dir,
    "USER": account.pw_name,
    "LOGNAME": account.pw_name,
    "CCM_PRIVILEGE_DROPPED": "1",
})
service_environment.setdefault("PATH", "/usr/local/bin:/usr/bin:/bin")
os.execve("/bin/bash", ["/bin/bash", script, *script_args], service_environment)
PY
fi

SCRIPT_RUN_UID="$(id -u)"
SCRIPT_RUN_GID="$(id -g)"
SCRIPT_RUN_HOME="${HOME:-}"

cgroup_text() {
    cat /proc/self/cgroup 2>/dev/null || true
}

resolve_scope() {
    if [ "$SYSTEMD_SCOPE" = "user" ] || [ "$SYSTEMD_SCOPE" = "system" ]; then
        printf '%s\n' "$SYSTEMD_SCOPE"
        return
    fi
    case "$(cgroup_text)" in
        *"/system.slice/"*) printf '%s\n' "system" ;;
        *) printf '%s\n' "user" ;;
    esac
}

systemctl_cmd() {
    if [ "$(resolve_scope)" = "system" ]; then
        sudo -n systemctl "$@"
    else
        systemctl --user "$@"
    fi
}

systemd_run_cmd() {
    if [ "$(resolve_scope)" = "system" ]; then
        sudo -n systemd-run "$@"
    else
        systemd-run --user "$@"
    fi
}

# Old backends launch the script inside the service cgroup.  Re-exec into a
# transient cgroup before `systemctl stop`; KillMode=control-group would
# otherwise kill the only process capable of starting the service again.
if [ -z "${CCM_ESCAPED:-}" ] && [ "$SERVICE_NAME" != "-" ] && \
   command -v systemd-run >/dev/null 2>&1; then
    case "$(cgroup_text)" in
        *"/${SERVICE_NAME}"*)
            ESCAPE_ID_ARGS=()
            if [ "$(resolve_scope)" = "system" ]; then
                ESCAPE_ID_ARGS=("--uid=${SCRIPT_RUN_UID}" "--gid=${SCRIPT_RUN_GID}")
            fi
            if systemd_run_cmd --collect \
                "--unit=ccm-update-escape-${PORT}-$$" \
                "--working-directory=${PROJECT_DIR}" \
                "${ESCAPE_ID_ARGS[@]}" \
                "--setenv=CCM_ESCAPED=1" \
                "--setenv=PATH=${PATH}" \
                "--setenv=HOME=${SCRIPT_RUN_HOME}" \
                "--setenv=SYSTEMD_SCOPE=$(resolve_scope)" \
                "--property=StandardOutput=journal" \
                "--property=StandardError=journal" \
                /bin/bash "$SELF" "$@"; then
                exit 0
            fi
            echo "cgroup escape failed; refusing to stop service" >&2
            exit 1
            ;;
    esac
fi

cd "$PROJECT_DIR" || exit 1
umask 077

# Fixed /tmp names are retained for UI compatibility.  Create/truncate the log
# without following links and reject non-regular or multiply-linked targets.
# The worker runs as the service user even for system-scope deployments.
if ! "$PYTHON_BIN" - "$LOG_FILE" <<'PY'
import os
import stat
import sys
from pathlib import Path

path = Path(sys.argv[1])
try:
    metadata = path.lstat()
except FileNotFoundError:
    metadata = None
if metadata is not None and (
    not stat.S_ISREG(metadata.st_mode)
    or metadata.st_uid != os.getuid()
    or metadata.st_nlink != 1
):
    raise SystemExit(f"unsafe update log path: {path}")
flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
flags |= getattr(os, "O_NOFOLLOW", 0)
fd = os.open(path, flags, 0o600)
try:
    metadata = os.fstat(fd)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or metadata.st_nlink != 1
    ):
        raise RuntimeError(f"unsafe opened update log: {path}")
    os.fchmod(fd, 0o600)
    os.fsync(fd)
finally:
    os.close(fd)
PY
then
    exit 1
fi
# Open once without truncating, verify the opened descriptor is still the exact
# safe path, and route every later log write through that descriptor.  This
# closes the lstat/open race and avoids repeatedly resolving a fixed /tmp name.
exec 9>> "$LOG_FILE"
if ! "$PYTHON_BIN" - "$LOG_FILE" <<'PY'
import os
import stat
import sys
from pathlib import Path

path_metadata = Path(sys.argv[1]).lstat()
fd_metadata = os.fstat(9)
if (
    not stat.S_ISREG(path_metadata.st_mode)
    or not stat.S_ISREG(fd_metadata.st_mode)
    or path_metadata.st_uid != os.getuid()
    or fd_metadata.st_uid != os.getuid()
    or path_metadata.st_nlink != 1
    or fd_metadata.st_nlink != 1
    or (path_metadata.st_dev, path_metadata.st_ino)
       != (fd_metadata.st_dev, fd_metadata.st_ino)
):
    raise SystemExit("update log descriptor/path identity mismatch")
PY
then
    exit 1
fi

TARGET_COMMIT=""
EXPECTED_COMMIT=""
LAST_STATUS=""
HANDOFF_MODE="$MODE"
TERMINAL_INTENT="completed"
ROLLBACK_INCOMPLETE="false"
DEPLOYMENT_INCOMPLETE="false"
SERVICE_STOPPED=0
STARTED=0
NEW_SERVER_PID=""
VERIFIED_SERVER_PID=""
ORIGINAL_SERVER_PID_START=""
FINAL_BACKUP_READY=0
MIGRATION_STARTED=0
TERMINAL_WRITTEN=0
MIGRATION_PROCESS_PID=""
if [ "$MODE" = "rollback" ] || [ "$MODE" = "rollback_code" ]; then
    TERMINAL_INTENT="rolled_back"
fi

process_start_identity() {
    local pid="$1"
    case "$pid" in
        ''|*[!0-9]*) return 1 ;;
    esac
    [ -r "/proc/${pid}/stat" ] || return 1
    sed -E 's/^.*\) //' "/proc/${pid}/stat" 2>/dev/null | awk '{print $20}'
}

ORIGINAL_SERVER_PID_START="$(process_start_identity "$SERVER_PID" || true)"

claim_legacy_deployment_lease() {
    local lease_path="$PROJECT_DIR/backups/deployment-lease.json"
    local lock_path="$PROJECT_DIR/backups/deployment-lease.lock"
    local token
    if ! token="$("$PYTHON_BIN" - \
        "$lease_path" "$lock_path" "$PORT" "$MODE" \
        "$DEPLOYMENT_OPERATION" "$OLD_COMMIT" "$TARGET_COMMIT" "$$" <<'PY'
import fcntl
import json
import os
import secrets
import stat
import sys
from datetime import datetime, timezone
from pathlib import Path

lease_path, lock_path = Path(sys.argv[1]), Path(sys.argv[2])
port, mode, operation, old_commit, target_commit = sys.argv[3:8]
worker_pid = int(sys.argv[8])
backups = lease_path.parent
backups.mkdir(mode=0o700, parents=True, exist_ok=True)
if backups.resolve() != backups.absolute():
    raise SystemExit("legacy deployment lease directory contains a symlink")
directory_metadata = backups.stat()
if (
    not stat.S_ISDIR(directory_metadata.st_mode)
    or directory_metadata.st_uid != os.getuid()
    or directory_metadata.st_mode & 0o022
):
    raise SystemExit("unsafe legacy deployment lease directory")

lock_flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
lock_fd = os.open(lock_path, lock_flags, 0o600)
with os.fdopen(lock_fd, "a+b") as lock:
    lock_metadata = os.fstat(lock.fileno())
    if (
        not stat.S_ISREG(lock_metadata.st_mode)
        or lock_metadata.st_uid != os.getuid()
        or lock_metadata.st_nlink != 1
        or lock_metadata.st_mode & 0o022
    ):
        raise SystemExit("unsafe legacy deployment lease lock")
    os.fchmod(lock.fileno(), 0o600)
    fcntl.flock(lock.fileno(), fcntl.LOCK_EX)

    try:
        existing_metadata = lease_path.lstat()
    except FileNotFoundError:
        existing_metadata = None
    if existing_metadata is not None:
        if (
            not stat.S_ISREG(existing_metadata.st_mode)
            or existing_metadata.st_uid != os.getuid()
            or existing_metadata.st_nlink != 1
            or existing_metadata.st_mode & 0o022
        ):
            raise SystemExit("unsafe existing deployment lease")
        read_flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        lease_fd = os.open(lease_path, read_flags)
        with os.fdopen(lease_fd) as stream:
            metadata = os.fstat(stream.fileno())
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != os.getuid()
                or metadata.st_nlink != 1
                or metadata.st_mode & 0o022
            ):
                raise SystemExit("unsafe existing deployment lease")
            existing = json.load(stream)
        if not isinstance(existing, dict):
            raise SystemExit("existing deployment lease is not an object")
        status = str(existing.get("status") or "")
        if status not in {
            "completed", "rolled_back", "failed", "rollback_failed"
        }:
            raise SystemExit(
                f"legacy update cannot take active/unknown lease: {status!r}"
            )
        if (
            existing.get("deployment_incomplete")
            or existing.get("rollback_incomplete")
            or status == "rollback_failed"
        ):
            raise SystemExit("legacy update cannot take incomplete lease")

    token = "legacy-" + secrets.token_hex(24)
    terminal_intent = (
        "rolled_back" if mode in {"rollback", "rollback_code"} else "completed"
    )
    try:
        fields = Path(f"/proc/{worker_pid}/stat").read_text().rsplit(
            ")", 1
        )[1].split()
        pid_start = fields[19]
    except (OSError, IndexError):
        raise SystemExit("cannot establish legacy worker PID identity")
    payload = {
        "status": "claimed",
        "message": "legacy backend deployment claimed by protocol-v2 worker",
        "step": "deployment_lease",
        "old_commit": old_commit,
        "target_commit": target_commit,
        "new_commit": target_commit,
        "expected_commit": target_commit,
        "mode": mode,
        "operation": operation,
        "handoff_mode": mode,
        "terminal_intent": terminal_intent,
        "rollback_incomplete": False,
        "database_migration_required": mode in {"migrate", "rollback"},
        "database_migration_applied": None if mode == "rollback" else False,
        "owner_token": token,
        "deployment_owner_token": token,
        "owner_pid": worker_pid,
        "owner_pid_start": pid_start,
        "handoff": True,
        "handoff_pid": worker_pid,
        "handoff_pid_start": pid_start,
        "handoff_provisional": False,
        "port": int(port),
        "timestamp": datetime.now(timezone.utc).astimezone().isoformat(),
    }
    temporary = lease_path.with_name(
        f".{lease_path.name}.legacy-{worker_pid}.tmp"
    )
    try:
        fd = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        with os.fdopen(fd, "w") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, lease_path)
        directory_fd = os.open(backups, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)
print(token)
PY
    )"; then
        return 1
    fi
    [ -n "$token" ] || return 1
    DEPLOYMENT_LEASE_FILE="$lease_path"
    DEPLOYMENT_OWNER_TOKEN="$token"
}

deployment_lease_lock_file() {
    case "$DEPLOYMENT_LEASE_FILE" in
        *.json) printf '%s.lock\n' "${DEPLOYMENT_LEASE_FILE%.json}" ;;
        *) printf '%s.lock\n' "$DEPLOYMENT_LEASE_FILE" ;;
    esac
}

assert_deployment_lease_owner() {
    [ -n "$DEPLOYMENT_LEASE_FILE" ] || return 0
    "$PYTHON_BIN" - "$DEPLOYMENT_LEASE_FILE" \
        "$(deployment_lease_lock_file)" "$DEPLOYMENT_OWNER_TOKEN" \
        "$DEPLOYMENT_OPERATION" <<'PY'
import fcntl
import json
import os
import stat
import sys
from pathlib import Path

lease_path, lock_path = Path(sys.argv[1]), Path(sys.argv[2])
token, operation = sys.argv[3:5]
lock_path.parent.mkdir(parents=True, exist_ok=True)
if lease_path.parent.resolve() != lease_path.parent.absolute():
    raise SystemExit("deployment lease parent may not contain symlinks")
lease_metadata = lease_path.lstat()
if (
    not stat.S_ISREG(lease_metadata.st_mode)
    or lease_metadata.st_uid != os.getuid()
    or lease_metadata.st_nlink != 1
    or lease_metadata.st_mode & 0o022
):
    raise SystemExit("unsafe deployment lease")
flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
lock_fd = os.open(lock_path, flags, 0o600)
with os.fdopen(lock_fd, "a+b") as lock:
    lock_metadata = os.fstat(lock.fileno())
    if (
        not stat.S_ISREG(lock_metadata.st_mode)
        or lock_metadata.st_uid != os.getuid()
        or lock_metadata.st_nlink != 1
        or lock_metadata.st_mode & 0o022
    ):
        raise SystemExit("unsafe deployment lease lock")
    fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
    read_flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    lease_fd = os.open(lease_path, read_flags)
    with os.fdopen(lease_fd) as stream:
        opened_metadata = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(opened_metadata.st_mode)
            or opened_metadata.st_uid != os.getuid()
            or opened_metadata.st_nlink != 1
            or opened_metadata.st_mode & 0o022
        ):
            raise SystemExit("unsafe opened deployment lease")
        payload = json.load(stream)
    if not isinstance(payload, dict) or payload.get("owner_token") != token:
        raise SystemExit("deployment lease owner mismatch")
    active_statuses = {
        "claimed", "running", "backing_up", "restarting", "starting",
        "stopping", "migrating", "rolling_back",
    }
    if payload.get("status") not in active_statuses:
        raise SystemExit("deployment lease is no longer active")
    if not payload.get("handoff"):
        raise SystemExit("deployment lease no longer permits worker handoff")
    current_operation = str(payload.get("operation") or "")
    if current_operation and operation and current_operation != operation:
        raise SystemExit("deployment lease operation mismatch")
PY
}

mirror_status_to_deployment_lease() {
    [ -n "$DEPLOYMENT_LEASE_FILE" ] || return 0
    "$PYTHON_BIN" - "$DEPLOYMENT_LEASE_FILE" \
        "$(deployment_lease_lock_file)" "$DEPLOYMENT_OWNER_TOKEN" \
        "$STATUS_FILE" "$$" "$MODE" "$DEPLOYMENT_OPERATION" \
        "$RESTART_FAILURE_POLICY" "$PORT" <<'PY'
import fcntl
import json
import os
import stat
import sys
from pathlib import Path

lease_path = Path(sys.argv[1])
lock_path = Path(sys.argv[2])
token = sys.argv[3]
status_path = Path(sys.argv[4])
pid = int(sys.argv[5])
mode, operation, policy, expected_port = sys.argv[6:10]
terminal_states = {"completed", "rolled_back", "failed", "rollback_failed"}

lock_path.parent.mkdir(parents=True, exist_ok=True)
if lease_path.parent.resolve() != lease_path.parent.absolute():
    raise SystemExit("deployment lease parent may not contain symlinks")
lease_metadata = lease_path.lstat()
if (
    not stat.S_ISREG(lease_metadata.st_mode)
    or lease_metadata.st_uid != os.getuid()
    or lease_metadata.st_nlink != 1
    or lease_metadata.st_mode & 0o022
):
    raise SystemExit("unsafe deployment lease")
flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
lock_fd = os.open(lock_path, flags, 0o600)
with os.fdopen(lock_fd, "a+b") as lock:
    lock_metadata = os.fstat(lock.fileno())
    if (
        not stat.S_ISREG(lock_metadata.st_mode)
        or lock_metadata.st_uid != os.getuid()
        or lock_metadata.st_nlink != 1
        or lock_metadata.st_mode & 0o022
    ):
        raise SystemExit("unsafe deployment lease lock")
    os.fchmod(lock.fileno(), 0o600)
    fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
    read_flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    lease_fd = os.open(lease_path, read_flags)
    with os.fdopen(lease_fd) as stream:
        opened_metadata = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(opened_metadata.st_mode)
            or opened_metadata.st_uid != os.getuid()
            or opened_metadata.st_nlink != 1
            or opened_metadata.st_mode & 0o022
        ):
            raise SystemExit("unsafe opened deployment lease")
        current = json.load(stream)
    status_flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    status_fd = os.open(status_path, status_flags)
    with os.fdopen(status_fd) as stream:
        status_metadata = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(status_metadata.st_mode)
            or status_metadata.st_uid != os.getuid()
            or status_metadata.st_nlink != 1
            or status_metadata.st_mode & 0o022
        ):
            raise SystemExit("unsafe deployment status")
        status = json.load(stream)
    if not isinstance(current, dict) or current.get("owner_token") != token:
        raise SystemExit("deployment lease owner mismatch")
    active_statuses = {
        "claimed", "running", "backing_up", "restarting", "starting",
        "stopping", "migrating", "rolling_back",
    }
    if current.get("status") not in active_statuses:
        raise SystemExit("deployment lease is no longer active")
    if not current.get("handoff"):
        raise SystemExit("deployment lease no longer permits worker handoff")
    current_operation = str(current.get("operation") or "")
    if current_operation and operation and current_operation != operation:
        raise SystemExit("deployment lease operation mismatch")
    if not isinstance(status, dict):
        raise SystemExit("deployment status is invalid")
    if status.get("deployment_owner_token") != token:
        raise SystemExit("deployment status owner token mismatch")
    if str(status.get("port") or "") != expected_port:
        raise SystemExit("deployment status port mismatch")
    if str(status.get("operation") or "") != operation:
        raise SystemExit("deployment status operation mismatch")
    terminal = status.get("status") in terminal_states
    current.update(status)
    current.update({
        "owner_token": token,
        "deployment_owner_token": token,
        "owner_pid": pid,
        "handoff_pid": 0 if terminal else pid,
        "handoff": not terminal,
        "handoff_provisional": False,
        "handoff_ack_deadline": None,
        "deployment_incomplete": (
            bool(status.get("deployment_incomplete"))
            or status.get("status") == "rollback_failed"
            or (
                status.get("status") == "failed"
                and not (
                    mode == "restart"
                    and operation == "restart"
                    and policy == "retry"
                )
            )
        ),
        "updated_at": status.get("timestamp"),
    })
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        current["owner_pid_start"] = fields[19]
        current["handoff_pid_start"] = "" if terminal else fields[19]
    except (OSError, IndexError):
        current["owner_pid_start"] = ""
        current["handoff_pid_start"] = ""
    temporary = lease_path.with_name(f".{lease_path.name}.worker-{pid}.tmp")
    try:
        fd = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        with os.fdopen(fd, "w") as stream:
            json.dump(current, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, lease_path)
        directory = os.open(lease_path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)
PY
}

write_status() {
    local status="$1" message="$2" step="${3:-}"
    # Never publish a status for an operation that no longer owns the repo.
    assert_deployment_lease_owner || return 1
    LAST_STATUS="$status"
    if ! "$PYTHON_BIN" - "$STATUS_FILE" "$status" "$message" "$step" \
        "$OLD_COMMIT" "$TARGET_COMMIT" "$EXPECTED_COMMIT" "$MODE" \
        "$DEPLOYMENT_OPERATION" "$HANDOFF_MODE" "$TERMINAL_INTENT" \
        "$ROLLBACK_INCOMPLETE" "$DEPLOYMENT_INCOMPLETE" \
        "$BACKUP_FILE" "$FRONTEND_BACKUP_DIR" \
        "$DATABASE_MIGRATION_REQUIRED" "$DATABASE_MIGRATION_APPLIED" \
        "$DEPLOYMENT_OWNER_TOKEN" "$LOG_FILE" "$PORT" <<'PY'
import json
import os
import stat
import sys
from datetime import datetime, timezone
from pathlib import Path

(
    path, status, message, step, old_commit, target_commit,
    expected_commit, mode, operation, handoff_mode, terminal_intent,
    rollback_incomplete, deployment_incomplete, backup_file, frontend_backup,
    migration_required, migration_applied, owner_token, log_file, port,
) = sys.argv[1:]
payload = {
    "status": status,
    "message": message,
    "step": step,
    "old_commit": old_commit,
    "target_commit": target_commit,
    "new_commit": target_commit,
    "expected_commit": expected_commit,
    "mode": mode,
    "operation": operation,
    "handoff_mode": handoff_mode,
    "terminal_intent": terminal_intent,
    "rollback_incomplete": rollback_incomplete == "true",
    "deployment_incomplete": deployment_incomplete == "true",
    "backup_file": backup_file,
    "frontend_dist_backup": frontend_backup,
    "database_migration_required": migration_required == "true",
    "database_migration_applied": (
        None if migration_applied == "null" else migration_applied == "true"
    ),
    "database_migrated": (
        None if migration_applied == "null" else migration_applied == "true"
    ),
    "deployment_owner_token": owner_token,
    "log_file": log_file,
    "port": int(port),
    "timestamp": datetime.now(timezone.utc).astimezone().isoformat(),
}
destination = Path(path)
temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
try:
    try:
        metadata = destination.lstat()
    except FileNotFoundError:
        metadata = None
    if metadata is not None and (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or metadata.st_nlink != 1
    ):
        raise RuntimeError(f"unsafe deployment status path: {destination}")
    fd = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL
        | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    with os.fdopen(fd, "w") as stream:
        json.dump(payload, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, destination)
    directory = os.open(destination.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
finally:
    temporary.unlink(missing_ok=True)
PY
    then
        return 1
    fi
    mirror_status_to_deployment_lease
}

cleanup_run_copy() {
    [ -n "$RUN_COPY_DIR" ] || return 0
    [ ! -L "$RUN_COPY_DIR" ] || return 1
    local canonical_dir canonical_self
    canonical_dir="$(readlink -f "$RUN_COPY_DIR" 2>/dev/null)" || return 1
    canonical_self="$(readlink -f "$SELF" 2>/dev/null)" || return 1
    [ "$(dirname "$canonical_dir")" = "/tmp" ] || return 1
    case "$(basename "$canonical_dir")" in
        ccm-update-runtime-*|ccm-update-run-*) ;;
        *) return 1 ;;
    esac
    [ "$(dirname "$canonical_self")" = "$canonical_dir" ] || return 1
    [ -f "$canonical_self" ] && [ ! -L "$canonical_self" ] || return 1
    rm -f -- "$canonical_self" && rmdir -- "$canonical_dir"
}

publish_terminal() {
    local status="$1" message="$2" step="${3:-}"
    trap - EXIT HUP INT TERM
    if ! cleanup_run_copy; then
        echo "WARNING: transient script copy cleanup failed" >&9
    fi
    RUN_COPY_DIR=""
    TERMINAL_WRITTEN=1
    # No filesystem, code, DB, dependency, or service mutation is permitted
    # after this durable terminal status/lease publication.
    if ! write_status "$status" "$message" "$step"; then
        TERMINAL_WRITTEN=0
        return 1
    fi
}

svc_is_active() {
    if [ "$SERVICE_NAME" != "-" ]; then
        systemctl_cmd is-active --quiet "$SERVICE_NAME"
    else
        local pid="${NEW_SERVER_PID:-$SERVER_PID}"
        [ -n "$pid" ] && pid_is_live "$pid"
    fi
}

pid_is_live() {
    local pid="$1" state
    kill -0 "$pid" 2>/dev/null || return 1
    state="$(
        sed -E 's/^.*\) ([A-Z]).*$/\1/' "/proc/${pid}/stat" 2>/dev/null \
            || true
    )"
    [ "$state" != "Z" ] && [ -n "$state" ]
}

svc_stop() {
    if [ "$SERVICE_NAME" != "-" ]; then
        systemctl_cmd stop "$SERVICE_NAME"
        return
    fi
    local pid="${NEW_SERVER_PID:-$SERVER_PID}"
    [ -n "$pid" ] || return 1
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 20); do
        pid_is_live "$pid" || return 0
        sleep 0.5
    done
    kill -9 "$pid" 2>/dev/null || true
    for _ in $(seq 1 20); do
        pid_is_live "$pid" || return 0
        sleep 0.1
    done
    return 1
}

stop_for_recovery() {
    if ! svc_stop && svc_is_active; then
        return 1
    fi
    for _ in $(seq 1 20); do
        if ! svc_is_active; then
            STARTED=0
            NEW_SERVER_PID=""
            return 0
        fi
        sleep 0.1
    done
    return 1
}

svc_start_once() {
    if [ "$SERVICE_NAME" != "-" ]; then
        systemctl_cmd start "$SERVICE_NAME"
    else
        (
            cd "$PROJECT_DIR" || exit 1
            exec "$PYTHON_BIN" -m uvicorn backend.main:app \
                --host 0.0.0.0 --port "$PORT"
        ) >&9 2>&1 &
        NEW_SERVER_PID=$!
        kill -0 "$NEW_SERVER_PID" 2>/dev/null
    fi
}

managed_main_pid() {
    systemctl_cmd show --property=MainPID --value "$SERVICE_NAME" 2>/dev/null
}

original_service_identity_is_gone() {
    [ "$PIDCHECK_MODE" = "skip" ] && return 0
    [ -n "$ORIGINAL_SERVER_PID_START" ] || return 0
    local current_start
    current_start="$(process_start_identity "$SERVER_PID" || true)"
    [ "$current_start" != "$ORIGINAL_SERVER_PID_START" ]
}

new_service_identity_is_valid() {
    [ "$PIDCHECK_MODE" = "skip" ] && return 0
    local pid start
    if [ "$SERVICE_NAME" != "-" ]; then
        pid="$(managed_main_pid || true)"
    else
        pid="$NEW_SERVER_PID"
    fi
    case "$pid" in
        ''|0|*[!0-9]*) return 1 ;;
    esac
    [ "$pid" != "$SERVER_PID" ] || return 1
    kill -0 "$pid" 2>/dev/null || return 1
    start="$(process_start_identity "$pid" || true)"
    [ -n "$start" ] || return 1
    if [ -n "$VERIFIED_SERVER_PID" ] && [ "$pid" != "$VERIFIED_SERVER_PID" ]; then
        return 1
    fi
    VERIFIED_SERVER_PID="$pid"
    return 0
}

svc_health_matches_commit() {
    local expected="$1"
    if [ "$HEALTHCHECK_MODE" = "skip" ]; then
        return 0
    fi
    [ -n "$expected" ] || return 1
    "$PYTHON_BIN" - "$PORT" "$expected" <<'PY'
import json
import sys
import urllib.request

port, expected = sys.argv[1:]
with urllib.request.urlopen(
    f"http://127.0.0.1:{port}/api/system/health", timeout=1.5
) as response:
    payload = json.load(response)
if payload.get("status") != "ok":
    raise SystemExit("health status is not ok")
actual = str(payload.get("commit") or "")
if actual != expected:
    raise SystemExit(f"commit mismatch: expected={expected} actual={actual}")
PY
}

svc_start() {
    [ "$STARTED" = "1" ] && return 0
    local attempt check stable
    for attempt in $(seq 1 "$START_ATTEMPTS"); do
        VERIFIED_SERVER_PID=""
        if svc_start_once; then
            stable=0
            for check in $(seq 1 "$HEALTH_ATTEMPTS"); do
                if original_service_identity_is_gone && \
                   svc_is_active && new_service_identity_is_valid && \
                   svc_health_matches_commit "$EXPECTED_COMMIT" \
                    >&9 2>&1; then
                    stable=$((stable + 1))
                    if [ "$stable" -ge "$STABILITY_CHECKS" ]; then
                        STARTED=1
                        return 0
                    fi
                else
                    stable=0
                fi
                sleep 0.5
            done
        fi
        stop_for_recovery >&9 2>&1 || true
        NEW_SERVER_PID=""
        sleep 0.5
    done
    return 1
}

assert_database_unheld() {
    "$PYTHON_BIN" - "$DB_FILE" "$$" "$SERVER_PID" <<'PY'
import os
import pwd
import re
import shutil
import stat
import subprocess
import sys
from pathlib import Path

database = Path(sys.argv[1]).resolve()
worker_pid = int(sys.argv[2])
try:
    stopped_service_pid = int(sys.argv[3])
except (TypeError, ValueError):
    stopped_service_pid = 0
if not database.exists():
    raise SystemExit(0)
if not database.is_file():
    raise SystemExit(f"database is not a regular file: {database}")
identities = {}
for candidate in (database, Path(str(database) + "-wal"), Path(str(database) + "-shm")):
    try:
        metadata = candidate.stat()
    except FileNotFoundError:
        continue
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or metadata.st_mode & 0o022
    ):
        raise SystemExit(
            f"unsafe SQLite owner/type/write permissions: {candidate}"
        )
    identities[(metadata.st_dev, metadata.st_ino)] = str(candidate)
holders = []
uninspectable = []
own_uid = os.getuid()

def inaccessible_process_detail(process, error, status=""):
    try:
        command = (process / "cmdline").read_bytes().replace(
            b"\0", b" "
        ).decode(errors="replace").strip()
    except OSError:
        command = ""
    try:
        cgroup = (process / "cgroup").read_text(errors="replace").strip()
    except OSError:
        cgroup = ""
    process_name = ""
    parent_pid = 0
    for line in status.splitlines():
        if line.startswith("Name:"):
            process_name = line.split(":", 1)[1].strip()
        elif line.startswith("PPid:"):
            try:
                parent_pid = int(line.split()[1])
            except (IndexError, ValueError):
                parent_pid = 0
    return (
        process.name, command, cgroup, str(error), process_name, parent_pid
    )

for process in Path("/proc").iterdir():
    if not process.name.isdigit() or int(process.name) in {
        os.getpid(), os.getppid(), worker_pid, stopped_service_pid
    }:
        continue
    try:
        status = (process / "status").read_text(errors="replace")
        uid_line = next(line for line in status.splitlines() if line.startswith("Uid:"))
        process_uid = int(uid_line.split()[1])
    except (FileNotFoundError, StopIteration, ValueError):
        continue
    except PermissionError as exc:
        # With safe DB permissions another uid cannot acquire a new writable
        # handle.  An unreadable process whose uid cannot be established is
        # still checked by fuser below.
        uninspectable.append(inaccessible_process_detail(process, exc))
        continue
    if process_uid != own_uid:
        continue
    try:
        descriptors = list((process / "fd").iterdir())
    except FileNotFoundError:
        continue
    except PermissionError as exc:
        uninspectable.append(
            inaccessible_process_detail(process, exc, status)
        )
        continue
    for descriptor in descriptors:
        try:
            metadata = descriptor.stat()
        except FileNotFoundError:
            continue
        except PermissionError as exc:
            uninspectable.append(
                inaccessible_process_detail(process, exc, status)
            )
            break
        matched = identities.get((metadata.st_dev, metadata.st_ino))
        if matched:
            holders.append((process.name, matched))
            break
if holders:
    raise SystemExit(
        "database still held after service stop: "
        + "; ".join(f"pid={pid} file={path}" for pid, path in holders)
    )
fuser = shutil.which("fuser")
if not fuser:
    raise SystemExit("cannot prove SQLite exclusivity: fuser is unavailable")
paths = sorted(set(identities.values()))
result = subprocess.run(
    [fuser, *paths],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
    timeout=10,
)
if result.returncode == 0:
    raise SystemExit(
        "fuser reports database holders: "
        + (result.stdout + " " + result.stderr).strip()
    )
if result.returncode != 1:
    raise SystemExit(
        f"cannot prove SQLite exclusivity: fuser exit={result.returncode}: "
        + result.stderr.strip()
    )
# fuser cannot see a process that has disabled dumpability on some kernels.
# The only inaccessible same-user processes ignored here are fixed systemd,
# ssh-agent, and verified server-side OpenSSH helpers that cannot load CCM or
# its database. These deliberately disable dumpability on some hosts, so even
# their same-UID /proc/fd is unreadable.
def fixed_systemd_user_manager(command):
    argv = command.split()
    return (
        len(argv) in {2, 3}
        and Path(argv[0]).name == "systemd"
        and argv[1] == "--user"
        and (
            len(argv) == 2
            or re.fullmatch(r"--deserialize=[0-9]+", argv[2]) is not None
        )
    )


def fixed_server_side_sshd_session(
    process_name,
    command,
    cgroup,
    parent_name,
    parent_uids,
    parent_command,
    parent_cgroup,
    own_uid,
    account_name,
):
    session_scope = re.compile(
        rf"[0-9]+:[^:]*:/user\.slice/user-{own_uid}\.slice/"
        r"session-[0-9]+\.scope"
    )
    child_command = re.fullmatch(
        rf"sshd-session: {re.escape(account_name)}@(notty|pts/[0-9]+)",
        command,
    )
    parent_command_matches = (
        parent_command == f"sshd-session: {account_name} [priv]"
    )
    return (
        bool(account_name)
        and process_name == "sshd-session"
        and child_command is not None
        and any(session_scope.fullmatch(line) for line in cgroup.splitlines())
        and parent_name == "sshd-session"
        and parent_uids == (0, 0, 0, 0)
        and parent_command_matches
        and parent_cgroup == cgroup
    )


unsafe_uninspectable = []
try:
    account_name = pwd.getpwuid(own_uid).pw_name
except KeyError:
    account_name = ""
for pid, command, cgroup, error, process_name, parent_pid in uninspectable:
    in_user_manager_init = (
        f"/user-{own_uid}.slice/user@{own_uid}.service/init.scope" in cgroup
    )
    # After a daemon-reexec the user manager's cmdline gains extra arguments
    # (e.g. "systemd --user --deserialize=19"), so match tokens, not a suffix.
    command_tokens = command.split()
    fixed_system_helper = (
        command == "(sd-pam)"
        or fixed_systemd_user_manager(command)
    )
    fixed_ssh_agent = (
        f"/user-{own_uid}.slice/user@{own_uid}.service/"
        "app.slice/ssh-agent.service" in cgroup
        and command == "/usr/bin/ssh-agent -D"
    )
    parent_name = ""
    parent_uids = ()
    parent_command = ""
    parent_cgroup = ""
    if parent_pid > 0:
        parent = Path("/proc") / str(parent_pid)
        try:
            parent_status = parent.joinpath("status").read_text(
                errors="replace"
            )
            for line in parent_status.splitlines():
                if line.startswith("Name:"):
                    parent_name = line.split(":", 1)[1].strip()
                elif line.startswith("Uid:"):
                    parent_uids = tuple(int(value) for value in line.split()[1:])
            parent_command = parent.joinpath("cmdline").read_bytes().replace(
                b"\0", b" "
            ).decode(errors="replace").strip()
            parent_cgroup = parent.joinpath("cgroup").read_text(
                errors="replace"
            ).strip()
        except (OSError, ValueError):
            pass
    fixed_sshd_session = fixed_server_side_sshd_session(
        process_name,
        command,
        cgroup,
        parent_name,
        parent_uids,
        parent_command,
        parent_cgroup,
        own_uid,
        account_name,
    )
    if not (
        (in_user_manager_init and fixed_system_helper)
        or fixed_ssh_agent
        or fixed_sshd_session
    ):
        unsafe_uninspectable.append((pid, command, error))
if unsafe_uninspectable:
    raise SystemExit(
        "cannot prove SQLite exclusivity; inaccessible same-user processes: "
        + "; ".join(
            f"pid={pid} cmd={command or '?'} error={error}"
            for pid, command, error in unsafe_uninspectable
        )
    )
PY
}

refresh_stopped_sqlite_backup() {
    [ -n "$BACKUP_FILE" ] || return 1
    assert_deployment_lease_owner || return 1
    assert_database_unheld || return 1
    "$PYTHON_BIN" - "$DB_FILE" "$BACKUP_FILE" <<'PY'
import os
import sqlite3
import sys
from pathlib import Path

source_path = Path(sys.argv[1]).resolve()
backup_path = Path(sys.argv[2]).resolve()
if not source_path.is_file() or source_path == backup_path:
    raise SystemExit("invalid SQLite source/backup path")
backup_path.parent.mkdir(parents=True, exist_ok=True)
temporary = backup_path.with_name(f".{backup_path.name}.stopped-{os.getpid()}.tmp")
try:
    temporary.unlink(missing_ok=True)
    source = sqlite3.connect(str(source_path))
    destination = sqlite3.connect(str(temporary))
    try:
        # sqlite3_backup reads a transactionally consistent source snapshot and
        # the destination integrity_check below validates every copied page.
        # Scanning the multi-GiB source first duplicates that full-table work
        # without improving the rollback artifact's guarantee.
        source.backup(destination)
        destination.commit()
        backup_check = [row[0] for row in destination.execute("PRAGMA integrity_check")]
        if backup_check != ["ok"]:
            raise RuntimeError("backup SQLite integrity check failed")
    finally:
        destination.close()
        source.close()
    os.chmod(temporary, 0o600)
    with temporary.open("rb") as stream:
        os.fsync(stream.fileno())
    os.replace(temporary, backup_path)
    directory = os.open(backup_path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
finally:
    temporary.unlink(missing_ok=True)
PY
}

restore_database() {
    [ -n "$BACKUP_FILE" ] && [ -f "$BACKUP_FILE" ] || return 1
    assert_deployment_lease_owner || return 1
    assert_database_unheld || return 1
    if ! "$PYTHON_BIN" - "$DB_FILE" "$BACKUP_FILE" <<'PY'
import os
import sqlite3
import sys
from pathlib import Path

database = Path(sys.argv[1]).resolve()
backup = Path(sys.argv[2]).resolve()
if not backup.is_file() or database == backup:
    raise SystemExit("invalid SQLite restore source")
temporary = database.with_name(f".{database.name}.restore-{os.getpid()}.tmp")
try:
    temporary.unlink(missing_ok=True)
    with sqlite3.connect(str(backup)) as connection:
        if [row[0] for row in connection.execute("PRAGMA integrity_check")] != ["ok"]:
            raise RuntimeError("backup SQLite integrity check failed")
    with backup.open("rb") as source, temporary.open("xb") as target:
        while chunk := source.read(1024 * 1024):
            target.write(chunk)
        target.flush()
        os.fsync(target.fileno())
    os.chmod(temporary, 0o600)
    with sqlite3.connect(str(temporary)) as connection:
        if [row[0] for row in connection.execute("PRAGMA integrity_check")] != ["ok"]:
            raise RuntimeError("staged SQLite integrity check failed")
    Path(str(database) + "-wal").unlink(missing_ok=True)
    Path(str(database) + "-shm").unlink(missing_ok=True)
    os.replace(temporary, database)
    directory = os.open(database.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    with sqlite3.connect(str(database)) as connection:
        if [row[0] for row in connection.execute("PRAGMA integrity_check")] != ["ok"]:
            raise RuntimeError("restored SQLite integrity check failed")
finally:
    temporary.unlink(missing_ok=True)
PY
    then
        return 1
    fi
    DATABASE_MIGRATION_APPLIED="false"
}

reset_code() {
    assert_deployment_lease_owner || return 1
    git cat-file -e "${OLD_COMMIT}^{commit}" >&9 2>&1 || return 1
    git reset --hard "$OLD_COMMIT" >&9 2>&1 || return 1
    local actual
    actual="$(git rev-parse HEAD 2>&9)" || return 1
    [ "$actual" = "$OLD_COMMIT" ]
}

restore_dependencies() {
    assert_deployment_lease_owner || return 1
    uv sync >&9 2>&1
}

restore_frontend_dist() {
    assert_deployment_lease_owner || return 1
    [ -n "$FRONTEND_BACKUP_DIR" ] || return 0
    [ -d "$FRONTEND_BACKUP_DIR" ] || return 1
    local frontend="$PROJECT_DIR/frontend"
    local dist="$frontend/dist"
    [ -d "$frontend" ] || return 1
    if [ -e "$FRONTEND_BACKUP_DIR/.ccm-dist-absent" ]; then
        [ -f "$FRONTEND_BACKUP_DIR/.ccm-dist-absent" ] && \
            [ ! -L "$FRONTEND_BACKUP_DIR/.ccm-dist-absent" ] || return 1
        # The pre-update tree had no dist at all.  Exact rollback therefore
        # removes a newly built dist instead of leaving a new UI over old code.
        rm -rf -- "$dist"
        return $?
    fi
    local staged old=""
    staged="$(mktemp -d "$frontend/.dist-restore.XXXXXX")" || return 1
    if ! cp -a -- "$FRONTEND_BACKUP_DIR/." "$staged/"; then
        rm -rf -- "$staged"
        return 1
    fi
    if [ -e "$dist" ] || [ -L "$dist" ]; then
        old="$(mktemp -d "$frontend/.dist-old.XXXXXX")" || {
            rm -rf -- "$staged"
            return 1
        }
        rmdir -- "$old"
        mv -- "$dist" "$old" || {
            rm -rf -- "$staged"
            return 1
        }
    fi
    if ! mv -- "$staged" "$dist"; then
        [ -n "$old" ] && mv -- "$old" "$dist" || true
        rm -rf -- "$staged"
        return 1
    fi
    [ -z "$old" ] || rm -rf -- "$old"
}

rollback_to_old() {
    local restore_db="$1" origin="$2"
    local failed=0 first_failure=""
    HANDOFF_MODE="rollback"
    TERMINAL_INTENT="rolled_back"
    ROLLBACK_INCOMPLETE="false"
    if [ "$restore_db" = "1" ] && \
       { [ "$DEPLOYMENT_OPERATION" = "repair" ] || \
         [ "$OLD_COMMIT" = "$TARGET_COMMIT" ]; }; then
        # A same-version migration rollback is not a clean deploy regardless
        # of its caller-supplied operation label. Retain maintenance-only
        # startup so pre-start/init_db cannot rerun the unproven migration
        # without a backup before the administrator retries.
        DEPLOYMENT_INCOMPLETE="true"
    fi
    EXPECTED_COMMIT="$OLD_COMMIT"
    if [ "$restore_db" = "1" ]; then
        DATABASE_MIGRATION_APPLIED="null"
        if ! restore_database >&9 2>&1; then
            failed=1
            first_failure="restore_database"
        fi
    fi
    if ! reset_code; then
        failed=1
        [ -n "$first_failure" ] || first_failure="git_reset"
    fi
    if ! restore_dependencies; then
        failed=1
        [ -n "$first_failure" ] || first_failure="uv_sync"
    fi
    if ! restore_frontend_dist; then
        failed=1
        [ -n "$first_failure" ] || first_failure="frontend_restore"
    fi
    if [ "$failed" != "0" ]; then
        ROLLBACK_INCOMPLETE="true"
        TERMINAL_INTENT="rollback_failed"
        publish_terminal "rollback_failed" \
            "回滚未完整完成（失败步骤：$first_failure），服务保持停止，请人工处理" \
            "$first_failure"
        return 1
    fi
    if ! write_status "starting" \
        "回退步骤已执行，正在启动并验证旧版本服务..." \
        "start_service"; then
        failed=1
        [ -n "$first_failure" ] || first_failure="deployment_lease"
    elif ! assert_deployment_lease_owner || ! svc_start; then
        failed=1
        [ -n "$first_failure" ] || first_failure="start_service"
    fi
    if [ "$failed" = "0" ]; then
        publish_terminal "rolled_back" "已验证回滚到 $OLD_COMMIT" "$origin" \
            || return 1
        return 0
    fi
    ROLLBACK_INCOMPLETE="true"
    TERMINAL_INTENT="rollback_failed"
    publish_terminal "rollback_failed" \
        "回滚未完整完成（失败步骤：$first_failure），请人工处理" \
        "$first_failure"
    return 1
}

on_exit() {
    local rc=$?
    trap - EXIT HUP INT TERM
    if [ -n "$MIGRATION_PROCESS_PID" ]; then
        kill -TERM -- "-$MIGRATION_PROCESS_PID" 2>/dev/null || true
        sleep 0.1
        kill -KILL -- "-$MIGRATION_PROCESS_PID" 2>/dev/null || true
        wait "$MIGRATION_PROCESS_PID" 2>/dev/null || true
        MIGRATION_PROCESS_PID=""
    fi
    [ "$TERMINAL_WRITTEN" = "0" ] || exit "$rc"
    if [ "$SERVICE_STOPPED" != "1" ]; then
        cleanup_run_copy || true
        exit "$rc"
    fi
    if ! stop_for_recovery >&9 2>&1; then
        HANDOFF_MODE="rollback"
        TERMINAL_INTENT="rollback_failed"
        ROLLBACK_INCOMPLETE="true"
        publish_terminal "rollback_failed" \
            "更新流程中断且无法确认新服务已停止，未恢复数据库或代码" \
            "stop_service"
        exit 1
    fi
    if [ "$MODE" = "restart" ]; then
        if [ "$RESTART_FAILURE_POLICY" = "rollback" ]; then
            rollback_to_old "0" "interrupted" || true
        elif svc_start; then
            publish_terminal "completed" "服务已重启并验证运行版本" "start_service"
        else
            publish_terminal "failed" "重启中断且健康验证失败，请重试" "start_service"
        fi
        exit "$rc"
    fi
    local restore_db=0
    if [ "$MODE" = "rollback" ]; then
        restore_db=1
    elif [ "$MODE" = "migrate" ] && [ "$MIGRATION_STARTED" = "1" ] && \
         [ "$FINAL_BACKUP_READY" = "1" ]; then
        restore_db=1
    fi
    rollback_to_old "$restore_db" "interrupted" || true
    exit "$rc"
}

trap on_exit EXIT
handle_signal() {
    local code="$1"
    trap - HUP INT TERM
    if [ -n "$MIGRATION_PROCESS_PID" ]; then
        kill -TERM -- "-$MIGRATION_PROCESS_PID" 2>/dev/null || true
        sleep 0.2
        kill -KILL -- "-$MIGRATION_PROCESS_PID" 2>/dev/null || true
        wait "$MIGRATION_PROCESS_PID" 2>/dev/null || true
        MIGRATION_PROCESS_PID=""
    fi
    exit "$code"
}
trap 'handle_signal 129' HUP
trap 'handle_signal 130' INT
trap 'handle_signal 143' TERM

echo "=== update worker started $(date -Iseconds) mode=$MODE protocol=$CCM_UPDATE_PROTOCOL_VERSION ===" >&9
TARGET_COMMIT="$(git rev-parse HEAD 2>&9 || true)"
EXPECTED_COMMIT="$TARGET_COMMIT"
if [ "$MODE" = "restart" ] && [ -z "$RESTART_FAILURE_POLICY" ]; then
    if [ "$OLD_COMMIT" = "$TARGET_COMMIT" ]; then
        RESTART_FAILURE_POLICY="retry"
    else
        RESTART_FAILURE_POLICY="rollback"
    fi
fi
if [ -z "$TARGET_COMMIT" ]; then
    trap - EXIT HUP INT TERM
    publish_terminal "failed" "无法确定磁盘代码版本，中止更新" "git_pull"
    exit 1
fi
if [ "$LEGACY_LEASE_BOOTSTRAP" = "1" ]; then
    if ! claim_legacy_deployment_lease >&9 2>&1; then
        echo "legacy backend could not safely claim the repository lease" >&9
        exit 1
    fi
fi
if ! assert_deployment_lease_owner >&9 2>&1; then
    trap - EXIT HUP INT TERM
    publish_terminal "failed" "部署租约所有权校验失败，拒绝停服" "deployment_lease" || true
    exit 1
fi

require_local_claude_isolation_prerequisites() {
    [ "$(uname -s 2>/dev/null)" = "Linux" ] || return 0
    local missing=()
    command -v bwrap >/dev/null 2>&1 || missing+=("bubblewrap (bwrap)")
    command -v socat >/dev/null 2>&1 || missing+=("socat")
    local seccomp_arch=""
    case "$(uname -m 2>/dev/null)" in
        x86_64|amd64) seccomp_arch="x64" ;;
        aarch64|arm64) seccomp_arch="arm64" ;;
        *) missing+=("apply-seccomp (unsupported architecture)") ;;
    esac
    local npm_root=""
    local apply_seccomp=""
    if [ -n "$seccomp_arch" ]; then
        if command -v npm >/dev/null 2>&1; then
            npm_root="$(npm root -g 2>/dev/null || true)"
        fi
        if [ -n "$npm_root" ] && [ "${npm_root#/}" != "$npm_root" ]; then
            apply_seccomp="${npm_root}/@anthropic-ai/sandbox-runtime/vendor/seccomp/${seccomp_arch}/apply-seccomp"
        fi
        if [ -z "$apply_seccomp" ] || [ ! -f "$apply_seccomp" ] || \
           [ -L "$apply_seccomp" ] || [ ! -x "$apply_seccomp" ]; then
            missing+=("matching apply-seccomp")
        fi
    fi
    [ "${#missing[@]}" -eq 0 ] && return 0
    echo "CCM deployment prerequisite check failed: missing ${missing[*]}." >&9
    echo "Install them before retrying; for Ubuntu/Debian:" >&9
    echo "  sudo apt-get install -y bubblewrap socat" >&9
    echo "  sudo npm install -g @anthropic-ai/sandbox-runtime@0.0.71" >&9
    echo "The updater will not run sudo or install host packages automatically." >&9
    return 1
}

# Rollback paths must remain usable to recover an older release. New-code
# migration/restart, however, may not stop or swap the service unless Linux can
# satisfy the local Claude Task isolation contract.
if { [ "$MODE" = "migrate" ] || [ "$MODE" = "restart" ]; } && \
   ! require_local_claude_isolation_prerequisites; then
    trap - EXIT HUP INT TERM
    publish_terminal "failed" \
        "缺少本地 Claude 隔离依赖（bubblewrap/socat/apply-seccomp）；服务未停止，请安装后重试" \
        "deployment_prerequisites" || true
    exit 1
fi

# Arm recovery before stop.  A failed systemctl call is not proof that the
# process survived the request.
write_status "stopping" "正在停止服务..." "stop_service" || exit 1
SERVICE_STOPPED=1
if ! stop_for_recovery >&9 2>&1; then
    if svc_is_active; then
        SERVICE_STOPPED=0
        publish_terminal "failed" "停止服务失败，原服务仍在运行" "stop_service"
        exit 1
    fi
    exit 1
fi
sleep 1

if [ "$MODE" = "restart" ]; then
    write_status "starting" "正在启动并验证服务..." "start_service" || exit 1
    if svc_start; then
        publish_terminal "completed" "服务已重启并验证运行版本" \
            "start_service" || exit 1
        exit 0
    fi
    if [ "$RESTART_FAILURE_POLICY" = "rollback" ]; then
        stop_for_recovery >&9 2>&1 || exit 1
        write_status "rolling_back" \
            "新版本健康验证失败，正在回退代码..." \
            "start_service" || exit 1
        rollback_to_old "0" "start_service"
        exit $?
    fi
    publish_terminal "failed" "服务重启后的健康或版本验证失败，请重试" "start_service"
    exit 1
fi

if [ "$MODE" = "rollback" ] || [ "$MODE" = "rollback_code" ]; then
    restore_db=1
    if [ "$MODE" = "rollback_code" ]; then
        restore_db=0
    else
        DATABASE_MIGRATION_APPLIED="null"
    fi
    write_status "rolling_back" "正在回滚..." "rollback" || exit 1
    rollback_to_old "$restore_db" "rollback"
    exit $?
fi

# The online backup can miss writes committed between backup and stop.  Replace
# it with a validated, fsynced SQLite snapshot only after proving all writers
# are gone.  A failure here is pre-migration, so preserve the untouched DB and
# roll back code only.
write_status "backing_up" \
    "服务已停止，正在生成最终数据库快照..." \
    "backup_database" || exit 1
if ! refresh_stopped_sqlite_backup >&9 2>&1; then
    write_status "rolling_back" \
        "最终数据库快照失败，正在回退代码..." \
        "backup_database" || exit 1
    rollback_to_old "0" "backup_database"
    exit $?
fi
FINAL_BACKUP_READY=1

MIGRATION_STARTED=1
DATABASE_MIGRATION_APPLIED="null"
write_status "migrating" \
    "正在执行数据库迁移..." \
    "alembic_upgrade" || exit 1
setsid timeout --kill-after=10s "$MIGRATION_TIMEOUT" \
    uv run alembic upgrade head >&9 2>&1 &
MIGRATION_PROCESS_PID=$!
if wait "$MIGRATION_PROCESS_PID"; then
    migration_exit=0
else
    migration_exit=$?
fi
MIGRATION_PROCESS_PID=""
if [ "$migration_exit" = "0" ]; then
    DATABASE_MIGRATION_APPLIED="true"
    write_status "starting" \
        "迁移成功，正在启动并验证服务..." \
        "start_service" || exit 1
    if svc_start; then
        publish_terminal "completed" "更新完成，服务启动已验证" \
            "start_service" || exit 1
        exit 0
    fi
    if ! stop_for_recovery >&9 2>&1; then
        ROLLBACK_INCOMPLETE="true"
        TERMINAL_INTENT="rollback_failed"
        publish_terminal "rollback_failed" \
            "新服务健康验证失败且无法确认已停止，未恢复数据库" \
            "stop_service"
        exit 1
    fi
    write_status "rolling_back" \
        "迁移成功但新服务健康验证失败，正在回退数据库和代码..." \
        "start_service" || exit 1
    rollback_to_old "1" "start_service"
    exit $?
else
    write_status "rolling_back" \
        "迁移失败(exit=$migration_exit)，正在回滚数据库和代码..." \
        "alembic_upgrade" || exit 1
    rollback_to_old "1" "alembic_upgrade"
    exit $?
fi
