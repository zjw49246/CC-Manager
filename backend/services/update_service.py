"""One-click update & restart pipeline for CCM."""

from __future__ import annotations

import asyncio
import fcntl
import json
import logging
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import or_, select

from backend.models.global_settings import GlobalSettings
from backend.models.instance import Instance
from backend.models.task import Task
from backend.services.cancellation import finish_awaitable, settle_awaitable
from backend.services.git_info import git_head_commit
from backend.services.update_runtime import (
    TrustedUpdateRuntime,
    UpdateRuntimeError,
)
from backend.services.ws_broadcaster import WebSocketBroadcaster

logger = logging.getLogger(__name__)

WS_CHANNEL = "system_update"
# A database snapshot is only useful for the deployment that produced it (plus
# one previous recovery point).  Large SQLite deployments can otherwise consume
# tens of GiB after only a handful of updates.
MAX_BACKUPS = 2
ACTIVE_TASK_STATUSES = ("in_progress", "executing")
DRY_RUN_CACHE_SECONDS = 30.0
DRY_RUN_ERROR_CACHE_SECONDS = 5.0
UPDATE_SCRIPT_PROTOCOL_VERSION = 2
ACTIVE_DEPLOYMENT_STATUSES = {
    "claimed",
    "running",
    "backing_up",
    "restarting",
    "starting",
    "stopping",
    "migrating",
    "rolling_back",
}


def _optional_bool(value: Any, default: bool | None = False) -> bool | None:
    """Decode the migration tri-state written by the external worker."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"none", "null", "unknown"}:
            return None
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    if value is ...:
        return default
    return bool(value)


@dataclass
class StepInfo:
    name: str
    status: str = "pending"  # pending | running | completed | failed | skipped
    duration_ms: int | None = None
    started_at: str | None = None
    result: dict[str, Any] | None = None
    message: str | None = None


@dataclass
class UpdateState:
    update_id: str = ""
    status: str = "idle"  # idle | running | completed | failed | rolled_back | restarting
    steps: list[StepInfo] = field(default_factory=list)
    old_commit: str = ""
    new_commit: str = ""
    backup_file: str = ""
    frontend_dist_backup: str = ""
    operation: str = "update"  # update | repair | restart | rollback
    deployment_incomplete: bool = False
    database_migration_required: bool = False
    database_migration_applied: bool | None = False
    started_at: str = ""
    completed_at: str = ""
    error: str = ""
    update_channel: str = "main"
    target_version: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "update_id": self.update_id,
            "status": self.status,
            "current_step": next(
                (i + 1 for i, s in enumerate(self.steps) if s.status == "running"),
                len([s for s in self.steps if s.status in ("completed", "skipped", "failed")]),
            ),
            "total_steps": len(self.steps),
            "steps": [
                {
                    "name": s.name,
                    "status": s.status,
                    "duration_ms": s.duration_ms,
                    "started_at": s.started_at,
                    "result": s.result,
                    "message": s.message,
                }
                for s in self.steps
            ],
            "old_commit": self.old_commit,
            "new_commit": self.new_commit,
            "backup_file": self.backup_file,
            "frontend_dist_backup": self.frontend_dist_backup,
            "operation": self.operation,
            "deployment_incomplete": self.deployment_incomplete,
            "database_migration_required": self.database_migration_required,
            "database_migration_applied": self.database_migration_applied,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "error": self.error,
            "update_channel": self.update_channel,
            "target_version": self.target_version,
        }


STEP_NAMES = [
    "git_pull",
    "detect_changes",
    "backup_database",
    "uv_sync",
    "refresh_pty",
    "npm_install",
    "frontend_build",
    "stop_service",
    "alembic_upgrade",
    "start_service",
]

STEP_LABELS = {
    "git_pull": "拉取最新代码",
    "detect_changes": "检测变更",
    "backup_database": "备份数据库",
    "uv_sync": "同步 Python 依赖",
    "refresh_pty": "更新 PTY 依赖",
    "npm_install": "安装前端依赖",
    "frontend_build": "构建前端",
    "stop_service": "停止服务",
    "alembic_upgrade": "数据库迁移",
    "start_service": "启动服务",
}

STEP_TIMEOUTS = {
    "git_pull": 60,
    "uv_sync": 300,
    "refresh_pty": 120,
    "npm_install": 120,
    "frontend_build": 300,
}


def _find_tool(name: str) -> str:
    """Find a CLI tool by searching PATH + common install locations."""
    import shutil
    found = shutil.which(name)
    if found:
        return found
    home = Path.home()
    extra_dirs = [
        home / ".local" / "bin",
        home / ".cargo" / "bin",
        Path("/usr/local/bin"),
        Path("/opt/homebrew/bin"),
    ]
    for d in extra_dirs:
        candidate = d / name
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    return name


def _resolve_db_path(project_dir: str) -> Path:
    """Resolve SQLite database path from settings."""
    from backend.config import settings
    url = settings.database_url
    if url.strip().lower().startswith("sqlite"):
        # "sqlite+aiosqlite:///./claude_manager.db" → "./claude_manager.db"
        raw = url.split("///", 1)[-1] if "///" in url else url
        p = Path(raw)
        if not p.is_absolute():
            p = Path(project_dir) / p
        return p.resolve()
    return Path(project_dir) / "claude_manager.db"


class UpdateService:
    def __init__(
        self,
        broadcaster: WebSocketBroadcaster,
        port: int,
        project_dir: str,
        db_factory: Any | None = None,
        dispatcher: Any | None = None,
        running_commit: str | None = None,
        update_runtime_root: str | os.PathLike[str] | None = None,
        legacy_update_runtime_root: str | os.PathLike[str] | None = "/tmp",
    ):
        self.broadcaster = broadcaster
        self.port = port
        self.project_dir = str(Path(project_dir).resolve())
        self.db_factory = db_factory
        self.dispatcher = dispatcher
        self.maintenance_only = False
        # Capture the version loaded by this process exactly once.  Reading
        # HEAD later only tells us what is on disk after a manual git pull.
        self._running_commit = (
            running_commit.strip()
            if running_commit is not None
            else git_head_commit(self.project_dir)
        )
        from backend.config import settings
        self._database_url = settings.database_url
        self._automatic_rollback_supported = (
            self._database_url.strip().lower().startswith("sqlite")
            and ":memory:" not in self._database_url.lower()
        )
        self.db_path = _resolve_db_path(self.project_dir)
        self._lock = asyncio.Lock()
        # Serialize admission of update and rollback operations. ``_lock``
        # protects the long-running pipeline itself, but a newly scheduled
        # pipeline has not necessarily acquired it when start_update returns.
        self._operation_lock = asyncio.Lock()
        self._dry_run_lock = asyncio.Lock()
        self._dry_run_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self._inspection_lock = asyncio.Lock()
        self._inspection_cache: tuple[float, dict[str, Any]] | None = None
        self._current: UpdateState | None = None
        self._status_file = Path(f"/tmp/ccm-update-status-{port}.json")
        self._journal_file = (
            Path(self.project_dir)
            / "backups"
            / f"deployment-status-{port}.json"
        )
        self._lease_file = (
            Path(self.project_dir) / "backups" / "deployment-lease.json"
        )
        self._lease_lock_file = (
            Path(self.project_dir) / "backups" / "deployment-lease.lock"
        )
        self._lease_token: str | None = None
        self._legacy_handoff = False
        self._trusted_update_script_lock = threading.RLock()
        self._trusted_update_script: Path | None = None
        self._trusted_update_script_error = ""
        self._trusted_update_runtime: TrustedUpdateRuntime | None = None
        try:
            self._trusted_update_runtime = TrustedUpdateRuntime(
                port=self.port,
                running_commit=self._running_commit,
                root=update_runtime_root,
                legacy_root=legacy_update_runtime_root,
            )
            self._trusted_update_script = self._snapshot_running_update_script()
        except Exception as exc:
            self._trusted_update_script_error = str(exc)
            logger.exception("Unable to snapshot the running update helper")
        self._service_name = settings.service_name
        self._service_scope = settings.service_scope
        self._tools = {
            "git": _find_tool("git"),
            "uv": _find_tool("uv"),
            "npm": _find_tool("npm"),
            "bash": _find_tool("bash"),
            "systemctl": _find_tool("systemctl"),
            "systemd-run": _find_tool("systemd-run"),
            "sudo": _find_tool("sudo"),
        }
        logger.info("Resolved tool paths: %s", self._tools)

    @property
    def running_commit(self) -> str:
        """Exact commit whose Python modules are loaded by this process."""
        return self._running_commit

    def _snapshot_running_update_script(self) -> Path:
        """Capture and materialize the immutable matching update worker.

        The checkout changes before the hand-off.  Executing the helper from
        that mutable checkout can pair this Python protocol with an older shell
        protocol (notably when updating to another branch or rolling back).
        """
        runtime = self._trusted_update_runtime
        if runtime is None:
            raise UpdateRuntimeError("更新脚本专用运行目录尚未初始化")
        source = Path(self.project_dir) / "scripts" / "update_migrate.sh"
        return runtime.capture(source)

    def ensure_runtime_snapshot(self) -> Path | None:
        """Retry or rematerialize the process-bound trusted helper."""

        with self._trusted_update_script_lock:
            runtime = self._trusted_update_runtime
            try:
                if runtime is None:
                    raise UpdateRuntimeError(
                        "更新脚本专用运行目录尚未初始化"
                    )
                if not runtime.has_captured_script:
                    raise UpdateRuntimeError(
                        self._trusted_update_script_error
                        or "进程启动时未能捕获匹配版本的更新脚本"
                    )
                snapshot = runtime.ensure_snapshot()
            except Exception as exc:
                self._trusted_update_script = None
                self._trusted_update_script_error = str(exc)
                logger.exception("Unable to ensure the trusted update helper")
                return None
            self._trusted_update_script = snapshot
            self._trusted_update_script_error = ""
            return snapshot

    def close_runtime_snapshot(self) -> None:
        """Remove only this process's exact trusted helper snapshot."""

        with self._trusted_update_script_lock:
            runtime = self._trusted_update_runtime
            if runtime is None:
                return
            runtime.close()
            self._trusted_update_script = None

    def _update_script_block_reason(self) -> str:
        with self._trusted_update_script_lock:
            snapshot = self.ensure_runtime_snapshot()
            runtime = self._trusted_update_runtime
            if snapshot is None or runtime is None:
                return (
                    "无法冻结与当前后端匹配的更新脚本，已拒绝停服操作: "
                    f"{self._trusted_update_script_error or '未知错误'}"
                )
            try:
                payload = runtime.read_verified_snapshot()
                script = payload.decode("utf-8")
            except (OSError, UnicodeDecodeError, UpdateRuntimeError) as exc:
                return f"更新脚本快照不可用，已拒绝停服操作: {exc}"
            protocol = self._parse_update_script_protocol(script)
            if protocol != UPDATE_SCRIPT_PROTOCOL_VERSION:
                return (
                    "当前运行版本的更新脚本协议不支持安全修复/重启"
                    f"（检测到 {protocol or 'legacy'}，需要 "
                    f"{UPDATE_SCRIPT_PROTOCOL_VERSION}）"
                )
            return ""

    @staticmethod
    def _atomic_write_json(path: Path, data: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        parent_metadata = path.parent.lstat()
        if (
            not stat.S_ISDIR(parent_metadata.st_mode)
            or stat.S_ISLNK(parent_metadata.st_mode)
        ):
            raise RuntimeError(f"状态目录不是安全的普通目录: {path.parent}")
        temporary = path.with_name(
            f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
        )
        fd = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(data, handle, ensure_ascii=False)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
            directory_fd = os.open(path.parent, os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except Exception:
            try:
                temporary.unlink()
            except OSError:
                pass
            raise

    def _secure_backup_dir(self) -> Path:
        backup_dir = Path(self.project_dir) / "backups"
        backup_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        metadata = backup_dir.lstat()
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or backup_dir.resolve(strict=True) != backup_dir.absolute()
        ):
            raise RuntimeError(f"备份目录不安全: {backup_dir}")
        if metadata.st_mode & 0o077:
            os.chmod(backup_dir, 0o700)
        return backup_dir

    def _locked_lease_file(self):
        """Return an exclusively locked descriptor for repo-wide admission."""
        if self._secure_backup_dir() != self._lease_lock_file.parent:
            raise RuntimeError("部署租约不在受管备份目录内")
        fd = os.open(
            self._lease_lock_file,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        metadata = os.fstat(fd)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or metadata.st_mode & 0o022
            or metadata.st_nlink != 1
        ):
            os.close(fd)
            raise RuntimeError("部署租约锁文件不安全")
        fcntl.flock(fd, fcntl.LOCK_EX)
        return fd

    def _read_deployment_lease(self) -> dict[str, Any]:
        try:
            metadata = self._lease_file.lstat()
        except FileNotFoundError:
            return {}
        except OSError as exc:
            return {"_invalid": True, "message": f"无法读取部署租约: {exc}"}
        if not stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
            return {"_invalid": True, "message": "部署租约不是安全的普通文件"}
        if (
            metadata.st_uid != os.getuid()
            or metadata.st_mode & 0o022
            or metadata.st_nlink != 1
        ):
            return {
                "_invalid": True,
                "message": "部署租约属主或写权限不安全",
            }
        try:
            flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
            fd = os.open(self._lease_file, flags)
            with os.fdopen(fd, "r", encoding="utf-8") as handle:
                value = json.load(handle)
        except (OSError, ValueError, TypeError) as exc:
            return {"_invalid": True, "message": f"部署租约损坏: {exc}"}
        if not isinstance(value, dict):
            return {"_invalid": True, "message": "部署租约不是 JSON object"}
        return value

    @staticmethod
    def _pid_start_identity(pid: int) -> str:
        try:
            suffix = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1]
            return suffix.split()[19]
        except (OSError, IndexError, ValueError):
            return ""

    @classmethod
    def _lease_pid_state(
        cls, lease: dict[str, Any], prefix: str
    ) -> str:
        try:
            pid = int(lease.get(f"{prefix}_pid", 0))
        except (TypeError, ValueError):
            return "unknown"
        if pid <= 0:
            return "unknown"
        identity = str(lease.get(f"{prefix}_pid_start") or "")
        if not identity:
            return "unknown"
        try:
            suffix = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1]
            current_identity = suffix.split()[19]
        except FileNotFoundError:
            return "dead"
        except (OSError, IndexError, ValueError):
            return "unknown"
        return "live" if identity == current_identity else "dead"

    @classmethod
    def _lease_owner_alive(cls, lease: dict[str, Any]) -> bool:
        return cls._lease_pid_state(lease, "owner") == "live"

    @classmethod
    def _lease_handoff_alive(cls, lease: dict[str, Any]) -> bool:
        return cls._lease_pid_state(lease, "handoff") == "live"

    @staticmethod
    def _provisional_handoff_expired(
        lease: dict[str, Any]
    ) -> bool | None:
        if not lease.get("handoff_provisional"):
            return False
        try:
            deadline = datetime.fromisoformat(
                str(lease.get("handoff_ack_deadline") or "")
            )
            if deadline.tzinfo is None:
                deadline = deadline.replace(tzinfo=timezone.utc)
            return datetime.now(timezone.utc) > deadline
        except (TypeError, ValueError):
            # Malformed/missing evidence is unknown, never proof that the
            # external worker died.
            return None

    @staticmethod
    def _lease_age_seconds(lease: dict[str, Any]) -> float:
        try:
            updated = datetime.fromisoformat(str(lease["updated_at"]))
            if updated.tzinfo is None:
                updated = updated.replace(tzinfo=timezone.utc)
            return max(
                0.0,
                (datetime.now(timezone.utc) - updated).total_seconds(),
            )
        except (KeyError, TypeError, ValueError):
            return float("inf")

    @staticmethod
    def _record_is_fresh(record: dict[str, Any], seconds: int = 180) -> bool:
        try:
            timestamp = datetime.fromisoformat(str(record.get("timestamp") or ""))
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            age = (datetime.now(timezone.utc) - timestamp).total_seconds()
            return -5 <= age <= seconds
        except (TypeError, ValueError):
            return False

    def _claim_deployment_lease(
        self,
        operation: str,
        *,
        allow_failed: bool = False,
        initial_state: dict[str, Any] | None = None,
    ) -> str | None:
        fd = self._locked_lease_file()
        try:
            existing = self._read_deployment_lease()
            if existing.get("_invalid"):
                return None
            status = str(existing.get("status") or "")
            if status in ACTIVE_DEPLOYMENT_STATUSES:
                owner_state = self._lease_pid_state(existing, "owner")
                handoff_state = self._lease_pid_state(existing, "handoff")
                handoff_waiting = bool(
                    existing.get("handoff")
                    and (
                        (
                            existing.get("handoff_provisional")
                            and not self._provisional_handoff_expired(existing)
                        )
                        or (
                            not existing.get("handoff_provisional")
                            and handoff_state in {"live", "unknown"}
                        )
                        or self._lease_age_seconds(existing) <= 30
                    )
                )
                if owner_state in {"live", "unknown"} or handoff_waiting:
                    return None
                existing.update(
                    {
                        "status": "failed",
                        "deployment_incomplete": True,
                        "message": "检测到部署进程异常退出，请先执行修复",
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
                self._atomic_write_json(self._lease_file, existing)
                if not allow_failed:
                    return None
            # An incomplete terminal is a repository-wide repair fence,
            # regardless of the terminal label.  In particular, a repair can
            # successfully restore its backup after an Alembic failure and
            # report ``rolled_back`` while still requiring maintenance mode:
            # the checked-out commit remains the same and its schema is still
            # behind.  Only the explicit repair/rollback admission paths may
            # replace that lease.
            if existing.get("deployment_incomplete") and not allow_failed:
                return None
            token = uuid.uuid4().hex
            now = datetime.now(timezone.utc).isoformat()
            lease = {
                "protocol_version": UPDATE_SCRIPT_PROTOCOL_VERSION,
                "owner_token": token,
                "owner_pid": os.getpid(),
                "owner_pid_start": self._pid_start_identity(os.getpid()),
                "port": self.port,
                "project_dir": str(Path(self.project_dir).resolve()),
                "operation": operation,
                "status": "claimed",
                "handoff": False,
                "deployment_incomplete": False,
                "created_at": now,
                "updated_at": now,
            }
            if initial_state:
                # State required to recover/retry an operation must be part of
                # the same atomic write that replaces the previous lease.
                # This is especially important for rollback: a process death
                # one instruction after claiming must not erase the only
                # old/new commit and backup references.
                recoverable_fields = {
                    "old_commit",
                    "new_commit",
                    "expected_commit",
                    "backup_file",
                    "frontend_dist_backup",
                    "database_migration_required",
                    "database_migration_applied",
                    "deployment_incomplete",
                }
                lease.update(
                    {
                        key: value
                        for key, value in initial_state.items()
                        if key in recoverable_fields
                    }
                )
            self._atomic_write_json(self._lease_file, lease)
            self._lease_token = token
            return token
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def _update_deployment_lease(self, **updates: Any) -> bool:
        token = self._lease_token
        if not token:
            return False
        fd = self._locked_lease_file()
        try:
            lease = self._read_deployment_lease()
            if lease.get("_invalid"):
                return False
            if lease.get("owner_token") != token:
                return False
            lease.update(updates)
            lease["updated_at"] = datetime.now(timezone.utc).isoformat()
            self._atomic_write_json(self._lease_file, lease)
            return True
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def _finish_deployment_claim(
        self, status: str, message: str, *, incomplete: bool
    ) -> bool:
        if self._update_deployment_lease(
            status=status,
            message=message,
            deployment_incomplete=incomplete,
            handoff=False,
            completed_at=datetime.now(timezone.utc).isoformat(),
        ):
            self._lease_token = None
            self._inspection_cache = None
            return True
        return False

    def _mark_deployment_handoff(self, mode: str) -> None:
        expected_commit = ""
        if self._current is not None:
            expected_commit = (
                self._current.old_commit
                if mode in {"rollback", "rollback_code"}
                else self._current.new_commit
            )
        if not self._update_deployment_lease(
            status="restarting",
            mode=mode,
            handoff=True,
            handoff_provisional=True,
            handoff_ack_deadline=(
                datetime.now(timezone.utc) + timedelta(seconds=30)
            ).isoformat(),
            deployment_incomplete=True,
            expected_commit=expected_commit,
            old_commit=self._current.old_commit if self._current else "",
            backup_file=self._current.backup_file if self._current else "",
            frontend_dist_backup=(
                self._current.frontend_dist_backup if self._current else ""
            ),
            database_migration_required=(
                self._current.database_migration_required if self._current else False
            ),
            database_migration_applied=(
                self._current.database_migration_applied if self._current else False
            ),
            update_id=self._current.update_id if self._current else "",
            update_channel=(
                self._current.update_channel if self._current else "main"
            ),
            target_version=(
                self._current.target_version if self._current else ""
            ),
        ):
            raise RuntimeError("部署租约已被其他进程替换，拒绝停服")

    def _cleanup_abandoned_run_copy(self, value: Any) -> None:
        if not value:
            return
        run_dir = Path(str(value))
        try:
            metadata = run_dir.lstat()
            resolved = run_dir.resolve(strict=True)
        except OSError:
            return
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or resolved.parent != Path("/tmp")
            or not resolved.name.startswith(
                f"ccm-update-run-{self.port}-"
            )
        ):
            logger.error("Refusing to clean unsafe deployment run dir: %s", run_dir)
            return
        shutil.rmtree(resolved)

    def recover_from_status_file(self):
        """Recover exact terminal state without assuming that startup == success."""
        lease = self._read_deployment_lease()
        if lease.get("_invalid"):
            message = str(
                lease.get("message") or "部署租约损坏，拒绝自动恢复"
            )
            self._current = UpdateState(
                update_id=f"recovered_{int(time.time())}",
                status="failed",
                operation="repair",
                deployment_incomplete=True,
                database_migration_applied=None,
                completed_at=datetime.now(timezone.utc).isoformat(),
                error=message,
                steps=[StepInfo(name=name) for name in STEP_NAMES],
            )
            return
        if lease and str(lease.get("port")) != str(self.port):
            lease = {}
        owner_token = str(lease.get("owner_token") or "")
        records: list[dict[str, Any]] = []
        # The repo-scoped lease is authoritative. /tmp can outlive a service or
        # port and the journal can contain the prior operation, so neither may
        # supersede a tokened lease unless it carries that exact owner token.
        if lease:
            records.append(lease)
        for path in (self._status_file, self._journal_file):
            try:
                value = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(value, dict):
                    continue
                if str(value.get("port")) != str(self.port):
                    continue
                if owner_token and value.get("owner_token") != owner_token:
                    continue
                records.append(value)
            except (OSError, ValueError, TypeError):
                continue
        if not records:
            return
        if lease:
            # A tokened v2 worker commits its result to the durable lease after
            # health verification. The /tmp mirror can be one write ahead and
            # therefore must never announce success while the lease is active.
            data = dict(lease)
            # Releases before update-channel metadata was added to the lease
            # already wrote it to the exact owner-token status/journal record.
            # Enrich display-only metadata without allowing that mirror to
            # override the lease's status, commit, migration, or fencing data.
            for record in reversed(records[1:]):
                if record.get("owner_token") != owner_token:
                    continue
                for key in ("update_id", "update_channel", "target_version"):
                    if not data.get(key) and record.get(key):
                        data[key] = record[key]
        else:
            records.sort(
                key=lambda item: str(
                    item.get("timestamp") or item.get("updated_at") or ""
                )
            )
            data = records[-1]
        status = str(data.get("status") or "")
        terminal = {"completed", "rolled_back", "failed", "rollback_failed"}
        tokenless_legacy = bool(
            not lease
            and not data.get("owner_token")
            and not data.get("deployment_owner_token")
            and self._record_is_fresh(data)
            and str(data.get("expected_commit") or "") == self._running_commit
        )
        if status not in terminal:
            # A new service may be the service started by the handoff worker.
            # Keep it visibly incomplete until the worker writes an exact,
            # token-matched terminal result; never infer migration success.
            status = (
                "restarting"
                if data.get("handoff") or tokenless_legacy
                else "failed"
            )
            data["deployment_incomplete"] = True
            if status == "failed":
                step_name = str(data.get("step") or "")
                data["message"] = (
                    "更新脚本在"
                    f"「{STEP_LABELS.get(step_name, step_name or '未知')}」"
                    "阶段中断，请执行修复"
                )
        normalized = "failed" if status == "rollback_failed" else status
        expected_commit = str(data.get("expected_commit") or "")
        if normalized in {"completed", "rolled_back"} and not expected_commit:
            normalized = "failed"
            data["deployment_incomplete"] = True
            data["message"] = "部署终态缺少 expected_commit，无法验证运行版本"
        if (
            normalized in {"completed", "rolled_back"}
            and expected_commit
            and self._running_commit != expected_commit
        ):
            normalized = "failed"
            data["deployment_incomplete"] = True
            data["message"] = (
                "部署脚本报告成功，但当前服务运行 commit "
                f"{self._running_commit or 'unknown'} 与期望 "
                f"{expected_commit} 不一致"
            )
        state = UpdateState(
            update_id=f"recovered_{int(time.time())}",
            status=normalized,
            old_commit=str(data.get("old_commit") or ""),
            new_commit=str(data.get("new_commit") or data.get("expected_commit") or ""),
            backup_file=str(data.get("backup_file") or ""),
            frontend_dist_backup=str(data.get("frontend_dist_backup") or ""),
            operation=str(data.get("operation") or "update"),
            deployment_incomplete=bool(
                data.get("deployment_incomplete", normalized in {"failed", "restarting"})
            ),
            database_migration_required=bool(
                data.get(
                    "database_migration_required",
                    bool(
                        data.get("old_commit")
                        and (
                            data.get("new_commit")
                            or data.get("expected_commit")
                        )
                        and data.get("old_commit")
                        != (
                            data.get("new_commit")
                            or data.get("expected_commit")
                        )
                    ),
                )
            ),
            database_migration_applied=_optional_bool(
                data.get("database_migration_applied", None)
            ),
            completed_at=str(data.get("timestamp") or data.get("updated_at") or ""),
            error=(
                str(data.get("message") or "")
                if normalized in {"failed", "rolled_back"}
                else ""
            ),
            update_channel=(
                str(data.get("update_channel") or "main")
                if str(data.get("update_channel") or "main")
                in {"stable", "main"}
                else "main"
            ),
            target_version=str(data.get("target_version") or ""),
            steps=[StepInfo(name=name) for name in STEP_NAMES],
        )
        step_name = str(data.get("step") or "")
        for step in state.steps:
            if normalized == "completed":
                step.status = "completed"
            elif step.name == step_name:
                step.status = "failed"
                step.message = state.error
                break
            else:
                step.status = "completed"
        self._current = state
        owner_token = str(data.get("owner_token") or "")
        if (
            normalized == "restarting"
            and owner_token
            and lease.get("owner_token") == owner_token
        ):
            self._lease_token = owner_token
        elif normalized == "restarting" and tokenless_legacy:
            self._legacy_handoff = True
        logger.info("Recovered update status: %s", normalized)

    async def get_status(self) -> dict[str, Any]:
        self._reconcile_external_terminal_status()
        environment = await self._inspect_environment()
        state = self._current.to_dict() if self._current else {"status": "idle"}
        return {**state, **environment}

    def _reconcile_external_terminal_status(self) -> None:
        """Consume only a result carrying the exact current lease token."""
        if not self._current or self._current.status != "restarting":
            return
        token = self._lease_token
        terminal_record: dict[str, Any] | None = None
        lease: dict[str, Any] = {}
        confirmed_no_worker = False
        if not token:
            if not self._legacy_handoff:
                return
            try:
                legacy = json.loads(
                    self._status_file.read_text(encoding="utf-8")
                )
            except (OSError, ValueError, TypeError):
                legacy = {}
            valid_legacy = bool(
                isinstance(legacy, dict)
                and str(legacy.get("port")) == str(self.port)
                and not legacy.get("owner_token")
                and not legacy.get("deployment_owner_token")
                and str(legacy.get("expected_commit") or "")
                == self._running_commit
            )
            if valid_legacy and legacy.get("status") in {
                "completed",
                "rolled_back",
                "failed",
                "rollback_failed",
            }:
                terminal_record = legacy
            elif valid_legacy and self._record_is_fresh(legacy):
                return
            else:
                terminal_record = {
                    **(legacy if isinstance(legacy, dict) else {}),
                    "status": "failed",
                    "message": "旧版部署交接未在宽限期内写入可验证终态",
                    "deployment_incomplete": True,
                }
        else:
            lease = self._read_deployment_lease()
            if (
                lease.get("owner_token") == token
                and lease.get("status")
                in {"completed", "rolled_back", "failed", "rollback_failed"}
            ):
                terminal_record = lease
            if terminal_record is None:
                if (
                    lease.get("owner_token") == token
                    and lease.get("status") in ACTIVE_DEPLOYMENT_STATUSES
                    and (
                        (
                            lease.get("handoff_provisional")
                            and self._provisional_handoff_expired(lease)
                        )
                        or (
                            not lease.get("handoff_provisional")
                            and self._lease_pid_state(lease, "handoff") == "dead"
                            and self._lease_age_seconds(lease) > 30
                        )
                    )
                ):
                    confirmed_no_worker = True
                    terminal_record = {
                        **lease,
                        "status": "failed",
                        "message": "外部部署进程已退出但没有写入终态，请执行修复",
                        "deployment_incomplete": True,
                    }
                else:
                    return
        status = str(terminal_record["status"])
        expected_commit = str(terminal_record.get("expected_commit") or "")
        if status in {"completed", "rolled_back"} and not expected_commit:
            status = "failed"
            terminal_record = {
                **terminal_record,
                "status": "failed",
                "deployment_incomplete": True,
                "message": "部署终态缺少 expected_commit，拒绝宣布成功",
            }
        if (
            status in {"completed", "rolled_back"}
            and expected_commit
            and self._running_commit != expected_commit
        ):
            status = "failed"
            terminal_record = {
                **terminal_record,
                "status": "failed",
                "deployment_incomplete": True,
                "message": (
                    "部署终态 commit 校验失败：当前运行 "
                    f"{self._running_commit or 'unknown'}，期望 {expected_commit}"
                ),
            }
        self._current.status = "failed" if status == "rollback_failed" else status
        self._current.error = (
            str(terminal_record.get("message") or "")
            if self._current.status in {"failed", "rolled_back"}
            else ""
        )
        self._current.deployment_incomplete = bool(
            terminal_record.get(
                "deployment_incomplete", self._current.status == "failed"
            )
        )
        self._current.database_migration_applied = _optional_bool(
            terminal_record.get(
                "database_migration_applied",
                self._current.database_migration_applied,
            )
        )
        self._current.completed_at = str(
            terminal_record.get("timestamp")
            or terminal_record.get("updated_at")
            or datetime.now(timezone.utc).isoformat()
        )
        recovered_channel = str(
            terminal_record.get("update_channel") or ""
        )
        if recovered_channel in {"stable", "main"}:
            self._current.update_channel = recovered_channel
        self._current.target_version = str(
            terminal_record.get("target_version")
            or self._current.target_version
        )
        if status in {"completed", "rolled_back"}:
            for step in self._current.steps:
                step.status = "completed"
                step.message = None
        if status in {"completed", "rolled_back"}:
            self._running_commit = (
                self._current.new_commit
                if status == "completed"
                else self._current.old_commit
            )
        terminal_persisted = not bool(token)
        if token and self._update_deployment_lease(
            status=status,
            message=str(terminal_record.get("message") or ""),
            handoff=False,
            deployment_incomplete=self._current.deployment_incomplete,
            database_migration_applied=self._current.database_migration_applied,
        ):
            self._lease_token = None
            terminal_persisted = True
        if token and not terminal_persisted:
            self._current.status = "restarting"
            self._current.deployment_incomplete = True
            self._current.error = (
                "无法持久化部署终态，继续保持维护栅栏"
            )
            return
        if not token:
            self._legacy_handoff = False
        if confirmed_no_worker and terminal_persisted:
            self._cleanup_abandoned_run_copy(
                terminal_record.get("run_copy_dir")
            )
            self._resume_dispatching()
        self._inspection_cache = None

    async def _get_active_tasks(self) -> list[dict[str, Any]]:
        """Return tasks that would be interrupted by a service restart."""
        if self.db_factory is None:
            return []
        async with self.db_factory() as db:
            rows = (await db.execute(
                select(Task.id, Task.title, Task.description, Task.status)
                .where(
                    or_(
                        Task.status.in_(ACTIVE_TASK_STATUSES),
                        Task.pty_background_generation.isnot(None),
                    ),
                    # Received shared tasks execute on their source CCM and
                    # are only remote-authoritative mirror rows here.
                    Task.shared_from_id.is_(None),
                )
                .order_by(Task.id.asc())
            )).all()
        return [
            {
                "id": row.id,
                "title": (
                    row.title
                    or row.description
                    or f"Task {row.id}"
                ),
                "status": row.status,
                "kind": "task",
            }
            for row in rows
        ]

    async def _get_running_taskless_instances(self) -> list[dict[str, Any]]:
        """Return every unresolved process claim, including quarantined rows.

        A Task can be marked terminal before late process output revives it to
        ``executing``.  During that window Instance.current_task_id remains
        non-null, so filtering only taskless instances would stop a real turn.
        Stale recovery deliberately marks an uncertain orphan ``error`` while
        retaining its PID/owner evidence. Such a row must continue to block a
        deployment even though its status is no longer ``running``.
        """
        if self.db_factory is None:
            return []
        async with self.db_factory() as db:
            rows = (await db.execute(
                select(
                    Instance.id,
                    Instance.name,
                    Instance.status,
                    Instance.current_task_id,
                    Instance.current_plan_run_id,
                )
                .where(
                    or_(
                        Instance.status == "running",
                        Instance.pid.isnot(None),
                        Instance.current_task_id.isnot(None),
                        Instance.current_plan_run_id.isnot(None),
                    )
                )
                .order_by(Instance.id.asc())
            )).all()
        return [
            {
                "id": row.id,
                "instance_id": row.id,
                "current_task_id": row.current_task_id,
                **(
                    {"current_plan_run_id": row.current_plan_run_id}
                    if row.current_plan_run_id is not None
                    else {}
                ),
                "title": (
                    (
                        f"实例 {row.name}（任务 #{row.current_task_id} 仍有运行进程）"
                        if row.current_task_id is not None
                        else (
                            f"实例 {row.name}（Plan Run #{row.current_plan_run_id} 仍有运行进程）"
                            if row.current_plan_run_id is not None
                            else f"实例 {row.name}（未关联任务）"
                        )
                    )
                    if row.status == "running"
                    else (
                        f"实例 {row.name}（任务 #{row.current_task_id} 仍有未解除运行证据）"
                        if row.current_task_id is not None
                        else (
                            f"实例 {row.name}（Plan Run #{row.current_plan_run_id} 仍有未解除运行证据）"
                            if row.current_plan_run_id is not None
                            else f"实例 {row.name}（仍有未解除运行证据）"
                        )
                    )
                ),
                "status": (
                    "running_instance"
                    if row.status == "running"
                    else "quarantined_process_evidence"
                ),
                "kind": "instance",
            }
            for row in rows
        ]

    async def _get_blocking_tasks(
        self,
        pending_task_ids: set[int] | None = None,
    ) -> list[dict[str, Any]]:
        """Combine every local runtime generation a restart would interrupt."""
        if self.maintenance_only:
            # Startup guard enters this mode before any dispatcher/runtime is
            # started. The repo-wide deployment fence remains authoritative;
            # querying a schema known to be behind would make repair impossible.
            return []
        active_tasks = await self._get_active_tasks()
        active_ids = {task["id"] for task in active_tasks}
        running_instances = await self._get_running_taskless_instances()
        claim_counts: dict[int, int] = {}
        for instance in running_instances:
            task_id = instance.get("current_task_id")
            if task_id in active_ids:
                claim_counts[task_id] = claim_counts.get(task_id, 0) + 1
        for task in active_tasks:
            claim_count = claim_counts.get(task["id"], 0)
            if claim_count:
                task["instance_claim_count"] = claim_count
        taskless_instances = []
        for instance in running_instances:
            if instance.get("current_task_id") in active_ids:
                continue
            blocker = dict(instance)
            blocker.pop("current_task_id", None)
            taskless_instances.append(blocker)
        auxiliary_blockers: list[dict[str, Any]] = []
        auxiliary_snapshot = getattr(
            self.dispatcher, "active_auxiliary_blockers", None
        )
        if callable(auxiliary_snapshot):
            snapshot = auxiliary_snapshot()
            if isinstance(snapshot, list):
                auxiliary_blockers = [
                    dict(blocker)
                    for blocker in snapshot
                    if isinstance(blocker, dict)
                ]
        if pending_task_ids is None:
            if self.dispatcher is None or not hasattr(
                self.dispatcher, "pending_task_start_ids"
            ):
                pending_task_ids = set()
            else:
                pending_task_ids = await self.dispatcher.pending_task_start_ids()
        queued_ids = set(pending_task_ids) - active_ids
        if not queued_ids:
            return (
                active_tasks
                + taskless_instances
                + auxiliary_blockers
            )

        if self.db_factory is None:
            queued_tasks = [
                {"id": task_id, "title": f"Task {task_id}", "status": "queued_resume"}
                for task_id in sorted(queued_ids)
            ]
        else:
            async with self.db_factory() as db:
                rows = (await db.execute(
                    select(Task.id, Task.title)
                    .where(Task.id.in_(queued_ids))
                    .order_by(Task.id.asc())
                )).all()
            found = {row.id for row in rows}
            queued_tasks = [
                {"id": row.id, "title": row.title, "status": "queued_resume"}
                for row in rows
            ]
            queued_tasks.extend(
                {"id": task_id, "title": f"Task {task_id}", "status": "queued_resume"}
                for task_id in sorted(queued_ids - found)
            )
        return (
            active_tasks
            + queued_tasks
            + taskless_instances
            + auxiliary_blockers
        )

    async def reconcile_blockers(self) -> dict[str, Any]:
        """Safely re-check runtime evidence without weakening deployment gates."""

        async with self._operation_lock:
            self._reconcile_external_terminal_status()
            if self.maintenance_only:
                return {
                    "error": "部署维护模式下不能访问可能落后的任务表，请先修复或回滚",
                    "repair_required": True,
                }
            if self._lock.locked() or (
                self._current
                and self._current.status in ("running", "restarting")
            ):
                return {"error": "有部署操作正在进行中"}
            if self.dispatcher is None or not hasattr(
                self.dispatcher,
                "reconcile_stale_state_for_maintenance",
            ):
                return {"error": "当前运行时不支持安全核对任务状态"}

            paused = False
            try:
                await self._pause_dispatching()
                paused = True
                await self.dispatcher.reconcile_stale_state_for_maintenance()
                blockers = await self._get_blocking_tasks()
                return {
                    "reconciled": True,
                    **self._blocker_payload(blockers),
                }
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Unable to reconcile deployment blockers")
                return {
                    "error": f"无法安全核对运行状态: {exc}",
                    "reconciled": False,
                    "update_blocked": True,
                }
            finally:
                if paused:
                    self._resume_dispatching()

    @staticmethod
    def _blocker_payload(active_tasks: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "update_blocked": bool(active_tasks),
            "active_task_count": len(active_tasks),
            "active_tasks": active_tasks,
        }

    async def _pause_dispatching(self) -> None:
        if self.dispatcher is not None:
            await self.dispatcher.pause_dispatching()

    def _resume_dispatching(self) -> None:
        if self.dispatcher is not None:
            self.dispatcher.resume_dispatching()

    def _cancel_claim_for_new_blockers(
        self,
        operation_label: str,
        blockers: list[dict[str, Any]],
        *,
        preserve_incomplete: bool,
    ) -> dict[str, Any]:
        """Release a just-claimed lease after the cross-process race recheck.

        Claiming the exclusive repository lease can wait behind a task
        process's shared start fence.  Once the claim wins, that task's DB
        commit is visible and no newer task can pass the active lease, so this
        second blocker result is the authoritative deployment admission check.
        """
        released = self._finish_deployment_claim(
            "failed",
            f"{operation_label}准入期间出现运行任务，操作已取消",
            incomplete=preserve_incomplete,
        )
        if released:
            self._resume_dispatching()
            return {
                "error": (
                    f"{operation_label}准入期间出现了 {len(blockers)} 个"
                    "运行任务，已取消操作"
                ),
                **self._blocker_payload(blockers),
            }
        return {
            "error": (
                f"{operation_label}准入期间出现运行任务，且无法安全释放"
                "部署租约；任务调度保持暂停，请检查部署状态"
            ),
            "repair_required": True,
            **self._blocker_payload(blockers),
        }

    def _finish_admission_and_resume(
        self,
        *,
        claimed: bool,
        message: str,
        incomplete: bool,
    ) -> bool:
        """Release an admission lease before reopening local task starts.

        If the durable terminal write fails, the repository fence must remain
        active and the local dispatcher must remain paused.  Reopening local
        admission first would let this process race its own unresolved lease.
        """
        if claimed and self._lease_token:
            try:
                released = self._finish_deployment_claim(
                    "failed", message, incomplete=incomplete
                )
            except Exception:
                released = False
                logger.exception(
                    "Unable to persist deployment admission terminal state"
                )
            if not released:
                logger.critical(
                    "Unable to release deployment admission lease; "
                    "dispatcher remains paused"
                )
                return False
        self._resume_dispatching()
        return True

    @asynccontextmanager
    async def _maintenance_shutdown_guard(self):
        if self.dispatcher is not None and hasattr(
            self.dispatcher, "maintenance_shutdown_guard"
        ):
            async with self.dispatcher.maintenance_shutdown_guard() as pending_ids:
                yield pending_ids
        else:
            yield set()

    async def _commit_shutdown_if_idle(self, action) -> list[dict[str, Any]]:
        """Atomically recheck blockers and synchronously schedule shutdown.

        All user-visible broadcasts and grace sleeps must happen before this
        helper. There is intentionally no await between the successful blocker
        query and ``action()`` while task-start admission is held closed.
        """
        async with self._maintenance_shutdown_guard() as pending_ids:
            blockers = await self._get_blocking_tasks(set(pending_ids))
            if blockers:
                return blockers
            action()
            if self.dispatcher is not None and hasattr(
                self.dispatcher, "commit_maintenance_shutdown"
            ):
                self.dispatcher.commit_maintenance_shutdown()
            return []

    async def _resolve_remote(self, branch: str) -> str:
        """Use the branch's configured tracking remote, falling back to origin."""
        result = await self._run_cmd(
            ["git", "config", "--get", f"branch.{branch}.remote"]
        )
        remote = result["stdout"].strip() if result["returncode"] == 0 else ""
        return remote if remote and remote != "." else "origin"

    async def _disk_commit(self) -> str:
        result = await self._run_cmd(["git", "rev-parse", "HEAD"])
        if result["returncode"] == 0:
            return result["stdout"].strip()
        deploy_commit = Path(self.project_dir) / ".deploy_commit"
        try:
            return deploy_commit.read_text().strip()
        except OSError:
            return ""

    async def _needs_restart(self, disk_commit: str | None = None) -> bool:
        """Check whether disk code differs from the version loaded in memory."""
        try:
            current_disk_commit = disk_commit or await self._disk_commit()
            return bool(
                self._running_commit
                and current_disk_commit
                and self._running_commit != current_disk_commit
            )
        except Exception:
            logger.debug("_needs_restart check failed", exc_info=True)
            return False

    async def _deployment_base_commit(self, disk_commit: str) -> str:
        """Include manually pulled changes in deployment analysis and rollback."""
        if not self._running_commit or self._running_commit == disk_commit:
            return disk_commit
        result = await self._run_cmd(
            ["git", "merge-base", "--is-ancestor", self._running_commit, disk_commit]
        )
        return self._running_commit if result["returncode"] == 0 else disk_commit

    async def _dirty_worktree_files(self) -> list[str]:
        status = await self._run_cmd(
            [
                "git",
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
            ]
        )
        if status["returncode"] != 0:
            raise RuntimeError(
                f"无法确认 Git 工作区状态: {status['stderr']}"
            )
        return [
            line
            for line in status["stdout"].splitlines()
            if line.strip()
        ]

    @staticmethod
    def _parse_alembic_revisions(output: str) -> list[str]:
        revisions: list[str] = []
        for line in output.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith(("INFO", "DEBUG", "WARNING")):
                continue
            match = re.match(
                r"^([0-9A-Za-z_]+)(?:\s+\([^)]*\))*$", stripped
            )
            if match:
                revisions.append(match.group(1))
        return sorted(set(revisions))

    async def _database_revision_status(self) -> dict[str, Any]:
        current = await self._run_cmd(
            [sys.executable, "-m", "alembic", "current"], timeout=30
        )
        heads = await self._run_cmd(
            [sys.executable, "-m", "alembic", "heads"], timeout=30
        )
        errors: list[str] = []
        if current["returncode"] != 0:
            errors.append(
                f"alembic current 失败: {current['stderr'] or current['stdout']}"
            )
        if heads["returncode"] != 0:
            errors.append(
                f"alembic heads 失败: {heads['stderr'] or heads['stdout']}"
            )
        current_revisions = (
            self._parse_alembic_revisions(current["stdout"])
            if current["returncode"] == 0
            else []
        )
        head_revisions = (
            self._parse_alembic_revisions(heads["stdout"])
            if heads["returncode"] == 0
            else []
        )
        if not head_revisions and not errors:
            errors.append("alembic heads 未返回目标 revision")
        database_up_to_date: bool | None = None
        if not errors:
            database_up_to_date = current_revisions == head_revisions
        return {
            "database_current_revisions": current_revisions,
            "database_head_revisions": head_revisions,
            "database_up_to_date": database_up_to_date,
            "db_current_revision": ",".join(current_revisions),
            "db_head_revision": ",".join(head_revisions),
            "db_up_to_date": database_up_to_date,
            "database_revision_error": "; ".join(errors),
        }

    async def _inspect_environment(
        self, *, force: bool = False
    ) -> dict[str, Any]:
        now = time.monotonic()
        if (
            not force
            and self._inspection_cache is not None
            and self._inspection_cache[0] > now
        ):
            return dict(self._inspection_cache[1])
        async with self._inspection_lock:
            now = time.monotonic()
            if (
                not force
                and self._inspection_cache is not None
                and self._inspection_cache[0] > now
            ):
                return dict(self._inspection_cache[1])
            result = await self._inspect_environment_uncached()
            self._inspection_cache = (time.monotonic() + 5.0, dict(result))
            return result

    async def _inspect_environment_uncached(self) -> dict[str, Any]:
        disk_commit = await self._disk_commit()
        database = await self._database_revision_status()
        lease = self._read_deployment_lease()
        # The terminal label describes what the worker managed to restore, not
        # whether the checked-out deployment is usable.  A repair migration
        # can roll its DB backup back successfully while leaving the same code
        # checked out and the schema behind, so ``rolled_back`` may still be an
        # explicit maintenance fence.
        previous_incomplete = bool(
            lease.get("deployment_incomplete")
            and str(lease.get("status") or "")
            not in ACTIVE_DEPLOYMENT_STATUSES
        )
        reason_codes: list[str] = []
        reasons: list[str] = []
        if lease.get("_invalid"):
            reason_codes.append("deployment_lease_invalid")
            reasons.append(str(lease.get("message") or "部署租约损坏"))
        if previous_incomplete:
            reason_codes.append("previous_deployment_failed")
            reasons.append(str(lease.get("message") or "上一次部署未完整完成"))
        if database["database_up_to_date"] is False:
            reason_codes.append("database_migration_pending")
            reasons.append("数据库 revision 落后于当前代码，需要补跑迁移")
        elif database["database_up_to_date"] is None:
            reason_codes.append("database_revision_unknown")
            reasons.append(
                database["database_revision_error"] or "无法确认数据库 revision"
            )
        needs_restart = bool(
            self._running_commit
            and disk_commit
            and self._running_commit != disk_commit
        )
        if needs_restart:
            reason_codes.append("runtime_code_stale")
            reasons.append(
                "磁盘代码与当前服务运行 commit 不同，需要先同步依赖、"
                "重建前端并确认数据库后再重启"
            )
        if (
            self._current is not None
            and self._current.deployment_incomplete
            and "previous_deployment_failed" not in reason_codes
        ):
            reason_codes.append("previous_deployment_failed")
            reasons.append(self._current.error or "上一次部署未完整完成")
        return {
            "running_commit": self._running_commit,
            "disk_commit": disk_commit,
            "current_commit": disk_commit,
            "needs_restart": needs_restart,
            "manual_update_detected": needs_restart,
            "repair_required": bool(reason_codes),
            "repair_reason_codes": reason_codes,
            "repair_reasons": reasons,
            "automatic_rollback_supported": self._automatic_rollback_supported,
            "restart_only_safe": bool(
                not needs_restart
                and database["database_up_to_date"] is True
                and not reason_codes
            ),
            "update_supported": self._automatic_rollback_supported,
            "update_block_reason": (
                ""
                if self._automatic_rollback_supported
                else "一键更新/修复仅支持文件型 SQLite；重启仍可使用"
            ),
            **database,
        }

    @staticmethod
    def _parse_update_script_protocol(script_text: str) -> int | None:
        match = re.search(
            r"^CCM_UPDATE_PROTOCOL_VERSION=(\d+)\s*$",
            script_text,
            flags=re.MULTILINE,
        )
        return int(match.group(1)) if match else None

    async def _fetch_and_validate_target_protocol(
        self, remote: str, target_branch: str, step: StepInfo | None = None
    ) -> tuple[bool, str, str]:
        refspec = (
            f"+refs/heads/{target_branch}:refs/remotes/{remote}/{target_branch}"
        )
        fetch = await self._run_cmd(
            ["git", "fetch", remote, refspec], timeout=60, step=step
        )
        if fetch["returncode"] != 0:
            return False, f"git fetch 失败: {fetch['stderr']}", ""
        remote_ref = f"{remote}/{target_branch}"
        revision = await self._run_cmd(
            ["git", "rev-parse", "--verify", remote_ref], timeout=30
        )
        target_commit = revision["stdout"].strip()
        if (
            revision["returncode"] != 0
            or not re.fullmatch(r"[0-9a-fA-F]{40,64}", target_commit)
        ):
            return False, "无法锁定目标分支 commit", ""
        show = await self._run_cmd(
            ["git", "show", f"{target_commit}:scripts/update_migrate.sh"],
            timeout=30,
        )
        if show["returncode"] != 0:
            return False, "目标版本缺少兼容的部署脚本，拒绝一键更新", ""
        protocol = self._parse_update_script_protocol(show["stdout"])
        if protocol != UPDATE_SCRIPT_PROTOCOL_VERSION:
            return (
                False,
                "目标版本的部署协议不兼容"
                f"（目标={protocol or 'legacy'}，当前={UPDATE_SCRIPT_PROTOCOL_VERSION}），"
                "请使用匹配版本手动部署",
                "",
            )
        return True, "", target_commit

    async def _cached_version_check(
        self,
        target_branch: str,
        *,
        force: bool = False,
    ) -> dict[str, Any]:
        """Coalesce concurrent fetches and briefly reuse their version result."""
        now = time.monotonic()
        cached = self._dry_run_cache.get(target_branch)
        if not force and cached and cached[0] > now:
            return dict(cached[1])

        async with self._dry_run_lock:
            now = time.monotonic()
            cached = self._dry_run_cache.get(target_branch)
            if not force and cached and cached[0] > now:
                return dict(cached[1])

            result = await self._check_remote_updates(target_branch)
            ttl = DRY_RUN_ERROR_CACHE_SECONDS if result.get("error") else DRY_RUN_CACHE_SECONDS
            expires_at = time.monotonic() + ttl
            self._dry_run_cache = {
                key: value
                for key, value in self._dry_run_cache.items()
                if value[0] > now
            }
            self._dry_run_cache[target_branch] = (expires_at, dict(result))
            return result

    async def dry_run(
        self,
        branch: str | None = None,
        *,
        force: bool = False,
        channel: str | None = None,
    ) -> dict[str, Any]:
        """Check for available updates without applying them."""
        selected_channel = channel or await self._configured_update_channel()
        if selected_channel == "stable":
            version_result = await self._cached_stable_version_check(force=force)
        else:
            target_branch = branch or "main"
            version_result = await self._cached_version_check(target_branch, force=force)
        version_result.setdefault("channel", selected_channel)
        environment = await self._inspect_environment()
        active_tasks = await self._get_blocking_tasks()
        return {
            **version_result,
            **environment,
            **self._blocker_payload(active_tasks),
        }

    async def _configured_update_channel(self) -> str:
        """Read this independent CCM instance's persisted update channel."""
        if self.db_factory is None:
            # Service unit tests without a DB retain the historical behavior.
            return "main"
        try:
            async with self.db_factory() as db:
                row = await db.get(GlobalSettings, 1)
                return row.update_channel if row and row.update_channel in {"stable", "main"} else "stable"
        except Exception:
            logger.exception("Unable to read update channel; refusing stable ambiguity")
            return "stable"

    @staticmethod
    def _stable_tag_key(tag: str) -> tuple[int, int, int] | None:
        match = re.fullmatch(
            r"v?(\d+)\.(\d+)\.(\d+)(?:\+[0-9A-Za-z.-]+)?", tag
        )
        if not match:
            return None
        return tuple(int(part) for part in match.groups())

    async def _check_stable_updates(self) -> dict[str, Any]:
        fetch = await self._run_cmd(["git", "fetch", "--tags"], timeout=60)
        head = await self._disk_commit()
        if fetch["returncode"] != 0:
            return {
                "has_updates": False,
                "channel": "stable",
                "current_commit": head,
                "running_commit": self._running_commit,
                "error": fetch["stderr"],
            }
        tags_result = await self._run_cmd(["git", "tag", "--list", "v*"])
        candidates = []
        for raw_tag in tags_result["stdout"].splitlines():
            tag = raw_tag.strip()
            version = self._stable_tag_key(tag)
            if version is None:
                continue
            commit_result = await self._run_cmd(["git", "rev-list", "-n", "1", tag])
            commit = commit_result["stdout"].strip()
            if commit_result["returncode"] == 0 and re.fullmatch(r"[0-9a-fA-F]{40,64}", commit):
                candidates.append((version, tag, commit))
        if not candidates:
            return {
                "has_updates": False,
                "channel": "stable",
                "current_commit": head,
                "running_commit": self._running_commit,
                "error": "仓库没有可用的正式版本 tag",
            }
        _, version_tag, release_commit = max(candidates, key=lambda item: item[0])
        # Stable is also an explicit escape hatch from a Main/test build.  A
        # release tag behind the current checkout is a channel switch (and
        # potentially a rollback), not "already up to date".
        ancestry = await self._run_cmd(
            ["git", "merge-base", "--is-ancestor", release_commit, head],
            timeout=30,
        )
        is_downgrade = bool(head != release_commit and ancestry["returncode"] == 0)
        commits_output = await self._run_cmd(["git", "log", "--oneline", f"{head}..{release_commit}"])
        commits = [line for line in commits_output["stdout"].splitlines() if line.strip()]
        diff_output = await self._run_cmd(["git", "diff", "--name-only", f"{head}..{release_commit}"])
        files = [line for line in diff_output["stdout"].splitlines() if line.strip()]
        migration_files = [path for path in files if path.startswith("alembic/versions/")]
        downgrade_blocked = bool(is_downgrade and migration_files)
        result = {
            "has_updates": head != release_commit,
            "update_kind": "stable_switch" if is_downgrade else "stable_upgrade",
            "is_stable_downgrade": is_downgrade,
            "stable_switch_blocked": downgrade_blocked,
            "channel": "stable",
            "version": version_tag,
            "latest_version": version_tag,
            "current_commit": head,
            "running_commit": self._running_commit,
            "latest_commit": release_commit,
            "commits_behind": len(commits),
            "commit_messages": [line.split(" ", 1)[-1] for line in commits[:20]],
            "has_new_migrations": bool(migration_files),
            "migration_count": len(migration_files),
            "has_frontend_changes": any(path.startswith("frontend/") for path in files),
            "has_package_changes": "frontend/package.json" in files,
        }
        if downgrade_blocked:
            result["has_updates"] = False
            result["error"] = (
                "当前测试版与正式版之间包含数据库迁移，不能自动切回 Stable；"
                "请先备份并制定数据库降级方案"
            )
        return result

    async def _cached_stable_version_check(self, *, force: bool = False) -> dict[str, Any]:
        key = "stable"
        now = time.monotonic()
        cached = self._dry_run_cache.get(key)
        if not force and cached and cached[0] > now:
            return dict(cached[1])
        async with self._dry_run_lock:
            cached = self._dry_run_cache.get(key)
            if not force and cached and cached[0] > time.monotonic():
                return dict(cached[1])
            result = await self._check_stable_updates()
            ttl = DRY_RUN_ERROR_CACHE_SECONDS if result.get("error") else DRY_RUN_CACHE_SECONDS
            self._dry_run_cache[key] = (time.monotonic() + ttl, dict(result))
            return result

    async def _check_remote_updates(self, target_branch: str) -> dict[str, Any]:
        """Fetch and compare versions; caller handles caching and task blockers."""
        remote = await self._resolve_remote(target_branch)
        remote_ref = f"{remote}/{target_branch}"
        refspec = f"+refs/heads/{target_branch}:refs/remotes/{remote}/{target_branch}"
        # Local restart detection must not depend on network availability.  A
        # manual pull changes disk HEAD even when the later fetch fails.
        head = await self._disk_commit()
        needs_restart = await self._needs_restart(head)
        result = await self._run_cmd(["git", "fetch", remote, refspec], timeout=60)
        if result["returncode"] != 0:
            return {
                "has_updates": False,
                "needs_restart": needs_restart,
                "manual_update_detected": needs_restart,
                "remote": remote,
                "current_commit": head,
                "running_commit": self._running_commit,
                "error": result["stderr"],
            }

        remote_head = (await self._run_cmd(["git", "rev-parse", remote_ref]))["stdout"].strip()

        if head == remote_head:
            return {
                "has_updates": False,
                "needs_restart": needs_restart,
                "manual_update_detected": needs_restart,
                "remote": remote,
                "current_commit": head,
                "running_commit": self._running_commit,
                "latest_commit": remote_head,
            }

        diff_output = (await self._run_cmd(
            ["git", "log", "--oneline", f"{head}..{remote_head}"]
        ))["stdout"].strip()
        commits = [line for line in diff_output.split("\n") if line.strip()]

        diff_files = (await self._run_cmd(
            ["git", "diff", "--name-only", f"{head}..{remote_head}"]
        ))["stdout"].strip()
        files = [f for f in diff_files.split("\n") if f.strip()]

        migration_files = [f for f in files if f.startswith("alembic/versions/")]
        frontend_files = [f for f in files if f.startswith("frontend/")]
        has_package_changes = "frontend/package.json" in files

        return {
            "has_updates": bool(commits),
            "needs_restart": needs_restart,
            "manual_update_detected": needs_restart,
            "branch": target_branch,
            "remote": remote,
            "commits_behind": len(commits),
            "has_new_migrations": len(migration_files) > 0,
            "migration_count": len(migration_files),
            "has_frontend_changes": len(frontend_files) > 0,
            "has_package_changes": has_package_changes,
            "current_commit": head,
            "running_commit": self._running_commit,
            "latest_commit": remote_head,
            "commit_messages": [c.split(" ", 1)[-1] if " " in c else c for c in commits[:20]],
        }

    async def start_update(
        self,
        skip_frontend_build: bool = False,
        force: bool = False,
        branch: str | None = None,
        channel: str | None = None,
    ) -> dict[str, Any]:
        async with self._operation_lock:
            self._reconcile_external_terminal_status()
            if self.maintenance_only:
                return {
                    "error": "服务处于部署维护模式，只允许修复或回滚"
                }
            if not self._automatic_rollback_supported:
                return {
                    "error": (
                        "一键更新仅支持可安全快照回滚的文件型 SQLite；"
                        "当前数据库请先手动备份并按运维流程部署"
                    )
                }
            if self._lock.locked() or (
                self._current and self._current.status in ("running", "restarting")
            ):
                if self._current:
                    return {"error": "更新正在进行中", "update_id": self._current.update_id}
                return {"error": "更新正在进行中"}
            try:
                dirty_files = await self._dirty_worktree_files()
            except Exception as exc:
                return {"error": str(exc)}
            if dirty_files:
                return {
                    "error": (
                        "工作区存在未提交的本地改动，自动更新无法证明"
                        "部署 commit；请先提交或手动处理"
                    ),
                    "dirty_files": dirty_files,
                }

            selected_channel = channel or await self._configured_update_channel()
            if selected_channel not in {"stable", "main"}:
                return {"error": "无效的更新渠道"}

            # Freeze new claims before checking the DB.  Existing tasks are
            # never cancelled; callers retry after they finish.
            claimed = False
            try:
                await self._pause_dispatching()
                try:
                    active_tasks = await self._get_blocking_tasks()
                except Exception as exc:
                    self._resume_dispatching()
                    logger.exception("Unable to verify active tasks before update")
                    return {"error": f"无法确认当前任务状态，已取消更新: {exc}"}
                if active_tasks:
                    self._resume_dispatching()
                    return {
                        "error": f"当前有 {len(active_tasks)} 个任务正在运行，请等待任务完成后再更新",
                        **self._blocker_payload(active_tasks),
                    }
                if self._update_script_block_reason():
                    self._resume_dispatching()
                    return {"error": self._update_script_block_reason()}
                if self._claim_deployment_lease("update") is None:
                    self._resume_dispatching()
                    return {
                        "error": "仓库存在进行中或未修复的部署，请刷新状态并先执行修复",
                        "repair_required": True,
                    }
                claimed = True
                post_claim_blockers = await self._get_blocking_tasks()
                if post_claim_blockers:
                    return self._cancel_claim_for_new_blockers(
                        "更新",
                        post_claim_blockers,
                        preserve_incomplete=False,
                    )

                update_id = f"upd_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
                state = UpdateState(
                    update_id=update_id,
                    status="running",
                    operation="update",
                    started_at=datetime.now(timezone.utc).isoformat(),
                    steps=[StepInfo(name=n) for n in STEP_NAMES],
                    update_channel=selected_channel,
                )
                self._current = state
                self._inspection_cache = None
                self._update_deployment_lease(
                    status="running",
                    old_commit=self._running_commit,
                    deployment_incomplete=False,
                    update_id=update_id,
                    update_channel=selected_channel,
                )

                asyncio.create_task(
                    self._run_pipeline(state, skip_frontend_build=skip_frontend_build, force=force, branch=branch, channel=selected_channel)
                )
                return {"update_id": update_id, "status": "started"}
            except asyncio.CancelledError:
                # Request disconnects and service shutdown can cancel admission
                # after pause_dispatching() has closed the gate but before the
                # background pipeline owns cleanup. Never leave task starts
                # paused when no maintenance operation was admitted.
                self._finish_admission_and_resume(
                    claimed=claimed,
                    message="更新在交接前被取消",
                    incomplete=False,
                )
                raise
            except Exception:
                self._finish_admission_and_resume(
                    claimed=claimed,
                    message="更新准入失败",
                    incomplete=False,
                )
                raise

    async def start_repair(
        self, skip_frontend_build: bool = False
    ) -> dict[str, Any]:
        async with self._operation_lock:
            self._reconcile_external_terminal_status()
            if skip_frontend_build:
                return {"error": "修复必须重建前端，不能跳过前端构建"}
            if not self._automatic_rollback_supported:
                return {
                    "error": (
                        "自动修复仅支持可安全快照回滚的文件型 SQLite；"
                        "当前数据库请使用手动维护流程"
                    )
                }
            if self._lock.locked() or (
                self._current and self._current.status in ("running", "restarting")
            ):
                return {"error": "有操作正在进行中"}
            block_reason = self._update_script_block_reason()
            if block_reason:
                return {"error": block_reason}
            try:
                dirty_files = await self._dirty_worktree_files()
            except Exception as exc:
                return {"error": str(exc)}
            if dirty_files:
                return {
                    "error": (
                        "工作区存在未提交的本地改动，修复会导致部署版本"
                        "无法由 commit 证明；请先提交或手动处理"
                    ),
                    "dirty_files": dirty_files,
                }
            claimed = False
            preserve_incomplete = bool(
                self.maintenance_only
                or (
                    self._current is not None
                    and self._current.deployment_incomplete
                )
            )
            try:
                await self._pause_dispatching()
                blockers = await self._get_blocking_tasks()
                if blockers:
                    self._resume_dispatching()
                    return {
                        "error": f"当前有 {len(blockers)} 个任务正在运行，请等待任务完成后再修复",
                        **self._blocker_payload(blockers),
                    }
                if self._claim_deployment_lease("repair", allow_failed=True) is None:
                    self._resume_dispatching()
                    return {"error": "另一个 CCM 进程正在操作此仓库"}
                claimed = True
                preserve_incomplete = bool(
                    preserve_incomplete
                    or self.maintenance_only
                    or (
                        self._current is not None
                        and self._current.deployment_incomplete
                    )
                )
                post_claim_blockers = await self._get_blocking_tasks()
                if post_claim_blockers:
                    return self._cancel_claim_for_new_blockers(
                        "修复",
                        post_claim_blockers,
                        preserve_incomplete=preserve_incomplete,
                    )
                disk_commit = await self._disk_commit()
                state = UpdateState(
                    update_id=f"repair_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}",
                    status="running",
                    operation="repair",
                    old_commit=self._running_commit or disk_commit,
                    new_commit=disk_commit,
                    deployment_incomplete=True,
                    started_at=datetime.now(timezone.utc).isoformat(),
                    steps=[StepInfo(name=name) for name in STEP_NAMES],
                )
                self._current = state
                self._inspection_cache = None
                self._update_deployment_lease(
                    status="running",
                    old_commit=state.old_commit,
                    expected_commit=state.new_commit,
                    deployment_incomplete=True,
                )
                asyncio.create_task(
                    self._run_repair(
                        state, skip_frontend_build=skip_frontend_build
                    )
                )
                return {"update_id": state.update_id, "status": "started"}
            except asyncio.CancelledError:
                self._finish_admission_and_resume(
                    claimed=claimed,
                    message="修复在交接前被取消",
                    incomplete=(
                        preserve_incomplete or claimed
                    ),
                )
                raise
            except Exception:
                self._finish_admission_and_resume(
                    claimed=claimed,
                    message="修复准入失败",
                    incomplete=(
                        preserve_incomplete or claimed
                    ),
                )
                raise

    async def restart(self) -> dict[str, Any]:
        async with self._operation_lock:
            self._reconcile_external_terminal_status()
            if self.maintenance_only:
                return {
                    "error": "服务处于部署维护模式，请先执行修复或回滚",
                    "repair_required": True,
                }
            if self._lock.locked() or (
                self._current and self._current.status in ("running", "restarting")
            ):
                return {"error": "有操作正在进行中"}
            block_reason = self._update_script_block_reason()
            if block_reason:
                return {"error": block_reason}
            try:
                dirty_files = await self._dirty_worktree_files()
            except Exception as exc:
                return {"error": str(exc)}
            if dirty_files:
                return {
                    "error": (
                        "工作区存在未提交的本地改动，重启会加载无法由"
                        " commit 证明的代码；请先提交或手动处理"
                    ),
                    "dirty_files": dirty_files,
                }
            environment = await self._inspect_environment(force=True)
            if environment["repair_required"]:
                return {
                    "error": "当前部署状态不能安全地直接重启，请先执行修复",
                    "repair_required": True,
                    "repair_reasons": environment["repair_reasons"],
                }
            claimed = False
            try:
                await self._pause_dispatching()
                blockers = await self._get_blocking_tasks()
                if blockers:
                    self._resume_dispatching()
                    return {
                        "error": f"当前有 {len(blockers)} 个任务正在运行，请等待任务完成后再重启",
                        **self._blocker_payload(blockers),
                    }
                if self._claim_deployment_lease("restart") is None:
                    self._resume_dispatching()
                    return {"error": "另一个 CCM 进程正在操作此仓库"}
                claimed = True
                post_claim_blockers = await self._get_blocking_tasks()
                if post_claim_blockers:
                    return self._cancel_claim_for_new_blockers(
                        "重启",
                        post_claim_blockers,
                        preserve_incomplete=False,
                    )
                disk_commit = await self._disk_commit()
                state = UpdateState(
                    update_id=f"restart_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}",
                    status="running",
                    operation="restart",
                    old_commit=disk_commit,
                    new_commit=disk_commit,
                    started_at=datetime.now(timezone.utc).isoformat(),
                    steps=[StepInfo(name=name) for name in STEP_NAMES],
                )
                for step in state.steps[:-1]:
                    step.status = "skipped"
                    step.message = "仅重启服务"
                self._current = state
                self._inspection_cache = None
                self._update_deployment_lease(
                    status="running",
                    old_commit=disk_commit,
                    expected_commit=disk_commit,
                )
                asyncio.create_task(self._run_restart(state))
                return {"update_id": state.update_id, "status": "started"}
            except asyncio.CancelledError:
                self._finish_admission_and_resume(
                    claimed=claimed,
                    message="重启在交接前被取消",
                    incomplete=False,
                )
                raise
            except Exception:
                self._finish_admission_and_resume(
                    claimed=claimed,
                    message="重启准入失败",
                    incomplete=False,
                )
                raise

    async def rollback(
        self, confirm_database_restore: bool = False
    ) -> dict[str, Any]:
        """Manual rollback to previous version."""
        async with self._operation_lock:
            self._reconcile_external_terminal_status()
            rollback_state = self._current
            if (
                not rollback_state
                or not rollback_state.old_commit
                or not rollback_state.new_commit
                or rollback_state.old_commit == rollback_state.new_commit
                or rollback_state.status == "rolled_back"
            ):
                return {"error": "没有可回滚的更新记录"}
            if self._lock.locked() or rollback_state.status in (
                "running",
                "restarting",
            ):
                return {"error": "有操作正在进行中"}
            block_reason = self._update_script_block_reason()
            if block_reason:
                return {"error": block_reason}

            # Capture the exact rollback record while operation admission is
            # reserved. No concurrent start_update/rollback may replace
            # self._current between this validation and the shutdown decision.
            old_commit = rollback_state.old_commit
            backup_file = rollback_state.backup_file
            migration_state = rollback_state.database_migration_applied
            restore_database = migration_state is not False
            if restore_database and not confirm_database_restore:
                return {
                    "error": "回滚将把数据库恢复到更新前快照，更新后的数据会丢失",
                    "confirmation_required": True,
                    "database_restore_required": True,
                    "database_migration_applied": migration_state,
                }
            if restore_database and (
                not self._automatic_rollback_supported or not backup_file
            ):
                return {"error": "缺少可验证的 SQLite 备份，拒绝数据库回滚"}

            claimed = False
            try:
                await self._pause_dispatching()
                try:
                    active_tasks = await self._get_blocking_tasks()
                except Exception as exc:
                    self._resume_dispatching()
                    logger.exception("Unable to verify active tasks before rollback")
                    return {"error": f"无法确认当前任务状态，已取消回滚: {exc}"}
                if active_tasks:
                    self._resume_dispatching()
                    return {
                        "error": f"当前有 {len(active_tasks)} 个任务正在运行，请等待任务完成后再回滚",
                        **self._blocker_payload(active_tasks),
                    }
                if self._claim_deployment_lease(
                    "rollback",
                    allow_failed=True,
                    initial_state={
                        "old_commit": old_commit,
                        "new_commit": rollback_state.new_commit,
                        "expected_commit": old_commit,
                        "backup_file": backup_file,
                        "frontend_dist_backup": (
                            rollback_state.frontend_dist_backup
                        ),
                        "database_migration_required": (
                            rollback_state.database_migration_required
                        ),
                        "database_migration_applied": migration_state,
                        "deployment_incomplete": (
                            rollback_state.deployment_incomplete
                        ),
                    },
                ) is None:
                    self._resume_dispatching()
                    return {"error": "另一个 CCM 进程正在操作此仓库"}
                claimed = True
                post_claim_blockers = await self._get_blocking_tasks()
                if post_claim_blockers:
                    return self._cancel_claim_for_new_blockers(
                        "回滚",
                        post_claim_blockers,
                        preserve_incomplete=(
                            rollback_state.deployment_incomplete
                        ),
                    )

                async with self._lock:
                    await self._broadcast("step_update", step="rollback", status="running", message="正在回滚...")

                    # Never restore the SQLite file while this process still holds open
                    # connections — a later write/checkpoint through the live connection
                    # corrupts the restored DB (2026-07-16 test-env rollback incident).
                    # The script stops the service (systemctl or kill, per deployment)
                    # BEFORE touching the DB, then resets code and starts it again.
                    await self._broadcast("restarting", message="服务即将停止进行回滚，请等待自动重连...")
                    await asyncio.sleep(1)

                    def spawn_rollback() -> None:
                        # Identity validation documents the invariant even for
                        # future writers that might mutate _current outside the
                        # operation admission API.
                        if self._current is not rollback_state:
                            raise RuntimeError("rollback state changed during admission")
                        rollback_state.status = "restarting"
                        rollback_state.operation = "rollback"
                        rollback_state.deployment_incomplete = True
                        self._mark_deployment_handoff(
                            "rollback" if restore_database else "rollback_code"
                        )
                        self._write_status_file(
                            "restarting",
                            "正在停服回滚...",
                            old_commit=old_commit,
                            new_commit=rollback_state.new_commit,
                            operation="rollback",
                            owner_token=self._lease_token,
                            database_migration_required=(
                                rollback_state.database_migration_required
                            ),
                            database_migration_applied=migration_state,
                        )
                        self._spawn_update_script(
                            "rollback" if restore_database else "rollback_code",
                            old_commit,
                            backup_file if restore_database else "",
                            state=rollback_state,
                            restart_failure_policy="rollback",
                        )

                    blockers = await self._commit_shutdown_if_idle(spawn_rollback)
                    if blockers:
                        return self._cancel_claim_for_new_blockers(
                            "回滚",
                            blockers,
                            preserve_incomplete=(
                                rollback_state.deployment_incomplete
                            ),
                        )
                    return {"status": "rolling_back", "old_commit": old_commit}
            except asyncio.CancelledError:
                self._finish_admission_and_resume(
                    claimed=claimed,
                    message="回滚在交接前被取消",
                    incomplete=rollback_state.deployment_incomplete,
                )
                raise
            except Exception:
                self._finish_admission_and_resume(
                    claimed=claimed,
                    message="回滚启动失败",
                    incomplete=True,
                )
                raise

    # ---- Pipeline implementation ----

    async def _run_pipeline(
        self,
        state: UpdateState,
        skip_frontend_build: bool = False,
        force: bool = False,
        branch: str | None = None,
        channel: str = "main",
    ):
        async with self._lock:
            try:
                await self._pipeline_inner(state, skip_frontend_build, force, branch=branch, channel=channel)
            except Exception as e:
                state.status = "failed"
                state.error = str(e)
                state.deployment_incomplete = bool(state.new_commit)
                state.completed_at = datetime.now(timezone.utc).isoformat()
                self._write_status_file(
                    "failed",
                    str(e),
                    step=next(
                        (step.name for step in state.steps if step.status == "running"),
                        "",
                    ),
                    old_commit=state.old_commit,
                    new_commit=state.new_commit,
                    operation=state.operation,
                    database_migration_required=state.database_migration_required,
                    database_migration_applied=state.database_migration_applied,
                    deployment_incomplete=state.deployment_incomplete,
                )
                await self._broadcast("update_failed", message=str(e))
                logger.exception("Update pipeline failed")
            finally:
                if state.status != "restarting":
                    if state.status == "completed":
                        self._finish_deployment_claim(
                            "completed", "更新完成", incomplete=False
                        )
                    elif self._lease_token:
                        self._finish_deployment_claim(
                            "failed",
                            state.error or "更新未完整完成",
                            incomplete=state.deployment_incomplete,
                        )
                    self._resume_dispatching()

    async def _run_repair(
        self, state: UpdateState, *, skip_frontend_build: bool
    ) -> None:
        async with self._lock:
            try:
                await self._repair_inner(
                    state, skip_frontend_build=skip_frontend_build
                )
            except Exception as exc:
                state.status = "failed"
                state.error = str(exc)
                state.deployment_incomplete = True
                state.completed_at = datetime.now(timezone.utc).isoformat()
                self._write_status_file(
                    "failed",
                    str(exc),
                    old_commit=state.old_commit,
                    new_commit=state.new_commit,
                    operation="repair",
                    deployment_incomplete=True,
                    database_migration_required=state.database_migration_required,
                    database_migration_applied=state.database_migration_applied,
                )
                await self._broadcast("update_failed", message=str(exc))
                logger.exception("Repair pipeline failed")
            finally:
                if state.status != "restarting":
                    if self._lease_token:
                        self._finish_deployment_claim(
                            "failed",
                            state.error or "修复未完整完成",
                            incomplete=True,
                        )
                    self._resume_dispatching()

    async def _run_restart(self, state: UpdateState) -> None:
        async with self._lock:
            try:
                # Recheck after admission because the request returned before
                # this background task acquired the operation lock.  This does
                # Gitignored runtime data is irrelevant, but every staged,
                # unstaged, or untracked path visible to Git must block a
                # restart whose identity is represented by a commit.
                dirty_files = await self._dirty_worktree_files()
                if dirty_files:
                    await self._fail_step(
                        state.steps[9],
                        state,
                        "重启前检测到未提交的本地改动，已取消停服",
                    )
                    return
                await self._fast_restart_path(
                    state, restart_failure_policy="retry"
                )
            except Exception as exc:
                state.status = "failed"
                state.error = str(exc)
                state.deployment_incomplete = True
                state.completed_at = datetime.now(timezone.utc).isoformat()
                self._write_status_file(
                    "failed",
                    str(exc),
                    old_commit=state.old_commit,
                    new_commit=state.new_commit,
                    operation="restart",
                    deployment_incomplete=True,
                )
                self._finish_deployment_claim(
                    "failed", str(exc), incomplete=True
                )
            finally:
                if state.status != "restarting":
                    if self._lease_token:
                        self._finish_deployment_claim(
                            "failed",
                            state.error or "重启未启动",
                            incomplete=state.deployment_incomplete,
                        )
                    self._resume_dispatching()

    async def _adopt_same_commit_repair_semantics(
        self,
        state: UpdateState,
        step: StepInfo,
    ) -> bool:
        """Persist repair semantics before continuing a same-version deploy.

        Rolling a failed migration back while old/new commits are identical
        necessarily leaves the restored schema behind the checked-out code.
        The external worker therefore must receive ``operation=repair`` so its
        controlled restart remains maintenance-only.  Mark the in-memory state
        first so even a lease write failure is fail-closed.
        """
        state.operation = "repair"
        state.deployment_incomplete = True
        if self._update_deployment_lease(
            operation="repair",
            deployment_incomplete=True,
        ):
            return True
        await self._fail_step(
            step,
            state,
            "无法把同版本更新切换为可恢复的修复事务，拒绝继续",
        )
        return False

    async def _pipeline_inner(
        self,
        state: UpdateState,
        skip_frontend_build: bool,
        force: bool,
        branch: str | None = None,
        channel: str = "main",
    ):
        target_branch = branch or "main"
        remote = await self._resolve_remote(target_branch)
        has_new_migrations = False
        has_frontend_changes = False
        has_package_changes = False
        is_stable_downgrade = False

        # Step 1: check clean → git pull
        step = state.steps[0]
        await self._start_step(step)
        disk_commit = await self._disk_commit()
        state.old_commit = await self._deployment_base_commit(disk_commit)
        self._update_deployment_lease(old_commit=state.old_commit)

        # Updating a dirty checkout makes the deployed commit unknowable.
        # Refuse every non-ignored staged, unstaged, or untracked path.
        try:
            dirty_files = await self._dirty_worktree_files()
        except Exception as exc:
            await self._fail_step(
                step, state, f"无法确认 Git 工作区状态: {exc}"
            )
            return
        if dirty_files:
            await self._fail_step(
                step,
                state,
                "工作区存在未提交的本地改动，拒绝自动处理；请先提交或手动处理",
            )
            return

        if channel == "stable":
            stable_check = await self._check_stable_updates()
            protocol_ok = False
            protocol_error = stable_check.get("error", "无法解析正式版本")
            target_commit = stable_check.get("latest_commit", "")
            is_stable_downgrade = bool(stable_check.get("is_stable_downgrade"))
            state.target_version = stable_check.get("latest_version", "")
            self._update_deployment_lease(
                target_version=state.target_version,
            )
            if stable_check.get("stable_switch_blocked"):
                await self._fail_step(
                    step,
                    state,
                    stable_check.get("error")
                    or "当前数据库状态不允许自动切回 Stable",
                )
                return
            if target_commit:
                show = await self._run_cmd(["git", "show", f"{target_commit}:scripts/update_migrate.sh"], timeout=30)
                protocol = self._parse_update_script_protocol(show["stdout"])
                protocol_ok = show["returncode"] == 0 and protocol == UPDATE_SCRIPT_PROTOCOL_VERSION
                if not protocol_ok:
                    protocol_error = "正式版本的部署协议不兼容，拒绝更新"
        else:
            (
                protocol_ok,
                protocol_error,
                target_commit,
            ) = await self._fetch_and_validate_target_protocol(
                remote, target_branch, step
            )
        if not protocol_ok:
            await self._fail_step(step, state, protocol_error)
            return

        # Checkout target branch before pulling (keeps main clean when
        # updating to a feature branch for testing)
        current_branch = (await self._run_cmd(["git", "rev-parse", "--abbrev-ref", "HEAD"]))["stdout"].strip()
        if channel == "stable":
            checkout_result = await self._run_cmd(["git", "checkout", "--detach", target_commit], step=step)
            if checkout_result["returncode"] != 0:
                await self._fail_step(step, state, f"正式版本 checkout 失败: {checkout_result['stderr']}")
                return
        elif current_branch != target_branch:
            # Create or reset local branch to match remote
            checkout_result = await self._run_cmd(
                ["git", "checkout", "-B", target_branch, target_commit],
                step=step,
            )
            if checkout_result["returncode"] != 0:
                await self._fail_step(step, state, f"git checkout 失败: {checkout_result['stderr']}")
                return
            await self._broadcast("log_line", step="git_pull", log=f"已切换到分支 {target_branch}", status="running")

        else:
            result = await self._run_cmd(
                ["git", "merge", "--ff-only", target_commit],
                timeout=60,
                step=step,
            )
            if result["returncode"] != 0:
                await self._fail_step(
                    step,
                    state,
                    "目标分支无法 fast-forward 到已验证 commit，"
                    f"拒绝改写本地历史: {result['stderr']}",
                )
                return

        state.new_commit = (await self._run_cmd(["git", "rev-parse", "HEAD"]))["stdout"].strip()
        if target_commit and state.new_commit != target_commit:
            await self._fail_step(
                step,
                state,
                "checkout 后 HEAD 与已验证目标 commit 不一致，拒绝继续",
            )
            return
        self._update_deployment_lease(expected_commit=state.new_commit)

        # Never attempt an Alembic upgrade against a release whose schema is
        # older than the currently-running Main build.  Alembic's normal
        # ``upgrade head`` cannot downgrade safely; leave the checkout exactly
        # as we found it and fail closed with an actionable message.
        if channel == "stable" and is_stable_downgrade:
            target_database = await self._database_revision_status()
            if target_database["database_up_to_date"] is not True:
                await self._run_cmd(
                    ["git", "checkout", "--detach", disk_commit],
                    timeout=60,
                )
                await self._fail_step(
                    step,
                    state,
                    "当前测试版数据库包含正式版没有的迁移，不能自动切回 Stable；"
                    "请先备份并按降级方案处理数据库",
                )
                return

        same_commit = state.old_commit == state.new_commit
        if same_commit and not force:
            database = await self._database_revision_status()
            if database["database_up_to_date"] is not True:
                if not await self._adopt_same_commit_repair_semantics(
                    state, step
                ):
                    return
                step.message = "代码已是最新，继续修复依赖与数据库"
                await self._complete_step(step)
                await self._repair_inner(
                    state, skip_frontend_build=skip_frontend_build
                )
                return
            step.message = (
                "代码已是最新，服务可通过独立重启按钮重新加载"
            )
            await self._complete_step(step)
            state.status = "completed"
            state.completed_at = datetime.now(timezone.utc).isoformat()
            for s in state.steps[1:]:
                s.status = "skipped"
            await self._broadcast(
                "update_complete",
                message="代码和数据库均已是最新；如需重新加载服务请点击重启",
            )
            return

        if same_commit:
            # ``force=True`` intentionally bypasses the no-op fast return, but
            # it must not bypass same-version rollback semantics.  This also
            # makes later DB-revision-unknown failures retain the maintenance
            # fence instead of looking like a clean update.
            if not await self._adopt_same_commit_repair_semantics(
                state, step
            ):
                return

        await self._complete_step(step)

        # Step 2: detect changes
        step = state.steps[1]
        await self._start_step(step)

        diff_result = await self._run_cmd(
            ["git", "diff", "--name-only", f"{state.old_commit}..{state.new_commit}"]
        )
        changed_files = [f for f in diff_result["stdout"].strip().split("\n") if f.strip()]

        migration_files = [f for f in changed_files if f.startswith("alembic/versions/")]
        has_new_migrations = len(migration_files) > 0
        frontend_files = [f for f in changed_files if f.startswith("frontend/")]
        has_frontend_changes = len(frontend_files) > 0
        has_package_changes = "frontend/package.json" in changed_files

        step.result = {
            "has_new_migrations": has_new_migrations,
            "migration_count": len(migration_files),
            "has_frontend_changes": has_frontend_changes,
            "has_package_changes": has_package_changes,
            "total_files_changed": len(changed_files),
        }
        await self._complete_step(step)

        # Step 3 is completed only after the authoritative revision check below.
        # No database bytes have changed yet, so taking an online snapshot here
        # would only be overwritten by the stopped-service snapshot immediately
        # before Alembic runs.
        step = state.steps[2]
        try:
            state.frontend_dist_backup = await self._backup_frontend_dist()
        except Exception as exc:
            await self._fail_step(
                step, state, f"备份前端产物失败，拒绝继续部署: {exc}"
            )
            return
        self._update_deployment_lease(
            backup_file=state.backup_file,
            frontend_dist_backup=state.frontend_dist_backup,
        )

        # Step 4: uv sync
        step = state.steps[3]
        await self._start_step(step)
        result = await self._run_cmd(["uv", "sync"], timeout=300, step=step)
        if result["returncode"] != 0:
            await self._fail_and_maybe_rollback(
                step, state, f"uv sync 失败: {result['stderr']}"
            )
            return
        await self._complete_step(step)

        # Step 5: refresh_pty.sh
        step = state.steps[4]
        await self._start_step(step)
        pty_script = Path(self.project_dir) / "scripts" / "refresh_pty.sh"
        if pty_script.exists():
            result = await self._run_cmd(
                ["bash", str(pty_script)], timeout=120, step=step
            )
            if result["returncode"] != 0:
                await self._fail_and_maybe_rollback(
                    step, state, f"refresh_pty.sh 失败: {result['stderr']}"
                )
                return
            await self._complete_step(step)
        else:
            step.status = "skipped"
            step.message = "脚本不存在"
            await self._broadcast_step(step)

        # Step 6: npm install
        step = state.steps[5]
        if has_package_changes:
            await self._start_step(step)
            result = await self._run_cmd(
                ["npm", "install"],
                timeout=120,
                step=step,
                cwd=str(Path(self.project_dir) / "frontend"),
            )
            if result["returncode"] != 0:
                await self._fail_and_maybe_rollback(
                    step, state, f"npm install 失败: {result['stderr']}"
                )
                return
            await self._complete_step(step)
        else:
            step.status = "skipped"
            step.message = "package.json 未变更"
            await self._broadcast_step(step)

        # Step 7: frontend build
        step = state.steps[6]
        if skip_frontend_build or not has_frontend_changes:
            step.status = "skipped"
            step.message = "跳过" if skip_frontend_build else "前端无变更"
            await self._broadcast_step(step)
        else:
            await self._start_step(step)
            result = await self._run_cmd(
                ["npm", "run", "build"],
                timeout=300,
                step=step,
                cwd=str(Path(self.project_dir) / "frontend"),
            )
            if result["returncode"] != 0:
                await self._fail_and_maybe_rollback(
                    step, state, f"前端构建失败: {result['stderr']}"
                )
                return
            await self._complete_step(step)

        database = await self._database_revision_status()
        if database["database_up_to_date"] is None:
            await self._fail_and_maybe_rollback(
                state.steps[8],
                state,
                database["database_revision_error"] or "无法确认数据库 revision",
            )
            return
        has_new_migrations = database["database_up_to_date"] is False
        state.database_migration_required = has_new_migrations
        state.database_migration_applied = None if has_new_migrations else False
        self._update_deployment_lease(
            database_migration_required=has_new_migrations,
            database_migration_applied=state.database_migration_applied,
        )
        if has_new_migrations and not self._automatic_rollback_supported:
            await self._fail_and_maybe_rollback(
                state.steps[8],
                state,
                "检测到待执行迁移，但当前数据库不支持自动快照回滚；请先手动备份并部署",
            )
            return

        if has_new_migrations:
            await self._start_step(step)
            try:
                state.backup_file = self._reserve_database_backup()
                step.message = "已预留回滚快照路径，停服后生成权威快照"
                await self._complete_step(step)
            except Exception as exc:
                await self._fail_step(
                    step, state, f"无法准备数据库回滚快照: {exc}"
                )
                return
        else:
            step.status = "skipped"
            step.message = "数据库已是最新，无需生成回滚快照"
            await self._broadcast_step(step)
        self._update_deployment_lease(backup_file=state.backup_file)

        # Steps 8-10: migration path vs fast path
        active_tasks = await self._get_blocking_tasks()
        if active_tasks:
            step = state.steps[7]
            await self._start_step(step)
            await self._fail_step(
                step,
                state,
                f"更新期间启动了 {len(active_tasks)} 个任务，已取消重启；请等待任务完成后重试",
            )
            return
        if has_new_migrations:
            await self._migration_path(state)
        else:
            await self._fast_restart_path(state)

    async def _repair_inner(
        self, state: UpdateState, *, skip_frontend_build: bool
    ) -> None:
        try:
            dirty_files = await self._dirty_worktree_files()
        except Exception as exc:
            await self._fail_step(
                state.steps[0], state, f"无法确认 Git 工作区状态: {exc}"
            )
            return
        if dirty_files:
            await self._fail_step(
                state.steps[0],
                state,
                "修复开始前检测到未提交的本地改动，拒绝部署",
            )
            return
        for index in (0, 1):
            if state.steps[index].status == "pending":
                state.steps[index].status = "skipped"
                state.steps[index].message = "修复不修改 Git checkout"
                await self._broadcast_step(state.steps[index])

        backup_step = state.steps[2]

        try:
            state.frontend_dist_backup = await self._backup_frontend_dist()
        except Exception as exc:
            await self._fail_step(
                backup_step, state, f"备份前端产物失败: {exc}"
            )
            return
        self._update_deployment_lease(
            backup_file=state.backup_file,
            frontend_dist_backup=state.frontend_dist_backup,
        )

        dependency_step = state.steps[3]
        await self._start_step(dependency_step)
        result = await self._run_cmd(
            ["uv", "sync"], timeout=300, step=dependency_step
        )
        if result["returncode"] != 0:
            await self._fail_step(
                dependency_step, state, f"uv sync 失败: {result['stderr']}"
            )
            return
        await self._complete_step(dependency_step)

        pty_step = state.steps[4]
        pty_script = Path(self.project_dir) / "scripts" / "refresh_pty.sh"
        if pty_script.exists():
            await self._start_step(pty_step)
            result = await self._run_cmd(
                ["bash", str(pty_script)], timeout=120, step=pty_step
            )
            if result["returncode"] != 0:
                await self._fail_step(
                    pty_step,
                    state,
                    f"refresh_pty.sh 失败: {result['stderr']}",
                )
                return
            await self._complete_step(pty_step)
        else:
            pty_step.status = "skipped"
            pty_step.message = "脚本不存在"
            await self._broadcast_step(pty_step)

        npm_step = state.steps[5]
        await self._start_step(npm_step)
        result = await self._run_cmd(
            ["npm", "install"],
            timeout=120,
            step=npm_step,
            cwd=str(Path(self.project_dir) / "frontend"),
        )
        if result["returncode"] != 0:
            await self._fail_step(
                npm_step, state, f"npm install 失败: {result['stderr']}"
            )
            return
        await self._complete_step(npm_step)

        frontend_step = state.steps[6]
        if skip_frontend_build:
            frontend_step.status = "skipped"
            frontend_step.message = "管理员显式跳过前端构建"
            await self._broadcast_step(frontend_step)
        else:
            await self._start_step(frontend_step)
            result = await self._run_cmd(
                ["npm", "run", "build"],
                timeout=300,
                step=frontend_step,
                cwd=str(Path(self.project_dir) / "frontend"),
            )
            if result["returncode"] != 0:
                await self._fail_step(
                    frontend_step,
                    state,
                    f"前端构建失败: {result['stderr']}",
                )
                return
            await self._complete_step(frontend_step)

        database = await self._database_revision_status()
        if database["database_up_to_date"] is None:
            await self._fail_step(
                state.steps[8],
                state,
                database["database_revision_error"] or "无法确认数据库 revision",
            )
            return
        state.database_migration_required = (
            database["database_up_to_date"] is False
        )
        state.database_migration_applied = (
            None if state.database_migration_required else False
        )
        self._update_deployment_lease(
            database_migration_required=state.database_migration_required,
            database_migration_applied=state.database_migration_applied,
        )
        if state.database_migration_required:
            if not self._automatic_rollback_supported:
                await self._fail_step(
                    state.steps[8],
                    state,
                    "当前数据库无法由 CCM 自动快照回滚，拒绝自动迁移",
                )
                return
            await self._start_step(backup_step)
            try:
                state.backup_file = self._reserve_database_backup()
                backup_step.message = (
                    "已预留回滚快照路径，停服后生成权威快照"
                )
                await self._complete_step(backup_step)
            except Exception as exc:
                await self._fail_step(
                    backup_step,
                    state,
                    f"无法准备数据库回滚快照: {exc}",
                )
                return
            self._update_deployment_lease(backup_file=state.backup_file)
            await self._migration_path(state)
        else:
            backup_step.status = "skipped"
            backup_step.message = "数据库已是最新，无需生成回滚快照"
            await self._broadcast_step(backup_step)
            await self._fast_restart_path(state)

    async def _fail_and_maybe_rollback(
        self, step: StepInfo, state: UpdateState, message: str
    ) -> None:
        await self._fail_step(step, state, message)
        if (
            state.operation != "update"
            or not state.old_commit
            or not state.new_commit
            or state.old_commit == state.new_commit
        ):
            return

        async def broadcast_rollback() -> None:
            await self._broadcast(
                "restarting",
                message="部署准备失败，正在恢复更新前代码和前端产物...",
            )

        await broadcast_rollback()

        def spawn_code_rollback() -> None:
            state.status = "restarting"
            state.deployment_incomplete = True
            state.database_migration_applied = False
            self._mark_deployment_handoff("rollback_code")
            self._write_status_file(
                "restarting",
                message,
                step=step.name,
                old_commit=state.old_commit,
                new_commit=state.new_commit,
                operation=state.operation,
                owner_token=self._lease_token,
                deployment_incomplete=True,
                database_migration_required=state.database_migration_required,
                database_migration_applied=False,
            )
            self._spawn_update_script(
                "rollback_code",
                state.old_commit,
                "",
                state=state,
                restart_failure_policy="rollback",
            )

        blockers = await self._commit_shutdown_if_idle(spawn_code_rollback)
        if blockers:
            state.status = "failed"
            state.error = (
                f"{message}；恢复前出现 {len(blockers)} 个待处理任务，"
                "请执行修复"
            )

    async def _migration_path(self, state: UpdateState) -> bool:
        """Has new migrations: launch external script that survives our own stop (steps 8-10)."""
        step8 = state.steps[7]
        step9 = state.steps[8]
        step10 = state.steps[9]

        step8.status = "running"
        step8.started_at = datetime.now(timezone.utc).isoformat()
        step9.message = "由外部脚本执行"
        step10.message = "由外部脚本执行"

        await self._broadcast("step_update", step="stop_service", status="running",
                              message="即将停服进行数据库迁移...")
        await self._broadcast("restarting", message="服务即将停止进行迁移，请等待自动重连...")
        await asyncio.sleep(1)

        def spawn_migration() -> None:
            state.status = "restarting"
            state.deployment_incomplete = True
            state.database_migration_required = True
            state.database_migration_applied = None
            self._mark_deployment_handoff("migrate")
            self._write_status_file(
                "restarting",
                "正在停服迁移...",
                step="stop_service",
                old_commit=state.old_commit,
                new_commit=state.new_commit,
                operation=state.operation,
                owner_token=self._lease_token,
                deployment_incomplete=True,
                database_migration_required=True,
                database_migration_applied=None,
            )
            self._spawn_update_script(
                "migrate",
                state.old_commit,
                state.backup_file,
                state=state,
                restart_failure_policy="rollback",
            )

        blockers = await self._commit_shutdown_if_idle(spawn_migration)
        if blockers:
            await self._fail_step(
                step8,
                state,
                f"停服前出现了 {len(blockers)} 个待处理任务，已取消重启；请等待任务完成后重试",
            )
            return False
        return True

    def _spawn_update_script(
        self,
        mode: str,
        old_commit: str,
        backup_file: str,
        *,
        state: UpdateState | None = None,
        restart_failure_policy: str = "rollback",
    ):
        """Launch update_migrate.sh so it survives this service being stopped."""
        token_prefix = (self._lease_token or uuid.uuid4().hex)[:8]
        run_dir = Path(
            tempfile.mkdtemp(
                prefix=f"ccm-update-run-{self.port}-{token_prefix}-",
                dir="/tmp",
            )
        )
        os.chmod(run_dir, 0o700)
        script = run_dir / "update_migrate.sh"
        temporary_script = run_dir / ".update_migrate.sh.tmp"
        try:
            with self._trusted_update_script_lock:
                block_reason = self._update_script_block_reason()
                if block_reason:
                    raise RuntimeError(block_reason)
                runtime = self._trusted_update_runtime
                if runtime is None:
                    raise RuntimeError("更新脚本专用运行目录尚未初始化")
                runtime.copy_snapshot_to(temporary_script, mode=0o700)
                os.replace(temporary_script, script)
                directory_fd = os.open(run_dir, os.O_DIRECTORY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
                if not self._update_deployment_lease(
                    run_copy_dir=str(run_dir)
                ):
                    raise RuntimeError("无法把部署脚本副本绑定到当前租约")
        except Exception:
            shutil.rmtree(run_dir, ignore_errors=True)
            raise
        log_file = f"/tmp/ccm-update-migrate-{self.port}.log"

        env = os.environ.copy()
        for tool_path in self._tools.values():
            tool_dir = str(Path(tool_path).parent)
            if tool_dir not in env.get("PATH", ""):
                env["PATH"] = tool_dir + ":" + env.get("PATH", "")

        scope = self._systemd_scope()
        managed = scope is not None
        script_argv = [
            self._tools["bash"], str(script),
            self.project_dir,
            old_commit,
            backup_file,
            str(self.port),
            str(self.db_path),
            # "-" tells the script to stop/start via kill/respawn instead of
            # systemctl (bare-uvicorn deployments)
            self._service_name if managed else "-",
            mode,
            str(os.getpid()),
            sys.executable,
            scope or "auto",
            state.frontend_dist_backup if state else "",
            (
                "true"
                if state and state.database_migration_required
                else "false"
            ),
            (
                "null"
                if state and state.database_migration_applied is None
                else (
                    "true"
                    if state and state.database_migration_applied
                    else "false"
                )
            ),
            str(self._lease_file),
            self._lease_token or "",
            restart_failure_policy,
            state.operation if state else mode,
            str(run_dir),
        ]

        if managed:
            # start_new_session only escapes the process group, NOT the service's
            # cgroup — `systemctl stop` kills the whole cgroup including the script,
            # leaving the service stopped with nobody left to start it again.
            # systemd-run puts the script in its own transient unit (own cgroup).
            transient_options = ["--collect"]
            if scope == "system":
                transient_options.extend(
                    [
                        f"--uid={os.getuid()}",
                        f"--gid={os.getgid()}",
                        f"--setenv=HOME={Path.home()}",
                    ]
                )
            unit_suffix = (
                f"-{self._lease_token[:8]}" if self._lease_token else ""
            )
            try:
                launcher = subprocess.Popen(
                    self._systemd_run_cmd(scope) + transient_options + [
                        f"--unit=ccm-update-{self.port}{unit_suffix}",
                        # transient units do NOT inherit our cwd — without this the
                        # script's git/uv/alembic would run from systemd's default dir
                        f"--working-directory={self.project_dir}",
                        f"--setenv=PATH={env['PATH']}",
                        "--property=StandardOutput=journal",
                        "--property=StandardError=journal",
                    ] + script_argv,
                )
            except Exception:
                shutil.rmtree(run_dir, ignore_errors=True)
                raise
            # `systemd-run` exit/timeout is not proof that the transient unit
            # was not created: the D-Bus request may already be committed.
            # Keep the lease/fence active and let the worker ACK it with its
            # PID.  The provisional deadline reconciler is the only path that
            # may later prove no worker took ownership.
            try:
                result = launcher.wait(timeout=15)
            except subprocess.TimeoutExpired:
                logger.warning(
                    "systemd-run launch acknowledgement timed out; "
                    "keeping deployment lease active"
                )
                threading.Thread(
                    target=launcher.wait,
                    name=f"ccm-update-launcher-{self.port}",
                    daemon=True,
                ).start()
                return
            if isinstance(result, int) and result != 0:
                logger.error(
                    "systemd-run returned exit=%s; outcome is ambiguous, "
                    "keeping deployment lease active until ACK deadline",
                    result,
                )
                return
        else:
            flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT
            flags |= getattr(os, "O_NOFOLLOW", 0)
            try:
                log_fd = os.open(log_file, flags, 0o600)
            except Exception:
                shutil.rmtree(run_dir, ignore_errors=True)
                raise
            metadata = os.fstat(log_fd)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != os.getuid()
                or metadata.st_mode & 0o022
                or metadata.st_nlink != 1
            ):
                os.close(log_fd)
                shutil.rmtree(run_dir, ignore_errors=True)
                raise RuntimeError("部署日志路径不是当前服务用户拥有的普通文件")
            log_stream = os.fdopen(log_fd, "a", closefd=True)
            try:
                try:
                    subprocess.Popen(
                        script_argv,
                        stdout=log_stream,
                        stderr=subprocess.STDOUT,
                        start_new_session=True,
                        env=env,
                    )
                except Exception:
                    shutil.rmtree(run_dir, ignore_errors=True)
                    raise
            finally:
                log_stream.close()

    async def _fast_restart_path(
        self,
        state: UpdateState,
        *,
        restart_failure_policy: str = "rollback",
    ) -> bool:
        """No migration: skip steps 8-9, do nohup restart for step 10."""
        state.steps[7].status = "skipped"
        state.steps[7].message = "无新迁移"
        state.steps[8].status = "skipped"
        state.steps[8].message = "无新迁移"
        await self._broadcast_step(state.steps[7])
        await self._broadcast_step(state.steps[8])

        step10 = state.steps[9]
        step10.status = "running"
        step10.started_at = datetime.now(timezone.utc).isoformat()

        await self._broadcast("restarting", message="服务即将重启，请等待自动重连...")
        await asyncio.sleep(1)

        def restart_service() -> None:
            state.status = "restarting"
            state.deployment_incomplete = True
            self._mark_deployment_handoff("restart")
            self._write_status_file(
                "restarting", "正在重启服务...",
                old_commit=state.old_commit,
                new_commit=state.new_commit,
                backup_file=state.backup_file,
                frontend_dist_backup=state.frontend_dist_backup,
                operation=state.operation,
                owner_token=self._lease_token,
                deployment_incomplete=True,
                database_migration_required=state.database_migration_required,
                database_migration_applied=state.database_migration_applied,
            )
            self._spawn_update_script(
                "restart",
                state.old_commit,
                state.backup_file,
                state=state,
                restart_failure_policy=restart_failure_policy,
            )

        blockers = await self._commit_shutdown_if_idle(restart_service)
        if blockers:
            await self._fail_step(
                step10,
                state,
                f"重启前出现了 {len(blockers)} 个待处理任务，已取消重启；请等待任务完成后重试",
            )
            return False
        return True

    # ---- Helpers ----

    def _cgroup_text(self) -> str:
        return Path("/proc/self/cgroup").read_text()

    def _normalized_service_name(self) -> str:
        name = self._service_name
        if not name.endswith(".service"):
            name += ".service"
        return name

    def _systemd_scope(self) -> str | None:
        """Return user/system only if THIS process is in its service cgroup.

        `systemctl is-active` is not enough: a manually launched uvicorn can
        coexist with an active (but port-less) systemd unit — it would then
        believe it is systemd-managed and stop/start the *other* instance
        (2026-07-16 test-env orphan incident). /proc/self/cgroup answers for
        this very process; on non-Linux it raises → False → fallback path.
        """
        try:
            text = self._cgroup_text()
        except Exception:
            return None
        if f"/{self._normalized_service_name()}" not in text:
            return None
        configured = (self._service_scope or "auto").strip().lower()
        if configured in {"user", "system"}:
            return configured
        if "/system.slice/" in text:
            return "system"
        if "/user.slice/" in text:
            return "user"
        return "user"

    def _is_managed_by_systemd(self) -> bool:
        return self._systemd_scope() is not None

    def _systemd_run_cmd(self, scope: str | None) -> list[str]:
        if scope == "system":
            return [self._tools["sudo"], "-n", self._tools["systemd-run"]]
        return [self._tools["systemd-run"], "--user"]

    def _reserve_database_backup(self) -> str:
        if not self._automatic_rollback_supported:
            raise RuntimeError("当前数据库不是可安全自动恢复的文件型 SQLite")
        db_path = self.db_path
        if not db_path.exists():
            raise FileNotFoundError(f"数据库文件不存在: {db_path}")

        backup_dir = self._secure_backup_dir()

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        backup_path = backup_dir / f"claude_manager.db.bak.{timestamp}"
        if backup_path.exists():
            raise RuntimeError("数据库回滚快照路径已存在")
        # The external worker creates the snapshot atomically after stopping all
        # writers.  Make room first so the newly-created snapshot leaves at most
        # MAX_BACKUPS recovery points.
        self._cleanup_old_backups(backup_dir, keep=MAX_BACKUPS - 1)
        return str(backup_path)

    async def _backup_frontend_dist(self) -> str:
        source = Path(self.project_dir) / "frontend" / "dist"
        backup_dir = self._secure_backup_dir()
        destination = backup_dir / (
            "frontend-dist.bak."
            + datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        )

        def copy_snapshot() -> None:
            if source.exists():
                shutil.copytree(source, destination, symlinks=True)
            else:
                destination.mkdir()
                (destination / ".ccm-dist-absent").write_text(
                    "frontend/dist did not exist before deployment\n"
                )

        await asyncio.get_event_loop().run_in_executor(None, copy_snapshot)
        snapshots = sorted(
            backup_dir.glob("frontend-dist.bak.*"),
            key=lambda item: item.stat().st_mtime,
        )
        while len(snapshots) > MAX_BACKUPS:
            shutil.rmtree(snapshots.pop(0), ignore_errors=True)
        return str(destination)

    def _cleanup_old_backups(
        self, backup_dir: Path, *, keep: int = MAX_BACKUPS
    ) -> None:
        backups = sorted(backup_dir.glob("claude_manager.db.bak.*"), key=lambda p: p.stat().st_mtime)
        while len(backups) > keep:
            old = backups.pop(0)
            old.unlink()
            logger.info("Removed old backup: %s", old.name)

    def _resolve_cmd(self, cmd: list[str]) -> list[str]:
        """Replace the first element with its resolved absolute path."""
        if cmd and cmd[0] in self._tools:
            return [self._tools[cmd[0]]] + cmd[1:]
        return cmd

    async def _run_cmd(
        self,
        cmd: list[str],
        timeout: int = 60,
        step: StepInfo | None = None,
        cwd: str | None = None,
    ) -> dict[str, Any]:
        cmd = self._resolve_cmd(cmd)
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=cwd or self.project_dir,
                start_new_session=(os.name == "posix"),
            )

            async def read_stream(stream, is_stderr=False):
                lines = []
                while True:
                    line = await stream.readline()
                    if not line:
                        break
                    text = line.decode("utf-8", errors="replace").rstrip()
                    lines.append(text)
                    if step:
                        await self._broadcast(
                            "log_line",
                            step=step.name,
                            log=text,
                            status="running",
                        )
                return "\n".join(lines)

            try:
                stdout_task = asyncio.create_task(read_stream(proc.stdout))
                stderr_task = asyncio.create_task(read_stream(proc.stderr, True))

                async def settle_process() -> None:
                    await proc.wait()
                    await asyncio.gather(
                        stdout_task,
                        stderr_task,
                        return_exceptions=True,
                    )

                await asyncio.wait_for(proc.wait(), timeout=timeout)
                stdout = await stdout_task
                stderr = await stderr_task
            except asyncio.TimeoutError:
                try:
                    if os.name == "posix":
                        os.killpg(proc.pid, signal.SIGKILL)
                    else:
                        proc.kill()
                except ProcessLookupError:
                    pass
                await finish_awaitable(settle_process())
                return {"returncode": -1, "stdout": "", "stderr": f"命令超时 ({timeout}s)"}
            except asyncio.CancelledError:
                try:
                    if os.name == "posix":
                        os.killpg(proc.pid, signal.SIGKILL)
                    else:
                        proc.kill()
                except ProcessLookupError:
                    pass
                operation, _ = await settle_awaitable(settle_process())
                operation.result()
                raise

            return {
                "returncode": proc.returncode,
                "stdout": stdout,
                "stderr": stderr,
            }
        except FileNotFoundError:
            return {"returncode": -1, "stdout": "", "stderr": f"命令不存在: {cmd[0]}"}

    async def _start_step(self, step: StepInfo):
        step.status = "running"
        step.started_at = datetime.now(timezone.utc).isoformat()
        label = STEP_LABELS.get(step.name, step.name)
        await self._broadcast("step_update", step=step.name, status="running", message=f"正在{label}...")

    async def _complete_step(self, step: StepInfo):
        step.status = "completed"
        if step.started_at:
            started = datetime.fromisoformat(step.started_at)
            step.duration_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
        await self._broadcast_step(step)

    async def _fail_step(self, step: StepInfo, state: UpdateState, message: str):
        step.status = "failed"
        step.message = message
        if step.started_at:
            started = datetime.fromisoformat(step.started_at)
            step.duration_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
        state.status = "failed"
        state.error = message
        state.deployment_incomplete = bool(
            state.new_commit
            and state.old_commit
            and state.new_commit != state.old_commit
        ) or state.operation == "repair"
        state.completed_at = datetime.now(timezone.utc).isoformat()
        self._write_status_file(
            "failed",
            message,
            step=step.name,
            old_commit=state.old_commit,
            new_commit=state.new_commit,
            backup_file=state.backup_file,
            frontend_dist_backup=state.frontend_dist_backup,
            operation=state.operation,
            owner_token=self._lease_token,
            deployment_incomplete=state.deployment_incomplete,
            database_migration_required=state.database_migration_required,
            database_migration_applied=state.database_migration_applied,
        )
        await self._broadcast("update_failed", step=step.name, message=message)

    async def _broadcast_step(self, step: StepInfo):
        await self._broadcast(
            "step_update",
            step=step.name,
            status=step.status,
            message=step.message or "",
            duration_ms=step.duration_ms,
            result=step.result,
        )

    async def _broadcast(self, event: str, **kwargs):
        data = {"event": event}
        if self._current:
            data["update_id"] = self._current.update_id
        data.update(kwargs)
        try:
            await self.broadcaster.broadcast(WS_CHANNEL, data)
        except Exception:
            logger.debug("broadcast failed", exc_info=True)

    def _write_status_file(self, status: str, message: str, **extra):
        data = {
            "status": status,
            "message": message,
            "port": self.port,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if self._current:
            data.setdefault("update_id", self._current.update_id)
            data.setdefault("update_channel", self._current.update_channel)
            data.setdefault("target_version", self._current.target_version)
        data.update(extra)
        try:
            self._atomic_write_json(self._status_file, data)
            self._atomic_write_json(self._journal_file, data)
        except Exception:
            logger.exception("Failed to write status file")
