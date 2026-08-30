"""Pressure-triggered cleanup for CCM-owned temporary artifacts.

The cleaner never sweeps arbitrary files from ``/tmp``. It only handles old,
uniquely named artifacts created by CCM itself.
"""

from __future__ import annotations

import asyncio
import errno
import logging
import os
import re
import stat
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable, Iterator

from backend.services.cancellation import await_task_completion

logger = logging.getLogger(__name__)

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows is not a supported deployment
    fcntl = None


# Every admitted name is unique for its producing task/session/temporary file.
# Fixed names such as ccm_mcp_<task>.json are intentionally excluded because a
# later turn can reuse them.
_FILE_ALLOWLIST = tuple(
    re.compile(pattern)
    for pattern in (
        r"ccm-skills(?:-\d+)?-[A-Za-z0-9_.-]+\.md",
        r"ccm-user-skills-\d+-[A-Za-z0-9_.-]+\.md",
        r"ccm_sub_agent_\d+\.log",
        r"ccm-docker-claude-\d+-[0-9a-f]+\.sh",
        r"ccm-ssh-download-[^/]+",
        r"discussion_\d+(?:_outputs)?_[A-Za-z0-9_.-]+\.md",
        r"\.ccm-reap-[0-9a-f]{32}",
    )
)


@dataclass(frozen=True)
class TmpCleanupReport:
    reason: str
    triggered: bool
    before_usage_ratio: float | None
    before_inode_ratio: float | None
    after_usage_ratio: float | None
    after_inode_ratio: float | None
    removed_count: int = 0
    removed_bytes: int = 0
    removed_entries: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()
    skipped_reason: str | None = None


@dataclass(frozen=True)
class _Usage:
    bytes_ratio: float
    inode_ratio: float | None


@dataclass(frozen=True)
class _DiskCapacity:
    total: int
    used: int
    free: int


@dataclass(frozen=True)
class _Candidate:
    path: Path
    device: int
    inode: int
    mode: int
    size: int
    newest_mtime: float


class TmpSpaceManager:
    """Check /tmp pressure and reap only stale CCM allow-list entries."""

    def __init__(
        self,
        *,
        root: str | os.PathLike[str] = "/tmp",
        enabled: bool = True,
        trigger_ratio: float = 0.80,
        min_age_seconds: float = 6 * 3600,
        interval_seconds: float = 3 * 3600,
        lock_wait_seconds: float = 10,
        lock_path: str | os.PathLike[str] | None = None,
        disk_usage_reader: Callable | None = None,
        inode_usage_reader: Callable[[str | os.PathLike[str]], float | None]
        | None = None,
        wall_clock: Callable[[], float] = time.time,
    ):
        if not 0 < trigger_ratio <= 1:
            raise ValueError("trigger_ratio must be in (0, 1]")
        if min_age_seconds < 0:
            raise ValueError("min_age_seconds must be non-negative")
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        if lock_wait_seconds < 0:
            raise ValueError("lock_wait_seconds must be non-negative")

        self.root = Path(root)
        self.enabled = bool(enabled)
        self.trigger_ratio = float(trigger_ratio)
        self.min_age_seconds = float(min_age_seconds)
        self.interval_seconds = float(interval_seconds)
        self.lock_wait_seconds = float(lock_wait_seconds)
        self.lock_path = Path(
            lock_path
            or Path("~/.cache/ccm/tmp-pressure-cleanup.lock").expanduser()
        )
        self._disk_usage_reader = (
            disk_usage_reader or self._read_available_disk_usage
        )
        self._inode_usage_reader = (
            inode_usage_reader or self._read_inode_usage_ratio
        )
        self._wall_clock = wall_clock
        self._inflight_lock = asyncio.Lock()
        self._inflight_check: asyncio.Task | None = None

    async def ensure_capacity(self, *, reason: str) -> TmpCleanupReport:
        """Run one single-flight check without blocking the event loop."""

        if not self.enabled:
            return TmpCleanupReport(
                reason=reason,
                triggered=False,
                before_usage_ratio=None,
                before_inode_ratio=None,
                after_usage_ratio=None,
                after_inode_ratio=None,
                skipped_reason="disabled",
            )

        async with self._inflight_lock:
            operation = self._inflight_check
            if operation is None or operation.done():
                operation = asyncio.create_task(
                    asyncio.to_thread(self._check_and_cleanup, reason),
                    name="ccm-tmp-pressure-check",
                )
                self._inflight_check = operation

        report = await self._await_check_despite_cancellation(operation)
        if report.reason != reason:
            report = replace(report, reason=reason)
        return report

    @staticmethod
    async def _await_check_despite_cancellation(
        operation: asyncio.Task,
    ) -> TmpCleanupReport:
        """Wait for an in-flight rename/unlink even if the caller is cancelled."""

        cancellation = await await_task_completion(operation)

        try:
            report = operation.result()
        except BaseException:
            if cancellation is not None:
                raise cancellation
            raise
        if cancellation is not None:
            raise cancellation
        return report

    def start_periodic(self) -> asyncio.Task | None:
        if not self.enabled:
            return None

        async def _loop() -> None:
            while True:
                await asyncio.sleep(self.interval_seconds)
                try:
                    await self.ensure_capacity(reason="periodic")
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Unexpected /tmp pressure watchdog error")

        return asyncio.create_task(_loop(), name="ccm-tmp-pressure-watchdog")

    def _check_and_cleanup(self, reason: str) -> TmpCleanupReport:
        try:
            with self._cross_process_lock() as acquired:
                if not acquired:
                    return TmpCleanupReport(
                        reason=reason,
                        triggered=False,
                        before_usage_ratio=None,
                        before_inode_ratio=None,
                        after_usage_ratio=None,
                        after_inode_ratio=None,
                        skipped_reason="cross_process_cleanup_busy",
                    )
                return self._check_and_cleanup_locked(reason)
        except Exception as exc:
            logger.exception("Could not check temporary filesystem pressure")
            return TmpCleanupReport(
                reason=reason,
                triggered=False,
                before_usage_ratio=None,
                before_inode_ratio=None,
                after_usage_ratio=None,
                after_inode_ratio=None,
                errors=(f"{type(exc).__name__}: {exc}",),
                skipped_reason="check_failed",
            )

    def _check_and_cleanup_locked(self, reason: str) -> TmpCleanupReport:
        root_metadata = self.root.lstat()
        if not stat.S_ISDIR(root_metadata.st_mode):
            raise RuntimeError(f"temporary root is not a real directory: {self.root}")

        before = self._read_usage()
        if not self._is_triggered(before):
            return TmpCleanupReport(
                reason=reason,
                triggered=False,
                before_usage_ratio=before.bytes_ratio,
                before_inode_ratio=before.inode_ratio,
                after_usage_ratio=before.bytes_ratio,
                after_inode_ratio=before.inode_ratio,
            )

        logger.warning(
            "Temporary filesystem reached %.1f%% bytes / %s inodes; "
            "starting scoped CCM cleanup (reason=%s)",
            before.bytes_ratio * 100,
            (
                f"{before.inode_ratio * 100:.1f}%"
                if before.inode_ratio is not None
                else "unknown"
            ),
            reason,
        )

        candidates, scan_errors = self._scan_candidates(
            root_device=root_metadata.st_dev
        )
        candidates.sort(
            key=lambda item: (-item.size, item.newest_mtime, item.path.name)
        )

        removed: list[str] = []
        removed_bytes = 0
        errors = list(scan_errors)
        current = before
        for candidate in candidates:
            try:
                deleted_size = self._delete_candidate(candidate)
            except Exception as exc:
                errors.append(
                    f"{candidate.path.name}: {type(exc).__name__}: {exc}"
                )
                logger.warning(
                    "Could not remove stale CCM temp artifact %s: %s",
                    candidate.path,
                    exc,
                )
                continue
            if deleted_size is None:
                continue
            removed.append(candidate.path.name)
            removed_bytes += deleted_size
            current = self._read_usage()

        after = current
        if self._is_triggered(after):
            logger.warning(
                "Temporary filesystem remains at %.1f%% bytes / %s inodes "
                "after scoped cleanup; unknown and recent files were preserved",
                after.bytes_ratio * 100,
                (
                    f"{after.inode_ratio * 100:.1f}%"
                    if after.inode_ratio is not None
                    else "unknown"
                ),
            )
        else:
            logger.info(
                "Temporary cleanup removed %d item(s), about %d bytes; "
                "usage is now %.1f%%",
                len(removed),
                removed_bytes,
                after.bytes_ratio * 100,
            )

        return TmpCleanupReport(
            reason=reason,
            triggered=True,
            before_usage_ratio=before.bytes_ratio,
            before_inode_ratio=before.inode_ratio,
            after_usage_ratio=after.bytes_ratio,
            after_inode_ratio=after.inode_ratio,
            removed_count=len(removed),
            removed_bytes=removed_bytes,
            removed_entries=tuple(removed),
            errors=tuple(errors),
        )

    def _read_usage(self) -> _Usage:
        usage = self._disk_usage_reader(self.root)
        total = int(usage.total)
        used = int(usage.used)
        bytes_ratio = (used / total) if total > 0 else 0.0
        inode_ratio = self._inode_usage_reader(self.root)
        return _Usage(
            bytes_ratio=max(0.0, min(1.0, bytes_ratio)),
            inode_ratio=(
                max(0.0, min(1.0, float(inode_ratio)))
                if inode_ratio is not None
                else None
            ),
        )

    @staticmethod
    def _read_available_disk_usage(
        path: str | os.PathLike[str],
    ) -> _DiskCapacity:
        values = os.statvfs(path)
        block_size = values.f_frsize or values.f_bsize
        total = max(0, values.f_blocks * block_size)
        available = max(0, values.f_bavail * block_size)
        used = max(0, total - available)
        return _DiskCapacity(total=total, used=used, free=available)

    @staticmethod
    def _read_inode_usage_ratio(path: str | os.PathLike[str]) -> float | None:
        values = os.statvfs(path)
        if values.f_files <= 0:
            return None
        available = max(0, values.f_favail)
        return 1.0 - min(1.0, available / values.f_files)

    def _is_triggered(self, usage: _Usage) -> bool:
        return usage.bytes_ratio >= self.trigger_ratio or (
            usage.inode_ratio is not None
            and usage.inode_ratio >= self.trigger_ratio
        )

    def _scan_candidates(
        self,
        *,
        root_device: int,
    ) -> tuple[list[_Candidate], list[str]]:
        candidates: list[_Candidate] = []
        errors: list[str] = []
        now = self._wall_clock()
        uid = os.geteuid()

        try:
            entries = list(os.scandir(self.root))
        except OSError as exc:
            return [], [f"scan root: {type(exc).__name__}: {exc}"]

        for entry in entries:
            path = Path(entry.path)
            try:
                metadata = entry.stat(follow_symlinks=False)
                if metadata.st_uid != uid or metadata.st_dev != root_device:
                    continue
                if stat.S_ISLNK(metadata.st_mode):
                    continue

                if not (
                    stat.S_ISREG(metadata.st_mode)
                    and any(
                        rule.fullmatch(entry.name)
                        for rule in _FILE_ALLOWLIST
                    )
                ):
                    continue

                size = metadata.st_size
                newest_mtime = metadata.st_mtime
                if now - newest_mtime < self.min_age_seconds:
                    continue
                candidates.append(
                    _Candidate(
                        path=path,
                        device=metadata.st_dev,
                        inode=metadata.st_ino,
                        mode=metadata.st_mode,
                        size=size,
                        newest_mtime=newest_mtime,
                    )
                )
            except FileNotFoundError:
                continue
            except OSError as exc:
                errors.append(f"{entry.name}: {type(exc).__name__}: {exc}")
        return candidates, errors

    def _delete_candidate(self, candidate: _Candidate) -> int | None:
        try:
            current = candidate.path.lstat()
        except FileNotFoundError:
            return None
        expected = (
            candidate.device,
            candidate.inode,
            stat.S_IFMT(candidate.mode),
        )
        identity = (
            current.st_dev,
            current.st_ino,
            stat.S_IFMT(current.st_mode),
        )
        if (
            identity != expected
            or current.st_uid != os.geteuid()
            or stat.S_ISLNK(current.st_mode)
        ):
            return None

        if not stat.S_ISREG(current.st_mode):
            return None
        current_size = current.st_size
        newest_mtime = current.st_mtime

        if self._wall_clock() - newest_mtime < self.min_age_seconds:
            return None

        quarantine = self.root / f".ccm-reap-{uuid.uuid4().hex}"
        os.rename(candidate.path, quarantine)
        try:
            moved = quarantine.lstat()
            moved_identity = (
                moved.st_dev,
                moved.st_ino,
                stat.S_IFMT(moved.st_mode),
            )
            if moved_identity != expected:
                self._restore_quarantine(quarantine, candidate.path)
                return None

            if not stat.S_ISREG(moved.st_mode):
                self._restore_quarantine(quarantine, candidate.path)
                return None
            quarantine.unlink()
        except BaseException:
            self._restore_quarantine(quarantine, candidate.path)
            raise
        return current_size

    @staticmethod
    def _restore_quarantine(quarantine: Path, original: Path) -> None:
        try:
            if quarantine.exists() and not original.exists():
                os.rename(quarantine, original)
        except OSError:
            logger.critical(
                "Could not restore quarantined temp artifact %s",
                quarantine,
            )

    @contextmanager
    def _cross_process_lock(self) -> Iterator[bool]:
        if fcntl is None:  # pragma: no cover
            yield True
            return

        parent = self.lock_path.parent
        parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        metadata = parent.lstat()
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or metadata.st_mode & 0o022
        ):
            raise RuntimeError(f"unsafe tmp cleanup lock directory: {parent}")

        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(self.lock_path, flags, 0o600)
        acquired = False
        try:
            metadata = os.fstat(descriptor)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != os.geteuid()
                or metadata.st_nlink != 1
            ):
                raise RuntimeError(f"unsafe tmp cleanup lock: {self.lock_path}")
            os.fchmod(descriptor, 0o600)
            deadline = time.monotonic() + self.lock_wait_seconds
            while True:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    acquired = True
                    break
                except OSError as exc:
                    if exc.errno not in (errno.EACCES, errno.EAGAIN):
                        raise
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    time.sleep(min(0.05, remaining))
            yield acquired
        finally:
            if acquired:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)


def _build_default_manager() -> TmpSpaceManager:
    from backend.config import settings

    return TmpSpaceManager(
        root="/tmp",
        enabled=settings.tmp_cleanup_enabled,
        trigger_ratio=settings.tmp_cleanup_usage_threshold,
        min_age_seconds=settings.tmp_cleanup_min_age_seconds,
        interval_seconds=settings.tmp_cleanup_interval_seconds,
    )


tmp_space_manager = _build_default_manager()
