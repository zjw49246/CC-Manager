"""Private persistent storage for frontend Test Harness evidence."""

from __future__ import annotations

import hashlib
import os
import re
import stat
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Iterator

from backend.config import settings


_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_ARTIFACT_RE = re.compile(
    r"(?:initial\.png|final\.png|step-\d{2,3}\.png|report\.md|"
    r"telemetry\.json|response\.json|actions\.jsonl)"
)
_STORAGE_KEY_RE = re.compile(
    r"^runs/task-(?P<task>[0-9]+)/(?P<run>[0-9a-f]{32})/"
    r"(?P<attempt>[0-9a-f]{32})/(?P<digest>[0-9a-f]{64})--(?P<name>[^/]+)$"
)
_PNG_MAGIC = b"\x89PNG\r\n\x1a\n"


class TestHarnessArtifactError(RuntimeError):
    """Evidence could not be stored or opened safely."""


class TestHarnessArtifactQuotaError(TestHarnessArtifactError):
    """A configured evidence quota would be exceeded."""


@dataclass(frozen=True, slots=True)
class ArchivedArtifact:
    storage_key: str
    sha256: str
    byte_size: int
    path: Path


@dataclass
class OpenedHarnessArtifact:
    fd: int
    name: str
    byte_size: int
    _closed: bool = False

    def chunks(self, chunk_size: int = 64 * 1024) -> Iterator[bytes]:
        while not self._closed:
            chunk = os.read(self.fd, chunk_size)
            if not chunk:
                break
            yield chunk

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            os.close(self.fd)


class TestHarnessArtifactStore:
    """Archive immutable evidence under one configured, private filesystem root."""

    def __init__(
        self,
        root: str | os.PathLike[str] | None = None,
        *,
        max_file_bytes: int | None = None,
        max_run_bytes: int | None = None,
        max_task_bytes: int | None = None,
        max_total_bytes: int | None = None,
        retention_days: int | None = None,
    ) -> None:
        configured = Path(root or settings.test_harness_artifact_root).expanduser()
        if not configured.is_absolute():
            raise TestHarnessArtifactError("Test Harness artifact root must be absolute")
        # Canonicalize once from administrator-controlled configuration.  This
        # handles platform roots such as macOS /var -> /private/var; every later
        # access still requires this exact canonical path to remain non-symlink.
        self.root = Path(os.path.abspath(configured)).resolve(strict=False)
        self.max_file_bytes = int(
            max_file_bytes or settings.test_harness_artifact_max_file_bytes
        )
        self.max_run_bytes = int(
            max_run_bytes or settings.test_harness_artifact_max_run_bytes
        )
        self.max_task_bytes = int(
            max_task_bytes or settings.test_harness_artifact_max_task_bytes
        )
        self.max_total_bytes = int(
            max_total_bytes or settings.test_harness_artifact_max_total_bytes
        )
        self.retention_days = int(
            retention_days
            if retention_days is not None
            else settings.test_harness_artifact_retention_days
        )
        if min(
            self.max_file_bytes,
            self.max_run_bytes,
            self.max_task_bytes,
            self.max_total_bytes,
        ) <= 0:
            raise TestHarnessArtifactError("Test Harness artifact quotas must be positive")
        if not (
            self.max_file_bytes <= self.max_run_bytes <= self.max_task_bytes <= self.max_total_bytes
        ):
            raise TestHarnessArtifactError("Test Harness artifact quotas are inconsistent")
        if self.retention_days < 1:
            raise TestHarnessArtifactError("Test Harness artifact retention must be positive")

    @property
    def jobs_root(self) -> Path:
        return self.root / "jobs"

    @property
    def runs_root(self) -> Path:
        return self.root / "runs"

    def ensure_root(self) -> None:
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._require_private_directory(self.root, fix_mode=True)
        for child in (self.jobs_root, self.runs_root):
            child.mkdir(mode=0o700, exist_ok=True)
            self._require_private_directory(child, fix_mode=True)

    def create_job_dir(self, job_id: str) -> Path:
        self._validate_id(job_id, "job")
        self.ensure_root()
        if self.total_bytes() >= self.max_total_bytes:
            raise TestHarnessArtifactQuotaError("Test Harness evidence storage is full")
        path = self.jobs_root / job_id
        try:
            path.mkdir(mode=0o700)
        except FileExistsError as exc:
            raise TestHarnessArtifactError("Browser Review job directory already exists") from exc
        self._require_private_directory(path, fix_mode=True)
        return path

    def ensure_job_capacity(self, job_dir: Path, name: str, incoming_size: int) -> None:
        self._validate_artifact_name(name)
        if incoming_size < 0 or incoming_size > self.max_file_bytes:
            raise TestHarnessArtifactQuotaError(
                f"Browser Review artifact exceeds {self.max_file_bytes} bytes"
            )
        managed = self._managed_job_dir(job_dir)
        existing = managed / name
        existing_size = self._regular_file_size(existing)
        delta = incoming_size - existing_size
        if self._tree_size(managed) + delta > self.max_run_bytes:
            raise TestHarnessArtifactQuotaError("Browser Review run evidence quota exceeded")
        if self.total_bytes() + delta > self.max_total_bytes:
            raise TestHarnessArtifactQuotaError("Test Harness evidence storage is full")

    def archive(
        self,
        source: Path,
        *,
        task_id: int,
        run_id: str,
        attempt_id: str,
        name: str,
    ) -> ArchivedArtifact:
        self._validate_id(run_id, "run")
        self._validate_id(attempt_id, "attempt")
        self._validate_artifact_name(name)
        if isinstance(task_id, bool) or not isinstance(task_id, int) or task_id < 0:
            raise TestHarnessArtifactError("Test Harness task identity is invalid")
        self.ensure_root()

        source_fd = self._open_regular_readonly(source)
        temporary: Path | None = None
        try:
            before = os.fstat(source_fd)
            if before.st_size > self.max_file_bytes:
                raise TestHarnessArtifactQuotaError(
                    f"Browser Review artifact exceeds {self.max_file_bytes} bytes"
                )
            task_root = self._ensure_child(self.runs_root, f"task-{task_id}")
            run_root = self._ensure_child(task_root, run_id)
            attempt_root = self._ensure_child(run_root, attempt_id)
            run_size_before = self._tree_size(run_root)
            task_size_before = self._tree_size(task_root)
            total_size_before = self.total_bytes()

            temporary = attempt_root / f".{name}.{uuid.uuid4().hex}.tmp"
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
            output_fd = os.open(temporary, flags, 0o600)
            digest = hashlib.sha256()
            total = 0
            prefix = b""
            try:
                while True:
                    chunk = os.read(source_fd, 1024 * 1024)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > self.max_file_bytes:
                        raise TestHarnessArtifactQuotaError(
                            f"Browser Review artifact exceeds {self.max_file_bytes} bytes"
                        )
                    if len(prefix) < len(_PNG_MAGIC):
                        prefix = (prefix + chunk)[: len(_PNG_MAGIC)]
                    digest.update(chunk)
                    _write_all(output_fd, chunk)
                os.fsync(output_fd)
            finally:
                os.close(output_fd)

            after = os.fstat(source_fd)
            if (
                total != before.st_size
                or before.st_dev != after.st_dev
                or before.st_ino != after.st_ino
                or before.st_size != after.st_size
                or before.st_mtime_ns != after.st_mtime_ns
            ):
                raise TestHarnessArtifactError("Browser Review artifact changed while archiving")
            if name.endswith(".png") and prefix != _PNG_MAGIC:
                raise TestHarnessArtifactError("Browser Review screenshot is not a valid PNG")

            digest_value = digest.hexdigest()
            final_name = f"{digest_value}--{name}"
            final = attempt_root / final_name
            existing_size = self._regular_file_size(final)
            delta = total - existing_size
            if run_size_before + delta > self.max_run_bytes:
                raise TestHarnessArtifactQuotaError("Test Harness run evidence quota exceeded")
            if task_size_before + delta > self.max_task_bytes:
                raise TestHarnessArtifactQuotaError("Test Harness Task evidence quota exceeded")
            if total_size_before + delta > self.max_total_bytes:
                raise TestHarnessArtifactQuotaError("Test Harness evidence storage is full")
            # The destination name is content-addressed from the verified
            # source bytes. Always replace it atomically: size equality alone
            # cannot prove that an existing archive was not corrupted, and a
            # retained staging directory must be able to repair that damage.
            os.replace(temporary, final)
            temporary = None
            os.chmod(final, 0o600, follow_symlinks=False)
            directory_fd = os.open(
                attempt_root,
                os.O_RDONLY
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
            )
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            storage_key = f"runs/task-{task_id}/{run_id}/{attempt_id}/{final_name}"
            return ArchivedArtifact(storage_key, digest_value, total, final)
        finally:
            os.close(source_fd)
            if temporary is not None:
                try:
                    temporary.unlink()
                except FileNotFoundError:
                    pass

    def open(
        self,
        storage_key: str,
        *,
        expected_sha256: str,
        expected_size: int,
    ) -> OpenedHarnessArtifact:
        path, name = self._storage_path(storage_key)
        fd = self._open_regular_readonly(path)
        try:
            before = os.fstat(fd)
            if before.st_size != expected_size:
                raise TestHarnessArtifactError("Test Harness evidence size does not match")
            digest = hashlib.sha256()
            total = 0
            while chunk := os.read(fd, 1024 * 1024):
                total += len(chunk)
                if total > self.max_file_bytes:
                    raise TestHarnessArtifactError("Test Harness evidence is too large")
                digest.update(chunk)
            after = os.fstat(fd)
            if (
                before.st_dev != after.st_dev
                or before.st_ino != after.st_ino
                or before.st_size != after.st_size
                or before.st_mtime_ns != after.st_mtime_ns
                or total != expected_size
                or digest.hexdigest() != expected_sha256
            ):
                raise TestHarnessArtifactError("Test Harness evidence integrity check failed")
            os.lseek(fd, 0, os.SEEK_SET)
            return OpenedHarnessArtifact(fd=fd, name=name, byte_size=total)
        except BaseException:
            os.close(fd)
            raise

    def resolve_path(self, storage_key: str) -> Path:
        path, _name = self._storage_path(storage_key)
        return path

    def remove(self, storage_key: str) -> bool:
        try:
            path, _name = self._storage_path(storage_key)
        except TestHarnessArtifactError:
            return False
        try:
            info = path.lstat()
        except FileNotFoundError:
            return True
        if not stat.S_ISREG(info.st_mode) or path.is_symlink():
            return False
        path.unlink()
        self._remove_empty_parents(path.parent, stop=self.runs_root)
        return True

    def remove_job_dir(self, job_id: str) -> bool:
        self._validate_id(job_id, "job")
        self.ensure_root()
        path = self.jobs_root / job_id
        if not path.exists():
            return True
        try:
            managed = self._managed_job_dir(path)
            _remove_private_tree(managed)
        except (OSError, TestHarnessArtifactError):
            return False
        return True

    def cleanup_job_dirs(
        self,
        *,
        active_job_ids: set[str] | None = None,
        now: datetime | None = None,
    ) -> int:
        self.ensure_root()
        active = active_job_ids or set()
        cutoff = (now or datetime.now(timezone.utc)) - timedelta(days=self.retention_days)
        removed = 0
        for candidate in list(self.jobs_root.iterdir()):
            if candidate.name in active or not _ID_RE.fullmatch(candidate.name):
                continue
            try:
                info = candidate.lstat()
            except FileNotFoundError:
                continue
            modified = datetime.fromtimestamp(info.st_mtime, tz=timezone.utc)
            over_quota = self.total_bytes() > self.max_total_bytes
            if modified <= cutoff or over_quota:
                if self.remove_job_dir(candidate.name):
                    removed += 1
        return removed

    def list_job_artifacts(self, job_dir: str | os.PathLike[str]) -> list[str]:
        managed = self._managed_job_dir(Path(job_dir))
        result: list[str] = []
        for candidate in managed.iterdir():
            if _ARTIFACT_RE.fullmatch(candidate.name) is None:
                continue
            try:
                info = candidate.lstat()
            except FileNotFoundError:
                continue
            if stat.S_ISREG(info.st_mode) and not candidate.is_symlink():
                result.append(candidate.name)
        return sorted(result)

    def cleanup_orphan_archives(
        self,
        referenced_keys: set[str],
        *,
        older_than: datetime | None = None,
    ) -> int:
        self.ensure_root()
        cutoff = older_than or (datetime.now(timezone.utc) - timedelta(days=1))
        removed = 0
        for directory, dirs, files in os.walk(self.runs_root, followlinks=False):
            current = Path(directory)
            dirs[:] = [name for name in dirs if not (current / name).is_symlink()]
            for name in files:
                candidate = current / name
                try:
                    info = candidate.lstat()
                    storage_key = candidate.relative_to(self.root).as_posix()
                except (FileNotFoundError, ValueError):
                    continue
                if (
                    storage_key in referenced_keys
                    or _STORAGE_KEY_RE.fullmatch(storage_key) is None
                    or candidate.is_symlink()
                    or not stat.S_ISREG(info.st_mode)
                    or datetime.fromtimestamp(info.st_mtime, tz=timezone.utc) > cutoff
                ):
                    continue
                candidate.unlink()
                self._remove_empty_parents(candidate.parent, stop=self.runs_root)
                removed += 1
        return removed

    def is_managed_job_dir(self, path: str | os.PathLike[str]) -> bool:
        try:
            self._managed_job_dir(Path(path))
        except TestHarnessArtifactError:
            return False
        return True

    def run_prefix(self, *, task_id: int, run_id: str, attempt_id: str) -> str:
        self._validate_id(run_id, "run")
        self._validate_id(attempt_id, "attempt")
        return f"runs/task-{task_id}/{run_id}/{attempt_id}"

    def total_bytes(self) -> int:
        self.ensure_root_shallow()
        return self._tree_size(self.root)

    def ensure_root_shallow(self) -> None:
        if not self.root.exists():
            self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._require_private_directory(self.root, fix_mode=True)

    def _storage_path(self, storage_key: str) -> tuple[Path, str]:
        match = _STORAGE_KEY_RE.fullmatch(storage_key)
        if match is None or not _ARTIFACT_RE.fullmatch(match.group("name")):
            raise TestHarnessArtifactError("Test Harness evidence storage key is invalid")
        self.ensure_root()
        parts = PurePosixPath(storage_key).parts
        candidate = self.root.joinpath(*parts)
        # Every parent is opened/verified without following a final file
        # symlink; root itself is also rejected when configured through one.
        cursor = self.root
        for part in parts[:-1]:
            cursor /= part
            self._require_private_directory(cursor, fix_mode=False)
        return candidate, parts[-1]

    def _managed_job_dir(self, value: Path) -> Path:
        self.ensure_root()
        candidate = Path(os.path.abspath(value))
        if candidate.parent != self.jobs_root or not _ID_RE.fullmatch(candidate.name):
            raise TestHarnessArtifactError("Browser Review job directory is outside the managed root")
        self._require_private_directory(candidate, fix_mode=False)
        return candidate

    def _ensure_child(self, parent: Path, name: str) -> Path:
        if not name or name in {".", ".."} or "/" in name or "\\" in name:
            raise TestHarnessArtifactError("Test Harness artifact path component is invalid")
        path = parent / name
        try:
            path.mkdir(mode=0o700)
        except FileExistsError:
            pass
        self._require_private_directory(path, fix_mode=True)
        return path

    def _require_private_directory(self, path: Path, *, fix_mode: bool) -> None:
        try:
            info = path.lstat()
            resolved = path.resolve(strict=True)
        except OSError as exc:
            raise TestHarnessArtifactError("Test Harness artifact directory is unavailable") from exc
        if path.is_symlink() or resolved != path or not stat.S_ISDIR(info.st_mode):
            raise TestHarnessArtifactError("Test Harness artifact directory is unsafe")
        if hasattr(os, "geteuid") and info.st_uid != os.geteuid():
            raise TestHarnessArtifactError("Test Harness artifact directory has a different owner")
        if fix_mode:
            path.chmod(0o700)
        elif stat.S_IMODE(info.st_mode) & 0o077:
            raise TestHarnessArtifactError("Test Harness artifact directory is not private")

    def _open_regular_readonly(self, path: Path) -> int:
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        try:
            fd = os.open(path, flags)
        except OSError as exc:
            raise TestHarnessArtifactError("Browser Review artifact could not be opened safely") from exc
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            os.close(fd)
            raise TestHarnessArtifactError("Browser Review artifact is not a regular file")
        return fd

    @staticmethod
    def _regular_file_size(path: Path) -> int:
        try:
            info = path.lstat()
        except FileNotFoundError:
            return 0
        if path.is_symlink() or not stat.S_ISREG(info.st_mode):
            raise TestHarnessArtifactError("Test Harness artifact target is unsafe")
        return info.st_size

    @staticmethod
    def _tree_size(root: Path) -> int:
        try:
            root_info = root.lstat()
        except FileNotFoundError:
            return 0
        if root.is_symlink() or not stat.S_ISDIR(root_info.st_mode):
            raise TestHarnessArtifactError("Test Harness artifact tree is unsafe")
        total = 0
        for directory, dirs, files in os.walk(root, followlinks=False):
            current = Path(directory)
            dirs[:] = [
                name
                for name in dirs
                if not (current / name).is_symlink()
            ]
            for name in files:
                candidate = current / name
                try:
                    info = candidate.lstat()
                except FileNotFoundError:
                    continue
                if stat.S_ISREG(info.st_mode) and not candidate.is_symlink():
                    total += info.st_size
        return total

    @staticmethod
    def _remove_empty_parents(path: Path, *, stop: Path) -> None:
        cursor = path
        while cursor != stop and stop in cursor.parents:
            try:
                cursor.rmdir()
            except OSError:
                break
            cursor = cursor.parent

    @staticmethod
    def _validate_id(value: str, label: str) -> None:
        if not isinstance(value, str) or _ID_RE.fullmatch(value) is None:
            raise TestHarnessArtifactError(f"Test Harness {label} identity is invalid")

    @staticmethod
    def _validate_artifact_name(name: str) -> None:
        if not isinstance(name, str) or _ARTIFACT_RE.fullmatch(name) is None:
            raise TestHarnessArtifactError("Browser Review artifact name is invalid")


def _write_all(fd: int, value: bytes) -> None:
    offset = 0
    while offset < len(value):
        written = os.write(fd, value[offset:])
        if written <= 0:
            raise TestHarnessArtifactError("Test Harness artifact write did not progress")
        offset += written


def _remove_private_tree(root: Path) -> None:
    info = root.lstat()
    if root.is_symlink() or not stat.S_ISDIR(info.st_mode):
        raise TestHarnessArtifactError("Browser Review job directory is unsafe")
    for candidate in list(root.iterdir()):
        item = candidate.lstat()
        if stat.S_ISDIR(item.st_mode) and not candidate.is_symlink():
            _remove_private_tree(candidate)
        else:
            candidate.unlink()
    root.rmdir()


test_harness_artifact_store = TestHarnessArtifactStore()
