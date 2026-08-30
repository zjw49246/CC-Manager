"""Private on-disk runtime files consumed before an Agent turn starts."""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import stat
import tempfile
import threading
from pathlib import Path
from typing import BinaryIO, Mapping


_NAMESPACE_RE = re.compile(r"^[a-z][a-z0-9-]{0,31}$")
_NAME_RE = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")
_GENERATION_KEY_RE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
_GENERATION_STRING_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_TASK_TEMP_MAX_PATH_BYTES = 60
_TASK_TEMP_MAX_CLEANUP_DEPTH = 128
_TASK_TEMP_MAX_CLEANUP_ENTRIES = 10_000


class TaskRuntimeSecretError(RuntimeError):
    """The private runtime root cannot be proven safe."""


class PrivateRuntimeTempDir:
    """One exact durable-runtime-generation scratch directory.

    Codex's request-local filesystem profile grants only this leaf, never its
    parent.  The parent remains mode 0700, so even if model code changes the
    leaf's mode it cannot make the contents traversable to another host user.
    Cleanup is inode-fenced and recursively unlinks entries without following
    symlinks; a stale generation can therefore never delete a replacement
    directory or a symlink target outside its own scratch tree.
    """

    def __init__(
        self,
        *,
        root: Path,
        name: str,
        root_device: int,
        root_inode: int,
        device: int,
        inode: int,
        leaf_fd: int,
    ) -> None:
        self.path = root / name
        self._root = root
        self._name = name
        self._root_device = root_device
        self._root_inode = root_inode
        self._device = device
        self._inode = inode
        self._leaf_fd: int | None = leaf_fd
        self._bound = False
        self._cleaned = False
        self._lock = threading.Lock()

    @property
    def bound(self) -> bool:
        with self._lock:
            return self._bound

    @property
    def cleaned(self) -> bool:
        with self._lock:
            return self._cleaned

    def bind_to_runtime(self) -> None:
        """Transfer cleanup ownership to one native process adapter."""

        with self._lock:
            if self._cleaned:
                raise TaskRuntimeSecretError(
                    "Task scratch directory was cleaned before runtime binding"
                )
            if self._bound:
                raise TaskRuntimeSecretError(
                    "Task scratch directory is already bound to a runtime"
                )
            self._bound = True

    def assert_valid(self) -> None:
        """Re-prove the exact root and leaf identities before model input."""

        with self._lock:
            if self._cleaned:
                raise TaskRuntimeSecretError(
                    "Task scratch directory is no longer available"
                )
            expected = self._root / self._name
            if (
                not self.path.is_absolute()
                or self.path != expected
                or self.path.parent != self._root
                or len(os.fsencode(self.path)) > _TASK_TEMP_MAX_PATH_BYTES
            ):
                raise TaskRuntimeSecretError(
                    "Task scratch path is not the exact bounded leaf"
                )
            _validate_task_temp_identity(
                self._root,
                self._name,
                root_device=self._root_device,
                root_inode=self._root_inode,
                device=self._device,
                inode=self._inode,
            )

    def cleanup_if_unbound(self) -> bool:
        """Clean a pre-turn allocation only when no runtime claimed it."""

        with self._lock:
            if self._bound or self._cleaned:
                return False
            self._cleanup_locked()
            return True

    def cleanup(self) -> None:
        """Remove this exact leaf after its native generation is terminal."""

        with self._lock:
            if self._cleaned:
                return
            self._cleanup_locked()

    def _cleanup_locked(self) -> None:
        root_fd = _open_private_task_temp_root(
            self._root,
            expected_device=self._root_device,
            expected_inode=self._root_inode,
        )
        leaf_fd = self._leaf_fd
        try:
            try:
                path_info = os.stat(
                    self._name,
                    dir_fd=root_fd,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                self._cleaned = True
                return
            if (
                not stat.S_ISDIR(path_info.st_mode)
                or stat.S_ISLNK(path_info.st_mode)
                or path_info.st_uid != os.geteuid()
                or path_info.st_dev != self._device
                or path_info.st_ino != self._inode
            ):
                raise TaskRuntimeSecretError(
                    "Task scratch directory identity changed before cleanup"
                )
            if leaf_fd is None:
                # A previous bounded cleanup attempt may have released the
                # retained descriptor.  Restore access through the private,
                # inode-fenced parent without following a replacement link.
                _restore_private_directory_access(
                    root_fd,
                    self._name,
                    expected=path_info,
                )
                leaf_fd = _open_private_child_directory(
                    root_fd,
                    self._name,
                    expected=path_info,
                )
            opened = os.fstat(leaf_fd)
            if (
                opened.st_dev != self._device
                or opened.st_ino != self._inode
                or not stat.S_ISDIR(opened.st_mode)
            ):
                raise TaskRuntimeSecretError(
                    "Task scratch directory changed while opening cleanup"
                )
            # The Task can chmod its exact writable TMPDIR to 000.  The
            # generation descriptor was retained before model input, so this
            # fchmod cannot be redirected to a replacement path.
            os.fchmod(leaf_fd, 0o700)
            restored = os.fstat(leaf_fd)
            if (
                restored.st_dev != self._device
                or restored.st_ino != self._inode
                or stat.S_IMODE(restored.st_mode) != 0o700
            ):
                raise TaskRuntimeSecretError(
                    "Task scratch directory access could not be restored"
                )
            _remove_private_task_temp_contents(
                leaf_fd,
                root_device=self._device,
                depth=0,
                counter=[0],
            )
            os.close(leaf_fd)
            leaf_fd = None
            self._leaf_fd = None
            os.rmdir(self._name, dir_fd=root_fd)
            self._cleaned = True
        finally:
            if leaf_fd is not None:
                os.close(leaf_fd)
                self._leaf_fd = None
            os.close(root_fd)


# Backward-compatible task-specific type name. The underlying lifecycle is
# provider-neutral so standalone Plan and future durable runners can use the
# same inode-fenced cleanup primitive without inventing negative Task ids.
PrivateTaskTempDir = PrivateRuntimeTempDir


class PrivateRuntimeOutput:
    """One random, exclusively-created auxiliary output file.

    The child receives only the already-open descriptor.  ``close`` removes
    the pathname only when it still names the exact inode we created, so a
    same-uid replacement cannot redirect cleanup to another host file.
    """

    def __init__(self, path: Path, stream: BinaryIO, *, device: int, inode: int):
        self.path = path
        self.name = str(path)
        self._stream = stream
        self._device = device
        self._inode = inode

    @property
    def closed(self) -> bool:
        return self._stream.closed

    def fileno(self) -> int:
        return self._stream.fileno()

    def close(self) -> None:
        if not self._stream.closed:
            self._stream.close()
        try:
            info = self.path.lstat()
        except FileNotFoundError:
            return
        if (
            not stat.S_ISREG(info.st_mode)
            or stat.S_ISLNK(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_dev != self._device
            or info.st_ino != self._inode
        ):
            raise TaskRuntimeSecretError(
                f"Auxiliary output path changed before cleanup: {self.path}"
            )
        self.path.unlink()


def runtime_secret_root() -> Path:
    from backend.config import settings

    expanded = os.path.expandvars(
        os.path.expanduser(settings.task_runtime_secret_dir)
    )
    if not expanded or not os.path.isabs(expanded):
        raise TaskRuntimeSecretError(
            "Task runtime secret directory must be an absolute path"
        )
    return Path(os.path.abspath(expanded))


def private_task_temp_root() -> Path:
    """Return the short, user-private parent for Codex Task scratch leaves."""

    # Codex and its local proxy may create AF_UNIX sockets below TMPDIR.  On
    # POSIX use the fixed short system root rather than an inherited TMPDIR;
    # the complete generation leaf is independently capped below.
    base = Path("/tmp") if os.name == "posix" else Path(tempfile.gettempdir())
    if not base.is_absolute():
        raise TaskRuntimeSecretError(
            "System temporary directory must be an absolute path"
        )
    # Keep the path comfortably below AF_UNIX and bubblewrap mount limits.
    # Darwin exposes /tmp as a system symlink to /private/tmp. Return the
    # canonical base so downstream no-symlink admission compares the exact
    # path we created instead of rejecting CCM's own scratch directory.
    try:
        canonical_base = base.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise TaskRuntimeSecretError(
            "System temporary directory is unavailable"
        ) from exc
    return canonical_base / f"ccm-tmp-{os.geteuid()}"


def _open_private_task_temp_root(
    root: Path,
    *,
    expected_device: int | None = None,
    expected_inode: int | None = None,
) -> int:
    """Open one proven 0700 root without following a replacement symlink."""

    try:
        info = root.lstat()
    except OSError as exc:
        raise TaskRuntimeSecretError(
            "Task scratch root is unavailable"
        ) from exc
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) != 0o700
        or (
            expected_device is not None
            and info.st_dev != expected_device
        )
        or (
            expected_inode is not None
            and info.st_ino != expected_inode
        )
    ):
        raise TaskRuntimeSecretError(
            "Task scratch root must be a stable service-owned 0700 directory"
        )
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(root, flags)
    except OSError as exc:
        raise TaskRuntimeSecretError(
            "Task scratch root could not be opened safely"
        ) from exc
    opened = os.fstat(descriptor)
    if (
        opened.st_dev != info.st_dev
        or opened.st_ino != info.st_ino
        or not stat.S_ISDIR(opened.st_mode)
    ):
        os.close(descriptor)
        raise TaskRuntimeSecretError(
            "Task scratch root changed while opening"
        )
    return descriptor


def _ensure_private_task_temp_root() -> tuple[Path, int, int]:
    root = private_task_temp_root()
    try:
        root.mkdir(mode=0o700)
    except FileExistsError:
        pass
    except OSError as exc:
        raise TaskRuntimeSecretError(
            "Task scratch root could not be created"
        ) from exc
    descriptor = _open_private_task_temp_root(root)
    try:
        info = os.fstat(descriptor)
        return root, info.st_dev, info.st_ino
    finally:
        os.close(descriptor)


def _validate_task_temp_identity(
    root: Path,
    name: str,
    *,
    root_device: int,
    root_inode: int,
    device: int,
    inode: int,
) -> None:
    root_fd = _open_private_task_temp_root(
        root,
        expected_device=root_device,
        expected_inode=root_inode,
    )
    try:
        try:
            info = os.stat(name, dir_fd=root_fd, follow_symlinks=False)
        except OSError as exc:
            raise TaskRuntimeSecretError(
                "Task scratch directory is unavailable"
            ) from exc
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_dev != device
            or info.st_ino != inode
            or stat.S_IMODE(info.st_mode) != 0o700
        ):
            raise TaskRuntimeSecretError(
                "Task scratch directory must remain a stable 0700 directory"
            )
    finally:
        os.close(root_fd)


def _remove_private_task_temp_contents(
    directory_fd: int,
    *,
    root_device: int,
    depth: int,
    counter: list[int],
) -> None:
    """Recursively unlink one tree using dirfds and never follow symlinks."""

    if depth > _TASK_TEMP_MAX_CLEANUP_DEPTH:
        raise TaskRuntimeSecretError(
            "Task scratch cleanup exceeded the safe depth limit"
        )
    with os.scandir(directory_fd) as entries:
        names = [entry.name for entry in entries]
    for name in names:
        counter[0] += 1
        if counter[0] > _TASK_TEMP_MAX_CLEANUP_ENTRIES:
            raise TaskRuntimeSecretError(
                "Task scratch cleanup exceeded the safe entry limit"
            )
        try:
            info = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        except FileNotFoundError:
            continue
        if stat.S_ISDIR(info.st_mode) and not stat.S_ISLNK(info.st_mode):
            if info.st_dev != root_device:
                raise TaskRuntimeSecretError(
                    "Task scratch cleanup refuses a nested mount"
                )
            _restore_private_directory_access(
                directory_fd,
                name,
                expected=info,
            )
            child_fd = _open_private_child_directory(
                directory_fd,
                name,
                expected=info,
            )
            try:
                opened = os.fstat(child_fd)
                if (
                    opened.st_dev != info.st_dev
                    or opened.st_ino != info.st_ino
                    or not stat.S_ISDIR(opened.st_mode)
                ):
                    raise TaskRuntimeSecretError(
                        "Task scratch child changed during cleanup"
                    )
                _remove_private_task_temp_contents(
                    child_fd,
                    root_device=root_device,
                    depth=depth + 1,
                    counter=counter,
                )
            finally:
                os.close(child_fd)
            os.rmdir(name, dir_fd=directory_fd)
        else:
            os.unlink(name, dir_fd=directory_fd)


def _restore_private_directory_access(
    parent_fd: int,
    name: str,
    *,
    expected: os.stat_result,
) -> None:
    """Restore owner access to an already-proven child directory.

    The native Task is terminal before cleanup begins.  Its exact parent is
    already open, and ``follow_symlinks=False`` prevents a chmod through a
    substituted link.  Rechecking the inode after chmod closes the remaining
    path race before recursion.
    """

    if (
        not stat.S_ISDIR(expected.st_mode)
        or stat.S_ISLNK(expected.st_mode)
        or expected.st_uid != os.geteuid()
    ):
        raise TaskRuntimeSecretError(
            "Task scratch child is not a service-owned directory"
        )
    try:
        os.chmod(
            name,
            0o700,
            dir_fd=parent_fd,
            follow_symlinks=False,
        )
        restored = os.stat(
            name,
            dir_fd=parent_fd,
            follow_symlinks=False,
        )
    except OSError as exc:
        raise TaskRuntimeSecretError(
            "Task scratch child access could not be restored"
        ) from exc
    if (
        restored.st_dev != expected.st_dev
        or restored.st_ino != expected.st_ino
        or not stat.S_ISDIR(restored.st_mode)
        or stat.S_ISLNK(restored.st_mode)
        or restored.st_uid != os.geteuid()
        or stat.S_IMODE(restored.st_mode) != 0o700
    ):
        raise TaskRuntimeSecretError(
            "Task scratch child changed while restoring access"
        )


def _open_private_child_directory(
    parent_fd: int,
    name: str,
    *,
    expected: os.stat_result,
) -> int:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        child_fd = os.open(name, flags, dir_fd=parent_fd)
    except OSError as exc:
        raise TaskRuntimeSecretError(
            "Task scratch child could not be opened safely"
        ) from exc
    opened = os.fstat(child_fd)
    if (
        opened.st_dev != expected.st_dev
        or opened.st_ino != expected.st_ino
        or not stat.S_ISDIR(opened.st_mode)
        or opened.st_uid != os.geteuid()
    ):
        os.close(child_fd)
        raise TaskRuntimeSecretError(
            "Task scratch child changed while opening"
        )
    return child_fd


def _runtime_generation_digest(
    *,
    runtime_namespace: str,
    owner_id: int,
    generation_components: Mapping[str, str | int],
) -> str:
    if not _NAMESPACE_RE.fullmatch(runtime_namespace):
        raise ValueError("Invalid private runtime namespace")
    if (
        isinstance(owner_id, bool)
        or not isinstance(owner_id, int)
        or owner_id <= 0
        or owner_id > (2**63 - 1)
    ):
        raise ValueError("Private runtime owner id must be a positive int64")
    if (
        not isinstance(generation_components, Mapping)
        or not generation_components
        or len(generation_components) > 16
    ):
        raise ValueError(
            "Private runtime generation requires bounded durable components"
        )
    normalized: dict[str, str | int] = {}
    for key, value in generation_components.items():
        if not isinstance(key, str) or not _GENERATION_KEY_RE.fullmatch(key):
            raise ValueError("Invalid private runtime generation key")
        if isinstance(value, bool):
            raise ValueError("Boolean runtime generation values are forbidden")
        if isinstance(value, int):
            if value < 0 or value > (2**63 - 1):
                raise ValueError("Runtime generation integer is out of range")
        elif not (
            isinstance(value, str)
            and _GENERATION_STRING_RE.fullmatch(value)
        ):
            raise ValueError("Invalid private runtime generation value")
        normalized[key] = value
    canonical = json.dumps(
        {
            "namespace": runtime_namespace,
            "owner_id": owner_id,
            "generation": normalized,
        },
        sort_keys=True,
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("ascii")
    return hashlib.sha256(canonical).hexdigest()


def create_private_runtime_temp_dir(
    *,
    runtime_namespace: str,
    owner_id: int,
    generation_components: Mapping[str, str | int],
) -> PrivateRuntimeTempDir:
    """Create one private leaf for an exact durable runtime generation."""

    generation_digest = _runtime_generation_digest(
        runtime_namespace=runtime_namespace,
        owner_id=owner_id,
        generation_components=generation_components,
    )
    root, root_device, root_inode = _ensure_private_task_temp_root()
    root_fd = _open_private_task_temp_root(
        root,
        expected_device=root_device,
        expected_inode=root_inode,
    )
    namespace_digest = hashlib.sha256(
        runtime_namespace.encode("ascii")
    ).hexdigest()[:3]
    name = (
        f"r{namespace_digest}{owner_id:x}-"
        f"{generation_digest[:8]}-{secrets.token_hex(4)}"
    )
    if len(name) > 96:
        os.close(root_fd)
        raise TaskRuntimeSecretError("Private runtime scratch name is too long")
    leaf_path = root / name
    if len(os.fsencode(leaf_path)) > _TASK_TEMP_MAX_PATH_BYTES:
        os.close(root_fd)
        raise TaskRuntimeSecretError(
            "Private runtime scratch path leaves insufficient socket headroom"
        )
    try:
        os.mkdir(name, mode=0o700, dir_fd=root_fd)
        flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        leaf_fd: int | None = os.open(name, flags, dir_fd=root_fd)
        try:
            os.fchmod(leaf_fd, 0o700)
            info = os.fstat(leaf_fd)
            if (
                not stat.S_ISDIR(info.st_mode)
                or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o700
            ):
                raise TaskRuntimeSecretError(
                    "Private runtime scratch could not be proven private"
                )
            result = PrivateRuntimeTempDir(
                root=root,
                name=name,
                root_device=root_device,
                root_inode=root_inode,
                device=info.st_dev,
                inode=info.st_ino,
                leaf_fd=leaf_fd,
            )
            leaf_fd = None
            return result
        finally:
            if leaf_fd is not None:
                os.close(leaf_fd)
    except BaseException:
        try:
            os.rmdir(name, dir_fd=root_fd)
        except OSError:
            pass
        raise
    finally:
        os.close(root_fd)


def create_private_task_temp_dir(
    *,
    task_id: int,
    task_incarnation_id: str,
    retry_count: int,
    turn_generation: int,
) -> PrivateTaskTempDir:
    """Create an exclusive short scratch path for one exact Task turn."""

    if (
        isinstance(task_id, bool)
        or not isinstance(task_id, int)
        or task_id <= 0
        or not isinstance(task_incarnation_id, str)
        or re.fullmatch(r"[0-9a-f]{32}", task_incarnation_id) is None
        or isinstance(retry_count, bool)
        or not isinstance(retry_count, int)
        or retry_count < 0
        or isinstance(turn_generation, bool)
        or not isinstance(turn_generation, int)
        or turn_generation < 0
    ):
        raise ValueError("Invalid Task generation for private scratch directory")
    return create_private_runtime_temp_dir(
        runtime_namespace="task",
        owner_id=task_id,
        generation_components={
            "incarnation": task_incarnation_id,
            "retry": retry_count,
            "turn": turn_generation,
        },
    )


def _ensure_private_directory(path: Path) -> None:
    for ancestor in path.parents:
        try:
            if ancestor.is_symlink():
                raise TaskRuntimeSecretError(
                    f"Task runtime secret directory has a symlink ancestor: {path}"
                )
        except OSError as exc:
            raise TaskRuntimeSecretError(
                "Task runtime secret directory is unavailable"
            ) from exc
    try:
        path.mkdir(parents=True, mode=0o700, exist_ok=True)
        info = path.lstat()
    except OSError as exc:
        raise TaskRuntimeSecretError(
            "Task runtime secret directory is unavailable"
        ) from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise TaskRuntimeSecretError(
            "Task runtime secret directory must be a real directory"
        )
    if info.st_uid != os.geteuid():
        raise TaskRuntimeSecretError(
            "Task runtime secret directory has the wrong owner"
        )
    try:
        os.chmod(path, 0o700)
    except OSError as exc:
        raise TaskRuntimeSecretError(
            "Task runtime secret directory permissions could not be secured"
        ) from exc


def _private_scope(namespace: str, identifier: int) -> Path:
    if not _NAMESPACE_RE.fullmatch(namespace):
        raise ValueError("Invalid task runtime namespace")
    if isinstance(identifier, bool) or not isinstance(identifier, int) or identifier <= 0:
        raise ValueError("Task runtime identifier must be positive")
    root = runtime_secret_root()
    _ensure_private_directory(root)
    scope = root / f"{namespace}-{identifier}"
    _ensure_private_directory(scope)
    return scope


def write_private_json(
    namespace: str,
    identifier: int,
    name: str,
    payload: Mapping[str, object],
) -> Path:
    """Atomically replace one known JSON file using mode 0600."""

    if not _NAME_RE.fullmatch(name):
        raise ValueError("Invalid task runtime filename")
    scope = _private_scope(namespace, identifier)
    target = scope / name
    temporary = scope / f".{name}.{secrets.token_hex(8)}.tmp"
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(temporary, flags, 0o600)
        try:
            os.fchmod(descriptor, 0o600)
            encoded = json.dumps(
                payload,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
            view = memoryview(encoded)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise OSError("short task runtime secret write")
                view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(temporary, target)
        return target
    except BaseException:
        try:
            temporary.unlink()
        except OSError:
            pass
        raise


def write_private_bytes(
    namespace: str,
    identifier: int,
    name: str,
    payload: bytes,
    *,
    mode: int = 0o600,
) -> Path:
    """Atomically materialize one private regular file without following links.

    Trusted runtime entrypoints use this alongside the JSON configuration
    writer.  Keeping the primitive here ensures their source snapshots inherit
    the same owner, directory, and no-symlink boundary as Task credentials.
    """

    if not _NAME_RE.fullmatch(name):
        raise ValueError("Invalid task runtime filename")
    if not isinstance(payload, bytes):
        raise TypeError("Task runtime payload must be bytes")
    if mode not in {0o400, 0o500, 0o600, 0o700}:
        raise ValueError("Task runtime file mode is not allowed")
    scope = _private_scope(namespace, identifier)
    target = scope / name
    temporary = scope / f".{name}.{secrets.token_hex(8)}.tmp"
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(temporary, flags, mode)
        try:
            os.fchmod(descriptor, mode)
            view = memoryview(payload)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise OSError("short task runtime file write")
                view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(temporary, target)
        info = target.lstat()
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) != mode
        ):
            raise TaskRuntimeSecretError(
                f"Task runtime file could not be proven private: {target}"
            )
        return target
    except BaseException:
        try:
            temporary.unlink()
        except OSError:
            pass
        raise


def create_private_output(
    namespace: str,
    identifier: int,
    prefix: str = "output",
) -> PrivateRuntimeOutput:
    """Create a random mode-0600 output inode under a private runtime scope."""

    if not _NAME_RE.fullmatch(prefix):
        raise ValueError("Invalid task runtime output prefix")
    scope = _private_scope(namespace, identifier)
    path = scope / f"{prefix}-{secrets.token_hex(16)}.log"
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = os.open(path, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise TaskRuntimeSecretError(
                "Auxiliary output file could not be proven private"
            )
        stream = os.fdopen(descriptor, "wb", closefd=True)
        descriptor = -1
        return PrivateRuntimeOutput(
            path,
            stream,
            device=info.st_dev,
            inode=info.st_ino,
        )
    except BaseException:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            path.unlink()
        except OSError:
            pass
        raise


def remove_private_file(
    namespace: str,
    identifier: int,
    name: str,
) -> None:
    """Remove one expected regular file without following links."""

    if not _NAME_RE.fullmatch(name):
        raise ValueError("Invalid task runtime filename")
    scope = _private_scope(namespace, identifier)
    target = scope / name
    try:
        info = target.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
        raise TaskRuntimeSecretError(
            f"Unexpected task runtime secret file: {target}"
        )
    target.unlink()
    try:
        scope.rmdir()
    except OSError:
        pass


def remove_private_scope(namespace: str, identifier: int) -> None:
    """Remove only regular files directly inside one proven private scope."""

    scope = _private_scope(namespace, identifier)
    try:
        entries = list(os.scandir(scope))
    except FileNotFoundError:
        return
    for entry in entries:
        try:
            info = entry.stat(follow_symlinks=False)
        except FileNotFoundError:
            continue
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
            raise TaskRuntimeSecretError(
                f"Unexpected entry in task runtime secret scope: {entry.name}"
            )
        os.unlink(entry.path)
    try:
        scope.rmdir()
    except FileNotFoundError:
        return
