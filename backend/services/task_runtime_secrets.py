"""Private on-disk runtime files consumed before an Agent turn starts."""

from __future__ import annotations

import json
import os
import re
import secrets
import stat
from pathlib import Path
from typing import Mapping


_NAMESPACE_RE = re.compile(r"^[a-z][a-z0-9-]{0,31}$")
_NAME_RE = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")


class TaskRuntimeSecretError(RuntimeError):
    """The private runtime root cannot be proven safe."""


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
