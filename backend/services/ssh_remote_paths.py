"""Canonical remote-path enforcement for managed SSH Profiles."""

from __future__ import annotations

import posixpath
from collections.abc import Iterable


class SSHRemotePathDenied(PermissionError):
    """The canonical remote path is outside the Profile's allowed roots."""


def _absolute_remote_path(value: str) -> str:
    if not isinstance(value, str) or "\x00" in value or not value.startswith("/"):
        raise SSHRemotePathDenied("Remote path must be an absolute POSIX path")
    return "/" + posixpath.normpath(value).lstrip("/")


def _within(path: str, root: str) -> bool:
    return root == "/" or path == root or path.startswith(root.rstrip("/") + "/")


def canonical_allowed_roots(sftp, roots: Iterable[str]) -> tuple[str, ...]:
    canonical: list[str] = []
    for root in roots:
        resolved = _absolute_remote_path(sftp.normalize(_absolute_remote_path(root)))
        if any(_within(resolved, parent) for parent in canonical):
            continue
        canonical = [parent for parent in canonical if not _within(parent, resolved)]
        canonical.append(resolved)
        canonical.sort(key=lambda item: (len(item), item))
    if not canonical:
        raise SSHRemotePathDenied("SSH Profile has no usable allowed roots")
    return tuple(canonical)


def resolve_existing_remote_path(sftp, path: str, roots: Iterable[str]) -> str:
    """Resolve symlinks and enforce containment for an existing path."""

    allowed = canonical_allowed_roots(sftp, roots)
    resolved = _absolute_remote_path(sftp.normalize(_absolute_remote_path(path)))
    if not any(_within(resolved, root) for root in allowed):
        raise SSHRemotePathDenied("Remote path is outside this SSH Profile's allowed roots")
    return resolved


def resolve_remote_write_path(sftp, path: str, roots: Iterable[str]) -> str:
    """Resolve an existing target or its parent before a bounded file write."""

    requested = _absolute_remote_path(path)
    name = posixpath.basename(requested)
    if not name or name in {".", ".."}:
        raise SSHRemotePathDenied("Remote write path must name a file")
    try:
        sftp.lstat(requested)
    except FileNotFoundError:
        parent = resolve_existing_remote_path(
            sftp,
            posixpath.dirname(requested),
            roots,
        )
        resolved = posixpath.join(parent, name)
    else:
        resolved = resolve_existing_remote_path(sftp, requested, roots)
    # Recheck the final result independently. This also documents the intended
    # fail-closed boundary if a server returns surprising normalize results.
    allowed = canonical_allowed_roots(sftp, roots)
    if not any(_within(resolved, root) for root in allowed):
        raise SSHRemotePathDenied("Remote path is outside this SSH Profile's allowed roots")
    return resolved
