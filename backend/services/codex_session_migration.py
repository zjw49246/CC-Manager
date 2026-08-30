"""Safe local migration of a Codex rollout between account homes.

Codex sessions are individual rollout files under::

    CODEX_HOME/sessions/YYYY/MM/DD/rollout-<timestamp>-<session_id>.jsonl

This helper deliberately copies only that rollout file.  In particular it
never copies ``auth.json`` and never removes or hard-links the source file.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import stat
import tempfile
import time
from pathlib import Path


_SESSION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_COPY_BUFFER_SIZE = 1024 * 1024
_MIGRATION_SIDECAR_SUFFIX = ".ccm-migration.json"
_MIGRATION_SCHEMA = "ccm.codex.rollout-migration"
_MIGRATION_VERSION = 1
_MIGRATION_SIDECAR_MAX_BYTES = 16 * 1024


class CodexSessionMigrationError(RuntimeError):
    """Base error for a Codex rollout migration failure."""


class InvalidCodexSessionIdError(CodexSessionMigrationError):
    """The requested session ID cannot safely be used in a glob pattern."""


class CodexSessionNotFoundError(CodexSessionMigrationError):
    """No rollout for the requested session exists in the source home."""


class AmbiguousCodexSessionError(CodexSessionMigrationError):
    """More than one source rollout matched the requested session ID."""


class CodexSessionConflictError(CodexSessionMigrationError):
    """The target path already contains different or aliased content."""


class CodexRolloutMigrationMetadataError(CodexSessionMigrationError):
    """A rollout migration sidecar is malformed or no longer matches its file."""


def rollout_migration_sidecar_path(rollout: str | os.PathLike[str]) -> Path:
    """Return the private metadata path adjacent to one rollout JSONL file."""

    path = Path(rollout)
    return path.with_name(path.name + _MIGRATION_SIDECAR_SUFFIX)


def _rollout_relative_identity(path: Path) -> str:
    """Return a stable path identity relative to the nearest ``sessions`` dir."""

    for parent in path.parents:
        if parent.name == "sessions":
            try:
                return path.relative_to(parent).as_posix()
            except ValueError:
                break
    return path.name


def _read_private_sidecar_bytes(path: Path) -> bytes:
    """Read a bounded owner-private regular file without following links."""

    descriptor = -1
    try:
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        descriptor = os.open(path, flags)
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise CodexRolloutMigrationMetadataError(
                f"Migration sidecar is not a regular file: {path}"
            )
        if metadata.st_mode & 0o077:
            raise CodexRolloutMigrationMetadataError(
                f"Migration sidecar is not owner-private: {path}"
            )
        if metadata.st_size > _MIGRATION_SIDECAR_MAX_BYTES:
            raise CodexRolloutMigrationMetadataError(
                f"Migration sidecar is too large: {path}"
            )
        chunks: list[bytes] = []
        remaining = _MIGRATION_SIDECAR_MAX_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        if len(raw) > _MIGRATION_SIDECAR_MAX_BYTES:
            raise CodexRolloutMigrationMetadataError(
                f"Migration sidecar is too large: {path}"
            )
        return raw
    except CodexRolloutMigrationMetadataError:
        raise
    except (OSError, ValueError) as exc:
        raise CodexRolloutMigrationMetadataError(
            f"Unable to read migration sidecar {path}: {exc}"
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _linux_process_start_ticks(pid: int) -> int | None:
    """Return Linux ``/proc/<pid>/stat`` start ticks when available."""

    if pid <= 0:
        return None
    try:
        raw = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
        _prefix, tail = raw.rsplit(") ", 1)
        fields = tail.split()
        # ``tail`` starts at stat field 3; field 22 is index 19.
        return int(fields[19])
    except (OSError, UnicodeError, ValueError, IndexError):
        return None


def _reservation_owner_state(payload: dict) -> bool | None:
    """Return whether a staging reservation owner is still the same process."""

    owner = payload.get("reservation_owner")
    if not isinstance(owner, dict):
        return None
    pid = owner.get("pid")
    start_ticks = owner.get("start_ticks")
    if (
        not isinstance(pid, int)
        or isinstance(pid, bool)
        or pid <= 0
        or not isinstance(start_ticks, int)
        or isinstance(start_ticks, bool)
        or start_ticks < 0
    ):
        return None
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return None
    except OSError:
        return None
    current_start_ticks = _linux_process_start_ticks(pid)
    if current_start_ticks is None:
        return None
    return current_start_ticks == start_ticks


def _read_migration_sidecar_payload(path: Path) -> dict | None:
    """Read and structurally validate a sidecar without checking its rollout inode."""

    sidecar = rollout_migration_sidecar_path(path)
    try:
        os.lstat(sidecar)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise CodexRolloutMigrationMetadataError(
            f"Unable to inspect migration sidecar {sidecar}: {exc}"
        ) from exc

    raw = _read_private_sidecar_bytes(sidecar)
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CodexRolloutMigrationMetadataError(
            f"Migration sidecar is not valid JSON: {sidecar}"
        ) from exc
    if not isinstance(payload, dict):
        raise CodexRolloutMigrationMetadataError(
            f"Migration sidecar has an invalid payload: {sidecar}"
        )
    version = payload.get("version")
    if (
        payload.get("schema") != _MIGRATION_SCHEMA
        or not isinstance(version, int)
        or isinstance(version, bool)
        or version != _MIGRATION_VERSION
    ):
        raise CodexRolloutMigrationMetadataError(
            f"Migration sidecar schema is unsupported: {sidecar}"
        )
    prefix = payload.get("foreign_prefix_bytes")
    if not isinstance(prefix, int) or isinstance(prefix, bool) or prefix < 0:
        raise CodexRolloutMigrationMetadataError(
            f"Migration sidecar has an invalid foreign prefix: {sidecar}"
        )
    identity = payload.get("rollout_identity")
    if not isinstance(identity, dict):
        raise CodexRolloutMigrationMetadataError(
            f"Migration sidecar has no rollout identity: {sidecar}"
        )
    if (
        identity.get("name") != path.name
        or identity.get("relative_path") != _rollout_relative_identity(path)
    ):
        raise CodexRolloutMigrationMetadataError(
            f"Migration sidecar rollout identity does not match {path}"
        )
    file_identity = payload.get("file_identity")
    if not isinstance(file_identity, dict):
        raise CodexRolloutMigrationMetadataError(
            f"Migration sidecar has no file identity: {sidecar}"
        )
    expected_device = file_identity.get("st_dev")
    expected_inode = file_identity.get("st_ino")
    if (
        not isinstance(expected_device, int)
        or isinstance(expected_device, bool)
        or not isinstance(expected_inode, int)
        or isinstance(expected_inode, bool)
        or expected_device < 0
        or expected_inode <= 0
    ):
        raise CodexRolloutMigrationMetadataError(
            f"Migration sidecar has an invalid file identity: {sidecar}"
        )
    return payload


def read_rollout_migration_marker(
    rollout: str | os.PathLike[str],
    *,
    rollout_stat: os.stat_result | None = None,
) -> int | None:
    """Read and validate a rollout's foreign-prefix marker.

    ``None`` means the sidecar is absent and the rollout follows the legacy
    local-history rules.  A present but malformed or stale sidecar raises a
    metadata error so quota readers can fail closed for that rollout.  Callers
    that already hold an open rollout may pass its ``fstat`` result to bind the
    validation to that exact file identity.
    """

    path = Path(rollout)
    payload = _read_migration_sidecar_payload(path)
    if payload is None:
        return None
    prefix = payload["foreign_prefix_bytes"]
    file_identity = payload["file_identity"]

    if rollout_stat is None:
        try:
            rollout_stat = os.stat(path, follow_symlinks=False)
        except OSError as exc:
            raise CodexRolloutMigrationMetadataError(
                f"Unable to inspect rollout for migration sidecar {path}: {exc}"
            ) from exc
    if not stat.S_ISREG(rollout_stat.st_mode):
        raise CodexRolloutMigrationMetadataError(
            f"Rollout referenced by migration sidecar is not regular: {path}"
        )
    if (
        rollout_stat.st_dev != file_identity["st_dev"]
        or rollout_stat.st_ino != file_identity["st_ino"]
        or prefix > rollout_stat.st_size
    ):
        raise CodexRolloutMigrationMetadataError(
            f"Migration sidecar no longer matches rollout {path}"
        )
    return prefix


def _prepare_rollout_migration_marker(
    target: Path,
    rollout_file: Path,
    foreign_prefix_bytes: int,
) -> Path:
    """Create and fsync private marker content without publishing it."""

    try:
        target_stat = os.stat(rollout_file, follow_symlinks=False)
    except OSError as exc:
        raise CodexSessionMigrationError(
            f"Unable to inspect migrated rollout {rollout_file}: {exc}"
        ) from exc
    if not stat.S_ISREG(target_stat.st_mode):
        raise CodexSessionMigrationError(
            f"Migrated rollout is not a regular file: {rollout_file}"
        )
    if (
        not isinstance(foreign_prefix_bytes, int)
        or isinstance(foreign_prefix_bytes, bool)
        or foreign_prefix_bytes < 0
        or foreign_prefix_bytes > target_stat.st_size
    ):
        raise CodexSessionMigrationError(
            f"Invalid foreign prefix for migrated rollout {target}"
        )

    sidecar = rollout_migration_sidecar_path(target)
    descriptor = -1
    temporary: Path | None = None
    try:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{sidecar.name}.", suffix=".tmp", dir=target.parent
        )
        temporary = Path(temporary_name)
        os.fchmod(descriptor, 0o600)
        is_staging = rollout_file != target
        payload = {
            "schema": _MIGRATION_SCHEMA,
            "version": _MIGRATION_VERSION,
            "foreign_prefix_bytes": foreign_prefix_bytes,
            "rollout_identity": {
                "name": target.name,
                "relative_path": _rollout_relative_identity(target),
            },
            "file_identity": {
                "st_dev": int(target_stat.st_dev),
                "st_ino": int(target_stat.st_ino),
            },
            # A committed marker has no disposable reservation.  Staging
            # markers carry enough exact identity to recover only their own
            # files after the owning process has definitely disappeared.
            "staging_state": "prepared" if is_staging else "committed",
        }
        if is_staging:
            payload.update(
                {
                    "reservation_id": f"{os.getpid()}-{time.time_ns()}",
                    "reservation_owner": {
                        "pid": os.getpid(),
                        "start_ticks": _linux_process_start_ticks(os.getpid()),
                    },
                    "staging_rollout_name": rollout_file.name,
                    "staging_marker_name": temporary.name,
                }
            )
        encoded = json.dumps(
            payload,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(encoded.encode("utf-8"))
            stream.write(b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        result = temporary
        temporary = None
        return result
    except Exception as exc:
        if descriptor >= 0:
            os.close(descriptor)
        if temporary is not None:
            try:
                temporary.unlink()
            except OSError:
                pass
        if isinstance(exc, CodexSessionMigrationError):
            raise
        raise CodexSessionMigrationError(
            f"Unable to prepare migration sidecar {sidecar}: {exc}"
        ) from exc


def _migration_payload_matches_rollout(path: Path, payload: dict) -> bool:
    """Return whether a sidecar is committed to the current rollout inode."""

    try:
        rollout_stat = os.stat(path, follow_symlinks=False)
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise CodexRolloutMigrationMetadataError(
            f"Unable to inspect rollout for migration sidecar {path}: {exc}"
        ) from exc
    if not stat.S_ISREG(rollout_stat.st_mode):
        raise CodexRolloutMigrationMetadataError(
            f"Rollout referenced by migration sidecar is not regular: {path}"
        )
    file_identity = payload["file_identity"]
    return (
        rollout_stat.st_dev == file_identity["st_dev"]
        and rollout_stat.st_ino == file_identity["st_ino"]
        and payload["foreign_prefix_bytes"] <= rollout_stat.st_size
    )


def _validate_staging_name(
    target: Path,
    name: object,
    *,
    prefix: str,
) -> Path:
    """Resolve one reservation basename while rejecting traversal and links."""

    if not isinstance(name, str) or not name or len(name) > 255:
        raise CodexRolloutMigrationMetadataError(
            f"Migration sidecar has an invalid staging name for {target}"
        )
    candidate = Path(name)
    if candidate.name != name or name in {".", ".."} or not name.startswith(prefix):
        raise CodexRolloutMigrationMetadataError(
            f"Migration sidecar has an unsafe staging name for {target}"
        )
    return target.parent / name


def _same_file_identity(left: os.stat_result, right: os.stat_result) -> bool:
    return left.st_dev == right.st_dev and left.st_ino == right.st_ino


def _recover_orphan_migration_sidecar(target: Path, payload: dict) -> None:
    """Reclaim a definitely abandoned staging reservation, fail closed otherwise."""

    state = payload.get("staging_state")
    if state != "prepared":
        raise CodexRolloutMigrationMetadataError(
            f"Migration sidecar for {target} is not a recoverable staging marker"
        )
    owner_state = _reservation_owner_state(payload)
    if owner_state is not False:
        if owner_state is True:
            reason = "reservation owner is still alive"
        else:
            reason = "reservation owner identity is unavailable"
        raise CodexRolloutMigrationMetadataError(
            f"Cannot recover migration sidecar for {target}: {reason}"
        )

    reservation_id = payload.get("reservation_id")
    if not isinstance(reservation_id, str) or not reservation_id or len(reservation_id) > 256:
        raise CodexRolloutMigrationMetadataError(
            f"Migration sidecar has no valid reservation identity for {target}"
        )
    sidecar = rollout_migration_sidecar_path(target)
    rollout_temp = _validate_staging_name(
        target,
        payload.get("staging_rollout_name"),
        prefix=f".{target.name}.",
    )
    marker_temp = _validate_staging_name(
        target,
        payload.get("staging_marker_name"),
        prefix=f".{sidecar.name}.",
    )
    if rollout_temp == target or marker_temp == sidecar:
        raise CodexRolloutMigrationMetadataError(
            f"Migration sidecar staging path aliases its logical target: {target}"
        )

    # Revalidate the sidecar identity after owner probing.  This prevents a
    # pathname replacement from turning a dead reservation into permission to
    # remove another operation's marker.
    try:
        sidecar_stat = sidecar.lstat()
    except FileNotFoundError:
        return
    except OSError as exc:
        raise CodexRolloutMigrationMetadataError(
            f"Unable to inspect migration sidecar {sidecar}: {exc}"
        ) from exc
    if not stat.S_ISREG(sidecar_stat.st_mode) or sidecar_stat.st_mode & 0o077:
        raise CodexRolloutMigrationMetadataError(
            f"Migration sidecar is unsafe during recovery: {sidecar}"
        )

    expected_rollout = payload["file_identity"]
    try:
        rollout_stat = rollout_temp.lstat()
    except FileNotFoundError:
        rollout_stat = None
    except OSError as exc:
        raise CodexRolloutMigrationMetadataError(
            f"Unable to inspect staging rollout {rollout_temp}: {exc}"
        ) from exc
    if rollout_stat is not None:
        if (
            not stat.S_ISREG(rollout_stat.st_mode)
            or rollout_stat.st_mode & 0o077
            or rollout_stat.st_dev != expected_rollout["st_dev"]
            or rollout_stat.st_ino != expected_rollout["st_ino"]
            or payload["foreign_prefix_bytes"] > rollout_stat.st_size
        ):
            raise CodexRolloutMigrationMetadataError(
                f"Staging rollout identity is unsafe during recovery: {rollout_temp}"
            )

    try:
        marker_stat = marker_temp.lstat()
    except FileNotFoundError:
        marker_stat = None
    except OSError as exc:
        raise CodexRolloutMigrationMetadataError(
            f"Unable to inspect staging marker {marker_temp}: {exc}"
        ) from exc
    if marker_stat is not None:
        if (
            not stat.S_ISREG(marker_stat.st_mode)
            or marker_stat.st_mode & 0o077
            or not _same_file_identity(marker_stat, sidecar_stat)
        ):
            raise CodexRolloutMigrationMetadataError(
                f"Staging marker identity is unsafe during recovery: {marker_temp}"
            )

    # Re-read the exact sidecar inode and compare the reservation token before
    # unlinking anything.  A concurrent winner is never ours to clean up.
    current_payload = _read_migration_sidecar_payload(target)
    if current_payload is None or current_payload.get("reservation_id") != reservation_id:
        raise CodexRolloutMigrationMetadataError(
            f"Migration sidecar changed during recovery: {sidecar}"
        )
    try:
        current_stat = sidecar.lstat()
    except FileNotFoundError:
        return
    if not _same_file_identity(current_stat, sidecar_stat):
        raise CodexRolloutMigrationMetadataError(
            f"Migration sidecar changed during recovery: {sidecar}"
        )

    try:
        sidecar.unlink()
        if marker_stat is not None:
            try:
                marker_temp.unlink()
            except FileNotFoundError:
                pass
        if rollout_stat is not None:
            try:
                rollout_temp.unlink()
            except FileNotFoundError:
                pass
        _fsync_directory(target.parent)
    except OSError as exc:
        raise CodexRolloutMigrationMetadataError(
            f"Unable to recover orphan migration staging for {target}: {exc}"
        ) from exc


def _reconcile_orphan_migration_sidecar(target: Path) -> None:
    """Make an unmatched sidecar either recoverable or an explicit hard stop."""

    payload = _read_migration_sidecar_payload(target)
    if payload is None or _migration_payload_matches_rollout(target, payload):
        return
    # A sidecar paired with an existing rollout may represent a replacement
    # interrupted after marker publication.  Without the previous target inode
    # in the marker, removing it could expose a concurrent winner or erase
    # evidence for a damaged target.  Only the marker-only window is
    # unambiguously recoverable here.
    try:
        target.lstat()
    except FileNotFoundError:
        pass
    except OSError as exc:
        raise CodexRolloutMigrationMetadataError(
            f"Unable to inspect rollout during migration recovery {target}: {exc}"
        ) from exc
    else:
        raise CodexRolloutMigrationMetadataError(
            f"Migration sidecar no longer matches rollout {target}"
        )
    _recover_orphan_migration_sidecar(target, payload)


def _install_rollout_migration_marker(target: Path, temporary: Path) -> None:
    """Atomically publish a prepared marker beside its logical rollout path."""

    sidecar = rollout_migration_sidecar_path(target)
    try:
        os.replace(temporary, sidecar)
        _fsync_directory(target.parent)
    except OSError as exc:
        raise CodexSessionMigrationError(
            f"Unable to install migration sidecar {sidecar}: {exc}"
        ) from exc


def _install_rollout_migration_marker_exclusive(
    target: Path,
    temporary: Path,
) -> None:
    """Publish a prepared marker without replacing a concurrent winner."""

    sidecar = rollout_migration_sidecar_path(target)
    try:
        os.link(temporary, sidecar, follow_symlinks=False)
        _fsync_directory(target.parent)
    except OSError as exc:
        if isinstance(exc, FileExistsError):
            raise
        raise CodexSessionMigrationError(
            f"Unable to reserve migration sidecar {sidecar}: {exc}"
        ) from exc


def _write_rollout_migration_marker(target: Path, foreign_prefix_bytes: int) -> None:
    """Atomically write and verify owner-private metadata for one rollout."""

    temporary = _prepare_rollout_migration_marker(
        target,
        target,
        foreign_prefix_bytes,
    )
    try:
        _install_rollout_migration_marker(target, temporary)
        if read_rollout_migration_marker(target) != foreign_prefix_bytes:
            raise CodexSessionMigrationError(
                f"Migration sidecar verification failed for {target}"
            )
    finally:
        try:
            temporary.unlink()
        except OSError:
            pass


def _normalise_home(codex_home: str | os.PathLike[str]) -> Path:
    try:
        return Path(codex_home).expanduser().resolve(strict=False)
    except (OSError, RuntimeError, TypeError) as exc:
        raise CodexSessionMigrationError(f"Invalid CODEX_HOME {codex_home!r}: {exc}") from exc


def _validate_session_id(session_id: str) -> None:
    if not isinstance(session_id, str) or not _SESSION_ID_RE.fullmatch(session_id):
        raise InvalidCodexSessionIdError(
            "Invalid Codex session ID; expected only letters, digits, '.', '_' or '-'"
        )


def find_codex_rollout_session(
    session_id: str,
    codex_home: str | os.PathLike[str],
) -> Path:
    """Return the unique rollout path for ``session_id`` in ``codex_home``.

    Raises a specific error when the source is missing or ambiguous instead of
    silently picking one rollout and potentially resuming the wrong history.
    """

    _validate_session_id(session_id)
    home = _normalise_home(codex_home)
    sessions_dir = home / "sessions"
    pattern = f"*/*/*/rollout-*-{session_id}.jsonl"
    matches = sorted(path for path in sessions_dir.glob(pattern) if path.is_file())

    if not matches:
        raise CodexSessionNotFoundError(
            f"Codex session {session_id!r} not found under {sessions_dir}"
        )
    if len(matches) > 1:
        locations = ", ".join(str(path) for path in matches)
        raise AmbiguousCodexSessionError(
            f"Codex session {session_id!r} has multiple rollouts: {locations}"
        )
    return matches[0]


def _ensure_private_directory(path: Path) -> None:
    """Create every missing directory in ``path`` with owner-only access."""

    missing: list[Path] = []
    cursor = path
    while not cursor.exists():
        missing.append(cursor)
        parent = cursor.parent
        if parent == cursor:
            break
        cursor = parent

    if not cursor.is_dir():
        raise CodexSessionMigrationError(f"Cannot create directory below non-directory {cursor}")

    for directory in reversed(missing):
        try:
            directory.mkdir(mode=0o700)
        except FileExistsError:
            # A concurrent migration may have created it after the existence
            # check.  Accept only a directory, never a file or broken link.
            if not directory.is_dir():
                raise CodexSessionMigrationError(
                    f"Target path component is not a directory: {directory}"
                )
        except OSError as exc:
            raise CodexSessionMigrationError(
                f"Unable to create target directory {directory}: {exc}"
            ) from exc


def _compare_rollouts(source: Path, target: Path) -> str:
    """Describe the byte-prefix relationship between two rollout files."""

    try:
        source_size = source.stat().st_size
        target_size = target.stat().st_size
        remaining = min(source_size, target_size)
        with source.open("rb") as source_file, target.open("rb") as target_file:
            while remaining:
                chunk_size = min(_COPY_BUFFER_SIZE, remaining)
                if source_file.read(chunk_size) != target_file.read(chunk_size):
                    return "diverged"
                remaining -= chunk_size
    except OSError as exc:
        raise CodexSessionMigrationError(
            f"Unable to compare rollout files {source} and {target}: {exc}"
        ) from exc

    if source_size == target_size:
        return "equal"
    if source_size > target_size:
        return "source_extends_target"
    return "target_extends_source"


def _copy_file_exclusive(source: Path, target: Path) -> int:
    """Create an independent copy at ``target`` without replacing anything."""

    source_stat = source.stat()
    descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    copied_bytes = 0
    try:
        with source.open("rb") as source_file, os.fdopen(descriptor, "wb") as target_file:
            descriptor = -1  # fdopen owns it now.
            shutil.copyfileobj(source_file, target_file, length=_COPY_BUFFER_SIZE)
            copied_bytes = target_file.tell()
            target_file.flush()
            os.fsync(target_file.fileno())
        os.chmod(target, 0o600)
        os.utime(
            target,
            ns=(source_stat.st_atime_ns, source_stat.st_mtime_ns),
            follow_symlinks=False,
        )
    except Exception:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            target.unlink()
        except OSError:
            pass
        raise
    return copied_bytes


def _create_recoverable_backup(target: Path) -> Path:
    """Copy the current target to a unique owner-only backup beside it."""

    for index in range(10_000):
        suffix = ".pre-migration.bak" if index == 0 else f".pre-migration.{index}.bak"
        backup = target.with_name(target.name + suffix)
        try:
            _copy_file_exclusive(target, backup)
        except FileExistsError:
            if backup.is_file():
                try:
                    aliases_target = target.samefile(backup)
                except OSError:
                    aliases_target = False
                if not aliases_target and _compare_rollouts(target, backup) == "equal":
                    return backup
            continue

        if _compare_rollouts(target, backup) != "equal":
            raise CodexSessionMigrationError(
                f"Target rollout changed while creating recoverable backup {backup}"
            )
        return backup

    raise CodexSessionMigrationError(
        f"Unable to allocate a recoverable backup name beside {target}"
    )


def _copy_source_to_temporary_file(source: Path, target: Path) -> tuple[Path, int]:
    """Create and fsync a private replacement file in the target directory."""

    descriptor, name = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=target.parent,
    )
    temporary = Path(name)
    copied_bytes = 0
    try:
        source_stat = source.stat()
        with source.open("rb") as source_file, os.fdopen(descriptor, "wb") as target_file:
            descriptor = -1
            shutil.copyfileobj(source_file, target_file, length=_COPY_BUFFER_SIZE)
            copied_bytes = target_file.tell()
            target_file.flush()
            os.fsync(target_file.fileno())
        os.chmod(temporary, 0o600)
        os.utime(
            temporary,
            ns=(source_stat.st_atime_ns, source_stat.st_mtime_ns),
            follow_symlinks=False,
        )
    except Exception:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except OSError:
            pass
        raise
    return temporary, copied_bytes


def _mark_preserved_target(
    source: Path,
    target: Path,
    relationship: str,
    *,
    owned_marker: Path | None = None,
) -> None:
    """Record only the source-derived prefix of a preserved target rollout."""

    current_relationship = _compare_rollouts(source, target)
    if current_relationship not in {"equal", "target_extends_source"}:
        raise CodexSessionMigrationError(
            f"Target rollout changed while marking migration: {target}"
        )
    if relationship not in {"equal", "target_extends_source"}:
        raise CodexSessionMigrationError(
            f"Cannot mark unrelated rollout histories: {source} and {target}"
        )

    sidecar = rollout_migration_sidecar_path(target)
    marker_is_owned = False
    if owned_marker is not None:
        try:
            marker_is_owned = sidecar.samefile(owned_marker)
        except OSError:
            marker_is_owned = False

    # A present marker is authoritative migration history. Preserve its exact
    # boundary: bytes appended by the target account remain target-native even
    # if a later round-trip source happens to contain the same suffix.
    existing_prefix = (
        None if marker_is_owned else read_rollout_migration_marker(target)
    )
    if existing_prefix is None:
        source_size = source.stat().st_size
        target_size = target.stat().st_size
        prefix = min(source_size, target_size)
    else:
        prefix = existing_prefix
    _write_rollout_migration_marker(target, prefix)


def _fsync_directory(directory: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(directory, flags)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        # Some filesystems do not support fsync on directories.  The rollout
        # replacement itself is already atomic; directory fsync is durability
        # hardening where supported.
        pass
    finally:
        os.close(descriptor)


def _replace_older_target(source: Path, target: Path) -> Path:
    """Back up an older target and atomically replace it with ``source``."""

    backup = _create_recoverable_backup(target)
    temporary, copied_bytes = _copy_source_to_temporary_file(source, target)
    marker_temporary: Path | None = None
    try:
        if _compare_rollouts(source, temporary) != "equal":
            raise CodexSessionMigrationError(
                f"Source rollout changed while preparing migration: {source}"
            )

        relationship = _compare_rollouts(source, target)
        if relationship in {"equal", "target_extends_source"}:
            # A concurrent writer already brought the target up to date (or
            # beyond it).  Preserve that newer target.
            _mark_preserved_target(source, target, relationship)
            return target
        if relationship == "diverged":
            raise CodexSessionConflictError(
                f"Target rollout diverged while preparing migration: {target}"
            )
        if _compare_rollouts(target, backup) != "equal":
            raise CodexSessionMigrationError(
                f"Target rollout changed after backup {backup}; retry migration"
            )
        # Publish the marker first, bound to the replacement file's inode. The
        # old target is temporarily ineligible rather than the foreign rollout
        # ever being visible without metadata.
        marker_temporary = _prepare_rollout_migration_marker(
            target,
            temporary,
            copied_bytes,
        )
        _install_rollout_migration_marker(target, marker_temporary)
        marker_temporary = None
        os.replace(temporary, target)
        _fsync_directory(target.parent)
        if read_rollout_migration_marker(target) != copied_bytes:
            raise CodexSessionMigrationError(
                f"Migration sidecar verification failed for {target}"
            )
        return target
    finally:
        if marker_temporary is not None:
            try:
                marker_temporary.unlink()
            except OSError:
                pass
        try:
            temporary.unlink()
        except OSError:
            pass


def _reconcile_existing_target(
    source: Path,
    target: Path,
    *,
    owned_marker: Path | None = None,
) -> Path:
    if not target.is_file():
        raise CodexSessionConflictError(f"Target rollout is not a regular file: {target}")

    try:
        aliases_source = source.samefile(target)
    except OSError as exc:
        raise CodexSessionMigrationError(f"Unable to inspect target rollout {target}: {exc}") from exc

    if aliases_source and source != target:
        raise CodexSessionConflictError(
            f"Target rollout aliases the source instead of containing an independent copy: {target}"
        )

    relationship = _compare_rollouts(source, target)
    if relationship == "equal":
        # An equal target may be a previous copy (or an independently created
        # identical history). Treat the bytes supplied by this migration as
        # foreign so they cannot be attributed to the destination account.
        _mark_preserved_target(
            source,
            target,
            relationship,
            owned_marker=owned_marker,
        )
        return target
    if relationship == "target_extends_source":
        # Preserve destination-native bytes appended after an earlier copy.
        # A valid marker is authoritative; when it is absent, this migration
        # proves at least the source prefix foreign.
        _mark_preserved_target(
            source,
            target,
            relationship,
            owned_marker=owned_marker,
        )
        return target
    if relationship == "source_extends_target":
        return _replace_older_target(source, target)
    raise CodexSessionConflictError(
        f"Target rollout already exists with diverged content: {target}"
    )


def _unlink_owned_marker(target: Path, marker_temporary: Path) -> None:
    """Remove a staging marker only while its hardlink ownership is intact."""

    sidecar = rollout_migration_sidecar_path(target)
    try:
        if sidecar.samefile(marker_temporary):
            sidecar.unlink()
            _fsync_directory(target.parent)
    except OSError:
        pass


def _copy_new_rollout(source: Path, target: Path) -> Path:
    """Publish a new target with marker-before-rollout no-clobber ordering."""

    temporary, copied_bytes = _copy_source_to_temporary_file(source, target)
    marker_temporary: Path | None = None
    marker_published = False
    rollout_published = False
    completed = False
    try:
        marker_temporary = _prepare_rollout_migration_marker(
            target,
            temporary,
            copied_bytes,
        )
        try:
            _install_rollout_migration_marker_exclusive(target, marker_temporary)
            marker_published = True
        except FileExistsError:
            if target.exists():
                result = _reconcile_existing_target(source, target)
                completed = True
                return result
            raise CodexSessionConflictError(
                f"Migration sidecar already exists without target rollout: {target}"
            )

        try:
            os.link(temporary, target, follow_symlinks=False)
        except FileExistsError:
            # Our marker reservation is an explicit staging identity. If a
            # concurrent target won, reconcile it while preserving any native
            # suffix already represented by its own marker.
            result = _reconcile_existing_target(
                source,
                target,
                owned_marker=marker_temporary,
            )
            completed = True
            return result
        rollout_published = True
        _fsync_directory(target.parent)
        if read_rollout_migration_marker(target) != copied_bytes:
            raise CodexSessionMigrationError(
                f"Migration sidecar verification failed for {target}"
            )
        completed = True
        return target
    finally:
        if marker_temporary is not None:
            if marker_published and not completed and not rollout_published:
                _unlink_owned_marker(target, marker_temporary)
            try:
                marker_temporary.unlink()
            except OSError:
                pass
        try:
            temporary.unlink()
        except OSError:
            pass


def migrate_codex_rollout_session(
    session_id: str,
    source_codex_home: str | os.PathLike[str],
    target_codex_home: str | os.PathLike[str],
) -> Path:
    """Copy one Codex rollout to the same relative path in another home.

    The operation is idempotent when an independent target file already has
    identical content.  If one rollout is a strict byte-prefix of the other,
    the longer history wins: a newer target is preserved, while an older target
    is backed up and atomically replaced.  Diverged histories are never
    overwritten.  New directories and copied files are owner-only (0700/0600).
    """

    source_home = _normalise_home(source_codex_home)
    target_home = _normalise_home(target_codex_home)
    source = find_codex_rollout_session(session_id, source_home)
    source_sessions = source_home / "sessions"

    try:
        relative_path = source.relative_to(source_sessions)
    except ValueError as exc:  # Defensive: the fixed-depth glob should guarantee this.
        raise CodexSessionMigrationError(
            f"Source rollout escaped its CODEX_HOME sessions directory: {source}"
        ) from exc

    target = target_home / "sessions" / relative_path
    if source == target:
        return target

    try:
        _ensure_private_directory(target.parent)
        # A process crash can leave the logical sidecar published while its
        # temporary rollout was not yet linked/renamed.  Recover only a
        # reservation whose owner identity is provably dead; all other
        # mismatches remain fail-closed.
        _reconcile_orphan_migration_sidecar(target)
        if target.exists():
            return _reconcile_existing_target(source, target)
        _copy_new_rollout(source, target)
    except CodexSessionMigrationError:
        raise
    except OSError as exc:
        raise CodexSessionMigrationError(
            f"Unable to copy Codex session {session_id!r} from {source} to {target}: {exc}"
        ) from exc

    return target


__all__ = [
    "AmbiguousCodexSessionError",
    "CodexSessionConflictError",
    "CodexSessionMigrationError",
    "CodexSessionNotFoundError",
    "CodexRolloutMigrationMetadataError",
    "InvalidCodexSessionIdError",
    "find_codex_rollout_session",
    "migrate_codex_rollout_session",
    "read_rollout_migration_marker",
    "rollout_migration_sidecar_path",
]
