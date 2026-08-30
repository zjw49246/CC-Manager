"""Shared process identity verification for Instance and Plan runtime probes.

This module provides PID identity readers that prevent false-positive detection
after PID reuse or host restart. It shares Darwin/Linux process-identity logic
between Instance lifecycle (Dispatcher/InstanceManager/TaskQueue) and Plan
runtime receipts without creating import cycles.
"""

from __future__ import annotations

import ctypes
import errno
import os
import sys
import uuid
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


class ProcessIdentityError(RuntimeError):
    """Exact process identity could not be read or verified."""


@dataclass(frozen=True)
class ProcessIdentity:
    """Exact POSIX process identity for safe PID reuse detection."""

    pid: int
    start_ticks: int
    boot_id: str
    uid: int | None = None
    process_group_id: int | None = None
    state: str | None = None


class _DarwinProcBsdInfo(ctypes.Structure):
    """Stable PROC_PIDTBSDINFO prefix for Darwin PID identity."""

    _fields_ = [
        ("pbi_flags", ctypes.c_uint32),
        ("pbi_status", ctypes.c_uint32),
        ("pbi_xstatus", ctypes.c_uint32),
        ("pbi_pid", ctypes.c_uint32),
        ("pbi_ppid", ctypes.c_uint32),
        ("pbi_uid", ctypes.c_uint32),
        ("pbi_gid", ctypes.c_uint32),
        ("pbi_ruid", ctypes.c_uint32),
        ("pbi_rgid", ctypes.c_uint32),
        ("pbi_svuid", ctypes.c_uint32),
        ("pbi_svgid", ctypes.c_uint32),
        ("rfu_1", ctypes.c_uint32),
        ("pbi_comm", ctypes.c_char * 16),
        ("pbi_name", ctypes.c_char * 32),
        ("pbi_nfiles", ctypes.c_uint32),
        ("pbi_pgid", ctypes.c_uint32),
        ("pbi_pjobc", ctypes.c_uint32),
        ("e_tdev", ctypes.c_uint32),
        ("e_tpgid", ctypes.c_uint32),
        ("pbi_nice", ctypes.c_int32),
        ("pbi_start_tvsec", ctypes.c_uint64),
        ("pbi_start_tvusec", ctypes.c_uint64),
    ]


@lru_cache(maxsize=1)
def _darwin_libproc():
    library = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    library.proc_pidinfo.argtypes = [
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_uint64,
        ctypes.c_void_p,
        ctypes.c_int,
    ]
    library.proc_pidinfo.restype = ctypes.c_int
    return library


@lru_cache(maxsize=1)
def _read_boot_id() -> str:
    """Return current boot session UUID for this host."""

    if sys.platform == "darwin":
        return _darwin_boot_session_uuid()
    try:
        value = (
            Path("/proc/sys/kernel/random/boot_id")
            .read_text(encoding="utf-8")
            .strip()
            .lower()
        )
    except OSError as exc:
        raise ProcessIdentityError(
            f"Could not read Linux boot identity: {exc}"
        ) from exc
    if not _is_canonical_boot_id(value):
        raise ProcessIdentityError("Linux boot identity has an invalid format")
    return value


@lru_cache(maxsize=1)
def _darwin_boot_session_uuid() -> str:
    library = ctypes.CDLL("/usr/lib/libSystem.B.dylib", use_errno=True)
    library.sysctlbyname.argtypes = [
        ctypes.c_char_p,
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_size_t),
        ctypes.c_void_p,
        ctypes.c_size_t,
    ]
    library.sysctlbyname.restype = ctypes.c_int
    name = b"kern.bootsessionuuid"
    length = ctypes.c_size_t()
    ctypes.set_errno(0)
    if library.sysctlbyname(name, None, ctypes.byref(length), None, 0) != 0:
        error = ctypes.get_errno()
        raise ProcessIdentityError(
            f"Could not read Darwin boot identity: {os.strerror(error)}"
        )
    if not 1 <= length.value <= 128:
        raise ProcessIdentityError("Darwin boot identity has an invalid size")
    payload = ctypes.create_string_buffer(length.value)
    ctypes.set_errno(0)
    if library.sysctlbyname(
        name, payload, ctypes.byref(length), None, 0
    ) != 0:
        error = ctypes.get_errno()
        raise ProcessIdentityError(
            f"Could not read Darwin boot identity: {os.strerror(error)}"
        )
    try:
        value = (
            payload.raw[: length.value]
            .rstrip(b"\0")
            .decode("ascii")
            .lower()
        )
    except UnicodeDecodeError as exc:
        raise ProcessIdentityError(
            "Darwin boot identity is not ASCII"
        ) from exc
    if not _is_canonical_boot_id(value):
        raise ProcessIdentityError("Darwin boot identity has an invalid format")
    return value


def darwin_boot_session_uuid() -> str:
    """Public accessor for the Darwin ``kern.bootsessionuuid`` read.

    A boot session UUID cannot change without a reboot, which would end this
    process, so the underlying read is cached for the process lifetime.
    """

    return _darwin_boot_session_uuid()


def is_canonical_boot_id(value: object) -> bool:
    """Return True when ``value`` is a canonical lowercase UUID boot id."""

    return _is_canonical_boot_id(value)


def _is_canonical_boot_id(value: object) -> bool:
    if not isinstance(value, str) or len(value) != 36:
        return False
    try:
        return str(uuid.UUID(value)) == value
    except (ValueError, AttributeError):
        return False


def read_process_identity(pid: int) -> ProcessIdentity | None:
    """Read exact POSIX PID identity without following filesystem links.

    Returns None if the process does not exist. Raises ProcessIdentityError
    for permission failures or platform-specific read failures that cannot
    prove the process is gone.
    """

    if type(pid) is not int or pid <= 1:
        raise ProcessIdentityError(f"Unsafe PID {pid!r}")

    if sys.platform == "darwin":
        return _read_darwin_process_identity(pid)

    proc_dir = Path("/proc") / str(pid)
    try:
        metadata = proc_dir.stat()
        raw = (proc_dir / "stat").read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ProcessIdentityError(
            f"Could not read process identity for PID {pid}: {exc}"
        ) from exc

    separator = raw.rfind(") ")
    if separator < 0:
        raise ProcessIdentityError(f"Could not parse PID {pid}")
    fields = raw[separator + 2 :].split()
    if len(fields) <= 19:
        raise ProcessIdentityError(f"PID {pid} identity is incomplete")

    try:
        state = fields[0]
        process_group_id = int(fields[2])
        start_ticks = int(fields[19])
    except (TypeError, ValueError) as exc:
        raise ProcessIdentityError(
            f"PID {pid} identity is invalid"
        ) from exc

    return ProcessIdentity(
        pid=pid,
        process_group_id=process_group_id,
        start_ticks=start_ticks,
        uid=metadata.st_uid,
        boot_id=_read_boot_id(),
        state=state,
    )


def _read_darwin_process_identity(pid: int) -> ProcessIdentity | None:
    info = _DarwinProcBsdInfo()
    expected_size = ctypes.sizeof(info)
    ctypes.set_errno(0)
    result = _darwin_libproc().proc_pidinfo(
        pid,
        3,  # PROC_PIDTBSDINFO
        0,
        ctypes.byref(info),
        expected_size,
    )
    if result == 0:
        error = ctypes.get_errno()
        if error in {0, errno.ENOENT, errno.ESRCH}:
            return None
        raise ProcessIdentityError(
            f"Could not read process identity for PID {pid}: "
            f"{os.strerror(error)}"
        )
    if result != expected_size or info.pbi_pid != pid:
        raise ProcessIdentityError(f"PID {pid} identity is incomplete")

    start_ticks = (
        int(info.pbi_start_tvsec) * 1_000_000 + int(info.pbi_start_tvusec)
    )
    if start_ticks <= 0:
        raise ProcessIdentityError(f"PID {pid} start identity is invalid")

    # Darwin's pbi_status uses SZOMB=5
    state = "Z" if int(info.pbi_status) == 5 else "R"

    return ProcessIdentity(
        pid=pid,
        process_group_id=int(info.pbi_pgid),
        start_ticks=start_ticks,
        uid=int(info.pbi_uid),
        boot_id=_read_boot_id(),
        state=state,
    )


_IDENTITY_PREFIX = "v1"


def encode_process_identity(identity: ProcessIdentity) -> str:
    """Serialize identity for persistence beside a PID column.

    The PID is encoded *inside* the value on purpose. A caller that writes a
    new PID without refreshing this column leaves an identity whose embedded
    PID no longer matches, which ``persisted_process_is_definitively_dead``
    treats as unusable rather than as proof of death. That makes a missed
    write site degrade to the conservative legacy probe instead of silently
    authorizing duplicate execution.
    """

    return (
        f"{_IDENTITY_PREFIX}:{identity.pid}:"
        f"{identity.start_ticks}:{identity.boot_id}"
    )


def capture_process_identity(pid: int | None) -> str | None:
    """Return an encoded identity for ``pid``, or None when unavailable.

    Never raises: identity is an optimization that lets recovery prove a PID
    is dead. When it cannot be read, callers persist the PID alone and stay
    fail-closed exactly as they did before identity capture existed.
    """

    if pid is None or type(pid) is not int or pid <= 1:
        return None
    try:
        identity = read_process_identity(pid)
    except Exception:
        # This helper runs in the launch persistence path. Platform/ctypes
        # surprises must disable the stronger recovery evidence, never turn a
        # successful subprocess spawn into a failed Task launch.
        return None
    if identity is None:
        return None
    return encode_process_identity(identity)


def decode_process_identity(
    persisted_identity: str | None,
    pid: int,
) -> tuple[int, str] | None:
    """Return ``(start_ticks, boot_id)`` when the identity is usable for ``pid``.

    Returns None when the value is absent, malformed, or was recorded for a
    different PID, meaning the caller must fall back to a conservative probe.
    """

    if not isinstance(persisted_identity, str):
        return None
    parts = persisted_identity.split(":")
    if len(parts) != 4 or parts[0] != _IDENTITY_PREFIX:
        return None
    try:
        persisted_pid = int(parts[1])
        start_ticks = int(parts[2])
    except ValueError:
        return None
    boot_id = parts[3]
    if persisted_pid != pid or start_ticks <= 0:
        return None
    if not _is_canonical_boot_id(boot_id):
        return None
    return start_ticks, boot_id


def persisted_process_liveness(
    pid: int | None,
    persisted_identity: str | None,
) -> str:
    """Classify a persisted PID as ``dead``, ``alive`` or ``unknown``.

    ``dead``     the exact recorded process is provably gone.
    ``alive``    a process answering this PID was positively observed.
    ``unknown``  death could not be proven; treat as still owning its work.

    Both ``alive`` and ``unknown`` are fail-closed for destructive cleanup.
    They are distinguished only so operator-facing messages can say whether a
    live process was actually seen, which changes what a human should do next.
    """

    if pid is None or type(pid) is not int or pid <= 0:
        return "unknown"

    decoded = decode_process_identity(persisted_identity, pid)
    if decoded is None:
        # No usable identity (legacy row, unreadable at launch, or recorded
        # for a different PID). Fall back to the bare probe.
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return "dead"
        except PermissionError:
            # Someone else's process answers to this PID; it is alive.
            return "alive"
        except OSError as exc:
            return "dead" if exc.errno == errno.ESRCH else "unknown"
        return "alive"

    persisted_start_ticks, persisted_boot_id = decoded

    # A different boot session proves the recorded process cannot survive:
    # every process from the previous boot died with it.
    try:
        current_boot_id = _read_boot_id()
    except Exception:
        return "unknown"
    if current_boot_id != persisted_boot_id:
        return "dead"

    try:
        current_identity = read_process_identity(pid)
    except Exception:
        return "unknown"
    if current_identity is None:
        return "dead"

    # The PID answers, but a different start time proves it was reused by an
    # unrelated process, so the recorded generation is gone.
    if current_identity.start_ticks != persisted_start_ticks:
        return "dead"
    return "alive"


def persisted_process_is_definitively_dead(
    pid: int | None,
    persisted_identity: str | None,
) -> bool:
    """Return True only when the exact recorded process is provably gone.

    This replaces the bare ``os.kill(pid, 0)`` probe, which cannot tell "this
    exact process is still alive" from "an unrelated process reused this PID
    number" and therefore pins tasks as permanently un-retryable after a host
    restart or PID wraparound.

    False means the recorded process may still be alive, or that death cannot
    be proven safely; destructive cleanup must preserve the owner evidence.
    """

    return persisted_process_liveness(pid, persisted_identity) == "dead"
