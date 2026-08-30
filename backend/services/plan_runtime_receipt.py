"""Durable provider-runtime identity and cleanup for Plan Agent attempts."""

from __future__ import annotations

import asyncio
import ctypes
import errno
import os
import signal
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from pathlib import Path

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError

from backend.models.plan_agent import (
    PlanAgentRun,
    PlanAgentRuntimeReceipt,
    PlanAgentStep,
)
from backend.services import process_identity
from backend.services.process_safety import require_safe_process_group_id


_RUNTIME_TOKEN_ENV = "CCM_PLAN_RUNTIME_TOKEN"
_PROC_ENV_LIMIT = 2 * 1024 * 1024
_PROCESS_REAP_TIMEOUT_SECONDS = 5.0


class _DarwinProcBsdInfo(ctypes.Structure):
    """Stable ``PROC_PIDTBSDINFO`` prefix used for exact Darwin PID identity."""

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
def _darwin_boot_session_uuid() -> str:
    """Delegate to the shared boot-identity primitive.

    ``process_identity`` owns the ``kern.bootsessionuuid`` read for every
    caller. Only the error taxonomy is translated here so durable Plan
    runtime callers keep failing closed on ``PlanRuntimeReceiptError``.
    """

    try:
        return process_identity.darwin_boot_session_uuid()
    except process_identity.ProcessIdentityError as exc:
        raise PlanRuntimeReceiptError(str(exc)) from exc


class PlanRuntimeReceiptError(RuntimeError):
    """Exact provider-runtime ownership or cleanup could not be proven."""


@dataclass(frozen=True)
class ProcessIdentity:
    pid: int
    process_group_id: int
    start_ticks: int
    uid: int
    boot_id: str
    state: str


@dataclass(frozen=True)
class RuntimeReceiptSnapshot:
    id: int
    run_id: int
    step_id: int
    run_generation: int
    attempt_index: int
    provider: str
    runtime_token: str
    prepared_boot_id: str
    prepared_start_ticks: int
    prepared_uid: int
    status: str
    process_id: int | None
    process_group_id: int | None
    process_start_ticks: int | None
    process_uid: int | None
    boot_id: str | None
    codex_home: str | None
    codex_thread_id: str | None
    cleanup_error: str | None = None
    cleaned_at: datetime | None = None


def _snapshot(receipt: PlanAgentRuntimeReceipt) -> RuntimeReceiptSnapshot:
    return RuntimeReceiptSnapshot(
        id=receipt.id,
        run_id=receipt.run_id,
        step_id=receipt.step_id,
        run_generation=receipt.run_generation,
        attempt_index=receipt.attempt_index,
        provider=receipt.provider,
        runtime_token=receipt.runtime_token,
        prepared_boot_id=receipt.prepared_boot_id,
        prepared_start_ticks=receipt.prepared_start_ticks,
        prepared_uid=receipt.prepared_uid,
        status=receipt.status,
        process_id=receipt.process_id,
        process_group_id=receipt.process_group_id,
        process_start_ticks=receipt.process_start_ticks,
        process_uid=receipt.process_uid,
        boot_id=receipt.boot_id,
        codex_home=receipt.codex_home,
        codex_thread_id=receipt.codex_thread_id,
        cleanup_error=receipt.cleanup_error,
        cleaned_at=receipt.cleaned_at,
    )


def _is_int_at_least(value: object, minimum: int) -> bool:
    return type(value) is int and value >= minimum


def _is_canonical_boot_id(value: object) -> bool:
    return process_identity.is_canonical_boot_id(value)


def _is_runtime_token(value: object) -> bool:
    return bool(
        isinstance(value, str)
        and len(value) == 32
        and all(character in "0123456789abcdef" for character in value)
    )


def _process_identity_shape(
    snapshot: RuntimeReceiptSnapshot,
) -> tuple[bool, bool]:
    values = (
        snapshot.process_id,
        snapshot.process_group_id,
        snapshot.process_start_ticks,
        snapshot.process_uid,
        snapshot.boot_id,
    )
    empty = all(value is None for value in values)
    complete = bool(
        _is_int_at_least(snapshot.process_id, 2)
        and _is_int_at_least(snapshot.process_group_id, 2)
        and _is_int_at_least(snapshot.process_start_ticks, 0)
        and _is_int_at_least(snapshot.process_uid, 0)
        and snapshot.process_uid == snapshot.prepared_uid
        and _is_canonical_boot_id(snapshot.boot_id)
        and snapshot.boot_id == snapshot.prepared_boot_id
    )
    return empty, complete


def _codex_identity_shape(
    snapshot: RuntimeReceiptSnapshot,
) -> tuple[bool, bool]:
    empty = snapshot.codex_home is None and snapshot.codex_thread_id is None
    complete = bool(
        isinstance(snapshot.codex_home, str)
        and snapshot.codex_home.strip()
        and isinstance(snapshot.codex_thread_id, str)
        and snapshot.codex_thread_id.strip()
    )
    return empty, complete


def runtime_receipt_shape_is_valid(snapshot: RuntimeReceiptSnapshot) -> bool:
    """Validate the full durable state, independent of database CHECK support."""

    if not (
        _is_int_at_least(snapshot.id, 1)
        and _is_int_at_least(snapshot.run_id, 1)
        and _is_int_at_least(snapshot.step_id, 1)
        and _is_int_at_least(snapshot.run_generation, 0)
        and _is_int_at_least(snapshot.attempt_index, 1)
        and snapshot.provider in {"claude", "codex"}
        and _is_runtime_token(snapshot.runtime_token)
        and _is_canonical_boot_id(snapshot.prepared_boot_id)
        and _is_int_at_least(snapshot.prepared_start_ticks, 0)
        and _is_int_at_least(snapshot.prepared_uid, 0)
    ):
        return False

    process_empty, process_complete = _process_identity_shape(snapshot)
    codex_empty, codex_complete = _codex_identity_shape(snapshot)
    if not (process_empty or process_complete):
        return False
    if snapshot.provider == "claude":
        provider_identity_valid = codex_empty
    else:
        provider_identity_valid = (codex_empty or codex_complete) and (
            process_empty or codex_complete
        )
    if not provider_identity_valid:
        return False

    if snapshot.status in {"prepared", "admitting"}:
        return bool(
            process_empty
            and codex_empty
            and snapshot.cleanup_error is None
            and snapshot.cleaned_at is None
        )
    if snapshot.status == "launching":
        return bool(
            snapshot.cleanup_error is None
            and snapshot.cleaned_at is None
            and (
                (
                    snapshot.provider == "claude"
                    and process_complete
                    and codex_empty
                )
                or (
                    snapshot.provider == "codex"
                    and codex_complete
                    and (process_empty or process_complete)
                )
            )
        )
    if snapshot.status == "cleaned":
        return bool(
            isinstance(snapshot.cleaned_at, datetime)
            and snapshot.cleanup_error is None
            and provider_identity_valid
        )
    if snapshot.status == "cleanup_failed":
        return bool(
            snapshot.cleaned_at is None
            and isinstance(snapshot.cleanup_error, str)
            and snapshot.cleanup_error.strip()
            and provider_identity_valid
        )
    return False


def _require_valid_runtime_snapshot(
    snapshot: RuntimeReceiptSnapshot,
    *,
    context: str,
) -> None:
    if not runtime_receipt_shape_is_valid(snapshot):
        raise PlanRuntimeReceiptError(
            f"Plan runtime receipt #{snapshot.id} has malformed {context} state"
        )


def new_prepared_runtime_receipt(
    step: PlanAgentStep,
    *,
    attempt_index: int,
) -> PlanAgentRuntimeReceipt:
    """Build the pre-provider receipt committed with a Step or retry slot."""

    if step.id is None:
        raise PlanRuntimeReceiptError("Plan Step must be flushed before runtime receipt")
    if (
        not _is_int_at_least(step.id, 1)
        or not _is_int_at_least(step.run_id, 1)
        or not _is_int_at_least(step.generation, 0)
        or not _is_int_at_least(attempt_index, 1)
        or step.provider not in {"claude", "codex"}
    ):
        raise PlanRuntimeReceiptError("Plan Step has invalid runtime receipt identity")
    return PlanAgentRuntimeReceipt(
        run_id=step.run_id,
        step_id=step.id,
        run_generation=step.generation,
        attempt_index=attempt_index,
        provider=step.provider,
        runtime_token=uuid.uuid4().hex,
        prepared_boot_id=_read_boot_id(),
        prepared_start_ticks=_read_current_start_ticks(),
        prepared_uid=os.getuid(),
        status="prepared",
    )


async def prepare_runtime_attempt(db_factory, step_id: int) -> RuntimeReceiptSnapshot:
    """Atomically claim one provider attempt before crossing admission."""

    async with db_factory() as db:
        step = await db.get(PlanAgentStep, step_id, with_for_update=True)
        if step is None or step.status != "running":
            raise PlanRuntimeReceiptError(
                f"Plan Step #{step_id} is not available for provider launch"
            )
        receipts = list(
            (
                await db.execute(
                    select(PlanAgentRuntimeReceipt)
                    .where(PlanAgentRuntimeReceipt.step_id == step.id)
                    .order_by(PlanAgentRuntimeReceipt.attempt_index)
                    .with_for_update()
                )
            ).scalars()
        )
        latest = receipts[-1] if receipts else None
        for expected_index, candidate in enumerate(receipts, start=1):
            candidate_snapshot = _snapshot(candidate)
            if (
                not runtime_receipt_shape_is_valid(candidate_snapshot)
                or candidate.run_id != step.run_id
                or candidate.step_id != step.id
                or candidate.run_generation != step.generation
                or candidate.provider != step.provider
                or candidate.attempt_index != expected_index
            ):
                await db.rollback()
                raise PlanRuntimeReceiptError(
                    f"Plan Step #{step_id} has invalid runtime receipt identity"
                )
        if latest is not None and latest.status == "prepared":
            claimed = await db.execute(
                update(PlanAgentRuntimeReceipt)
                .where(
                    PlanAgentRuntimeReceipt.id == latest.id,
                    PlanAgentRuntimeReceipt.status == "prepared",
                )
                .values(status="admitting", updated_at=datetime.utcnow())
            )
            if claimed.rowcount != 1:
                await db.rollback()
                raise PlanRuntimeReceiptError(
                    f"Plan Step #{step_id} runtime attempt was claimed concurrently"
                )
            await db.commit()
            await db.refresh(latest)
            snapshot = _snapshot(latest)
            _require_valid_runtime_snapshot(snapshot, context="claimed")
            return snapshot
        if latest is not None and latest.status != "cleaned":
            # ``rollback()`` expires ORM state.  Preserve the audit identity
            # before rolling back so this deterministic conflict path never
            # attempts implicit async I/O while formatting the error.
            latest_attempt_index = latest.attempt_index
            latest_status = latest.status
            await db.rollback()
            raise PlanRuntimeReceiptError(
                f"Plan Step #{step_id} retained unclean runtime attempt "
                f"#{latest_attempt_index} ({latest_status})"
            )
        receipt = new_prepared_runtime_receipt(
            step,
            attempt_index=(latest.attempt_index + 1 if latest is not None else 1),
        )
        receipt.status = "admitting"
        db.add(receipt)
        try:
            await db.commit()
        except IntegrityError as exc:
            await db.rollback()
            raise PlanRuntimeReceiptError(
                f"Plan Step #{step_id} runtime attempt was claimed concurrently"
            ) from exc
        await db.refresh(receipt)
        snapshot = _snapshot(receipt)
        _require_valid_runtime_snapshot(snapshot, context="claimed")
        return snapshot


def runtime_token_environment(snapshot: RuntimeReceiptSnapshot) -> dict[str, str]:
    _require_valid_runtime_snapshot(snapshot, context="environment")
    return {_RUNTIME_TOKEN_ENV: snapshot.runtime_token}


def _read_boot_id() -> str:
    if sys.platform == "darwin":
        return _darwin_boot_session_uuid()
    try:
        value = Path("/proc/sys/kernel/random/boot_id").read_text(
            encoding="utf-8"
        ).strip().lower()
    except OSError as exc:
        raise PlanRuntimeReceiptError(f"Could not read Linux boot identity: {exc}") from exc
    if not _is_canonical_boot_id(value):
        raise PlanRuntimeReceiptError("Linux boot identity has an invalid format")
    return value


def _read_current_start_ticks() -> int:
    """Return a conservative /proc start-time boundary for future children."""

    if sys.platform == "darwin":
        # Darwin's PROC_PIDTBSDINFO exposes process start time in epoch
        # microseconds. Keep the same conservative overlap used by Linux so a
        # launch race can include an extra candidate but never hide a child.
        return max(0, time.time_ns() // 1_000 - 2_000_000)
    try:
        clock_ticks = int(os.sysconf("SC_CLK_TCK"))
        uptime_text = Path("/proc/uptime").read_text(encoding="utf-8").split()[0]
        # Keep a two-tick overlap so conversion/measurement races can only
        # create a harmless extra candidate, never hide a token-bearing child.
        ticks = int(float(uptime_text) * clock_ticks) - 2
    except (OSError, ValueError, IndexError) as exc:
        raise PlanRuntimeReceiptError(
            f"Could not read Linux process start boundary: {exc}"
        ) from exc
    if clock_ticks <= 0 or ticks < -2:
        raise PlanRuntimeReceiptError("Linux process start boundary is invalid")
    return max(0, ticks)


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
        raise PlanRuntimeReceiptError(
            f"Could not read Plan runtime process identity for PID {pid}: "
            f"{os.strerror(error)}"
        )
    if result != expected_size or info.pbi_pid != pid:
        raise PlanRuntimeReceiptError(
            f"Plan runtime PID {pid} identity is incomplete"
        )
    try:
        process_group_id = require_safe_process_group_id(
            int(info.pbi_pgid),
            context="durable Plan runtime",
        )
    except (TypeError, ValueError) as exc:
        raise PlanRuntimeReceiptError(
            f"Plan runtime PID {pid} identity is invalid"
        ) from exc
    start_ticks = (
        int(info.pbi_start_tvsec) * 1_000_000
        + int(info.pbi_start_tvusec)
    )
    if start_ticks <= 0:
        raise PlanRuntimeReceiptError(
            f"Plan runtime PID {pid} start identity is invalid"
        )
    # Darwin's pbi_status uses SZOMB=5. Other states are live for the receipt
    # protocol; their exact scheduler distinction is irrelevant here.
    state = "Z" if int(info.pbi_status) == 5 else "R"
    return ProcessIdentity(
        pid=pid,
        process_group_id=process_group_id,
        start_ticks=start_ticks,
        uid=int(info.pbi_uid),
        boot_id=_read_boot_id(),
        state=state,
    )


def read_process_identity(pid: int) -> ProcessIdentity | None:
    """Read one exact POSIX PID identity without following filesystem links."""

    if type(pid) is not int or pid <= 1:
        raise PlanRuntimeReceiptError(f"Unsafe Plan runtime PID {pid!r}")
    if sys.platform == "darwin":
        return _read_darwin_process_identity(pid)
    proc_dir = Path("/proc") / str(pid)
    try:
        metadata = proc_dir.stat()
        raw = (proc_dir / "stat").read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise PlanRuntimeReceiptError(
            f"Could not read Plan runtime process identity for PID {pid}: {exc}"
        ) from exc
    separator = raw.rfind(") ")
    if separator < 0:
        raise PlanRuntimeReceiptError(f"Could not parse Plan runtime PID {pid}")
    fields = raw[separator + 2 :].split()
    if len(fields) <= 19:
        raise PlanRuntimeReceiptError(f"Plan runtime PID {pid} identity is incomplete")
    try:
        state = fields[0]
        process_group_id = require_safe_process_group_id(
            int(fields[2]),
            context="durable Plan runtime",
        )
        start_ticks = int(fields[19])
    except (TypeError, ValueError) as exc:
        raise PlanRuntimeReceiptError(
            f"Plan runtime PID {pid} identity is invalid"
        ) from exc
    return ProcessIdentity(
        pid=pid,
        process_group_id=process_group_id,
        start_ticks=start_ticks,
        uid=metadata.st_uid,
        boot_id=_read_boot_id(),
        state=state,
    )


async def _locked_receipt(db_factory, receipt_id: int):
    db = db_factory()
    receipt = await db.get(
        PlanAgentRuntimeReceipt,
        receipt_id,
        with_for_update=True,
        populate_existing=True,
    )
    if receipt is None:
        await db.close()
        raise PlanRuntimeReceiptError(
            f"Plan runtime receipt #{receipt_id} disappeared"
        )
    try:
        _require_valid_runtime_snapshot(_snapshot(receipt), context="persisted")
    except BaseException:
        await db.close()
        raise
    return db, receipt


async def bind_claude_process(
    db_factory,
    receipt_id: int,
    pid: int,
) -> RuntimeReceiptSnapshot:
    identity = read_process_identity(pid)
    if identity is None or identity.state == "Z":
        raise PlanRuntimeReceiptError(
            f"Claude Plan runtime PID {pid} was not live at ownership commit"
        )
    if identity.process_group_id != pid:
        raise PlanRuntimeReceiptError(
            "Claude Plan runtime did not start in its own process group"
        )
    db, receipt = await _locked_receipt(db_factory, receipt_id)
    try:
        expected = (
            identity.pid,
            identity.process_group_id,
            identity.start_ticks,
            identity.uid,
            identity.boot_id,
        )
        current = (
            receipt.process_id,
            receipt.process_group_id,
            receipt.process_start_ticks,
            receipt.process_uid,
            receipt.boot_id,
        )
        if receipt.provider != "claude":
            raise PlanRuntimeReceiptError("Claude process bound to a non-Claude receipt")
        if receipt.status == "launching" and current == expected:
            await db.commit()
            return _snapshot(receipt)
        if receipt.status != "admitting" or any(value is not None for value in current):
            raise PlanRuntimeReceiptError(
                f"Plan runtime receipt #{receipt_id} changed before process binding"
            )
        receipt.process_id = identity.pid
        receipt.process_group_id = identity.process_group_id
        receipt.process_start_ticks = identity.start_ticks
        receipt.process_uid = identity.uid
        receipt.boot_id = identity.boot_id
        receipt.status = "launching"
        receipt.cleanup_error = None
        receipt.updated_at = datetime.utcnow()
        _require_valid_runtime_snapshot(_snapshot(receipt), context="bound")
        await db.commit()
        return _snapshot(receipt)
    except BaseException:
        await db.rollback()
        raise
    finally:
        await db.close()


async def bind_codex_thread(
    db_factory,
    receipt_id: int,
    *,
    codex_home: str,
    thread_id: str,
) -> RuntimeReceiptSnapshot:
    if not codex_home or not thread_id:
        raise PlanRuntimeReceiptError("Codex Plan runtime identity is incomplete")
    db, receipt = await _locked_receipt(db_factory, receipt_id)
    try:
        if receipt.provider != "codex":
            raise PlanRuntimeReceiptError("Codex thread bound to a non-Codex receipt")
        current = (receipt.codex_home, receipt.codex_thread_id)
        expected = (codex_home, thread_id)
        if receipt.status == "launching" and current == expected:
            await db.commit()
            return _snapshot(receipt)
        if receipt.status != "admitting" or current != (None, None):
            raise PlanRuntimeReceiptError(
                f"Plan runtime receipt #{receipt_id} changed before thread binding"
            )
        receipt.codex_home = codex_home
        receipt.codex_thread_id = thread_id
        receipt.status = "launching"
        receipt.cleanup_error = None
        receipt.updated_at = datetime.utcnow()
        _require_valid_runtime_snapshot(_snapshot(receipt), context="bound")
        await db.commit()
        return _snapshot(receipt)
    except BaseException:
        await db.rollback()
        raise
    finally:
        await db.close()


async def bind_codex_transport(
    db_factory,
    receipt_id: int,
    *,
    pid: int,
    codex_home: str,
    thread_id: str,
) -> RuntimeReceiptSnapshot:
    identity = read_process_identity(pid)
    if identity is None or identity.state == "Z":
        raise PlanRuntimeReceiptError(
            f"Codex app-server PID {pid} was not live at turn admission"
        )
    db, receipt = await _locked_receipt(db_factory, receipt_id)
    try:
        if (
            receipt.provider != "codex"
            or receipt.status != "launching"
            or receipt.codex_home != codex_home
            or receipt.codex_thread_id != thread_id
        ):
            raise PlanRuntimeReceiptError(
                f"Plan runtime receipt #{receipt_id} lost its Codex thread owner"
            )
        current = (
            receipt.process_id,
            receipt.process_group_id,
            receipt.process_start_ticks,
            receipt.process_uid,
            receipt.boot_id,
        )
        expected = (
            identity.pid,
            identity.process_group_id,
            identity.start_ticks,
            identity.uid,
            identity.boot_id,
        )
        if all(value is None for value in current):
            receipt.process_id = identity.pid
            receipt.process_group_id = identity.process_group_id
            receipt.process_start_ticks = identity.start_ticks
            receipt.process_uid = identity.uid
            receipt.boot_id = identity.boot_id
            receipt.updated_at = datetime.utcnow()
            _require_valid_runtime_snapshot(
                _snapshot(receipt),
                context="transport-bound",
            )
            await db.commit()
            return _snapshot(receipt)
        if current != expected:
            raise PlanRuntimeReceiptError(
                f"Plan runtime receipt #{receipt_id} Codex transport changed"
            )
        await db.commit()
        return _snapshot(receipt)
    except BaseException:
        await db.rollback()
        raise
    finally:
        await db.close()


def _runtime_identity_tuple(snapshot: RuntimeReceiptSnapshot) -> tuple[object, ...]:
    return (
        snapshot.id,
        snapshot.run_id,
        snapshot.step_id,
        snapshot.run_generation,
        snapshot.attempt_index,
        snapshot.provider,
        snapshot.runtime_token,
        snapshot.prepared_boot_id,
        snapshot.prepared_start_ticks,
        snapshot.prepared_uid,
        snapshot.process_id,
        snapshot.process_group_id,
        snapshot.process_start_ticks,
        snapshot.process_uid,
        snapshot.boot_id,
        snapshot.codex_home,
        snapshot.codex_thread_id,
    )


def _require_expected_runtime_identity(
    receipt: PlanAgentRuntimeReceipt,
    expected: RuntimeReceiptSnapshot,
) -> None:
    current = _snapshot(receipt)
    _require_valid_runtime_snapshot(expected, context="expected cleanup")
    _require_valid_runtime_snapshot(current, context="persisted cleanup")
    if _runtime_identity_tuple(current) != _runtime_identity_tuple(expected):
        raise PlanRuntimeReceiptError(
            f"Plan runtime receipt #{expected.id} changed during cleanup"
        )


async def mark_runtime_cleaned(
    db_factory,
    expected: RuntimeReceiptSnapshot,
) -> None:
    db, receipt = await _locked_receipt(db_factory, expected.id)
    try:
        _require_expected_runtime_identity(receipt, expected)
        if receipt.status == "cleaned":
            _require_valid_runtime_snapshot(
                _snapshot(receipt),
                context="cleaned",
            )
            await db.commit()
            return
        # A concurrent reconciler may fail closed while the retained live
        # runner is still completing the exact same cleanup. Once that runner
        # has actually removed the process/thread, its success is authoritative
        # as long as the durable runtime identity still matches.
        if receipt.status not in {expected.status, "cleanup_failed"}:
            raise PlanRuntimeReceiptError(
                f"Plan runtime receipt #{expected.id} state changed during cleanup"
            )
        if receipt.status != "cleaned":
            now = datetime.utcnow()
            receipt.status = "cleaned"
            receipt.cleanup_error = None
            receipt.cleaned_at = now
            receipt.updated_at = now
        _require_valid_runtime_snapshot(
            _snapshot(receipt),
            context="cleaned",
        )
        await db.commit()
    except BaseException:
        await db.rollback()
        raise
    finally:
        await db.close()


async def mark_runtime_cleanup_failed(
    db_factory,
    expected: RuntimeReceiptSnapshot,
    error: BaseException | str,
) -> None:
    db, receipt = await _locked_receipt(db_factory, expected.id)
    try:
        _require_expected_runtime_identity(receipt, expected)
        if receipt.status == "cleaned":
            _require_valid_runtime_snapshot(
                _snapshot(receipt),
                context="cleaned",
            )
            await db.commit()
            return
        if receipt.status != expected.status:
            raise PlanRuntimeReceiptError(
                f"Plan runtime receipt #{expected.id} state changed during cleanup"
            )
        if receipt.status != "cleaned":
            error_text = str(error).strip() or type(error).__name__
            receipt.status = "cleanup_failed"
            receipt.cleanup_error = error_text[:4000]
            receipt.cleaned_at = None
            receipt.updated_at = datetime.utcnow()
        _require_valid_runtime_snapshot(
            _snapshot(receipt),
            context="cleanup-failed",
        )
        await db.commit()
    except BaseException:
        await db.rollback()
        raise
    finally:
        await db.close()


def _receipt_identity_matches(
    receipt: RuntimeReceiptSnapshot,
    identity: ProcessIdentity | None,
) -> bool:
    return bool(
        identity is not None
        and identity.state != "Z"
        and receipt.process_id == identity.pid
        and receipt.process_group_id == identity.process_group_id
        and receipt.process_start_ticks == identity.start_ticks
        and receipt.process_uid == identity.uid
        and receipt.boot_id == identity.boot_id
    )


def _group_alive(process_group_id: int) -> bool:
    process_group_id = require_safe_process_group_id(
        process_group_id,
        context="durable Plan runtime recovery",
    )
    try:
        os.killpg(process_group_id, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _token_process_identities(
    receipt: RuntimeReceiptSnapshot,
) -> list[ProcessIdentity]:
    if not sys.platform.startswith("linux"):
        # Normal in-memory Darwin cleanup uses the exact retained process or
        # Codex thread and marks the receipt cleaned directly. After a hard
        # crash Darwin does not expose a race-safe equivalent of /proc/environ;
        # retain the receipt fail-closed instead of guessing token ownership.
        raise PlanRuntimeReceiptError(
            "Plan runtime token recovery requires Linux /proc"
        )
    marker = f"{_RUNTIME_TOKEN_ENV}={receipt.runtime_token}".encode("utf-8")
    if receipt.prepared_uid != os.getuid():
        raise PlanRuntimeReceiptError(
            "Plan runtime receipt belongs to a different operating-system user"
        )
    if receipt.prepared_boot_id != _read_boot_id():
        return []
    identities: list[ProcessIdentity] = []
    proc = Path("/proc")
    try:
        candidates = list(proc.iterdir())
    except OSError as exc:
        raise PlanRuntimeReceiptError(f"Could not enumerate /proc: {exc}") from exc
    for candidate in candidates:
        if not candidate.name.isdigit():
            continue
        try:
            metadata = candidate.stat()
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise PlanRuntimeReceiptError(
                f"Could not inspect /proc/{candidate.name}: {exc}"
            ) from exc
        if metadata.st_uid != receipt.prepared_uid:
            continue
        identity = read_process_identity(int(candidate.name))
        if identity is None or identity.state in {"Z", "X"}:
            continue
        # A process that predates the committed receipt boundary cannot have
        # inherited its freshly generated token.  Crucially, skip it before
        # opening environ: systemd --user and similar same-UID supervisors may
        # intentionally deny that read even to their service account.
        if identity.start_ticks < receipt.prepared_start_ticks:
            continue
        try:
            flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            fd = os.open(candidate / "environ", flags)
            try:
                payload = os.read(fd, _PROC_ENV_LIMIT + 1)
            finally:
                os.close(fd)
        except FileNotFoundError:
            continue
        except PermissionError as exc:
            raise PlanRuntimeReceiptError(
                "Could not prove Plan runtime token absence because a same-user "
                f"process environment is unreadable: PID {candidate.name}"
            ) from exc
        except OSError:
            # Kernel threads and already-dead processes have no usable
            # environment. Re-read their identity; only a live user process
            # makes absence uncertain.
            current = read_process_identity(int(candidate.name))
            if current is not None and current.state not in {"Z", "X"}:
                raise PlanRuntimeReceiptError(
                    "Could not prove Plan runtime token absence for live PID "
                    f"{candidate.name}"
                )
            continue
        if len(payload) > _PROC_ENV_LIMIT:
            raise PlanRuntimeReceiptError(
                f"Process environment exceeded audit limit for PID {candidate.name}"
            )
        if marker not in payload.split(b"\0"):
            continue
        current = read_process_identity(int(candidate.name))
        if current is None or current.state in {"Z", "X"}:
            continue
        if current != identity:
            raise PlanRuntimeReceiptError(
                f"PID {candidate.name} changed while auditing Plan runtime token"
            )
        identities.append(current)
    return identities


async def _terminate_token_groups(receipt: RuntimeReceiptSnapshot) -> None:
    deadline = asyncio.get_running_loop().time() + _PROCESS_REAP_TIMEOUT_SECONDS
    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGKILL):
        identities = _token_process_identities(receipt)
        if not identities:
            break
        groups = {identity.process_group_id for identity in identities}
        if receipt.process_group_id is not None and groups != {
            receipt.process_group_id
        }:
            raise PlanRuntimeReceiptError(
                "Plan runtime token appeared in an unexpected process group"
            )
        for process_group_id in groups:
            try:
                os.killpg(
                    require_safe_process_group_id(
                        process_group_id,
                        context="durable Plan runtime cleanup",
                    ),
                    signum,
                )
            except ProcessLookupError:
                pass
        stage_deadline = min(deadline, asyncio.get_running_loop().time() + 1.5)
        while asyncio.get_running_loop().time() < stage_deadline:
            if not _token_process_identities(receipt):
                break
            await asyncio.sleep(0.05)

    if _token_process_identities(receipt):
        raise PlanRuntimeReceiptError("Plan runtime token survived SIGKILL")
    if (
        receipt.process_group_id is not None
        and receipt.boot_id == _read_boot_id()
        and _group_alive(receipt.process_group_id)
    ):
        raise PlanRuntimeReceiptError(
            "Plan runtime process group remains live without token-bearing evidence"
        )


async def _reconcile_claude_receipt(
    db_factory,
    receipt: RuntimeReceiptSnapshot,
) -> bool:
    try:
        if receipt.status == "prepared":
            # ``prepare_runtime_attempt`` advances the receipt to admitting
            # before exposing its token to a provider environment. A receipt
            # that is still prepared therefore proves launch never began and
            # can be closed without an operating-system process scan.
            await mark_runtime_cleaned(db_factory, receipt)
            return True
        if receipt.boot_id is not None and receipt.boot_id != _read_boot_id():
            await mark_runtime_cleaned(db_factory, receipt)
            return True
        identities = _token_process_identities(receipt)
        if identities:
            await _terminate_token_groups(receipt)
        elif receipt.process_group_id is not None:
            identity = (
                read_process_identity(receipt.process_id)
                if receipt.process_id is not None
                else None
            )
            if _receipt_identity_matches(receipt, identity):
                raise PlanRuntimeReceiptError(
                    "Exact Claude runtime is live but lost its durable token"
                )
            if _group_alive(receipt.process_group_id):
                raise PlanRuntimeReceiptError(
                    "Claude runtime group is live but exact ownership is ambiguous"
                )
        await mark_runtime_cleaned(db_factory, receipt)
        return True
    except Exception as exc:
        try:
            await mark_runtime_cleanup_failed(db_factory, receipt, exc)
        except PlanRuntimeReceiptError:
            pass
        return False


def _codex_thread_absent(exc: BaseException) -> bool:
    message = str(exc).lower()
    return any(
        marker in message
        for marker in (
            "thread not found",
            "unknown thread",
            "no such thread",
            "thread does not exist",
            "thread already deleted",
            "no rollout found for thread id",
        )
    )


def _exact_codex_transport_is_live(
    receipt: RuntimeReceiptSnapshot,
) -> bool:
    if receipt.process_id is None or receipt.process_group_id is None:
        return False
    if receipt.boot_id != _read_boot_id():
        return False
    identity = read_process_identity(receipt.process_id)
    if identity is None or identity.state == "Z":
        if _group_alive(receipt.process_group_id):
            raise PlanRuntimeReceiptError(
                "Codex transport leader is gone but its process group remains live"
            )
        return False
    if not _receipt_identity_matches(receipt, identity):
        if _group_alive(receipt.process_group_id):
            raise PlanRuntimeReceiptError(
                "Codex transport PID/PGID was reused; refusing to signal it"
            )
        return False
    return True


async def _reconcile_codex_receipt(
    db_factory,
    instance_manager,
    receipt: RuntimeReceiptSnapshot,
    *,
    allow_transport_kill: bool,
) -> bool:
    try:
        if receipt.codex_thread_id is None and receipt.codex_home is None:
            # on_thread_started is invoked and durably awaited before
            # turn/start. A still-prepared receipt therefore never admitted
            # model input, even if an idle thread was created before a crash.
            if receipt.status not in {"prepared", "admitting", "cleanup_failed"}:
                raise PlanRuntimeReceiptError(
                    "Codex runtime lost its exact thread/home identity"
                )
            await mark_runtime_cleaned(db_factory, receipt)
            return True
        if not receipt.codex_thread_id or not receipt.codex_home:
            raise PlanRuntimeReceiptError("Codex runtime identity is incomplete")
        registry = instance_manager._ensure_codex_app_server_registry()
        if allow_transport_kill and _exact_codex_transport_is_live(receipt):
            # ``process_id`` identifies the account-level app-server, not this
            # Plan attempt.  After a Manager restart the new registry can be
            # empty while that old transport still serves unrelated turns.
            # Losing the in-memory adapter therefore loses the only proof
            # needed for an exact interrupt: a durable PID/PGID match is *not*
            # authority to signal a shared process group.  Keep the receipt
            # fail-closed until the old transport exits; a later reconciliation
            # can then delete only the disposable native thread below.
            raise PlanRuntimeReceiptError(
                "Codex Plan transport is still live but exact attempt cleanup "
                "cannot be proven from shared transport identity"
            )
        try:
            async with instance_manager.codex_home_app_server_guard(
                receipt.codex_home
            ) as admitted_home:
                await registry.delete_thread(
                    admitted_home,
                    receipt.codex_thread_id,
                )
        except Exception as exc:
            if not _codex_thread_absent(exc):
                raise
        await mark_runtime_cleaned(db_factory, receipt)
        return True
    except Exception as exc:
        try:
            await mark_runtime_cleanup_failed(db_factory, receipt, exc)
        except PlanRuntimeReceiptError:
            pass
        return False


async def _runtime_generation_snapshots(
    db,
    *,
    run_id: int,
    generation: int,
) -> list[RuntimeReceiptSnapshot] | None:
    steps = list(
        (
            await db.execute(
                select(PlanAgentStep).where(
                    PlanAgentStep.run_id == run_id,
                    PlanAgentStep.generation == generation,
                )
            )
        ).scalars()
    )
    step_ids = {step.id for step in steps}
    receipts_for_generation = list(
        (
            await db.execute(
                select(PlanAgentRuntimeReceipt).where(
                    PlanAgentRuntimeReceipt.run_id == run_id,
                    PlanAgentRuntimeReceipt.run_generation == generation,
                )
            )
        ).scalars()
    )
    if not steps:
        # Step creation precedes every provider boundary.  Orphan receipts for
        # the generation are corruption, not proof that no provider ran.
        return [] if not receipts_for_generation else None
    receipts_for_steps = list(
        (
            await db.execute(
                select(PlanAgentRuntimeReceipt).where(
                    PlanAgentRuntimeReceipt.step_id.in_(step_ids)
                )
            )
        ).scalars()
    )
    if {item.id for item in receipts_for_generation} != {
        item.id for item in receipts_for_steps
    }:
        return None

    grouped: dict[int, list[PlanAgentRuntimeReceipt]] = {
        step.id: [] for step in steps
    }
    step_by_id = {step.id: step for step in steps}
    for receipt in receipts_for_steps:
        step = step_by_id.get(receipt.step_id)
        snapshot = _snapshot(receipt)
        if (
            step is None
            or not runtime_receipt_shape_is_valid(snapshot)
            or receipt.run_id != run_id
            or receipt.run_generation != generation
            or receipt.provider != step.provider
        ):
            return None
        grouped[step.id].append(receipt)

    snapshots: list[RuntimeReceiptSnapshot] = []
    for step in steps:
        attempts = sorted(grouped[step.id], key=lambda item: item.attempt_index)
        if not attempts or [item.attempt_index for item in attempts] != list(
            range(1, len(attempts) + 1)
        ):
            return None
        snapshots.extend(_snapshot(item) for item in attempts)
    return snapshots


async def runtime_generation_is_clean(
    db,
    *,
    run_id: int,
    generation: int,
) -> bool:
    snapshots = await _runtime_generation_snapshots(
        db,
        run_id=run_id,
        generation=generation,
    )
    return snapshots is not None and all(
        receipt.status == "cleaned"
        and runtime_receipt_shape_is_valid(receipt)
        for receipt in snapshots
    )


async def reconcile_runtime_generation(
    db_factory,
    instance_manager,
    *,
    run_id: int,
    generation: int,
    allow_transport_kill: bool,
) -> bool:
    if not await _discard_cleaned_receipts_from_reused_run_id(
        db_factory,
        run_id=run_id,
    ):
        return False
    async with db_factory() as db:
        snapshots = await _runtime_generation_snapshots(
            db,
            run_id=run_id,
            generation=generation,
        )
        if snapshots is None:
            return False

    for receipt in snapshots:
        if receipt.status == "cleaned":
            continue
        if receipt.provider == "claude":
            cleaned = await _reconcile_claude_receipt(db_factory, receipt)
        elif receipt.provider == "codex":
            cleaned = await _reconcile_codex_receipt(
                db_factory,
                instance_manager,
                receipt,
                allow_transport_kill=allow_transport_kill,
            )
        else:
            await mark_runtime_cleanup_failed(
                db_factory,
                receipt,
                f"Unsupported Plan runtime provider {receipt.provider!r}",
            )
            cleaned = False
        if not cleaned:
            return False

    async with db_factory() as db:
        return await runtime_generation_is_clean(
            db,
            run_id=run_id,
            generation=generation,
        )


async def _discard_cleaned_receipts_from_reused_run_id(
    db_factory,
    *,
    run_id: int,
) -> bool:
    """Remove terminal receipts that provably predate the current Run row.

    SQLite can reuse an INTEGER primary key after a deleted Plan graph. If an
    interrupted/manual cleanup left runtime receipts behind, a later Run may
    inherit their ``run_id`` even though those receipts belong to an older
    aggregate. Only already-cleaned receipts created before the current Run
    are safe to discard. Any non-terminal predecessor remains fail-closed.
    """

    async with db_factory() as db:
        run = await db.get(
            PlanAgentRun,
            run_id,
            with_for_update=True,
            populate_existing=True,
        )
        if run is None:
            await db.rollback()
            return False
        stale_receipts = list(
            (
                await db.execute(
                    select(PlanAgentRuntimeReceipt)
                    .where(
                        PlanAgentRuntimeReceipt.run_id == run_id,
                        PlanAgentRuntimeReceipt.created_at < run.created_at,
                    )
                    .order_by(PlanAgentRuntimeReceipt.id)
                    .with_for_update()
                )
            ).scalars()
        )
        if any(receipt.status != "cleaned" for receipt in stale_receipts):
            await db.rollback()
            return False
        for receipt in stale_receipts:
            await db.delete(receipt)
        await db.commit()
        return True


async def runtime_run_is_clean(db, *, run_id: int) -> bool:
    step_generations = set(
        (
            await db.execute(
                select(PlanAgentStep.generation).where(
                    PlanAgentStep.run_id == run_id
                )
            )
        ).scalars()
    )
    receipt_generations = set(
        (
            await db.execute(
                select(PlanAgentRuntimeReceipt.run_generation).where(
                    PlanAgentRuntimeReceipt.run_id == run_id
                )
            )
        ).scalars()
    )
    if step_generations != receipt_generations and (step_generations or receipt_generations):
        return False
    for generation in sorted(step_generations):
        if not await runtime_generation_is_clean(
            db,
            run_id=run_id,
            generation=generation,
        ):
            return False
    return True


async def reconcile_runtime_run(
    db_factory,
    instance_manager,
    *,
    run_id: int,
    allow_transport_kill: bool,
) -> bool:
    async with db_factory() as db:
        generations = set(
            (
                await db.execute(
                    select(PlanAgentStep.generation).where(
                        PlanAgentStep.run_id == run_id
                    )
                )
            ).scalars()
        ) | set(
            (
                await db.execute(
                    select(PlanAgentRuntimeReceipt.run_generation).where(
                        PlanAgentRuntimeReceipt.run_id == run_id
                    )
                )
            ).scalars()
        )
    for generation in sorted(generations):
        if not await reconcile_runtime_generation(
            db_factory,
            instance_manager,
            run_id=run_id,
            generation=generation,
            allow_transport_kill=allow_transport_kill,
        ):
            return False
    async with db_factory() as db:
        return await runtime_run_is_clean(db, run_id=run_id)


async def reconcile_runtime_receipt(
    db_factory,
    instance_manager,
    *,
    receipt_id: int,
    allow_transport_kill: bool,
) -> bool:
    """Idempotently reconcile one known attempt after launch/start failure."""

    async with db_factory() as db:
        receipt = await db.get(
            PlanAgentRuntimeReceipt,
            receipt_id,
            populate_existing=True,
        )
        if receipt is None:
            return False
        snapshot = _snapshot(receipt)
        if not runtime_receipt_shape_is_valid(snapshot):
            return False
    if snapshot.status == "cleaned":
        return True
    if snapshot.provider == "claude":
        return await _reconcile_claude_receipt(db_factory, snapshot)
    if snapshot.provider == "codex":
        return await _reconcile_codex_receipt(
            db_factory,
            instance_manager,
            snapshot,
            allow_transport_kill=allow_transport_kill,
        )
    await mark_runtime_cleanup_failed(
        db_factory,
        snapshot,
        f"Unsupported Plan runtime provider {snapshot.provider!r}",
    )
    return False
