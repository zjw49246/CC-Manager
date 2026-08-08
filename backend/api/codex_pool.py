"""API endpoints for Codex account pool management."""

import asyncio
import base64
import json
import logging
import math
import os
import re
import secrets
import signal
import shutil
import stat
import tempfile
import time
import uuid
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from backend.api.deps import require_admin
from backend.services.codex_app_server import CodexAppServerBusyError
from backend.services.cloudrouter_accounts import is_api_auth_kind
from backend.services.login_runtime import (
    LoginRuntimeError,
    ensure_login_runtime,
    login_child_environment,
    login_lock,
)
from backend.services.process_safety import (
    UnsafeProcessGroupError,
    require_safe_process_group_id,
)

router = APIRouter(prefix="/api/codex-pool", tags=["codex-pool"])
logger = logging.getLogger(__name__)

# Background task state
_relogin_state: dict[str, dict] = {}
_add_state: dict[str, dict] = {}
_login_lock = login_lock
_login_attempts: dict[str, dict] = {}
ACTIVE_LOGIN_STATUSES = {
    "running", "awaiting_otp", "verifying_otp", "finalizing",
}
LOGIN_EVENT_PREFIX = "CCM_CODEX_LOGIN_EVENT:"
# ``mailcatcher`` is the source-level name.  Domain-shaped values remain
# accepted for saved credentials created by older CCM builds; MailCatcher's
# query token itself identifies the account and is not restricted to mail.com.
MAIL_PROVIDERS = {"171mail", "mailcatcher", "mailcom", "onet", "gazeta"}
LOGIN_TRANSACTION_VERSION = 1
LOGIN_TRANSACTION_DIR = "login-transactions"
LOGIN_REAP_TIMEOUT_SECONDS = 15.0


class LoginProcessNotTerminal(RuntimeError):
    """Wrapper termination could not be proven; credential files stay frozen."""


def _fsync_directory(path: Path) -> None:
    """Durably persist a create/replace/unlink in a private directory."""

    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_regular_file_and_parent(path: Path) -> None:
    """Durably pin one exact non-symlink file and its directory entry."""

    directory_flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        directory_flags |= os.O_DIRECTORY
    if hasattr(os, "O_NOFOLLOW"):
        directory_flags |= os.O_NOFOLLOW
    if hasattr(os, "O_CLOEXEC"):
        directory_flags |= os.O_CLOEXEC

    file_flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        file_flags |= os.O_NOFOLLOW
    if hasattr(os, "O_CLOEXEC"):
        file_flags |= os.O_CLOEXEC

    directory_fd = os.open(path.parent, directory_flags)
    file_fd = -1
    try:
        file_fd = os.open(path.name, file_flags, dir_fd=directory_fd)
        opened = os.fstat(file_fd)
        if not stat.S_ISREG(opened.st_mode):
            raise RuntimeError(f"Login commit target is not a regular file: {path}")
        os.fsync(file_fd)
        current = os.stat(
            path.name, dir_fd=directory_fd, follow_symlinks=False,
        )
        if (
            not stat.S_ISREG(current.st_mode)
            or current.st_dev != opened.st_dev
            or current.st_ino != opened.st_ino
        ):
            raise RuntimeError(f"Login commit target changed during fsync: {path}")
        os.fsync(directory_fd)
    finally:
        if file_fd >= 0:
            os.close(file_fd)
        os.close(directory_fd)


def _write_private_bytes(path: Path, value: bytes) -> None:
    """Atomically write secret bytes with mode 0600 and durable rename."""

    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path.parent, 0o700)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _write_private_json(path: Path, data: dict) -> None:
    """Atomically write a credential-bearing JSON file as mode 0600."""

    payload = (json.dumps(data, indent=2, ensure_ascii=False) + "\n").encode(
        "utf-8"
    )
    _write_private_bytes(path, payload)


def _write_private_text(path: Path, value: str) -> None:
    """Atomically write a small private text file as mode 0600."""

    _write_private_bytes(path, value.encode("utf-8"))


def _snapshot_private_file(path: Path) -> dict:
    """Capture exact pre-transaction bytes without following symlinks."""

    if not path.exists() and not path.is_symlink():
        return {"path": str(path), "existed": False, "content_b64": ""}
    if path.is_symlink() or not path.is_file():
        raise RuntimeError(f"Refusing unsafe login transaction file: {path}")
    return {
        "path": str(path),
        "existed": True,
        "content_b64": base64.b64encode(path.read_bytes()).decode("ascii"),
    }


def _restore_private_file(snapshot: dict) -> None:
    path = Path(str(snapshot["path"]))
    if snapshot.get("existed"):
        try:
            content = base64.b64decode(
                str(snapshot.get("content_b64") or ""), validate=True,
            )
        except (ValueError, TypeError) as exc:
            raise RuntimeError(f"Invalid login transaction snapshot for {path}") from exc
        _write_private_bytes(path, content)
    else:
        path.unlink(missing_ok=True)
        if path.parent.exists():
            _fsync_directory(path.parent)


def _login_transaction_directory(pool=None) -> Path:
    return _pool_config_path(pool).parent / LOGIN_TRANSACTION_DIR


def _pending_login_transaction_paths(pool=None) -> list[Path]:
    transaction_dir = _login_transaction_directory(pool)
    if not transaction_dir.exists():
        return []
    if transaction_dir.is_symlink() or not transaction_dir.is_dir():
        raise RuntimeError(
            f"Unsafe Codex login transaction directory: {transaction_dir}"
        )
    return sorted(transaction_dir.glob("*.json"))


def _reject_unresolved_login_transactions(pool=None) -> None:
    pending = _pending_login_transaction_paths(pool)
    if pending:
        raise HTTPException(
            status_code=409,
            detail=(
                "Codex 登录恢复仍处于隔离状态；请先重启 CCM 完成 journal "
                f"恢复（pending={len(pending)}）"
            ),
        )


def _begin_login_transaction(
    *,
    attempt_id: str,
    kind: str,
    account_id: str,
    codex_home: str,
    pool=None,
    reused_retired_slot: bool = False,
    expected_email: str | None = None,
) -> Path:
    """Persist the parent's rollback point before the wrapper can mutate state."""

    if reused_retired_slot and (
        kind != "add"
        or not isinstance(expected_email, str)
        or not expected_email.strip()
    ):
        raise RuntimeError("Retired slot reuse requires an add email identity")

    home = Path(codex_home).expanduser()
    if home.is_symlink():
        raise RuntimeError(f"Refusing symlink CODEX_HOME transaction: {home}")
    home = home.resolve()
    transaction_dir = _login_transaction_directory(pool)
    if transaction_dir.parent.is_symlink() or transaction_dir.is_symlink():
        raise RuntimeError(
            f"Refusing symlink Codex transaction directory: {transaction_dir}"
        )
    transaction_dir_existed = transaction_dir.exists()
    transaction_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(transaction_dir, 0o700)
    if not transaction_dir_existed:
        _fsync_directory(transaction_dir.parent)
    journal = {
        "version": LOGIN_TRANSACTION_VERSION,
        "attempt_id": attempt_id,
        "kind": kind,
        "account_id": account_id,
        "codex_home": str(home),
        "reused_retired_slot": bool(reused_retired_slot),
        "expected_email": expected_email if reused_retired_slot else None,
        "created_at": time.time(),
        "auth": _snapshot_private_file(home / "auth.json"),
        "previous_backups": sorted(
            path.name for path in home.glob(".auth.json.login-backup-*")
        ),
        # Relogin does not normally mutate the pool file, but its emergency
        # quarantine may disable the account. Keeping the snapshot makes that
        # isolation reversible on the next clean startup recovery.
        "pool_config": _snapshot_private_file(_pool_config_path(pool)),
    }
    if kind == "add":
        journal["credential_store"] = _snapshot_private_file(
            _credential_store_path(pool)
        )
    if reused_retired_slot:
        journal["retired_marker"] = _snapshot_private_file(
            home / ".ccm-retired-account"
        )
        # Validate the on-disk pool snapshot and marker before the journal is
        # committed or a login wrapper gets any chance to mutate this home.
        _assert_reused_retired_snapshot_is_safe(
            journal, require_current_marker=True,
        )
    journal_path = transaction_dir / f"{attempt_id}.json"
    _write_private_json(journal_path, journal)
    return journal_path


def _read_login_transaction(
    journal_path: Path,
    *,
    expected_pool_path: Path | None = None,
) -> dict:
    if journal_path.is_symlink() or not journal_path.is_file():
        raise RuntimeError(f"Unsafe Codex login transaction journal: {journal_path}")
    journal = json.loads(journal_path.read_text(encoding="utf-8"))
    if not isinstance(journal, dict) or journal.get("version") != LOGIN_TRANSACTION_VERSION:
        raise RuntimeError(f"Unsupported Codex login transaction: {journal_path}")
    if journal.get("attempt_id") != journal_path.stem:
        raise RuntimeError(f"Mismatched Codex login transaction id: {journal_path}")
    if journal.get("kind") not in {"add", "relogin"}:
        raise RuntimeError(f"Invalid Codex login transaction kind: {journal_path}")
    created_at = journal.get("created_at")
    if (
        not isinstance(created_at, (int, float))
        or isinstance(created_at, bool)
        or not math.isfinite(float(created_at))
        or created_at <= 0
    ):
        raise RuntimeError(f"Invalid login transaction timestamp: {journal_path}")
    reused_retired_slot = journal.get("reused_retired_slot", False)
    if not isinstance(reused_retired_slot, bool) or (
        reused_retired_slot and journal.get("kind") != "add"
    ):
        raise RuntimeError(
            f"Invalid retired-slot state in login transaction: {journal_path}"
        )
    expected_email = journal.get("expected_email")
    if reused_retired_slot and (
        not isinstance(expected_email, str) or not expected_email.strip()
    ):
        raise RuntimeError(
            f"Missing retired-slot email in login transaction: {journal_path}"
        )
    home = Path(str(journal.get("codex_home") or ""))
    if (
        not home.is_absolute()
        or home.is_symlink()
        or home.resolve() != home
    ):
        raise RuntimeError(f"Unsafe CODEX_HOME in login transaction: {journal_path}")
    auth = journal.get("auth")
    if not isinstance(auth, dict) or Path(str(auth.get("path") or "")) != home / "auth.json":
        raise RuntimeError(f"Invalid auth snapshot in login transaction: {journal_path}")
    pool_snapshot = journal.get("pool_config")
    if not isinstance(pool_snapshot, dict):
        raise RuntimeError(f"Missing pool snapshot in login transaction: {journal_path}")
    pool_path = Path(str(pool_snapshot.get("path") or ""))
    if not pool_path.is_absolute() or pool_path.parent != journal_path.parent.parent:
        raise RuntimeError(f"Invalid pool snapshot path in login transaction: {journal_path}")
    if expected_pool_path is not None and pool_path != expected_pool_path.resolve():
        raise RuntimeError(
            f"Mismatched pool snapshot path in login transaction: {journal_path}"
        )
    if journal["kind"] == "add":
        credential = journal.get("credential_store")
        credential_path = Path(str((credential or {}).get("path") or ""))
        if (
            not isinstance(credential, dict)
            or credential_path != pool_path.parent / "email_tokens.json"
        ):
            raise RuntimeError(
                f"Invalid credential snapshot in login transaction: {journal_path}"
            )
    if reused_retired_slot:
        retired_marker = journal.get("retired_marker")
        if (
            not isinstance(retired_marker, dict)
            or Path(str(retired_marker.get("path") or ""))
            != home / ".ccm-retired-account"
            or retired_marker.get("existed") is not True
        ):
            raise RuntimeError(
                f"Invalid retired marker snapshot in login transaction: {journal_path}"
            )
    return journal


def _snapshot_json_object(snapshot: dict, *, label: str) -> dict:
    """Decode a transaction snapshot as one JSON object without restoring it."""

    if snapshot.get("existed") is not True:
        raise RuntimeError(f"Missing {label} snapshot")
    try:
        raw = base64.b64decode(
            str(snapshot.get("content_b64") or ""), validate=True,
        )
        value = json.loads(raw.decode("utf-8"))
    except (ValueError, TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Invalid {label} snapshot") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"Invalid {label} snapshot")
    return value


def _pool_record_home_matches(record: dict, expected_home: Path) -> bool:
    raw_home = record.get("codex_home")
    if not isinstance(raw_home, str) or not raw_home:
        return False
    candidate = Path(os.path.expandvars(os.path.expanduser(raw_home)))
    return candidate.is_absolute() and candidate.resolve(strict=False) == expected_home


def _assert_reused_retired_snapshot_is_safe(
    journal: dict,
    *,
    require_current_marker: bool = False,
) -> None:
    """Prove a reuse journal originated from one finalized tombstone."""

    if not journal.get("reused_retired_slot", False):
        return
    account_id = str(journal["account_id"])
    home = Path(str(journal["codex_home"]))
    if journal["auth"].get("existed") is not False:
        raise RuntimeError("Retired slot auth snapshot was not empty")

    pool_data = _snapshot_json_object(
        journal["pool_config"], label="retired pool config",
    )
    accounts = pool_data.get("accounts")
    if not isinstance(accounts, list):
        raise RuntimeError("Retired pool snapshot has no accounts list")
    matches = [
        record for record in accounts
        if isinstance(record, dict) and record.get("id") == account_id
    ]
    if len(matches) != 1:
        raise RuntimeError("Retired pool snapshot does not contain one account id")
    record = matches[0]
    if (
        not _pool_record_home_matches(record, home)
        or record.get("retired") is not True
        or record.get("enabled") is not False
        or bool(record.get("cleanup_pending", False))
        or bool(record.get("login_recovery_failed", False))
        or str(record.get("email") or "") != ""
    ):
        raise RuntimeError("Pool snapshot is not a finalized retired account")

    marker_snapshot = journal["retired_marker"]
    try:
        marker_content = base64.b64decode(
            str(marker_snapshot.get("content_b64") or ""), validate=True,
        ).decode("utf-8")
    except (ValueError, TypeError, UnicodeDecodeError) as exc:
        raise RuntimeError("Invalid retired marker snapshot") from exc
    if marker_content.strip() != account_id:
        raise RuntimeError("Retired marker snapshot does not match account id")

    managed_home = _managed_codex_home_path(home)
    if require_current_marker:
        marker = managed_home / ".ccm-retired-account"
        if (
            marker.is_symlink()
            or not marker.is_file()
            or stat.S_IMODE(marker.stat().st_mode) & 0o077
            or marker.stat().st_size > 256
            or marker.read_text(encoding="utf-8").strip() != account_id
        ):
            raise RuntimeError("Current retired marker is unsafe or mismatched")


def _assert_reused_retired_commit_is_active(journal: dict) -> None:
    """Validate wrapper registration before committing a reused account."""

    if not journal.get("reused_retired_slot", False):
        return
    account_id = str(journal["account_id"])
    home = Path(str(journal["codex_home"]))
    pool_path = Path(str(journal["pool_config"]["path"]))
    try:
        pool_data = json.loads(pool_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("Committed pool config is unreadable") from exc
    accounts = pool_data.get("accounts") if isinstance(pool_data, dict) else None
    if not isinstance(accounts, list):
        raise RuntimeError("Committed pool config has no accounts list")
    id_matches = [
        record for record in accounts
        if isinstance(record, dict) and record.get("id") == account_id
    ]
    home_matches = [
        record for record in accounts
        if isinstance(record, dict) and _pool_record_home_matches(record, home)
    ]
    if len(id_matches) != 1 or len(home_matches) != 1 or id_matches[0] is not home_matches[0]:
        raise RuntimeError("Committed reused account id/home is not unique")
    record = id_matches[0]
    cutoff = record.get("quota_valid_after")
    if (
        record.get("enabled") is not True
        or record.get("email") != journal["expected_email"]
        or bool(record.get("retired", False))
        or bool(record.get("cleanup_pending", False))
        or bool(record.get("login_recovery_failed", False))
        or not isinstance(cutoff, (int, float))
        or isinstance(cutoff, bool)
        or not math.isfinite(float(cutoff))
        or cutoff <= float(journal["created_at"])
    ):
        raise RuntimeError("Committed reused account is not fully activated")


def _remove_login_transaction(journal_path: Path) -> None:
    journal_path.unlink(missing_ok=True)
    if journal_path.parent.exists():
        _fsync_directory(journal_path.parent)


def _remove_reactivated_retired_marker(journal: dict) -> None:
    """Drop a retired marker only after an add transaction has committed.

    The marker snapshot proves the slot was retired and lets rollback rebuild
    it after a failed reuse attempt. Once the journal has been durably removed,
    marker cleanup failure is cosmetic and must not roll back valid new auth.
    """

    if (
        journal.get("kind") != "add"
        or not journal.get("reused_retired_slot", False)
    ):
        return
    home = Path(str(journal.get("codex_home") or ""))
    try:
        if home.is_symlink() or not home.is_dir():
            logger.warning(
                "Refusing retired marker cleanup from unsafe home %s",
                home,
            )
            return
        marker = home / ".ccm-retired-account"
        if not marker.exists() and not marker.is_symlink():
            return
        account_id = str(journal.get("account_id") or "")
        if (
            marker.is_symlink()
            or not marker.is_file()
            or marker.stat().st_size > 256
            or marker.read_text(encoding="utf-8").strip() != account_id
        ):
            logger.warning(
                "Retaining mismatched retired marker after activating %s at %s",
                account_id,
                home,
            )
            return
        marker.unlink()
        _fsync_directory(home)
    except (OSError, UnicodeError):
        logger.warning(
            "Failed to remove retired marker after activating %s at %s",
            journal.get("account_id"),
            home,
            exc_info=True,
        )


def _durably_prepare_login_commit(
    journal_path: Path,
    *,
    expected_pool_path: Path,
) -> dict:
    """Make every wrapper-owned success file durable before journal commit."""

    journal = _read_login_transaction(
        journal_path, expected_pool_path=expected_pool_path,
    )
    _assert_reused_retired_snapshot_is_safe(journal)
    auth_path = Path(str(journal["auth"]["path"]))
    _fsync_regular_file_and_parent(auth_path)
    if journal["kind"] == "add":
        _fsync_regular_file_and_parent(
            Path(str(journal["credential_store"]["path"]))
        )
        _fsync_regular_file_and_parent(
            Path(str(journal["pool_config"]["path"]))
        )
    _assert_reused_retired_commit_is_active(journal)
    return journal


def _rollback_login_transaction(
    journal_path: Path,
    *,
    expected_pool_path: Path | None = None,
) -> dict:
    """Idempotently restore every parent-owned file, then delete the journal."""

    journal = _read_login_transaction(
        journal_path, expected_pool_path=expected_pool_path,
    )
    _assert_reused_retired_snapshot_is_safe(journal)
    home = Path(str(journal["codex_home"]))
    if home.is_symlink():
        raise RuntimeError(f"CODEX_HOME became a symlink during rollback: {home}")
    _restore_private_file(journal["auth"])
    previous = set(journal.get("previous_backups") or [])
    for backup in home.glob(".auth.json.login-backup-*"):
        if backup.name not in previous:
            backup.unlink(missing_ok=True)
    (home / f".auth.json.ccm-quarantine-{journal['attempt_id']}").unlink(
        missing_ok=True
    )
    (home / ".ccm-login-recovery-failed").unlink(missing_ok=True)
    # Persist removal of wrapper backups/quarantine artifacts before the
    # journal is allowed to disappear. A retry remains idempotent if this
    # fsync itself is interrupted.
    if home.exists():
        _fsync_directory(home)
    if journal["kind"] == "add":
        _restore_private_file(journal["credential_store"])
    _restore_private_file(journal["pool_config"])
    if journal.get("reused_retired_slot", False):
        # A failed Codex login can create caches/config/state beyond auth.json.
        # Restore the exact finalized tombstone shape before committing the
        # rollback, otherwise the allocator would skip this slot next time.
        _purge_retired_codex_home(home, str(journal["account_id"]))
    _remove_login_transaction(journal_path)
    return journal


def _quarantine_login_transaction(
    journal_path: Path,
    reason: str,
    *,
    expected_pool_path: Path | None = None,
) -> bool:
    """Fail closed when rollback itself cannot complete.

    Removing ``auth.json`` from the well-known location prevents a fresh Codex
    process from loading partial credentials.  If the pool record exists it is
    also disabled atomically.  The journal remains for a later startup retry.
    """

    journal = _read_login_transaction(
        journal_path, expected_pool_path=expected_pool_path,
    )
    home = Path(str(journal["codex_home"]))
    if home.is_symlink():
        raise RuntimeError(f"Refusing to quarantine symlink CODEX_HOME: {home}")
    home.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(home, 0o700)
    auth_path = home / "auth.json"
    try:
        auth_stat = auth_path.lstat()
    except FileNotFoundError:
        auth_stat = None
    auth_isolated = auth_stat is None
    if auth_stat is not None and stat.S_ISLNK(auth_stat.st_mode):
        # Never chmod or otherwise follow a partial/dangling auth symlink. The
        # parent-owned journal already contains the only rollback copy needed.
        auth_path.unlink()
        _fsync_directory(home)
        auth_isolated = True
    elif auth_stat is not None and stat.S_ISREG(auth_stat.st_mode):
        quarantine_path = home / (
            f".auth.json.ccm-quarantine-{journal['attempt_id']}"
        )
        os.replace(auth_path, quarantine_path)
        quarantine_flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            quarantine_flags |= os.O_NOFOLLOW
        quarantine_fd = os.open(quarantine_path, quarantine_flags)
        try:
            quarantined = os.fstat(quarantine_fd)
            if not stat.S_ISREG(quarantined.st_mode):
                raise RuntimeError(
                    f"Unsafe quarantined auth file: {quarantine_path}"
                )
            os.fchmod(quarantine_fd, 0o600)
            os.fsync(quarantine_fd)
        finally:
            os.close(quarantine_fd)
        _fsync_directory(home)
        auth_isolated = True
    elif auth_stat is not None:
        raise RuntimeError(f"Unsafe auth entry during quarantine: {auth_path}")
    _write_private_text(
        home / ".ccm-login-recovery-failed",
        f"attempt={journal['attempt_id']}\nreason={reason[:1000]}\n",
    )

    pool_snapshot = journal.get("pool_config")
    pool_disabled = False
    if isinstance(pool_snapshot, dict):
        pool_path = Path(str(pool_snapshot["path"]))
    else:
        pool_path = journal_path.parent.parent / "accounts.json"
    if pool_path.exists() and not pool_path.is_symlink():
        data = json.loads(pool_path.read_text(encoding="utf-8"))
        accounts = data.get("accounts") if isinstance(data, dict) else None
        if isinstance(accounts, list):
            for record in accounts:
                if not isinstance(record, dict):
                    continue
                if (
                    record.get("id") == journal.get("account_id")
                    or os.path.abspath(os.path.expanduser(str(record.get("codex_home") or "")))
                    == os.path.abspath(str(home))
                ):
                    record["enabled"] = False
                    record["login_recovery_failed"] = True
                    pool_disabled = True
            if pool_disabled:
                _write_private_json(pool_path, data)
    # An absent auth is sufficient isolation even when an add never registered
    # its account record. A registered record is disabled as defense in depth.
    return auth_isolated


def recover_pending_codex_login_transactions(
    pool_config_path: str | os.PathLike[str] | None,
) -> dict:
    """Rollback journals left by an all-process service restart.

    This function is synchronous by design and must run before ``CodexPool`` is
    constructed, so no task can observe wrapper-mutated files first.
    """

    raw_pool_path = (
        Path(os.path.expandvars(os.path.expanduser(os.fspath(pool_config_path))))
        if pool_config_path
        else Path.home() / ".codex-pool" / "accounts.json"
    )
    if raw_pool_path.is_symlink() or raw_pool_path.parent.is_symlink():
        raise RuntimeError(
            f"Refusing symlink Codex pool recovery path: {raw_pool_path}"
        )
    pool_path = raw_pool_path.resolve()
    transaction_dir = pool_path.parent / LOGIN_TRANSACTION_DIR
    recovered: list[str] = []
    quarantined: list[str] = []
    if not transaction_dir.exists():
        return {"recovered": recovered, "quarantined": quarantined}
    if transaction_dir.is_symlink() or not transaction_dir.is_dir():
        raise RuntimeError(f"Unsafe Codex login transaction directory: {transaction_dir}")
    os.chmod(transaction_dir, 0o700)
    # A quarantined transaction can coexist with a later journal only after a
    # bug/manual intervention. Roll newest to oldest so snapshots unwind like
    # a stack and converge on the earliest pre-transaction state.
    journal_paths = sorted(
        transaction_dir.glob("*.json"),
        key=lambda path: (path.lstat().st_mtime_ns, path.name),
        reverse=True,
    )
    for journal_path in journal_paths:
        try:
            journal = _rollback_login_transaction(
                journal_path, expected_pool_path=pool_path,
            )
            recovered.append(str(journal.get("attempt_id") or journal_path.stem))
        except Exception as exc:
            logger.exception("Failed to rollback Codex login transaction %s", journal_path)
            if _quarantine_login_transaction(
                journal_path, str(exc), expected_pool_path=pool_path,
            ):
                quarantined.append(journal_path.stem)
            else:
                raise RuntimeError(
                    f"Unable to isolate Codex login transaction {journal_path}"
                ) from exc
    return {"recovered": recovered, "quarantined": quarantined}


async def _stop_unfinished_login_process(
    proc: asyncio.subprocess.Process,
    *,
    operation: str,
) -> bool:
    """Prevent a failed/cancelled watcher from releasing a live login process.

    The return value records whether the process was live when cleanup began,
    not merely whether ``kill()`` happened to succeed.  A process can disappear
    between the returncode check and ``killpg``; its auth transaction still
    needs reconciliation before home maintenance is released.
    """

    if proc.returncode is not None:
        return False
    was_unfinished = True
    try:
        pid = getattr(proc, "pid", None)
        if hasattr(os, "killpg"):
            process_group_id = require_safe_process_group_id(
                pid,
                context=f"Codex {operation} wrapper",
            )
            os.killpg(process_group_id, signal.SIGKILL)
        else:
            proc.kill()
    except UnsafeProcessGroupError as exc:
        raise LoginProcessNotTerminal(
            f"Codex {operation} wrapper has an unsafe process identity"
        ) from exc
    except ProcessLookupError:
        pass
    except Exception:
        logger.exception("Failed to stop Codex %s process group", operation)
        try:
            proc.kill()
        except (ProcessLookupError, Exception):
            logger.exception("Failed to stop Codex %s wrapper process", operation)
    waiter = asyncio.create_task(proc.wait())
    deadline = asyncio.get_running_loop().time() + LOGIN_REAP_TIMEOUT_SECONDS
    while not waiter.done():
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            waiter.cancel()
            try:
                await waiter
            except asyncio.CancelledError:
                pass
            raise LoginProcessNotTerminal(
                f"Codex {operation} wrapper did not terminate after SIGKILL"
            )
        try:
            done, _pending = await asyncio.wait({waiter}, timeout=remaining)
        except asyncio.CancelledError:
            # Cleanup itself may be targeted during application shutdown. Keep
            # waiting; the persistent journal is the fallback for hard kill.
            continue
        if not done:
            waiter.cancel()
            try:
                await waiter
            except asyncio.CancelledError:
                pass
            raise LoginProcessNotTerminal(
                f"Codex {operation} wrapper termination timed out"
            )
    if waiter.cancelled():
        raise LoginProcessNotTerminal(
            f"Codex {operation} wrapper waiter was cancelled"
        )
    wait_error = waiter.exception()
    if wait_error is not None and proc.returncode is None:
        raise LoginProcessNotTerminal(
            f"Codex {operation} wrapper wait failed: {wait_error}"
        ) from wait_error
    if proc.returncode is None:
        raise LoginProcessNotTerminal(
            f"Codex {operation} wrapper wait returned without a terminal status"
        )
    return was_unfinished


async def _await_login_cleanup(coro):
    """Delay caller cancellation until the isolated cleanup task is complete."""

    cleanup_task = asyncio.create_task(coro)
    cancelled = False
    while not cleanup_task.done():
        try:
            await asyncio.shield(cleanup_task)
        except asyncio.CancelledError:
            # Do not let HTTP disconnect/app shutdown cancel process reaping or
            # credential rollback. Repeated cancellation is handled by looping.
            cancelled = True
    result = cleanup_task.result()
    if cancelled:
        raise asyncio.CancelledError
    return result


async def _finalize_login_transaction(
    *,
    proc: asyncio.subprocess.Process,
    operation: str,
    journal_path: Path,
    commit_requested: bool,
    instance_manager,
    codex_home: str,
    login_lock: asyncio.Lock,
    attempt_id: str,
    state_store: dict[str, dict],
    state_key: str,
    expected_pool_path: Path | None = None,
) -> dict:
    """Reap wrapper and atomically commit, rollback, or quarantine its files."""

    committed = False
    cleanup_safe = False
    detail = ""
    recovery_failed = False
    try:
        interrupted_live_process = await _stop_unfinished_login_process(
            proc, operation=operation,
        )
        if (
            commit_requested
            and not interrupted_live_process
            and proc.returncode == 0
        ):
            try:
                commit_pool_path = expected_pool_path
                if commit_pool_path is None:
                    journal = _read_login_transaction(journal_path)
                    commit_pool_path = Path(
                        str(journal["pool_config"]["path"])
                    )
                committed_journal = _durably_prepare_login_commit(
                    journal_path,
                    expected_pool_path=commit_pool_path,
                )
                _remove_login_transaction(journal_path)
                committed = True
                _remove_reactivated_retired_marker(committed_journal)
            except BaseException as exc:
                detail = (
                    "Login commit validation failed and was rolled back: "
                    f"{exc}"
                )
                logger.exception("Failed to commit Codex %s", operation)
                _rollback_login_transaction(
                    journal_path,
                    expected_pool_path=expected_pool_path,
                )
        else:
            _rollback_login_transaction(
                journal_path,
                expected_pool_path=expected_pool_path,
            )
        cleanup_safe = True
    except LoginProcessNotTerminal as exc:
        # Never touch auth while the wrapper could still write it. Retaining
        # home maintenance is the isolation mechanism; startup journal replay
        # completes rollback after systemd has killed the orphan.
        detail = f"Login wrapper termination is unconfirmed: {exc}"
        logger.exception("Could not prove Codex %s wrapper termination", operation)
        cleanup_safe = False
    except BaseException as exc:
        # This coroutine runs in its own shielded task, so CancelledError here
        # means the cleanup primitive itself failed rather than caller
        # cancellation. Fail closed and keep a persistent journal for startup.
        detail = (
            f"{detail}; " if detail else ""
        ) + f"Login transaction recovery failed: {exc}"
        logger.exception("Failed to finalize Codex %s", operation)
        recovery_failed = True
        try:
            cleanup_safe = _quarantine_login_transaction(
                journal_path,
                detail,
                expected_pool_path=expected_pool_path,
            )
        except Exception as quarantine_exc:
            detail = (
                f"{detail}; credential quarantine failed: {quarantine_exc}"
            )
            logger.exception("Failed to quarantine Codex %s", operation)
            cleanup_safe = False

    _login_attempts.pop(attempt_id, None)
    # Add-account state carries the allocated slot id.  Keep it through the
    # watcher/finalizer hand-off so a Manager that joins an already-running
    # attempt can persist the exact slot and retry idempotently.
    previous_state = state_store.get(state_key, {})
    account_identity = (
        {"account_id": previous_state["account_id"]}
        if previous_state.get("account_id") else {}
    )
    if committed:
        state_store[state_key] = {
            "status": "success",
            "finished_at": time.time(),
            "attempt_id": attempt_id,
            **account_identity,
        }
    elif cleanup_safe and recovery_failed:
        state_store[state_key] = {
            "status": "recovery_failed",
            "detail": detail,
            "finished_at": time.time(),
            "attempt_id": attempt_id,
            **account_identity,
        }
    elif cleanup_safe and state_store.get(state_key, {}).get("status") in ACTIVE_LOGIN_STATUSES:
        state_store[state_key] = {
            "status": "failed",
            "detail": (
                detail
                or "Login attempt was interrupted and rolled back safely"
            ),
            "finished_at": time.time(),
            "attempt_id": attempt_id,
            **account_identity,
        }
    elif not cleanup_safe:
        state_store[state_key] = {
            "status": "recovery_failed",
            "detail": (
                detail
                or "Login wrapper could not be stopped and credentials remain isolated by maintenance"
            ),
            "finished_at": time.time(),
            "attempt_id": attempt_id,
            **account_identity,
        }

    if cleanup_safe:
        try:
            await instance_manager.end_codex_app_server_home_maintenance(
                codex_home
            )
        except Exception as exc:
            logger.exception("Failed to release Codex maintenance after %s", operation)
            state_store[state_key] = {
                "status": "recovery_failed",
                "detail": f"Credentials are safe but maintenance release failed: {exc}",
                "finished_at": time.time(),
                "attempt_id": attempt_id,
                **account_identity,
            }
        finally:
            if login_lock.locked():
                login_lock.release()
    else:
        # Keep the per-home maintenance reservation: it is the final isolation
        # barrier when neither rollback nor filesystem quarantine succeeded.
        # Release only the global lock so other accounts remain operable; the
        # explicit recovery_failed state makes the affected home visible.
        if login_lock.locked():
            login_lock.release()

    try:
        pool = _get_pool()
        pool.reload()
        pool._quota_cache = None
    except Exception:
        logger.exception("Failed to reload Codex pool after %s", operation)
    return {
        "committed": committed,
        "cleanup_safe": cleanup_safe,
        "detail": detail,
    }


async def _rollback_unspawned_login_transaction(
    *,
    journal_path: Path,
    instance_manager,
    codex_home: str,
    login_lock: asyncio.Lock,
    attempt_id: str,
    state_store: dict[str, dict],
    state_key: str,
    expected_pool_path: Path | None = None,
) -> None:
    """Close a prepared transaction when subprocess creation never returned."""

    cleanup_safe = False
    detail = ""
    try:
        _rollback_login_transaction(
            journal_path, expected_pool_path=expected_pool_path,
        )
        cleanup_safe = True
    except Exception as exc:
        detail = f"Prepared login transaction rollback failed: {exc}"
        logger.exception("Failed to rollback unspawned login %s", attempt_id)
        try:
            cleanup_safe = _quarantine_login_transaction(
                journal_path,
                detail,
                expected_pool_path=expected_pool_path,
            )
        except Exception as quarantine_exc:
            detail = f"{detail}; credential quarantine failed: {quarantine_exc}"
            logger.exception("Failed to quarantine unspawned login %s", attempt_id)

    if detail:
        state_store[state_key] = {
            "status": "recovery_failed",
            "detail": detail,
            "finished_at": time.time(),
            "attempt_id": attempt_id,
        }
    if not cleanup_safe:
        # Keep per-home maintenance as the final isolation barrier.
        if login_lock.locked():
            login_lock.release()
        return

    try:
        await instance_manager.end_codex_app_server_home_maintenance(codex_home)
    except Exception as exc:
        logger.exception(
            "Failed to release Codex maintenance for unspawned login %s",
            attempt_id,
        )
        state_store[state_key] = {
            "status": "recovery_failed",
            "detail": f"Credentials are safe but maintenance release failed: {exc}",
            "finished_at": time.time(),
            "attempt_id": attempt_id,
        }
    finally:
        if login_lock.locked():
            login_lock.release()


_FAILED_LOGIN_REUSABLE_FILES = frozenset({"models_cache.json"})
_FAILED_LOGIN_REUSABLE_DIRS = frozenset({"log", "tmp"})


def _plain_failed_login_runtime_tree(path: Path) -> bool:
    """Accept only ordinary files/directories below a disposable runtime dir."""

    pending = [path]
    try:
        while pending:
            directory = pending.pop()
            for child in directory.iterdir():
                mode = child.lstat().st_mode
                if stat.S_ISLNK(mode):
                    return False
                if stat.S_ISDIR(mode):
                    # Never recursively remove a mounted tree as login residue.
                    if os.path.ismount(child):
                        return False
                    pending.append(child)
                elif not stat.S_ISREG(mode):
                    return False
    except OSError:
        return False
    return True


def _failed_login_home_is_reusable(codex_home: Path) -> bool:
    """Prove an orphan home contains only disposable failed-login residue.

    Codex may create ``log/`` and ``tmp/`` before authentication completes,
    and may also refresh ``models_cache.json``.  Identity-bearing or durable
    state (for example auth, config, state, history, or sessions) is not on
    this allowlist and keeps the slot quarantined.
    """

    try:
        if not codex_home.is_dir() or codex_home.is_symlink():
            return False
        for child in codex_home.iterdir():
            mode = child.lstat().st_mode
            if child.name in _FAILED_LOGIN_REUSABLE_FILES:
                if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
                    return False
            elif child.name in _FAILED_LOGIN_REUSABLE_DIRS:
                if (
                    stat.S_ISLNK(mode)
                    or not stat.S_ISDIR(mode)
                    or os.path.ismount(child)
                    or not _plain_failed_login_runtime_tree(child)
                ):
                    return False
            else:
                return False
    except OSError:
        return False
    return True


def _purge_failed_login_codex_home(codex_home: Path) -> None:
    """Clear a proven credential-free orphan home before assigning it."""

    codex_home = _managed_codex_home_path(codex_home)
    if not _failed_login_home_is_reusable(codex_home):
        raise RuntimeError(
            f"Refusing to purge unsafe failed-login CODEX_HOME: {codex_home}"
        )

    os.chmod(codex_home, 0o700)
    for child in list(codex_home.iterdir()):
        # Re-check the entry type immediately before removing it.  rmtree does
        # not follow directory symlinks, but rejecting a changed entry keeps
        # the operation fail-closed instead of silently normalizing a race.
        mode = child.lstat().st_mode
        if child.name in _FAILED_LOGIN_REUSABLE_FILES and stat.S_ISREG(mode):
            child.unlink()
        elif (
            child.name in _FAILED_LOGIN_REUSABLE_DIRS
            and stat.S_ISDIR(mode)
            and not stat.S_ISLNK(mode)
            and not os.path.ismount(child)
            and _plain_failed_login_runtime_tree(child)
        ):
            shutil.rmtree(child)
        else:
            raise RuntimeError(
                f"Failed-login CODEX_HOME changed during cleanup: {child}"
            )

    if any(codex_home.iterdir()):
        raise RuntimeError(
            f"Failed-login CODEX_HOME was not fully cleaned: {codex_home}"
        )
    _fsync_directory(codex_home)


def _retired_account_slot_index(account) -> int | None:
    """Return the stable numeric slot for a standard ``codex-N`` tombstone."""

    match = re.fullmatch(r"codex-([1-9][0-9]*)", str(getattr(account, "id", "")))
    return int(match.group(1)) if match else None


def _clean_retired_home_is_reusable(account) -> bool:
    """Prove that a retired account home completed CCM's safe cleanup.

    Deletion deliberately preserves native sessions so old tasks remain
    recoverable.  A new identity may reuse that exact slot only when the pool
    tombstone is final, no login recovery is outstanding, and the directory
    still has precisely the post-delete shape written by
    ``_purge_retired_codex_home``.
    """

    if (
        not bool(getattr(account, "retired", False))
        or bool(getattr(account, "cleanup_pending", False))
        or bool(getattr(account, "login_recovery_failed", False))
        or str(getattr(account, "email", "") or "") != ""
        or _retired_account_slot_index(account) is None
    ):
        return False

    try:
        codex_home = _managed_codex_home_path(
            Path(str(getattr(account, "codex_home", "")))
        )
        if not codex_home.is_dir() or codex_home.is_symlink():
            return False
        # Deletion locks the managed home to the service user.  Treat later
        # permission broadening as evidence that CCM no longer controls it.
        if stat.S_IMODE(codex_home.stat().st_mode) & 0o077:
            return False

        children = {child.name: child for child in codex_home.iterdir()}
        if not set(children).issubset({"sessions", ".ccm-retired-account"}):
            return False

        sessions = children.get("sessions")
        if sessions is not None and (sessions.is_symlink() or not sessions.is_dir()):
            return False

        marker = children.get(".ccm-retired-account")
        if (
            marker is None
            or marker.is_symlink()
            or not marker.is_file()
            or stat.S_IMODE(marker.stat().st_mode) & 0o077
            or marker.stat().st_size > 256
        ):
            return False
        return marker.read_text(encoding="utf-8").strip() == account.id
    except (OSError, RuntimeError, UnicodeError, ValueError):
        return False


def _managed_codex_home_path(codex_home: Path) -> Path:
    """Validate and canonicalize a CODEX_HOME safe for recursive cleanup."""

    if codex_home.is_symlink() or not re.fullmatch(
        r"\.codex(?:-[A-Za-z0-9][A-Za-z0-9._-]*)?", codex_home.name,
    ):
        raise RuntimeError(f"Refusing to purge unmanaged CODEX_HOME: {codex_home}")
    codex_home = codex_home.resolve()
    if codex_home.parent != Path.home().resolve():
        raise RuntimeError(
            f"Refusing to purge CODEX_HOME outside the service user's home: {codex_home}"
        )
    if codex_home.exists() and not codex_home.is_dir():
        raise RuntimeError(f"CODEX_HOME is not a directory: {codex_home}")
    return codex_home


def _purge_retired_codex_home(codex_home: Path, account_id: str) -> None:
    """Remove all account runtime data except native rollout sessions."""

    codex_home = _managed_codex_home_path(codex_home)

    codex_home.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(codex_home, 0o700)
    children = list(codex_home.iterdir())
    sessions = next((child for child in children if child.name == "sessions"), None)
    if sessions is not None and (sessions.is_symlink() or not sessions.is_dir()):
        raise RuntimeError(f"Refusing unsafe sessions entry in {codex_home}")

    for child in children:
        if child.name == "sessions":
            continue
        if child.is_symlink() or not child.is_dir():
            child.unlink(missing_ok=True)
        else:
            shutil.rmtree(child)

    _write_private_text(codex_home / ".ccm-retired-account", f"{account_id}\n")


def _credential_store_path(pool=None) -> Path:
    parent = _pool_config_path(pool).parent
    return parent / "email_tokens.json"


def _pool_config_path(pool=None) -> Path:
    configured = getattr(pool, "_config_path", None)
    if configured:
        raw = Path(
            os.path.expandvars(os.path.expanduser(os.fspath(configured)))
        )
    else:
        raw = Path.home() / ".codex-pool" / "accounts.json"
    if raw.is_symlink() or raw.parent.is_symlink():
        raise RuntimeError(f"Refusing symlink Codex pool config path: {raw}")
    return raw.resolve()


def _sanitize_login_detail(text: str) -> str:
    """Keep diagnostic output while removing the OAuth authorize URL."""
    return re.sub(
        r"https://auth\.openai\.com/oauth/authorize\S+",
        "[redacted OpenAI OAuth URL]",
        text,
    )[-5000:]


def _attempt_state(attempt: dict) -> dict:
    store = _relogin_state if attempt["kind"] == "relogin" else _add_state
    return store.setdefault(attempt["state_key"], {})


def _handle_login_event(attempt_id: str, line: str) -> bool:
    if not line.startswith(LOGIN_EVENT_PREFIX):
        return False
    try:
        event = json.loads(line[len(LOGIN_EVENT_PREFIX):])
    except (TypeError, ValueError):
        return True
    if event.get("attempt_id") != attempt_id:
        return True
    attempt = _login_attempts.get(attempt_id)
    if not attempt:
        return True

    state = _attempt_state(attempt)
    event_type = event.get("type")
    if event_type == "otp_required":
        challenge_id = str(event.get("challenge_id") or "")
        expires_at = int(event.get("expires_at") or 0)
        attempt["challenge_id"] = challenge_id
        attempt["expires_at"] = expires_at
        state.update({
            "status": "awaiting_otp",
            "attempt_id": attempt_id,
            "challenge_id": challenge_id,
            "expires_at": expires_at,
        })
    elif event_type == "otp_received":
        state.update({
            "status": "verifying_otp",
            "attempt_id": attempt_id,
            "challenge_id": str(event.get("challenge_id") or ""),
        })
    elif event_type == "otp_expired":
        state.update({
            "status": "expired",
            "attempt_id": attempt_id,
            "detail": "等待邮箱验证码超时，请重新发起登录",
        })
    return True


async def _collect_login_output(
    proc: asyncio.subprocess.Process,
    attempt_id: str,
) -> str:
    """Consume output live so an OTP challenge can reach the UI immediately."""
    stdout = getattr(proc, "stdout", None)
    if stdout is None or not hasattr(stdout, "readline"):
        out, _ = await proc.communicate()
        return _sanitize_login_detail((out or b"").decode("utf-8", errors="replace"))

    tail = ""
    while True:
        raw = await stdout.readline()
        if not raw:
            break
        line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
        if _handle_login_event(attempt_id, line):
            continue
        tail = _sanitize_login_detail(f"{tail}\n{line}".lstrip())
    await proc.wait()
    return tail


async def _send_login_credentials(
    proc: asyncio.subprocess.Process,
    *,
    attempt_id: str,
    token: str,
    password: str,
) -> None:
    """Send secrets once over the private stdin pipe, never argv or state."""
    stdin = getattr(proc, "stdin", None)
    if stdin is None:
        raise RuntimeError("Codex login process has no credential input channel")
    message = json.dumps({
        "type": "credentials",
        "attempt_id": attempt_id,
        "token": token,
        "password": password,
    }, separators=(",", ":"))
    try:
        stdin.write((message + "\n").encode("utf-8"))
        await stdin.drain()
    except (BrokenPipeError, ConnectionError, RuntimeError) as exc:
        raise RuntimeError("Codex login process rejected its credentials") from exc


def _read_saved_mailbox_credential(
    email: str, *, tokens_path: Path | None = None,
) -> tuple[str, str, str]:
    """Return mailbox token, provider and OpenAI password, including legacy entries."""
    tokens_path = tokens_path or _credential_store_path()
    if not tokens_path.exists():
        return "", "", ""

    try:
        tokens = json.loads(tokens_path.read_text())
    except (OSError, ValueError):
        return "", "", ""
    if not isinstance(tokens, dict):
        return "", "", ""

    saved = tokens.get(email)
    if saved is None:
        email_key = email.casefold()
        saved = next(
            (value for key, value in tokens.items() if isinstance(key, str) and key.casefold() == email_key),
            "",
        )
    if isinstance(saved, str):
        # Legacy entries contain a 171mail token only. Keep them pinned to
        # 171mail so a token is never reinterpreted as a MailCatcher token.
        return saved.strip(), "171mail", ""
    if not isinstance(saved, dict):
        return "", "", ""

    token = saved.get("token", "")
    provider = saved.get("provider", "")
    password = saved.get("password", "")
    return (
        token.strip() if isinstance(token, str) else "",
        provider.strip().lower() if isinstance(provider, str) else "",
        password if isinstance(password, str) else "",
    )


def _get_optional_pool():
    from backend.main import codex_pool

    return codex_pool


def _get_pool():
    pool = _get_optional_pool()
    if not pool:
        raise HTTPException(status_code=404, detail="Codex pool not enabled. Set CODEX_POOL_ENABLED=true in .env")
    return pool


def _disabled_pool_status() -> dict:
    return {
        "enabled": False,
        "total": 0,
        "available": 0,
        "cooldown": 0,
        "disabled": 0,
        "preferred": None,
        "last_selected": None,
        "accounts": [],
    }


def _get_instance_manager():
    from backend.main import instance_manager
    if not instance_manager:
        raise HTTPException(status_code=503, detail="Instance manager is not available")
    return instance_manager


def _get_dispatcher():
    from backend.main import dispatcher
    if not dispatcher:
        raise HTTPException(status_code=503, detail="Dispatcher is not available")
    return dispatcher


@router.get("/status")
async def codex_pool_status():
    pool = _get_optional_pool()
    return pool.status() if pool else _disabled_pool_status()


@router.get("/usage")
async def codex_pool_usage(force: bool = False):
    """Pool status merged with per-account quota.

    A forced user refresh queries each account through its own Codex
    app-server; rollout snapshots remain the background/failure fallback.
    """
    pool = _get_pool()
    status = pool.status()
    quota_list = await pool.fetch_quota(force=force, live=force)
    quota_by_id = {q["id"]: q for q in quota_list}
    for account in status["accounts"]:
        q = quota_by_id.get(account["id"], {})
        account["plan_type"] = q.get("plan_type")
        account["quota"] = q.get("quota")
        account["quota_error"] = q.get("error")
        account["api_quota"] = q.get("api_quota")
    return status


@router.post("/reload")
async def codex_pool_reload(request: Request):
    require_admin(request)
    pool = _get_pool()
    pool.reload()
    return pool.status()


@router.post("/accounts/{account_id}/clear-cooldown")
async def codex_clear_cooldown(request: Request, account_id: str):
    require_admin(request)
    pool = _get_pool()
    pool.clear_cooldown(account_id)
    return {"ok": True, "account_id": account_id}


@router.get("/accounts/{account_id}/verify")
async def codex_verify_account(
    request: Request,
    account_id: str,
    live: bool = False,
):
    """Check local credentials and optionally prove them with a live RPC."""
    require_admin(request)
    pool = _get_pool()
    acc = pool.account(account_id)
    if not acc or getattr(acc, "retired", False):
        raise HTTPException(status_code=404, detail=f"Unknown account: {account_id}")
    if live:
        return await pool.verify_account_live(account_id)
    from backend.services.codex_pool import verify_login
    return verify_login(
        acc.codex_home,
        auth_kind=getattr(acc, "auth_kind", "oauth"),
    )


# ---------------------------------------------------------------------------
# Relogin (automated)
# ---------------------------------------------------------------------------

async def _watch_relogin(
    account_id: str,
    attempt_id: str,
    proc: asyncio.subprocess.Process,
    instance_manager,
    codex_home: str,
    login_lock: asyncio.Lock,
    journal_path: Path,
    expected_pool_path: Path | None = None,
):
    watch_completed = False
    try:
        tail = await _collect_login_output(proc, attempt_id)
        watch_completed = True
        previous_status = _relogin_state.get(account_id, {}).get("status")
        _relogin_state[account_id] = {
            "status": (
                "finalizing" if proc.returncode == 0
                else previous_status
                if previous_status in {"expired", "cancelled"}
                else "failed"
            ),
            "detail": tail,
            "attempt_id": attempt_id,
        }
        if proc.returncode != 0:
            _relogin_state[account_id]["finished_at"] = time.time()
    except Exception as exc:
        logger.exception("Codex relogin watcher failed for %s", account_id)
        _relogin_state[account_id] = {
            "status": "failed",
            "detail": str(exc),
            "finished_at": time.time(),
        }
    finally:
        await _await_login_cleanup(_finalize_login_transaction(
            proc=proc,
            operation=f"relogin for {account_id}",
            journal_path=journal_path,
            commit_requested=watch_completed and proc.returncode == 0,
            instance_manager=instance_manager,
            codex_home=codex_home,
            login_lock=login_lock,
            attempt_id=attempt_id,
            state_store=_relogin_state,
            state_key=account_id,
            expected_pool_path=expected_pool_path,
        ))


@router.post("/accounts/{account_id}/relogin")
async def codex_relogin(request: Request, account_id: str):
    require_admin(request)
    pool = _get_pool()
    acc = pool.account(account_id)
    if not acc or getattr(acc, "retired", False):
        raise HTTPException(status_code=404, detail=f"Unknown account: {account_id}")
    if is_api_auth_kind(getattr(acc, "auth_kind", "")):
        raise HTTPException(
            status_code=400,
            detail="API 账号不使用 OAuth 登录，请通过 API 账号刷新入口校验",
        )

    state = _relogin_state.get(account_id)
    if state and state.get("status") in ACTIVE_LOGIN_STATUSES:
        return {
            "ok": True,
            "status": state["status"],
            "attempt_id": state.get("attempt_id"),
        }
    if _login_lock.locked():
        running = [
            k for k, v in _relogin_state.items()
            if v.get("status") in ACTIVE_LOGIN_STATUSES
        ]
        raise HTTPException(status_code=409, detail=f"另一个账号正在登录中（{', '.join(running)}）")
    _reject_unresolved_login_transactions(pool)

    receiver_token, mail_provider, openai_password = _read_saved_mailbox_credential(
        acc.email,
        tokens_path=_credential_store_path(pool),
    )
    if mail_provider and mail_provider not in MAIL_PROVIDERS:
        raise HTTPException(status_code=400, detail=f"Unsupported saved mailbox provider: {mail_provider}")

    root = Path(__file__).resolve().parents[2]
    login_py = root / ".venv" / "bin" / "python3"
    if not login_py.exists():
        raise HTTPException(status_code=501, detail="Python venv not found")

    instance_manager = _get_instance_manager()
    login_lock = _login_lock
    pool_path = _pool_config_path(pool)
    await login_lock.acquire()
    maintenance_started = False
    watcher_started = False
    proc: asyncio.subprocess.Process | None = None
    journal_path: Path | None = None
    try:
        # Starting Xvfb does not touch CODEX_HOME, so reserve the account only
        # after the shared browser runtime is ready.
        await _ensure_xvfb()
        try:
            await instance_manager.begin_codex_app_server_home_maintenance(
                acc.codex_home, require_idle=True,
            )
        except CodexAppServerBusyError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        maintenance_started = True

        script = root / "scripts" / "codex_login.py"
        attempt_id = uuid.uuid4().hex
        journal_path = _begin_login_transaction(
            attempt_id=attempt_id,
            kind="relogin",
            account_id=account_id,
            codex_home=acc.codex_home,
            pool=pool,
        )
        cmd = [
            str(login_py), str(script),
            "--email", acc.email,
            "--codex-home", acc.codex_home,
            "--attempt-id", attempt_id,
            "--credentials-stdin",
        ]
        if mail_provider:
            cmd.extend(["--mail-provider", mail_provider])
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            env=login_child_environment(extra={"PYTHONUNBUFFERED": "1"}),
            start_new_session=True,
        )
        await _send_login_credentials(
            proc,
            attempt_id=attempt_id,
            token=receiver_token,
            password=openai_password,
        )
        _relogin_state[account_id] = {
            "status": "running",
            "started_at": time.time(),
            "attempt_id": attempt_id,
        }
        _login_attempts[attempt_id] = {
            "kind": "relogin",
            "state_key": account_id,
            "proc": proc,
            "challenge_id": None,
            "expires_at": None,
        }
        asyncio.get_running_loop().create_task(
            _watch_relogin(
                account_id,
                attempt_id,
                proc,
                instance_manager,
                acc.codex_home,
                login_lock,
                journal_path,
                pool_path,
            )
        )
        watcher_started = True
        return {"ok": True, "status": "running", "attempt_id": attempt_id}
    finally:
        if not watcher_started:
            if "attempt_id" in locals():
                _login_attempts.pop(attempt_id, None)
            if proc is not None and journal_path is not None:
                await _await_login_cleanup(_finalize_login_transaction(
                    proc=proc,
                    operation=f"relogin startup for {account_id}",
                    journal_path=journal_path,
                    commit_requested=False,
                    instance_manager=instance_manager,
                    codex_home=acc.codex_home,
                    login_lock=login_lock,
                    attempt_id=attempt_id,
                    state_store=_relogin_state,
                    state_key=account_id,
                    expected_pool_path=pool_path,
                ))
            else:
                if journal_path is not None:
                    await _await_login_cleanup(
                        _rollback_unspawned_login_transaction(
                            journal_path=journal_path,
                            instance_manager=instance_manager,
                            codex_home=acc.codex_home,
                            login_lock=login_lock,
                            attempt_id=attempt_id,
                            state_store=_relogin_state,
                            state_key=account_id,
                            expected_pool_path=pool_path,
                        )
                    )
                else:
                    try:
                        if maintenance_started:
                            await instance_manager.end_codex_app_server_home_maintenance(
                                acc.codex_home
                            )
                    finally:
                        if login_lock.locked():
                            login_lock.release()


@router.get("/accounts/{account_id}/relogin")
async def codex_relogin_status(request: Request, account_id: str):
    require_admin(request)
    return _relogin_state.get(account_id) or {"status": "idle"}


# ---------------------------------------------------------------------------
# Add account
# ---------------------------------------------------------------------------

class AddCodexAccountRequest(BaseModel):
    email: str
    token: str = ""  # Optional; only needed when OpenAI requests an email OTP.
    password: str = ""
    login_method: str = ""


async def _ensure_xvfb():
    try:
        return await ensure_login_runtime()
    except LoginRuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


async def _watch_add(
    email: str,
    account_id: str,
    attempt_id: str,
    proc: asyncio.subprocess.Process,
    instance_manager,
    codex_home: str,
    login_lock: asyncio.Lock,
    journal_path: Path,
    expected_pool_path: Path | None = None,
):
    watch_completed = False
    try:
        tail = await _collect_login_output(proc, attempt_id)
        watch_completed = True
        previous_state = _add_state.get(email, {})
        previous_status = previous_state.get("status")
        _add_state[email] = {
            "status": (
                "finalizing" if proc.returncode == 0
                else previous_status
                if previous_status in {"expired", "cancelled"}
                else "failed"
            ),
            "detail": tail,
            "attempt_id": attempt_id,
            **(
                {"account_id": previous_state["account_id"]}
                if previous_state.get("account_id") else {}
            ),
        }
        if proc.returncode != 0:
            _add_state[email]["finished_at"] = time.time()
    except Exception as exc:
        logger.exception("Codex add-account watcher failed for %s", email)
        previous_state = _add_state.get(email, {})
        _add_state[email] = {
            "status": "failed",
            "detail": str(exc),
            "finished_at": time.time(),
            "attempt_id": attempt_id,
            **(
                {"account_id": previous_state["account_id"]}
                if previous_state.get("account_id") else {}
            ),
        }
    finally:
        await _await_login_cleanup(_finalize_login_transaction(
            proc=proc,
            operation=f"add-account for {email}",
            journal_path=journal_path,
            commit_requested=watch_completed and proc.returncode == 0,
            instance_manager=instance_manager,
            codex_home=codex_home,
            login_lock=login_lock,
            attempt_id=attempt_id,
            state_store=_add_state,
            state_key=email,
            expected_pool_path=expected_pool_path,
        ))


def _allocate_codex_account_home(pool) -> tuple[str, str]:
    """Allocate the lowest safe retired slot, then a never-used numeric slot.

    A failed first login can leave a harmless directory containing no auth or
    rollout; that exact slot is reusable so retries do not skip account ids.
    A fully cleaned retired home is also reusable because its retained sessions
    are the only account data left.  Pending cleanup/recovery/login transactions
    remain fail-closed and active account ids are never renumbered.
    """
    # Keep the safety invariant inside the allocator too, rather than relying
    # only on its HTTP caller.  An in-flight add/relogin always owns a journal.
    _reject_unresolved_login_transactions(pool)

    accounts_by_id = {account.id: account for account in pool._accounts}
    index = 1
    while True:
        account_id = f"codex-{index}"
        existing_account = accounts_by_id.get(account_id)
        if existing_account is not None:
            if _clean_retired_home_is_reusable(existing_account):
                return existing_account.id, existing_account.codex_home
            index += 1
            continue
        codex_home = (
            Path.home() / ".codex"
            if index == 1
            else Path.home() / f".codex-{account_id}"
        )
        reusable_existing_home = _failed_login_home_is_reusable(codex_home)
        if not codex_home.exists() or reusable_existing_home:
            return account_id, str(codex_home)
        index += 1


@router.post("/add")
async def codex_add_account(request: Request, body: AddCodexAccountRequest):
    require_admin(request)
    email = body.email.strip()
    receiver_token = body.token.strip()
    if not email:
        raise HTTPException(400, "email 必填")
    login_method = body.login_method.strip().lower()
    if login_method and login_method not in MAIL_PROVIDERS:
        raise HTTPException(400, f"Unsupported login_method: {body.login_method}")

    state = _add_state.get(email)
    if state and state.get("status") in ACTIVE_LOGIN_STATUSES:
        return {
            "ok": True,
            "status": state["status"],
            "attempt_id": state.get("attempt_id"),
            "account_id": state.get("account_id"),
        }

    if _login_lock.locked():
        running = [
            key for key, value in {**_relogin_state, **_add_state}.items()
            if value.get("status") in ACTIVE_LOGIN_STATUSES
        ]
        raise HTTPException(
            status_code=409,
            detail=f"另一个账号正在登录中（{', '.join(running)}）",
        )

    pool = _get_pool()
    _reject_unresolved_login_transactions(pool)
    account_id, codex_home = _allocate_codex_account_home(pool)
    allocated_account = pool.account(account_id) if hasattr(pool, "account") else next(
        (account for account in pool._accounts if account.id == account_id),
        None,
    )
    reusing_retired_slot = bool(
        allocated_account is not None
        and getattr(allocated_account, "retired", False)
    )

    root = Path(__file__).resolve().parents[2]
    login_py = root / ".venv" / "bin" / "python3"
    if not login_py.exists():
        raise HTTPException(status_code=501, detail="Python venv not found")

    script = root / "scripts" / "codex_login.py"
    attempt_id = uuid.uuid4().hex
    pool_path = _pool_config_path(pool)
    cmd = [
        str(login_py), str(script),
        "--email", email,
        "--codex-home", codex_home,
        "--add-to-pool", account_id,
        "--save-token",
        "--attempt-id", attempt_id,
        "--credentials-stdin",
        "--pool-config", str(pool_path),
        "--credential-store", str(_credential_store_path(pool)),
    ]
    if login_method:
        cmd.extend(["--mail-provider", login_method])

    instance_manager = _get_instance_manager()
    login_lock = _login_lock
    await login_lock.acquire()
    maintenance_started = False
    watcher_started = False
    proc: asyncio.subprocess.Process | None = None
    journal_path: Path | None = None
    try:
        # The browser/Xvfb runtime and account-id allocation are process-wide;
        # serialize the full login and reserve the destination CODEX_HOME so
        # credentials cannot change underneath an active exec/app-server turn.
        await _ensure_xvfb()
        try:
            await instance_manager.begin_codex_app_server_home_maintenance(
                codex_home, require_idle=True,
            )
        except CodexAppServerBusyError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        maintenance_started = True
        if reusing_retired_slot:
            # Re-prove the finalized tombstone under home maintenance, then
            # normalize it once more before the wrapper can write new auth.
            current_account = (
                pool.account(account_id)
                if hasattr(pool, "account")
                else allocated_account
            )
            if (
                current_account is None
                or current_account.codex_home != codex_home
                or not _clean_retired_home_is_reusable(current_account)
            ):
                raise HTTPException(
                    status_code=409,
                    detail=f"Codex 账号槽位 {account_id} 已变化，请重试",
                )
            _purge_retired_codex_home(Path(codex_home), account_id)
        elif Path(codex_home).exists():
            # A failed first login has no pool record or rollback marker.
            # Re-prove and remove only its credential-free runtime residue
            # while the destination home is fenced from app-server traffic.
            try:
                _purge_failed_login_codex_home(Path(codex_home))
            except (OSError, RuntimeError) as exc:
                raise HTTPException(
                    status_code=409,
                    detail=f"Codex 账号槽位 {account_id} 已变化，请重试",
                ) from exc
        journal_path = _begin_login_transaction(
            attempt_id=attempt_id,
            kind="add",
            account_id=account_id,
            codex_home=codex_home,
            pool=pool,
            reused_retired_slot=reusing_retired_slot,
            expected_email=email if reusing_retired_slot else None,
        )

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            env=login_child_environment(extra={"PYTHONUNBUFFERED": "1"}),
            start_new_session=True,
        )
        await _send_login_credentials(
            proc,
            attempt_id=attempt_id,
            token=receiver_token,
            password=body.password,
        )
        _add_state[email] = {
            "status": "running",
            "started_at": time.time(),
            "account_id": account_id,
            "attempt_id": attempt_id,
        }
        _login_attempts[attempt_id] = {
            "kind": "add",
            "state_key": email,
            "proc": proc,
            "challenge_id": None,
            "expires_at": None,
        }
        asyncio.get_running_loop().create_task(
            _watch_add(
                email,
                account_id,
                attempt_id,
                proc,
                instance_manager,
                codex_home,
                login_lock,
                journal_path,
                pool_path,
            )
        )
        watcher_started = True
        return {
            "ok": True,
            "status": "running",
            "account_id": account_id,
            "attempt_id": attempt_id,
        }
    finally:
        if not watcher_started:
            _login_attempts.pop(attempt_id, None)
            if proc is not None and journal_path is not None:
                await _await_login_cleanup(_finalize_login_transaction(
                    proc=proc,
                    operation=f"add-account startup for {email}",
                    journal_path=journal_path,
                    commit_requested=False,
                    instance_manager=instance_manager,
                    codex_home=codex_home,
                    login_lock=login_lock,
                    attempt_id=attempt_id,
                    state_store=_add_state,
                    state_key=email,
                    expected_pool_path=pool_path,
                ))
            else:
                if journal_path is not None:
                    await _await_login_cleanup(
                        _rollback_unspawned_login_transaction(
                            journal_path=journal_path,
                            instance_manager=instance_manager,
                            codex_home=codex_home,
                            login_lock=login_lock,
                            attempt_id=attempt_id,
                            state_store=_add_state,
                            state_key=email,
                            expected_pool_path=pool_path,
                        )
                    )
                else:
                    try:
                        if maintenance_started:
                            await instance_manager.end_codex_app_server_home_maintenance(
                                codex_home,
                            )
                    finally:
                        if login_lock.locked():
                            login_lock.release()


@router.get("/add/{email}")
async def codex_add_status(request: Request, email: str):
    require_admin(request)
    return _add_state.get(email) or {"status": "idle"}


# ---------------------------------------------------------------------------
# Human-assisted email verification
# ---------------------------------------------------------------------------

class SubmitCodexOtpRequest(BaseModel):
    challenge_id: str
    code: str


@router.post("/login-attempts/{attempt_id}/otp")
async def codex_submit_login_otp(
    request: Request,
    attempt_id: str,
    body: SubmitCodexOtpRequest,
):
    """Deliver one user-entered OTP to the still-running browser login."""
    require_admin(request)
    attempt = _login_attempts.get(attempt_id)
    if not attempt:
        raise HTTPException(status_code=404, detail="登录流程已结束或不存在")

    state = _attempt_state(attempt)
    if state.get("status") != "awaiting_otp":
        raise HTTPException(status_code=409, detail="当前登录流程不在等待验证码")
    if body.challenge_id != attempt.get("challenge_id"):
        raise HTTPException(status_code=409, detail="验证码挑战已更新，请使用最新页面")
    if float(attempt.get("expires_at") or 0) <= time.time():
        raise HTTPException(status_code=409, detail="验证码挑战已过期，请重新登录")

    code = body.code.strip()
    if not re.fullmatch(r"\d{6}", code):
        raise HTTPException(status_code=422, detail="请输入 6 位数字验证码")

    proc = attempt.get("proc")
    stdin = getattr(proc, "stdin", None)
    if proc is None or proc.returncode is not None or stdin is None:
        raise HTTPException(status_code=409, detail="登录进程已经结束")

    payload = json.dumps({
        "challenge_id": body.challenge_id,
        "code": code,
    }, separators=(",", ":"))
    try:
        stdin.write((payload + "\n").encode("utf-8"))
        await stdin.drain()
    except (BrokenPipeError, ConnectionError, RuntimeError) as exc:
        raise HTTPException(status_code=409, detail="登录进程已无法接收验证码") from exc

    # Never retain the OTP. Only the opaque challenge id remains in state.
    state.update({
        "status": "verifying_otp",
        "attempt_id": attempt_id,
        "challenge_id": body.challenge_id,
    })
    return {"ok": True, "status": "verifying_otp"}


@router.delete("/login-attempts/{attempt_id}")
async def codex_cancel_login(request: Request, attempt_id: str):
    """Abort an interactive login and wait until its rollback releases locks."""
    require_admin(request)
    attempt = _login_attempts.get(attempt_id)
    if not attempt:
        raise HTTPException(status_code=404, detail="登录流程已结束或不存在")

    state = _attempt_state(attempt)
    if state.get("status") not in ACTIVE_LOGIN_STATUSES:
        raise HTTPException(status_code=409, detail="当前登录流程无法取消")
    proc = attempt.get("proc")
    if proc is None or proc.returncode is not None:
        raise HTTPException(status_code=409, detail="登录进程已经结束")

    state.update({
        "status": "cancelled",
        "detail": "登录已取消，凭据变更已回滚",
        "attempt_id": attempt_id,
    })
    try:
        await _stop_unfinished_login_process(
            proc,
            operation=f"cancelled {attempt.get('kind', 'login')}",
        )
    except LoginProcessNotTerminal as exc:
        raise HTTPException(
            status_code=503,
            detail=f"登录进程取消状态无法确认，请重启 CCM 完成恢复：{exc}",
        ) from exc

    # The watcher owns journal rollback and home-maintenance release.  Do not
    # report cancellation complete while a following account would still hit
    # the process-wide login lock.
    deadline = asyncio.get_running_loop().time() + LOGIN_REAP_TIMEOUT_SECONDS
    while attempt_id in _login_attempts and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.05)
    if attempt_id in _login_attempts:
        raise HTTPException(
            status_code=503,
            detail="登录进程已停止，但凭据回滚尚未完成，请稍后重试",
        )
    return {"ok": True, "status": "cancelled"}


# ---------------------------------------------------------------------------
# Delete account
# ---------------------------------------------------------------------------

@router.delete("/accounts/{account_id}")
async def codex_delete_account(request: Request, account_id: str):
    require_admin(request)
    pool = _get_pool()
    acc = pool.account(account_id)
    if not acc or (
        getattr(acc, "retired", False)
        and not getattr(acc, "cleanup_pending", False)
    ):
        raise HTTPException(status_code=404, detail=f"Unknown account: {account_id}")
    if is_api_auth_kind(getattr(acc, "auth_kind", "")):
        raise HTTPException(
            status_code=400,
            detail="API 账号请通过 API 账号删除入口处理",
        )

    if _login_lock.locked():
        raise HTTPException(
            status_code=409,
            detail="另一个 Codex 账号登录或删除操作正在进行中",
        )
    _reject_unresolved_login_transactions(pool)

    instance_manager = _get_instance_manager()
    dispatcher = _get_dispatcher()

    monitor_users = await dispatcher.codex_monitor_runtime_users(
        acc.codex_home,
        account_id=acc.id,
    )
    if monitor_users:
        raise HTTPException(
            status_code=409,
            detail=(
                "Codex account is still used by "
                + ", ".join(monitor_users[:5])
                + "; stop it and retry deletion"
            ),
        )
    await _login_lock.acquire()
    maintenance_started = False
    try:
        try:
            await instance_manager.begin_codex_app_server_home_maintenance(
                acc.codex_home, require_idle=True,
            )
        except CodexAppServerBusyError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        maintenance_started = True
        monitor_users = await dispatcher.codex_monitor_runtime_users(
            acc.codex_home,
            account_id=acc.id,
        )
        if monitor_users:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Codex account acquired a Monitor owner before deletion "
                    "was fenced; retry after stopping "
                    + ", ".join(monitor_users[:5])
                ),
            )

        pool_path = _pool_config_path(pool)
        data = json.loads(pool_path.read_text())
        original_data = json.loads(json.dumps(data))
        accounts = data.get("accounts")
        if not isinstance(accounts, list):
            raise HTTPException(status_code=500, detail="Invalid Codex pool config")

        target_record = next(
            (
                record for record in accounts
                if isinstance(record, dict) and record.get("id") == account_id
            ),
            None,
        )
        if target_record is None:
            raise HTTPException(status_code=404, detail=f"Unknown account: {account_id}")

        try:
            managed_home = _managed_codex_home_path(Path(acc.codex_home))
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

        # Remove reusable mailbox/OpenAI credentials unless another live pool
        # entry intentionally shares this email identity.
        account_email = str(getattr(acc, "email", "") or "")
        shared_email = any(
            isinstance(record, dict)
            and record is not target_record
            and not record.get("retired", False)
            and str(record.get("email") or "").casefold() == account_email.casefold()
            for record in accounts
        )
        tokens_path = _credential_store_path(pool)
        filtered_credentials: dict | None = None
        if account_email and not shared_email and tokens_path.exists():
            try:
                saved = json.loads(tokens_path.read_text())
            except (OSError, ValueError) as exc:
                raise HTTPException(
                    status_code=500,
                    detail="Unable to safely read saved Codex credentials",
                ) from exc
            if not isinstance(saved, dict):
                raise HTTPException(
                    status_code=500,
                    detail="Invalid saved Codex credential store",
                )
            filtered_credentials = {
                key: value for key, value in saved.items()
                if not isinstance(key, str) or key.casefold() != account_email.casefold()
            }

        # A hidden pending tombstone disables selection before destructive
        # cleanup. Keep only the email identity temporarily so a failed cleanup
        # can be retried through this endpoint; it is cleared on success.
        target_record.clear()
        target_record.update({
            "id": account_id,
            "codex_home": acc.codex_home,
            "email": account_email,
            "enabled": False,
            "retired": True,
            "cleanup_pending": True,
        })

        # Commit the disabled tombstone before deleting credentials. If this
        # atomic write/reload fails, the active account and all of its auth data
        # remain untouched; after it succeeds, no new work can select the home.
        _write_private_json(pool_path, data)
        try:
            pool.reload()
            retired_account = pool.account(account_id)
            if not retired_account or not (
                getattr(retired_account, "retired", False)
                and getattr(retired_account, "cleanup_pending", False)
            ):
                raise RuntimeError("retired tombstone was not loaded")
        except Exception:
            logger.exception("Failed to reload Codex pool after retiring %s", account_id)
            try:
                _write_private_json(pool_path, original_data)
                pool.reload()
            except Exception:
                logger.exception(
                    "Failed to restore Codex pool config after retiring %s",
                    account_id,
                )
            raise HTTPException(
                status_code=500,
                detail="Pool reload failed; account deletion was rolled back",
            )

        cleanup_errors: list[str] = []
        if filtered_credentials is not None:
            try:
                if filtered_credentials:
                    _write_private_json(tokens_path, filtered_credentials)
                else:
                    tokens_path.unlink(missing_ok=True)
            except Exception as exc:
                logger.exception("Failed to scrub saved credentials for %s", account_id)
                cleanup_errors.append(f"saved credentials: {exc}")

        try:
            _purge_retired_codex_home(managed_home, account_id)
        except Exception as exc:
            logger.exception("Failed to purge retired CODEX_HOME for %s", account_id)
            cleanup_errors.append(f"CODEX_HOME: {exc}")

        if cleanup_errors:
            raise HTTPException(
                status_code=500,
                detail=(
                    "Account is disabled, but private-data cleanup was incomplete: "
                    + "; ".join(cleanup_errors)
                ),
            )

        # Cleanup is complete. Remove the temporary email/retry marker while
        # retaining the hidden id -> home tombstone for old task migrations.
        target_record.clear()
        target_record.update({
            "id": account_id,
            "codex_home": acc.codex_home,
            "email": "",
            "enabled": False,
            "retired": True,
        })
        _write_private_json(pool_path, data)
        pool.reload()
        finalized_account = pool.account(account_id)
        if not finalized_account or not (
            getattr(finalized_account, "retired", False)
            and not getattr(finalized_account, "cleanup_pending", False)
        ):
            raise HTTPException(
                status_code=500,
                detail="Private data was removed, but deletion finalization failed",
            )
        _relogin_state.pop(account_id, None)
        for state_email, state in list(_add_state.items()):
            if (
                str(state_email).casefold() == account_email.casefold()
                or state.get("account_id") == account_id
            ):
                _add_state.pop(state_email, None)
        return {
            "ok": True,
            "deleted": account_id,
            "retained_sessions": True,
        }
    finally:
        try:
            if maintenance_started:
                await instance_manager.end_codex_app_server_home_maintenance(
                    acc.codex_home
                )
        finally:
            if _login_lock.locked():
                _login_lock.release()


# ---------------------------------------------------------------------------
# Preferred account
# ---------------------------------------------------------------------------

@router.post("/preferred")
async def codex_set_preferred(request: Request, body: dict):
    """Pin a Codex route, or clear it to restore automatic selection.

    A compatible available pin takes effect on the next turn. Existing native
    rollout context is copied and the app-server thread is rebound before the
    Task binding changes; a failed migration keeps the intact resident route.
    Without a pin, resident threads stay sticky and fresh routes prefer API.
    """
    require_admin(request)
    pool = _get_pool()
    account_id = body.get("account_id")
    if not pool.set_preferred(account_id):
        raise HTTPException(status_code=404, detail=f"Unknown account: {account_id}")
    return {"ok": True, "preferred": pool.preferred_account_id}
