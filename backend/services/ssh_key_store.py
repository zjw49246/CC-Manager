from __future__ import annotations

import errno
import os
import re
import secrets
import stat
import time
from dataclasses import dataclass
from pathlib import Path

from backend.services.ssh_executor import (
    openssh_public_key_fingerprint,
    preflight_private_key,
)


MAX_SSH_PRIVATE_KEY_UPLOAD_BYTES = 1024 * 1024
_PENDING_KEY_MAX_AGE_SECONDS = 24 * 60 * 60
_UPLOAD_TOKEN_RE = re.compile(r"^[0-9a-f]{32}$")


class SSHManagedKeyStoreError(ValueError):
    def __init__(self, code: str, detail: str):
        super().__init__(detail)
        self.code = code
        self.detail = detail


@dataclass(frozen=True)
class UploadedSSHPrivateKey:
    upload_token: str
    filename: str
    public_key_fingerprint: str


def _safe_display_filename(filename: str | None) -> str:
    raw = filename or "private-key"
    name = os.path.basename(raw.replace("\\", "/"))
    if (
        not name
        or name in {".", ".."}
        or len(name) > 255
        or any(ord(character) < 32 or ord(character) == 127 for character in name)
    ):
        raise SSHManagedKeyStoreError(
            "upload_filename_invalid", "SSH private-key filename is invalid",
        )
    return name


class SSHManagedKeyStore:
    """Private host storage for browser-uploaded SSH keys.

    Upload tokens name files only in the private pending directory. Saving a
    Profile atomically hard-links one into the managed directory and consumes
    the pending token. Neither the token nor the managed path contains key
    material or leaves the admin API after the Profile is saved.
    """

    def __init__(self, configured_root: str):
        expanded = os.path.expandvars(os.path.expanduser(configured_root))
        if not expanded or not os.path.isabs(expanded):
            raise SSHManagedKeyStoreError(
                "key_store_invalid", "SSH managed-key directory must be an absolute path",
            )
        self.root = Path(os.path.abspath(expanded))
        self.pending_dir = self.root / "pending"
        self.managed_dir = self.root / "managed"

    @staticmethod
    def _ensure_private_directory(path: Path) -> None:
        for ancestor in path.parents:
            try:
                if ancestor.is_symlink():
                    raise SSHManagedKeyStoreError(
                        "key_store_unsafe", "SSH managed-key directory has a symlink ancestor",
                    )
            except OSError as exc:
                raise SSHManagedKeyStoreError(
                    "key_store_unavailable", "SSH managed-key directory is unavailable",
                ) from exc
        try:
            path.mkdir(mode=0o700, parents=True, exist_ok=True)
            info = path.lstat()
        except OSError as exc:
            raise SSHManagedKeyStoreError(
                "key_store_unavailable", "SSH managed-key directory is unavailable",
            ) from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise SSHManagedKeyStoreError(
                "key_store_unsafe", "SSH managed-key directory must not be a symlink",
            )
        if info.st_uid != os.geteuid():
            raise SSHManagedKeyStoreError(
                "key_store_owner", "SSH managed-key directory must be owned by the CCM service user",
            )
        if stat.S_IMODE(info.st_mode) != 0o700:
            try:
                path.chmod(0o700)
            except OSError as exc:
                raise SSHManagedKeyStoreError(
                    "key_store_permissions", "SSH managed-key directory must use mode 0700",
                ) from exc

    def _prepare(self) -> None:
        self._ensure_private_directory(self.root)
        self._ensure_private_directory(self.pending_dir)
        self._ensure_private_directory(self.managed_dir)

    @staticmethod
    def _validate_token(upload_token: str) -> str:
        if not isinstance(upload_token, str) or _UPLOAD_TOKEN_RE.fullmatch(upload_token) is None:
            raise SSHManagedKeyStoreError(
                "upload_token_invalid", "SSH private-key upload is invalid or expired",
            )
        return upload_token

    def _cleanup_expired_pending(self) -> None:
        cutoff = time.time() - _PENDING_KEY_MAX_AGE_SECONDS
        try:
            entries = list(os.scandir(self.pending_dir))
        except OSError:
            return
        for entry in entries:
            if _UPLOAD_TOKEN_RE.fullmatch(entry.name) is None:
                continue
            try:
                info = entry.stat(follow_symlinks=False)
                if (
                    stat.S_ISREG(info.st_mode)
                    and info.st_uid == os.geteuid()
                    and info.st_mtime < cutoff
                ):
                    os.unlink(entry.path)
            except OSError:
                continue

    def store_upload(self, data: bytes, filename: str | None) -> UploadedSSHPrivateKey:
        if not data or len(data) > MAX_SSH_PRIVATE_KEY_UPLOAD_BYTES:
            raise SSHManagedKeyStoreError(
                "key_size", "SSH private key must be between 1 byte and 1 MB",
            )
        display_name = _safe_display_filename(filename)
        self._prepare()
        self._cleanup_expired_pending()

        upload_token = secrets.token_hex(16)
        path = self.pending_dir / upload_token
        flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        try:
            descriptor = os.open(path, flags, 0o600)
        except OSError as exc:
            raise SSHManagedKeyStoreError(
                "key_store_write_failed", "SSH private key could not be stored safely",
            ) from exc
        try:
            os.fchmod(descriptor, 0o600)
            view = memoryview(data)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise OSError(errno.EIO, "short SSH key write")
                view = view[written:]
            os.fsync(descriptor)
        except BaseException:
            try:
                path.unlink()
            except OSError:
                pass
            raise
        finally:
            os.close(descriptor)

        try:
            material = preflight_private_key(path)
        except BaseException:
            try:
                path.unlink()
            except OSError:
                pass
            raise
        return UploadedSSHPrivateKey(
            upload_token=upload_token,
            filename=display_name,
            public_key_fingerprint=openssh_public_key_fingerprint(
                material.openssh_public_key,
            ),
        )

    def claim_upload(self, upload_token: str) -> str:
        token = self._validate_token(upload_token)
        self._prepare()
        pending = self.pending_dir / token
        managed = self.managed_dir / token
        try:
            pending_info = pending.lstat()
            if pending_info.st_mtime < time.time() - _PENDING_KEY_MAX_AGE_SECONDS:
                pending.unlink()
                raise SSHManagedKeyStoreError(
                    "upload_token_invalid", "SSH private-key upload is invalid or expired",
                )
        except FileNotFoundError as exc:
            raise SSHManagedKeyStoreError(
                "upload_token_invalid", "SSH private-key upload is invalid or expired",
            ) from exc
        try:
            # A hard link provides O_EXCL-like destination semantics. Validate
            # the linked inode again. Keep the pending capability until the
            # caller's database transaction commits so failed saves can retry.
            os.link(pending, managed, follow_symlinks=False)
        except (FileNotFoundError, FileExistsError) as exc:
            raise SSHManagedKeyStoreError(
                "upload_token_invalid", "SSH private-key upload is invalid or expired",
            ) from exc
        except OSError as exc:
            raise SSHManagedKeyStoreError(
                "key_store_write_failed", "SSH private key could not be claimed safely",
            ) from exc
        try:
            preflight_private_key(managed)
        except BaseException:
            try:
                managed.unlink()
            except OSError:
                pass
            raise
        return str(managed)

    def finalize_upload(self, upload_token: str) -> None:
        token = self._validate_token(upload_token)
        self._prepare()
        try:
            (self.pending_dir / token).unlink()
        except FileNotFoundError:
            return
        except OSError as exc:
            raise SSHManagedKeyStoreError(
                "key_store_write_failed", "SSH private-key upload could not be finalized safely",
            ) from exc

    def cancel_upload(self, upload_token: str) -> bool:
        token = self._validate_token(upload_token)
        self._prepare()
        try:
            (self.pending_dir / token).unlink()
            return True
        except FileNotFoundError:
            return False
        except OSError as exc:
            raise SSHManagedKeyStoreError(
                "key_store_write_failed", "SSH private-key upload could not be removed safely",
            ) from exc

    def discard_managed_key(self, key_path: str) -> bool:
        """Delete a key only when it is an exact file owned by this store."""

        self._prepare()
        candidate = Path(os.path.abspath(key_path))
        if (
            candidate.parent != self.managed_dir
            or _UPLOAD_TOKEN_RE.fullmatch(candidate.name) is None
        ):
            return False
        try:
            info = candidate.lstat()
            if (
                stat.S_ISLNK(info.st_mode)
                or not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.geteuid()
            ):
                return False
            candidate.unlink()
            return True
        except FileNotFoundError:
            return False
