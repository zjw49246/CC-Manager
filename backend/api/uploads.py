import asyncio
from dataclasses import dataclass
import logging
import os
import re
import stat
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, UploadFile, File
from fastapi.responses import FileResponse

from backend.config import settings
from backend.services.upload_references import is_managed_upload_basename

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/uploads", tags=["uploads"])

# Project root / uploads
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
UPLOAD_DIR = _PROJECT_ROOT / "uploads"

_CLEANUP_MAX_AGE_DAYS = 15
_CLEANUP_INTERVAL_HOURS = 24
_BLOCKED_EXTENSIONS = {".exe"}
_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".webp"}
_MAX_SIZE_BYTES = 50 * 1024 * 1024  # 50 MB
_MAX_TOTAL_SIZE_BYTES = 50 * 1024 * 1024  # bound request memory
_MAX_FILES = 10
_SAFE_EXTENSION_RE = re.compile(r"^\.[a-z0-9]{1,16}$")
_UPLOAD_FS_LOCK = threading.RLock()


class UploadAttachmentValidationError(ValueError):
    """A client-provided upload reference is not one CCM created."""


@dataclass(frozen=True, slots=True)
class ValidatedUploadAttachment:
    """Server-authoritative metadata for one already-uploaded regular file."""

    path: str
    url: str
    name: str
    is_image: bool

    def public_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "name": self.name,
            "is_image": self.is_image,
        }


def _get_upload_dir() -> Path:
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    return UPLOAD_DIR


def _upload_storage_usage_locked(upload_dir: Path) -> tuple[int, int]:
    """Return regular-file bytes/count while ``_UPLOAD_FS_LOCK`` is held."""

    total_bytes = 0
    total_files = 0
    try:
        for candidate in upload_dir.iterdir():
            try:
                info = candidate.stat(follow_symlinks=False)
            except FileNotFoundError:
                # In-process writers and cleanup use the same lock. Tolerate a
                # file removed independently by an administrator.
                continue
            if stat.S_ISREG(info.st_mode):
                total_bytes += max(0, info.st_size)
                total_files += 1
    except OSError as exc:
        # A quota that cannot be measured must fail closed; accepting the
        # upload would make the configured hard limit advisory.
        raise HTTPException(
            503,
            "Upload storage quota could not be verified",
        ) from exc
    return total_bytes, total_files


def _commit_uploaded_files(
    pending: list[tuple[str, str, bytes, str, str]],
    total_size: int,
) -> list[dict[str, Any]]:
    """Atomically admit and persist one validated upload batch."""

    written: list[Path] = []
    results: list[dict[str, Any]] = []
    with _UPLOAD_FS_LOCK:
        upload_dir = _get_upload_dir()
        existing_size, existing_files = _upload_storage_usage_locked(upload_dir)
        if (
            existing_size + total_size > settings.upload_max_total_bytes
            or existing_files + len(pending) > settings.upload_max_total_files
        ):
            raise HTTPException(507, "Upload storage quota exceeded")
        try:
            for display_name, ext, data, file_id, saved_name in pending:
                save_path = upload_dir / saved_name
                # Exclusive creation preserves the UUID capability contract
                # without ever following or overwriting an unexpected entry.
                with save_path.open("xb") as destination:
                    written.append(save_path)
                    destination.write(data)
                results.append(
                    {
                        "id": file_id,
                        "filename": display_name,
                        "path": str(save_path.resolve()),
                        "url": f"/api/uploads/{saved_name}",
                        "is_image": ext in _IMAGE_EXTENSIONS,
                    }
                )
        except BaseException:
            for path in written:
                try:
                    path.unlink()
                except FileNotFoundError:
                    pass
                except OSError:
                    logger.exception("Failed to roll back partial upload %s", path)
            raise
    return results


def _safe_display_filename(value: str | None) -> str:
    """Normalize the untrusted multipart filename used only for display."""

    raw = value or "file"
    name = os.path.basename(raw.replace("\\", "/"))
    if (
        not name
        or name in {".", ".."}
        or len(name) > 255
        or any(ord(character) < 32 or ord(character) == 127 for character in name)
    ):
        raise HTTPException(400, "Invalid upload filename")
    return name


def _safe_saved_extension(filename: str) -> str:
    extension = Path(filename).suffix.lower()
    return extension if _SAFE_EXTENSION_RE.fullmatch(extension) else ""


def validate_upload_attachments(
    *,
    file_paths: list[str] | None,
    image_paths: list[str] | None = None,
    attachments: list[dict[str, Any]] | None = None,
) -> list[ValidatedUploadAttachment]:
    """Validate and refresh attachment TTL against the cleanup thread."""

    with _UPLOAD_FS_LOCK:
        return _validate_upload_attachments_locked(
            file_paths=file_paths,
            image_paths=image_paths,
            attachments=attachments,
        )


def _validate_upload_attachments_locked(
    *,
    file_paths: list[str] | None,
    image_paths: list[str] | None = None,
    attachments: list[dict[str, Any]] | None = None,
) -> list[ValidatedUploadAttachment]:
    """Resolve client attachment references without trusting paths or flags.

    Uploaded paths are capabilities returned by ``POST /api/uploads``.  Only
    direct, owned, non-symlink regular files below this process's upload root
    are accepted.  Metadata is derived from those files; when a caller sends
    display metadata, its ordering and URL/type claims must match exactly.
    """

    raw_files = list(file_paths or [])
    raw_images = list(image_paths or [])
    # ``image_paths`` predates ``file_paths``.  Keep an image-only legacy
    # client working, but never silently union two independent attachment
    # lists because that would break the attachment metadata ordering.
    if not raw_files and raw_images:
        raw_files = list(raw_images)
    if len(raw_files) > _MAX_FILES:
        raise UploadAttachmentValidationError(
            f"Maximum {_MAX_FILES} files allowed per request"
        )
    if len(set(raw_files)) != len(raw_files):
        raise UploadAttachmentValidationError("Duplicate upload paths are not allowed")
    if len(set(raw_images)) != len(raw_images):
        raise UploadAttachmentValidationError("Duplicate image paths are not allowed")

    upload_root = _get_upload_dir().resolve(strict=True)
    resolved: list[tuple[str, str, bool, int]] = []
    by_raw_path: dict[str, tuple[str, bool]] = {}
    total_size = 0
    open_flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    for raw_path in raw_files:
        if not isinstance(raw_path, str) or not raw_path or "\x00" in raw_path:
            raise UploadAttachmentValidationError("Invalid upload path")
        if raw_path.startswith("/api/uploads/"):
            # Public Task projections never disclose the absolute host upload
            # root.  A managed URL is the opaque browser-side reference and is
            # resolved only inside this already-confined validation boundary.
            filename = raw_path.removeprefix("/api/uploads/")
            if not is_managed_upload_basename(filename):
                raise UploadAttachmentValidationError(
                    "Attachment URL is not a CCM-managed upload"
                )
            candidate = upload_root / filename
        elif os.path.isabs(raw_path):
            candidate = Path(os.path.abspath(raw_path))
        else:
            raise UploadAttachmentValidationError("Invalid upload path")
        if candidate.parent != upload_root:
            raise UploadAttachmentValidationError(
                "Attachments must come from the CCM upload directory"
            )
        if not is_managed_upload_basename(candidate.name):
            raise UploadAttachmentValidationError(
                "Attachment path is not a CCM-managed upload"
            )
        if candidate.suffix.lower() in _BLOCKED_EXTENSIONS:
            raise UploadAttachmentValidationError("Blocked upload type")
        try:
            descriptor = os.open(candidate, open_flags)
        except OSError as exc:
            raise UploadAttachmentValidationError(
                "Uploaded file is missing or unsafe"
            ) from exc
        try:
            opened = os.fstat(descriptor)
            current = candidate.stat(follow_symlinks=False)
            if (
                not stat.S_ISREG(opened.st_mode)
                or stat.S_ISLNK(current.st_mode)
                or not stat.S_ISREG(current.st_mode)
                or opened.st_dev != current.st_dev
                or opened.st_ino != current.st_ino
                or (
                    hasattr(opened, "st_uid")
                    and opened.st_uid != os.getuid()
                )
                or opened.st_size > _MAX_SIZE_BYTES
            ):
                raise UploadAttachmentValidationError(
                    "Uploaded file is not a safe regular file"
                )
            size = opened.st_size
        finally:
            os.close(descriptor)
        total_size += size
        if total_size > _MAX_TOTAL_SIZE_BYTES:
            raise UploadAttachmentValidationError(
                "Combined uploads exceed 50 MB limit"
            )
        canonical = str(candidate)
        is_image = candidate.suffix.lower() in _IMAGE_EXTENSIONS
        resolved.append((canonical, candidate.name, is_image, size))
        by_raw_path[raw_path] = (canonical, is_image)

    for raw_image in raw_images:
        match = by_raw_path.get(raw_image)
        if match is None:
            raise UploadAttachmentValidationError(
                "image_paths must be a subset of file_paths"
            )
        if not match[1]:
            raise UploadAttachmentValidationError(
                "image_paths contains a non-image upload"
            )

    if attachments is not None and len(attachments) != len(resolved):
        raise UploadAttachmentValidationError(
            "Attachment metadata must match file_paths ordering"
        )

    result: list[ValidatedUploadAttachment] = []
    for index, (canonical, saved_name, is_image, _size) in enumerate(resolved):
        url = f"/api/uploads/{saved_name}"
        display_name = saved_name
        if attachments is not None:
            supplied = attachments[index]
            if not isinstance(supplied, dict):
                raise UploadAttachmentValidationError(
                    "Invalid attachment metadata"
                )
            supplied_name = supplied.get("name")
            if (
                not isinstance(supplied_name, str)
                or not supplied_name
                or len(supplied_name) > 255
                or supplied_name != os.path.basename(supplied_name)
                or supplied_name in {".", ".."}
                or any(ord(character) < 32 for character in supplied_name)
                or supplied.get("url") != url
                or type(supplied.get("is_image")) is not bool
                or supplied["is_image"] is not is_image
            ):
                raise UploadAttachmentValidationError(
                    "Attachment metadata does not match the uploaded file"
                )
            display_name = supplied_name
        result.append(ValidatedUploadAttachment(
            path=canonical,
            url=url,
            name=display_name,
            is_image=is_image,
        ))
    # Reusing a fork attachment is an active reference. Refresh its cleanup
    # TTL while serialized with the expiry scanner so it cannot disappear
    # between validation and transport admission.
    try:
        for upload in result:
            os.utime(upload.path, None, follow_symlinks=False)
    except OSError as exc:
        raise UploadAttachmentValidationError(
            "Uploaded file changed while it was being validated"
        ) from exc
    return result


@router.post("")
async def upload_files(files: list[UploadFile] = File(...)):
    """Upload up to 10 files. Returns list of {id, filename, path, url, is_image}."""
    if len(files) > _MAX_FILES:
        raise HTTPException(400, f"Maximum {_MAX_FILES} files allowed per request")

    pending: list[tuple[str, str, bytes, str, str]] = []
    total_size = 0
    for file in files:
        display_name = _safe_display_filename(file.filename)
        ext = _safe_saved_extension(display_name)
        if ext in _BLOCKED_EXTENSIONS:
            raise HTTPException(400, f"File type '{ext}' is not allowed")

        remaining = _MAX_TOTAL_SIZE_BYTES - total_size
        data = await file.read(min(_MAX_SIZE_BYTES, remaining) + 1)
        if len(data) > _MAX_SIZE_BYTES:
            raise HTTPException(400, f"File '{file.filename}' exceeds 50 MB limit")
        if len(data) > remaining:
            raise HTTPException(400, "Combined uploads exceed 50 MB limit")
        total_size += len(data)

        file_id = str(uuid.uuid4())
        saved_name = f"{file_id}{ext}" if ext else file_id
        pending.append((display_name, ext, data, file_id, saved_name))

    # Validation is intentionally all-or-nothing.  If any later write fails,
    # remove files created by this request so a 4xx/5xx never leaves uploads
    # that the client did not receive identifiers for.
    # Quota scanning and filesystem writes are synchronous. Keep their shared
    # lock off the event loop because the cleanup thread may currently own it.
    return await asyncio.to_thread(_commit_uploaded_files, pending, total_size)


@router.get("/{filename}")
async def get_file(filename: str):
    """Serve an uploaded file."""
    upload_dir = _get_upload_dir().resolve()
    file_path = upload_dir / filename

    try:
        resolved_path = file_path.resolve(strict=True)
    except FileNotFoundError:
        raise HTTPException(404, "File not found")
    try:
        resolved_path.relative_to(upload_dir)
    except ValueError:
        raise HTTPException(400, "Invalid filename")
    # Uploads are regular files created by this endpoint.  Never follow a
    # repository/user-created symlink through the unauthenticated download
    # route.
    if file_path.is_symlink() or not resolved_path.is_file():
        raise HTTPException(400, "Invalid filename")

    return FileResponse(str(resolved_path))


def cleanup_expired_uploads() -> int:
    """Delete files in UPLOAD_DIR older than _CLEANUP_MAX_AGE_DAYS. Returns count deleted."""
    with _UPLOAD_FS_LOCK:
        if not UPLOAD_DIR.is_dir():
            return 0
        cutoff = time.time() - _CLEANUP_MAX_AGE_DAYS * 86400
        deleted = 0
        for f in UPLOAD_DIR.iterdir():
            if f.is_file() and f.stat().st_mtime < cutoff:
                f.unlink()
                deleted += 1
        return deleted


async def start_upload_cleanup_loop() -> asyncio.Task:
    """Start a background loop that cleans expired uploads every 24 hours."""

    async def _loop():
        while True:
            try:
                deleted = await asyncio.to_thread(cleanup_expired_uploads)
                if deleted:
                    logger.info("Upload cleanup: deleted %d expired file(s)", deleted)
            except Exception:
                logger.exception("Upload cleanup error")
            await asyncio.sleep(_CLEANUP_INTERVAL_HOURS * 3600)

    return asyncio.create_task(_loop())
