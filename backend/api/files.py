import asyncio
import logging
import os
import posixpath
import socket
import stat as stat_mod
import tempfile
from pathlib import Path
from typing import Optional

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    Query,
    UploadFile,
)
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.background import BackgroundTask

from backend.api.deps import require_admin
from backend.config import settings
from backend.database import get_db
from backend.models.ssh_profile import SSHProfile
from backend.services.ssh_executor import (
    SSHHostKeyMismatchError,
    SSHKeyPreflightError,
)
from backend.services.ssh_profiles import executor_for_profile
from backend.services.ssh_remote_paths import resolve_existing_remote_path
from backend.services.ssh_sftp import (
    SSHSFTPBusyError,
    SSHSFTPOperationTimeout,
    configure_sftp_channel_timeout,
    run_bounded_sftp,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/files",
    tags=["files"],
    dependencies=[Depends(require_admin)],
)

MAX_FILE_SIZE = 1 * 1024 * 1024  # 1 MB (for reading)
MAX_UPLOAD_SIZE = 50 * 1024 * 1024  # 50 MB (for uploading)
MAX_UPLOAD_TOTAL_SIZE = 50 * 1024 * 1024  # bound request memory
MAX_UPLOAD_FILES = 10
MAX_MANAGED_SSH_DIRECTORY_ENTRIES = 2000


def _unlink_temporary_download(path: str) -> None:
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


def _safe_upload_filename(filename: str | None) -> str:
    """Return one plain filename, rejecting path-bearing client values."""
    name = filename or "upload"
    if (
        name in {".", ".."}
        or "/" in name
        or "\\" in name
        or "\x00" in name
        or Path(name).name != name
    ):
        raise HTTPException(400, "Upload filename must not contain a path")
    return name


# ---------------------------------------------------------------------------
# SSH helpers
# ---------------------------------------------------------------------------

class SSHCreds(BaseModel):
    host: str
    port: int = 22
    username: str
    password: Optional[str] = None
    key_path: Optional[str] = None  # path to private key on the backend machine


class SSHListRequest(SSHCreds):
    path: str


class SSHReadRequest(SSHCreds):
    path: str


class ManagedSSHPathRequest(BaseModel):
    path: str = Field(min_length=1, max_length=4096)

    @field_validator("path")
    @classmethod
    def validate_remote_path(cls, value: str) -> str:
        if not value.startswith("/") or "\x00" in value:
            raise ValueError("Remote path must be an absolute POSIX path")
        return value


def _make_ssh_client(creds: SSHCreds):
    try:
        import paramiko
    except ImportError:
        raise HTTPException(status_code=500, detail="paramiko is not installed")

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        connect_kwargs: dict = {
            "hostname": creds.host,
            "port": creds.port,
            "username": creds.username,
            "timeout": 10,
        }
        key_path = os.path.expanduser(creds.key_path) if creds.key_path else None
        if key_path and os.path.isfile(key_path):
            connect_kwargs["key_filename"] = key_path
        elif creds.password:
            connect_kwargs["password"] = creds.password
        client.connect(**connect_kwargs)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"SSH connection failed: {e}")
    return client


async def _live_managed_ssh_profile(
    profile_id: int,
    db: AsyncSession,
) -> SSHProfile:
    profile = await db.get(SSHProfile, profile_id)
    if profile is None or profile.deleted_at is not None:
        raise HTTPException(404, "SSH profile not found")
    if not profile.enabled:
        raise HTTPException(409, "SSH profile is disabled")
    return profile


def _managed_ssh_error(exc: Exception) -> HTTPException:
    if isinstance(exc, HTTPException):
        return exc
    if isinstance(exc, FileNotFoundError):
        return HTTPException(404, "Remote path not found")
    if isinstance(exc, PermissionError):
        return HTTPException(403, "Remote permission denied")
    if isinstance(exc, SSHHostKeyMismatchError):
        return HTTPException(409, "SSH host key does not match the pinned identity")
    if isinstance(exc, SSHKeyPreflightError):
        return HTTPException(400, {"code": exc.code, "message": exc.detail})
    if isinstance(exc, SSHSFTPBusyError):
        return HTTPException(503, "SSH file capacity is busy; try again shortly")
    if isinstance(exc, SSHSFTPOperationTimeout):
        return HTTPException(504, "SSH file operation timed out")
    if isinstance(exc, (TimeoutError, socket.timeout)):
        return HTTPException(504, "SSH operation timed out")
    logger.warning("Managed SSH file operation failed: %s", type(exc).__name__)
    return HTTPException(400, "SSH operation failed")


def _managed_ssh_list_sync(
    profile: SSHProfile,
    path: str,
) -> tuple[str, list[dict], bool]:
    client = executor_for_profile(profile).connect(timeout=10)
    try:
        sftp = client.open_sftp()
        try:
            configure_sftp_channel_timeout(sftp)
            path = resolve_existing_remote_path(
                sftp,
                path,
                profile.allowed_roots or (),
            )
            attrs = []
            for attr in sftp.listdir_iter(path, read_aheads=10):
                attrs.append(attr)
                if len(attrs) > MAX_MANAGED_SSH_DIRECTORY_ENTRIES:
                    break
            truncated = len(attrs) > MAX_MANAGED_SSH_DIRECTORY_ENTRIES
            entries = []
            for attr in sorted(
                attrs[:MAX_MANAGED_SSH_DIRECTORY_ENTRIES],
                key=lambda item: (
                    not stat_mod.S_ISDIR(item.st_mode or 0),
                    (item.filename or "").lower(),
                ),
            ):
                is_dir = stat_mod.S_ISDIR(attr.st_mode or 0)
                entries.append({
                    "name": attr.filename,
                    "path": posixpath.join(path, attr.filename),
                    "is_dir": is_dir,
                    "size": attr.st_size if not is_dir else None,
                })
            return path, entries, truncated
        finally:
            sftp.close()
    finally:
        client.close()


def _managed_ssh_read_sync(
    profile: SSHProfile,
    path: str,
) -> tuple[str, str, int]:
    client = executor_for_profile(profile).connect(timeout=10)
    try:
        sftp = client.open_sftp()
        try:
            configure_sftp_channel_timeout(sftp)
            path = resolve_existing_remote_path(
                sftp,
                path,
                profile.allowed_roots or (),
            )
            size = sftp.stat(path).st_size or 0
            if size > MAX_FILE_SIZE:
                raise HTTPException(
                    413,
                    f"File too large ({size // 1024} KB). Max is {MAX_FILE_SIZE // 1024} KB.",
                )
            with sftp.open(path, "rb") as remote_file:
                raw = remote_file.read(MAX_FILE_SIZE + 1)
            if len(raw) > MAX_FILE_SIZE:
                raise HTTPException(413, "Remote file exceeds the 1 MB preview limit")
            return path, raw.decode("utf-8", errors="replace"), size
        finally:
            sftp.close()
    finally:
        client.close()


def _remote_download_name(path: str) -> str:
    name = posixpath.basename(path.rstrip("/")) or "download"
    return "".join(character if character.isprintable() else "_" for character in name)


def _managed_ssh_download_sync(
    profile: SSHProfile,
    path: str,
) -> tuple[str, str]:
    client = executor_for_profile(profile).connect(timeout=10)
    temporary_path: str | None = None
    try:
        sftp = client.open_sftp()
        try:
            configure_sftp_channel_timeout(sftp)
            path = resolve_existing_remote_path(
                sftp,
                path,
                profile.allowed_roots or (),
            )
            size = sftp.stat(path).st_size or 0
            if size > MAX_DOWNLOAD_SIZE:
                raise HTTPException(
                    413,
                    f"File too large ({size // 1024 // 1024} MB). Max is {MAX_DOWNLOAD_SIZE // 1024 // 1024} MB.",
                )
            filename = _remote_download_name(path)
            temporary_file = tempfile.NamedTemporaryFile(
                delete=False,
                prefix="ccm-managed-ssh-download-",
                suffix=".tmp",
            )
            temporary_path = temporary_file.name
            try:
                with sftp.open(path, "rb") as remote_file:
                    transferred = 0
                    while True:
                        chunk = remote_file.read(64 * 1024)
                        if not chunk:
                            break
                        transferred += len(chunk)
                        if transferred > MAX_DOWNLOAD_SIZE:
                            raise HTTPException(
                                413,
                                "Remote file exceeds the 100 MB download limit",
                            )
                        temporary_file.write(chunk)
            finally:
                temporary_file.close()
            return temporary_path, filename
        finally:
            sftp.close()
    except BaseException:
        if temporary_path:
            _unlink_temporary_download(temporary_path)
        raise
    finally:
        client.close()


@router.post("/upload")
async def upload_to_directory(
    target_dir: str = Form(...),
    files: list[UploadFile] = File(...),
):
    """Upload files to a specific directory on the server."""
    target = Path(target_dir).expanduser().resolve()
    if not target.exists():
        raise HTTPException(400, f"Directory not found: {target_dir}")
    if not target.is_dir():
        raise HTTPException(400, f"Not a directory: {target_dir}")
    if len(files) > MAX_UPLOAD_FILES:
        raise HTTPException(400, f"Maximum {MAX_UPLOAD_FILES} files per request")

    pending: list[tuple[str, bytes]] = []
    total_size = 0
    for f in files:
        safe_name = _safe_upload_filename(f.filename)
        remaining = MAX_UPLOAD_TOTAL_SIZE - total_size
        data = await f.read(min(MAX_UPLOAD_SIZE, remaining) + 1)
        if len(data) > MAX_UPLOAD_SIZE:
            raise HTTPException(400, f"File '{f.filename}' exceeds 50 MB limit")
        if len(data) > remaining:
            raise HTTPException(400, "Combined uploads exceed 50 MB limit")
        total_size += len(data)
        pending.append((safe_name, data))

    # Validate every part before touching disk.  Unexpected write failures are
    # also rolled back so a failed multipart request never leaves an
    # unreported prefix of files behind.
    results = []
    written: list[Path] = []
    try:
        for safe_name, data in pending:
            save_path = target / safe_name
            stem = save_path.stem
            suffix = save_path.suffix
            counter = 1
            while True:
                try:
                    with save_path.open("xb") as destination:
                        written.append(save_path)
                        destination.write(data)
                    break
                except FileExistsError:
                    save_path = target / f"{stem}_{counter}{suffix}"
                    counter += 1
            results.append({
                "name": save_path.name,
                "path": str(save_path),
                "size": len(data),
            })
    except BaseException:
        for path in written:
            try:
                path.unlink()
            except OSError:
                logger.exception("Failed to roll back partial file upload %s", path)
        raise
    return results


MAX_DIFF_SIZE = 2 * 1024 * 1024  # 2 MB max diff output


async def _run_git(
    repo_path: str, *args: str,
    max_output: int = MAX_DIFF_SIZE,
    allow_nonzero: bool = False,
) -> str:
    """Run a git command in repo_path and return stdout."""
    proc = await asyncio.create_subprocess_exec(
        "git", *args,
        cwd=repo_path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
    if proc.returncode != 0 and not allow_nonzero:
        err_msg = stderr.decode(errors="replace").strip()
        raise HTTPException(400, f"git error: {err_msg}")
    output = stdout[:max_output].decode(errors="replace")
    return output


@router.get("/git/status")
async def git_status(path: str = Query(..., description="Git repository root path")):
    """Get git status (changed files) for a repository."""
    target = Path(path).expanduser().resolve()
    if not target.exists():
        raise HTTPException(404, f"Path not found: {path}")
    if not (target / ".git").exists() and not (target / ".git").is_file():
        raise HTTPException(400, f"Not a git repository: {path}")

    raw = await _run_git(str(target), "status", "--porcelain=v1", "-uall")
    files = []
    for line in raw.splitlines():
        if len(line) < 4:
            continue
        x, y = line[0], line[1]
        filepath = line[3:]
        if " -> " in filepath:
            filepath = filepath.split(" -> ", 1)[1]
        if x == "?" and y == "?":
            status = "untracked"
        elif x in ("A", " ") and y == " ":
            status = "added" if x == "A" else "clean"
        elif x == "D" or y == "D":
            status = "deleted"
        elif x == "M" or y == "M":
            status = "modified"
        elif x == "R":
            status = "renamed"
        else:
            status = "modified"
        if status != "clean":
            files.append({"path": filepath, "status": status, "x": x, "y": y})

    branch = (await _run_git(str(target), "branch", "--show-current")).strip()
    return {"path": str(target), "branch": branch, "files": files}


async def _untracked_diff(repo_path: str, files: list[str]) -> str:
    """Generate diff-like output for untracked files using git diff --no-index."""
    parts = []
    for f in files:
        out = await _run_git(
            repo_path, "diff", "--no-index", "--no-color", "/dev/null", f,
            allow_nonzero=True,
        )
        if out.strip():
            parts.append(out)
    return "\n".join(parts)


async def _get_untracked_files(repo_path: str) -> list[str]:
    """List untracked files in the repo."""
    raw = await _run_git(repo_path, "ls-files", "--others", "--exclude-standard")
    return [f for f in raw.splitlines() if f.strip()]


@router.get("/git/diff")
async def git_diff(
    path: str = Query(..., description="Git repository root path"),
    file: Optional[str] = Query(None, description="Specific file to diff"),
    staged: bool = Query(False, description="Show staged changes"),
):
    """Get git diff output for a repository or specific file."""
    target = Path(path).expanduser().resolve()
    if not target.exists():
        raise HTTPException(404, f"Path not found: {path}")

    repo = str(target)

    if file:
        untracked = await _get_untracked_files(repo)
        if file in untracked:
            diff_output = await _untracked_diff(repo, [file])
        else:
            args = ["diff"]
            if staged:
                args.append("--cached")
            args.extend(["--no-color", "--", file])
            diff_output = await _run_git(repo, *args)
    else:
        args = ["diff"]
        if staged:
            args.append("--cached")
        args.append("--no-color")
        diff_output = await _run_git(repo, *args)

        if not staged:
            untracked = await _get_untracked_files(repo)
            if untracked:
                ut_diff = await _untracked_diff(repo, untracked[:50])
                if ut_diff:
                    diff_output = diff_output + ("\n" if diff_output else "") + ut_diff

    return {"path": repo, "diff": diff_output, "file": file, "staged": staged}


def _safe_path(path: str) -> Path:
    """Resolve path and guard against empty input."""
    if not path or not path.strip():
        raise HTTPException(status_code=400, detail="path is required")
    resolved = Path(path).expanduser().resolve()
    return resolved


@router.get("/list")
async def list_directory(path: str = Query(..., description="Absolute directory path")):
    """List contents of a directory."""
    target = _safe_path(path)
    if not target.exists():
        raise HTTPException(status_code=404, detail=f"Path not found: {path}")
    if not target.is_dir():
        raise HTTPException(status_code=400, detail="Path is not a directory")

    entries = []
    try:
        for entry in sorted(target.iterdir(), key=lambda e: (not e.is_dir(), e.name.lower())):
            try:
                stat = entry.stat()
                entries.append({
                    "name": entry.name,
                    "path": str(entry),
                    "is_dir": entry.is_dir(),
                    "size": stat.st_size if entry.is_file() else None,
                })
            except OSError:
                pass  # skip unreadable entries
    except PermissionError:
        raise HTTPException(status_code=403, detail="Permission denied")

    return {"path": str(target), "entries": entries}


@router.get("/read")
async def read_file(path: str = Query(..., description="Absolute file path")):
    """Read a file's content (max 1 MB)."""
    target = _safe_path(path)
    if not target.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {path}")
    if not target.is_file():
        raise HTTPException(status_code=400, detail="Path is not a file")

    size = target.stat().st_size
    if size > MAX_FILE_SIZE:
        raise HTTPException(
            status_code=413,
            detail=f"File too large ({size // 1024} KB). Max is {MAX_FILE_SIZE // 1024} KB.",
        )

    try:
        content = target.read_text(encoding="utf-8", errors="replace")
    except PermissionError:
        raise HTTPException(status_code=403, detail="Permission denied")

    return {"path": str(target), "content": content, "size": size}


MAX_DOWNLOAD_SIZE = 100 * 1024 * 1024  # 100 MB


@router.get("/download")
async def download_file(path: str = Query(..., description="Absolute file path")):
    """Download a file (max 100 MB)."""
    target = _safe_path(path)
    if not target.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {path}")
    if not target.is_file():
        raise HTTPException(status_code=400, detail="Path is not a file")

    size = target.stat().st_size
    if size > MAX_DOWNLOAD_SIZE:
        raise HTTPException(
            status_code=413,
            detail=f"File too large ({size // 1024 // 1024} MB). Max is {MAX_DOWNLOAD_SIZE // 1024 // 1024} MB.",
        )

    return FileResponse(
        path=str(target),
        filename=target.name,
        media_type="application/octet-stream",
    )


# ---------------------------------------------------------------------------
# SSH endpoints
# ---------------------------------------------------------------------------

@router.post("/ssh/{profile_id}/list")
async def managed_ssh_list_directory(
    profile_id: int,
    req: ManagedSSHPathRequest,
    db: AsyncSession = Depends(get_db),
):
    """List a directory through a backend-managed, host-key-pinned profile."""

    profile = await _live_managed_ssh_profile(profile_id, db)
    try:
        canonical_path, entries, truncated = await run_bounded_sftp(
            _managed_ssh_list_sync,
            profile,
            req.path,
        )
    except Exception as exc:
        raise _managed_ssh_error(exc) from exc
    return {
        "path": canonical_path,
        "entries": entries,
        "truncated": truncated,
    }


@router.post("/ssh/{profile_id}/read")
async def managed_ssh_read_file(
    profile_id: int,
    req: ManagedSSHPathRequest,
    db: AsyncSession = Depends(get_db),
):
    """Preview a file without returning profile credentials to the browser."""

    profile = await _live_managed_ssh_profile(profile_id, db)
    try:
        canonical_path, content, size = await run_bounded_sftp(
            _managed_ssh_read_sync,
            profile,
            req.path,
        )
    except Exception as exc:
        raise _managed_ssh_error(exc) from exc
    return {"path": canonical_path, "content": content, "size": size}


@router.post("/ssh/{profile_id}/download")
async def managed_ssh_download_file(
    profile_id: int,
    req: ManagedSSHPathRequest,
    db: AsyncSession = Depends(get_db),
):
    """Download a bounded file through a managed SSH profile."""

    profile = await _live_managed_ssh_profile(profile_id, db)
    try:
        temporary_path, filename = await run_bounded_sftp(
            _managed_ssh_download_sync,
            profile,
            req.path,
            operation_timeout=settings.ssh_sftp_download_timeout_seconds,
            abandoned_result_cleanup=lambda result: _unlink_temporary_download(
                result[0]
            ),
        )
    except Exception as exc:
        raise _managed_ssh_error(exc) from exc
    return FileResponse(
        path=temporary_path,
        filename=filename,
        media_type="application/octet-stream",
        background=BackgroundTask(
            _unlink_temporary_download,
            temporary_path,
        ),
    )

@router.post("/ssh/list")
async def ssh_list_directory(req: SSHListRequest):
    """List contents of a directory on a remote SSH server."""
    client = _make_ssh_client(req)
    try:
        sftp = client.open_sftp()
        try:
            attrs = sftp.listdir_attr(req.path)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail=f"Path not found: {req.path}")
        except PermissionError:
            raise HTTPException(status_code=403, detail="Permission denied")

        import stat as stat_mod
        entries = []
        for a in sorted(attrs, key=lambda e: (not stat_mod.S_ISDIR(e.st_mode or 0), (e.filename or '').lower())):
            is_dir = stat_mod.S_ISDIR(a.st_mode or 0)
            entries.append({
                "name": a.filename,
                "path": req.path.rstrip("/") + "/" + a.filename,
                "is_dir": is_dir,
                "size": a.st_size if not is_dir else None,
            })
        sftp.close()
    finally:
        client.close()

    return {"path": req.path, "entries": entries}


@router.post("/ssh/read")
async def ssh_read_file(req: SSHReadRequest):
    """Read a file from a remote SSH server (max 1 MB)."""
    client = _make_ssh_client(req)
    try:
        sftp = client.open_sftp()
        try:
            file_attr = sftp.stat(req.path)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail=f"File not found: {req.path}")

        size = file_attr.st_size or 0
        if size > MAX_FILE_SIZE:
            raise HTTPException(
                status_code=413,
                detail=f"File too large ({size // 1024} KB). Max is {MAX_FILE_SIZE // 1024} KB.",
            )

        try:
            with sftp.open(req.path, "r") as f:
                raw = f.read()
            content = raw.decode("utf-8", errors="replace")
        except PermissionError:
            raise HTTPException(status_code=403, detail="Permission denied")
        finally:
            sftp.close()
    finally:
        client.close()

    return {"path": req.path, "content": content, "size": size}


@router.post("/ssh/download")
async def ssh_download_file(req: SSHReadRequest):
    """Download a file from a remote SSH server (max 100 MB)."""
    client = _make_ssh_client(req)
    try:
        sftp = client.open_sftp()
        try:
            file_attr = sftp.stat(req.path)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail=f"File not found: {req.path}")

        size = file_attr.st_size or 0
        if size > MAX_DOWNLOAD_SIZE:
            raise HTTPException(
                status_code=413,
                detail=f"File too large ({size // 1024 // 1024} MB). Max is {MAX_DOWNLOAD_SIZE // 1024 // 1024} MB.",
            )

        filename = req.path.rstrip("/").rsplit("/", 1)[-1] or "download"
        tmp = tempfile.NamedTemporaryFile(
            delete=False,
            prefix="ccm-ssh-download-",
            suffix=f"_{filename}",
        )
        try:
            sftp.getfo(req.path, tmp)
            tmp.close()
        except PermissionError:
            tmp.close()
            _unlink_temporary_download(tmp.name)
            raise HTTPException(status_code=403, detail="Permission denied")
        except BaseException:
            tmp.close()
            _unlink_temporary_download(tmp.name)
            raise
        finally:
            sftp.close()
    finally:
        client.close()

    return FileResponse(
        path=tmp.name,
        filename=filename,
        media_type="application/octet-stream",
        background=BackgroundTask(_unlink_temporary_download, tmp.name),
    )
