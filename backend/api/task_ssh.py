import errno
import posixpath
import socket
import stat as stat_mod

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from backend.api.deps import (
    get_current_user_id,
    require_admin,
    require_internal_service,
    require_task_access,
    require_task_control,
)
from backend.database import get_db
from backend.models.ssh_profile import SSHProfile
from backend.models.task import Task
from backend.schemas.task_ssh_grant import (
    TaskSSHExecuteRequest,
    TaskSSHExecuteResponse,
    TaskSSHDirectoryResponse,
    TaskSSHGrantReplace,
    TaskSSHGrantResponse,
    TaskSSHPathRequest,
    TaskSSHReadRequest,
    TaskSSHReadResponse,
    TaskSSHWriteRequest,
    TaskSSHWriteResponse,
)
from backend.services.ssh_executor import (
    SSHHostKeyMismatchError,
    SSHKeyPreflightError,
)
from backend.services.ssh_profiles import executor_for_profile
from backend.services.ssh_remote_paths import (
    resolve_existing_remote_path,
    resolve_remote_write_path,
)
from backend.services.ssh_sftp import (
    SSHSFTPBusyError,
    SSHSFTPOperationTimeout,
    configure_sftp_channel_timeout,
    run_bounded_sftp,
)
from backend.services.task_ssh_access import (
    TaskSSHAccessError,
    replace_task_ssh_grants,
    resolve_task_ssh_profile,
    task_ssh_grant_snapshots,
)


router = APIRouter(prefix="/api/tasks/{task_id}", tags=["task-ssh"])
MAX_TASK_SSH_DIRECTORY_ENTRIES = 2000
MAX_TASK_SSH_WRITE_BYTES = 1024 * 1024


def _access_error(exc: TaskSSHAccessError) -> HTTPException:
    return HTTPException(exc.status_code, exc.detail)


def _operation_error(exc: Exception) -> HTTPException:
    if isinstance(exc, HTTPException):
        return exc
    if isinstance(exc, SSHHostKeyMismatchError):
        return HTTPException(409, "SSH host key does not match the pinned identity")
    if isinstance(exc, SSHKeyPreflightError):
        return HTTPException(409, "SSH private key is no longer usable")
    if isinstance(exc, SSHSFTPBusyError):
        return HTTPException(503, "SSH file capacity is busy; try again shortly")
    if isinstance(exc, SSHSFTPOperationTimeout):
        return HTTPException(504, "SSH file operation timed out")
    if isinstance(exc, FileNotFoundError):
        return HTTPException(404, "Remote path not found")
    if isinstance(exc, PermissionError):
        return HTTPException(403, "Remote permission denied")
    if isinstance(exc, (TimeoutError, socket.timeout)):
        return HTTPException(504, "SSH operation timed out")
    if isinstance(exc, OSError) and exc.errno == errno.EEXIST:
        return HTTPException(409, "Remote file already exists")
    return HTTPException(400, "SSH operation failed")


def _list_directory_sync(
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
                if len(attrs) > MAX_TASK_SSH_DIRECTORY_ENTRIES:
                    break
            truncated = len(attrs) > MAX_TASK_SSH_DIRECTORY_ENTRIES
            attrs = sorted(
                attrs[:MAX_TASK_SSH_DIRECTORY_ENTRIES],
                key=lambda item: (
                    not stat_mod.S_ISDIR(item.st_mode or 0),
                    (item.filename or "").lower(),
                ),
            )
            entries = []
            for attr in attrs:
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


def _read_file_sync(
    profile: SSHProfile,
    path: str,
    max_bytes: int,
) -> tuple[str, str, int, bool]:
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
            with sftp.open(path, "rb") as remote_file:
                raw = remote_file.read(max_bytes + 1)
            truncated = len(raw) > max_bytes or size > max_bytes
            return (
                path,
                raw[:max_bytes].decode("utf-8", errors="replace"),
                size,
                truncated,
            )
        finally:
            sftp.close()
    finally:
        client.close()


def _write_file_sync(
    profile: SSHProfile,
    path: str,
    content: str,
    overwrite: bool,
) -> tuple[str, int]:
    payload = content.encode("utf-8")
    if len(payload) > MAX_TASK_SSH_WRITE_BYTES:
        raise HTTPException(413, "Remote write exceeds the 1 MB limit")
    client = executor_for_profile(profile).connect(timeout=10)
    try:
        sftp = client.open_sftp()
        try:
            configure_sftp_channel_timeout(sftp)
            path = resolve_remote_write_path(
                sftp,
                path,
                profile.allowed_roots or (),
            )
            mode = "wb" if overwrite else "wx"
            with sftp.open(path, mode) as remote_file:
                remote_file.write(payload)
            return path, len(payload)
        finally:
            sftp.close()
    finally:
        client.close()


async def _task_or_404(db: AsyncSession, task_id: int) -> Task:
    task = await db.get(Task, task_id)
    if task is None:
        raise HTTPException(404, "Task not found")
    return task


@router.get("/ssh-grants", response_model=list[TaskSSHGrantResponse])
async def list_task_ssh_grants(
    task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    task = await _task_or_404(db, task_id)
    await require_task_access(request, task, db)
    return await task_ssh_grant_snapshots(db, task)


@router.put("/ssh-grants", response_model=list[TaskSSHGrantResponse])
async def update_task_ssh_grants(
    task_id: int,
    body: TaskSSHGrantReplace,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    require_admin(request)
    task = await _task_or_404(db, task_id)
    await require_task_control(request, task, db)
    try:
        return await replace_task_ssh_grants(
            db,
            task,
            body.grants,
            created_by=get_current_user_id(request),
        )
    except TaskSSHAccessError as exc:
        raise _access_error(exc) from exc


@router.get("/ssh-access", response_model=list[TaskSSHGrantResponse])
async def internal_task_ssh_access(
    task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    require_internal_service(request)
    task = await _task_or_404(db, task_id)
    return await task_ssh_grant_snapshots(db, task)


@router.post(
    "/ssh-access/{profile_id}/execute",
    response_model=TaskSSHExecuteResponse,
)
async def internal_task_ssh_execute(
    task_id: int,
    profile_id: int,
    body: TaskSSHExecuteRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    require_internal_service(request)
    try:
        profile = await resolve_task_ssh_profile(
            db,
            task_id=task_id,
            profile_id=profile_id,
            required_capability="exec",
        )
        result = await executor_for_profile(profile).run_result(
            body.command,
            timeout=body.timeout_seconds,
            max_output_bytes=body.max_output_bytes,
            sensitive=True,
        )
    except TaskSSHAccessError as exc:
        raise _access_error(exc) from exc
    except Exception as exc:
        # Managed profile endpoints intentionally never reflect credential
        # paths, Paramiko messages, or command contents to Task callers.
        raise _operation_error(exc) from exc
    return TaskSSHExecuteResponse(
        exit_code=result.exit_code,
        stdout=result.stdout,
        stderr=result.stderr,
        truncated=result.truncated,
        duration_ms=result.duration_ms,
    )


@router.post(
    "/ssh-access/{profile_id}/list",
    response_model=TaskSSHDirectoryResponse,
)
async def internal_task_ssh_list_directory(
    task_id: int,
    profile_id: int,
    body: TaskSSHPathRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    require_internal_service(request)
    try:
        profile = await resolve_task_ssh_profile(
            db,
            task_id=task_id,
            profile_id=profile_id,
            required_capability="read",
        )
        canonical_path, entries, truncated = await run_bounded_sftp(
            _list_directory_sync,
            profile,
            body.path,
        )
    except TaskSSHAccessError as exc:
        raise _access_error(exc) from exc
    except Exception as exc:
        raise _operation_error(exc) from exc
    return {"path": canonical_path, "entries": entries, "truncated": truncated}


@router.post(
    "/ssh-access/{profile_id}/read",
    response_model=TaskSSHReadResponse,
)
async def internal_task_ssh_read_file(
    task_id: int,
    profile_id: int,
    body: TaskSSHReadRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    require_internal_service(request)
    try:
        profile = await resolve_task_ssh_profile(
            db,
            task_id=task_id,
            profile_id=profile_id,
            required_capability="read",
        )
        canonical_path, content, size, truncated = await run_bounded_sftp(
            _read_file_sync,
            profile,
            body.path,
            body.max_bytes,
        )
    except TaskSSHAccessError as exc:
        raise _access_error(exc) from exc
    except Exception as exc:
        raise _operation_error(exc) from exc
    return {
        "path": canonical_path,
        "content": content,
        "size": size,
        "truncated": truncated,
    }


@router.post(
    "/ssh-access/{profile_id}/write",
    response_model=TaskSSHWriteResponse,
)
async def internal_task_ssh_write_file(
    task_id: int,
    profile_id: int,
    body: TaskSSHWriteRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    require_internal_service(request)
    try:
        profile = await resolve_task_ssh_profile(
            db,
            task_id=task_id,
            profile_id=profile_id,
            required_capability="write",
        )
        canonical_path, bytes_written = await run_bounded_sftp(
            _write_file_sync,
            profile,
            body.path,
            body.content,
            body.overwrite,
        )
    except TaskSSHAccessError as exc:
        raise _access_error(exc) from exc
    except Exception as exc:
        raise _operation_error(exc) from exc
    return {"path": canonical_path, "bytes_written": bytes_written}
