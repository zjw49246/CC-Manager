import asyncio
import errno
import hashlib
import json
import posixpath
import socket
import stat as stat_mod
from dataclasses import dataclass
from datetime import datetime
from functools import wraps

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from backend.api.deps import (
    get_current_user_id,
    require_admin,
    require_internal_service,
    require_internal_task_incarnation,
    require_managed_ssh_auth_configured,
    require_task_control,
)
from backend.database import get_db
from backend.models.ssh_profile import SSHProfile
from backend.models.project import Project
from backend.models.task import Task
from backend.models.task_share import ProjectShare, TaskShare
from backend.models.task_ssh_effect import (
    SQLITE_TASK_SSH_EFFECT_TRIGGER_NAMES,
    TaskSSHEffectReceipt,
)
from backend.models.task_ssh_grant import TaskSSHGrant
from backend.schemas.task_ssh_grant import (
    TASK_SSH_MAX_EXEC_OUTPUT_BYTES,
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
from backend.services.cancellation import settle_awaitable
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
from backend.services.worker_node_control import fence_worker_node_mutation
from backend.services.worker_proxy import get_task_operation_lock


router = APIRouter(
    prefix="/api/tasks/{task_id}",
    tags=["task-ssh"],
    dependencies=[Depends(require_managed_ssh_auth_configured)],
)
MAX_TASK_SSH_DIRECTORY_ENTRIES = 2000
MAX_TASK_SSH_WRITE_BYTES = 1024 * 1024
# The executor captures at most 64 KiB across stdout+stderr. JSON escaping can
# expand control/replacement characters by up to 6x, so reserve a hard 512 KiB
# envelope. Every successful response fits this envelope and therefore remains
# replayable after an ACK loss. Receipts remain permanently auditable; SSH
# authorization and the one-effect-at-a-time fence are the admission controls.
MAX_TASK_SSH_REPLAY_PAYLOAD_BYTES = 512 * 1024
_UNKNOWN_EFFECT_STATUSES = ("running", "ambiguous")


@dataclass(frozen=True)
class _TaskEffectGeneration:
    task_id: int
    incarnation_id: str
    retry_count: int
    turn_generation: int
    status: str


@dataclass(frozen=True)
class _EffectAdmission:
    receipt_id: int
    generation: _TaskEffectGeneration
    profile_revision: int


async def _settle_despite_cancellation(awaitable):
    """Finish a finite receipt write before re-delivering cancellation."""

    operation, delayed = await settle_awaitable(awaitable)
    return operation.result(), delayed


async def _rollback_settled(db: AsyncSession) -> None:
    try:
        await _settle_despite_cancellation(db.rollback())
    except BaseException:
        # A still-running receipt is the conservative crash result. Never
        # obscure the original request failure with a rollback detail.
        pass


def _canonical_json_bytes(payload: dict) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _canonical_digest(payload: dict) -> str:
    return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def _truncate_utf8(value: str, budget: int) -> tuple[str, int, bool]:
    encoded = value.encode("utf-8")
    if len(encoded) <= budget:
        return value, budget - len(encoded), False
    # Decode a complete prefix only. SSH output already uses replacement for
    # invalid remote bytes; this second bound handles contract-violating test
    # doubles and protects durable replay independently of the executor.
    bounded = encoded[:budget].decode("utf-8", errors="ignore")
    return bounded, 0, True


def _bounded_execute_payload(result) -> dict:
    stdout, remaining, stdout_cut = _truncate_utf8(
        result.stdout,
        TASK_SSH_MAX_EXEC_OUTPUT_BYTES,
    )
    stderr, _remaining, stderr_cut = _truncate_utf8(
        result.stderr,
        remaining,
    )
    payload = {
        "exit_code": result.exit_code,
        "stdout": stdout,
        "stderr": stderr,
        "truncated": bool(result.truncated or stdout_cut or stderr_cut),
        "duration_ms": result.duration_ms,
    }
    if len(_canonical_json_bytes(payload)) > MAX_TASK_SSH_REPLAY_PAYLOAD_BYTES:
        # With the public output bound this is unreachable for JSON strings,
        # but fail closed if the serializer contract ever changes.
        raise RuntimeError("SSH result exceeds the durable replay envelope")
    return payload


def _with_task_operation_lock(handler):
    """Serialize local SSH mutations with Task lifecycle transitions."""

    @wraps(handler)
    async def wrapped(task_id: int, *args, **kwargs):
        async with get_task_operation_lock(task_id):
            return await handler(task_id, *args, **kwargs)

    return wrapped


def _is_sqlite_session(db: AsyncSession) -> bool:
    return db.get_bind().dialect.name == "sqlite"


async def _verify_sqlite_effect_permit(db: AsyncSession, effect_id: str) -> None:
    if not _is_sqlite_session(db):
        return
    rows = await db.execute(text(
        "SELECT name FROM sqlite_master WHERE type = 'trigger'"
    ))
    installed = {row[0] for row in rows if isinstance(row[0], str)}
    missing = set(SQLITE_TASK_SSH_EFFECT_TRIGGER_NAMES) - installed
    if missing:
        raise _effect_http_error(
            503,
            "SQLite Task SSH effect permit is unavailable; migration required",
            effect_id,
        )


def _effect_request_digest(
    operation: str,
    profile_id: int,
    body: TaskSSHExecuteRequest | TaskSSHWriteRequest,
) -> str:
    request_identity = (
        {"command": body.command}
        if isinstance(body, TaskSSHExecuteRequest)
        else body.model_dump(exclude={"effect_id"})
    )
    return _canonical_digest({
        "operation": operation,
        "profile_id": profile_id,
        "request": request_identity,
    })


def _effect_detail(
    message: str,
    effect_id: str,
    *,
    status: str | None = None,
    existing_effect_id: str | None = None,
) -> dict[str, str]:
    detail = {"message": message, "effect_id": effect_id}
    if status is not None:
        detail["effect_status"] = status
    if existing_effect_id is not None:
        detail["existing_effect_id"] = existing_effect_id
    return detail


def _effect_http_error(
    status_code: int,
    message: str,
    effect_id: str,
    *,
    status: str | None = None,
    existing_effect_id: str | None = None,
) -> HTTPException:
    return HTTPException(
        status_code,
        _effect_detail(
            message,
            effect_id,
            status=status,
            existing_effect_id=existing_effect_id,
        ),
    )


def _effect_operation_error(
    exc: Exception,
    effect_id: str,
    *,
    status: str,
) -> HTTPException:
    mapped = _operation_error(exc)
    message = (
        mapped.detail
        if isinstance(mapped.detail, str)
        else "SSH operation failed"
    )
    return _effect_http_error(
        mapped.status_code,
        message,
        effect_id,
        status=status,
    )


def _effect_from_http_error(
    exc: HTTPException,
    effect_id: str,
    *,
    status: str,
) -> HTTPException:
    if (
        isinstance(exc.detail, dict)
        and exc.detail.get("effect_id") == effect_id
    ):
        return exc
    message = exc.detail if isinstance(exc.detail, str) else "SSH operation failed"
    return _effect_http_error(
        exc.status_code,
        message,
        effect_id,
        status=status,
    )


def _task_effect_generation(task: Task, effect_id: str) -> _TaskEffectGeneration:
    if (
        not isinstance(task.incarnation_id, str)
        or len(task.incarnation_id) != 32
        or task.retry_count is None
        or task.turn_generation is None
        or not isinstance(task.status, str)
    ):
        raise _effect_http_error(
            409,
            "Task execution identity is unavailable for this SSH effect",
            effect_id,
        )
    return _TaskEffectGeneration(
        task_id=task.id,
        incarnation_id=task.incarnation_id,
        retry_count=task.retry_count,
        turn_generation=task.turn_generation,
        status=task.status,
    )


async def _fence_effect_caller_task(
    db: AsyncSession,
    request: Request,
    task_id: int,
    effect_id: str,
) -> Task:
    try:
        task = await require_internal_task_incarnation(
            request,
            task_id,
            db,
            write_fence=True,
        )
    except IntegrityError as exc:
        await _rollback_settled(db)
        if _is_sqlite_session(db):
            running = await db.scalar(
                select(TaskSSHEffectReceipt)
                .where(
                    TaskSSHEffectReceipt.task_id == task_id,
                    TaskSSHEffectReceipt.status == "running",
                )
                .order_by(TaskSSHEffectReceipt.id.asc())
                .limit(1)
            )
            if running is not None:
                raise _effect_http_error(
                    409,
                    "Task has another SSH effect in progress",
                    effect_id,
                    status=running.status,
                    existing_effect_id=running.effect_id,
                ) from exc
        raise _effect_http_error(
            503,
            "Task SSH execution fence could not be acquired",
            effect_id,
        ) from exc
    if task is not None:
        return task

    # The deployment credential remains a recovery/admin credential. Freeze
    # its observed generation with the same writer fence used by scoped Task
    # tokens so even this compatibility path cannot cross a retry or turn.
    observed = await db.get(Task, task_id)
    if observed is None:
        raise _effect_http_error(404, "Task not found", effect_id)
    generation = _task_effect_generation(observed, effect_id)
    try:
        fenced = await db.execute(
            update(Task)
            .where(
                Task.id == generation.task_id,
                Task.incarnation_id == generation.incarnation_id,
                Task.retry_count == generation.retry_count,
                Task.turn_generation == generation.turn_generation,
                Task.status == generation.status,
            )
            .values(status=Task.status)
        )
    except IntegrityError as exc:
        await _rollback_settled(db)
        if _is_sqlite_session(db):
            running = await db.scalar(
                select(TaskSSHEffectReceipt)
                .where(
                    TaskSSHEffectReceipt.task_id == task_id,
                    TaskSSHEffectReceipt.status == "running",
                )
                .order_by(TaskSSHEffectReceipt.id.asc())
                .limit(1)
            )
            if running is not None:
                raise _effect_http_error(
                    409,
                    "Task has another SSH effect in progress",
                    effect_id,
                    status=running.status,
                    existing_effect_id=running.effect_id,
                ) from exc
        raise _effect_http_error(
            503,
            "Task SSH execution fence could not be acquired",
            effect_id,
        ) from exc
    if fenced.rowcount != 1:
        raise _effect_http_error(
            409,
            "Task execution generation changed before SSH admission",
            effect_id,
        )
    refreshed = await db.get(Task, task_id, populate_existing=True)
    if refreshed is None:
        raise _effect_http_error(404, "Task not found", effect_id)
    return refreshed


async def _fence_exact_effect_generation(
    db: AsyncSession,
    generation: _TaskEffectGeneration,
) -> Task | None:
    fenced = await db.execute(
        update(Task)
        .where(
            Task.id == generation.task_id,
            Task.incarnation_id == generation.incarnation_id,
            Task.retry_count == generation.retry_count,
            Task.turn_generation == generation.turn_generation,
            Task.status == generation.status,
        )
        .values(status=Task.status)
    )
    if fenced.rowcount != 1:
        return None
    return await db.get(Task, generation.task_id, populate_existing=True)


async def _persist_receipt_outcome(
    db: AsyncSession,
    receipt_id: int,
    *,
    status: str,
    outcome_code: str,
    result_payload: dict | None = None,
) -> None:
    now = datetime.utcnow()
    if (
        result_payload is not None
        and len(_canonical_json_bytes(result_payload))
        > MAX_TASK_SSH_REPLAY_PAYLOAD_BYTES
    ):
        # A completed receipt must always be replayable. Never silently trade
        # an ACK-loss guarantee for storage compaction.
        raise RuntimeError("SSH effect result exceeds durable replay limit")
    result_digest = (
        _canonical_digest(result_payload)
        if result_payload is not None
        else None
    )
    changed = await db.execute(
        update(TaskSSHEffectReceipt)
        .where(
            TaskSSHEffectReceipt.id == receipt_id,
            TaskSSHEffectReceipt.status == "running",
        )
        .values(
            status=status,
            outcome_code=outcome_code,
            result_payload=result_payload,
            result_digest=result_digest,
            result_compacted=False,
            completed_at=now,
            updated_at=now,
        )
    )
    if changed.rowcount != 1:
        raise RuntimeError("SSH effect receipt state changed")
    await db.commit()


async def _settled_receipt_outcome(
    db: AsyncSession,
    receipt_id: int,
    *,
    status: str,
    outcome_code: str,
    result_payload: dict | None = None,
) -> asyncio.CancelledError | None:
    _, cancellation = await _settle_despite_cancellation(
        _persist_receipt_outcome(
            db,
            receipt_id,
            status=status,
            outcome_code=outcome_code,
            result_payload=result_payload,
        )
    )
    return cancellation


def _completed_receipt_payload(
    receipt: TaskSSHEffectReceipt,
    effect_id: str,
) -> dict:
    if receipt.result_compacted:
        raise _effect_http_error(
            409,
            "SSH effect completed, but its result was compacted and cannot "
            "be replayed; do not execute it again",
            effect_id,
            status=receipt.status,
        )
    payload = receipt.result_payload
    if (
        not isinstance(payload, dict)
        or receipt.result_digest != _canonical_digest(payload)
    ):
        raise _effect_http_error(
            409,
            "SSH effect receipt result is unavailable",
            effect_id,
            status=receipt.status,
        )
    return {"effect_id": effect_id, "replayed": True, **payload}


async def _admit_task_ssh_effect(
    db: AsyncSession,
    request: Request,
    *,
    task_id: int,
    profile_id: int,
    operation: str,
    effect_id: str,
    request_digest: str,
    required_capability: str,
) -> _EffectAdmission | dict:
    if _is_sqlite_session(db):
        await _verify_sqlite_effect_permit(db, effect_id)
        # End the sqlite_master snapshot before attempting a writer fence.
        await db.rollback()
    # A remote SSH effect is durable Worker-owned execution.  Take the node
    # drain fence before the Task generation writer and hold it through the
    # running-receipt commit.  An effect admitted first is therefore visible to
    # the drain proof; a drain claim admitted first refuses the effect without
    # touching the remote host.
    await fence_worker_node_mutation(db)
    # The short Task writer fence serializes the validation + receipt INSERT.
    # It is released by the admission commit before any network call. SQLite's
    # permit trigger deliberately allows this no-op fence (and benign runtime
    # fields such as heartbeat/unread updates), while rejecting changes to the
    # exact execution/authorization columns until the receipt settles.
    task = await _fence_effect_caller_task(
        db,
        request,
        task_id,
        effect_id,
    )
    generation = _task_effect_generation(task, effect_id)
    existing = await db.scalar(
        select(TaskSSHEffectReceipt).where(
            TaskSSHEffectReceipt.task_id == task_id,
            TaskSSHEffectReceipt.effect_id == effect_id,
        )
    )
    if existing is not None:
        if (
            existing.task_incarnation_id != generation.incarnation_id
            or existing.operation != operation
            or existing.profile_id != profile_id
            or existing.request_digest != request_digest
        ):
            raise _effect_http_error(
                409,
                "SSH effect id is already bound to a different request",
                effect_id,
                status=existing.status,
            )
        if existing.status == "completed":
            if (
                existing.task_retry_count != generation.retry_count
                or existing.task_turn_generation
                != generation.turn_generation
                or existing.task_status != generation.status
            ):
                raise _effect_http_error(
                    409,
                    "SSH effect belongs to a different Task execution generation",
                    effect_id,
                    status=existing.status,
                )
            try:
                current_profile = await resolve_task_ssh_profile(
                    db,
                    task_id=task_id,
                    profile_id=profile_id,
                    required_capability=required_capability,
                )
            except TaskSSHAccessError as exc:
                raise _effect_http_error(
                    exc.status_code,
                    exc.detail,
                    effect_id,
                    status=existing.status,
                ) from exc
            if current_profile.revision != existing.profile_revision:
                raise _effect_http_error(
                    409,
                    "SSH effect belongs to a different SSH profile revision",
                    effect_id,
                    status=existing.status,
                )
            return _completed_receipt_payload(existing, effect_id)
        if existing.status in _UNKNOWN_EFFECT_STATUSES:
            raise _effect_http_error(
                409,
                "SSH effect outcome is unknown; reuse this effect id only "
                "for explicit reconciliation and do not retry blindly",
                effect_id,
                status=existing.status,
            )
        raise _effect_http_error(
            409,
            "SSH effect was safely aborted before remote execution; "
            "revalidate the request before creating a new effect id",
            effect_id,
            status=existing.status,
        )

    unknown_same_request = await db.scalar(
        select(TaskSSHEffectReceipt)
        .where(
            TaskSSHEffectReceipt.task_id == task_id,
            TaskSSHEffectReceipt.task_incarnation_id
            == generation.incarnation_id,
            TaskSSHEffectReceipt.request_digest == request_digest,
            TaskSSHEffectReceipt.status.in_(_UNKNOWN_EFFECT_STATUSES),
        )
        .order_by(TaskSSHEffectReceipt.id.asc())
        .limit(1)
    )
    if unknown_same_request is not None:
        raise _effect_http_error(
            409,
            "An identical SSH effect already has an unknown outcome; do not "
            "bypass it with a new effect id",
            effect_id,
            status=unknown_same_request.status,
            existing_effect_id=unknown_same_request.effect_id,
        )

    active_receipt = await db.scalar(
        select(TaskSSHEffectReceipt)
        .where(
            TaskSSHEffectReceipt.task_id == task_id,
            TaskSSHEffectReceipt.status == "running",
        )
        .order_by(TaskSSHEffectReceipt.id.asc())
        .limit(1)
    )
    if active_receipt is not None:
        raise _effect_http_error(
            409,
            "Task has another SSH effect in progress",
            effect_id,
            status=active_receipt.status,
            existing_effect_id=active_receipt.effect_id,
        )

    profile = await resolve_task_ssh_profile(
        db,
        task_id=task_id,
        profile_id=profile_id,
        required_capability=required_capability,
    )
    receipt = TaskSSHEffectReceipt(
        effect_id=effect_id,
        task_id=task_id,
        task_incarnation_id=generation.incarnation_id,
        task_retry_count=generation.retry_count,
        task_turn_generation=generation.turn_generation,
        task_status=generation.status,
        profile_id=profile_id,
        profile_revision=profile.revision,
        operation=operation,
        request_digest=request_digest,
        status="running",
    )
    db.add(receipt)
    try:
        _, cancellation = await _settle_despite_cancellation(db.commit())
    except BaseException:
        await _rollback_settled(db)
        raise _effect_http_error(
            503,
            "SSH effect could not be durably admitted",
            effect_id,
        )
    if cancellation is not None:
        try:
            await _settled_receipt_outcome(
                db,
                receipt.id,
                status="aborted",
                outcome_code="cancelled_before_execution",
            )
        finally:
            raise cancellation
    return _EffectAdmission(
        receipt_id=receipt.id,
        generation=generation,
        profile_revision=profile.revision,
    )


async def _prepare_admitted_effect(
    db: AsyncSession,
    admission: _EffectAdmission,
    *,
    profile_id: int,
    required_capability: str,
    effect_id: str,
) -> SSHProfile:
    if _is_sqlite_session(db):
        await _verify_sqlite_effect_permit(db, effect_id)
        # Do not upgrade the sqlite_master read snapshot after a concurrent
        # drain commit.  Start a fresh transaction whose first write is the
        # node-control fence.
        await db.rollback()
    await fence_worker_node_mutation(db)
    if _is_sqlite_session(db):
        task = await db.scalar(
            select(Task).where(
                Task.id == admission.generation.task_id,
                Task.incarnation_id == admission.generation.incarnation_id,
                Task.retry_count == admission.generation.retry_count,
                Task.turn_generation
                == admission.generation.turn_generation,
                Task.status == admission.generation.status,
            )
        )
    else:
        # Project sharing takes Project -> Task locks everywhere else. Observe
        # the exact Task first, then preserve that order so insertion of a new
        # Project share cannot cross the effect's authorization snapshot.
        observed = await db.scalar(
            select(Task).where(
                Task.id == admission.generation.task_id,
                Task.incarnation_id == admission.generation.incarnation_id,
                Task.retry_count == admission.generation.retry_count,
                Task.turn_generation
                == admission.generation.turn_generation,
                Task.status == admission.generation.status,
            )
        )
        if observed is None:
            task = None
        else:
            observed_project_id = observed.project_id
            if observed_project_id is not None:
                project_fenced = await db.execute(
                    update(Project)
                    .where(Project.id == observed_project_id)
                    .values(id=Project.id)
                )
                if project_fenced.rowcount != 1:
                    raise TaskSSHAccessError(
                        409,
                        "Task Project changed before remote SSH execution",
                    )
            task = await _fence_exact_effect_generation(
                db,
                admission.generation,
            )
            if task is not None and task.project_id != observed_project_id:
                raise TaskSSHAccessError(
                    409,
                    "Task Project changed before remote SSH execution",
                )
    if task is None:
        raise _effect_http_error(
            409,
            "Task execution generation changed before remote SSH execution",
            effect_id,
            status="aborted",
        )
    receipt = await db.get(
        TaskSSHEffectReceipt,
        admission.receipt_id,
        populate_existing=True,
    )
    if receipt is None or receipt.status != "running":
        raise _effect_http_error(
            409,
            "SSH effect receipt is no longer executable",
            effect_id,
            status=receipt.status if receipt is not None else "missing",
        )
    if not _is_sqlite_session(db):
        # Row-locking databases can hold these exact rows without blocking
        # unrelated work. SQLite instead relies on the committed running
        # receipt triggers and must never retain a global writer transaction
        # across the network call.
        profile_fenced = await db.execute(
            update(SSHProfile)
            .where(
                SSHProfile.id == profile_id,
                SSHProfile.revision == admission.profile_revision,
            )
            .values(revision=SSHProfile.revision)
        )
        if profile_fenced.rowcount != 1:
            raise TaskSSHAccessError(
                409,
                "SSH profile revision changed before remote execution",
            )
        # Hold every existing authorization row through the network call.
        # Insertions are serialized by the Project/Task locks above in the
        # application mutation paths; locking existing rows additionally
        # covers revocation and group deletion on row-locking databases.
        grant_id = await db.scalar(
            select(TaskSSHGrant.id)
            .where(
                TaskSSHGrant.task_id == admission.generation.task_id,
                TaskSSHGrant.ssh_profile_id == profile_id,
            )
            .with_for_update()
        )
        if grant_id is None:
            raise TaskSSHAccessError(
                409,
                "SSH authorization changed before remote execution",
            )
        await db.execute(
            select(TaskShare.id)
            .where(TaskShare.task_id == admission.generation.task_id)
            .with_for_update()
        )
        if task.project_id is not None:
            await db.execute(
                select(ProjectShare.id)
                .where(ProjectShare.project_id == task.project_id)
                .with_for_update()
            )
    profile = await resolve_task_ssh_profile(
        db,
        task_id=admission.generation.task_id,
        profile_id=profile_id,
        required_capability=required_capability,
    )
    if profile.revision != admission.profile_revision:
        raise TaskSSHAccessError(
            409,
            "SSH profile revision changed before remote execution",
        )
    if _is_sqlite_session(db):
        # Release the read snapshot before remote I/O. The durable trigger
        # permit remains active until this receipt becomes completed,
        # ambiguous, or safely aborted.
        await db.commit()
    return profile


async def _abort_admitted_effect(
    db: AsyncSession,
    admission: _EffectAdmission,
    *,
    outcome_code: str,
) -> asyncio.CancelledError | None:
    await _rollback_settled(db)
    return await _settled_receipt_outcome(
        db,
        admission.receipt_id,
        status="aborted",
        outcome_code=outcome_code,
    )


async def _mark_admitted_effect_ambiguous(
    db: AsyncSession,
    admission: _EffectAdmission,
) -> asyncio.CancelledError | None:
    try:
        return await _settled_receipt_outcome(
            db,
            admission.receipt_id,
            status="ambiguous",
            outcome_code="remote_outcome_unknown",
        )
    except BaseException:
        # ``running`` is itself durable unknown-outcome evidence. If the
        # stronger ambiguous transition cannot commit, preserve it and never
        # turn the failure into an invitation to replay.
        await _rollback_settled(db)
        return None


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
    # A chat share permits conversation only.  Grant snapshots expose the SSH
    # host, username, host-key fingerprint, capability set and allowed remote
    # roots, so they are Task control-plane configuration rather than chat
    # history.
    await require_task_control(request, task, db)
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
    task = await require_internal_task_incarnation(
        request,
        task_id,
        db,
    )
    if task is None:
        task = await _task_or_404(db, task_id)
    return await task_ssh_grant_snapshots(db, task)


@router.post(
    "/ssh-access/{profile_id}/execute",
    response_model=TaskSSHExecuteResponse,
)
@_with_task_operation_lock
async def internal_task_ssh_execute(
    task_id: int,
    profile_id: int,
    body: TaskSSHExecuteRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    require_internal_service(request)
    effect_id = body.effect_id
    request_digest = _effect_request_digest(
        "execute",
        profile_id,
        body,
    )
    try:
        admitted = await _admit_task_ssh_effect(
            db,
            request,
            task_id=task_id,
            profile_id=profile_id,
            operation="execute",
            effect_id=effect_id,
            request_digest=request_digest,
            required_capability="exec",
        )
    except TaskSSHAccessError as exc:
        mapped = _access_error(exc)
        raise _effect_from_http_error(
            mapped,
            effect_id,
            status="aborted",
        ) from exc
    if isinstance(admitted, dict):
        return admitted

    try:
        profile = await _prepare_admitted_effect(
            db,
            admitted,
            profile_id=profile_id,
            required_capability="exec",
            effect_id=effect_id,
        )
    except asyncio.CancelledError:
        await _abort_admitted_effect(
            db,
            admitted,
            outcome_code="cancelled_before_execution",
        )
        raise
    except TaskSSHAccessError as exc:
        cancellation = await _abort_admitted_effect(
            db,
            admitted,
            outcome_code="authorization_changed",
        )
        if cancellation is not None:
            raise cancellation
        mapped = _access_error(exc)
        raise _effect_from_http_error(
            mapped,
            effect_id,
            status="aborted",
        ) from exc
    except HTTPException as exc:
        cancellation = await _abort_admitted_effect(
            db,
            admitted,
            outcome_code="generation_changed",
        )
        if cancellation is not None:
            raise cancellation
        raise _effect_from_http_error(
            exc,
            effect_id,
            status="aborted",
        ) from exc
    except Exception as exc:
        cancellation = await _abort_admitted_effect(
            db,
            admitted,
            outcome_code="preflight_failed",
        )
        if cancellation is not None:
            raise cancellation
        raise _effect_http_error(
            503,
            "SSH effect preflight failed safely before remote execution",
            effect_id,
            status="aborted",
        ) from exc

    try:
        result = await executor_for_profile(profile).run_result(
            body.command,
            timeout=body.timeout_seconds,
            max_output_bytes=body.max_output_bytes,
            sensitive=True,
        )
    except asyncio.CancelledError:
        await _mark_admitted_effect_ambiguous(db, admitted)
        raise
    except Exception as exc:
        cancellation = await _mark_admitted_effect_ambiguous(db, admitted)
        if cancellation is not None:
            raise cancellation
        # Managed profile endpoints intentionally never reflect credential
        # paths, Paramiko messages, or command contents to Task callers.
        raise _effect_operation_error(
            exc,
            effect_id,
            status="ambiguous",
        ) from exc

    try:
        payload = _bounded_execute_payload(result)
    except Exception as exc:
        await _mark_admitted_effect_ambiguous(db, admitted)
        raise _effect_http_error(
            503,
            "SSH effect completed remotely but its replay result is unavailable",
            effect_id,
            status="ambiguous",
        ) from exc
    try:
        cancellation = await _settled_receipt_outcome(
            db,
            admitted.receipt_id,
            status="completed",
            outcome_code="success",
            result_payload=payload,
        )
    except BaseException as exc:
        await _rollback_settled(db)
        await _mark_admitted_effect_ambiguous(db, admitted)
        raise _effect_http_error(
            503,
            "SSH effect completed remotely but its acknowledgement is unknown",
            effect_id,
            status="running",
        ) from exc
    if cancellation is not None:
        raise cancellation
    return TaskSSHExecuteResponse(
        effect_id=effect_id,
        replayed=False,
        **payload,
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
    await require_internal_task_incarnation(
        request,
        task_id,
        db,
        write_fence=True,
    )
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
    await require_internal_task_incarnation(
        request,
        task_id,
        db,
        write_fence=True,
    )
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
@_with_task_operation_lock
async def internal_task_ssh_write_file(
    task_id: int,
    profile_id: int,
    body: TaskSSHWriteRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    require_internal_service(request)
    effect_id = body.effect_id
    if len(body.content.encode("utf-8")) > MAX_TASK_SSH_WRITE_BYTES:
        raise _effect_http_error(
            413,
            "Remote write exceeds the 1 MB limit",
            effect_id,
        )
    request_digest = _effect_request_digest(
        "write",
        profile_id,
        body,
    )
    try:
        admitted = await _admit_task_ssh_effect(
            db,
            request,
            task_id=task_id,
            profile_id=profile_id,
            operation="write",
            effect_id=effect_id,
            request_digest=request_digest,
            required_capability="write",
        )
    except TaskSSHAccessError as exc:
        mapped = _access_error(exc)
        raise _effect_from_http_error(
            mapped,
            effect_id,
            status="aborted",
        ) from exc
    if isinstance(admitted, dict):
        return admitted

    try:
        profile = await _prepare_admitted_effect(
            db,
            admitted,
            profile_id=profile_id,
            required_capability="write",
            effect_id=effect_id,
        )
    except asyncio.CancelledError:
        await _abort_admitted_effect(
            db,
            admitted,
            outcome_code="cancelled_before_execution",
        )
        raise
    except TaskSSHAccessError as exc:
        cancellation = await _abort_admitted_effect(
            db,
            admitted,
            outcome_code="authorization_changed",
        )
        if cancellation is not None:
            raise cancellation
        mapped = _access_error(exc)
        raise _effect_from_http_error(
            mapped,
            effect_id,
            status="aborted",
        ) from exc
    except HTTPException as exc:
        cancellation = await _abort_admitted_effect(
            db,
            admitted,
            outcome_code="generation_changed",
        )
        if cancellation is not None:
            raise cancellation
        raise _effect_from_http_error(
            exc,
            effect_id,
            status="aborted",
        ) from exc
    except Exception as exc:
        cancellation = await _abort_admitted_effect(
            db,
            admitted,
            outcome_code="preflight_failed",
        )
        if cancellation is not None:
            raise cancellation
        raise _effect_http_error(
            503,
            "SSH effect preflight failed safely before remote execution",
            effect_id,
            status="aborted",
        ) from exc

    try:
        canonical_path, bytes_written = await run_bounded_sftp(
            _write_file_sync,
            profile,
            body.path,
            body.content,
            body.overwrite,
        )
    except asyncio.CancelledError:
        await _mark_admitted_effect_ambiguous(db, admitted)
        raise
    except Exception as exc:
        cancellation = await _mark_admitted_effect_ambiguous(db, admitted)
        if cancellation is not None:
            raise cancellation
        raise _effect_operation_error(
            exc,
            effect_id,
            status="ambiguous",
        ) from exc

    payload = {
        "path": canonical_path,
        "bytes_written": bytes_written,
    }
    try:
        cancellation = await _settled_receipt_outcome(
            db,
            admitted.receipt_id,
            status="completed",
            outcome_code="success",
            result_payload=payload,
        )
    except BaseException as exc:
        await _rollback_settled(db)
        await _mark_admitted_effect_ambiguous(db, admitted)
        raise _effect_http_error(
            503,
            "SSH effect completed remotely but its acknowledgement is unknown",
            effect_id,
            status="running",
        ) from exc
    if cancellation is not None:
        raise cancellation
    return TaskSSHWriteResponse(
        effect_id=effect_id,
        replayed=False,
        **payload,
    )
