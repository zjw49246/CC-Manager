import asyncio
import logging
from datetime import datetime

from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from backend.api.deps import get_current_user_id, require_admin
from backend.config import settings
from backend.database import get_db
from backend.models.ssh_profile import SSHProfile
from backend.schemas.ssh_profile import (
    SSHHostKeyProbeRequest,
    SSHHostKeyProbeResponse,
    SSHProfileCreate,
    SSHProfileResponse,
    SSHProfileTestResponse,
    SSHProfileUpdate,
    SSHPrivateKeyUploadResponse,
)
from backend.services.ssh_executor import SSHKeyPreflightError, probe_ssh_host_key
from backend.services.ssh_key_store import (
    MAX_SSH_PRIVATE_KEY_UPLOAD_BYTES,
    SSHManagedKeyStore,
    SSHManagedKeyStoreError,
)
from backend.services.ssh_profiles import (
    CONNECTION_IDENTITY_FIELDS,
    test_profile,
    validated_profile_material,
)


router = APIRouter(
    prefix="/api/ssh-profiles",
    tags=["ssh-profiles"],
    dependencies=[Depends(require_admin)],
)
logger = logging.getLogger(__name__)


def _validate_task_policy(enabled: bool, capabilities: list[str]) -> None:
    if enabled and not capabilities:
        raise HTTPException(
            422,
            "Select at least one Task capability when Task access is enabled",
        )
    if not enabled and capabilities:
        raise HTTPException(
            422,
            "Task capabilities require Task access to be enabled",
        )


def _profile_error(exc: Exception) -> HTTPException:
    if isinstance(exc, (SSHKeyPreflightError, SSHManagedKeyStoreError)):
        return HTTPException(400, {"code": exc.code, "message": exc.detail})
    return HTTPException(400, str(exc))


def _key_store() -> SSHManagedKeyStore:
    return SSHManagedKeyStore(settings.ssh_key_storage_dir)


async def _discard_unreferenced_managed_key(
    db: AsyncSession, store: SSHManagedKeyStore, key_path: str,
) -> None:
    try:
        referenced = await db.scalar(
            select(SSHProfile.id).where(
                SSHProfile.key_path == key_path,
                SSHProfile.deleted_at.is_(None),
            ).limit(1)
        )
        if referenced is None:
            store.discard_managed_key(key_path)
    except Exception:
        # Profile mutation has already committed. A retained mode-0600 key is
        # safer than turning a successful API operation into an ambiguous one.
        logger.exception("Failed to clean up an unreferenced managed SSH key")


async def _live_profile(db: AsyncSession, profile_id: int) -> SSHProfile:
    profile = await db.get(SSHProfile, profile_id)
    if profile is None or profile.deleted_at is not None:
        raise HTTPException(404, "SSH profile not found")
    return profile


@router.get("", response_model=list[SSHProfileResponse])
async def list_ssh_profiles(
    include_disabled: bool = True,
    task_eligible_only: bool = False,
    db: AsyncSession = Depends(get_db),
):
    stmt = select(SSHProfile).where(SSHProfile.deleted_at.is_(None))
    if not include_disabled:
        stmt = stmt.where(SSHProfile.enabled.is_(True))
    if task_eligible_only:
        stmt = stmt.where(
            SSHProfile.enabled.is_(True),
            SSHProfile.task_access_enabled.is_(True),
        )
    result = await db.execute(stmt.order_by(SSHProfile.name.asc()))
    return list(result.scalars().all())


@router.post("/probe-host-key", response_model=SSHHostKeyProbeResponse)
async def probe_host_key(body: SSHHostKeyProbeRequest):
    try:
        info = await asyncio.to_thread(
            probe_ssh_host_key,
            body.host,
            port=body.port,
            timeout=body.timeout_seconds,
        )
    except Exception as exc:
        raise _profile_error(exc) from exc
    return SSHHostKeyProbeResponse(
        key_type=info.key_type,
        host_key_value=info.openssh_public_key,
        fingerprint=info.sha256_fingerprint,
    )


@router.post("/upload-key", response_model=SSHPrivateKeyUploadResponse)
async def upload_private_key(file: UploadFile = File(...)):
    try:
        data = await file.read(MAX_SSH_PRIVATE_KEY_UPLOAD_BYTES + 1)
        if len(data) > MAX_SSH_PRIVATE_KEY_UPLOAD_BYTES:
            raise SSHManagedKeyStoreError(
                "key_size", "SSH private key must be no larger than 1 MB",
            )
        uploaded = _key_store().store_upload(data, file.filename)
    except Exception as exc:
        raise _profile_error(exc) from exc
    finally:
        await file.close()
    return SSHPrivateKeyUploadResponse(
        upload_token=uploaded.upload_token,
        filename=uploaded.filename,
        public_key_fingerprint=uploaded.public_key_fingerprint,
    )


@router.delete("/upload-key/{upload_token}")
async def cancel_private_key_upload(upload_token: str):
    try:
        removed = _key_store().cancel_upload(upload_token)
    except Exception as exc:
        raise _profile_error(exc) from exc
    if not removed:
        raise HTTPException(404, "SSH private-key upload not found")
    return {"ok": True}


@router.post("", response_model=SSHProfileResponse, status_code=201)
async def create_ssh_profile(
    body: SSHProfileCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    store = _key_store()
    claimed_key_path: str | None = None
    committed = False
    try:
        key_path = body.key_path
        if body.key_upload_token:
            claimed_key_path = store.claim_upload(body.key_upload_token)
            key_path = claimed_key_path
        material = validated_profile_material(
            key_path=key_path or "",
            host_key_value=body.host_key_value,
        )
    except Exception as exc:
        if claimed_key_path:
            store.discard_managed_key(claimed_key_path)
        raise _profile_error(exc) from exc
    profile = SSHProfile(
        name=body.name,
        host=body.host,
        port=body.port,
        username=body.username,
        enabled=body.enabled,
        task_access_enabled=body.task_access_enabled,
        task_capabilities=body.task_capabilities,
        allowed_roots=body.allowed_roots,
        created_by=get_current_user_id(request),
        **material,
    )
    db.add(profile)
    try:
        await db.commit()
        committed = True
    except IntegrityError as exc:
        await db.rollback()
        raise HTTPException(409, "SSH profile name already exists") from exc
    finally:
        if claimed_key_path and not committed:
            store.discard_managed_key(claimed_key_path)
    if body.key_upload_token:
        try:
            store.finalize_upload(body.key_upload_token)
        except Exception:
            logger.exception("Failed to finalize a committed managed SSH key upload")
    await db.refresh(profile)
    return profile


@router.get("/{profile_id}", response_model=SSHProfileResponse)
async def get_ssh_profile(
    profile_id: int, db: AsyncSession = Depends(get_db),
):
    return await _live_profile(db, profile_id)


@router.put("/{profile_id}", response_model=SSHProfileResponse)
async def update_ssh_profile(
    profile_id: int,
    body: SSHProfileUpdate,
    db: AsyncSession = Depends(get_db),
):
    profile = await _live_profile(db, profile_id)
    values = body.model_dump(exclude_unset=True)
    upload_token = values.pop("key_upload_token", None)
    if values.get("key_path") is None:
        values.pop("key_path", None)
    next_task_access_enabled = values.get(
        "task_access_enabled",
        profile.task_access_enabled,
    )
    next_task_capabilities = values.get(
        "task_capabilities",
        profile.task_capabilities,
    )
    _validate_task_policy(
        next_task_access_enabled,
        next_task_capabilities,
    )
    if (
        any(field in values and values[field] != getattr(profile, field) for field in ("host", "port"))
        and "host_key_value" not in values
    ):
        raise HTTPException(
            400,
            "Changing the SSH endpoint requires a newly confirmed host key",
        )
    store = _key_store()
    claimed_key_path: str | None = None
    committed = False
    old_key_path = profile.key_path
    if upload_token:
        try:
            claimed_key_path = store.claim_upload(upload_token)
        except Exception as exc:
            raise _profile_error(exc) from exc
        values["key_path"] = claimed_key_path
    # An explicitly re-entered key or host key is a re-authorization request,
    # even when its path/value string is unchanged. The file at a stable path
    # may have been rotated outside CCM and must receive a new revision.
    identity_changed = bool({"key_path", "host_key_value"} & values.keys()) or any(
        field in values and values[field] != getattr(profile, field)
        for field in CONNECTION_IDENTITY_FIELDS - {"key_path", "host_key_value"}
    )
    policy_changed = (
        next_task_access_enabled != profile.task_access_enabled
        or next_task_capabilities != profile.task_capabilities
        or values.get("allowed_roots", profile.allowed_roots)
        != profile.allowed_roots
    )
    if identity_changed:
        try:
            material = validated_profile_material(
                key_path=values.get("key_path", profile.key_path),
                host_key_value=values.get(
                    "host_key_value", profile.host_key_value,
                ),
            )
        except Exception as exc:
            if claimed_key_path:
                store.discard_managed_key(claimed_key_path)
            raise _profile_error(exc) from exc
        values.update(material)
        profile.revision += 1
        profile.last_tested_at = None
        profile.last_test_ok = None
        profile.last_error_code = None
        profile.last_error_detail = None
    elif policy_changed:
        # Grants snapshot the profile revision. Any policy change requires an
        # administrator to explicitly re-authorize existing Tasks, while the
        # runtime policy checks below remain an independent fail-closed gate.
        profile.revision += 1
    for field, value in values.items():
        setattr(profile, field, value)
    try:
        await db.commit()
        committed = True
    except IntegrityError as exc:
        await db.rollback()
        raise HTTPException(409, "SSH profile name already exists") from exc
    finally:
        if claimed_key_path and not committed:
            store.discard_managed_key(claimed_key_path)
    if upload_token:
        try:
            store.finalize_upload(upload_token)
        except Exception:
            logger.exception("Failed to finalize a committed managed SSH key upload")
    await db.refresh(profile)
    if old_key_path != profile.key_path:
        await _discard_unreferenced_managed_key(db, store, old_key_path)
    return profile


@router.post("/{profile_id}/test", response_model=SSHProfileTestResponse)
async def test_ssh_profile(
    profile_id: int, db: AsyncSession = Depends(get_db),
):
    profile = await _live_profile(db, profile_id)
    try:
        result = await test_profile(profile)
    except Exception as exc:
        result_error = _profile_error(exc)
        profile.last_tested_at = datetime.utcnow()
        profile.last_test_ok = False
        detail = result_error.detail
        profile.last_error_code = (
            detail.get("code") if isinstance(detail, dict) else "connection_failed"
        )
        profile.last_error_detail = (
            detail.get("message") if isinstance(detail, dict) else str(detail)
        )[:500]
        await db.commit()
        return SSHProfileTestResponse(
            ok=False,
            error_code=profile.last_error_code,
            detail=profile.last_error_detail,
        )
    await db.commit()
    return SSHProfileTestResponse(
        ok=result.ok,
        error_code=result.error_code,
        detail=result.detail,
    )


@router.delete("/{profile_id}")
async def delete_ssh_profile(
    profile_id: int, db: AsyncSession = Depends(get_db),
):
    profile = await _live_profile(db, profile_id)
    key_path = profile.key_path
    profile.enabled = False
    profile.revision += 1
    profile.deleted_at = datetime.utcnow()
    await db.commit()
    await _discard_unreferenced_managed_key(db, _key_store(), key_path)
    return {"ok": True}
