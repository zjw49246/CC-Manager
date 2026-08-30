from __future__ import annotations

from datetime import datetime

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.ssh_profile import SSHProfile
from backend.services.ssh_executor import (
    SSHExecutor,
    openssh_public_key_fingerprint,
    preflight_private_key,
    validate_openssh_public_key,
)


CONNECTION_IDENTITY_FIELDS = {
    "host",
    "port",
    "username",
    "key_path",
    "host_key_value",
}


async def update_profile_revision_cas(
    db: AsyncSession,
    *,
    profile_id: int,
    expected_revision: int,
    values: dict,
    increment_revision: bool,
) -> bool:
    """Stage one exact-revision Profile mutation in the current transaction."""

    persisted = dict(values)
    persisted["updated_at"] = datetime.utcnow()
    if increment_revision:
        persisted["revision"] = SSHProfile.revision + 1
    result = await db.execute(
        update(SSHProfile)
        .where(
            SSHProfile.id == profile_id,
            SSHProfile.revision == expected_revision,
            SSHProfile.deleted_at.is_(None),
        )
        .values(**persisted)
    )
    return result.rowcount == 1


def validated_profile_material(
    *, key_path: str, host_key_value: str,
) -> dict[str, str]:
    key = preflight_private_key(key_path)
    host_key = validate_openssh_public_key(host_key_value)
    return {
        "key_path": key.private_key_path,
        "public_key_fingerprint": openssh_public_key_fingerprint(
            key.openssh_public_key,
        ),
        "host_key_type": host_key.split(" ", 1)[0],
        "host_key_value": host_key,
        "host_key_fingerprint": openssh_public_key_fingerprint(host_key),
    }


def executor_for_profile(profile: SSHProfile) -> SSHExecutor:
    if profile.deleted_at is not None:
        raise ValueError("SSH profile has been deleted")
    return SSHExecutor(
        profile.host,
        profile.username,
        profile.key_path,
        port=profile.port,
        host_key_policy="pinned",
        pinned_host_key=profile.host_key_value,
        expected_public_key_fingerprint=profile.public_key_fingerprint,
    )


async def test_profile(profile: SSHProfile, *, timeout: int = 10):
    return await executor_for_profile(profile).probe(timeout=timeout)
