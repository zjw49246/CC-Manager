from __future__ import annotations

from datetime import datetime

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
    result = await executor_for_profile(profile).probe(timeout=timeout)
    profile.last_tested_at = datetime.utcnow()
    profile.last_test_ok = result.ok
    profile.last_error_code = result.error_code
    profile.last_error_detail = (result.detail or "")[:500] or None
    return result
