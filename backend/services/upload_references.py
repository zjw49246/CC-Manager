"""Pure validation helpers for public CCM upload references."""

import re


_MANAGED_UPLOAD_NAME_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12}(?:\.[a-z0-9]{1,16})?$"
)
_MANAGED_UPLOAD_ID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-"
    r"[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
_FORK_SEED_UPLOAD_ID_RE = re.compile(
    r"^fork-seed-(?:0|[1-9][0-9]{0,5})$"
)


def is_managed_upload_basename(value: object) -> bool:
    """Return whether *value* can only be a CCM-generated upload basename."""

    return (
        isinstance(value, str)
        and _MANAGED_UPLOAD_NAME_RE.fullmatch(value) is not None
    )


def is_public_upload_reference_id(value: object) -> bool:
    """Accept only IDs CCM itself emits for browser-restorable uploads.

    A normal upload receives a canonical lowercase UUIDv4.  A Codex fork
    rebuilds existing attachments without creating another upload row and
    therefore uses the deterministic ``fork-seed-N`` form.  The bounded,
    canonical decimal form prevents arbitrary metadata strings from being
    reflected into the public Task response.
    """

    return bool(
        isinstance(value, str)
        and (
            _MANAGED_UPLOAD_ID_RE.fullmatch(value) is not None
            or _FORK_SEED_UPLOAD_ID_RE.fullmatch(value) is not None
        )
    )


def managed_upload_url_basename(value: object) -> str | None:
    """Extract a validated basename from one public upload API URL."""

    if not isinstance(value, str) or not value.startswith("/api/uploads/"):
        return None
    basename = value.removeprefix("/api/uploads/")
    return basename if is_managed_upload_basename(basename) else None


def is_safe_upload_display_name(value: object) -> bool:
    """Reject path/control payloads while preserving an original filename."""

    return bool(
        isinstance(value, str)
        and value
        and len(value) <= 255
        and value not in {".", ".."}
        and "/" not in value
        and "\\" not in value
        and all(32 <= ord(character) != 127 for character in value)
    )
