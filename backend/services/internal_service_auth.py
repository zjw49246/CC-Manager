"""Short-lived, route-scoped credentials for CCM child processes.

The deployment ``AUTH_TOKEN`` is an administrator credential and must never be
handed to a Task-launched MCP process.  This module derives signed bearer
tokens whose audience and identifiers are checked against every HTTP request.
The raw deployment secret never leaves the Manager process.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
import secrets
import threading
import time
from dataclasses import dataclass
from typing import Any, Mapping


INTERNAL_TOKEN_ENV = "CCM_INTERNAL_SERVICE_TOKEN"
ASK_USER_TOKEN_ENV = "CCM_ASK_USER_TOKEN"
_TOKEN_PREFIX = "ccm-internal-v1"
_TOKEN_TTL_SECONDS = 24 * 60 * 60
_CLOCK_SKEW_SECONDS = 30
_MAX_TOKEN_LENGTH = 4096
_SEGMENT_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,160}$")
_ACTIVE_TASK_STATUSES = frozenset({"in_progress", "executing"})
_GENERATION_BOUND_TASK_AUDIENCES = frozenset({
    "ccm_ssh",
    "ccm_skills",
    "ccm_ask_user",
    "ccm_frontend_review",
    "ccm_workspace_review",
    "ccm_browser_review",
})


class InternalServiceTokenError(ValueError):
    """Authentication or route-authorization failure for a scoped token."""

    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


@dataclass(frozen=True, slots=True)
class InternalServiceClaims:
    audience: str
    token_id: str
    expires_at: int
    task_id: int | None = None
    task_incarnation_id: str | None = None
    task_retry_count: int | None = None
    task_turn_generation: int | None = None
    task_status: str | None = None
    monitor_session_id: int | None = None
    sub_agent_session_id: int | None = None
    owner_kind: str | None = None
    owner_id: str | None = None


_revocation_lock = threading.Lock()
_owner_tokens: dict[tuple[str, str], set[str]] = {}
_revoked_tokens: dict[str, int] = {}
_owner_token_cache: dict[
    tuple[str, ...],
    tuple[str, int, str],
] = {}


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _b64decode(value: str) -> bytes:
    if not value or not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise ValueError("invalid base64url")
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _deployment_secret() -> bytes:
    from backend.config import settings

    value = getattr(settings, "auth_token", "")
    if not isinstance(value, str) or not value.strip():
        return b""
    return value.encode("utf-8")


def _positive_int(value: Any, field: str, *, optional: bool = True) -> int | None:
    if value is None and optional:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise InternalServiceTokenError(401, f"Invalid internal {field}")
    return value


def _non_negative_int(
    value: Any,
    field: str,
    *,
    optional: bool = True,
) -> int | None:
    if value is None and optional:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise InternalServiceTokenError(401, f"Invalid internal {field}")
    return value


def _safe_segment(value: Any, field: str, *, optional: bool = True) -> str | None:
    if value is None and optional:
        return None
    if not isinstance(value, str) or not _SEGMENT_RE.fullmatch(value):
        raise InternalServiceTokenError(401, f"Invalid internal {field}")
    return value


def _task_incarnation(value: Any, *, optional: bool = True) -> str | None:
    if value is None and optional:
        return None
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{32}", value):
        raise InternalServiceTokenError(401, "Invalid internal task incarnation")
    return value


def _cleanup_revocations(now: int) -> None:
    expired_cached = [
        (cache_key, cached[2])
        for cache_key, cached in _owner_token_cache.items()
        if cached[1] <= now
    ]
    for cache_key, token_id in expired_cached:
        _owner_token_cache.pop(cache_key, None)
        owner = (cache_key[0], cache_key[1])
        token_ids = _owner_tokens.get(owner)
        if token_ids is not None:
            token_ids.discard(token_id)
            if not token_ids:
                _owner_tokens.pop(owner, None)
    expired = [token_id for token_id, expiry in _revoked_tokens.items() if expiry < now]
    for token_id in expired:
        _revoked_tokens.pop(token_id, None)
    if not expired:
        return
    expired_set = set(expired)
    for owner, token_ids in list(_owner_tokens.items()):
        token_ids.difference_update(expired_set)
        if not token_ids:
            _owner_tokens.pop(owner, None)


def issue_internal_service_token(
    *,
    audience: str,
    task_id: int | None = None,
    task_incarnation_id: str | None = None,
    task_retry_count: int | None = None,
    task_turn_generation: int | None = None,
    task_status: str | None = None,
    monitor_session_id: int | None = None,
    sub_agent_session_id: int | None = None,
    owner_kind: str,
    owner_id: str | int,
    ttl_seconds: int = _TOKEN_TTL_SECONDS,
) -> str:
    """Create one signed credential whose routes are derived from its audience."""

    secret = _deployment_secret()
    if not secret:
        return ""
    audience = _safe_segment(audience, "audience", optional=False) or ""
    owner_kind = _safe_segment(owner_kind, "owner kind", optional=False) or ""
    owner_id_value = _safe_segment(str(owner_id), "owner id", optional=False) or ""
    task_id = _positive_int(task_id, "task id")
    task_incarnation_id = _task_incarnation(task_incarnation_id)
    if task_id is not None and task_incarnation_id is None:
        raise ValueError("Task-scoped credential requires a Task incarnation")
    if task_incarnation_id is not None and task_id is None:
        raise ValueError("Task incarnation requires a task id")
    task_retry_count = _non_negative_int(
        task_retry_count,
        "Task retry count",
    )
    task_turn_generation = _non_negative_int(
        task_turn_generation,
        "Task turn generation",
    )
    task_status = _safe_segment(task_status, "Task status")
    task_generation_claims = (
        task_retry_count,
        task_turn_generation,
        task_status,
    )
    if any(value is not None for value in task_generation_claims) and (
        task_id is None
        or any(value is None for value in task_generation_claims)
    ):
        raise ValueError(
            "Task generation credential requires task id, retry count, "
            "turn generation, and status"
        )
    if audience in _GENERATION_BOUND_TASK_AUDIENCES:
        if any(value is None for value in task_generation_claims):
            raise ValueError(
                f"{audience} credential requires an exact active generation"
            )
        if task_status not in _ACTIVE_TASK_STATUSES:
            raise ValueError(
                f"{audience} credential requires an active Task status"
            )
    monitor_session_id = _positive_int(
        monitor_session_id,
        "monitor session id",
    )
    sub_agent_session_id = _positive_int(
        sub_agent_session_id,
        "sub-agent session id",
    )
    if (
        isinstance(ttl_seconds, bool)
        or not isinstance(ttl_seconds, int)
        or ttl_seconds <= 0
        or ttl_seconds > _TOKEN_TTL_SECONDS
    ):
        raise ValueError("Internal credential TTL is out of range")

    now = int(time.time())
    expires_at = now + ttl_seconds
    cache_key = (
        owner_kind,
        owner_id_value,
        audience,
        str(task_id or ""),
        str(task_incarnation_id or ""),
        str(task_retry_count if task_retry_count is not None else ""),
        str(
            task_turn_generation
            if task_turn_generation is not None
            else ""
        ),
        str(task_status or ""),
        str(monitor_session_id or ""),
        str(sub_agent_session_id or ""),
        str(ttl_seconds),
        hashlib.sha256(secret).hexdigest(),
    )
    with _revocation_lock:
        _cleanup_revocations(now)
        cached = _owner_token_cache.get(cache_key)
        # Reusing an identical scoped credential keeps a persistent Claude PTY
        # session's MCP children and AskUser hook valid across follow-up turns.
        # Refresh shortly before expiry; the caller's runtime fingerprint then
        # cold-resumes the PTY with the replacement credential.
        refresh_window = min(60, max(1, ttl_seconds // 10))
        if cached is not None and cached[1] > now + refresh_window:
            return cached[0]

    token_id = secrets.token_urlsafe(18)
    payload: dict[str, Any] = {
        "v": 1,
        "aud": audience,
        "jti": token_id,
        "iat": now,
        "exp": expires_at,
        "owner_kind": owner_kind,
        "owner_id": owner_id_value,
    }
    for key, value in (
        ("task_id", task_id),
        ("task_incarnation_id", task_incarnation_id),
        ("task_retry_count", task_retry_count),
        ("task_turn_generation", task_turn_generation),
        ("task_status", task_status),
        ("monitor_session_id", monitor_session_id),
        ("sub_agent_session_id", sub_agent_session_id),
    ):
        if value is not None:
            payload[key] = value
    encoded = _b64encode(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    )
    signed = f"{_TOKEN_PREFIX}.{encoded}"
    signature = _b64encode(hmac.new(secret, signed.encode("ascii"), hashlib.sha256).digest())
    token = f"{signed}.{signature}"

    with _revocation_lock:
        _cleanup_revocations(now)
        cached = _owner_token_cache.get(cache_key)
        if cached is not None and cached[1] > now + refresh_window:
            return cached[0]
        _owner_tokens.setdefault((owner_kind, owner_id_value), set()).add(token_id)
        _owner_token_cache[cache_key] = (token, expires_at, token_id)
    return token


def revoke_internal_service_owner(owner_kind: str, owner_id: str | int) -> None:
    """Revoke all credentials issued for one exact process/config lifecycle."""

    owner = (str(owner_kind), str(owner_id))
    now = int(time.time())
    with _revocation_lock:
        _cleanup_revocations(now)
        for token_id in _owner_tokens.pop(owner, set()):
            _revoked_tokens[token_id] = now + _TOKEN_TTL_SECONDS
        for cache_key in [
            key for key in _owner_token_cache if key[:2] == owner
        ]:
            _owner_token_cache.pop(cache_key, None)


def revoke_internal_service_owner_prefix(owner_kind: str, owner_id_prefix: str) -> None:
    """Revoke all generations belonging to one long-lived child session."""

    now = int(time.time())
    with _revocation_lock:
        _cleanup_revocations(now)
        owners = [
            owner
            for owner in _owner_tokens
            if owner[0] == str(owner_kind)
            and owner[1].startswith(str(owner_id_prefix))
        ]
        for owner in owners:
            for token_id in _owner_tokens.pop(owner, set()):
                _revoked_tokens[token_id] = now + _TOKEN_TTL_SECONDS
        for cache_key in [
            key
            for key in _owner_token_cache
            if key[0] == str(owner_kind)
            and key[1].startswith(str(owner_id_prefix))
        ]:
            _owner_token_cache.pop(cache_key, None)


def _decode_claims(token: str) -> InternalServiceClaims:
    if len(token) > _MAX_TOKEN_LENGTH:
        raise InternalServiceTokenError(401, "Invalid internal service credential")
    try:
        prefix, encoded, signature = token.split(".", 2)
    except ValueError as exc:
        raise InternalServiceTokenError(401, "Invalid internal service credential") from exc
    if prefix != _TOKEN_PREFIX:
        raise InternalServiceTokenError(401, "Invalid internal service credential")
    secret = _deployment_secret()
    if not secret:
        raise InternalServiceTokenError(401, "Internal service authentication is disabled")
    signed = f"{prefix}.{encoded}"
    expected = _b64encode(hmac.new(secret, signed.encode("ascii"), hashlib.sha256).digest())
    if not hmac.compare_digest(signature, expected):
        raise InternalServiceTokenError(401, "Invalid internal service credential")
    try:
        payload = json.loads(_b64decode(encoded))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InternalServiceTokenError(401, "Invalid internal service credential") from exc
    if not isinstance(payload, dict) or payload.get("v") != 1:
        raise InternalServiceTokenError(401, "Invalid internal service credential")

    issued_at = _positive_int(payload.get("iat"), "issued-at", optional=False)
    expires_at = _positive_int(payload.get("exp"), "expiry", optional=False)
    assert issued_at is not None and expires_at is not None
    now = int(time.time())
    if issued_at > now + _CLOCK_SKEW_SECONDS or expires_at <= now:
        raise InternalServiceTokenError(401, "Internal service credential expired")
    if expires_at - issued_at > _TOKEN_TTL_SECONDS:
        raise InternalServiceTokenError(401, "Invalid internal service credential lifetime")

    claims = InternalServiceClaims(
        audience=_safe_segment(payload.get("aud"), "audience", optional=False) or "",
        token_id=_safe_segment(payload.get("jti"), "token id", optional=False) or "",
        expires_at=expires_at,
        task_id=_positive_int(payload.get("task_id"), "task id"),
        task_incarnation_id=_task_incarnation(
            payload.get("task_incarnation_id")
        ),
        task_retry_count=_non_negative_int(
            payload.get("task_retry_count"),
            "Task retry count",
        ),
        task_turn_generation=_non_negative_int(
            payload.get("task_turn_generation"),
            "Task turn generation",
        ),
        task_status=_safe_segment(
            payload.get("task_status"),
            "Task status",
        ),
        monitor_session_id=_positive_int(
            payload.get("monitor_session_id"),
            "monitor session id",
        ),
        sub_agent_session_id=_positive_int(
            payload.get("sub_agent_session_id"),
            "sub-agent session id",
        ),
        owner_kind=_safe_segment(payload.get("owner_kind"), "owner kind"),
        owner_id=_safe_segment(payload.get("owner_id"), "owner id"),
    )
    if claims.task_incarnation_id is not None and claims.task_id is None:
        raise InternalServiceTokenError(
            401,
            "Invalid internal task incarnation binding",
        )
    task_generation_claims = (
        claims.task_retry_count,
        claims.task_turn_generation,
        claims.task_status,
    )
    if any(value is not None for value in task_generation_claims) and (
        claims.task_id is None
        or any(value is None for value in task_generation_claims)
    ):
        raise InternalServiceTokenError(
            401,
            "Invalid internal Task generation binding",
        )
    if claims.audience in _GENERATION_BOUND_TASK_AUDIENCES and (
        any(value is None for value in task_generation_claims)
        or claims.task_status not in _ACTIVE_TASK_STATUSES
    ):
        raise InternalServiceTokenError(
            401,
            "Invalid internal active Task generation binding",
        )
    with _revocation_lock:
        _cleanup_revocations(now)
        if claims.token_id in _revoked_tokens:
            raise InternalServiceTokenError(401, "Internal service credential revoked")
    return claims


def _fullmatch(pattern: str, path: str) -> bool:
    return re.fullmatch(pattern, path) is not None


def _route_allowed(claims: InternalServiceClaims, method: str, path: str) -> bool:
    method = method.upper()
    task_id = claims.task_id
    if path != "/" and path.endswith("/"):
        path = path[:-1]

    if claims.audience == "ccm_ssh" and task_id is not None:
        base = rf"/api/tasks/{task_id}/ssh-access"
        return (method == "GET" and path == base) or (
            method == "POST"
            and _fullmatch(rf"{base}/[1-9][0-9]*/(execute|list|read|write)", path)
        )

    if claims.audience == "ccm_ask_user" and task_id is not None:
        return method == "POST" and path == "/api/ask-user/wait"

    if claims.audience in {
        "ccm_frontend_review",
        "ccm_workspace_review",
    } and task_id is not None:
        base = rf"/api/tasks/{task_id}/test-runs"
        if method == "GET" and path == f"{base}/capabilities":
            return True
        if method == "POST" and path == f"{base}/internal/start":
            return True
        if method in {"GET", "POST"} and _fullmatch(
            rf"{base}/[0-9a-f]{{32}}/internal/(status|stop)",
            path,
        ):
            return (method == "GET") == path.endswith("/status")
        return method == "GET" and _fullmatch(
            rf"{base}/[0-9a-f]{{32}}/compare/[0-9a-f]{{32}}",
            path,
        )

    if (
        claims.audience == "ccm_browser_review"
        and task_id is not None
        and claims.owner_kind == "browser-review-job"
        and claims.owner_id is not None
        and re.fullmatch(r"[0-9a-f]{32}", claims.owner_id)
    ):
        base = rf"/api/browser-reviews/{claims.owner_id}/internal"
        if method == "GET" and path == f"{base}/context":
            return True
        if method == "POST" and path == f"{base}/events":
            return True
        return method == "POST" and _fullmatch(
            rf"{base}/operations/[0-9a-f]{{32}}/(permit|ack)",
            path,
        )

    if claims.audience == "ccm_skills" and task_id is not None:
        task_path = f"/api/tasks/{task_id}"
        if path == task_path and method == "GET":
            return True
        if path == f"{task_path}/internal/skill-tools" and method == "POST":
            return True
        if path == f"{task_path}/internal/enabled-skills" and method == "PUT":
            return True
        if path in {
            f"{task_path}/monitor-sessions",
            f"{task_path}/sub-agent-sessions",
        } and method in {"GET", "POST"}:
            return True
        return method == "DELETE" and _fullmatch(
            rf"{re.escape(task_path)}/(monitor-sessions|sub-agent-sessions)/[1-9][0-9]*",
            path,
        )

    if (
        claims.audience == "ccm_monitor_agent"
        and task_id is not None
        and claims.monitor_session_id is not None
    ):
        base = f"/api/tasks/{task_id}/monitor-sessions/{claims.monitor_session_id}"
        return (method, path) in {
            ("GET", base),
            ("POST", f"{base}/checks"),
            ("POST", f"{base}/complete"),
        }

    if (
        claims.audience == "ccm_sub_agent"
        and task_id is not None
        and claims.sub_agent_session_id is not None
    ):
        base = f"/api/tasks/{task_id}/sub-agent-sessions/{claims.sub_agent_session_id}"
        return (method, path) in {
            ("GET", f"{base}/context"),
            ("POST", f"{base}/progress"),
            ("POST", f"{base}/result"),
        }
    return False


def authenticate_internal_service_token(
    token: str,
    *,
    method: str,
    path: str,
) -> InternalServiceClaims:
    """Verify signature, expiry, revocation, audience, and exact HTTP route."""

    claims = _decode_claims(token)
    if not _route_allowed(claims, method, path):
        raise InternalServiceTokenError(
            403,
            "Internal service credential is not authorized for this route",
        )
    return claims


def is_internal_service_token(token: str) -> bool:
    return token.startswith(f"{_TOKEN_PREFIX}.")


def internal_task_id(claims: object) -> int | None:
    if isinstance(claims, InternalServiceClaims):
        return claims.task_id
    if isinstance(claims, Mapping):
        value = claims.get("task_id")
        return value if isinstance(value, int) and not isinstance(value, bool) else None
    return None
