"""One-shot Manager permits for Worker provider launches.

The Worker bearer token authenticates the Manager control plane; it is never
the user principal.  A Worker that is about to cross a real provider boundary
broadcasts an exact Task-generation request over the already-authenticated
relay socket.  The Manager revalidates its own User/Task rows and signs a
single response for that request.  Losing the relay or timing out is a veto.

Only the in-memory waiter which created a random request id can consume the
response.  This makes the permit request-scoped, one-shot, and short lived
without relying on synchronized wall clocks between Manager and Worker hosts.
At-most-once provider admission for the Task generation remains the Worker's
durable ``LogEntry.actual_transport`` CAS plus its Instance lifecycle lock: a
Manager may answer a replacement request after a Worker crash only while no
provider boundary was durably crossed.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import secrets
from dataclasses import dataclass
from weakref import WeakKeyDictionary

from backend.config import settings
from backend.services.task_creation import task_execution_principal_values


WORKER_DELEGATED_LAUNCH_ADMISSION_PROTOCOL = 2
WORKER_LAUNCH_ADMISSION_EVENT = "worker_launch_admission_request"
WORKER_LAUNCH_ADMISSION_RESPONSE_ACTION = (
    "worker_launch_admission_response"
)
WORKER_LAUNCH_ADMISSION_TIMEOUT_SECONDS = 30.0
WORKER_CONTEXT_PREFLIGHT_PROOF_KEY = "_ccm_context_preflight_proof"
WORKER_CONTEXT_PREFLIGHT_PROOF_VERSION = 1
WORKER_CONTEXT_RETRY_MARKER_METADATA_KEY = (
    "worker_context_retry_authority"
)
WORKER_CONTEXT_RETRY_MARKER_VERSION = 1
WORKER_EXACT_LAUNCH_MARKER_METADATA_KEY = "worker_exact_launch_authority"
WORKER_EXACT_LAUNCH_MARKER_VERSION = 1

_ACTUAL_TRANSPORTS = frozenset(
    {"claude_pty", "claude_exec", "codex_app_server", "codex_exec"}
)
_WORKER_PRINCIPAL_KINDS = frozenset(
    {"delegated_user", "delegated_deployment_token", "system"}
)


class WorkerLaunchAdmissionError(RuntimeError):
    """A Worker launch did not receive an exact Manager permit."""


@dataclass(frozen=True, slots=True)
class WorkerContextRetryAuthority:
    authority_id: str
    retry_count: int
    from_generation: int
    source_log_id: int
    claimed_source_log_id: int


@dataclass(frozen=True, slots=True)
class WorkerLaunchAdmissionRequest:
    request_id: str
    request_digest: str
    task_id: int
    incarnation_id: str
    retry_count: int
    turn_generation: int
    actual_transport: str
    principal: dict[str, object]
    principal_digest: str
    context_retry: WorkerContextRetryAuthority | None = None


@dataclass(slots=True)
class _PendingPermit:
    request_digest: str
    future: asyncio.Future[dict]


_pending_by_loop: WeakKeyDictionary[
    asyncio.AbstractEventLoop,
    dict[str, _PendingPermit],
] = WeakKeyDictionary()


def canonical_payload_digest(payload: object) -> str:
    """Return the cross-host digest used by requests and signed responses."""

    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def canonical_delegated_launch_principal(
    payload: object,
) -> dict[str, object] | None:
    """Accept one complete Worker-wire principal snapshot.

    Human/token authority is explicitly delegated by the Manager. Derived
    system work remains sandboxed but still needs an exact-generation permit
    before a Worker mirror may cross the provider boundary.
    """

    if not isinstance(payload, dict):
        return None
    principal = {
        "execution_user_id": payload.get("execution_user_id"),
        "execution_user_role": payload.get("execution_user_role"),
        "execution_mode": payload.get("execution_mode"),
        "execution_principal_kind": payload.get("execution_principal_kind"),
    }
    if principal["execution_principal_kind"] not in _WORKER_PRINCIPAL_KINDS:
        return None
    try:
        expected = task_execution_principal_values(
            user_id=principal["execution_user_id"],
            role=principal["execution_user_role"],
            principal_kind=principal["execution_principal_kind"],
        )
    except (TypeError, ValueError):
        return None
    return principal if principal == expected else None


def _valid_hex(value: object, length: int) -> bool:
    return bool(
        isinstance(value, str)
        and len(value) == length
        and all(char in "0123456789abcdef" for char in value)
    )


def parse_worker_context_retry_authority(
    payload: object,
) -> WorkerContextRetryAuthority | None:
    """Parse the exact rejected-generation authority carried by a Worker."""

    if not isinstance(payload, dict) or set(payload) != {
        "authority_id",
        "retry_count",
        "from_generation",
        "source_log_id",
        "claimed_source_log_id",
    }:
        return None
    authority_id = payload.get("authority_id")
    retry_count = payload.get("retry_count")
    from_generation = payload.get("from_generation")
    source_log_id = payload.get("source_log_id")
    claimed_source_log_id = payload.get("claimed_source_log_id")
    if (
        not _valid_hex(authority_id, 32)
        or isinstance(retry_count, bool)
        or not isinstance(retry_count, int)
        or retry_count < 0
        or isinstance(from_generation, bool)
        or not isinstance(from_generation, int)
        or from_generation < 1
        or isinstance(source_log_id, bool)
        or not isinstance(source_log_id, int)
        or source_log_id <= 0
        or isinstance(claimed_source_log_id, bool)
        or not isinstance(claimed_source_log_id, int)
        or claimed_source_log_id <= 0
    ):
        return None
    return WorkerContextRetryAuthority(
        authority_id=authority_id,
        retry_count=retry_count,
        from_generation=from_generation,
        source_log_id=source_log_id,
        claimed_source_log_id=claimed_source_log_id,
    )


def build_codex_context_preflight_relay_proof(
    raw_payload: object,
    event: object,
    *,
    retry_count: int,
    turn_generation: int,
    source_log_id: int,
    actual_transport: str,
) -> dict[str, object] | None:
    """Normalize only non-sensitive Codex preflight evidence for relay.

    Provider ``raw_json`` can contain model/user data and is deliberately not
    relayed.  This proof retains only the raw envelope type, exact local turn
    identity/transport, the terminal error class, and a digest which binds the
    already-public error message without duplicating its content.
    """

    if isinstance(raw_payload, dict):
        raw = raw_payload
    elif isinstance(raw_payload, str) and raw_payload:
        try:
            parsed = json.loads(raw_payload)
        except (TypeError, ValueError, RecursionError):
            return None
        raw = parsed if isinstance(parsed, dict) else None
    else:
        raw = None
    if (
        raw is None
        or not isinstance(event, dict)
        or type(retry_count) is not int
        or retry_count < 0
        or type(turn_generation) is not int
        or turn_generation < 1
        or type(source_log_id) is not int
        or source_log_id <= 0
        or actual_transport not in {"codex_app_server", "codex_exec"}
    ):
        return None
    raw_type = raw.get("type")
    common: dict[str, object] = {
        "version": WORKER_CONTEXT_PREFLIGHT_PROOF_VERSION,
        "provider": "codex",
        "raw_type": raw_type,
        "retry_count": retry_count,
        "turn_generation": turn_generation,
        "source_log_id": source_log_id,
        "actual_transport": actual_transport,
    }
    if raw_type in {"thread.started", "turn.started"}:
        if not (
            event.get("event_type") == "system_event"
            and event.get("is_error") is False
        ):
            return None
        return common
    if raw_type != "turn.failed":
        return None
    error = raw.get("error")
    error_code = (
        error.get("codexErrorInfo") if isinstance(error, dict) else None
    )
    error_message = error.get("message") if isinstance(error, dict) else None
    if not (
        event.get("event_type") == "system_event"
        and event.get("role") is None
        and event.get("is_error") is True
        and isinstance(error_message, str)
        and event.get("content") == error_message
        and isinstance(error_code, str)
        and error_code.strip().lower() == "contextwindowexceeded"
    ):
        return None
    common.update(
        codex_error_info="ContextWindowExceeded",
        message_sha256=hashlib.sha256(
            error_message.encode("utf-8")
        ).hexdigest(),
    )
    return common


def parse_codex_context_preflight_relay_proof(
    payload: object,
) -> dict[str, object] | None:
    """Strictly parse one normalized proof persisted by Manager relay."""

    if isinstance(payload, str) and payload:
        try:
            payload = json.loads(payload)
        except (TypeError, ValueError, RecursionError):
            return None
    if not isinstance(payload, dict):
        return None
    raw_type = payload.get("raw_type")
    expected_keys = {
        "version",
        "provider",
        "raw_type",
        "retry_count",
        "turn_generation",
        "source_log_id",
        "actual_transport",
    }
    if raw_type == "turn.failed":
        expected_keys.update({"codex_error_info", "message_sha256"})
    if set(payload) != expected_keys:
        return None
    if (
        payload.get("version") != WORKER_CONTEXT_PREFLIGHT_PROOF_VERSION
        or payload.get("provider") != "codex"
        or raw_type not in {"thread.started", "turn.started", "turn.failed"}
        or type(payload.get("retry_count")) is not int
        or payload["retry_count"] < 0
        or type(payload.get("turn_generation")) is not int
        or payload["turn_generation"] < 1
        or type(payload.get("source_log_id")) is not int
        or payload["source_log_id"] <= 0
        or payload.get("actual_transport")
        not in {"codex_app_server", "codex_exec"}
    ):
        return None
    if raw_type == "turn.failed" and not (
        payload.get("codex_error_info") == "ContextWindowExceeded"
        and _valid_hex(payload.get("message_sha256"), 64)
    ):
        return None
    return dict(payload)


def parse_worker_launch_admission_request(
    payload: object,
) -> WorkerLaunchAdmissionRequest | None:
    """Parse and recompute every identity field in one Worker request."""

    if not isinstance(payload, dict):
        return None
    request_id = payload.get("request_id")
    request_digest = payload.get("request_digest")
    task_id = payload.get("task_id")
    incarnation_id = payload.get("incarnation_id")
    retry_count = payload.get("retry_count")
    turn_generation = payload.get("turn_generation")
    actual_transport = payload.get("actual_transport")
    principal = canonical_delegated_launch_principal(
        payload.get("execution_principal")
    )
    principal_digest = payload.get("principal_digest")
    context_retry = None
    if "context_retry" in payload:
        context_retry = parse_worker_context_retry_authority(
            payload.get("context_retry")
        )
    if (
        payload.get("event_type") != WORKER_LAUNCH_ADMISSION_EVENT
        or payload.get("protocol_version")
        != WORKER_DELEGATED_LAUNCH_ADMISSION_PROTOCOL
        or not _valid_hex(request_id, 32)
        or not _valid_hex(request_digest, 64)
        or isinstance(task_id, bool)
        or not isinstance(task_id, int)
        or task_id <= 0
        or not _valid_hex(incarnation_id, 32)
        or isinstance(retry_count, bool)
        or not isinstance(retry_count, int)
        or retry_count < 0
        or isinstance(turn_generation, bool)
        or not isinstance(turn_generation, int)
        or turn_generation < 1
        or actual_transport not in _ACTUAL_TRANSPORTS
        or principal is None
        or not _valid_hex(principal_digest, 64)
        or canonical_payload_digest(principal) != principal_digest
        or ("context_retry" in payload and context_retry is None)
        or (
            context_retry is not None
            and (
                context_retry.retry_count != retry_count
                or turn_generation != context_retry.from_generation + 1
            )
        )
    ):
        return None
    identity = {
        key: value
        for key, value in payload.items()
        if key != "request_digest"
    }
    if canonical_payload_digest(identity) != request_digest:
        return None
    return WorkerLaunchAdmissionRequest(
        request_id=request_id,
        request_digest=request_digest,
        task_id=task_id,
        incarnation_id=incarnation_id,
        retry_count=retry_count,
        turn_generation=turn_generation,
        actual_transport=actual_transport,
        principal=principal,
        principal_digest=principal_digest,
        context_retry=context_retry,
    )


def build_worker_launch_admission_response(
    request: WorkerLaunchAdmissionRequest,
    *,
    worker_id: int,
    admitted: bool,
    reason_code: str,
    control_token: str,
) -> dict[str, object]:
    """Sign a response with the Worker control-plane credential.

    The signature proves which Manager/Worker control-plane pair issued the
    result.  Runtime authority remains the Manager's independently validated
    Task/User principal and is represented only by the request digest.
    """

    if (
        isinstance(worker_id, bool)
        or not isinstance(worker_id, int)
        or worker_id <= 0
        or type(admitted) is not bool
        or not isinstance(reason_code, str)
        or not reason_code
        or len(reason_code) > 100
        or not isinstance(control_token, str)
        or not control_token
    ):
        raise ValueError("invalid Worker launch admission response")
    response: dict[str, object] = {
        "action": WORKER_LAUNCH_ADMISSION_RESPONSE_ACTION,
        "protocol_version": WORKER_DELEGATED_LAUNCH_ADMISSION_PROTOCOL,
        "request_id": request.request_id,
        "request_digest": request.request_digest,
        "worker_id": worker_id,
        "task_id": request.task_id,
        "incarnation_id": request.incarnation_id,
        "retry_count": request.retry_count,
        "turn_generation": request.turn_generation,
        "principal_digest": request.principal_digest,
        "admitted": admitted,
        "reason_code": reason_code,
    }
    response["signature"] = hmac.new(
        control_token.encode("utf-8"),
        canonical_payload_digest(response).encode("ascii"),
        hashlib.sha256,
    ).hexdigest()
    return response


def _response_signature_valid(payload: dict, control_token: str) -> bool:
    signature = payload.get("signature")
    if not _valid_hex(signature, 64) or not control_token:
        return False
    unsigned = {key: value for key, value in payload.items() if key != "signature"}
    expected = hmac.new(
        control_token.encode("utf-8"),
        canonical_payload_digest(unsigned).encode("ascii"),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(signature, expected)


def accept_worker_launch_admission_response(
    payload: object,
    *,
    control_token: str,
) -> bool:
    """Resolve exactly one live Worker-side waiter for a signed response."""

    if not isinstance(payload, dict):
        return False
    request_id = payload.get("request_id")
    request_digest = payload.get("request_digest")
    if (
        payload.get("action") != WORKER_LAUNCH_ADMISSION_RESPONSE_ACTION
        or payload.get("protocol_version")
        != WORKER_DELEGATED_LAUNCH_ADMISSION_PROTOCOL
        or not _valid_hex(request_id, 32)
        or not _valid_hex(request_digest, 64)
        or type(payload.get("admitted")) is not bool
        or isinstance(payload.get("worker_id"), bool)
        or not isinstance(payload.get("worker_id"), int)
        or payload.get("worker_id") <= 0
        or not _response_signature_valid(payload, control_token)
    ):
        return False
    loop = asyncio.get_running_loop()
    pending = _pending_by_loop.get(loop, {}).get(request_id)
    if (
        pending is None
        or pending.future.done()
        or pending.request_digest != request_digest
    ):
        return False
    pending.future.set_result(dict(payload))
    return True


async def request_worker_launch_admission(
    *,
    broadcaster,
    task_id: int,
    incarnation_id: str,
    retry_count: int,
    turn_generation: int,
    actual_transport: str,
    execution_principal: dict[str, object],
    context_retry: dict[str, object] | None = None,
    timeout_seconds: float = WORKER_LAUNCH_ADMISSION_TIMEOUT_SECONDS,
) -> dict[str, object]:
    """Request and consume one exact Manager permit at provider boundary."""

    if settings.ccm_node_role != "worker":
        raise WorkerLaunchAdmissionError(
            "Worker launch admission is valid only on a Worker node"
        )
    if not isinstance(settings.auth_token, str) or not settings.auth_token:
        raise WorkerLaunchAdmissionError(
            "Worker launch admission requires an authenticated control plane"
        )
    if timeout_seconds <= 0:
        raise ValueError("Worker launch admission timeout must be positive")
    principal = canonical_delegated_launch_principal(execution_principal)
    if principal is None:
        raise WorkerLaunchAdmissionError(
            "Worker launch admission principal is invalid"
        )
    request: dict[str, object] = {
        "event_type": WORKER_LAUNCH_ADMISSION_EVENT,
        "protocol_version": WORKER_DELEGATED_LAUNCH_ADMISSION_PROTOCOL,
        "request_id": secrets.token_hex(16),
        "task_id": task_id,
        "incarnation_id": incarnation_id,
        "retry_count": retry_count,
        "turn_generation": turn_generation,
        "actual_transport": actual_transport,
        "execution_principal": principal,
        "principal_digest": canonical_payload_digest(principal),
    }
    if context_retry is not None:
        parsed_context_retry = parse_worker_context_retry_authority(
            context_retry
        )
        if (
            parsed_context_retry is None
            or parsed_context_retry.retry_count != retry_count
            or turn_generation
            != parsed_context_retry.from_generation + 1
        ):
            raise WorkerLaunchAdmissionError(
                "Worker context retry authority is invalid"
            )
        request["context_retry"] = {
            "authority_id": parsed_context_retry.authority_id,
            "retry_count": parsed_context_retry.retry_count,
            "from_generation": parsed_context_retry.from_generation,
            "source_log_id": parsed_context_retry.source_log_id,
            "claimed_source_log_id": (
                parsed_context_retry.claimed_source_log_id
            ),
        }
    request["request_digest"] = canonical_payload_digest(request)
    parsed = parse_worker_launch_admission_request(request)
    if parsed is None:
        raise WorkerLaunchAdmissionError(
            "Worker launch admission identity is invalid"
        )

    loop = asyncio.get_running_loop()
    pending_for_loop = _pending_by_loop.setdefault(loop, {})
    future: asyncio.Future[dict] = loop.create_future()
    pending_for_loop[parsed.request_id] = _PendingPermit(
        request_digest=parsed.request_digest,
        future=future,
    )
    try:
        await broadcaster.broadcast(f"task:{task_id}", request)
        try:
            response = await asyncio.wait_for(future, timeout=timeout_seconds)
        except asyncio.TimeoutError as exc:
            raise WorkerLaunchAdmissionError(
                "Manager launch admission timed out; provider launch was denied"
            ) from exc
    finally:
        pending_for_loop.pop(parsed.request_id, None)
        if not pending_for_loop:
            _pending_by_loop.pop(loop, None)
    if (
        response.get("task_id") != task_id
        or response.get("incarnation_id") != incarnation_id
        or response.get("retry_count") != retry_count
        or response.get("turn_generation") != turn_generation
        or response.get("principal_digest") != parsed.principal_digest
    ):
        raise WorkerLaunchAdmissionError(
            "Manager launch admission response identity changed"
        )
    if response.get("admitted") is not True:
        raise WorkerLaunchAdmissionError(
            "Manager denied the Worker provider launch"
        )
    return response
