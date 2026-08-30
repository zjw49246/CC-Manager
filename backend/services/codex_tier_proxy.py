"""Loopback Responses API proxy that fences Codex service-tier requests.

Codex 0.144.6 accepts a service tier through app-server, but its app-server
protocol does not expose the upstream ``response.service_tier`` field.  This
proxy is therefore deliberately placed on the HTTP Responses path:

* the request body must carry the tier expected for its native thread;
* a child thread inherits the expectation through
  ``x-codex-parent-thread-id``;
* Fast verifies that the exact native turn sends ``service_tier=priority``;
* a successful ``response.created`` accepts the request even when its
  informational ``service_tier`` reports ``auto``/``default`` or is absent;
* Standard proves that the outgoing request did not ask for priority, then
  transparently streams the upstream response without depending on optional
  response-tier metadata or a particular SSE prelude.

The listener is per app-server/CODEX_HOME, loopback-only, and protected by a
high-entropy path.  Authentication headers are forwarded but never retained or
logged.  Unsupported model endpoints and ambiguous thread lineage fail closed.
"""

from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
import os
import secrets
import stat
import time
import tomllib
from collections import deque
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, AsyncIterator, Callable
from urllib.parse import parse_qsl, urlencode, urlsplit

import httpx

logger = logging.getLogger(__name__)

CODEX_TIER_DEFAULT = "default"
CODEX_TIER_PRIORITY = "priority"
_CODEX_TIERS = frozenset({CODEX_TIER_DEFAULT, CODEX_TIER_PRIORITY})
_CODEX_REPORTED_TIERS = frozenset({"auto", "default", "flex", "priority"})

_MAX_HEADER_BYTES = 64 * 1024
_MAX_HEADER_LINE_BYTES = 16 * 1024
_MAX_REQUEST_BYTES = 64 * 1024 * 1024
_MAX_FIRST_EVENT_BYTES = 1024 * 1024
_MAX_ERROR_BODY_BYTES = 4 * 1024 * 1024
_MAX_CONFIG_BYTES = 1024 * 1024
_MAX_AUTH_BYTES = 1024 * 1024
_REQUEST_HEADER_TIMEOUT = 10.0
_REQUEST_BODY_TIMEOUT = 30.0
_FIRST_EVENT_TIMEOUT = 60.0
_SHUTDOWN_TIMEOUT = 5.0

_HOP_BY_HOP = frozenset({
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
})
_SENSITIVE_HEADERS = frozenset({
    "authorization",
    "cookie",
    "proxy-authorization",
    "x-api-key",
})
_ALLOWED_CATALOG_PATHS = frozenset({"/models"})
_ALLOWED_MODEL_PATHS = frozenset({"/responses"})


class CodexTierProxyError(RuntimeError):
    """The tier request proxy could not safely serve a Codex app-server."""


class CodexTierProofError(CodexTierProxyError):
    """A request or upstream response could not prove safe tier admission."""


class _CodexTierRequestMismatch(CodexTierProofError):
    """A parsed request identified its turn but carried the wrong tier."""

    def __init__(
        self,
        message: str,
        identity: "_RequestIdentity",
    ) -> None:
        super().__init__(message)
        self.identity = identity


@dataclass(frozen=True, slots=True)
class CodexTierProxyRoute:
    """One proven upstream and the non-persistent Codex override it needs."""

    upstream_base_url: str
    provider_id: str = "openai"
    provider_aliases: tuple[str, ...] = ()
    built_in_openai: bool = True
    label: str = "OpenAI"

    def __post_init__(self) -> None:
        _validate_upstream_base_url(self.upstream_base_url)
        for provider_id in (self.provider_id, *self.provider_aliases):
            if (
                not provider_id
                or len(provider_id) > 128
                or not all(
                    char.isalnum() or char in "._-"
                    for char in provider_id
                )
            ):
                raise CodexTierProxyError("Unsafe Codex model provider id")


@dataclass(frozen=True, slots=True)
class CodexTierRequestProof:
    """Auditable priority request accepted by one upstream response."""

    thread_id: str
    turn_id: str
    parent_thread_id: str | None
    requested_tier: str
    upstream_reported_tier: str | None
    response_id: str
    observed_at: float


@dataclass(slots=True)
class _ProofWaiter:
    expected_tier: str
    future: asyncio.Future[CodexTierRequestProof]
    users: int = 0


@dataclass(frozen=True, slots=True)
class _RequestIdentity:
    thread_id: str
    turn_id: str
    parent_thread_id: str | None
    root_thread_id: str
    expected_tier: str
    requested_tier: str


def _validate_tier(value: str) -> str:
    tier = str(value or "").strip().lower()
    if tier not in _CODEX_TIERS:
        raise CodexTierProofError(f"Unsupported expected service tier: {value!r}")
    return tier


def _validate_identifier(value: Any, *, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 256
        or any(ord(char) < 0x20 or ord(char) == 0x7F for char in value)
    ):
        raise CodexTierProofError(f"Missing or invalid Codex {field}")
    return value


def _validate_upstream_base_url(value: str) -> str:
    try:
        parsed = urlsplit(str(value))
    except ValueError as exc:
        raise CodexTierProxyError("Invalid Codex upstream URL") from exc
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise CodexTierProxyError(
            "Codex tier-proxy upstream must be an HTTPS base URL "
            "without credentials, query, or fragment"
        )
    return str(value).rstrip("/")


def _read_private_regular_json(
    path: Path,
    *,
    maximum: int,
) -> dict[str, Any] | None:
    """Read bounded local state without following a final-component symlink."""

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise CodexTierProxyError(
            f"Could not safely read {path.name}"
        ) from exc
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or metadata.st_size > maximum
        ):
            raise CodexTierProxyError(f"Unsafe {path.name}")
        payload = bytearray()
        while len(payload) <= maximum:
            chunk = os.read(descriptor, min(65536, maximum + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
        if len(payload) > maximum:
            raise CodexTierProxyError(f"{path.name} is too large")
    finally:
        os.close(descriptor)
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CodexTierProxyError(f"Invalid {path.name}") from exc
    if not isinstance(value, dict):
        raise CodexTierProxyError(f"Invalid {path.name}")
    return value


def _read_private_regular_toml(
    path: Path,
    *,
    maximum: int,
) -> dict[str, Any]:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except FileNotFoundError:
        return {}
    except OSError as exc:
        raise CodexTierProxyError(
            f"Could not safely read {path.name}"
        ) from exc
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or metadata.st_size > maximum
        ):
            raise CodexTierProxyError(f"Unsafe {path.name}")
        payload = bytearray()
        while len(payload) <= maximum:
            chunk = os.read(descriptor, min(65536, maximum + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
        if len(payload) > maximum:
            raise CodexTierProxyError(f"{path.name} is too large")
    finally:
        os.close(descriptor)
    try:
        value = tomllib.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise CodexTierProxyError(f"Invalid {path.name}") from exc
    if not isinstance(value, dict):
        raise CodexTierProxyError(f"Invalid {path.name}")
    return value


def resolve_native_codex_tier_route(
    codex_home: str | os.PathLike[str],
    *,
    environ: dict[str, str] | None = None,
) -> CodexTierProxyRoute:
    """Resolve the built-in OpenAI request route without reading credentials.

    Only the built-in ``openai`` provider is accepted here.  CCM-owned API
    providers are resolved by their account store and passed as explicit
    :class:`CodexTierProxyRoute` objects.  Arbitrary custom providers fail
    closed because reproducing their auth/header/query semantics in an
    untrusted override would be unsafe.
    """

    home = Path(codex_home).expanduser().resolve(strict=False)
    config = _read_private_regular_toml(
        home / "config.toml",
        maximum=_MAX_CONFIG_BYTES,
    )
    provider = str(config.get("model_provider") or "openai").strip()
    if provider != "openai":
        raise CodexTierProxyError(
            f"Actual service-tier proof is unavailable for custom provider {provider!r}"
        )

    configured_openai_base = config.get("openai_base_url")
    if configured_openai_base is not None:
        if not isinstance(configured_openai_base, str):
            raise CodexTierProxyError("Invalid openai_base_url")
        upstream = _validate_upstream_base_url(configured_openai_base)
        return CodexTierProxyRoute(
            upstream_base_url=upstream,
            provider_id="openai",
            built_in_openai=True,
            label="OpenAI configured route",
        )

    env = os.environ if environ is None else environ
    if str(env.get("CODEX_API_KEY") or "").strip():
        return CodexTierProxyRoute(
            upstream_base_url="https://api.openai.com/v1",
            provider_id="openai",
            built_in_openai=True,
            label="OpenAI API",
        )
    # Codex 0.144.6 checks this before persistent auth.json and classifies it
    # as either a Personal Access Token or Agent Identity JWT.  Both modes use
    # the ChatGPT Codex backend; never let a stale auth.json API key redirect
    # the higher-precedence ephemeral token to api.openai.com.
    if str(env.get("CODEX_ACCESS_TOKEN") or "").strip():
        configured_chatgpt_base = config.get("chatgpt_base_url")
        if configured_chatgpt_base is None:
            upstream = "https://chatgpt.com/backend-api/codex"
        elif isinstance(configured_chatgpt_base, str):
            chatgpt_base = _validate_upstream_base_url(
                configured_chatgpt_base,
            )
            upstream = (
                chatgpt_base
                if chatgpt_base.rstrip("/").endswith("/codex")
                else f"{chatgpt_base.rstrip('/')}/codex"
            )
        else:
            raise CodexTierProxyError("Invalid chatgpt_base_url")
        return CodexTierProxyRoute(
            upstream_base_url=upstream,
            provider_id="openai",
            built_in_openai=True,
            label="ChatGPT Codex access token",
        )

    auth = _read_private_regular_json(
        home / "auth.json",
        maximum=_MAX_AUTH_BYTES,
    )
    if auth is None:
        raise CodexTierProxyError(
            "Codex auth type cannot be proven for actual service-tier routing"
        )
    auth_mode = str(auth.get("auth_mode") or "").strip().lower()
    stored_api_key = auth.get("OPENAI_API_KEY")
    if isinstance(stored_api_key, str) and stored_api_key.strip():
        upstream = "https://api.openai.com/v1"
        label = "OpenAI API"
    elif auth_mode in {
        "chatgpt",
        "chatgptauthtokens",
        "headers",
        "agentidentity",
        "personalaccesstoken",
    } or isinstance(auth.get("tokens"), dict):
        configured_chatgpt_base = config.get("chatgpt_base_url")
        if configured_chatgpt_base is None:
            upstream = "https://chatgpt.com/backend-api/codex"
        elif isinstance(configured_chatgpt_base, str):
            chatgpt_base = _validate_upstream_base_url(
                configured_chatgpt_base,
            )
            upstream = (
                chatgpt_base
                if chatgpt_base.rstrip("/").endswith("/codex")
                else f"{chatgpt_base.rstrip('/')}/codex"
            )
        else:
            raise CodexTierProxyError("Invalid chatgpt_base_url")
        label = "ChatGPT Codex"
    else:
        raise CodexTierProxyError(
            "Codex auth type cannot be proven for actual service-tier routing"
        )
    return CodexTierProxyRoute(
        upstream_base_url=upstream,
        provider_id="openai",
        built_in_openai=True,
        label=label,
    )


def _connection_tokens(headers: dict[str, str]) -> set[str]:
    value = headers.get("connection", "")
    return {
        token.strip().lower()
        for token in value.split(",")
        if token.strip()
    }


def _filter_request_headers(
    headers: dict[str, str],
    *,
    expect_sse: bool = False,
) -> dict[str, str]:
    blocked = _HOP_BY_HOP | _connection_tokens(headers) | {
        "host",
        "content-length",
        # Raw compressed bodies cannot be audited.  App-server launches also
        # disable request compression, and this remains a fail-closed guard.
        "content-encoding",
    }
    filtered = {
        name: value
        for name, value in headers.items()
        if name not in blocked
    }
    # SSE must remain inspectable.  Rejecting an unexpected compressed
    # response is still required, but this prevents normal upstreams from
    # selecting one.
    filtered["accept-encoding"] = "identity"
    if expect_sse:
        filtered["accept"] = "text/event-stream"
    return filtered


def _filter_response_headers(headers: httpx.Headers) -> list[tuple[str, str]]:
    lowered = {name.lower(): value for name, value in headers.multi_items()}
    blocked = _HOP_BY_HOP | _connection_tokens(lowered) | {
        "content-length",
        "content-encoding",
    }
    return [
        (name, value)
        for name, value in headers.multi_items()
        if name.lower() not in blocked
        and name.lower() not in _SENSITIVE_HEADERS
    ]


def _verified_sse_response_headers(
    headers: httpx.Headers,
) -> list[tuple[str, str]]:
    """Normalize a byte-verified Responses stream for the Codex client.

    The ChatGPT Codex backend can omit the SSE MIME header even though the
    response body is a valid Responses event stream.  The proxy proves the
    framing and service tier from the buffered body before calling this
    helper, so the downstream header can safely be canonicalized here.
    """

    filtered = [
        (name, value)
        for name, value in _filter_response_headers(headers)
        if name.lower() != "content-type"
    ]
    filtered.append(("Content-Type", "text/event-stream"))
    return filtered


def _split_sse_events(buffer: bytearray) -> tuple[list[bytes], bytearray]:
    """Split complete SSE records while preserving their original bytes."""

    normalized = bytes(buffer).replace(b"\r\n", b"\n")
    records: list[bytes] = []
    offset = 0
    while True:
        marker = normalized.find(b"\n\n", offset)
        if marker < 0:
            break
        # Mapping normalized offsets back to CRLF input is ambiguous.  This
        # parser is used only for validation; the caller retains and forwards
        # the untouched original buffer once proof succeeds.
        records.append(normalized[offset:marker + 2])
        offset = marker + 2
    return records, bytearray(normalized[offset:])


def _parse_sse_json(record: bytes) -> dict[str, Any] | None:
    data_lines: list[bytes] = []
    for raw_line in record.replace(b"\r\n", b"\n").split(b"\n"):
        if not raw_line or raw_line.startswith(b":"):
            continue
        field, separator, value = raw_line.partition(b":")
        if field != b"data":
            continue
        if separator and value.startswith(b" "):
            value = value[1:]
        data_lines.append(value)
    if not data_lines:
        return None
    payload = b"\n".join(data_lines)
    if payload == b"[DONE]":
        raise CodexTierProofError(
            "Responses stream ended before response.created"
        )
    try:
        parsed = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CodexTierProofError(
            "Invalid JSON before response.created"
        ) from exc
    if not isinstance(parsed, dict):
        raise CodexTierProofError("Invalid Responses SSE event")
    return parsed


def _extract_request_identity(
    headers: dict[str, str],
    body: bytes,
    expected_lineage: Callable[[str, str | None], tuple[str, str]],
) -> tuple[_RequestIdentity, bytes]:
    if headers.get("content-encoding", "identity").lower() not in {
        "",
        "identity",
    }:
        raise CodexTierProofError(
            "Compressed Codex requests cannot be audited"
        )
    try:
        payload = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CodexTierProofError("Invalid Codex Responses request JSON") from exc
    if not isinstance(payload, dict):
        raise CodexTierProofError("Invalid Codex Responses request")

    metadata = payload.get("client_metadata")
    if not isinstance(metadata, dict):
        metadata = {}
    turn_metadata: dict[str, Any] = {}
    encoded_turn_metadata = (
        metadata.get("x-codex-turn-metadata")
        or headers.get("x-codex-turn-metadata")
    )
    if isinstance(encoded_turn_metadata, str):
        try:
            candidate = json.loads(encoded_turn_metadata)
        except json.JSONDecodeError as exc:
            raise CodexTierProofError(
                "Invalid x-codex-turn-metadata"
            ) from exc
        if isinstance(candidate, dict):
            turn_metadata = candidate

    def exact_identifier(
        values: tuple[Any, ...],
        *,
        field: str,
        required: bool,
    ) -> str | None:
        present = [
            _validate_identifier(value, field=field)
            for value in values
            if value is not None
        ]
        if not present:
            if required:
                raise CodexTierProofError(
                    f"Missing or invalid Codex {field}"
                )
            return None
        if any(value != present[0] for value in present[1:]):
            raise CodexTierProofError(
                f"Conflicting Codex {field} metadata"
            )
        return present[0]

    thread_id = exact_identifier(
        (
            headers.get("x-client-request-id"),
            metadata.get("thread_id"),
            turn_metadata.get("thread_id"),
        ),
        field="thread id",
        required=True,
    )
    turn_id = exact_identifier(
        (
            metadata.get("turn_id"),
            turn_metadata.get("turn_id"),
        ),
        field="turn id",
        required=True,
    )
    parent_thread_id = exact_identifier(
        (
            headers.get("x-codex-parent-thread-id"),
            metadata.get("x-codex-parent-thread-id"),
            turn_metadata.get("parent_thread_id"),
        ),
        field="parent thread id",
        required=False,
    )
    assert thread_id is not None
    assert turn_id is not None
    root_thread_id, expected_raw = expected_lineage(
        thread_id,
        parent_thread_id,
    )
    root_thread_id = _validate_identifier(
        root_thread_id,
        field="root thread id",
    )
    expected = _validate_tier(expected_raw)
    requested_raw = payload.get("service_tier")
    if requested_raw in (None, CODEX_TIER_DEFAULT):
        requested = CODEX_TIER_DEFAULT
    elif requested_raw == CODEX_TIER_PRIORITY:
        requested = CODEX_TIER_PRIORITY
    else:
        requested = str(requested_raw)
    identity = _RequestIdentity(
        thread_id=thread_id,
        turn_id=turn_id,
        parent_thread_id=parent_thread_id,
        root_thread_id=root_thread_id,
        expected_tier=expected,
        requested_tier=requested,
    )
    if requested not in _CODEX_TIERS:
        raise _CodexTierRequestMismatch(
            f"Unsupported request service_tier {requested_raw!r}",
            identity,
        )
    if requested != expected and not (
        expected == CODEX_TIER_PRIORITY
        and requested == CODEX_TIER_DEFAULT
    ):
        raise _CodexTierRequestMismatch(
            f"Codex request tier mismatch for thread {thread_id}: "
            f"expected {expected}, got {requested}",
            identity,
        )
    if expected == CODEX_TIER_PRIORITY and requested == CODEX_TIER_DEFAULT:
        # Codex 0.147's app-server accepts serviceTier on thread/start but its
        # custom-provider Responses transport can omit (or reset) the matching
        # HTTP field.  The exact CCM lineage mapping is authoritative for
        # request construction. ``fast`` is the Codex UI/config spelling;
        # Responses API requests use the canonical ``priority`` wire value.
        payload["service_tier"] = CODEX_TIER_PRIORITY
        body = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(body) > _MAX_REQUEST_BYTES:
            raise CodexTierProofError("Codex request body is too large")
        identity = replace(
            identity,
            requested_tier=CODEX_TIER_PRIORITY,
        )
    return identity, body


async def _iter_response_raw(
    response: httpx.Response,
) -> AsyncIterator[bytes]:
    """Yield an httpx response in both real-stream and mock-buffered modes."""

    if response.is_stream_consumed:
        if response.content:
            yield response.content
        return
    async for chunk in response.aiter_raw():
        yield chunk


class CodexActualTierProxy:
    """One loopback service-tier request fence for one app-server."""

    def __init__(
        self,
        route: CodexTierProxyRoute,
        *,
        first_event_timeout: float = _FIRST_EVENT_TIMEOUT,
        http_transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.route = route
        self.first_event_timeout = max(1.0, float(first_event_timeout))
        self._http_transport = http_transport
        self._server: asyncio.AbstractServer | None = None
        self._client: httpx.AsyncClient | None = None
        self._secret = secrets.token_urlsafe(32)
        self._port = 0
        self._closing = False
        self._handlers: set[asyncio.Task[Any]] = set()
        self._committed_writer_ids: set[int] = set()
        self._root_tiers: dict[str, str] = {}
        self._parents: dict[str, str] = {}
        self._active_requests_by_root: dict[str, int] = {}
        self._waiters: dict[tuple[str, str], _ProofWaiter] = {}
        self._proofs: dict[tuple[str, str], CodexTierRequestProof] = {}
        self._failures: dict[tuple[str, str], CodexTierProofError] = {}
        self._proof_order: deque[tuple[str, str]] = deque()
        self._max_proofs = 1024

    @property
    def is_alive(self) -> bool:
        return self._server is not None and not self._closing

    @property
    def local_base_url(self) -> str:
        if not self.is_alive or not self._port:
            raise CodexTierProxyError("Codex tier request proxy is not running")
        return f"http://127.0.0.1:{self._port}/{self._secret}"

    def codex_override_args(self) -> tuple[str, ...]:
        local = json.dumps(self.local_base_url)
        if self.route.built_in_openai:
            # ``openai_base_url`` keeps the built-in provider's WebSocket
            # capability enabled.  Recent Codex versions treat a 426 from
            # that transport as fatal instead of reliably falling back to
            # HTTP, so describe the loopback proof proxy as a separate
            # provider whose transport capability is explicit.
            provider_id = "ccm_actual_tier"
            overrides = [
                f"model_provider={json.dumps(provider_id)}",
                f"model_providers.{provider_id}.name={json.dumps('OpenAI via CCM tier proof')}",
                f"model_providers.{provider_id}.base_url={local}",
                f"model_providers.{provider_id}.wire_api={json.dumps('responses')}",
                f"model_providers.{provider_id}.requires_openai_auth=true",
                f"model_providers.{provider_id}.supports_websockets=false",
            ]
        else:
            overrides = [
                f"model_providers.{provider_id}.base_url={local}"
                for provider_id in (
                    self.route.provider_id,
                    *self.route.provider_aliases,
                )
            ]
        args: list[str] = []
        for override in overrides:
            args.extend(("-c", override))
        args.extend((
            # ChatGPT requests are zstd-compressed by default in 0.144.6.
            # The proxy must inspect exact JSON before any upstream work.
            "--disable",
            "enable_request_compression",
        ))
        return tuple(args)

    async def start(self) -> None:
        if self.is_alive:
            return
        if self._closing:
            raise CodexTierProxyError("Codex tier request proxy is closing")
        self._client = httpx.AsyncClient(
            follow_redirects=False,
            timeout=httpx.Timeout(
                connect=10.0,
                read=None,
                write=30.0,
                pool=10.0,
            ),
            transport=self._http_transport,
            trust_env=False,
        )
        try:
            self._server = await asyncio.start_server(
                self._handle_connection,
                host="127.0.0.1",
                port=0,
                limit=_MAX_HEADER_BYTES,
            )
        except BaseException:
            await self._client.aclose()
            self._client = None
            raise
        sockets = self._server.sockets or ()
        if len(sockets) != 1:
            await self.close()
            raise CodexTierProxyError(
                "Codex tier request proxy did not bind one loopback socket"
            )
        address = sockets[0].getsockname()
        host = ipaddress.ip_address(address[0])
        if not host.is_loopback:
            await self.close()
            raise CodexTierProxyError(
                "Codex tier request proxy did not bind loopback"
            )
        self._port = int(address[1])

    def set_thread_tier(self, thread_id: str, tier: str) -> None:
        thread = _validate_identifier(thread_id, field="thread id")
        requested = _validate_tier(tier)
        if thread in self._parents:
            raise CodexTierProofError(
                f"Cannot assign an independent tier to child thread {thread}"
            )
        current = self._root_tiers.get(thread)
        if (
            current is not None
            and current != requested
            and self._active_requests_by_root.get(thread, 0) > 0
        ):
            raise CodexTierProofError(
                "Cannot change Codex service tier while the thread lineage "
                f"has {self._active_requests_by_root[thread]} active request(s)"
            )
        self._root_tiers[thread] = requested

    def register_thread_parent(
        self,
        thread_id: str,
        parent_thread_id: str,
    ) -> None:
        child = _validate_identifier(thread_id, field="thread id")
        parent = _validate_identifier(
            parent_thread_id,
            field="parent thread id",
        )
        if child == parent:
            raise CodexTierProofError("Codex child thread cannot parent itself")
        if child in self._root_tiers:
            raise CodexTierProofError(
                f"Codex root thread {child} cannot become a child"
            )
        existing = self._parents.get(child)
        if existing is not None and existing != parent:
            raise CodexTierProofError(
                f"Codex child thread {child} changed parent"
            )
        # Resolve without mutating first so a malformed/cyclic notification
        # cannot poison later valid lineage.
        self._resolve_lineage(child, parent)
        self._parents[child] = parent

    def remove_thread(self, thread_id: str) -> None:
        thread = _validate_identifier(thread_id, field="thread id")
        root: str | None = None
        if thread in self._root_tiers:
            root = thread
        elif thread in self._parents:
            try:
                root, _tier = self._resolve_lineage(thread, None)
            except CodexTierProofError:
                root = None
        if (
            root is not None
            and self._active_requests_by_root.get(root, 0) > 0
        ):
            raise CodexTierProofError(
                "Cannot remove Codex tier lineage while requests are active"
            )
        self._root_tiers.pop(thread, None)
        self._parents.pop(thread, None)

    def _resolve_lineage(
        self,
        thread_id: str,
        parent_thread_id: str | None,
    ) -> tuple[str, str]:
        if parent_thread_id is not None:
            existing = self._parents.get(thread_id)
            if existing is not None and existing != parent_thread_id:
                raise CodexTierProofError(
                    f"Codex child thread {thread_id} has ambiguous lineage"
                )

        current = thread_id
        seen: set[str] = set()
        while True:
            if current in seen:
                raise CodexTierProofError(
                    "Codex thread lineage contains a cycle"
                )
            seen.add(current)
            direct = self._root_tiers.get(current)
            if direct is not None:
                effective_parent = (
                    parent_thread_id
                    if current == thread_id and parent_thread_id is not None
                    else self._parents.get(current)
                )
                if effective_parent is not None:
                    raise CodexTierProofError(
                        f"Codex thread {current} is both a root and child"
                    )
                return current, direct
            parent = (
                parent_thread_id
                if current == thread_id and parent_thread_id is not None
                else self._parents.get(current)
            )
            if parent is None:
                break
            current = parent
        raise CodexTierProofError(
            f"No registered service tier for Codex thread {thread_id}"
        )

    def _begin_request(self, identity: _RequestIdentity) -> None:
        root, expected = self._resolve_lineage(
            identity.thread_id,
            identity.parent_thread_id,
        )
        if (
            root != identity.root_thread_id
            or expected != identity.expected_tier
        ):
            raise CodexTierProofError(
                "Codex tier lineage changed during request admission"
            )
        if (
            identity.parent_thread_id is not None
            and identity.thread_id not in self._parents
        ):
            # Commit only after the full request identity and requested tier
            # have been validated. This keeps rejected requests from
            # permanently claiming a child lineage.
            self._parents[identity.thread_id] = identity.parent_thread_id
        self._active_requests_by_root[root] = (
            self._active_requests_by_root.get(root, 0) + 1
        )

    def _end_request(self, identity: _RequestIdentity) -> None:
        root = identity.root_thread_id
        active = self._active_requests_by_root.get(root, 0)
        if active <= 1:
            self._active_requests_by_root.pop(root, None)
        else:
            self._active_requests_by_root[root] = active - 1

    async def wait_for_request_acceptance(
        self,
        thread_id: str,
        turn_id: str,
        expected_tier: str,
        *,
        timeout: float,
    ) -> CodexTierRequestProof:
        key = (
            _validate_identifier(thread_id, field="thread id"),
            _validate_identifier(turn_id, field="turn id"),
        )
        expected = _validate_tier(expected_tier)
        # A later failure for the same native turn supersedes an earlier
        # successful sampling proof; every model request must remain valid.
        failure = self._failures.get(key)
        if failure is not None:
            raise failure
        existing = self._proofs.get(key)
        if existing is not None:
            return existing
        waiter = self._waiters.get(key)
        if waiter is None:
            waiter = _ProofWaiter(
                expected_tier=expected,
                future=asyncio.get_running_loop().create_future(),
            )
            self._waiters[key] = waiter
        elif waiter.expected_tier != expected:
            raise CodexTierProofError(
                "Conflicting service-tier request expectations"
            )
        waiter.users += 1
        try:
            return await asyncio.wait_for(
                asyncio.shield(waiter.future),
                timeout=max(0.1, float(timeout)),
            )
        except asyncio.TimeoutError as exc:
            raise CodexTierProofError(
                f"Timed out waiting for {expected} request acceptance"
            ) from exc
        finally:
            waiter.users = max(0, waiter.users - 1)
            if (
                self._waiters.get(key) is waiter
                and waiter.users == 0
            ):
                self._waiters.pop(key, None)
                if not waiter.future.done():
                    waiter.future.cancel()

    def _publish_proof(self, proof: CodexTierRequestProof) -> None:
        key = (proof.thread_id, proof.turn_id)
        self._proofs[key] = proof
        self._failures.pop(key, None)
        self._remember_proof_key(key)
        waiter = self._waiters.pop(key, None)
        if waiter is not None and not waiter.future.done():
            waiter.future.set_result(proof)

    def _publish_failure(
        self,
        identity: _RequestIdentity | None,
        error: BaseException,
    ) -> None:
        if identity is None:
            return
        if isinstance(error, CodexTierProofError):
            failure = error
        else:
            failure = CodexTierProofError(
                "Actual service-tier proxy failed before proof"
            )
        key = (identity.thread_id, identity.turn_id)
        self._proofs.pop(key, None)
        self._failures[key] = failure
        self._remember_proof_key(key)
        waiter = self._waiters.pop(
            key,
            None,
        )
        if waiter is not None and not waiter.future.done():
            waiter.future.set_exception(failure)

    def _remember_proof_key(self, key: tuple[str, str]) -> None:
        # A turn can issue multiple Responses requests (for example remote
        # compaction followed by sampling).  Keep one recency entry so a later
        # failure can reliably supersede an earlier proof without stale deque
        # entries evicting the current state.
        try:
            self._proof_order.remove(key)
        except ValueError:
            pass
        self._proof_order.append(key)
        while len(self._proof_order) > self._max_proofs:
            stale = self._proof_order.popleft()
            self._proofs.pop(stale, None)
            self._failures.pop(stale, None)

    async def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        server = self._server
        self._server = None
        if server is not None:
            server.close()
            await server.wait_closed()
        current = asyncio.current_task()
        handlers = [
            task for task in self._handlers
            if task is not current and not task.done()
        ]
        for task in handlers:
            task.cancel()
        if handlers:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*handlers, return_exceptions=True),
                    timeout=_SHUTDOWN_TIMEOUT,
                )
            except asyncio.TimeoutError:
                logger.error(
                    "Timed out closing Codex tier request proxy handlers"
                )
        client = self._client
        self._client = None
        if client is not None:
            await client.aclose()
        error = CodexTierProofError("Codex tier request proxy closed")
        for waiter in self._waiters.values():
            if not waiter.future.done():
                waiter.future.set_exception(error)
        self._waiters.clear()
        self._root_tiers.clear()
        self._parents.clear()
        self._active_requests_by_root.clear()
        self._proofs.clear()
        self._failures.clear()
        self._proof_order.clear()

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._handlers.add(task)
        try:
            await self._serve_one(reader, writer)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Do not include headers/body in logs: they may contain auth.
            logger.warning(
                "Codex tier request proxy failed: %s",
                type(exc).__name__,
            )
            if (
                not writer.is_closing()
                and id(writer) not in self._committed_writer_ids
            ):
                await self._send_error(
                    writer,
                    502,
                    "Codex actual service tier could not be verified",
                )
        finally:
            if not writer.is_closing():
                writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass
            self._committed_writer_ids.discard(id(writer))
            if task is not None:
                self._handlers.discard(task)

    async def _serve_one(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        peer = writer.get_extra_info("peername")
        if (
            not isinstance(peer, tuple)
            or not peer
            or not ipaddress.ip_address(peer[0]).is_loopback
        ):
            await self._send_error(writer, 403, "Loopback only")
            return
        request_line = await asyncio.wait_for(
            reader.readline(),
            timeout=_REQUEST_HEADER_TIMEOUT,
        )
        if (
            not request_line
            or len(request_line) > _MAX_HEADER_LINE_BYTES
        ):
            await self._send_error(writer, 400, "Invalid request")
            return
        try:
            method_raw, target_raw, version_raw = request_line.rstrip(
                b"\r\n"
            ).split(b" ", 2)
            method = method_raw.decode("ascii").upper()
            target = target_raw.decode("ascii")
            version = version_raw.decode("ascii")
        except (ValueError, UnicodeDecodeError):
            await self._send_error(writer, 400, "Invalid request")
            return
        if version not in {"HTTP/1.1", "HTTP/1.0"}:
            await self._send_error(writer, 505, "HTTP version unsupported")
            return

        headers: dict[str, str] = {}
        total_headers = len(request_line)
        while True:
            line = await asyncio.wait_for(
                reader.readline(),
                timeout=_REQUEST_HEADER_TIMEOUT,
            )
            total_headers += len(line)
            if (
                not line
                or len(line) > _MAX_HEADER_LINE_BYTES
                or total_headers > _MAX_HEADER_BYTES
            ):
                await self._send_error(writer, 431, "Headers too large")
                return
            if line in {b"\r\n", b"\n"}:
                break
            try:
                name_raw, value_raw = line.rstrip(b"\r\n").split(b":", 1)
                name = name_raw.decode("ascii").strip().lower()
                value = value_raw.decode("latin-1").strip()
            except (ValueError, UnicodeDecodeError):
                await self._send_error(writer, 400, "Invalid header")
                return
            if (
                not name
                or name in headers
                or any(char.isspace() for char in name)
            ):
                await self._send_error(writer, 400, "Invalid header")
                return
            headers[name] = value

        expected_prefix = f"/{self._secret}"
        if not target.startswith(expected_prefix):
            await self._send_error(writer, 404, "Not found")
            return
        relative_target = target[len(expected_prefix):] or "/"
        try:
            parsed_target = urlsplit(relative_target)
        except ValueError:
            await self._send_error(writer, 400, "Invalid target")
            return
        if (
            parsed_target.scheme
            or parsed_target.netloc
            or parsed_target.fragment
        ):
            await self._send_error(writer, 403, "Invalid target")
            return
        relative_path = parsed_target.path
        query = parsed_target.query
        if query:
            if relative_path != "/models":
                await self._send_error(writer, 403, "Query not permitted")
                return
            try:
                query_items = parse_qsl(
                    query,
                    keep_blank_values=True,
                    strict_parsing=True,
                    max_num_fields=4,
                )
            except (ValueError, TypeError):
                await self._send_error(writer, 403, "Invalid model query")
                return
            if (
                len(query_items) != 1
                or query_items[0][0] != "client_version"
                or not query_items[0][1]
                or len(query_items[0][1]) > 64
                or any(
                    not (char.isalnum() or char in ".+-")
                    for char in query_items[0][1]
                )
            ):
                await self._send_error(writer, 403, "Invalid model query")
                return
            query = urlencode(query_items)
        if headers.get("upgrade"):
            # Codex treats 426 as an authoritative signal to use its HTTP SSE
            # fallback; no websocket prompt bytes reach an upstream.
            await self._send_error(
                writer,
                426,
                "HTTP SSE transport required",
                extra_headers={"Upgrade": "HTTP/1.1"},
            )
            return

        if method == "GET" and relative_path in _ALLOWED_CATALOG_PATHS:
            body = b""
            identity = None
            identity_error = None
        elif (
            method == "POST"
            and relative_path in _ALLOWED_MODEL_PATHS
            and not query
        ):
            body = await self._read_body(reader, headers)
            try:
                identity, body = _extract_request_identity(
                    headers,
                    body,
                    self._resolve_lineage,
                )
                identity_error = None
            except _CodexTierRequestMismatch as exc:
                identity = exc.identity
                identity_error = exc
        else:
            await self._send_error(
                writer,
                403,
                "Unsupported Codex upstream endpoint",
            )
            return

        request_active = (
            identity is not None and identity_error is None
        )
        if request_active:
            # No await is allowed between the lineage resolution performed by
            # extraction and this increment. Tier mutation runs on the same
            # event loop, so the root mapping and its active-request fence
            # become one atomic admission step.
            self._begin_request(identity)
        try:
            if identity_error is not None:
                raise identity_error
            client = self._client
            if client is None or self._closing:
                await self._send_error(writer, 503, "Proxy unavailable")
                if identity is not None:
                    raise CodexTierProofError(
                        "Actual service-tier proxy became unavailable"
                    )
                return
            upstream_url = (
                f"{self.route.upstream_base_url.rstrip('/')}"
                f"{relative_path}"
                f"{'?' + query if query else ''}"
            )
            request = client.build_request(
                method,
                upstream_url,
                headers=_filter_request_headers(
                    headers,
                    expect_sse=identity is not None,
                ),
                content=body,
            )
            upstream = await client.send(request, stream=True)
            try:
                if identity is None:
                    await self._forward_unverified_response(writer, upstream)
                    return
                if identity.expected_tier == CODEX_TIER_DEFAULT:
                    # Standard is proven entirely at request admission:
                    # _extract_request_identity rejected priority or unknown
                    # service_tier values before any upstream request. Do not
                    # make ordinary turns depend on optional response metadata
                    # or an upstream-specific SSE event order. Fast remains
                    # fail-closed in _forward_verified_sse below.
                    logger.info(
                        "Codex Standard request tier fenced "
                        "thread=%s turn=%s parent=%s",
                        identity.thread_id,
                        identity.turn_id,
                        identity.parent_thread_id or "-",
                    )
                    await self._forward_stream_response(writer, upstream)
                    return
                await self._forward_verified_sse(writer, upstream, identity)
            finally:
                await upstream.aclose()
        except BaseException as exc:
            self._publish_failure(identity, exc)
            raise
        finally:
            if request_active:
                assert identity is not None
                self._end_request(identity)

    async def _read_body(
        self,
        reader: asyncio.StreamReader,
        headers: dict[str, str],
    ) -> bytes:
        transfer = headers.get("transfer-encoding", "").lower()
        if transfer:
            if transfer != "chunked":
                raise CodexTierProofError("Unsupported transfer encoding")
            return await asyncio.wait_for(
                self._read_chunked_body(reader),
                timeout=_REQUEST_BODY_TIMEOUT,
            )
        length_raw = headers.get("content-length")
        if length_raw is None:
            return b""
        try:
            length = int(length_raw)
        except ValueError as exc:
            raise CodexTierProofError("Invalid Content-Length") from exc
        if length < 0 or length > _MAX_REQUEST_BYTES:
            raise CodexTierProofError("Codex request body is too large")
        return await asyncio.wait_for(
            reader.readexactly(length),
            timeout=_REQUEST_BODY_TIMEOUT,
        )

    async def _read_chunked_body(
        self,
        reader: asyncio.StreamReader,
    ) -> bytes:
        payload = bytearray()
        while True:
            line = await reader.readline()
            if not line or len(line) > _MAX_HEADER_LINE_BYTES:
                raise CodexTierProofError("Invalid chunked request")
            size_text = line.split(b";", 1)[0].strip()
            try:
                size = int(size_text, 16)
            except ValueError as exc:
                raise CodexTierProofError("Invalid chunk size") from exc
            if size < 0 or len(payload) + size > _MAX_REQUEST_BYTES:
                raise CodexTierProofError("Codex request body is too large")
            if size == 0:
                # Consume bounded trailers.
                trailer_bytes = 0
                while True:
                    trailer = await reader.readline()
                    trailer_bytes += len(trailer)
                    if (
                        not trailer
                        or trailer_bytes > _MAX_HEADER_BYTES
                    ):
                        raise CodexTierProofError("Invalid chunk trailer")
                    if trailer in {b"\r\n", b"\n"}:
                        return bytes(payload)
            chunk = await reader.readexactly(size)
            terminator = await reader.readexactly(2)
            if terminator != b"\r\n":
                raise CodexTierProofError("Invalid chunk terminator")
            payload.extend(chunk)

    async def _forward_unverified_response(
        self,
        writer: asyncio.StreamWriter,
        upstream: httpx.Response,
    ) -> None:
        body = bytearray()
        async for chunk in _iter_response_raw(upstream):
            body.extend(chunk)
            if len(body) > _MAX_ERROR_BODY_BYTES:
                raise CodexTierProxyError("Catalog response is too large")
        await self._send_response(
            writer,
            upstream.status_code,
            list(_filter_response_headers(upstream.headers)),
            bytes(body),
        )

    async def _forward_stream_response(
        self,
        writer: asyncio.StreamWriter,
        upstream: httpx.Response,
    ) -> None:
        """Stream a response after all required request-side checks passed."""

        await self._send_stream_headers(
            writer,
            upstream.status_code,
            list(_filter_response_headers(upstream.headers)),
        )
        async for chunk in _iter_response_raw(upstream):
            writer.write(chunk)
            await writer.drain()

    async def _forward_verified_sse(
        self,
        writer: asyncio.StreamWriter,
        upstream: httpx.Response,
        identity: _RequestIdentity,
    ) -> None:
        if upstream.status_code < 200 or upstream.status_code >= 300:
            body = bytearray()
            async for chunk in _iter_response_raw(upstream):
                body.extend(chunk)
                if len(body) > _MAX_ERROR_BODY_BYTES:
                    raise CodexTierProxyError(
                        "Upstream error response is too large"
                    )
            await self._send_response(
                writer,
                upstream.status_code,
                list(_filter_response_headers(upstream.headers)),
                bytes(body),
            )
            raise CodexTierProofError(
                "Upstream rejected the Responses request before tier proof"
            )
        encoding = upstream.headers.get("content-encoding", "identity").lower()
        if encoding not in {"", "identity"}:
            raise CodexTierProofError(
                "Compressed Responses stream cannot be audited"
            )

        iterator = _iter_response_raw(upstream)
        untouched = bytearray()
        parse_buffer = bytearray()
        deadline = asyncio.get_running_loop().time() + self.first_event_timeout
        proof: CodexTierRequestProof | None = None
        while proof is None:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise CodexTierProofError(
                    "Timed out before response.created"
                )
            try:
                chunk = await asyncio.wait_for(
                    anext(iterator),
                    timeout=remaining,
                )
            except StopAsyncIteration as exc:
                raise CodexTierProofError(
                    "Responses stream ended before response.created"
                ) from exc
            untouched.extend(chunk)
            parse_buffer.extend(chunk)
            if len(untouched) > _MAX_FIRST_EVENT_BYTES:
                raise CodexTierProofError(
                    "Responses prelude is too large"
                )
            records, remainder = _split_sse_events(parse_buffer)
            parse_buffer = remainder
            for record in records:
                event = _parse_sse_json(record)
                if event is None:
                    continue
                if event.get("type") != "response.created":
                    raise CodexTierProofError(
                        "First Responses event was not response.created"
                    )
                response = event.get("response")
                if not isinstance(response, dict):
                    raise CodexTierProofError(
                        "response.created omitted response"
                )
                reported_tier = response.get("service_tier")
                normalised_reported_tier = (
                    reported_tier.strip().lower()
                    if isinstance(reported_tier, str)
                    else None
                )
                reported = (
                    normalised_reported_tier
                    if normalised_reported_tier in _CODEX_REPORTED_TIERS
                    else None
                )
                response_id = _validate_identifier(
                    response.get("id"),
                    field="response id",
                )
                proof = CodexTierRequestProof(
                    thread_id=identity.thread_id,
                    turn_id=identity.turn_id,
                    parent_thread_id=identity.parent_thread_id,
                    requested_tier=identity.requested_tier,
                    upstream_reported_tier=reported,
                    response_id=response_id,
                    observed_at=time.time(),
                )
                break

        self._publish_proof(proof)
        if proof.requested_tier == CODEX_TIER_PRIORITY:
            logger.info(
                "Codex Fast priority request accepted "
                "thread=%s turn=%s requested=priority upstream_reported=%s "
                "response=%s parent=%s",
                proof.thread_id,
                proof.turn_id,
                proof.upstream_reported_tier or "unreported",
                proof.response_id,
                proof.parent_thread_id or "-",
            )
        else:
            logger.info(
                "Codex Standard request tier fenced "
                "thread=%s turn=%s upstream_reported=%s response=%s parent=%s",
                proof.thread_id,
                proof.turn_id,
                proof.upstream_reported_tier or "unreported",
                proof.response_id,
                proof.parent_thread_id or "-",
            )
        await self._send_stream_headers(
            writer,
            upstream.status_code,
            _verified_sse_response_headers(upstream.headers),
        )
        writer.write(untouched)
        await writer.drain()
        async for chunk in iterator:
            writer.write(chunk)
            await writer.drain()

    async def _send_stream_headers(
        self,
        writer: asyncio.StreamWriter,
        status: int,
        headers: list[tuple[str, str]],
    ) -> None:
        self._committed_writer_ids.add(id(writer))
        reason = httpx.codes.get_reason_phrase(status) or "Response"
        lines = [f"HTTP/1.1 {status} {reason}\r\n"]
        for name, value in headers:
            lines.append(f"{name}: {value}\r\n")
        lines.extend(["Connection: close\r\n", "\r\n"])
        writer.write("".join(lines).encode("latin-1"))
        await writer.drain()

    async def _send_response(
        self,
        writer: asyncio.StreamWriter,
        status: int,
        headers: list[tuple[str, str]],
        body: bytes,
    ) -> None:
        self._committed_writer_ids.add(id(writer))
        reason = httpx.codes.get_reason_phrase(status) or "Response"
        lines = [f"HTTP/1.1 {status} {reason}\r\n"]
        for name, value in headers:
            if name.lower() not in {"content-length", "connection"}:
                lines.append(f"{name}: {value}\r\n")
        lines.extend([
            f"Content-Length: {len(body)}\r\n",
            "Connection: close\r\n",
            "\r\n",
        ])
        writer.write("".join(lines).encode("latin-1") + body)
        await writer.drain()

    async def _send_error(
        self,
        writer: asyncio.StreamWriter,
        status: int,
        message: str,
        *,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        payload = json.dumps(
            {
                "error": {
                    "type": "ccm_actual_service_tier_unverified",
                    "message": message,
                }
            },
            separators=(",", ":"),
        ).encode("utf-8")
        headers = [("Content-Type", "application/json")]
        headers.extend((extra_headers or {}).items())
        await self._send_response(writer, status, headers, payload)
