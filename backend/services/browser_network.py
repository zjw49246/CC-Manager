"""Fail-closed network policy for Browser Review targets.

Every browser target is reached through a small loopback HTTP CONNECT proxy.
External targets resolve every new upstream connection and reject any
non-public answer.  Managed previews pin every connection to one literal
loopback address and port.  This makes the network decision apply to redirects,
subresources, and WebSockets as well as the first page navigation, without
trusting Chromium routing or DNS caches.
"""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal
from urllib.parse import urlsplit, urlunsplit


BrowserNetworkPolicy = Literal["external_public", "managed_preview"]
_BLOCKED_HOSTS = frozenset({"metadata.google.internal"})
_MAX_PROXY_HEADER_BYTES = 64 * 1024


class BrowserNetworkPolicyError(ValueError):
    """A browser destination violates the configured network boundary."""


@dataclass(frozen=True, slots=True)
class ResolvedEndpoint:
    address: str
    family: socket.AddressFamily
    port: int


EndpointResolver = Callable[[str, int], Awaitable[ResolvedEndpoint]]
BlockedCallback = Callable[[str, str], None]


def canonical_target_origin(
    url: str,
    *,
    policy: BrowserNetworkPolicy = "external_public",
) -> str:
    """Validate a browser URL and return its canonical origin.

    Domain resolution for ``external_public`` is deliberately performed by the
    egress proxy at connection time.  Literal IPs are still rejected here so an
    invalid request fails before a browser job is reserved.
    """

    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"}:
        raise BrowserNetworkPolicyError("target URL must use http or https")
    if not parsed.hostname:
        raise BrowserNetworkPolicyError("target URL must include a hostname")
    if parsed.username is not None or parsed.password is not None:
        raise BrowserNetworkPolicyError("target URL must not contain credentials")

    hostname = _canonical_hostname(parsed.hostname)
    try:
        port = parsed.port
    except ValueError as exc:
        raise BrowserNetworkPolicyError("target URL has an invalid port") from exc
    if port is not None and not 1 <= port <= 65535:
        raise BrowserNetworkPolicyError("target URL has an invalid port")

    address = _literal_address(hostname)
    if policy == "managed_preview":
        if parsed.scheme != "http":
            raise BrowserNetworkPolicyError("managed previews must use http")
        if port is None:
            raise BrowserNetworkPolicyError("managed previews require an explicit port")
        if address is None or not address.is_loopback:
            raise BrowserNetworkPolicyError(
                "managed previews must use a literal loopback address"
            )
    elif policy == "external_public":
        if hostname in _BLOCKED_HOSTS or hostname == "localhost" or hostname.endswith(
            ".localhost"
        ):
            raise BrowserNetworkPolicyError("local and cloud metadata hosts are not allowed")
        if address is not None and not _is_public_address(address):
            raise BrowserNetworkPolicyError("external browser targets must use public IP addresses")
    else:  # pragma: no cover - Literal plus callers validate this at construction.
        raise BrowserNetworkPolicyError("unsupported browser network policy")

    default_port = 80 if parsed.scheme == "http" else 443
    host_for_origin = f"[{hostname}]" if ":" in hostname else hostname
    if port is None or port == default_port:
        return f"{parsed.scheme}://{host_for_origin}"
    return f"{parsed.scheme}://{host_for_origin}:{port}"


async def resolve_public_endpoint(hostname: str, port: int) -> ResolvedEndpoint:
    """Resolve one host and pin a connection only when every answer is public."""

    canonical = _canonical_hostname(hostname)
    literal = _literal_address(canonical)
    if literal is not None:
        if not _is_public_address(literal):
            raise BrowserNetworkPolicyError(
                "external browser targets must use public IP addresses"
            )
        family = socket.AF_INET6 if literal.version == 6 else socket.AF_INET
        return ResolvedEndpoint(str(literal), family, port)
    if canonical in _BLOCKED_HOSTS or canonical == "localhost" or canonical.endswith(
        ".localhost"
    ):
        raise BrowserNetworkPolicyError("local and cloud metadata hosts are not allowed")

    loop = asyncio.get_running_loop()
    try:
        answers = await loop.getaddrinfo(
            canonical,
            port,
            family=socket.AF_UNSPEC,
            type=socket.SOCK_STREAM,
            proto=socket.IPPROTO_TCP,
        )
    except socket.gaierror as exc:
        raise BrowserNetworkPolicyError("target hostname could not be resolved") from exc
    if not answers:
        raise BrowserNetworkPolicyError("target hostname returned no addresses")

    endpoints: list[ResolvedEndpoint] = []
    seen: set[tuple[str, int]] = set()
    for family, _socktype, _proto, _canonname, sockaddr in answers:
        if family not in {socket.AF_INET, socket.AF_INET6}:
            continue
        address = ipaddress.ip_address(sockaddr[0])
        if not _is_public_address(address):
            raise BrowserNetworkPolicyError(
                f"target hostname resolved to a non-public address ({address})"
            )
        key = (str(address), family)
        if key not in seen:
            seen.add(key)
            endpoints.append(ResolvedEndpoint(str(address), family, port))
    if not endpoints:
        raise BrowserNetworkPolicyError("target hostname returned no usable addresses")
    return endpoints[0]


class _ValidatingBrowserProxy:
    """Loopback-only proxy with a frozen per-connection network policy."""

    def __init__(
        self,
        *,
        resolver: EndpointResolver,
        network_policy: BrowserNetworkPolicy,
        target_origin: str | None,
        on_blocked: BlockedCallback | None = None,
    ) -> None:
        self._resolver = resolver
        self._network_policy = network_policy
        self._target_origin = target_origin
        self._on_blocked = on_blocked
        self._server: asyncio.AbstractServer | None = None
        self._connections: set[asyncio.Task[None]] = set()
        self.url = ""

    async def __aenter__(self) -> "_ValidatingBrowserProxy":
        self._server = await asyncio.start_server(
            self._accept,
            host="127.0.0.1",
            port=0,
            limit=_MAX_PROXY_HEADER_BYTES,
        )
        sockets = self._server.sockets or []
        if len(sockets) != 1:
            await self.close()
            raise BrowserNetworkPolicyError("browser egress proxy failed to bind safely")
        port = int(sockets[0].getsockname()[1])
        self.url = f"http://127.0.0.1:{port}"
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()

    async def close(self) -> None:
        server = self._server
        self._server = None
        if server is not None:
            server.close()
            await server.wait_closed()
        tasks = list(self._connections)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.create_task(
            self._handle_client(reader, writer),
            name=f"browser-{self._network_policy.replace('_', '-')}-proxy",
        )
        self._connections.add(task)
        task.add_done_callback(self._connections.discard)

    async def _handle_client(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
    ) -> None:
        upstream_writer: asyncio.StreamWriter | None = None
        target_label = "unknown"
        try:
            try:
                header = await client_reader.readuntil(b"\r\n\r\n")
            except (asyncio.IncompleteReadError, asyncio.LimitOverrunError) as exc:
                raise BrowserNetworkPolicyError("proxy request headers are invalid or too large") from exc
            if len(header) > _MAX_PROXY_HEADER_BYTES:
                raise BrowserNetworkPolicyError("proxy request headers are too large")
            lines = header.split(b"\r\n")
            try:
                method_raw, target_raw, version_raw = lines[0].decode("ascii").split(" ", 2)
            except (UnicodeDecodeError, ValueError) as exc:
                raise BrowserNetworkPolicyError("proxy request line is invalid") from exc
            method = method_raw.upper()
            if version_raw not in {"HTTP/1.0", "HTTP/1.1"}:
                raise BrowserNetworkPolicyError("proxy HTTP version is unsupported")

            if method == "CONNECT":
                hostname, port = _parse_authority(target_raw, default_port=443)
                target_label = f"https://{hostname}:{port}"
                endpoint = await self._resolver(hostname, port)
                upstream_reader, upstream_writer = await asyncio.open_connection(
                    endpoint.address,
                    endpoint.port,
                    family=endpoint.family,
                )
                client_writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                await client_writer.drain()
            else:
                parsed = urlsplit(target_raw)
                request_origin = canonical_target_origin(
                    target_raw,
                    policy=self._network_policy,
                )
                if (
                    self._target_origin is not None
                    and request_origin != self._target_origin
                ):
                    raise BrowserNetworkPolicyError(
                        f"managed preview request {request_origin} is outside "
                        f"{self._target_origin}"
                    )
                assert parsed.hostname is not None
                hostname = _canonical_hostname(parsed.hostname)
                port = parsed.port or (443 if parsed.scheme == "https" else 80)
                target_label = target_raw
                if parsed.scheme != "http":
                    raise BrowserNetworkPolicyError(
                        "encrypted proxy requests must use CONNECT"
                    )
                endpoint = await self._resolver(hostname, port)
                upstream_reader, upstream_writer = await asyncio.open_connection(
                    endpoint.address,
                    endpoint.port,
                    family=endpoint.family,
                )
                origin_target = urlunsplit(("", "", parsed.path or "/", parsed.query, ""))
                forwarded = [f"{method_raw} {origin_target} {version_raw}".encode("ascii")]
                for line in lines[1:]:
                    lowered = line.lower()
                    if lowered.startswith(b"proxy-authorization:") or lowered.startswith(
                        b"proxy-connection:"
                    ):
                        continue
                    forwarded.append(line)
                upstream_writer.write(b"\r\n".join(forwarded))
                await upstream_writer.drain()

            await _relay_bidirectionally(
                client_reader,
                client_writer,
                upstream_reader,
                upstream_writer,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            reason = str(exc).strip() or exc.__class__.__name__
            if self._on_blocked is not None:
                self._on_blocked(target_label, reason)
            if not client_writer.is_closing():
                status = b"403 Forbidden" if isinstance(exc, BrowserNetworkPolicyError) else b"502 Bad Gateway"
                try:
                    client_writer.write(
                        b"HTTP/1.1 " + status + b"\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
                    )
                    await client_writer.drain()
                except (ConnectionError, OSError):
                    pass
        finally:
            if upstream_writer is not None:
                upstream_writer.close()
                await _wait_closed(upstream_writer)
            client_writer.close()
            await _wait_closed(client_writer)


class PublicEgressProxy(_ValidatingBrowserProxy):
    """Validating proxy used by one external-public Browser Review."""

    def __init__(
        self,
        *,
        resolver: EndpointResolver = resolve_public_endpoint,
        on_blocked: BlockedCallback | None = None,
    ) -> None:
        super().__init__(
            resolver=resolver,
            network_policy="external_public",
            target_origin=None,
            on_blocked=on_blocked,
        )


class ManagedPreviewProxy(_ValidatingBrowserProxy):
    """Proxy that permits exactly one literal loopback origin."""

    def __init__(
        self,
        target_url: str,
        *,
        on_blocked: BlockedCallback | None = None,
    ) -> None:
        target_origin = canonical_target_origin(
            target_url,
            policy="managed_preview",
        )
        parsed = urlsplit(target_origin)
        assert parsed.hostname is not None and parsed.port is not None
        hostname = _canonical_hostname(parsed.hostname)
        address = _literal_address(hostname)
        assert address is not None and address.is_loopback
        expected_port = parsed.port
        family = socket.AF_INET6 if address.version == 6 else socket.AF_INET

        async def resolve_managed_endpoint(
            requested_hostname: str,
            requested_port: int,
        ) -> ResolvedEndpoint:
            if (
                _canonical_hostname(requested_hostname) != hostname
                or requested_port != expected_port
            ):
                raise BrowserNetworkPolicyError(
                    f"managed preview endpoint {requested_hostname}:{requested_port} "
                    f"is outside {hostname}:{expected_port}"
                )
            return ResolvedEndpoint(str(address), family, expected_port)

        super().__init__(
            resolver=resolve_managed_endpoint,
            network_policy="managed_preview",
            target_origin=target_origin,
            on_blocked=on_blocked,
        )


async def _relay_bidirectionally(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    upstream_reader: asyncio.StreamReader,
    upstream_writer: asyncio.StreamWriter,
) -> None:
    async def copy(source: asyncio.StreamReader, destination: asyncio.StreamWriter) -> None:
        while chunk := await source.read(64 * 1024):
            destination.write(chunk)
            await destination.drain()

    tasks = {
        asyncio.create_task(copy(client_reader, upstream_writer)),
        asyncio.create_task(copy(upstream_reader, client_writer)),
    }
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    await asyncio.gather(*done, *pending, return_exceptions=True)


async def _wait_closed(writer: asyncio.StreamWriter) -> None:
    try:
        await writer.wait_closed()
    except (ConnectionError, OSError):
        pass


def _parse_authority(authority: str, *, default_port: int) -> tuple[str, int]:
    parsed = urlsplit(f"//{authority}")
    if not parsed.hostname or parsed.username is not None or parsed.password is not None:
        raise BrowserNetworkPolicyError("proxy CONNECT authority is invalid")
    try:
        port = parsed.port or default_port
    except ValueError as exc:
        raise BrowserNetworkPolicyError("proxy CONNECT port is invalid") from exc
    if not 1 <= port <= 65535:
        raise BrowserNetworkPolicyError("proxy CONNECT port is invalid")
    return _canonical_hostname(parsed.hostname), port


def _canonical_hostname(hostname: str) -> str:
    value = hostname.rstrip(".").lower()
    if not value or "\x00" in value:
        raise BrowserNetworkPolicyError("target URL has an invalid hostname")
    try:
        return value.encode("idna").decode("ascii")
    except UnicodeError as exc:
        raise BrowserNetworkPolicyError("target URL has an invalid hostname") from exc


def _literal_address(hostname: str) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
    try:
        return ipaddress.ip_address(hostname)
    except ValueError:
        return None


def _is_public_address(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    mapped = getattr(address, "ipv4_mapped", None)
    if mapped is not None:
        return mapped.is_global
    return address.is_global
