"""Minimal HTTPS CONNECT proxy used by untrusted Harness setup containers.

The source container is attached only to a Docker ``--internal`` network.  A
separate copy of this manager-owned program is the sole member with outbound
network access.  Every CONNECT re-resolves the requested hostname and rejects
the whole answer set if any address is not globally routable.

This file intentionally uses only the Python standard library because it is
copied verbatim into the small Harness image and executed there.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
import ipaddress
import json
import os
import re
import ssl
from urllib.parse import quote


_HOST_RE = re.compile(
    r"(?=.{1,253}\Z)(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)*"
    r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\Z"
)
_MAX_HEADER_BYTES = 64 * 1024
_MAX_DOH_BODY_BYTES = 64 * 1024
_DOH_HOST = "cloudflare-dns.com"
_DOH_ENDPOINTS = ("1.1.1.1", "1.0.0.1")


class EgressPolicyError(ValueError):
    pass


def normalize_allowed_hosts(raw: str) -> frozenset[str]:
    if not isinstance(raw, str) or len(raw) > 8192:
        raise EgressPolicyError("egress host allowlist is invalid")
    hosts: set[str] = set()
    for item in raw.split(","):
        host = item.strip().lower().rstrip(".")
        if not host:
            continue
        if _HOST_RE.fullmatch(host) is None:
            raise EgressPolicyError("egress host allowlist contains an invalid host")
        try:
            ipaddress.ip_address(host)
        except ValueError:
            pass
        else:
            raise EgressPolicyError("egress host allowlist cannot contain IP literals")
        hosts.add(host)
    if not hosts or len(hosts) > 64:
        raise EgressPolicyError("egress host allowlist must contain 1 to 64 hosts")
    return frozenset(hosts)


def require_public_addresses(addresses: list[str]) -> tuple[str, ...]:
    if not addresses:
        raise EgressPolicyError("egress DNS returned no addresses")
    normalized: list[str] = []
    for value in addresses:
        try:
            address = ipaddress.ip_address(value)
        except ValueError as exc:
            raise EgressPolicyError("egress DNS returned an invalid address") from exc
        if (
            not address.is_global
            or address.is_multicast
            or address.is_reserved
            or address.is_unspecified
            or address.is_loopback
            or address.is_link_local
            or address.is_private
        ):
            raise EgressPolicyError("egress DNS returned a non-public address")
        normalized.append(address.compressed)
    return tuple(dict.fromkeys(normalized))


def _parse_doh_payload(
    payload: bytes,
    *,
    host: str,
    record_type: int,
) -> list[str]:
    try:
        document = json.loads(payload.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise EgressPolicyError("egress DoH response is malformed") from exc
    if not isinstance(document, dict) or document.get("Status") != 0:
        raise EgressPolicyError("egress DoH lookup failed")
    question = document.get("Question")
    if not isinstance(question, list) or len(question) != 1:
        raise EgressPolicyError("egress DoH question is malformed")
    expected_name = host.lower().rstrip(".")
    asked = question[0]
    if (
        not isinstance(asked, dict)
        or not isinstance(asked.get("name"), str)
        or asked["name"].lower().rstrip(".") != expected_name
        or asked.get("type") != record_type
    ):
        raise EgressPolicyError("egress DoH question does not match the request")
    answers = document.get("Answer", [])
    if not isinstance(answers, list) or len(answers) > 64:
        raise EgressPolicyError("egress DoH answer is malformed")
    records: list[tuple[str, int, str]] = []
    for answer in answers:
        if not isinstance(answer, dict):
            raise EgressPolicyError("egress DoH answer is malformed")
        answer_name = answer.get("name")
        answer_type = answer.get("type")
        data = answer.get("data")
        if answer_type not in {1, 5, 28}:
            continue
        if not isinstance(answer_name, str) or not isinstance(data, str):
            raise EgressPolicyError("egress DoH answer is malformed")
        normalized_name = answer_name.lower().rstrip(".")
        if _HOST_RE.fullmatch(normalized_name) is None:
            raise EgressPolicyError("egress DoH answer name is invalid")
        if answer_type == 5:
            normalized_data = data.lower().rstrip(".")
            if _HOST_RE.fullmatch(normalized_data) is None:
                raise EgressPolicyError("egress DoH CNAME is invalid")
            data = normalized_data
        records.append((normalized_name, answer_type, data))

    reachable_names = {expected_name}
    for _ in range(len(records)):
        additions = {
            data
            for name, answer_type, data in records
            if answer_type == 5 and name in reachable_names
        }
        if additions.issubset(reachable_names):
            break
        reachable_names.update(additions)
    addresses = [
        data
        for name, answer_type, data in records
        if answer_type == record_type and name in reachable_names
    ]
    if any(
        answer_type == record_type and name not in reachable_names
        for name, answer_type, _ in records
    ):
        raise EgressPolicyError("egress DoH answer is unrelated to the request")
    return addresses


async def _doh_query(host: str, *, record_name: str, record_type: int) -> list[str]:
    path = f"/dns-query?name={quote(host, safe='')}&type={record_name}"
    request = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {_DOH_HOST}\r\n"
        "Accept: application/dns-json\r\n"
        "Connection: close\r\n\r\n"
    ).encode("ascii")
    context = ssl.create_default_context()
    last_error: BaseException | None = None
    for endpoint in _DOH_ENDPOINTS:
        writer: asyncio.StreamWriter | None = None
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(
                    endpoint,
                    443,
                    ssl=context,
                    server_hostname=_DOH_HOST,
                    limit=_MAX_HEADER_BYTES,
                ),
                timeout=10,
            )
            writer.write(request)
            await asyncio.wait_for(writer.drain(), timeout=10)
            header = await asyncio.wait_for(
                reader.readuntil(b"\r\n\r\n"),
                timeout=10,
            )
            if len(header) > _MAX_HEADER_BYTES:
                raise EgressPolicyError("egress DoH header is too large")
            lines = header.decode("ascii", errors="strict").split("\r\n")
            if lines[0] not in {"HTTP/1.1 200 OK", "HTTP/1.0 200 OK"}:
                raise EgressPolicyError("egress DoH returned a non-success status")
            headers: dict[str, str] = {}
            for line in lines[1:]:
                if not line:
                    continue
                if ":" not in line:
                    raise EgressPolicyError("egress DoH header is malformed")
                name, value = line.split(":", 1)
                normalized_name = name.strip().lower()
                if normalized_name in headers:
                    raise EgressPolicyError("egress DoH header is ambiguous")
                headers[normalized_name] = value.strip()
            if headers.get("content-type", "").split(";", 1)[0].strip().lower() != (
                "application/dns-json"
            ):
                raise EgressPolicyError("egress DoH content type is invalid")
            raw_length = headers.get("content-length")
            if raw_length is None or not raw_length.isdigit():
                raise EgressPolicyError("egress DoH content length is invalid")
            length = int(raw_length)
            if not 1 <= length <= _MAX_DOH_BODY_BYTES:
                raise EgressPolicyError("egress DoH body is too large")
            payload = await asyncio.wait_for(reader.readexactly(length), timeout=10)
            return _parse_doh_payload(
                payload,
                host=host,
                record_type=record_type,
            )
        except (
            asyncio.IncompleteReadError,
            asyncio.TimeoutError,
            OSError,
            UnicodeError,
            EgressPolicyError,
        ) as exc:
            last_error = exc
        finally:
            if writer is not None and not writer.is_closing():
                writer.close()
                with suppress(Exception):
                    await writer.wait_closed()
    raise EgressPolicyError("egress DoH lookup failed") from last_error


async def resolve_public_host(host: str, port: int) -> tuple[str, ...]:
    if port != 443 or _HOST_RE.fullmatch(host) is None:
        raise EgressPolicyError("egress DNS request is invalid")
    ipv4, ipv6 = await asyncio.gather(
        _doh_query(host, record_name="A", record_type=1),
        _doh_query(host, record_name="AAAA", record_type=28),
    )
    return require_public_addresses([*ipv4, *ipv6])


async def _copy_limited(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *,
    max_bytes: int,
) -> None:
    total = 0
    while True:
        chunk = await reader.read(64 * 1024)
        if not chunk:
            return
        total += len(chunk)
        if total > max_bytes:
            raise EgressPolicyError("egress connection exceeded its byte limit")
        writer.write(chunk)
        await writer.drain()


async def _reject(writer: asyncio.StreamWriter, status: str) -> None:
    writer.write(
        f"HTTP/1.1 {status}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n".encode(
            "ascii"
        )
    )
    try:
        await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


class ConnectProxy:
    def __init__(
        self,
        *,
        allowed_hosts: frozenset[str],
        max_bytes: int,
        connection_timeout: float,
    ) -> None:
        self.allowed_hosts = allowed_hosts
        self.max_bytes = max_bytes
        self.connection_timeout = connection_timeout

    async def handle(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        upstream_writer: asyncio.StreamWriter | None = None
        pumps: set[asyncio.Task[None]] = set()
        try:
            header = await asyncio.wait_for(
                reader.readuntil(b"\r\n\r\n"),
                timeout=10,
            )
            if len(header) > _MAX_HEADER_BYTES:
                raise EgressPolicyError("proxy request header is too large")
            lines = header.decode("ascii", errors="strict").split("\r\n")
            request = lines[0].split(" ")
            if len(request) != 3 or request[0] != "CONNECT":
                await _reject(writer, "405 Method Not Allowed")
                return
            authority = request[1]
            if authority.count(":") != 1:
                raise EgressPolicyError("CONNECT authority is invalid")
            host, raw_port = authority.rsplit(":", 1)
            host = host.lower().rstrip(".")
            if (
                _HOST_RE.fullmatch(host) is None
                or host not in self.allowed_hosts
                or raw_port != "443"
            ):
                await _reject(writer, "403 Forbidden")
                return
            addresses = await resolve_public_host(host, 443)
            upstream_reader: asyncio.StreamReader | None = None
            last_error: OSError | None = None
            for address in addresses:
                try:
                    upstream_reader, upstream_writer = await asyncio.wait_for(
                        asyncio.open_connection(address, 443),
                        timeout=10,
                    )
                    break
                except OSError as exc:
                    last_error = exc
            if upstream_reader is None or upstream_writer is None:
                raise EgressPolicyError(
                    "could not connect to the approved public host"
                ) from last_error
            writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            await writer.drain()
            downstream = asyncio.create_task(
                _copy_limited(reader, upstream_writer, max_bytes=self.max_bytes)
            )
            upstream = asyncio.create_task(
                _copy_limited(upstream_reader, writer, max_bytes=self.max_bytes)
            )
            pumps = {downstream, upstream}
            done, pending = await asyncio.wait(
                pumps,
                timeout=self.connection_timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            for task in done:
                task.result()
        except (asyncio.IncompleteReadError, UnicodeError, EgressPolicyError):
            if not writer.is_closing():
                await _reject(writer, "403 Forbidden")
        except (asyncio.TimeoutError, OSError):
            if not writer.is_closing():
                await _reject(writer, "502 Bad Gateway")
        finally:
            for task in pumps:
                if not task.done():
                    task.cancel()
            if pumps:
                await asyncio.gather(*pumps, return_exceptions=True)
            if upstream_writer is not None and not upstream_writer.is_closing():
                upstream_writer.close()
                with suppress(Exception):
                    await upstream_writer.wait_closed()
            if not writer.is_closing():
                writer.close()
                with suppress(Exception):
                    await writer.wait_closed()


async def main() -> None:
    allowed = normalize_allowed_hosts(os.environ.get("CCM_ALLOWED_HOSTS", ""))
    try:
        max_bytes = int(os.environ.get("CCM_PROXY_MAX_BYTES", str(1024**3)))
        timeout = float(os.environ.get("CCM_PROXY_TIMEOUT_SECONDS", "1200"))
    except ValueError as exc:
        raise EgressPolicyError("egress proxy limits are invalid") from exc
    if not 1024 <= max_bytes <= 4 * 1024**3 or not 10 <= timeout <= 3600:
        raise EgressPolicyError("egress proxy limits are out of range")
    proxy = ConnectProxy(
        allowed_hosts=allowed,
        max_bytes=max_bytes,
        connection_timeout=timeout,
    )
    server = await asyncio.start_server(
        proxy.handle,
        host="0.0.0.0",
        port=3128,
        limit=_MAX_HEADER_BYTES,
    )
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
