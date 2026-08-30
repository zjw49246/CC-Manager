"""Resolve the Manager-local API endpoint used by CCM child processes.

``uvicorn --port`` changes the listening socket without mutating Pydantic
settings.  Remembering the server tuple from an actual ASGI request keeps MCP
servers and hooks on the same Manager even when an operator starts Uvicorn
with CLI-only host/port overrides.
"""

from __future__ import annotations

from threading import Lock
from typing import Any
from urllib.parse import urlsplit


_lock = Lock()
_observed_api_base: str | None = None


def _normalize_api_base(value: str) -> str:
    normalized = value.strip().rstrip("/")
    parsed = urlsplit(normalized)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("Internal API base URL must be an HTTP(S) origin")
    return normalized


def observe_asgi_server(server: Any) -> None:
    """Remember a trustworthy bound server tuple from the ASGI scope."""

    if not isinstance(server, (tuple, list)) or len(server) < 2:
        return
    host, port = server[0], server[1]
    if (
        not isinstance(host, str)
        or type(port) is not int
        or not (1 <= port <= 65535)
    ):
        return
    host = host.strip()
    if not host:
        return
    if host in {"0.0.0.0", "::", "[::]"}:
        host = "127.0.0.1"
    elif ":" in host and not host.startswith("["):
        host = f"[{host}]"
    observed = f"http://{host}:{port}"
    with _lock:
        global _observed_api_base
        _observed_api_base = observed


def resolve_internal_api_base(explicit: str | None = None) -> str:
    """Return explicit/configured/observed/fallback API base in that order."""

    if explicit is not None and explicit.strip():
        return _normalize_api_base(explicit)

    from backend.config import settings

    configured = settings.internal_api_base_url.strip()
    if configured:
        return _normalize_api_base(configured)
    with _lock:
        observed = _observed_api_base
    if observed:
        return observed

    host = (
        settings.host
        if settings.host not in {"0.0.0.0", "::", "[::]"}
        else "127.0.0.1"
    )
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"http://{host}:{settings.port}"
