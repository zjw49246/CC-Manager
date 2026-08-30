"""Early request-body limits for multipart upload routes.

FastAPI resolves ``UploadFile`` parameters only after Starlette has parsed the
entire multipart body.  The endpoint-level payload checks therefore cannot
protect the host temporary directory from an oversized upload.  This ASGI
middleware counts bytes at the receive boundary, before the multipart parser
can spool an unbounded file to disk.
"""

from __future__ import annotations

import json
from collections.abc import Mapping

from starlette.formparsers import MultiPartException
from starlette.types import ASGIApp, Message, Receive, Scope, Send


# Leave bounded room for multipart headers and boundaries above the business
# payload limits (50 MiB for ordinary uploads and 1 MiB for SSH private keys).
UPLOAD_REQUEST_BODY_MAX_BYTES = 52 * 1024 * 1024
SSH_KEY_REQUEST_BODY_MAX_BYTES = 2 * 1024 * 1024
VOICE_REQUEST_BODY_MAX_BYTES = 27 * 1024 * 1024

DEFAULT_REQUEST_BODY_LIMITS: dict[tuple[str, str], int] = {
    ("POST", "/api/uploads"): UPLOAD_REQUEST_BODY_MAX_BYTES,
    ("POST", "/api/files/upload"): UPLOAD_REQUEST_BODY_MAX_BYTES,
    ("POST", "/api/ssh-profiles/upload-key"): SSH_KEY_REQUEST_BODY_MAX_BYTES,
    ("POST", "/api/voice/transcribe"): VOICE_REQUEST_BODY_MAX_BYTES,
}


def _json_response(status: int, detail: str) -> tuple[Message, Message]:
    body = json.dumps(
        {"detail": detail},
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return (
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode("ascii")),
            ],
        },
        {"type": "http.response.body", "body": body, "more_body": False},
    )


async def _send_json(send: Send, status: int, detail: str) -> None:
    start, body = _json_response(status, detail)
    await send(start)
    await send(body)


class RequestBodyLimitMiddleware:
    """Bound selected HTTP request bodies before framework form parsing."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        limits: Mapping[tuple[str, str], int] | None = None,
    ) -> None:
        self.app = app
        configured = DEFAULT_REQUEST_BODY_LIMITS if limits is None else limits
        self._limits = {
            (method.upper(), path): int(limit)
            for (method, path), limit in configured.items()
            if int(limit) > 0
        }

    @staticmethod
    def _declared_content_length(scope: Scope) -> tuple[int | None, str | None]:
        values = [
            value
            for name, value in scope.get("headers", [])
            if name.lower() == b"content-length"
        ]
        if not values:
            return None, None
        if len(values) != 1:
            return None, "Request must contain at most one Content-Length header"
        try:
            text = values[0].decode("ascii")
            if not text or not text.isdigit():
                raise ValueError
            length = int(text)
        except (UnicodeDecodeError, ValueError):
            return None, "Invalid Content-Length header"
        return length, None

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        limit = self._limits.get(
            (str(scope.get("method", "")).upper(), str(scope.get("path", "")))
        )
        if limit is None:
            await self.app(scope, receive, send)
            return

        declared_length, framing_error = self._declared_content_length(scope)
        if framing_error is not None:
            await _send_json(send, 400, framing_error)
            return
        if declared_length is not None and declared_length > limit:
            await _send_json(send, 413, "Request body is too large")
            return

        received = 0
        exceeded = False
        replacement_sent = False
        response_started = False

        async def limited_receive() -> Message:
            nonlocal received, exceeded
            message = await receive()
            if message.get("type") == "http.request":
                body = message.get("body", b"")
                received += len(body) if isinstance(body, bytes) else 0
                if received > limit:
                    exceeded = True
                    # MultiPartParser catches this exact exception and closes
                    # every SpooledTemporaryFile it has already opened.
                    raise MultiPartException("Request body is too large")
            return message

        async def limited_send(message: Message) -> None:
            nonlocal replacement_sent, response_started
            if exceeded:
                if not replacement_sent and not response_started:
                    replacement_sent = True
                    await _send_json(send, 413, "Request body is too large")
                return
            if message.get("type") == "http.response.start":
                response_started = True
            await send(message)

        try:
            await self.app(scope, limited_receive, limited_send)
        except MultiPartException:
            if exceeded and not response_started:
                if not replacement_sent:
                    await _send_json(send, 413, "Request body is too large")
                return
            raise
