from __future__ import annotations

import json
from tempfile import SpooledTemporaryFile

import httpx
import pytest
from fastapi import FastAPI, File, UploadFile
from starlette import formparsers
from starlette.formparsers import MultiPartException

from backend.middleware.request_body_limit import (
    DEFAULT_REQUEST_BODY_LIMITS,
    RequestBodyLimitMiddleware,
    SSH_KEY_REQUEST_BODY_MAX_BYTES,
    UPLOAD_REQUEST_BODY_MAX_BYTES,
    VOICE_REQUEST_BODY_MAX_BYTES,
)


def _scope(
    path: str = "/api/uploads",
    *,
    headers: list[tuple[bytes, bytes]] | None = None,
) -> dict:
    return {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("ascii"),
        "query_string": b"",
        "headers": headers or [],
        "client": ("127.0.0.1", 12345),
        "server": ("127.0.0.1", 8000),
    }


async def _invoke(app, scope: dict, messages: list[dict]) -> list[dict]:
    sent: list[dict] = []

    async def receive() -> dict:
        if messages:
            return messages.pop(0)
        return {"type": "http.disconnect"}

    async def send(message: dict) -> None:
        sent.append(message)

    await app(scope, receive, send)
    return sent


def _response(sent: list[dict]) -> tuple[int, dict]:
    assert [item["type"] for item in sent] == [
        "http.response.start",
        "http.response.body",
    ]
    return sent[0]["status"], json.loads(sent[1]["body"])


@pytest.mark.asyncio
async def test_declared_oversized_upload_is_rejected_before_downstream() -> None:
    called = False

    async def downstream(_scope, _receive, _send) -> None:
        nonlocal called
        called = True

    middleware = RequestBodyLimitMiddleware(
        downstream,
        limits={("POST", "/api/uploads"): 8},
    )
    sent = await _invoke(
        middleware,
        _scope(headers=[(b"content-length", b"9")]),
        [],
    )

    assert called is False
    assert _response(sent) == (413, {"detail": "Request body is too large"})


@pytest.mark.asyncio
async def test_streaming_upload_is_cut_off_before_framework_error_response() -> None:
    consumed_chunks = 0

    async def multipart_like_downstream(_scope, receive, send) -> None:
        nonlocal consumed_chunks
        try:
            while True:
                message = await receive()
                consumed_chunks += 1
                if not message.get("more_body", False):
                    break
        except MultiPartException:
            # FastAPI translates the parser exception to 400. The outer byte
            # limiter must replace that framework response with the truthful
            # 413 while suppressing its original body.
            await send({"type": "http.response.start", "status": 400, "headers": []})
            await send(
                {
                    "type": "http.response.body",
                    "body": b"framework parse error",
                    "more_body": False,
                }
            )

    middleware = RequestBodyLimitMiddleware(
        multipart_like_downstream,
        limits={("POST", "/api/uploads"): 8},
    )
    sent = await _invoke(
        middleware,
        _scope(),
        [
            {"type": "http.request", "body": b"12345", "more_body": True},
            {"type": "http.request", "body": b"6789", "more_body": True},
            {"type": "http.request", "body": b"ignored", "more_body": False},
        ],
    )

    assert consumed_chunks == 1
    assert _response(sent) == (413, {"detail": "Request body is too large"})


@pytest.mark.asyncio
async def test_real_fastapi_multipart_stops_before_endpoint_and_closes_spool(
    monkeypatch,
) -> None:
    boundary = "ccm-upload-limit"
    prefix = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="files"; filename="large.txt"\r\n'
        "Content-Type: text/plain\r\n\r\n"
    ).encode("ascii")
    suffix = f"\r\n--{boundary}--\r\n".encode("ascii")
    limit = len(prefix) + 4
    opened_spools = []
    endpoint_called = False
    real_spooled_file = SpooledTemporaryFile

    def tracked_spooled_file(*args, **kwargs):
        opened = real_spooled_file(*args, **kwargs)
        opened_spools.append(opened)
        return opened

    monkeypatch.setattr(formparsers, "SpooledTemporaryFile", tracked_spooled_file)

    app = FastAPI()

    @app.post("/api/uploads")
    async def upload(files: list[UploadFile] = File(...)):
        nonlocal endpoint_called
        endpoint_called = True
        return {"count": len(files)}

    app.add_middleware(
        RequestBodyLimitMiddleware,
        limits={("POST", "/api/uploads"): limit},
    )

    async def body_chunks():
        yield prefix + b"1234"
        yield b"56789"
        yield suffix

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post(
            "/api/uploads",
            headers={"content-type": f"multipart/form-data; boundary={boundary}"},
            content=body_chunks(),
        )

    assert response.status_code == 413
    assert response.json() == {"detail": "Request body is too large"}
    assert endpoint_called is False
    assert opened_spools
    assert all(spool.closed for spool in opened_spools)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "headers",
    [
        [(b"content-length", b"1"), (b"content-length", b"1")],
        [(b"content-length", b"-1")],
        [(b"content-length", b"not-a-number")],
    ],
)
async def test_upload_rejects_ambiguous_content_length(headers) -> None:
    async def downstream(_scope, _receive, _send) -> None:
        raise AssertionError("invalid framing reached downstream")

    middleware = RequestBodyLimitMiddleware(
        downstream,
        limits={("POST", "/api/uploads"): 8},
    )
    status, payload = _response(
        await _invoke(middleware, _scope(headers=headers), [])
    )
    assert status == 400
    assert "Content-Length" in payload["detail"]


@pytest.mark.asyncio
async def test_unlimited_route_is_not_consumed_or_changed() -> None:
    async def downstream(_scope, receive, send) -> None:
        message = await receive()
        assert message["body"] == b"large-but-unrelated"
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    middleware = RequestBodyLimitMiddleware(
        downstream,
        limits={("POST", "/api/uploads"): 1},
    )
    sent = await _invoke(
        middleware,
        _scope("/api/other"),
        [
            {
                "type": "http.request",
                "body": b"large-but-unrelated",
                "more_body": False,
            }
        ],
    )
    assert sent[0]["status"] == 204


def test_production_limits_bound_multipart_overhead_above_payload_limits() -> None:
    assert 50 * 1024 * 1024 < UPLOAD_REQUEST_BODY_MAX_BYTES <= 64 * 1024 * 1024
    assert 1 * 1024 * 1024 < SSH_KEY_REQUEST_BODY_MAX_BYTES <= 4 * 1024 * 1024
    assert 25 * 1024 * 1024 < VOICE_REQUEST_BODY_MAX_BYTES <= 32 * 1024 * 1024
    assert DEFAULT_REQUEST_BODY_LIMITS == {
        ("POST", "/api/uploads"): UPLOAD_REQUEST_BODY_MAX_BYTES,
        ("POST", "/api/files/upload"): UPLOAD_REQUEST_BODY_MAX_BYTES,
        ("POST", "/api/ssh-profiles/upload-key"): SSH_KEY_REQUEST_BODY_MAX_BYTES,
        ("POST", "/api/voice/transcribe"): VOICE_REQUEST_BODY_MAX_BYTES,
    }


@pytest.mark.asyncio
async def test_voice_endpoint_bounds_its_post_parse_read(monkeypatch) -> None:
    from backend.api import voice as voice_api

    requested_sizes: list[int] = []

    class FakeUpload:
        filename = "voice.webm"

        async def read(self, size: int = -1) -> bytes:
            requested_sizes.append(size)
            return b"audio"

    async def fake_transcribe(audio_bytes: bytes, *, filename: str) -> str:
        assert audio_bytes == b"audio"
        assert filename == "voice.webm"
        return "ok"

    monkeypatch.setattr(voice_api.whisper_client, "transcribe", fake_transcribe)

    assert await voice_api.transcribe(FakeUpload()) == {"text": "ok"}
    assert requested_sizes == [voice_api._MAX_AUDIO_SIZE_BYTES + 1]
    assert voice_api._MAX_AUDIO_SIZE_BYTES == 25 * 1024 * 1024


@pytest.mark.asyncio
async def test_voice_endpoint_rejects_limit_plus_one_before_transcription(
    monkeypatch,
) -> None:
    from fastapi import HTTPException

    from backend.api import voice as voice_api

    monkeypatch.setattr(voice_api, "_MAX_AUDIO_SIZE_BYTES", 4)
    transcribed = False

    class FakeUpload:
        filename = "voice.webm"

        async def read(self, size: int = -1) -> bytes:
            assert size == 5
            return b"12345"

    async def fake_transcribe(*_args, **_kwargs):
        nonlocal transcribed
        transcribed = True

    monkeypatch.setattr(voice_api.whisper_client, "transcribe", fake_transcribe)

    with pytest.raises(HTTPException) as caught:
        await voice_api.transcribe(FakeUpload())

    assert caught.value.status_code == 400
    assert transcribed is False
