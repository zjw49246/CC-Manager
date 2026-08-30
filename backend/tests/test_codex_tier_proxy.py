import asyncio
import gzip
import json
import os
from pathlib import Path

import httpx
import pytest

from backend.services.codex_tier_proxy import (
    CodexActualTierProxy,
    CodexTierProofError,
    CodexTierProxyError,
    CodexTierProxyRoute,
    resolve_native_codex_tier_route,
)


def _request_body(
    *,
    thread_id: str = "thread-1",
    turn_id: str = "turn-1",
    tier: str | None = "priority",
    parent_thread_id: str | None = None,
) -> dict:
    turn_metadata = {
        "thread_id": thread_id,
        "turn_id": turn_id,
    }
    metadata = {
        "thread_id": thread_id,
        "turn_id": turn_id,
        "x-codex-turn-metadata": json.dumps(turn_metadata),
    }
    if parent_thread_id is not None:
        turn_metadata["parent_thread_id"] = parent_thread_id
        metadata["x-codex-parent-thread-id"] = parent_thread_id
        metadata["x-codex-turn-metadata"] = json.dumps(turn_metadata)
    body = {
        "model": "gpt-test",
        "stream": True,
        "input": [],
        "client_metadata": metadata,
    }
    if tier is not None:
        body["service_tier"] = tier
    return body


def _sse(*, tier: str | None, response_id: str = "resp-1") -> bytes:
    response = {"id": response_id}
    if tier is not None:
        response["service_tier"] = tier
    created = json.dumps({
        "type": "response.created",
        "response": response,
    })
    completed = json.dumps({
        "type": "response.completed",
        "response": {"id": response_id},
    })
    return (
        f"event: response.created\ndata: {created}\n\n"
        f"event: response.completed\ndata: {completed}\n\n"
    ).encode()


async def _running_proxy(handler):
    proxy = CodexActualTierProxy(
        CodexTierProxyRoute("https://upstream.example/v1"),
        http_transport=httpx.MockTransport(handler),
    )
    await proxy.start()
    return proxy


@pytest.mark.asyncio
async def test_fast_proxy_requires_request_and_actual_priority():
    seen = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        seen["request"] = request
        seen["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            headers={
                "connection": "keep-alive",
            },
            content=_sse(tier="priority"),
        )

    proxy = await _running_proxy(handler)
    proxy.set_thread_tier("thread-1", "priority")
    try:
        async with httpx.AsyncClient(trust_env=False) as client:
            response = await client.post(
                f"{proxy.local_base_url}/responses",
                headers={
                    "x-client-request-id": "thread-1",
                    "authorization": "Bearer do-not-log",
                    "connection": "keep-alive, x-hop",
                    "x-hop": "drop-me",
                },
                json=_request_body(),
            )
        assert response.status_code == 200
        assert b"response.created" in response.content
        proof = await proxy.wait_for_request_acceptance(
            "thread-1",
            "turn-1",
            "priority",
            timeout=0.2,
        )
        assert proof.upstream_reported_tier == "priority"
        assert proof.response_id == "resp-1"
        assert seen["body"]["service_tier"] == "priority"
        assert seen["request"].headers["authorization"] == "Bearer do-not-log"
        assert "x-hop" not in seen["request"].headers
        assert seen["request"].headers["accept"] == "text/event-stream"
        assert seen["request"].headers["accept-encoding"] == "identity"
        assert response.headers["content-type"] == "text/event-stream"
        assert response.headers["connection"] == "close"
    finally:
        await proxy.close()


@pytest.mark.asyncio
async def test_standard_proxy_accepts_reported_default_and_omitted_request_tier():
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content=_sse(tier="default"),
        )

    proxy = await _running_proxy(handler)
    proxy.set_thread_tier("thread-1", "default")
    try:
        async with httpx.AsyncClient(trust_env=False) as client:
            response = await client.post(
                f"{proxy.local_base_url}/responses",
                headers={"x-client-request-id": "thread-1"},
                json=_request_body(tier=None),
            )
        assert response.status_code == 200
        assert b'"service_tier": "default"' in response.content
    finally:
        await proxy.close()


@pytest.mark.asyncio
async def test_standard_proxy_accepts_unreported_actual_tier():
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content=_sse(tier=None),
        )

    proxy = await _running_proxy(handler)
    proxy.set_thread_tier("thread-1", "default")
    try:
        async with httpx.AsyncClient(trust_env=False) as client:
            response = await client.post(
                f"{proxy.local_base_url}/responses",
                headers={"x-client-request-id": "thread-1"},
                json=_request_body(tier=None),
            )
        assert response.status_code == 200
        assert b"response.created" in response.content
    finally:
        await proxy.close()


@pytest.mark.asyncio
async def test_standard_proxy_does_not_depend_on_responses_sse_prelude():
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content=(
                b"event: response.in_progress\n"
                b'data: {"type":"response.in_progress","response":{}}\n\n'
                b"event: response.completed\n"
                b'data: {"type":"response.completed","response":{}}\n\n'
            ),
        )

    proxy = await _running_proxy(handler)
    proxy.set_thread_tier("thread-1", "default")
    try:
        async with httpx.AsyncClient(trust_env=False) as client:
            response = await client.post(
                f"{proxy.local_base_url}/responses",
                headers={"x-client-request-id": "thread-1"},
                json=_request_body(tier=None),
            )
        assert response.status_code == 200
        assert b"response.in_progress" in response.content
        assert b"response.completed" in response.content
    finally:
        await proxy.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("reported_tier", ["default", "auto", None])
async def test_fast_accepts_informational_response_tier(
    reported_tier: str | None,
):
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=_sse(tier=reported_tier),
        )

    proxy = await _running_proxy(handler)
    proxy.set_thread_tier("thread-1", "priority")
    try:
        async with httpx.AsyncClient(trust_env=False) as client:
            response = await client.post(
                f"{proxy.local_base_url}/responses",
                headers={"x-client-request-id": "thread-1"},
                json=_request_body(),
            )
        assert response.status_code == 200
        assert b"response.created" in response.content
        proof = await proxy.wait_for_request_acceptance(
            "thread-1",
            "turn-1",
            "priority",
            timeout=0.2,
        )
        assert proof.requested_tier == "priority"
        assert proof.upstream_reported_tier == reported_tier
    finally:
        await proxy.close()


@pytest.mark.asyncio
async def test_fast_proxy_rejects_non_sse_success_without_releasing_body():
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=b'{"secret_upstream_error":"not an SSE stream"}',
        )

    proxy = await _running_proxy(handler)
    proxy.set_thread_tier("thread-1", "priority")
    try:
        async with httpx.AsyncClient(trust_env=False) as client:
            response = await client.post(
                f"{proxy.local_base_url}/responses",
                headers={"x-client-request-id": "thread-1"},
                json=_request_body(),
            )
        assert response.status_code == 502
        assert b"secret_upstream_error" not in response.content
        with pytest.raises(
            CodexTierProofError,
            match="ended before response.created",
        ):
            await proxy.wait_for_request_acceptance(
                "thread-1",
                "turn-1",
                "priority",
                timeout=0.2,
            )
    finally:
        await proxy.close()


@pytest.mark.asyncio
async def test_fast_proxy_rejects_compressed_sse_before_releasing_body():
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-encoding": "gzip"},
            content=gzip.compress(_sse(tier="priority")),
        )

    proxy = await _running_proxy(handler)
    proxy.set_thread_tier("thread-1", "priority")
    try:
        async with httpx.AsyncClient(trust_env=False) as client:
            response = await client.post(
                f"{proxy.local_base_url}/responses",
                headers={"x-client-request-id": "thread-1"},
                json=_request_body(),
            )
        assert response.status_code == 502
        assert b"response.created" not in response.content
        with pytest.raises(
            CodexTierProofError,
            match="Compressed Responses stream cannot be audited",
        ):
            await proxy.wait_for_request_acceptance(
                "thread-1",
                "turn-1",
                "priority",
                timeout=0.2,
            )
    finally:
        await proxy.close()


@pytest.mark.asyncio
async def test_upstream_non_success_fails_waiter_without_second_response():
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            headers={"content-type": "application/json"},
            json={"error": {"message": "rate limited"}},
        )

    proxy = await _running_proxy(handler)
    proxy.set_thread_tier("thread-1", "priority")
    waiter = asyncio.create_task(proxy.wait_for_request_acceptance(
        "thread-1",
        "turn-1",
        "priority",
        timeout=1.0,
    ))
    await asyncio.sleep(0)
    try:
        async with httpx.AsyncClient(trust_env=False) as client:
            response = await client.post(
                f"{proxy.local_base_url}/responses",
                headers={"x-client-request-id": "thread-1"},
                json=_request_body(),
            )
        assert response.status_code == 429
        assert response.content.count(b"HTTP/1.1") == 0
        with pytest.raises(CodexTierProofError, match="rejected"):
            await waiter
    finally:
        await proxy.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("request_tier", [None, "default"])
async def test_fast_proxy_repairs_missing_transport_tier(request_tier):
    seen = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, content=_sse(tier="priority"))

    proxy = await _running_proxy(handler)
    proxy.set_thread_tier("thread-1", "priority")
    try:
        waiter = asyncio.create_task(proxy.wait_for_request_acceptance(
            "thread-1",
            "turn-1",
            "priority",
            timeout=1.0,
        ))
        await asyncio.sleep(0)
        async with httpx.AsyncClient(trust_env=False) as client:
            response = await client.post(
                f"{proxy.local_base_url}/responses",
                headers={"x-client-request-id": "thread-1"},
                json=_request_body(tier=request_tier),
            )
        assert response.status_code == 200
        assert seen["body"]["service_tier"] == "priority"
        proof = await waiter
        assert proof.upstream_reported_tier == "priority"
    finally:
        await proxy.close()


@pytest.mark.asyncio
async def test_rejected_child_request_does_not_claim_lineage():
    calls = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500)

    proxy = await _running_proxy(handler)
    proxy.set_thread_tier("root-1", "priority")
    try:
        async with httpx.AsyncClient(trust_env=False) as client:
            response = await client.post(
                f"{proxy.local_base_url}/responses",
                headers={
                    "x-client-request-id": "child-rejected",
                    "x-codex-parent-thread-id": "root-1",
                },
                json=_request_body(
                    thread_id="child-rejected",
                    turn_id="turn-rejected",
                    tier="flex",
                    parent_thread_id="root-1",
                ),
            )
        assert response.status_code == 502
        assert calls == 0
        assert "child-rejected" not in proxy._parents
        assert proxy._active_requests_by_root == {}
    finally:
        await proxy.close()


@pytest.mark.asyncio
async def test_conflicting_identity_metadata_fails_before_upstream():
    calls = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500)

    proxy = await _running_proxy(handler)
    proxy.set_thread_tier("thread-1", "priority")
    body = _request_body()
    body["client_metadata"]["thread_id"] = "different-thread"
    try:
        async with httpx.AsyncClient(trust_env=False) as client:
            response = await client.post(
                f"{proxy.local_base_url}/responses",
                headers={"x-client-request-id": "thread-1"},
                json=body,
            )
        assert response.status_code == 502
        assert calls == 0
        assert proxy._active_requests_by_root == {}
    finally:
        await proxy.close()


@pytest.mark.asyncio
async def test_one_waiter_timeout_does_not_cancel_another_waiter():
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content=_sse(tier="priority"),
        )

    proxy = await _running_proxy(handler)
    proxy.set_thread_tier("thread-1", "priority")
    try:
        short_wait = asyncio.create_task(proxy.wait_for_request_acceptance(
            "thread-1",
            "turn-1",
            "priority",
            timeout=0.1,
        ))
        long_wait = asyncio.create_task(proxy.wait_for_request_acceptance(
            "thread-1",
            "turn-1",
            "priority",
            timeout=1.0,
        ))
        with pytest.raises(CodexTierProofError, match="Timed out"):
            await short_wait
        async with httpx.AsyncClient(trust_env=False) as client:
            response = await client.post(
                f"{proxy.local_base_url}/responses",
                headers={"x-client-request-id": "thread-1"},
                json=_request_body(),
            )
        assert response.status_code == 200
        assert (await long_wait).upstream_reported_tier == "priority"
    finally:
        await proxy.close()


@pytest.mark.asyncio
async def test_later_informational_tier_updates_same_turn_observation():
    calls = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        tier = "priority" if calls == 1 else "default"
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content=_sse(tier=tier, response_id=f"resp-{calls}"),
        )

    proxy = await _running_proxy(handler)
    proxy.set_thread_tier("thread-1", "priority")
    try:
        async with httpx.AsyncClient(trust_env=False) as client:
            first = await client.post(
                f"{proxy.local_base_url}/responses",
                headers={"x-client-request-id": "thread-1"},
                json=_request_body(),
            )
            assert first.status_code == 200
            assert (
                await proxy.wait_for_request_acceptance(
                    "thread-1",
                    "turn-1",
                    "priority",
                    timeout=0.2,
                )
            ).response_id == "resp-1"
            second = await client.post(
                f"{proxy.local_base_url}/responses",
                headers={"x-client-request-id": "thread-1"},
                json=_request_body(),
            )
        assert second.status_code == 200
        proof = await proxy.wait_for_request_acceptance(
            "thread-1",
            "turn-1",
            "priority",
            timeout=0.2,
        )
        assert proof.response_id == "resp-2"
        assert proof.upstream_reported_tier == "default"
    finally:
        await proxy.close()


@pytest.mark.asyncio
async def test_child_thread_inherits_exact_parent_tier():
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content=_sse(tier="priority", response_id="resp-child"),
        )

    proxy = await _running_proxy(handler)
    proxy.set_thread_tier("parent-1", "priority")
    try:
        async with httpx.AsyncClient(trust_env=False) as client:
            response = await client.post(
                f"{proxy.local_base_url}/responses",
                headers={
                    "x-client-request-id": "child-1",
                    "x-codex-parent-thread-id": "parent-1",
                    "x-openai-subagent": "collab_spawn",
                },
                json=_request_body(
                    thread_id="child-1",
                    turn_id="child-turn",
                    parent_thread_id="parent-1",
                ),
            )
        assert response.status_code == 200
        proof = await proxy.wait_for_request_acceptance(
            "child-1",
            "child-turn",
            "priority",
            timeout=0.2,
        )
        assert proof.parent_thread_id == "parent-1"
    finally:
        await proxy.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        "/responses/compact",
        "/memories",
        "/memory",
        "/responses?unverified=1",
    ],
)
async def test_hidden_model_endpoints_fail_closed_before_upstream(path):
    calls = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500)

    proxy = await _running_proxy(handler)
    proxy.set_thread_tier("thread-1", "priority")
    try:
        async with httpx.AsyncClient(trust_env=False) as client:
            response = await client.post(
                f"{proxy.local_base_url}{path}",
                headers={"x-client-request-id": "thread-1"},
                json=_request_body(),
            )
        assert response.status_code in {403, 502}
        assert calls == 0
    finally:
        await proxy.close()


@pytest.mark.asyncio
async def test_responses_without_exact_turn_identity_fail_before_upstream():
    calls = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500)

    proxy = await _running_proxy(handler)
    proxy.set_thread_tier("thread-1", "priority")
    body = _request_body()
    body["client_metadata"].pop("turn_id")
    body["client_metadata"].pop("x-codex-turn-metadata")
    try:
        async with httpx.AsyncClient(trust_env=False) as client:
            response = await client.post(
                f"{proxy.local_base_url}/responses",
                headers={"x-client-request-id": "thread-1"},
                json=body,
            )
        assert response.status_code == 502
        assert calls == 0
    finally:
        await proxy.close()


class _ExplodingSseStream(httpx.AsyncByteStream):
    async def __aiter__(self):
        yield _sse(tier="priority", response_id="resp-partial")
        raise RuntimeError("simulated upstream stream failure")

    async def aclose(self) -> None:
        return None


@pytest.mark.asyncio
async def test_active_child_stream_blocks_root_tier_mutation():
    stream_active = asyncio.Event()
    release_stream = asyncio.Event()

    class BlockingSseStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield _sse(tier="default", response_id="resp-old-standard")
            stream_active.set()
            await release_stream.wait()

        async def aclose(self) -> None:
            return None

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=BlockingSseStream(),
        )

    proxy = await _running_proxy(handler)
    proxy.set_thread_tier("root-standard", "default")
    proxy.register_thread_parent("child-standard", "root-standard")
    try:
        async with httpx.AsyncClient(trust_env=False) as client:
            request = asyncio.create_task(client.post(
                f"{proxy.local_base_url}/responses",
                headers={
                    "x-client-request-id": "child-standard",
                    "x-codex-parent-thread-id": "root-standard",
                    "x-openai-subagent": "collab_spawn",
                },
                json=_request_body(
                    thread_id="child-standard",
                    turn_id="child-old-turn",
                    tier=None,
                    parent_thread_id="root-standard",
                ),
            ))
            await asyncio.wait_for(stream_active.wait(), timeout=1.0)
            # Idempotently restating the same root tier is safe, but neither
            # the root nor a child may be reclassified while any descendant
            # request is still streaming.
            proxy.set_thread_tier("root-standard", "default")
            with pytest.raises(CodexTierProofError, match="active request"):
                proxy.set_thread_tier("root-standard", "priority")
            with pytest.raises(
                CodexTierProofError,
                match="independent tier",
            ):
                proxy.set_thread_tier("child-standard", "priority")
            assert proxy._active_requests_by_root == {"root-standard": 1}
            release_stream.set()
            response = await asyncio.wait_for(request, timeout=1.0)
        assert response.status_code == 200
        assert proxy._active_requests_by_root == {}
        proxy.set_thread_tier("root-standard", "priority")
    finally:
        release_stream.set()
        await proxy.close()


@pytest.mark.asyncio
async def test_stream_failure_after_commit_never_writes_second_http_response():
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=_ExplodingSseStream(),
        )

    proxy = await _running_proxy(handler)
    proxy.set_thread_tier("thread-1", "priority")
    try:
        body = json.dumps(_request_body()).encode()
        path = proxy.local_base_url.split(
            f"http://127.0.0.1:{proxy._port}",
            1,
        )[1]
        reader, writer = await asyncio.open_connection(
            "127.0.0.1",
            proxy._port,
        )
        writer.write((
            f"POST {path}/responses HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "x-client-request-id: thread-1\r\n"
            "Content-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n\r\n"
        ).encode() + body)
        await writer.drain()
        response = await reader.read()
        writer.close()
        await writer.wait_closed()
        assert response.startswith(b"HTTP/1.1 200")
        assert response.count(b"HTTP/1.1") == 1
        assert b"response.created" in response
        assert b"502" not in response
        with pytest.raises(CodexTierProofError, match="proxy failed"):
            await proxy.wait_for_request_acceptance(
                "thread-1",
                "turn-1",
                "priority",
                timeout=0.2,
            )
    finally:
        await proxy.close()


@pytest.mark.asyncio
async def test_unknown_child_lineage_fails_before_upstream():
    calls = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500)

    proxy = await _running_proxy(handler)
    try:
        async with httpx.AsyncClient(trust_env=False) as client:
            response = await client.post(
                f"{proxy.local_base_url}/responses",
                headers={
                    "x-client-request-id": "child-1",
                    "x-codex-parent-thread-id": "unknown-parent",
                    "x-openai-subagent": "collab_spawn",
                },
                json=_request_body(
                    thread_id="child-1",
                    turn_id="child-turn",
                    parent_thread_id="unknown-parent",
                ),
            )
        assert response.status_code == 502
        assert calls == 0
    finally:
        await proxy.close()


@pytest.mark.asyncio
async def test_websocket_upgrade_gets_426_without_upstream():
    calls = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500)

    proxy = await _running_proxy(handler)
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", proxy._port)
        path = proxy.local_base_url.split("127.0.0.1:" + str(proxy._port), 1)[1]
        writer.write((
            f"GET {path}/responses HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: Upgrade\r\n"
            "Upgrade: websocket\r\n\r\n"
        ).encode())
        await writer.drain()
        response = await reader.read()
        writer.close()
        await writer.wait_closed()
        assert response.startswith(b"HTTP/1.1 426")
        assert calls == 0
    finally:
        await proxy.close()


@pytest.mark.asyncio
async def test_catalog_query_is_bounded_and_redirect_is_not_followed():
    seen = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(
            307,
            headers={"location": "https://attacker.example/models"},
        )

    proxy = await _running_proxy(handler)
    try:
        async with httpx.AsyncClient(
            trust_env=False,
            follow_redirects=False,
        ) as client:
            response = await client.get(
                f"{proxy.local_base_url}/models?client_version=0.144.6"
            )
        assert response.status_code == 307
        assert seen == ["https://upstream.example/v1/models?client_version=0.144.6"]
    finally:
        await proxy.close()


def _write(path: Path, content: str, mode: int = 0o600) -> None:
    path.write_text(content, encoding="utf-8")
    os.chmod(path, mode)


def test_native_route_resolves_chatgpt_without_exposing_tokens(tmp_path):
    _write(tmp_path / "config.toml", "")
    _write(
        tmp_path / "auth.json",
        json.dumps({
            "auth_mode": "chatgpt",
            "tokens": {"access_token": "do-not-copy"},
        }),
    )
    route = resolve_native_codex_tier_route(tmp_path, environ={})
    assert route.upstream_base_url == "https://chatgpt.com/backend-api/codex"
    assert "do-not-copy" not in repr(route)


def test_native_route_honors_chatgpt_base_url(tmp_path):
    _write(
        tmp_path / "config.toml",
        'chatgpt_base_url = "https://chatgpt.example/backend-api/"\n',
    )
    _write(
        tmp_path / "auth.json",
        json.dumps({"auth_mode": "chatgpt", "tokens": {}}),
    )
    route = resolve_native_codex_tier_route(tmp_path, environ={})
    assert route.upstream_base_url == "https://chatgpt.example/backend-api/codex"


def test_native_route_api_key_and_explicit_openai_base(tmp_path):
    _write(tmp_path / "config.toml", "")
    _write(
        tmp_path / "auth.json",
        json.dumps({"OPENAI_API_KEY": "do-not-copy"}),
    )
    route = resolve_native_codex_tier_route(tmp_path, environ={})
    assert route.upstream_base_url == "https://api.openai.com/v1"

    _write(
        tmp_path / "config.toml",
        'openai_base_url = "https://residency.example/v1"\n',
    )
    route = resolve_native_codex_tier_route(tmp_path, environ={})
    assert route.upstream_base_url == "https://residency.example/v1"


def test_native_route_environment_auth_precedence(tmp_path):
    _write(tmp_path / "config.toml", "")
    _write(
        tmp_path / "auth.json",
        json.dumps({"OPENAI_API_KEY": "stale-file-key"}),
    )
    route = resolve_native_codex_tier_route(
        tmp_path,
        environ={"CODEX_ACCESS_TOKEN": "ephemeral-chatgpt-token"},
    )
    assert route.upstream_base_url == "https://chatgpt.com/backend-api/codex"

    route = resolve_native_codex_tier_route(
        tmp_path,
        environ={
            "CODEX_API_KEY": "higher-precedence-api-key",
            "CODEX_ACCESS_TOKEN": "ephemeral-chatgpt-token",
        },
    )
    assert route.upstream_base_url == "https://api.openai.com/v1"


def test_native_route_rejects_unknown_custom_provider(tmp_path):
    _write(tmp_path / "config.toml", 'model_provider = "custom"\n')
    with pytest.raises(CodexTierProxyError, match="custom provider"):
        resolve_native_codex_tier_route(tmp_path, environ={})


def test_override_args_are_non_persistent_and_disable_compression():
    proxy = CodexActualTierProxy(
        CodexTierProxyRoute(
            "https://gateway.example/v1",
            provider_id="cloudrouter",
            built_in_openai=False,
        ),
    )
    proxy._server = object()  # type: ignore[assignment]
    proxy._port = 4321
    args = proxy.codex_override_args()
    assert args[:2] == (
        "-c",
        (
            "model_providers.cloudrouter.base_url="
            + json.dumps(proxy.local_base_url)
        ),
    )
    assert args[2:] == ("--disable", "enable_request_compression")


def test_native_override_uses_http_only_authenticated_provider():
    proxy = CodexActualTierProxy(
        CodexTierProxyRoute(
            "https://chatgpt.com/backend-api/codex",
            provider_id="openai",
            built_in_openai=True,
        ),
    )
    proxy._server = object()  # type: ignore[assignment]
    proxy._port = 4321

    args = proxy.codex_override_args()

    assert "model_provider=\"ccm_actual_tier\"" in args
    assert (
        "model_providers.ccm_actual_tier.base_url="
        + json.dumps(proxy.local_base_url)
    ) in args
    assert "model_providers.ccm_actual_tier.wire_api=\"responses\"" in args
    assert "model_providers.ccm_actual_tier.requires_openai_auth=true" in args
    assert "model_providers.ccm_actual_tier.supports_websockets=false" in args
    assert args[-2:] == ("--disable", "enable_request_compression")
