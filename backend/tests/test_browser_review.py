"""Focused tests for the standalone Computer Use browser review harness."""

from __future__ import annotations

import asyncio
import base64
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

import httpx
import pytest
import websockets

from backend.services import browser_review
from backend.services.browser_review import (
    ActionBlockedError,
    BrowserReviewError,
    BrowserReviewOptions,
    BrowserTelemetry,
    ResponsesClient,
    build_followup_payload,
    build_initial_payload,
    execute_computer_actions,
    extract_computer_call,
    extract_output_text,
    normalize_button,
    normalize_drag_path,
    normalize_key,
    run_browser_review,
    validate_target_url,
)
from backend.services.browser_network import (
    BrowserNetworkPolicyError,
    ManagedPreviewProxy,
    PublicEgressProxy,
    resolve_public_endpoint,
)


@pytest.mark.asyncio
async def test_finish_review_mcp_schema_is_canonical_and_constrained():
    from backend.mcp.ccm_browser_review_server import mcp

    tools = await mcp.list_tools()
    schema = next(tool.inputSchema for tool in tools if tool.name == "finish_review")

    assert schema["properties"]["verdict"]["enum"] == [
        "passed",
        "failed",
        "inconclusive",
    ]
    finding = schema["$defs"]["BrowserReviewFindingInput"]
    assert set(finding["properties"]) == {
        "scenario_id",
        "severity",
        "category",
        "title",
        "route",
        "locator",
        "expected",
        "actual",
        "reproduction",
        "evidence",
        "confidence",
    }
    assert "JSON array" in finding["properties"]["reproduction"]["description"]
    assert "Numeric confidence" in finding["properties"]["confidence"]["description"]


@pytest.mark.parametrize(
    ("url", "origin"),
    [
        ("https://Example.COM/path", "https://example.com"),
        ("https://例子.测试/path", "https://xn--fsqu00a.xn--0zwm56d"),
        ("https://example.com:8443", "https://example.com:8443"),
    ],
)
def test_validate_target_url_returns_canonical_origin(url: str, origin: str):
    assert validate_target_url(url) == origin


@pytest.mark.parametrize(
    "url",
    [
        "file:///tmp/page.html",
        "https://user:secret@example.com",
        "http://0.0.0.0:8000",
        "http://127.0.0.1:8000",
        "http://10.0.0.2:8000",
        "http://[::1]:8000",
        "http://169.254.169.254/latest/meta-data",
        "http://metadata.google.internal",
        "http://example.com:99999",
    ],
)
def test_validate_target_url_rejects_unsafe_targets(url: str):
    with pytest.raises(ValueError):
        validate_target_url(url)


@pytest.mark.parametrize(
    ("url", "origin"),
    [
        ("http://127.0.0.1:5173/tasks?x=1", "http://127.0.0.1:5173"),
        ("http://[::1]:8000/", "http://[::1]:8000"),
    ],
)
def test_managed_preview_policy_allows_only_explicit_loopback(url: str, origin: str):
    assert validate_target_url(url, network_policy="managed_preview") == origin


def test_managed_preview_rejects_hostname_aliases():
    with pytest.raises(ValueError, match="literal loopback"):
        validate_target_url(
            "http://localhost:8000/",
            network_policy="managed_preview",
        )


@pytest.mark.asyncio
async def test_managed_preview_proxy_allows_only_frozen_endpoint():
    allowed_hits = 0
    blocked_hits = 0

    async def allowed_handler(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        nonlocal allowed_hits
        allowed_hits += 1
        await reader.readuntil(b"\r\n\r\n")
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    async def blocked_handler(
        _reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        nonlocal blocked_hits
        blocked_hits += 1
        writer.close()
        await writer.wait_closed()

    allowed_server = await asyncio.start_server(allowed_handler, "127.0.0.1", 0)
    blocked_server = await asyncio.start_server(blocked_handler, "127.0.0.1", 0)
    allowed_port = int(allowed_server.sockets[0].getsockname()[1])
    blocked_port = int(blocked_server.sockets[0].getsockname()[1])
    blocked_events: list[tuple[str, str]] = []
    try:
        async with ManagedPreviewProxy(
            f"http://127.0.0.1:{allowed_port}",
            on_blocked=lambda target, reason: blocked_events.append((target, reason)),
        ) as proxy:
            async with httpx.AsyncClient(proxy=proxy.url, trust_env=False) as client:
                allowed = await client.get(f"http://127.0.0.1:{allowed_port}/ok")
                denied = await client.get(f"http://127.0.0.1:{blocked_port}/secret")
        assert allowed.text == "ok"
        assert denied.status_code == 403
        assert allowed_hits == 1
        assert blocked_hits == 0
        assert blocked_events and "outside" in blocked_events[-1][1]
    finally:
        allowed_server.close()
        blocked_server.close()
        await allowed_server.wait_closed()
        await blocked_server.wait_closed()


@pytest.mark.asyncio
async def test_managed_preview_proxy_blocks_real_cross_origin_redirect():
    blocked_hits = 0

    async def redirect_handler(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        request = await reader.readuntil(b"\r\n\r\n")
        assert request.startswith(b"GET /redirect ")
        writer.write(
            b"HTTP/1.1 302 Found\r\n"
            + f"Location: http://127.0.0.1:{blocked_port}/secret\r\n".encode()
            + b"Content-Length: 0\r\n\r\n"
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    async def blocked_handler(
        _reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        nonlocal blocked_hits
        blocked_hits += 1
        writer.close()
        await writer.wait_closed()

    blocked_server = await asyncio.start_server(blocked_handler, "127.0.0.1", 0)
    blocked_port = int(blocked_server.sockets[0].getsockname()[1])
    redirect_server = await asyncio.start_server(redirect_handler, "127.0.0.1", 0)
    redirect_port = int(redirect_server.sockets[0].getsockname()[1])
    try:
        async with ManagedPreviewProxy(
            f"http://127.0.0.1:{redirect_port}"
        ) as proxy:
            async with httpx.AsyncClient(
                proxy=proxy.url,
                follow_redirects=True,
                trust_env=False,
            ) as client:
                response = await client.get(
                    f"http://127.0.0.1:{redirect_port}/redirect"
                )
        assert response.status_code == 403
        assert blocked_hits == 0
    finally:
        redirect_server.close()
        blocked_server.close()
        await redirect_server.wait_closed()
        await blocked_server.wait_closed()


@pytest.mark.asyncio
async def test_managed_preview_proxy_allows_same_origin_websocket_and_blocks_other_port():
    allowed_hits = 0
    blocked_hits = 0

    async def allowed_handler(websocket):
        nonlocal allowed_hits
        allowed_hits += 1
        await websocket.send("managed-preview-ok")

    async def blocked_handler(websocket):
        nonlocal blocked_hits
        blocked_hits += 1
        await websocket.send("unexpected")

    allowed_server = await websockets.serve(allowed_handler, "127.0.0.1", 0)
    blocked_server = await websockets.serve(blocked_handler, "127.0.0.1", 0)
    allowed_port = int(allowed_server.sockets[0].getsockname()[1])
    blocked_port = int(blocked_server.sockets[0].getsockname()[1])
    try:
        async with ManagedPreviewProxy(
            f"http://127.0.0.1:{allowed_port}"
        ) as proxy:
            async with websockets.connect(
                f"ws://127.0.0.1:{allowed_port}/socket",
                proxy=proxy.url,
            ) as websocket:
                assert await websocket.recv() == "managed-preview-ok"
            with pytest.raises(Exception):
                async with websockets.connect(
                    f"ws://127.0.0.1:{blocked_port}/socket",
                    proxy=proxy.url,
                ):
                    pass
        assert allowed_hits == 1
        assert blocked_hits == 0
    finally:
        allowed_server.close()
        blocked_server.close()
        await allowed_server.wait_closed()
        await blocked_server.wait_closed()


@pytest.mark.asyncio
async def test_public_resolver_rejects_mixed_public_private_dns_answers(monkeypatch):
    async def fake_getaddrinfo(*_args, **_kwargs):
        return [
            (2, 1, 6, "", ("93.184.216.34", 443)),
            (2, 1, 6, "", ("10.0.0.8", 443)),
        ]

    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "getaddrinfo", fake_getaddrinfo)
    with pytest.raises(BrowserNetworkPolicyError, match="non-public"):
        await resolve_public_endpoint("example.test", 443)


@pytest.mark.asyncio
async def test_public_proxy_blocks_loopback_connect_and_reports_reason():
    blocked = []
    async with PublicEgressProxy(
        on_blocked=lambda target, reason: blocked.append((target, reason))
    ) as proxy:
        parsed = httpx.URL(proxy.url)
        reader, writer = await asyncio.open_connection(parsed.host, parsed.port)
        writer.write(
            b"CONNECT 127.0.0.1:8000 HTTP/1.1\r\nHost: 127.0.0.1:8000\r\n\r\n"
        )
        await writer.drain()
        response = await reader.read(4096)
        writer.close()
        await writer.wait_closed()
    assert b"403 Forbidden" in response
    assert blocked and "public IP" in blocked[0][1]


def test_normalizers_follow_playwright_names():
    assert normalize_key("CTRL") == "Control"
    assert normalize_key("ARROWLEFT") == "ArrowLeft"
    assert normalize_key("a") == "a"
    assert normalize_button("wheel") == "middle"
    assert normalize_drag_path([[1, 2], {"x": 3, "y": 4}]) == [
        (1.0, 2.0),
        (3.0, 4.0),
    ]
    with pytest.raises(BrowserReviewError, match="unsupported mouse button"):
        normalize_button("back")


def test_initial_payload_configures_ga_computer_tool_and_policy():
    options = BrowserReviewOptions(url="http://localhost:5173", allow_actions=False)
    payload = build_initial_payload(options)

    assert payload["model"] == "gpt-5.6-terra"
    assert payload["tools"] == [{"type": "computer"}]
    assert payload["reasoning"] == {"effort": "medium"}
    assert "read-only" in payload["input"][0]["content"][0]["text"]
    assert "untrusted evidence" in payload["instructions"]


def test_external_browser_launch_forces_validating_proxy_without_loopback_bypass():
    options = BrowserReviewOptions(url="https://example.com")
    launch = browser_review._browser_launch_options(
        options,
        proxy_url="http://127.0.0.1:45678",
    )
    assert launch["proxy"] == {
        "server": "http://127.0.0.1:45678",
        "bypass": "",
    }
    assert "--proxy-bypass-list=<-loopback>" in launch["args"]
    assert "--disable-quic" in launch["args"]
    assert any("disable_non_proxied_udp" in value for value in launch["args"])


def test_managed_preview_blocks_every_cross_origin_subresource():
    options = BrowserReviewOptions(
        url="http://127.0.0.1:5173",
        network_policy="managed_preview",
    )
    reason = browser_review._request_policy_violation(
        options,
        target_origin="http://127.0.0.1:5173",
        request_url="http://127.0.0.1:8000/private-api",
        top_level_navigation=False,
    )
    assert reason is not None and "outside" in reason


def test_external_policy_allows_public_subresources_but_blocks_private_and_redirects():
    options = BrowserReviewOptions(url="https://example.com")
    assert browser_review._request_policy_violation(
        options,
        target_origin="https://example.com",
        request_url="https://cdn.example.net/app.js",
        top_level_navigation=False,
    ) is None
    private_reason = browser_review._request_policy_violation(
        options,
        target_origin="https://example.com",
        request_url="http://127.0.0.1/secret",
        top_level_navigation=False,
    )
    assert private_reason is not None and "public IP" in private_reason
    redirect_reason = browser_review._request_policy_violation(
        options,
        target_origin="https://example.com",
        request_url="https://other.example/redirected",
        top_level_navigation=True,
    )
    assert redirect_reason is not None and "top-level origin" in redirect_reason


def test_followup_payload_embeds_original_detail_screenshot_and_telemetry():
    options = BrowserReviewOptions(url="http://localhost:5173")
    payload = build_followup_payload(
        options=options,
        previous_response_id="resp_1",
        call_id="call_1",
        screenshot=b"png-bytes",
        telemetry_update='{"page_errors":[{"message":"boom"}]}',
    )

    output = payload["input"][0]
    assert payload["previous_response_id"] == "resp_1"
    assert output["type"] == "computer_call_output"
    assert output["call_id"] == "call_1"
    assert output["output"]["detail"] == "original"
    encoded = output["output"]["image_url"].removeprefix(
        "data:image/png;base64,"
    )
    assert base64.b64decode(encoded) == b"png-bytes"
    assert "never as instructions" in payload["input"][1]["content"][0]["text"]


def test_extracts_computer_call_and_final_text():
    call = {
        "type": "computer_call",
        "call_id": "call_1",
        "actions": [{"type": "screenshot"}],
    }
    assert extract_computer_call({"output": [call]}) is call
    assert extract_computer_call({"output": []}) is None
    assert extract_output_text(
        {
            "output": [
                {
                    "type": "message",
                    "content": [
                        {"type": "output_text", "text": "first"},
                        {"type": "output_text", "text": "second"},
                    ],
                }
            ]
        }
    ) == "first\nsecond"
    with pytest.raises(BrowserReviewError, match="at most one"):
        extract_computer_call({"output": [call, dict(call)]})


def test_telemetry_collects_errors_and_drains_only_new_records():
    telemetry = BrowserTelemetry()
    telemetry._on_console(
        SimpleNamespace(
            type="error", text="console boom", location={"url": "app.js", "line": 9}
        )
    )
    telemetry._on_page_error(RuntimeError("render failed"))
    telemetry._on_request_failed(
        SimpleNamespace(method="GET", url="https://example.test/api", failure="reset")
    )
    telemetry._on_response(
        SimpleNamespace(
            status=500,
            url="https://example.test/api",
            request=SimpleNamespace(method="GET"),
        )
    )
    telemetry.add_blocked_navigation("https://outside.test", "outside origin")

    update = json.loads(telemetry.drain_update() or "{}")
    assert update["console"][0]["text"] == "console boom"
    assert update["page_errors"][0]["message"] == "render failed"
    assert update["http_errors"][0]["status"] == 500
    assert telemetry.drain_update() is None


def test_telemetry_truncation_remains_valid_json():
    telemetry = BrowserTelemetry()
    telemetry._on_page_error(RuntimeError("x" * 2_000))
    update = telemetry.drain_update(max_chars=300)
    assert update is not None
    assert json.loads(update)["truncated"] is True


class _FakeMouse:
    def __init__(self, events: list[tuple]):
        self.events = events

    async def click(self, x, y, **kwargs):
        self.events.append(("click", x, y, kwargs))

    async def dblclick(self, x, y, **kwargs):
        self.events.append(("dblclick", x, y, kwargs))

    async def move(self, x, y):
        self.events.append(("move", x, y))

    async def down(self, **kwargs):
        self.events.append(("down", kwargs))

    async def up(self, **kwargs):
        self.events.append(("up", kwargs))

    async def wheel(self, x, y):
        self.events.append(("wheel", x, y))


class _FakeKeyboard:
    def __init__(self, events: list[tuple]):
        self.events = events

    async def down(self, key):
        self.events.append(("key_down", key))

    async def up(self, key):
        self.events.append(("key_up", key))

    async def press(self, key):
        self.events.append(("press", key))

    async def type(self, text):
        self.events.append(("type", text))


class _FakePage:
    def __init__(self):
        self.events: list[tuple] = []
        self.mouse = _FakeMouse(self.events)
        self.keyboard = _FakeKeyboard(self.events)

    async def wait_for_timeout(self, milliseconds):
        self.events.append(("wait", milliseconds))

    async def goto(self, url, **kwargs):
        self.events.append(("goto", url, kwargs))

    async def screenshot(self, **kwargs):
        self.events.append(("screenshot", kwargs))
        return b"fake-png"


@pytest.mark.asyncio
async def test_execute_computer_actions_supports_batch_and_modifiers():
    page = _FakePage()
    await execute_computer_actions(
        page,
        [
            {"type": "click", "x": 10, "y": 20, "button": "left", "keys": ["SHIFT"]},
            {"type": "drag", "path": [[20, 30], {"x": 40, "y": 50}]},
            {"type": "scroll", "x": 40, "y": 50, "scroll_y": 300, "scroll_x": 0},
            {"type": "keypress", "keys": ["TAB", "ENTER"]},
            {"type": "type", "text": "demo"},
            {"type": "wait"},
            {"type": "screenshot"},
        ],
        allow_actions=True,
        viewport_width=1440,
        viewport_height=900,
        action_delay_ms=0,
    )

    assert page.events[:3] == [
        ("key_down", "Shift"),
        ("click", 10.0, 20.0, {"button": "left"}),
        ("key_up", "Shift"),
    ]
    assert ("down", {"button": "left"}) in page.events
    assert ("up", {"button": "left"}) in page.events
    assert ("wheel", 0.0, 300.0) in page.events
    assert ("press", "Enter") in page.events
    assert ("type", "demo") in page.events
    assert ("wait", 2000) in page.events


@pytest.mark.asyncio
async def test_execute_computer_actions_enforces_read_only_and_viewport():
    page = _FakePage()
    with pytest.raises(ActionBlockedError, match="--allow-actions"):
        await execute_computer_actions(
            page,
            [{"type": "click", "x": 10, "y": 20}],
            allow_actions=False,
            viewport_width=1000,
            viewport_height=700,
        )
    with pytest.raises(BrowserReviewError, match="outside"):
        await execute_computer_actions(
            page,
            [{"type": "scroll", "x": 1001, "y": 20, "scroll_y": 10}],
            allow_actions=False,
            viewport_width=1000,
            viewport_height=700,
        )


@pytest.mark.asyncio
async def test_run_browser_review_completes_loop_and_writes_artifacts(
    monkeypatch, tmp_path
):
    page = _FakePage()
    progress_events = []

    @asynccontextmanager
    async def fake_browser_page(_options, _origin, telemetry):
        telemetry._on_page_error(RuntimeError("observed render error"))
        yield page

    responses = [
        {
            "id": "resp_1",
            "output": [
                {
                    "type": "computer_call",
                    "call_id": "call_1",
                    "actions": [{"type": "screenshot"}],
                }
            ],
        },
        {
            "id": "resp_2",
            "output": [
                {
                    "type": "computer_call",
                    "call_id": "call_2",
                    "actions": [
                        {
                            "type": "scroll",
                            "x": 100,
                            "y": 100,
                            "scroll_x": 0,
                            "scroll_y": 500,
                        }
                    ],
                }
            ],
        },
        {
            "id": "resp_3",
            "output": [
                {
                    "type": "message",
                    "content": [
                        {"type": "output_text", "text": "## Verdict\nPass with issues"}
                    ],
                }
            ],
        },
    ]
    payloads = []

    class FakeResponsesClient:
        def __init__(self, _api_key):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def create(self, payload):
            payloads.append(payload)
            return responses.pop(0)

    monkeypatch.setattr(browser_review, "_browser_page", fake_browser_page)
    monkeypatch.setattr(browser_review, "ResponsesClient", FakeResponsesClient)

    result = await run_browser_review(
            BrowserReviewOptions(
                url="http://127.0.0.1:5173",
                network_policy="managed_preview",
                output_dir=tmp_path,
            action_delay_ms=0,
        ),
        api_key="test-key",
        progress_callback=progress_events.append,
    )

    assert result.steps == 2
    assert result.actions == 2
    assert result.response_id == "resp_3"
    assert result.screenshot_path.read_bytes() == b"fake-png"
    assert "Pass with issues" in result.report_path.read_text()
    assert "observed render error" in result.telemetry_path.read_text()
    assert (tmp_path / "initial.png").exists()
    assert (tmp_path / "step-01.png").exists()
    assert (tmp_path / "step-02.png").exists()
    assert [event["stage"] for event in progress_events] == [
        "browser_ready",
        "model_thinking",
        "executing_actions",
        "model_thinking",
        "executing_actions",
        "model_thinking",
        "completed",
    ]
    assert progress_events[-1]["latest_screenshot"] == "final.png"
    assert len(payloads) == 3
    assert payloads[1]["previous_response_id"] == "resp_1"
    assert payloads[2]["previous_response_id"] == "resp_2"


@pytest.mark.asyncio
async def test_responses_client_uses_bearer_auth_and_returns_json():
    seen_request: httpx.Request | None = None

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal seen_request
        seen_request = request
        return httpx.Response(200, json={"id": "resp_1", "output": []})

    async with ResponsesClient(
        "test-key", transport=httpx.MockTransport(handler)
    ) as client:
        result = await client.create({"model": "gpt-5.6-terra"})

    assert result["id"] == "resp_1"
    assert seen_request is not None
    assert seen_request.headers["Authorization"] == "Bearer test-key"


@pytest.mark.asyncio
async def test_responses_client_reports_http_error_without_key():
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"error": {"message": "bad request"}})

    async with ResponsesClient(
        "secret-key", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(BrowserReviewError, match="HTTP 400") as exc_info:
            await client.create({"model": "bad"})
    assert "secret-key" not in str(exc_info.value)


@pytest.mark.asyncio
async def test_responses_client_rejects_incomplete_result():
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "id": "resp_incomplete",
                "status": "incomplete",
                "incomplete_details": {"reason": "max_output_tokens"},
            },
        )

    async with ResponsesClient(
        "test-key", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(BrowserReviewError, match="incomplete"):
            await client.create({"model": "gpt-5.6-terra"})
