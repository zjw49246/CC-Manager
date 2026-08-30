from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import signal
import stat
import subprocess
import sys
from collections.abc import Iterable
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import HTTPException
from starlette.requests import Request

import backend.api.codex_pool as codex_pool_api
from backend.services.codex_app_server import CodexAppServerBusyError
from backend.services.codex_pool import CodexPool
import scripts.codex_login as codex_login


class _Response:
    def __init__(self, payload: dict):
        self._payload = payload

    def json(self) -> dict:
        return self._payload


class _MailClient:
    def __init__(self, responses: Iterable[dict], calls: list[dict]):
        self._responses = iter(responses)
        self._calls = calls

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def get(self, url: str, **kwargs):
        self._calls.append({"url": url, **kwargs})
        return _Response(next(self._responses))


def _install_mail_client(monkeypatch, responses: list[dict]) -> dict:
    state: dict = {"timeouts": [], "calls": []}

    def factory(*_args, **kwargs):
        state["timeouts"].append(kwargs.get("timeout"))
        return _MailClient(responses, state["calls"])

    monkeypatch.setattr(codex_login.httpx, "AsyncClient", factory)
    monkeypatch.setattr(codex_login, "MAIL_POLL_INTERVAL", 0)
    return state


def _success(*, code: str = "654321", **timestamp_fields) -> dict:
    return {
        "code": 200,
        "message": "success",
        "data": {
            "subject": "Your OpenAI verification code",
            "code": code,
            **timestamp_fields,
        },
    }


def test_detect_mail_provider():
    assert codex_login.detect_mail_provider("test-user@163.com") == "mailcatcher"
    assert codex_login.detect_mail_provider("user@MAIL.COM") == "mailcom"
    assert codex_login.detect_mail_provider("user@onet.pl") == "onet"
    assert codex_login.detect_mail_provider("user@GAZETA.PL") == "gazeta"
    assert codex_login.detect_mail_provider("user@israelmail.com") == "171mail"


async def test_authorize_navigation_retries_one_timeout(monkeypatch):
    class NavigationTimeout(Exception):
        pass

    class Page:
        def __init__(self):
            self.calls = 0

        async def goto(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                raise NavigationTimeout("slow")

    page = Page()
    logs: list[str] = []
    monkeypatch.setattr(codex_login.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(
        codex_login,
        "_host_diagnostic",
        lambda: "mem_available_mb=77, load1=12.16",
    )

    await codex_login._navigate_authorize_page(
        page,
        "https://auth.openai.com/oauth/authorize?redacted",
        logs,
        timeout_error=NavigationTimeout,
    )

    assert page.calls == 2
    assert logs == [
        "Authorize navigation timeout attempt=1/2 "
        "(mem_available_mb=77, load1=12.16)",
    ]


async def test_authorize_navigation_stops_after_limited_retries(monkeypatch):
    class NavigationTimeout(Exception):
        pass

    page = SimpleNamespace(goto=AsyncMock(side_effect=NavigationTimeout("slow")))
    logs: list[str] = []
    monkeypatch.setattr(codex_login.asyncio, "sleep", AsyncMock())

    with pytest.raises(NavigationTimeout):
        await codex_login._navigate_authorize_page(
            page,
            "https://auth.openai.com/oauth/authorize?redacted",
            logs,
            timeout_error=NavigationTimeout,
        )

    assert page.goto.await_count == 2
    assert len(logs) == 2


def test_codex_login_output_redacts_complete_oauth_authorize_url(caplog):
    authorize_url = (
        "https://auth.openai.com/oauth/authorize?client_id=public-client"
        "&state=sensitive-state&code_challenge=sensitive-pkce"
    )

    with caplog.at_level(logging.INFO, logger=codex_login.logger.name):
        codex_login._log_codex_login_output(f"Open this URL: {authorize_url}")

    assert "Open this URL: <OAuth authorize URL redacted>" in caplog.text
    assert authorize_url not in caplog.text
    assert "sensitive-state" not in caplog.text
    assert "sensitive-pkce" not in caplog.text


@pytest.mark.parametrize(
    "timestamp_fields",
    [
        {"date": "2023-11-14T22:13:30+00:00"},
        {"Date": "Tue, 14 Nov 2023 22:13:30 +0000"},
        {"subject": "Your OpenAI verification code | 2023-11-14 22:13:30"},
    ],
    ids=["mailcatcher-lowercase-date", "legacy-uppercase-Date", "subject-fallback"],
)
async def test_mailcatcher_accepts_only_fresh_timestamped_code(monkeypatch, timestamp_fields):
    state = _install_mail_client(monkeypatch, [_success(**timestamp_fields)])

    code = await codex_login.poll_verification_code(
        "platform-query-token",
        after_ts=1_699_999_990,
        timeout_s=1,
        email="user@onet.pl",
        provider="onet",
    )

    assert code == "654321"
    assert state["timeouts"] and state["timeouts"][0] >= 90
    assert state["calls"] == [{
        "url": codex_login.MAIL_DECODE_API,
        "params": {"token": "platform-query-token", "type": "gpt"},
    }]


async def test_generic_mailcatcher_source_supports_163_query_token(monkeypatch):
    state = _install_mail_client(monkeypatch, [
        _success(date="2023-11-14T22:13:30+00:00"),
    ])

    code = await codex_login.poll_verification_code(
        "mailcatcher-query-token",
        after_ts=1_699_999_990,
        timeout_s=1,
        email="test-user@163.com",
        provider="mailcatcher",
    )

    assert code == "654321"
    assert state["calls"] == [{
        "url": codex_login.MAIL_DECODE_API,
        "params": {"token": "mailcatcher-query-token", "type": "gpt"},
    }]


async def test_mailcatcher_202_is_polled_until_200(monkeypatch):
    state = _install_mail_client(monkeypatch, [
        {"code": 202, "message": "processing"},
        _success(date="2023-11-14T22:13:30+00:00"),
    ])

    code = await codex_login.poll_verification_code(
        "platform-query-token",
        after_ts=1_699_999_990,
        timeout_s=1,
        provider="gazeta",
    )

    assert code == "654321"
    assert len(state["calls"]) == 2


async def test_mailcatcher_invalid_query_token_fails_immediately(monkeypatch):
    state = _install_mail_client(monkeypatch, [
        {"code": 401, "message": "invalid token"},
    ])

    with pytest.raises(RuntimeError, match="rejected.*token|token.*rejected"):
        await codex_login.poll_verification_code(
            "not-a-mailbox-password",
            after_ts=1_699_999_990,
            timeout_s=1,
            provider="onet",
        )

    assert len(state["calls"]) == 1


async def test_mailcatcher_stale_code_is_not_returned(monkeypatch):
    _install_mail_client(monkeypatch, [
        # Even a message one second before the login attempt is stale.  A
        # grace window here can reuse the previous attempt's still-valid OTP.
        _success(code="111111", date="2023-11-14T22:13:19+00:00"),
        _success(code="222222", date="2023-11-14T22:13:21+00:00"),
    ])

    code = await codex_login.poll_verification_code(
        "platform-query-token",
        after_ts=1_700_000_000,
        timeout_s=1,
        provider="onet",
    )

    assert code == "222222"


async def test_mailcatcher_undated_code_times_out_instead_of_being_reused(monkeypatch):
    _install_mail_client(monkeypatch, [_success(code="111111")])
    clock = iter([0.0, 0.0, 2.0])
    monkeypatch.setattr(codex_login.time, "time", lambda: next(clock))

    with pytest.raises(RuntimeError, match="No fresh OpenAI verification code"):
        await codex_login.poll_verification_code(
            "platform-query-token",
            after_ts=0.5,
            timeout_s=1,
            provider="gazeta",
        )


class _OtpField:
    def __init__(self, auth_path: Path):
        self.auth_path = auth_path
        self.value = ""

    async def fill(self, value: str):
        self.value = value
        if value:
            self.auth_path.write_text("authenticated")


class _OtpPage:
    url = "https://auth.openai.com/verify"

    async def wait_for_timeout(self, _milliseconds: int):
        return None

    async def screenshot(self, **_kwargs):
        return None


class _EmptyPasswordField:
    async def input_value(self):
        return ""


class _EmailCodeAction:
    def __init__(self):
        self.clicked = False

    async def is_visible(self):
        return True

    async def inner_text(self):
        return "Continue with a one-time code"

    async def get_attribute(self, _name: str):
        return None

    async def click(self):
        self.clicked = True


class _SingleLocator:
    def __init__(self, candidate):
        self.candidate = candidate

    async def count(self):
        return 1

    def nth(self, _index: int):
        return self.candidate


class _PersistentOtpField:
    def __init__(self):
        self.values: list[str] = []

    async def fill(self, value: str):
        self.values.append(value)


class _DelayedAuthOtpPage(_OtpPage):
    def __init__(self, auth_path: Path):
        self.auth_path = auth_path
        self.waits = 0

    async def wait_for_timeout(self, _milliseconds: int):
        self.waits += 1
        if self.waits >= 6:
            self.auth_path.write_text("authenticated")


class _SecondOtpSucceedsField(_PersistentOtpField):
    def __init__(self, auth_path: Path):
        super().__init__()
        self.auth_path = auth_path
        self.submissions = 0

    async def fill(self, value: str):
        await super().fill(value)
        if value:
            self.submissions += 1
            if self.submissions == 2:
                self.auth_path.write_text("authenticated")


async def test_password_only_login_waits_for_user_otp_and_continues(monkeypatch, tmp_path):
    auth_path = tmp_path / "auth.json"
    otp_field = _OtpField(auth_path)
    manual_reader = SimpleNamespace(read_code=AsyncMock(return_value="123456"))

    async def first_visible(_page, selector: str):
        return otp_field if selector == codex_login.OTP_SELECTOR else None

    poll = AsyncMock()
    monkeypatch.setattr(codex_login, "_first_visible", first_visible)
    monkeypatch.setattr(codex_login, "poll_verification_code", poll)
    monkeypatch.setattr(codex_login, "_click_continue", AsyncMock(return_value=True))

    await codex_login._run_state_machine(
        _OtpPage(),
        "user@mail.com",
        "openai-password",
        "",
        1,
        auth_path,
        [],
        "mailcom",
        "attempt-1",
        manual_reader,
    )

    manual_reader.read_code.assert_awaited_once()
    assert otp_field.value == "123456"
    poll.assert_not_awaited()


async def test_email_only_login_switches_to_code_and_waits_for_user_otp(
    monkeypatch, tmp_path,
):
    auth_path = tmp_path / "auth.json"
    password_field = _EmptyPasswordField()
    otp_field = _OtpField(auth_path)
    manual_reader = SimpleNamespace(read_code=AsyncMock(return_value="123456"))
    state = {"email_code": False}

    async def first_visible(_page, selector: str):
        if selector == codex_login.PASSWORD_SELECTOR and not state["email_code"]:
            return password_field
        if selector == codex_login.OTP_SELECTOR and state["email_code"]:
            return otp_field
        return None

    async def switch_to_email_code(_page, _logs):
        state["email_code"] = True
        return True

    switch = AsyncMock(side_effect=switch_to_email_code)
    monkeypatch.setattr(codex_login, "_first_visible", first_visible)
    monkeypatch.setattr(codex_login, "_switch_to_email_code", switch)
    monkeypatch.setattr(codex_login, "_click_continue", AsyncMock(return_value=True))

    await codex_login._run_state_machine(
        _OtpPage(),
        "email-only@163.com",
        "",
        "",
        1,
        auth_path,
        [],
        "mailcatcher",
        "attempt-email-only",
        manual_reader,
    )

    switch.assert_awaited_once()
    manual_reader.read_code.assert_awaited_once()
    assert otp_field.value == "123456"


async def test_switch_to_email_code_recognizes_one_time_code_action(monkeypatch):
    action = _EmailCodeAction()
    page = _OtpPage()
    page.locator = lambda _selector: _SingleLocator(action)

    async def first_visible(_page, selector: str):
        if selector == codex_login.OTP_SELECTOR and action.clicked:
            return object()
        if selector == codex_login.PASSWORD_SELECTOR and not action.clicked:
            return _EmptyPasswordField()
        return None

    monkeypatch.setattr(codex_login, "_first_visible", first_visible)
    logs: list[str] = []

    switched = await codex_login._switch_to_email_code(page, logs)

    assert switched is True
    assert action.clicked is True
    assert "one-time code" in logs[0]


async def test_email_only_login_reports_when_openai_requires_password(
    monkeypatch, tmp_path,
):
    async def first_visible(_page, selector: str):
        if selector == codex_login.PASSWORD_SELECTOR:
            return _EmptyPasswordField()
        return None

    monkeypatch.setattr(codex_login, "_first_visible", first_visible)
    monkeypatch.setattr(
        codex_login,
        "_switch_to_email_code",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(
        codex_login,
        "_visible_action_labels",
        AsyncMock(return_value=["Continue"]),
    )

    with pytest.raises(RuntimeError, match="provide the OpenAI password"):
        await codex_login._run_state_machine(
            _OtpPage(),
            "password-required@example.com",
            "",
            "",
            1,
            tmp_path / "auth.json",
            [],
            "171mail",
            "attempt-password-required",
            SimpleNamespace(read_code=AsyncMock()),
        )


async def test_visible_otp_form_waits_for_delayed_auth_without_new_challenge(
    monkeypatch, tmp_path,
):
    auth_path = tmp_path / "auth.json"
    otp_field = _PersistentOtpField()
    page = _DelayedAuthOtpPage(auth_path)
    manual_reader = SimpleNamespace(read_code=AsyncMock(return_value="123456"))

    async def first_visible(_page, selector: str):
        return otp_field if selector == codex_login.OTP_SELECTOR else None

    monkeypatch.setattr(codex_login, "_first_visible", first_visible)
    monkeypatch.setattr(codex_login, "_visible_otp_error", AsyncMock(return_value=None))
    monkeypatch.setattr(codex_login, "_click_continue", AsyncMock(return_value=True))

    await codex_login._run_state_machine(
        page,
        "user@mail.com",
        "openai-password",
        "",
        1,
        auth_path,
        [],
        "mailcom",
        "attempt-delayed-auth",
        manual_reader,
    )

    manual_reader.read_code.assert_awaited_once()
    assert otp_field.values == ["123456"]
    assert page.waits >= 6


async def test_explicit_otp_error_opens_one_new_challenge(monkeypatch, tmp_path):
    auth_path = tmp_path / "auth.json"
    otp_field = _SecondOtpSucceedsField(auth_path)
    manual_reader = SimpleNamespace(
        read_code=AsyncMock(side_effect=["111111", "222222"]),
    )

    async def first_visible(_page, selector: str):
        return otp_field if selector == codex_login.OTP_SELECTOR else None

    monkeypatch.setattr(codex_login, "_first_visible", first_visible)
    monkeypatch.setattr(
        codex_login,
        "_visible_otp_error",
        AsyncMock(return_value="The code you entered is incorrect"),
    )
    monkeypatch.setattr(codex_login, "_click_continue", AsyncMock(return_value=True))

    await codex_login._run_state_machine(
        _OtpPage(),
        "user@mail.com",
        "openai-password",
        "",
        1,
        auth_path,
        [],
        "mailcom",
        "attempt-retry",
        manual_reader,
    )

    assert manual_reader.read_code.await_count == 2
    assert otp_field.values == ["111111", "", "222222"]


async def test_login_input_reader_consumes_credentials_then_otp_on_same_stream(
    monkeypatch,
):
    attempt_id = "attempt-shared-stdin"
    stream = asyncio.StreamReader()
    stream.feed_data((json.dumps({
        "type": "credentials",
        "attempt_id": attempt_id,
        "token": "mailbox-query-token",
        "password": "openai-password",
    }) + "\n").encode())
    reader = codex_login._ManualOtpReader()
    reader._reader = stream

    token, password = await reader.read_credentials(attempt_id=attempt_id)

    assert token == "mailbox-query-token"
    assert password == "openai-password"

    monkeypatch.setattr(
        codex_login.uuid,
        "uuid4",
        lambda: SimpleNamespace(hex="challenge-shared-stdin"),
    )
    stream.feed_data(b'{"challenge_id":"challenge-shared-stdin","code":"123456"}\n')

    code = await reader.read_code(attempt_id=attempt_id, timeout_s=60, logs=[])

    assert code == "123456"
    assert reader._reader is stream


async def test_login_input_reader_accepts_email_only_initialization():
    attempt_id = "attempt-email-only"
    stream = asyncio.StreamReader()
    stream.feed_data((json.dumps({
        "type": "credentials",
        "attempt_id": attempt_id,
        "token": "",
        "password": "",
    }) + "\n").encode())
    reader = codex_login._ManualOtpReader()
    reader._reader = stream

    token, password = await reader.read_credentials(attempt_id=attempt_id)

    assert token == ""
    assert password == ""


class _OtpStdin:
    def __init__(self):
        self.writes: list[bytes] = []

    def write(self, value: bytes):
        self.writes.append(value)

    async def drain(self):
        return None


async def test_backend_exposes_otp_challenge_and_forwards_code_only_over_stdin():
    attempt_id = "attempt-otp"
    challenge_id = "challenge-otp"
    email = "user@example.com"
    stdin = _OtpStdin()
    proc = SimpleNamespace(returncode=None, stdin=stdin)
    codex_pool_api._add_state.clear()
    codex_pool_api._login_attempts.clear()
    codex_pool_api._add_state[email] = {"status": "running", "attempt_id": attempt_id}
    codex_pool_api._login_attempts[attempt_id] = {
        "kind": "add",
        "state_key": email,
        "proc": proc,
        "challenge_id": None,
        "expires_at": None,
    }

    handled = codex_pool_api._handle_login_event(
        attempt_id,
        codex_pool_api.LOGIN_EVENT_PREFIX + json.dumps({
            "type": "otp_required",
            "attempt_id": attempt_id,
            "challenge_id": challenge_id,
            "expires_at": codex_pool_api.time.time() + 60,
        }),
    )

    assert handled is True
    state = codex_pool_api._add_state[email]
    assert state["status"] == "awaiting_otp"
    assert state["challenge_id"] == challenge_id

    response = await codex_pool_api.codex_submit_login_otp(
        _admin_request(),
        attempt_id,
        codex_pool_api.SubmitCodexOtpRequest(
            challenge_id=challenge_id,
            code="123456",
        ),
    )

    assert response == {"ok": True, "status": "verifying_otp"}
    assert json.loads(stdin.writes[0]) == {
        "challenge_id": challenge_id,
        "code": "123456",
    }
    assert "123456" not in repr(codex_pool_api._add_state[email])
    assert "123456" not in repr(codex_pool_api._login_attempts[attempt_id])


async def test_backend_rejects_invalid_or_stale_otp_without_writing_stdin():
    attempt_id = "attempt-invalid"
    challenge_id = "challenge-current"
    email = "user@example.com"
    stdin = _OtpStdin()
    proc = SimpleNamespace(returncode=None, stdin=stdin)
    codex_pool_api._add_state.clear()
    codex_pool_api._login_attempts.clear()
    codex_pool_api._add_state[email] = {
        "status": "awaiting_otp",
        "attempt_id": attempt_id,
    }
    codex_pool_api._login_attempts[attempt_id] = {
        "kind": "add",
        "state_key": email,
        "proc": proc,
        "challenge_id": challenge_id,
        "expires_at": codex_pool_api.time.time() + 60,
    }

    with pytest.raises(HTTPException) as stale:
        await codex_pool_api.codex_submit_login_otp(
            _admin_request(),
            attempt_id,
            codex_pool_api.SubmitCodexOtpRequest(
                challenge_id="old-challenge",
                code="123456",
            ),
        )
    assert stale.value.status_code == 409

    with pytest.raises(HTTPException) as invalid:
        await codex_pool_api.codex_submit_login_otp(
            _admin_request(),
            attempt_id,
            codex_pool_api.SubmitCodexOtpRequest(
                challenge_id=challenge_id,
                code="12ab",
            ),
        )
    assert invalid.value.status_code == 422
    assert stdin.writes == []


async def test_backend_closes_otp_gate_before_awaiting_stdin_ack():
    attempt_id = "attempt-concurrent-otp"
    challenge_id = "challenge-concurrent-otp"
    email = "concurrent@example.com"
    drain_entered = asyncio.Event()
    release_drain = asyncio.Event()

    class BlockingStdin(_OtpStdin):
        async def drain(self):
            drain_entered.set()
            await release_drain.wait()

    stdin = BlockingStdin()
    proc = SimpleNamespace(returncode=None, stdin=stdin)
    codex_pool_api._add_state.clear()
    codex_pool_api._login_attempts.clear()
    codex_pool_api._add_state[email] = {
        "status": "awaiting_otp",
        "attempt_id": attempt_id,
    }
    codex_pool_api._login_attempts[attempt_id] = {
        "kind": "add",
        "state_key": email,
        "proc": proc,
        "challenge_id": challenge_id,
        "expires_at": codex_pool_api.time.time() + 60,
    }
    request = codex_pool_api.SubmitCodexOtpRequest(
        challenge_id=challenge_id,
        code="123456",
    )
    first = asyncio.create_task(
        codex_pool_api.codex_submit_login_otp(
            _admin_request(), attempt_id, request,
        )
    )
    await asyncio.wait_for(drain_entered.wait(), timeout=1)

    with pytest.raises(HTTPException) as duplicate:
        await codex_pool_api.codex_submit_login_otp(
            _admin_request(), attempt_id, request,
        )
    assert duplicate.value.status_code == 409
    assert codex_pool_api._add_state[email]["status"] == "verifying_otp"

    release_drain.set()
    assert await first == {"ok": True, "status": "verifying_otp"}
    assert len(stdin.writes) == 1


async def test_backend_does_not_reopen_otp_gate_after_ambiguous_pipe_failure():
    attempt_id = "attempt-uncertain-otp"
    challenge_id = "challenge-uncertain-otp"
    email = "uncertain@example.com"

    class FailingStdin(_OtpStdin):
        async def drain(self):
            raise ConnectionError("ack lost")

    stdin = FailingStdin()
    codex_pool_api._add_state.clear()
    codex_pool_api._login_attempts.clear()
    codex_pool_api._add_state[email] = {
        "status": "awaiting_otp",
        "attempt_id": attempt_id,
    }
    codex_pool_api._login_attempts[attempt_id] = {
        "kind": "add",
        "state_key": email,
        "proc": SimpleNamespace(returncode=None, stdin=stdin),
        "challenge_id": challenge_id,
        "expires_at": codex_pool_api.time.time() + 60,
    }
    body = codex_pool_api.SubmitCodexOtpRequest(
        challenge_id=challenge_id,
        code="123456",
    )

    with pytest.raises(HTTPException) as uncertain:
        await codex_pool_api.codex_submit_login_otp(
            _admin_request(), attempt_id, body,
        )
    assert uncertain.value.status_code == 409
    assert codex_pool_api._add_state[email]["status"] == "verifying_otp"
    with pytest.raises(HTTPException) as replay:
        await codex_pool_api.codex_submit_login_otp(
            _admin_request(), attempt_id, body,
        )
    assert replay.value.status_code == 409
    assert len(stdin.writes) == 1


def test_login_detail_redacts_oauth_authorize_url():
    detail = codex_pool_api._sanitize_login_detail(
        "open https://auth.openai.com/oauth/authorize?client_id=secret&state=private now"
    )
    assert "client_id" not in detail
    assert "state=" not in detail
    assert "[redacted OpenAI OAuth URL]" in detail


def _admin_request() -> Request:
    request = Request({"type": "http", "method": "POST", "path": "/"})
    request.state.user_role = "admin"
    return request


class _FinishedProcess:
    def __init__(self):
        self.returncode = 0
        self.stdin = _OtpStdin()

    async def communicate(self):
        return b"", b""


class _BlockingProcess:
    def __init__(self):
        self.returncode = None
        self.finished = asyncio.Event()
        self.stdin = _OtpStdin()

    async def communicate(self):
        await self.finished.wait()
        self.returncode = 0
        return b"", b""


def _maintenance_manager(*, begin_side_effect=None):
    begin = AsyncMock(return_value=False)
    if begin_side_effect is not None:
        begin.side_effect = begin_side_effect
    return SimpleNamespace(
        begin_codex_app_server_home_maintenance=begin,
        end_codex_app_server_home_maintenance=AsyncMock(),
    )


def _prepare_relogin_account(monkeypatch, tmp_path, manager):
    token_dir = tmp_path / ".codex-pool"
    token_dir.mkdir(exist_ok=True)
    (token_dir / "email_tokens.json").write_text(json.dumps({
        "password-only@mail.com": {
            "token": "",
            "provider": "mailcom",
            "password": "openai-password",
        },
    }))
    monkeypatch.setattr(codex_pool_api.Path, "home", lambda: tmp_path)

    account = SimpleNamespace(
        id="codex-2",
        email="password-only@mail.com",
        codex_home=str(tmp_path / ".codex-codex-2"),
    )
    pool = SimpleNamespace(
        account=lambda account_id: account if account_id == account.id else None,
        reload=lambda: None,
        _quota_cache=None,
    )
    monkeypatch.setattr(codex_pool_api, "_get_pool", lambda: pool)
    monkeypatch.setattr(codex_pool_api, "_get_instance_manager", lambda: manager)
    monkeypatch.setattr(codex_pool_api, "_ensure_xvfb", AsyncMock())
    monkeypatch.setattr(codex_pool_api, "_login_lock", asyncio.Lock())
    codex_pool_api._relogin_state.clear()
    return account, pool


async def test_add_account_keeps_credentials_out_of_argv_and_reuses_stdin_for_otp(
    monkeypatch,
):
    captured: dict[str, object] = {}
    pool = SimpleNamespace(_accounts=[], reload=lambda: None, _quota_cache=None)
    manager = _maintenance_manager()
    proc = _BlockingProcess()

    async def create_process(*cmd, **kwargs):
        captured["cmd"] = cmd
        captured["kwargs"] = kwargs
        return proc

    monkeypatch.setattr(codex_pool_api, "_get_pool", lambda: pool)
    monkeypatch.setattr(codex_pool_api, "_get_instance_manager", lambda: manager)
    monkeypatch.setattr(codex_pool_api, "_ensure_xvfb", AsyncMock())
    monkeypatch.setattr(codex_pool_api.asyncio, "create_subprocess_exec", create_process)
    monkeypatch.setattr(codex_pool_api, "_login_lock", asyncio.Lock())
    codex_pool_api._add_state.clear()

    result = await codex_pool_api.codex_add_account(
        _admin_request(),
        codex_pool_api.AddCodexAccountRequest(
            email="password-only@mail.com",
            token="mailbox-query-token",
            password="openai-password",
            login_method="mailcom",
        ),
    )

    cmd = captured["cmd"]
    assert result["status"] == "running"
    assert "--token" not in cmd
    assert "--password" not in cmd
    assert "mailbox-query-token" not in cmd
    assert "openai-password" not in cmd
    assert "--credentials-stdin" in cmd
    assert cmd[cmd.index("--mail-provider") + 1] == "mailcom"
    assert captured["kwargs"]["stdin"] is asyncio.subprocess.PIPE
    assert captured["kwargs"]["start_new_session"] is True
    assert json.loads(proc.stdin.writes[0]) == {
        "type": "credentials",
        "attempt_id": result["attempt_id"],
        "token": "mailbox-query-token",
        "password": "openai-password",
    }
    assert "mailbox-query-token" not in repr(codex_pool_api._add_state)
    assert "openai-password" not in repr(codex_pool_api._login_attempts)

    challenge_id = "challenge-after-credentials"
    codex_pool_api._handle_login_event(
        result["attempt_id"],
        codex_pool_api.LOGIN_EVENT_PREFIX + json.dumps({
            "type": "otp_required",
            "attempt_id": result["attempt_id"],
            "challenge_id": challenge_id,
            "expires_at": codex_pool_api.time.time() + 60,
        }),
    )
    await codex_pool_api.codex_submit_login_otp(
        _admin_request(),
        result["attempt_id"],
        codex_pool_api.SubmitCodexOtpRequest(
            challenge_id=challenge_id,
            code="123456",
        ),
    )
    assert json.loads(proc.stdin.writes[1]) == {
        "challenge_id": challenge_id,
        "code": "123456",
    }

    proc.finished.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    manager.begin_codex_app_server_home_maintenance.assert_awaited_once()
    manager.end_codex_app_server_home_maintenance.assert_awaited_once()
    assert not codex_pool_api._login_lock.locked()


async def test_add_account_holds_home_maintenance_until_login_finishes(
    monkeypatch, tmp_path,
):
    pool = SimpleNamespace(_accounts=[], reload=lambda: None, _quota_cache=None)
    manager = _maintenance_manager()
    proc = _BlockingProcess()
    monkeypatch.setattr(codex_pool_api.Path, "home", lambda: tmp_path)
    monkeypatch.setattr(codex_pool_api, "_get_pool", lambda: pool)
    monkeypatch.setattr(codex_pool_api, "_get_instance_manager", lambda: manager)
    monkeypatch.setattr(codex_pool_api, "_ensure_xvfb", AsyncMock())
    monkeypatch.setattr(
        codex_pool_api.asyncio,
        "create_subprocess_exec",
        AsyncMock(return_value=proc),
    )
    monkeypatch.setattr(codex_pool_api, "_login_lock", asyncio.Lock())
    codex_pool_api._add_state.clear()

    result = await codex_pool_api.codex_add_account(
        _admin_request(),
        codex_pool_api.AddCodexAccountRequest(
            email="new@mail.com",
            password="openai-password",
            login_method="mailcom",
        ),
    )
    await asyncio.sleep(0)

    codex_home = str(tmp_path / ".codex")
    assert result["status"] == "running"
    manager.begin_codex_app_server_home_maintenance.assert_awaited_once_with(
        codex_home, require_idle=True,
    )
    manager.end_codex_app_server_home_maintenance.assert_not_awaited()
    assert codex_pool_api._login_lock.locked()

    proc.finished.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    manager.end_codex_app_server_home_maintenance.assert_awaited_once_with(codex_home)
    assert not codex_pool_api._login_lock.locked()


async def test_add_account_busy_home_returns_409_and_releases_login_lock(
    monkeypatch, tmp_path,
):
    pool = SimpleNamespace(_accounts=[], reload=lambda: None, _quota_cache=None)
    manager = _maintenance_manager(
        begin_side_effect=CodexAppServerBusyError("active Codex turn"),
    )
    spawn = AsyncMock()
    monkeypatch.setattr(codex_pool_api.Path, "home", lambda: tmp_path)
    monkeypatch.setattr(codex_pool_api, "_get_pool", lambda: pool)
    monkeypatch.setattr(codex_pool_api, "_get_instance_manager", lambda: manager)
    monkeypatch.setattr(codex_pool_api, "_ensure_xvfb", AsyncMock())
    monkeypatch.setattr(codex_pool_api.asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(codex_pool_api, "_login_lock", asyncio.Lock())
    codex_pool_api._add_state.clear()

    with pytest.raises(HTTPException) as exc_info:
        await codex_pool_api.codex_add_account(
            _admin_request(),
            codex_pool_api.AddCodexAccountRequest(
                email="busy@mail.com",
                password="openai-password",
                login_method="mailcom",
            ),
        )

    assert exc_info.value.status_code == 409
    spawn.assert_not_awaited()
    manager.end_codex_app_server_home_maintenance.assert_not_awaited()
    assert not codex_pool_api._login_lock.locked()


async def test_add_spawn_failure_rolls_back_journal_without_creating_home(
    monkeypatch, tmp_path,
):
    pool = SimpleNamespace(_accounts=[], reload=Mock(), _quota_cache=None)
    manager = _maintenance_manager()
    monkeypatch.setattr(codex_pool_api.Path, "home", lambda: tmp_path)
    monkeypatch.setattr(codex_pool_api, "_get_pool", lambda: pool)
    monkeypatch.setattr(codex_pool_api, "_get_instance_manager", lambda: manager)
    monkeypatch.setattr(codex_pool_api, "_ensure_xvfb", AsyncMock())
    monkeypatch.setattr(codex_pool_api, "_login_lock", asyncio.Lock())
    monkeypatch.setattr(
        codex_pool_api.asyncio,
        "create_subprocess_exec",
        AsyncMock(side_effect=OSError("could not spawn login")),
    )
    codex_pool_api._add_state.clear()

    with pytest.raises(OSError, match="could not spawn login"):
        await codex_pool_api.codex_add_account(
            _admin_request(),
            codex_pool_api.AddCodexAccountRequest(
                email="spawn-failed@mail.com",
                password="openai-password",
                login_method="mailcom",
            ),
        )

    assert not (tmp_path / ".codex").exists()
    transaction_dir = tmp_path / ".codex-pool" / codex_pool_api.LOGIN_TRANSACTION_DIR
    assert not list(transaction_dir.glob("*.json"))
    manager.end_codex_app_server_home_maintenance.assert_awaited_once_with(
        str(tmp_path / ".codex")
    )
    assert not codex_pool_api._login_lock.locked()


async def test_add_cleans_failed_login_runtime_residue_before_spawn(
    monkeypatch, tmp_path,
):
    failed_home = tmp_path / ".codex-codex-2"
    (failed_home / "log" / "nested").mkdir(parents=True)
    (failed_home / "log" / "nested" / "codex.log").write_text("no credentials\n")
    (failed_home / "tmp").mkdir()
    (failed_home / "tmp" / "browser.tmp").write_text("transient\n")
    (failed_home / "models_cache.json").write_text("{}\n")
    pool = SimpleNamespace(
        _accounts=[SimpleNamespace(id="codex-1")],
        reload=Mock(),
        _quota_cache=None,
    )
    manager = _maintenance_manager()

    async def fail_after_asserting_clean_home(*_cmd, **_kwargs):
        assert failed_home.is_dir()
        assert list(failed_home.iterdir()) == []
        assert failed_home.stat().st_mode & 0o777 == 0o700
        raise OSError("stop after cleanup")

    monkeypatch.setattr(codex_pool_api.Path, "home", lambda: tmp_path)
    monkeypatch.setattr(codex_pool_api, "_get_pool", lambda: pool)
    monkeypatch.setattr(codex_pool_api, "_get_instance_manager", lambda: manager)
    monkeypatch.setattr(codex_pool_api, "_ensure_xvfb", AsyncMock())
    monkeypatch.setattr(codex_pool_api, "_login_lock", asyncio.Lock())
    monkeypatch.setattr(
        codex_pool_api.asyncio,
        "create_subprocess_exec",
        fail_after_asserting_clean_home,
    )
    codex_pool_api._add_state.clear()

    with pytest.raises(OSError, match="stop after cleanup"):
        await codex_pool_api.codex_add_account(
            _admin_request(),
            codex_pool_api.AddCodexAccountRequest(
                email="reuse-failed-slot@mail.com",
                password="openai-password",
                login_method="mailcom",
            ),
        )

    manager.begin_codex_app_server_home_maintenance.assert_awaited_once_with(
        str(failed_home), require_idle=True,
    )
    manager.end_codex_app_server_home_maintenance.assert_awaited_once_with(
        str(failed_home)
    )
    assert list(failed_home.iterdir()) == []
    assert not codex_pool_api._login_lock.locked()


def test_add_account_home_allocator_never_reuses_retained_rollout_directory(
    monkeypatch, tmp_path,
):
    monkeypatch.setattr(codex_pool_api.Path, "home", lambda: tmp_path)
    (tmp_path / ".codex").mkdir()
    retained = (
        tmp_path / ".codex-codex-2" / "sessions" / "2026" / "07" / "21"
    )
    retained.mkdir(parents=True)
    (retained / "rollout-old-thread.jsonl").write_text("{}\n")
    pool = SimpleNamespace(_accounts=[SimpleNamespace(id="codex-1")])

    account_id, codex_home = codex_pool_api._allocate_codex_account_home(pool)

    assert account_id == "codex-3"
    assert codex_home == str(tmp_path / ".codex-codex-3")


def test_add_account_home_allocator_reuses_failed_empty_slot(monkeypatch, tmp_path):
    monkeypatch.setattr(codex_pool_api.Path, "home", lambda: tmp_path)
    (tmp_path / ".codex").mkdir()
    failed_home = tmp_path / ".codex-codex-2"
    failed_home.mkdir()
    (failed_home / "models_cache.json").write_text("{}\n")
    pool = SimpleNamespace(_accounts=[SimpleNamespace(id="codex-1")])

    account_id, codex_home = codex_pool_api._allocate_codex_account_home(pool)

    assert account_id == "codex-2"
    assert codex_home == str(failed_home)


def test_add_account_home_allocator_reuses_failed_runtime_only_slot(
    monkeypatch, tmp_path,
):
    monkeypatch.setattr(codex_pool_api.Path, "home", lambda: tmp_path)
    failed_home = tmp_path / ".codex-codex-2"
    (failed_home / "log" / "nested").mkdir(parents=True)
    (failed_home / "log" / "nested" / "codex.log").write_text("runtime only\n")
    (failed_home / "tmp").mkdir()
    (failed_home / "tmp" / "browser.tmp").write_text("runtime only\n")
    (failed_home / "models_cache.json").write_text("{}\n")
    pool = SimpleNamespace(_accounts=[SimpleNamespace(id="codex-1")])

    account_id, codex_home = codex_pool_api._allocate_codex_account_home(pool)

    assert account_id == "codex-2"
    assert codex_home == str(failed_home)


@pytest.mark.parametrize(
    ("relative_path", "entry_type"),
    [
        ("auth.json", "file"),
        ("sessions", "directory"),
        ("config.toml", "file"),
        ("state_5.sqlite", "file"),
    ],
)
def test_add_account_home_allocator_rejects_identity_or_durable_state(
    monkeypatch, tmp_path, relative_path, entry_type,
):
    monkeypatch.setattr(codex_pool_api.Path, "home", lambda: tmp_path)
    failed_home = tmp_path / ".codex-codex-2"
    failed_home.mkdir()
    entry = failed_home / relative_path
    if entry_type == "directory":
        entry.mkdir()
    else:
        entry.write_text("identity-bearing state\n")
    pool = SimpleNamespace(_accounts=[SimpleNamespace(id="codex-1")])

    account_id, codex_home = codex_pool_api._allocate_codex_account_home(pool)

    assert account_id == "codex-3"
    assert codex_home == str(tmp_path / ".codex-codex-3")


def test_failed_login_home_reuse_rejects_symlinks_and_special_files(
    monkeypatch, tmp_path,
):
    monkeypatch.setattr(codex_pool_api.Path, "home", lambda: tmp_path)
    failed_home = tmp_path / ".codex-codex-2"
    log_dir = failed_home / "log"
    log_dir.mkdir(parents=True)
    outside = tmp_path / "outside.log"
    outside.write_text("must remain untouched\n")
    (log_dir / "linked.log").symlink_to(outside)

    assert codex_pool_api._failed_login_home_is_reusable(failed_home) is False
    with pytest.raises(RuntimeError, match="unsafe failed-login CODEX_HOME"):
        codex_pool_api._purge_failed_login_codex_home(failed_home)
    assert outside.read_text() == "must remain untouched\n"
    assert (log_dir / "linked.log").is_symlink()

    (log_dir / "linked.log").unlink()
    os.mkfifo(log_dir / "runtime.fifo")
    assert codex_pool_api._failed_login_home_is_reusable(failed_home) is False
    with pytest.raises(RuntimeError, match="unsafe failed-login CODEX_HOME"):
        codex_pool_api._purge_failed_login_codex_home(failed_home)
    assert stat.S_ISFIFO((log_dir / "runtime.fifo").lstat().st_mode)


def test_add_account_home_allocator_rejects_unknown_failed_home_data(
    monkeypatch, tmp_path,
):
    monkeypatch.setattr(codex_pool_api.Path, "home", lambda: tmp_path)
    (tmp_path / ".codex").mkdir()
    stale_home = tmp_path / ".codex-codex-2"
    stale_home.mkdir()
    (stale_home / "history.jsonl").write_text("old identity\n")
    pool = SimpleNamespace(_accounts=[SimpleNamespace(id="codex-1")])

    account_id, codex_home = codex_pool_api._allocate_codex_account_home(pool)

    assert account_id == "codex-3"
    assert codex_home == str(tmp_path / ".codex-codex-3")


def test_add_account_home_allocator_reuses_lowest_clean_retired_slot(
    monkeypatch, tmp_path,
):
    monkeypatch.setattr(codex_pool_api.Path, "home", lambda: tmp_path)
    first_home = tmp_path / ".codex"
    third_home = tmp_path / ".codex-codex-3"
    for account_id, home in (("codex-1", first_home), ("codex-3", third_home)):
        (home / "sessions" / "2026" / "07" / "21").mkdir(parents=True)
        codex_pool_api._purge_retired_codex_home(home, account_id)

    pool = SimpleNamespace(_accounts=[
        SimpleNamespace(
            id="codex-3", codex_home=str(third_home.resolve()),
            retired=True, cleanup_pending=False, login_recovery_failed=False,
        ),
        SimpleNamespace(
            id="codex-1", codex_home=str(first_home.resolve()),
            retired=True, cleanup_pending=False, login_recovery_failed=False,
        ),
        SimpleNamespace(
            id="codex-2", codex_home=str((tmp_path / ".codex-codex-2").resolve()),
            retired=False, cleanup_pending=False, login_recovery_failed=False,
        ),
    ])

    account_id, codex_home = codex_pool_api._allocate_codex_account_home(pool)

    assert account_id == "codex-1"
    assert codex_home == str(first_home.resolve())


def test_add_account_home_allocator_preserves_retired_slot_canonical_home(
    monkeypatch, tmp_path,
):
    monkeypatch.setattr(codex_pool_api.Path, "home", lambda: tmp_path)
    custom_home = tmp_path / ".codex-recycled-primary"
    codex_pool_api._purge_retired_codex_home(custom_home, "codex-2")
    pool = SimpleNamespace(_accounts=[
        SimpleNamespace(
            id="codex-1", codex_home=str((tmp_path / ".codex").resolve()),
            retired=False, cleanup_pending=False, login_recovery_failed=False,
        ),
        SimpleNamespace(
            id="codex-2", codex_home=str(custom_home.resolve()),
            retired=True, cleanup_pending=False, login_recovery_failed=False,
        ),
    ])

    account_id, codex_home = codex_pool_api._allocate_codex_account_home(pool)

    assert account_id == "codex-2"
    assert codex_home == str(custom_home.resolve())


def test_add_account_home_allocator_prefers_lower_empty_gap_over_retired_slot(
    monkeypatch, tmp_path,
):
    monkeypatch.setattr(codex_pool_api.Path, "home", lambda: tmp_path)
    fifth_home = tmp_path / ".codex-codex-5"
    codex_pool_api._purge_retired_codex_home(fifth_home, "codex-5")
    pool = SimpleNamespace(_accounts=[
        SimpleNamespace(
            id="codex-1", codex_home=str((tmp_path / ".codex").resolve()),
            retired=False, cleanup_pending=False, login_recovery_failed=False,
        ),
        SimpleNamespace(
            id="codex-5", codex_home=str(fifth_home.resolve()),
            retired=True, cleanup_pending=False, login_recovery_failed=False,
        ),
    ])

    account_id, codex_home = codex_pool_api._allocate_codex_account_home(pool)

    assert account_id == "codex-2"
    assert codex_home == str(tmp_path / ".codex-codex-2")


@pytest.mark.parametrize(
    ("cleanup_pending", "recovery_failed", "extra_name", "email"),
    [
        (True, False, None, ""),
        (False, True, None, ""),
        (False, False, "auth.json", ""),
        (False, False, None, "stale@example.com"),
    ],
)
def test_add_account_home_allocator_rejects_unsafe_retired_slot(
    monkeypatch, tmp_path, cleanup_pending, recovery_failed, extra_name, email,
):
    monkeypatch.setattr(codex_pool_api.Path, "home", lambda: tmp_path)
    retired_home = tmp_path / ".codex-codex-2"
    codex_pool_api._purge_retired_codex_home(retired_home, "codex-2")
    if extra_name:
        (retired_home / extra_name).write_text("stale identity\n")
    pool = SimpleNamespace(_accounts=[
        SimpleNamespace(
            id="codex-1", codex_home=str((tmp_path / ".codex").resolve()),
            retired=False, cleanup_pending=False, login_recovery_failed=False,
        ),
        SimpleNamespace(
            id="codex-2", codex_home=str(retired_home.resolve()),
            retired=True, cleanup_pending=cleanup_pending,
            login_recovery_failed=recovery_failed, email=email,
        ),
    ])

    account_id, codex_home = codex_pool_api._allocate_codex_account_home(pool)

    assert account_id == "codex-3"
    assert codex_home == str(tmp_path / ".codex-codex-3")


def test_add_account_home_allocator_blocks_pending_login_transaction(
    monkeypatch, tmp_path,
):
    monkeypatch.setattr(codex_pool_api.Path, "home", lambda: tmp_path)
    pool_path = tmp_path / ".codex-pool" / "accounts.json"
    transaction_dir = pool_path.parent / codex_pool_api.LOGIN_TRANSACTION_DIR
    transaction_dir.mkdir(parents=True)
    (transaction_dir / "pending.json").write_text("{}\n")
    pool = SimpleNamespace(_accounts=[], _config_path=pool_path)

    with pytest.raises(HTTPException) as exc_info:
        codex_pool_api._allocate_codex_account_home(pool)

    assert exc_info.value.status_code == 409


@pytest.mark.parametrize("unsafe_pid", [None, -1, 0, 1, False, True])
async def test_stop_unfinished_login_rejects_unsafe_group_without_signal(
    monkeypatch, unsafe_pid,
):
    proc = SimpleNamespace(
        pid=unsafe_pid,
        returncode=None,
        kill=Mock(),
        wait=AsyncMock(),
    )
    killpg = Mock()
    monkeypatch.setattr(codex_pool_api.os, "killpg", killpg)

    with pytest.raises(
        codex_pool_api.LoginProcessNotTerminal,
        match="unsafe process identity",
    ):
        await codex_pool_api._stop_unfinished_login_process(
            proc,
            operation="unsafe-pid test",
        )

    killpg.assert_not_called()
    proc.kill.assert_not_called()
    proc.wait.assert_not_awaited()


async def test_stop_unfinished_login_does_not_spin_under_anyio_cancellation(
    monkeypatch,
):
    from anyio import CancelScope

    wait_started = asyncio.Event()
    allow_exit = asyncio.Event()
    wait_calls = 0
    real_wait = asyncio.wait
    proc = SimpleNamespace(pid=54_344, returncode=None, kill=Mock())

    async def delayed_wait():
        wait_started.set()
        await allow_exit.wait()
        proc.returncode = -9
        return -9

    async def counting_wait(*args, **kwargs):
        nonlocal wait_calls
        if kwargs.get("timeout") is not None:
            wait_calls += 1
        return await real_wait(*args, **kwargs)

    proc.wait = AsyncMock(side_effect=delayed_wait)
    monkeypatch.setattr(codex_pool_api.os, "killpg", Mock())
    monkeypatch.setattr(codex_pool_api.asyncio, "wait", counting_wait)

    async def release_process():
        await wait_started.wait()
        await asyncio.sleep(0)
        allow_exit.set()

    releaser = asyncio.create_task(release_process())
    try:
        with CancelScope() as scope:
            scope.cancel()
            result = await codex_pool_api._stop_unfinished_login_process(
                proc,
                operation="AnyIO cancellation regression",
            )
        await releaser
    finally:
        if not releaser.done():
            releaser.cancel()
            await asyncio.gather(releaser, return_exceptions=True)

    assert result is True
    assert proc.returncode == -9
    assert wait_calls == 1


@pytest.mark.parametrize("watcher_kind", ["add", "relogin"])
async def test_login_watcher_kills_live_process_before_releasing_home(
    monkeypatch, tmp_path, watcher_kind,
):
    proc = SimpleNamespace(pid=54_342, returncode=None, kill=Mock())

    async def finish_killed_process():
        return -9

    proc.wait = AsyncMock(side_effect=finish_killed_process)
    killpg = Mock()

    def kill_wrapper_group(pid, sig):
        assert pid == proc.pid
        assert sig == signal.SIGKILL
        proc.returncode = -9

    killpg.side_effect = kill_wrapper_group
    monkeypatch.setattr(codex_pool_api.os, "killpg", killpg)
    manager = _maintenance_manager()
    login_lock = asyncio.Lock()
    await login_lock.acquire()
    home = tmp_path / ".codex-codex-watch"
    home.mkdir()
    auth_path = home / "auth.json"
    auth_path.write_text("old-auth")
    pool_path = tmp_path / ".codex-pool" / "accounts.json"
    pool_path.parent.mkdir()
    pool_path.write_text(json.dumps({"accounts": []}))
    pool = SimpleNamespace(
        _config_path=pool_path,
        reload=Mock(),
        _quota_cache=None,
    )
    monkeypatch.setattr(codex_pool_api, "_get_pool", lambda: pool)
    journal_path = codex_pool_api._begin_login_transaction(
        attempt_id="attempt-watch",
        kind=watcher_kind,
        account_id="codex-watch",
        codex_home=str(home),
        pool=pool,
    )
    backup = home / ".auth.json.login-backup-current"
    os.replace(auth_path, backup)
    auth_path.write_text("partial-new-auth")
    monkeypatch.setattr(
        codex_pool_api,
        "_collect_login_output",
        AsyncMock(side_effect=RuntimeError("collector failed")),
    )

    if watcher_kind == "add":
        codex_pool_api._add_state.clear()
        await codex_pool_api._watch_add(
            "watch@example.com", "codex-watch", "attempt-watch", proc,
            manager, str(home), login_lock, journal_path,
        )
    else:
        codex_pool_api._relogin_state.clear()
        await codex_pool_api._watch_relogin(
            "codex-watch", "attempt-watch", proc, manager,
            str(home), login_lock, journal_path,
        )

    killpg.assert_called_once_with(proc.pid, signal.SIGKILL)
    proc.kill.assert_not_called()
    proc.wait.assert_awaited_once_with()
    manager.end_codex_app_server_home_maintenance.assert_awaited_once_with(
        str(home)
    )
    assert auth_path.read_text() == "old-auth"
    assert not backup.exists()
    assert not journal_path.exists()
    assert not login_lock.locked()


@pytest.mark.parametrize("watcher_kind", ["add", "relogin"])
@pytest.mark.parametrize("returncode", [-15, -9])
async def test_login_watcher_restores_auth_when_wrapper_already_signaled(
    monkeypatch, tmp_path, watcher_kind, returncode,
):
    """SIGTERM/SIGKILL may land before the watcher enters its finalizer."""

    proc = SimpleNamespace(
        returncode=returncode,
        communicate=AsyncMock(return_value=(b"terminated", b"")),
        kill=Mock(),
        wait=AsyncMock(),
    )
    manager = _maintenance_manager()
    login_lock = asyncio.Lock()
    await login_lock.acquire()
    home = tmp_path / "codex-signaled-watch"
    home.mkdir()
    auth_path = home / "auth.json"
    auth_path.write_text("old-auth")
    pool_path = tmp_path / ".codex-pool" / "accounts.json"
    pool_path.parent.mkdir()
    pool_path.write_text(json.dumps({"accounts": []}))
    pool = SimpleNamespace(
        _config_path=pool_path,
        reload=Mock(),
        _quota_cache=None,
    )
    monkeypatch.setattr(codex_pool_api, "_get_pool", lambda: pool)
    journal_path = codex_pool_api._begin_login_transaction(
        attempt_id="attempt-signaled",
        kind=watcher_kind,
        account_id="codex-watch",
        codex_home=str(home),
        pool=pool,
    )
    backup = home / ".auth.json.login-backup-current"
    os.replace(auth_path, backup)
    auth_path.write_text("partial-new-auth")

    if watcher_kind == "add":
        codex_pool_api._add_state.clear()
        await codex_pool_api._watch_add(
            "watch@example.com", "codex-watch", "attempt-signaled", proc,
            manager, str(home), login_lock, journal_path,
        )
    else:
        codex_pool_api._relogin_state.clear()
        await codex_pool_api._watch_relogin(
            "codex-watch", "attempt-signaled", proc, manager,
            str(home), login_lock, journal_path,
        )

    proc.kill.assert_not_called()
    proc.wait.assert_not_awaited()
    assert auth_path.read_text() == "old-auth"
    assert not backup.exists()
    assert auth_path.stat().st_mode & 0o777 == 0o600
    assert not journal_path.exists()
    manager.end_codex_app_server_home_maintenance.assert_awaited_once_with(
        str(home)
    )
    assert not login_lock.locked()


async def test_relogin_startup_failure_restores_auth_after_preexited_sigterm(
    monkeypatch, tmp_path,
):
    manager = _maintenance_manager()
    account, _pool = _prepare_relogin_account(monkeypatch, tmp_path, manager)
    home = Path(account.codex_home)
    home.mkdir()
    auth_path = home / "auth.json"
    auth_path.write_text("old-auth")
    proc = _FinishedProcess()

    async def reject_credentials(*_args, **_kwargs):
        backup = home / ".auth.json.login-backup-startup"
        os.replace(auth_path, backup)
        auth_path.write_text("partial-new-auth")
        proc.returncode = -15
        raise RuntimeError("wrapper exited while accepting credentials")

    monkeypatch.setattr(
        codex_pool_api.asyncio,
        "create_subprocess_exec",
        AsyncMock(return_value=proc),
    )
    monkeypatch.setattr(
        codex_pool_api, "_send_login_credentials", reject_credentials,
    )

    with pytest.raises(RuntimeError, match="wrapper exited"):
        await codex_pool_api.codex_relogin(_admin_request(), account.id)

    assert auth_path.read_text() == "old-auth"
    assert not list(home.glob(".auth.json.login-backup-*"))
    manager.end_codex_app_server_home_maintenance.assert_awaited_once_with(
        account.codex_home
    )
    assert not codex_pool_api._login_lock.locked()


async def test_add_startup_failure_restores_no_auth_after_preexited_sigkill(
    monkeypatch, tmp_path,
):
    pool = SimpleNamespace(_accounts=[], reload=lambda: None, _quota_cache=None)
    manager = _maintenance_manager()
    proc = _FinishedProcess()
    monkeypatch.setattr(codex_pool_api.Path, "home", lambda: tmp_path)
    monkeypatch.setattr(codex_pool_api, "_get_pool", lambda: pool)
    monkeypatch.setattr(codex_pool_api, "_get_instance_manager", lambda: manager)
    monkeypatch.setattr(codex_pool_api, "_ensure_xvfb", AsyncMock())
    monkeypatch.setattr(codex_pool_api, "_login_lock", asyncio.Lock())
    monkeypatch.setattr(
        codex_pool_api.asyncio,
        "create_subprocess_exec",
        AsyncMock(return_value=proc),
    )
    codex_pool_api._add_state.clear()
    codex_home = tmp_path / ".codex"

    async def reject_credentials(*_args, **_kwargs):
        codex_home.mkdir(exist_ok=True)
        (codex_home / "auth.json").write_text("partial-new-auth")
        proc.returncode = -9
        raise RuntimeError("wrapper exited while accepting credentials")

    monkeypatch.setattr(
        codex_pool_api, "_send_login_credentials", reject_credentials,
    )

    with pytest.raises(RuntimeError, match="wrapper exited"):
        await codex_pool_api.codex_add_account(
            _admin_request(),
            codex_pool_api.AddCodexAccountRequest(
                email="failed@mail.com",
                password="openai-password",
                login_method="mailcom",
            ),
        )

    assert not (codex_home / "auth.json").exists()
    manager.end_codex_app_server_home_maintenance.assert_awaited_once_with(
        str(codex_home)
    )
    assert not codex_pool_api._login_lock.locked()


@pytest.mark.parametrize("watcher_kind", ["add", "relogin"])
async def test_login_watcher_delays_cancellation_until_reap_and_rollback(
    monkeypatch, tmp_path, watcher_kind,
):
    manager = _maintenance_manager()
    login_lock = asyncio.Lock()
    await login_lock.acquire()
    home = tmp_path / "codex-cancelled-watch"
    home.mkdir()
    auth_path = home / "auth.json"
    auth_path.write_text("old-auth")
    pool_path = tmp_path / ".codex-pool" / "accounts.json"
    pool_path.parent.mkdir()
    pool_path.write_text(json.dumps({"accounts": []}))
    pool = SimpleNamespace(
        _config_path=pool_path,
        reload=Mock(),
        _quota_cache=None,
    )
    monkeypatch.setattr(codex_pool_api, "_get_pool", lambda: pool)
    attempt_id = f"cancel-{watcher_kind}"
    journal_path = codex_pool_api._begin_login_transaction(
        attempt_id=attempt_id,
        kind=watcher_kind,
        account_id="codex-cancel",
        codex_home=str(home),
        pool=pool,
    )
    backup = home / ".auth.json.login-backup-cancel"
    os.replace(auth_path, backup)
    auth_path.write_text("partial-new-auth")
    wait_started = asyncio.Event()
    allow_exit = asyncio.Event()
    proc = SimpleNamespace(
        pid=54_343,
        returncode=None,
        kill=Mock(),
    )

    async def wait_for_terminal():
        wait_started.set()
        await allow_exit.wait()
        proc.returncode = -9
        return -9

    proc.wait = AsyncMock(side_effect=wait_for_terminal)
    killpg = Mock()

    def kill_wrapper_group(pid, sig):
        assert pid == proc.pid
        assert sig == signal.SIGKILL

    killpg.side_effect = kill_wrapper_group
    monkeypatch.setattr(codex_pool_api.os, "killpg", killpg)
    monkeypatch.setattr(
        codex_pool_api,
        "_collect_login_output",
        AsyncMock(side_effect=RuntimeError("collector failed")),
    )

    if watcher_kind == "add":
        watcher = asyncio.create_task(codex_pool_api._watch_add(
            "cancel@example.com", "codex-cancel", attempt_id, proc, manager,
            str(home), login_lock, journal_path,
        ))
    else:
        watcher = asyncio.create_task(codex_pool_api._watch_relogin(
            "codex-cancel", attempt_id, proc, manager, str(home), login_lock,
            journal_path,
        ))

    await asyncio.wait_for(wait_started.wait(), timeout=1)
    watcher.cancel()
    await asyncio.sleep(0)
    assert login_lock.locked()
    manager.end_codex_app_server_home_maintenance.assert_not_awaited()
    assert auth_path.read_text() == "partial-new-auth"

    allow_exit.set()
    with pytest.raises(asyncio.CancelledError):
        await watcher

    killpg.assert_called_once_with(proc.pid, signal.SIGKILL)
    proc.kill.assert_not_called()
    proc.wait.assert_awaited_once_with()
    assert auth_path.read_text() == "old-auth"
    assert not backup.exists()
    assert not journal_path.exists()
    manager.end_codex_app_server_home_maintenance.assert_awaited_once_with(
        str(home)
    )
    assert not login_lock.locked()


async def test_login_rollback_failure_quarantines_and_disables_account(
    monkeypatch, tmp_path,
):
    home = tmp_path / ".codex-codex-quarantine"
    home.mkdir()
    auth_path = home / "auth.json"
    auth_path.write_text("old-auth")
    pool_path = tmp_path / ".codex-pool" / "accounts.json"
    pool_path.parent.mkdir()
    pool_path.write_text(json.dumps({"accounts": [{
        "id": "codex-quarantine",
        "codex_home": str(home),
        "enabled": True,
    }]}))
    pool = SimpleNamespace(
        _config_path=pool_path,
        reload=Mock(),
        _quota_cache=None,
    )
    monkeypatch.setattr(codex_pool_api, "_get_pool", lambda: pool)
    journal_path = codex_pool_api._begin_login_transaction(
        attempt_id="quarantine-attempt",
        kind="relogin",
        account_id="codex-quarantine",
        codex_home=str(home),
        pool=pool,
    )
    auth_path.write_text("partial-new-auth")
    monkeypatch.setattr(
        codex_pool_api,
        "_rollback_login_transaction",
        Mock(side_effect=OSError("snapshot storage unavailable")),
    )
    manager = _maintenance_manager()
    login_lock = asyncio.Lock()
    await login_lock.acquire()
    state: dict[str, dict] = {}
    proc = SimpleNamespace(returncode=-15)

    result = await codex_pool_api._finalize_login_transaction(
        proc=proc,
        operation="quarantine test",
        journal_path=journal_path,
        commit_requested=False,
        instance_manager=manager,
        codex_home=str(home),
        login_lock=login_lock,
        attempt_id="quarantine-attempt",
        state_store=state,
        state_key="codex-quarantine",
    )

    assert result["cleanup_safe"] is True
    assert result["committed"] is False
    assert state["codex-quarantine"]["status"] == "recovery_failed"
    assert not auth_path.exists()
    assert (
        home / ".auth.json.ccm-quarantine-quarantine-attempt"
    ).read_text() == "partial-new-auth"
    assert (home / ".ccm-login-recovery-failed").exists()
    account = json.loads(pool_path.read_text())["accounts"][0]
    assert account["enabled"] is False
    assert account["login_recovery_failed"] is True
    assert journal_path.exists()
    manager.end_codex_app_server_home_maintenance.assert_awaited_once_with(
        str(home)
    )
    assert not login_lock.locked()


async def test_unconfirmed_wrapper_keeps_home_maintenance_and_journal(
    monkeypatch, tmp_path,
):
    home = tmp_path / ".codex-codex-unconfirmed"
    home.mkdir()
    auth_path = home / "auth.json"
    auth_path.write_text("old-auth")
    pool_path = tmp_path / ".codex-pool" / "accounts.json"
    pool_path.parent.mkdir()
    pool_path.write_text('{"accounts": []}\n')
    pool = SimpleNamespace(
        _config_path=pool_path,
        reload=Mock(),
        _quota_cache=None,
    )
    monkeypatch.setattr(codex_pool_api, "_get_pool", lambda: pool)
    monkeypatch.setattr(codex_pool_api, "LOGIN_REAP_TIMEOUT_SECONDS", 0)
    journal_path = codex_pool_api._begin_login_transaction(
        attempt_id="unconfirmed-wrapper",
        kind="relogin",
        account_id="codex-unconfirmed",
        codex_home=str(home),
        pool=pool,
    )
    auth_path.write_text("partial-new-auth")
    proc = SimpleNamespace(
        pid=54_344,
        returncode=None,
        kill=Mock(),
        wait=AsyncMock(side_effect=asyncio.Event().wait),
    )
    killpg = Mock()
    monkeypatch.setattr(codex_pool_api.os, "killpg", killpg)
    manager = _maintenance_manager()
    login_lock = asyncio.Lock()
    await login_lock.acquire()
    state: dict[str, dict] = {"codex-unconfirmed": {"status": "running"}}

    result = await codex_pool_api._finalize_login_transaction(
        proc=proc,
        operation="unconfirmed test",
        journal_path=journal_path,
        commit_requested=False,
        instance_manager=manager,
        codex_home=str(home),
        login_lock=login_lock,
        attempt_id="unconfirmed-wrapper",
        state_store=state,
        state_key="codex-unconfirmed",
    )

    assert result["cleanup_safe"] is False
    assert state["codex-unconfirmed"]["status"] == "recovery_failed"
    killpg.assert_called_once_with(proc.pid, signal.SIGKILL)
    proc.kill.assert_not_called()
    # A zero reap deadline cancels the scheduled waiter before its coroutine
    # receives a loop tick, but the exact wait operation must still be created.
    proc.wait.assert_called_once_with()
    assert auth_path.read_text() == "partial-new-auth"
    assert journal_path.exists()
    manager.end_codex_app_server_home_maintenance.assert_not_awaited()
    assert not login_lock.locked()


def test_startup_recovery_rolls_back_add_half_commit(monkeypatch, tmp_path):
    home = tmp_path / ".codex-codex-restart"
    home.mkdir()
    auth_path = home / "auth.json"
    auth_path.write_bytes(b"old-auth")
    pool_path = tmp_path / ".codex-pool" / "accounts.json"
    tokens_path = pool_path.parent / "email_tokens.json"
    pool_path.parent.mkdir()
    old_pool = b'{"accounts": []}\n'
    old_tokens = b'{"old@example.com": {"password": "old"}}\n'
    pool_path.write_bytes(old_pool)
    tokens_path.write_bytes(old_tokens)
    pool = SimpleNamespace(_config_path=pool_path)
    journal_path = codex_pool_api._begin_login_transaction(
        attempt_id="restart-add",
        kind="add",
        account_id="codex-restart",
        codex_home=str(home),
        pool=pool,
    )
    assert journal_path.stat().st_mode & 0o777 == 0o600
    assert journal_path.parent.stat().st_mode & 0o777 == 0o700

    # Simulate SIGKILL after wrapper wrote auth + credential store but before
    # its second accounts.json write and before the parent watcher could commit.
    auth_path.write_bytes(b"new-auth")
    tokens_path.write_bytes(b'{"new@example.com": {"password": "orphan"}}\n')
    assert journal_path.exists()

    recovered = codex_pool_api.recover_pending_codex_login_transactions(pool_path)

    assert recovered == {"recovered": ["restart-add"], "quarantined": []}
    assert auth_path.read_bytes() == b"old-auth"
    assert pool_path.read_bytes() == old_pool
    assert tokens_path.read_bytes() == old_tokens
    assert not journal_path.exists()


def test_startup_recovery_restores_relogin_after_wrapper_deleted_backup(
    tmp_path,
):
    home = tmp_path / ".codex-codex-relogin-restart"
    home.mkdir()
    auth_path = home / "auth.json"
    auth_path.write_bytes(b"old-auth")
    pool_path = tmp_path / ".codex-pool" / "accounts.json"
    pool_path.parent.mkdir()
    pool_path.write_text(json.dumps({"accounts": [{
        "id": "codex-restart",
        "codex_home": str(home),
        "enabled": True,
    }]}))
    pool = SimpleNamespace(_config_path=pool_path)
    journal_path = codex_pool_api._begin_login_transaction(
        attempt_id="restart-relogin",
        kind="relogin",
        account_id="codex-restart",
        codex_home=str(home),
        pool=pool,
    )

    # The wrapper has already installed new auth and removed its short-lived
    # backup, then systemd kills both wrapper and watcher before parent commit.
    auth_path.write_bytes(b"new-auth")
    assert not list(home.glob(".auth.json.login-backup-*"))

    recovered = codex_pool_api.recover_pending_codex_login_transactions(pool_path)

    assert recovered == {
        "recovered": ["restart-relogin"],
        "quarantined": [],
    }
    assert auth_path.read_bytes() == b"old-auth"
    assert not journal_path.exists()


def test_main_startup_recovers_journal_even_when_pool_is_disabled(tmp_path):
    home = tmp_path / ".codex"
    home.mkdir()
    auth_path = home / "auth.json"
    auth_path.write_bytes(b"old-auth")
    pool_path = tmp_path / ".codex-pool" / "accounts.json"
    pool_path.parent.mkdir()
    pool_path.write_text('{"accounts": []}\n')
    pool = SimpleNamespace(_config_path=pool_path)
    journal_path = codex_pool_api._begin_login_transaction(
        attempt_id="disabled-pool-restart",
        kind="relogin",
        account_id="codex-1",
        codex_home=str(home),
        pool=pool,
    )
    auth_path.write_bytes(b"partial-new-auth")
    env = os.environ.copy()
    env.update({
        "CODEX_POOL_ENABLED": "false",
        "CODEX_POOL_CONFIG_PATH": str(pool_path),
    })

    result = subprocess.run(
        [sys.executable, "-c", "import backend.main"],
        cwd=Path(__file__).resolve().parents[2],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    assert auth_path.read_bytes() == b"old-auth"
    assert not journal_path.exists()


def test_login_transaction_rejects_symlink_pool_config(tmp_path):
    home = tmp_path / ".codex"
    home.mkdir()
    (home / "auth.json").write_text("old-auth")
    real_pool = tmp_path / "real-accounts.json"
    real_pool.write_text('{"accounts": []}\n')
    linked_pool = tmp_path / "linked-accounts.json"
    linked_pool.symlink_to(real_pool)
    pool = SimpleNamespace(_config_path=linked_pool)

    with pytest.raises(RuntimeError, match="symlink Codex pool config"):
        codex_pool_api._begin_login_transaction(
            attempt_id="symlink-pool",
            kind="relogin",
            account_id="codex-1",
            codex_home=str(home),
            pool=pool,
        )

    assert (home / "auth.json").read_text() == "old-auth"
    assert not (tmp_path / codex_pool_api.LOGIN_TRANSACTION_DIR).exists()


def test_startup_recovery_rejects_tampered_snapshot_path(tmp_path):
    home = tmp_path / ".codex"
    home.mkdir()
    (home / "auth.json").write_text("old-auth")
    outside = tmp_path / "outside-secret"
    outside.write_text("must-not-change")
    pool_path = tmp_path / ".codex-pool" / "accounts.json"
    pool_path.parent.mkdir()
    pool_path.write_text('{"accounts": []}\n')
    pool = SimpleNamespace(_config_path=pool_path)
    journal_path = codex_pool_api._begin_login_transaction(
        attempt_id="tampered-path",
        kind="relogin",
        account_id="codex-1",
        codex_home=str(home),
        pool=pool,
    )
    journal = json.loads(journal_path.read_text())
    journal["auth"]["path"] = str(outside)
    codex_pool_api._write_private_json(journal_path, journal)
    (home / "auth.json").write_text("partial-auth")

    with pytest.raises(RuntimeError, match="Invalid auth snapshot"):
        codex_pool_api.recover_pending_codex_login_transactions(pool_path)

    assert outside.read_text() == "must-not-change"
    assert journal_path.exists()


def test_startup_recovery_rejects_different_pool_file_in_same_directory(tmp_path):
    home = tmp_path / ".codex"
    home.mkdir()
    auth_path = home / "auth.json"
    auth_path.write_text("old-auth")
    pool_dir = tmp_path / ".codex-pool"
    pool_dir.mkdir()
    original_pool = pool_dir / "accounts-a.json"
    configured_pool = pool_dir / "accounts-b.json"
    original_pool.write_text('{"accounts": []}\n')
    configured_pool.write_text('{"accounts": [{"id": "must-stay"}]}\n')
    pool = SimpleNamespace(_config_path=original_pool)
    journal_path = codex_pool_api._begin_login_transaction(
        attempt_id="pool-path-mismatch",
        kind="relogin",
        account_id="codex-1",
        codex_home=str(home),
        pool=pool,
    )
    auth_path.write_text("partial-auth")

    with pytest.raises(RuntimeError, match="Mismatched pool snapshot path"):
        codex_pool_api.recover_pending_codex_login_transactions(configured_pool)

    assert auth_path.read_text() == "partial-auth"
    assert original_pool.read_text() == '{"accounts": []}\n'
    assert configured_pool.read_text() == '{"accounts": [{"id": "must-stay"}]}\n'
    assert journal_path.exists()


@pytest.mark.parametrize("dangling", [False, True])
def test_login_quarantine_unlinks_auth_symlink_without_touching_target(
    tmp_path, dangling,
):
    home = tmp_path / ".codex"
    home.mkdir()
    auth_path = home / "auth.json"
    auth_path.write_text("old-auth")
    pool_path = tmp_path / ".codex-pool" / "accounts.json"
    pool_path.parent.mkdir()
    pool_path.write_text(json.dumps({"accounts": [{
        "id": "codex-1",
        "codex_home": str(home),
        "enabled": True,
    }]}))
    pool = SimpleNamespace(_config_path=pool_path)
    journal_path = codex_pool_api._begin_login_transaction(
        attempt_id=f"symlink-quarantine-{dangling}",
        kind="relogin",
        account_id="codex-1",
        codex_home=str(home),
        pool=pool,
    )
    auth_path.unlink()
    target = tmp_path / "external-auth-target"
    if not dangling:
        target.write_text("external-secret")
        target.chmod(0o640)
    auth_path.symlink_to(target)

    assert codex_pool_api._quarantine_login_transaction(
        journal_path,
        "forced rollback failure",
        expected_pool_path=pool_path.resolve(),
    ) is True

    assert not auth_path.exists()
    assert not auth_path.is_symlink()
    if dangling:
        assert not target.exists()
    else:
        assert target.read_text() == "external-secret"
        assert target.stat().st_mode & 0o777 == 0o640
    account = json.loads(pool_path.read_text())["accounts"][0]
    assert account["enabled"] is False
    assert account["login_recovery_failed"] is True
    assert journal_path.exists()


def test_login_rollback_fsyncs_removed_home_artifacts_before_journal_delete(
    monkeypatch, tmp_path,
):
    home = tmp_path / ".codex"
    home.mkdir()
    auth_path = home / "auth.json"
    auth_path.write_text("old-auth")
    pool_path = tmp_path / ".codex-pool" / "accounts.json"
    pool_path.parent.mkdir()
    pool_path.write_text('{"accounts": []}\n')
    pool = SimpleNamespace(_config_path=pool_path)
    journal_path = codex_pool_api._begin_login_transaction(
        attempt_id="artifact-fsync",
        kind="relogin",
        account_id="codex-1",
        codex_home=str(home),
        pool=pool,
    )
    backup = home / ".auth.json.login-backup-new"
    os.replace(auth_path, backup)
    auth_path.write_text("partial-auth")
    marker = home / ".ccm-login-recovery-failed"
    marker.write_text("partial")
    artifact_sync_seen = False
    original_fsync_directory = codex_pool_api._fsync_directory
    original_remove = codex_pool_api._remove_login_transaction

    def observe_fsync(path):
        nonlocal artifact_sync_seen
        original_fsync_directory(path)
        if path == home and not backup.exists() and not marker.exists():
            artifact_sync_seen = True

    def observe_remove(path):
        assert artifact_sync_seen is True
        original_remove(path)

    monkeypatch.setattr(codex_pool_api, "_fsync_directory", observe_fsync)
    monkeypatch.setattr(codex_pool_api, "_remove_login_transaction", observe_remove)

    codex_pool_api._rollback_login_transaction(
        journal_path, expected_pool_path=pool_path.resolve(),
    )

    assert auth_path.read_text() == "old-auth"
    assert not backup.exists()
    assert not marker.exists()
    assert not journal_path.exists()


async def test_successful_watcher_commit_preserves_new_files_and_removes_journal(
    monkeypatch, tmp_path,
):
    home = tmp_path / ".codex-codex-commit"
    home.mkdir()
    auth_path = home / "auth.json"
    auth_path.write_bytes(b"old-auth")
    pool_path = tmp_path / ".codex-pool" / "accounts.json"
    tokens_path = pool_path.parent / "email_tokens.json"
    pool_path.parent.mkdir()
    pool_path.write_bytes(b'{"accounts": []}\n')
    tokens_path.write_bytes(b'{}\n')
    pool = SimpleNamespace(
        _config_path=pool_path,
        reload=Mock(),
        _quota_cache=None,
    )
    monkeypatch.setattr(codex_pool_api, "_get_pool", lambda: pool)
    journal_path = codex_pool_api._begin_login_transaction(
        attempt_id="commit-add",
        kind="add",
        account_id="codex-commit",
        codex_home=str(home),
        pool=pool,
    )
    auth_path.write_bytes(b"new-auth")
    pool_path.write_bytes(b'{"accounts": [{"id": "codex-commit"}]}\n')
    tokens_path.write_bytes(b'{"new@example.com": {"password": "new"}}\n')
    manager = _maintenance_manager()
    login_lock = asyncio.Lock()
    await login_lock.acquire()
    state: dict[str, dict] = {"new@example.com": {"status": "running"}}
    proc = SimpleNamespace(returncode=0)

    result = await codex_pool_api._finalize_login_transaction(
        proc=proc,
        operation="commit test",
        journal_path=journal_path,
        commit_requested=True,
        instance_manager=manager,
        codex_home=str(home),
        login_lock=login_lock,
        attempt_id="commit-add",
        state_store=state,
        state_key="new@example.com",
    )

    assert result["committed"] is True
    assert auth_path.read_bytes() == b"new-auth"
    assert b"codex-commit" in pool_path.read_bytes()
    assert b"new@example.com" in tokens_path.read_bytes()
    assert not journal_path.exists()
    assert state["new@example.com"]["status"] == "success"
    manager.end_codex_app_server_home_maintenance.assert_awaited_once()
    assert not login_lock.locked()


async def test_reused_retired_slot_commit_removes_marker_after_commit(
    monkeypatch, tmp_path,
):
    monkeypatch.setattr(codex_pool_api.Path, "home", lambda: tmp_path)
    home = tmp_path / ".codex-codex-2"
    codex_pool_api._purge_retired_codex_home(home, "codex-2")
    marker = home / ".ccm-retired-account"
    pool_path = tmp_path / ".codex-pool" / "accounts.json"
    tokens_path = pool_path.parent / "email_tokens.json"
    pool_path.parent.mkdir()
    pool_path.write_text(json.dumps({"accounts": [{
        "id": "codex-2",
        "codex_home": str(home),
        "retired": True,
        "enabled": False,
    }]}))
    tokens_path.write_text("{}\n")
    pool = SimpleNamespace(
        _config_path=pool_path,
        reload=Mock(),
        _quota_cache=None,
    )
    monkeypatch.setattr(codex_pool_api, "_get_pool", lambda: pool)
    journal_path = codex_pool_api._begin_login_transaction(
        attempt_id="reuse-commit",
        kind="add",
        account_id="codex-2",
        codex_home=str(home),
        pool=pool,
        reused_retired_slot=True,
        expected_email="replacement@example.com",
    )
    (home / "auth.json").write_text("new-auth\n")
    pool_path.write_text(json.dumps({"accounts": [{
        "id": "codex-2",
        "codex_home": str(home),
        "email": "replacement@example.com",
        "enabled": True,
        "quota_valid_after": codex_pool_api.time.time() + 1,
    }]}))
    tokens_path.write_text(json.dumps({
        "replacement@example.com": {"password": "new-password"},
    }))
    login_lock = asyncio.Lock()
    await login_lock.acquire()

    result = await codex_pool_api._finalize_login_transaction(
        proc=SimpleNamespace(returncode=0),
        operation="retired slot commit",
        journal_path=journal_path,
        commit_requested=True,
        instance_manager=_maintenance_manager(),
        codex_home=str(home),
        login_lock=login_lock,
        attempt_id="reuse-commit",
        state_store={"replacement@example.com": {"status": "finalizing"}},
        state_key="replacement@example.com",
        expected_pool_path=pool_path.resolve(),
    )

    assert result["committed"] is True
    assert not journal_path.exists()
    assert not marker.exists()
    assert (home / "auth.json").read_text() == "new-auth\n"


def test_reused_retired_slot_rollback_restores_reusable_home(
    monkeypatch, tmp_path,
):
    monkeypatch.setattr(codex_pool_api.Path, "home", lambda: tmp_path)
    home = tmp_path / ".codex-codex-2"
    rollout = home / "sessions" / "2026" / "07" / "21" / "rollout.jsonl"
    rollout.parent.mkdir(parents=True)
    rollout.write_text("old session\n")
    codex_pool_api._purge_retired_codex_home(home, "codex-2")
    pool_path = tmp_path / ".codex-pool" / "accounts.json"
    tokens_path = pool_path.parent / "email_tokens.json"
    pool_path.parent.mkdir()
    retired_pool = {"accounts": [{
        "id": "codex-2",
        "codex_home": str(home),
        "email": "",
        "enabled": False,
        "retired": True,
    }]}
    pool_path.write_text(json.dumps(retired_pool))
    tokens_path.write_text("{}\n")
    pool = SimpleNamespace(_config_path=pool_path)
    journal_path = codex_pool_api._begin_login_transaction(
        attempt_id="reuse-rollback",
        kind="add",
        account_id="codex-2",
        codex_home=str(home),
        pool=pool,
        reused_retired_slot=True,
        expected_email="partial@example.com",
    )

    # Simulate a failed wrapper leaving all the identity-specific files that
    # made the old allocator permanently skip this number.
    (home / "auth.json").write_text("partial-auth\n")
    (home / "models_cache.json").write_text("{}\n")
    (home / "config.toml").write_text("new identity\n")
    (home / "log").mkdir()
    (home / "log" / "codex.log").write_text("partial login\n")
    (home / ".ccm-retired-account").unlink()
    pool_path.write_text(json.dumps({"accounts": [{
        "id": "codex-2", "codex_home": str(home), "enabled": True,
    }]}))
    tokens_path.write_text(json.dumps({
        "partial@example.com": {"password": "partial"},
    }))

    recovery = codex_pool_api.recover_pending_codex_login_transactions(
        pool_path,
    )

    assert recovery == {"recovered": ["reuse-rollback"], "quarantined": []}
    assert not journal_path.exists()
    assert json.loads(pool_path.read_text()) == retired_pool
    assert json.loads(tokens_path.read_text()) == {}
    assert rollout.read_text() == "old session\n"
    assert {path.name for path in home.iterdir()} == {
        "sessions", ".ccm-retired-account",
    }
    assert (home / ".ccm-retired-account").read_text().strip() == "codex-2"

    reusable = SimpleNamespace(
        id="codex-2",
        codex_home=str(home.resolve()),
        retired=True,
        cleanup_pending=False,
        login_recovery_failed=False,
    )
    assert codex_pool_api._clean_retired_home_is_reusable(reusable) is True


def test_reused_retired_slot_rollback_rejects_tampered_active_snapshot(
    monkeypatch, tmp_path,
):
    monkeypatch.setattr(codex_pool_api.Path, "home", lambda: tmp_path)
    home = tmp_path / ".codex-codex-2"
    codex_pool_api._purge_retired_codex_home(home, "codex-2")
    pool_path = tmp_path / ".codex-pool" / "accounts.json"
    pool_path.parent.mkdir()
    pool_path.write_text(json.dumps({"accounts": [{
        "id": "codex-2", "codex_home": str(home),
        "email": "", "enabled": False, "retired": True,
    }]}))
    (pool_path.parent / "email_tokens.json").write_text("{}\n")
    pool = SimpleNamespace(_config_path=pool_path)
    journal_path = codex_pool_api._begin_login_transaction(
        attempt_id="reuse-tampered",
        kind="add",
        account_id="codex-2",
        codex_home=str(home),
        pool=pool,
        reused_retired_slot=True,
        expected_email="replacement@example.com",
    )
    journal = json.loads(journal_path.read_text())
    active_snapshot = json.dumps({"accounts": [{
        "id": "codex-2", "codex_home": str(home),
        "email": "old@example.com", "enabled": True,
    }]}).encode()
    journal["pool_config"]["content_b64"] = base64.b64encode(
        active_snapshot
    ).decode()
    codex_pool_api._write_private_json(journal_path, journal)
    sentinel = home / "must-not-purge"
    sentinel.write_text("keep\n")

    with pytest.raises(RuntimeError, match="not a finalized retired account"):
        codex_pool_api._rollback_login_transaction(
            journal_path,
            expected_pool_path=pool_path.resolve(),
        )

    assert journal_path.exists()
    assert sentinel.read_text() == "keep\n"


async def test_reused_retired_slot_commit_rejects_duplicate_registration(
    monkeypatch, tmp_path,
):
    monkeypatch.setattr(codex_pool_api.Path, "home", lambda: tmp_path)
    home = tmp_path / ".codex-codex-2"
    rollout = home / "sessions" / "2026" / "07" / "21" / "rollout.jsonl"
    rollout.parent.mkdir(parents=True)
    rollout.write_text("session\n")
    codex_pool_api._purge_retired_codex_home(home, "codex-2")
    pool_path = tmp_path / ".codex-pool" / "accounts.json"
    pool_path.parent.mkdir()
    retired_pool = {"accounts": [{
        "id": "codex-2", "codex_home": str(home),
        "email": "", "enabled": False, "retired": True,
    }]}
    pool_path.write_text(json.dumps(retired_pool))
    tokens_path = pool_path.parent / "email_tokens.json"
    tokens_path.write_text("{}\n")
    pool = SimpleNamespace(
        _config_path=pool_path, reload=Mock(), _quota_cache=None,
    )
    monkeypatch.setattr(codex_pool_api, "_get_pool", lambda: pool)
    journal_path = codex_pool_api._begin_login_transaction(
        attempt_id="reuse-duplicate",
        kind="add",
        account_id="codex-2",
        codex_home=str(home),
        pool=pool,
        reused_retired_slot=True,
        expected_email="replacement@example.com",
    )
    (home / "auth.json").write_text("new auth\n")
    active_record = {
        "id": "codex-2", "codex_home": str(home),
        "email": "replacement@example.com", "enabled": True,
        "quota_valid_after": codex_pool_api.time.time() + 1,
    }
    pool_path.write_text(json.dumps({"accounts": [active_record, active_record]}))
    tokens_path.write_text(json.dumps({
        "replacement@example.com": {"password": "new"},
    }))
    login_lock = asyncio.Lock()
    await login_lock.acquire()

    result = await codex_pool_api._finalize_login_transaction(
        proc=SimpleNamespace(returncode=0),
        operation="duplicate reused registration",
        journal_path=journal_path,
        commit_requested=True,
        instance_manager=_maintenance_manager(),
        codex_home=str(home),
        login_lock=login_lock,
        attempt_id="reuse-duplicate",
        state_store={"replacement@example.com": {"status": "finalizing"}},
        state_key="replacement@example.com",
        expected_pool_path=pool_path.resolve(),
    )

    assert result["committed"] is False
    assert not journal_path.exists()
    assert json.loads(pool_path.read_text()) == retired_pool
    assert rollout.read_text() == "session\n"
    assert {path.name for path in home.iterdir()} == {
        "sessions", ".ccm-retired-account",
    }


async def test_successful_exit_does_not_publish_success_until_commit_barrier(
    monkeypatch, tmp_path,
):
    home = tmp_path / ".codex"
    home.mkdir()
    auth_path = home / "auth.json"
    auth_path.write_text("old-auth")
    pool_path = tmp_path / ".codex-pool" / "accounts.json"
    pool_path.parent.mkdir()
    pool_path.write_text('{"accounts": []}\n')
    pool = SimpleNamespace(
        _config_path=pool_path,
        reload=Mock(),
        _quota_cache=None,
    )
    monkeypatch.setattr(codex_pool_api, "_get_pool", lambda: pool)
    journal_path = codex_pool_api._begin_login_transaction(
        attempt_id="barrier-failure",
        kind="relogin",
        account_id="codex-1",
        codex_home=str(home),
        pool=pool,
    )
    auth_path.unlink()
    external = tmp_path / "external-auth"
    external.write_text("do-not-touch")
    external.chmod(0o640)
    auth_path.symlink_to(external)
    manager = _maintenance_manager()
    login_lock = asyncio.Lock()
    await login_lock.acquire()
    state = {"codex-1": {"status": "finalizing"}}

    result = await codex_pool_api._finalize_login_transaction(
        proc=SimpleNamespace(returncode=0),
        operation="commit barrier failure",
        journal_path=journal_path,
        commit_requested=True,
        instance_manager=manager,
        codex_home=str(home),
        login_lock=login_lock,
        attempt_id="barrier-failure",
        state_store=state,
        state_key="codex-1",
        expected_pool_path=pool_path.resolve(),
    )

    assert result["committed"] is False
    assert result["cleanup_safe"] is True
    assert state["codex-1"]["status"] == "failed"
    assert "commit validation failed" in state["codex-1"]["detail"].lower()
    assert auth_path.read_text() == "old-auth"
    assert not auth_path.is_symlink()
    assert external.read_text() == "do-not-touch"
    assert external.stat().st_mode & 0o777 == 0o640
    assert not journal_path.exists()
    assert not login_lock.locked()


async def test_add_commit_fsyncs_all_files_before_journal_removal(
    monkeypatch, tmp_path,
):
    home = tmp_path / ".codex"
    home.mkdir()
    auth_path = home / "auth.json"
    auth_path.write_text("old-auth")
    pool_path = tmp_path / ".codex-pool" / "accounts.json"
    tokens_path = pool_path.parent / "email_tokens.json"
    pool_path.parent.mkdir()
    pool_path.write_text('{"accounts": []}\n')
    tokens_path.write_text('{}\n')
    pool = SimpleNamespace(
        _config_path=pool_path,
        reload=Mock(),
        _quota_cache=None,
    )
    monkeypatch.setattr(codex_pool_api, "_get_pool", lambda: pool)
    journal_path = codex_pool_api._begin_login_transaction(
        attempt_id="commit-order",
        kind="add",
        account_id="codex-1",
        codex_home=str(home),
        pool=pool,
    )
    events = []
    original_remove = codex_pool_api._remove_login_transaction

    monkeypatch.setattr(
        codex_pool_api,
        "_fsync_regular_file_and_parent",
        lambda path: events.append(("fsync", path)),
    )

    def observe_remove(path):
        events.append(("remove", path))
        original_remove(path)

    monkeypatch.setattr(codex_pool_api, "_remove_login_transaction", observe_remove)
    login_lock = asyncio.Lock()
    await login_lock.acquire()

    result = await codex_pool_api._finalize_login_transaction(
        proc=SimpleNamespace(returncode=0),
        operation="commit ordering",
        journal_path=journal_path,
        commit_requested=True,
        instance_manager=_maintenance_manager(),
        codex_home=str(home),
        login_lock=login_lock,
        attempt_id="commit-order",
        state_store={"new@example.com": {"status": "finalizing"}},
        state_key="new@example.com",
        expected_pool_path=pool_path.resolve(),
    )

    assert result["committed"] is True
    assert events == [
        ("fsync", auth_path),
        ("fsync", tokens_path),
        ("fsync", pool_path),
        ("remove", journal_path),
    ]


def test_private_json_writer_never_exposes_group_read_permissions(
    monkeypatch, tmp_path,
):
    destination = tmp_path / "private" / "email_tokens.json"
    fsync_directory = Mock(wraps=codex_login._fsync_directory)
    monkeypatch.setattr(codex_login, "_fsync_directory", fsync_directory)

    codex_login._write_private_json(destination, {"user@example.com": {"password": "p"}})

    assert destination.stat().st_mode & 0o777 == 0o600
    assert destination.parent.stat().st_mode & 0o777 == 0o700
    assert not list(destination.parent.glob(".email_tokens.json.*.tmp"))
    fsync_directory.assert_called_once_with(destination.parent)


def test_pool_login_registration_rolls_back_credentials_when_pool_write_fails(
    monkeypatch, tmp_path,
):
    pool_path = tmp_path / "accounts.json"
    tokens_path = tmp_path / "email_tokens.json"
    original_pool = {"accounts": [{
        "id": "codex-1",
        "codex_home": "/tmp/.codex",
        "email": "existing@example.com",
        "enabled": True,
    }]}
    original_tokens = {
        "existing@example.com": {"password": "existing-password"},
    }
    pool_path.write_text(json.dumps(original_pool))
    tokens_path.write_text(json.dumps(original_tokens))
    real_write = codex_login._write_private_json

    def fail_pool_write(path, data):
        if path == pool_path:
            raise OSError("pool disk full")
        real_write(path, data)

    monkeypatch.setattr(codex_login, "_write_private_json", fail_pool_write)

    with pytest.raises(OSError, match="pool disk full"):
        codex_login._persist_new_pool_login(
            account_id="codex-2",
            email="new@example.com",
            codex_home="/tmp/.codex-codex-2",
            token="mail-token",
            password="openai-password",
            provider="mailcatcher",
            pool_path=pool_path,
            tokens_path=tokens_path,
        )

    assert json.loads(tokens_path.read_text()) == original_tokens
    assert json.loads(pool_path.read_text()) == original_pool


def test_pool_login_registration_reactivates_tombstone_with_quota_cutoff(
    monkeypatch, tmp_path,
):
    pool_path = tmp_path / "accounts.json"
    tokens_path = tmp_path / "email_tokens.json"
    pool_path.write_text(json.dumps({"accounts": [{
        "id": "codex-2",
        "codex_home": "/tmp/.codex-codex-2",
        "email": "",
        "enabled": False,
        "retired": True,
        "cleanup_pending": False,
        "login_recovery_failed": False,
    }]}))
    tokens_path.write_text("{}\n")
    monkeypatch.setattr(codex_login.time, "time", lambda: 1234.5)

    codex_login._persist_new_pool_login(
        account_id="codex-2",
        email="replacement@example.com",
        codex_home="/tmp/.codex-codex-2",
        token="mail-token",
        password="openai-password",
        provider="mailcatcher",
        pool_path=pool_path,
        tokens_path=tokens_path,
    )

    record = json.loads(pool_path.read_text())["accounts"][0]
    assert record == {
        "id": "codex-2",
        "codex_home": "/tmp/.codex-codex-2",
        "email": "replacement@example.com",
        "enabled": True,
        "quota_valid_after": 1234.5,
    }


@pytest.mark.asyncio
async def test_delete_account_blocks_persisted_codex_monitor_owner(
    monkeypatch,
    tmp_path,
):
    account = SimpleNamespace(
        id="codex-2",
        codex_home=str(tmp_path / ".codex-codex-2"),
        email="",
        auth_kind="oauth",
        retired=False,
        cleanup_pending=False,
    )
    pool = SimpleNamespace(account=lambda account_id: account)
    manager = _maintenance_manager()
    dispatcher = SimpleNamespace(
        codex_monitor_runtime_users=AsyncMock(
            return_value=["monitor 23"]
        ),
    )
    monkeypatch.setattr(codex_pool_api, "_get_pool", lambda: pool)
    monkeypatch.setattr(
        codex_pool_api,
        "_get_instance_manager",
        lambda: manager,
    )
    monkeypatch.setattr(
        codex_pool_api,
        "_get_dispatcher",
        lambda: dispatcher,
    )
    monkeypatch.setattr(
        codex_pool_api,
        "_reject_unresolved_login_transactions",
        Mock(),
    )
    monkeypatch.setattr(codex_pool_api, "_login_lock", asyncio.Lock())

    with pytest.raises(HTTPException) as blocked:
        await codex_pool_api.codex_delete_account(
            _admin_request(),
            account.id,
        )

    assert blocked.value.status_code == 409
    assert "monitor 23" in blocked.value.detail
    manager.begin_codex_app_server_home_maintenance.assert_not_awaited()


@pytest.mark.asyncio
async def test_delete_account_rechecks_monitor_owner_after_home_fence(
    monkeypatch,
    tmp_path,
):
    """A Monitor admitted during the precheck window still blocks deletion."""

    account = SimpleNamespace(
        id="codex-2",
        codex_home=str(tmp_path / ".codex-codex-2"),
        email="",
        auth_kind="oauth",
        retired=False,
        cleanup_pending=False,
    )
    pool = SimpleNamespace(account=lambda account_id: account)
    manager = _maintenance_manager()
    monitor_users = AsyncMock(
        side_effect=[[], ["monitor 24"]]
    )
    dispatcher = SimpleNamespace(
        codex_monitor_runtime_users=monitor_users,
    )
    monkeypatch.setattr(codex_pool_api, "_get_pool", lambda: pool)
    monkeypatch.setattr(
        codex_pool_api,
        "_get_instance_manager",
        lambda: manager,
    )
    monkeypatch.setattr(
        codex_pool_api,
        "_get_dispatcher",
        lambda: dispatcher,
    )
    monkeypatch.setattr(
        codex_pool_api,
        "_reject_unresolved_login_transactions",
        Mock(),
    )
    monkeypatch.setattr(codex_pool_api, "_login_lock", asyncio.Lock())

    with pytest.raises(HTTPException) as blocked:
        await codex_pool_api.codex_delete_account(
            _admin_request(),
            account.id,
        )

    assert blocked.value.status_code == 409
    assert "monitor 24" in blocked.value.detail
    assert monitor_users.await_count == 2
    manager.begin_codex_app_server_home_maintenance.assert_awaited_once_with(
        account.codex_home,
        require_idle=True,
    )
    manager.end_codex_app_server_home_maintenance.assert_awaited_once_with(
        account.codex_home
    )
    assert not codex_pool_api._login_lock.locked()


async def test_delete_account_scrubs_credentials_but_keeps_routable_sessions(
    monkeypatch, tmp_path,
):
    monkeypatch.setattr(codex_pool_api.Path, "home", lambda: tmp_path)
    pool_dir = tmp_path / ".codex-pool"
    pool_dir.mkdir()
    account_home = tmp_path / ".codex-codex-2"
    rollout = (
        account_home / "sessions" / "2026" / "07" / "21"
        / "rollout-now-thread-delete.jsonl"
    )
    rollout.parent.mkdir(parents=True)
    rollout.write_text("history\n")
    (account_home / "auth.json").write_text("oauth-secret")
    (account_home / ".auth.json.login-backup-old").write_text("old-oauth-secret")
    (account_home / "history.jsonl").write_text("account history\n")
    (account_home / "config.toml").write_text("profile = 'old'\n")
    (account_home / "state_5.sqlite").write_bytes(b"sqlite-state")
    (account_home / "log").mkdir()
    (account_home / "log" / "codex.log").write_text("sensitive log\n")
    (account_home / "shell_snapshots").mkdir()
    (account_home / "archived_sessions").mkdir()
    config_path = pool_dir / "accounts.json"
    config_path.write_text(json.dumps({"accounts": [{
        "id": "codex-2",
        "codex_home": str(account_home),
        "email": "delete@example.com",
        "enabled": True,
    }]}))
    tokens_path = pool_dir / "email_tokens.json"
    tokens_path.write_text(json.dumps({
        "delete@example.com": {
            "token": "mailbox-token",
            "provider": "mailcatcher",
            "password": "openai-password",
        },
    }))
    pool = CodexPool(config_path=config_path)
    manager = _maintenance_manager()
    monkeypatch.setattr(codex_pool_api, "_get_pool", lambda: pool)
    monkeypatch.setattr(codex_pool_api, "_get_instance_manager", lambda: manager)
    monkeypatch.setattr(
        codex_pool_api,
        "_get_dispatcher",
        lambda: SimpleNamespace(
            codex_monitor_runtime_users=AsyncMock(return_value=[]),
        ),
    )
    monkeypatch.setattr(codex_pool_api, "_login_lock", asyncio.Lock())

    result = await codex_pool_api.codex_delete_account(
        _admin_request(), "codex-2",
    )

    assert result == {
        "ok": True,
        "deleted": "codex-2",
        "retained_sessions": True,
    }
    assert rollout.read_text() == "history\n"
    assert {path.name for path in account_home.iterdir()} == {
        "sessions",
        ".ccm-retired-account",
    }
    assert not tokens_path.exists()
    assert (account_home / ".ccm-retired-account").stat().st_mode & 0o777 == 0o600
    record = json.loads(config_path.read_text())["accounts"][0]
    assert record == {
        "id": "codex-2",
        "codex_home": str(account_home.resolve()),
        "email": "",
        "enabled": False,
        "retired": True,
    }
    assert pool.list_accounts() == []
    assert pool.home_for_account("codex-2") == str(account_home.resolve())
    manager.begin_codex_app_server_home_maintenance.assert_awaited_once_with(
        str(account_home.resolve()), require_idle=True,
    )
    manager.end_codex_app_server_home_maintenance.assert_awaited_once_with(
        str(account_home.resolve())
    )
    assert not codex_pool_api._login_lock.locked()


async def test_delete_config_commit_failure_leaves_live_credentials_untouched(
    monkeypatch, tmp_path,
):
    monkeypatch.setattr(codex_pool_api.Path, "home", lambda: tmp_path)
    pool_dir = tmp_path / ".codex-pool"
    pool_dir.mkdir()
    account_home = tmp_path / ".codex-codex-2"
    account_home.mkdir()
    auth_path = account_home / "auth.json"
    auth_path.write_text("oauth-secret")
    config_path = pool_dir / "accounts.json"
    original_config = {"accounts": [{
        "id": "codex-2",
        "codex_home": str(account_home),
        "email": "keep@example.com",
        "enabled": True,
    }]}
    config_path.write_text(json.dumps(original_config))
    tokens_path = pool_dir / "email_tokens.json"
    original_tokens = {"keep@example.com": {"password": "keep-me"}}
    tokens_path.write_text(json.dumps(original_tokens))
    pool = CodexPool(config_path=config_path)
    manager = _maintenance_manager()
    monkeypatch.setattr(codex_pool_api, "_get_pool", lambda: pool)
    monkeypatch.setattr(codex_pool_api, "_get_instance_manager", lambda: manager)
    monkeypatch.setattr(
        codex_pool_api,
        "_get_dispatcher",
        lambda: SimpleNamespace(
            codex_monitor_runtime_users=AsyncMock(return_value=[]),
        ),
    )
    monkeypatch.setattr(codex_pool_api, "_login_lock", asyncio.Lock())
    monkeypatch.setattr(
        codex_pool_api,
        "_write_private_json",
        Mock(side_effect=OSError("disk full")),
    )

    with pytest.raises(OSError, match="disk full"):
        await codex_pool_api.codex_delete_account(
            _admin_request(), "codex-2",
        )

    assert json.loads(config_path.read_text()) == original_config
    assert json.loads(tokens_path.read_text()) == original_tokens
    assert auth_path.read_text() == "oauth-secret"
    assert not (account_home / ".ccm-retired-account").exists()
    manager.end_codex_app_server_home_maintenance.assert_awaited_once_with(
        str(account_home.resolve())
    )


async def test_delete_cleanup_failure_can_be_retried_from_pending_tombstone(
    monkeypatch, tmp_path,
):
    monkeypatch.setattr(codex_pool_api.Path, "home", lambda: tmp_path)
    pool_dir = tmp_path / ".codex-pool"
    pool_dir.mkdir()
    account_home = tmp_path / ".codex-codex-2"
    account_home.mkdir()
    (account_home / "auth.json").write_text("oauth-secret")
    config_path = pool_dir / "accounts.json"
    config_path.write_text(json.dumps({"accounts": [{
        "id": "codex-2",
        "codex_home": str(account_home),
        "email": "retry@example.com",
        "enabled": True,
    }]}))
    (pool_dir / "email_tokens.json").write_text(json.dumps({
        "retry@example.com": {"password": "openai-password"},
    }))
    pool = CodexPool(config_path=config_path)
    manager = _maintenance_manager()
    monkeypatch.setattr(codex_pool_api, "_get_pool", lambda: pool)
    monkeypatch.setattr(codex_pool_api, "_get_instance_manager", lambda: manager)
    monkeypatch.setattr(
        codex_pool_api,
        "_get_dispatcher",
        lambda: SimpleNamespace(
            codex_monitor_runtime_users=AsyncMock(return_value=[]),
        ),
    )
    monkeypatch.setattr(codex_pool_api, "_login_lock", asyncio.Lock())
    real_purge = codex_pool_api._purge_retired_codex_home
    purge_calls = 0

    def fail_once(home, account_id):
        nonlocal purge_calls
        purge_calls += 1
        if purge_calls == 1:
            raise OSError("temporary cleanup failure")
        real_purge(home, account_id)

    monkeypatch.setattr(codex_pool_api, "_purge_retired_codex_home", fail_once)

    with pytest.raises(HTTPException) as exc_info:
        await codex_pool_api.codex_delete_account(_admin_request(), "codex-2")

    assert exc_info.value.status_code == 500
    pending = pool.account("codex-2")
    assert pending.retired is True
    assert pending.cleanup_pending is True
    assert pending.email == "retry@example.com"

    result = await codex_pool_api.codex_delete_account(
        _admin_request(), "codex-2",
    )

    assert result["ok"] is True
    finalized = pool.account("codex-2")
    assert finalized.retired is True
    assert finalized.cleanup_pending is False
    assert finalized.email == ""
    assert {path.name for path in account_home.iterdir()} == {
        ".ccm-retired-account",
    }


def test_retired_home_purge_refuses_matching_directory_outside_user_home(
    monkeypatch, tmp_path,
):
    user_home = tmp_path / "user"
    user_home.mkdir()
    unmanaged = tmp_path / "service-data" / ".codex-prod"
    unmanaged.mkdir(parents=True)
    secret = unmanaged / "important.db"
    secret.write_text("must survive")
    monkeypatch.setattr(codex_pool_api.Path, "home", lambda: user_home)

    with pytest.raises(RuntimeError, match="outside the service user's home"):
        codex_pool_api._purge_retired_codex_home(unmanaged, "codex-prod")

    assert secret.read_text() == "must survive"


async def test_delete_rejects_unmanaged_home_before_mutating_account(
    monkeypatch, tmp_path,
):
    user_home = tmp_path / "user"
    user_home.mkdir()
    unmanaged = tmp_path / "service-data" / ".codex-prod"
    unmanaged.mkdir(parents=True)
    auth_path = unmanaged / "auth.json"
    auth_path.write_text("oauth-secret")
    pool_dir = tmp_path / ".codex-pool"
    pool_dir.mkdir()
    config_path = pool_dir / "accounts.json"
    original = {"accounts": [{
        "id": "codex-prod",
        "codex_home": str(unmanaged),
        "email": "prod@example.com",
        "enabled": True,
    }]}
    config_path.write_text(json.dumps(original))
    pool = CodexPool(config_path=config_path)
    manager = _maintenance_manager()
    monkeypatch.setattr(codex_pool_api.Path, "home", lambda: user_home)
    monkeypatch.setattr(codex_pool_api, "_get_pool", lambda: pool)
    monkeypatch.setattr(codex_pool_api, "_get_instance_manager", lambda: manager)
    monkeypatch.setattr(
        codex_pool_api,
        "_get_dispatcher",
        lambda: SimpleNamespace(
            codex_monitor_runtime_users=AsyncMock(return_value=[]),
        ),
    )
    monkeypatch.setattr(codex_pool_api, "_login_lock", asyncio.Lock())

    with pytest.raises(HTTPException) as exc_info:
        await codex_pool_api.codex_delete_account(
            _admin_request(), "codex-prod",
        )

    assert exc_info.value.status_code == 409
    assert json.loads(config_path.read_text()) == original
    assert auth_path.read_text() == "oauth-secret"


async def test_xvfb_uses_shared_login_runtime(monkeypatch):
    runtime = object()
    ensure = AsyncMock(return_value=runtime)
    monkeypatch.setattr(codex_pool_api, "ensure_login_runtime", ensure)

    assert await codex_pool_api._ensure_xvfb() is runtime
    ensure.assert_awaited_once()


async def test_add_account_accepts_email_without_token_or_password(monkeypatch):
    captured: dict[str, object] = {}
    pool = SimpleNamespace(_accounts=[], reload=lambda: None, _quota_cache=None)
    manager = _maintenance_manager()
    proc = _BlockingProcess()

    async def create_process(*cmd, **kwargs):
        captured["cmd"] = cmd
        captured["kwargs"] = kwargs
        return proc

    monkeypatch.setattr(codex_pool_api, "_get_pool", lambda: pool)
    monkeypatch.setattr(codex_pool_api, "_get_instance_manager", lambda: manager)
    monkeypatch.setattr(codex_pool_api, "_ensure_xvfb", AsyncMock())
    monkeypatch.setattr(codex_pool_api.asyncio, "create_subprocess_exec", create_process)
    monkeypatch.setattr(codex_pool_api, "_login_lock", asyncio.Lock())
    codex_pool_api._add_state.clear()

    result = await codex_pool_api.codex_add_account(
        _admin_request(),
        codex_pool_api.AddCodexAccountRequest(email="email-only@163.com"),
    )

    assert result["status"] == "running"
    assert "--credentials-stdin" in captured["cmd"]
    assert json.loads(proc.stdin.writes[0]) == {
        "type": "credentials",
        "attempt_id": result["attempt_id"],
        "token": "",
        "password": "",
    }

    proc.finished.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    manager.end_codex_app_server_home_maintenance.assert_awaited_once()
    assert not codex_pool_api._login_lock.locked()


async def test_relogin_accepts_saved_password_without_mailbox_token(monkeypatch, tmp_path):
    manager = _maintenance_manager()
    account, _pool = _prepare_relogin_account(monkeypatch, tmp_path, manager)
    captured: dict[str, object] = {}
    proc = _FinishedProcess()

    async def create_process(*cmd, **kwargs):
        captured["cmd"] = cmd
        captured["kwargs"] = kwargs
        return proc

    monkeypatch.setattr(codex_pool_api.asyncio, "create_subprocess_exec", create_process)

    result = await codex_pool_api.codex_relogin(_admin_request(), account.id)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    cmd = captured["cmd"]
    assert result["ok"] is True
    assert result["status"] == "running"
    assert result["attempt_id"]
    assert "--token" not in cmd
    assert "--password" not in cmd
    assert "openai-password" not in cmd
    assert "--credentials-stdin" in cmd
    assert captured["kwargs"]["stdin"] is asyncio.subprocess.PIPE
    assert captured["kwargs"]["start_new_session"] is True
    assert json.loads(proc.stdin.writes[0]) == {
        "type": "credentials",
        "attempt_id": result["attempt_id"],
        "token": "",
        "password": "openai-password",
    }
    manager.begin_codex_app_server_home_maintenance.assert_awaited_once_with(
        account.codex_home, require_idle=True,
    )
    manager.end_codex_app_server_home_maintenance.assert_awaited_once_with(account.codex_home)
    assert not codex_pool_api._login_lock.locked()


async def test_relogin_accepts_email_only_account(monkeypatch, tmp_path):
    manager = _maintenance_manager()
    account, _pool = _prepare_relogin_account(monkeypatch, tmp_path, manager)
    credential_path = tmp_path / ".codex-pool" / "email_tokens.json"
    credential_path.write_text(json.dumps({
        account.email: {
            "token": "",
            "provider": "mailcom",
            "password": "",
        },
    }))
    captured: dict[str, object] = {}
    proc = _FinishedProcess()

    async def create_process(*cmd, **kwargs):
        captured["cmd"] = cmd
        captured["kwargs"] = kwargs
        return proc

    monkeypatch.setattr(codex_pool_api.asyncio, "create_subprocess_exec", create_process)

    result = await codex_pool_api.codex_relogin(_admin_request(), account.id)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert result["ok"] is True
    assert result["status"] == "running"
    assert json.loads(proc.stdin.writes[0]) == {
        "type": "credentials",
        "attempt_id": result["attempt_id"],
        "token": "",
        "password": "",
    }
    manager.end_codex_app_server_home_maintenance.assert_awaited_once_with(
        account.codex_home,
    )
    assert not codex_pool_api._login_lock.locked()


async def test_relogin_holds_maintenance_until_subprocess_finishes(monkeypatch, tmp_path):
    manager = _maintenance_manager()
    account, _pool = _prepare_relogin_account(monkeypatch, tmp_path, manager)
    proc = _BlockingProcess()
    monkeypatch.setattr(
        codex_pool_api.asyncio,
        "create_subprocess_exec",
        AsyncMock(return_value=proc),
    )

    result = await codex_pool_api.codex_relogin(_admin_request(), account.id)
    await asyncio.sleep(0)

    assert result["status"] == "running"
    manager.end_codex_app_server_home_maintenance.assert_not_awaited()
    assert codex_pool_api._login_lock.locked()

    proc.finished.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    manager.end_codex_app_server_home_maintenance.assert_awaited_once_with(account.codex_home)
    assert not codex_pool_api._login_lock.locked()


async def test_relogin_busy_account_returns_409_and_releases_login_lock(monkeypatch, tmp_path):
    manager = _maintenance_manager(
        begin_side_effect=CodexAppServerBusyError("active Codex turn"),
    )
    account, _pool = _prepare_relogin_account(monkeypatch, tmp_path, manager)
    create_process = AsyncMock()
    monkeypatch.setattr(codex_pool_api.asyncio, "create_subprocess_exec", create_process)

    with pytest.raises(HTTPException) as exc_info:
        await codex_pool_api.codex_relogin(_admin_request(), account.id)

    assert exc_info.value.status_code == 409
    assert "active Codex turn" in str(exc_info.value.detail)
    create_process.assert_not_awaited()
    manager.end_codex_app_server_home_maintenance.assert_not_awaited()
    assert not codex_pool_api._login_lock.locked()


async def test_relogin_spawn_failure_releases_maintenance_and_login_lock(monkeypatch, tmp_path):
    manager = _maintenance_manager()
    account, _pool = _prepare_relogin_account(monkeypatch, tmp_path, manager)
    monkeypatch.setattr(
        codex_pool_api.asyncio,
        "create_subprocess_exec",
        AsyncMock(side_effect=OSError("could not spawn login")),
    )

    with pytest.raises(OSError, match="could not spawn login"):
        await codex_pool_api.codex_relogin(_admin_request(), account.id)

    manager.end_codex_app_server_home_maintenance.assert_awaited_once_with(account.codex_home)
    assert not codex_pool_api._login_lock.locked()


async def test_delete_account_is_wrapped_in_idle_maintenance(monkeypatch, tmp_path):
    account = SimpleNamespace(
        id="codex-2",
        codex_home=str(tmp_path / ".codex-codex-2"),
        email="",
    )
    reload_calls: list[bool] = []
    reload_count = 0

    def reload_pool():
        nonlocal reload_count
        reload_calls.append(True)
        reload_count += 1

    def get_account(account_id: str):
        if account_id != account.id:
            return None
        if reload_count == 1:
            return SimpleNamespace(
                **vars(account), retired=True, cleanup_pending=True,
            )
        if reload_count >= 2:
            return SimpleNamespace(
                **vars(account), retired=True, cleanup_pending=False,
            )
        return account

    pool = SimpleNamespace(
        account=get_account,
        reload=reload_pool,
    )
    manager = _maintenance_manager()
    pool_dir = tmp_path / ".codex-pool"
    pool_dir.mkdir()
    accounts_path = pool_dir / "accounts.json"
    accounts_path.write_text(json.dumps({"accounts": [
        {"id": "codex-1"},
        {"id": account.id},
    ]}))
    monkeypatch.setattr(codex_pool_api.Path, "home", lambda: tmp_path)
    monkeypatch.setattr(codex_pool_api, "_get_pool", lambda: pool)
    monkeypatch.setattr(codex_pool_api, "_get_instance_manager", lambda: manager)
    monkeypatch.setattr(
        codex_pool_api,
        "_get_dispatcher",
        lambda: SimpleNamespace(
            codex_monitor_runtime_users=AsyncMock(return_value=[]),
        ),
    )
    monkeypatch.setattr(codex_pool_api, "_login_lock", asyncio.Lock())

    result = await codex_pool_api.codex_delete_account(_admin_request(), account.id)

    assert result == {
        "ok": True,
        "deleted": account.id,
        "retained_sessions": True,
    }
    assert json.loads(accounts_path.read_text())["accounts"] == [
        {"id": "codex-1"},
        {
            "id": account.id,
            "codex_home": account.codex_home,
            "email": "",
            "enabled": False,
            "retired": True,
        },
    ]
    assert reload_calls == [True, True]
    manager.begin_codex_app_server_home_maintenance.assert_awaited_once_with(
        account.codex_home, require_idle=True,
    )
    manager.end_codex_app_server_home_maintenance.assert_awaited_once_with(account.codex_home)


async def test_delete_busy_account_returns_409_without_mutating_pool(monkeypatch, tmp_path):
    account = SimpleNamespace(id="codex-2", codex_home=str(tmp_path / ".codex-codex-2"))
    pool = SimpleNamespace(account=lambda _account_id: account, reload=lambda: None)
    manager = _maintenance_manager(
        begin_side_effect=CodexAppServerBusyError("active Codex turn"),
    )
    pool_dir = tmp_path / ".codex-pool"
    pool_dir.mkdir()
    accounts_path = pool_dir / "accounts.json"
    original = json.dumps({"accounts": [{"id": account.id}]})
    accounts_path.write_text(original)
    monkeypatch.setattr(codex_pool_api.Path, "home", lambda: tmp_path)
    monkeypatch.setattr(codex_pool_api, "_get_pool", lambda: pool)
    monkeypatch.setattr(codex_pool_api, "_get_instance_manager", lambda: manager)
    monkeypatch.setattr(
        codex_pool_api,
        "_get_dispatcher",
        lambda: SimpleNamespace(
            codex_monitor_runtime_users=AsyncMock(return_value=[]),
        ),
    )
    monkeypatch.setattr(codex_pool_api, "_login_lock", asyncio.Lock())

    with pytest.raises(HTTPException) as exc_info:
        await codex_pool_api.codex_delete_account(_admin_request(), account.id)

    assert exc_info.value.status_code == 409
    assert accounts_path.read_text() == original
    manager.end_codex_app_server_home_maintenance.assert_not_awaited()


@pytest.mark.parametrize("missing_prerequisite", ["codex", "display"])
async def test_login_preflight_does_not_touch_existing_auth(
    monkeypatch, tmp_path, missing_prerequisite,
):
    codex_home = tmp_path / "codex-home"
    codex_home.mkdir(mode=0o755)
    auth_path = codex_home / "auth.json"
    original = b'{"tokens":"existing"}'
    auth_path.write_bytes(original)
    spawn = AsyncMock()
    monkeypatch.setattr(codex_login.asyncio, "create_subprocess_exec", spawn)

    if missing_prerequisite == "codex":
        monkeypatch.setattr(codex_login.shutil, "which", lambda _name: None)
        monkeypatch.setenv("DISPLAY", ":99")
    else:
        monkeypatch.setattr(codex_login.shutil, "which", lambda _name: "/usr/bin/codex")
        monkeypatch.delenv("DISPLAY", raising=False)

    result = await codex_login.codex_login(
        "user@example.com", "token", str(codex_home), password="password",
    )

    assert result["ok"] is False
    assert auth_path.read_bytes() == original
    assert not list(codex_home.glob(".auth.json.login-backup-*"))
    assert codex_home.stat().st_mode & 0o777 == 0o755
    spawn.assert_not_awaited()


class _NoAuthorizeUrlProcess:
    returncode = 0
    stdout = object()


async def test_failed_login_restores_old_auth_and_secures_codex_home(monkeypatch, tmp_path):
    codex_home = tmp_path / "codex-home"
    codex_home.mkdir(mode=0o755)
    auth_path = codex_home / "auth.json"
    original = b'{"tokens":"existing"}'
    auth_path.write_bytes(original)
    monkeypatch.setattr(codex_login.shutil, "which", lambda _name: "/usr/bin/codex")
    monkeypatch.setenv("DISPLAY", ":99")
    monkeypatch.setattr(codex_login, "AUTH_URL_TIMEOUT", 0)
    monkeypatch.setattr(
        codex_login.asyncio,
        "create_subprocess_exec",
        AsyncMock(return_value=_NoAuthorizeUrlProcess()),
    )

    result = await codex_login.codex_login(
        "user@example.com", "token", str(codex_home), password="password",
    )

    assert result["ok"] is False
    assert auth_path.read_bytes() == original
    assert not list(codex_home.glob(".auth.json.login-backup-*"))
    assert codex_home.stat().st_mode & 0o777 == 0o700


async def test_login_spawn_exception_restores_old_auth(monkeypatch, tmp_path):
    codex_home = tmp_path / "codex-home"
    codex_home.mkdir()
    auth_path = codex_home / "auth.json"
    original = b'{"tokens":"existing"}'
    auth_path.write_bytes(original)
    monkeypatch.setattr(codex_login.shutil, "which", lambda _name: "/usr/bin/codex")
    monkeypatch.setenv("DISPLAY", ":99")
    monkeypatch.setattr(
        codex_login.asyncio,
        "create_subprocess_exec",
        AsyncMock(side_effect=OSError("could not spawn codex")),
    )

    with pytest.raises(OSError, match="could not spawn codex"):
        await codex_login.codex_login(
            "user@example.com", "token", str(codex_home), password="password",
        )

    assert auth_path.read_bytes() == original
    assert not list(codex_home.glob(".auth.json.login-backup-*"))


class _AuthorizeUrlStdout:
    def __init__(self):
        self._sent = False

    async def readline(self):
        if self._sent:
            return b""
        self._sent = True
        return b"https://auth.openai.com/oauth/authorize?client_id=test\n"


class _SuccessfulLoginProcess:
    def __init__(self):
        self.returncode = None
        self.stdout = _AuthorizeUrlStdout()

    def kill(self):
        self.returncode = -9

    async def wait(self):
        return self.returncode


class _FakePage:
    async def goto(self, *_args, **_kwargs):
        return None


class _FakeBrowserContext:
    async def new_page(self):
        return _FakePage()


class _FakeBrowser:
    async def new_context(self, **_kwargs):
        return _FakeBrowserContext()

    async def close(self):
        return None


class _FakeChromium:
    async def launch(self, **_kwargs):
        return _FakeBrowser()


class _FakePlaywrightContextManager:
    async def __aenter__(self):
        return SimpleNamespace(chromium=_FakeChromium())

    async def __aexit__(self, *_args):
        return False


async def test_successful_login_replaces_auth_and_removes_backup(monkeypatch, tmp_path):
    codex_home = tmp_path / "codex-home"
    codex_home.mkdir(mode=0o755)
    auth_path = codex_home / "auth.json"
    auth_path.write_text("old-auth")
    monkeypatch.setattr(codex_login.shutil, "which", lambda _name: "/usr/bin/codex")
    monkeypatch.setenv("DISPLAY", ":99")
    spawn = AsyncMock(return_value=_SuccessfulLoginProcess())
    monkeypatch.setattr(
        codex_login.asyncio,
        "create_subprocess_exec",
        spawn,
    )

    playwright_package = ModuleType("playwright")
    playwright_async_api = ModuleType("playwright.async_api")
    playwright_async_api.async_playwright = lambda: _FakePlaywrightContextManager()
    playwright_async_api.TimeoutError = TimeoutError
    playwright_package.async_api = playwright_async_api
    monkeypatch.setitem(sys.modules, "playwright", playwright_package)
    monkeypatch.setitem(sys.modules, "playwright.async_api", playwright_async_api)

    async def write_new_auth(
        _page, _email, _password, _token, _timeout, target_auth_path, _logs, _provider,
        _attempt_id, _manual_reader,
    ):
        target_auth_path.write_text("new-auth")

    monkeypatch.setattr(codex_login, "_run_state_machine", write_new_auth)
    monkeypatch.setattr(codex_login, "_smoke_test", AsyncMock(return_value=True))

    result = await codex_login.codex_login(
        "user@example.com", "token", str(codex_home), password="password",
    )

    assert result["ok"] is True
    assert auth_path.read_text() == "new-auth"
    assert (codex_home / "config.toml").read_text() == (
        'approval_policy = "never"\n'
        'sandbox_mode = "danger-full-access"\n'
    )
    assert (codex_home / "config.toml").stat().st_mode & 0o777 == 0o600
    assert not list(codex_home.glob(".auth.json.login-backup-*"))
    assert codex_home.stat().st_mode & 0o777 == 0o700
    assert spawn.await_args.kwargs["stdin"] is asyncio.subprocess.DEVNULL


def test_full_access_config_preserves_existing_codex_settings(tmp_path):
    codex_home = tmp_path / "codex-home"
    codex_home.mkdir()
    config = codex_home / "config.toml"
    config.write_text(
        'approval_policy = "on-request"\n'
        'personality = "pragmatic"\n'
        '\n'
        '[projects."/workspace"]\n'
        'trust_level = "trusted"\n'
    )

    codex_login._ensure_full_access_config(codex_home)

    assert config.read_text() == (
        'approval_policy = "never"\n'
        'sandbox_mode = "danger-full-access"\n'
        'personality = "pragmatic"\n'
        '\n'
        '[projects."/workspace"]\n'
        'trust_level = "trusted"\n'
    )
    assert config.stat().st_mode & 0o777 == 0o600
