"""Tests for ask_user：拦截 AskUserQuestion → 前端卡片 → 答案喂回模型。

覆盖纯逻辑（registry / format / settings 注入）+ hook 脚本决策输出
（subprocess + 单次 stub HTTP server）；完整 HTTP+claude 回环由
集成测试在真实环境验证（见 PROGRESS.md task ask_user）。
"""
import asyncio
import json
import os
import socket
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

from backend.services.ask_user import (
    AskUserRevocation,
    AskUserRegistry,
    format_answer_reason,
)
from backend.services.ask_user_settings import (
    ensure_ask_user_hook,
    _is_our_pretool_entry,
    _MATCHER,
    _MARKER,
)


# ----------------------------------------------------------------- registry

TASK_RETRY_COUNT = 2
TASK_TURN_GENERATION = 7
TASK_STATUS = "executing"


def _generation_kwargs():
    return {
        "task_retry_count": TASK_RETRY_COUNT,
        "task_turn_generation": TASK_TURN_GENERATION,
        "task_status": TASK_STATUS,
    }

@pytest.mark.asyncio
async def test_registry_create_resolve_roundtrip():
    reg = AskUserRegistry()
    incarnation = "7" * 32
    pending = reg.create(
        task_id=7,
        task_incarnation_id=incarnation,
        **_generation_kwargs(),
        session_id="sid",
        questions=[{"question": "?"}],
    )
    assert pending.request_id
    assert reg.get(pending.request_id) is pending
    assert reg.list_for_task(
        7, incarnation, TASK_RETRY_COUNT, TASK_TURN_GENERATION, TASK_STATUS,
    ) == [pending]
    assert reg.list_for_task(
        8, incarnation, TASK_RETRY_COUNT, TASK_TURN_GENERATION, TASK_STATUS,
    ) == []
    assert reg.list_for_task(
        7, "8" * 32, TASK_RETRY_COUNT, TASK_TURN_GENERATION, TASK_STATUS,
    ) == []

    answers = [{"labels": ["A"], "text": ""}]
    assert reg.resolve(pending.request_id, answers) is True
    assert await pending.future == answers


@pytest.mark.asyncio
async def test_registry_resolve_unknown_and_double():
    reg = AskUserRegistry()
    assert reg.resolve("nope", []) is False
    pending = reg.create(
        task_id=1,
        task_incarnation_id="1" * 32,
        **_generation_kwargs(),
        session_id="s",
        questions=[{"question": "?"}],
    )
    assert reg.resolve(pending.request_id, [{"labels": ["x"]}]) is True
    # second resolve must fail (future already done)
    assert reg.resolve(pending.request_id, [{"labels": ["y"]}]) is False


@pytest.mark.asyncio
async def test_registry_answer_claim_allows_one_committer_and_release():
    reg = AskUserRegistry()
    incarnation = "4" * 32
    pending = reg.create(
        task_id=4,
        task_incarnation_id=incarnation,
        **_generation_kwargs(),
        session_id="session-4",
        questions=[{"question": "?"}],
    )
    first = reg.claim_answer(
        pending.request_id,
        task_id=4,
        task_incarnation_id=incarnation,
        **_generation_kwargs(),
        session_id="session-4",
    )
    assert first is not None
    assert reg.list_for_task(
        4, incarnation, TASK_RETRY_COUNT, TASK_TURN_GENERATION, TASK_STATUS,
    ) == []
    assert reg.claim_answer(
        pending.request_id,
        task_id=4,
        task_incarnation_id=incarnation,
        **_generation_kwargs(),
        session_id="session-4",
    ) is None
    assert reg.release_answer_claim(pending.request_id, first) is True

    second = reg.claim_answer(
        pending.request_id,
        task_id=4,
        task_incarnation_id=incarnation,
        **_generation_kwargs(),
        session_id="session-4",
    )
    assert second is not None and second != first
    answers = [{"labels": ["A"]}]
    assert reg.fulfill_answer(pending.request_id, first, answers) is False
    assert reg.fulfill_answer(pending.request_id, second, answers) is True
    assert await pending.future == answers


@pytest.mark.asyncio
async def test_registry_discard_and_list_excludes_done():
    reg = AskUserRegistry()
    incarnation = "3" * 32
    p1 = reg.create(
        task_id=3,
        task_incarnation_id=incarnation,
        **_generation_kwargs(),
        session_id="s",
        questions=[{"question": "?"}],
    )
    p2 = reg.create(
        task_id=3,
        task_incarnation_id=incarnation,
        **_generation_kwargs(),
        session_id="s",
        questions=[{"question": "?"}],
    )
    assert len(reg.list_for_task(
        3, incarnation, TASK_RETRY_COUNT, TASK_TURN_GENERATION, TASK_STATUS,
    )) == 2
    reg.resolve(p1.request_id, [])
    # resolved (future done) → excluded from pending list
    assert reg.list_for_task(
        3, incarnation, TASK_RETRY_COUNT, TASK_TURN_GENERATION, TASK_STATUS,
    ) == [p2]
    reg.discard(p2.request_id)
    assert reg.get(p2.request_id) is None


@pytest.mark.asyncio
async def test_registry_list_all_spans_tasks_and_excludes_done():
    reg = AskUserRegistry()
    a = reg.create(
        task_id=1,
        task_incarnation_id="1" * 32,
        **_generation_kwargs(),
        session_id="s",
        questions=[{"question": "?"}],
    )
    b = reg.create(
        task_id=2,
        task_incarnation_id="2" * 32,
        **_generation_kwargs(),
        session_id="s",
        questions=[{"question": "?"}],
    )
    # list_all 跨 task 汇总（驱动全局通知）
    assert {p.request_id for p in reg.list_all()} == {a.request_id, b.request_id}
    reg.resolve(a.request_id, [])
    # 已回答（future done）从全局列表剔除
    assert [p.request_id for p in reg.list_all()] == [b.request_id]


@pytest.mark.asyncio
async def test_registry_discards_old_turn_and_terminal_pending():
    reg = AskUserRegistry()
    incarnation = "5" * 32
    old = reg.create(
        task_id=5,
        task_incarnation_id=incarnation,
        task_retry_count=1,
        task_turn_generation=3,
        task_status="executing",
        session_id="session-5",
        questions=[{"question": "old?"}],
    )

    reg.discard_stale_for_task(5, incarnation, 1, 4, "executing")
    assert reg.get(old.request_id) is None
    assert isinstance(await old.future, AskUserRevocation)

    terminal = reg.create(
        task_id=5,
        task_incarnation_id=incarnation,
        task_retry_count=1,
        task_turn_generation=4,
        task_status="executing",
        session_id="session-5",
        questions=[{"question": "terminal?"}],
    )
    reg.discard_stale_for_task(5, incarnation, 1, 4, "completed")
    assert reg.get(terminal.request_id) is None
    assert isinstance(await terminal.future, AskUserRevocation)


# ------------------------------------------------------------------- format

def test_format_answer_reason_single_select():
    questions = [{
        "question": "Tabs or spaces?",
        "header": "Indent",
        "options": [
            {"label": "Tabs", "description": "tab chars"},
            {"label": "Spaces", "description": "space chars"},
        ],
        "multiSelect": False,
    }]
    reason = format_answer_reason(questions, [{"labels": ["Spaces"], "text": ""}])
    assert "Tabs or spaces?" in reason
    assert "Spaces (space chars)" in reason
    assert "Do NOT call AskUserQuestion again" in reason


def test_format_answer_reason_multiselect_and_custom_text():
    questions = [{
        "question": "Pick langs",
        "options": [{"label": "Py"}, {"label": "Go"}, {"label": "Rust"}],
        "multiSelect": True,
    }]
    reason = format_answer_reason(questions, [{"labels": ["Py", "Rust"], "text": "also C"}])
    assert "Py" in reason and "Rust" in reason
    assert 'also C' in reason


def test_format_answer_reason_missing_answer():
    questions = [{"question": "Q1", "options": []}, {"question": "Q2", "options": []}]
    # only one answer provided for two questions → no crash, '(no selection)'
    reason = format_answer_reason(questions, [{"labels": ["x"]}])
    assert "Q1" in reason and "Q2" in reason
    assert "(no selection)" in reason


# ----------------------------------------------------------- settings inject

def _set_enabled(value: bool):
    from backend.config import settings
    settings.ask_user_enabled = value


def test_inject_adds_hook_and_is_idempotent(tmp_path):
    _set_enabled(True)
    sp = tmp_path / "settings.json"
    sp.write_text(json.dumps({
        "theme": "dark",
        "hooks": {"PreToolUse": [
            {"matcher": "Bash", "hooks": [{"type": "command", "command": "echo x"}]}
        ]},
    }))

    ensure_ask_user_hook(str(tmp_path))
    d1 = json.loads(sp.read_text())
    pre = d1["hooks"]["PreToolUse"]
    # 保留原有 key 与其它 hook
    assert d1["theme"] == "dark"
    assert any(e.get("matcher") == "Bash" for e in pre)
    ours = [e for e in pre if _is_our_pretool_entry(e)]
    assert len(ours) == 1
    assert ours[0]["matcher"] == _MATCHER
    assert _MARKER in ours[0]["hooks"][0]["command"]

    # 第二次注入不重复
    ensure_ask_user_hook(str(tmp_path))
    d2 = json.loads(sp.read_text())
    ours2 = [e for e in d2["hooks"]["PreToolUse"] if _is_our_pretool_entry(e)]
    assert len(ours2) == 1


def test_disable_removes_our_hook_only(tmp_path):
    _set_enabled(True)
    ensure_ask_user_hook(str(tmp_path))
    sp = tmp_path / "settings.json"
    # add an unrelated hook alongside
    data = json.loads(sp.read_text())
    data["hooks"]["PreToolUse"].append(
        {"matcher": "Bash", "hooks": [{"type": "command", "command": "echo x"}]}
    )
    sp.write_text(json.dumps(data))

    try:
        _set_enabled(False)
        ensure_ask_user_hook(str(tmp_path))
    finally:
        _set_enabled(True)

    d = json.loads(sp.read_text())
    pre = d.get("hooks", {}).get("PreToolUse", [])
    assert not any(_is_our_pretool_entry(e) for e in pre)
    assert any(e.get("matcher") == "Bash" for e in pre)


def test_inject_handles_corrupt_settings(tmp_path):
    _set_enabled(True)
    sp = tmp_path / "settings.json"
    sp.write_text("{ not valid json ")
    ensure_ask_user_hook(str(tmp_path))  # must not raise
    d = json.loads(sp.read_text())
    assert any(_is_our_pretool_entry(e) for e in d["hooks"]["PreToolUse"])


def test_inject_creates_missing_dir(tmp_path):
    _set_enabled(True)
    target = tmp_path / "newconf"
    ensure_ask_user_hook(str(target))
    sp = target / "settings.json"
    assert sp.exists()
    d = json.loads(sp.read_text())
    assert any(_is_our_pretool_entry(e) for e in d["hooks"]["PreToolUse"])


def test_inject_hook_carries_cli_timeout(tmp_path):
    """hook 项必须带显式 timeout：CLI 默认 600s 杀 hook 命令，会把 /wait
    阻塞中的 hook 杀掉 → fail-open 弹原生交互框冻死 PTY turn（task 32）。"""
    from backend.config import settings

    _set_enabled(True)
    ensure_ask_user_hook(str(tmp_path))
    d = json.loads((tmp_path / "settings.json").read_text())
    ours = [e for e in d["hooks"]["PreToolUse"] if _is_our_pretool_entry(e)]
    assert ours[0]["hooks"][0]["timeout"] == int(settings.ask_user_timeout) + 60


# -------------------------------------------------------------- hook script

_HOOK_SCRIPT = Path(__file__).resolve().parents[1] / "hooks" / "ask_user_hook.py"


def _run_hook_against(
    response_body: dict,
    *,
    status: int = 200,
    scoped_token: bool = False,
) -> subprocess.CompletedProcess:
    """跑真实 hook 脚本，stub 后端返回给定 /wait 响应。"""

    class _Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            self.rfile.read(int(self.headers.get("Content-Length", 0)))
            body = json.dumps(response_body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):  # 静音测试输出
            pass

    srv = HTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        payload = json.dumps({
            "tool_name": "AskUserQuestion",
            "session_id": "sid-hook-test",
            "tool_input": {"questions": [{"question": "?"}]},
        })
        env = os.environ.copy()
        if scoped_token:
            env["CCM_ASK_USER_TOKEN"] = "scoped-hook-test-token"
        else:
            env.pop("CCM_ASK_USER_TOKEN", None)
        return subprocess.run(
            [sys.executable, str(_HOOK_SCRIPT),
             "--api-base", f"http://127.0.0.1:{srv.server_address[1]}",
             "--timeout", "10"],
            input=payload,
            capture_output=True,
            text=True,
            timeout=30,
            env=env,
        )
    finally:
        srv.shutdown()


def test_hook_script_answer_feeds_deny_reason():
    cp = _run_hook_against({"answered": True, "reason": "The user picked A."})
    assert cp.returncode == 0
    out = json.loads(cp.stdout)
    assert out["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert out["hookSpecificOutput"]["permissionDecisionReason"] == "The user picked A."


def test_hook_script_timed_out_denies_not_fail_open():
    """超时不放行：PTY 下原生 AskUserQuestion 会冻死 turn（task 32, 2026-07-17）。"""
    cp = _run_hook_against({"answered": False, "timed_out": True})
    assert cp.returncode == 0
    out = json.loads(cp.stdout)
    assert out["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert "best judgment" in out["hookSpecificOutput"]["permissionDecisionReason"]


def test_hook_script_stale_generation_revocation_denies_not_fail_open():
    cp = _run_hook_against({
        "answered": False,
        "revoked": True,
        "reason": "The Task generation changed before the user answered.",
    })
    assert cp.returncode == 0
    out = json.loads(cp.stdout)
    assert out["hookSpecificOutput"]["permissionDecision"] == "deny"
    reason = out["hookSpecificOutput"]["permissionDecisionReason"]
    assert "Task generation changed" in reason
    assert "best judgment" in reason


@pytest.mark.parametrize("status", [401, 403, 409, 410])
def test_hook_script_scoped_http_rejection_denies_not_fail_open(status):
    cp = _run_hook_against(
        {"detail": "Internal hook Task generation is stale"},
        status=status,
        scoped_token=True,
    )
    assert cp.returncode == 0
    out = json.loads(cp.stdout)
    assert out["hookSpecificOutput"]["permissionDecision"] == "deny"
    reason = out["hookSpecificOutput"]["permissionDecisionReason"]
    assert "generation is stale" in reason
    assert "best judgment" in reason


def test_hook_script_structured_stale_http_response_denies_without_token():
    cp = _run_hook_against(
        {"answered": False, "revoked": True, "reason": "Generation revoked."},
        status=422,
    )
    assert cp.returncode == 0
    out = json.loads(cp.stdout)
    assert out["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert "Generation revoked" in out["hookSpecificOutput"][
        "permissionDecisionReason"
    ]


def test_hook_script_scoped_server_error_denies_not_fail_open():
    cp = _run_hook_against(
        {"detail": "temporary failure"},
        status=503,
        scoped_token=True,
    )
    assert cp.returncode == 0
    out = json.loads(cp.stdout)
    assert out["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert "best judgment" in out["hookSpecificOutput"][
        "permissionDecisionReason"
    ]


def test_hook_script_true_network_unreachable_still_fails_open():
    payload = json.dumps({
        "tool_name": "AskUserQuestion",
        "session_id": "sid-unreachable-test",
        "tool_input": {"questions": [{"question": "?"}]},
    })
    env = os.environ.copy()
    env["CCM_ASK_USER_TOKEN"] = "scoped-unreachable-test-token"
    with socket.socket() as probe:
        # Keep the port bound but deliberately not listening so another test
        # process cannot claim it between discovery and the hook connection.
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        cp = subprocess.run(
            [
                sys.executable,
                str(_HOOK_SCRIPT),
                "--api-base",
                f"http://127.0.0.1:{port}",
                "--timeout",
                "2",
            ],
            input=payload,
            capture_output=True,
            text=True,
            timeout=10,
            env=env,
        )
    assert cp.returncode == 0
    assert cp.stdout.strip() == ""
    assert "CCM unreachable" in cp.stderr


def test_hook_script_no_session_fails_open():
    cp = _run_hook_against({"answered": False, "no_session": True})
    assert cp.returncode == 0
    assert cp.stdout.strip() == ""  # 无决策输出 = 放行原生工具
