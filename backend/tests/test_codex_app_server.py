"""Protocol regression tests for the persistent Codex app-server backend."""

import asyncio
import json
import os
import signal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from backend.services.process_safety import UnsafeProcessGroupError
from backend.services.codex_app_server import (
    CodexAppServer,
    CodexAppServerBusyError,
    CodexAppServerError,
    CodexAppServerRegistry,
    CodexRequiredMcpError,
    CodexRequiredMcpPreTurnError,
    CodexThreadHomeMismatchError,
    CodexTurnProcess,
    normalize_codex_home,
)
from backend.services.mcp_config import McpServerSpec


def _task_mcp_spec(task_id: int) -> McpServerSpec:
    return McpServerSpec(
        name="ccm_skills",
        command="python",
        args=(
            "-m",
            "backend.mcp.ccm_skills_server",
            "--task-id",
            str(task_id),
        ),
        cwd="/ccm",
        required=True,
        enabled_tools=("ccm_command_help",),
        startup_timeout_sec=10,
        tool_timeout_sec=60,
    )


@pytest.mark.asyncio
async def test_start_turn_uses_native_resume_and_turn_start():
    server = CodexAppServer("codex")
    server._process = SimpleNamespace(pid=4321, returncode=None)
    server.ensure_started = AsyncMock()
    server._request = AsyncMock(side_effect=[
        {"thread": {"id": "thread-123"}},
        {"turn": {"id": "turn-456"}},
    ])

    process, thread_id = await server.start_turn(
        prompt="continue",
        cwd="/tmp",
        model="gpt-5.6-luna",
        effort="max",
        resume_session_id="thread-123",
        git_env={"GIT_AUTHOR_NAME": "CCM"},
        task_id=9,
        mcp_specs=(_task_mcp_spec(9),),
    )

    assert thread_id == "thread-123"
    resume_call, turn_call = server._request.await_args_list
    assert resume_call.args[0] == "thread/resume"
    assert resume_call.args[1]["threadId"] == "thread-123"
    assert resume_call.args[1]["approvalPolicy"] == "never"
    assert resume_call.args[1]["sandbox"] == "danger-full-access"
    assert resume_call.args[1]["config"]["shell_environment_policy"]["set"] == {
        "GIT_AUTHOR_NAME": "CCM"
    }
    assert resume_call.args[1]["config"]["mcp_servers"]["ccm_skills"] == {
        "command": "python",
        "args": [
            "-m",
            "backend.mcp.ccm_skills_server",
            "--task-id",
            "9",
        ],
        "cwd": "/ccm",
        "required": True,
        "enabled_tools": ["ccm_command_help"],
        "startup_timeout_sec": 10,
        "tool_timeout_sec": 60,
    }
    assert turn_call.args[0] == "turn/start"
    assert turn_call.args[1]["effort"] == "max"
    assert turn_call.args[1]["model"] == "gpt-5.6-luna"

    first = json.loads((await process.stdout.readline()).decode())
    assert first == {"type": "thread.started", "thread_id": "thread-123"}


@pytest.mark.asyncio
async def test_start_turn_injects_mcp_config_into_new_thread():
    server = CodexAppServer("codex")
    server._process = SimpleNamespace(pid=4321, returncode=None)
    server.ensure_started = AsyncMock()
    server._request = AsyncMock(side_effect=[
        {"thread": {"id": "thread-new"}},
        {"turn": {"id": "turn-new"}},
    ])

    await server.start_turn(
        prompt="start",
        cwd="/tmp",
        model="gpt-5.6-sol",
        effort="high",
        resume_session_id=None,
        git_env=None,
        task_id=41,
        mcp_specs=(_task_mcp_spec(41),),
    )

    thread_call = server._request.await_args_list[0]
    assert thread_call.args[0] == "thread/start"
    assert thread_call.args[1]["config"]["mcp_servers"]["ccm_skills"]["args"][-2:] == [
        "--task-id",
        "41",
    ]
    assert thread_call.args[1]["config"]["mcp_servers"]["ccm_skills"]["required"] is True


@pytest.mark.asyncio
async def test_delete_thread_rejects_active_turn_then_releases_known_thread():
    server = CodexAppServer("codex")
    server._process = SimpleNamespace(pid=4321, returncode=None)
    server._request = AsyncMock(return_value={})
    process = MagicMock(returncode=None)
    server._contexts_by_thread["thread-child"] = SimpleNamespace(
        process=process,
        thread_id="thread-child",
    )
    server._known_threads.add("thread-child")

    with pytest.raises(CodexAppServerBusyError, match="active turn"):
        await server.delete_thread("thread-child")

    process.returncode = 0
    await server.delete_thread("thread-child")

    server._request.assert_awaited_once_with(
        "thread/delete",
        {"threadId": "thread-child"},
    )
    assert "thread-child" not in server._known_threads
    assert "thread-child" not in server._contexts_by_thread


@pytest.mark.asyncio
async def test_unsubscribe_thread_rejects_active_turn_and_validates_status():
    server = CodexAppServer("codex")
    server._process = SimpleNamespace(pid=4321, returncode=None)
    server._request = AsyncMock(return_value={"status": "unsubscribed"})
    process = MagicMock(returncode=None)
    server._contexts_by_thread["thread-parent"] = SimpleNamespace(
        process=process,
        thread_id="thread-parent",
    )
    server._known_threads.add("thread-parent")

    with pytest.raises(CodexAppServerBusyError, match="active turn"):
        await server.unsubscribe_thread("thread-parent")

    process.returncode = 0
    assert await server.unsubscribe_thread("thread-parent") == "unsubscribed"
    server._request.assert_awaited_once_with(
        "thread/unsubscribe",
        {"threadId": "thread-parent"},
    )
    assert "thread-parent" in server._known_threads

    server._request = AsyncMock(return_value={"status": "unexpected"})
    with pytest.raises(CodexAppServerError, match="invalid status"):
        await server.unsubscribe_thread("thread-parent")


@pytest.mark.asyncio
async def test_concurrent_task_threads_keep_mcp_context_isolated():
    server = CodexAppServer("codex")
    server._process = SimpleNamespace(pid=4321, returncode=None)
    server.ensure_started = AsyncMock()
    thread_configs: dict[str, dict] = {}

    async def request(method, params):
        if method == "thread/start":
            args = params["config"]["mcp_servers"]["ccm_skills"]["args"]
            task_id = args[args.index("--task-id") + 1]
            thread_configs[task_id] = params["config"]
            await asyncio.sleep(0)
            return {"thread": {"id": f"thread-{task_id}"}}
        if method == "turn/start":
            await asyncio.sleep(0)
            return {"turn": {"id": f"turn-{params['threadId']}"}}
        raise AssertionError(f"unexpected method: {method}")

    server._request = AsyncMock(side_effect=request)

    await asyncio.gather(*(
        server.start_turn(
            prompt=f"task {task_id}",
            cwd="/tmp",
            model="gpt-5.6-sol",
            effort="high",
            resume_session_id=None,
            git_env=None,
            task_id=task_id,
            mcp_specs=(_task_mcp_spec(task_id),),
        )
        for task_id in (101, 202)
    ))

    assert set(thread_configs) == {"101", "202"}
    assert thread_configs["101"] is not thread_configs["202"]
    assert (
        thread_configs["101"]["mcp_servers"]["ccm_skills"]["args"][-1]
        == "101"
    )
    assert (
        thread_configs["202"]["mcp_servers"]["ccm_skills"]["args"][-1]
        == "202"
    )


@pytest.mark.asyncio
async def test_required_mcp_thread_rejection_is_explicit():
    server = CodexAppServer("codex")
    server._process = SimpleNamespace(pid=4321, returncode=None)
    server.ensure_started = AsyncMock()
    server._request = AsyncMock(
        side_effect=CodexAppServerError(
            "thread/start failed: required MCP server ccm_skills failed to initialize"
        )
    )

    with pytest.raises(CodexRequiredMcpPreTurnError, match="required MCP"):
        await server.start_turn(
            prompt="start",
            cwd="/tmp",
            model="gpt-5.6-sol",
            effort="high",
            resume_session_id=None,
            git_env=None,
            task_id=51,
            mcp_specs=(_task_mcp_spec(51),),
        )


@pytest.mark.asyncio
async def test_invalid_required_mcp_config_is_explicit_before_thread_rpc():
    server = CodexAppServer("codex")
    server._process = SimpleNamespace(pid=4321, returncode=None)
    server.ensure_started = AsyncMock()
    server._request = AsyncMock()
    invalid_spec = McpServerSpec(
        name="invalid.name",
        command="python",
        required=True,
    )

    with pytest.raises(
        CodexRequiredMcpError,
        match="Invalid required Codex MCP configuration",
    ):
        await server.start_turn(
            prompt="start",
            cwd="/tmp",
            model="gpt-5.6-sol",
            effort="high",
            resume_session_id=None,
            git_env=None,
            task_id=52,
            mcp_specs=(invalid_spec,),
        )

    server._request.assert_not_awaited()


@pytest.mark.asyncio
async def test_required_mcp_app_server_startup_failure_is_explicit():
    server = CodexAppServer("codex")
    server.ensure_started = AsyncMock(
        side_effect=CodexAppServerError("initialize failed")
    )
    server._request = AsyncMock()

    with pytest.raises(
        CodexRequiredMcpPreTurnError,
        match="could not start required MCP transport",
    ):
        await server.start_turn(
            prompt="start",
            cwd="/tmp",
            model="gpt-5.6-sol",
            effort="high",
            resume_session_id=None,
            git_env=None,
            task_id=53,
            mcp_specs=(_task_mcp_spec(53),),
        )

    server._request.assert_not_awaited()


@pytest.mark.asyncio
async def test_required_mcp_missing_thread_id_is_explicit():
    server = CodexAppServer("codex")
    server._process = SimpleNamespace(pid=4321, returncode=None)
    server.ensure_started = AsyncMock()
    server._request = AsyncMock(return_value={"thread": {}})

    with pytest.raises(
        CodexRequiredMcpPreTurnError,
        match="Required MCP configuration was not admitted",
    ):
        await server.start_turn(
            prompt="start",
            cwd="/tmp",
            model="gpt-5.6-sol",
            effort="high",
            resume_session_id=None,
            git_env=None,
            task_id=54,
            mcp_specs=(_task_mcp_spec(54),),
        )

    assert server._request.await_count == 1


@pytest.mark.asyncio
async def test_required_mcp_startup_cleanup_uncertain_is_not_replay_safe():
    server = CodexAppServer("codex")

    async def fail_after_uncertain_cleanup():
        server._shutdown_requested = True
        raise CodexAppServerError("initialize failed and cleanup was not confirmed")

    server.ensure_started = AsyncMock(side_effect=fail_after_uncertain_cleanup)
    server._request = AsyncMock()

    with pytest.raises(
        CodexRequiredMcpError,
        match="could not start required MCP transport",
    ) as exc_info:
        await server.start_turn(
            prompt="start",
            cwd="/tmp",
            model="gpt-5.6-sol",
            effort="high",
            resume_session_id=None,
            git_env=None,
            task_id=56,
            mcp_specs=(_task_mcp_spec(56),),
        )

    assert not isinstance(exc_info.value, CodexRequiredMcpPreTurnError)
    server._request.assert_not_awaited()


@pytest.mark.asyncio
async def test_required_mcp_missing_turn_id_is_explicit_and_detaches_context():
    server = CodexAppServer("codex")
    server._process = SimpleNamespace(pid=4321, returncode=None)
    server.ensure_started = AsyncMock()
    server._request = AsyncMock(side_effect=[
        {"thread": {"id": "thread-missing-turn"}},
        {"turn": {}},
    ])

    with pytest.raises(
        CodexRequiredMcpError,
        match="Required MCP turn was not admitted",
    ):
        await server.start_turn(
            prompt="start",
            cwd="/tmp",
            model="gpt-5.6-sol",
            effort="high",
            resume_session_id=None,
            git_env=None,
            task_id=55,
            mcp_specs=(_task_mcp_spec(55),),
        )

    assert "thread-missing-turn" not in server._contexts_by_thread


@pytest.mark.asyncio
async def test_steer_turn_targets_the_active_turn():
    server = CodexAppServer("codex")
    server._process = SimpleNamespace(pid=4321, returncode=None)
    server.ensure_started = AsyncMock()
    server._request = AsyncMock(side_effect=[
        {"thread": {"id": "thread-1"}},
        {"turn": {"id": "turn-1"}},
        {"turnId": "turn-1"},
    ])
    await server.start_turn(
        prompt="work", cwd="/tmp", model="gpt-5.5", effort="low",
        resume_session_id=None, git_env=None, task_id=1,
    )

    assert await server.steer_turn("thread-1", "focus on the failing test") is True
    steer_call = server._request.await_args_list[2]
    assert steer_call.args == (
        "turn/steer",
        {
            "threadId": "thread-1",
            "expectedTurnId": "turn-1",
            "input": [{"type": "text", "text": "focus on the failing test"}],
        },
    )


@pytest.mark.asyncio
async def test_existing_goal_turn_notification_rebinds_submission_id():
    """A turn/start submission can be steered into an older native goal turn."""

    server = CodexAppServer("codex")
    server._process = SimpleNamespace(pid=4321, returncode=None)
    server.ensure_started = AsyncMock()

    async def request(method, _params):
        if method == "thread/resume":
            return {"thread": {"id": "thread-goal"}}
        if method == "turn/start":
            # Task 208's active goal emitted output before the turn/start RPC
            # returned its distinct submission id.
            server._handle_notification("item/agentMessage/delta", {
                "threadId": "thread-goal",
                "turnId": "turn-active-goal",
                "itemId": "msg-goal",
                "delta": "still working",
            })
            return {"turn": {"id": "turn-submission"}}
        raise AssertionError(f"unexpected request: {method}")

    server._request = AsyncMock(side_effect=request)
    process, _ = await server.start_turn(
        prompt="continue watching",
        cwd="/tmp",
        model="gpt-5.6-sol",
        effort="high",
        resume_session_id="thread-goal",
        git_env=None,
        task_id=208,
    )

    context = server._contexts_by_thread["thread-goal"]
    assert context.admitted_turn_id == "turn-submission"
    assert context.observed_turn_id == "turn-active-goal"
    assert context.turn_id == "turn-active-goal"
    assert server._contexts_by_turn["turn-submission"] is context
    assert server._contexts_by_turn["turn-active-goal"] is context

    # Codex 0.144.6 can use the submission id again for item notifications
    # even though the native goal's active id was observed first.
    server._handle_notification("item/agentMessage/delta", {
        "threadId": "thread-goal",
        "turnId": "turn-submission",
        "itemId": "msg-submission",
        "delta": "submission-routed output",
    })

    # Once an actual turn is confirmed, an unrelated late notification must
    # not be routed into this process by thread-id fallback.
    server._handle_notification("item/agentMessage/delta", {
        "threadId": "thread-goal",
        "turnId": "turn-unrelated",
        "itemId": "msg-unrelated",
        "delta": "wrong turn",
    })
    server._handle_notification("turn/completed", {
        "threadId": "thread-goal",
        "turn": {
            "id": "turn-active-goal",
            "status": "completed",
            "error": None,
        },
    })

    rows = []
    while line := await process.stdout.readline():
        rows.append(json.loads(line))
    deltas = [row.get("delta") for row in rows if "delta" in row]
    assert deltas == ["still working", "submission-routed output"]
    assert await process.wait() == 0
    assert server._contexts_by_thread == {}
    assert server._contexts_by_turn == {}


@pytest.mark.asyncio
async def test_terminal_first_notification_settles_provisional_context():
    """A hook can finish the adopted active turn before any user item event."""

    server = CodexAppServer("codex")
    server._process = SimpleNamespace(pid=4321, returncode=None)
    server.ensure_started = AsyncMock()

    async def request(method, _params):
        if method == "thread/resume":
            return {"thread": {"id": "thread-hook"}}
        if method == "turn/start":
            server._handle_notification("turn/completed", {
                "threadId": "thread-hook",
                "turn": {
                    "id": "turn-active-hook",
                    "status": "completed",
                    "error": None,
                },
            })
            return {"turn": {"id": "turn-submission"}}
        raise AssertionError(f"unexpected request: {method}")

    server._request = AsyncMock(side_effect=request)
    process, _ = await server.start_turn(
        prompt="input intercepted by a hook",
        cwd="/tmp",
        model="gpt-5.6-sol",
        effort="high",
        resume_session_id="thread-hook",
        git_env=None,
        task_id=210,
    )

    assert await process.wait() == 0
    assert server._contexts_by_thread == {}
    assert server._contexts_by_turn == {}


@pytest.mark.asyncio
async def test_signal_interrupt_reconciles_and_pauses_existing_goal_turn():
    """Stop must pause a native goal and retry its authoritative active id."""

    server = CodexAppServer("codex")
    server._process = SimpleNamespace(pid=4321, returncode=None)
    server.ensure_started = AsyncMock()
    interrupt_ids: list[str] = []
    goal_statuses: list[str] = []

    async def request(method, params):
        if method == "thread/resume":
            return {"thread": {"id": "thread-goal"}}
        if method == "turn/start":
            return {"turn": {"id": "turn-submission"}}
        if method == "turn/interrupt":
            interrupt_ids.append(params["turnId"])
            if params["turnId"] == "turn-submission":
                raise CodexAppServerError(
                    "turn/interrupt failed: expected active turn id "
                    "turn-submission but found turn-active-goal"
                )
            asyncio.get_running_loop().call_soon(
                server._handle_notification,
                "turn/completed",
                {
                    "threadId": "thread-goal",
                    "turn": {
                        "id": "turn-active-goal",
                        "status": "interrupted",
                        "error": None,
                    },
                },
            )
            return {}
        if method == "thread/goal/set":
            goal_statuses.append(params["status"])
            return {"goal": {"status": params["status"]}}
        raise AssertionError(f"unexpected request: {method}")

    server._request = AsyncMock(side_effect=request)
    process, _ = await server.start_turn(
        prompt="continue watching",
        cwd="/tmp",
        model="gpt-5.6-sol",
        effort="high",
        resume_session_id="thread-goal",
        git_env=None,
        task_id=208,
    )

    process.send_signal(signal.SIGINT)
    assert await asyncio.wait_for(process.wait(), timeout=1) == 130
    assert interrupt_ids == ["turn-submission", "turn-active-goal"]
    assert goal_statuses == ["paused"]
    assert "turn-submission" not in server._contexts_by_turn
    assert server._contexts_by_thread == {}
    assert server._contexts_by_turn == {}


@pytest.mark.asyncio
async def test_interrupt_continues_when_goals_feature_is_disabled():
    """A normal adopted turn remains stoppable when Goals is disabled."""

    server = CodexAppServer("codex")
    server._process = SimpleNamespace(pid=4321, returncode=None)
    server.ensure_started = AsyncMock()
    interrupt_ids: list[str] = []

    async def request(method, params):
        if method == "thread/resume":
            return {"thread": {"id": "thread-regular"}}
        if method == "turn/start":
            return {"turn": {"id": "turn-submission"}}
        if method == "turn/interrupt":
            interrupt_ids.append(params["turnId"])
            if params["turnId"] == "turn-submission":
                raise CodexAppServerError(
                    "turn/interrupt failed: expected active turn id "
                    "`turn-submission` but found `turn-active-regular`"
                )
            asyncio.get_running_loop().call_soon(
                server._handle_notification,
                "turn/completed",
                {
                    "threadId": "thread-regular",
                    "turn": {
                        "id": "turn-active-regular",
                        "status": "interrupted",
                        "error": None,
                    },
                },
            )
            return {}
        if method == "thread/goal/set":
            raise CodexAppServerError(
                "thread/goal/set failed: goals feature is disabled"
            )
        raise AssertionError(f"unexpected request: {method}")

    server._request = AsyncMock(side_effect=request)
    process, _ = await server.start_turn(
        prompt="continue regular work",
        cwd="/tmp",
        model="gpt-5.6-sol",
        effort="high",
        resume_session_id="thread-regular",
        git_env=None,
        task_id=209,
    )

    process.send_signal(signal.SIGTERM)
    assert await asyncio.wait_for(process.wait(), timeout=1) == 130
    assert interrupt_ids == ["turn-submission", "turn-active-regular"]
    assert server._contexts_by_thread == {}
    assert server._contexts_by_turn == {}


@pytest.mark.asyncio
async def test_steer_retries_authoritative_active_turn_id():
    server = CodexAppServer("codex")
    server._process = SimpleNamespace(pid=4321, returncode=None)
    server.ensure_started = AsyncMock()
    steer_ids: list[str] = []

    async def request(method, params):
        if method == "thread/resume":
            return {"thread": {"id": "thread-goal"}}
        if method == "turn/start":
            return {"turn": {"id": "turn-submission"}}
        if method == "turn/steer":
            steer_ids.append(params["expectedTurnId"])
            if params["expectedTurnId"] == "turn-submission":
                raise CodexAppServerError(
                    "turn/steer failed: expected active turn id "
                    "`turn-submission` but found `turn-active-goal`"
                )
            return {"turnId": "turn-active-goal"}
        raise AssertionError(f"unexpected request: {method}")

    server._request = AsyncMock(side_effect=request)
    process, _ = await server.start_turn(
        prompt="continue watching",
        cwd="/tmp",
        model="gpt-5.6-sol",
        effort="high",
        resume_session_id="thread-goal",
        git_env=None,
        task_id=208,
    )

    assert await server.steer_turn("thread-goal", "new evidence") is True
    assert steer_ids == ["turn-submission", "turn-active-goal"]
    assert (
        server._contexts_by_thread["thread-goal"].turn_id
        == "turn-active-goal"
    )
    process.finish(0)
    server._detach_turn_context(server._contexts_by_thread["thread-goal"])


@pytest.mark.asyncio
async def test_second_turn_on_same_active_thread_is_typed_busy_error():
    server = CodexAppServer("codex")
    server._process = SimpleNamespace(pid=4321, returncode=None)
    server.ensure_started = AsyncMock()
    server._request = AsyncMock(side_effect=[
        {"thread": {"id": "thread-1"}},
        {"turn": {"id": "turn-1"}},
        {"thread": {"id": "thread-1"}},
    ])
    await server.start_turn(
        prompt="first", cwd="/tmp", model="gpt-5.5", effort="low",
        resume_session_id="thread-1", git_env=None, task_id=1,
    )

    with pytest.raises(CodexAppServerBusyError, match="already has an active turn"):
        await server.start_turn(
            prompt="second", cwd="/tmp", model="gpt-5.5", effort="low",
            resume_session_id="thread-1", git_env=None, task_id=1,
        )


@pytest.mark.asyncio
async def test_steer_turn_without_active_context_does_not_send_rpc():
    server = CodexAppServer("codex")
    server._process = SimpleNamespace(pid=4321, returncode=None)
    server._request = AsyncMock()

    assert await server.steer_turn("thread-gone", "too late") is False
    server._request.assert_not_awaited()


@pytest.mark.asyncio
async def test_read_rate_limits_uses_parameterless_account_rpc():
    server = CodexAppServer("codex")
    server.ensure_started = AsyncMock()
    server._request = AsyncMock(return_value={
        "rateLimits": {"primary": {"usedPercent": 100}},
    })

    result = await server.read_rate_limits()

    assert result["rateLimits"]["primary"]["usedPercent"] == 100
    server.ensure_started.assert_awaited_once()
    server._request.assert_awaited_once_with("account/rateLimits/read", None)


@pytest.mark.asyncio
async def test_parameterless_request_omits_params_field():
    server = CodexAppServer("codex")
    server._process = SimpleNamespace(
        pid=4321,
        returncode=None,
        stdin=object(),
    )
    sent = []

    async def respond(message):
        sent.append(message)
        server._pending[message["id"]].set_result({
            "id": message["id"],
            "result": {"rateLimits": {}},
        })

    server._write = AsyncMock(side_effect=respond)

    await server._request("account/rateLimits/read", None)

    assert sent == [{"id": 1, "method": "account/rateLimits/read"}]


@pytest.mark.asyncio
async def test_request_cancelled_during_write_drops_pending_future():
    server = CodexAppServer("codex")
    server._process = SimpleNamespace(
        pid=4321,
        returncode=None,
        stdin=object(),
    )
    write_entered = asyncio.Event()

    async def blocked_write(_message):
        write_entered.set()
        await asyncio.Event().wait()

    server._write = AsyncMock(side_effect=blocked_write)
    request = asyncio.create_task(server._request("turn/start", {}))
    await write_entered.wait()
    assert len(server._pending) == 1

    request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request
    assert server._pending == {}


@pytest.mark.asyncio
async def test_steer_turn_protocol_rejection_is_a_normal_false_result():
    server = CodexAppServer("codex")
    server._process = SimpleNamespace(pid=4321, returncode=None)
    server.ensure_started = AsyncMock()
    server._request = AsyncMock(side_effect=[
        {"thread": {"id": "thread-1"}},
        {"turn": {"id": "turn-1"}},
        CodexAppServerError("active turn changed"),
    ])
    await server.start_turn(
        prompt="work", cwd="/tmp", model="gpt-5.5", effort="low",
        resume_session_id=None, git_env=None, task_id=1,
    )

    assert await server.steer_turn("thread-1", "late input") is False


@pytest.mark.asyncio
async def test_notifications_stream_delta_and_finish_process():
    server = CodexAppServer("codex")
    server._process = SimpleNamespace(pid=4321, returncode=None)
    server.ensure_started = AsyncMock()
    server._request = AsyncMock(side_effect=[
        {"thread": {"id": "thread-1"}},
        {"turn": {"id": "turn-1"}},
    ])
    process, _ = await server.start_turn(
        prompt="hi", cwd="/tmp", model="gpt-5.5", effort="low",
        resume_session_id=None, git_env=None, task_id=1,
    )
    # Consume the synthetic thread.started line.
    await process.stdout.readline()

    server._handle_notification("item/agentMessage/delta", {
        "threadId": "thread-1", "turnId": "turn-1",
        "itemId": "msg-1", "delta": "Hel",
    })
    server._handle_notification("item/completed", {
        "threadId": "thread-1", "turnId": "turn-1",
        "item": {"type": "agentMessage", "id": "msg-1", "text": "Hello"},
    })
    server._handle_notification("thread/tokenUsage/updated", {
        "threadId": "thread-1", "turnId": "turn-1",
        "tokenUsage": {
            "last": {
                "inputTokens": 100,
                "cachedInputTokens": 80,
                "outputTokens": 5,
                "reasoningOutputTokens": 2,
                "totalTokens": 105,
            },
            "total": {
                "inputTokens": 300,
                "cachedInputTokens": 240,
                "outputTokens": 15,
                "reasoningOutputTokens": 6,
                "totalTokens": 315,
            },
            "modelContextWindow": 258_400,
        },
    })
    server._handle_notification("turn/completed", {
        "threadId": "thread-1",
        "turn": {"id": "turn-1", "status": "completed", "error": None},
    })

    lines = []
    while True:
        line = await process.stdout.readline()
        if not line:
            break
        lines.append(json.loads(line))
    assert lines[0] == {
        "type": "item.agent_message.delta",
        "delta": "Hel",
        "item_id": "msg-1",
        "turn_id": "turn-1",
    }
    assert lines[1]["type"] == "item.completed"
    assert lines[1]["item"]["type"] == "agent_message"
    assert lines[1]["turn_id"] == "turn-1"
    assert lines[2] == {
        "type": "turn.completed",
        "turn_id": "turn-1",
        "usage": {
            "input_tokens": 100,
            "cached_input_tokens": 80,
            "output_tokens": 5,
            "reasoning_output_tokens": 2,
            "total_tokens": 105,
            "context_window": 258_400,
        },
    }
    assert await process.wait() == 0


@pytest.mark.asyncio
async def test_read_and_fork_thread_use_native_app_server_protocol():
    server = CodexAppServer("codex")
    server.ensure_started = AsyncMock()
    server._request = AsyncMock(side_effect=[
        {
            "thread": {
                "id": "thread-source",
                "turns": [{"id": "turn-1", "status": "completed", "items": []}],
            },
        },
        {
            "thread": {
                "id": "thread-fork",
                "forkedFromId": "thread-source",
                "turns": [{"id": "turn-1", "status": "completed", "items": []}],
            },
        },
    ])

    source = await server.read_thread("thread-source")
    forked = await server.fork_thread(
        "thread-source",
        last_turn_id="turn-1",
    )

    assert source["turns"][0]["id"] == "turn-1"
    assert forked["id"] == "thread-fork"
    assert "thread-fork" in server._known_threads
    assert server._request.await_args_list[0].args == (
        "thread/read",
        {"threadId": "thread-source", "includeTurns": True},
    )
    assert server._request.await_args_list[1].args == (
        "thread/fork",
        {
            "threadId": "thread-source",
            "lastTurnId": "turn-1",
            "approvalPolicy": "never",
            "sandbox": "danger-full-access",
        },
    )


@pytest.mark.asyncio
async def test_create_empty_thread_uses_native_start_without_turn():
    server = CodexAppServer("codex")
    server.ensure_started = AsyncMock()
    server._request = AsyncMock(return_value={
        "thread": {"id": "thread-empty", "turns": []},
    })

    created = await server.create_thread(
        cwd="/tmp/project",
        model="gpt-5.6-sol",
    )

    assert created["id"] == "thread-empty"
    assert "thread-empty" in server._known_threads
    server._request.assert_awaited_once_with(
        "thread/start",
        {
            "cwd": "/tmp/project",
            "approvalPolicy": "never",
            "sandbox": "danger-full-access",
            "model": "gpt-5.6-sol",
        },
    )


@pytest.mark.asyncio
async def test_fork_thread_settles_mutating_rpc_after_cancellation():
    server = CodexAppServer("codex")
    server.ensure_started = AsyncMock()
    entered = asyncio.Event()
    release = asyncio.Event()

    async def request(_method, _params):
        entered.set()
        await release.wait()
        return {"thread": {"id": "thread-fork"}}

    server._request = AsyncMock(side_effect=request)
    operation = asyncio.create_task(server.fork_thread(
        "thread-source",
        last_turn_id="turn-1",
    ))
    await entered.wait()
    operation.cancel()
    await asyncio.sleep(0)
    assert not operation.done()

    release.set()
    result = await operation
    assert result["id"] == "thread-fork"


@pytest.mark.asyncio
async def test_context_window_error_keeps_structured_codex_error_info():
    server = CodexAppServer("codex")
    server._process = SimpleNamespace(pid=4321, returncode=None)
    server.ensure_started = AsyncMock()
    server._request = AsyncMock(side_effect=[
        {"thread": {"id": "thread-1"}},
        {"turn": {"id": "turn-1"}},
    ])
    process, _ = await server.start_turn(
        prompt="continue",
        cwd="/tmp",
        model="gpt-5.6-terra",
        effort="medium",
        resume_session_id=None,
        git_env=None,
        task_id=1,
    )
    await process.stdout.readline()

    error = {
        "message": "The request could not be completed.",
        "codexErrorInfo": "contextWindowExceeded",
        "additionalDetails": "effective window exhausted",
    }
    server._handle_notification("turn/completed", {
        "threadId": "thread-1",
        "turn": {
            "id": "turn-1",
            "status": "failed",
            "error": error,
        },
    })

    failed = json.loads((await process.stdout.readline()).decode())
    assert failed == {"type": "turn.failed", "error": error}
    assert await process.wait() == 1


@pytest.mark.asyncio
async def test_interleaved_notifications_are_isolated_by_turn():
    """Concurrent tasks must never receive another thread's output."""
    server = CodexAppServer("codex")
    server._process = SimpleNamespace(pid=4321, returncode=None)
    server.ensure_started = AsyncMock()
    server._request = AsyncMock(side_effect=[
        {"thread": {"id": "thread-a"}},
        {"turn": {"id": "turn-a"}},
        {"thread": {"id": "thread-b"}},
        {"turn": {"id": "turn-b"}},
    ])
    process_a, _ = await server.start_turn(
        prompt="a", cwd="/tmp", model="gpt-5.5", effort="low",
        resume_session_id=None, git_env=None, task_id=1,
    )
    process_b, _ = await server.start_turn(
        prompt="b", cwd="/tmp", model="gpt-5.5", effort="low",
        resume_session_id=None, git_env=None, task_id=2,
    )
    await process_a.stdout.readline()
    await process_b.stdout.readline()

    # Deliberately deliver B before A, as happens under real concurrent turns.
    for thread, turn, item, text in (
        ("thread-b", "turn-b", "msg-b", "B"),
        ("thread-a", "turn-a", "msg-a", "A"),
    ):
        server._handle_notification("item/agentMessage/delta", {
            "threadId": thread, "turnId": turn, "itemId": item, "delta": text,
        })
        server._handle_notification("item/completed", {
            "threadId": thread, "turnId": turn,
            "item": {"type": "agentMessage", "id": item, "text": text},
        })
        server._handle_notification("turn/completed", {
            "threadId": thread,
            "turn": {"id": turn, "status": "completed", "error": None},
        })

    async def read_all(process):
        rows = []
        while line := await process.stdout.readline():
            rows.append(json.loads(line))
        return rows

    rows_a, rows_b = await asyncio.gather(read_all(process_a), read_all(process_b))
    assert [row.get("delta") for row in rows_a if "delta" in row] == ["A"]
    assert [row.get("delta") for row in rows_b if "delta" in row] == ["B"]
    assert rows_a[1]["item"]["text"] == "A"
    assert rows_b[1]["item"]["text"] == "B"
    assert await process_a.wait() == await process_b.wait() == 0


@pytest.mark.asyncio
async def test_reader_exit_fails_pending_requests_and_active_turns():
    """A crashed shared process must unblock every waiter instead of hanging."""
    server = CodexAppServer("codex")
    server._process = SimpleNamespace(pid=4321, returncode=None)
    server.ensure_started = AsyncMock()
    server._request = AsyncMock(side_effect=[
        {"thread": {"id": "thread-1"}},
        {"turn": {"id": "turn-1"}},
    ])
    turn_process, _ = await server.start_turn(
        prompt="hi", cwd="/tmp", model="gpt-5.5", effort="low",
        resume_session_id=None, git_env=None, task_id=1,
    )
    await turn_process.stdout.readline()
    pending = asyncio.get_running_loop().create_future()
    server._pending[99] = pending

    stdout = asyncio.StreamReader()
    stdout.feed_eof()
    fake_process = SimpleNamespace(
        stdout=stdout,
        wait=AsyncMock(return_value=1),
    )
    await server._read_loop(fake_process)

    assert await turn_process.wait() == 1
    assert not server._contexts_by_thread
    assert not server._contexts_by_turn
    assert not server._pending
    with pytest.raises(CodexAppServerError, match="exited unexpectedly"):
        await pending


def test_normalize_app_server_command_item():
    normalized = CodexAppServer._normalize_item({
        "type": "commandExecution",
        "id": "cmd-1",
        "command": "pwd",
        "aggregatedOutput": "/tmp\n",
        "exitCode": 0,
        "status": "completed",
    })
    assert normalized["type"] == "command_execution"
    assert normalized["aggregated_output"] == "/tmp\n"
    assert normalized["exit_code"] == 0


@pytest.mark.asyncio
async def test_turn_process_interrupt_is_nonblocking_and_completes():
    interrupted = asyncio.Event()

    async def interrupt():
        interrupted.set()

    process = CodexTurnProcess(1, interrupt)
    process.send_signal(2)
    await asyncio.wait_for(interrupted.wait(), timeout=1)
    process.finish(130)
    assert await process.wait() == 130


@pytest.mark.asyncio
async def test_turn_process_failed_interrupt_preserves_active_evidence():
    interrupt_attempted = asyncio.Event()

    async def interrupt():
        interrupt_attempted.set()
        raise CodexAppServerError("interrupt RPC failed")

    process = CodexTurnProcess(4321, interrupt)
    process.send_signal(signal.SIGINT)
    await asyncio.wait_for(interrupt_attempted.wait(), timeout=1)
    await asyncio.sleep(0)

    assert process.returncode is None
    wait_task = asyncio.create_task(process.wait())
    await asyncio.sleep(0)
    assert not wait_task.done()

    # Complete the adapter explicitly so this test does not leave a pending
    # process waiter; only a real turn terminal event may do this in service.
    process.finish(1)
    assert await wait_task == 1


@pytest.mark.asyncio
@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group contract")
async def test_app_server_spawn_uses_independent_session(tmp_path):
    server = CodexAppServer("codex", codex_home=tmp_path / "account")
    spawn = AsyncMock(side_effect=RuntimeError("synthetic spawn failure"))

    with patch(
        "backend.services.codex_app_server.asyncio.create_subprocess_exec",
        spawn,
    ):
        with pytest.raises(RuntimeError, match="synthetic spawn failure"):
            await server._start()

    assert spawn.await_args.kwargs["start_new_session"] is True


@pytest.mark.asyncio
async def test_app_server_removes_account_specific_inherited_auth_env(tmp_path):
    server = CodexAppServer(
        "codex",
        codex_home=tmp_path / "api-account",
        env_remove={"OPENAI_API_KEY", "CLOUDROUTER_API_KEY"},
    )
    spawn = AsyncMock(side_effect=RuntimeError("synthetic spawn failure"))

    with (
        patch.dict(
            os.environ,
            {
                "OPENAI_API_KEY": "must-not-leak",
                "CLOUDROUTER_API_KEY": "must-not-leak",
                "UNRELATED": "preserved",
            },
            clear=False,
        ),
        patch(
            "backend.services.codex_app_server.asyncio.create_subprocess_exec",
            spawn,
        ),
    ):
        with pytest.raises(RuntimeError, match="synthetic spawn failure"):
            await server._start()

    child_env = spawn.await_args.kwargs["env"]
    assert "OPENAI_API_KEY" not in child_env
    assert "CLOUDROUTER_API_KEY" not in child_env
    assert child_env["UNRELATED"] == "preserved"


@pytest.mark.asyncio
async def test_shutdown_intent_blocks_a_start_already_waiting_on_lifecycle_lock(
    tmp_path,
):
    server = CodexAppServer("codex", codex_home=tmp_path / "account")
    server._start = AsyncMock()
    await server._lifecycle_lock.acquire()
    start = asyncio.create_task(server.ensure_started())
    await asyncio.sleep(0)
    shutdown = asyncio.create_task(server.shutdown())
    await asyncio.sleep(0)

    assert server._shutdown_requested is True
    server._lifecycle_lock.release()
    with pytest.raises(CodexAppServerBusyError, match="shutting down"):
        await start
    await shutdown
    server._start.assert_not_awaited()


class _ShutdownProcess:
    def __init__(self, pid: int = 4321) -> None:
        self.pid = pid
        self.returncode = None
        self.stdin = MagicMock()
        self._exited = asyncio.Event()
        self.terminate = MagicMock()
        self.kill = MagicMock()
        self.send_signal = MagicMock()

    async def wait(self):
        await self._exited.wait()
        return self.returncode

    def exit(self, returncode: int) -> None:
        self.returncode = returncode
        self._exited.set()


@pytest.mark.asyncio
@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group contract")
async def test_restart_stops_dead_leader_group_before_waiting_on_reader(tmp_path):
    server = CodexAppServer("codex", codex_home=tmp_path / "account")
    process = _ShutdownProcess()
    process.returncode = 1
    server._process = process
    server._process_group_process = process
    server._reader_task = asyncio.create_task(asyncio.Event().wait())
    server._start = AsyncMock()
    group_alive = True
    sent_signals = []

    def killpg(_pid, sig):
        nonlocal group_alive
        if sig == 0:
            if group_alive:
                return
            raise ProcessLookupError
        sent_signals.append(sig)
        if sig == signal.SIGKILL:
            group_alive = False

    with (
        patch.multiple(
            "backend.services.codex_app_server",
            _APP_SERVER_GRACEFUL_SHUTDOWN_TIMEOUT=0.001,
            _APP_SERVER_TERM_SHUTDOWN_TIMEOUT=0.001,
            _APP_SERVER_KILL_SHUTDOWN_TIMEOUT=0.001,
            _APP_SERVER_GROUP_POLL_INTERVAL=0.001,
        ),
        patch("backend.services.codex_app_server.os.killpg", side_effect=killpg),
    ):
        await asyncio.wait_for(server.ensure_started(), timeout=1)

    assert sent_signals == [signal.SIGTERM, signal.SIGKILL]
    assert server._reader_task is None
    server._start.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_failed_initialize_cleanup_permanently_gates_server_and_home(
    tmp_path,
):
    home = normalize_codex_home(tmp_path / "account")
    server = CodexAppServer("codex", codex_home=home)
    process = _ShutdownProcess()
    process.stdout = asyncio.StreamReader()
    process.stderr = asyncio.StreamReader()
    server._request = AsyncMock(side_effect=RuntimeError("initialize failed"))
    server._shutdown_locked = AsyncMock(
        side_effect=CodexAppServerError("cleanup unconfirmed"),
    )

    with patch(
        "backend.services.codex_app_server.asyncio.create_subprocess_exec",
        AsyncMock(return_value=process),
    ):
        with pytest.raises(CodexAppServerError, match="cleanup unconfirmed"):
            await server._start()

    assert server.shutdown_requested is True
    with pytest.raises(CodexAppServerBusyError, match="shutting down"):
        await server.ensure_started()

    registry = CodexAppServerRegistry("codex")
    registry._servers[home] = server
    with pytest.raises(CodexAppServerBusyError, match="shutting down"):
        await registry.start_turn(
            codex_home=home,
            prompt="work",
            cwd="/tmp",
            model=None,
            effort=None,
            resume_session_id=None,
            git_env=None,
            task_id=1,
        )
    assert registry._servers[home] is server
    assert home in registry._draining

    process.exit(1)
    process.stdout.feed_eof()
    process.stderr.feed_eof()
    for task in (server._reader_task, server._stderr_task):
        if task and not task.done():
            task.cancel()
    await asyncio.gather(
        *(task for task in (server._reader_task, server._stderr_task) if task),
        return_exceptions=True,
    )


@pytest.mark.asyncio
@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group contract")
async def test_app_server_shutdown_terminates_and_verifies_full_process_group(
    tmp_path,
):
    server = CodexAppServer("codex", codex_home=tmp_path / "account")
    process = _ShutdownProcess()
    server._process = process
    server._process_group_process = process
    group_alive = True
    sent_signals = []

    def killpg(pid, sig):
        nonlocal group_alive
        assert pid == process.pid
        if sig == 0:
            if group_alive:
                return
            raise ProcessLookupError
        sent_signals.append(sig)
        if sig == signal.SIGKILL:
            group_alive = False
            process.exit(-signal.SIGKILL)

    with (
        patch.multiple(
            "backend.services.codex_app_server",
            _APP_SERVER_GRACEFUL_SHUTDOWN_TIMEOUT=0.001,
            _APP_SERVER_TERM_SHUTDOWN_TIMEOUT=0.001,
            _APP_SERVER_KILL_SHUTDOWN_TIMEOUT=0.001,
            _APP_SERVER_GROUP_POLL_INTERVAL=0.001,
        ),
        patch("backend.services.codex_app_server.os.killpg", side_effect=killpg),
    ):
        await server.shutdown()

    assert sent_signals == [signal.SIGTERM, signal.SIGKILL]
    process.terminate.assert_not_called()
    process.kill.assert_not_called()
    assert server._process is None
    assert server._process_group_process is None


@pytest.mark.asyncio
@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group contract")
async def test_app_server_shutdown_retains_evidence_if_group_survives_sigkill(
    tmp_path,
):
    server = CodexAppServer("codex", codex_home=tmp_path / "account")
    process = _ShutdownProcess()
    server._process = process
    server._process_group_process = process
    sent_signals = []

    def killpg(_pid, sig):
        if sig != 0:
            sent_signals.append(sig)
            if sig == signal.SIGKILL:
                # The leader exited, but a descendant still answers the group
                # liveness probe.
                process.exit(-signal.SIGKILL)

    with (
        patch.multiple(
            "backend.services.codex_app_server",
            _APP_SERVER_GRACEFUL_SHUTDOWN_TIMEOUT=0.001,
            _APP_SERVER_TERM_SHUTDOWN_TIMEOUT=0.001,
            _APP_SERVER_KILL_SHUTDOWN_TIMEOUT=0.001,
            _APP_SERVER_GROUP_POLL_INTERVAL=0.001,
        ),
        patch("backend.services.codex_app_server.os.killpg", side_effect=killpg),
    ):
        with pytest.raises(CodexAppServerError, match="survived SIGKILL"):
            await server.shutdown()

    assert sent_signals == [signal.SIGTERM, signal.SIGKILL]
    assert server._process is process
    assert server._process_group_process is process


@pytest.mark.asyncio
@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group contract")
async def test_app_server_shutdown_rejects_unsafe_process_group(tmp_path):
    server = CodexAppServer("codex", codex_home=tmp_path / "account")
    process = _ShutdownProcess(pid=1)
    server._process = process
    server._process_group_process = process

    with (
        patch(
            "backend.services.codex_app_server."
            "_APP_SERVER_GRACEFUL_SHUTDOWN_TIMEOUT",
            0.001,
        ),
        patch("backend.services.codex_app_server.os.killpg") as killpg,
    ):
        with pytest.raises(UnsafeProcessGroupError, match="unsafe process group"):
            await server.shutdown()

    killpg.assert_not_called()
    assert server._process is process
    assert server._process_group_process is process
    process.exit(0)
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_reader_delivers_json_rpc_response_to_pending_request():
    """A protocol response must resolve exactly one waiting request."""
    stdout = asyncio.StreamReader()
    stdout.feed_data(b'{"id":7,"result":{"ok":true}}\n')
    stdout.feed_eof()
    fake_process = SimpleNamespace(
        stdout=stdout,
        wait=AsyncMock(return_value=0),
    )
    server = CodexAppServer("codex")
    pending = asyncio.get_running_loop().create_future()
    server._pending[7] = pending

    await server._read_loop(fake_process)

    assert pending.result() == {"id": 7, "result": {"ok": True}}


@pytest.mark.asyncio
async def test_server_requests_use_protocol_specific_approval_shapes():
    server = CodexAppServer("codex")
    server._write = AsyncMock()

    await server._handle_server_request({
        "id": 1,
        "method": "item/commandExecution/requestApproval",
        "params": {},
    })
    await server._handle_server_request({
        "id": 2,
        "method": "item/permissions/requestApproval",
        "params": {"permissions": {"network": {"enabled": True}}},
    })
    await server._handle_server_request({
        "id": 3,
        "method": "execCommandApproval",
        "params": {},
    })

    assert server._write.await_args_list[0].args[0] == {
        "id": 1, "result": {"decision": "accept"}
    }
    assert server._write.await_args_list[1].args[0] == {
        "id": 2,
        "result": {
            "permissions": {"network": {"enabled": True}},
            "scope": "turn",
        },
    }
    assert server._write.await_args_list[2].args[0] == {
        "id": 3, "result": {"decision": "approved"}
    }


class _RegistryFakeServer:
    instances = []

    def __init__(self, binary, request_timeout=30.0, *, codex_home=None):
        self.binary = binary
        self.request_timeout = request_timeout
        self.codex_home = normalize_codex_home(codex_home)
        self.active_threads = set()
        self.known_threads = set()
        self.shutdown_count = 0
        self.steered = []
        type(self).instances.append(self)

    @property
    def has_active_turns(self):
        return bool(self.active_threads)

    def has_active_thread(self, thread_id):
        return thread_id in self.active_threads

    def knows_thread(self, thread_id):
        return thread_id in self.known_threads

    async def start_turn(self, **kwargs):
        thread_id = kwargs.get("resume_session_id") or f"thread-{kwargs['task_id']}"
        self.active_threads.add(thread_id)
        self.known_threads.add(thread_id)
        return MagicMock(terminate=MagicMock()), thread_id

    async def steer_turn(self, thread_id, content):
        self.steered.append((thread_id, content))
        return thread_id in self.active_threads

    async def read_thread(self, thread_id):
        self.known_threads.add(thread_id)
        return {
            "id": thread_id,
            "turns": [{"id": "turn-1", "status": "completed", "items": []}],
        }

    async def create_thread(self, *, cwd, model=None):
        thread_id = f"thread-empty-{len(self.known_threads) + 1}"
        self.known_threads.add(thread_id)
        return {"id": thread_id, "cwd": cwd, "model": model, "turns": []}

    async def fork_thread(self, thread_id, *, last_turn_id):
        fork_id = f"{thread_id}-fork"
        self.known_threads.add(fork_id)
        return {
            "id": fork_id,
            "forkedFromId": thread_id,
            "turns": [{"id": last_turn_id, "status": "completed", "items": []}],
        }

    async def delete_thread(self, thread_id):
        if thread_id in self.active_threads:
            raise CodexAppServerBusyError(f"{thread_id} is active")
        self.known_threads.discard(thread_id)

    async def unsubscribe_thread(self, thread_id):
        if thread_id in self.active_threads:
            raise CodexAppServerBusyError(f"{thread_id} is active")
        return "unsubscribed"

    async def read_rate_limits(self):
        return {
            "rateLimits": {
                "limitId": "codex",
                "primary": {"usedPercent": 42},
            }
        }

    async def shutdown(self):
        self.shutdown_count += 1
        self.active_threads.clear()


@pytest.fixture(autouse=False)
def reset_registry_fake_servers():
    _RegistryFakeServer.instances = []
    yield
    _RegistryFakeServer.instances = []


@pytest.mark.asyncio
@pytest.mark.parametrize("shutdown_fails", [False, True])
async def test_cancelled_turn_with_unconfirmed_interrupt_escalates_transport(
    tmp_path,
    shutdown_fails,
):
    """A failed interrupt cannot reopen a home onto untracked model work."""

    turn_start_entered = asyncio.Event()
    release_turn_start = asyncio.Event()
    home = normalize_codex_home(tmp_path / "interrupt-failure")
    thread_id = "thread-interrupt-failure"
    server = CodexAppServer("codex", codex_home=home)
    server._process = SimpleNamespace(pid=4321, returncode=None)
    server.ensure_started = AsyncMock()

    async def request(method, _params):
        if method == "thread/resume":
            return {"thread": {"id": thread_id}}
        if method == "turn/start":
            turn_start_entered.set()
            await release_turn_start.wait()
            return {"turn": {"id": "turn-interrupt-failure"}}
        if method == "turn/interrupt":
            raise CodexAppServerError("interrupt RPC failed")
        raise AssertionError(f"unexpected request: {method}")

    server._request = AsyncMock(side_effect=request)
    if shutdown_fails:
        server.shutdown = AsyncMock(side_effect=RuntimeError("shutdown failed"))
    else:
        server.shutdown = AsyncMock()

    registry = CodexAppServerRegistry("codex")
    registry._servers[home] = server
    start = asyncio.create_task(registry.start_turn(
        codex_home=home,
        prompt="continue",
        cwd="/tmp",
        model=None,
        effort=None,
        resume_session_id=thread_id,
        git_env=None,
        task_id=1,
    ))
    await turn_start_entered.wait()
    start.cancel()
    await asyncio.sleep(0)
    release_turn_start.set()

    if shutdown_fails:
        with pytest.raises(RuntimeError, match="shutdown failed"):
            await start
        assert home in registry._draining
        assert registry._servers[home] is server
    else:
        with pytest.raises(asyncio.CancelledError):
            await start
        assert home not in registry._draining
        assert home not in registry._servers

    server.shutdown.assert_awaited_once()
    assert home not in registry._starting
    assert thread_id not in registry._thread_owners
    assert server._contexts_by_thread == {}
    assert server._contexts_by_turn == {}


@pytest.mark.asyncio
async def test_turn_start_timeout_shuts_transport_to_avoid_untracked_work(
    tmp_path,
):
    home = normalize_codex_home(tmp_path / "turn-timeout")
    thread_id = "thread-timeout"
    server = CodexAppServer("codex", codex_home=home)
    server._process = SimpleNamespace(pid=4321, returncode=None)
    server.ensure_started = AsyncMock()
    server._request = AsyncMock(side_effect=[
        {"thread": {"id": thread_id}},
        asyncio.TimeoutError(),
    ])
    server.shutdown = AsyncMock()

    registry = CodexAppServerRegistry("codex")
    registry._servers[home] = server

    with pytest.raises(
        CodexAppServerError,
        match="turn/start timed out with unknown server state",
    ):
        await registry.start_turn(
            codex_home=home,
            prompt="continue",
            cwd="/tmp",
            model=None,
            effort=None,
            resume_session_id=thread_id,
            git_env=None,
            task_id=1,
        )

    server.shutdown.assert_awaited_once()
    assert home not in registry._servers
    assert home not in registry._draining
    assert home not in registry._starting
    assert thread_id not in registry._thread_owners
    assert server._contexts_by_thread == {}


@pytest.mark.asyncio
async def test_registry_cancel_while_final_admission_lock_contended_cleans_once(
    tmp_path,
):
    start_entered = asyncio.Event()
    release_start = asyncio.Event()

    class LockContentionServer(_RegistryFakeServer):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.process = None
            self.abandoned = 0

        async def start_turn(self, **kwargs):
            start_entered.set()
            await release_start.wait()

            async def interrupt():
                return None

            self.process = CodexTurnProcess(99, interrupt)
            thread = kwargs["resume_session_id"]
            self.active_threads.add(thread)
            return self.process, thread

        async def abandon_turn(self, process, reason):
            self.abandoned += 1
            self.active_threads.clear()
            process.finish(130, reason)
            return True

    registry = CodexAppServerRegistry("codex")
    home = normalize_codex_home(tmp_path / "lock-contention")
    thread_id = "thread-lock-contention"
    server = LockContentionServer("codex", codex_home=home)
    registry._servers[home] = server

    start = asyncio.create_task(registry.start_turn(
        codex_home=home,
        resume_session_id=thread_id,
        task_id=2,
    ))
    await start_entered.wait()
    await registry._lock.acquire()
    try:
        release_start.set()
        await asyncio.sleep(0)
        assert not start.done()
        start.cancel()
        await asyncio.sleep(0)
        assert home in registry._starting
    finally:
        registry._lock.release()

    with pytest.raises(asyncio.CancelledError):
        await start

    assert server.abandoned == 1
    assert server.process.returncode == 130
    assert home not in registry._starting
    assert thread_id not in registry._thread_owners
    assert home not in registry._draining
    assert registry._servers[home] is server


@pytest.mark.asyncio
async def test_registry_rejects_concurrent_resume_of_same_thread(tmp_path):
    entered = asyncio.Event()
    release = asyncio.Event()

    class BlockingResumeServer(_RegistryFakeServer):
        async def start_turn(self, **kwargs):
            entered.set()
            await release.wait()
            return await super().start_turn(**kwargs)

    registry = CodexAppServerRegistry("codex")
    home = normalize_codex_home(tmp_path / "same-thread")
    thread_id = "thread-one-resume"
    server = BlockingResumeServer("codex", codex_home=home)
    registry._servers[home] = server

    first = asyncio.create_task(registry.start_turn(
        codex_home=home,
        resume_session_id=thread_id,
        task_id=1,
    ))
    await entered.wait()
    try:
        with pytest.raises(CodexAppServerBusyError, match="resume request in flight"):
            await registry.start_turn(
                codex_home=home,
                resume_session_id=thread_id,
                task_id=2,
            )
    finally:
        release.set()
        await first

    assert registry._thread_owners[thread_id] == home
    assert thread_id not in registry._starting_threads


@pytest.mark.asyncio
async def test_registry_routes_each_canonical_home_to_one_server(
    tmp_path, reset_registry_fake_servers,
):
    registry = CodexAppServerRegistry("codex", request_timeout=7)
    home_a = tmp_path / "a" / ".." / "a"
    home_b = tmp_path / "b"

    with patch(
        "backend.services.codex_app_server.CodexAppServer",
        _RegistryFakeServer,
    ):
        _, thread_a = await registry.start_turn(
            codex_home=home_a, resume_session_id=None, task_id=1,
        )
        _, thread_b = await registry.start_turn(
            codex_home=home_b, resume_session_id=None, task_id=2,
        )
        assert await registry.steer_turn(thread_a, "a-only") is True

    assert thread_a == "thread-1"
    assert thread_b == "thread-2"
    assert len(_RegistryFakeServer.instances) == 2
    assert {server.codex_home for server in _RegistryFakeServer.instances} == {
        normalize_codex_home(home_a),
        normalize_codex_home(home_b),
    }
    server_a = next(
        server for server in _RegistryFakeServer.instances
        if server.codex_home == normalize_codex_home(home_a)
    )
    server_b = next(
        server for server in _RegistryFakeServer.instances
        if server.codex_home == normalize_codex_home(home_b)
    )
    assert server_a.steered == [(thread_a, "a-only")]
    assert server_b.steered == []


@pytest.mark.asyncio
async def test_registry_delete_thread_releases_exact_owner_without_shutdown(
    tmp_path, reset_registry_fake_servers,
):
    registry = CodexAppServerRegistry("codex")
    home = normalize_codex_home(tmp_path / "ephemeral")

    with patch(
        "backend.services.codex_app_server.CodexAppServer",
        _RegistryFakeServer,
    ):
        _, thread_id = await registry.start_turn(
            codex_home=home,
            resume_session_id=None,
            task_id=51,
        )
        server = registry._servers[home]
        server.active_threads.discard(thread_id)

        await registry.delete_thread(home, thread_id)

    assert thread_id not in registry._thread_owners
    assert thread_id not in registry._starting_threads
    assert home not in registry._starting
    assert server.shutdown_count == 0
    assert registry._servers[home] is server


@pytest.mark.asyncio
async def test_registry_fork_keeps_source_and_new_thread_in_same_home(
    tmp_path, reset_registry_fake_servers,
):
    registry = CodexAppServerRegistry("codex")
    home = normalize_codex_home(tmp_path / "fork-home")
    source_id = "thread-source"

    with patch(
        "backend.services.codex_app_server.CodexAppServer",
        _RegistryFakeServer,
    ):
        source = await registry.read_thread(home, source_id)
        forked = await registry.fork_thread(
            home,
            source_id,
            last_turn_id="turn-1",
        )
        assert source["id"] == source_id
        assert forked["id"] == "thread-source-fork"
        assert registry._thread_owners[source_id] == home
        assert registry._thread_owners[forked["id"]] == home
        assert source_id not in registry._starting_threads
        assert home not in registry._starting

        await registry.delete_thread(home, forked["id"])

    assert forked["id"] not in registry._thread_owners
    assert forked["id"] not in registry._starting_threads
    assert home not in registry._starting


@pytest.mark.asyncio
async def test_registry_registers_new_empty_thread_owner(
    tmp_path, reset_registry_fake_servers,
):
    registry = CodexAppServerRegistry("codex")
    home = normalize_codex_home(tmp_path / "empty-home")

    with patch(
        "backend.services.codex_app_server.CodexAppServer",
        _RegistryFakeServer,
    ):
        created = await registry.create_thread(
            home,
            cwd="/tmp/project",
            model="gpt-5.6-sol",
        )

    assert registry._thread_owners[created["id"]] == home
    assert home not in registry._starting


@pytest.mark.asyncio
async def test_registry_unsubscribe_preserves_resumable_owner_without_shutdown(
    tmp_path, reset_registry_fake_servers,
):
    registry = CodexAppServerRegistry("codex")
    home = normalize_codex_home(tmp_path / "resumable")

    with patch(
        "backend.services.codex_app_server.CodexAppServer",
        _RegistryFakeServer,
    ):
        _, thread_id = await registry.start_turn(
            codex_home=home,
            resume_session_id=None,
            task_id=52,
        )
        server = registry._servers[home]
        server.active_threads.discard(thread_id)

        assert await registry.unsubscribe_thread(thread_id) == "unsubscribed"

    assert registry._thread_owners[thread_id] == home
    assert thread_id not in registry._starting_threads
    assert home not in registry._starting
    assert server.shutdown_count == 0
    assert registry._servers[home] is server


@pytest.mark.asyncio
async def test_registry_routes_rate_limit_reads_by_canonical_home(
    tmp_path, reset_registry_fake_servers,
):
    registry = CodexAppServerRegistry("codex", request_timeout=7)
    home = tmp_path / "quota" / ".." / "quota"

    with patch(
        "backend.services.codex_app_server.CodexAppServer",
        _RegistryFakeServer,
    ):
        result = await registry.read_rate_limits(home)
        second = await registry.read_rate_limits(home)

    assert result["rateLimits"]["primary"]["usedPercent"] == 42
    assert second == result
    assert len(_RegistryFakeServer.instances) == 1
    assert _RegistryFakeServer.instances[0].codex_home == normalize_codex_home(home)
    assert normalize_codex_home(home) not in registry._starting


@pytest.mark.asyncio
async def test_registry_rate_limit_read_blocks_home_maintenance_and_cleans_up(
    tmp_path,
):
    entered = asyncio.Event()
    release = asyncio.Event()

    class BlockingRateLimitServer(_RegistryFakeServer):
        async def read_rate_limits(self):
            entered.set()
            await release.wait()
            return await super().read_rate_limits()

    registry = CodexAppServerRegistry("codex")
    home = normalize_codex_home(tmp_path / "quota-maintenance")
    server = BlockingRateLimitServer("codex", codex_home=home)
    registry._servers[home] = server

    read = asyncio.create_task(registry.read_rate_limits(home))
    await entered.wait()
    with pytest.raises(CodexAppServerBusyError, match="active or starting"):
        await registry.begin_home_maintenance(home)
    release.set()

    assert (await read)["rateLimits"]["primary"]["usedPercent"] == 42
    assert home not in registry._starting
    assert home not in registry._draining


@pytest.mark.asyncio
async def test_cancelled_registry_rate_limit_read_releases_home_reservation(
    tmp_path,
):
    entered = asyncio.Event()

    class CancelledRateLimitServer(_RegistryFakeServer):
        async def read_rate_limits(self):
            entered.set()
            await asyncio.Event().wait()

    registry = CodexAppServerRegistry("codex")
    home = normalize_codex_home(tmp_path / "cancelled-quota")
    server = CancelledRateLimitServer("codex", codex_home=home)
    registry._servers[home] = server

    read = asyncio.create_task(registry.read_rate_limits(home))
    await entered.wait()
    read.cancel()
    with pytest.raises(asyncio.CancelledError):
        await read

    assert home not in registry._starting
    assert await registry.begin_home_maintenance(home) is True
    await registry.end_home_maintenance(home)
    assert home not in registry._draining


@pytest.mark.asyncio
async def test_registry_rejects_cross_home_resume_without_rebind(
    tmp_path, reset_registry_fake_servers,
):
    registry = CodexAppServerRegistry("codex")

    with patch(
        "backend.services.codex_app_server.CodexAppServer",
        _RegistryFakeServer,
    ):
        await registry.start_turn(
            codex_home=tmp_path / "a",
            resume_session_id="thread-owned",
            task_id=1,
        )
        with pytest.raises(CodexThreadHomeMismatchError, match="migrate and rebind"):
            await registry.start_turn(
                codex_home=tmp_path / "b",
                resume_session_id="thread-owned",
                task_id=1,
            )

    assert len(_RegistryFakeServer.instances) == 1


@pytest.mark.asyncio
async def test_registry_rebind_moves_resume_ownership_after_migration(
    tmp_path, reset_registry_fake_servers,
):
    registry = CodexAppServerRegistry("codex")
    home_a = tmp_path / "a"
    home_b = tmp_path / "b"

    with patch(
        "backend.services.codex_app_server.CodexAppServer",
        _RegistryFakeServer,
    ):
        await registry.start_turn(
            codex_home=home_a,
            resume_session_id="thread-migrated",
            task_id=1,
        )
        _RegistryFakeServer.instances[0].active_threads.clear()
        await registry.rebind_thread(
            "thread-migrated",
            source_codex_home=home_a,
            target_codex_home=home_b,
        )
        await registry.start_turn(
            codex_home=home_b,
            resume_session_id="thread-migrated",
            task_id=1,
        )
        with pytest.raises(CodexThreadHomeMismatchError):
            await registry.start_turn(
                codex_home=home_a,
                resume_session_id="thread-migrated",
                task_id=1,
            )

    assert len(_RegistryFakeServer.instances) == 2


@pytest.mark.asyncio
async def test_registry_recovery_clear_restores_db_authoritative_cold_route(
    tmp_path, reset_registry_fake_servers,
):
    registry = CodexAppServerRegistry("codex")
    old_home = normalize_codex_home(tmp_path / "old")
    new_home = normalize_codex_home(tmp_path / "new")
    thread_id = "thread-binding-failed"
    new_server = _RegistryFakeServer("codex", codex_home=new_home)
    new_server.known_threads.add(thread_id)
    registry._servers[new_home] = new_server
    registry._thread_owners[thread_id] = new_home

    assert await registry.clear_thread_owner_for_recovery(
        thread_id,
        expected_codex_home=new_home,
    )
    assert thread_id not in registry._thread_owners

    with patch(
        "backend.services.codex_app_server.CodexAppServer",
        _RegistryFakeServer,
    ):
        await registry.start_turn(
            codex_home=old_home,
            resume_session_id=thread_id,
            task_id=1,
        )

    assert registry._thread_owners[thread_id] == old_home


@pytest.mark.asyncio
async def test_registry_rebind_will_not_restart_target_during_start_rpc(tmp_path):
    """A cached target must not be shutdown under an admitted start/resume RPC."""

    entered = asyncio.Event()
    release = asyncio.Event()

    class BlockingTargetServer(_RegistryFakeServer):
        async def start_turn(self, **kwargs):
            entered.set()
            await release.wait()
            return await super().start_turn(**kwargs)

    registry = CodexAppServerRegistry("codex")
    source_home = normalize_codex_home(tmp_path / "source")
    target_home = normalize_codex_home(tmp_path / "target")
    source_server = _RegistryFakeServer("codex", codex_home=source_home)
    target_server = BlockingTargetServer("codex", codex_home=target_home)
    migrated_thread = "thread-migrated"
    target_server.known_threads.add(migrated_thread)
    registry._servers[source_home] = source_server
    registry._servers[target_home] = target_server
    registry._thread_owners[migrated_thread] = source_home

    start_task = asyncio.create_task(registry.start_turn(
        codex_home=target_home,
        resume_session_id=None,
        task_id=99,
    ))
    await entered.wait()
    try:
        with pytest.raises(CodexAppServerBusyError, match="request in flight"):
            await registry.rebind_thread(
                migrated_thread,
                source_codex_home=source_home,
                target_codex_home=target_home,
            )
        assert target_server.shutdown_count == 0
        assert registry._thread_owners[migrated_thread] == source_home
        assert target_home not in registry._draining
    finally:
        release.set()
        await start_task


@pytest.mark.asyncio
async def test_registry_rebind_rejects_source_resume_rpc_in_flight(tmp_path):
    entered = asyncio.Event()
    release = asyncio.Event()

    class BlockingSourceServer(_RegistryFakeServer):
        async def start_turn(self, **kwargs):
            entered.set()
            await release.wait()
            return await super().start_turn(**kwargs)

    registry = CodexAppServerRegistry("codex")
    source_home = normalize_codex_home(tmp_path / "source")
    target_home = normalize_codex_home(tmp_path / "target")
    thread_id = "thread-source-starting"
    source_server = BlockingSourceServer("codex", codex_home=source_home)
    registry._servers[source_home] = source_server
    registry._thread_owners[thread_id] = source_home

    start_task = asyncio.create_task(registry.start_turn(
        codex_home=source_home,
        resume_session_id=thread_id,
        task_id=1,
    ))
    await entered.wait()
    try:
        with pytest.raises(CodexAppServerBusyError, match="source account"):
            await registry.rebind_thread(
                thread_id,
                source_codex_home=source_home,
                target_codex_home=target_home,
            )
        assert registry._thread_owners[thread_id] == source_home
    finally:
        release.set()
        await start_task


@pytest.mark.asyncio
async def test_registry_rebind_reserves_thread_across_target_shutdown(tmp_path):
    entered_shutdown = asyncio.Event()
    release_shutdown = asyncio.Event()

    class BlockingShutdownServer(_RegistryFakeServer):
        async def shutdown(self):
            entered_shutdown.set()
            await release_shutdown.wait()
            await super().shutdown()

    registry = CodexAppServerRegistry("codex")
    source_home = normalize_codex_home(tmp_path / "source")
    target_home = normalize_codex_home(tmp_path / "target")
    thread_id = "thread-rebind-reserved"
    source_server = _RegistryFakeServer("codex", codex_home=source_home)
    target_server = BlockingShutdownServer("codex", codex_home=target_home)
    target_server.known_threads.add(thread_id)
    registry._servers[source_home] = source_server
    registry._servers[target_home] = target_server
    registry._thread_owners[thread_id] = source_home

    rebind = asyncio.create_task(registry.rebind_thread(
        thread_id,
        source_codex_home=source_home,
        target_codex_home=target_home,
    ))
    await entered_shutdown.wait()
    try:
        with pytest.raises(CodexAppServerBusyError, match="being rebound"):
            await registry.start_turn(
                codex_home=source_home,
                resume_session_id=thread_id,
                task_id=1,
            )
        with pytest.raises(CodexAppServerBusyError, match="rebind in flight"):
            await registry.begin_home_maintenance(source_home)
        assert registry._thread_owners[thread_id] == source_home
    finally:
        release_shutdown.set()
        await rebind

    assert registry._thread_owners[thread_id] == target_home
    assert thread_id not in registry._rebindings


@pytest.mark.asyncio
async def test_registry_rebind_shutdown_failure_keeps_target_draining(tmp_path):
    class FailingShutdownServer(_RegistryFakeServer):
        async def shutdown(self):
            raise RuntimeError("target process group survived")

    registry = CodexAppServerRegistry("codex")
    source_home = normalize_codex_home(tmp_path / "source")
    target_home = normalize_codex_home(tmp_path / "target")
    thread_id = "thread-rebind-failed"
    source_server = _RegistryFakeServer("codex", codex_home=source_home)
    target_server = FailingShutdownServer("codex", codex_home=target_home)
    target_server.known_threads.add(thread_id)
    registry._servers[source_home] = source_server
    registry._servers[target_home] = target_server
    registry._thread_owners[thread_id] = source_home

    with pytest.raises(RuntimeError, match="process group survived"):
        await registry.rebind_thread(
            thread_id,
            source_codex_home=source_home,
            target_codex_home=target_home,
        )

    assert registry._servers[target_home] is target_server
    assert target_home in registry._draining
    assert registry._thread_owners[thread_id] == source_home
    assert thread_id not in registry._rebindings
    with pytest.raises(CodexAppServerBusyError, match="draining"):
        await registry.start_turn(codex_home=target_home, task_id=2)


@pytest.mark.asyncio
async def test_registry_maintenance_rejects_active_and_blocks_new_turns(
    tmp_path, reset_registry_fake_servers,
):
    registry = CodexAppServerRegistry("codex")
    home = tmp_path / "account"

    with patch(
        "backend.services.codex_app_server.CodexAppServer",
        _RegistryFakeServer,
    ):
        await registry.start_turn(
            codex_home=home, resume_session_id="thread-active", task_id=1,
        )
        with pytest.raises(CodexAppServerBusyError, match="active or starting"):
            await registry.begin_home_maintenance(home, require_idle=True)

        _RegistryFakeServer.instances[0].active_threads.clear()
        assert await registry.begin_home_maintenance(home) is True
        with pytest.raises(CodexAppServerBusyError, match="draining"):
            await registry.start_turn(
                codex_home=home, resume_session_id=None, task_id=2,
            )
        await registry.end_home_maintenance(home)
        _, thread_id = await registry.start_turn(
            codex_home=home, resume_session_id=None, task_id=2,
        )

    assert thread_id == "thread-2"
    assert len(_RegistryFakeServer.instances) == 2


@pytest.mark.asyncio
async def test_registry_maintenance_cancellation_detaches_closed_server_before_reopen(
    tmp_path,
):
    """Cancellation after shutdown must not permanently poison the home."""

    shutdown_entered = asyncio.Event()
    release_shutdown = asyncio.Event()

    class BlockingShutdownServer(_RegistryFakeServer):
        async def shutdown(self):
            shutdown_entered.set()
            await release_shutdown.wait()
            await super().shutdown()

    registry = CodexAppServerRegistry("codex")
    home = normalize_codex_home(tmp_path / "cancelled-maintenance")
    server = BlockingShutdownServer("codex", codex_home=home)
    registry._servers[home] = server
    registry._thread_owners["thread-cancelled-maintenance"] = home

    maintenance = asyncio.create_task(registry.begin_home_maintenance(home))
    await shutdown_entered.wait()

    # Hold the registry lock so cancellation lands in the post-shutdown
    # bookkeeping await—the window that previously leaked ``_draining``.
    await registry._lock.acquire()
    release_shutdown.set()
    await asyncio.sleep(0)
    maintenance.cancel()
    await asyncio.sleep(0)
    assert home in registry._draining

    registry._lock.release()
    with pytest.raises(asyncio.CancelledError):
        await maintenance

    assert home not in registry._draining
    assert home not in registry._servers
    assert "thread-cancelled-maintenance" not in registry._thread_owners


@pytest.mark.asyncio
async def test_registry_maintenance_cancel_during_shutdown_stays_fail_closed(
    tmp_path,
):
    shutdown_entered = asyncio.Event()

    class IndeterminateShutdownServer(_RegistryFakeServer):
        async def shutdown(self):
            shutdown_entered.set()
            await asyncio.Event().wait()

    registry = CodexAppServerRegistry("codex")
    home = normalize_codex_home(tmp_path / "indeterminate-shutdown")
    server = IndeterminateShutdownServer("codex", codex_home=home)
    registry._servers[home] = server

    maintenance = asyncio.create_task(registry.begin_home_maintenance(home))
    await shutdown_entered.wait()
    maintenance.cancel()
    with pytest.raises(asyncio.CancelledError):
        await maintenance

    assert home in registry._draining
    assert registry._servers[home] is server
    with pytest.raises(CodexAppServerBusyError, match="draining"):
        await registry.start_turn(codex_home=home, task_id=2)


@pytest.mark.asyncio
async def test_registry_maintenance_sees_start_rpc_in_flight(tmp_path):
    entered = asyncio.Event()
    release = asyncio.Event()

    class BlockingServer(_RegistryFakeServer):
        async def start_turn(self, **kwargs):
            entered.set()
            await release.wait()
            return await super().start_turn(**kwargs)

    BlockingServer.instances = []
    registry = CodexAppServerRegistry("codex")
    home = tmp_path / "account"

    with patch("backend.services.codex_app_server.CodexAppServer", BlockingServer):
        start_task = asyncio.create_task(registry.start_turn(
            codex_home=home, resume_session_id=None, task_id=1,
        ))
        await entered.wait()
        with pytest.raises(CodexAppServerBusyError, match="active or starting"):
            await registry.begin_home_maintenance(home, require_idle=True)
        release.set()
        await start_task


@pytest.mark.asyncio
async def test_registry_shutdown_failure_keeps_server_and_route_draining(
    tmp_path,
):
    registry = CodexAppServerRegistry("codex")
    stopped_home = normalize_codex_home(tmp_path / "stopped")
    failed_home = normalize_codex_home(tmp_path / "failed")
    stopped_server = SimpleNamespace(shutdown=AsyncMock())
    failed_server = SimpleNamespace(
        shutdown=AsyncMock(side_effect=RuntimeError("group survived")),
    )
    registry._servers = {
        stopped_home: stopped_server,
        failed_home: failed_server,
    }
    registry._thread_owners = {
        "thread-stopped": stopped_home,
        "thread-failed": failed_home,
    }
    registry._starting = {
        stopped_home: 1,
        failed_home: 1,
    }

    with pytest.raises(
        CodexAppServerError,
        match="left draining",
    ) as exc_info:
        await registry.shutdown()

    assert isinstance(exc_info.value.__cause__, RuntimeError)
    assert stopped_home not in registry._servers
    assert stopped_home not in registry._draining
    assert stopped_home not in registry._starting
    assert "thread-stopped" not in registry._thread_owners
    assert registry._servers[failed_home] is failed_server
    assert failed_home in registry._draining
    assert registry._starting[failed_home] == 1
    assert registry._thread_owners["thread-failed"] == failed_home
    with pytest.raises(CodexAppServerBusyError, match="registry is shutting down"):
        await registry.start_turn(
            codex_home=tmp_path / "new-home",
            task_id=99,
        )

    # A later successful retry can prove the retained generation gone and
    # clears the fail-closed evidence normally.
    failed_server.shutdown = AsyncMock()
    await registry.shutdown()
    assert registry._servers == {}
    assert registry._draining == set()
    assert registry._starting == {}
    assert registry._thread_owners == {}
