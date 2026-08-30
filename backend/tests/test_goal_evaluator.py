"""Tests for GoalEvaluator — parsing and evaluation logic."""
import asyncio
import json
import os
import signal
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import backend.services.goal_evaluator as goal_evaluator_module
from backend.services.codex_app_server import CodexTurnProcess
from backend.services.goal_evaluator import (
    GoalEvaluationError,
    GoalEvaluatorCleanupError,
    GoalEvaluator,
    GoalEvalResult,
    _GOAL_EVALUATOR_PROCESS_CLEANUPS,
    _GOAL_EVALUATOR_RUNTIME_ROUTES,
    _GOAL_EVALUATOR_TASK_IDS,
    _UNREAPED_CODEX_GOAL_EVALUATOR_TURNS,
    _UNREAPED_GOAL_EVALUATOR_PROCESSES,
    codex_goal_evaluator_runtime_homes,
    _register_goal_evaluator_process,
    _terminate_process_shielded,
    goal_evaluator_runtime_users,
    has_unreaped_goal_evaluator_for_task,
    reap_unreaped_goal_evaluators,
)


@pytest.fixture(autouse=True)
def _stub_claude_zero_tool_preflight(monkeypatch, tmp_path):
    """Unit tests inspect admission args without invoking installed CLIs."""

    # Goal evaluation is an Agent workload and therefore requires an explicit
    # service-token boundary.  Never inherit this security precondition from
    # the developer's .env or from mutable state left by an earlier API test.
    monkeypatch.setattr(
        goal_evaluator_module.settings,
        "auth_token",
        "goal-evaluator-test-token",
    )
    probe = MagicMock()
    monkeypatch.setattr(
        goal_evaluator_module,
        "validate_claude_zero_tool_isolation_settings",
        probe,
    )
    monkeypatch.setattr(
        goal_evaluator_module,
        "manager_secret_protected_paths",
        MagicMock(return_value=("/manager-secret",)),
    )
    monkeypatch.setattr(
        goal_evaluator_module,
        "generate_claude_zero_tool_isolation_settings",
        MagicMock(return_value=tmp_path / "goal-zero-tool.json"),
    )
    clean_home = tmp_path / "goal-clean-home"
    clean_home.mkdir()
    monkeypatch.setattr(
        goal_evaluator_module,
        "prepare_claude_auth_projection",
        MagicMock(
            return_value=SimpleNamespace(
                config_dir=clean_home,
                oauth_access_token=None,
            )
        ),
    )

    def apply_projection(env, projection):
        env["CLAUDE_CONFIG_DIR"] = str(projection.config_dir)

    monkeypatch.setattr(
        goal_evaluator_module,
        "apply_claude_auth_projection",
        apply_projection,
    )
    monkeypatch.setattr(
        goal_evaluator_module,
        "remove_claude_auth_projection",
        MagicMock(),
    )

    def project_cloudrouter(env, _store, _home):
        for key in (
            "ANTHROPIC_AUTH_TOKEN",
            "ANTHROPIC_API_KEY",
            "CLAUDE_CODE_OAUTH_TOKEN",
        ):
            env.pop(key, None)
        return True

    monkeypatch.setattr(
        goal_evaluator_module,
        "inject_cloudrouter_claude_direct_auth",
        MagicMock(side_effect=project_cloudrouter),
    )
    return probe


def _pid_is_running(pid: int) -> bool:
    """Treat a reparented zombie as stopped for process-leak assertions."""

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True

    stat_path = f"/proc/{pid}/stat"
    try:
        with open(stat_path, encoding="utf-8") as stat_file:
            stat = stat_file.read()
    except (FileNotFoundError, PermissionError):
        return True
    close_paren = stat.rfind(")")
    state = stat[close_paren + 2:].split()[0] if close_paren >= 0 else ""
    return state != "Z"


async def _wait_for_child_pid(path, task: asyncio.Task, timeout: float = 2.0) -> int:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if path.exists():
            return int(path.read_text(encoding="utf-8"))
        if task.done():
            await task
            raise AssertionError("Evaluator exited before spawning its child")
        await asyncio.sleep(0.01)
    raise AssertionError("Timed out waiting for evaluator child PID")


async def _wait_for_event_or_task(
    event: asyncio.Event,
    task: asyncio.Task,
    *,
    timeout: float = 2.0,
) -> None:
    """Wait for a launch event while surfacing an early evaluator failure."""

    event_waiter = asyncio.create_task(event.wait())
    try:
        done, _ = await asyncio.wait(
            {event_waiter, task},
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if task in done:
            await task
            raise AssertionError("Evaluator exited before the expected event")
        if event_waiter not in done:
            raise AssertionError("Timed out waiting for evaluator event")
        await event_waiter
    finally:
        if not event_waiter.done():
            event_waiter.cancel()
        await asyncio.gather(event_waiter, return_exceptions=True)


async def _wait_until_not_running(pid: int, timeout: float = 2.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if not _pid_is_running(pid):
            return
        await asyncio.sleep(0.02)
    assert not _pid_is_running(pid), f"Process {pid} is still running"


async def _force_cleanup_process_tree(process, child_pid: int | None) -> None:
    """Explicit, validated test cleanup for failed real-process assertions."""

    parent_pid = getattr(process, "pid", None) if process is not None else None
    if os.name == "posix" and isinstance(parent_pid, int) and parent_pid > 1:
        try:
            os.killpg(parent_pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    if isinstance(child_pid, int) and child_pid > 1 and _pid_is_running(child_pid):
        try:
            os.kill(child_pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    if process is not None and process.returncode is None:
        try:
            process.kill()
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(process.wait(), timeout=2)
        except asyncio.TimeoutError:
            pass


def _process_tree_command(pid_file) -> list[str]:
    script = """
import pathlib
import subprocess
import sys
import time

child = subprocess.Popen(
    [sys.executable, "-c", "import time; time.sleep(30)"],
    stdout=sys.stdout,
    stderr=sys.stderr,
)
pathlib.Path(sys.argv[1]).write_text(str(child.pid), encoding="utf-8")
print("child-started", flush=True)
time.sleep(30)
"""
    return [sys.executable, "-c", script, str(pid_file)]


def _successful_process_tree_command(pid_file) -> list[str]:
    """Exit parent cleanly while a same-group child with closed stdio remains."""

    script = """
import json
import pathlib
import subprocess
import sys

child = subprocess.Popen(
    [sys.executable, "-c", "import time; time.sleep(30)"],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
)
pathlib.Path(sys.argv[1]).write_text(str(child.pid), encoding="utf-8")
print(json.dumps({"achieved": True, "reason": "done"}), flush=True)
"""
    return [sys.executable, "-c", script, str(pid_file)]


class TestGoalEvalResult:
    def test_slots(self):
        r = GoalEvalResult(achieved=True, reason="done")
        assert r.achieved is True
        assert r.reason == "done"

    def test_false_result(self):
        r = GoalEvalResult(achieved=False, reason="not yet")
        assert r.achieved is False
        assert r.reason == "not yet"


@pytest.mark.asyncio
async def test_retained_goal_evaluator_is_retried_by_shutdown_reaper():
    process = MagicMock(pid=54_901, returncode=None)
    process_token = id(process)
    _UNREAPED_GOAL_EVALUATOR_PROCESSES[process_token] = process
    try:
        with patch(
            "backend.services.goal_evaluator._terminate_process",
            new_callable=AsyncMock,
            side_effect=RuntimeError("still alive"),
        ):
            with pytest.raises(GoalEvaluatorCleanupError, match="54901"):
                await reap_unreaped_goal_evaluators()
        assert _UNREAPED_GOAL_EVALUATOR_PROCESSES[process_token] is process

        with patch(
            "backend.services.goal_evaluator._terminate_process",
            new_callable=AsyncMock,
        ):
            await reap_unreaped_goal_evaluators()
        assert process_token not in _UNREAPED_GOAL_EVALUATOR_PROCESSES
    finally:
        _UNREAPED_GOAL_EVALUATOR_PROCESSES.pop(process_token, None)
        _GOAL_EVALUATOR_TASK_IDS.pop(process_token, None)


def test_retained_goal_evaluator_is_queryable_by_task():
    process = MagicMock(pid=54_902, returncode=None)
    process_token = id(process)
    _UNREAPED_GOAL_EVALUATOR_PROCESSES[process_token] = process
    _GOAL_EVALUATOR_TASK_IDS[process_token] = 812
    try:
        assert has_unreaped_goal_evaluator_for_task(812) is True
        assert has_unreaped_goal_evaluator_for_task(813) is False
    finally:
        _UNREAPED_GOAL_EVALUATOR_PROCESSES.pop(process_token, None)
        _GOAL_EVALUATOR_TASK_IDS.pop(process_token, None)


def test_runtime_registry_does_not_overwrite_reused_numeric_pid(tmp_path):
    provider_home = tmp_path / "claude-home"
    old_process = MagicMock(pid=54_903, returncode=None)
    new_process = MagicMock(pid=54_903, returncode=None)
    old_token = id(old_process)
    new_token = id(new_process)
    try:
        _register_goal_evaluator_process(
            old_process,
            provider="claude",
            provider_home=str(provider_home),
            task_id=821,
        )
        _register_goal_evaluator_process(
            new_process,
            provider="claude",
            provider_home=str(provider_home),
            task_id=822,
        )

        assert old_token != new_token
        assert _UNREAPED_GOAL_EVALUATOR_PROCESSES[old_token] is old_process
        assert _UNREAPED_GOAL_EVALUATOR_PROCESSES[new_token] is new_process
        assert goal_evaluator_runtime_users(
            "claude", str(provider_home),
        ) == [
            "goal-evaluator:claude:task=821:pid=54903",
            "goal-evaluator:claude:task=822:pid=54903",
        ]
    finally:
        for process_token in (old_token, new_token):
            _UNREAPED_GOAL_EVALUATOR_PROCESSES.pop(process_token, None)
            _GOAL_EVALUATOR_TASK_IDS.pop(process_token, None)
            _GOAL_EVALUATOR_RUNTIME_ROUTES.pop(process_token, None)


@pytest.mark.asyncio
async def test_active_claude_runtime_user_matches_exact_canonical_home(
    tmp_path,
):
    evaluator = GoalEvaluator()
    real_home = tmp_path / "claude-home"
    real_home.mkdir()
    alias_home = tmp_path / "claude-home-alias"
    alias_home.symlink_to(real_home, target_is_directory=True)
    other_home = tmp_path / "other-claude-home"
    communicate_started = asyncio.Event()
    release_communicate = asyncio.Event()

    mock_proc = MagicMock(pid=55_101, returncode=None)

    async def communicate():
        communicate_started.set()
        await release_communicate.wait()
        mock_proc.returncode = 0
        return (
            json.dumps({"achieved": True, "reason": "ok"}).encode(),
            b"",
        )

    mock_proc.communicate = AsyncMock(side_effect=communicate)
    evaluation = None
    try:
        with (
            patch("asyncio.create_subprocess_exec", return_value=mock_proc),
            patch(
                "backend.services.goal_evaluator.os.killpg",
                side_effect=ProcessLookupError,
            ),
        ):
            evaluation = asyncio.create_task(evaluator.evaluate(
                condition="cond",
                conversation_summary="conv",
                provider="claude",
                config_dir=str(alias_home),
                task_id=901,
            ))
            await _wait_for_event_or_task(communicate_started, evaluation)

            expected = ["goal-evaluator:claude:task=901:pid=55101"]
            assert goal_evaluator_runtime_users(
                "claude", str(real_home),
            ) == expected
            assert goal_evaluator_runtime_users(
                "CLAUDE", str(alias_home),
            ) == expected
            assert goal_evaluator_runtime_users(
                "claude", str(other_home),
            ) == []
            assert goal_evaluator_runtime_users(
                "codex", str(real_home),
            ) == []

            release_communicate.set()
            result = await evaluation

        assert result.achieved is True
        assert goal_evaluator_runtime_users("claude", str(real_home)) == []
    finally:
        release_communicate.set()
        if evaluation is not None and not evaluation.done():
            evaluation.cancel()
            await asyncio.gather(evaluation, return_exceptions=True)
        process_token = id(mock_proc)
        _UNREAPED_GOAL_EVALUATOR_PROCESSES.pop(process_token, None)
        _GOAL_EVALUATOR_TASK_IDS.pop(process_token, None)
        _GOAL_EVALUATOR_RUNTIME_ROUTES.pop(process_token, None)


@pytest.mark.asyncio
async def test_cleanup_failed_runtime_user_remains_until_reaper_proves_terminal(
    tmp_path,
):
    evaluator = GoalEvaluator()
    provider_home = tmp_path / "claude-retained"
    mock_proc = MagicMock(pid=55_102, returncode=0)
    mock_proc.communicate = AsyncMock(return_value=(
        json.dumps({"achieved": True, "reason": "ok"}).encode(),
        b"",
    ))

    try:
        with (
            patch("asyncio.create_subprocess_exec", return_value=mock_proc),
            patch(
                "backend.services.goal_evaluator._terminate_process",
                new_callable=AsyncMock,
                side_effect=RuntimeError("terminal proof unavailable"),
            ),
        ):
            with pytest.raises(GoalEvaluatorCleanupError):
                await evaluator.evaluate(
                    condition="cond",
                    conversation_summary="conv",
                    provider="claude",
                    config_dir=str(provider_home),
                    task_id=902,
                )

        assert goal_evaluator_runtime_users(
            "claude", str(provider_home),
        ) == ["goal-evaluator:claude:task=902:pid=55102"]

        with patch(
            "backend.services.goal_evaluator._terminate_process",
            new_callable=AsyncMock,
        ):
            await reap_unreaped_goal_evaluators()

        assert goal_evaluator_runtime_users(
            "claude", str(provider_home),
        ) == []
    finally:
        process_token = id(mock_proc)
        _UNREAPED_GOAL_EVALUATOR_PROCESSES.pop(process_token, None)
        _GOAL_EVALUATOR_TASK_IDS.pop(process_token, None)
        _GOAL_EVALUATOR_RUNTIME_ROUTES.pop(process_token, None)


@pytest.mark.asyncio
async def test_codex_standard_refuses_direct_exec_without_audited_registry(
    tmp_path,
):
    evaluator = GoalEvaluator()
    provider_home = str((tmp_path / "codex-retained").resolve())
    with patch(
        "asyncio.create_subprocess_exec",
        new_callable=AsyncMock,
    ) as spawn:
        with pytest.raises(
            GoalEvaluationError,
            match="exact app-server account route",
        ):
            await evaluator.evaluate(
                condition="cond",
                conversation_summary="conv",
                provider="codex",
                config_dir=provider_home,
                task_id=903,
            )
    spawn.assert_not_awaited()
    assert codex_goal_evaluator_runtime_homes() == set()


class TestParseResponse:
    def setup_method(self):
        self.evaluator = GoalEvaluator()

    def test_direct_json(self):
        raw = json.dumps({"achieved": True, "reason": "all tests pass"})
        result = self.evaluator._parse_response(raw)
        assert result.achieved is True
        assert result.reason == "all tests pass"

    def test_json_in_result_envelope(self):
        envelope = {"result": json.dumps({"achieved": False, "reason": "2 tests fail"})}
        result = self.evaluator._parse_response(json.dumps(envelope))
        assert result.achieved is False
        assert result.reason == "2 tests fail"

    def test_json_in_content_envelope(self):
        envelope = {"content": json.dumps({"achieved": True, "reason": "done"})}
        result = self.evaluator._parse_response(json.dumps(envelope))
        assert result.achieved is True

    def test_json_in_markdown_code_block(self):
        raw = '```json\n{"achieved": true, "reason": "clean"}\n```'
        result = self.evaluator._parse_response(raw)
        assert result.achieved is True
        assert result.reason == "clean"

    def test_malformed_response(self):
        result = self.evaluator._parse_response("I think the goal is met")
        assert result.achieved is False
        assert "Could not parse" in result.reason

    def test_empty_response(self):
        result = self.evaluator._parse_response("")
        assert result.achieved is False

    def test_achieved_false_string(self):
        raw = json.dumps({"achieved": "false", "reason": "lint errors remain"})
        result = self.evaluator._parse_response(raw)
        assert result.achieved is False
        assert "Could not parse" in result.reason

    def test_missing_reason_field(self):
        raw = json.dumps({"achieved": True})
        result = self.evaluator._parse_response(raw)
        assert result.achieved is True
        assert result.reason == ""

    def test_nested_result_with_direct_json(self):
        """Result envelope where result is already a dict (not a string)."""
        envelope = {"result": '{"achieved": true, "reason": "ok"}'}
        result = self.evaluator._parse_response(json.dumps(envelope))
        assert result.achieved is True


class TestBuildEvalPrompt:
    def setup_method(self):
        self.evaluator = GoalEvaluator()

    def test_contains_condition(self):
        prompt = self.evaluator._build_eval_prompt("all tests pass", "some conversation")
        assert "all tests pass" in prompt

    def test_contains_conversation(self):
        prompt = self.evaluator._build_eval_prompt("condition", "Claude ran pytest and got 0 failures")
        assert "Claude ran pytest and got 0 failures" in prompt

    def test_json_template_present(self):
        prompt = self.evaluator._build_eval_prompt("cond", "conv")
        assert '"achieved": true' in prompt
        assert '"achieved": false' in prompt


class TestEvaluateIntegration:
    @pytest.mark.asyncio
    async def test_security_admission_requires_auth_before_provider_effect(
        self,
        monkeypatch,
    ):
        evaluator = GoalEvaluator()
        claude_pool = MagicMock()
        claude_pool.ensure_oauth_access_token = AsyncMock()
        monkeypatch.setattr(goal_evaluator_module.settings, "auth_token", "")
        with patch(
            "asyncio.create_subprocess_exec",
            new_callable=AsyncMock,
        ) as spawn:
            with pytest.raises(GoalEvaluationError, match="AUTH_TOKEN"):
                await evaluator.evaluate(
                    condition="cond",
                    conversation_summary="conv",
                    task_id=71,
                    claude_pool=claude_pool,
                )
        spawn.assert_not_awaited()
        claude_pool.ensure_oauth_access_token.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_security_admission_precedes_codex_app_server_effect(
        self,
        monkeypatch,
        tmp_path,
    ):
        evaluator = GoalEvaluator()
        registry = MagicMock()
        registry.start_turn = AsyncMock()
        monkeypatch.setattr(goal_evaluator_module.settings, "auth_token", "")

        with pytest.raises(GoalEvaluationError, match="AUTH_TOKEN"):
            await evaluator.evaluate(
                condition="cond",
                conversation_summary="conv",
                provider="codex",
                codex_home=str(tmp_path / "codex-home"),
                task_id=72,
                codex_app_server_registry=registry,
            )

        registry.start_turn.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_claude_evaluator_is_zero_tool_and_scrubs_unknown_secret(
        self,
        monkeypatch,
    ):
        evaluator = GoalEvaluator()
        monkeypatch.setenv(
            "CCM_TEST_UNKNOWN_MANAGER_SECRET",
            "must-not-reach-provider",
        )
        process = MagicMock(pid=55_000, returncode=0)
        process.communicate = AsyncMock(return_value=(
            json.dumps({"achieved": True, "reason": "safe"}).encode(),
            b"",
        ))
        with (
            patch("asyncio.create_subprocess_exec", return_value=process) as spawn,
            patch(
                "backend.services.goal_evaluator.os.killpg",
                side_effect=ProcessLookupError,
            ),
        ):
            result = await evaluator.evaluate(
                condition="cond",
                conversation_summary="conv",
                task_id=71,
            )

        assert result.achieved is True
        argv = list(spawn.call_args.args)
        assert "--dangerously-skip-permissions" not in argv
        assert argv[argv.index("--tools") + 1] == ""
        assert argv[argv.index("--allowedTools") + 1] == ""
        assert argv[argv.index("--setting-sources") + 1] == ""
        assert "--strict-mcp-config" in argv
        assert "--no-session-persistence" in argv
        child_env = spawn.call_args.kwargs["env"]
        assert "CCM_TEST_UNKNOWN_MANAGER_SECRET" not in child_env
        goal_evaluator_module.generate_claude_zero_tool_isolation_settings.assert_called_once_with(
            "goal-evaluator",
            71,
            ("/manager-secret",),
        )

    """Test the evaluate method with mocked subprocess."""

    @pytest.mark.asyncio
    async def test_evaluate_achieved(self):
        evaluator = GoalEvaluator()
        mock_result = json.dumps({"achieved": True, "reason": "all tests pass"})

        mock_proc = MagicMock()
        mock_proc.pid = 55_001
        mock_proc.communicate = AsyncMock(return_value=(mock_result.encode(), b""))
        mock_proc.returncode = 0

        with (
            patch("asyncio.create_subprocess_exec", return_value=mock_proc),
            patch(
                "backend.services.goal_evaluator.os.killpg",
                side_effect=ProcessLookupError,
            ),
        ):
            result = await evaluator.evaluate(
                condition="all tests pass",
                conversation_summary="pytest: 10 passed, 0 failed",
            )

        assert result.achieved is True
        assert result.reason == "all tests pass"

    @pytest.mark.asyncio
    async def test_posix_evaluator_starts_in_dedicated_session(self):
        evaluator = GoalEvaluator()
        mock_result = json.dumps({"achieved": True, "reason": "ok"})
        mock_proc = MagicMock()
        mock_proc.pid = 55_002
        mock_proc.communicate = AsyncMock(return_value=(mock_result.encode(), b""))
        mock_proc.returncode = 0

        with (
            patch(
                "asyncio.create_subprocess_exec",
                return_value=mock_proc,
            ) as mock_exec,
            patch(
                "backend.services.goal_evaluator.os.killpg",
                side_effect=ProcessLookupError,
            ),
        ):
            await evaluator.evaluate(condition="cond", conversation_summary="conv")

        if os.name == "posix":
            assert mock_exec.call_args.kwargs["start_new_session"] is True

    @pytest.mark.asyncio
    async def test_non_posix_evaluator_uses_portable_spawn_and_kill(self):
        evaluator = GoalEvaluator()
        mock_proc = MagicMock()
        mock_proc.pid = None
        mock_proc.communicate = AsyncMock(side_effect=asyncio.TimeoutError)
        mock_proc.returncode = None
        mock_proc.kill = MagicMock()
        mock_proc.wait = AsyncMock(return_value=-9)

        with (
            patch("backend.services.goal_evaluator.os.name", "nt"),
            patch("asyncio.create_subprocess_exec", return_value=mock_proc) as mock_exec,
        ):
            with pytest.raises(GoalEvaluationError):
                await evaluator.evaluate(condition="cond", conversation_summary="conv")

        assert "start_new_session" not in mock_exec.call_args.kwargs
        mock_proc.kill.assert_called_once()
        mock_proc.wait.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_evaluate_not_achieved(self):
        evaluator = GoalEvaluator()
        mock_result = json.dumps({"achieved": False, "reason": "3 tests still failing"})

        mock_proc = MagicMock()
        mock_proc.pid = 55_003
        mock_proc.communicate = AsyncMock(return_value=(mock_result.encode(), b""))
        mock_proc.returncode = 0

        with (
            patch("asyncio.create_subprocess_exec", return_value=mock_proc),
            patch(
                "backend.services.goal_evaluator.os.killpg",
                side_effect=ProcessLookupError,
            ),
        ):
            result = await evaluator.evaluate(
                condition="all tests pass",
                conversation_summary="pytest: 7 passed, 3 failed",
            )

        assert result.achieved is False
        assert "3 tests still failing" in result.reason

    @pytest.mark.asyncio
    async def test_evaluate_timeout(self):
        evaluator = GoalEvaluator()

        mock_proc = MagicMock()
        mock_proc.pid = 55_004
        mock_proc.communicate = AsyncMock(side_effect=asyncio.TimeoutError)
        mock_proc.returncode = None
        mock_proc.kill = MagicMock()
        mock_proc.wait = AsyncMock(return_value=-9)

        with (
            patch("asyncio.create_subprocess_exec", return_value=mock_proc),
            patch(
                "backend.services.goal_evaluator.os.killpg",
                side_effect=ProcessLookupError,
            ),
        ):
            with pytest.raises(GoalEvaluationError) as exc_info:
                await evaluator.evaluate(
                    condition="cond",
                    conversation_summary="conv",
                )

        assert "timed out" in str(exc_info.value).lower()
        mock_proc.kill.assert_called_once()
        mock_proc.wait.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_timeout_kills_exact_posix_process_group(self):
        if os.name != "posix":
            pytest.skip("POSIX process groups only")

        evaluator = GoalEvaluator()
        mock_proc = MagicMock()
        mock_proc.pid = 43210
        mock_proc.communicate = AsyncMock(side_effect=asyncio.TimeoutError)
        mock_proc.returncode = None
        mock_proc.kill = MagicMock()
        mock_proc.wait = AsyncMock(return_value=-9)

        group_alive = True

        def kill_group(pid, sig):
            nonlocal group_alive
            assert pid == 43210
            if sig == 0:
                if group_alive:
                    return None
                raise ProcessLookupError
            assert sig == signal.SIGKILL
            group_alive = False

        with (
            patch("asyncio.create_subprocess_exec", return_value=mock_proc),
            patch(
                "backend.services.goal_evaluator.os.killpg",
                side_effect=kill_group,
            ) as killpg,
        ):
            with pytest.raises(GoalEvaluationError):
                await evaluator.evaluate(condition="cond", conversation_summary="conv")

        assert (43210, signal.SIGKILL) in [
            call.args for call in killpg.call_args_list
        ]
        mock_proc.kill.assert_not_called()
        mock_proc.wait.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_repeated_cancellation_waits_for_shielded_reap(self):
        evaluator = GoalEvaluator()
        communicate_started = asyncio.Event()
        communicate_released = asyncio.Event()
        wait_started = asyncio.Event()
        wait_released = asyncio.Event()

        async def communicate():
            communicate_started.set()
            await communicate_released.wait()
            return b"", b""

        async def wait():
            wait_started.set()
            await wait_released.wait()
            return -9

        mock_proc = MagicMock()
        mock_proc.pid = 55_009
        mock_proc.returncode = None
        mock_proc.communicate = AsyncMock(side_effect=communicate)
        mock_proc.wait = AsyncMock(side_effect=wait)
        mock_proc.kill = MagicMock()

        group_alive = True

        def kill_group(pid, sig):
            nonlocal group_alive
            assert pid == 55_009
            if sig == 0:
                if group_alive:
                    return None
                raise ProcessLookupError
            assert sig == signal.SIGKILL
            group_alive = False
            communicate_released.set()

        with (
            patch("asyncio.create_subprocess_exec", return_value=mock_proc),
            patch(
                "backend.services.goal_evaluator.os.killpg",
                side_effect=kill_group,
            ) as killpg,
        ):
            evaluation = asyncio.create_task(
                evaluator.evaluate(condition="cond", conversation_summary="conv")
            )
            await communicate_started.wait()
            evaluation.cancel()
            await wait_started.wait()
            evaluation.cancel()
            assert not evaluation.done()
            wait_released.set()
            with pytest.raises(asyncio.CancelledError):
                await evaluation

        assert (55_009, signal.SIGKILL) in [
            call.args for call in killpg.call_args_list
        ]
        mock_proc.kill.assert_not_called()
        mock_proc.wait.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_cancellation_during_successful_cleanup_never_resignals_reused_pgid(
        self, tmp_path,
    ):
        evaluator = GoalEvaluator()
        provider_home = tmp_path / "claude-cleanup-cancel"
        cleanup_started = asyncio.Event()
        release_cleanup = asyncio.Event()
        pgid_reused = False
        cleanup_calls = 0

        mock_proc = MagicMock(pid=55_105, returncode=0)
        mock_proc.communicate = AsyncMock(return_value=(
            json.dumps({"achieved": True, "reason": "ok"}).encode(),
            b"",
        ))

        async def terminate_once(
            process,
            communicate_task,
            *,
            managed_process_group,
        ):
            nonlocal cleanup_calls, pgid_reused
            cleanup_calls += 1
            assert process is mock_proc
            assert cleanup_calls == 1, (
                "a second cleanup would signal a reused numeric PID/PGID"
            )
            assert pgid_reused is False
            cleanup_started.set()
            await release_cleanup.wait()
            # Model the exact evaluator group becoming terminal and the
            # numeric PGID immediately being assigned elsewhere.
            pgid_reused = True

        evaluation = None
        process_token = id(mock_proc)
        try:
            with (
                patch("asyncio.create_subprocess_exec", return_value=mock_proc),
                patch(
                    "backend.services.goal_evaluator._terminate_process",
                    side_effect=terminate_once,
                ),
            ):
                evaluation = asyncio.create_task(evaluator.evaluate(
                    condition="cond",
                    conversation_summary="conv",
                    provider="claude",
                    config_dir=str(provider_home),
                    task_id=905,
                ))
                await cleanup_started.wait()
                assert goal_evaluator_runtime_users(
                    "claude", str(provider_home),
                ) == ["goal-evaluator:claude:task=905:pid=55105"]

                evaluation.cancel()
                await asyncio.sleep(0)
                release_cleanup.set()
                with pytest.raises(asyncio.CancelledError):
                    await evaluation

            assert pgid_reused is True
            assert cleanup_calls == 1
            assert process_token not in _UNREAPED_GOAL_EVALUATOR_PROCESSES
            assert goal_evaluator_runtime_users(
                "claude", str(provider_home),
            ) == []
        finally:
            release_cleanup.set()
            if evaluation is not None and not evaluation.done():
                evaluation.cancel()
                await asyncio.gather(evaluation, return_exceptions=True)
            _UNREAPED_GOAL_EVALUATOR_PROCESSES.pop(process_token, None)
            _GOAL_EVALUATOR_TASK_IDS.pop(process_token, None)
            _GOAL_EVALUATOR_RUNTIME_ROUTES.pop(process_token, None)

    @pytest.mark.asyncio
    async def test_request_cleanup_and_shutdown_reaper_share_exact_termination(
        self, tmp_path,
    ):
        evaluator = GoalEvaluator()
        provider_home = tmp_path / "claude-concurrent-cleanup"
        cleanup_started = asyncio.Event()
        release_cleanup = asyncio.Event()
        cleanup_calls = 0

        mock_proc = MagicMock(pid=55_106, returncode=0)
        mock_proc.communicate = AsyncMock(return_value=(
            json.dumps({"achieved": True, "reason": "ok"}).encode(),
            b"",
        ))

        async def terminate_once(
            process,
            communicate_task,
            *,
            managed_process_group,
        ):
            nonlocal cleanup_calls
            cleanup_calls += 1
            assert process is mock_proc
            assert cleanup_calls == 1, (
                "request cleanup and shutdown reaper must not signal "
                "the same exact process group twice"
            )
            cleanup_started.set()
            await release_cleanup.wait()

        evaluation = None
        reaper = None
        process_token = id(mock_proc)
        try:
            with (
                patch("asyncio.create_subprocess_exec", return_value=mock_proc),
                patch(
                    "backend.services.goal_evaluator._terminate_process",
                    side_effect=terminate_once,
                ),
            ):
                evaluation = asyncio.create_task(evaluator.evaluate(
                    condition="cond",
                    conversation_summary="conv",
                    provider="claude",
                    config_dir=str(provider_home),
                    task_id=906,
                ))
                await cleanup_started.wait()
                assert process_token in _GOAL_EVALUATOR_PROCESS_CLEANUPS

                reaper = asyncio.create_task(
                    reap_unreaped_goal_evaluators()
                )
                await asyncio.sleep(0)
                assert cleanup_calls == 1

                release_cleanup.set()
                result, _ = await asyncio.gather(evaluation, reaper)
                # A shutdown caller may already hold a stale registry
                # snapshot and arrive after the shared cleanup entry was
                # removed.  Weak exact-object terminal proof must still make
                # that late call a no-op.
                await _terminate_process_shielded(
                    mock_proc,
                    None,
                    managed_process_group=(os.name == "posix"),
                )

            assert result.achieved is True
            assert cleanup_calls == 1
            assert process_token not in _GOAL_EVALUATOR_PROCESS_CLEANUPS
            assert process_token not in _UNREAPED_GOAL_EVALUATOR_PROCESSES
            assert goal_evaluator_runtime_users(
                "claude", str(provider_home),
            ) == []
        finally:
            release_cleanup.set()
            pending = [
                task
                for task in (evaluation, reaper)
                if task is not None and not task.done()
            ]
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            _GOAL_EVALUATOR_PROCESS_CLEANUPS.pop(process_token, None)
            _UNREAPED_GOAL_EVALUATOR_PROCESSES.pop(process_token, None)
            _GOAL_EVALUATOR_TASK_IDS.pop(process_token, None)
            _GOAL_EVALUATOR_RUNTIME_ROUTES.pop(process_token, None)

    @pytest.mark.asyncio
    async def test_evaluate_subprocess_error(self):
        evaluator = GoalEvaluator()

        with patch("asyncio.create_subprocess_exec", side_effect=OSError("binary not found")):
            with pytest.raises(GoalEvaluationError) as exc_info:
                await evaluator.evaluate(
                    condition="cond",
                    conversation_summary="conv",
                )

        assert "binary not found" in exc_info.value.stderr
        assert "binary not found" in exc_info.value.combined_output

    @pytest.mark.asyncio
    async def test_nonzero_exit_exposes_stderr_for_pool_classification(self):
        evaluator = GoalEvaluator()
        process = CodexTurnProcess(
            55_005,
            AsyncMock(),
            thread_id="failed-standard-evaluator",
        )
        process.feed({"type": "turn.failed"})
        process.finish(
            1,
            stderr="You have hit your usage limit. Try again later.",
        )
        registry = MagicMock()
        registry.start_turn = AsyncMock(return_value=(
            process,
            "failed-standard-evaluator",
        ))
        registry.abort_unclaimed_turn = AsyncMock()
        registry.delete_thread = AsyncMock()

        with patch(
            "asyncio.create_subprocess_exec",
            new_callable=AsyncMock,
        ) as spawn:
            with pytest.raises(GoalEvaluationError) as exc_info:
                await evaluator.evaluate(
                    condition="cond",
                    conversation_summary="conv",
                    provider="codex",
                    codex_home="/tmp/codex-a",
                    codex_app_server_registry=registry,
                )

        spawn.assert_not_awaited()
        error = exc_info.value
        assert error.provider == "codex"
        assert error.returncode == 1
        assert "usage limit" in error.stderr
        assert "usage limit" in error.combined_output
        assert "turn.failed" in error.combined_output

    @pytest.mark.asyncio
    async def test_evaluate_uses_custom_model(self):
        evaluator = GoalEvaluator()
        mock_result = json.dumps({"achieved": True, "reason": "ok"})

        mock_proc = MagicMock()
        mock_proc.pid = 55_006
        mock_proc.communicate = AsyncMock(return_value=(mock_result.encode(), b""))
        mock_proc.returncode = 0

        with (
            patch(
                "asyncio.create_subprocess_exec",
                return_value=mock_proc,
            ) as mock_exec,
            patch(
                "backend.services.goal_evaluator.os.killpg",
                side_effect=ProcessLookupError,
            ),
        ):
            await evaluator.evaluate(
                condition="cond",
                conversation_summary="conv",
                model="claude-sonnet-4-6",
            )

        call_args = mock_exec.call_args[0]
        assert "--model" in call_args
        model_idx = list(call_args).index("--model")
        assert call_args[model_idx + 1] == "claude-sonnet-4-6"

    @pytest.mark.asyncio
    async def test_evaluate_passes_max_turns_1(self):
        evaluator = GoalEvaluator()
        mock_result = json.dumps({"achieved": True, "reason": "ok"})

        mock_proc = MagicMock()
        mock_proc.pid = 55_007
        mock_proc.communicate = AsyncMock(return_value=(mock_result.encode(), b""))
        mock_proc.returncode = 0

        with (
            patch(
                "asyncio.create_subprocess_exec",
                return_value=mock_proc,
            ) as mock_exec,
            patch(
                "backend.services.goal_evaluator.os.killpg",
                side_effect=ProcessLookupError,
            ),
        ):
            await evaluator.evaluate(
                condition="cond",
                conversation_summary="conv",
            )

        call_args = mock_exec.call_args[0]
        assert "--max-turns" in call_args
        idx = list(call_args).index("--max-turns")
        assert call_args[idx + 1] == "1"

    @pytest.mark.asyncio
    async def test_codex_evaluation_sets_explicit_codex_home(self, tmp_path):
        evaluator = GoalEvaluator()
        agent_text = json.dumps({"achieved": True, "reason": "ok"})
        process = CodexTurnProcess(
            55_008,
            AsyncMock(),
            thread_id="standard-evaluator-thread",
        )
        process.feed({
            "type": "item.completed",
            "item": {"type": "agent_message", "text": agent_text},
        })
        process.finish(0)
        registry = MagicMock()
        registry.start_turn = AsyncMock(return_value=(
            process,
            "standard-evaluator-thread",
        ))
        registry.abort_unclaimed_turn = AsyncMock()
        registry.delete_thread = AsyncMock()
        codex_home = tmp_path / "codex-account-2"

        with patch(
            "asyncio.create_subprocess_exec",
            new_callable=AsyncMock,
        ) as spawn:
            result = await evaluator.evaluate(
                condition="cond",
                conversation_summary="conv",
                provider="codex",
                codex_home=str(codex_home),
                codex_app_server_registry=registry,
            )

        assert result.achieved is True
        spawn.assert_not_awaited()
        kwargs = registry.start_turn.await_args.kwargs
        assert kwargs["codex_home"] == str(codex_home)
        assert kwargs["codex_service_tier"] == "default"
        assert kwargs["tools_disabled"] is True
        assert kwargs["disable_project_config"] is True
        assert kwargs["disable_user_mcp"] is True
        assert kwargs["disable_autonomous_features"] is True
        assert kwargs["sandbox_mode"] == "read-only"

    @pytest.mark.asyncio
    async def test_active_codex_standard_runtime_user_matches_exact_home(
        self, tmp_path,
    ):
        evaluator = GoalEvaluator()
        codex_home = tmp_path / "codex-standard"
        agent_text = json.dumps({"achieved": True, "reason": "ok"})
        process = CodexTurnProcess(
            55_103,
            AsyncMock(),
            thread_id="active-standard-evaluator",
        )
        registry = MagicMock()
        registry.start_turn = AsyncMock(return_value=(
            process,
            "active-standard-evaluator",
        ))
        registry.abort_unclaimed_turn = AsyncMock()
        registry.delete_thread = AsyncMock()
        evaluation = None
        try:
            evaluation = asyncio.create_task(evaluator.evaluate(
                condition="cond",
                conversation_summary="conv",
                provider="codex",
                codex_home=str(codex_home),
                task_id=903,
                codex_app_server_registry=registry,
            ))
            expected = [
                "goal-evaluator:codex:"
                "task=903:thread=active-standard-evaluator"
            ]
            for _ in range(100):
                if goal_evaluator_runtime_users(
                    "codex", str(codex_home),
                ) == expected:
                    break
                await asyncio.sleep(0.01)
            assert goal_evaluator_runtime_users(
                "codex", str(codex_home),
            ) == expected

            process.feed({
                "type": "item.completed",
                "item": {"type": "agent_message", "text": agent_text},
            })
            process.finish(0)
            result = await evaluation

            assert result.achieved is True
            assert goal_evaluator_runtime_users(
                "codex", str(codex_home),
            ) == []
        finally:
            if process.returncode is None:
                process.finish(1, stderr="test cleanup")
            if evaluation is not None and not evaluation.done():
                evaluation.cancel()
                await asyncio.gather(evaluation, return_exceptions=True)
            _UNREAPED_CODEX_GOAL_EVALUATOR_TURNS.pop(id(process), None)

    @pytest.mark.asyncio
    async def test_codex_fast_requires_exact_app_server_route_before_execution(
        self, tmp_path,
    ):
        evaluator = GoalEvaluator()
        with patch(
            "asyncio.create_subprocess_exec",
            new_callable=AsyncMock,
        ) as spawn:
            with pytest.raises(
                GoalEvaluationError,
                match="exact app-server account route",
            ):
                await evaluator.evaluate(
                    condition="cond",
                    conversation_summary="conv",
                    provider="codex",
                    model="gpt-5.4",
                    codex_home=str(tmp_path / "codex-fast"),
                    codex_service_tier="priority",
                )
        spawn.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_codex_fast_uses_priority_app_server_and_deletes_thread(
        self, tmp_path,
    ):
        evaluator = GoalEvaluator()
        process = CodexTurnProcess(
            55_012,
            AsyncMock(),
            thread_id="fast-evaluator-thread",
        )
        process.feed({
            "type": "item.completed",
            "item": {
                "type": "agent_message",
                "text": json.dumps({
                    "achieved": True,
                    "reason": "priority evaluation completed",
                }),
            },
        })
        process.finish(0)
        registry = MagicMock()
        registry.start_turn = AsyncMock(return_value=(
            process,
            "fast-evaluator-thread",
        ))
        registry.abort_unclaimed_turn = AsyncMock()
        registry.delete_thread = AsyncMock()
        codex_home = str(tmp_path / "codex-fast")

        with patch(
            "asyncio.create_subprocess_exec",
            new_callable=AsyncMock,
        ) as spawn:
            result = await evaluator.evaluate(
                condition="cond",
                conversation_summary="conv",
                provider="codex",
                model="gpt-5.4",
                codex_home=codex_home,
                task_id=812,
                codex_service_tier="priority",
                codex_app_server_registry=registry,
            )

        assert result.achieved is True
        assert result.reason == "priority evaluation completed"
        spawn.assert_not_awaited()
        start_kwargs = registry.start_turn.await_args.kwargs
        assert start_kwargs["codex_home"] == codex_home
        assert start_kwargs["model"] == "gpt-5.4"
        assert start_kwargs["codex_service_tier"] == "priority"
        assert start_kwargs["resume_session_id"] is None
        registry.abort_unclaimed_turn.assert_not_awaited()
        registry.delete_thread.assert_awaited_once_with(
            codex_home,
            "fast-evaluator-thread",
        )
        assert not _UNREAPED_CODEX_GOAL_EVALUATOR_TURNS

    @pytest.mark.asyncio
    async def test_active_codex_fast_runtime_user_matches_canonical_home(
        self, tmp_path,
    ):
        evaluator = GoalEvaluator()
        real_home = tmp_path / "codex-fast-home"
        real_home.mkdir()
        alias_home = tmp_path / "codex-fast-alias"
        alias_home.symlink_to(real_home, target_is_directory=True)
        process = CodexTurnProcess(
            55_104,
            AsyncMock(),
            thread_id="active-fast-evaluator",
        )
        registry = MagicMock()
        registry.start_turn = AsyncMock(return_value=(
            process,
            "active-fast-evaluator",
        ))
        registry.abort_unclaimed_turn = AsyncMock()
        registry.delete_thread = AsyncMock()
        evaluation = asyncio.create_task(evaluator.evaluate(
            condition="cond",
            conversation_summary="conv",
            provider="codex",
            model="gpt-5.4",
            codex_home=str(alias_home),
            task_id=904,
            codex_service_tier="priority",
            codex_app_server_registry=registry,
        ))

        try:
            expected = [
                "goal-evaluator:codex:"
                "task=904:thread=active-fast-evaluator"
            ]
            for _ in range(100):
                if goal_evaluator_runtime_users(
                    "codex", str(real_home),
                ) == expected:
                    break
                await asyncio.sleep(0)
            assert goal_evaluator_runtime_users(
                "codex", str(real_home),
            ) == expected
            assert goal_evaluator_runtime_users(
                "codex", str(alias_home),
            ) == expected
            assert goal_evaluator_runtime_users(
                "claude", str(real_home),
            ) == []

            process.feed({
                "type": "item.completed",
                "item": {
                    "type": "agent_message",
                    "text": json.dumps({
                        "achieved": True,
                        "reason": "priority evaluation completed",
                    }),
                },
            })
            process.finish(0)
            result = await evaluation

            assert result.achieved is True
            assert goal_evaluator_runtime_users(
                "codex", str(real_home),
            ) == []
            registry.delete_thread.assert_awaited_once_with(
                str(alias_home),
                "active-fast-evaluator",
            )
        finally:
            if process.returncode is None:
                process.finish(130, "test cleanup")
            if not evaluation.done():
                await asyncio.gather(evaluation, return_exceptions=True)
            _UNREAPED_CODEX_GOAL_EVALUATOR_TURNS.pop(
                id(process), None,
            )

    @pytest.mark.asyncio
    async def test_codex_fast_timeout_aborts_and_deletes_exact_thread(
        self, tmp_path,
    ):
        evaluator = GoalEvaluator()
        process = CodexTurnProcess(
            55_013,
            AsyncMock(),
            thread_id="timed-out-fast-evaluator",
        )
        registry = MagicMock()
        registry.start_turn = AsyncMock(return_value=(
            process,
            "timed-out-fast-evaluator",
        ))

        async def abort_turn(home, candidate, *, reason):
            assert home == str(tmp_path / "codex-fast")
            assert candidate is process
            assert "timed out" in reason
            candidate.finish(130, reason)
            return False

        registry.abort_unclaimed_turn = AsyncMock(side_effect=abort_turn)
        registry.delete_thread = AsyncMock()

        with (
            patch(
                "asyncio.create_subprocess_exec",
                new_callable=AsyncMock,
            ) as spawn,
            patch(
                "backend.services.goal_evaluator.settings.goal_evaluation_timeout",
                0.01,
            ),
        ):
            with pytest.raises(GoalEvaluationError, match="timed out"):
                await evaluator.evaluate(
                    condition="cond",
                    conversation_summary="conv",
                    provider="codex",
                    model="gpt-5.4",
                    codex_home=str(tmp_path / "codex-fast"),
                    task_id=813,
                    codex_service_tier="priority",
                    codex_app_server_registry=registry,
                )

        spawn.assert_not_awaited()
        registry.abort_unclaimed_turn.assert_awaited_once()
        registry.delete_thread.assert_awaited_once_with(
            str(tmp_path / "codex-fast"),
            "timed-out-fast-evaluator",
        )
        assert process.returncode == 130
        assert not _UNREAPED_CODEX_GOAL_EVALUATOR_TURNS

    @pytest.mark.asyncio
    async def test_codex_fast_cancel_cleanup_failure_is_retained_for_reaper(
        self, tmp_path,
    ):
        evaluator = GoalEvaluator()
        process = CodexTurnProcess(
            55_014,
            AsyncMock(),
            thread_id="retained-fast-evaluator",
        )
        registry = MagicMock()
        registry.start_turn = AsyncMock(return_value=(
            process,
            "retained-fast-evaluator",
        ))
        abort_calls = 0

        async def abort_turn(_home, candidate, *, reason):
            nonlocal abort_calls
            abort_calls += 1
            if abort_calls == 1:
                raise RuntimeError("interrupt and shutdown unconfirmed")
            candidate.finish(130, reason)
            return False

        registry.abort_unclaimed_turn = AsyncMock(side_effect=abort_turn)
        registry.delete_thread = AsyncMock()
        codex_home = str(tmp_path / "codex-fast")
        evaluation = asyncio.create_task(evaluator.evaluate(
            condition="cond",
            conversation_summary="conv",
            provider="codex",
            model="gpt-5.4",
            codex_home=codex_home,
            task_id=814,
            codex_service_tier="priority",
            codex_app_server_registry=registry,
        ))
        await asyncio.sleep(0)
        evaluation.cancel()

        with pytest.raises(
            GoalEvaluatorCleanupError,
            match="could not be proven terminal",
        ):
            await evaluation

        assert process.returncode is None
        assert has_unreaped_goal_evaluator_for_task(814) is True
        assert goal_evaluator_runtime_users(
            "codex", codex_home,
        ) == [
            "goal-evaluator:codex:"
            "task=814:thread=retained-fast-evaluator"
        ]
        await reap_unreaped_goal_evaluators()
        assert process.returncode == 130
        assert has_unreaped_goal_evaluator_for_task(814) is False
        assert goal_evaluator_runtime_users("codex", codex_home) == []
        registry.delete_thread.assert_awaited_once_with(
            codex_home,
            "retained-fast-evaluator",
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("provider", ["claude", "codex"])
    async def test_api_evaluation_uses_provider_home_and_scrubs_auth(
        self, provider, tmp_path, monkeypatch,
    ):
        evaluator = GoalEvaluator()
        provider_home = tmp_path / f"{provider}-api-account"
        store = MagicMock()
        store._read_api_key = MagicMock(
            side_effect=AssertionError("evaluator must not read API key"),
        )
        if provider == "codex":
            store.account_for_codex_home.return_value = object()
            agent_text = json.dumps({"achieved": True, "reason": "ok"})
            stdout = b""
            auth_keys = (
                "OPENAI_API_KEY",
                "CODEX_API_KEY",
                "CLOUDROUTER_API_KEY",
                "APEX_CODEX_GATEWAY_KEY",
                "APEX_CODEX_API_KEY",
                "APEXROUTER_API_KEY",
                "APEXROUTER_CODEX_API_KEY",
            )
            process = CodexTurnProcess(
                55_011,
                AsyncMock(),
                thread_id="api-standard-evaluator",
            )
            process.feed({
                "type": "item.completed",
                "item": {"type": "agent_message", "text": agent_text},
            })
            process.finish(0)
            registry = MagicMock()
            registry.start_turn = AsyncMock(return_value=(
                process,
                "api-standard-evaluator",
            ))
            registry.abort_unclaimed_turn = AsyncMock()
            registry.delete_thread = AsyncMock()
            evaluate_home = {
                "codex_home": str(provider_home),
                "codex_app_server_registry": registry,
            }
        else:
            store.account_for_claude_config_dir.return_value = object()
            stdout = json.dumps({"achieved": True, "reason": "ok"}).encode()
            auth_keys = (
                "ANTHROPIC_AUTH_TOKEN",
                "ANTHROPIC_API_KEY",
                "CLAUDE_CODE_OAUTH_TOKEN",
            )
            evaluate_home = {"config_dir": str(provider_home)}

        for key in auth_keys:
            monkeypatch.setenv(key, f"secret-{key}")
        mock_proc = MagicMock(pid=55_010, returncode=0)
        mock_proc.communicate = AsyncMock(return_value=(stdout, b""))

        with (
            patch(
                "asyncio.create_subprocess_exec",
                return_value=mock_proc if provider == "claude" else None,
            ) as mock_exec,
            patch(
                "backend.services.goal_evaluator.os.killpg",
                side_effect=ProcessLookupError,
            ),
        ):
            result = await evaluator.evaluate(
                condition="cond",
                conversation_summary="conv",
                provider=provider,
                cloudrouter_store=store,
                **evaluate_home,
            )

        assert result.achieved is True
        if provider == "codex":
            mock_exec.assert_not_awaited()
            kwargs = registry.start_turn.await_args.kwargs
            assert kwargs["codex_home"] == str(provider_home)
            assert kwargs["tools_disabled"] is True
            assert kwargs["disable_project_config"] is True
            assert kwargs["disable_user_mcp"] is True
        else:
            child_env = mock_exec.call_args.kwargs["env"]
            command = list(mock_exec.call_args.args)
            clean_home = (
                goal_evaluator_module.prepare_claude_auth_projection
                .return_value.config_dir
            )
            assert child_env["CLAUDE_CONFIG_DIR"] == str(clean_home)
            assert not set(auth_keys) & child_env.keys()
            assert mock_exec.call_args.kwargs["cwd"] == os.path.abspath(os.sep)
            assert "-c" not in command
            goal_evaluator_module.inject_cloudrouter_claude_direct_auth.assert_called()
        store._read_api_key.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.skipif(os.name != "posix", reason="POSIX process groups only")
    async def test_normal_completion_kills_closed_stdio_descendant(self, tmp_path):
        evaluator = GoalEvaluator()
        pid_file = tmp_path / "normal-child.pid"
        captured: dict[str, object] = {}
        child_pid: int | None = None
        real_create_subprocess_exec = asyncio.create_subprocess_exec

        async def capture_process(*args, **kwargs):
            process = await real_create_subprocess_exec(*args, **kwargs)
            captured["process"] = process
            return process

        try:
            with (
                patch.object(
                    evaluator,
                    "_build_eval_command",
                    return_value=_successful_process_tree_command(pid_file),
                ),
                patch(
                    "asyncio.create_subprocess_exec",
                    side_effect=capture_process,
                ),
            ):
                result = await evaluator.evaluate(
                    condition="cond",
                    conversation_summary="conv",
                )
            child_pid = int(pid_file.read_text(encoding="utf-8"))
            assert result.achieved is True
            await _wait_until_not_running(child_pid)
            assert captured["process"].returncode == 0
        finally:
            await _force_cleanup_process_tree(captured.get("process"), child_pid)

    @pytest.mark.asyncio
    @pytest.mark.skipif(os.name != "posix", reason="POSIX process groups only")
    async def test_spawn_cancellation_settles_and_reaps_process_tree(
        self, tmp_path
    ):
        evaluator = GoalEvaluator()
        pid_file = tmp_path / "spawn-cancel-child.pid"
        captured: dict[str, object] = {}
        spawned = asyncio.Event()
        release_spawn = asyncio.Event()
        child_pid: int | None = None
        evaluation: asyncio.Task | None = None
        real_create_subprocess_exec = asyncio.create_subprocess_exec

        async def delayed_spawn(*args, **kwargs):
            process = await real_create_subprocess_exec(*args, **kwargs)
            captured["process"] = process
            spawned.set()
            await release_spawn.wait()
            return process

        try:
            with (
                patch.object(
                    evaluator,
                    "_build_eval_command",
                    return_value=_process_tree_command(pid_file),
                ),
                patch(
                    "asyncio.create_subprocess_exec",
                    side_effect=delayed_spawn,
                ),
            ):
                evaluation = asyncio.create_task(
                    evaluator.evaluate(
                        condition="cond",
                        conversation_summary="conv",
                    )
                )
                await spawned.wait()
                child_pid = await _wait_for_child_pid(pid_file, evaluation)
                evaluation.cancel()
                await asyncio.sleep(0)
                assert not evaluation.done()
                release_spawn.set()
                with pytest.raises(asyncio.CancelledError):
                    await evaluation

            process = captured["process"]
            assert process.returncode is not None
            await _wait_until_not_running(child_pid)
        finally:
            release_spawn.set()
            if evaluation is not None and not evaluation.done():
                evaluation.cancel()
            await _force_cleanup_process_tree(captured.get("process"), child_pid)

    @pytest.mark.asyncio
    @pytest.mark.skipif(os.name != "posix", reason="POSIX process groups only")
    async def test_real_timeout_kills_child_that_inherits_output_pipes(self, tmp_path):
        evaluator = GoalEvaluator()
        pid_file = tmp_path / "timeout-child.pid"
        captured: dict[str, object] = {}
        child_pid: int | None = None
        real_create_subprocess_exec = asyncio.create_subprocess_exec

        async def capture_process(*args, **kwargs):
            process = await real_create_subprocess_exec(*args, **kwargs)
            captured["process"] = process
            return process

        try:
            with (
                patch.object(
                    evaluator,
                    "_build_eval_command",
                    return_value=_process_tree_command(pid_file),
                ),
                patch(
                    "asyncio.create_subprocess_exec",
                    side_effect=capture_process,
                ),
                patch(
                    "backend.services.goal_evaluator.settings.goal_evaluation_timeout",
                    0.3,
                ),
            ):
                with pytest.raises(GoalEvaluationError, match="timed out"):
                    await evaluator.evaluate(
                        condition="cond",
                        conversation_summary="conv",
                    )

            process = captured["process"]
            child_pid = int(pid_file.read_text(encoding="utf-8"))
            assert process.returncode is not None
            await _wait_until_not_running(child_pid)
        finally:
            await _force_cleanup_process_tree(captured.get("process"), child_pid)

    @pytest.mark.asyncio
    @pytest.mark.skipif(os.name != "posix", reason="POSIX process groups only")
    async def test_real_cancellation_reaps_parent_and_inherited_pipe_child(
        self, tmp_path
    ):
        evaluator = GoalEvaluator()
        pid_file = tmp_path / "cancel-child.pid"
        captured: dict[str, object] = {}
        child_pid: int | None = None
        real_create_subprocess_exec = asyncio.create_subprocess_exec

        async def capture_process(*args, **kwargs):
            process = await real_create_subprocess_exec(*args, **kwargs)
            captured["process"] = process
            return process

        evaluation: asyncio.Task | None = None
        try:
            with (
                patch.object(
                    evaluator,
                    "_build_eval_command",
                    return_value=_process_tree_command(pid_file),
                ),
                patch(
                    "asyncio.create_subprocess_exec",
                    side_effect=capture_process,
                ),
            ):
                evaluation = asyncio.create_task(
                    evaluator.evaluate(
                        condition="cond",
                        conversation_summary="conv",
                    )
                )
                child_pid = await _wait_for_child_pid(pid_file, evaluation)
                evaluation.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await evaluation

            process = captured["process"]
            assert process.returncode is not None
            await _wait_until_not_running(child_pid)
        finally:
            if evaluation is not None and not evaluation.done():
                evaluation.cancel()
            await _force_cleanup_process_tree(captured.get("process"), child_pid)
