"""Tests for Claude account pool — rotation, detection, session migration."""
import asyncio
import json
import os
import threading
import time
import pytest
from pathlib import Path
from unittest.mock import patch, AsyncMock, MagicMock

from backend.services.claude_pool import (
    ClaudePool,
    PoolAccount,
    is_rate_limited,
    is_auth_failure,
    is_pool_rotatable,
    is_transient_overload,
    transient_retry_delay,
    migrate_session,
    migrate_session_async,
    collect_process_output_for_detection,
    api_quota_at_or_above,
    quota_cooldown_seconds,
    quota_usage_at_or_above,
    rate_limit_event_is_actionable,
)


# ---------------------------------------------------------------------------
# Rate-limit / auth-failure detection
# ---------------------------------------------------------------------------

class TestRateLimitDetection:
    def test_hit_your_limit(self):
        assert is_rate_limited("You've hit your limit, you can continue using Claude at 3pm")

    def test_usage_limit_reached(self):
        assert is_rate_limited("Claude AI usage limit reached")

    def test_resets_timestamp(self):
        assert is_rate_limited("resets 3pm (America/New_York)")

    def test_organization_disabled(self):
        assert is_rate_limited("Your organization has been disabled")

    def test_account_disabled(self):
        assert is_rate_limited("Your account has been disabled")

    def test_chinese_rate_limit(self):
        assert is_rate_limited("当前限速，请稍后再试")

    def test_case_insensitive(self):
        assert is_rate_limited("YOU'VE HIT YOUR LIMIT")

    def test_no_false_positive_on_generic_429(self):
        assert not is_rate_limited("The API returned a 429 rate limit error")

    def test_no_false_positive_on_tool_output(self):
        assert not is_rate_limited("Semantic Scholar returned: Too many requests")

    def test_empty_string(self):
        assert not is_rate_limited("")

    def test_none(self):
        assert not is_rate_limited(None)

    def test_normal_output(self):
        assert not is_rate_limited("Task completed successfully. All tests pass.")


class TestAuthFailureDetection:
    def test_not_logged_in(self):
        assert is_auth_failure("Error: Not logged in. Please run `claude login`")

    def test_please_run_login(self):
        assert is_auth_failure("please run /login to authenticate")

    def test_not_authenticated(self):
        assert is_auth_failure("not authenticated, please sign in")

    def test_please_log_in(self):
        assert is_auth_failure("Please log in first")

    def test_failed_to_authenticate(self):
        assert is_auth_failure("Failed to authenticate with the server")

    def test_no_false_positive(self):
        assert not is_auth_failure("User login endpoint created")

    def test_empty(self):
        assert not is_auth_failure("")


class TestPoolRotatable:
    def test_rate_limit(self):
        assert is_pool_rotatable("You've hit your limit")

    def test_auth_failure(self):
        assert is_pool_rotatable("Not logged in")

    def test_normal_error(self):
        assert not is_pool_rotatable("SyntaxError: unexpected token")


class TestTransientOverloadDetection:
    """Server-side transient 429/overload — wait-and-retry the same account."""

    def test_the_actual_cli_message(self):
        # The exact wording the Claude Code CLI prints for an HTTP 429
        # rate_limit that is NOT the account usage limit.
        msg = ("API Error: Server is temporarily limiting requests "
               "(not your usage limit) · Rate limited")
        assert is_transient_overload(msg)

    def test_not_your_usage_limit_phrase(self):
        assert is_transient_overload("the server is busy (not your usage limit)")

    def test_overloaded_error_type(self):
        assert is_transient_overload('{"type":"error","error":{"type":"overloaded_error"}}')

    def test_api_overloaded(self):
        assert is_transient_overload("API overloaded — wait and retry")

    def test_case_insensitive(self):
        assert is_transient_overload("SERVER IS TEMPORARILY LIMITING REQUESTS")

    def test_pty_jsonl_inactivity_timeout_does_not_replay_same_session(self):
        assert not is_transient_overload("Response timed out after 900.0s")

    # --- precedence: account usage-limit / auth-failure must rotate, not wait ---
    def test_usage_limit_takes_precedence(self):
        # A genuine usage-limit banner must NOT be treated as transient.
        assert not is_transient_overload("You've hit your limit · resets 5pm (UTC)")

    def test_usage_limit_reached_not_transient(self):
        assert not is_transient_overload("Claude AI usage limit reached")

    def test_auth_failure_not_transient(self):
        assert not is_transient_overload("Not logged in. Please run /login")

    # --- no false positives ---
    def test_no_false_positive_generic_429(self):
        assert not is_transient_overload("The API returned a 429 rate limit error")

    def test_no_false_positive_normal_output(self):
        assert not is_transient_overload("Task completed successfully. All tests pass.")

    def test_no_false_positive_tool_discussion(self):
        assert not is_transient_overload("I implemented the request throttling middleware")

    def test_empty(self):
        assert not is_transient_overload("")

    def test_none(self):
        assert not is_transient_overload(None)


class TestTransientRetryDelay:
    def test_first_attempt_near_base(self):
        # attempt 1 => base ± 20% jitter
        d = transient_retry_delay(1, base=10.0, cap=120.0)
        assert 8.0 <= d <= 12.0

    def test_exponential_growth(self):
        # attempt 3 => base * 4 = 40 ± 20% => [32, 48]
        d = transient_retry_delay(3, base=10.0, cap=120.0)
        assert 32.0 <= d <= 48.0

    def test_capped(self):
        # huge attempt clamps to cap (then ± jitter on the cap)
        d = transient_retry_delay(20, base=10.0, cap=120.0)
        assert d <= 120.0 * 1.2

    def test_minimum_one_second(self):
        d = transient_retry_delay(1, base=0.0, cap=120.0)
        assert d >= 1.0

    def test_attempt_floor(self):
        # attempt < 1 is treated as 1, never negative/zero delay
        assert transient_retry_delay(0, base=10.0, cap=120.0) >= 1.0


# ---------------------------------------------------------------------------
# Pool configuration and selection
# ---------------------------------------------------------------------------

@pytest.fixture
def pool_config(tmp_path):
    config = {
        "accounts": [
            {"id": "acc-1", "config_dir": str(tmp_path / "claude-1"), "email": "a@test.com", "enabled": True},
            {"id": "acc-2", "config_dir": str(tmp_path / "claude-2"), "email": "b@test.com", "enabled": True},
            {"id": "acc-3", "config_dir": str(tmp_path / "claude-3"), "email": "c@test.com", "enabled": False},
        ],
    }
    config_path = tmp_path / "accounts.json"
    config_path.write_text(json.dumps(config))
    return config_path


@pytest.fixture
def pool(pool_config):
    return ClaudePool(config_path=pool_config, cooldown_seconds=60)


class TestClaudePool:
    def test_load_accounts(self, pool):
        assert pool.enabled is True
        accounts = pool.list_accounts()
        assert len(accounts) == 3

    def test_single_account_pool_still_enabled(self, tmp_path):
        config = {"accounts": [{"id": "only", "config_dir": "/tmp/x", "enabled": True}]}
        path = tmp_path / "accounts.json"
        path.write_text(json.dumps(config))
        p = ClaudePool(config_path=path)
        assert p.enabled is True

    def test_missing_config_file(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        p = ClaudePool(config_path=tmp_path / "nonexistent.json")
        assert p.enabled is True
        assert p.list_accounts() == []

    def test_select_returns_config_dir(self, pool, tmp_path):
        result = pool.select()
        assert result in [str(tmp_path / "claude-1"), str(tmp_path / "claude-2")]

    @pytest.mark.asyncio
    async def test_access_token_projection_refresh_uses_exact_pool_account(
        self,
        pool,
        tmp_path,
    ):
        pool._refresh_oauth = AsyncMock(return_value={"accessToken": "fresh"})

        refreshed = await pool.ensure_oauth_access_token(
            tmp_path / "claude-1",
            minimum_remaining_seconds=300.0,
        )

        assert refreshed is True
        pool._refresh_oauth.assert_awaited_once()
        account, credential_path = pool._refresh_oauth.await_args.args
        assert account.id == "acc-1"
        assert credential_path == tmp_path / "claude-1" / ".credentials.json"
        assert pool._refresh_oauth.await_args.kwargs == {
            "minimum_remaining_seconds": 300.0,
        }

    @pytest.mark.asyncio
    async def test_access_token_projection_rejects_unmanaged_or_disabled_home(
        self,
        pool,
        tmp_path,
    ):
        pool._refresh_oauth = AsyncMock(return_value={"accessToken": "fresh"})

        assert not await pool.ensure_oauth_access_token(tmp_path / "unknown")
        assert not await pool.ensure_oauth_access_token(tmp_path / "claude-3")
        pool._refresh_oauth.assert_not_awaited()

    def test_select_excludes_disabled(self, pool, tmp_path):
        # acc-3 is disabled, should never be selected
        for _ in range(20):
            result = pool.select()
            assert result != str(tmp_path / "claude-3")

    def test_select_excludes_specified(self, pool, tmp_path):
        result = pool.select(exclude={"acc-1"})
        assert result == str(tmp_path / "claude-2")


    def test_select_returns_none_when_all_excluded(self, pool):
        result = pool.select(exclude={"acc-1", "acc-2"})
        assert result is None

    def test_mark_rate_limited(self, pool, tmp_path):
        config_dir = str(tmp_path / "claude-1")
        pool.mark_rate_limited(config_dir, duration=60)
        # acc-1 should be unavailable
        accounts = pool.list_accounts()
        acc1 = next(a for a in accounts if a["id"] == "acc-1")
        assert acc1["available"] is False
        assert acc1["cooldown_remaining"] > 0
        # select should skip acc-1
        result = pool.select()
        assert result == str(tmp_path / "claude-2")

    def test_mark_auth_failure(self, pool, tmp_path):
        config_dir = str(tmp_path / "claude-1")
        pool.mark_auth_failure(config_dir)
        accounts = pool.list_accounts()
        acc1 = next(a for a in accounts if a["id"] == "acc-1")
        assert acc1["available"] is False
        # Should have a very long cooldown
        assert acc1["cooldown_remaining"] > 86000

    def test_auth_failure_is_not_an_infinite_routing_retry(
        self, pool, tmp_path
    ):
        for account in pool._accounts:
            account.enabled = account.id == "acc-1"
        config_dir = str(tmp_path / "claude-1")
        pool.mark_auth_failure(config_dir)

        assert pool.has_compatible_enabled_account(None)
        assert not pool.has_retryable_compatible_account(None)

        pool.clear_cooldown("acc-1")
        assert pool.has_retryable_compatible_account(None)

    def test_clear_cooldown(self, pool, tmp_path):
        config_dir = str(tmp_path / "claude-1")
        pool.mark_rate_limited(config_dir)
        pool.clear_cooldown("acc-1")
        accounts = pool.list_accounts()
        acc1 = next(a for a in accounts if a["id"] == "acc-1")
        assert acc1["available"] is True

    def test_select_none_when_all_rate_limited(self, pool, tmp_path):
        pool.mark_rate_limited(str(tmp_path / "claude-1"))
        pool.mark_rate_limited(str(tmp_path / "claude-2"))
        assert pool.select() is None

    def test_cooldown_expires(self, pool, tmp_path):
        config_dir = str(tmp_path / "claude-1")
        pool.mark_rate_limited(config_dir, duration=1)
        # Immediately after, still in cooldown
        assert pool.select(exclude=set()) is not None  # acc-2 is available
        # Manually expire the cooldown
        pool._cooldowns["acc-1"] = time.time() - 1
        result = pool.select()
        # acc-1 should be available again
        assert result is not None

    def test_account_id_from_config_dir(self, pool, tmp_path):
        assert pool.account_id_from_config_dir(str(tmp_path / "claude-1")) == "acc-1"
        assert pool.account_id_from_config_dir("/nonexistent") is None

    def test_status(self, pool, tmp_path):
        pool.mark_rate_limited(str(tmp_path / "claude-1"))
        status = pool.status()
        assert status["enabled"] is True
        assert status["total"] == 3
        assert status["available"] == 1  # acc-2
        assert status["cooldown"] == 1  # acc-1
        assert status["disabled"] == 1  # acc-3

    def test_reload(self, pool, pool_config):
        pool.reload()
        assert len(pool.list_accounts()) == 3

    def test_select_round_robin(self, pool, tmp_path):
        """Accounts with earlier cooldown expiry are preferred → distributes load."""
        pool.mark_rate_limited(str(tmp_path / "claude-1"), duration=1)
        pool._cooldowns["acc-1"] = time.time() - 10  # expired 10s ago
        pool.mark_rate_limited(str(tmp_path / "claude-2"), duration=1)
        pool._cooldowns["acc-2"] = time.time() - 5   # expired 5s ago
        # acc-1 expired earlier, should be selected first
        result = pool.select()
        assert result == str(tmp_path / "claude-1")


# ---------------------------------------------------------------------------
# Session migration
# ---------------------------------------------------------------------------

class TestSessionMigration:
    def test_successful_hardlink(self, tmp_path):
        old_dir = tmp_path / "old-account"
        new_dir = tmp_path / "new-account"
        session_id = "test-session-123"

        # Create the session file in old_config_dir
        session_dir = old_dir / "projects" / "encoded-cwd"
        session_dir.mkdir(parents=True)
        session_file = session_dir / f"{session_id}.jsonl"
        session_file.write_text('{"event": "init"}\n')

        result = migrate_session(
            old_config_dir=str(old_dir),
            new_config_dir=str(new_dir),
            session_id=session_id,
        )
        assert result is True

        new_file = new_dir / "projects" / "encoded-cwd" / f"{session_id}.jsonl"
        assert new_file.exists()
        # Verify it's a hardlink (same inode)
        assert session_file.stat().st_ino == new_file.stat().st_ino

    def test_hardlinks_nested_sidecar_files(self, tmp_path):
        old_dir = tmp_path / "old-account"
        new_dir = tmp_path / "new-account"
        session_id = "sidecar-session-123"
        project_dir = old_dir / "projects" / "encoded-cwd"
        project_dir.mkdir(parents=True)
        session_file = project_dir / f"{session_id}.jsonl"
        session_file.write_text('{"event": "init"}\n')
        tool_result = (
            project_dir / session_id / "tool-results" / "large-output.txt"
        )
        tool_result.parent.mkdir(parents=True)
        tool_result.write_text("complete tool output")
        subagent = (
            project_dir
            / session_id
            / "subagents"
            / "nested"
            / "agent-1.jsonl"
        )
        subagent.parent.mkdir(parents=True)
        subagent.write_text('{"status":"done"}\n')
        empty_dir = project_dir / session_id / "empty"
        empty_dir.mkdir()

        assert migrate_session(
            old_config_dir=str(old_dir),
            new_config_dir=str(new_dir),
            session_id=session_id,
        )

        target_project = new_dir / "projects" / "encoded-cwd"
        target_tool_result = (
            target_project / session_id / "tool-results" / "large-output.txt"
        )
        target_subagent = (
            target_project
            / session_id
            / "subagents"
            / "nested"
            / "agent-1.jsonl"
        )
        assert target_tool_result.read_text() == "complete tool output"
        assert target_subagent.read_text() == '{"status":"done"}\n'
        assert target_tool_result.stat().st_ino == tool_result.stat().st_ino
        assert target_subagent.stat().st_ino == subagent.stat().st_ino
        assert (target_project / session_id / "empty").is_dir()

    def test_already_hardlinked(self, tmp_path):
        old_dir = tmp_path / "old"
        new_dir = tmp_path / "new"
        session_id = "sid-abc"

        session_dir = old_dir / "projects" / "cwd"
        session_dir.mkdir(parents=True)
        session_file = session_dir / f"{session_id}.jsonl"
        session_file.write_text("data\n")

        # First migration
        migrate_session(old_config_dir=str(old_dir), new_config_dir=str(new_dir), session_id=session_id)
        # Second migration should return True (idempotent)
        result = migrate_session(old_config_dir=str(old_dir), new_config_dir=str(new_dir), session_id=session_id)
        assert result is True

    def test_existing_jsonl_hardlink_still_repairs_missing_sidecar(self, tmp_path):
        old_dir = tmp_path / "old"
        new_dir = tmp_path / "new"
        session_id = "sid-repair"
        source_project = old_dir / "projects" / "cwd"
        target_project = new_dir / "projects" / "cwd"
        source_project.mkdir(parents=True)
        target_project.mkdir(parents=True)
        source_jsonl = source_project / f"{session_id}.jsonl"
        source_jsonl.write_text("data\n")
        target_jsonl = target_project / f"{session_id}.jsonl"
        os.link(source_jsonl, target_jsonl)
        source_tool_result = (
            source_project / session_id / "tool-results" / "result.txt"
        )
        source_tool_result.parent.mkdir(parents=True)
        source_tool_result.write_text("result")

        assert migrate_session(
            old_config_dir=str(old_dir),
            new_config_dir=str(new_dir),
            session_id=session_id,
        )

        target_tool_result = (
            target_project / session_id / "tool-results" / "result.txt"
        )
        assert target_tool_result.stat().st_ino == source_tool_result.stat().st_ino

    def test_existing_jsonl_hardlink_rejects_sidecar_file_conflict(
        self, tmp_path
    ):
        old_dir = tmp_path / "old"
        new_dir = tmp_path / "new"
        session_id = "sid-sidecar-conflict"
        source_project = old_dir / "projects" / "cwd"
        target_project = new_dir / "projects" / "cwd"
        source_project.mkdir(parents=True)
        target_project.mkdir(parents=True)
        source_jsonl = source_project / f"{session_id}.jsonl"
        source_jsonl.write_text("data\n")
        target_jsonl = target_project / f"{session_id}.jsonl"
        os.link(source_jsonl, target_jsonl)
        source_file = source_project / session_id / "tool-results" / "result.txt"
        target_file = target_project / session_id / "tool-results" / "result.txt"
        source_file.parent.mkdir(parents=True)
        target_file.parent.mkdir(parents=True)
        source_file.write_text("source")
        target_file.write_text("different")

        assert migrate_session(
            old_config_dir=str(old_dir),
            new_config_dir=str(new_dir),
            session_id=session_id,
        ) is False
        assert target_jsonl.exists()
        assert target_file.read_text() == "different"

    def test_source_sidecar_symlink_is_rejected_without_partial_target(
        self, tmp_path
    ):
        old_dir = tmp_path / "old"
        new_dir = tmp_path / "new"
        session_id = "sid-symlink"
        source_project = old_dir / "projects" / "cwd"
        source_project.mkdir(parents=True)
        (source_project / f"{session_id}.jsonl").write_text("data\n")
        sidecar = source_project / session_id
        sidecar.mkdir()
        (sidecar / "real.txt").write_text("secret")
        (sidecar / "unsafe-link").symlink_to("real.txt")

        assert migrate_session(
            old_config_dir=str(old_dir),
            new_config_dir=str(new_dir),
            session_id=session_id,
        ) is False
        assert not (new_dir / "projects").exists()

    def test_source_sidecar_special_file_is_rejected(self, tmp_path):
        old_dir = tmp_path / "old"
        new_dir = tmp_path / "new"
        session_id = "sid-special"
        source_project = old_dir / "projects" / "cwd"
        source_project.mkdir(parents=True)
        (source_project / f"{session_id}.jsonl").write_text("data\n")
        sidecar = source_project / session_id
        sidecar.mkdir()
        os.mkfifo(sidecar / "pipe")

        assert migrate_session(
            old_config_dir=str(old_dir),
            new_config_dir=str(new_dir),
            session_id=session_id,
        ) is False
        assert not (new_dir / "projects").exists()

    def test_link_failure_rolls_back_every_entry_created_by_call(
        self, tmp_path, monkeypatch
    ):
        old_dir = tmp_path / "old"
        new_dir = tmp_path / "new"
        session_id = "sid-rollback"
        source_project = old_dir / "projects" / "cwd"
        source_project.mkdir(parents=True)
        (source_project / f"{session_id}.jsonl").write_text("data\n")
        sidecar = source_project / session_id / "tool-results"
        sidecar.mkdir(parents=True)
        (sidecar / "a-ok.txt").write_text("ok")
        (sidecar / "z-fail.txt").write_text("fail")
        real_link = os.link

        def flaky_link(source, target, *, follow_symlinks=True):
            if Path(source).name == "z-fail.txt":
                raise OSError("simulated hardlink failure")
            return real_link(
                source,
                target,
                follow_symlinks=follow_symlinks,
            )

        monkeypatch.setattr(os, "link", flaky_link)

        assert migrate_session(
            old_config_dir=str(old_dir),
            new_config_dir=str(new_dir),
            session_id=session_id,
        ) is False
        assert not (
            new_dir / "projects" / "cwd" / f"{session_id}.jsonl"
        ).exists()
        assert not (new_dir / "projects").exists()

    def test_ambiguous_source_jsonls_are_rejected(self, tmp_path):
        old_dir = tmp_path / "old"
        for encoded_cwd in ("cwd-one", "cwd-two"):
            project = old_dir / "projects" / encoded_cwd
            project.mkdir(parents=True)
            (project / "sid-ambiguous.jsonl").write_text(encoded_cwd)

        assert migrate_session(
            old_config_dir=str(old_dir),
            new_config_dir=str(tmp_path / "new"),
            session_id="sid-ambiguous",
        ) is False

    @pytest.mark.parametrize(
        "unsafe_session_id",
        ["../escape", "with/slash", "wild*card", "[class]", ""],
    )
    def test_unsafe_session_id_is_rejected(self, tmp_path, unsafe_session_id):
        assert migrate_session(
            old_config_dir=str(tmp_path / "old"),
            new_config_dir=str(tmp_path / "new"),
            session_id=unsafe_session_id,
        ) is False

    def test_missing_session_file(self, tmp_path):
        result = migrate_session(
            old_config_dir=str(tmp_path / "old"),
            new_config_dir=str(tmp_path / "new"),
            session_id="nonexistent",
        )
        assert result is False

    def test_different_inode_refuses(self, tmp_path):
        old_dir = tmp_path / "old"
        new_dir = tmp_path / "new"
        session_id = "sid-conflict"

        # Create in old
        (old_dir / "projects" / "cwd").mkdir(parents=True)
        (old_dir / "projects" / "cwd" / f"{session_id}.jsonl").write_text("old\n")

        # Create a different file at the target
        (new_dir / "projects" / "cwd").mkdir(parents=True)
        (new_dir / "projects" / "cwd" / f"{session_id}.jsonl").write_text("different\n")

        result = migrate_session(
            old_config_dir=str(old_dir),
            new_config_dir=str(new_dir),
            session_id=session_id,
        )
        assert result is False

    @pytest.mark.asyncio
    async def test_async_migration_does_not_block_and_settles_cancellation(
        self, monkeypatch
    ):
        started = threading.Event()
        release = threading.Event()

        def blocking_migration(**_kwargs):
            started.set()
            assert release.wait(timeout=2)
            return True

        monkeypatch.setattr(
            "backend.services.claude_pool.migrate_session",
            blocking_migration,
        )
        operation = asyncio.create_task(
            migrate_session_async(
                old_config_dir="/old",
                new_config_dir="/new",
                session_id="sid-async",
            )
        )
        assert await asyncio.to_thread(started.wait, 1)

        operation.cancel()
        await asyncio.sleep(0)
        assert not operation.done()

        release.set()
        with pytest.raises(asyncio.CancelledError):
            await operation


# ---------------------------------------------------------------------------
# collect_process_output_for_detection
# ---------------------------------------------------------------------------

class TestCollectProcessOutput:
    def test_combines_stderr_and_logs(self):
        result = collect_process_output_for_detection(
            "stderr line",
            ["log line 1", "log line 2"],
        )
        assert "stderr line" in result
        assert "log line 1" in result
        assert "log line 2" in result

    def test_empty_inputs(self):
        result = collect_process_output_for_detection("", [])
        assert result == ""

    def test_none_logs_filtered(self):
        result = collect_process_output_for_detection("err", [None, "content", None])
        assert "content" in result


# ---------------------------------------------------------------------------
# Dispatcher pool integration (unit-level)
# ---------------------------------------------------------------------------

class TestDispatcherPoolIntegration:
    """Test that the dispatcher correctly initializes and uses the pool."""

    @pytest.mark.asyncio
    async def test_pool_not_initialized_when_disabled(self):
        """Pool should be None when pool_enabled is False."""
        from backend.services.dispatcher import GlobalDispatcher
        from backend.services.instance_manager import InstanceManager
        from backend.services.ws_broadcaster import WebSocketBroadcaster

        mock_db = MagicMock()
        broadcaster = WebSocketBroadcaster()
        im = InstanceManager(db_factory=mock_db, broadcaster=broadcaster)
        dispatcher = GlobalDispatcher(db_factory=mock_db, instance_manager=im, broadcaster=broadcaster)
        assert dispatcher.pool is None

    @pytest.mark.asyncio
    async def test_pool_select_returns_none_when_no_pool(self):
        from backend.services.dispatcher import GlobalDispatcher
        from backend.services.instance_manager import InstanceManager
        from backend.services.ws_broadcaster import WebSocketBroadcaster

        mock_db = MagicMock()
        broadcaster = WebSocketBroadcaster()
        im = InstanceManager(db_factory=mock_db, broadcaster=broadcaster)
        dispatcher = GlobalDispatcher(db_factory=mock_db, instance_manager=im, broadcaster=broadcaster)
        assert await dispatcher._pool_select() is None


# ---------- 2026-06-10 生产事故回归：新版 CC 限流文案未被识别 ----------

class TestRateLimitWordingVariants:
    """CC 2.1.x 的实际限流文案必须全部命中（task 79 撞限未换号的根因）。"""

    def test_session_limit_wording(self):
        from backend.services.claude_pool import is_rate_limited
        # 2026-06-10 生产实际文案
        assert is_rate_limited("You've hit your session limit · resets 5:50pm (UTC)")

    def test_weekly_limit_wording(self):
        from backend.services.claude_pool import is_rate_limited
        assert is_rate_limited("You've hit your weekly limit · resets 8am (America/Los_Angeles)")

    def test_legacy_wordings_still_match(self):
        from backend.services.claude_pool import is_rate_limited
        assert is_rate_limited("You've hit your limit")
        assert is_rate_limited("usage limit reached")
        assert is_rate_limited("resets 5pm (America/New_York)")

    def test_resets_with_minutes_any_timezone(self):
        from backend.services.claude_pool import is_rate_limited
        assert is_rate_limited("resets 11:30am (UTC)")
        assert is_rate_limited("resets 5:50pm (Asia/Shanghai)")

    def test_normal_text_not_matched(self):
        from backend.services.claude_pool import is_rate_limited
        assert not is_rate_limited("I implemented the rate limiter middleware as requested")
        assert not is_rate_limited("the function resets the counter")


# ---------- 2026-06-11 修复回归：chat 路径 pool 切换 + probe + 额度 ----------

class TestChatPoolRotationRegression:
    """回归：_try_chat_pool_rotation 曾用位置参数调用 keyword-only 的
    migrate_session，TypeError 被吞掉导致 chat 路径切换从未生效。"""

    @pytest.mark.asyncio
    async def test_chat_rotation_succeeds_and_migrates_session(self, pool, tmp_path, monkeypatch):
        from backend.services.instance_manager import InstanceManager

        # session jsonl 存在于 acc-1 的 config dir 下
        old_dir = tmp_path / "claude-1"
        proj = old_dir / "projects" / "-home-user-repo"
        proj.mkdir(parents=True)
        (proj / "sess-123.jsonl").write_text("{}")

        import backend.main
        fake_dispatcher = MagicMock()
        fake_dispatcher.pool = pool
        fake_dispatcher._persist_claude_binding_for_route = AsyncMock(
            return_value=True
        )
        monkeypatch.setattr(backend.main, "dispatcher", fake_dispatcher)

        task = MagicMock()
        task.session_id = "sess-123"
        task.last_cwd = "/home/user/repo"
        task.target_repo = None

        db = AsyncMock()
        db.get = AsyncMock(return_value=task)
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=db)
        ctx.__aexit__ = AsyncMock(return_value=False)
        db_factory = MagicMock(return_value=ctx)

        broadcaster = MagicMock()
        broadcaster.broadcast = AsyncMock()

        im = InstanceManager(db_factory=db_factory, broadcaster=broadcaster)
        im._config_dirs[1] = str(old_dir)
        im._launch_params[1] = {"prompt": "hello"}
        im.get_recent_log_contents = AsyncMock(return_value=["You've hit your limit"])
        im.launch = AsyncMock()
        # This regression isolates account/session migration after admission.
        # Exact pre-provider replay fencing is covered by
        # test_service_instance_manager; bypass it here rather than relying on
        # the old source-less MagicMock Task shape, which now fails closed.
        im._chat_automatic_relaunch_is_blocked = AsyncMock(return_value=False)

        ok = await im._try_chat_pool_rotation(1, 42, 1, "You've hit your limit")

        assert ok is True
        im.launch.assert_awaited_once()
        # session 已硬链接到新账号 (acc-2)
        new_jsonl = tmp_path / "claude-2" / "projects" / "-home-user-repo" / "sess-123.jsonl"
        assert new_jsonl.exists()
        assert im.launch.await_args.kwargs["config_dir"] == str(tmp_path / "claude-2")
        fake_dispatcher._persist_claude_binding_for_route.assert_awaited_once_with(
            task_id=42,
            config_dir=str(tmp_path / "claude-2"),
            expected_generation=None,
        )


class TestLocateSessionConfigDir:
    def test_finds_session_under_account_dir(self, pool, tmp_path):
        proj = tmp_path / "claude-2" / "projects" / "-x"
        proj.mkdir(parents=True)
        (proj / "sid-9.jsonl").write_text("{}")
        assert pool.locate_session_config_dir("sid-9") == str(tmp_path / "claude-2")

    def test_returns_none_when_not_found(self, pool):
        assert pool.locate_session_config_dir("nope") is None

    def test_returns_all_session_copies_in_search_order(self, pool, tmp_path):
        for account_dir in ("claude-1", "claude-2"):
            project = tmp_path / account_dir / "projects" / "-x"
            project.mkdir(parents=True)
            (project / "sid-copied.jsonl").write_text("{}")

        assert pool.locate_session_config_dirs("sid-copied") == [
            str(tmp_path / "claude-1"),
            str(tmp_path / "claude-2"),
        ]
        assert (
            pool.locate_session_config_dir("sid-copied")
            == str(tmp_path / "claude-1")
        )

    def test_resident_copy_overrides_legacy_search_order(self, pool, tmp_path):
        for account_dir in ("claude-1", "claude-2"):
            project = tmp_path / account_dir / "projects" / "-x"
            project.mkdir(parents=True)
            (project / "sid-resident.jsonl").write_text("{}")

        assert pool.locate_session_config_dir(
            "sid-resident",
            resident_config_dir=str(tmp_path / "claude-2"),
        ) == str(tmp_path / "claude-2")

    def test_authoritative_copy_is_the_unique_sidecar_superset(
        self, pool, tmp_path
    ):
        first_project = tmp_path / "claude-1" / "projects" / "-x"
        second_project = tmp_path / "claude-2" / "projects" / "-x"
        first_project.mkdir(parents=True)
        second_project.mkdir(parents=True)
        first_jsonl = first_project / "sid-sidecar.jsonl"
        first_jsonl.write_text("{}")
        os.link(first_jsonl, second_project / "sid-sidecar.jsonl")
        tool_result = (
            second_project
            / "sid-sidecar"
            / "tool-results"
            / "latest.txt"
        )
        tool_result.parent.mkdir(parents=True)
        tool_result.write_text("newer resident state")

        assert pool.authoritative_session_config_dir(
            "sid-sidecar",
            [str(tmp_path / "claude-1"), str(tmp_path / "claude-2")],
        ) == str(tmp_path / "claude-2")

    def test_disjoint_sidecars_have_no_provable_owner(self, pool, tmp_path):
        first_project = tmp_path / "claude-1" / "projects" / "-x"
        second_project = tmp_path / "claude-2" / "projects" / "-x"
        first_project.mkdir(parents=True)
        second_project.mkdir(parents=True)
        first_jsonl = first_project / "sid-split.jsonl"
        first_jsonl.write_text("{}")
        os.link(first_jsonl, second_project / "sid-split.jsonl")
        for project, filename in (
            (first_project, "left.txt"),
            (second_project, "right.txt"),
        ):
            sidecar = project / "sid-split" / filename
            sidecar.parent.mkdir(parents=True)
            sidecar.write_text(filename)

        assert pool.authoritative_session_config_dir(
            "sid-split",
            [str(tmp_path / "claude-1"), str(tmp_path / "claude-2")],
        ) is None

    def test_symlink_jsonl_is_not_session_evidence(self, pool, tmp_path):
        target = tmp_path / "real.jsonl"
        target.write_text("{}")
        project = tmp_path / "claude-1" / "projects" / "-x"
        project.mkdir(parents=True)
        (project / "sid-link.jsonl").symlink_to(target)

        assert pool.locate_session_config_dirs("sid-link") == []


class TestSelectAsync:
    @pytest.mark.asyncio
    async def test_select_async_without_validate(self, pool, tmp_path):
        result = await pool.select_async()
        assert result in [str(tmp_path / "claude-1"), str(tmp_path / "claude-2")]


class TestProbeEnvCleanup:
    def test_probe_strips_nested_session_vars(self, pool, monkeypatch):
        monkeypatch.setenv("CLAUDECODE", "1")
        monkeypatch.setenv("CLAUDE_CODE", "1")
        captured = {}

        def fake_run(cmd, *, env, **kwargs):
            captured["env"] = env
            r = MagicMock()
            r.returncode = 0
            r.stdout, r.stderr = "ok", ""
            return r

        monkeypatch.setattr("backend.services.claude_pool.subprocess.run", fake_run)
        account = pool._accounts[0]
        assert pool._probe_account(account) is True
        assert "CLAUDECODE" not in captured["env"]
        assert "CLAUDE_CODE" not in captured["env"]
        assert captured["env"]["CLAUDE_CONFIG_DIR"] == account.config_dir


class TestFetchUsage:
    def _write_creds(self, config_dir, expires_in_ms=10**15, refresh_token=None):
        config_dir.mkdir(parents=True, exist_ok=True)
        creds = {
            "accessToken": "sk-ant-oat01-test",
            "expiresAt": expires_in_ms,
            "subscriptionType": "max",
        }
        if refresh_token:
            creds["refreshToken"] = refresh_token
        (config_dir / ".credentials.json").write_text(json.dumps({"claudeAiOauth": creds}))

    def _fake_client(self, payload, status_code=200, counter=None,
                     post_payload=None, post_status=200):
        class FakeResp:
            def __init__(self, status, body):
                self.status_code = status
                self._body = body
            def json(self):
                return self._body

        class FakeClient:
            def __init__(self, *a, **k):
                pass
            async def __aenter__(self):
                return self
            async def __aexit__(self, *a):
                return False
            async def get(self, url, headers=None):
                if counter is not None:
                    counter["n"] += 1
                return FakeResp(status_code, payload)
            async def post(self, url, json=None, headers=None):
                return FakeResp(post_status, post_payload or {})

        return FakeClient

    @pytest.mark.asyncio
    async def test_fetch_usage_returns_utilization(self, pool, tmp_path, monkeypatch):
        self._write_creds(tmp_path / "claude-1")
        payload = {
            "five_hour": {"utilization": 14.0, "resets_at": "2026-06-11T06:59:59+00:00"},
            "seven_day": {"utilization": 38.0, "resets_at": "2026-06-12T17:59:59+00:00"},
        }
        monkeypatch.setattr("httpx.AsyncClient", self._fake_client(payload))
        results = await pool.fetch_usage()
        by_id = {r["id"]: r for r in results}
        assert by_id["acc-1"]["usage"]["five_hour"]["utilization"] == 14.0
        assert by_id["acc-1"]["usage"]["seven_day"]["utilization"] == 38.0
        assert by_id["acc-1"]["subscription_type"] == "max"
        # acc-2 没有 credentials 文件
        assert by_id["acc-2"]["error"] == "no_credentials"
        assert by_id["acc-2"]["usage"] is None

    @pytest.mark.asyncio
    async def test_fetch_usage_skips_disabled_no_request(self, pool, tmp_path, monkeypatch):
        """Disabled accounts make zero outbound usage requests, even with valid
        creds on disk: no token read/refresh, no usage API call."""
        self._write_creds(tmp_path / "claude-1")  # acc-1 enabled
        self._write_creds(tmp_path / "claude-3")  # acc-3 disabled — must be skipped
        counter = {"n": 0}
        payload = {"five_hour": {"utilization": 1.0, "resets_at": None}}
        monkeypatch.setattr("httpx.AsyncClient", self._fake_client(payload, counter=counter))
        results = await pool.fetch_usage()
        by_id = {r["id"]: r for r in results}
        # acc-3 reported as disabled, never queried.
        assert by_id["acc-3"]["error"] == "disabled"
        assert by_id["acc-3"]["usage"] is None
        # Only the one enabled account with creds hit the usage API.
        assert counter["n"] == 1

    @pytest.mark.asyncio
    async def test_fetch_usage_expired_token(self, pool, tmp_path, monkeypatch):
        # 无 refreshToken → 无法刷新，报 token_expired
        self._write_creds(tmp_path / "claude-1", expires_in_ms=1000)
        monkeypatch.setattr("httpx.AsyncClient", self._fake_client({}))
        results = await pool.fetch_usage()
        by_id = {r["id"]: r for r in results}
        assert by_id["acc-1"]["error"] == "token_expired"

    @pytest.mark.asyncio
    async def test_fetch_usage_expired_token_auto_refresh(self, pool, tmp_path, monkeypatch):
        # 过期但有 refreshToken → 自动刷新成功，正常返回 usage 并写回新凭证
        self._write_creds(tmp_path / "claude-1", expires_in_ms=1000, refresh_token="sk-ant-ort01-old")
        payload = {"five_hour": {"utilization": 5.0, "resets_at": None}}
        monkeypatch.setattr("httpx.AsyncClient", self._fake_client(
            payload,
            post_payload={"access_token": "sk-ant-oat01-new",
                          "refresh_token": "sk-ant-ort01-new", "expires_in": 28800},
        ))
        results = await pool.fetch_usage()
        by_id = {r["id"]: r for r in results}
        assert by_id["acc-1"]["error"] is None
        assert by_id["acc-1"]["usage"]["five_hour"]["utilization"] == 5.0
        # refresh token 轮换后必须落盘
        saved = json.loads((tmp_path / "claude-1" / ".credentials.json").read_text())["claudeAiOauth"]
        assert saved["accessToken"] == "sk-ant-oat01-new"
        assert saved["refreshToken"] == "sk-ant-ort01-new"
        assert saved["expiresAt"] / 1000 > __import__("time").time()

    @pytest.mark.asyncio
    async def test_fetch_usage_expired_token_refresh_fails(self, pool, tmp_path, monkeypatch):
        # refresh 被拒（refreshToken 失效）→ 才真正报 token_expired
        self._write_creds(tmp_path / "claude-1", expires_in_ms=1000, refresh_token="sk-ant-ort01-revoked")
        monkeypatch.setattr("httpx.AsyncClient", self._fake_client({}, post_status=401))
        results = await pool.fetch_usage()
        by_id = {r["id"]: r for r in results}
        assert by_id["acc-1"]["error"] == "token_expired"

    @pytest.mark.asyncio
    async def test_fetch_usage_cached(self, pool, tmp_path, monkeypatch):
        self._write_creds(tmp_path / "claude-1")
        counter = {"n": 0}
        payload = {"five_hour": {"utilization": 1.0, "resets_at": None}}
        monkeypatch.setattr("httpx.AsyncClient", self._fake_client(payload, counter=counter))
        await pool.fetch_usage()
        first = counter["n"]
        await pool.fetch_usage()
        assert counter["n"] == first  # 第二次走缓存


# ---------- 手动切号（preferred account）+ PTY 模式 limit 检测 ----------

class TestPreferredAccount:
    """手动切号：preferred 账号插队，不可用时回落自动轮换。"""

    def test_preferred_jumps_queue(self, pool):
        # 默认顺序会选 acc-1；置 preferred 后应选 acc-2
        assert pool.set_preferred("acc-2") is True
        assert pool.select() == pool._accounts[1].config_dir
        assert pool.status()["preferred"] == "acc-2"

    def test_preferred_falls_back_when_cooled(self, pool):
        pool.set_preferred("acc-2")
        pool.mark_rate_limited(pool._accounts[1].config_dir)
        # acc-2 冷却中 → 自动回落 acc-1
        assert pool.select() == pool._accounts[0].config_dir

    def test_preferred_respects_exclude(self, pool):
        pool.set_preferred("acc-2")
        assert pool.select(exclude={"acc-2"}) == pool._accounts[0].config_dir

    def test_clear_preferred(self, pool):
        pool.set_preferred("acc-2")
        assert pool.set_preferred(None) is True
        assert pool.status()["preferred"] is None
        assert pool.select() == pool._accounts[0].config_dir

    def test_unknown_account_rejected(self, pool):
        assert pool.set_preferred("nope") is False
        assert pool.preferred_account_id is None


class TestPtyModeRateLimitDetection:
    """PTY 模式 limit 后切号：PTY 框架以错误 message 事件结束 turn，
    其文案必须命中 is_rate_limited，使 _check_rate_limit_and_rotate 生效。"""

    def test_pty_banner_message_matches(self):
        # claude_pty session.py 在 rate-limit 时 yield 的固定文案
        msg = ("usage limit reached — account hit its rate limit "
               "(detected in PTY session)")
        assert is_rate_limited(msg)
        assert is_pool_rotatable(msg)

    def test_pty_jsonl_rate_limit_event_content(self):
        # jsonl_reader 对 rate_limit_event 的 normalize 内容
        assert is_pool_rotatable("usage limit reached") is True


class TestLastSelected:
    def test_select_is_only_a_proposal(self, pool):
        pool.select()
        assert pool.status()["last_selected"] is None

    def test_real_route_can_override_provisional_selection(self, pool):
        pool.select()

        assert pool.record_routed_account(pool._accounts[1].config_dir) is True
        assert pool.status()["last_selected"] == "acc-2"

    def test_unknown_real_route_does_not_replace_marker(self, pool, tmp_path):
        pool.record_routed_account(pool._accounts[1].config_dir)
        pool.select()

        assert pool.record_routed_account(str(tmp_path / "unknown")) is False
        assert pool.status()["last_selected"] == "acc-2"

    def test_no_selection_yet(self, pool):
        assert pool.status()["last_selected"] is None


class TestRateLimitEventActionable:
    """Only a genuine near-limit/blocked rate_limit_event may bench an account.

    Routine "allowed" pings (emitted almost every turn) and low-utilization
    warnings must NOT — that was starving the 3-account pool and making resumes
    hit "no available accounts" (prod #734/#740).
    """

    def test_allowed_is_not_actionable(self):
        assert rate_limit_event_is_actionable(
            {"status": "allowed", "rateLimitType": "five_hour"}
        ) is False

    def test_seven_day_warning_never_actionable(self):
        # The actual prod trigger: 37% of the 7-day quota benched an account 5 min.
        assert rate_limit_event_is_actionable(
            {"status": "allowed_warning", "rateLimitType": "seven_day", "utilization": 0.37}
        ) is False

    def test_seven_day_warning_high_util_is_actionable(self):
        assert rate_limit_event_is_actionable(
            {"status": "allowed_warning", "rateLimitType": "seven_day", "utilization": 0.99}
        ) is True

    def test_five_hour_low_warning_not_actionable(self):
        assert rate_limit_event_is_actionable(
            {"status": "allowed_warning", "rateLimitType": "five_hour", "utilization": 0.5}
        ) is False

    def test_five_hour_high_warning_actionable(self):
        assert rate_limit_event_is_actionable(
            {"status": "allowed_warning", "rateLimitType": "five_hour", "utilization": 0.9}
        ) is True

    def test_five_hour_warning_uses_surpassed_threshold_fallback(self):
        assert rate_limit_event_is_actionable(
            {"status": "allowed_warning", "rateLimitType": "five_hour", "surpassedThreshold": 0.95}
        ) is True

    def test_rejected_is_actionable(self):
        assert rate_limit_event_is_actionable(
            {"status": "rejected", "rateLimitType": "five_hour"}
        ) is True

    def test_none_and_non_dict_not_actionable(self):
        assert rate_limit_event_is_actionable(None) is False
        assert rate_limit_event_is_actionable("nope") is False

    def test_warning_with_unparseable_util_not_actionable(self):
        assert rate_limit_event_is_actionable(
            {"status": "allowed_warning", "rateLimitType": "five_hour", "utilization": None}
        ) is False


class TestQuotaAwareSelection:
    def test_usage_threshold_checks_five_hour_or_week(self):
        assert quota_usage_at_or_above({
            "five_hour": {"utilization": 90},
            "seven_day": {"utilization": 20},
        })
        assert quota_usage_at_or_above({
            "five_hour": {"utilization": 20},
            "seven_day": {"utilization": 91},
        })
        assert not quota_usage_at_or_above({
            "five_hour": {"utilization": 89.9},
            "seven_day": {"utilization": 89},
        })

    @pytest.mark.asyncio
    async def test_selects_below_threshold_alternative(self, pool, tmp_path):
        pool.fetch_usage = AsyncMock(return_value=[
            {"id": "acc-1", "usage": {"five_hour": {"utilization": 95}}},
            {"id": "acc-2", "usage": {"seven_day": {"utilization": 35}}},
        ])

        selected = await pool.select_quota_alternative(
            str(tmp_path / "claude-1")
        )

        assert selected == str(tmp_path / "claude-2")
        pool.fetch_usage.assert_awaited_once_with(force=True)

    @pytest.mark.asyncio
    async def test_known_high_alternative_is_rejected_but_unknown_is_allowed(
        self, pool, tmp_path,
    ):
        # acc-2 is known-high, so no enabled alternative remains.
        pool.fetch_usage = AsyncMock(return_value=[
            {"id": "acc-1", "usage": {"five_hour": {"utilization": 95}}},
            {"id": "acc-2", "usage": {"seven_day": {"utilization": 90}}},
        ])
        assert await pool.select_quota_alternative(
            str(tmp_path / "claude-1")
        ) is None

        # A failed/missing snapshot is explicitly an eligible fallback.
        pool.fetch_usage = AsyncMock(return_value=[
            {"id": "acc-1", "usage": {"five_hour": {"utilization": 95}}},
            {"id": "acc-2", "usage": None, "error": "request_failed"},
        ])
        assert await pool.select_quota_alternative(
            str(tmp_path / "claude-1")
        ) == str(tmp_path / "claude-2")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "error",
        ["no_credentials", "token_expired", "http_401", "http_403"],
    )
    async def test_definitive_auth_error_is_not_an_alternative(
        self, pool, tmp_path, error,
    ):
        pool.fetch_usage = AsyncMock(return_value=[
            {"id": "acc-1", "usage": {"five_hour": {"utilization": 95}}},
            {"id": "acc-2", "usage": None, "error": error},
        ])

        assert await pool.select_quota_alternative(
            str(tmp_path / "claude-1")
        ) is None

    @pytest.mark.asyncio
    async def test_request_failure_remains_an_unknown_but_eligible_alternative(
        self, pool, tmp_path,
    ):
        pool.fetch_usage = AsyncMock(return_value=[
            {"id": "acc-1", "usage": {"five_hour": {"utilization": 95}}},
            {"id": "acc-2", "usage": None, "error": "request_failed: timeout"},
        ])

        assert await pool.select_quota_alternative(
            str(tmp_path / "claude-1")
        ) == str(tmp_path / "claude-2")

    def test_reset_timestamp_controls_soft_cooldown(self):
        assert quota_cooldown_seconds(
            {"resetsAt": 1_700_000_900}, now=1_700_000_000
        ) == 900
        assert quota_cooldown_seconds(
            {"resetsAt": 1_700_000_900_000}, now=1_700_000_000
        ) == 900
        assert quota_cooldown_seconds(
            {"resetsAt": 1_699_999_999}, now=1_700_000_000, fallback=77
        ) == 77
        assert quota_cooldown_seconds(
            {"resetsAt": 1_800_000_000},
            now=1_700_000_000,
            maximum=123,
        ) == 123


# ---------------------------------------------------------------------------
# Codex-provider detection (texts from codex-rs rust-v0.144.6
# protocol/src/error.rs — CLI 同版实证)
# ---------------------------------------------------------------------------

from backend.services.claude_pool import (
    is_codex_transient,
    is_codex_usage_limited,
    is_codex_auth_failure,
    is_transient_for,
)


class TestCodexTransientDetection:
    def test_stream_disconnected(self):
        assert is_codex_transient("stream disconnected before completion: transport error")

    def test_request_timed_out(self):
        assert is_codex_transient("request timed out")

    def test_connection_failed(self):
        assert is_codex_transient("Connection failed: error sending request")

    def test_response_stream_failed(self):
        assert is_codex_transient("Error while reading the server response: connection reset, request id: req_1")

    def test_internal_server_error(self):
        assert is_codex_transient("We're currently experiencing high demand, which may cause temporary errors.")

    def test_server_overloaded(self):
        assert is_codex_transient("Selected model is at capacity. Please try a different model.")

    def test_unexpected_status_429(self):
        assert is_codex_transient("unexpected status 429 Too Many Requests: rate limited")

    def test_unexpected_status_500(self):
        assert is_codex_transient("unexpected status 500 Internal Server Error: oops")

    def test_retry_limit_429(self):
        assert is_codex_transient("exceeded retry limit, last status: 429 Too Many Requests")

    @pytest.mark.parametrize(
        "detail",
        [
            "all logged-in accounts are busy",
            "no eligible logged-in account is ready",
        ],
    )
    def test_unexpected_status_409_is_not_generic_transient(self, detail):
        assert not is_codex_transient(
            f"unexpected status 409 Conflict: {detail}"
        )

    def test_unexpected_status_401_not_transient(self):
        # 401 是认证失败，不能退避重试
        assert not is_codex_transient(
            "unexpected status 401 Unauthorized: Your authentication token has been invalidated."
        )

    def test_usage_limit_not_transient(self):
        assert not is_codex_transient(
            "You've hit your usage limit for gpt-5.6-sol. Switch to another model now, or retry after 5pm"
        )

    def test_quota_exceeded_not_transient(self):
        assert not is_codex_transient("Quota exceeded. Check your plan and billing details.")

    def test_auth_revoked_not_transient(self):
        assert not is_codex_transient(
            "Your access token could not be refreshed because your refresh token was revoked. "
            "Please log out and sign in again."
        )

    def test_normal_output_not_transient(self):
        assert not is_codex_transient("All tests passed, pushed to main.")
        assert not is_codex_transient("")


class TestCodexUsageAndAuthDetection:
    def test_usage_limit_variants(self):
        assert is_codex_usage_limited("You've hit your usage limit for codex.")
        assert is_codex_usage_limited("Quota exceeded. Check your plan and billing details.")
        assert is_codex_usage_limited("Your workspace is out of credits. Add credits to continue.")
        assert is_codex_usage_limited("You hit your spend cap set in your workspace.")

    def test_auth_variants(self):
        assert is_codex_auth_failure("Your authentication token has been invalidated. Please try signing in again.")
        assert is_codex_auth_failure("refresh token was revoked. Please log out and sign in again.")
        assert is_codex_auth_failure("unexpected status 401 Unauthorized: nope")


class TestProviderAwareTransientRouting:
    def test_claude_provider_uses_claude_detector(self):
        text = "API Error: Server is temporarily limiting requests (not your usage limit)"
        assert is_transient_for("claude", text)
        assert is_transient_for(None, text)  # 默认 claude
        assert not is_transient_for("codex", text)

    def test_codex_provider_uses_codex_detector(self):
        text = "stream disconnected before completion: transport error"
        assert is_transient_for("codex", text)
        assert not is_transient_for("claude", text)

    def test_codex_usage_limit_text_matches_claude_rate_limit_regex(self):
        """危险重叠回归锚点：codex 的限额文案会命中 claude 的 _RATE_LIMIT_RE
        （"hit your usage limit" ⊂ "hit your (\\w+ )?limit"）。号池轮换路径
        因此必须按 provider 显式 gate（dispatcher._check_rate_limit_and_rotate /
        instance_manager._try_chat_pool_rotation），否则 codex 撞限额会冷却
        无辜的 claude 账号并用 claude --resume 重启 codex session。"""
        codex_banner = "You've hit your usage limit for gpt-5.6-sol."
        assert is_rate_limited(codex_banner)  # 重叠事实（若哪天不再重叠，gate 仍无害）
        assert is_codex_usage_limited(codex_banner)
        assert not is_transient_for("codex", codex_banner)


class TestChatPoolRotationCodexGate:
    """codex 任务绝不能进 claude 号池轮换：其限额文案会命中 claude 正则，
    不 gate 会用 claude --resume 重启 codex session（provider 丢失）。"""

    @pytest.mark.asyncio
    async def test_codex_provider_skips_rotation(self, pool, monkeypatch):
        from backend.services.instance_manager import InstanceManager

        import backend.main
        fake_dispatcher = MagicMock()
        fake_dispatcher.pool = pool
        monkeypatch.setattr(backend.main, "dispatcher", fake_dispatcher)

        im = InstanceManager(db_factory=MagicMock(), broadcaster=MagicMock())
        im._launch_params[1] = {"prompt": "hello", "provider": "codex"}
        im.get_recent_log_contents = AsyncMock(
            return_value=["You've hit your usage limit for gpt-5.6-sol."]
        )
        im.launch = AsyncMock()

        ok = await im._try_chat_pool_rotation(1, 42, 1, "")

        assert ok is False
        im.launch.assert_not_awaited()


def _async_db_ctx(task):
    """db_factory 替身：async with db_factory() as db 里 db.get 返回 task。"""
    db = AsyncMock()
    db.get = AsyncMock(return_value=task)
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=db)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return MagicMock(return_value=ctx)


class TestDispatcherRotationCodexGate:
    """dispatcher._check_rate_limit_and_rotate 的 provider gate：
    codex 限额文案命中 claude 正则也绝不轮换/冷却。"""

    def _dispatcher(self, pool, task):
        from backend.services.dispatcher import GlobalDispatcher
        disp = GlobalDispatcher(
            db_factory=_async_db_ctx(task),
            instance_manager=MagicMock(),
            broadcaster=MagicMock(),
        )
        disp.pool = pool
        disp.broadcaster.broadcast = AsyncMock()
        return disp

    @pytest.mark.asyncio
    async def test_codex_task_never_rotates_nor_cools_down(self, pool):
        task = MagicMock()
        task.provider = "codex"
        task.session_id = None
        disp = self._dispatcher(pool, task)

        result = await disp._check_rate_limit_and_rotate(
            1, 42, 1, combined="You've hit your usage limit for gpt-5.6-sol."
        )

        assert result is None
        assert pool._cooldowns == {}  # 没有任何账号被冷却

    @pytest.mark.asyncio
    async def test_claude_task_still_rotates(self, pool, tmp_path):
        """正向对照：同一段限速文案，claude 任务照常轮换 + 冷却旧号。"""
        task = MagicMock()
        task.provider = "claude"
        task.session_id = None  # 无 session → 跳过 migrate
        disp = self._dispatcher(pool, task)
        disp.instance_manager.get_config_dir = MagicMock(
            return_value=str(tmp_path / "claude-1")
        )
        # validate=True 会起 claude -p 探测子进程，测试里直接 stub 选号结果
        disp._pool_select = AsyncMock(return_value=str(tmp_path / "claude-2"))
        disp._persist_claude_binding_for_route = AsyncMock(return_value=True)

        result = await disp._check_rate_limit_and_rotate(
            1, 42, 1, combined="You've hit your limit, resets 3pm (UTC)"
        )

        assert result is not None
        assert result["config_dir"] == str(tmp_path / "claude-2")
        assert "acc-1" in pool._cooldowns  # 旧号进冷却


class TestChatTransientRetryCodex:
    """instance_manager._try_chat_transient_retry 的 codex 链路：
    codex 文案触发重试且 relaunch 带 provider=codex（丢 provider 会用
    claude --resume 重启 codex session）。"""

    def _im(self, provider, log_text, monkeypatch):
        from backend.services.instance_manager import InstanceManager
        monkeypatch.setattr("backend.config.settings.transient_retry_base_delay", 0.01)
        monkeypatch.setattr("backend.config.settings.transient_retry_max_delay", 0.02)

        task = MagicMock()
        task.session_id = "thread-1"
        task.last_cwd = "/repo"
        task.target_repo = None

        broadcaster = MagicMock()
        broadcaster.broadcast = AsyncMock()
        im = InstanceManager(db_factory=_async_db_ctx(task), broadcaster=broadcaster)
        im._launch_params[1] = {"prompt": "hello", "provider": provider}
        im.get_recent_log_contents = AsyncMock(return_value=[log_text])
        im.launch = AsyncMock()
        # These provider-routing tests intentionally isolate the relaunch
        # arguments. Source/transport admission safety has its own exact-turn
        # matrix in test_service_instance_manager.
        im._chat_automatic_relaunch_is_blocked = AsyncMock(return_value=False)
        return im

    @pytest.mark.asyncio
    async def test_codex_transient_relaunches_with_codex_provider(self, monkeypatch):
        im = self._im(
            "codex",
            "stream disconnected before completion: transport error",
            monkeypatch,
        )
        ok = await im._try_chat_transient_retry(1, 42, 1, "")
        assert ok is True
        im.launch.assert_awaited_once()
        assert im.launch.await_args.kwargs["provider"] == "codex"
        assert im.launch.await_args.kwargs["resume_session_id"] == "thread-1"

    @pytest.mark.asyncio
    async def test_claude_wording_does_not_trigger_codex_retry(self, monkeypatch):
        # claude 的 transient 文案对 codex 任务不生效（provider 分流）
        im = self._im(
            "codex",
            "API Error: Server is temporarily limiting requests (not your usage limit)",
            monkeypatch,
        )
        ok = await im._try_chat_transient_retry(1, 42, 1, "")
        assert ok is False
        im.launch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_codex_usage_limit_does_not_trigger_retry(self, monkeypatch):
        # 限额不是 transient：退避重试救不了，应走正常失败路径
        im = self._im(
            "codex",
            "You've hit your usage limit for gpt-5.6-sol.",
            monkeypatch,
        )
        ok = await im._try_chat_transient_retry(1, 42, 1, "")
        assert ok is False
        im.launch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_claude_transient_still_relaunches_claude(self, monkeypatch):
        """正向对照：claude 任务 + claude 文案照常重试，provider 透传 claude。"""
        im = self._im(
            "claude",
            "API Error: Server is temporarily limiting requests (not your usage limit)",
            monkeypatch,
        )
        ok = await im._try_chat_transient_retry(1, 42, 1, "")
        assert ok is True
        assert im.launch.await_args.kwargs["provider"] == "claude"


class _FakeCloudRouterAccount:
    def __init__(self, root: Path):
        self.id = "cloudrouter-1"
        self.name = "CloudRouter test"
        self.auth_kind = "cloudrouter_api"
        self.api_provider = "cloudrouter"
        self.enabled = True
        self.retired = False
        self.models = {
            "claude": ["claude-sonnet-5"],
            "codex": [],
        }
        self.root = root
        self.claude_config_dir = str(root / "claude")
        self.codex_home = str(root / "codex")

    def supports_model(self, provider, model):
        choices = self.models.get(provider, [])
        return bool(choices) if not model else model in choices


class _FakeCloudRouterStore:
    def __init__(self, account, snapshot=None):
        self._account = account
        self._snapshot = snapshot or {
            "available": True,
            "known": True,
            "reason": "active",
            "state": "active",
        }

    def all_accounts(self, include_retired=False):
        return [self._account]

    def cached_quota_decision(self, _account_id):
        return {
            key: self._snapshot[key]
            for key in ("available", "known", "reason")
        }

    async def fetch_usage(self, _account_id, force=False):
        return dict(self._snapshot)


class TestCloudRouterClaudeProjection:
    @staticmethod
    def _mixed_pool(tmp_path, account, snapshot=None):
        native_dir = tmp_path / "native-claude"
        native_path = tmp_path / "native-accounts.json"
        native_path.write_text(json.dumps({
            "accounts": [{
                "id": "native-1",
                "config_dir": str(native_dir),
                "enabled": True,
            }],
        }))
        pool = ClaudePool(
            config_path=native_path,
            cloudrouter_store=_FakeCloudRouterStore(account, snapshot),
            bootstrap_default=False,
        )
        return pool, native_dir

    def test_fresh_selection_prefers_compatible_api_over_native(self, tmp_path):
        account = _FakeCloudRouterAccount(tmp_path / "cloudrouter-1")
        pool, _native_dir = self._mixed_pool(tmp_path, account)

        assert pool.select(model="claude-sonnet-5") == account.claude_config_dir
        assert pool.status()["last_selected"] is None

    def test_unknown_api_health_uses_native_until_probe_settles(self, tmp_path):
        account = _FakeCloudRouterAccount(tmp_path / "cloudrouter-1")
        snapshot = {
            "available": True,
            "known": False,
            "reason": "not_probed",
            "state": "unknown",
        }
        pool, native_dir = self._mixed_pool(tmp_path, account, snapshot)

        assert pool.select(model="claude-sonnet-5") == str(native_dir)

    def test_unknown_api_health_remains_api_only_fallback(self, tmp_path):
        account = _FakeCloudRouterAccount(tmp_path / "cloudrouter-1")
        snapshot = {
            "available": True,
            "known": False,
            "reason": "not_probed",
            "state": "unknown",
        }
        pool = ClaudePool(
            config_path=tmp_path / "missing-native-pool.json",
            cloudrouter_store=_FakeCloudRouterStore(account, snapshot),
            bootstrap_default=False,
        )

        assert pool.select(model="claude-sonnet-5") == account.claude_config_dir

    def test_unsupported_api_model_preserves_native_fallback(self, tmp_path):
        account = _FakeCloudRouterAccount(tmp_path / "cloudrouter-1")
        pool, native_dir = self._mixed_pool(tmp_path, account)

        assert pool.select(model="claude-opus-4-8") == str(native_dir)

    def test_exhausted_api_preserves_native_fallback(self, tmp_path):
        account = _FakeCloudRouterAccount(tmp_path / "cloudrouter-1")
        snapshot = {
            "available": False,
            "known": True,
            "reason": "quota_exhausted",
            "state": "exhausted",
        }
        pool, native_dir = self._mixed_pool(tmp_path, account, snapshot)

        assert pool.select(model="claude-sonnet-5") == str(native_dir)

    def test_preferred_native_account_outranks_available_api(self, tmp_path):
        account = _FakeCloudRouterAccount(tmp_path / "cloudrouter-1")
        pool, native_dir = self._mixed_pool(tmp_path, account)

        assert pool.set_preferred("native-1") is True
        assert pool.select(model="claude-sonnet-5") == str(native_dir)

    def test_api_only_pool_loads_without_native_config_and_filters_models(
        self, tmp_path
    ):
        account = _FakeCloudRouterAccount(tmp_path / "cloudrouter-1")
        pool = ClaudePool(
            config_path=tmp_path / "missing-native-pool.json",
            cloudrouter_store=_FakeCloudRouterStore(account),
            bootstrap_default=False,
        )

        assert pool.select(model="claude-sonnet-5") == account.claude_config_dir
        assert pool.select(model="claude-opus-4-8") is None
        public = pool.list_accounts()[0]
        assert public["auth_kind"] == "cloudrouter_api"
        assert public["api_provider"] == "cloudrouter"
        assert public["api_account_id"] == "cloudrouter-1"
        assert public["supported_models"] == ["claude-sonnet-5"]

    @pytest.mark.asyncio
    async def test_api_usage_uses_store_and_known_exhaustion_blocks_selection(
        self, tmp_path
    ):
        account = _FakeCloudRouterAccount(tmp_path / "cloudrouter-1")
        snapshot = {
            "available": False,
            "known": True,
            "reason": "quota_exhausted",
            "state": "exhausted",
        }
        pool = ClaudePool(
            config_path=tmp_path / "missing-native-pool.json",
            cloudrouter_store=_FakeCloudRouterStore(account, snapshot),
            bootstrap_default=False,
        )

        rows = await pool.fetch_usage(force=True)

        assert rows[0]["api_quota"] == snapshot
        assert rows[0]["error"] == "quota_exhausted"
        assert pool.select(model="claude-sonnet-5") is None

    @pytest.mark.asyncio
    async def test_projection_survives_provider_model_removal_for_session_discovery(
        self, tmp_path
    ):
        account = _FakeCloudRouterAccount(tmp_path / "cloudrouter-1")
        account.models["claude"] = []
        pool = ClaudePool(
            config_path=tmp_path / "missing-native-pool.json",
            cloudrouter_store=_FakeCloudRouterStore(account),
            bootstrap_default=False,
        )
        session = (
            Path(account.claude_config_dir)
            / "projects"
            / "-repo"
            / "session-after-refresh.jsonl"
        )
        session.parent.mkdir(parents=True)
        session.write_text("{}")

        assert pool.is_known_account(account.claude_config_dir)
        assert pool.is_disabled(account.claude_config_dir)
        assert (
            pool.locate_session_config_dir("session-after-refresh")
            == account.claude_config_dir
        )
        assert pool.list_accounts() == []
        assert pool.status()["total"] == 0
        assert await pool.fetch_usage(force=True) == []

    def test_native_config_is_excluded_when_native_pool_toggle_is_off(
        self, tmp_path
    ):
        native_path = tmp_path / "native-accounts.json"
        native_path.write_text(json.dumps({
            "accounts": [{
                "id": "native-old",
                "config_dir": str(tmp_path / "native-old"),
                "enabled": True,
            }],
        }))
        account = _FakeCloudRouterAccount(tmp_path / "cloudrouter-1")

        pool = ClaudePool(
            config_path=native_path,
            cloudrouter_store=_FakeCloudRouterStore(account),
            bootstrap_default=False,
            include_native=False,
        )

        assert [item.id for item in pool._accounts] == ["cloudrouter-1"]

    @pytest.mark.asyncio
    async def test_api_quota_threshold_selects_only_below_threshold_alternative(
        self, tmp_path
    ):
        current = _FakeCloudRouterAccount(tmp_path / "cloudrouter-1")
        replacement = _FakeCloudRouterAccount(tmp_path / "cloudrouter-2")
        replacement.id = "cloudrouter-2"
        replacement.name = "CloudRouter replacement"
        pool = ClaudePool(
            config_path=tmp_path / "missing-native-pool.json",
            bootstrap_default=False,
        )
        pool._accounts = [
            PoolAccount.from_cloudrouter(current),
            PoolAccount.from_cloudrouter(replacement),
        ]
        pool._cloudrouter_store = MagicMock()
        pool._cloudrouter_store.cached_quota_decision.return_value = {
            "available": True,
            "known": True,
            "reason": "active",
        }
        pool.fetch_usage = AsyncMock(return_value=[
            {
                "id": current.id,
                "api_quota": {
                    "known": True,
                    "available": True,
                    "quota": {"used": 95, "limit": 100},
                },
            },
            {
                "id": replacement.id,
                "api_quota": {
                    "known": True,
                    "available": True,
                    "windows": [{"used": 20, "limit": 100}],
                },
            },
        ])

        assert api_quota_at_or_above(
            {"known": True, "available": True, "quota": {"used": 90, "limit": 100}}
        )
        assert await pool.select_quota_alternative(
            current.claude_config_dir,
            model="claude-sonnet-5",
        ) == replacement.claude_config_dir

        pool.fetch_usage = AsyncMock(return_value=[
            {
                "id": current.id,
                "api_quota": {
                    "known": True,
                    "available": True,
                    "quota": {"used": 50, "limit": 100},
                },
            },
            {
                "id": replacement.id,
                "api_quota": {
                    "known": False,
                    "available": True,
                },
            },
        ])
        assert await pool.select_quota_alternative(
            current.claude_config_dir,
            model="claude-sonnet-5",
        ) is None

    @pytest.mark.asyncio
    async def test_apex_api_projection_is_generic_and_hidden_from_claude(
        self, tmp_path
    ):
        account = _FakeCloudRouterAccount(tmp_path / "apex-1")
        account.id = "apex-1"
        account.name = "Apex API"
        account.auth_kind = "apex_api"
        account.api_provider = "apex"
        account.models = {
            "claude": [],
            "codex": ["gpt-5.4"],
        }
        pool = ClaudePool(
            config_path=tmp_path / "missing-native-pool.json",
            cloudrouter_store=_FakeCloudRouterStore(account),
            bootstrap_default=False,
        )

        projection = pool.account("apex-1")
        assert projection is not None
        assert projection.auth_kind == "apex_api"
        assert projection.api_provider == "apex"
        assert projection.enabled is False
        assert pool.is_cloudrouter_account(account.claude_config_dir) is True
        assert pool.list_accounts() == []
        assert await pool.fetch_usage(force=True) == []
