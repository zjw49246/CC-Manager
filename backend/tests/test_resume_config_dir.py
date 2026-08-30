"""Regression tests for GlobalDispatcher._resolve_resume_config_dir.

Pins the fix for prod tasks #734/#740: when every pool account is rate-limited
(``select`` returns None), a resume must still be anchored to the account dir
that actually holds the session JSONL — otherwise the launch falls through to an
inherited ``CLAUDE_CONFIG_DIR`` that lacks the file and ``claude --resume`` dies
with "No conversation found with session ID", hard-failing the task and losing
the session.
"""
import asyncio
import json
import threading
import time
import pytest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call

from backend.services.claude_pool import ClaudePool, PoolAccount
from backend.services.codex_pool import CodexPool, CodexPoolAccount
from backend.services.dispatcher import (
    CODEX_ROUTING_RETRY_DELAY,
    ClaudeAccountRoutingError,
    CodexAccountRoutingError,
    GlobalDispatcher,
    TaskLifecycleSupersededError,
    _TaskStatusGeneration,
)
from backend.services.instance_manager import InstanceManager


@pytest.fixture
def pool_config(tmp_path):
    config = {
        "accounts": [
            {"id": "acc-1", "config_dir": str(tmp_path / "claude-1"), "email": "a@test.com", "enabled": True},
            {"id": "acc-2", "config_dir": str(tmp_path / "claude-2"), "email": "b@test.com", "enabled": True},
        ],
    }
    config_path = tmp_path / "accounts.json"
    config_path.write_text(json.dumps(config))
    return config_path


@pytest.fixture
def pool(pool_config):
    return ClaudePool(config_path=pool_config, cooldown_seconds=60)


@pytest.fixture
def dispatcher(pool):
    # The helper only touches self.pool; the rest can be inert.
    disp = GlobalDispatcher(
        db_factory=MagicMock(),
        instance_manager=MagicMock(),
        broadcaster=MagicMock(),
    )
    disp.pool = pool
    return disp


def _seed_session(config_dir: Path, session_id: str, encoded_cwd: str = "-home-user-repo") -> Path:
    proj = config_dir / "projects" / encoded_cwd
    proj.mkdir(parents=True)
    jsonl = proj / f"{session_id}.jsonl"
    jsonl.write_text("{}")
    return jsonl


class TestResolveResumeConfigDir:
    @pytest.mark.asyncio
    async def test_pool_exhausted_anchors_to_resident_dir(self, dispatcher, pool, tmp_path, monkeypatch):
        """The bug: all accounts rate-limited → must return the session's dir, not None."""
        monkeypatch.setenv("HOME", str(tmp_path))  # isolate the ~/.claude* home scan
        _seed_session(tmp_path / "claude-2", "sess-734")
        # Every account in cooldown → select() returns None without probing.
        future = time.time() + 999
        pool._cooldowns = {"acc-1": future, "acc-2": future}

        result = await dispatcher._resolve_resume_config_dir("sess-734")

        assert result == str(tmp_path / "claude-2")

    @pytest.mark.asyncio
    async def test_pool_exhausted_fresh_launch_fails_closed(self, dispatcher, pool, tmp_path, monkeypatch):
        """An active pool must not leak through to inherited service credentials."""
        monkeypatch.setenv("HOME", str(tmp_path))
        future = time.time() + 999
        pool._cooldowns = {"acc-1": future, "acc-2": future}

        with pytest.raises(RuntimeError, match="currently unavailable"):
            await dispatcher._resolve_resume_config_dir(None)

    @pytest.mark.asyncio
    async def test_pool_exhausted_unknown_session_returns_none(self, dispatcher, pool, tmp_path, monkeypatch):
        """Exhausted pool + session JSONL nowhere on disk → None (recovery handles it)."""
        monkeypatch.setenv("HOME", str(tmp_path))
        future = time.time() + 999
        pool._cooldowns = {"acc-1": future, "acc-2": future}

        assert await dispatcher._resolve_resume_config_dir("ghost-sid") is None

    @pytest.mark.asyncio
    async def test_healthy_account_migrates_session(self, dispatcher, pool, tmp_path, monkeypatch):
        """Happy path preserved: a healthy account is chosen and the session hardlinked into it."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setattr(pool, "_probe_account", lambda acc: True)  # avoid real `claude -p`
        old_jsonl = _seed_session(tmp_path / "claude-1", "sess-1")
        # Make acc-2 the only selectable account so we get a deterministic migration target.
        pool._cooldowns = {"acc-1": time.time() + 999}

        result = await dispatcher._resolve_resume_config_dir("sess-1")

        assert result == str(tmp_path / "claude-2")
        new_jsonl = tmp_path / "claude-2" / "projects" / "-home-user-repo" / "sess-1.jsonl"
        assert new_jsonl.exists()
        # Hardlinked, not copied — same inode.
        assert new_jsonl.stat().st_ino == old_jsonl.stat().st_ino

    @pytest.mark.asyncio
    async def test_healthy_resident_reused_without_probe(self, dispatcher, pool, tmp_path, monkeypatch):
        """Hot path: session lives on a healthy account → reuse it as-is.

        No ``claude -p`` probe (the old per-message latency) and no migration /
        config_dir drift (which would drop the PTY hot session). We prove the
        probe is never reached by making it explode if called.
        """
        monkeypatch.setenv("HOME", str(tmp_path))
        def _boom(acc):
            raise AssertionError("probe must not run on the resume hot path")
        monkeypatch.setattr(pool, "_probe_account", _boom)
        _seed_session(tmp_path / "claude-1", "sess-hot")
        # acc-1 healthy (no cooldown) — must be returned untouched.

        result = await dispatcher._resolve_resume_config_dir("sess-hot")

        assert result == str(tmp_path / "claude-1")
        # Session NOT copied into the other account (no drift).
        assert not (tmp_path / "claude-2" / "projects").exists()
        assert pool.status()["last_selected"] == "acc-1"

    @pytest.mark.asyncio
    async def test_preferred_account_migrates_healthy_resident(
        self, dispatcher, pool, tmp_path, monkeypatch,
    ):
        """An explicit switch overrides sticky resume affinity on the next turn."""

        monkeypatch.setenv("HOME", str(tmp_path))
        old_jsonl = _seed_session(tmp_path / "claude-1", "sess-preferred")
        assert pool.set_preferred("acc-2")
        dispatcher._claude_task_binding = AsyncMock(return_value=None)
        async def persist_route(*, config_dir, **_kwargs):
            pool.record_routed_account(config_dir)
            return True

        dispatcher._persist_claude_binding_for_route = AsyncMock(
            side_effect=persist_route
        )

        result = await dispatcher._resolve_resume_config_dir(
            "sess-preferred",
            task_id=42,
        )

        assert result == str(tmp_path / "claude-2")
        new_jsonl = (
            tmp_path
            / "claude-2"
            / "projects"
            / "-home-user-repo"
            / "sess-preferred.jsonl"
        )
        assert new_jsonl.stat().st_ino == old_jsonl.stat().st_ino
        assert pool.status()["last_selected"] == "acc-2"

    @pytest.mark.asyncio
    async def test_preferred_migration_failure_keeps_intact_resident(
        self, dispatcher, pool, tmp_path, monkeypatch,
    ):
        """A failed manual switch must not launch without native context."""

        monkeypatch.setenv("HOME", str(tmp_path))
        _seed_session(tmp_path / "claude-1", "sess-preferred-fail")
        assert pool.set_preferred("acc-2")
        dispatcher._claude_task_binding = AsyncMock(return_value=None)
        async def persist_route(*, config_dir, **_kwargs):
            pool.record_routed_account(config_dir)
            return True

        dispatcher._persist_claude_binding_for_route = AsyncMock(
            side_effect=persist_route
        )
        monkeypatch.setattr(
            "backend.services.claude_pool.migrate_session",
            lambda **_kwargs: False,
        )

        result = await dispatcher._resolve_resume_config_dir(
            "sess-preferred-fail",
            task_id=42,
        )

        assert result == str(tmp_path / "claude-1")
        assert pool.status()["last_selected"] == "acc-1"
        assert not (tmp_path / "claude-2" / "projects").exists()

    @pytest.mark.asyncio
    async def test_disabled_resident_migrates_off(self, dispatcher, pool, tmp_path, monkeypatch):
        """enabled=false is a hard guarantee: a session sitting on a disabled
        account is migrated off it on resume, never reused — even though the
        account is healthy (no cooldown) and still holds the JSONL."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setattr(pool, "_probe_account", lambda acc: True)
        # Retire acc-1 (where the session lives); acc-2 stays enabled.
        for a in pool._accounts:
            if a.id == "acc-1":
                a.enabled = False
        _seed_session(tmp_path / "claude-1", "sess-dis")

        result = await dispatcher._resolve_resume_config_dir("sess-dis")

        # Must NOT reuse the disabled resident — migrated to the enabled account.
        assert result == str(tmp_path / "claude-2")
        assert (tmp_path / "claude-2" / "projects" / "-home-user-repo" / "sess-dis.jsonl").exists()

    @pytest.mark.asyncio
    async def test_pool_disabled_returns_none(self, tmp_path, monkeypatch):
        """No pool → use the inherited/default account (return None)."""
        monkeypatch.setenv("HOME", str(tmp_path))
        disp = GlobalDispatcher(
            db_factory=MagicMock(), instance_manager=MagicMock(), broadcaster=MagicMock()
        )
        disp.pool = None
        assert await disp._resolve_resume_config_dir("sess-x") is None

    @pytest.mark.asyncio
    async def test_api_only_fresh_task_with_unsupported_model_fails_closed(
        self, dispatcher, pool, tmp_path
    ):
        api = MagicMock()
        api.supports_model.side_effect = (
            lambda provider, model: model == "claude-sonnet-5"
        )
        pool._accounts = [PoolAccount({
            "id": "cloudrouter-1",
            "config_dir": str(tmp_path / "api" / "claude"),
            "enabled": True,
            "auth_kind": "cloudrouter_api",
            "_api_account": api,
        })]

        with pytest.raises(RuntimeError, match="no enabled account supports"):
            await dispatcher._resolve_resume_config_dir(
                None,
                model="claude-opus-4-8",
            )

    @pytest.mark.asyncio
    async def test_api_only_compatible_but_cooled_fresh_task_fails_closed(
        self, dispatcher, pool, tmp_path
    ):
        api = MagicMock()
        api.supports_model.return_value = True
        pool._accounts = [PoolAccount({
            "id": "cloudrouter-1",
            "config_dir": str(tmp_path / "api" / "claude"),
            "enabled": True,
            "auth_kind": "cloudrouter_api",
            "_api_account": api,
        })]
        pool._cooldowns["cloudrouter-1"] = time.time() + 999

        with pytest.raises(RuntimeError, match="currently unavailable"):
            await dispatcher._resolve_resume_config_dir(
                None,
                model="claude-sonnet-5",
            )

    @pytest.mark.asyncio
    async def test_api_only_known_exhaustion_is_visible_permanent_failure(
        self, dispatcher, pool, tmp_path
    ):
        api = MagicMock()
        api.supports_model.return_value = True
        pool._accounts = [PoolAccount({
            "id": "cloudrouter-1",
            "config_dir": str(tmp_path / "api" / "claude"),
            "enabled": True,
            "auth_kind": "cloudrouter_api",
            "api_account_id": "cloudrouter-1",
            "_api_account": api,
        })]
        pool._cloudrouter_store = MagicMock()
        pool._cloudrouter_store.cached_quota_decision.return_value = {
            "available": False,
            "known": True,
            "reason": "quota_exhausted",
        }

        with pytest.raises(ClaudeAccountRoutingError) as caught:
            await dispatcher._resolve_resume_config_dir(
                None,
                model="claude-sonnet-5",
            )

        assert caught.value.permanent is True
        assert caught.value.retry_after is None

    @pytest.mark.asyncio
    async def test_model_switch_migrates_off_incompatible_api_resident(
        self, dispatcher, pool, tmp_path, monkeypatch
    ):
        monkeypatch.setenv("HOME", str(tmp_path))
        api_dir = tmp_path / "api" / "claude"
        native_dir = tmp_path / "native"
        api = MagicMock()
        api.supports_model.side_effect = (
            lambda provider, model: model == "claude-sonnet-5"
        )
        pool._accounts = [
            PoolAccount({
                "id": "cloudrouter-1",
                "config_dir": str(api_dir),
                "enabled": True,
                "auth_kind": "cloudrouter_api",
                "_api_account": api,
            }),
            PoolAccount({
                "id": "native",
                "config_dir": str(native_dir),
                "enabled": True,
            }),
        ]
        old = _seed_session(api_dir, "model-switch")

        result = await dispatcher._resolve_resume_config_dir(
            "model-switch",
            model="claude-opus-4-8",
        )

        assert result == str(native_dir)
        copied = (
            native_dir
            / "projects"
            / "-home-user-repo"
            / "model-switch.jsonl"
        )
        assert copied.stat().st_ino == old.stat().st_ino

    @pytest.mark.asyncio
    async def test_incompatible_api_resident_without_model_fallback_is_permanent(
        self, dispatcher, pool, tmp_path, monkeypatch
    ):
        monkeypatch.setenv("HOME", str(tmp_path))
        api_dir = tmp_path / "api" / "claude"
        api = MagicMock()
        api.supports_model.side_effect = (
            lambda provider, model: model == "claude-sonnet-5"
        )
        pool._accounts = [PoolAccount({
            "id": "cloudrouter-1",
            "config_dir": str(api_dir),
            "enabled": True,
            "auth_kind": "cloudrouter_api",
            "_api_account": api,
        })]
        _seed_session(api_dir, "unsupported-resume")

        with pytest.raises(ClaudeAccountRoutingError) as caught:
            await dispatcher._resolve_resume_config_dir(
                "unsupported-resume",
                model="claude-opus-4-8",
            )

        assert caught.value.permanent is True
        assert caught.value.retry_after is None

    @pytest.mark.asyncio
    async def test_cooled_api_resident_is_retryable_without_losing_message(
        self, dispatcher, pool, tmp_path, monkeypatch
    ):
        monkeypatch.setenv("HOME", str(tmp_path))
        api_dir = tmp_path / "api" / "claude"
        api = MagicMock()
        api.supports_model.return_value = True
        pool._accounts = [PoolAccount({
            "id": "cloudrouter-1",
            "config_dir": str(api_dir),
            "enabled": True,
            "auth_kind": "cloudrouter_api",
            "_api_account": api,
        })]
        pool._cooldowns["cloudrouter-1"] = time.time() + 999
        _seed_session(api_dir, "cooled-resume")

        with pytest.raises(ClaudeAccountRoutingError) as caught:
            await dispatcher._resolve_resume_config_dir(
                "cooled-resume",
                model="claude-sonnet-5",
            )

        assert caught.value.permanent is False
        assert caught.value.retry_after is not None


def _codex_db_factory(task):
    db = MagicMock()
    db.get = AsyncMock(return_value=task)
    locked = MagicMock()
    locked.scalar_one_or_none.return_value = task
    db.execute = AsyncMock(return_value=locked)
    db.commit = AsyncMock()
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=db)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return MagicMock(return_value=ctx)


def _codex_rollout(home: Path, session_id: str, text: str = '{"type":"session_meta"}\n') -> Path:
    path = home / "sessions" / "2026" / "07" / "21" / f"rollout-now-{session_id}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


class TestResolveResumeConfigDirClaudeBinding:
    @staticmethod
    def _dispatcher(pool: ClaudePool, task):
        disp = GlobalDispatcher(
            db_factory=_codex_db_factory(task),
            instance_manager=MagicMock(),
            broadcaster=MagicMock(),
        )
        disp.pool = pool
        return disp

    @pytest.mark.asyncio
    async def test_manual_switch_persists_owner_across_auto_restore_and_restart(
        self,
        pool_config,
        tmp_path,
        monkeypatch,
    ):
        monkeypatch.setenv("HOME", str(tmp_path))
        task = MagicMock(id=42, metadata_={})
        first_pool = ClaudePool(config_path=pool_config, cooldown_seconds=60)
        first = self._dispatcher(first_pool, task)
        _seed_session(tmp_path / "claude-1", "claude-bound")
        assert first_pool.set_preferred("acc-2")

        switched = await first._resolve_resume_config_dir(
            "claude-bound",
            task_id=42,
        )

        assert switched == str(tmp_path / "claude-2")
        assert task.metadata_["claude_account_id"] == "acc-2"

        # Simulate both "恢复自动" and a service restart. The two JSONLs are
        # hardlinks, so filesystem order alone would otherwise jump back to
        # acc-1; the durable Task owner must keep the chat on acc-2.
        assert first_pool.set_preferred(None)
        restarted_pool = ClaudePool(
            config_path=pool_config,
            cooldown_seconds=60,
        )
        restarted = self._dispatcher(restarted_pool, task)

        resumed = await restarted._resolve_resume_config_dir(
            "claude-bound",
            task_id=42,
        )

        assert resumed == str(tmp_path / "claude-2")
        assert restarted_pool.status()["last_selected"] == "acc-2"

    @pytest.mark.asyncio
    async def test_bound_hot_session_without_flushed_jsonl_stays_on_owner(
        self,
        pool_config,
        tmp_path,
        monkeypatch,
    ):
        monkeypatch.setenv("HOME", str(tmp_path))
        task = MagicMock(
            id=42,
            metadata_={"claude_account_id": "acc-2"},
        )
        pool = ClaudePool(config_path=pool_config, cooldown_seconds=60)
        disp = self._dispatcher(pool, task)

        result = await disp._resolve_resume_config_dir(
            "claude-not-flushed",
            task_id=42,
        )

        assert result == str(tmp_path / "claude-2")
        assert task.metadata_["claude_account_id"] == "acc-2"
        assert pool.status()["last_selected"] == "acc-2"

    @pytest.mark.asyncio
    async def test_unbound_divergent_session_copies_fail_closed(
        self,
        pool,
        tmp_path,
        monkeypatch,
    ):
        monkeypatch.setenv("HOME", str(tmp_path))
        task = MagicMock(id=42, metadata_={})
        disp = self._dispatcher(pool, task)
        _seed_session(tmp_path / "claude-1", "claude-diverged").write_text(
            "old"
        )
        _seed_session(tmp_path / "claude-2", "claude-diverged").write_text(
            "new"
        )

        with pytest.raises(
            ClaudeAccountRoutingError,
            match="multiple copies",
        ):
            await disp._resolve_resume_config_dir(
                "claude-diverged",
                task_id=42,
            )
        assert task.metadata_ == {}

    @pytest.mark.asyncio
    async def test_unbound_hardlinks_bind_to_complete_sidecar_superset(
        self,
        pool,
        tmp_path,
        monkeypatch,
    ):
        monkeypatch.setenv("HOME", str(tmp_path))
        task = MagicMock(id=42, metadata_={})
        disp = self._dispatcher(pool, task)
        first = _seed_session(tmp_path / "claude-1", "claude-sidecar-owner")
        second_project = tmp_path / "claude-2" / "projects" / first.parent.name
        second_project.mkdir(parents=True)
        (second_project / first.name).hardlink_to(first)
        sidecar = (
            second_project
            / "claude-sidecar-owner"
            / "tool-results"
            / "latest.txt"
        )
        sidecar.parent.mkdir(parents=True)
        sidecar.write_text("complete")

        result = await disp._resolve_resume_config_dir(
            "claude-sidecar-owner",
            task_id=42,
        )

        assert result == str(tmp_path / "claude-2")
        assert task.metadata_["claude_account_id"] == "acc-2"

    @pytest.mark.asyncio
    async def test_explicit_preference_disambiguates_divergent_claude_copies(
        self,
        pool,
        tmp_path,
        monkeypatch,
    ):
        monkeypatch.setenv("HOME", str(tmp_path))
        task = MagicMock(id=42, metadata_={})
        disp = self._dispatcher(pool, task)
        _seed_session(tmp_path / "claude-1", "claude-pick-copy").write_text(
            "old"
        )
        preferred = _seed_session(
            tmp_path / "claude-2",
            "claude-pick-copy",
        )
        preferred.write_text("chosen")
        assert pool.set_preferred("acc-2")

        result = await disp._resolve_resume_config_dir(
            "claude-pick-copy",
            task_id=42,
        )

        assert result == str(tmp_path / "claude-2")
        assert preferred.read_text() == "chosen"
        assert task.metadata_["claude_account_id"] == "acc-2"

    @pytest.mark.asyncio
    async def test_binding_database_failure_is_retryable_before_launch(
        self,
        pool,
    ):
        task = MagicMock(id=42, metadata_={})
        disp = self._dispatcher(pool, task)
        disp._set_claude_task_binding = AsyncMock(
            side_effect=OSError("database unavailable")
        )

        with pytest.raises(ClaudeAccountRoutingError) as caught:
            await disp._resolve_resume_config_dir(None, task_id=42)

        assert caught.value.retry_after is not None


class TestResolveResumeConfigDirCodex:
    def _dispatcher(self, tmp_path: Path, task):
        config_path = tmp_path / "codex-accounts.json"
        config_path.write_text(json.dumps({"accounts": [
            {"id": "codex-1", "codex_home": str(tmp_path / "codex-1"), "enabled": True},
            {"id": "codex-2", "codex_home": str(tmp_path / "codex-2"), "enabled": True},
        ]}))
        manager = MagicMock()
        manager.rebind_codex_thread = AsyncMock()
        manager.clear_codex_thread_owner_for_recovery = AsyncMock(
            return_value=True
        )
        disp = GlobalDispatcher(
            db_factory=_codex_db_factory(task),
            instance_manager=manager,
            broadcaster=MagicMock(),
        )
        disp.codex_pool = CodexPool(config_path=config_path, cooldown_seconds=60)
        return disp

    @pytest.mark.asyncio
    async def test_fresh_task_selects_and_persists_account_binding(self, tmp_path):
        task = MagicMock(id=42, metadata_={})
        disp = self._dispatcher(tmp_path, task)

        result = await disp._resolve_resume_config_dir(None, "codex", task_id=42)

        assert result == str((tmp_path / "codex-1").resolve())
        assert task.metadata_["codex_account_id"] == "codex-1"

    @pytest.mark.asyncio
    async def test_cold_fresh_route_refreshes_rollouts_before_selecting(
        self, tmp_path,
    ):
        task = MagicMock(id=42, metadata_={})
        disp = self._dispatcher(tmp_path, task)
        terminal_path = _codex_rollout(
            tmp_path / "codex-1",
            "terminal",
            json.dumps({
                "timestamp": "2026-08-18T19:40:04.821Z",
                "type": "event_msg",
                "payload": {
                    "type": "task_complete",
                    "error": {
                        "message": "try again at Aug 20th, 2026 7:15 AM",
                        "codex_error_info": "usage_limit_exceeded",
                    },
                },
            }) + "\n",
        )
        assert terminal_path.exists()
        _codex_rollout(
            tmp_path / "codex-2",
            "healthy",
            json.dumps({
                "timestamp": "2026-08-18T19:41:00Z",
                "payload": {
                    "type": "token_count",
                    "rate_limits": {
                        "primary": {"used_percent": 10},
                    },
                },
            }) + "\n",
        )

        result = await disp._resolve_codex_home(
            None,
            task_id=42,
            model="gpt-5.6-sol",
        )

        assert result == str((tmp_path / "codex-2").resolve())
        assert task.metadata_["codex_account_id"] == "codex-2"

    @pytest.mark.asyncio
    async def test_fresh_task_skips_a_runtime_blocked_account(self, tmp_path):
        task = MagicMock(id=42, metadata_={})
        disp = self._dispatcher(tmp_path, task)
        disp.instance_manager.busy_codex_homes.return_value = {
            str((tmp_path / "codex-1").resolve())
        }

        result = await disp._resolve_resume_config_dir(None, "codex", task_id=42)

        assert result == str((tmp_path / "codex-2").resolve())
        assert task.metadata_["codex_account_id"] == "codex-2"

    @pytest.mark.asyncio
    async def test_all_runtime_blocked_accounts_use_short_retry(self, tmp_path):
        task = MagicMock(id=42, metadata_={})
        disp = self._dispatcher(tmp_path, task)
        disp.instance_manager.busy_codex_homes.return_value = {
            str((tmp_path / "codex-1").resolve()),
            str((tmp_path / "codex-2").resolve()),
        }

        with pytest.raises(CodexAccountRoutingError) as caught:
            await disp._resolve_resume_config_dir(None, "codex", task_id=42)

        assert caught.value.permanent is False
        assert caught.value.retry_after == CODEX_ROUTING_RETRY_DELAY
        assert task.metadata_ == {}

    @pytest.mark.asyncio
    async def test_busy_resident_ignores_unrelated_long_cooldown(self, tmp_path):
        task = MagicMock(id=42, metadata_={"codex_account_id": "codex-1"})
        disp = self._dispatcher(tmp_path, task)
        source = tmp_path / "codex-1"
        _codex_rollout(source, "thread-busy-cooldown")
        disp.instance_manager.busy_codex_homes.return_value = {
            str(source.resolve())
        }
        disp.codex_pool.mark_rate_limited(
            str(tmp_path / "codex-2"),
            duration=3600,
        )

        with pytest.raises(CodexAccountRoutingError) as caught:
            await disp._resolve_resume_config_dir(
                "thread-busy-cooldown",
                "codex",
                task_id=42,
            )

        assert caught.value.permanent is False
        assert caught.value.retry_after == CODEX_ROUTING_RETRY_DELAY
        assert task.metadata_["codex_account_id"] == "codex-1"

    @pytest.mark.asyncio
    async def test_disabled_resident_waits_for_busy_migration_target(
        self,
        tmp_path,
    ):
        task = MagicMock(id=42, metadata_={"codex_account_id": "codex-1"})
        disp = self._dispatcher(tmp_path, task)
        source = tmp_path / "codex-1"
        target = tmp_path / "codex-2"
        _codex_rollout(source, "thread-disabled-source")
        disp.codex_pool.account("codex-1").enabled = False
        disp.instance_manager.busy_codex_homes.return_value = {
            str(target.resolve())
        }

        with pytest.raises(CodexAccountRoutingError) as caught:
            await disp._resolve_resume_config_dir(
                "thread-disabled-source",
                "codex",
                task_id=42,
            )

        assert caught.value.permanent is False
        assert caught.value.retry_after == CODEX_ROUTING_RETRY_DELAY
        assert task.metadata_["codex_account_id"] == "codex-1"

    @pytest.mark.asyncio
    async def test_fresh_task_pool_exhaustion_never_falls_back_to_default_home(
        self, tmp_path,
    ):
        task = MagicMock(id=42, metadata_={})
        disp = self._dispatcher(tmp_path, task)
        future = time.time() + 999
        disp.codex_pool._cooldowns = {"codex-1": future, "codex-2": future}

        with pytest.raises(RuntimeError, match="no available account"):
            await disp._resolve_resume_config_dir(None, "codex", task_id=42)

    @pytest.mark.asyncio
    async def test_api_only_unsupported_model_fails_without_downgrade(
        self, tmp_path
    ):
        task = MagicMock(id=42, metadata_={})
        disp = self._dispatcher(tmp_path, task)
        api = MagicMock()
        api.supports_model.side_effect = (
            lambda provider, model: model == "gpt-5.5"
        )
        disp.codex_pool._accounts = [CodexPoolAccount({
            "id": "cloudrouter-1",
            "codex_home": str(tmp_path / "api" / "codex"),
            "enabled": True,
            "auth_kind": "cloudrouter_api",
            "_api_account": api,
        })]

        with pytest.raises(
            CodexAccountRoutingError,
            match="no enabled account supporting model",
        ):
            await disp._resolve_resume_config_dir(
                None,
                "codex",
                task_id=42,
                model="gpt-5.6-sol",
            )

    @pytest.mark.asyncio
    async def test_api_only_compatible_but_cooled_never_uses_default_home(
        self, tmp_path
    ):
        task = MagicMock(id=42, metadata_={})
        disp = self._dispatcher(tmp_path, task)
        api = MagicMock()
        api.supports_model.return_value = True
        disp.codex_pool._accounts = [CodexPoolAccount({
            "id": "cloudrouter-1",
            "codex_home": str(tmp_path / "api" / "codex"),
            "enabled": True,
            "auth_kind": "cloudrouter_api",
            "_api_account": api,
        })]
        disp.codex_pool._cooldowns["cloudrouter-1"] = time.time() + 999

        with pytest.raises(CodexAccountRoutingError, match="no available account"):
            await disp._resolve_resume_config_dir(
                None,
                "codex",
                task_id=42,
                model="gpt-5.5",
            )

    @pytest.mark.asyncio
    async def test_api_only_known_exhaustion_is_permanent_for_codex(
        self, tmp_path
    ):
        task = MagicMock(id=42, metadata_={})
        disp = self._dispatcher(tmp_path, task)
        api = MagicMock()
        api.supports_model.return_value = True
        disp.codex_pool._accounts = [CodexPoolAccount({
            "id": "cloudrouter-1",
            "codex_home": str(tmp_path / "api" / "codex"),
            "enabled": True,
            "auth_kind": "cloudrouter_api",
            "api_account_id": "cloudrouter-1",
            "_api_account": api,
        })]
        disp.codex_pool._cloudrouter_store = MagicMock()
        (
            disp.codex_pool._cloudrouter_store.cached_quota_decision.return_value
        ) = {
            "available": False,
            "known": True,
            "reason": "quota_exhausted",
        }

        with pytest.raises(CodexAccountRoutingError) as caught:
            await disp._resolve_resume_config_dir(
                None,
                "codex",
                task_id=42,
                model="gpt-5.5",
            )

        assert caught.value.permanent is True
        assert caught.value.retry_after is None

    @pytest.mark.asyncio
    async def test_cooldown_migrates_rollout_rebinds_and_updates_owner(self, tmp_path):
        task = MagicMock(id=42, metadata_={"codex_account_id": "codex-1"})
        disp = self._dispatcher(tmp_path, task)
        source = tmp_path / "codex-1"
        target = tmp_path / "codex-2"
        old = _codex_rollout(source, "thread-switch", "one\ntwo\n")
        disp.codex_pool.mark_rate_limited(str(source), duration=999)

        result = await disp._resolve_resume_config_dir(
            "thread-switch", "codex", task_id=42
        )

        assert result == str(target.resolve())
        copied = target / old.relative_to(source)
        assert copied.read_text() == old.read_text()
        assert copied.stat().st_ino != old.stat().st_ino
        assert task.metadata_["codex_account_id"] == "codex-2"
        disp.instance_manager.rebind_codex_thread.assert_awaited_once_with(
            "thread-switch",
            source_codex_home=str(source.resolve()),
            target_codex_home=str(target.resolve()),
        )

    @pytest.mark.asyncio
    async def test_busy_bound_thread_migrates_to_an_idle_account(self, tmp_path):
        """An idle task must not queue behind a non-app-server home owner."""
        task = MagicMock(id=42, metadata_={"codex_account_id": "codex-1"})
        disp = self._dispatcher(tmp_path, task)
        source = tmp_path / "codex-1"
        target = tmp_path / "codex-2"
        _codex_rollout(source, "thread-busy")
        disp.instance_manager.busy_codex_homes.return_value = {
            str(source.resolve())
        }

        result = await disp._resolve_resume_config_dir(
            "thread-busy", "codex", task_id=42
        )

        assert result == str(target.resolve())
        assert task.metadata_["codex_account_id"] == "codex-2"
        disp.instance_manager.rebind_codex_thread.assert_awaited_once_with(
            "thread-busy",
            source_codex_home=str(source.resolve()),
            target_codex_home=str(target.resolve()),
        )

    @pytest.mark.asyncio
    async def test_busy_resident_migration_failure_retries_instead_of_reusing(
        self,
        tmp_path,
        monkeypatch,
    ):
        from backend.services.codex_session_migration import (
            CodexSessionMigrationError,
        )

        task = MagicMock(id=42, metadata_={"codex_account_id": "codex-1"})
        disp = self._dispatcher(tmp_path, task)
        source = tmp_path / "codex-1"
        _codex_rollout(source, "thread-busy-migration-fail")
        disp.instance_manager.busy_codex_homes.return_value = {
            str(source.resolve())
        }

        def fail_migration(*_args, **_kwargs):
            raise CodexSessionMigrationError("disk full")

        monkeypatch.setattr(
            "backend.services.codex_session_migration.migrate_codex_rollout_session",
            fail_migration,
        )

        with pytest.raises(CodexAccountRoutingError) as caught:
            await disp._resolve_resume_config_dir(
                "thread-busy-migration-fail",
                "codex",
                task_id=42,
            )

        assert caught.value.permanent is False
        assert caught.value.retry_after == CODEX_ROUTING_RETRY_DELAY
        assert task.metadata_["codex_account_id"] == "codex-1"

    @pytest.mark.asyncio
    async def test_preferred_account_migrates_healthy_bound_thread(
        self, tmp_path,
    ):
        task = MagicMock(id=42, metadata_={"codex_account_id": "codex-1"})
        disp = self._dispatcher(tmp_path, task)
        source = tmp_path / "codex-1"
        target = tmp_path / "codex-2"
        old = _codex_rollout(source, "thread-preferred", "one\ntwo\n")
        assert disp.codex_pool.set_preferred("codex-2")

        result = await disp._resolve_resume_config_dir(
            "thread-preferred", "codex", task_id=42
        )

        assert result == str(target.resolve())
        copied = target / old.relative_to(source)
        assert copied.read_text() == old.read_text()
        assert task.metadata_["codex_account_id"] == "codex-2"
        assert disp.codex_pool.status()["last_selected"] == "codex-2"
        disp.instance_manager.rebind_codex_thread.assert_awaited_once_with(
            "thread-preferred",
            source_codex_home=str(source.resolve()),
            target_codex_home=str(target.resolve()),
        )

    @pytest.mark.asyncio
    async def test_bound_resident_updates_true_last_used_account(self, tmp_path):
        task = MagicMock(id=42, metadata_={"codex_account_id": "codex-2"})
        disp = self._dispatcher(tmp_path, task)
        _codex_rollout(tmp_path / "codex-2", "thread-last-used")

        result = await disp._resolve_resume_config_dir(
            "thread-last-used", "codex", task_id=42
        )

        assert result == str((tmp_path / "codex-2").resolve())
        assert disp.codex_pool.status()["last_selected"] == "codex-2"
        disp.instance_manager.rebind_codex_thread.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_preferred_migration_failure_keeps_and_binds_resident(
        self, tmp_path, monkeypatch,
    ):
        from backend.services.codex_session_migration import (
            CodexSessionMigrationError,
        )

        task = MagicMock(id=42, metadata_={})
        disp = self._dispatcher(tmp_path, task)
        source = tmp_path / "codex-1"
        _codex_rollout(source, "thread-preferred-fail")
        assert disp.codex_pool.set_preferred("codex-2")

        def fail_migration(*_args, **_kwargs):
            raise CodexSessionMigrationError("disk full")

        monkeypatch.setattr(
            "backend.services.codex_session_migration.migrate_codex_rollout_session",
            fail_migration,
        )

        result = await disp._resolve_resume_config_dir(
            "thread-preferred-fail", "codex", task_id=42
        )

        assert result == str(source.resolve())
        assert task.metadata_["codex_account_id"] == "codex-1"
        assert disp.codex_pool.status()["last_selected"] == "codex-1"
        disp.instance_manager.rebind_codex_thread.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_rebind_rolls_back_when_generation_loses_binding_cas(
        self, tmp_path,
    ):
        task = MagicMock(id=42, metadata_={"codex_account_id": "codex-1"})
        disp = self._dispatcher(tmp_path, task)
        source = str((tmp_path / "codex-1").resolve())
        target = str((tmp_path / "codex-2").resolve())
        _codex_rollout(Path(source), "thread-stale-rebind")
        disp.codex_pool.mark_rate_limited(source, duration=999)
        generation = _TaskStatusGeneration(
            task_id=42,
            worker_id=None,
            shared_from_id=None,
            status="completed",
            retry_count=0,
            turn_generation=0,
            instance_id=None,
            started_at=None,
            completed_at=None,
        )
        disp._set_codex_task_binding = AsyncMock(
            side_effect=[True, False]
        )

        with pytest.raises(TaskLifecycleSupersededError):
            await disp._resolve_resume_config_dir(
                "thread-stale-rebind",
                "codex",
                task_id=42,
                expected_generation=generation,
            )

        assert disp.instance_manager.rebind_codex_thread.await_args_list == [
            call(
                "thread-stale-rebind",
                source_codex_home=source,
                target_codex_home=target,
            ),
            call(
                "thread-stale-rebind",
                source_codex_home=target,
                target_codex_home=source,
            ),
        ]
        disp.instance_manager.clear_codex_thread_owner_for_recovery.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cancel_at_forward_rebind_settles_binding_and_rollback(
        self, tmp_path,
    ):
        task = MagicMock(id=42, metadata_={"codex_account_id": "codex-1"})
        disp = self._dispatcher(tmp_path, task)
        source = str((tmp_path / "codex-1").resolve())
        target = str((tmp_path / "codex-2").resolve())
        _codex_rollout(Path(source), "thread-cancel-rebind")
        disp.codex_pool.mark_rate_limited(source, duration=999)
        generation = _TaskStatusGeneration(
            task_id=42,
            worker_id=None,
            shared_from_id=None,
            status="completed",
            retry_count=0,
            turn_generation=0,
            instance_id=None,
            started_at=None,
            completed_at=None,
        )
        disp._set_codex_task_binding = AsyncMock(
            side_effect=[True, False]
        )
        calls = []
        resolver_task = None

        async def cancel_outer_after_owner_move(*args, **kwargs):
            calls.append(call(*args, **kwargs))
            if len(calls) == 1:
                resolver_task.cancel()

        disp.instance_manager.rebind_codex_thread = AsyncMock(
            side_effect=cancel_outer_after_owner_move
        )
        resolver_task = asyncio.create_task(
            disp._resolve_resume_config_dir(
                "thread-cancel-rebind",
                "codex",
                task_id=42,
                expected_generation=generation,
            )
        )
        with pytest.raises(asyncio.CancelledError):
            await resolver_task

        assert calls == [
            call(
                "thread-cancel-rebind",
                source_codex_home=source,
                target_codex_home=target,
            ),
            call(
                "thread-cancel-rebind",
                source_codex_home=target,
                target_codex_home=source,
            ),
        ]

    @pytest.mark.asyncio
    async def test_cancel_during_rollout_copy_finishes_target_binding(
        self, tmp_path, monkeypatch,
    ):
        from backend.services.codex_session_migration import (
            migrate_codex_rollout_session as real_migrate,
        )

        task = MagicMock(id=42, metadata_={"codex_account_id": "codex-1"})
        disp = self._dispatcher(tmp_path, task)
        source = str((tmp_path / "codex-1").resolve())
        target = str((tmp_path / "codex-2").resolve())
        _codex_rollout(Path(source), "thread-cancel-copy")
        disp.codex_pool.mark_rate_limited(source, duration=999)

        copy_started = asyncio.Event()
        release_copy = threading.Event()
        loop = asyncio.get_running_loop()

        def blocked_migrate(*args, **kwargs):
            loop.call_soon_threadsafe(copy_started.set)
            assert release_copy.wait(timeout=2)
            return real_migrate(*args, **kwargs)

        monkeypatch.setattr(
            "backend.services.codex_session_migration."
            "migrate_codex_rollout_session",
            blocked_migrate,
        )

        resolver = asyncio.create_task(
            disp._resolve_resume_config_dir(
                "thread-cancel-copy",
                "codex",
                task_id=42,
            )
        )
        await asyncio.wait_for(copy_started.wait(), timeout=1)
        resolver.cancel()
        await asyncio.sleep(0)
        assert not resolver.done()
        release_copy.set()

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(resolver, timeout=2)

        assert task.metadata_["codex_account_id"] == "codex-2"
        assert disp.codex_pool.status()["last_selected"] == "codex-2"
        disp.instance_manager.rebind_codex_thread.assert_awaited_once_with(
            "thread-cancel-copy",
            source_codex_home=source,
            target_codex_home=target,
        )

    @pytest.mark.asyncio
    async def test_generation_change_during_copy_keeps_source_binding(
        self, tmp_path, monkeypatch,
    ):
        from backend.services.codex_session_migration import (
            migrate_codex_rollout_session as real_migrate,
        )

        task = MagicMock(id=42, metadata_={})
        disp = self._dispatcher(tmp_path, task)
        source = str((tmp_path / "codex-1").resolve())
        target = str((tmp_path / "codex-2").resolve())
        _codex_rollout(Path(source), "thread-generation-copy")
        disp.codex_pool.mark_rate_limited(source, duration=999)

        copy_started = asyncio.Event()
        release_copy = threading.Event()
        loop = asyncio.get_running_loop()
        superseded = False

        def blocked_migrate(*args, **kwargs):
            loop.call_soon_threadsafe(copy_started.set)
            assert release_copy.wait(timeout=2)
            return real_migrate(*args, **kwargs)

        async def require_current(_generation):
            if superseded:
                raise TaskLifecycleSupersededError("replacement generation")

        monkeypatch.setattr(
            "backend.services.codex_session_migration."
            "migrate_codex_rollout_session",
            blocked_migrate,
        )
        disp._require_task_lifecycle_active = AsyncMock(
            side_effect=require_current
        )

        resolver = asyncio.create_task(
            disp._resolve_resume_config_dir(
                "thread-generation-copy",
                "codex",
                task_id=42,
            )
        )
        await asyncio.wait_for(copy_started.wait(), timeout=1)
        superseded = True
        release_copy.set()

        with pytest.raises(
            TaskLifecycleSupersededError,
            match="replacement generation",
        ):
            await asyncio.wait_for(resolver, timeout=2)

        assert task.metadata_["codex_account_id"] == "codex-1"
        assert disp.codex_pool.status()["last_selected"] is None
        disp.instance_manager.rebind_codex_thread.assert_not_awaited()
        assert disp.codex_pool.locate_session_homes(
            "thread-generation-copy"
        ) == [source, target]

    @pytest.mark.asyncio
    async def test_real_usage_limit_rotation_preserves_rollout_and_binding(self, tmp_path):
        """Exercise the real detector -> cooldown -> copy -> rebind path.

        Mode/chat unit tests mock the rotation helper.  This integration anchor
        proves a production usage-limit message moves the native Codex thread
        between two isolated homes without aliasing or losing task ownership.
        """
        task = MagicMock(
            id=42,
            session_id="thread-real-rotation",
            metadata_={"codex_account_id": "codex-1"},
        )
        disp = self._dispatcher(tmp_path, task)
        source = tmp_path / "codex-1"
        target = tmp_path / "codex-2"
        old = _codex_rollout(
            source,
            task.session_id,
            '{"type":"session_meta"}\n{"type":"response_item"}\n',
        )
        disp.instance_manager.get_config_dir.return_value = str(source)
        disp.broadcaster = MagicMock()
        disp.broadcaster.broadcast = AsyncMock()

        result = await disp._check_codex_rate_limit_and_rotate(
            instance_id=7,
            task_id=task.id,
            combined="You've hit your usage limit. Try again later.",
        )

        assert result == {
            "config_dir": str(target.resolve()),
            "session_id": task.session_id,
            "excluded": {"codex-1"},
        }
        assert not disp.codex_pool.is_home_available(source)
        copied = target / old.relative_to(source)
        assert copied.read_text() == old.read_text()
        assert copied.stat().st_ino != old.stat().st_ino
        assert task.metadata_["codex_account_id"] == "codex-2"
        disp.instance_manager.rebind_codex_thread.assert_awaited_once_with(
            task.session_id,
            source_codex_home=str(source.resolve()),
            target_codex_home=str(target.resolve()),
        )

    @pytest.mark.asyncio
    async def test_usage_limit_with_no_alternative_is_retryable_backpressure(self, tmp_path):
        task = MagicMock(
            id=42,
            session_id="thread-no-alternative",
            metadata_={"codex_account_id": "codex-1"},
        )
        disp = self._dispatcher(tmp_path, task)
        source = tmp_path / "codex-1"
        _codex_rollout(source, task.session_id)
        disp.instance_manager.get_config_dir.return_value = str(source)
        disp.codex_pool.mark_rate_limited(str(tmp_path / "codex-2"), duration=999)

        with pytest.raises(CodexAccountRoutingError) as exc_info:
            await disp._check_codex_rate_limit_and_rotate(
                instance_id=7,
                task_id=task.id,
                combined="You've hit your usage limit. Try again later.",
            )

        assert exc_info.value.retry_after is not None
        assert exc_info.value.retry_after > 0
        assert task.metadata_["codex_account_id"] == "codex-1"
        disp.instance_manager.rebind_codex_thread.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_binding_disambiguates_retained_source_copy(self, tmp_path):
        task = MagicMock(id=42, metadata_={"codex_account_id": "codex-2"})
        disp = self._dispatcher(tmp_path, task)
        _codex_rollout(tmp_path / "codex-1", "thread-copied")
        _codex_rollout(tmp_path / "codex-2", "thread-copied")

        result = await disp._resolve_resume_config_dir(
            "thread-copied", "codex", task_id=42
        )

        assert result == str((tmp_path / "codex-2").resolve())
        disp.instance_manager.rebind_codex_thread.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stale_binding_is_repaired_from_unique_physical_rollout(self, tmp_path):
        task = MagicMock(id=42, metadata_={"codex_account_id": "codex-2"})
        disp = self._dispatcher(tmp_path, task)
        _codex_rollout(tmp_path / "codex-1", "thread-stale-binding")

        result = await disp._resolve_resume_config_dir(
            "thread-stale-binding", "codex", task_id=42
        )

        assert result == str((tmp_path / "codex-1").resolve())
        assert task.metadata_["codex_account_id"] == "codex-1"

    @pytest.mark.asyncio
    async def test_failed_migration_never_returns_unavailable_resident(
        self, tmp_path, monkeypatch,
    ):
        from backend.services.codex_session_migration import CodexSessionMigrationError

        task = MagicMock(id=42, metadata_={"codex_account_id": "codex-1"})
        disp = self._dispatcher(tmp_path, task)
        source = tmp_path / "codex-1"
        _codex_rollout(source, "thread-migrate-fail")
        disp.codex_pool.mark_rate_limited(str(source), duration=999)

        def fail_migration(*_args, **_kwargs):
            raise CodexSessionMigrationError("disk full")

        monkeypatch.setattr(
            "backend.services.codex_session_migration.migrate_codex_rollout_session",
            fail_migration,
        )

        with pytest.raises(RuntimeError, match="could not be migrated"):
            await disp._resolve_resume_config_dir(
                "thread-migrate-fail", "codex", task_id=42
            )

    @pytest.mark.asyncio
    async def test_unbound_multiple_copies_fail_instead_of_guessing(self, tmp_path):
        task = MagicMock(id=42, metadata_={})
        disp = self._dispatcher(tmp_path, task)
        _codex_rollout(tmp_path / "codex-1", "thread-ambiguous")
        _codex_rollout(tmp_path / "codex-2", "thread-ambiguous")

        with pytest.raises(RuntimeError, match="multiple account homes"):
            await disp._resolve_resume_config_dir(
                "thread-ambiguous", "codex", task_id=42
            )

    @pytest.mark.asyncio
    async def test_explicit_preference_disambiguates_codex_copies(self, tmp_path):
        task = MagicMock(id=42, metadata_={})
        disp = self._dispatcher(tmp_path, task)
        _codex_rollout(tmp_path / "codex-1", "thread-explicit-copy", "old\n")
        _codex_rollout(
            tmp_path / "codex-2",
            "thread-explicit-copy",
            "chosen\n",
        )
        assert disp.codex_pool.set_preferred("codex-2")

        result = await disp._resolve_resume_config_dir(
            "thread-explicit-copy",
            "codex",
            task_id=42,
        )

        assert result == str((tmp_path / "codex-2").resolve())
        assert task.metadata_["codex_account_id"] == "codex-2"
        disp.instance_manager.rebind_codex_thread.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_claude_provider_unaffected(self, dispatcher, pool, tmp_path):
        _seed_session(tmp_path / "claude-1", "sess-claude")
        result = await dispatcher._resolve_resume_config_dir("sess-claude", "claude")
        assert result == str(tmp_path / "claude-1")


def test_busy_codex_homes_includes_non_app_server_runtime_blockers(tmp_path):
    manager = object.__new__(InstanceManager)
    exec_home = str(tmp_path / "exec")
    ephemeral_home = str(tmp_path / "ephemeral")
    maintenance_home = str(tmp_path / "maintenance")
    idle_ephemeral_home = str(tmp_path / "idle-ephemeral")
    manager._codex_exec_homes = {1: exec_home}
    manager._codex_ephemeral_home_users = {
        ephemeral_home: 1,
        idle_ephemeral_home: 0,
    }
    manager._codex_home_maintenance = {maintenance_home}

    assert manager.busy_codex_homes() == {
        str((tmp_path / "exec").resolve()),
        str((tmp_path / "ephemeral").resolve()),
        str((tmp_path / "maintenance").resolve()),
    }
