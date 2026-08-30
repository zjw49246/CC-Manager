"""Worker-node admission tests for the legacy Claude account-pool API."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from backend.api import pool as pool_api
from backend.config import settings
from backend.database import Base
from backend.models.worker import (
    WORKER_NODE_CONTROL_SINGLETON_ID,
    WorkerNodeControl,
)
from backend.services.worker_node_control import (
    admit_worker_node_login_attempt,
    begin_worker_node_drain,
    recover_worker_node_login_after_restart,
)


pytestmark = pytest.mark.asyncio


def _admin_request():
    return SimpleNamespace(
        state=SimpleNamespace(user_role="admin", auth_type="token")
    )


class _FakeClaudePool:
    def __init__(self, *, accounts: list | None = None) -> None:
        self._accounts = accounts or []
        self._usage_cache = object()
        self._preferred_account_id = None
        self.reload = MagicMock()
        self.clear_cooldown = MagicMock()
        self.refresh_oauth_token = AsyncMock(return_value=False)
        self.fetch_usage = AsyncMock(return_value=[])

    def account(self, account_id: str):
        return next(
            (account for account in self._accounts if account.id == account_id),
            None,
        )

    def status(self) -> dict:
        return {"enabled": True, "accounts": []}

    def set_preferred(self, account_id: str | None) -> bool:
        if account_id is not None and self.account(account_id) is None:
            return False
        self._preferred_account_id = account_id
        return True

    @property
    def preferred_account_id(self) -> str | None:
        return self._preferred_account_id


class _ControlledProcess:
    def __init__(self, *, pid: int = 424242) -> None:
        self.pid = pid
        self.returncode = None
        self.release = asyncio.Event()

    async def communicate(self):
        await self.release.wait()
        if self.returncode is None:
            self.returncode = 0
        return b"login complete", b""

    async def wait(self):
        await self.release.wait()
        return self.returncode

    def kill(self) -> None:
        self.returncode = -9
        self.release.set()


def _native_account(tmp_path):
    return SimpleNamespace(
        id="account-1",
        email="worker-claude@example.com",
        config_dir=str(tmp_path / ".claude"),
        auth_kind="oauth",
        enabled=True,
    )


async def _wait_for_login_watchers() -> None:
    deadline = asyncio.get_running_loop().time() + 2
    while pool_api._login_watch_tasks:
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("Claude login watcher did not finish")
        await asyncio.sleep(0.01)


async def test_manager_claude_pool_refresh_keeps_existing_behavior(
    session_factory,
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(settings, "ccm_node_role", "manager")
    account = _native_account(tmp_path)
    pool = _FakeClaudePool(accounts=[account])
    pool.refresh_oauth_token.return_value = True
    monkeypatch.setattr(pool_api, "_get_pool", lambda: pool)

    async with session_factory() as db:
        result = await pool_api.relogin_account(
            _admin_request(), account.id, db
        )

    assert result == {"ok": True, "method": "refresh", "status": "success"}
    pool.refresh_oauth_token.assert_awaited_once_with(account.id)


async def test_worker_first_cc_settings_write_keeps_node_fence_through_commit(
    session_factory,
    monkeypatch,
):
    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    transaction_states: list[bool] = []
    active_db = None

    def observe_sync(_template: dict) -> int:
        transaction_states.append(active_db.in_transaction())
        return 2

    sync = MagicMock(side_effect=observe_sync)
    monkeypatch.setattr(pool_api, "_sync_cc_settings_to_accounts", sync)
    body = pool_api.CcSettingsBody(
        settings={"model": "claude-opus", "effortLevel": "high"}
    )

    async with session_factory() as db:
        active_db = db
        response = await pool_api.put_cc_settings(
            _admin_request(), body, db
        )

    assert response == {"ok": True, "synced": 2, "settings": body.settings}
    sync.assert_called_once_with(body.settings)
    assert transaction_states == [True]
    async with session_factory() as db:
        saved = await db.get(pool_api.GlobalSettings, 1)
    assert saved is not None
    assert saved.cc_settings_template == (
        '{"model": "claude-opus", "effortLevel": "high"}'
    )


async def test_worker_drain_first_rejects_every_claude_pool_mutation(
    session_factory,
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(pool_api, "_login_lock", asyncio.Lock())
    pool_api._relogin_state.clear()
    pool_api._add_state.clear()
    account = _native_account(tmp_path)
    pool = _FakeClaudePool(accounts=[account])
    monkeypatch.setattr(pool_api, "_get_pool", lambda: pool)
    monkeypatch.setattr(pool_api, "_ensure_xvfb", AsyncMock())
    monkeypatch.setattr(pool_api.shutil, "which", lambda _name: "/usr/bin/chrome")

    async with session_factory() as db:
        await begin_worker_node_drain(db, claim="d" * 64)
        await db.commit()

    calls = (
        lambda db: pool_api.pool_usage(False, db),
        lambda db: pool_api.pool_reload(_admin_request(), db),
        lambda db: pool_api.clear_cooldown(
            _admin_request(), account.id, db
        ),
        lambda db: pool_api.relogin_account(
            _admin_request(), account.id, db
        ),
        lambda db: pool_api.delete_account(
            _admin_request(), account.id, db
        ),
        lambda db: pool_api.set_preferred(
            _admin_request(), {"account_id": account.id}, db
        ),
        lambda db: pool_api.add_account(
            _admin_request(),
            pool_api.AddAccountRequest(
                email="new-worker-claude@example.com",
                token="mailbox-token",
            ),
            db,
        ),
        lambda db: pool_api.put_cc_settings(
            _admin_request(),
            pool_api.CcSettingsBody(settings={"model": "claude"}),
            db,
        ),
    )
    for call in calls:
        async with session_factory() as db:
            with pytest.raises(HTTPException) as rejected:
                await call(db)
        assert rejected.value.status_code == 409

    pool.reload.assert_not_called()
    pool.clear_cooldown.assert_not_called()
    pool.refresh_oauth_token.assert_not_awaited()
    pool.fetch_usage.assert_not_awaited()


async def test_worker_mutation_that_wins_first_holds_drain_until_commit(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'claude-pool-node-fence.db'}"
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    account = _native_account(tmp_path)
    pool = _FakeClaudePool(accounts=[account])
    mutation_started = asyncio.Event()
    release_mutation = asyncio.Event()

    async def refresh(_account_id: str) -> bool:
        mutation_started.set()
        await release_mutation.wait()
        return True

    pool.refresh_oauth_token.side_effect = refresh
    monkeypatch.setattr(pool_api, "_get_pool", lambda: pool)

    async def run_refresh():
        async with session_factory() as db:
            return await pool_api.relogin_account(
                _admin_request(), account.id, db
            )

    async def claim_drain():
        async with session_factory() as db:
            await begin_worker_node_drain(db, claim="e" * 64)
            await db.commit()

    refresh_task = None
    drain_task = None
    try:
        refresh_task = asyncio.create_task(run_refresh())
        await asyncio.wait_for(mutation_started.wait(), timeout=2)
        drain_task = asyncio.create_task(claim_drain())
        await asyncio.sleep(0.05)
        assert not drain_task.done()

        release_mutation.set()
        result = await asyncio.wait_for(refresh_task, timeout=2)
        assert result == {
            "ok": True,
            "method": "refresh",
            "status": "success",
        }
        await asyncio.wait_for(drain_task, timeout=2)

        async with session_factory() as db:
            control = await db.get(
                WorkerNodeControl, WORKER_NODE_CONTROL_SINGLETON_ID
            )
        assert control.drain_claim == "e" * 64
    finally:
        release_mutation.set()
        if refresh_task is not None and not refresh_task.done():
            await refresh_task
        if drain_task is not None and not drain_task.done():
            await drain_task
        await engine.dispose()


@pytest.mark.parametrize("operation", ["add", "relogin"])
async def test_worker_background_claude_login_blocks_drain_until_terminal(
    session_factory,
    monkeypatch,
    tmp_path,
    operation,
):
    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(pool_api, "async_session", session_factory)
    monkeypatch.setattr(pool_api, "_login_lock", asyncio.Lock())
    pool_api._relogin_state.clear()
    pool_api._add_state.clear()
    account = _native_account(tmp_path)
    pool = _FakeClaudePool(accounts=[account] if operation == "relogin" else [])
    monkeypatch.setattr(pool_api, "_get_pool", lambda: pool)
    monkeypatch.setattr(pool_api, "_ensure_xvfb", AsyncMock())
    monkeypatch.setattr(pool_api.shutil, "which", lambda _name: "/usr/bin/chrome")
    process = _ControlledProcess()
    spawn = AsyncMock(return_value=process)
    monkeypatch.setattr(pool_api.asyncio, "create_subprocess_exec", spawn)

    async with session_factory() as db:
        if operation == "add":
            response = await pool_api.add_account(
                _admin_request(),
                pool_api.AddAccountRequest(
                    email="new-worker-claude@example.com",
                    token="mailbox-token",
                ),
                db,
            )
        else:
            response = await pool_api.relogin_account(
                _admin_request(), account.id, db
            )

    attempt_id = response["attempt_id"]
    assert len(attempt_id) == 32
    assert spawn.await_args.kwargs["start_new_session"] is True
    async with session_factory() as db:
        control = await db.get(
            WorkerNodeControl, WORKER_NODE_CONTROL_SINGLETON_ID
        )
        assert control.active_login_attempt_id == attempt_id
        assert control.active_login_kind == pool_api._CLAUDE_NODE_LOGIN_KIND

    async with session_factory() as db:
        with pytest.raises(HTTPException) as mutation_blocked:
            await pool_api.pool_reload(_admin_request(), db)
    assert mutation_blocked.value.status_code == 409
    pool.reload.assert_not_called()

    async with session_factory() as db:
        with pytest.raises(HTTPException) as blocked:
            await begin_worker_node_drain(db, claim="f" * 64)
    assert blocked.value.status_code == 409

    process.release.set()
    await _wait_for_login_watchers()
    assert not pool_api._login_lock.locked()
    async with session_factory() as db:
        control = await db.get(
            WorkerNodeControl, WORKER_NODE_CONTROL_SINGLETON_ID
        )
        assert control.active_login_attempt_id is None
        assert control.active_login_kind is None
        await begin_worker_node_drain(db, claim="f" * 64)
        await db.commit()


async def test_worker_claude_login_spawn_failure_clears_exact_journal(
    session_factory,
    monkeypatch,
):
    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(pool_api, "async_session", session_factory)
    monkeypatch.setattr(pool_api, "_login_lock", asyncio.Lock())
    pool_api._add_state.clear()
    pool = _FakeClaudePool()
    monkeypatch.setattr(pool_api, "_get_pool", lambda: pool)
    monkeypatch.setattr(pool_api, "_ensure_xvfb", AsyncMock())
    monkeypatch.setattr(
        pool_api.asyncio,
        "create_subprocess_exec",
        AsyncMock(side_effect=OSError("spawn failed")),
    )

    async with session_factory() as db:
        with pytest.raises(OSError, match="spawn failed"):
            await pool_api.add_account(
                _admin_request(),
                pool_api.AddAccountRequest(
                    email="spawn-failure@example.com",
                    token="mailbox-token",
                ),
                db,
            )

    assert not pool_api._login_lock.locked()
    async with session_factory() as db:
        control = await db.get(
            WorkerNodeControl, WORKER_NODE_CONTROL_SINGLETON_ID
        )
    assert control.active_login_attempt_id is None
    assert control.active_login_kind is None


async def test_claude_login_reap_does_not_spin_under_anyio_cancellation(
    monkeypatch,
):
    from anyio import CancelScope

    process = _ControlledProcess(pid=424243)
    wait_started = asyncio.Event()
    allow_exit = asyncio.Event()
    wait_calls = 0
    real_wait = asyncio.wait

    async def delayed_wait():
        wait_started.set()
        await allow_exit.wait()
        process.returncode = -9
        return -9

    async def counting_wait(*args, **kwargs):
        nonlocal wait_calls
        if kwargs.get("timeout") is not None:
            wait_calls += 1
        return await real_wait(*args, **kwargs)

    process.wait = AsyncMock(side_effect=delayed_wait)
    monkeypatch.setattr(pool_api.os, "killpg", MagicMock())
    monkeypatch.setattr(pool_api.asyncio, "wait", counting_wait)

    async def release_process():
        await wait_started.wait()
        await asyncio.sleep(0)
        allow_exit.set()

    releaser = asyncio.create_task(release_process())
    try:
        with CancelScope() as scope:
            scope.cancel()
            await pool_api._stop_unfinished_claude_login_process(
                process,
                operation="AnyIO cancellation regression",
            )
        await releaser
    finally:
        if not releaser.done():
            releaser.cancel()
            await asyncio.gather(releaser, return_exceptions=True)

    assert process.returncode == -9
    assert wait_calls == 1


@pytest.mark.parametrize("safe_reap", [True, False])
async def test_worker_claude_watcher_cancellation_reaps_or_stays_fail_closed(
    session_factory,
    monkeypatch,
    safe_reap,
):
    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(pool_api, "async_session", session_factory)
    monkeypatch.setattr(pool_api, "_login_lock", asyncio.Lock())
    pool_api._add_state.clear()
    pool = _FakeClaudePool()
    monkeypatch.setattr(pool_api, "_get_pool", lambda: pool)
    monkeypatch.setattr(pool_api, "_ensure_xvfb", AsyncMock())
    process = _ControlledProcess(pid=424242 if safe_reap else 1)
    monkeypatch.setattr(
        pool_api.asyncio,
        "create_subprocess_exec",
        AsyncMock(return_value=process),
    )
    killpg = MagicMock(side_effect=lambda _pid, _sig: process.kill())
    monkeypatch.setattr(pool_api.os, "killpg", killpg)

    async with session_factory() as db:
        response = await pool_api.add_account(
            _admin_request(),
            pool_api.AddAccountRequest(
                email="cancelled-watcher@example.com",
                token="mailbox-token",
            ),
            db,
        )

    assert len(pool_api._login_watch_tasks) == 1
    watcher = next(iter(pool_api._login_watch_tasks))
    watcher.cancel()
    await asyncio.gather(watcher, return_exceptions=True)
    await _wait_for_login_watchers()

    async with session_factory() as db:
        control = await db.get(
            WorkerNodeControl, WORKER_NODE_CONTROL_SINGLETON_ID
        )
    if safe_reap:
        killpg.assert_called_once_with(424242, pool_api.signal.SIGKILL)
        assert process.returncode == -9
        assert not pool_api._login_lock.locked()
        assert control.active_login_attempt_id is None
        assert control.active_login_kind is None
    else:
        killpg.assert_not_called()
        assert process.returncode is None
        assert pool_api._login_lock.locked()
        assert control.active_login_attempt_id == response["attempt_id"]
        assert control.active_login_kind == pool_api._CLAUDE_NODE_LOGIN_KIND
        assert pool_api._add_state["cancelled-watcher@example.com"][
            "recovery_required"
        ] is True


async def test_worker_restart_keeps_crash_left_claude_login_fail_closed(
    session_factory,
    monkeypatch,
):
    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    attempt_id = "a" * 32
    async with session_factory() as db:
        assert await admit_worker_node_login_attempt(
            db,
            attempt_id=attempt_id,
            kind=pool_api._CLAUDE_NODE_LOGIN_KIND,
        )
        await db.commit()

    async with session_factory() as db:
        with pytest.raises(RuntimeError, match="cannot prove completion"):
            await recover_worker_node_login_after_restart(
                db,
                unresolved_attempt_ids=set(),
            )

    async with session_factory() as db:
        control = await db.get(
            WorkerNodeControl, WORKER_NODE_CONTROL_SINGLETON_ID
        )
    assert control.active_login_attempt_id == attempt_id
    assert control.active_login_kind == pool_api._CLAUDE_NODE_LOGIN_KIND
