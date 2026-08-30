"""API endpoints for Claude account pool management."""

import asyncio
import glob
import os
import signal
import shutil
import time
import uuid
from pathlib import Path

import json
import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from backend.api.deps import require_admin
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.database import async_session, get_db
from backend.models.global_settings import GlobalSettings
from backend.services.cloudrouter_accounts import is_api_auth_kind
from backend.services.cancellation import (
    await_task_completion,
)
from backend.services.login_runtime import (
    LoginRuntimeError,
    ensure_login_runtime,
    login_child_environment,
    login_lock,
)
from backend.services.process_safety import (
    UnsafeProcessGroupError,
    require_safe_process_group_id,
)
from backend.services.worker_node_control import (
    admit_worker_node_login_attempt,
    fence_worker_node_account_mutation,
    finish_worker_node_login_attempt,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/pool", tags=["pool"])

# 重新登录后台任务状态：account_id -> {"status": running|success|failed, ...}
_relogin_state: dict[str, dict] = {}
# Claude/Codex 自动登录共用同一个浏览器运行时锁。
_login_lock = login_lock
# Background watchers must remain strongly referenced until their process and
# Worker-node journal have both reached a terminal state.
_login_watch_tasks: set[asyncio.Task] = set()
_CLAUDE_NODE_LOGIN_KIND = "claude_ssh"
_CLAUDE_LOGIN_REAP_TIMEOUT_SECONDS = 15.0


class _ClaudeLoginProcessNotTerminal(RuntimeError):
    """The login wrapper could not be proven stopped; keep every fence closed."""


async def _spawn_login_watcher(coro) -> asyncio.Task:
    """Transfer process ownership to a started watcher before HTTP returns."""

    started = asyncio.Event()

    async def _run():
        # Awaiting ``coro`` starts it synchronously through its first await. The
        # event therefore cannot wake the endpoint until the watcher's cleanup
        # ``finally`` owns the spawned process.
        started.set()
        return await coro

    task = asyncio.get_running_loop().create_task(_run())
    _login_watch_tasks.add(task)

    def _done(completed: asyncio.Task) -> None:
        _login_watch_tasks.discard(completed)
        if completed.cancelled():
            return
        try:
            error = completed.exception()
        except asyncio.CancelledError:
            return
        if error is not None:
            logger.error(
                "Claude pool login watcher failed closed",
                exc_info=(type(error), error, error.__traceback__),
            )

    task.add_done_callback(_done)
    await asyncio.shield(started.wait())
    return task


async def _admit_worker_node_claude_login(
    db: AsyncSession,
    *,
    attempt_id: str,
) -> bool:
    """Commit the Worker-local login journal before a wrapper can be spawned."""

    admitted = await admit_worker_node_login_attempt(
        db,
        attempt_id=attempt_id,
        kind=_CLAUDE_NODE_LOGIN_KIND,
    )
    if admitted:
        await db.commit()
    return admitted


async def _finish_worker_node_claude_login(attempt_id: str) -> bool:
    """Clear only the exact terminal Claude login journal."""

    async with async_session() as db:
        cleared = await finish_worker_node_login_attempt(
            db,
            attempt_id=attempt_id,
            kind=_CLAUDE_NODE_LOGIN_KIND,
        )
        if cleared:
            await db.commit()
        if settings.ccm_node_role == "worker" and not cleared:
            raise RuntimeError(
                "Worker Claude login journal changed before terminal cleanup"
            )
        return cleared


async def _stop_unfinished_claude_login_process(
    proc: asyncio.subprocess.Process,
    *,
    operation: str,
) -> None:
    """Kill and reap the exact isolated wrapper before releasing its journal."""

    if proc.returncode is not None:
        return
    try:
        pid = getattr(proc, "pid", None)
        if hasattr(os, "killpg"):
            process_group_id = require_safe_process_group_id(
                pid,
                context=f"Claude {operation} wrapper",
            )
            os.killpg(process_group_id, signal.SIGKILL)
        else:
            proc.kill()
    except UnsafeProcessGroupError as exc:
        raise _ClaudeLoginProcessNotTerminal(
            f"Claude {operation} wrapper has an unsafe process identity"
        ) from exc
    except ProcessLookupError:
        pass
    except Exception:
        logger.exception("Failed to stop Claude %s process group", operation)
        try:
            proc.kill()
        except Exception as exc:
            raise _ClaudeLoginProcessNotTerminal(
                f"Claude {operation} wrapper could not be stopped"
            ) from exc

    waiter = asyncio.create_task(proc.wait())
    deadline = (
        asyncio.get_running_loop().time()
        + _CLAUDE_LOGIN_REAP_TIMEOUT_SECONDS
    )
    remaining = max(0.0, deadline - asyncio.get_running_loop().time())
    deadline_wait = asyncio.create_task(
        asyncio.wait({waiter}, timeout=remaining)
    )
    # This cleanup task is intentionally uncancellable: it must finish the
    # process/journal invariant before its owning watcher can unwind.  The
    # outer _await_claude_login_cleanup boundary is responsible for reporting
    # request cancellation after the complete journal finalizer settles.
    await await_task_completion(deadline_wait)
    done, _pending = deadline_wait.result()
    if not done:
        waiter.cancel()
        await await_task_completion(waiter)
        raise _ClaudeLoginProcessNotTerminal(
            f"Claude {operation} wrapper termination timed out"
        )
    if waiter.cancelled() or (waiter.exception() and proc.returncode is None):
        raise _ClaudeLoginProcessNotTerminal(
            f"Claude {operation} wrapper wait failed"
        )
    if proc.returncode is None:
        raise _ClaudeLoginProcessNotTerminal(
            f"Claude {operation} wrapper has no terminal status"
        )


async def _await_claude_login_cleanup(coro) -> tuple[bool, object]:
    """Finish cleanup and report whether cancellation arrived meanwhile."""

    cleanup_task = asyncio.create_task(coro)
    cancellation = await await_task_completion(cleanup_task)
    result = cleanup_task.result()
    return cancellation is not None, result


async def _finalize_claude_login_process(
    proc: asyncio.subprocess.Process,
    *,
    operation: str,
    attempt_id: str,
    node_login_admitted: bool,
) -> None:
    """Prove process terminal, then clear the exact Worker login journal."""

    await _stop_unfinished_claude_login_process(proc, operation=operation)
    if node_login_admitted:
        await _finish_worker_node_claude_login(attempt_id)


def _is_api_account(account) -> bool:
    return is_api_auth_kind(getattr(account, "auth_kind", ""))


def _get_optional_pool():
    from backend.main import dispatcher

    return dispatcher.pool


def _get_pool():
    pool = _get_optional_pool()
    if not pool:
        raise HTTPException(status_code=404, detail="Pool is not enabled. Set POOL_ENABLED=true in .env")
    return pool


def _disabled_pool_status() -> dict:
    return {
        "enabled": False,
        "total": 0,
        "available": 0,
        "cooldown": 0,
        "disabled": 0,
        "preferred": None,
        "last_selected": None,
        "accounts": [],
    }


@router.get("/status")
async def pool_status():
    pool = _get_optional_pool()
    return pool.status() if pool else _disabled_pool_status()


@router.get("/usage")
async def pool_usage(
    force: bool = False,
    db: AsyncSession = Depends(get_db),
):
    """Pool status merged with per-account quota utilization (OAuth usage API).

    Pass ``?force=true`` to bypass the 60s usage cache (e.g. after a manual
    token refresh via the retry button).
    """
    pool = _get_pool()
    # Although this is exposed as a read, expired OAuth credentials are
    # refreshed and atomically written by ``fetch_usage``. Treat the whole
    # request as an account mutation on Worker nodes.
    await fence_worker_node_account_mutation(db)
    status = pool.status()
    usage_by_id = {u["id"]: u for u in await pool.fetch_usage(force=force)}
    for account in status["accounts"]:
        u = usage_by_id.get(account["id"], {})
        account["subscription_type"] = u.get("subscription_type")
        account["usage"] = u.get("usage")
        account["usage_error"] = u.get("error")
        account["api_quota"] = u.get("api_quota")
    await db.commit()
    return status


@router.post("/reload")
async def pool_reload(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    require_admin(request)
    pool = _get_pool()
    await fence_worker_node_account_mutation(db)
    pool.reload()
    await db.commit()
    return pool.status()


@router.post("/accounts/{account_id}/clear-cooldown")
async def clear_cooldown(
    request: Request,
    account_id: str,
    db: AsyncSession = Depends(get_db),
):
    require_admin(request)
    pool = _get_pool()
    await fence_worker_node_account_mutation(db)
    pool.clear_cooldown(account_id)
    await db.commit()
    return {"ok": True, "account_id": account_id}


async def _watch_relogin(
    account_id: str,
    proc: asyncio.subprocess.Process,
    *,
    attempt_id: str,
    node_login_admitted: bool,
):
    cancelled = False
    cleanup_complete = False
    try:
        out, _ = await proc.communicate()
        tail = (out or b"").decode("utf-8", errors="replace")[-5000:]
        _relogin_state[account_id] = {
            "status": "success" if proc.returncode == 0 else "failed",
            "detail": tail,
            "finished_at": time.time(),
        }
        if proc.returncode == 0:
            try:
                _get_pool()._usage_cache = None
            except HTTPException as exc:
                if node_login_admitted:
                    raise RuntimeError(
                        "Worker Claude relogin could not reconcile pool state"
                    ) from exc
    except asyncio.CancelledError:
        cancelled = True
    except Exception as exc:
        logger.exception("Claude relogin watcher failed for %s", account_id)
        _relogin_state[account_id] = {
            "status": "failed",
            "detail": str(exc)[-1000:],
            "finished_at": time.time(),
            "attempt_id": attempt_id,
        }
    finally:
        try:
            cleanup_cancelled, _ = await _await_claude_login_cleanup(
                _finalize_claude_login_process(
                    proc,
                    operation=f"relogin for {account_id}",
                    attempt_id=attempt_id,
                    node_login_admitted=node_login_admitted,
                )
            )
            cancelled = cancelled or cleanup_cancelled
            cleanup_complete = True
        except Exception as exc:
            # An unproven process or mismatched journal keeps both the durable
            # Worker fence and the process-wide login lock closed.
            logger.critical(
                "Claude relogin cleanup failed closed for %s",
                account_id,
                exc_info=True,
            )
            _relogin_state[account_id] = {
                "status": "failed",
                "detail": str(exc)[-1000:],
                "finished_at": time.time(),
                "attempt_id": attempt_id,
                "recovery_required": True,
            }
        if cleanup_complete and _login_lock.locked():
            _login_lock.release()
    if cancelled:
        raise asyncio.CancelledError


@router.post("/accounts/{account_id}/relogin")
async def relogin_account(
    request: Request,
    account_id: str,
    db: AsyncSession = Depends(get_db),
):
    require_admin(request)
    """重新登录账号。先试 OAuth refresh（token 过期 ≠ 要重新登录，CLI 平时
    会自动刷，闲置账号刷一下就恢复）；refresh 真失败才跑 auto_login.py。"""
    pool = _get_pool()
    acc = pool.account(account_id)
    if acc is None:
        raise HTTPException(status_code=404, detail=f"Unknown account: {account_id}")
    if _is_api_account(acc):
        raise HTTPException(
            status_code=400,
            detail="API 账号不使用 OAuth 登录，请通过 API 账号刷新入口校验",
        )

    state = _relogin_state.get(account_id)
    if state and state.get("status") == "running":
        return {
            "ok": True,
            "method": "auto_login",
            "status": "running",
            **(
                {"attempt_id": state["attempt_id"]}
                if state.get("attempt_id") is not None
                else {}
            ),
        }

    # 1) OAuth refresh——绝大多数"过期"到这一步就解决了
    # OAuth refresh itself rewrites credentials. Hold the node writer until it
    # has either completed or failed, before considering a background login.
    await fence_worker_node_account_mutation(db)
    refreshed = await pool.refresh_oauth_token(account_id)
    await db.commit()
    if refreshed:
        _relogin_state.pop(account_id, None)
        return {"ok": True, "method": "refresh", "status": "success"}

    # 2) refresh 失败（refreshToken 失效/吊销）→ 真正重新登录
    # Chrome CDP 绑定固定端口，同时只能跑一个登录流程
    if _login_lock.locked():
        running = [k for k, v in _relogin_state.items() if v.get("status") == "running"]
        raise HTTPException(status_code=409, detail=f"另一个账号正在登录中（{', '.join(running)}），请等它完成后再试")

    root = Path(__file__).resolve().parents[2]
    # CDP 登录只依赖 httpx/websockets，已在主 venv 中；优先用 .venv，兼容旧 .login-venv
    login_py = root / ".venv" / "bin" / "python3"
    if not login_py.exists():
        login_py = root / ".login-venv" / "bin" / "python3"
    if not login_py.exists():
        raise HTTPException(status_code=501, detail=(
            "Token 刷新失败，找不到 .venv 或 .login-venv。"
            f"请手动登录：python3 scripts/auto_login.py --email {acc.email} "
            f"--config-dir {acc.config_dir}"
        ))
    # auto_login 用 channel="chrome"（系统 Google Chrome，headed 过 Cloudflare）；
    # playwright 自带 chromium 仅作兜底
    has_browser = (
        Path("/opt/google/chrome/chrome").exists()
        or shutil.which("google-chrome") or shutil.which("google-chrome-stable")
        or glob.glob(str(Path.home() / ".cache/ms-playwright/chromium-*/chrome-linux64/chrome"))
    )
    if not has_browser:
        raise HTTPException(status_code=501, detail=(
            "Token 刷新失败，且找不到浏览器（auto_login 需要系统 Google Chrome）。"
            "安装 google-chrome-stable 或 .login-venv/bin/python3 -m playwright install chromium"
        ))
    await _login_lock.acquire()
    attempt_id = uuid.uuid4().hex
    node_login_admitted = False
    proc: asyncio.subprocess.Process | None = None
    try:
        node_login_admitted = await _admit_worker_node_claude_login(
            db,
            attempt_id=attempt_id,
        )
        await _ensure_xvfb()
        script = root / "scripts" / "auto_login.py"
        proc = await asyncio.create_subprocess_exec(
            str(login_py), str(script), "--email", acc.email, "--config-dir", acc.config_dir,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            env=login_child_environment(extra={"PYTHONUNBUFFERED": "1"}),
            start_new_session=True,
        )
    except BaseException:
        cleanup_complete = not node_login_admitted
        if proc is None and node_login_admitted:
            try:
                await _await_claude_login_cleanup(
                    _finish_worker_node_claude_login(attempt_id)
                )
                cleanup_complete = True
            except Exception:
                logger.critical(
                    "Could not roll back unspawned Worker Claude relogin %s",
                    attempt_id,
                    exc_info=True,
                )
        if cleanup_complete and _login_lock.locked():
            _login_lock.release()
        raise
    _relogin_state[account_id] = {
        "status": "running",
        "started_at": time.time(),
        "attempt_id": attempt_id,
    }
    # _watch_relogin 负责在进程结束后 release lock
    await _spawn_login_watcher(_watch_relogin(
        account_id,
        proc,
        attempt_id=attempt_id,
        node_login_admitted=node_login_admitted,
    ))
    return {
        "ok": True,
        "method": "auto_login",
        "status": "running",
        "attempt_id": attempt_id,
    }


@router.get("/accounts/{account_id}/relogin")
async def relogin_status(account_id: str):
    return _relogin_state.get(account_id) or {"status": "idle"}


@router.delete("/accounts/{account_id}")
async def delete_account(
    request: Request,
    account_id: str,
    db: AsyncSession = Depends(get_db),
):
    require_admin(request)
    """从号池中删除账号（不删 config_dir 文件夹，方便以后重新登录其他号）。"""
    pool = _get_pool()
    acc = pool.account(account_id)
    if acc is None:
        raise HTTPException(status_code=404, detail=f"Unknown account: {account_id}")
    if _is_api_account(acc):
        raise HTTPException(
            status_code=400,
            detail="API 账号请通过 API 账号删除入口处理",
        )
    await fence_worker_node_account_mutation(db)
    # 从 accounts.json 中删除
    accounts_path = Path.home() / ".claude-pool" / "accounts.json"
    data = json.loads(accounts_path.read_text())
    data["accounts"] = [a for a in data["accounts"] if a["id"] != account_id]
    _atomic_write_json(accounts_path, data)
    pool.reload()
    await db.commit()
    return {"ok": True, "deleted": account_id}


@router.post("/preferred")
async def set_preferred(
    request: Request,
    body: dict,
    db: AsyncSession = Depends(get_db),
):
    require_admin(request)
    """Pin an account for subsequent routes (manual switch).

    Body: {"account_id": "account-1"} or {"account_id": null} to clear.
    The next turn on an existing session safely hardlink-migrates its JSONL to
    the pinned account; a migration failure keeps an intact healthy resident
    instead of launching without context. With no pin, existing sessions stay
    resident while fresh launches prefer a compatible available API account.
    If the pinned account is unavailable, automatic routing falls back.
    """
    pool = _get_pool()
    account_id = body.get("account_id")
    await fence_worker_node_account_mutation(db)
    if not pool.set_preferred(account_id):
        raise HTTPException(status_code=404, detail=f"Unknown account: {account_id}")
    await db.commit()
    return {"ok": True, "preferred": pool.preferred_account_id}


# ---------------------------------------------------------------------------
# Add account (三参数自动登录)
# ---------------------------------------------------------------------------

class AddAccountRequest(BaseModel):
    email: str
    token: str
    login_method: str = ""  # 171mail | mailcom | onet | gazeta | "" (auto-detect)


async def _ensure_xvfb():
    try:
        return await ensure_login_runtime()
    except LoginRuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


# 后台 add 状态：key = email -> {"status": running|success|failed, ...}
_add_state: dict[str, dict] = {}


async def _watch_add(
    email: str,
    proc: asyncio.subprocess.Process,
    *,
    attempt_id: str,
    node_login_admitted: bool,
):
    cancelled = False
    cleanup_complete = False
    try:
        out, _ = await proc.communicate()
        tail = (out or b"").decode("utf-8", errors="replace")[-5000:]
        _add_state[email] = {
            "status": "success" if proc.returncode == 0 else "failed",
            "detail": tail,
            "finished_at": time.time(),
        }
        if proc.returncode == 0:
            try:
                _get_pool().reload()
                _get_pool()._usage_cache = None
            except Exception as exc:
                if node_login_admitted:
                    raise RuntimeError(
                        "Worker Claude add could not reconcile pool state"
                    ) from exc
    except asyncio.CancelledError:
        cancelled = True
    except Exception as exc:
        logger.exception("Claude add watcher failed for %s", email)
        _add_state[email] = {
            "status": "failed",
            "detail": str(exc)[-1000:],
            "finished_at": time.time(),
            "attempt_id": attempt_id,
        }
    finally:
        try:
            cleanup_cancelled, _ = await _await_claude_login_cleanup(
                _finalize_claude_login_process(
                    proc,
                    operation=f"add for {email}",
                    attempt_id=attempt_id,
                    node_login_admitted=node_login_admitted,
                )
            )
            cancelled = cancelled or cleanup_cancelled
            cleanup_complete = True
        except Exception as exc:
            logger.critical(
                "Claude add cleanup failed closed for %s",
                email,
                exc_info=True,
            )
            _add_state[email] = {
                "status": "failed",
                "detail": str(exc)[-1000:],
                "finished_at": time.time(),
                "attempt_id": attempt_id,
                "recovery_required": True,
            }
        if cleanup_complete and _login_lock.locked():
            _login_lock.release()
    if cancelled:
        raise asyncio.CancelledError


@router.post("/add")
async def add_account(
    request: Request,
    body: AddAccountRequest,
    db: AsyncSession = Depends(get_db),
):
    require_admin(request)
    """自动登录新账号并加入号池。三参数：email、接码 token、接码渠道。

    后台跑 auto_login.py，前端轮询 GET /api/pool/add/{email} 看进度。"""
    email = body.email.strip()
    if not email or not body.token.strip():
        raise HTTPException(400, "email 和 token 必填")

    state = _add_state.get(email)
    if state and state.get("status") == "running":
        return {
            "ok": True,
            "status": "running",
            **(
                {"attempt_id": state["attempt_id"]}
                if state.get("attempt_id") is not None
                else {}
            ),
            **(
                {"account_id": state["account_id"]}
                if state.get("account_id") is not None
                else {}
            ),
        }
    if _login_lock.locked():
        raise HTTPException(
            status_code=409,
            detail="另一个 Claude/Codex 账号正在登录中，请等待完成后再试",
        )

    root = Path(__file__).resolve().parents[2]
    login_py = root / ".venv" / "bin" / "python3"
    if not login_py.exists():
        login_py = root / ".login-venv" / "bin" / "python3"
    if not login_py.exists():
        login_py = Path(shutil.which("python3") or "python3")

    script = root / "scripts" / "auto_login.py"
    # 找最小可用编号作为 slot 名
    pool = _get_pool()
    existing_ids = {a.id for a in pool._accounts} if pool else set()
    if not existing_ids:
        account_id = "account-1"
    else:
        n = 1
        while f"account-{n}" in existing_ids:
            n += 1
        account_id = f"account-{n}"
    config_dir = str(Path.home() / ".claude") if account_id == "account-1" else str(
        Path.home() / f".claude-{account_id}"
    )

    cmd = [
        str(login_py), str(script),
        "--email", email,
        "--token", body.token.strip(),
        "--config-dir", config_dir,
        "--add-to-pool", account_id,
        "--save-token",
    ]
    if body.login_method in ("171mail", "mailcom", "onet", "gazeta"):
        cmd.extend(["--login-method", body.login_method])
    await _login_lock.acquire()
    attempt_id = uuid.uuid4().hex
    node_login_admitted = False
    proc: asyncio.subprocess.Process | None = None
    try:
        node_login_admitted = await _admit_worker_node_claude_login(
            db,
            attempt_id=attempt_id,
        )
        await _ensure_xvfb()
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            env=login_child_environment(extra={"PYTHONUNBUFFERED": "1"}),
            start_new_session=True,
        )
    except BaseException:
        cleanup_complete = not node_login_admitted
        if proc is None and node_login_admitted:
            try:
                await _await_claude_login_cleanup(
                    _finish_worker_node_claude_login(attempt_id)
                )
                cleanup_complete = True
            except Exception:
                logger.critical(
                    "Could not roll back unspawned Worker Claude add %s",
                    attempt_id,
                    exc_info=True,
                )
        if cleanup_complete and _login_lock.locked():
            _login_lock.release()
        raise
    _add_state[email] = {
        "status": "running",
        "started_at": time.time(),
        "account_id": account_id,
        "attempt_id": attempt_id,
    }
    await _spawn_login_watcher(_watch_add(
        email,
        proc,
        attempt_id=attempt_id,
        node_login_admitted=node_login_admitted,
    ))
    return {
        "ok": True,
        "status": "running",
        "account_id": account_id,
        "attempt_id": attempt_id,
    }


@router.get("/add/{email}")
async def add_status(email: str):
    return _add_state.get(email) or {"status": "idle"}


# ---------------------------------------------------------------------------
# CC Settings template (synced to all pool account config dirs)
# ---------------------------------------------------------------------------

DEFAULT_CC_SETTINGS: dict = {
    "permissions": {
        "defaultMode": "bypassPermissions",
        "additionalDirectories": ["/home/ubuntu/Claude-Code-Manager"],
    },
    "model": "claude-opus-4-6",
    "effortLevel": "medium",
    "skipDangerousModePermissionPrompt": True,
    "hasCompletedOnboarding": True,
    "theme": "dark",
    "showThinkingSummaries": True,
}


def _atomic_write_json(path: Path, data: dict) -> None:
    """Atomic write: write to temp file then rename (same as ask_user_settings)."""
    import tempfile
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".settings.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _sync_cc_settings_to_accounts(template: dict) -> int:
    """Merge *template* into settings.json for every pool account (+ default ~/.claude).

    PRESERVES the existing ``hooks`` key so that dynamically-injected hooks
    (e.g. ask_user_hook) are not overwritten.

    Returns the number of config dirs synced.
    """
    config_dirs: list[str] = []

    try:
        pool = _get_pool()
        for acc in pool._accounts:
            if acc.enabled:
                config_dirs.append(acc.config_dir)
    except HTTPException:
        # Pool not enabled — fall back to default ~/.claude
        pass

    default_dir = str(Path.home() / ".claude")
    if default_dir not in config_dirs:
        config_dirs.append(default_dir)

    synced = 0
    for config_dir in config_dirs:
        try:
            cfg_path = Path(config_dir)
            cfg_path.mkdir(parents=True, exist_ok=True)
            settings_path = cfg_path / "settings.json"

            existing: dict = {}
            if settings_path.exists():
                try:
                    existing = json.loads(settings_path.read_text(encoding="utf-8")) or {}
                except (json.JSONDecodeError, OSError):
                    existing = {}
            if not isinstance(existing, dict):
                existing = {}

            # Preserve existing hooks
            saved_hooks = existing.get("hooks")

            # Merge: template overwrites everything except hooks
            merged = {**existing, **template}

            # Restore hooks if they existed
            if saved_hooks is not None:
                merged["hooks"] = saved_hooks
            elif "hooks" in template:
                # Template should not inject hooks, but if it does, remove them
                del merged["hooks"]

            _atomic_write_json(settings_path, merged)
            synced += 1
        except Exception:
            logger.exception("Failed to sync CC settings to %s", config_dir)

    return synced


async def _get_or_create_settings(db: AsyncSession) -> GlobalSettings:
    """Materialize the singleton without committing the caller's writer fence."""

    row = await db.get(GlobalSettings, 1)
    if not row:
        row = GlobalSettings(id=1)
        db.add(row)
        await db.flush()
    return row


class CcSettingsBody(BaseModel):
    settings: dict


@router.get("/cc-settings")
async def get_cc_settings(db: AsyncSession = Depends(get_db)):
    """Return the current CC settings template (or default if none saved)."""
    # GET must not create the singleton: on a draining Worker that would turn a
    # read into an unfenced mutation. PUT materializes it under the node writer.
    row = await db.get(GlobalSettings, 1)
    if row is not None and row.cc_settings_template:
        try:
            return {"settings": json.loads(row.cc_settings_template)}
        except (json.JSONDecodeError, TypeError):
            pass
    return {"settings": DEFAULT_CC_SETTINGS}


@router.put("/cc-settings")
async def put_cc_settings(
    request: Request,
    body: CcSettingsBody,
    db: AsyncSession = Depends(get_db),
):
    """Save CC settings template and sync to all pool accounts."""
    require_admin(request)
    await fence_worker_node_account_mutation(db)
    row = await _get_or_create_settings(db)
    row.cc_settings_template = json.dumps(body.settings, ensure_ascii=False)
    synced = _sync_cc_settings_to_accounts(body.settings)
    await db.commit()
    return {"ok": True, "synced": synced, "settings": body.settings}
