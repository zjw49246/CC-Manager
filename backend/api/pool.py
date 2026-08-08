"""API endpoints for Claude account pool management."""

import asyncio
import glob
import os
import shutil
import time
from pathlib import Path

import json
import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from backend.api.deps import require_admin
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from backend.database import get_db
from backend.models.global_settings import GlobalSettings
from backend.services.cloudrouter_accounts import is_api_auth_kind
from backend.services.login_runtime import (
    LoginRuntimeError,
    ensure_login_runtime,
    login_child_environment,
    login_lock,
)

router = APIRouter(prefix="/api/pool", tags=["pool"])

# 重新登录后台任务状态：account_id -> {"status": running|success|failed, ...}
_relogin_state: dict[str, dict] = {}
# Claude/Codex 自动登录共用同一个浏览器运行时锁。
_login_lock = login_lock


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
async def pool_usage(force: bool = False):
    """Pool status merged with per-account quota utilization (OAuth usage API).

    Pass ``?force=true`` to bypass the 60s usage cache (e.g. after a manual
    token refresh via the retry button).
    """
    pool = _get_pool()
    status = pool.status()
    usage_by_id = {u["id"]: u for u in await pool.fetch_usage(force=force)}
    for account in status["accounts"]:
        u = usage_by_id.get(account["id"], {})
        account["subscription_type"] = u.get("subscription_type")
        account["usage"] = u.get("usage")
        account["usage_error"] = u.get("error")
        account["api_quota"] = u.get("api_quota")
    return status


@router.post("/reload")
async def pool_reload(request: Request):
    require_admin(request)
    pool = _get_pool()
    pool.reload()
    return pool.status()


@router.post("/accounts/{account_id}/clear-cooldown")
async def clear_cooldown(request: Request, account_id: str):
    require_admin(request)
    pool = _get_pool()
    pool.clear_cooldown(account_id)
    return {"ok": True, "account_id": account_id}


async def _watch_relogin(account_id: str, proc: asyncio.subprocess.Process):
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
            except HTTPException:
                pass
    finally:
        _login_lock.release()


@router.post("/accounts/{account_id}/relogin")
async def relogin_account(request: Request, account_id: str):
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

    # 1) OAuth refresh——绝大多数"过期"到这一步就解决了
    if await pool.refresh_oauth_token(account_id):
        _relogin_state.pop(account_id, None)
        return {"ok": True, "method": "refresh", "status": "success"}

    # 2) refresh 失败（refreshToken 失效/吊销）→ 真正重新登录
    state = _relogin_state.get(account_id)
    if state and state.get("status") == "running":
        return {"ok": True, "method": "auto_login", "status": "running"}

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
    try:
        await _ensure_xvfb()
        script = root / "scripts" / "auto_login.py"
        proc = await asyncio.create_subprocess_exec(
            str(login_py), str(script), "--email", acc.email, "--config-dir", acc.config_dir,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            env=login_child_environment(extra={"PYTHONUNBUFFERED": "1"}),
        )
    except BaseException:
        _login_lock.release()
        raise
    _relogin_state[account_id] = {"status": "running", "started_at": time.time()}
    # _watch_relogin 负责在进程结束后 release lock
    asyncio.get_running_loop().create_task(_watch_relogin(account_id, proc))
    return {"ok": True, "method": "auto_login", "status": "running"}


@router.get("/accounts/{account_id}/relogin")
async def relogin_status(account_id: str):
    return _relogin_state.get(account_id) or {"status": "idle"}


@router.delete("/accounts/{account_id}")
async def delete_account(request: Request, account_id: str):
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
    # 从 accounts.json 中删除
    accounts_path = Path.home() / ".claude-pool" / "accounts.json"
    data = json.loads(accounts_path.read_text())
    data["accounts"] = [a for a in data["accounts"] if a["id"] != account_id]
    accounts_path.write_text(json.dumps(data, indent=2))
    pool.reload()
    return {"ok": True, "deleted": account_id}


@router.post("/preferred")
async def set_preferred(request: Request, body: dict):
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
    if not pool.set_preferred(account_id):
        raise HTTPException(status_code=404, detail=f"Unknown account: {account_id}")
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


async def _watch_add(email: str, proc: asyncio.subprocess.Process):
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
            except Exception:
                pass
    finally:
        _login_lock.release()


@router.post("/add")
async def add_account(request: Request, body: AddAccountRequest):
    require_admin(request)
    """自动登录新账号并加入号池。三参数：email、接码 token、接码渠道。

    后台跑 auto_login.py，前端轮询 GET /api/pool/add/{email} 看进度。"""
    email = body.email.strip()
    if not email or not body.token.strip():
        raise HTTPException(400, "email 和 token 必填")

    state = _add_state.get(email)
    if state and state.get("status") == "running":
        return {"ok": True, "status": "running"}
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
    try:
        await _ensure_xvfb()
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            env=login_child_environment(extra={"PYTHONUNBUFFERED": "1"}),
        )
    except BaseException:
        _login_lock.release()
        raise
    _add_state[email] = {"status": "running", "started_at": time.time(), "account_id": account_id}
    asyncio.get_running_loop().create_task(_watch_add(email, proc))
    return {"ok": True, "status": "running", "account_id": account_id}


@router.get("/add/{email}")
async def add_status(email: str):
    return _add_state.get(email) or {"status": "idle"}


# ---------------------------------------------------------------------------
# CC Settings template (synced to all pool account config dirs)
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)

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
    row = await db.get(GlobalSettings, 1)
    if not row:
        row = GlobalSettings(id=1)
        db.add(row)
        await db.commit()
        await db.refresh(row)
    return row


class CcSettingsBody(BaseModel):
    settings: dict


@router.get("/cc-settings")
async def get_cc_settings(db: AsyncSession = Depends(get_db)):
    """Return the current CC settings template (or default if none saved)."""
    row = await _get_or_create_settings(db)
    if row.cc_settings_template:
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
    row = await _get_or_create_settings(db)
    row.cc_settings_template = json.dumps(body.settings, ensure_ascii=False)
    await db.commit()

    synced = _sync_cc_settings_to_accounts(body.settings)
    return {"ok": True, "synced": synced, "settings": body.settings}
