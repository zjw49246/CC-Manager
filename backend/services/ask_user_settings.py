"""把 AskUserQuestion 的 PreToolUse hook 合并进 {config_dir}/settings.json。

为什么走 settings.json 而不是 CLI flag：claude-pty 的命令构建是固定字段、不接受
`--settings`，且本仓库对 PTY 仓库只有 READ 权限无法 bump 依赖。好在 `-p` 和 PTY 两条
链路都用 CLAUDE_CONFIG_DIR，Claude Code 在 --dangerously-skip-permissions 下会自动
加载 {CLAUDE_CONFIG_DIR}/settings.json 的 hook（无审批弹窗，已实测）。

在 instance_manager.launch()（两路统一入口）每次启动前调用，幂等：
  enabled  → 确保我们的 hook 项存在且参数最新；
  disabled → 移除我们的 hook 项（保持文件干净）。
靠 command 里包含 "ask_user_hook.py" 识别"我们的"项，避免重复追加。
"""
from __future__ import annotations

import json
import logging
import os
import shlex
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

_CCM_ROOT = Path(__file__).resolve().parent.parent.parent
_VENV_PYTHON = _CCM_ROOT / ".venv" / "bin" / "python3"
_HOOK_SCRIPT = _CCM_ROOT / "backend" / "hooks" / "ask_user_hook.py"
_MATCHER = "AskUserQuestion"
_MARKER = "ask_user_hook.py"  # 识别"我们的"hook 项


def _hook_command() -> str:
    from backend.config import settings
    from backend.services.internal_api_endpoint import resolve_internal_api_base

    api_base = resolve_internal_api_base()
    python = str(_VENV_PYTHON) if _VENV_PYTHON.exists() else "python3"
    timeout = int(getattr(settings, "ask_user_timeout", 1800)) + 60

    parts = [
        python, str(_HOOK_SCRIPT),
        "--api-base", api_base,
        "--timeout", str(timeout),
    ]
    token = getattr(settings, "auth_token", "") or ""
    if token:
        parts.extend(["--auth-token", token])
    return " ".join(shlex.quote(p) for p in parts)


def _is_our_pretool_entry(entry: dict) -> bool:
    if not isinstance(entry, dict):
        return False
    if entry.get("matcher") != _MATCHER:
        return False
    for h in entry.get("hooks") or []:
        if isinstance(h, dict) and _MARKER in (h.get("command") or ""):
            return True
    return False


def ensure_ask_user_hook(config_dir: str) -> None:
    """幂等地把（或从）{config_dir}/settings.json 加入/移除 AskUserQuestion hook。"""
    from backend.config import settings

    enabled = bool(getattr(settings, "ask_user_enabled", True))
    try:
        cfg_path = Path(config_dir).expanduser()
        cfg_path.mkdir(parents=True, exist_ok=True)
        settings_path = cfg_path / "settings.json"

        data: dict = {}
        if settings_path.exists():
            try:
                data = json.loads(settings_path.read_text(encoding="utf-8")) or {}
            except (json.JSONDecodeError, OSError):
                data = {}
        if not isinstance(data, dict):
            data = {}

        hooks = data.get("hooks")
        if not isinstance(hooks, dict):
            hooks = {}
        pretool = hooks.get("PreToolUse")
        if not isinstance(pretool, list):
            pretool = []

        # 去掉旧的"我们的"项
        new_pretool = [e for e in pretool if not _is_our_pretool_entry(e)]
        changed = len(new_pretool) != len(pretool)

        if enabled:
            new_pretool.append({
                "matcher": _MATCHER,
                "hooks": [{
                    "type": "command",
                    "command": _hook_command(),
                    # CLI 对 hook 命令默认 600s 就杀；必须显式抬到服务端等待窗口
                    # 之上，否则 hook 在 /wait 阻塞中途被杀 → 放行原生
                    # AskUserQuestion → PTY 弹无人应答的交互框冻死整个 turn。
                    "timeout": int(getattr(settings, "ask_user_timeout", 1800)) + 60,
                }],
            })
            changed = True

        # Ensure thinking summaries are visible in stream output —
        # without this, CC returns encrypted thinking only.
        if not data.get("showThinkingSummaries"):
            data["showThinkingSummaries"] = True
            changed = True

        # 没变化（disabled 且本来就没有我们的项）→ 不写盘
        if not changed and not enabled:
            return

        if new_pretool:
            hooks["PreToolUse"] = new_pretool
        else:
            hooks.pop("PreToolUse", None)
        if hooks:
            data["hooks"] = hooks
        else:
            data.pop("hooks", None)

        _atomic_write_json(settings_path, data)
    except Exception:  # noqa: BLE001 — 注入失败绝不能阻断 launch
        logger.exception("ensure_ask_user_hook failed for %s", config_dir)


def _atomic_write_json(path: Path, data: dict) -> None:
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
