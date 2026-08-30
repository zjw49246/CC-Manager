"""Build exact Task hooks and maintain the legacy account-level AskUser hook.

Normal Tasks receive ``ask_user_hook_entry()`` in a private exact ``--settings``
file; ambient account/project settings are disabled. ``ensure_ask_user_hook``
remains only for prompt-only Claude processes that have no Task-scoped file.
It is idempotent:
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

from backend.services.trusted_runtime import RUNNING_PYTHON

logger = logging.getLogger(__name__)

_CCM_ROOT = Path(__file__).resolve().parent.parent.parent
_HOOK_SCRIPT = _CCM_ROOT / "backend" / "hooks" / "ask_user_hook.py"
_SSH_GUARD_SCRIPT = _CCM_ROOT / "backend" / "hooks" / "task_ssh_guard_hook.py"
_MATCHER = "AskUserQuestion"
_MARKER = "ask_user_hook.py"  # 识别"我们的"hook 项
_SSH_GUARD_MATCHER = "Bash|Read|Write|Edit|MultiEdit|Glob|Grep"
_SSH_GUARD_MARKER = "task_ssh_guard_hook.py"


def _hook_command(*, script_path: str | Path | None = None) -> str:
    from backend.config import settings
    from backend.services.internal_api_endpoint import resolve_internal_api_base

    api_base = resolve_internal_api_base()
    timeout = int(getattr(settings, "ask_user_timeout", 1800)) + 60

    parts = [
        RUNNING_PYTHON, str(script_path or _HOOK_SCRIPT),
        "--api-base", api_base,
        "--timeout", str(timeout),
    ]
    return " ".join(shlex.quote(p) for p in parts)


def ask_user_hook_entry(*, script_path: str | Path | None = None) -> dict:
    """Return CCM's secret-free AskUser hook entry for exact settings."""

    from backend.config import settings

    return {
        "matcher": _MATCHER,
        "hooks": [{
            "type": "command",
            "command": _hook_command(script_path=script_path),
            "timeout": int(getattr(settings, "ask_user_timeout", 1800)) + 60,
        }],
    }


def _is_our_pretool_entry(entry: dict) -> bool:
    if not isinstance(entry, dict):
        return False
    if entry.get("matcher") != _MATCHER:
        return False
    for h in entry.get("hooks") or []:
        if isinstance(h, dict) and _MARKER in (h.get("command") or ""):
            return True
    return False


def _is_our_ssh_guard_entry(entry: dict) -> bool:
    if not isinstance(entry, dict):
        return False
    for hook in entry.get("hooks") or []:
        if isinstance(hook, dict) and _SSH_GUARD_MARKER in (
            hook.get("command") or ""
        ):
            return True
    return False


def _ssh_guard_command(
    protected_paths: tuple[str, ...],
    *,
    script_path: str | Path | None = None,
) -> str:
    parts = [RUNNING_PYTHON, str(script_path or _SSH_GUARD_SCRIPT)]
    for path in protected_paths:
        parts.extend(["--protected-path", path])
    return " ".join(shlex.quote(part) for part in parts)


def task_ssh_guard_hook_entry(
    protected_paths: tuple[str, ...],
    *,
    script_path: str | Path | None = None,
) -> dict:
    """Return the advisory SSH guard used inside the OS-enforced sandbox."""

    return {
        "matcher": _SSH_GUARD_MATCHER,
        "hooks": [{
            "type": "command",
            "command": _ssh_guard_command(
                protected_paths,
                script_path=script_path,
            ),
            "timeout": 5,
        }],
    }


def ensure_ask_user_hook(
    config_dir: str,
    *,
    ssh_guard: bool = False,
    ssh_protected_paths: tuple[str, ...] = (),
) -> bool:
    """Merge CCM's Claude hooks into ``settings.json``.

    This compatibility path is not the enforcement boundary for normal Tasks;
    they use an exact private settings file plus the provider OS sandbox.
    """
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
        new_pretool = [
            entry
            for entry in pretool
            if not _is_our_pretool_entry(entry)
            and not (ssh_guard and _is_our_ssh_guard_entry(entry))
        ]
        changed = len(new_pretool) != len(pretool)

        if enabled:
            # CLI 对 hook 命令默认 600s 就杀；这里的 entry 抬高到服务端
            # 等待窗口之上，避免 PTY 回退为无人应答的原生交互框。
            new_pretool.append(ask_user_hook_entry())
            changed = True

        if ssh_guard:
            new_pretool.append(
                task_ssh_guard_hook_entry(ssh_protected_paths)
            )
            changed = True

        # Ensure thinking summaries are visible in stream output —
        # without this, CC returns encrypted thinking only.
        if not data.get("showThinkingSummaries"):
            data["showThinkingSummaries"] = True
            changed = True

        # 没变化（disabled 且本来就没有我们的项）→ 不写盘
        if not changed and not enabled:
            return True

        if new_pretool:
            hooks["PreToolUse"] = new_pretool
        else:
            hooks.pop("PreToolUse", None)
        if hooks:
            data["hooks"] = hooks
        else:
            data.pop("hooks", None)

        _atomic_write_json(settings_path, data)
        return True
    except Exception:  # noqa: BLE001 — SSH caller decides whether to fail closed
        logger.exception("ensure_ask_user_hook failed for %s", config_dir)
        return False


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
