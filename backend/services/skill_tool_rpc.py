"""Manager-side implementation of the task-scoped Skills MCP tools.

The stdio MCP child is deliberately a tiny HTTP-only wrapper.  It must never
import CCM's database or service package from a checkout that the calling Task
can edit.  This module is imported with ``backend.api.tasks`` during Manager
startup, before any Task process is admitted, and owns every privileged skill
effect behind the scoped internal-service route.
"""

from __future__ import annotations

import json
import logging
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.database import async_session
from backend.models.task import Task
from backend.models.user import User
from backend.models.user_skill import UserSkill
from backend.services.command_registry import COMMAND_REGISTRY
from backend.services.skill_context import (
    codex_monitor_supported_for_scope,
    normalize_user_skill_ids,
    skill_supported,
    user_skill_snapshots_from_metadata,
)
from backend.services.skill_distill import analyze_patterns
from backend.services.skill_evolution import get_lessons_for_skill
from backend.services.skill_loader import discover_skills


SKILL_TOOL_RPC_NAMES = frozenset({
    "ccm_command_help",
    "ccm_read_skill",
    "ccm_read_user_skill",
    "ccm_create_skill",
    "ccm_distill",
    "ccm_enable_skill",
    "ccm_disable_skill",
})

_CCM_ROOT = Path(__file__).resolve().parents[2]
_SKILLS_ROOT = _CCM_ROOT / "skills"
_SKILL_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,79}$")
_MAX_DESCRIPTION_CHARS = 16_384
_MAX_BODY_CHARS = 1_000_000
_MAX_TAGS_CHARS = 8_192
logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SkillToolRPCOutcome:
    """Serialized MCP result plus an optional validated Task update."""

    result: str
    enabled_skills: dict[str, bool] | None = None


class SkillToolRPCError(ValueError):
    """The frozen MCP wrapper supplied an invalid tool request."""


def _json_result(payload: Mapping[str, Any] | list[Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, default=str)


def _failure(message: str) -> SkillToolRPCOutcome:
    return SkillToolRPCOutcome(
        _json_result({"success": False, "error": message})
    )


def _arguments(
    raw: Mapping[str, Any],
    *,
    allowed: frozenset[str],
    required: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise SkillToolRPCError("Skill tool arguments must be an object")
    keys = set(raw)
    unexpected = sorted(keys - allowed)
    missing = sorted(required - keys)
    if unexpected:
        raise SkillToolRPCError(
            "Unexpected skill tool arguments: " + ", ".join(unexpected)
        )
    if missing:
        raise SkillToolRPCError(
            "Missing skill tool arguments: " + ", ".join(missing)
        )
    return dict(raw)


def _string_argument(
    values: Mapping[str, Any],
    name: str,
    *,
    default: str | None = None,
    max_chars: int,
    allow_empty: bool = False,
) -> str:
    value = values.get(name, default)
    if not isinstance(value, str):
        raise SkillToolRPCError(f"{name} must be a string")
    if not allow_empty and not value.strip():
        raise SkillToolRPCError(f"{name} must not be empty")
    if len(value) > max_chars:
        raise SkillToolRPCError(f"{name} is too large")
    return value


def _positive_int_argument(
    values: Mapping[str, Any],
    name: str,
    *,
    default: int | None = None,
    maximum: int,
) -> int:
    value = values.get(name, default)
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value <= 0
        or value > maximum
    ):
        raise SkillToolRPCError(
            f"{name} must be an integer between 1 and {maximum}"
        )
    return value


def _codex_monitor_enabled(task: Task) -> bool:
    return codex_monitor_supported_for_scope(
        provider=task.provider,
        worker_id=task.worker_id,
        shared_from_id=task.shared_from_id,
        metadata=task.metadata_,
        codex_main_mcp_enabled=settings.codex_main_mcp_enabled,
    )


async def _command_help(task: Task, raw: Mapping[str, Any]) -> SkillToolRPCOutcome:
    _arguments(raw, allowed=frozenset())
    provider = task.provider or "claude"
    monitor_enabled = _codex_monitor_enabled(task)
    commands = []
    for command in COMMAND_REGISTRY.values():
        if any(
            not skill_supported(
                provider,
                skill_name,
                codex_monitor_enabled=monitor_enabled,
            )
            for skill_name in command.required_skills
        ):
            continue
        commands.append({
            "command": f"${command.name}",
            "description": command.description,
            "type": "command",
        })

    skills = discover_skills(
        exclude=(
            {"monitor"}
            if provider.lower() == "codex" and not monitor_enabled
            else None
        )
    )
    enabled = task.enabled_skills or {}
    catalog = [{
        "name": name,
        "description": skill.description.strip()[:150],
        "enabled": bool(enabled.get(name, False)),
        "commands": [command["name"] for command in skill.ccm.commands],
        "type": "skill",
    } for name, skill in skills.items()]
    return SkillToolRPCOutcome(_json_result({
        "success": True,
        "commands": commands,
        "skills": catalog,
        "usage": (
            "用 $命令名 触发命令，用 ccm_read_skill(name) 读取技能详情，"
            "用 ccm_enable_skill(name) 启用技能。"
        ),
    }))


async def _read_skill(task: Task, raw: Mapping[str, Any]) -> SkillToolRPCOutcome:
    values = _arguments(
        raw,
        allowed=frozenset({"skill_name"}),
        required=frozenset({"skill_name"}),
    )
    skill_name = _string_argument(
        values,
        "skill_name",
        max_chars=160,
    )
    provider = (task.provider or "claude").lower()
    if not skill_supported(
        provider,
        skill_name,
        codex_monitor_enabled=_codex_monitor_enabled(task),
    ):
        return _failure(
            f"Skill '{skill_name}' is not supported by provider {provider}"
        )

    skills = discover_skills()
    skill = skills.get(skill_name)
    if skill is None:
        return _failure(
            f"技能 '{skill_name}' 不存在。可用技能: {', '.join(skills)}"
        )
    if (
        provider == "codex"
        and not settings.codex_main_mcp_enabled
        and skill_name != "sub-agent"
    ):
        return _failure(
            f"Skill '{skill_name}' is unavailable while Codex main-task MCP "
            "is disabled"
        )
    if not (task.enabled_skills or {}).get(skill_name, False) and not skill.ccm.always:
        return _failure(f"Skill '{skill_name}' is not enabled for this task")

    body = skill.body
    try:
        async with async_session() as lessons_db:
            lessons = await get_lessons_for_skill(skill_name, lessons_db)
        if lessons:
            body += "\n\n## Learned Lessons\n"
            body += "\n".join(f"- {lesson}" for lesson in lessons)
    except Exception:
        pass

    return SkillToolRPCOutcome(_json_result({
        "success": True,
        "name": skill.name,
        "description": skill.description,
        "body": body,
        "commands": skill.ccm.commands,
        "tags": skill.ccm.tags,
    }))


async def _read_user_skill(
    task: Task,
    raw: Mapping[str, Any],
) -> SkillToolRPCOutcome:
    values = _arguments(
        raw,
        allowed=frozenset({"skill_id"}),
        required=frozenset({"skill_id"}),
    )
    skill_id = _positive_int_argument(
        values,
        "skill_id",
        maximum=2_147_483_647,
    )
    selected = normalize_user_skill_ids(task.selected_user_skills)
    if skill_id not in selected:
        return _failure(f"User skill {skill_id} is not selected for this task")

    metadata = task.metadata_
    snapshots = {
        snapshot.id: snapshot
        for snapshot in user_skill_snapshots_from_metadata(metadata)
    }
    snapshot = snapshots.get(skill_id)
    if snapshot is not None:
        return SkillToolRPCOutcome(_json_result({
            "success": True,
            "id": snapshot.id,
            "name": snapshot.name,
            "description": snapshot.description,
            "content": snapshot.content,
        }))
    if isinstance(metadata, dict) and "ccm_user_skill_snapshots" in metadata:
        return _failure(
            f"User skill {skill_id} is unavailable in the authoritative task snapshot"
        )

    async with async_session() as user_skill_db:
        skill = await user_skill_db.get(UserSkill, skill_id)
    if skill is None:
        return _failure(f"User skill {skill_id} not found")
    return SkillToolRPCOutcome(_json_result({
        "success": True,
        "id": skill.id,
        "name": skill.name,
        "description": skill.description,
        "content": skill.content,
    }))


def _validate_skills_root() -> None:
    try:
        info = _SKILLS_ROOT.lstat()
    except OSError as exc:
        raise SkillToolRPCError("CCM skills directory is unavailable") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise SkillToolRPCError("CCM skills directory is unsafe")


def _write_new_skill(
    *,
    name: str,
    description: str,
    body: str,
    tags: str,
    always: bool,
) -> Path:
    _validate_skills_root()
    tag_list = [value.strip() for value in tags.split(",") if value.strip()]
    frontmatter = yaml.safe_dump(
        {
            "name": name,
            "description": description,
            "ccm": {
                "always": always,
                "priority": 5,
                "version": 1,
                "tags": tag_list,
            },
        },
        allow_unicode=True,
        sort_keys=False,
    ).strip()
    payload = (
        f"---\n{frontmatter}\n---\n\n{body}\n\n"
        "## Lessons Learned\n<!-- 自进化系统自动追加 -->\n"
    ).encode("utf-8")

    root_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    root_flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    root_fd = os.open(_SKILLS_ROOT, root_flags)
    created_dir = False
    try:
        os.mkdir(name, mode=0o755, dir_fd=root_fd)
        created_dir = True
        directory_fd = os.open(name, root_flags, dir_fd=root_fd)
        try:
            file_flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            file_flags |= getattr(os, "O_CLOEXEC", 0)
            file_flags |= getattr(os, "O_NOFOLLOW", 0)
            file_fd = os.open(
                "SKILL.md",
                file_flags,
                0o644,
                dir_fd=directory_fd,
            )
            try:
                view = memoryview(payload)
                while view:
                    written = os.write(file_fd, view)
                    if written <= 0:
                        raise OSError("short write")
                    view = view[written:]
                os.fsync(file_fd)
            finally:
                os.close(file_fd)
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        os.fsync(root_fd)
    except FileExistsError as exc:
        raise SkillToolRPCError(f"技能 '{name}' 已存在") from exc
    except Exception:
        if created_dir:
            try:
                os.unlink(f"{name}/SKILL.md", dir_fd=root_fd)
            except OSError:
                pass
            try:
                os.rmdir(name, dir_fd=root_fd)
            except OSError:
                pass
        raise
    finally:
        os.close(root_fd)
    return _SKILLS_ROOT / name / "SKILL.md"


async def _create_skill(raw: Mapping[str, Any]) -> SkillToolRPCOutcome:
    values = _arguments(
        raw,
        allowed=frozenset({"name", "description", "body", "tags", "always"}),
        required=frozenset({"name", "description", "body"}),
    )
    name = _string_argument(values, "name", max_chars=80)
    if not _SKILL_NAME_RE.fullmatch(name):
        return _failure("名称只能包含小写字母、数字和连字符")
    description = _string_argument(
        values,
        "description",
        max_chars=_MAX_DESCRIPTION_CHARS,
    )
    body = _string_argument(
        values,
        "body",
        max_chars=_MAX_BODY_CHARS,
    )
    tags = _string_argument(
        values,
        "tags",
        default="",
        max_chars=_MAX_TAGS_CHARS,
        allow_empty=True,
    )
    always = values.get("always", False)
    if not isinstance(always, bool):
        raise SkillToolRPCError("always must be a boolean")
    path = _write_new_skill(
        name=name,
        description=description,
        body=body,
        tags=tags,
        always=always,
    )
    return SkillToolRPCOutcome(_json_result({
        "success": True,
        "message": f"技能 '{name}' 创建成功。下次 task 启动时自动可用。",
        "path": str(path),
    }))


async def _distill(raw: Mapping[str, Any]) -> SkillToolRPCOutcome:
    values = _arguments(raw, allowed=frozenset({"days"}))
    days = _positive_int_argument(values, "days", default=30, maximum=3650)
    async with async_session() as distill_db:
        result = await analyze_patterns(distill_db, days=days)
    return SkillToolRPCOutcome(_json_result(result))


async def _owned_by_active_admin(task: Task, db: AsyncSession) -> bool:
    owner_id = task.created_by
    if (
        owner_id is None
        or isinstance(owner_id, bool)
        or not isinstance(owner_id, int)
        or owner_id <= 0
    ):
        return False
    # The route already holds the exact Task generation writer fence. Acquire
    # the owner fence second (Task -> User lock order) so demotion/deactivation
    # cannot win between authorization and a global filesystem/distill effect.
    fenced = await db.execute(
        update(User)
        .where(
            User.id == owner_id,
            User.is_active.is_(True),
            User.role.in_(["admin", "super_admin"]),
        )
        .values(role=User.role)
    )
    if fenced.rowcount != 1:
        return False
    owner = await db.get(User, owner_id, populate_existing=True)
    return bool(
        owner is not None
        and owner.is_active
        and owner.role in {"admin", "super_admin"}
    )


async def _toggle_skill(
    task: Task,
    raw: Mapping[str, Any],
    *,
    enabled: bool,
) -> SkillToolRPCOutcome:
    values = _arguments(
        raw,
        allowed=frozenset({"skill_name"}),
        required=frozenset({"skill_name"}),
    )
    skill_name = _string_argument(values, "skill_name", max_chars=160)
    current = {
        str(name): bool(value)
        for name, value in (task.enabled_skills or {}).items()
        if isinstance(name, str)
    }
    if enabled:
        provider = (task.provider or "claude").lower()
        if not skill_supported(
            provider,
            skill_name,
            codex_monitor_enabled=_codex_monitor_enabled(task),
        ):
            return _failure(
                f"Skill '{skill_name}' is not supported by provider {provider} "
                "for this Task scope"
            )
        if current.get(skill_name):
            return SkillToolRPCOutcome(_json_result({
                "success": True,
                "message": f"{skill_name} 已经是启用状态",
            }))
        current[skill_name] = True
        message = f"已启用 {skill_name}"
    else:
        command = COMMAND_REGISTRY.get(skill_name)
        if command is not None and command.always_available:
            return _failure(f"{skill_name} 是内置命令，不可禁用")
        if not current.get(skill_name):
            return SkillToolRPCOutcome(_json_result({
                "success": True,
                "message": f"{skill_name} 已经是禁用状态",
            }))
        current.pop(skill_name, None)
        message = f"已禁用 {skill_name}"
    return SkillToolRPCOutcome(
        _json_result({"success": True, "message": message}),
        enabled_skills=current,
    )


async def execute_skill_tool_rpc(
    task: Task,
    tool_name: str,
    arguments: Mapping[str, Any],
    db: AsyncSession,
) -> SkillToolRPCOutcome:
    """Execute one allow-listed Skills MCP call inside the Manager process."""

    if tool_name not in SKILL_TOOL_RPC_NAMES:
        return _failure(f"Unknown CCM skill tool: {tool_name}")
    try:
        if tool_name == "ccm_command_help":
            return await _command_help(task, arguments)
        if tool_name == "ccm_read_skill":
            return await _read_skill(task, arguments)
        if tool_name == "ccm_read_user_skill":
            return await _read_user_skill(task, arguments)
        if tool_name == "ccm_create_skill":
            if not await _owned_by_active_admin(task, db):
                return _failure(
                    "Active administrator ownership is required for global "
                    "skill operations"
                )
            return await _create_skill(arguments)
        if tool_name == "ccm_distill":
            if not await _owned_by_active_admin(task, db):
                return _failure(
                    "Active administrator ownership is required for global "
                    "skill operations"
                )
            return await _distill(arguments)
        if tool_name == "ccm_enable_skill":
            return await _toggle_skill(task, arguments, enabled=True)
        return await _toggle_skill(task, arguments, enabled=False)
    except SkillToolRPCError as exc:
        return _failure(str(exc))
    except Exception:
        logger.exception(
            "Manager-side CCM skill tool failed (task_id=%s tool=%s)",
            task.id,
            tool_name,
        )
        return _failure("CCM skill tool failed inside the Manager")
