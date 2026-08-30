"""CCM Skills MCP Server — 给 Task 的 Claude 主 session 注入工具能力。

Usage:
    python -m backend.mcp.ccm_skills_server --task-id 123 --api-base http://localhost:8000
"""
import argparse
import json
import os
import logging
import sys

import httpx
from mcp.server.fastmcp import FastMCP

logger = logging.getLogger(__name__)

mcp = FastMCP("ccm-skills", instructions="CCM task skill tools")

_TASK_ID: int = 0
_API_BASE: str = "http://localhost:8000"
_AUTH_TOKEN: str = ""


def _api_url(path: str) -> str:
    return f"{_API_BASE}/api/tasks/{_TASK_ID}{path}"


def _headers() -> dict[str, str]:
    if _AUTH_TOKEN:
        return {"Authorization": f"Bearer {_AUTH_TOKEN}"}
    return {}


async def _get_task_data() -> dict:
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(_api_url(""), headers=_headers())
        resp.raise_for_status()
        data = resp.json()
    return data if isinstance(data, dict) else {}


def _codex_monitor_enabled(task_data: dict) -> bool:
    from backend.config import settings
    from backend.services.skill_context import (
        codex_monitor_supported_for_scope,
    )

    return codex_monitor_supported_for_scope(
        provider=task_data.get("provider"),
        worker_id=task_data.get("worker_id"),
        shared_from_id=task_data.get("shared_from_id"),
        metadata=task_data.get("metadata_"),
        codex_main_mcp_enabled=settings.codex_main_mcp_enabled,
    )


@mcp.tool()
async def ccm_command_help() -> str:
    """列出所有可用的 CCM 命令和技能。

    返回：
    - 内置命令列表（$help 等）
    - 可用技能列表（从 SKILL.md 文件加载）及启用状态
    用户可以通过 $command_name 语法使用命令，或通过 ccm_read_skill 读取技能详情。
    """
    try:
        from backend.services.command_registry import COMMAND_REGISTRY
        from backend.services.skill_loader import discover_skills
        from backend.services.skill_context import skill_supported

        task_data = await _get_task_data()
        enabled_skills = task_data.get("enabled_skills") or {}
        provider = task_data.get("provider") or "claude"
        codex_monitor_enabled = _codex_monitor_enabled(task_data)

        # Built-in commands
        commands = []
        for cmd in COMMAND_REGISTRY.values():
            if any(
                not skill_supported(
                    provider,
                    skill_name,
                    codex_monitor_enabled=codex_monitor_enabled,
                )
                for skill_name in cmd.required_skills
            ):
                continue
            commands.append({
                "command": f"${cmd.name}",
                "description": cmd.description,
                "type": "command",
            })

        # Skills from SKILL.md files
        skills = discover_skills(
            exclude=(
                {"monitor"}
                if provider == "codex" and not codex_monitor_enabled
                else None
            )
        )
        skill_list = []
        for name, skill in skills.items():
            skill_list.append({
                "name": name,
                "description": skill.description.strip()[:150],
                "enabled": enabled_skills.get(name, False),
                "commands": [c["name"] for c in skill.ccm.commands],
                "type": "skill",
            })

        return json.dumps({
            "success": True,
            "commands": commands,
            "skills": skill_list,
            "usage": "用 $命令名 触发命令，用 ccm_read_skill(name) 读取技能详情，用 ccm_enable_skill(name) 启用技能。",
        }, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)}, ensure_ascii=False)


@mcp.tool()
async def ccm_read_skill(skill_name: str) -> str:
    """读取一个技能的完整内容。

    技能目录（name + description）在 system prompt 中可见。
    当需要某个技能的详细指南时，调用此工具获取全文。

    Args:
        skill_name: 技能名称（如 monitor, code-review）
    """
    try:
        from backend.services.skill_context import skill_supported
        task_data = await _get_task_data()
        provider = (task_data.get("provider") or "claude").lower()
        if not skill_supported(
            provider,
            skill_name,
            codex_monitor_enabled=_codex_monitor_enabled(task_data),
        ):
            return json.dumps({
                "success": False,
                "error": (
                    f"Skill '{skill_name}' is not supported by provider "
                    f"{provider}"
                ),
            }, ensure_ascii=False)
        from backend.services.skill_loader import discover_skills
        skills = discover_skills()
        skill = skills.get(skill_name)
        if not skill:
            available = ", ".join(skills.keys())
            return json.dumps({
                "success": False,
                "error": f"技能 '{skill_name}' 不存在。可用技能: {available}",
            }, ensure_ascii=False)
        enabled_skills = task_data.get("enabled_skills") or {}
        from backend.config import settings
        if (
            provider == "codex"
            and not settings.codex_main_mcp_enabled
            and skill_name != "sub-agent"
        ):
            return json.dumps({
                "success": False,
                "error": (
                    f"Skill '{skill_name}' is unavailable while Codex "
                    "main-task MCP is disabled"
                ),
            }, ensure_ascii=False)
        if (
            not enabled_skills.get(skill_name, False)
            and not skill.ccm.always
        ):
            return json.dumps({
                "success": False,
                "error": f"Skill '{skill_name}' is not enabled for this task",
            }, ensure_ascii=False)
        # Log usage
        try:
            from backend.services.skill_curator import log_skill_usage
            from backend.database import async_session
            async with async_session() as udb:
                await log_skill_usage(udb, skill_name, "read", task_id=_TASK_ID)
        except Exception:
            pass

        # Merge DB lessons into skill body
        body_with_lessons = skill.body
        try:
            from backend.services.skill_evolution import get_lessons_for_skill
            from backend.database import async_session
            async with async_session() as db:
                lessons = await get_lessons_for_skill(skill_name, db)
                if lessons:
                    body_with_lessons += "\n\n## Learned Lessons\n"
                    body_with_lessons += "\n".join(f"- {l}" for l in lessons)
        except Exception:
            pass

        return json.dumps({
            "success": True,
            "name": skill.name,
            "description": skill.description,
            "body": body_with_lessons,
            "commands": skill.ccm.commands,
            "tags": skill.ccm.tags,
        }, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)}, ensure_ascii=False)


@mcp.tool()
async def ccm_read_user_skill(skill_id: int) -> str:
    """读取一个用户自定义 Skill 的完整内容。

    用户 Skill 目录（name + description）在 system prompt 中可见。
    当需要某个 Skill 的详细内容时，调用此工具获取全文。

    Args:
        skill_id: Skill ID（在目录中显示为 id=N）
    """
    try:
        from backend.services.skill_context import (
            normalize_user_skill_ids,
            user_skill_snapshots_from_metadata,
        )

        task_data = await _get_task_data()
        selected = normalize_user_skill_ids(
            task_data.get("selected_user_skills")
        )
        if skill_id not in selected:
            return json.dumps({
                "success": False,
                "error": f"User skill {skill_id} is not selected for this task",
            }, ensure_ascii=False)
        metadata = task_data.get("metadata_")
        snapshots = {
            snapshot.id: snapshot
            for snapshot in user_skill_snapshots_from_metadata(
                metadata
            )
        }
        snapshot = snapshots.get(skill_id)
        if snapshot is not None:
            return json.dumps({
                "success": True,
                "id": snapshot.id,
                "name": snapshot.name,
                "description": snapshot.description,
                "content": snapshot.content,
            }, ensure_ascii=False)
        if (
            isinstance(metadata, dict)
            and "ccm_user_skill_snapshots" in metadata
        ):
            return json.dumps({
                "success": False,
                "error": (
                    f"User skill {skill_id} is unavailable in the "
                    "authoritative task snapshot"
                ),
            }, ensure_ascii=False)

        from backend.database import async_session
        from backend.models.user_skill import UserSkill
        async with async_session() as db:
            skill = await db.get(UserSkill, skill_id)
        if not skill:
            return json.dumps({"success": False, "error": f"User skill {skill_id} not found"}, ensure_ascii=False)
        return json.dumps({
            "success": True,
            "id": skill.id,
            "name": skill.name,
            "description": skill.description,
            "content": skill.content,
        }, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)}, ensure_ascii=False)


@mcp.tool()
async def ccm_create_skill(
    name: str,
    description: str,
    body: str,
    tags: str = "",
    always: bool = False,
) -> str:
    """创建一个新的技能（SKILL.md 文件）。

    新技能保存在 CCM 仓库的 skills/ 目录下。
    创建后立即可用（下次 task 启动时会发现它）。

    Args:
        name: 技能名称（英文小写+连字符，如 code-review）
        description: 技能描述（描述"何时使用"而非"做什么"）
        body: 技能内容（Markdown 格式，包含规则和指南）
        tags: 标签（逗号分隔，如 "quality,review"）
        always: 是否始终注入 system prompt
    """
    import os
    import re
    from pathlib import Path

    # Validate name
    if not re.match(r'^[a-z0-9][a-z0-9-]*$', name):
        return json.dumps({"success": False, "error": "名称只能包含小写字母、数字和连字符"}, ensure_ascii=False)

    # Find skills directory
    ccm_root = Path(__file__).resolve().parents[2]
    skill_dir = ccm_root / "skills" / name
    if skill_dir.exists():
        return json.dumps({"success": False, "error": f"技能 '{name}' 已存在"}, ensure_ascii=False)

    # Build SKILL.md content
    tag_list = [t.strip() for t in tags.split(",") if t.strip()] if tags else []
    frontmatter = {
        "name": name,
        "description": description,
    }
    ccm_section = {
        "always": always,
        "priority": 5,
        "version": 1,
        "tags": tag_list,
    }

    lines = ["---"]
    lines.append(f"name: {name}")
    lines.append(f"description: >")
    for line in description.strip().split("\n"):
        lines.append(f"  {line}")
    lines.append("")
    lines.append("ccm:")
    lines.append(f"  always: {str(always).lower()}")
    lines.append(f"  priority: 5")
    lines.append(f"  version: 1")
    if tag_list:
        lines.append(f"  tags: [{', '.join(tag_list)}]")
    lines.append("---")
    lines.append("")
    lines.append(body)
    lines.append("")
    lines.append("## Lessons Learned")
    lines.append("<!-- 自进化系统自动追加 -->")

    try:
        skill_dir.mkdir(parents=True, exist_ok=True)
        (skill_dir / "SKILL.md").write_text("\n".join(lines), encoding="utf-8")
        return json.dumps({
            "success": True,
            "message": f"技能 '{name}' 创建成功。下次 task 启动时自动可用。",
            "path": str(skill_dir / "SKILL.md"),
        }, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)}, ensure_ascii=False)


@mcp.tool()
async def ccm_distill(days: int = 30) -> str:
    """分析近期使用模式，发现重复工作流和高错误率工具，提炼新技能建议。

    返回分析报告，包括：
    - 高频使用的工具
    - 高错误率的工具（建议创建对应 skill）
    - 常一起使用的工具组合（建议创建工作流 skill）

    如果有好的建议，可以用 ccm_create_skill 创建新技能。

    Args:
        days: 分析多少天的历史（默认 30 天）
    """
    try:
        from backend.services.skill_distill import analyze_patterns
        from backend.database import async_session
        async with async_session() as db:
            result = await analyze_patterns(db, days=days)
        return json.dumps(result, ensure_ascii=False, default=str)
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)}, ensure_ascii=False)


@mcp.tool()
async def ccm_enable_skill(skill_name: str) -> str:
    """为当前 task 启用一个工具/技能（持久生效）。

    Args:
        skill_name: 要启用的工具名称（如 monitor, help）
    """
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(_api_url(""), headers=_headers())
            resp.raise_for_status()
            task_data = resp.json()
            from backend.services.skill_context import skill_supported

            provider = (task_data.get("provider") or "claude").lower()
            if not skill_supported(
                provider,
                skill_name,
                codex_monitor_enabled=_codex_monitor_enabled(task_data),
            ):
                return json.dumps({
                    "success": False,
                    "error": (
                        f"Skill '{skill_name}' is not supported by provider "
                        f"{provider} for this Task scope"
                    ),
                }, ensure_ascii=False)
            skills = task_data.get("enabled_skills") or {}
            if skills.get(skill_name):
                return json.dumps({"success": True, "message": f"{skill_name} 已经是启用状态"}, ensure_ascii=False)
            skills[skill_name] = True
            resp = await client.put(
                _api_url("/internal/enabled-skills"),
                headers=_headers(),
                json={"enabled_skills": skills},
            )
            resp.raise_for_status()
            return json.dumps({"success": True, "message": f"已启用 {skill_name}"}, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)}, ensure_ascii=False)


@mcp.tool()
async def ccm_disable_skill(skill_name: str) -> str:
    """为当前 task 禁用一个工具/技能（持久生效）。

    Args:
        skill_name: 要禁用的工具名称（如 monitor）。help 不可禁用。
    """
    try:
        from backend.services.command_registry import COMMAND_REGISTRY
        cmd = COMMAND_REGISTRY.get(skill_name)
        if cmd and cmd.always_available:
            return json.dumps({"success": False, "error": f"{skill_name} 是内置命令，不可禁用"}, ensure_ascii=False)
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(_api_url(""), headers=_headers())
            resp.raise_for_status()
            task_data = resp.json()
            skills = task_data.get("enabled_skills") or {}
            if not skills.get(skill_name):
                return json.dumps({"success": True, "message": f"{skill_name} 已经是禁用状态"}, ensure_ascii=False)
            skills.pop(skill_name, None)
            resp = await client.put(
                _api_url("/internal/enabled-skills"),
                headers=_headers(),
                json={"enabled_skills": skills},
            )
            resp.raise_for_status()
            return json.dumps({"success": True, "message": f"已禁用 {skill_name}"}, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)}, ensure_ascii=False)


@mcp.tool()
async def create_monitor(
    description: str,
    context: str = "",
    interval: int = 120,
    max_checks: int = 50,
) -> str:
    """启动一个后台监控子 session。不阻塞当前对话。

    子 session 是只读的（不能 Edit/Write），会定期检查进程状态和日志，
    将摘要报告写入数据库。你可以随时用 check_monitors() 查看最新状态。

    Args:
        description: 监控什么（如"编译进度"、"测试运行"、"后台训练"）
        context: 额外上下文（如日志路径、进程名、PID、如何判断完成）
        interval: 检查间隔秒数（默认 120）
        max_checks: 最大检查次数（默认 50，达到后自动停止）
    """
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                _api_url("/monitor-sessions"),
                headers=_headers(),
                json={
                    "description": description,
                    "monitor_context": context,
                    "interval": interval,
                    "max_checks": max_checks,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            return json.dumps({
                "success": True,
                "monitor_id": data["id"],
                "status": "created",
                "message": f"Monitor #{data['id']} 已启动，每 {interval} 秒检查一次，最多 {max_checks} 次。",
            }, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)}, ensure_ascii=False)


@mcp.tool()
async def check_monitors() -> str:
    """查询当前 task 下所有活跃的 monitor 子 session 的最新状态。

    返回每个 monitor 的: id, description, status, checks_done, last_summary。
    当用户询问监控情况、或你需要了解后台任务进展时调用此工具。
    """
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(_api_url("/monitor-sessions"), headers=_headers())
            resp.raise_for_status()
            sessions = resp.json()
            if not sessions:
                return json.dumps({"success": True, "monitors": [], "message": "当前没有活跃的监控。"}, ensure_ascii=False)
            summary = []
            for s in sessions:
                summary.append({
                    "monitor_id": s["id"],
                    "description": s["description"],
                    "status": s["status"],
                    "checks_done": s["checks_done"],
                    "max_checks": s["max_checks"],
                    "last_summary": s.get("last_summary"),
                })
            return json.dumps({"success": True, "monitors": summary}, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)}, ensure_ascii=False)


@mcp.tool()
async def stop_monitor(monitor_id: int) -> str:
    """停止指定的 monitor 子 session。

    当后台任务已完成或不再需要监控时调用。

    Args:
        monitor_id: 要停止的 monitor ID（从 create_monitor 或 check_monitors 获取）
    """
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.delete(
                _api_url(f"/monitor-sessions/{monitor_id}"),
                headers=_headers(),
            )
            resp.raise_for_status()
            return json.dumps({
                "success": True,
                "status": "cancelled",
                "message": f"Monitor #{monitor_id} 已停止。",
            }, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)}, ensure_ascii=False)


@mcp.tool()
async def create_sub_agent(
    task_description: str,
    context: str = "",
    readonly: bool = False,
    model: str | None = None,
) -> str:
    """创建一个 Sub-Agent 执行一次性任务。Sub-Agent 使用主任务对应的
    Claude Code 或 Codex provider 独立运行，会自主完成任务并将结果返回给你。
    你可以继续工作，稍后用 check_sub_agents 查看进度。

    Args:
        task_description: 任务描述（如"审查 src/ 下所有文件的 SQL 注入风险"）
        context: 额外上下文（可选，附加到 prompt 前）
        readonly: 是否只读模式（默认 False，只读时禁止编辑文件）
        model: 使用的模型（可选，默认跟随 task 配置）
    """
    try:
        # Use task_description as name (truncated) and full prompt
        name = task_description[:60].strip()
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                _api_url("/sub-agent-sessions"),
                headers=_headers(),
                json={
                    "name": name,
                    "prompt": task_description,
                    "context": context,
                    "model": model,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            return json.dumps({
                "success": True,
                "sub_agent_id": data["id"],
                "status": "created",
                "message": f"Sub-Agent '{name}' (#{data['id']}) 已创建，正在执行任务。用 check_sub_agents() 查看进度。",
            }, ensure_ascii=False)
    except httpx.HTTPStatusError as e:
        detail = ""
        try:
            detail = e.response.json().get("detail", "")
        except Exception:
            detail = e.response.text
        return json.dumps({"success": False, "error": detail or str(e)}, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)}, ensure_ascii=False)


@mcp.tool()
async def check_sub_agents() -> str:
    """查看当前所有 Sub-Agent 的状态、进度和结果。

    返回每个 Sub-Agent 的: id, 名称, 状态, 最新进度, 最终结果。
    """
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                _api_url("/sub-agent-sessions"),
                headers=_headers(),
                params={"agent_type": "sub_agent"},
            )
            resp.raise_for_status()
            sessions = resp.json()
            if not sessions:
                return json.dumps({"success": True, "sub_agents": [], "message": "当前没有 Sub-Agent。"}, ensure_ascii=False)
            summary = []
            for s in sessions:
                summary.append({
                    "sub_agent_id": s["id"],
                    "name": s["description"],
                    "status": s["status"],
                    "progress_count": s["checks_done"],
                    "last_progress": s.get("last_summary"),
                })
            return json.dumps({"success": True, "sub_agents": summary}, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)}, ensure_ascii=False)


@mcp.tool()
async def stop_sub_agent(sub_agent_id: int) -> str:
    """停止一个正在运行的 Sub-Agent。

    Args:
        sub_agent_id: 要停止的 Sub-Agent ID（从 create_sub_agent 或 check_sub_agents 获取）
    """
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.delete(
                _api_url(f"/sub-agent-sessions/{sub_agent_id}"),
                headers=_headers(),
            )
            resp.raise_for_status()
            return json.dumps({
                "success": True,
                "status": "stopped",
                "message": f"Sub-Agent #{sub_agent_id} 已停止。",
            }, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)}, ensure_ascii=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CCM Skills MCP Server")
    parser.add_argument("--task-id", type=int, required=True)
    parser.add_argument("--api-base", default="http://localhost:8000")
    parser.add_argument("--auth-token", default="")
    args = parser.parse_args()

    _TASK_ID = args.task_id
    _API_BASE = args.api_base
    _AUTH_TOKEN = os.environ.get("CCM_INTERNAL_SERVICE_TOKEN", "") or args.auth_token

    mcp.run(transport="stdio")
