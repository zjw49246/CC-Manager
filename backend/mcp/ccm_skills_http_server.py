"""Standalone HTTP-only CCM Skills MCP wrapper.

This file is snapshotted by the Manager at startup and executed from a private,
content-addressed Task runtime path.  Keep it independent from ``backend.*``:
all database, filesystem, and validation effects belong to the authenticated
Manager RPC endpoint.
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any

import httpx
from mcp.server.fastmcp import FastMCP


mcp = FastMCP("ccm-skills", instructions="CCM task skill tools")

_TASK_ID = 0
_API_BASE = "http://localhost:8000"
_AUTH_TOKEN = ""


def _api_url(path: str) -> str:
    return f"{_API_BASE}/api/tasks/{_TASK_ID}{path}"


def _headers() -> dict[str, str]:
    return (
        {"Authorization": f"Bearer {_AUTH_TOKEN}"}
        if _AUTH_TOKEN
        else {}
    )


def _error(exc: Exception) -> str:
    detail = ""
    if isinstance(exc, httpx.HTTPStatusError):
        try:
            payload = exc.response.json()
            if isinstance(payload, dict):
                value = payload.get("detail")
                if isinstance(value, str):
                    detail = value
        except Exception:
            detail = exc.response.text
    return json.dumps(
        {"success": False, "error": detail or str(exc)},
        ensure_ascii=False,
    )


async def _call_skill_tool(tool: str, arguments: dict[str, Any]) -> str:
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                _api_url("/internal/skill-tools"),
                headers=_headers(),
                json={"tool": tool, "arguments": arguments},
            )
            response.raise_for_status()
            payload = response.json()
        result = payload.get("result") if isinstance(payload, dict) else None
        if not isinstance(result, str):
            raise RuntimeError("CCM skill tool returned an invalid response")
        return result
    except Exception as exc:
        return _error(exc)


@mcp.tool()
async def ccm_command_help() -> str:
    """列出可用 CCM 命令、技能及其启用状态。"""

    return await _call_skill_tool("ccm_command_help", {})


@mcp.tool()
async def ccm_read_skill(skill_name: str) -> str:
    """读取一个已授权技能的完整内容。

    Args:
        skill_name: 技能名称（如 monitor, code-review）
    """

    return await _call_skill_tool(
        "ccm_read_skill",
        {"skill_name": skill_name},
    )


@mcp.tool()
async def ccm_read_user_skill(skill_id: int) -> str:
    """读取当前 Task 已选择的用户自定义 Skill。

    Args:
        skill_id: Task 技能目录中显示的用户 Skill ID
    """

    return await _call_skill_tool(
        "ccm_read_user_skill",
        {"skill_id": skill_id},
    )


@mcp.tool()
async def ccm_create_skill(
    name: str,
    description: str,
    body: str,
    tags: str = "",
    always: bool = False,
) -> str:
    """创建一个新的 CCM 技能。

    Args:
        name: 英文小写字母、数字和连字符组成的技能名
        description: 说明何时应使用该技能
        body: Markdown 技能正文
        tags: 逗号分隔标签
        always: 是否作为始终启用的技能
    """

    return await _call_skill_tool("ccm_create_skill", {
        "name": name,
        "description": description,
        "body": body,
        "tags": tags,
        "always": always,
    })


@mcp.tool()
async def ccm_distill(days: int = 30) -> str:
    """分析近期使用模式并生成技能提炼建议。

    Args:
        days: 分析最近多少天
    """

    return await _call_skill_tool("ccm_distill", {"days": days})


@mcp.tool()
async def ccm_enable_skill(skill_name: str) -> str:
    """为当前 Task 持久启用一个受支持的技能。"""

    return await _call_skill_tool(
        "ccm_enable_skill",
        {"skill_name": skill_name},
    )


@mcp.tool()
async def ccm_disable_skill(skill_name: str) -> str:
    """为当前 Task 持久禁用一个非内置技能。"""

    return await _call_skill_tool(
        "ccm_disable_skill",
        {"skill_name": skill_name},
    )


@mcp.tool()
async def create_monitor(
    description: str,
    context: str = "",
    interval: int = 120,
    max_checks: int = 50,
) -> str:
    """启动一个后台 Monitor 子 session。"""

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(
                _api_url("/monitor-sessions"),
                headers=_headers(),
                json={
                    "description": description,
                    "monitor_context": context,
                    "interval": interval,
                    "max_checks": max_checks,
                },
            )
            response.raise_for_status()
            data = response.json()
        return json.dumps({
            "success": True,
            "monitor_id": data["id"],
            "status": "created",
            "message": (
                f"Monitor #{data['id']} 已启动，每 {interval} 秒检查一次，"
                f"最多 {max_checks} 次。"
            ),
        }, ensure_ascii=False)
    except Exception as exc:
        return _error(exc)


@mcp.tool()
async def check_monitors() -> str:
    """查询当前 Task 下所有 Monitor 的最新状态。"""

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(
                _api_url("/monitor-sessions"),
                headers=_headers(),
            )
            response.raise_for_status()
            sessions = response.json()
        if not sessions:
            return json.dumps({
                "success": True,
                "monitors": [],
                "message": "当前没有活跃的监控。",
            }, ensure_ascii=False)
        return json.dumps({
            "success": True,
            "monitors": [{
                "monitor_id": session["id"],
                "description": session["description"],
                "status": session["status"],
                "checks_done": session["checks_done"],
                "max_checks": session["max_checks"],
                "last_summary": session.get("last_summary"),
            } for session in sessions],
        }, ensure_ascii=False)
    except Exception as exc:
        return _error(exc)


@mcp.tool()
async def stop_monitor(monitor_id: int) -> str:
    """停止指定 Monitor 子 session。"""

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.delete(
                _api_url(f"/monitor-sessions/{monitor_id}"),
                headers=_headers(),
            )
            response.raise_for_status()
        return json.dumps({
            "success": True,
            "status": "cancelled",
            "message": f"Monitor #{monitor_id} 已停止。",
        }, ensure_ascii=False)
    except Exception as exc:
        return _error(exc)


@mcp.tool()
async def create_sub_agent(
    task_description: str,
    context: str = "",
    readonly: bool = False,
    model: str | None = None,
) -> str:
    """创建一个一次性 Sub-Agent，并在后台跟踪结果。"""

    del readonly  # The Manager owns the exact child policy.
    name = task_description[:60].strip()
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(
                _api_url("/sub-agent-sessions"),
                headers=_headers(),
                json={
                    "name": name,
                    "prompt": task_description,
                    "context": context,
                    "model": model,
                },
            )
            response.raise_for_status()
            data = response.json()
        return json.dumps({
            "success": True,
            "sub_agent_id": data["id"],
            "status": "created",
            "message": (
                f"Sub-Agent '{name}' (#{data['id']}) 已创建，正在执行任务。"
                "用 check_sub_agents() 查看进度。"
            ),
        }, ensure_ascii=False)
    except Exception as exc:
        return _error(exc)


@mcp.tool()
async def check_sub_agents() -> str:
    """查看当前 Task 下 Sub-Agent 的状态和进度。"""

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(
                _api_url("/sub-agent-sessions"),
                headers=_headers(),
                params={"agent_type": "sub_agent"},
            )
            response.raise_for_status()
            sessions = response.json()
        if not sessions:
            return json.dumps({
                "success": True,
                "sub_agents": [],
                "message": "当前没有 Sub-Agent。",
            }, ensure_ascii=False)
        return json.dumps({
            "success": True,
            "sub_agents": [{
                "sub_agent_id": session["id"],
                "name": session["description"],
                "status": session["status"],
                "progress_count": session["checks_done"],
                "last_progress": session.get("last_summary"),
            } for session in sessions],
        }, ensure_ascii=False)
    except Exception as exc:
        return _error(exc)


@mcp.tool()
async def stop_sub_agent(sub_agent_id: int) -> str:
    """停止一个正在运行的 Sub-Agent。"""

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.delete(
                _api_url(f"/sub-agent-sessions/{sub_agent_id}"),
                headers=_headers(),
            )
            response.raise_for_status()
        return json.dumps({
            "success": True,
            "status": "stopped",
            "message": f"Sub-Agent #{sub_agent_id} 已停止。",
        }, ensure_ascii=False)
    except Exception as exc:
        return _error(exc)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CCM Skills MCP Server")
    parser.add_argument("--task-id", type=int, required=True)
    parser.add_argument("--api-base", default="http://localhost:8000")
    args = parser.parse_args()

    _TASK_ID = args.task_id
    _API_BASE = args.api_base
    _AUTH_TOKEN = os.environ.get("CCM_INTERNAL_SERVICE_TOKEN", "")
    mcp.run(transport="stdio")
