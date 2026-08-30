"""CCM Monitor Agent MCP Server — 子 Agent 专用工具。

子 Agent 通过这些工具与 CCM 系统通信：报告状态、标记完成、获取上下文。

Usage:
    python -m backend.mcp.ccm_monitor_agent_server \
        --monitor-session-id 42 --task-id 7 \
        --api-base http://localhost:8000 --auth-token xxx
"""
import argparse
import json
import logging
import os
import sys

import httpx
from mcp.server.fastmcp import FastMCP

logger = logging.getLogger(__name__)

mcp = FastMCP("ccm-monitor-agent", instructions="Monitor sub-agent tools")

_MONITOR_SESSION_ID: int = 0
_TASK_ID: int = 0
_TURN_GENERATION: int | None = None
_API_BASE: str = "http://localhost:8000"
_AUTH_TOKEN: str = ""


def _api_url(path: str) -> str:
    return f"{_API_BASE}/api/tasks/{_TASK_ID}/monitor-sessions/{_MONITOR_SESSION_ID}{path}"


def _headers() -> dict[str, str]:
    if _AUTH_TOKEN:
        return {"Authorization": f"Bearer {_AUTH_TOKEN}"}
    return {}


@mcp.tool()
async def report_status(summary: str, is_important: bool = False) -> str:
    """报告当前监控状态。每次检查后调用。

    Args:
        summary: 状态摘要（如"编译完成 90%"、"测试 3/10 通过"）
        is_important: 是否为重要变化（状态转变、错误、完成等设为 True）
    """
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                _api_url("/checks"),
                headers=_headers(),
                json={
                    "summary": summary,
                    "status": "success",
                    "is_important": is_important,
                    "turn_generation": _TURN_GENERATION,
                },
            )
            if resp.status_code in (400, 404, 409):
                return json.dumps({
                    "success": False,
                    "session_ended": True,
                    "error": f"Session or turn is no longer active (HTTP {resp.status_code}). Stop all activity now.",
                }, ensure_ascii=False)
            resp.raise_for_status()
            data = resp.json()
            return json.dumps({
                "success": True,
                "checks_done": data.get("checks_done", 0),
                "remaining": data.get("remaining", 0),
                "message": "Status reported.",
            }, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)}, ensure_ascii=False)


@mcp.tool()
async def mark_complete(reason: str) -> str:
    """标记监控任务完成。调用后立即停止所有活动。

    当监控目标已完成、失败、或不再需要监控时调用。

    Args:
        reason: 完成原因（如"编译成功"、"进程已退出"、"达到最大检查次数"）
    """
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                _api_url("/complete"),
                headers=_headers(),
                json={
                    "reason": reason,
                    "turn_generation": _TURN_GENERATION,
                },
            )
            if resp.status_code in (400, 404, 409):
                return "Session already ended. Stop all activity now."
            resp.raise_for_status()
            return "Session completed. Your task is done — stop all activity now."
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)}, ensure_ascii=False)


@mcp.tool()
async def get_context() -> str:
    """获取当前监控会话的配置和上下文信息。

    返回: description, monitor_context, checks_done, max_checks, status。
    用于了解监控目标和当前进度。
    """
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                _api_url(""),
                headers=_headers(),
            )
            resp.raise_for_status()
            data = resp.json()
            return json.dumps({
                "success": True,
                "description": data.get("description", ""),
                "monitor_context": data.get("monitor_context", ""),
                "checks_done": data.get("checks_done", 0),
                "max_checks": data.get("max_checks", 50),
                "status": data.get("status", "unknown"),
            }, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)}, ensure_ascii=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CCM Monitor Agent MCP Server")
    parser.add_argument("--monitor-session-id", type=int, required=True)
    parser.add_argument("--task-id", type=int, required=True)
    parser.add_argument("--turn-generation", type=int)
    parser.add_argument("--api-base", default="http://localhost:8000")
    parser.add_argument("--auth-token", default="")
    args = parser.parse_args()

    _MONITOR_SESSION_ID = args.monitor_session_id
    _TASK_ID = args.task_id
    _TURN_GENERATION = args.turn_generation
    _API_BASE = args.api_base
    _AUTH_TOKEN = os.environ.get("CCM_INTERNAL_SERVICE_TOKEN", "") or args.auth_token

    mcp.run(transport="stdio")
