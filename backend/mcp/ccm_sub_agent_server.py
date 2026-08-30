"""CCM Sub-Agent MCP Server — Sub-Agent 专用工具。

Sub-Agent 通过这些工具与 CCM 系统通信：报告进度、提交结果、获取上下文。

Usage:
    python -m backend.mcp.ccm_sub_agent_server \
        --sub-agent-session-id 42 --task-id 7 \
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

mcp = FastMCP("ccm-sub-agent", instructions="Sub-Agent tools for one-shot task execution")

_SUB_AGENT_SESSION_ID: int = 0
_TASK_ID: int = 0
_API_BASE: str = "http://localhost:8000"
_AUTH_TOKEN: str = ""


def _api_url(path: str) -> str:
    return f"{_API_BASE}/api/tasks/{_TASK_ID}/sub-agent-sessions/{_SUB_AGENT_SESSION_ID}{path}"


def _headers() -> dict[str, str]:
    if _AUTH_TOKEN:
        return {"Authorization": f"Bearer {_AUTH_TOKEN}"}
    return {}


@mcp.tool()
async def report_progress(summary: str) -> str:
    """报告当前任务进度。进度信息会实时显示在主 session 的聊天界面中。

    Args:
        summary: 当前进度摘要（如"已审查 15/42 个文件，发现 2 个高危问题"）
    """
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                _api_url("/progress"),
                headers=_headers(),
                json={"summary": summary},
            )
            if resp.status_code in (400, 404):
                return json.dumps({
                    "success": False,
                    "session_ended": True,
                    "error": f"Session is no longer active (HTTP {resp.status_code}). Call submit_result or stop.",
                }, ensure_ascii=False)
            resp.raise_for_status()
            return json.dumps({
                "success": True,
                "message": "Progress reported.",
            }, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)}, ensure_ascii=False)


@mcp.tool()
async def submit_result(result: str, success: bool = True) -> str:
    """提交最终结果并结束 Sub-Agent。结果会注入主 session 唤醒主 Agent。
    调用此工具后，Sub-Agent 进程将自动终止。

    Args:
        result: 最终结果（Markdown 格式）
        success: 任务是否成功完成（默认 True）
    """
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                _api_url("/result"),
                headers=_headers(),
                json={
                    "result": result,
                    "status": "completed" if success else "failed",
                },
            )
            if resp.status_code in (400, 404):
                return "Session already ended. Stop all activity now."
            resp.raise_for_status()
            return "Result submitted. Your task is done — stop all activity now."
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)}, ensure_ascii=False)


@mcp.tool()
async def get_context() -> str:
    """获取任务上下文，包括项目信息、task 描述等。

    返回: 格式化的上下文信息，帮助你了解任务背景。
    """
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                _api_url("/context"),
                headers=_headers(),
            )
            resp.raise_for_status()
            data = resp.json()
            return json.dumps({
                "success": True,
                "task_description": data.get("task_description", ""),
                "task_prompt": data.get("task_prompt", ""),
                "project_name": data.get("project_name", ""),
                "project_path": data.get("project_path", ""),
                "sub_agent_prompt": data.get("sub_agent_prompt", ""),
                "sub_agent_context": data.get("sub_agent_context", ""),
            }, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)}, ensure_ascii=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CCM Sub-Agent MCP Server")
    parser.add_argument("--sub-agent-session-id", type=int, required=True)
    parser.add_argument("--task-id", type=int, required=True)
    parser.add_argument("--api-base", default="http://localhost:8000")
    parser.add_argument("--auth-token", default="")
    args = parser.parse_args()

    _SUB_AGENT_SESSION_ID = args.sub_agent_session_id
    _TASK_ID = args.task_id
    _API_BASE = args.api_base
    _AUTH_TOKEN = os.environ.get("CCM_INTERNAL_SERVICE_TOKEN", "") or args.auth_token

    mcp.run(transport="stdio")
