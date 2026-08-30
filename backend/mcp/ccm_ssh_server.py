"""Task-scoped SSH MCP server.

The server never reads a private key.  Every operation is sent to CCM's
internal API, which rechecks the Task grant, capability, profile revision,
enabled state, and pinned host identity before opening a connection.

Usage:
    python -m backend.mcp.ccm_ssh_server --task-id 123 --api-base http://localhost:8000
"""

import argparse
import json
import os
import re
import secrets

import httpx
from mcp.server.fastmcp import FastMCP


mcp = FastMCP(
    "ccm-ssh",
    instructions=(
        "Use only the SSH profiles explicitly granted to this Task. "
        "List connections before choosing a profile."
    ),
)

_TASK_ID = 0
_API_BASE = "http://localhost:8000"
_AUTH_TOKEN = ""
_EFFECT_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_MAX_EXEC_OUTPUT_BYTES = 64 * 1024
_CAPABILITY_TOOLS = {
    "exec": {"new_effect_id", "run_command"},
    "read": {"list_directory", "read_file"},
    "write": {"new_effect_id", "write_file"},
}


def _restrict_tools_for_capabilities(capabilities: set[str]) -> None:
    """Hide ungranted tools in providers without an MCP tool allow-list."""

    allowed = {"list_connections"}
    for capability in capabilities:
        allowed.update(_CAPABILITY_TOOLS.get(capability, set()))
    for tool_name in {
        tool
        for tools in _CAPABILITY_TOOLS.values()
        for tool in tools
    } - allowed:
        mcp.remove_tool(tool_name)


def _url(path: str) -> str:
    return f"{_API_BASE}/api/tasks/{_TASK_ID}/ssh-access{path}"


def _headers() -> dict[str, str]:
    if not _AUTH_TOKEN:
        return {}
    return {"Authorization": f"Bearer {_AUTH_TOKEN}"}


def _error_detail(response: httpx.Response) -> str:
    try:
        detail = response.json().get("detail")
    except Exception:
        detail = None
    if isinstance(detail, dict):
        value = detail.get("message") or detail.get("error")
        if isinstance(value, str):
            return value
    if isinstance(detail, str):
        return detail
    return f"CCM SSH request failed with status {response.status_code}"


async def _request(
    method: str,
    path: str,
    *,
    body: dict | None = None,
    timeout: float = 30,
) -> dict | list:
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.request(
            method,
            _url(path),
            headers=_headers(),
            json=body,
        )
    if not response.is_success:
        raise RuntimeError(_error_detail(response))
    data = response.json()
    if not isinstance(data, (dict, list)):
        raise RuntimeError("CCM SSH returned an invalid response")
    return data


def _result(payload: dict) -> str:
    return json.dumps(payload, ensure_ascii=False)


def _mutation_effect_id(value: str) -> str:
    if not isinstance(value, str) or not _EFFECT_ID_RE.fullmatch(value):
        raise ValueError("effect_id must be exactly 32 lowercase hexadecimal characters")
    return value


def _mutation_error(exc: Exception) -> str:
    if isinstance(exc, httpx.RequestError):
        return (
            "CCM SSH did not return an authoritative result; reuse the same "
            "effect_id and do not create a replacement id"
        )
    if isinstance(exc, (RuntimeError, ValueError)):
        return str(exc)
    return "CCM SSH mutation failed without an authoritative result"


@mcp.tool()
async def list_connections() -> str:
    """List the SSH profiles and capabilities granted to this Task.

    Use the returned numeric ``profile_id`` in other ccm_ssh tools. Invalid or
    stale grants are included with ``valid=false`` so you can report that an
    administrator must re-authorize them. ``profile_allowed_roots`` limits
    file tools only; command execution is not path-scoped.
    """

    try:
        grants = await _request("GET", "")
        return _result({"success": True, "connections": grants})
    except Exception as exc:
        return _result({"success": False, "error": str(exc)})


@mcp.tool()
async def new_effect_id() -> str:
    """Create an id for exactly one future mutating SSH operation.

    Call this before ``run_command`` or ``write_file``. Persist the returned id
    in that tool call and reuse it after every error or lost response. Never use
    one id for two different mutations and never replace an id whose outcome is
    unknown.
    """

    return _result({"success": True, "effect_id": secrets.token_hex(16)})


@mcp.tool()
async def run_command(
    profile_id: int,
    command: str,
    effect_id: str,
    timeout_seconds: int = 60,
    max_output_bytes: int = _MAX_EXEC_OUTPUT_BYTES,
) -> str:
    """Run one non-interactive command on an authorized SSH profile.

    Requires the profile's ``exec`` capability. stdout and stderr are returned
    separately and are bounded; ``truncated=true`` means the output limit was
    reached. Interactive shells and password prompts are not supported. Keep
    First call ``new_effect_id`` and pass that id here. Reuse it after any
    lost/failed HTTP response; never create a replacement id for the same
    unknown command.
    """

    resolved_effect_id: str | None = effect_id
    try:
        resolved_effect_id = _mutation_effect_id(effect_id)
        data = await _request(
            "POST",
            f"/{profile_id}/execute",
            body={
                "effect_id": resolved_effect_id,
                "command": command,
                "timeout_seconds": timeout_seconds,
                "max_output_bytes": max_output_bytes,
            },
            timeout=max(30, min(timeout_seconds + 15, 330)),
        )
        if not isinstance(data, dict) or data.get("effect_id") != resolved_effect_id:
            raise RuntimeError("CCM SSH returned a mismatched effect receipt")
        return _result({"success": True, **data})
    except Exception as exc:
        return _result({
            "success": False,
            "effect_id": resolved_effect_id,
            "error": _mutation_error(exc),
        })


@mcp.tool()
async def list_directory(profile_id: int, path: str) -> str:
    """List an allowed absolute remote directory using ``read``."""

    try:
        data = await _request(
            "POST",
            f"/{profile_id}/list",
            body={"path": path},
        )
        return _result({"success": True, **data})
    except Exception as exc:
        return _result({"success": False, "error": str(exc)})


@mcp.tool()
async def read_file(
    profile_id: int,
    path: str,
    max_bytes: int = 256 * 1024,
) -> str:
    """Read bounded UTF-8 text from an absolute remote path.

    Requires ``read``. Invalid bytes are replaced and ``truncated=true`` means
    only the first ``max_bytes`` bytes were returned.
    """

    try:
        data = await _request(
            "POST",
            f"/{profile_id}/read",
            body={"path": path, "max_bytes": max_bytes},
        )
        return _result({"success": True, **data})
    except Exception as exc:
        return _result({"success": False, "error": str(exc)})


@mcp.tool()
async def write_file(
    profile_id: int,
    path: str,
    content: str,
    effect_id: str,
    overwrite: bool = False,
) -> str:
    """Write up to 1 MB of UTF-8 text to an absolute remote path.

    Requires ``write``. Existing files are rejected unless ``overwrite`` is
    explicitly true. This tool does not create parent directories. First call
    ``new_effect_id`` and pass that id here. Reuse it after any failed/lost
    response; never change ids for a write whose remote outcome is unknown.
    """

    resolved_effect_id: str | None = effect_id
    try:
        resolved_effect_id = _mutation_effect_id(effect_id)
        data = await _request(
            "POST",
            f"/{profile_id}/write",
            body={
                "effect_id": resolved_effect_id,
                "path": path,
                "content": content,
                "overwrite": overwrite,
            },
        )
        if not isinstance(data, dict) or data.get("effect_id") != resolved_effect_id:
            raise RuntimeError("CCM SSH returned a mismatched effect receipt")
        return _result({"success": True, **data})
    except Exception as exc:
        return _result({
            "success": False,
            "effect_id": resolved_effect_id,
            "error": _mutation_error(exc),
        })


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CCM Task SSH MCP Server")
    parser.add_argument("--task-id", type=int, required=True)
    parser.add_argument("--api-base", default="http://localhost:8000")
    parser.add_argument("--auth-token", default="")
    parser.add_argument(
        "--capability",
        action="append",
        choices=tuple(_CAPABILITY_TOOLS),
        default=[],
    )
    args = parser.parse_args()

    _TASK_ID = args.task_id
    _API_BASE = args.api_base.rstrip("/")
    _AUTH_TOKEN = os.environ.get("CCM_INTERNAL_SERVICE_TOKEN", "") or args.auth_token
    _restrict_tools_for_capabilities(set(args.capability))
    mcp.run(transport="stdio")
