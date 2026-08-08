import json
from unittest.mock import AsyncMock

import pytest

from backend.mcp import ccm_ssh_server


@pytest.mark.asyncio
async def test_ccm_ssh_lists_task_scoped_connections(monkeypatch):
    request = AsyncMock(return_value=[{
        "profile_id": 7,
        "profile_name": "production",
        "capabilities": ["exec"],
        "valid": True,
    }])
    monkeypatch.setattr(ccm_ssh_server, "_request", request)

    result = json.loads(await ccm_ssh_server.list_connections())

    assert result["success"] is True
    assert result["connections"][0]["profile_id"] == 7
    request.assert_awaited_once_with("GET", "")


@pytest.mark.asyncio
async def test_ccm_ssh_run_command_preserves_structured_bounded_result(monkeypatch):
    request = AsyncMock(return_value={
        "exit_code": 2,
        "stdout": "",
        "stderr": "not active\n",
        "truncated": False,
        "duration_ms": 8,
    })
    monkeypatch.setattr(ccm_ssh_server, "_request", request)

    result = json.loads(await ccm_ssh_server.run_command(
        7,
        "systemctl is-active app",
        timeout_seconds=20,
        max_output_bytes=4096,
    ))

    assert result == {
        "success": True,
        "exit_code": 2,
        "stdout": "",
        "stderr": "not active\n",
        "truncated": False,
        "duration_ms": 8,
    }
    request.assert_awaited_once_with(
        "POST",
        "/7/execute",
        body={
            "command": "systemctl is-active app",
            "timeout_seconds": 20,
            "max_output_bytes": 4096,
        },
        timeout=35,
    )


@pytest.mark.asyncio
async def test_ccm_ssh_tools_return_sanitized_api_error(monkeypatch):
    request = AsyncMock(side_effect=RuntimeError(
        "Task SSH grant is no longer valid: profile_revision_changed"
    ))
    monkeypatch.setattr(ccm_ssh_server, "_request", request)

    result = json.loads(await ccm_ssh_server.read_file(
        7,
        "/etc/app.conf",
    ))

    assert result["success"] is False
    assert result["error"].endswith("profile_revision_changed")
