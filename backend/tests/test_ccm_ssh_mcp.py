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
    effect_id = "1" * 32
    request = AsyncMock(return_value={
        "effect_id": effect_id,
        "replayed": False,
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
        effect_id,
        timeout_seconds=20,
        max_output_bytes=4096,
    ))

    assert result == {
        "success": True,
        "effect_id": effect_id,
        "replayed": False,
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
            "effect_id": effect_id,
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


@pytest.mark.asyncio
async def test_ccm_ssh_mutation_failure_returns_reusable_effect_id(monkeypatch):
    request = AsyncMock(side_effect=RuntimeError(
        "SSH effect outcome is unknown; do not retry blindly"
    ))
    monkeypatch.setattr(ccm_ssh_server, "_request", request)
    effect_id = "2" * 32

    result = json.loads(await ccm_ssh_server.write_file(
        7,
        "/etc/app.conf",
        "PORT=9000\n",
        effect_id,
        overwrite=True,
    ))

    assert result == {
        "success": False,
        "effect_id": effect_id,
        "error": "SSH effect outcome is unknown; do not retry blindly",
    }
    request.assert_awaited_once_with(
        "POST",
        "/7/write",
        body={
            "effect_id": effect_id,
            "path": "/etc/app.conf",
            "content": "PORT=9000\n",
            "overwrite": True,
        },
    )


@pytest.mark.asyncio
async def test_ccm_ssh_effect_id_is_generated_before_remote_mutation(monkeypatch):
    effect_id = "3" * 32
    monkeypatch.setattr(ccm_ssh_server.secrets, "token_hex", lambda _size: effect_id)
    generated = json.loads(await ccm_ssh_server.new_effect_id())
    assert generated == {"success": True, "effect_id": effect_id}

    request = AsyncMock(return_value={
        "effect_id": effect_id,
        "replayed": False,
        "exit_code": 0,
        "stdout": "ok\n",
        "stderr": "",
        "truncated": False,
        "duration_ms": 1,
    })
    monkeypatch.setattr(ccm_ssh_server, "_request", request)

    result = json.loads(await ccm_ssh_server.run_command(7, "true", effect_id))

    assert result["success"] is True
    assert result["effect_id"] == effect_id
    request.assert_awaited_once()
    assert request.await_args.kwargs["body"]["effect_id"] == effect_id
