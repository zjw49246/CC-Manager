"""Tests for MCP Skills Server — tool registration and HTTP calls."""
import json
import pytest
from unittest.mock import AsyncMock, patch, MagicMock

import backend.mcp.ccm_skills_server as mcp_mod
import backend.mcp.ccm_skills_http_server as http_mcp_mod


@pytest.fixture(autouse=True)
def _set_mcp_globals():
    for module in (mcp_mod, http_mcp_mod):
        module._TASK_ID = 42
        module._API_BASE = "http://localhost:9999"
    yield
    for module in (mcp_mod, http_mcp_mod):
        module._TASK_ID = 0
        module._API_BASE = "http://localhost:8000"


def test_mcp_server_tools_registered():
    tools = mcp_mod.mcp._tool_manager._tools
    names = set(tools.keys())
    assert "create_monitor" in names
    assert "check_monitors" in names
    assert "stop_monitor" in names


def test_api_url():
    assert mcp_mod._api_url("/monitor-sessions") == "http://localhost:9999/api/tasks/42/monitor-sessions"
    assert mcp_mod._api_url("/monitor-sessions/5") == "http://localhost:9999/api/tasks/42/monitor-sessions/5"


@pytest.mark.asyncio
async def test_create_monitor_success():
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"id": 7}
    mock_resp.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_resp)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("backend.mcp.ccm_skills_server.httpx.AsyncClient", return_value=mock_client):
        result = await mcp_mod.create_monitor("build progress", "tail -f build.log", 60, 10)

    data = json.loads(result)
    assert data["success"] is True
    assert data["monitor_id"] == 7
    assert data["status"] == "created"


@pytest.mark.asyncio
async def test_check_monitors_returns_sessions():
    mock_resp = MagicMock()
    mock_resp.json.return_value = [
        {"id": 1, "description": "test", "status": "running", "checks_done": 3, "max_checks": 50, "last_summary": "ok"},
    ]
    mock_resp.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_resp)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("backend.mcp.ccm_skills_server.httpx.AsyncClient", return_value=mock_client):
        result = await mcp_mod.check_monitors()

    data = json.loads(result)
    assert data["success"] is True
    assert len(data["monitors"]) == 1
    assert data["monitors"][0]["monitor_id"] == 1


@pytest.mark.asyncio
async def test_check_monitors_empty():
    mock_resp = MagicMock()
    mock_resp.json.return_value = []
    mock_resp.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_resp)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("backend.mcp.ccm_skills_server.httpx.AsyncClient", return_value=mock_client):
        result = await mcp_mod.check_monitors()

    data = json.loads(result)
    assert data["success"] is True
    assert data["monitors"] == []
    assert "没有活跃" in data["message"]


@pytest.mark.asyncio
@pytest.mark.parametrize("module", [mcp_mod, http_mcp_mod], ids=["stdio", "http"])
async def test_check_sub_agents_includes_native_sessions(module):
    mock_resp = MagicMock()
    mock_resp.json.return_value = [
        {
            "id": 1,
            "description": "CCM sub-agent",
            "agent_type": "sub_agent",
            "source": "ccm",
            "provider": "claude",
            "status": "completed",
            "checks_done": 1,
            "last_summary": "done",
        },
        {
            "id": 2,
            "description": "Native agent",
            "agent_type": "native-agent",
            "source": "native",
            "provider": "codex",
            "status": "running",
            "checks_done": 2,
            "last_summary": "inspecting",
        },
    ]
    mock_resp.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_resp)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch.object(module.httpx, "AsyncClient", return_value=mock_client):
        result = await module.check_sub_agents()

    data = json.loads(result)
    assert data["success"] is True
    assert data["sub_agents"] == [
        {
            "sub_agent_id": 1,
            "name": "CCM sub-agent",
            "agent_type": "sub_agent",
            "source": "ccm",
            "provider": "claude",
            "status": "completed",
            "progress_count": 1,
            "last_progress": "done",
        },
        {
            "sub_agent_id": 2,
            "name": "Native agent",
            "agent_type": "native-agent",
            "source": "native",
            "provider": "codex",
            "status": "running",
            "progress_count": 2,
            "last_progress": "inspecting",
        },
    ]
    mock_client.get.assert_awaited_once_with(
        "http://localhost:9999/api/tasks/42/sub-agents/sessions",
        headers={},
    )


@pytest.mark.asyncio
async def test_stop_monitor_success():
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.delete = AsyncMock(return_value=mock_resp)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("backend.mcp.ccm_skills_server.httpx.AsyncClient", return_value=mock_client):
        result = await mcp_mod.stop_monitor(5)

    data = json.loads(result)
    assert data["success"] is True
    assert data["status"] == "cancelled"


@pytest.mark.asyncio
async def test_create_monitor_api_error():
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(side_effect=Exception("Connection refused"))
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("backend.mcp.ccm_skills_server.httpx.AsyncClient", return_value=mock_client):
        result = await mcp_mod.create_monitor("test")

    data = json.loads(result)
    assert data["success"] is False
    assert "Connection refused" in data["error"]


@pytest.mark.asyncio
async def test_check_monitors_api_error():
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(side_effect=Exception("timeout"))
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("backend.mcp.ccm_skills_server.httpx.AsyncClient", return_value=mock_client):
        result = await mcp_mod.check_monitors()

    data = json.loads(result)
    assert data["success"] is False
    assert "timeout" in data["error"]


@pytest.mark.asyncio
async def test_stop_monitor_api_error():
    mock_client = AsyncMock()
    mock_client.delete = AsyncMock(side_effect=Exception("not found"))
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("backend.mcp.ccm_skills_server.httpx.AsyncClient", return_value=mock_client):
        result = await mcp_mod.stop_monitor(99)

    data = json.loads(result)
    assert data["success"] is False
    assert "not found" in data["error"]


@pytest.mark.asyncio
async def test_codex_cannot_read_monitor_skill():
    with patch.object(
        mcp_mod,
        "_get_task_data",
        new=AsyncMock(return_value={"provider": "codex"}),
    ):
        result = await mcp_mod.ccm_read_skill("monitor")

    data = json.loads(result)
    assert data["success"] is False
    assert "not supported" in data["error"]


@pytest.mark.asyncio
async def test_local_codex_can_read_enabled_monitor_skill(monkeypatch):
    from backend.config import settings
    from backend.services.skill_loader import Skill

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    task_data = {
        "provider": "codex",
        "worker_id": None,
        "shared_from_id": None,
        "metadata_": {},
        "enabled_skills": {"monitor": True},
    }
    skills = {
        "monitor": Skill(
            name="monitor",
            description="Watch work",
            body="monitor body",
        ),
    }
    with patch.object(
        mcp_mod,
        "_get_task_data",
        new=AsyncMock(return_value=task_data),
    ), patch(
        "backend.services.skill_loader.discover_skills",
        return_value=skills,
    ):
        result = json.loads(await mcp_mod.ccm_read_skill("monitor"))

    assert result["success"] is True
    assert result["body"] == "monitor body"


@pytest.mark.asyncio
async def test_worker_managed_codex_cannot_enable_monitor(monkeypatch):
    from backend.config import settings

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    get_response = MagicMock()
    get_response.raise_for_status = MagicMock()
    get_response.json.return_value = {
        "provider": "codex",
        "worker_id": None,
        "shared_from_id": None,
        "metadata_": {"ccm_worker_managed_task": True},
        "enabled_skills": {},
    }
    client = MagicMock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)
    client.get = AsyncMock(return_value=get_response)
    client.put = AsyncMock()

    with patch(
        "backend.mcp.ccm_skills_server.httpx.AsyncClient",
        return_value=client,
    ):
        result = json.loads(await mcp_mod.ccm_enable_skill("monitor"))

    assert result["success"] is False
    assert "not supported" in result["error"]
    client.put.assert_not_awaited()


@pytest.mark.asyncio
async def test_read_skill_rejects_skill_not_enabled_for_task(monkeypatch):
    from backend.config import settings
    from backend.services.skill_loader import Skill

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    task_data = {
        "provider": "codex",
        "enabled_skills": {},
    }
    skills = {
        "code-review": Skill(
            name="code-review",
            description="Review changes",
            body="review body",
        ),
    }
    with patch.object(
        mcp_mod,
        "_get_task_data",
        new=AsyncMock(return_value=task_data),
    ), patch(
        "backend.services.skill_loader.discover_skills",
        return_value=skills,
    ):
        result = json.loads(await mcp_mod.ccm_read_skill("code-review"))

    assert result["success"] is False
    assert "not enabled for this task" in result["error"]


@pytest.mark.asyncio
async def test_codex_kill_switch_allows_only_selected_sub_agent_skill(
    monkeypatch,
):
    from backend.config import settings
    from backend.services.skill_loader import Skill

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", False)
    task_data = {
        "provider": "codex",
        # Include a stale ordinary selection to prove the kill switch remains
        # authoritative even if legacy task state bypassed API validation.
        "enabled_skills": {
            "code-review": True,
            "sub-agent": True,
        },
    }
    skills = {
        "code-review": Skill(
            name="code-review",
            description="Review changes",
            body="review body",
        ),
        "sub-agent": Skill(
            name="sub-agent",
            description="Delegate tracked work",
            body="sub-agent body",
        ),
    }
    with patch.object(
        mcp_mod,
        "_get_task_data",
        new=AsyncMock(return_value=task_data),
    ), patch(
        "backend.services.skill_loader.discover_skills",
        return_value=skills,
    ):
        ordinary = json.loads(
            await mcp_mod.ccm_read_skill("code-review")
        )
        sub_agent = json.loads(
            await mcp_mod.ccm_read_skill("sub-agent")
        )

    assert ordinary["success"] is False
    assert "main-task MCP is disabled" in ordinary["error"]
    assert sub_agent["success"] is True
    assert sub_agent["body"] == "sub-agent body"


@pytest.mark.asyncio
async def test_user_skill_read_is_scoped_to_selected_worker_snapshot():
    task_data = {
        "provider": "codex",
        "selected_user_skills": [8],
        "metadata_": {
            "ccm_user_skill_snapshots": [{
                "id": 8,
                "name": "Worker copy",
                "description": "Manager snapshot",
                "content": "full copied body",
            }],
        },
    }
    with patch.object(
        mcp_mod,
        "_get_task_data",
        new=AsyncMock(return_value=task_data),
    ):
        selected = json.loads(await mcp_mod.ccm_read_user_skill(8))
        unselected = json.loads(await mcp_mod.ccm_read_user_skill(9))

    assert selected == {
        "success": True,
        "id": 8,
        "name": "Worker copy",
        "description": "Manager snapshot",
        "content": "full copied body",
    }
    assert unselected["success"] is False
    assert "not selected" in unselected["error"]


@pytest.mark.asyncio
async def test_missing_worker_snapshot_never_falls_back_to_local_user_skill():
    task_data = {
        "provider": "codex",
        "selected_user_skills": [8],
        "metadata_": {"ccm_user_skill_snapshots": []},
    }
    with patch.object(
        mcp_mod,
        "_get_task_data",
        new=AsyncMock(return_value=task_data),
    ), patch(
        "backend.database.async_session",
    ) as local_db:
        result = json.loads(await mcp_mod.ccm_read_user_skill(8))

    assert result["success"] is False
    assert "authoritative task snapshot" in result["error"]
    local_db.assert_not_called()
