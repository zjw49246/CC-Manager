from __future__ import annotations

import pytest
from fastapi import HTTPException

from backend.api import codex_pool as codex_pool_api
from backend.api import pool as claude_pool_api


_DISABLED_STATUS = {
    "enabled": False,
    "total": 0,
    "available": 0,
    "cooldown": 0,
    "disabled": 0,
    "preferred": None,
    "last_selected": None,
    "accounts": [],
}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("module", "status_handler", "usage_handler"),
    [
        (claude_pool_api, claude_pool_api.pool_status, claude_pool_api.pool_usage),
        (
            codex_pool_api,
            codex_pool_api.codex_pool_status,
            codex_pool_api.codex_pool_usage,
        ),
    ],
)
async def test_disabled_pool_status_is_a_successful_empty_capability_response(
    monkeypatch,
    module,
    status_handler,
    usage_handler,
):
    monkeypatch.setattr(module, "_get_optional_pool", lambda: None)

    assert await status_handler() == _DISABLED_STATUS

    with pytest.raises(HTTPException) as exc_info:
        await usage_handler()
    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("module", "status_handler"),
    [
        (claude_pool_api, claude_pool_api.pool_status),
        (codex_pool_api, codex_pool_api.codex_pool_status),
    ],
)
async def test_enabled_pool_status_preserves_the_live_pool_payload(
    monkeypatch,
    module,
    status_handler,
):
    expected = {**_DISABLED_STATUS, "enabled": True, "total": 1}
    pool = type("Pool", (), {"status": lambda self: expected})()
    monkeypatch.setattr(module, "_get_optional_pool", lambda: pool)

    assert await status_handler() is expected
