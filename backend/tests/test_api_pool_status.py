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

_CODEX_DISABLED_STATUS = {
    **_DISABLED_STATUS,
    "settings": {
        "enabled": False,
        "cooldown_seconds": 300,
        "quota_switch_threshold_percent": 90.0,
        "routing_policy": "api_first",
        "preferred_account_id": None,
    },
}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("module", "status_handler", "usage_handler", "expected_status"),
    [
        (
            claude_pool_api,
            claude_pool_api.pool_status,
            claude_pool_api.pool_usage,
            _DISABLED_STATUS,
        ),
        (
            codex_pool_api,
            codex_pool_api.codex_pool_status,
            codex_pool_api.codex_pool_usage,
            _CODEX_DISABLED_STATUS,
        ),
    ],
)
async def test_disabled_pool_status_is_a_successful_empty_capability_response(
    monkeypatch,
    module,
    status_handler,
    usage_handler,
    expected_status,
):
    monkeypatch.setattr(module, "_get_optional_pool", lambda: None)

    assert await status_handler() == expected_status

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
