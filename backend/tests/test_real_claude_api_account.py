"""Opt-in real Claude PTY probe for managed API gateway authentication."""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import pytest


@pytest.mark.asyncio
@pytest.mark.skipif(
    os.environ.get("CCM_RUN_REAL_CLAUDE_API_TESTS") != "1"
    or shutil.which("claude") is None,
    reason="opt-in real Claude API PTY probe",
)
async def test_real_claude_pty_uses_managed_bearer_token(tmp_path):
    """A rejected API-key consent record must not block a managed PTY turn."""

    from claude_pty.bridge import BridgeHub
    from claude_pty.config import PTYConfig
    from claude_pty.session import Session

    key_file = os.environ.get("CCM_REAL_CLAUDE_API_KEY_FILE")
    base_url = os.environ.get("CCM_REAL_CLAUDE_BASE_URL")
    if not key_file or not base_url:
        pytest.fail(
            "CCM_REAL_CLAUDE_API_KEY_FILE and CCM_REAL_CLAUDE_BASE_URL are required"
        )
    api_key = Path(key_file).read_text(encoding="utf-8")
    assert api_key and api_key.strip() == api_key
    config_dir = tmp_path / "claude-home"
    config_dir.mkdir(mode=0o700)
    (config_dir / ".claude.json").write_text(
        json.dumps(
            {
                "hasCompletedOnboarding": True,
                "customApiKeyResponses": {
                    "approved": [],
                    "rejected": [api_key[-20:]],
                },
            }
        ),
        encoding="utf-8",
    )
    (config_dir / "settings.json").write_text(
        json.dumps({"skipDangerousModePermissionPrompt": True}),
        encoding="utf-8",
    )

    bridge = BridgeHub()
    bridge.start()
    session = Session(
        cwd=str(tmp_path),
        bridge=bridge,
        config=PTYConfig(
            config_dir=str(config_dir),
            default_model=os.environ.get(
                "CCM_REAL_CLAUDE_MODEL", "claude-haiku-4-5-20251001"
            ),
            dangerously_skip_permissions=True,
            startup_wait=3,
            max_restart_attempts=0,
            response_timeout=60,
            env_overrides={
                "ANTHROPIC_AUTH_TOKEN": api_key,
                "ANTHROPIC_API_KEY": "",
                "ANTHROPIC_BASE_URL": base_url,
            },
        ),
    )
    content: list[str] = []
    try:
        await session.start()
        async for event in session.send_prompt("Reply with exactly CCM_PTY_AUTH_OK"):
            if event.content:
                content.append(str(event.content))
            joined = "\n".join(content)
            if "CCM_PTY_AUTH_OK" in joined or "Not logged in" in joined:
                break
    finally:
        await session.stop()
        bridge.stop()

    output = "\n".join(content)
    assert "Not logged in" not in output
    assert "CCM_PTY_AUTH_OK" in output
