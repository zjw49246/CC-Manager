from __future__ import annotations

import json
import os
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from backend.services import claude_auth_projection
from backend.services.claude_auth_projection import (
    ClaudeAuthProjectionError,
    apply_claude_auth_projection,
    inject_cloudrouter_claude_direct_auth,
    prepare_claude_auth_projection,
    remove_claude_auth_projection,
)


def _oauth_home(tmp_path: Path, *, expires_in: float = 3600) -> tuple[Path, str, str]:
    source = tmp_path / "source"
    source.mkdir(mode=0o700)
    access_token = "access-token-must-stay-in-memory"
    refresh_token = "refresh-token-must-never-be-projected"
    credentials = source / ".credentials.json"
    credentials.write_text(
        json.dumps(
            {
                "claudeAiOauth": {
                    "accessToken": access_token,
                    "refreshToken": refresh_token,
                    "expiresAt": int((time.time() + expires_in) * 1000),
                }
            }
        ),
        encoding="utf-8",
    )
    credentials.chmod(0o600)
    return source, access_token, refresh_token


def _patch_root(monkeypatch, tmp_path: Path) -> Path:
    root = tmp_path / "runtime"
    monkeypatch.setattr(
        claude_auth_projection,
        "runtime_secret_root",
        lambda: root,
    )
    return root


def test_oauth_projection_injects_only_bounded_access_token(monkeypatch, tmp_path):
    root = _patch_root(monkeypatch, tmp_path)
    source, access_token, refresh_token = _oauth_home(tmp_path)

    projection = prepare_claude_auth_projection(
        source,
        namespace="discussion-agent",
        identifier=7,
        binding="agent-generation-a",
        environment={},
    )
    env: dict[str, str] = {}
    apply_claude_auth_projection(env, projection)

    assert projection.config_dir.is_dir()
    assert projection.config_dir.stat().st_mode & 0o777 == 0o700
    assert not (projection.config_dir / ".credentials.json").exists()
    assert env["CLAUDE_CONFIG_DIR"] == str(projection.config_dir)
    assert env["CLAUDE_CODE_OAUTH_TOKEN"] == access_token
    assert refresh_token not in repr(projection)
    assert refresh_token not in repr(env)
    assert all(
        refresh_token not in path.read_text(encoding="utf-8", errors="ignore")
        for path in root.rglob("*")
        if path.is_file()
    )
    assert env["CLAUDE_CODE_DISABLE_AUTO_MEMORY"] == "1"
    assert env["DISABLE_PLUGIN_AUTOLOAD"] == "1"


def test_projection_binding_prevents_id_reuse_session_inheritance(
    monkeypatch,
    tmp_path,
):
    _patch_root(monkeypatch, tmp_path)
    source, _, _ = _oauth_home(tmp_path)
    first = prepare_claude_auth_projection(
        source,
        namespace="discussion-agent",
        identifier=9,
        binding="old-row",
        environment={},
    )
    (first.config_dir / "session.jsonl").write_text("old", encoding="utf-8")

    replacement = prepare_claude_auth_projection(
        source,
        namespace="discussion-agent",
        identifier=9,
        binding="new-row",
        environment={},
    )

    assert replacement.config_dir != first.config_dir
    assert not (replacement.config_dir / "session.jsonl").exists()


def test_expired_oauth_access_token_fails_closed(monkeypatch, tmp_path):
    _patch_root(monkeypatch, tmp_path)
    source, _, _ = _oauth_home(tmp_path, expires_in=-1)

    with pytest.raises(ClaudeAuthProjectionError, match="expired"):
        prepare_claude_auth_projection(
            source,
            namespace="goal-evaluator",
            identifier=1,
            binding="turn-a",
            environment={},
        )


def test_direct_provider_auth_never_reads_oauth_capsule(monkeypatch, tmp_path):
    _patch_root(monkeypatch, tmp_path)
    projection = prepare_claude_auth_projection(
        tmp_path / "missing-source",
        namespace="task-distill",
        identifier=3,
        binding="turn-a",
        environment={"ANTHROPIC_API_KEY": "direct-key"},
    )
    env = {"ANTHROPIC_API_KEY": "direct-key"}
    apply_claude_auth_projection(env, projection)

    assert projection.uses_environment_auth is True
    assert projection.oauth_access_token is None
    assert env["ANTHROPIC_API_KEY"] == "direct-key"
    assert "CLAUDE_CODE_OAUTH_TOKEN" not in env


def test_cloudrouter_projection_keeps_key_out_of_disk_and_argv():
    store = MagicMock()
    account = SimpleNamespace(api_provider="cloudrouter")
    store.account_for_claude_config_dir.return_value = account
    store._read_api_key.return_value = "managed-api-key"
    env = {
        "CLAUDE_CODE_OAUTH_TOKEN": "ambient-oauth",
        "ANTHROPIC_AUTH_TOKEN": "ambient-auth",
    }

    assert inject_cloudrouter_claude_direct_auth(
        env,
        store,
        "/managed/account/claude",
    ) is True
    assert env["ANTHROPIC_AUTH_TOKEN"] == "managed-api-key"
    assert env["ANTHROPIC_BASE_URL"].startswith("https://")
    assert "CLAUDE_CODE_OAUTH_TOKEN" not in env
    assert "ANTHROPIC_API_KEY" not in env


def test_reused_projection_rejects_ambient_memory(monkeypatch, tmp_path):
    _patch_root(monkeypatch, tmp_path)
    source, _, _ = _oauth_home(tmp_path)
    projection = prepare_claude_auth_projection(
        source,
        namespace="discussion-agent",
        identifier=11,
        binding="same-row",
        environment={},
    )
    memory = projection.config_dir / "projects" / "repo" / "memory"
    memory.mkdir(parents=True)
    (memory / "MEMORY.md").write_text("ambient", encoding="utf-8")

    with pytest.raises(ClaudeAuthProjectionError, match="ambient customization"):
        prepare_claude_auth_projection(
            source,
            namespace="discussion-agent",
            identifier=11,
            binding="same-row",
            environment={},
        )


def test_cleanup_refuses_symlink_and_removes_exact_bindings(monkeypatch, tmp_path):
    _patch_root(monkeypatch, tmp_path)
    source, _, _ = _oauth_home(tmp_path)
    projection = prepare_claude_auth_projection(
        source,
        namespace="discussion-agent",
        identifier=13,
        binding="row-a",
        environment={},
    )
    outside = tmp_path / "outside"
    outside.write_text("keep", encoding="utf-8")
    link = projection.config_dir / "unsafe"
    link.symlink_to(outside)

    with pytest.raises(ClaudeAuthProjectionError, match="Unsafe entry"):
        remove_claude_auth_projection(
            namespace="discussion-agent",
            identifier=13,
        )
    assert outside.read_text(encoding="utf-8") == "keep"

    os.unlink(link)
    remove_claude_auth_projection(
        namespace="discussion-agent",
        identifier=13,
    )
    assert not projection.config_dir.exists()
