import asyncio
import json
import os
import stat
import sys
import tomllib
import types
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from fastapi import HTTPException
from pydantic import SecretStr
from starlette.requests import Request

import backend.services.cloudrouter_accounts as cloudrouter_module
import backend.api.cloudrouter_accounts as cloudrouter_api
from backend.services.cloudrouter_accounts import (
    APIBEST_CLAUDE_BASE_URL,
    APIBEST_CODEX_BASE_URL,
    APIBEST_MODELS_URL,
    APIBEST_PRICING_URL,
    APEX_CLAUDE_BASE_URL,
    APEX_CODEX_BASE_URL,
    APEX_MODELS_URL,
    APEX_USAGE_URL,
    CLAUDE_BASE_URL,
    CODEX_BASE_URL,
    MAX_API_RESPONSE_BYTES,
    CloudRouterAccountBusyError,
    CloudRouterAccountError,
    CloudRouterAccountNotFound,
    CloudRouterAccountStore,
    CloudRouterUnsafePathError,
    CloudRouterUpstreamError,
)


MODELS = {
    "claude": ["claude-opus-4-8", "claude-sonnet-5"],
    "codex": ["gpt-5.4", "gpt-5.5"],
}


def test_apex_gateway_uses_apexin_endpoint():
    assert APEX_CLAUDE_BASE_URL == "https://api.apexin.ai"
    assert APEX_CODEX_BASE_URL == "https://api.apexin.ai/v1"
    assert APEX_MODELS_URL == "https://api.apexin.ai/v1/models"
    assert APEX_USAGE_URL == "https://api.apexin.ai/v1/usage"


def test_api_auth_kind_is_limited_to_registered_gateways():
    assert cloudrouter_module.is_api_auth_kind("cloudrouter_api")
    assert cloudrouter_module.is_api_auth_kind("apex_api")
    assert cloudrouter_module.is_api_auth_kind("apibest_api")
    assert not cloudrouter_module.is_api_auth_kind("legacy_api")
    assert not cloudrouter_module.is_api_auth_kind("oauth")


def test_apibest_gateway_uses_fixed_compatible_endpoints():
    assert APIBEST_CLAUDE_BASE_URL == "https://apibest.ai"
    assert APIBEST_CODEX_BASE_URL == "https://apibest.ai/v1"
    assert APIBEST_MODELS_URL == "https://apibest.ai/v1/models"
    assert APIBEST_PRICING_URL == "https://apibest.ai/api/pricing"


async def _add(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    models: dict[str, list[str]] | None = None,
) -> tuple[CloudRouterAccountStore, object]:
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store, "probe_models", AsyncMock(return_value=models or MODELS),
    )
    return store, await store.add_account("Primary API", "cr-secret-value")


def _permissions(path: Path) -> int:
    return stat.S_IMODE(path.lstat().st_mode)


def _admin_request() -> Request:
    request = Request({"type": "http", "method": "GET", "path": "/", "headers": []})
    request.state.user_role = "admin"
    return request


@pytest.mark.asyncio
async def test_add_builds_private_dual_cli_home_without_leaking_key(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    root = account.root

    assert account.id == "cloudrouter-1"
    assert account.providers == ["claude", "codex"]
    assert _permissions(store.root) == 0o700
    assert _permissions(root) == 0o700
    assert _permissions(root / "claude") == 0o700
    assert _permissions(root / "codex") == 0o700
    assert _permissions(root / "account.json") == 0o600
    assert _permissions(root / "api.key") == 0o600
    assert _permissions(root / "key-helper") == 0o700

    metadata = json.loads((root / "account.json").read_text())
    assert metadata["models"] == MODELS
    assert metadata["endpoints"]["claude_base_url"] == CLAUDE_BASE_URL
    assert metadata["endpoints"]["codex_base_url"] == CODEX_BASE_URL
    assert "cr-secret-value" not in json.dumps(metadata)

    settings_text = (root / "claude" / "settings.json").read_text()
    settings = json.loads(settings_text)
    assert settings["env"] == {"ANTHROPIC_BASE_URL": CLAUDE_BASE_URL}
    assert "/home/sandbox/.ccm-api-account/key-helper" in settings["apiKeyHelper"]
    assert str(root / "key-helper") in settings["apiKeyHelper"]
    assert settings["skipDangerousModePermissionPrompt"] is True
    assert "model" not in settings
    assert json.loads((root / "claude" / ".claude.json").read_text()) == {
        "hasCompletedOnboarding": True,
    }

    codex_config = (root / "codex" / "config.toml").read_text()
    assert 'model_provider = "cloudrouter"' in codex_config
    assert f'base_url = "{CODEX_BASE_URL}"' in codex_config
    assert 'wire_api = "responses"' in codex_config
    assert "supports_websockets = false" in codex_config
    assert "[model_providers.cloudrouter.auth]" in codex_config
    assert str(root / "key-helper") in codex_config
    assert "\nmodel =" not in codex_config
    assert "cr-secret-value" not in settings_text + codex_config

    helper_output = os.popen(str(root / "key-helper")).read()
    assert helper_output == "cr-secret-value"


@pytest.mark.asyncio
async def test_add_apex_builds_private_dual_provider_home_without_leaking_key(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={
            "claude": ["claude-opus-4-8"],
            "codex": ["gpt-5.4"],
        }),
    )

    account = await store.add_account(
        "Apex primary",
        "lck-test-secret",
        api_provider="apex",
    )
    root = account.root

    assert account.id == "apex-1"
    assert account.api_provider == "apex"
    assert account.auth_kind == "apex_api"
    assert account.providers == ["claude", "codex"]
    assert account.models == {
        "claude": ["claude-opus-4-8"],
        "codex": ["gpt-5.4"],
    }
    settings = json.loads((root / "claude" / "settings.json").read_text())
    assert settings["env"] == {"ANTHROPIC_BASE_URL": APEX_CLAUDE_BASE_URL}
    assert settings["apiKeyHelper"] == cloudrouter_module._claude_helper_command(root)
    assert json.loads((root / "claude" / ".claude.json").read_text()) == {
        "hasCompletedOnboarding": True,
    }

    metadata = json.loads((root / "account.json").read_text())
    assert metadata["api_provider"] == "apex"
    assert metadata["endpoints"]["claude_base_url"] == APEX_CLAUDE_BASE_URL
    assert metadata["endpoints"]["codex_base_url"] == APEX_CODEX_BASE_URL
    assert metadata["endpoints"]["usage_url"] == APEX_USAGE_URL
    assert "lck-test-secret" not in json.dumps(metadata)

    codex_config = (root / "codex" / "config.toml").read_text()
    assert 'model_provider = "apexrouter"' in codex_config
    assert "[model_providers.apexrouter]" in codex_config
    assert 'name = "ApexRouter"' in codex_config
    assert f'base_url = "{APEX_CODEX_BASE_URL}"' in codex_config
    assert "[model_providers.apexrouter.auth]" in codex_config
    assert "[model_providers.apex_gateway]" in codex_config
    assert "[model_providers.apex_gateway.auth]" in codex_config
    assert str(root / "key-helper") in codex_config
    assert "lck-test-secret" not in codex_config
    assert os.popen(str(root / "key-helper")).read() == "lck-test-secret"


@pytest.mark.asyncio
async def test_legacy_codex_only_apex_account_adds_safe_claude_runtime(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={"claude": [], "codex": ["gpt-5.4"]}),
    )
    account = await store.add_account(
        "Apex", "lck-test-secret", api_provider="apex",
    )
    settings_path = account.root / "claude" / "settings.json"
    onboarding_path = account.root / "claude" / ".claude.json"
    settings_path.unlink()
    onboarding_path.unlink()
    metadata_path = account.root / "account.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["endpoints"]["claude_base_url"] = None
    metadata_path.write_text(json.dumps(metadata))

    migrated = store.reload()[0]

    assert migrated.id == account.id
    assert json.loads(settings_path.read_text()) == {
        "env": {"ANTHROPIC_BASE_URL": APEX_CLAUDE_BASE_URL},
        "apiKeyHelper": cloudrouter_module._claude_helper_command(account.root),
        cloudrouter_module.CLAUDE_SKIP_DANGEROUS_PROMPT: True,
    }
    assert json.loads(onboarding_path.read_text()) == {
        "hasCompletedOnboarding": True,
    }
    assert json.loads(metadata_path.read_text())["endpoints"] == (
        cloudrouter_module.API_PROVIDER_SPECS["apex"].endpoints
    )


@pytest.mark.asyncio
async def test_legacy_apex_migration_preflights_codex_before_writing(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={"claude": [], "codex": ["gpt-5.4"]}),
    )
    account = await store.add_account(
        "Apex", "lck-test-secret", api_provider="apex",
    )
    settings_path = account.root / "claude" / "settings.json"
    onboarding_path = account.root / "claude" / ".claude.json"
    settings_path.unlink()
    onboarding_path.unlink()
    metadata_path = account.root / "account.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["endpoints"] = dict(
        cloudrouter_module.LEGACY_APEX_CODEX_ONLY_ENDPOINTS,
    )
    metadata_path.write_text(json.dumps(metadata))
    metadata_before = metadata_path.read_bytes()
    codex_path = account.root / "codex" / "config.toml"
    codex_path.write_text(
        codex_path.read_text().replace(
            APEX_CODEX_BASE_URL,
            "https://attacker.invalid/v1",
        ),
    )

    with pytest.raises(
        CloudRouterUnsafePathError,
        match="Modified Codex API routing",
    ):
        store.reload()

    assert not settings_path.exists()
    assert not onboarding_path.exists()
    assert metadata_path.read_bytes() == metadata_before


@pytest.mark.asyncio
async def test_legacy_codex_only_apex_rejects_existing_claude_redirect(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={"claude": [], "codex": ["gpt-5.4"]}),
    )
    account = await store.add_account(
        "Apex", "lck-test-secret", api_provider="apex",
    )
    settings_path = account.root / "claude" / "settings.json"
    settings = json.loads(settings_path.read_text())
    settings["env"]["ANTHROPIC_BASE_URL"] = "https://attacker.invalid"
    settings_path.write_text(json.dumps(settings))
    metadata_path = account.root / "account.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["endpoints"]["claude_base_url"] = None
    metadata_path.write_text(json.dumps(metadata))

    with pytest.raises(
        CloudRouterUnsafePathError,
        match="Modified legacy Apex Claude config",
    ):
        store.reload()


@pytest.mark.asyncio
async def test_add_apibest_builds_private_dual_provider_home(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(store, "probe_models", AsyncMock(return_value=MODELS))

    account = await store.add_account(
        "APIBest primary", "sk-test-secret", api_provider="apibest",
    )

    assert account.id == "apibest-1"
    assert account.api_provider == "apibest"
    assert account.auth_kind == "apibest_api"
    assert account.providers == ["claude", "codex"]
    settings = json.loads((account.root / "claude" / "settings.json").read_text())
    assert settings["env"] == {"ANTHROPIC_BASE_URL": APIBEST_CLAUDE_BASE_URL}
    codex = (account.root / "codex" / "config.toml").read_text()
    assert 'model_provider = "apibest"' in codex
    assert f'base_url = "{APIBEST_CODEX_BASE_URL}"' in codex
    assert "sk-test-secret" not in settings.__repr__() + codex
    usage = await store.fetch_usage(account.id, force=True)
    assert usage["known"] is False
    assert usage["available"] is True
    assert usage["reason"] == "usage_not_supported"


@pytest.mark.asyncio
async def test_legacy_apex_endpoint_is_migrated_to_apexin(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={"claude": [], "codex": ["gpt-5.4"]}),
    )
    account = await store.add_account(
        "Apex", "lck-test-secret", api_provider="apex",
    )
    legacy_base_url = "https://35-75-22-186.sslip.io/v1"
    metadata_path = account.root / "account.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["endpoints"] = {
        "claude_base_url": None,
        "codex_base_url": legacy_base_url,
        "models_url": f"{legacy_base_url}/models",
        "usage_url": f"{legacy_base_url}/usage",
    }
    metadata_path.write_text(json.dumps(metadata))
    config_path = account.root / "codex" / "config.toml"
    config_path.write_text(
        config_path.read_text().replace(APEX_CODEX_BASE_URL, legacy_base_url)
    )

    assert [item.id for item in store.reload()] == [account.id]
    migrated_metadata = json.loads(metadata_path.read_text())
    assert migrated_metadata["endpoints"]["codex_base_url"] == APEX_CODEX_BASE_URL
    migrated_config = tomllib.loads(config_path.read_text())
    assert {
        provider["base_url"]
        for provider in migrated_config["model_providers"].values()
    } == {APEX_CODEX_BASE_URL}


@pytest.mark.asyncio
async def test_legacy_apex_gateway_config_migrates_with_resume_alias(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={"claude": [], "codex": ["gpt-5.4"]}),
    )
    account = await store.add_account(
        "Apex", "lck-test-secret", api_provider="apex",
    )
    helper = account.root / "key-helper"
    config = account.root / "codex" / "config.toml"
    config.write_text(
        'model_provider = "apex_gateway"\n'
        'personality = "pragmatic"\n\n'
        "[model_providers.apex_gateway]\n"
        'name = "Apex Gateway"\n'
        f'base_url = "{APEX_CODEX_BASE_URL}"\n'
        'wire_api = "responses"\n'
        "supports_websockets = false\n\n"
        "[model_providers.apex_gateway.auth]\n"
        f'command = "{helper}"\n'
        "timeout_ms = 5000\n"
        "refresh_interval_ms = 0\n\n"
        '[projects."/tmp/project"]\n'
        'trust_level = "trusted"\n',
    )
    os.chmod(config, 0o600)

    assert [item.id for item in store.reload()] == [account.id]
    migrated = tomllib.loads(config.read_text())
    assert migrated["model_provider"] == "apexrouter"
    assert migrated["personality"] == "pragmatic"
    assert "projects" not in migrated
    assert set(migrated["model_providers"]) == {
        "apexrouter",
        "apex_gateway",
    }
    assert (
        migrated["model_providers"]["apexrouter"]
        == migrated["model_providers"]["apex_gateway"]
    )
    assert migrated["model_providers"]["apexrouter"]["name"] == "ApexRouter"


@pytest.mark.asyncio
async def test_apex_resume_alias_tampering_fails_closed(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={"claude": [], "codex": ["gpt-5.4"]}),
    )
    account = await store.add_account(
        "Apex", "lck-test-secret", api_provider="apex",
    )
    config = account.root / "codex" / "config.toml"
    config.write_text(
        config.read_text().replace(
            "[model_providers.apex_gateway]\n"
            'name = "ApexRouter"\n'
            f'base_url = "{APEX_CODEX_BASE_URL}"',
            "[model_providers.apex_gateway]\n"
            'name = "ApexRouter"\n'
            'base_url = "https://attacker.invalid/v1"',
        ),
    )

    with pytest.raises(
        CloudRouterUnsafePathError,
        match="Codex API routing",
    ):
        store.reload()


@pytest.mark.asyncio
async def test_apex_usage_separates_key_usage_from_shared_group_quota(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={"claude": [], "codex": ["gpt-5.4"]}),
    )
    monkeypatch.setattr(
        store,
        "_request_json",
        AsyncMock(return_value={
            "key_name": "test-key",
            "group_name": "apex-research",
            "used": {
                "requests_5h": 0,
                "requests_day": 0,
                "tokens_day": 0,
                "tokens_month": 0,
            },
            "remaining": {
                "requests_5h": 25_000,
                "requests_day": 50_000,
                "tokens_day": 10_000_000,
                "tokens_month": 100_000_000,
            },
            "limits": {
                "requests_5h": 25_000,
                "requests_day": 50_000,
                "tokens_day": 10_000_000,
                "tokens_month": 100_000_000,
                "concurrency": 20,
            },
        }),
    )
    account = await store.add_account(
        "Apex", "lck-test-secret", api_provider="apex"
    )
    request = AsyncMock(return_value={
        "key_name": "test-key",
        "group_name": "apex-research",
        "used": {
            "requests_5h": 3,
            "requests_day": 7,
            "tokens_day": 1_000,
            "tokens_month": 2_000,
        },
        "remaining": {
            "requests_5h": 24_000,
            "requests_day": 49_000,
            "tokens_day": 9_000_000,
            "tokens_month": 90_000_000,
        },
        "limits": {
            "requests_5h": 25_000,
            "requests_day": 50_000,
            "tokens_day": 10_000_000,
            "tokens_month": 100_000_000,
            "concurrency": 20,
        },
    })
    monkeypatch.setattr(store, "_request_json", request)

    snapshot = await store.fetch_usage(account.id, force=True)

    assert snapshot["known"] is True
    assert snapshot["available"] is True
    assert snapshot["mode"] == "shared_group"
    assert snapshot["key_name"] == "test-key"
    assert snapshot["group_name"] == "apex-research"
    assert snapshot["concurrency"] == 20
    assert snapshot["key_usage"]["requests_5h"] == 3
    assert snapshot["windows"][0]["used"] == 1_000
    assert snapshot["windows"][0]["remaining"] == 24_000
    assert snapshot["windows"][0]["limit"] == 25_000
    assert snapshot["windows"][0]["key_used"] == 3
    assert snapshot["windows"][0]["scope"] == "group"
    assert store.cached_quota_decision(account.id) == {
        "available": True,
        "known": True,
        "reason": "active",
    }
    request.assert_awaited_once_with(
        APEX_USAGE_URL,
        "lck-test-secret",
    )


@pytest.mark.asyncio
async def test_apex_null_quota_windows_are_known_unlimited(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={"claude": [], "codex": ["gpt-5.4"]}),
    )
    account = await store.add_account(
        "Apex", "lck-test-secret", api_provider="apex",
    )
    monkeypatch.setattr(
        store,
        "_request_json",
        AsyncMock(return_value={
            "key_name": "unlimited-key",
            "group_name": "unlimited-group",
            "used": {
                "requests_5h": 3,
                "requests_day": 7,
                "tokens_day": 1_000,
                "tokens_month": 2_000,
            },
            "remaining": {
                "requests_5h": None,
                "requests_day": None,
                "tokens_day": None,
                "tokens_month": None,
            },
            "limits": {
                "requests_5h": None,
                "requests_day": None,
                "tokens_day": None,
                "tokens_month": None,
                "concurrency": 20,
            },
        }),
    )

    snapshot = await store.fetch_usage(account.id, force=True)

    assert snapshot["state"] == "active"
    assert snapshot["known"] is True
    assert snapshot["available"] is True
    assert snapshot["concurrency"] == 20
    assert snapshot["key_usage"] == {
        "requests_5h": 3,
        "requests_day": 7,
        "tokens_day": 1_000,
        "tokens_month": 2_000,
    }
    assert len(snapshot["windows"]) == 4
    assert all(window["unlimited"] is True for window in snapshot["windows"])
    assert all("limit" not in window for window in snapshot["windows"])
    assert all("remaining" not in window for window in snapshot["windows"])
    assert all("utilization" not in window for window in snapshot["windows"])
    assert store.cached_quota_decision(account.id) == {
        "available": True,
        "known": True,
        "reason": "active",
    }


def test_apex_usage_supports_mixed_limited_and_unlimited_windows():
    snapshot = cloudrouter_module._normalise_apex_usage("apex-1", {
        "used": {
            "requests_5h": 3,
            "requests_day": 7,
            "tokens_day": 1_000,
            "tokens_month": 2_000,
        },
        "remaining": {
            "requests_5h": None,
            "requests_day": 93,
            "tokens_day": None,
            "tokens_month": 8_000,
        },
        "limits": {
            "requests_5h": None,
            "requests_day": 100,
            "tokens_day": None,
            "tokens_month": 10_000,
            "concurrency": 5,
        },
    })

    windows = {window["id"]: window for window in snapshot["windows"]}
    assert windows["requests_5h"]["unlimited"] is True
    assert windows["requests_5h"]["key_used"] == 3
    assert windows["tokens_day"]["unlimited"] is True
    assert windows["requests_day"]["limit"] == 100
    assert windows["requests_day"]["remaining"] == 93
    assert windows["tokens_month"]["used"] == 2_000
    assert snapshot["state"] == "active"
    assert snapshot["available"] is True


@pytest.mark.parametrize(
    ("remaining", "limit"),
    [
        (None, 100),
        (100, None),
        ("invalid", 100),
        (100, "invalid"),
    ],
)
def test_apex_usage_rejects_asymmetric_or_invalid_window_values(
    remaining, limit,
):
    payload = {
        "used": {
            "requests_5h": 3,
            "requests_day": 7,
            "tokens_day": 1_000,
            "tokens_month": 2_000,
        },
        "remaining": {
            "requests_5h": remaining,
            "requests_day": 93,
            "tokens_day": 9_000,
            "tokens_month": 8_000,
        },
        "limits": {
            "requests_5h": limit,
            "requests_day": 100,
            "tokens_day": 10_000,
            "tokens_month": 10_000,
            "concurrency": 5,
        },
    }

    with pytest.raises(
        CloudRouterUpstreamError,
        match="invalid_usage_response",
    ):
        cloudrouter_module._normalise_apex_usage("apex-1", payload)


@pytest.mark.asyncio
async def test_partial_apex_group_usage_cannot_replace_known_exhaustion(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={"claude": [], "codex": ["gpt-5.4"]}),
    )
    account = await store.add_account(
        "Apex", "lck-test-secret", api_provider="apex"
    )
    store._quota_cache[account.id] = {
        "account_id": account.id,
        "known": True,
        "available": False,
        "state": "exhausted",
        "reason": "exhausted",
    }
    store._quota_cached_at[account.id] = 1
    monkeypatch.setattr(
        store,
        "_request_json",
        AsyncMock(return_value={
            "used": {
                "requests_5h": 3,
                "requests_day": 7,
                "tokens_day": 1_000,
                "tokens_month": 2_000,
            },
            # A partial response cannot prove that the shared group is usable.
            "remaining": {},
            "limits": {"concurrency": 20},
        }),
    )

    snapshot = await store.fetch_usage(account.id, force=True)

    assert snapshot["known"] is False
    assert snapshot["last_known_available"] is False
    assert snapshot["reason"] == "invalid_usage_response"
    assert store.cached_quota_decision(account.id) == {
        "available": False,
        "known": True,
        "reason": "exhausted",
    }


@pytest.mark.asyncio
async def test_models_gate_each_provider_independently(tmp_path, monkeypatch):
    store, account = await _add(
        tmp_path, monkeypatch,
        models={"claude": ["claude-opus-4-8"], "codex": []},
    )

    assert account.providers == ["claude"]
    assert account.supports_model("claude", None)
    assert account.supports_model("claude", "default")
    assert account.supports_model("claude", "claude-opus-4-8[1m]")
    assert not account.supports_model("claude", "claude-sonnet-5")
    assert not account.supports_model("codex", None)
    assert store.account_for_claude_config_dir(account.claude_config_dir) == account
    assert store.account_for_codex_home(account.codex_home) == account
    assert store.account_for_runtime_home(account.codex_home) == account
    assert store.account_for_runtime_home(account.root) is None


@pytest.mark.asyncio
async def test_claude_short_alias_matches_only_exact_dated_model(
    tmp_path, monkeypatch,
):
    _store, account = await _add(
        tmp_path,
        monkeypatch,
        models={
            "claude": ["claude-haiku-4-5-20251001"],
            "codex": [],
        },
    )

    assert account.supports_model("claude", "claude-haiku-4-5")
    assert account.supports_model("claude", "claude-haiku-4-5[1m]")
    assert not account.supports_model("claude", "claude-haiku-4")
    assert not account.supports_model("claude", "claude-haiku-4-5-fast")


@pytest.mark.asyncio
async def test_account_numbers_do_not_reuse_retired_folders(tmp_path, monkeypatch):
    store, first = await _add(tmp_path, monkeypatch)
    await store.retire_account(first.id)
    monkeypatch.setattr(store, "probe_models", AsyncMock(return_value=MODELS))
    second = await store.add_account("Second", "cr-second")

    assert second.id == "cloudrouter-2"
    assert [item.id for item in store.all_accounts()] == ["cloudrouter-2"]
    assert [item.id for item in store.all_accounts(include_retired=True)] == [
        "cloudrouter-1", "cloudrouter-2",
    ]


@pytest.mark.asyncio
async def test_retire_clears_credentials_and_config_but_preserves_sessions(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    claude_project = account.root / "claude" / "projects" / "p" / "history.jsonl"
    claude_project.parent.mkdir(parents=True)
    claude_project.write_text("history")
    (account.root / "claude" / "plugins").mkdir()
    (account.root / "claude" / "plugins" / "state").write_text("state")
    codex_session = account.root / "codex" / "sessions" / "rollout.jsonl"
    codex_session.parent.mkdir()
    codex_session.write_text("session")
    (account.root / "codex" / "history.jsonl").write_text("history")

    retired = await store.retire_account(account.id)

    assert retired.retired
    assert not retired.enabled
    assert not (account.root / "api.key").exists()
    assert not (account.root / "key-helper").exists()
    assert claude_project.read_text() == "history"
    assert codex_session.read_text() == "session"
    assert not (account.root / "claude" / "settings.json").exists()
    assert not (account.root / "claude" / "plugins").exists()
    assert not (account.root / "codex" / "config.toml").exists()
    assert not (account.root / "codex" / "history.jsonl").exists()
    metadata = json.loads((account.root / "account.json").read_text())
    assert metadata["retired"] is True
    assert metadata["enabled"] is False
    assert metadata["cleanup_pending"] is False
    assert "cr-secret-value" not in json.dumps(metadata)
    assert await store.retire_account(account.id) == retired


@pytest.mark.asyncio
async def test_failed_retirement_is_disabled_and_idempotently_resumable(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    original = store._remove_except
    monkeypatch.setattr(
        store,
        "_remove_except",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("interrupted")),
    )

    with pytest.raises(RuntimeError, match="interrupted"):
        await store.retire_account(account.id)

    pending = store.account(account.id)
    assert pending is not None
    assert pending.retired is True
    assert pending.cleanup_pending is True
    assert store.cached_quota_decision(account.id) == {
        "available": False, "known": True, "reason": "disabled",
    }

    monkeypatch.setattr(store, "_remove_except", original)
    completed = await store.retire_account(account.id)
    assert completed.retired is True
    assert completed.cleanup_pending is False
    assert not (account.root / "api.key").exists()


@pytest.mark.asyncio
async def test_cleanup_diagnostics_are_bounded_persistent_and_idempotent(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    await store.stage_retirement(account.id)
    first_attempt = await store.mark_cleanup_attempt(account.id)
    assert first_attempt.cleanup_last_attempt_at is not None

    failure = CloudRouterAccountBusyError(
        "active sk-secret-value\n" + ("x" * 1000),
        code="../invalid",
    )
    failed = await store.record_cleanup_failure(
        account.id,
        code=failure.code,
        reason=failure.reason,
    )
    assert failed.cleanup_code == "cleanup_blocked"
    assert "sk-secret-value" not in failed.cleanup_reason
    assert "\n" not in failed.cleanup_reason
    assert len(failed.cleanup_reason.encode("utf-8")) <= 512
    assert failed.cleanup_last_error_at is not None

    # Starting a retry updates the attempt receipt but preserves the last
    # useful diagnosis if the process exits before it reaches the runtime
    # fence or writes a replacement failure.
    retrying = await store.mark_cleanup_attempt(account.id)
    assert retrying.cleanup_code == failed.cleanup_code
    assert retrying.cleanup_reason == failed.cleanup_reason
    assert retrying.cleanup_last_error_at == failed.cleanup_last_error_at
    assert retrying.cleanup_last_attempt_at >= failed.cleanup_last_attempt_at

    restarted = CloudRouterAccountStore(store.root)
    restored = restarted.account(account.id)
    assert restored is not None
    assert restored.cleanup_code == retrying.cleanup_code
    assert restored.cleanup_reason == retrying.cleanup_reason
    assert restored.cleanup_last_attempt_at == retrying.cleanup_last_attempt_at
    assert restored.cleanup_last_error_at == retrying.cleanup_last_error_at

    completed = await restarted.finalize_retirement(account.id)
    assert completed.cleanup_pending is False
    assert completed.cleanup_code is None
    assert completed.cleanup_reason is None
    assert completed.cleanup_last_attempt_at is None
    assert completed.cleanup_last_error_at is None

    # A late duplicate error receipt is a no-op after successful finalization.
    duplicate = await restarted.record_cleanup_failure(
        account.id,
        code="runtime_busy",
        reason="late runtime",
    )
    assert duplicate == completed


@pytest.mark.asyncio
async def test_modified_cleanup_diagnostics_fail_closed(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    await store.stage_retirement(account.id)
    metadata_path = account.root / "account.json"
    metadata = json.loads(metadata_path.read_text())
    metadata.update({
        "cleanup_code": "runtime_busy",
        "cleanup_reason": "unexpected\nsecond line",
        "cleanup_last_attempt_at": 1.0,
        "cleanup_last_error_at": 1.0,
    })
    metadata_path.write_text(json.dumps(metadata))

    with pytest.raises(
        CloudRouterUnsafePathError,
        match="Invalid cleanup metadata",
    ):
        store.reload()


@pytest.mark.asyncio
async def test_finalize_retirement_refuses_active_credential_lease(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def hold_credential():
        async with store.credential_admission(account.id):
            entered.set()
            await release.wait()

    lease_task = asyncio.create_task(hold_credential())
    await entered.wait()
    await store.stage_retirement(account.id)

    with pytest.raises(CloudRouterAccountBusyError, match="credential"):
        await store.finalize_retirement(account.id)

    assert (account.root / "api.key").is_file()
    assert store.account(account.id).cleanup_pending is True
    release.set()
    await lease_task
    completed = await store.finalize_retirement(account.id)
    assert completed.cleanup_pending is False
    assert not (account.root / "api.key").exists()


@pytest.mark.asyncio
async def test_pending_tombstone_survives_restart_in_both_pool_tabs(
    tmp_path, monkeypatch,
):
    from backend.services.claude_pool import ClaudePool
    from backend.services.codex_pool import CodexPool

    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={
            "claude": [],
            "codex": ["gpt-5.5"],
        }),
    )
    account = await store.add_account(
        "Apex API",
        "lck-secret",
        api_provider="apex",
    )
    await store.stage_retirement(account.id)

    # Reconstruct every in-memory projection as a process restart would.
    restarted = CloudRouterAccountStore(store.root)
    claude = ClaudePool(
        tmp_path / "missing-claude.json",
        cloudrouter_store=restarted,
        bootstrap_default=False,
        include_native=False,
    )
    codex = CodexPool(
        tmp_path / "missing-codex.json",
        cloudrouter_store=restarted,
        bootstrap_default=False,
        include_native=False,
    )

    for projected in (claude.list_accounts(), codex.list_accounts()):
        assert len(projected) == 1
        assert projected[0]["api_account_id"] == account.id
        assert projected[0]["retired"] is True
        assert projected[0]["cleanup_pending"] is True
        assert projected[0]["enabled"] is False

    await restarted.finalize_retirement(account.id)
    claude.reload()
    codex.reload()
    assert claude.list_accounts() == []
    assert codex.list_accounts() == []


@pytest.mark.asyncio
async def test_usage_quota_exhaustion_is_known_unavailable_and_cached(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    request = AsyncMock(return_value={
        "mode": "quota_limited",
        "status": "quota_exhausted",
        "isValid": True,
        "quota": {"limit": 100, "used": 100, "remaining": 0},
        "rate_limits": [{
            "window": "7d", "used": 100, "limit": 100,
            "reset_at": "2026-08-01T00:00:00Z",
        }],
        "usage": {
            "today": {"requests": 4, "input_tokens": 10, "actual_cost": 1.25},
        },
    })
    monkeypatch.setattr(store, "_request_json", request)

    snapshot = await store.fetch_usage(account.id)
    again = await store.fetch_usage(account.id)

    assert snapshot["available"] is False
    assert snapshot["known"] is True
    assert snapshot["reason"] == "quota_exhausted"
    assert snapshot["state"] == "exhausted"
    assert snapshot["currency"] == "USD"
    assert snapshot["quota"]["remaining"] == 0
    assert snapshot["windows"][0]["reset_at"] == "2026-08-01T00:00:00Z"
    assert snapshot["usage"]["today"]["actual_cost"] == 1.25
    assert store.cached_quota_decision(account.id) == {
        "available": False, "known": True, "reason": "quota_exhausted",
    }
    assert again == snapshot
    request.assert_awaited_once()


@pytest.mark.asyncio
async def test_subscription_usage_preserves_credit_units(tmp_path, monkeypatch):
    store, account = await _add(tmp_path, monkeypatch)
    monkeypatch.setattr(store, "_request_json", AsyncMock(return_value={
        "mode": "subscription",
        "status": "active",
        "subscription": {
            "daily_usage_credits": 3,
            "daily_limit_credits": 10,
            "weekly_usage_credits": 8,
            "weekly_limit_credits": 50,
        },
        "balance": 25,
    }))

    snapshot = await store.fetch_usage(account.id)

    assert snapshot["available"] is True
    assert snapshot["known"] is True
    assert snapshot["unit"] == "credits"
    assert [window["currency"] for window in snapshot["windows"]] == [
        "credits", "credits",
    ]
    assert snapshot["windows"][0]["remaining"] == 7
    assert snapshot["windows"][0]["utilization"] == 30.0


@pytest.mark.asyncio
async def test_subscription_usd_window_and_expiry_are_normalised(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    monkeypatch.setattr(store, "_request_json", AsyncMock(return_value={
        "mode": "subscription",
        "status": "active",
        "subscription": {
            "planName": "API Pro",
            "daily_usage_usd": "1.25",
            "daily_limit_usd": "5.00",
            "weekly_usage_usd": "20.00",
            "weekly_limit_usd": "20.00",
            "expiry": "2026-09-01T00:00:00Z",
            "daysUntilExpiry": 39,
        },
    }))

    snapshot = await store.fetch_usage(account.id)

    assert snapshot["state"] == "exhausted"
    assert snapshot["currency"] == "USD"
    assert snapshot["plan_name"] == "API Pro"
    assert snapshot["expires_at"] == "2026-09-01T00:00:00Z"
    assert snapshot["days_until_expiry"] == 39
    assert snapshot["windows"][0]["remaining"] == 3.75
    assert snapshot["windows"][1]["remaining"] == 0


@pytest.mark.asyncio
async def test_wallet_negative_one_remaining_means_unlimited(tmp_path, monkeypatch):
    store, account = await _add(tmp_path, monkeypatch)
    monkeypatch.setattr(store, "_request_json", AsyncMock(return_value={
        "mode": "wallet",
        "status": "active",
        "remaining": -1,
        "balance": 5,
    }))

    snapshot = await store.fetch_usage(account.id)

    assert snapshot["state"] == "active"
    assert snapshot["remaining"] == -1
    assert snapshot["available"] is True


@pytest.mark.asyncio
async def test_unrestricted_real_usage_shape_is_unlimited_and_json_safe(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    request = AsyncMock(return_value={
        "balance": 0,
        "isValid": True,
        "mode": "unrestricted",
        "planName": "钱包余额",
        "remaining": 0,
        "unit": "USD",
        "usage": {
            "today": {
                "requests": 2,
                "input_tokens": 10,
                "output_tokens": 3,
                "total_tokens": 13,
                "cache_creation_tokens": 4,
                "cache_read_tokens": 5,
                "cost": 1.5,
                "actual_cost": 0.75,
                "average_duration_ms": float("nan"),
            },
            "total": {
                "requests": 20,
                "total_tokens": 130,
                "actual_cost": 7.5,
            },
            "rpm": 1,
            "tpm": 13,
        },
        "daily_usage": [{
            "date": "2026-07-28",
            "requests": 2,
            "input_tokens": 10,
            "output_tokens": 3,
            "total_tokens": 13,
            "cache_read_tokens": 5,
            "cache_write_tokens": 4,
            "cost": 1.5,
            "actual_cost": 0.75,
            "average_duration_ms": float("inf"),
        }],
        "model_stats": [{
            "model": "claude-test-model",
            "requests": 2,
            "total_tokens": 13,
            "cost": 1.5,
            "actual_cost": 0.75,
            "account_cost": 0.5,
            "average_duration_ms": float("-inf"),
            "untrusted_extra": "must-not-be-forwarded",
        }],
    })
    monkeypatch.setattr(store, "_request_json", request)

    snapshot = await store.fetch_usage(account.id)
    cached = await store.fetch_usage(account.id)

    assert snapshot["mode"] == "unrestricted"
    assert snapshot["state"] == "active"
    assert snapshot["available"] is True
    assert snapshot["known"] is True
    assert snapshot["unlimited"] is True
    assert snapshot["currency"] == "USD"
    assert "balance" not in snapshot
    assert "remaining" not in snapshot
    assert "expires_at" not in snapshot
    assert "days_until_expiry" not in snapshot
    assert snapshot["quota"] is None
    assert snapshot["windows"] == []
    assert snapshot["usage"]["today"]["actual_cost"] == 0.75
    assert snapshot["usage"]["today"]["cost"] == 1.5
    assert "average_duration_ms" not in snapshot["usage"]["today"]
    assert snapshot["usage"]["daily_usage"] == [{
        "date": "2026-07-28",
        "requests": 2,
        "input_tokens": 10,
        "output_tokens": 3,
        "total_tokens": 13,
        "cache_write_tokens": 4,
        "cache_read_tokens": 5,
        "cost": 1.5,
        "actual_cost": 0.75,
    }]
    assert snapshot["usage"]["model_stats"] == [{
        "model": "claude-test-model",
        "requests": 2,
        "total_tokens": 13,
        "cost": 1.5,
        "actual_cost": 0.75,
        "account_cost": 0.5,
    }]
    assert cached == snapshot
    request.assert_awaited_once()
    assert json.loads(json.dumps(snapshot, allow_nan=False)) == snapshot
    assert store.cached_quota_decision(account.id) == {
        "available": True,
        "known": True,
        "reason": "active",
    }


@pytest.mark.asyncio
async def test_unrestricted_stale_refresh_never_resurrects_zero_balance(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    monkeypatch.setattr(store, "_request_json", AsyncMock(return_value={
        "balance": 0,
        "isValid": True,
        "mode": "unrestricted",
        "remaining": 0,
        "usage": {"today": {"actual_cost": 0.75}},
    }))
    success_snapshot = await store.fetch_usage(account.id, force=True)
    assert success_snapshot["unlimited"] is True
    monkeypatch.setattr(
        store,
        "_request_json",
        AsyncMock(side_effect=CloudRouterUpstreamError(
            "upstream_unavailable", status_code=503,
        )),
    )

    snapshot = await store.fetch_usage(account.id, force=True)

    assert snapshot["known"] is False
    assert snapshot["stale"] is True
    assert snapshot["unlimited"] is True
    assert "balance" not in snapshot
    assert "remaining" not in snapshot
    assert snapshot["usage"]["today"]["actual_cost"] == 0.75
    assert snapshot["fetched_at"] == success_snapshot["fetched_at"]
    assert snapshot["refresh_failed_at"]
    assert store.cached_quota_decision(account.id) == {
        "available": True,
        "known": False,
        "reason": "upstream_unavailable",
    }


@pytest.mark.asyncio
async def test_wallet_negative_balance_other_than_unlimited_is_exhausted(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    monkeypatch.setattr(store, "_request_json", AsyncMock(return_value={
        "mode": "wallet",
        "status": "active",
        "balance": -0.5,
    }))

    snapshot = await store.fetch_usage(account.id)

    assert snapshot["balance"] == -0.5
    assert snapshot["state"] == "exhausted"
    assert snapshot["available"] is False


@pytest.mark.asyncio
async def test_nested_negative_one_remaining_is_unlimited_not_exhausted(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    monkeypatch.setattr(store, "_request_json", AsyncMock(return_value={
        "mode": "quota_limited",
        "status": "active",
        "quota": {"limit": 100, "used": 100, "remaining": -1},
        "rate_limits": [{
            "window": "7d",
            "used": 100,
            "limit": 100,
            "remaining": -1,
        }],
    }))

    snapshot = await store.fetch_usage(account.id)

    assert snapshot["state"] == "active"
    assert snapshot["available"] is True
    assert snapshot["quota"]["unlimited"] is True
    assert snapshot["windows"][0]["unlimited"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code,reason", [(401, "invalid_api_key"), (403, "forbidden")])
async def test_usage_auth_failure_is_known_unavailable(
    tmp_path, monkeypatch, status_code, reason,
):
    store, account = await _add(tmp_path, monkeypatch)
    monkeypatch.setattr(store, "_request_json", AsyncMock(side_effect=
        CloudRouterUpstreamError(reason, status_code=status_code)
    ))

    snapshot = await store.fetch_usage(account.id, force=True)

    assert snapshot["status"] == "unavailable"
    assert snapshot["available"] is False
    assert snapshot["known"] is True
    assert snapshot["reason"] == reason


@pytest.mark.asyncio
async def test_timeout_or_5xx_returns_unknown_stale_without_disabling(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    success = AsyncMock(return_value={"mode": "wallet", "status": "active", "balance": 5})
    monkeypatch.setattr(store, "_request_json", success)
    success_snapshot = await store.fetch_usage(account.id, force=True)
    assert success_snapshot["known"] is True
    monkeypatch.setattr(store, "_request_json", AsyncMock(side_effect=
        CloudRouterUpstreamError("upstream_unavailable", status_code=503)
    ))

    snapshot = await store.fetch_usage(account.id, force=True)

    assert snapshot["status"] == "unknown"
    assert snapshot["available"] is True
    assert snapshot["known"] is False
    assert snapshot["stale"] is True
    assert snapshot["last_known_available"] is True
    assert snapshot["fetched_at"] == success_snapshot["fetched_at"]
    assert snapshot["refresh_failed_at"]
    assert store.cached_quota_decision(account.id)["available"] is True


@pytest.mark.asyncio
async def test_unknown_refresh_cannot_resurrect_last_known_dead_key(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    monkeypatch.setattr(store, "_request_json", AsyncMock(return_value={
        "mode": "quota_limited",
        "status": "quota_exhausted",
        "quota": {"limit": 10, "used": 10, "remaining": 0},
    }))
    assert (await store.fetch_usage(account.id, force=True))["available"] is False

    monkeypatch.setattr(
        store,
        "_request_json",
        AsyncMock(side_effect=CloudRouterUpstreamError(
            "upstream_unavailable", status_code=503,
        )),
    )
    first_unknown = await store.fetch_usage(account.id, force=True)
    second_unknown = await store.fetch_usage(account.id, force=True)

    assert first_unknown["known"] is False
    assert first_unknown["available"] is True
    assert first_unknown["last_known_available"] is False
    assert second_unknown["last_known_available"] is False
    assert store.cached_quota_decision(account.id) == {
        "available": False,
        "known": True,
        "reason": "quota_exhausted",
    }


@pytest.mark.asyncio
async def test_probe_models_uses_bounded_non_redirecting_request(
    tmp_path, monkeypatch,
):
    captured = {}

    class Response:
        status_code = 200

        async def aiter_bytes(self):
            yield json.dumps({
                "data": [
                    {"id": "claude-opus-4-8"},
                    {
                        "id": "gpt-5.5",
                        "service_tiers": [{"id": "priority"}],
                    },
                ],
            }).encode()

    class Stream:
        async def __aenter__(self):
            return Response()

        async def __aexit__(self, *_args):
            return False

    class Client:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def stream(self, method, url, *, headers):
            captured.update({"method": method, "url": url, "headers": headers})
            return Stream()

    monkeypatch.setattr(
        "backend.services.cloudrouter_accounts.httpx.AsyncClient", Client,
    )
    store = CloudRouterAccountStore(tmp_path / "accounts")

    models = await store.probe_models("cr-private")

    assert models == {
        "claude": ["claude-opus-4-8"],
        "codex": ["gpt-5.5"],
        "service_tiers": {"gpt-5.5": ["priority"]},
    }
    assert captured["follow_redirects"] is False
    assert captured["method"] == "GET"
    assert captured["headers"]["Authorization"] == "Bearer cr-private"


@pytest.mark.asyncio
async def test_apex_model_probe_uses_apex_endpoint_and_projects_both_providers(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    request = AsyncMock(return_value={
        "models": [
            {"slug": "claude-opus-4-8", "supported_in_api": True},
            {
                "slug": "gpt-5.4",
                "supported_in_api": True,
                "visibility": "list",
                "service_tiers": [
                    {"id": "priority", "name": "Fast"},
                ],
            },
            {"slug": "gpt-hidden", "supported_in_api": True, "visibility": "hide"},
            {"slug": "gpt-disabled", "supported_in_api": False},
        ],
    })
    monkeypatch.setattr(store, "_request_json", request)

    models = await store.probe_models(
        "lck-test-secret",
        api_provider="apex",
    )

    assert models == {
        "claude": ["claude-opus-4-8"],
        "codex": ["gpt-5.4"],
        "service_tiers": {"gpt-5.4": ["priority"]},
    }
    request.assert_awaited_once_with(
        (
            f"{APEX_MODELS_URL}?client_version="
            f"{cloudrouter_module.APEX_CODEX_CLIENT_VERSION}"
        ),
        "lck-test-secret",
    )


@pytest.mark.asyncio
async def test_apex_model_probe_accepts_openai_compatible_response(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "_request_json",
        AsyncMock(return_value={
            "object": "list",
            "data": [
                {"id": "claude-opus-4-8"},
                {
                    "id": "gpt-5.4",
                    "service_tiers": [{"id": "priority"}],
                },
                {"id": "gpt-5.4"},
                {"id": "unknown-model"},
            ],
        }),
    )

    models = await store.probe_models(
        "lck-test-secret",
        api_provider="apex",
    )

    assert models == {
        "claude": ["claude-opus-4-8"],
        "codex": ["gpt-5.4"],
        "service_tiers": {"gpt-5.4": ["priority"]},
    }


@pytest.mark.asyncio
async def test_apibest_empty_authenticated_models_falls_back_to_pricing_catalog(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    request = AsyncMock(side_effect=[
        {"object": "list", "data": [], "success": True},
        {
            "data": [
                {
                    "model_name": "claude-sonnet-5",
                    "supported_endpoint_types": ["anthropic", "openai"],
                },
                {
                    "model_name": "gpt-5.6-luna",
                    "supported_endpoint_types": ["openai-response"],
                    "service_tiers": [{"id": "priority"}],
                },
                {
                    "model_name": "gpt-5.5",
                    "supported_endpoint_types": ["openai"],
                },
                {
                    "model_name": "claude-openai-only",
                    "supported_endpoint_types": ["openai"],
                },
            ],
        },
    ])
    monkeypatch.setattr(store, "_request_json", request)

    models = await store.probe_models("sk-test", api_provider="apibest")

    assert models == {
        "claude": ["claude-sonnet-5"],
        "codex": ["gpt-5.5", "gpt-5.6-luna"],
        "service_tiers": {"gpt-5.6-luna": ["priority"]},
    }
    assert [item.args for item in request.await_args_list] == [
        (APIBEST_MODELS_URL, "sk-test"),
        (APIBEST_PRICING_URL, "sk-test"),
    ]


@pytest.mark.asyncio
async def test_apibest_pricing_rejects_non_string_endpoint_types(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "_request_json",
        AsyncMock(side_effect=[
            {"object": "list", "data": [], "success": True},
            {
                "data": [{
                    "model_name": "gpt-5.6-luna",
                    "supported_endpoint_types": [{}],
                }],
            },
        ]),
    )

    with pytest.raises(
        CloudRouterUpstreamError,
        match="invalid_models_response",
    ):
        await store.probe_models("sk-test", api_provider="apibest")


@pytest.mark.asyncio
async def test_apex_service_tiers_are_persisted_and_reloaded(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={
            "claude": [],
            "codex": ["gpt-5.4", "gpt-5.4-mini"],
            "service_tiers": {"gpt-5.4": ["priority"]},
        }),
    )

    account = await store.add_account(
        "Apex Fast",
        "lck-test-secret",
        api_provider="apex",
    )

    assert account.service_tiers == {"gpt-5.4": ["priority"]}
    assert account.supports_service_tier(
        "codex", "gpt-5.4", "priority"
    )
    assert not account.supports_service_tier(
        "codex", "gpt-5.4-mini", "priority"
    )
    metadata = json.loads((account.root / "account.json").read_text())
    assert metadata["models"] == {
        "claude": [],
        "codex": ["gpt-5.4", "gpt-5.4-mini"],
    }
    assert metadata["service_tiers"] == {
        "gpt-5.4": ["priority"],
    }
    public = store.reload()[0].public_dict()
    assert public["service_tiers"] == {"gpt-5.4": ["priority"]}


@pytest.mark.asyncio
@pytest.mark.parametrize("api_provider", ["cloudrouter", "apibest"])
async def test_generic_api_accounts_require_upstream_fast_capability_and_reload(
    tmp_path, monkeypatch, api_provider,
):
    store = CloudRouterAccountStore(tmp_path / f"{api_provider}-accounts")
    probe = AsyncMock(return_value={
        "claude": [],
        "codex": ["gpt-5.4", "gpt-5.4-mini"],
    })
    monkeypatch.setattr(
        store,
        "probe_models",
        probe,
    )

    account = await store.add_account(
        f"{api_provider} Fast",
        "sk-test-secret",
        api_provider=api_provider,
    )

    assert account.service_tiers == {}
    assert not account.supports_service_tier(
        "codex", "gpt-5.4", "priority",
    )
    assert not account.supports_service_tier(
        "codex", "gpt-5.4-mini", "priority",
    )
    metadata = json.loads((account.root / "account.json").read_text())
    assert metadata["service_tiers"] == {}
    assert metadata["service_tiers_source"] == "none"

    # An intermediate development build persisted model-name inference without
    # provenance. It must not make existing accounts look Fast-capable.
    metadata["service_tiers"] = {"gpt-5.4": ["priority"]}
    metadata["service_tier_denials"] = {"gpt-5.4": ["priority"]}
    metadata.pop("service_tiers_source")
    (account.root / "account.json").write_text(json.dumps(metadata))
    assert store.reload()[0].service_tiers == {}

    with pytest.raises(
        CloudRouterAccountError,
        match="does not advertise service tier",
    ):
        async with store.runtime_admission(
            "codex",
            account.codex_home,
            "gpt-5.4",
            service_tier="priority",
        ):
            pass

    probe.return_value = {
        "claude": [],
        "codex": ["gpt-5.5", "gpt-5.4-mini"],
        "service_tiers": {"gpt-5.5": ["priority"]},
    }
    refreshed = await store.refresh_account(account.id)
    assert refreshed.service_tiers == {"gpt-5.5": ["priority"]}
    refreshed_metadata = json.loads(
        (refreshed.root / "account.json").read_text()
    )
    assert refreshed_metadata["service_tiers_source"] == "upstream"
    assert "service_tier_denials" not in refreshed_metadata
    assert store.reload()[0].service_tiers == {"gpt-5.5": ["priority"]}


@pytest.mark.asyncio
async def test_generic_account_rejects_tiers_claimed_by_none_source(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    metadata_path = account.root / "account.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["service_tiers"] = {"gpt-5.5": ["priority"]}
    assert metadata["service_tiers_source"] == "none"
    metadata_path.write_text(json.dumps(metadata))

    with pytest.raises(
        CloudRouterUnsafePathError,
        match="Inconsistent service tier metadata",
    ):
        store.reload()


@pytest.mark.asyncio
async def test_legacy_apex_uses_exact_nofollow_models_cache_as_fast_candidate(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={
            "claude": [],
            "codex": ["gpt-5.4", "gpt-5.5"],
        }),
    )
    account = await store.add_account(
        "Legacy Apex",
        "lck-test-secret",
        api_provider="apex",
    )
    metadata_path = account.root / "account.json"
    metadata = json.loads(metadata_path.read_text())
    metadata.pop("service_tiers")
    metadata_path.write_text(json.dumps(metadata))
    (account.root / "codex" / "models_cache.json").write_text(json.dumps({
        "models": [
            {
                "slug": "gpt-5.4",
                "service_tiers": [{"id": "priority"}],
            },
            {
                "slug": "gpt-5.5",
                "service_tiers": [],
            },
            {
                "slug": "gpt-not-advertised-by-account",
                "service_tiers": [{"id": "priority"}],
            },
        ],
    }))

    legacy = store.reload()[0]

    assert legacy.service_tiers_explicit is False
    assert legacy.service_tiers == {"gpt-5.4": ["priority"]}
    assert legacy.supports_service_tier(
        "codex", "gpt-5.4", "priority"
    )
    assert not legacy.supports_service_tier(
        "codex", "gpt-5.5", "priority"
    )
    async with store.runtime_admission(
        "codex",
        legacy.codex_home,
        "gpt-5.4",
        service_tier="priority",
    ) as admitted:
        assert admitted.id == legacy.id


@pytest.mark.asyncio
async def test_explicit_apex_and_generic_accounts_never_use_models_cache_fallback(
    tmp_path, monkeypatch,
):
    apex_store = CloudRouterAccountStore(tmp_path / "apex-accounts")
    monkeypatch.setattr(
        apex_store,
        "probe_models",
        AsyncMock(return_value={
            "claude": [],
            "codex": ["gpt-5.4"],
        }),
    )
    apex = await apex_store.add_account(
        "Explicit Apex",
        "lck-test-secret",
        api_provider="apex",
    )
    (apex.root / "codex" / "models_cache.json").write_text(json.dumps({
        "models": [{
            "slug": "gpt-5.4",
            "service_tiers": [{"id": "priority"}],
        }],
    }))
    explicit = apex_store.reload()[0]
    assert explicit.service_tiers_explicit is True
    assert not explicit.supports_service_tier(
        "codex", "gpt-5.4", "priority"
    )

    generic_store, generic = await _add(
        tmp_path,
        monkeypatch,
        models={"claude": [], "codex": ["gpt-provider-specific"]},
    )
    generic_metadata_path = generic.root / "account.json"
    generic_metadata = json.loads(generic_metadata_path.read_text())
    generic_metadata.pop("service_tiers")
    generic_metadata_path.write_text(json.dumps(generic_metadata))
    (generic.root / "codex" / "models_cache.json").write_text(json.dumps({
        "models": [{
            "slug": "gpt-provider-specific",
            "service_tiers": [{"id": "priority"}],
        }],
    }))
    legacy_generic = generic_store.reload()[0]
    assert legacy_generic.service_tiers_explicit is False
    assert not legacy_generic.supports_service_tier(
        "codex", "gpt-provider-specific", "priority"
    )


@pytest.mark.asyncio
async def test_legacy_apex_models_cache_symlink_fails_closed(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={
            "claude": [],
            "codex": ["gpt-5.4"],
        }),
    )
    account = await store.add_account(
        "Legacy Apex",
        "lck-test-secret",
        api_provider="apex",
    )
    metadata_path = account.root / "account.json"
    metadata = json.loads(metadata_path.read_text())
    metadata.pop("service_tiers")
    metadata_path.write_text(json.dumps(metadata))
    outside = tmp_path / "outside-models-cache.json"
    outside.write_text(json.dumps({
        "models": [{
            "slug": "gpt-5.4",
            "service_tiers": [{"id": "priority"}],
        }],
    }))
    (account.root / "codex" / "models_cache.json").symlink_to(outside)

    legacy = store.reload()[0]

    assert legacy.service_tiers == {}
    assert not legacy.supports_service_tier(
        "codex", "gpt-5.4", "priority"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {
            "models": [{
                "slug": "gpt-5.4",
                "service_tiers": [{"id": "priority tier"}],
            }],
        },
        {
            "data": [{
                "id": "gpt-5.4",
                "service_tiers": [{"id": "priority tier"}],
            }],
        },
    ],
)
async def test_apex_probe_rejects_malformed_service_tiers(
    tmp_path, monkeypatch, payload,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "_request_json",
        AsyncMock(return_value=payload),
    )

    with pytest.raises(
        CloudRouterUpstreamError,
        match="invalid_models_response",
    ):
        await store.probe_models(
            "lck-test-secret",
            api_provider="apex",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("api_provider", ["cloudrouter", "apibest"])
async def test_generic_probe_rejects_malformed_service_tiers(
    tmp_path, monkeypatch, api_provider,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "_request_json",
        AsyncMock(return_value={
            "data": [{
                "id": "gpt-5.4",
                "service_tiers": [{"id": "priority tier"}],
            }],
        }),
    )

    with pytest.raises(
        CloudRouterUpstreamError,
        match="invalid_models_response",
    ):
        await store.probe_models(
            "sk-test-secret",
            api_provider=api_provider,
        )


@pytest.mark.asyncio
async def test_model_probe_rejects_unbounded_model_lists(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "_request_json",
        AsyncMock(return_value={
            "models": [
                {"slug": f"gpt-test-{index}"}
                for index in range(cloudrouter_module.MAX_DISCOVERED_MODELS + 1)
            ],
        }),
    )

    with pytest.raises(CloudRouterUpstreamError, match="too_many_models"):
        await store.probe_models("lck-test-secret", api_provider="apex")


@pytest.mark.asyncio
async def test_oversized_model_metadata_never_leaves_a_poisoned_account(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={
            "claude": [],
            "codex": [
                f"gpt-{index}-{'x' * 400}"
                for index in range(cloudrouter_module.MAX_DISCOVERED_MODELS)
            ],
        }),
    )

    with pytest.raises(CloudRouterUpstreamError, match="metadata_too_large"):
        await store.add_account(
            "Apex", "lck-test-secret", api_provider="apex"
        )

    assert store.all_accounts() == []
    assert not (store.root / "apex-1").exists()
    assert not any(
        child.name.startswith(".apex-1.")
        for child in store.root.iterdir()
    )


@pytest.mark.asyncio
async def test_upstream_response_size_is_bounded(tmp_path, monkeypatch):
    class Response:
        status_code = 200

        async def aiter_bytes(self):
            yield b"x" * (MAX_API_RESPONSE_BYTES + 1)

    class Stream:
        async def __aenter__(self):
            return Response()

        async def __aexit__(self, *_args):
            return False

    class Client:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def stream(self, *_args, **_kwargs):
            return Stream()

    monkeypatch.setattr(
        "backend.services.cloudrouter_accounts.httpx.AsyncClient", Client,
    )
    store = CloudRouterAccountStore(tmp_path / "accounts")

    with pytest.raises(CloudRouterUpstreamError, match="response_too_large"):
        await store.probe_models("cr-private")


@pytest.mark.asyncio
async def test_path_traversal_and_symlink_metadata_fail_closed(tmp_path, monkeypatch):
    store, account = await _add(tmp_path, monkeypatch)
    with pytest.raises(CloudRouterAccountNotFound):
        store.account("../cloudrouter-1")

    metadata = account.root / "account.json"
    outside = tmp_path / "outside.json"
    outside.write_text(metadata.read_text())
    metadata.unlink()
    metadata.symlink_to(outside)

    with pytest.raises(CloudRouterUnsafePathError):
        store.reload()


@pytest.mark.asyncio
async def test_claude_settings_allow_hooks_but_reject_routing_tampering(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    settings_path = account.root / "claude" / "settings.json"
    settings = json.loads(settings_path.read_text())
    settings["hooks"] = {"PreToolUse": []}
    settings_path.write_text(json.dumps(settings))
    assert store.reload()[0].id == account.id

    settings["env"]["ANTHROPIC_BASE_URL"] = "https://attacker.invalid"
    settings_path.write_text(json.dumps(settings))
    with pytest.raises(CloudRouterUnsafePathError, match="Claude API routing"):
        store.reload()


@pytest.mark.asyncio
async def test_reload_migrates_unattended_claude_ack_and_preserves_hooks(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    settings_path = account.root / "claude" / "settings.json"
    settings = json.loads(settings_path.read_text())
    settings.pop("skipDangerousModePermissionPrompt")
    settings["hooks"] = {"PreToolUse": [{"matcher": "AskUserQuestion"}]}
    settings_path.write_text(json.dumps(settings))

    assert store.reload()[0].id == account.id
    migrated = json.loads(settings_path.read_text())
    assert migrated["skipDangerousModePermissionPrompt"] is True
    assert migrated["hooks"] == settings["hooks"]
    assert _permissions(settings_path) == 0o600


@pytest.mark.asyncio
async def test_reload_converges_cli_mutated_claude_json_mode_without_data_loss(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    state_path = account.root / "claude" / ".claude.json"
    state = {
        "hasCompletedOnboarding": True,
        "theme": "dark",
        "cliOwnedState": {"kept": True},
    }
    state_path.write_text(json.dumps(state))
    os.chmod(state_path, 0o664)

    assert store.reload()[0].id == account.id
    assert json.loads(state_path.read_text()) == state
    assert _permissions(state_path) == 0o600


@pytest.mark.asyncio
async def test_runtime_admission_converts_storage_oserror_to_safe_failure(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    monkeypatch.setattr(
        store,
        "reload",
        Mock(side_effect=OSError("/private/account became read-only")),
    )

    with pytest.raises(
        CloudRouterUnsafePathError,
        match="account storage is unavailable",
    ) as captured:
        async with store.runtime_admission(
            "claude",
            account.claude_config_dir,
            "claude-opus-4-8",
        ):
            pass

    assert "/private/" not in str(captured.value)


@pytest.mark.asyncio
async def test_configuration_admission_validates_route_without_quota_gate(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    store._quota_cache[account.id] = {
        "known": True,
        "available": False,
        "reason": "exhausted",
    }

    async with store.configuration_admission(
        "codex", account.codex_home,
    ) as admitted:
        assert admitted.id == account.id

    config = account.root / "codex" / "config.toml"
    config.write_text(
        config.read_text().replace(
            CODEX_BASE_URL,
            "https://attacker.invalid/v1",
        ),
    )
    with pytest.raises(CloudRouterUnsafePathError, match="Codex API routing"):
        async with store.configuration_admission(
            "codex", account.codex_home,
        ):
            pass


@pytest.mark.asyncio
async def test_runtime_admission_requires_exact_fast_capability(
    tmp_path, monkeypatch,
):
    cloudrouter_store, cloudrouter = await _add(tmp_path, monkeypatch)
    with pytest.raises(
        CloudRouterAccountError,
        match="does not advertise service tier",
    ):
        async with cloudrouter_store.runtime_admission(
            "codex",
            cloudrouter.codex_home,
            "gpt-5.5",
            service_tier="priority",
        ):
            pass

    apex_store = CloudRouterAccountStore(tmp_path / "apex-accounts")
    monkeypatch.setattr(
        apex_store,
        "probe_models",
        AsyncMock(return_value={
            "claude": [],
            "codex": ["gpt-5.4"],
            "service_tiers": {"gpt-5.4": ["priority"]},
        }),
    )
    apex = await apex_store.add_account(
        "Apex Fast",
        "lck-test-secret",
        api_provider="apex",
    )
    async with apex_store.runtime_admission(
        "codex",
        apex.codex_home,
        "gpt-5.4",
        service_tier="priority",
    ) as admitted:
        assert admitted.id == apex.id


@pytest.mark.asyncio
async def test_codex_provider_and_key_helper_tampering_fail_closed(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    config = account.root / "codex" / "config.toml"
    config.write_text(
        config.read_text().replace(CODEX_BASE_URL, "https://attacker.invalid/v1"),
    )
    with pytest.raises(CloudRouterUnsafePathError, match="Codex API routing"):
        store.reload()

    config.write_text(
        config.read_text().replace(
            "https://attacker.invalid/v1", CODEX_BASE_URL,
        ),
    )
    helper = account.root / "key-helper"
    helper.write_text(helper.read_text() + "\n# modified\n")
    os.chmod(helper, 0o700)
    with pytest.raises(CloudRouterUnsafePathError, match="credential helper"):
        store.reload()


@pytest.mark.asyncio
async def test_codex_cli_personality_migration_is_allowed(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={"claude": [], "codex": ["gpt-5.5"]}),
    )
    account = await store.add_account(
        "Apex", "lck-test-secret", api_provider="apex",
    )
    config = account.root / "codex" / "config.toml"
    config.write_text(
        config.read_text().replace(
            'model_provider = "apexrouter"\n',
            'model_provider = "apexrouter"\npersonality = "pragmatic"\n',
        )
    )
    os.chmod(config, 0o600)

    assert [item.id for item in store.reload()] == [account.id]
    async with store.runtime_admission(
        "codex", account.codex_home, "gpt-5.5",
    ) as admitted:
        assert admitted.id == account.id


@pytest.mark.asyncio
@pytest.mark.parametrize("trust_level", ["trusted", "untrusted"])
async def test_codex_cli_project_trust_state_is_allowed(
    tmp_path, monkeypatch, trust_level,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={"claude": [], "codex": ["gpt-5.5"]}),
    )
    account = await store.add_account(
        "Apex", "lck-test-secret", api_provider="apex",
    )
    project_root = (tmp_path / "project").absolute()
    config = account.root / "codex" / "config.toml"
    with config.open("a") as stream:
        stream.write(
            f'\n[projects.{json.dumps(str(project_root))}]\n'
            f'trust_level = "{trust_level}"\n'
        )

    assert [item.id for item in store.reload()] == [account.id]
    migrated = tomllib.loads(config.read_text())
    assert "projects" not in migrated
    assert migrated["model_provider"] == "apexrouter"
    async with store.runtime_admission(
        "codex", account.codex_home, "gpt-5.5",
    ) as admitted:
        assert admitted.id == account.id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "project_path, project_config",
    [
        ("relative/project", 'trust_level = "trusted"'),
        ("/tmp/project", 'trust_level = "unknown"'),
        (
            "/tmp/project",
            'trust_level = "trusted"\ncommand = "/tmp/untrusted-command"',
        ),
    ],
)
async def test_modified_codex_project_trust_state_fails_closed(
    tmp_path, monkeypatch, project_path, project_config,
):
    store, account = await _add(tmp_path, monkeypatch)
    config = account.root / "codex" / "config.toml"
    with config.open("a") as stream:
        stream.write(
            f'\n[projects.{json.dumps(project_path)}]\n{project_config}\n'
        )

    with pytest.raises(CloudRouterUnsafePathError, match="Codex API routing"):
        store.reload()


@pytest.mark.asyncio
async def test_codex_project_trust_rewrite_failure_fails_closed(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    config = account.root / "codex" / "config.toml"
    with config.open("a") as stream:
        stream.write(
            '\n[projects."/tmp/project"]\ntrust_level = "trusted"\n'
        )
    monkeypatch.setattr(
        cloudrouter_module,
        "_atomic_private_write",
        Mock(side_effect=OSError("read-only filesystem")),
    )

    with pytest.raises(
        CloudRouterUnsafePathError,
        match="Could not secure Codex project state",
    ):
        store.reload()


@pytest.mark.asyncio
async def test_unknown_codex_personality_still_fails_closed(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    config = account.root / "codex" / "config.toml"
    config.write_text(
        config.read_text().replace(
            'model_provider = "cloudrouter"\n',
            'model_provider = "cloudrouter"\npersonality = "injected"\n',
        )
    )
    os.chmod(config, 0o600)

    with pytest.raises(CloudRouterUnsafePathError, match="Codex API routing"):
        store.reload()


@pytest.mark.asyncio
async def test_apex_codex_provider_tampering_fails_closed(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={"claude": [], "codex": ["gpt-5.4"]}),
    )
    account = await store.add_account(
        "Apex", "lck-test-secret", api_provider="apex"
    )
    config = account.root / "codex" / "config.toml"
    config.write_text(
        config.read_text().replace(
            APEX_CODEX_BASE_URL,
            "https://attacker.invalid/v1",
        )
    )

    with pytest.raises(
        CloudRouterUnsafePathError,
        match="Codex API routing",
    ):
        store.reload()


@pytest.mark.asyncio
async def test_api_codex_extra_persistent_command_config_fails_closed(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    config = account.root / "codex" / "config.toml"
    with config.open("a") as stream:
        stream.write(
            '\n[mcp_servers.injected]\ncommand = "/tmp/untrusted-command"\n'
        )

    with pytest.raises(
        CloudRouterUnsafePathError,
        match="Codex API routing",
    ):
        store.reload()


def test_store_rejects_symlink_root(tmp_path):
    target = tmp_path / "target"
    target.mkdir()
    root = tmp_path / "accounts"
    root.symlink_to(target, target_is_directory=True)

    with pytest.raises(CloudRouterUnsafePathError):
        CloudRouterAccountStore(root)


@pytest.mark.asyncio
async def test_managed_metadata_owner_mismatch_fails_closed(tmp_path, monkeypatch):
    store, account = await _add(tmp_path, monkeypatch)
    real_uid = os.getuid()
    monkeypatch.setattr(cloudrouter_module.os, "getuid", lambda: real_uid + 1)

    with pytest.raises(CloudRouterUnsafePathError, match="another owner"):
        cloudrouter_module._open_regular_nofollow(
            account.root / "account.json", maximum=1024 * 1024,
        )


@pytest.mark.asyncio
async def test_key_helper_rejects_non_private_key_file(tmp_path, monkeypatch):
    _store, account = await _add(tmp_path, monkeypatch)
    os.chmod(account.root / "api.key", 0o640)

    process = await asyncio.create_subprocess_exec(
        str(account.root / "key-helper"),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _stderr = await process.communicate()

    assert process.returncode != 0
    assert stdout == b""


def test_runtime_pool_reload_is_deduplicated(monkeypatch):
    class Pool:
        def __init__(self):
            self.calls = 0

        def reload(self):
            self.calls += 1

    claude = Pool()
    codex = Pool()
    monkeypatch.setattr(
        cloudrouter_api, "_runtime_pools", lambda: (claude, codex),
    )
    cloudrouter_api._reload_runtime_pools()
    assert claude.calls == 1
    assert codex.calls == 1

    monkeypatch.setattr(
        cloudrouter_api, "_runtime_pools", lambda: (claude, claude),
    )
    cloudrouter_api._reload_runtime_pools()
    assert claude.calls == 2


def test_unsafe_storage_error_is_not_reported_as_staged_busy_cleanup():
    busy = cloudrouter_api._http_error(
        CloudRouterAccountBusyError("active turn")
    )
    unsafe = cloudrouter_api._http_error(
        CloudRouterUnsafePathError("ancestor changed")
    )

    assert busy.status_code == 409
    assert busy.detail == {
        "message": "active turn",
        "error": "active turn",
        "code": "cleanup_blocked",
        "reason": "active turn",
        "cleanup_pending": True,
    }
    assert unsafe.status_code == 500
    assert unsafe.detail == "API account storage is unsafe"


@pytest.mark.asyncio
async def test_first_api_account_lazily_creates_both_runtime_pools(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv("HOME", str(tmp_path))
    store, account = await _add(tmp_path, monkeypatch)
    dispatcher = types.SimpleNamespace(pool=None, codex_pool=None)
    manager = types.SimpleNamespace(
        read_codex_rate_limits=AsyncMock(),
    )
    fake_main = types.SimpleNamespace(
        cloudrouter_store=store,
        dispatcher=dispatcher,
        instance_manager=manager,
        codex_pool=None,
    )
    monkeypatch.setitem(sys.modules, "backend.main", fake_main)
    # `import backend.main as runtime` may resolve the package attribute when
    # another full-suite test imported the real module first. Patch both import
    # caches so this test remains order-independent.
    import backend
    monkeypatch.setattr(backend, "main", fake_main, raising=False)
    monkeypatch.setattr(
        "backend.config.settings.pool_config_path",
        str(tmp_path / "missing-claude-pool.json"),
    )
    monkeypatch.setattr(
        "backend.config.settings.codex_pool_config_path",
        str(tmp_path / "missing-codex-pool.json"),
    )
    monkeypatch.setattr("backend.config.settings.pool_enabled", False)
    monkeypatch.setattr("backend.config.settings.codex_pool_enabled", False)

    cloudrouter_api._reload_runtime_pools()

    assert dispatcher.pool.select(
        model="claude-opus-4-8"
    ) == account.claude_config_dir
    assert fake_main.codex_pool.select(
        model="gpt-5.5"
    ) == str(Path(account.codex_home).resolve())
    assert dispatcher.codex_pool is fake_main.codex_pool


@pytest.mark.asyncio
async def test_pending_apex_tombstone_initializes_both_pool_tabs_after_restart(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv("HOME", str(tmp_path))
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={"claude": [], "codex": ["gpt-5.5"]}),
    )
    account = await store.add_account(
        "Apex API",
        "lck-secret",
        api_provider="apex",
    )
    await store.stage_retirement(account.id)
    restarted = CloudRouterAccountStore(store.root)
    dispatcher = types.SimpleNamespace(pool=None, codex_pool=None)
    fake_main = types.SimpleNamespace(
        cloudrouter_store=restarted,
        dispatcher=dispatcher,
        instance_manager=types.SimpleNamespace(
            read_codex_rate_limits=AsyncMock(),
        ),
        codex_pool=None,
    )
    monkeypatch.setitem(sys.modules, "backend.main", fake_main)
    import backend
    monkeypatch.setattr(backend, "main", fake_main, raising=False)
    monkeypatch.setattr(
        "backend.config.settings.pool_config_path",
        str(tmp_path / "missing-claude-pool.json"),
    )
    monkeypatch.setattr(
        "backend.config.settings.codex_pool_config_path",
        str(tmp_path / "missing-codex-pool.json"),
    )
    monkeypatch.setattr("backend.config.settings.pool_enabled", False)
    monkeypatch.setattr("backend.config.settings.codex_pool_enabled", False)

    claude, codex = cloudrouter_api._runtime_pools()

    assert claude is not None
    assert codex is not None
    assert claude.list_accounts()[0]["api_account_id"] == account.id
    assert codex.list_accounts()[0]["api_account_id"] == account.id


@pytest.mark.asyncio
async def test_create_endpoint_returns_public_account_quota_and_reloads_pools(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(store, "probe_models", AsyncMock(return_value=MODELS))
    monkeypatch.setattr(store, "_request_json", AsyncMock(return_value={
        "mode": "wallet", "status": "active", "balance": 10,
    }))
    reload_pools = Mock()
    monkeypatch.setattr(cloudrouter_api, "_get_store", lambda: store)
    monkeypatch.setattr(cloudrouter_api, "_reload_runtime_pools", reload_pools)

    result = await cloudrouter_api.create_account(
        _admin_request(),
        cloudrouter_api.CloudRouterAccountCreate(
            name="API account", api_key=SecretStr("cr-private-value"),
        ),
    )

    assert result["id"] == "cloudrouter-1"
    assert result["supported_models"] == sorted(MODELS["claude"] + MODELS["codex"])
    assert result["api_quota"]["state"] == "active"
    assert "cr-private-value" not in json.dumps(result)
    reload_pools.assert_called_once_with()


@pytest.mark.asyncio
async def test_create_endpoint_accepts_apex_provider_without_exposing_key(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={"claude": [], "codex": ["gpt-5.4"]}),
    )
    monkeypatch.setattr(
        store,
        "_request_json",
        AsyncMock(return_value={
            "key_name": "test-key",
            "group_name": "apex-research",
            "used": {
                "requests_5h": 0,
                "requests_day": 0,
                "tokens_day": 0,
                "tokens_month": 0,
            },
            "remaining": {
                "requests_5h": 25_000,
                "requests_day": 50_000,
                "tokens_day": 10_000_000,
                "tokens_month": 100_000_000,
            },
            "limits": {
                "requests_5h": 25_000,
                "requests_day": 50_000,
                "tokens_day": 10_000_000,
                "tokens_month": 100_000_000,
                "concurrency": 20,
            },
        }),
    )
    reload_pools = Mock()
    monkeypatch.setattr(cloudrouter_api, "_get_store", lambda: store)
    monkeypatch.setattr(
        cloudrouter_api,
        "_reload_runtime_pools",
        reload_pools,
    )

    result = await cloudrouter_api.create_account(
        _admin_request(),
        cloudrouter_api.CloudRouterAccountCreate(
            name="Apex API",
            api_key=SecretStr("lck-test-secret"),
            api_provider="apex",
        ),
    )

    assert result["id"] == "apex-1"
    assert result["api_provider"] == "apex"
    assert result["auth_kind"] == "apex_api"
    assert result["providers"] == ["codex"]
    assert result["api_quota"]["known"] is True
    assert result["api_quota"]["group_name"] == "apex-research"
    assert "lck-test-secret" not in json.dumps(result)
    reload_pools.assert_called_once_with()


@pytest.mark.asyncio
async def test_delete_endpoint_stages_busy_account_and_retry_finishes_cleanup(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    monkeypatch.setattr(cloudrouter_api, "_get_store", lambda: store)
    reload_pools = Mock()
    monkeypatch.setattr(cloudrouter_api, "_reload_runtime_pools", reload_pools)

    @asynccontextmanager
    async def busy_fence(_account, _store):
        raise CloudRouterAccountBusyError("active turn")
        yield

    monkeypatch.setattr(
        cloudrouter_api, "_runtime_retirement_fence", busy_fence,
    )
    with pytest.raises(HTTPException) as blocked:
        await cloudrouter_api.retire_account(_admin_request(), account.id)
    assert blocked.value.status_code == 409
    assert blocked.value.detail == {
        "message": "active turn",
        "error": "active turn",
        "code": "cleanup_blocked",
        "reason": "active turn",
        "cleanup_pending": True,
    }
    pending = store.account(account.id)
    assert pending is not None
    assert pending.retired is True
    assert pending.cleanup_pending is True
    assert pending.cleanup_code == "cleanup_blocked"
    assert pending.cleanup_reason == "active turn"
    assert pending.cleanup_last_attempt_at is not None
    assert pending.cleanup_last_error_at is not None
    assert (account.root / "api.key").is_file()
    assert store.visible_accounts() == [pending]

    restarted = CloudRouterAccountStore(store.root)
    restored = restarted.account(account.id)
    assert restored is not None
    assert restored.cleanup_code == pending.cleanup_code
    assert restored.cleanup_reason == pending.cleanup_reason
    assert restored.cleanup_last_attempt_at == pending.cleanup_last_attempt_at
    assert restored.cleanup_last_error_at == pending.cleanup_last_error_at

    listed = await cloudrouter_api.list_accounts(_admin_request())
    assert listed[0]["id"] == account.id
    assert listed[0]["cleanup_pending"] is True
    assert listed[0]["cleanup_code"] == "cleanup_blocked"
    assert listed[0]["cleanup_reason"] == "active turn"
    assert listed[0]["cleanup_last_attempt_at"] is not None
    assert listed[0]["cleanup_last_error_at"] is not None
    assert listed[0]["api_quota"] is None

    @asynccontextmanager
    async def idle_fence(_account, _store):
        yield

    monkeypatch.setattr(
        cloudrouter_api, "_runtime_retirement_fence", idle_fence,
    )
    result = await cloudrouter_api.retire_account(
        _admin_request(), account.id,
    )
    assert result["ok"] is True
    assert result["retired"] is True
    assert result["cleanup_pending"] is False
    assert result["cleanup_code"] is None
    assert result["cleanup_reason"] is None
    assert result["cleanup_last_attempt_at"] is None
    assert result["cleanup_last_error_at"] is None
    assert result["key_hint"] == ""
    assert not (account.root / "api.key").exists()
    assert store.visible_accounts() == []
    completed_metadata = json.loads(
        (account.root / "account.json").read_text()
    )
    assert completed_metadata["cleanup_code"] is None
    assert completed_metadata["cleanup_reason"] is None
    assert completed_metadata["cleanup_last_attempt_at"] is None
    assert completed_metadata["cleanup_last_error_at"] is None
    assert reload_pools.call_count >= 4


@pytest.mark.asyncio
async def test_delete_does_not_record_diagnostics_after_storage_failure(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    monkeypatch.setattr(cloudrouter_api, "_get_store", lambda: store)
    monkeypatch.setattr(cloudrouter_api, "_reload_runtime_pools", Mock())
    record_failure = AsyncMock()
    monkeypatch.setattr(store, "record_cleanup_failure", record_failure)

    @asynccontextmanager
    async def unsafe_fence(_account, _store):
        raise CloudRouterUnsafePathError("account root changed")
        yield

    monkeypatch.setattr(
        cloudrouter_api, "_runtime_retirement_fence", unsafe_fence,
    )

    with pytest.raises(HTTPException) as failed:
        await cloudrouter_api.retire_account(_admin_request(), account.id)

    assert failed.value.status_code == 500
    assert failed.value.detail == "API account storage is unsafe"
    record_failure.assert_not_awaited()


def _install_retirement_runtime(
    monkeypatch,
    *,
    store,
    instance_manager,
    dispatcher=None,
):
    runtime = types.SimpleNamespace(
        cloudrouter_store=store,
        instance_manager=instance_manager,
        dispatcher=dispatcher or types.SimpleNamespace(
            api_account_aux_runtime_users=Mock(return_value=[]),
            codex_monitor_runtime_users=AsyncMock(return_value=[]),
        ),
        task_migrator=None,
    )
    monkeypatch.setitem(sys.modules, "backend.main", runtime)
    import backend
    monkeypatch.setattr(backend, "main", runtime, raising=False)
    return runtime


@pytest.mark.asyncio
async def test_cloudrouter_retirement_redacts_migration_failure(
    tmp_path, monkeypatch,
):
    from backend.services.task_migrator import MigrationError

    store, account = await _add(tmp_path, monkeypatch)
    manager = types.SimpleNamespace()
    runtime = _install_retirement_runtime(
        monkeypatch,
        store=store,
        instance_manager=manager,
    )

    class BusyMigrator:
        @asynccontextmanager
        async def api_account_retirement_guard(self):
            raise MigrationError("active migration sk-secret-value")
            yield

    runtime.task_migrator = BusyMigrator()

    with pytest.raises(CloudRouterAccountBusyError) as blocked:
        async with cloudrouter_api._runtime_retirement_fence(account, store):
            pass

    assert blocked.value.code == "migration_busy"
    assert blocked.value.reason == (
        "API account cleanup is blocked by an active task migration; "
        "retry after it finishes"
    )
    assert "sk-secret-value" not in blocked.value.reason


@pytest.mark.asyncio
async def test_cloudrouter_retirement_preserves_storage_integrity_failure(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    manager = types.SimpleNamespace(
        api_account_runtime_users=AsyncMock(
            side_effect=CloudRouterUnsafePathError("account root changed"),
        ),
    )
    _install_retirement_runtime(
        monkeypatch,
        store=store,
        instance_manager=manager,
    )

    with pytest.raises(
        CloudRouterUnsafePathError,
        match="account root changed",
    ):
        async with cloudrouter_api._runtime_retirement_fence(account, store):
            pass


@pytest.mark.asyncio
async def test_cloudrouter_retirement_blocks_persisted_codex_monitor_owner(
    tmp_path,
    monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    manager = types.SimpleNamespace(
        api_account_runtime_users=AsyncMock(return_value=[]),
        begin_codex_app_server_home_maintenance=AsyncMock(),
        end_codex_app_server_home_maintenance=AsyncMock(),
        detach_api_account_containers=AsyncMock(),
    )
    dispatcher = types.SimpleNamespace(
        api_account_aux_runtime_users=Mock(return_value=[]),
        codex_monitor_runtime_users=AsyncMock(
            return_value=["monitor 17"]
        ),
    )
    _install_retirement_runtime(
        monkeypatch,
        store=store,
        instance_manager=manager,
        dispatcher=dispatcher,
    )

    with pytest.raises(
        CloudRouterAccountBusyError,
        match="monitor 17",
    ) as blocked:
        async with cloudrouter_api._runtime_retirement_fence(
            account,
            store,
        ):
            pass

    assert blocked.value.code == "runtime_busy"
    manager.begin_codex_app_server_home_maintenance.assert_not_awaited()


@pytest.mark.asyncio
async def test_cloudrouter_retirement_rechecks_monitor_after_home_fence(
    tmp_path,
    monkeypatch,
):
    """The post-maintenance check closes precheck-to-fence admission races."""

    store, account = await _add(tmp_path, monkeypatch)
    manager = types.SimpleNamespace(
        api_account_runtime_users=AsyncMock(return_value=[]),
        begin_codex_app_server_home_maintenance=AsyncMock(),
        end_codex_app_server_home_maintenance=AsyncMock(),
        detach_api_account_containers=AsyncMock(),
    )
    monitor_users = AsyncMock(
        side_effect=[[], ["monitor 18"]]
    )
    dispatcher = types.SimpleNamespace(
        api_account_aux_runtime_users=Mock(return_value=[]),
        codex_monitor_runtime_users=monitor_users,
    )
    _install_retirement_runtime(
        monkeypatch,
        store=store,
        instance_manager=manager,
        dispatcher=dispatcher,
    )

    with pytest.raises(
        CloudRouterAccountBusyError,
        match="acquired a runtime user",
    ):
        async with cloudrouter_api._runtime_retirement_fence(
            account,
            store,
        ):
            pass

    assert monitor_users.await_count == 2
    manager.begin_codex_app_server_home_maintenance.assert_awaited_once_with(
        account.codex_home,
        require_idle=True,
    )
    manager.end_codex_app_server_home_maintenance.assert_awaited_once_with(
        account.codex_home
    )
    manager.detach_api_account_containers.assert_not_awaited()


@pytest.mark.asyncio
async def test_apex_without_claude_models_skips_claude_container_scan(
    tmp_path, monkeypatch,
):
    store = CloudRouterAccountStore(tmp_path / "accounts")
    monkeypatch.setattr(
        store,
        "probe_models",
        AsyncMock(return_value={"claude": [], "codex": ["gpt-5.5"]}),
    )
    account = await store.add_account(
        "Apex API",
        "lck-secret",
        api_provider="apex",
    )
    manager = types.SimpleNamespace(
        api_account_runtime_users=AsyncMock(return_value=[]),
        begin_codex_app_server_home_maintenance=AsyncMock(return_value=False),
        end_codex_app_server_home_maintenance=AsyncMock(),
        detach_api_account_containers=AsyncMock(
            side_effect=AssertionError("Account has no Claude models"),
        ),
    )
    _install_retirement_runtime(
        monkeypatch,
        store=store,
        instance_manager=manager,
    )
    monkeypatch.setattr(cloudrouter_api, "_get_store", lambda: store)
    monkeypatch.setattr(cloudrouter_api, "_reload_runtime_pools", Mock())

    result = await cloudrouter_api.retire_account(
        _admin_request(), account.id,
    )

    assert result["ok"] is True
    assert result["cleanup_pending"] is False
    manager.detach_api_account_containers.assert_not_awaited()
    manager.end_codex_app_server_home_maintenance.assert_awaited_once_with(
        account.codex_home
    )


@pytest.mark.asyncio
async def test_cloudrouter_retirement_container_failure_is_busy_and_releases_home(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    manager = types.SimpleNamespace(
        api_account_runtime_users=AsyncMock(return_value=[]),
        begin_codex_app_server_home_maintenance=AsyncMock(return_value=False),
        end_codex_app_server_home_maintenance=AsyncMock(),
        detach_api_account_containers=AsyncMock(
            side_effect=RuntimeError("Docker unavailable"),
        ),
    )
    _install_retirement_runtime(
        monkeypatch,
        store=store,
        instance_manager=manager,
    )

    with pytest.raises(
        CloudRouterAccountBusyError,
        match="could not be verified",
    ) as blocked:
        async with cloudrouter_api._runtime_retirement_fence(account, store):
            pass

    assert blocked.value.code == "runtime_verification_failed"
    manager.end_codex_app_server_home_maintenance.assert_awaited_once_with(
        account.codex_home
    )


@pytest.mark.asyncio
async def test_retirement_home_release_failure_never_reaches_finalize(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    manager = types.SimpleNamespace(
        api_account_runtime_users=AsyncMock(return_value=[]),
        begin_codex_app_server_home_maintenance=AsyncMock(return_value=False),
        end_codex_app_server_home_maintenance=AsyncMock(
            side_effect=RuntimeError("release failed"),
        ),
        detach_api_account_containers=AsyncMock(return_value=0),
    )
    _install_retirement_runtime(
        monkeypatch,
        store=store,
        instance_manager=manager,
    )

    with pytest.raises(
        CloudRouterAccountBusyError,
        match="could not be verified",
    ):
        async with cloudrouter_api._runtime_retirement_fence(account, store):
            raise AssertionError("fence must not yield after release failure")

    assert (account.root / "api.key").is_file()


@pytest.mark.asyncio
async def test_retirement_cancellation_releases_codex_home(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    detach_started = asyncio.Event()

    async def wait_for_cancel(_account):
        detach_started.set()
        await asyncio.Event().wait()

    manager = types.SimpleNamespace(
        api_account_runtime_users=AsyncMock(return_value=[]),
        begin_codex_app_server_home_maintenance=AsyncMock(return_value=False),
        end_codex_app_server_home_maintenance=AsyncMock(),
        detach_api_account_containers=AsyncMock(side_effect=wait_for_cancel),
    )
    _install_retirement_runtime(
        monkeypatch,
        store=store,
        instance_manager=manager,
    )

    async def run_fence():
        async with cloudrouter_api._runtime_retirement_fence(account, store):
            pass

    request_task = asyncio.create_task(run_fence())
    await asyncio.wait_for(detach_started.wait(), timeout=1)
    request_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request_task
    manager.end_codex_app_server_home_maintenance.assert_awaited_once_with(
        account.codex_home
    )


@pytest.mark.asyncio
async def test_fetch_usage_lease_blocks_cleanup_and_never_republishes_cache(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def delayed_request(_url, _api_key):
        entered.set()
        await release.wait()
        return {"mode": "wallet", "status": "active", "balance": 7}

    monkeypatch.setattr(store, "_request_json", delayed_request)
    usage_task = asyncio.create_task(store.fetch_usage(account.id, force=True))
    await entered.wait()
    assert store.active_credential_users(account.id) == 1

    staged = await store.stage_retirement(account.id)
    assert staged.cleanup_pending is True
    assert store._quota_cache.get(account.id) is None
    release.set()
    result = await usage_task

    assert result["state"] == "active"
    assert store.active_credential_users(account.id) == 0
    assert store._quota_cache.get(account.id) is None


@pytest.mark.asyncio
async def test_retire_does_not_follow_nested_symlink(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    external = tmp_path / "external"
    external.mkdir()
    secret = external / "keep.txt"
    secret.write_text("keep")
    plugins = account.root / "claude" / "plugins"
    plugins.mkdir()
    (plugins / "outside").symlink_to(external, target_is_directory=True)

    await store.retire_account(account.id)

    assert secret.read_text() == "keep"
    assert not plugins.exists()


@pytest.mark.asyncio
async def test_stage_account_ancestor_swap_never_writes_external_metadata(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    original_root = tmp_path / "original-account"
    external_root = tmp_path / "external-account"
    external_root.mkdir()
    external_metadata = external_root / "account.json"
    external_metadata.write_text('{"sentinel":"external"}\n')
    original_atomic = cloudrouter_module._atomic_private_json_at
    swapped = False

    def swap_before_atomic(parent_fd, name, value, **kwargs):
        nonlocal swapped
        if not swapped:
            swapped = True
            account.root.rename(original_root)
            account.root.symlink_to(external_root, target_is_directory=True)
        return original_atomic(parent_fd, name, value, **kwargs)

    monkeypatch.setattr(
        cloudrouter_module,
        "_atomic_private_json_at",
        swap_before_atomic,
    )

    with pytest.raises(
        CloudRouterUnsafePathError,
        match="directory changed",
    ):
        await store.stage_retirement(account.id)

    original_metadata = json.loads(
        (original_root / "account.json").read_text()
    )
    assert original_metadata["retired"] is True
    assert original_metadata["cleanup_pending"] is True
    assert external_metadata.read_text() == '{"sentinel":"external"}\n'


@pytest.mark.asyncio
async def test_finalize_account_ancestor_swap_never_deletes_external_tree(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    await store.stage_retirement(account.id)
    original_root = tmp_path / "pending-account"
    external_root = tmp_path / "external-account"
    (external_root / "claude" / "plugins").mkdir(parents=True)
    (external_root / "codex").mkdir()
    claude_sentinel = external_root / "claude" / "plugins" / "keep.txt"
    codex_sentinel = external_root / "codex" / "keep.txt"
    claude_sentinel.write_text("keep")
    codex_sentinel.write_text("keep")
    original_remove = store._remove_except
    swapped = False

    def swap_before_runtime_open(*args):
        nonlocal swapped
        if not swapped:
            swapped = True
            account.root.rename(original_root)
            account.root.symlink_to(external_root, target_is_directory=True)
        return original_remove(*args)

    monkeypatch.setattr(store, "_remove_except", swap_before_runtime_open)

    with pytest.raises(
        CloudRouterUnsafePathError,
        match="directory changed",
    ):
        await store.finalize_retirement(account.id)

    assert claude_sentinel.read_text() == "keep"
    assert codex_sentinel.read_text() == "keep"
    original_metadata = json.loads(
        (original_root / "account.json").read_text()
    )
    assert original_metadata["retired"] is True
    assert original_metadata["cleanup_pending"] is True
    assert store.account(account.id).cleanup_pending is True


@pytest.mark.asyncio
async def test_store_root_replacement_fails_identity_check_before_cleanup(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    await store.stage_retirement(account.id)
    original_store = tmp_path / "original-store"
    external_store = tmp_path / "external-store"
    external_store.mkdir()
    sentinel = external_store / "keep.txt"
    sentinel.write_text("keep")
    store.root.rename(original_store)
    store.root.symlink_to(external_store, target_is_directory=True)

    with pytest.raises(CloudRouterUnsafePathError, match="store root"):
        await store.finalize_retirement(account.id)

    assert sentinel.read_text() == "keep"
    metadata = json.loads(
        (original_store / account.id / "account.json").read_text()
    )
    assert metadata["cleanup_pending"] is True


@pytest.mark.asyncio
async def test_retire_rejects_symlink_credential_without_following_it(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    external = tmp_path / "external-key"
    external.write_text("outside")
    (account.root / "api.key").unlink()
    (account.root / "api.key").symlink_to(external)

    with pytest.raises(CloudRouterUnsafePathError):
        await store.retire_account(account.id)

    pending = store.account(account.id)
    assert pending is not None
    assert pending.retired is True
    assert pending.cleanup_pending is True
    assert external.read_text() == "outside"
