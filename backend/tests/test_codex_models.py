"""Codex 模型目录覆盖 GPT-6 Astra 及 GPT-5.6 sol/terra/luna。

GPT-6 Astra 的 low..ultra 与 priority 由 2026-09-05 model/list 确认。
证据：Codex CLI 0.144.6 服务端模型列表（~/.codex/models_cache.json）：
sol/terra 支持 effort low..ultra，luna 支持 low..max，gpt-5.5 及更早只到 xhigh。
"""

import pytest

from backend.config import settings
from backend.services.codex_models import (
    CODEX_MODEL_SERVICE_TIERS,
    CODEX_MODEL_EFFORTS,
    clamp_codex_effort,
    supported_codex_service_tiers,
    supported_codex_efforts,
    validate_codex_service_tier,
)

GPT56_MODELS = ["gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna"]


def _option_list() -> list[str]:
    return [m.strip() for m in settings.codex_model_options.split(",") if m.strip()]


def test_gpt6_astra_is_selectable_with_native_fast_support():
    assert "gpt-6-astra" in _option_list()
    assert supported_codex_service_tiers("gpt-6-astra") == ["default", "priority"]
    assert validate_codex_service_tier("codex", "gpt-6-astra", "priority") == "priority"


@pytest.mark.parametrize("effort", ["low", "medium", "high", "xhigh", "max", "ultra"])
def test_gpt6_astra_preserves_supported_efforts(effort):
    assert effort in supported_codex_efforts("gpt-6-astra")
    assert clamp_codex_effort("gpt-6-astra", effort) == effort


def test_codex_model_options_contain_all_three_gpt56_models():
    options = _option_list()
    for model in GPT56_MODELS:
        assert model in options, f"{model} missing from codex_model_options"


def test_codex_model_options_have_no_bare_gpt56():
    # 裸 "gpt-5.6" 不是有效的 Codex 模型 ID（服务端只有 -sol/-terra/-luna）
    assert "gpt-5.6" not in _option_list()


def test_gpt56_sol_terra_support_max_and_ultra():
    for model in ("gpt-5.6-sol", "gpt-5.6-terra"):
        efforts = supported_codex_efforts(model)
        assert "max" in efforts
        assert "ultra" in efforts


def test_gpt56_luna_supports_max_but_not_ultra():
    efforts = supported_codex_efforts("gpt-5.6-luna")
    assert "max" in efforts
    assert "ultra" not in efforts


def test_older_models_fall_back_to_base_efforts():
    assert supported_codex_efforts("gpt-5.5") == ["low", "medium", "high", "xhigh"]
    assert supported_codex_efforts("gpt-5.4-mini") == ["low", "medium", "high", "xhigh"]


def test_default_model_used_when_model_is_none_or_default():
    expected = supported_codex_efforts(settings.default_codex_model)
    assert supported_codex_efforts(None) == expected
    assert supported_codex_efforts("default") == expected


def test_clamp_passes_supported_effort_through():
    assert clamp_codex_effort("gpt-5.6-sol", "max") == "max"
    assert clamp_codex_effort("gpt-5.6-sol", "ultra") == "ultra"
    assert clamp_codex_effort("gpt-5.6-luna", "max") == "max"
    assert clamp_codex_effort("gpt-5.5", "xhigh") == "xhigh"


def test_clamp_lowers_unsupported_effort_to_model_max():
    # 旧行为是静默丢弃 max（不传 flag）；现在夹到该模型最高档
    assert clamp_codex_effort("gpt-5.5", "max") == "xhigh"
    assert clamp_codex_effort("gpt-5.5", "ultra") == "xhigh"
    assert clamp_codex_effort("gpt-5.6-luna", "ultra") == "max"


def test_clamp_handles_none_and_unknown():
    assert clamp_codex_effort("gpt-5.6-sol", None) is None
    assert clamp_codex_effort("gpt-5.6-sol", "bogus") is None


def test_effort_map_keys_are_valid_model_options():
    options = _option_list()
    for model in CODEX_MODEL_EFFORTS:
        assert model in options


def test_fast_service_tier_capabilities_match_catalog():
    for model in (
        "gpt-6-astra",
        "gpt-5.6-sol",
        "gpt-5.6-terra",
        "gpt-5.6-luna",
        "gpt-5.5",
        "gpt-5.4",
    ):
        assert supported_codex_service_tiers(model) == ["default", "priority"]

    assert supported_codex_service_tiers("gpt-5.4-mini") == ["default"]
    assert supported_codex_service_tiers("gpt-5.3-codex-spark") == ["default"]
    assert supported_codex_service_tiers("future-model") == ["default"]


def test_default_model_service_tier_uses_configured_model():
    assert supported_codex_service_tiers(None) == CODEX_MODEL_SERVICE_TIERS[
        settings.default_codex_model
    ]
    assert supported_codex_service_tiers("default") == CODEX_MODEL_SERVICE_TIERS[
        settings.default_codex_model
    ]


def test_validate_fast_service_tier_requires_codex_and_supported_model():
    assert (
        validate_codex_service_tier("codex", "gpt-5.6-sol", "priority")
        == "priority"
    )
    assert validate_codex_service_tier("claude", "opus", "default") == "default"

    with pytest.raises(ValueError, match="only available for Codex"):
        validate_codex_service_tier("claude", "gpt-5.6-sol", "priority")
    with pytest.raises(ValueError, match="not supported by model"):
        validate_codex_service_tier("codex", "gpt-5.4-mini", "priority")
    with pytest.raises(ValueError, match="must be 'default' or 'priority'"):
        validate_codex_service_tier("codex", "gpt-5.6-sol", "turbo")
    with pytest.raises(ValueError, match="provider must be"):
        validate_codex_service_tier("", "gpt-5.6-sol", "priority")
    with pytest.raises(ValueError, match="provider must be"):
        validate_codex_service_tier("unknown", None, "default")


# ---------------------------------------------------------------------------
# Context windows（~/.codex/models_cache.json 实测，2026-07-19）
# ---------------------------------------------------------------------------

from backend.services.codex_models import (
    codex_context_window,
    CODEX_CONTEXT_WINDOWS,
    DEFAULT_CODEX_CONTEXT_WINDOW,
)


class TestCodexContextWindow:
    def test_known_models(self):
        assert codex_context_window("gpt-5.6-sol") == 272_000
        assert codex_context_window("gpt-5.5") == 272_000
        assert codex_context_window("gpt-5.3-codex-spark") == 128_000

    def test_unknown_model_falls_back_to_default(self):
        assert codex_context_window("gpt-9000") == DEFAULT_CODEX_CONTEXT_WINDOW

    def test_none_and_default_use_configured_default_model(self):
        from backend.config import settings
        expected = CODEX_CONTEXT_WINDOWS.get(
            settings.default_codex_model, DEFAULT_CODEX_CONTEXT_WINDOW
        )
        assert codex_context_window(None) == expected
        assert codex_context_window("default") == expected
