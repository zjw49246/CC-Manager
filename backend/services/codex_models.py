"""Codex model catalog: per-model reasoning effort support.

Source of truth: Codex CLI 服务端模型列表（~/.codex/models_cache.json, 2026-07-19 实测）。
GPT-5.6 是一个家族、三个模型（无裸 "gpt-5.6" ID）：
  - gpt-5.6-sol   (GPT-5.6 Sol,   frontier)  efforts: low..max + ultra
  - gpt-5.6-terra (GPT-5.6 Terra, balanced)  efforts: low..max + ultra
  - gpt-5.6-luna  (GPT-5.6 Luna,  fast)      efforts: low..max
旧模型（gpt-5.5 及更早）只支持 low..xhigh。

Fast mode is a service tier, independent from the selected model and its
reasoning effort.  CCM stores the upstream canonical values:
  - default  -> Standard
  - priority -> Fast
"""

from backend.config import settings

# 档位从低到高的全序，用于把不支持的高档位向下夹到该模型的最高档
EFFORT_ORDER = ["low", "medium", "high", "xhigh", "max", "ultra"]

DEFAULT_CODEX_SERVICE_TIER = "default"
FAST_CODEX_SERVICE_TIER = "priority"
CODEX_SERVICE_TIERS = (
    DEFAULT_CODEX_SERVICE_TIER,
    FAST_CODEX_SERVICE_TIER,
)

# Codex CLI model catalog (`service_tiers[].id == "priority"`). This is the
# request/UI safety gate for native accounts. API gateway accounts additionally
# require their own catalog capability before CCM sends a priority request.
CODEX_MODEL_SERVICE_TIERS: dict[str, list[str]] = {
    "gpt-5.6-sol": ["default", "priority"],
    "gpt-5.6-terra": ["default", "priority"],
    "gpt-5.6-luna": ["default", "priority"],
    "gpt-5.5": ["default", "priority"],
    "gpt-5.4": ["default", "priority"],
    "gpt-5.4-mini": ["default"],
    "gpt-5.3-codex-spark": ["default"],
}

# 基线档位：codex_effort_options（gpt-5.5 及更早的模型）
CODEX_MODEL_EFFORTS: dict[str, list[str]] = {
    "gpt-5.6-sol": ["low", "medium", "high", "xhigh", "max", "ultra"],
    "gpt-5.6-terra": ["low", "medium", "high", "xhigh", "max", "ultra"],
    "gpt-5.6-luna": ["low", "medium", "high", "xhigh", "max"],
}


# context_window per model（~/.codex/models_cache.json 实测，2026-07-19：
# gpt-5.6-* / gpt-5.5 / gpt-5.4 / gpt-5.4-mini 均 272000，gpt-5.3-codex-spark 128000）
CODEX_CONTEXT_WINDOWS: dict[str, int] = {
    "gpt-5.6-sol": 272_000,
    "gpt-5.6-terra": 272_000,
    "gpt-5.6-luna": 272_000,
    "gpt-5.5": 272_000,
    "gpt-5.4": 272_000,
    "gpt-5.4-mini": 272_000,
    "gpt-5.3-codex-spark": 128_000,
}
DEFAULT_CODEX_CONTEXT_WINDOW = 272_000


def supported_codex_service_tiers(model: str | None) -> list[str]:
    """Return the statically advertised service tiers for a Codex model."""
    if not model or model == "default":
        model = settings.default_codex_model
    return CODEX_MODEL_SERVICE_TIERS.get(
        model,
        [DEFAULT_CODEX_SERVICE_TIER],
    )


def validate_codex_service_tier(
    provider: str | None,
    model: str | None,
    service_tier: str | None,
) -> str:
    """Validate the merged Task provider/model/service-tier configuration.

    Standard is valid for every provider. Fast is Codex-only and is accepted
    here only for models whose shipped catalog advertises the priority tier.
    The transport performs a second live capability check before starting a
    turn so a route cannot silently downgrade Fast to Standard.
    """
    raw_provider = (
        settings.default_provider
        if provider is None
        else provider
    )
    if not isinstance(raw_provider, str) or not raw_provider.strip():
        raise ValueError("provider must be 'claude' or 'codex'")
    normalized_provider = raw_provider.strip().lower()
    if normalized_provider not in {"claude", "codex"}:
        raise ValueError("provider must be 'claude' or 'codex'")

    tier = service_tier or DEFAULT_CODEX_SERVICE_TIER
    if tier not in CODEX_SERVICE_TIERS:
        raise ValueError(
            "codex_service_tier must be 'default' or 'priority'"
        )
    if tier == DEFAULT_CODEX_SERVICE_TIER:
        return tier

    if normalized_provider != "codex":
        raise ValueError("Codex Fast mode is only available for Codex tasks")

    resolved_model = (
        settings.default_codex_model
        if not model or model == "default"
        else model
    )
    if tier not in supported_codex_service_tiers(resolved_model):
        raise ValueError(
            f"Codex Fast mode is not supported by model '{resolved_model}'"
        )
    return tier


def codex_context_window(model: str | None) -> int:
    """Context window for a codex model (falls back to the family default)."""
    if not model or model == "default":
        model = settings.default_codex_model
    return CODEX_CONTEXT_WINDOWS.get(model, DEFAULT_CODEX_CONTEXT_WINDOW)


def base_codex_efforts() -> list[str]:
    return [e.strip() for e in settings.codex_effort_options.split(",") if e.strip()]


def supported_codex_efforts(model: str | None) -> list[str]:
    """Effort levels supported by the given codex model (falls back to the base list)."""
    if not model or model == "default":
        model = settings.default_codex_model
    return CODEX_MODEL_EFFORTS.get(model, base_codex_efforts())


def clamp_codex_effort(model: str | None, effort: str | None) -> str | None:
    """Clamp an effort level to what the model supports.

    Supported efforts pass through; unsupported ones clamp to the model's
    highest supported level (e.g. "max" on gpt-5.5 → "xhigh") instead of the
    legacy behavior of silently dropping the flag. Unknown effort strings
    return None so the CLI default applies.
    """
    if not effort:
        return None
    supported = supported_codex_efforts(model)
    if effort in supported:
        return effort
    if effort not in EFFORT_ORDER:
        return None
    # 向下夹到该模型支持的最高档
    idx = EFFORT_ORDER.index(effort)
    for lower in reversed(EFFORT_ORDER[:idx]):
        if lower in supported:
            return lower
    return None
