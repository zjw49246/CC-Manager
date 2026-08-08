"""Independent Browser Review runtime selection for Task-owned harness runs."""

from __future__ import annotations

from typing import Any

from backend.config import settings
from backend.models.task import Task
from backend.services.claude_models import (
    CLAUDE_MODEL_EFFORTS,
    supported_claude_efforts,
)
from backend.services.codex_models import (
    CODEX_MODEL_EFFORTS,
    CODEX_MODEL_SERVICE_TIERS,
    CODEX_SERVICE_TIERS,
    supported_codex_efforts,
    validate_codex_service_tier,
)


HARNESS_RUNTIME_METADATA_KEY = "test_harness_runtime"
HARNESS_RUNTIME_CONFIG_VERSION = 1
_PROVIDERS = frozenset({"claude", "codex"})


def _csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _provider_options() -> list[str]:
    providers = [
        item for item in _csv(settings.provider_options) if item in _PROVIDERS
    ]
    return providers or ["codex", "claude"]


def _models(provider: str) -> list[str]:
    return _csv(
        settings.codex_model_options if provider == "codex" else settings.model_options
    )


def _default_model(provider: str) -> str:
    return (
        settings.default_codex_model
        if provider == "codex"
        else settings.default_model
    )


def harness_runtime_catalog() -> dict[str, Any]:
    """Return the static runtime choices shared by the API and right panel."""

    providers = _provider_options()
    default_provider = (
        settings.default_provider
        if settings.default_provider in providers
        else providers[0]
    )
    return {
        "default_provider": default_provider,
        "providers": providers,
        "default_models": {
            "claude": settings.default_model,
            "codex": settings.default_codex_model,
        },
        "models_by_provider": {
            "claude": _models("claude"),
            "codex": _models("codex"),
        },
        "default_effort": settings.default_effort,
        "effort_options": {
            "claude": _csv(settings.effort_options),
            "codex": _csv(settings.codex_effort_options),
        },
        "model_efforts": {
            "claude": CLAUDE_MODEL_EFFORTS,
            "codex": CODEX_MODEL_EFFORTS,
        },
        "codex_service_tiers": list(CODEX_SERVICE_TIERS),
        "codex_model_service_tiers": CODEX_MODEL_SERVICE_TIERS,
    }


def task_harness_runtime(task: Task) -> dict[str, str]:
    """Resolve the legacy/default Browser Agent route from its owning Task."""

    provider = task.provider if task.provider in _PROVIDERS else "codex"
    model = task.model or _default_model(provider)
    return {
        "provider": provider,
        "model": model,
        "reasoning_effort": task.effort_level or settings.default_effort,
        "codex_service_tier": task.codex_service_tier or "default",
    }


def validate_harness_runtime(
    *,
    provider: str,
    model: str,
    reasoning_effort: str,
    codex_service_tier: str = "default",
    require_configured_provider: bool = True,
) -> dict[str, str]:
    """Validate one explicit Browser Agent route without silently clamping it."""

    normalized_provider = provider.strip().lower() if isinstance(provider, str) else ""
    normalized_model = model.strip() if isinstance(model, str) else ""
    normalized_effort = (
        reasoning_effort.strip().lower()
        if isinstance(reasoning_effort, str)
        else ""
    )
    normalized_tier = (
        codex_service_tier.strip().lower()
        if isinstance(codex_service_tier, str)
        else ""
    )
    if normalized_provider not in _PROVIDERS:
        raise ValueError("Browser Review provider must be 'claude' or 'codex'")
    if require_configured_provider and normalized_provider not in _provider_options():
        raise ValueError(
            f"Browser Review provider '{normalized_provider}' is not configured"
        )
    if not normalized_model or len(normalized_model) > 100:
        raise ValueError("Browser Review model must contain 1 to 100 characters")
    if normalized_model not in _models(normalized_provider):
        raise ValueError(
            f"Model '{normalized_model}' is not configured for {normalized_provider}"
        )
    supported_efforts = (
        supported_codex_efforts(normalized_model)
        if normalized_provider == "codex"
        else supported_claude_efforts(normalized_model)
    )
    if normalized_effort not in supported_efforts:
        raise ValueError(
            f"Effort '{normalized_effort}' is not supported by model "
            f"'{normalized_model}'"
        )
    normalized_tier = validate_codex_service_tier(
        normalized_provider,
        normalized_model,
        normalized_tier or "default",
    )
    return {
        "provider": normalized_provider,
        "model": normalized_model,
        "reasoning_effort": normalized_effort,
        "codex_service_tier": normalized_tier,
    }


def saved_harness_runtime(task: Task) -> dict[str, Any]:
    """Read the durable per-Task preference, defaulting to Task inheritance."""

    raw = (task.metadata_ or {}).get(HARNESS_RUNTIME_METADATA_KEY)
    if raw is None:
        return {"version": HARNESS_RUNTIME_CONFIG_VERSION, "inherit_task": True}
    if not isinstance(raw, dict) or raw.get("version") != HARNESS_RUNTIME_CONFIG_VERSION:
        raise ValueError("Saved Browser Review runtime configuration is invalid")
    if raw.get("inherit_task") is not False:
        return {"version": HARNESS_RUNTIME_CONFIG_VERSION, "inherit_task": True}
    selection = validate_harness_runtime(
        provider=raw.get("provider", ""),
        model=raw.get("model", ""),
        reasoning_effort=raw.get("reasoning_effort", ""),
        codex_service_tier=raw.get("codex_service_tier", "default"),
    )
    return {
        "version": HARNESS_RUNTIME_CONFIG_VERSION,
        "inherit_task": False,
        **selection,
    }


def build_saved_harness_runtime(
    *,
    inherit_task: bool,
    provider: str | None,
    model: str | None,
    reasoning_effort: str | None,
    codex_service_tier: str | None,
) -> dict[str, Any]:
    if inherit_task:
        return {"version": HARNESS_RUNTIME_CONFIG_VERSION, "inherit_task": True}
    selection = validate_harness_runtime(
        provider=provider or "",
        model=model or "",
        reasoning_effort=reasoning_effort or "",
        codex_service_tier=codex_service_tier or "default",
    )
    return {
        "version": HARNESS_RUNTIME_CONFIG_VERSION,
        "inherit_task": False,
        **selection,
    }


def resolve_harness_runtime(
    task: Task,
    *,
    provider: str | None = None,
    model: str | None = None,
    reasoning_effort: str | None = None,
    codex_service_tier: str | None = None,
) -> dict[str, str]:
    """Freeze the Browser Agent route for one Harness run.

    Precedence is an explicit run override, then the saved per-Task Browser
    Review preference, then the parent Task route. A provider/model override
    resets unspecified dependent fields to safe defaults so values from a
    different provider cannot leak across the boundary.
    """

    task_runtime = task_harness_runtime(task)
    preference = saved_harness_runtime(task)
    if preference["inherit_task"]:
        selected = dict(task_runtime)
        source = "task"
    else:
        selected = {
            key: str(preference[key])
            for key in (
                "provider",
                "model",
                "reasoning_effort",
                "codex_service_tier",
            )
        }
        source = "browser_review_config"

    has_override = any(
        value is not None
        for value in (provider, model, reasoning_effort, codex_service_tier)
    )
    if has_override:
        next_provider = provider or selected["provider"]
        route_changed = next_provider != selected["provider"] or (
            model is not None and model != selected["model"]
        )
        selected = {
            "provider": next_provider,
            "model": model or (
                _default_model(next_provider) if route_changed else selected["model"]
            ),
            "reasoning_effort": reasoning_effort or (
                settings.default_effort
                if route_changed
                else selected["reasoning_effort"]
            ),
            "codex_service_tier": codex_service_tier or (
                "default" if route_changed else selected["codex_service_tier"]
            ),
        }
        source = "run_override"

    validated = validate_harness_runtime(
        **selected,
        require_configured_provider=(source != "task"),
    )
    return {**validated, "selection_source": source}


def harness_runtime_config_payload(task: Task) -> dict[str, Any]:
    """Serialize current preference, resolved route, Task route, and choices."""

    preference = saved_harness_runtime(task)
    resolved = resolve_harness_runtime(task)
    return {
        "inherit_task": bool(preference["inherit_task"]),
        "provider": resolved["provider"],
        "model": resolved["model"],
        "reasoning_effort": resolved["reasoning_effort"],
        "codex_service_tier": resolved["codex_service_tier"],
        "source": resolved["selection_source"],
        "task_runtime": task_harness_runtime(task),
        **harness_runtime_catalog(),
    }
