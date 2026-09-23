"""Claude model capabilities shared by runtime context accounting and the UI."""

from backend.config import settings


CLAUDE_MODEL_EFFORTS: dict[str, list[str]] = {
    "claude-opus-5": ["low", "medium", "high", "xhigh", "max"],
}

DEFAULT_CLAUDE_CONTEXT_WINDOW = 200_000


def _configured_model(model: str | None) -> str:
    value = (model or "").strip().lower()
    if not value or value == "default":
        value = settings.default_model.strip().lower()
    return value


def claude_context_window(model: str | None) -> int:
    """Return the effective context window for a Claude CLI model choice."""
    value = _configured_model(model)
    if value.endswith("[1m]"):
        return 1_000_000
    return DEFAULT_CLAUDE_CONTEXT_WINDOW


def supported_claude_efforts(model: str | None) -> list[str]:
    """Return explicit model efforts, or the configured Claude defaults."""
    value = _configured_model(model)
    if value.endswith("[1m]"):
        value = value[:-4]
    configured = [
        effort.strip()
        for effort in settings.effort_options.split(",")
        if effort.strip()
    ]
    return CLAUDE_MODEL_EFFORTS.get(value, configured)
