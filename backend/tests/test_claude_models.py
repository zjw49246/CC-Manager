from unittest.mock import patch

from backend.config import settings
from backend.services.claude_models import (
    DEFAULT_CLAUDE_CONTEXT_WINDOW,
    claude_context_window,
    supported_claude_efforts,
)


def test_opus5_has_fixed_1m_context_window():
    assert claude_context_window("claude-opus-5") == 1_000_000


def test_existing_1m_suffix_remains_supported():
    assert claude_context_window("claude-opus-4-8[1m]") == 1_000_000


def test_fable51_uses_canonical_hyphenated_id_and_1m_alias():
    assert claude_context_window("claude-fable-5-1") == 1_000_000
    assert claude_context_window("claude-fable-5-1[1m]") == 1_000_000


def test_unknown_claude_model_uses_default_context_window():
    assert (
        claude_context_window("claude-future-model")
        == DEFAULT_CLAUDE_CONTEXT_WINDOW
    )


def test_default_model_is_resolved_before_context_lookup():
    with patch.object(settings, "default_model", "claude-opus-5"):
        assert claude_context_window(None) == 1_000_000
        assert claude_context_window("default") == 1_000_000


def test_opus5_supports_full_effort_scale():
    assert supported_claude_efforts("claude-opus-5") == [
        "low",
        "medium",
        "high",
        "xhigh",
        "max",
    ]
