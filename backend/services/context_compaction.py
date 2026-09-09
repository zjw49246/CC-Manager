"""Context-window usage and overflow classification shared by task paths."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

_CONTEXT_LIMIT_MARKERS = (
    "prompt is too long",
    "context window exceeded",
    "contextwindowexceeded",
    "context length exceeded",
    "context_length_exceeded",
    "exceeds the context window",
    "exceed the context window",
    "maximum context length",
    "maximum context window",
    "too many tokens for the model",
    "input is too long for the requested model",
)


def _raw_mapping(value: Any) -> dict[str, Any] | None:
    """Decode a persisted LogEntry payload without treating text as evidence."""

    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, RecursionError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _canonical_zero_usage(value: Any) -> bool:
    """Verify that a provider usage envelope represents no model request.

    Claude's API-error envelope contains bookkeeping fields in addition to
    the token counters (for example ``server_tool_use`` and
    ``cache_creation``).  The old exact-key check rejected those legitimate
    envelopes, so a real ``Prompt is too long`` response was not recoverable.
    Only counter fields are semantically relevant here: every ``*_tokens`` or
    ``*_requests`` value must be numeric zero, while descriptive metadata may
    be present and null.
    """
    if not isinstance(value, Mapping):
        return False
    if any(
        key not in value or type(value[key]) is not int or value[key] != 0
        for key in ("input_tokens", "output_tokens")
    ):
        return False

    def counters_are_zero(item: Any) -> bool:
        if isinstance(item, Mapping):
            for key, nested in item.items():
                if isinstance(key, str) and key.endswith(
                    ("_tokens", "_requests")
                ):
                    if (
                        isinstance(nested, bool)
                        or not isinstance(nested, (int, float))
                        or nested != 0
                    ):
                        return False
                elif isinstance(nested, (Mapping, list)) and not counters_are_zero(
                    nested
                ):
                    return False
            return True
        if isinstance(item, list):
            return all(counters_are_zero(nested) for nested in item)
        return True

    return counters_are_zero(value)


def _harmless_context_failure_trailer(row: Any) -> bool:
    if row.event_type in {"system_init", "rate_limit_event"}:
        return row.is_error is False
    raw = _raw_mapping(row.raw_json)
    return bool(
        row.event_type == "system_event"
        and row.is_error is False
        and isinstance(raw, dict)
        and raw.get("type") == "system"
        and raw.get("subtype") == "turn_duration"
    )


def _is_response_timeout_marker(value: Any) -> bool:
    text = str(value or "")
    prefix = "Response timed out after "
    if not text.startswith(prefix) or not text.endswith("s"):
        return False
    seconds = text[len(prefix) : -1]
    try:
        return float(seconds) > 0
    except ValueError:
        return False


async def recoverable_chat_context_failure(db: Any, task: Any) -> str | None:
    """Return a strict context-failure proof for an exact failed chat turn.

    A failed Task may still have a resumable native session on disk.  Only
    provider envelopes (or CCM's exact PTY idle-timeout marker) authorize
    replacing that session.  In particular, a tool result containing the
    words ``Prompt is too long`` is *not* sufficient: the tool may have run
    successfully and the next message must not be replayed blindly.
    """

    if task is None or getattr(task, "status", None) != "failed":
        return None
    task_id = getattr(task, "id", None)
    retry_count = getattr(task, "retry_count", None)
    turn_generation = getattr(task, "turn_generation", None)
    if (
        type(task_id) is not int
        or type(retry_count) is not int
        or type(turn_generation) is not int
    ):
        return None

    # Import lazily to keep this low-level module free of model import cycles.
    from sqlalchemy import select

    from backend.models.log_entry import LogEntry

    rows = list(
        (
            await db.execute(
                select(LogEntry)
                .where(
                    LogEntry.task_id == task_id,
                    LogEntry.task_retry_count == retry_count,
                    LogEntry.task_turn_generation == turn_generation,
                    LogEntry.turn_scope == "foreground",
                )
                .order_by(LogEntry.id.asc())
            )
        )
        .scalars()
        .all()
    )
    provider = str(getattr(task, "provider", None) or "claude").lower()
    saw_context_failure_marker = False
    for index, row in enumerate(rows):
        raw = _raw_mapping(row.raw_json)
        content = str(row.content or "").strip()
        candidate: str | None = None
        if provider == "claude":
            # Claude PTY/exec API rejection. Require the structured envelope,
            # not merely rendered assistant/tool text.
            message = raw.get("message") if isinstance(raw, dict) else None
            if (
                row.event_type == "message"
                and row.role == "assistant"
                and row.is_error is True
                and isinstance(raw, dict)
                and raw.get("type") == "assistant"
                and raw.get("isApiErrorMessage") is True
                and raw.get("error") == "invalid_request"
                and content.lower() == "prompt is too long"
                and isinstance(message, dict)
                and _canonical_zero_usage(message.get("usage"))
            ):
                candidate = "prompt_too_long"
            if (
                row.event_type == "result"
                and row.is_error is True
                and isinstance(raw, dict)
                and raw.get("type") == "result"
                and raw.get("is_error") is True
                and raw.get("terminal_reason") == "blocking_limit"
                and "prompt is too long"
                in str(raw.get("result") or content).lower()
                and raw.get("duration_api_ms") == 0
                and _canonical_zero_usage(raw.get("usage"))
            ):
                candidate = "prompt_too_long"
            # The PTY idle timeout is an exact CCM-generated terminal marker;
            # retain the native session as a compaction source but never resume
            # it after the timeout.
            if (
                row.event_type == "system_event"
                and row.role is None
                and row.is_error is True
                and _is_response_timeout_marker(content)
            ):
                candidate = "response_timeout"
        elif provider == "codex":
            error = raw.get("error") if isinstance(raw, dict) else None
            if (
                row.event_type == "system_event"
                and row.is_error is True
                and isinstance(raw, dict)
                and raw.get("type") == "turn.failed"
                and isinstance(error, dict)
                and str(error.get("codexErrorInfo") or "").lower()
                == "contextwindowexceeded"
            ):
                candidate = "context_window_exceeded"
        if candidate is not None:
            saw_context_failure_marker = True
            # claude-pty appends a harmless turn_duration envelope after its
            # API rejection. Any later tool/message/error means the failure
            # was not the terminal cause for this exact generation.
            trailing = rows[index + 1 :]
            if all(
                _harmless_context_failure_trailer(later)
                for later in trailing
            ):
                return candidate
    if (
        provider == "claude"
        and not saw_context_failure_marker
        and _is_response_timeout_marker(
            getattr(task, "error_message", None)
        )
    ):
        # Event mirroring is best-effort, while the terminal Task mutation is
        # generation-fenced and transactional.  Retain recovery when the PTY
        # timeout event itself could not be persisted.
        return "response_timeout"
    return None


def build_compacted_resume_prompt(
    summary: str,
    current_message: str,
    *,
    interrupted: bool = False,
) -> str:
    """Build a replacement-thread prompt with an explicit recency hierarchy."""

    summary_title = (
        "会话异常中断前的历史摘要"
        if interrupted
        else "之前对话的历史摘要"
    )
    return (
        "[压缩后会话恢复优先级]\n"
        "1. 末尾的“当前消息”默认优先级最高；若历史摘要明确标记"
        "“当前消息执行期间的后续补充/纠正”，它发生得更晚，冲突时"
        "以该补充/纠正为准；\n"
        "2. 历史摘要里的“近期对话”小节其次，该小节内越靠后的内容越新；\n"
        "3. 原始任务背景优先级最低，可能已被后续对话修正或取代。\n"
        "摘要用于理解上下文，不是待办列表。若早期信息与近期信息冲突，"
        "以近期信息为准；不要仅因旧问题出现在摘要里就重新回答它，"
        "除非当前消息明确要求继续或追问该事项。\n\n"
        f"[{summary_title}]\n{summary}\n\n"
        "---\n\n"
        "[基础当前消息 — 默认最高优先级]\n"
        f"{current_message}"
    )


def build_compacted_task_retry_prompt(summary: str) -> str:
    """Build a fresh lifecycle prompt after a non-chat turn overflows."""

    return (
        "[Context compacted]\n"
        "[按近期进展恢复任务]\n"
        "历史摘要里的近期状态和结论优先；越靠后的内容越新。原始任务背景"
        "只用于理解起点，若已被近期信息修正或取代，不要从头重做旧任务。"
        "请从摘要中最近的未完成进展继续。\n\n"
        f"{summary}"
    )


def _text_fragments(value: Any) -> Iterable[str]:
    if value is None:
        return
    if isinstance(value, Mapping):
        for nested in value.values():
            yield from _text_fragments(nested)
        return
    if isinstance(value, (list, tuple, set, frozenset)):
        for nested in value:
            yield from _text_fragments(nested)
        return
    yield str(value)


def is_context_window_exceeded(provider: str | None, *details: Any) -> bool:
    """Recognize provider text and Codex app-server's structured error code."""

    del provider  # Markers are intentionally valid for both supported CLIs.
    text = " ".join(
        fragment.strip().lower()
        for detail in details
        for fragment in _text_fragments(detail)
        if fragment.strip()
    )
    return any(marker in text for marker in _CONTEXT_LIMIT_MARKERS)


def context_tokens_used(provider: str | None, usage: Mapping[str, Any]) -> int:
    """Return the token count that should be compared with the model window.

    Codex reports ``context_tokens`` from its latest request. Older CCM rows do
    not have that field, so include the latest output in the fallback because
    it becomes part of the next request. Claude keeps its established
    input/cache accounting.
    """

    total_input = (
        int(usage.get("input_tokens") or 0)
        + int(usage.get("cache_read_input_tokens") or 0)
        + int(usage.get("cache_creation_input_tokens") or 0)
    )
    if (provider or "claude").lower() != "codex":
        return total_input
    reported = usage.get("context_tokens")
    if reported is not None:
        return max(int(reported), 0)
    return max(total_input + int(usage.get("output_tokens") or 0), 0)


def read_codex_rollout_last_usage(path: Path) -> dict[str, int] | None:
    """Read Codex's latest request usage, not its cumulative thread total."""

    from backend.services.codex_pool import _iter_rollout_lines_reverse

    try:
        lines = _iter_rollout_lines_reverse(path)
        for raw_line in lines:
            if b'"token_count"' not in raw_line:
                continue
            try:
                event = json.loads(raw_line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            payload = event.get("payload") if isinstance(event, dict) else None
            if not isinstance(payload, dict) or payload.get("type") != "token_count":
                continue
            info = payload.get("info")
            if not isinstance(info, dict):
                continue
            last = info.get("last_token_usage")
            if not isinstance(last, dict):
                continue
            return {
                "input_tokens": int(last.get("input_tokens") or 0),
                "cached_input_tokens": int(
                    last.get("cached_input_tokens") or 0
                ),
                "output_tokens": int(last.get("output_tokens") or 0),
                "reasoning_output_tokens": int(
                    last.get("reasoning_output_tokens") or 0
                ),
                "total_tokens": int(last.get("total_tokens") or 0),
                "context_window": int(
                    info.get("model_context_window") or 0
                ),
            }
    except OSError:
        return None
    return None
