import json
import re
from datetime import datetime


LEGACY_TOOL_MARKUP_ANOMALY = "legacy_tool_markup"

_LEGACY_TOOL_TAG = re.compile(
    r"<\s*(?P<closing>/\s*)?(?:antml:)?"
    r"(?P<tag>invoke|parameter)\b(?P<attrs>[^>]{0,512})>",
    re.IGNORECASE,
)
_LEGACY_TOOL_NAME_ATTR = re.compile(
    r"\bname\s*=\s*(?:\"[^\"]+\"|'[^']+')",
    re.IGNORECASE,
)
_MAX_LEGACY_TOOL_TAG_DEPTH = 1024


def _contains_complete_legacy_tool_call(content: str) -> bool:
    """Scan legacy tool tags once without backtracking across message text."""

    # Each entry is [tag, has_name, has_closed_named_parameter]. Lists keep
    # the final flag mutable when a matching parameter closes.
    stack: list[list[object]] = []
    for match in _LEGACY_TOOL_TAG.finditer(content):
        tag = match.group("tag").lower()
        if not match.group("closing"):
            # Parameter text is opaque. A shell command or file body may
            # contain literal target-looking tags; the first real closing
            # parameter tag still determines the legacy call boundary.
            if stack and stack[-1][0] == "parameter":
                continue
            if len(stack) >= _MAX_LEGACY_TOOL_TAG_DEPTH:
                stack.clear()
            stack.append([
                tag,
                bool(_LEGACY_TOOL_NAME_ATTR.search(match.group("attrs"))),
                False,
            ])
            continue

        if not stack or stack[-1][0] != tag:
            if stack and stack[-1][0] == "parameter":
                # See the opaque-parameter rule above: an invoke-looking
                # closing string inside a command is not structural.
                continue
            # A malformed target-tag nesting cannot prove a complete call.
            # Reset so a later independent, well-formed call can still match.
            stack.clear()
            continue

        closed = stack.pop()
        if tag == "parameter":
            if closed[1]:
                for parent in reversed(stack):
                    if parent[0] == "invoke":
                        if parent[1]:
                            parent[2] = True
                        break
            continue
        if closed[1] and closed[2]:
            return True
    return False


def detect_assistant_protocol_anomaly(
    event_type: object,
    role: object,
    content: object,
    *,
    provider: object = "claude",
) -> str | None:
    """Classify legacy tool markup emitted as assistant text.

    Claude tools are valid only when the provider emits a structured
    ``tool_use`` content block. Legacy XML-like wrappers inside a text block
    are evidence of a provider/CLI protocol mismatch; they must remain inert
    text and must never be parsed into an executable tool call by CCM.
    """

    if str(provider or "").strip().lower() != "claude":
        return None
    if event_type not in ("message", "result"):
        return None
    if (role is not None and role != "assistant") or not isinstance(content, str):
        return None
    if _contains_complete_legacy_tool_call(content):
        return LEGACY_TOOL_MARKUP_ANOMALY
    return None


class StreamParser:
    """Parse Claude Code stream-json (NDJSON) output into structured events."""

    def parse_line(self, line: str) -> list[dict]:
        """Parse a single NDJSON line into one or more events.

        Returns a list because a single assistant/user event may contain
        multiple content blocks (e.g. text + tool_use), each yielding a
        separate event.
        """
        if not line.strip():
            return []
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            return [{
                "event_type": "parse_error",
                "content": line,
                "is_error": True,
                "timestamp": datetime.utcnow().isoformat(),
            }]

        event_type = data.get("type", "unknown")
        now = datetime.utcnow().isoformat()

        def _base_event() -> dict:
            return {
                "event_type": event_type,
                "role": data.get("role"),
                "content": self._extract_content(data),
                "tool_name": None,
                "tool_input": None,
                "tool_output": None,
                "raw_json": line,
                "is_error": False,
                "timestamp": now,
            }

        # Extract session_id from system/init or result events
        if event_type == "system" and data.get("subtype") == "init":
            event = _base_event()
            event["session_id"] = data.get("session_id")
            event["event_type"] = "system_init"
            return [event]
        elif event_type == "system":
            subtype = data.get("subtype", "system")
            # Skip noisy telemetry subtypes that flood the chat UI
            _SKIP_SUBTYPES = {"thinking_tokens", "token_usage", "api_request", "api_response"}
            if subtype in _SKIP_SUBTYPES:
                return []
            event = _base_event()
            event["event_type"] = "system_event"
            event["content"] = subtype
            return [event]
        elif event_type == "assistant":
            # Extract token usage from assistant message for context window tracking
            usage_data = None
            message_obj = data.get("message", {}) if isinstance(data.get("message"), dict) else {}
            usage = message_obj.get("usage")
            if isinstance(usage, dict):
                input_tokens = usage.get("input_tokens", 0)
                cache_read = usage.get("cache_read_input_tokens", 0)
                cache_creation = usage.get("cache_creation_input_tokens", 0)
                output_tokens = usage.get("output_tokens", 0)
                usage_data = {
                    "input_tokens": input_tokens,
                    "cache_read_input_tokens": cache_read,
                    "cache_creation_input_tokens": cache_creation,
                    "output_tokens": output_tokens,
                    "total_input_tokens": input_tokens + cache_read + cache_creation,
                }

            # Parse ALL content blocks — one event per block
            content_blocks = message_obj.get("content", []) if message_obj else data.get("content", [])
            if not isinstance(content_blocks, list):
                event = _base_event()
                event["role"] = "assistant"
                event["event_type"] = "message"
                if "stop_reason" in message_obj:
                    event["stop_reason"] = message_obj["stop_reason"]
                anomaly = detect_assistant_protocol_anomaly(
                    event["event_type"],
                    event["role"],
                    event.get("content"),
                )
                if anomaly:
                    event["protocol_anomaly"] = anomaly
                if data.get("isApiErrorMessage"):
                    event["is_error"] = True
                if usage_data:
                    event["context_usage"] = usage_data
                return [event]
            events = []
            envelope_has_tool_use = any(
                isinstance(block, dict) and block.get("type") == "tool_use"
                for block in content_blocks
            )
            for block in content_blocks:
                if not isinstance(block, dict):
                    continue
                evt = _base_event()
                evt["role"] = "assistant"
                evt["assistant_envelope_has_tool_use"] = (
                    envelope_has_tool_use
                )
                if block.get("type") == "tool_use":
                    evt["event_type"] = "tool_use"
                    evt["tool_name"] = block.get("name")
                    evt["tool_input"] = json.dumps(block.get("input", {}))
                    evt["content"] = None
                    events.append(evt)
                elif block.get("type") == "thinking":
                    evt["event_type"] = "thinking"
                    evt["content"] = self._extract_thinking_text(block)
                    events.append(evt)
                elif block.get("type") == "text":
                    evt["event_type"] = "message"
                    evt["content"] = block.get("text", "")
                    anomaly = detect_assistant_protocol_anomaly(
                        evt["event_type"],
                        evt["role"],
                        evt["content"],
                    )
                    if anomaly:
                        evt["protocol_anomaly"] = anomaly
                    events.append(evt)
            if not events:
                event = _base_event()
                event["role"] = "assistant"
                event["event_type"] = "message"
                if "stop_reason" in message_obj:
                    event["stop_reason"] = message_obj["stop_reason"]
                if data.get("isApiErrorMessage"):
                    event["is_error"] = True
                if usage_data:
                    event["context_usage"] = usage_data
                return [event]
            if data.get("isApiErrorMessage"):
                for event in events:
                    event["is_error"] = True
            if "stop_reason" in message_obj:
                stop_reason = message_obj["stop_reason"]
                for event in events:
                    event["stop_reason"] = stop_reason
            # Attach usage data to the first event only
            if usage_data and events:
                events[0]["context_usage"] = usage_data
            return events
        elif event_type == "user":
            # Claude Code sends tool results as type: "user" with tool_result content blocks
            msg_content = data.get("message", {}).get("content", []) if isinstance(data.get("message"), dict) else []
            if isinstance(msg_content, list):
                events = []
                for block in msg_content:
                    if isinstance(block, dict) and block.get("type") == "tool_result":
                        evt = _base_event()
                        evt["event_type"] = "tool_result"
                        evt["role"] = "tool"
                        # content can be a string or a list of content blocks
                        raw_content = block.get("content", "")
                        if isinstance(raw_content, list):
                            texts = [b.get("text", "") for b in raw_content if isinstance(b, dict) and b.get("type") == "text"]
                            evt["tool_output"] = "\n".join(texts) if texts else str(raw_content)
                        else:
                            evt["tool_output"] = raw_content
                        if block.get("is_error"):
                            evt["is_error"] = True
                        events.append(evt)
                if events:
                    return events
            event = _base_event()
            event["event_type"] = "tool_result"
            event["role"] = "tool"
            return [event]
        elif event_type == "tool_use":
            event = _base_event()
            event["tool_name"] = data.get("name")
            event["tool_input"] = json.dumps(data.get("input", {}))
            return [event]
        elif event_type == "tool_result":
            event = _base_event()
            event["tool_output"] = self._extract_content(data) or ""
            if isinstance(event["tool_output"], str) and "error" in event["tool_output"].lower():
                event["is_error"] = True
            return [event]
        elif event_type == "rate_limit_event":
            # Routine quota-status ping. Surface rate_limit_info so the pool can
            # tell a benign "allowed" ping from a genuine near-limit warning and
            # avoid benching healthy accounts (see rate_limit_event_is_actionable).
            event = _base_event()
            info = data.get("rate_limit_info")
            event["rate_limit_info"] = info if isinstance(info, dict) else None
            return [event]
        elif event_type == "result":
            event = _base_event()
            event["content"] = self._extract_content(data)
            anomaly = detect_assistant_protocol_anomaly(
                event["event_type"],
                event["role"],
                event["content"],
            )
            if anomaly:
                event["protocol_anomaly"] = anomaly
            event["session_id"] = data.get("session_id")
            cost = data.get("total_cost_usd")
            if cost is not None:
                event["cost_usd"] = cost
            # NOTE: result.usage is CUMULATIVE across all API requests of the
            # run (observed 45x the real context size on long tool-use turns)
            # — never use it for the context indicator. The per-request
            # assistant events already carry accurate usage; only forward the
            # authoritative context_window here.
            model_usage = data.get("modelUsage")
            if isinstance(model_usage, dict):
                for _model_name, model_data in model_usage.items():
                    if isinstance(model_data, dict) and "contextWindow" in model_data:
                        event["context_usage"] = {
                            "context_window": model_data["contextWindow"],
                        }
                        break
            if data.get("is_error"):
                event["is_error"] = True
            return [event]
        else:
            return [_base_event()]

    def _extract_thinking_text(self, block: dict) -> str:
        """Extract thinking text from a thinking content block.

        Newer Claude Code / API versions vary the field name and may return
        encrypted thinking (no plaintext at all). Try the known field names in
        order, and if only an encrypted payload is present, return a marker so
        the UI can communicate the situation rather than rendering an empty box.
        """
        for key in ("thinking", "text", "content", "summary"):
            value = block.get(key)
            if isinstance(value, str) and value:
                return value
            if isinstance(value, list):
                texts = [
                    b.get("text", "")
                    for b in value
                    if isinstance(b, dict) and b.get("type") == "text"
                ]
                joined = "\n".join(t for t in texts if t)
                if joined:
                    return joined
        # Encrypted thinking blocks carry only `signature` + `data` (no plaintext).
        if block.get("signature") or block.get("data"):
            return "[encrypted thinking — no plaintext returned by the API]"
        return ""

    def _extract_content(self, data: dict) -> str | None:
        # Handle content blocks (list of {type, text})
        content = data.get("content")
        if isinstance(content, list):
            texts = [b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text"]
            return "\n".join(texts) if texts else None
        if isinstance(content, str):
            return content
        # Handle message wrapper
        message = data.get("message")
        if isinstance(message, dict):
            return self._extract_content(message)
        # Claude Code's terminal stream-json envelope uses a top-level result
        # string for successful and failed turns. Some gateway failures expose
        # a structured top-level error instead.
        result = data.get("result")
        if isinstance(result, str):
            return result
        error = data.get("error")
        if isinstance(error, str):
            return error
        if isinstance(error, dict):
            for key in ("message", "detail", "error"):
                value = error.get(key)
                if isinstance(value, str):
                    return value
        return None
