"""Provider-neutral terminal protocol for yielding a Task to a Capability.

The protocol deliberately lives outside the dispatcher and persistence layers.
It turns an otherwise unstructured final assistant response into one of two
outcomes:

* no terminal marker: an ordinary assistant completion; or
* one valid marker at the very end: a typed Capability request.

Anything that resembles a marker but is malformed raises instead of being
silently treated as a request or an ordinary completion.  This keeps the later
controller integration fail-closed at the trust boundary.
"""

from __future__ import annotations

from collections.abc import Collection, Iterable
from dataclasses import dataclass, field
import json
import math
import re
from typing import Literal, TypeAlias


TERMINAL_ACTION_OPEN_TAG = "<ccm_terminal_action>"
TERMINAL_ACTION_CLOSE_TAG = "</ccm_terminal_action>"
TERMINAL_ACTION_SCHEMA_VERSION = 1

MAX_TERMINAL_PAYLOAD_BYTES = 32 * 1024
MAX_JSON_DEPTH = 12
MAX_JSON_NODES = 512
MAX_CONTAINER_ITEMS = 256
MAX_JSON_KEY_CHARS = 128
MAX_JSON_STRING_CHARS = 16 * 1024
MAX_TOTAL_JSON_STRING_CHARS = 24 * 1024
MAX_REASON_CHARS = 2 * 1024
MAX_NUMBER_TOKEN_CHARS = 64
MAX_SAFE_JSON_INTEGER = 9_007_199_254_740_991

_CAPABILITY_KEY_RE = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")
_TAG_LIKE_RE = re.compile(r"<\s*/?\s*ccm_terminal_action\b[^>]*>", re.IGNORECASE)
_TAG_HINT_RE = re.compile(r"<\s*/?\s*ccm_terminal_action\b", re.IGNORECASE)
_TOP_LEVEL_FIELDS = frozenset(
    {"schema_version", "terminal_action", "capability", "reason", "request"}
)

JsonScalar: TypeAlias = None | bool | int | float | str
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]


class CapabilityProtocolError(ValueError):
    """A marker was present but did not satisfy the terminal protocol."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class CapabilityRequestAction:
    """Validated request represented by a terminal action marker."""

    capability: str
    reason: str
    request: dict[str, JsonValue]
    schema_version: Literal[1] = field(default=1, init=False)
    terminal_action: Literal["request_capability"] = field(
        default="request_capability",
        init=False,
    )


@dataclass(slots=True)
class _JsonBudget:
    nodes: int = 0
    string_chars: int = 0


def _error(code: str, message: str) -> CapabilityProtocolError:
    return CapabilityProtocolError(code, message)


def _validate_capability_key(value: object) -> str:
    if not isinstance(value, str) or not _CAPABILITY_KEY_RE.fullmatch(value):
        raise _error(
            "invalid_capability",
            "capability must be a lowercase capability key of at most 64 characters",
        )
    return value


def _normalize_allowed_capabilities(
    allowed_capabilities: Collection[str] | None,
) -> frozenset[str] | None:
    if allowed_capabilities is None:
        return None
    if isinstance(allowed_capabilities, str):
        raise TypeError("allowed_capabilities must be a collection of keys, not a string")

    normalized: set[str] = set()
    for capability in allowed_capabilities:
        try:
            normalized.add(_validate_capability_key(capability))
        except CapabilityProtocolError as exc:
            raise ValueError(f"invalid allowed capability: {capability!r}") from exc
    return frozenset(normalized)


def _reject_excessive_lexical_depth(payload: str) -> None:
    """Bound nesting before ``json.loads`` gets a chance to recurse."""

    depth = 0
    in_string = False
    escaped = False
    for character in payload:
        if in_string:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
            continue

        if character == '"':
            in_string = True
        elif character in "[{":
            depth += 1
            if depth > MAX_JSON_DEPTH:
                raise _error(
                    "payload_too_deep",
                    f"terminal payload exceeds maximum JSON depth {MAX_JSON_DEPTH}",
                )
        elif character in "]}":
            depth -= 1
            # Invalid syntax is reported consistently by the JSON decoder; the
            # lower bound prevents a crafted prefix from hiding later depth.
            if depth < 0:
                raise _error("invalid_json", "terminal payload is not valid JSON")


def _parse_integer(token: str) -> int:
    if len(token) > MAX_NUMBER_TOKEN_CHARS:
        raise _error("number_too_large", "JSON number token is too large")
    value = int(token)
    if abs(value) > MAX_SAFE_JSON_INTEGER:
        raise _error(
            "number_too_large",
            "JSON integers must fit the interoperable safe-integer range",
        )
    return value


def _parse_float(token: str) -> float:
    if len(token) > MAX_NUMBER_TOKEN_CHARS:
        raise _error("number_too_large", "JSON number token is too large")
    value = float(token)
    if not math.isfinite(value):
        raise _error("number_too_large", "JSON numbers must be finite")
    return value


def _reject_non_json_number(token: str) -> None:
    raise _error("invalid_number", f"{token} is not a valid JSON number")


def _object_without_duplicate_keys(
    pairs: list[tuple[str, JsonValue]],
) -> dict[str, JsonValue]:
    result: dict[str, JsonValue] = {}
    for key, value in pairs:
        if key in result:
            raise _error("duplicate_field", f"duplicate JSON field: {key!r}")
        result[key] = value
    return result


def _load_payload(payload: str) -> object:
    try:
        return json.loads(
            payload,
            parse_int=_parse_integer,
            parse_float=_parse_float,
            parse_constant=_reject_non_json_number,
            object_pairs_hook=_object_without_duplicate_keys,
        )
    except CapabilityProtocolError:
        raise
    except (json.JSONDecodeError, RecursionError, UnicodeError, ValueError) as exc:
        raise _error("invalid_json", "terminal payload is not valid JSON") from exc


def _validate_json_value(value: object, *, depth: int, budget: _JsonBudget) -> None:
    if depth > MAX_JSON_DEPTH:
        raise _error(
            "payload_too_deep",
            f"terminal payload exceeds maximum JSON depth {MAX_JSON_DEPTH}",
        )

    budget.nodes += 1
    if budget.nodes > MAX_JSON_NODES:
        raise _error("payload_too_complex", "terminal payload has too many JSON values")

    if value is None or isinstance(value, bool):
        return
    if isinstance(value, int):
        if abs(value) > MAX_SAFE_JSON_INTEGER:
            raise _error("number_too_large", "JSON integer is too large")
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise _error("invalid_number", "JSON numbers must be finite")
        return
    if isinstance(value, str):
        if len(value) > MAX_JSON_STRING_CHARS:
            raise _error("string_too_large", "JSON string is too large")
        try:
            value.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise _error("invalid_unicode", "JSON strings must contain valid Unicode") from exc
        budget.string_chars += len(value)
        if budget.string_chars > MAX_TOTAL_JSON_STRING_CHARS:
            raise _error(
                "payload_too_large",
                "terminal payload contains too much string data",
            )
        return
    if isinstance(value, list):
        if len(value) > MAX_CONTAINER_ITEMS:
            raise _error("payload_too_complex", "JSON array has too many items")
        for item in value:
            _validate_json_value(item, depth=depth + 1, budget=budget)
        return
    if isinstance(value, dict):
        if len(value) > MAX_CONTAINER_ITEMS:
            raise _error("payload_too_complex", "JSON object has too many fields")
        for key, item in value.items():
            if len(key) > MAX_JSON_KEY_CHARS:
                raise _error("key_too_large", "JSON object key is too large")
            _validate_json_value(key, depth=depth + 1, budget=budget)
            _validate_json_value(item, depth=depth + 1, budget=budget)
        return
    raise _error("invalid_json_type", "terminal payload contains a non-JSON value")


def parse_capability_terminal_action(
    output: str,
    *,
    allowed_capabilities: Collection[str] | None = None,
) -> CapabilityRequestAction | None:
    """Parse a final assistant output into an optional Capability request.

    ``None`` means the output contains no terminal-action marker and is an
    ordinary completion.  If a marker (or marker-like tag) is present, every
    protocol violation raises :class:`CapabilityProtocolError`.
    """

    if not isinstance(output, str):
        raise TypeError("output must be a string")
    allowed = _normalize_allowed_capabilities(allowed_capabilities)

    open_count = output.count(TERMINAL_ACTION_OPEN_TAG)
    close_count = output.count(TERMINAL_ACTION_CLOSE_TAG)
    tag_like = _TAG_LIKE_RE.findall(output)
    tag_hints = _TAG_HINT_RE.findall(output)

    if open_count == 0 and close_count == 0 and not tag_hints:
        return None
    if (
        open_count != 1
        or close_count != 1
        or len(tag_hints) != 2
        or len(tag_like) != 2
        or tag_like[0] != TERMINAL_ACTION_OPEN_TAG
        or tag_like[1] != TERMINAL_ACTION_CLOSE_TAG
    ):
        raise _error(
            "invalid_marker",
            "output must contain exactly one canonical terminal-action marker",
        )

    marker_start = output.find(TERMINAL_ACTION_OPEN_TAG)
    payload_start = marker_start + len(TERMINAL_ACTION_OPEN_TAG)
    marker_end = output.find(TERMINAL_ACTION_CLOSE_TAG, payload_start)
    if marker_end < payload_start:
        raise _error("invalid_marker", "terminal-action tags are out of order")

    trailing = output[marker_end + len(TERMINAL_ACTION_CLOSE_TAG) :]
    if trailing.strip():
        raise _error(
            "marker_not_terminal",
            "terminal-action marker must be the final non-whitespace output",
        )

    payload = output[payload_start:marker_end]
    try:
        payload_bytes = len(payload.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise _error("invalid_unicode", "terminal payload must be valid Unicode") from exc
    if payload_bytes == 0:
        raise _error("invalid_json", "terminal payload cannot be empty")
    if payload_bytes > MAX_TERMINAL_PAYLOAD_BYTES:
        raise _error(
            "payload_too_large",
            f"terminal payload exceeds {MAX_TERMINAL_PAYLOAD_BYTES} bytes",
        )

    _reject_excessive_lexical_depth(payload)
    decoded = _load_payload(payload)
    _validate_json_value(decoded, depth=1, budget=_JsonBudget())

    if not isinstance(decoded, dict):
        raise _error("invalid_envelope", "terminal payload must be a JSON object")
    actual_fields = frozenset(decoded)
    if actual_fields != _TOP_LEVEL_FIELDS:
        unknown = sorted(actual_fields - _TOP_LEVEL_FIELDS)
        missing = sorted(_TOP_LEVEL_FIELDS - actual_fields)
        details = []
        if unknown:
            details.append(f"unknown fields: {', '.join(unknown)}")
        if missing:
            details.append(f"missing fields: {', '.join(missing)}")
        raise _error("invalid_envelope", "; ".join(details))
    if (
        type(decoded["schema_version"]) is not int
        or decoded["schema_version"] != TERMINAL_ACTION_SCHEMA_VERSION
    ):
        raise _error("unsupported_version", "schema_version must be the integer 1")
    if decoded["terminal_action"] != "request_capability":
        raise _error(
            "invalid_action",
            "terminal_action must be 'request_capability'",
        )

    capability = _validate_capability_key(decoded["capability"])
    if allowed is not None and capability not in allowed:
        raise _error(
            "capability_not_allowed",
            f"capability {capability!r} is not allowed for this turn",
        )

    reason = decoded["reason"]
    if not isinstance(reason, str) or not reason.strip():
        raise _error("invalid_reason", "reason must be a non-empty string")
    reason = reason.strip()
    if len(reason) > MAX_REASON_CHARS:
        raise _error(
            "reason_too_large",
            f"reason exceeds {MAX_REASON_CHARS} characters",
        )

    request = decoded["request"]
    if not isinstance(request, dict):
        raise _error("invalid_request", "request must be a JSON object")

    return CapabilityRequestAction(
        capability=capability,
        reason=reason,
        request=request,
    )


def build_capability_protocol_instructions(
    allowed_capabilities: Iterable[str],
) -> str:
    """Build provider-neutral prompt instructions for the terminal protocol."""

    if isinstance(allowed_capabilities, str):
        raise TypeError("allowed_capabilities must be an iterable of keys, not a string")
    allowed_tuple = tuple(allowed_capabilities)
    allowed = _normalize_allowed_capabilities(allowed_tuple)
    assert allowed is not None

    if not allowed:
        return (
            "Capability handoff protocol (schema version 1): no capabilities are "
            "available for this turn. Complete the task normally and do not output "
            "or mention <ccm_terminal_action> or </ccm_terminal_action>."
        )

    allowed_json = json.dumps(sorted(allowed), ensure_ascii=True)
    example = json.dumps(
        {
            "schema_version": 1,
            "terminal_action": "request_capability",
            "capability": sorted(allowed)[0],
            "reason": "Explain why control must be yielded.",
            "request": {},
        },
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return "\n".join(
        (
            "Capability handoff protocol (schema version 1):",
            f"- Capabilities allowed for this turn: {allowed_json}.",
            "- Continue working normally unless you must yield control to one allowed capability.",
            "- To yield, end your final response with exactly one canonical "
            "marker containing JSON:",
            f"  {TERMINAL_ACTION_OPEN_TAG}{example}{TERMINAL_ACTION_CLOSE_TAG}",
            "- The JSON object must contain exactly schema_version, "
            "terminal_action, capability, reason, and request; request must be "
            "an object.",
            "- The closing tag must be the final non-whitespace content in the response.",
            "- For an ordinary completion, do not output or mention either terminal-action tag.",
        )
    )
