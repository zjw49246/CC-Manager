"""Tests for the provider-neutral Capability terminal protocol."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
import json

import pytest

from backend.services.capability_protocol import (
    MAX_CONTAINER_ITEMS,
    MAX_JSON_DEPTH,
    MAX_JSON_KEY_CHARS,
    MAX_JSON_NODES,
    MAX_JSON_STRING_CHARS,
    MAX_REASON_CHARS,
    MAX_SAFE_JSON_INTEGER,
    MAX_TERMINAL_PAYLOAD_BYTES,
    TERMINAL_ACTION_CLOSE_TAG,
    TERMINAL_ACTION_OPEN_TAG,
    CapabilityProtocolError,
    CapabilityRequestAction,
    build_capability_protocol_instructions,
    parse_capability_terminal_action,
)


def _valid_envelope(**overrides: object) -> dict[str, object]:
    envelope: dict[str, object] = {
        "schema_version": 1,
        "terminal_action": "request_capability",
        "capability": "plan",
        "reason": "A separate planning pass is required.",
        "request": {},
    }
    envelope.update(overrides)
    return envelope


def _marked(payload: object, *, prefix: str = "", suffix: str = "") -> str:
    encoded = (
        payload
        if isinstance(payload, str)
        else json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
    )
    return (
        f"{prefix}{TERMINAL_ACTION_OPEN_TAG}{encoded}"
        f"{TERMINAL_ACTION_CLOSE_TAG}{suffix}"
    )


@pytest.mark.parametrize(
    "provider_output",
    [
        "Claude completed the work without a handoff.",
        "Codex completed the work without a handoff.\n",
        "A plain mention of ccm_terminal_action is not a marker.",
        "",
    ],
)
def test_plain_provider_outputs_are_ordinary_completions(provider_output: str):
    assert parse_capability_terminal_action(provider_output) is None


@pytest.mark.parametrize(
    "prefix",
    [
        "",
        "I inspected the repository and need a planning pass.\n\n",
        "Claude final response: ",
        "Codex final response: ",
    ],
)
def test_accepts_one_terminal_marker_after_arbitrary_provider_text(prefix: str):
    output = _marked(
        _valid_envelope(
            reason="  Establish the implementation boundary.  ",
            request={"scope": ["backend", "frontend"], "retry": 0},
        ),
        prefix=prefix,
        suffix=" \n\t",
    )

    action = parse_capability_terminal_action(
        output,
        allowed_capabilities={"plan", "code_review"},
    )

    assert action == CapabilityRequestAction(
        capability="plan",
        reason="Establish the implementation boundary.",
        request={"scope": ["backend", "frontend"], "retry": 0},
    )
    assert action.schema_version == 1
    assert action.terminal_action == "request_capability"


def test_result_dataclass_does_not_allow_identity_fields_to_be_reassigned():
    action = parse_capability_terminal_action(_marked(_valid_envelope()))
    assert action is not None

    with pytest.raises(FrozenInstanceError):
        action.capability = "code_review"  # type: ignore[misc]


def test_allowlist_is_enforced_at_parse_boundary():
    with pytest.raises(CapabilityProtocolError, match="not allowed") as caught:
        parse_capability_terminal_action(
            _marked(_valid_envelope()),
            allowed_capabilities={"code_review"},
        )

    assert caught.value.code == "capability_not_allowed"


@pytest.mark.parametrize(
    ("output", "expected_code"),
    [
        (
            _marked(_valid_envelope()) + _marked(_valid_envelope()),
            "invalid_marker",
        ),
        (TERMINAL_ACTION_OPEN_TAG + "{}", "invalid_marker"),
        ("{}" + TERMINAL_ACTION_CLOSE_TAG, "invalid_marker"),
        ("<ccm_terminal_action", "invalid_marker"),
        ("</ccm_terminal_action", "invalid_marker"),
        (
            TERMINAL_ACTION_CLOSE_TAG
            + _marked(_valid_envelope()).removesuffix(TERMINAL_ACTION_CLOSE_TAG),
            "invalid_marker",
        ),
        (
            _marked(_valid_envelope()).replace(
                TERMINAL_ACTION_OPEN_TAG,
                "<CCM_TERMINAL_ACTION>",
            ),
            "invalid_marker",
        ),
        (
            _marked(_valid_envelope()).replace(
                TERMINAL_ACTION_OPEN_TAG,
                "<ccm_terminal_action >",
            ),
            "invalid_marker",
        ),
    ],
)
def test_marker_must_be_unique_canonical_and_ordered(output: str, expected_code: str):
    with pytest.raises(CapabilityProtocolError) as caught:
        parse_capability_terminal_action(output)

    assert caught.value.code == expected_code


def test_marker_text_inside_payload_is_rejected_as_a_second_marker():
    output = _marked(
        _valid_envelope(request={"untrusted": TERMINAL_ACTION_OPEN_TAG})
    )

    with pytest.raises(CapabilityProtocolError) as caught:
        parse_capability_terminal_action(output)

    assert caught.value.code == "invalid_marker"


@pytest.mark.parametrize("suffix", ["done", "\nnext step", "\x00"])
def test_marker_must_be_final_non_whitespace_content(suffix: str):
    with pytest.raises(CapabilityProtocolError) as caught:
        parse_capability_terminal_action(
            _marked(_valid_envelope(), suffix=suffix)
        )

    assert caught.value.code == "marker_not_terminal"


@pytest.mark.parametrize(
    "payload",
    [
        "",
        "not JSON",
        "{",
        "[1,2,3]",
        "null",
        '"request_capability"',
    ],
)
def test_payload_must_be_a_valid_json_object(payload: str):
    with pytest.raises(CapabilityProtocolError):
        parse_capability_terminal_action(_marked(payload))


@pytest.mark.parametrize(
    "envelope",
    [
        {key: value for key, value in _valid_envelope().items() if key != "reason"},
        {**_valid_envelope(), "extra": True},
    ],
)
def test_envelope_requires_exact_top_level_fields(envelope: dict[str, object]):
    with pytest.raises(CapabilityProtocolError) as caught:
        parse_capability_terminal_action(_marked(envelope))

    assert caught.value.code == "invalid_envelope"


@pytest.mark.parametrize("version", [0, 2, "1", True, 1.0, None])
def test_schema_version_is_exact_integer_one(version: object):
    with pytest.raises(CapabilityProtocolError) as caught:
        parse_capability_terminal_action(
            _marked(_valid_envelope(schema_version=version))
        )

    assert caught.value.code == "unsupported_version"


@pytest.mark.parametrize(
    "action",
    ["complete", "REQUEST_CAPABILITY", "request-capability", None, 1],
)
def test_terminal_action_is_exact(action: object):
    with pytest.raises(CapabilityProtocolError) as caught:
        parse_capability_terminal_action(
            _marked(_valid_envelope(terminal_action=action))
        )

    assert caught.value.code == "invalid_action"


@pytest.mark.parametrize(
    "capability",
    [
        "",
        "Plan",
        " plan",
        "plan step",
        "_plan",
        "plan/execute",
        "a" * 65,
        1,
        None,
    ],
)
def test_capability_key_is_strict(capability: object):
    with pytest.raises(CapabilityProtocolError) as caught:
        parse_capability_terminal_action(
            _marked(_valid_envelope(capability=capability))
        )

    assert caught.value.code == "invalid_capability"


@pytest.mark.parametrize("reason", ["", "  \n\t", None, 1, [], {}])
def test_reason_must_be_a_nonempty_string(reason: object):
    with pytest.raises(CapabilityProtocolError) as caught:
        parse_capability_terminal_action(_marked(_valid_envelope(reason=reason)))

    assert caught.value.code == "invalid_reason"


def test_reason_has_a_specific_size_bound():
    with pytest.raises(CapabilityProtocolError) as caught:
        parse_capability_terminal_action(
            _marked(_valid_envelope(reason="r" * (MAX_REASON_CHARS + 1)))
        )

    assert caught.value.code == "reason_too_large"


@pytest.mark.parametrize("request_value", [None, [], "plan", 1, True])
def test_request_must_be_an_object(request_value: object):
    with pytest.raises(CapabilityProtocolError) as caught:
        parse_capability_terminal_action(
            _marked(_valid_envelope(request=request_value))
        )

    assert caught.value.code == "invalid_request"


def test_duplicate_fields_are_rejected_at_every_object_level():
    top_level_duplicate = (
        '{"schema_version":1,"schema_version":1,'
        '"terminal_action":"request_capability","capability":"plan",'
        '"reason":"why","request":{}}'
    )
    nested_duplicate = (
        '{"schema_version":1,"terminal_action":"request_capability",'
        '"capability":"plan","reason":"why",'
        '"request":{"scope":1,"scope":2}}'
    )

    for payload in (top_level_duplicate, nested_duplicate):
        with pytest.raises(CapabilityProtocolError) as caught:
            parse_capability_terminal_action(_marked(payload))
        assert caught.value.code == "duplicate_field"


def test_payload_byte_limit_is_checked_before_decoding():
    oversized = "x" * MAX_TERMINAL_PAYLOAD_BYTES

    with pytest.raises(CapabilityProtocolError) as caught:
        parse_capability_terminal_action(
            _marked(_valid_envelope(request={"data": oversized}))
        )

    assert caught.value.code == "payload_too_large"


def test_single_json_string_size_is_bounded():
    with pytest.raises(CapabilityProtocolError) as caught:
        parse_capability_terminal_action(
            _marked(
                _valid_envelope(
                    request={"data": "x" * (MAX_JSON_STRING_CHARS + 1)}
                )
            )
        )

    assert caught.value.code == "string_too_large"


def test_json_object_keys_are_bounded():
    with pytest.raises(CapabilityProtocolError) as caught:
        parse_capability_terminal_action(
            _marked(
                _valid_envelope(request={"k" * (MAX_JSON_KEY_CHARS + 1): True})
            )
        )

    assert caught.value.code == "key_too_large"


def test_json_container_size_is_bounded():
    request = {f"k{index}": index for index in range(MAX_CONTAINER_ITEMS + 1)}

    with pytest.raises(CapabilityProtocolError) as caught:
        parse_capability_terminal_action(_marked(_valid_envelope(request=request)))

    assert caught.value.code == "payload_too_complex"


def test_total_json_node_count_is_bounded():
    # A max-sized object is lexically small but contributes a key and a value
    # for every entry, exceeding the global node budget.
    request = {f"k{index}": False for index in range(MAX_CONTAINER_ITEMS)}
    assert MAX_CONTAINER_ITEMS * 2 >= MAX_JSON_NODES

    with pytest.raises(CapabilityProtocolError) as caught:
        parse_capability_terminal_action(_marked(_valid_envelope(request=request)))

    assert caught.value.code == "payload_too_complex"


def test_json_depth_is_bounded_before_decoder_recursion():
    nested: object = {}
    for _ in range(MAX_JSON_DEPTH + 1):
        nested = {"next": nested}

    with pytest.raises(CapabilityProtocolError) as caught:
        parse_capability_terminal_action(
            _marked(_valid_envelope(request={"nested": nested}))
        )

    assert caught.value.code == "payload_too_deep"


@pytest.mark.parametrize(
    "number_payload",
    [
        str(MAX_SAFE_JSON_INTEGER + 1),
        "1e9999",
        "NaN",
        "Infinity",
        "-Infinity",
    ],
)
def test_numeric_abuse_is_rejected(number_payload: str):
    payload = (
        '{"schema_version":1,"terminal_action":"request_capability",'
        '"capability":"plan","reason":"why",'
        f'"request":{{"number":{number_payload}}}}}'
    )

    with pytest.raises(CapabilityProtocolError) as caught:
        parse_capability_terminal_action(_marked(payload))

    assert caught.value.code in {"number_too_large", "invalid_number"}


def test_lone_surrogate_escape_is_rejected_as_invalid_unicode():
    output = _marked(_valid_envelope(request={"data": "\ud800"}))

    with pytest.raises(CapabilityProtocolError) as caught:
        parse_capability_terminal_action(output)

    assert caught.value.code == "invalid_unicode"


def test_parser_rejects_non_string_output():
    with pytest.raises(TypeError, match="output must be a string"):
        parse_capability_terminal_action(None)  # type: ignore[arg-type]


def test_invalid_allowlist_configuration_is_rejected():
    with pytest.raises(TypeError, match="not a string"):
        parse_capability_terminal_action(
            _marked(_valid_envelope()),
            allowed_capabilities="plan",  # type: ignore[arg-type]
        )

    with pytest.raises(ValueError, match="invalid allowed capability"):
        parse_capability_terminal_action(
            _marked(_valid_envelope()),
            allowed_capabilities={"Plan"},
        )


def test_instruction_builder_is_deterministic_and_provider_neutral():
    instructions = build_capability_protocol_instructions(
        ["plan", "code_review", "plan"]
    )

    assert '["code_review", "plan"]' in instructions
    assert '"schema_version":1' in instructions
    assert '"terminal_action":"request_capability"' in instructions
    assert TERMINAL_ACTION_OPEN_TAG in instructions
    assert TERMINAL_ACTION_CLOSE_TAG in instructions
    assert "final non-whitespace" in instructions
    assert "ordinary completion" in instructions
    assert "do not output or mention" in instructions


def test_instruction_builder_disables_marker_when_nothing_is_available():
    instructions = build_capability_protocol_instructions([])

    assert "no capabilities are available" in instructions
    assert "do not output" in instructions


def test_instruction_builder_rejects_invalid_capability_configuration():
    with pytest.raises(TypeError, match="not a string"):
        build_capability_protocol_instructions("plan")

    with pytest.raises(ValueError, match="invalid allowed capability"):
        build_capability_protocol_instructions(["plan", "Review"])
