from __future__ import annotations

import json

import pytest

from backend.services.test_harness_egress_proxy import (
    EgressPolicyError,
    _parse_doh_payload,
    normalize_allowed_hosts,
    require_public_addresses,
)


def test_egress_allowlist_is_exact_and_normalized():
    assert normalize_allowed_hosts(
        "GitHub.com, registry.npmjs.org.,github.com"
    ) == frozenset({"github.com", "registry.npmjs.org"})


@pytest.mark.parametrize(
    "value",
    [
        "",
        "*.github.com",
        "github.com/path",
        "127.0.0.1",
        "good.example,bad host",
    ],
)
def test_egress_allowlist_rejects_ambiguous_hosts(value):
    with pytest.raises(EgressPolicyError):
        normalize_allowed_hosts(value)


@pytest.mark.parametrize(
    "address",
    [
        "127.0.0.1",
        "10.0.0.1",
        "172.16.0.1",
        "192.168.1.1",
        "169.254.169.254",
        "::1",
        "fc00::1",
        "fe80::1",
        "0.0.0.0",
        "224.0.0.1",
    ],
)
def test_egress_rejects_every_nonpublic_dns_answer(address):
    with pytest.raises(EgressPolicyError, match="non-public"):
        require_public_addresses(["8.8.8.8", address])


def test_egress_accepts_only_fully_public_answer_sets():
    assert require_public_addresses(["8.8.8.8", "1.1.1.1", "8.8.8.8"]) == (
        "8.8.8.8",
        "1.1.1.1",
    )


def test_doh_response_is_bound_to_the_exact_question_and_record_type():
    payload = json.dumps(
        {
            "Status": 0,
            "Question": [{"name": "github.com", "type": 1}],
            "Answer": [
                {"name": "github.com", "type": 1, "data": "140.82.116.4"},
                {"name": "github.com", "type": 28, "data": "2001:db8::1"},
            ],
        }
    ).encode()

    assert _parse_doh_payload(payload, host="github.com", record_type=1) == [
        "140.82.116.4"
    ]
    with pytest.raises(EgressPolicyError, match="does not match"):
        _parse_doh_payload(payload, host="registry.npmjs.org", record_type=1)


def test_doh_response_accepts_only_addresses_on_the_question_cname_chain():
    payload = json.dumps(
        {
            "Status": 0,
            "Question": [{"name": "registry.npmjs.org", "type": 1}],
            "Answer": [
                {
                    "name": "registry.npmjs.org",
                    "type": 5,
                    "data": "registry.npmjs.org.cdn.cloudflare.net",
                },
                {
                    "name": "registry.npmjs.org.cdn.cloudflare.net",
                    "type": 1,
                    "data": "104.16.1.35",
                },
            ],
        }
    ).encode()

    assert _parse_doh_payload(
        payload,
        host="registry.npmjs.org",
        record_type=1,
    ) == ["104.16.1.35"]

    poisoned = json.loads(payload)
    poisoned["Answer"].append(
        {"name": "unrelated.example", "type": 1, "data": "8.8.8.8"}
    )
    with pytest.raises(EgressPolicyError, match="unrelated"):
        _parse_doh_payload(
            json.dumps(poisoned).encode(),
            host="registry.npmjs.org",
            record_type=1,
        )
