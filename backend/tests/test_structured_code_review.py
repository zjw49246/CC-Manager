"""Pure contract tests for provider-neutral structured code review."""

import json

import pytest

from backend.services.structured_code_review import (
    CODE_REVIEWER_ROLE,
    STRUCTURED_CODE_REVIEW_SCHEMA_V1,
    CommitRangeSubject,
    build_structured_review_prompt,
    finding_fingerprint,
    parse_structured_review_output,
)


BASE_SHA = "a" * 40
HEAD_SHA = "b" * 40
HEAD_TREE_SHA = "c" * 40
PATCH = "diff --git a/app.py b/app.py\n+fixed = True\n"


@pytest.fixture
def subject() -> CommitRangeSubject:
    return CommitRangeSubject.from_patch(
        base_sha=BASE_SHA,
        head_sha=HEAD_SHA,
        head_tree_sha=HEAD_TREE_SHA,
        patch=PATCH,
    )


def _finding(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "severity": "medium",
        "category": "correctness",
        "path": "backend/services/example.py",
        "line": 17,
        "hunk": None,
        "title": "Terminal state can be lost",
        "evidence": "The wake happens before the durable state commit.",
        "impact": "A crash can leave the work permanently pending.",
        "required_fix": "Commit the state before publishing the wake.",
        "test": "Crash after the commit and assert recovery observes the state.",
    }
    value.update(overrides)
    return value


def _payload(
    subject: CommitRangeSubject,
    *,
    verdict: str = "pass",
    findings: list[dict[str, object]] | None = None,
    **overrides: object,
) -> dict[str, object]:
    value: dict[str, object] = {
        "schema_version": 1,
        "subject": subject.as_dict(),
        "role": CODE_REVIEWER_ROLE,
        "verdict": verdict,
        "summary": "The immutable change was reviewed.",
        "findings": [] if findings is None else findings,
    }
    value.update(overrides)
    return value


def _output(value: object) -> str:
    return (
        "<ccm_code_review>\n"
        + json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        + "\n</ccm_code_review>"
    )


def test_schema_v1_is_closed_and_pins_commit_range_and_role():
    assert STRUCTURED_CODE_REVIEW_SCHEMA_V1["additionalProperties"] is False
    properties = STRUCTURED_CODE_REVIEW_SCHEMA_V1["properties"]
    assert isinstance(properties, dict)
    assert properties["schema_version"] == {"const": 1}
    assert properties["role"] == {"const": "code_reviewer"}
    subject_schema = properties["subject"]
    assert isinstance(subject_schema, dict)
    assert subject_schema["additionalProperties"] is False
    assert set(subject_schema["required"]) == {
        "kind",
        "base_sha",
        "head_sha",
        "head_tree_sha",
        "patch_sha256",
    }


def test_build_pre_pr_prompt_is_provider_neutral_and_subject_pinned(subject):
    prompt = build_structured_review_prompt(
        subject=subject,
        surface="pre_pr",
        expected_role="code_reviewer",
        guidance={"AGENTS.md": "Keep state transitions durable."},
        material={"patch": PATCH, "changed_paths": ["app.py"]},
    )

    assert "Surface: `pre_pr`" in prompt
    assert json.dumps(
        subject.as_dict(), sort_keys=True, separators=(",", ":")
    ) in prompt
    assert "<ccm_code_review>" in prompt
    assert '"verdict":"pass"' in prompt
    assert '"findings":[]' in prompt
    assert "pass|changes_required" not in prompt
    assert "Claude" not in prompt
    assert "Codex" not in prompt


def test_build_retry_prompt_calls_out_complete_finding_shape(subject):
    prompt = build_structured_review_prompt(
        subject=subject,
        surface="pre_pr",
        expected_role="code_reviewer",
        material={"patch": PATCH, "changed_paths": ["app.py"]},
        retry_after_schema_failure=True,
    )

    assert "## Retry correction" in prompt
    assert "previous response" in prompt
    assert "`title`" in prompt
    assert "complete replacement result" in prompt


def test_build_rejects_unknown_surface_or_role(subject):
    with pytest.raises(ValueError, match="unsupported.*surface"):
        build_structured_review_prompt(
            subject=subject,
            material=PATCH,
            surface="pull_request",
        )
    with pytest.raises(ValueError, match="unsupported.*role"):
        build_structured_review_prompt(
            subject=subject,
            material=PATCH,
            expected_role="senior_engineer",
        )


def test_build_rejects_material_patch_that_does_not_match_subject(subject):
    with pytest.raises(ValueError, match="patch does not match"):
        build_structured_review_prompt(
            subject=subject,
            material={"patch": PATCH + "tampered\n"},
        )


def test_parse_legal_pass_result(subject):
    result = parse_structured_review_output(
        "\n" + _output(_payload(subject)) + "\n",
        expected_subject=subject,
    )

    assert result == {
        "schema_version": 1,
        "subject": subject.as_dict(),
        "role": "code_reviewer",
        "verdict": "pass",
        "summary": "The immutable change was reviewed.",
        "findings": [],
    }


def test_parse_legal_changes_normalizes_location_and_tokens(subject):
    result = parse_structured_review_output(
        _output(_payload(
            subject,
            verdict="changes_required",
            findings=[_finding(
                severity=" Medium ",
                category=" Concurrency ",
                path=" ./backend\\services//example.py ",
                line=None,
                hunk=" @@ -4,2 +4,3 @@\r\n state = pending ",
            )],
        )),
        expected_subject=subject.as_dict(),
        surface="pre_pr",
    )

    finding = result["findings"][0]
    assert finding["severity"] == "medium"
    assert finding["category"] == "concurrency"
    assert finding["path"] == "backend/services/example.py"
    assert finding["line"] is None
    assert finding["hunk"] == "@@ -4,2 +4,3 @@\n state = pending"
    assert len(finding["fingerprint"]) == 64
    int(finding["fingerprint"], 16)


@pytest.mark.parametrize(
    "content",
    [
        "not json",
        "<ccm_code_review>\n{}\n</ccm_code_review> trailing",
        "```json\n<ccm_code_review>\n{}\n</ccm_code_review>\n```",
        '<ccm_code_review>\n{"schema_version":1,,}\n</ccm_code_review>',
    ],
)
def test_parse_rejects_malformed_terminal_output(subject, content):
    with pytest.raises(ValueError):
        parse_structured_review_output(content, expected_subject=subject)


def test_parse_rejects_multiple_result_blocks(subject):
    block = _output(_payload(subject))
    with pytest.raises(ValueError, match="one result block"):
        parse_structured_review_output(
            block + "\n" + block,
            expected_subject=subject,
        )


def test_parse_rejects_subject_mismatch_in_every_immutable_dimension(subject):
    fields = {
        "base_sha": "d" * 40,
        "head_sha": "d" * 40,
        "head_tree_sha": "d" * 40,
        "patch_sha256": "d" * 64,
    }
    for field, replacement in fields.items():
        wrong = subject.as_dict()
        wrong[field] = replacement
        payload = _payload(subject)
        payload["subject"] = wrong
        with pytest.raises(ValueError, match="subject does not match"):
            parse_structured_review_output(
                _output(payload),
                expected_subject=subject,
            )


def test_parse_rejects_role_mismatch(subject):
    with pytest.raises(ValueError, match="role does not match"):
        parse_structured_review_output(
            _output(_payload(subject, role="qa_engineer")),
            expected_subject=subject,
        )


@pytest.mark.parametrize(
    ("verdict", "findings"),
    [
        ("pass", [_finding(severity="critical")]),
        ("pass", [_finding(severity="high")]),
        ("pass", [_finding(severity="medium")]),
        ("changes_required", []),
        ("changes_required", [_finding(severity="low")]),
    ],
)
def test_parse_rejects_verdict_blocker_mismatch(subject, verdict, findings):
    with pytest.raises(ValueError, match="verdict does not match"):
        parse_structured_review_output(
            _output(_payload(subject, verdict=verdict, findings=findings)),
            expected_subject=subject,
        )


def test_parse_allows_advisory_findings_with_pass(subject):
    result = parse_structured_review_output(
        _output(_payload(subject, findings=[_finding(severity="low")])),
        expected_subject=subject,
    )
    assert result["verdict"] == "pass"
    assert result["findings"][0]["severity"] == "low"


def test_duplicate_finding_is_rejected_after_normalization(subject):
    first = _finding(
        severity="medium",
        category="concurrency",
        path="./backend/services/example.py",
        title="Terminal state can be lost",
        evidence="The wake happens before the durable state commit.",
    )
    duplicate = _finding(
        severity="HIGH",
        category=" CONCURRENCY ",
        path="backend//services\\example.py",
        title="terminal STATE can be LOST",
        evidence="The wake  happens before the durable state commit.",
    )

    with pytest.raises(ValueError, match="duplicate finding"):
        parse_structured_review_output(
            _output(_payload(
                subject,
                verdict="changes_required",
                findings=[first, duplicate],
            )),
            expected_subject=subject,
        )


def test_finding_fingerprint_is_stable_and_excludes_severity():
    first = _finding(severity="medium")
    second = _finding(
        severity="critical",
        category="CORRECTNESS",
        path="./backend//services\\example.py",
        title="terminal STATE can be LOST",
        evidence="The wake  happens before the durable state commit.",
    )

    assert finding_fingerprint(first) == finding_fingerprint(second)
    fingerprint = finding_fingerprint(first)
    assert len(fingerprint) == 64
    int(fingerprint, 16)


def test_parse_rejects_unknown_fields_and_duplicate_json_keys(subject):
    extra = _payload(subject, unexpected=True)
    with pytest.raises(ValueError, match="invalid fields"):
        parse_structured_review_output(_output(extra), expected_subject=subject)

    duplicate_key_json = (
        '<ccm_code_review>\n{"schema_version":1,"schema_version":1,'
        f'"subject":{json.dumps(subject.as_dict())},"role":"code_reviewer",'
        '"verdict":"pass","summary":"ok","findings":[]}\n'
        "</ccm_code_review>"
    )
    with pytest.raises(ValueError, match="repeats key"):
        parse_structured_review_output(duplicate_key_json, expected_subject=subject)


def test_parse_reports_missing_required_finding_fields(subject):
    finding = _finding()
    finding.pop("title")

    with pytest.raises(ValueError, match="missing required fields: title"):
        parse_structured_review_output(
            _output(_payload(
                subject,
                verdict="changes_required",
                findings=[finding],
            )),
            expected_subject=subject,
        )


def test_parse_rejects_finding_without_attachable_location(subject):
    with pytest.raises(ValueError, match="line or hunk"):
        parse_structured_review_output(
            _output(_payload(
                subject,
                verdict="changes_required",
                findings=[_finding(line=None, hunk=None)],
            )),
            expected_subject=subject,
        )
