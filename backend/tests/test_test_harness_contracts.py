from __future__ import annotations

import pytest

from backend.services.test_harness_contracts import (
    BrowserReviewFindingInput,
    TestHarnessContractError as HarnessContractError,
    TestHarnessSpec as HarnessSpec,
    compile_test_plan,
    normalize_findings,
    request_fingerprint,
)


def test_harness_spec_normalizes_profile_budgets_and_pr_target():
    spec = HarnessSpec(
        target_kind="pull_request",
        target={"pr_number": 99, "remote": "origin"},
        goal="  Verify the changed settings flow  ",
        profile="exhaustive",
        allow_actions=True,
    ).normalized()

    assert spec.goal == "Verify the changed settings flow"
    assert spec.max_steps == 40
    assert spec.max_actions == 80
    assert spec.browser_channel == "chromium"
    assert spec.target == {"remote": "origin", "pr_number": 99}


def test_read_only_harness_rejects_an_action_budget():
    with pytest.raises(HarnessContractError, match="read-only"):
        HarnessSpec(
            target_kind="fixed_url",
            target={"url": "http://127.0.0.1:5173"},
            goal="Inspect the page",
            allow_actions=False,
            max_actions=1,
        ).normalized()


def test_browser_finding_schema_exposes_only_canonical_fields():
    finding = BrowserReviewFindingInput.model_validate(
        {
            "scenario_id": "runtime-health",
            "severity": "medium",
            "category": "network",
            "title": "Status endpoint returned 404",
            "route": "/#/tasks",
            "locator": "GET /api/pool/status",
            "expected": "The endpoint succeeds",
            "actual": "The endpoint returned 404",
            "reproduction": ["Open Tasks"],
            "evidence": ["final.png"],
        }
    )
    assert finding.model_dump()["reproduction"] == ["Open Tasks"]

    with pytest.raises(ValueError, match="reproduction_steps"):
        BrowserReviewFindingInput.model_validate(
            {
                "severity": "medium",
                "category": "network",
                "title": "Status endpoint returned 404",
                "reproduction_steps": ["Open Tasks"],
            }
        )


def test_explicit_plan_is_bounded_and_normalized():
    plan = compile_test_plan(
        goal="unused when an explicit plan is supplied",
        profile="standard",
        allow_actions=True,
        viewport_width=1440,
        viewport_height=900,
        max_steps=24,
        max_actions=80,
        supplied={
            "version": 1,
            "objective": "  Validate checkout  ",
            "scenarios": [
                {
                    "id": "checkout",
                    "title": "Checkout",
                    "route": "/checkout",
                    "steps": ["Open checkout"],
                    "assertions": ["The submit control is visible"],
                }
            ],
        },
    )

    assert plan["objective"] == "Validate checkout"
    assert plan["scenarios"][0]["id"] == "checkout"


def test_findings_have_stable_severity_independent_fingerprints():
    common = {
        "scenario_id": "checkout",
        "category": "functional",
        "title": "Submit button is clipped",
        "route": "/checkout",
        "locator": "button[type=submit]",
        "expected": "The complete button is visible",
        "actual": "The right edge is clipped",
        "reproduction": ["Open checkout"],
        "evidence": ["final.png"],
    }
    first = normalize_findings([{**common, "severity": "high"}])[0]
    second = normalize_findings([{**common, "severity": "medium"}])[0]

    assert first["fingerprint"] == second["fingerprint"]
    assert first["severity"] == "high"


def test_request_fingerprint_is_canonical():
    one = request_fingerprint(
        target_kind="git_ref",
        target={"ref": "feature", "remote": "origin"},
        test_plan={"version": 1, "scenarios": [], "objective": "x"},
        runtime={"provider": "codex", "model": "gpt-5.6-sol"},
    )
    two = request_fingerprint(
        target_kind="git_ref",
        target={"remote": "origin", "ref": "feature"},
        test_plan={"objective": "x", "scenarios": [], "version": 1},
        runtime={"model": "gpt-5.6-sol", "provider": "codex"},
    )

    assert one == two
