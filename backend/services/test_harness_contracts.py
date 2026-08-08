"""Provider-neutral contracts and normalization for frontend test runs."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


HarnessTargetKind = Literal["current_workspace", "fixed_url", "pull_request", "git_ref"]
HarnessProfile = Literal["quick", "standard", "exhaustive"]
DEFAULT_BROWSER_CHANNEL = "chromium"

HARNESS_TERMINAL_STATUSES = frozenset(
    {"completed", "failed", "cancelled", "stale"}
)
HARNESS_ACTIVE_STATUSES = frozenset(
    {
        "queued",
        "resolving_target",
        "preparing_environment",
        "preview_ready",
        "running",
        "collecting_evidence",
        "evaluating",
        "cleaning",
        "cancelling",
    }
)
HARNESS_VERDICTS = frozenset(
    {"passed", "failed", "inconclusive", "error", "cancelled", "stale"}
)
FINDING_SEVERITIES = frozenset({"critical", "high", "medium", "low", "info"})

_SAFE_ID_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,119}$")


class TestHarnessContractError(ValueError):
    """The caller supplied an invalid harness contract."""


class BrowserReviewFindingInput(BaseModel):
    """Canonical structured finding shared by API and Browser MCP schemas."""

    model_config = ConfigDict(extra="forbid")

    scenario_id: str = Field(default="primary-flow", min_length=1, max_length=120)
    severity: Literal["critical", "high", "medium", "low", "info"]
    category: str = Field(min_length=1, max_length=80)
    title: str = Field(min_length=1, max_length=500)
    route: str | None = Field(default=None, max_length=1000)
    locator: str | None = Field(default=None, max_length=1000)
    expected: str | None = Field(default=None, max_length=8000)
    actual: str | None = Field(default=None, max_length=8000)
    reproduction: list[str] = Field(
        default_factory=list,
        max_length=30,
        description="JSON array of separate reproduction-step strings; never one string",
    )
    evidence: list[str] = Field(
        default_factory=list,
        max_length=30,
        description="JSON array of evidence artifact names or concise evidence references",
    )
    confidence: float | None = Field(
        default=None,
        ge=0,
        le=1,
        description="Numeric confidence from 0 to 1; omit instead of using words",
    )


@dataclass(frozen=True, slots=True)
class TestHarnessSpec:
    target_kind: HarnessTargetKind
    target: dict[str, Any]
    goal: str
    profile: HarnessProfile = "standard"
    allow_actions: bool = True
    browser_channel: str = DEFAULT_BROWSER_CHANNEL
    viewport_width: int = 1440
    viewport_height: int = 900
    max_steps: int | None = None
    max_actions: int | None = None
    provider: Literal["claude", "codex"] | None = None
    model: str | None = None
    reasoning_effort: str | None = None
    codex_service_tier: Literal["default", "priority"] | None = None
    test_plan: dict[str, Any] | None = None
    parent_run_id: str | None = None
    idempotency_key: str | None = None

    def normalized(self) -> "TestHarnessSpec":
        goal = self.goal.strip()
        if not goal or len(goal) > 20_000:
            raise TestHarnessContractError("test harness goal must contain 1 to 20000 characters")
        if self.target_kind not in {
            "current_workspace",
            "fixed_url",
            "pull_request",
            "git_ref",
        }:
            raise TestHarnessContractError("unsupported test harness target kind")
        if self.profile not in {"quick", "standard", "exhaustive"}:
            raise TestHarnessContractError("test harness profile is invalid")
        if self.browser_channel not in {"chrome", "chromium"}:
            raise TestHarnessContractError("browser channel must be chrome or chromium")
        if not 320 <= self.viewport_width <= 3840:
            raise TestHarnessContractError("viewport width must be between 320 and 3840")
        if not 320 <= self.viewport_height <= 2160:
            raise TestHarnessContractError("viewport height must be between 320 and 2160")
        max_steps = self.max_steps
        if max_steps is None:
            max_steps = {"quick": 12, "standard": 24, "exhaustive": 40}[self.profile]
        if not 1 <= max_steps <= 50:
            raise TestHarnessContractError("max_steps must be between 1 and 50")
        max_actions = self.max_actions
        if max_actions is None:
            max_actions = 80 if self.allow_actions else 0
        if not 0 <= max_actions <= 200:
            raise TestHarnessContractError("max_actions must be between 0 and 200")
        if not self.allow_actions and max_actions != 0:
            raise TestHarnessContractError("read-only harness runs must use max_actions=0")
        if self.provider is not None and self.provider not in {"claude", "codex"}:
            raise TestHarnessContractError("test harness provider is invalid")
        if self.codex_service_tier is not None and self.codex_service_tier not in {
            "default",
            "priority",
        }:
            raise TestHarnessContractError("test harness Codex service tier is invalid")
        model = self.model.strip() if self.model is not None else None
        if model is not None and (not model or len(model) > 100 or "\x00" in model):
            raise TestHarnessContractError("test harness model is invalid")
        reasoning_effort = (
            self.reasoning_effort.strip().lower()
            if self.reasoning_effort is not None
            else None
        )
        if reasoning_effort is not None and (
            not reasoning_effort
            or len(reasoning_effort) > 20
            or "\x00" in reasoning_effort
        ):
            raise TestHarnessContractError("test harness reasoning effort is invalid")
        target = normalize_target(self.target_kind, self.target)
        idempotency_key = self.idempotency_key.strip() if self.idempotency_key else None
        if idempotency_key is not None and (
            len(idempotency_key) > 200 or "\x00" in idempotency_key
        ):
            raise TestHarnessContractError("idempotency key is invalid")
        return TestHarnessSpec(
            target_kind=self.target_kind,
            target=target,
            goal=goal,
            profile=self.profile,
            allow_actions=self.allow_actions,
            browser_channel=self.browser_channel,
            viewport_width=self.viewport_width,
            viewport_height=self.viewport_height,
            max_steps=max_steps,
            max_actions=max_actions,
            provider=self.provider,
            model=model,
            reasoning_effort=reasoning_effort,
            codex_service_tier=self.codex_service_tier,
            test_plan=self.test_plan,
            parent_run_id=self.parent_run_id,
            idempotency_key=idempotency_key,
        )


def normalize_target(kind: HarnessTargetKind, target: object) -> dict[str, Any]:
    if not isinstance(target, dict):
        raise TestHarnessContractError("target must be an object")
    unknown = set(target) - {
        "url",
        "pr_number",
        "remote",
        "ref",
        "fetch",
    }
    if unknown:
        raise TestHarnessContractError(
            f"target contains unsupported field: {sorted(unknown)[0]}"
        )
    if kind == "current_workspace":
        if target:
            raise TestHarnessContractError("current_workspace target takes no fields")
        return {}
    if kind == "fixed_url":
        url = target.get("url")
        if not isinstance(url, str) or not url.strip() or len(url) > 2048:
            raise TestHarnessContractError("fixed_url target requires a URL")
        return {"url": url.strip()}
    remote = target.get("remote", "origin")
    if not isinstance(remote, str) or not remote.strip() or len(remote) > 120:
        raise TestHarnessContractError("target remote is invalid")
    result: dict[str, Any] = {"remote": remote.strip()}
    if kind == "pull_request":
        number = target.get("pr_number")
        if isinstance(number, bool) or not isinstance(number, int) or number <= 0:
            raise TestHarnessContractError("pull_request target requires a positive pr_number")
        result["pr_number"] = number
        return result
    ref = target.get("ref")
    if not isinstance(ref, str) or not ref.strip() or len(ref) > 300 or "\x00" in ref:
        raise TestHarnessContractError("git_ref target requires a ref")
    result["ref"] = ref.strip()
    result["fetch"] = bool(target.get("fetch", False))
    return result


def compile_test_plan(
    *,
    goal: str,
    profile: HarnessProfile,
    allow_actions: bool,
    viewport_width: int,
    viewport_height: int,
    max_steps: int,
    max_actions: int,
    supplied: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Validate an explicit plan or create a deterministic, immutable default."""

    if supplied is not None:
        return validate_test_plan(supplied)
    scenarios = [
        {
            "id": "primary-flow",
            "title": "Requested user-visible flow",
            "route": None,
            "steps": [
                "Open the target and capture the initial visible state.",
                "Exercise the requested primary flow using only permitted actions.",
                "Verify visible feedback after every meaningful state transition.",
            ],
            "assertions": [
                goal,
                "No blocking visual clipping, overlap, or unreadable control labels are visible.",
            ],
        },
        {
            "id": "runtime-health",
            "title": "Runtime, network, and recovery signals",
            "route": None,
            "steps": [
                "Inspect console, page, request, and HTTP telemetry.",
                "Safely inspect reachable loading, empty, validation, and error feedback.",
            ],
            "assertions": [
                "No unexplained page errors, failed requests, or HTTP errors block the flow.",
                "Every reported defect has reproducible browser evidence.",
            ],
        },
    ]
    if profile == "exhaustive":
        scenarios.append(
            {
                "id": "boundary-accessibility",
                "title": "Boundary layout and keyboard behavior",
                "route": None,
                "steps": [
                    "Inspect narrow-layout risks visible at the configured viewport.",
                    "Exercise safe keyboard focus and boundary inputs where reachable.",
                ],
                "assertions": [
                    "Primary controls remain operable and focus feedback is visible.",
                    "Boundary states do not silently lose user input or navigation context.",
                ],
            }
        )
    return {
        "version": 1,
        "objective": goal,
        "profile": profile,
        "scenarios": scenarios,
        "viewport": {"width": viewport_width, "height": viewport_height},
        "policy": {
            "allow_actions": allow_actions,
            "destructive_actions": "forbidden",
            "cross_origin_navigation": "forbidden",
            "credentials": "forbidden",
        },
        "budgets": {"max_steps": max_steps, "max_actions": max_actions},
    }


def validate_test_plan(plan: object) -> dict[str, Any]:
    if not isinstance(plan, dict) or plan.get("version") != 1:
        raise TestHarnessContractError("test_plan version must be 1")
    objective = plan.get("objective")
    if not isinstance(objective, str) or not objective.strip() or len(objective) > 20_000:
        raise TestHarnessContractError("test_plan objective is invalid")
    scenarios = plan.get("scenarios")
    if not isinstance(scenarios, list) or not 1 <= len(scenarios) <= 30:
        raise TestHarnessContractError("test_plan requires 1 to 30 scenarios")
    normalized_scenarios: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in scenarios:
        if not isinstance(raw, dict):
            raise TestHarnessContractError("test_plan scenarios must be objects")
        scenario_id = raw.get("id")
        if not isinstance(scenario_id, str) or _SAFE_ID_RE.fullmatch(scenario_id) is None:
            raise TestHarnessContractError("test_plan scenario id is invalid")
        if scenario_id in seen:
            raise TestHarnessContractError("test_plan scenario ids must be unique")
        seen.add(scenario_id)
        title = _bounded_text(raw.get("title"), "scenario title", 240)
        route = raw.get("route")
        if route is not None and (
            not isinstance(route, str) or len(route) > 1000 or "\x00" in route
        ):
            raise TestHarnessContractError("scenario route is invalid")
        steps = _bounded_text_list(raw.get("steps"), "scenario steps", 1, 30, 2000)
        assertions = _bounded_text_list(
            raw.get("assertions"), "scenario assertions", 1, 30, 4000
        )
        normalized_scenarios.append(
            {
                "id": scenario_id,
                "title": title,
                "route": route,
                "steps": steps,
                "assertions": assertions,
            }
        )
    normalized = json.loads(json.dumps(plan))
    normalized["objective"] = objective.strip()
    normalized["scenarios"] = normalized_scenarios
    # The complete plan is injected into an agent prompt; keep it bounded.
    if len(canonical_json(normalized)) > 100_000:
        raise TestHarnessContractError("test_plan is too large")
    return normalized


def normalize_findings(raw_findings: object) -> list[dict[str, Any]]:
    if raw_findings is None:
        return []
    if not isinstance(raw_findings, list) or len(raw_findings) > 100:
        raise TestHarnessContractError("findings must contain at most 100 items")
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, raw in enumerate(raw_findings, start=1):
        if not isinstance(raw, dict):
            raise TestHarnessContractError("findings must be objects")
        scenario_id = raw.get("scenario_id", "primary-flow")
        if not isinstance(scenario_id, str) or _SAFE_ID_RE.fullmatch(scenario_id) is None:
            raise TestHarnessContractError("finding scenario_id is invalid")
        severity = str(raw.get("severity") or "medium").lower()
        if severity not in FINDING_SEVERITIES:
            raise TestHarnessContractError("finding severity is invalid")
        category = _bounded_text(raw.get("category", "functional"), "finding category", 80)
        title = _bounded_text(raw.get("title"), "finding title", 500)
        route = _optional_text(raw.get("route"), 1000)
        locator = _optional_text(raw.get("locator"), 1000)
        expected = _optional_text(raw.get("expected"), 8000)
        actual = _optional_text(raw.get("actual"), 8000)
        reproduction = _bounded_text_list(
            raw.get("reproduction", []), "finding reproduction", 0, 30, 2000
        )
        evidence_names = _bounded_text_list(
            raw.get("evidence", []), "finding evidence", 0, 30, 255
        )
        confidence_raw = raw.get("confidence")
        confidence = None
        if confidence_raw is not None:
            if isinstance(confidence_raw, bool) or not isinstance(confidence_raw, (int, float)):
                raise TestHarnessContractError("finding confidence must be a number")
            confidence = float(confidence_raw)
            if not 0 <= confidence <= 1:
                raise TestHarnessContractError("finding confidence must be between 0 and 1")
        stable_material = {
            "scenario_id": scenario_id.lower(),
            "severity_independent_category": category.lower(),
            "route": (route or "").strip().lower(),
            "locator": (locator or "").strip().lower(),
            "title": " ".join(title.lower().split()),
        }
        fingerprint = hashlib.sha256(canonical_json(stable_material).encode()).hexdigest()
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        normalized.append(
            {
                "ordinal": index,
                "fingerprint": fingerprint,
                "scenario_id": scenario_id,
                "severity": severity,
                "category": category,
                "title": title,
                "route": route,
                "locator": locator,
                "expected": expected,
                "actual": actual,
                "reproduction": reproduction,
                "evidence_names": evidence_names,
                "confidence": confidence,
            }
        )
    return normalized


def normalize_verdict(value: object, *, report: str | None = None) -> str:
    if isinstance(value, str):
        candidate = value.strip().lower()
        if candidate in {"passed", "failed", "inconclusive"}:
            return candidate
    text = (report or "").lower()
    if any(token in text for token in ("verdict: fail", "verdict：fail", "结论：失败", "未通过")):
        return "failed"
    if any(token in text for token in ("verdict: pass", "verdict：pass", "结论：通过", "通过验收")):
        return "passed"
    return "inconclusive"


def request_fingerprint(
    *,
    target_kind: str,
    target: dict[str, Any],
    test_plan: dict[str, Any],
    runtime: dict[str, Any],
) -> str:
    payload = {
        "contract": "ccm-test-harness-v1",
        "target_kind": target_kind,
        "target": target,
        "test_plan": test_plan,
        "runtime": runtime,
    }
    return hashlib.sha256(canonical_json(payload).encode()).hexdigest()


def canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _bounded_text(value: object, label: str, limit: int) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > limit or "\x00" in value:
        raise TestHarnessContractError(f"{label} is invalid")
    return value.strip()


def _optional_text(value: object, limit: int) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or len(value) > limit or "\x00" in value:
        raise TestHarnessContractError("finding text field is invalid")
    return value.strip() or None


def _bounded_text_list(
    value: object,
    label: str,
    minimum: int,
    maximum: int,
    item_limit: int,
) -> list[str]:
    if not isinstance(value, list) or not minimum <= len(value) <= maximum:
        raise TestHarnessContractError(f"{label} has an invalid number of items")
    result: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip() or len(item) > item_limit or "\x00" in item:
            raise TestHarnessContractError(f"{label} contains an invalid item")
        result.append(item.strip())
    return result
