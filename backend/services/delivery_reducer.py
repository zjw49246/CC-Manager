"""Pure state reducer for the autonomous delivery loop.

This module never performs I/O.  Controllers persist the returned state with
an optimistic ``state_version`` compare-and-swap and only then execute the
returned effect hints.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import Any, Mapping

from backend.models.delivery import (
    DELIVERY_ACTIVITIES,
    DELIVERY_OUTCOMES,
    DELIVERY_PHASES,
)


class DeliveryReducerError(ValueError):
    """Base class for invalid or stale delivery transitions."""


class DeliveryStateVersionError(DeliveryReducerError):
    """The caller reduced a stale DeliveryRun snapshot."""


class DeliveryTransitionError(DeliveryReducerError):
    """The requested event is not legal from the current state."""


@dataclass(frozen=True, slots=True)
class DeliveryState:
    phase: str
    activity: str
    outcome: str | None = None
    wait_reason: str | None = None
    paused_from_activity: str | None = None
    pause_reason: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    state_version: int = 1


@dataclass(frozen=True, slots=True)
class DeliveryReducerEvent:
    kind: str
    payload: Mapping[str, Any] = MappingProxyType({})


@dataclass(frozen=True, slots=True)
class DeliveryReduction:
    state: DeliveryState
    effects: tuple[str, ...]


_READY_EFFECTS = {
    "planning": ("request_plan",),
    "coding": ("dispatch_code",),
    "pre_review": ("request_code_review",),
    "frontend_review": ("request_frontend_review",),
    "publishing": ("ensure_pull_request",),
}


def initial_delivery_state() -> DeliveryState:
    return DeliveryState(phase="planning", activity="ready")


def validate_delivery_state(state: DeliveryState) -> None:
    if state.phase not in DELIVERY_PHASES:
        raise DeliveryReducerError(f"Unknown Delivery phase {state.phase!r}")
    if state.activity not in DELIVERY_ACTIVITIES:
        raise DeliveryReducerError(f"Unknown Delivery activity {state.activity!r}")
    if state.outcome is not None and state.outcome not in DELIVERY_OUTCOMES:
        raise DeliveryReducerError(f"Unknown Delivery outcome {state.outcome!r}")
    if (
        isinstance(state.state_version, bool)
        or not isinstance(state.state_version, int)
        or state.state_version < 1
    ):
        raise DeliveryReducerError("state_version must be a positive integer")

    terminal = state.phase == "done" or state.activity == "terminal"
    if terminal:
        if (
            state.phase != "done"
            or state.activity != "terminal"
            or state.outcome is None
        ):
            raise DeliveryReducerError(
                "Terminal Delivery state requires done/terminal with an outcome"
            )
        if state.wait_reason is not None or state.paused_from_activity is not None:
            raise DeliveryReducerError("Terminal Delivery state cannot retain a wait")
        if state.outcome != "failed" and (
            state.error_code is not None or state.error_message is not None
        ):
            raise DeliveryReducerError(
                "Only a failed Delivery state may retain error metadata"
            )
        return

    if state.outcome is not None:
        raise DeliveryReducerError(
            "A non-terminal Delivery state cannot have an outcome"
        )
    if state.activity == "waiting" and not state.wait_reason:
        raise DeliveryReducerError("Waiting Delivery state requires wait_reason")
    if state.activity != "waiting" and state.wait_reason is not None:
        raise DeliveryReducerError("Only a waiting Delivery state may have wait_reason")
    if state.activity == "paused":
        if state.paused_from_activity not in {"ready", "running", "waiting"}:
            raise DeliveryReducerError(
                "Paused Delivery state requires its prior activity"
            )
        if not state.pause_reason:
            raise DeliveryReducerError("Paused Delivery state requires pause_reason")
    elif state.paused_from_activity is not None or state.pause_reason is not None:
        raise DeliveryReducerError(
            "Only a paused Delivery state may retain pause metadata"
        )


def effects_for_state(state: DeliveryState) -> tuple[str, ...]:
    validate_delivery_state(state)
    if state.activity == "ready":
        return _READY_EFFECTS.get(state.phase, ())
    if state.phase == "planning" and state.activity == "waiting":
        return ("observe_plan",)
    if state.phase == "pre_review" and state.activity == "waiting":
        return ("observe_code_review",)
    if state.phase == "frontend_review" and state.activity == "waiting":
        return ("observe_frontend_review",)
    if state.phase == "monitoring" and state.activity == "waiting":
        return ("observe_pr_monitor",)
    return ()


def _require_state(
    state: DeliveryState,
    event: DeliveryReducerEvent,
    *allowed: tuple[str, str],
) -> None:
    if (state.phase, state.activity) not in allowed:
        expected = ", ".join(f"{phase}/{activity}" for phase, activity in allowed)
        raise DeliveryTransitionError(
            f"Event {event.kind!r} requires {expected}; current state is "
            f"{state.phase}/{state.activity}"
        )


def _active_state(
    state: DeliveryState,
    *,
    phase: str,
    activity: str,
    wait_reason: str | None = None,
) -> DeliveryState:
    return replace(
        state,
        phase=phase,
        activity=activity,
        outcome=None,
        wait_reason=wait_reason,
        paused_from_activity=None,
        pause_reason=None,
        error_code=None,
        error_message=None,
    )


def _terminal_state(
    state: DeliveryState,
    *,
    outcome: str,
    error_code: str | None = None,
    error_message: str | None = None,
) -> DeliveryState:
    return replace(
        state,
        phase="done",
        activity="terminal",
        outcome=outcome,
        wait_reason=None,
        paused_from_activity=None,
        pause_reason=None,
        error_code=error_code,
        error_message=error_message,
    )


def _non_empty_text(
    event: DeliveryReducerEvent,
    key: str,
    *,
    default: str | None = None,
) -> str | None:
    value = event.payload.get(key, default)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise DeliveryTransitionError(f"Event field {key!r} must be non-empty text")
    return value.strip()


def _require_wait_reason(
    state: DeliveryState,
    event: DeliveryReducerEvent,
    expected: str,
) -> None:
    if state.wait_reason != expected:
        raise DeliveryTransitionError(
            f"Event {event.kind!r} requires wait_reason {expected!r}; current "
            f"wait_reason is {state.wait_reason!r}"
        )


_PHASE_WAIT_REASONS = {
    "planning": "plan_capability",
    "pre_review": "code_review_capability",
    "frontend_review": "frontend_review",
    "monitoring": "pr_monitor",
}


def reduce_delivery_state(
    state: DeliveryState,
    event: DeliveryReducerEvent,
    *,
    expected_version: int | None = None,
) -> DeliveryReduction:
    """Apply one event and return a new immutable state plus effect hints."""

    validate_delivery_state(state)
    if expected_version is not None and state.state_version != expected_version:
        raise DeliveryStateVersionError(
            f"Stale Delivery state version: expected {expected_version}, "
            f"current {state.state_version}"
        )
    if not isinstance(event.kind, str) or not event.kind.strip():
        raise DeliveryTransitionError("Delivery event kind is required")
    if not isinstance(event.payload, Mapping):
        raise DeliveryTransitionError("Delivery event payload must be a mapping")

    kind = event.kind.strip()
    if state.activity == "terminal" and kind != "retry":
        raise DeliveryTransitionError("A terminal DeliveryRun is immutable")

    if kind == "retry":
        _require_state(state, event, ("done", "terminal"))
        if state.outcome != "failed":
            raise DeliveryTransitionError("Only a failed DeliveryRun can be retried")
        retry_phase = event.payload.get("phase")
        retry_activity = event.payload.get("activity")
        allowed_retry_states = {
            ("planning", "ready"),
            ("coding", "ready"),
            ("pre_review", "ready"),
            ("frontend_review", "ready"),
            ("publishing", "ready"),
            ("monitoring", "waiting"),
        }
        if (retry_phase, retry_activity) not in allowed_retry_states:
            raise DeliveryTransitionError(
                "Retry requires an exact supported Delivery phase and activity"
            )
        next_state = _active_state(
            state,
            phase=retry_phase,
            activity=retry_activity,
            wait_reason=("pr_monitor" if retry_phase == "monitoring" else None),
        )
    elif kind == "pause":
        if state.activity == "paused":
            raise DeliveryTransitionError("DeliveryRun is already paused")
        reason = _non_empty_text(event, "reason")
        next_state = replace(
            state,
            activity="paused",
            outcome=None,
            wait_reason=None,
            paused_from_activity=state.activity,
            pause_reason=reason,
            error_code=None,
            error_message=None,
        )
    elif kind == "resume":
        _require_state(state, event, (state.phase, "paused"))
        requested = event.payload.get("activity")
        if requested is None:
            requested = state.paused_from_activity
        if requested not in {"ready", "running", "waiting"}:
            raise DeliveryTransitionError(
                "Resume activity must be 'ready', 'running', or 'waiting'"
            )
        wait_reason = None
        if requested == "waiting":
            default_wait_reason = _PHASE_WAIT_REASONS.get(state.phase)
            if default_wait_reason is None and "wait_reason" not in event.payload:
                raise DeliveryTransitionError(
                    f"Phase {state.phase!r} has no resumable waiting observer"
                )
            wait_reason = _non_empty_text(
                event,
                "wait_reason",
                default=default_wait_reason,
            )
        next_state = _active_state(
            state,
            phase=state.phase,
            activity=requested,
            wait_reason=wait_reason,
        )
    elif kind == "cancel":
        next_state = _terminal_state(state, outcome="cancelled")
    elif kind == "supersede":
        next_state = _terminal_state(state, outcome="superseded")
    elif kind == "fail":
        next_state = _terminal_state(
            state,
            outcome="failed",
            error_code=_non_empty_text(event, "error_code", default="delivery_failed"),
            error_message=_non_empty_text(event, "error_message"),
        )
    elif kind == "plan_requested":
        _require_state(state, event, ("planning", "ready"))
        next_state = _active_state(
            state,
            phase="planning",
            activity="waiting",
            wait_reason="plan_capability",
        )
    elif kind == "plan_ready":
        _require_state(state, event, ("planning", "waiting"))
        _require_wait_reason(state, event, "plan_capability")
        next_state = _active_state(state, phase="coding", activity="ready")
    elif kind == "code_started":
        _require_state(state, event, ("coding", "ready"))
        next_state = _active_state(state, phase="coding", activity="running")
    elif kind == "code_completed":
        _require_state(state, event, ("coding", "running"))
        next_state = _active_state(state, phase="pre_review", activity="ready")
    elif kind == "report_completed":
        _require_state(state, event, ("coding", "running"))
        next_state = _terminal_state(state, outcome="success")
    elif kind == "developer_no_progress":
        _require_state(state, event, ("coding", "running"))
        next_state = _active_state(state, phase="coding", activity="ready")
    elif kind == "review_requested":
        _require_state(state, event, ("pre_review", "ready"))
        next_state = _active_state(
            state,
            phase="pre_review",
            activity="waiting",
            wait_reason="code_review_capability",
        )
    elif kind == "review_approved":
        _require_state(state, event, ("pre_review", "waiting"))
        _require_wait_reason(state, event, "code_review_capability")
        next_state = _active_state(
            state,
            phase="frontend_review",
            activity="ready",
        )
    elif kind == "review_changes_requested":
        _require_state(state, event, ("pre_review", "waiting"))
        _require_wait_reason(state, event, "code_review_capability")
        next_state = _active_state(state, phase="planning", activity="ready")
    elif kind == "frontend_review_requested":
        _require_state(state, event, ("frontend_review", "ready"))
        next_state = _active_state(
            state,
            phase="frontend_review",
            activity="waiting",
            wait_reason="frontend_review",
        )
    elif kind in {"frontend_review_passed", "frontend_review_skipped"}:
        if kind == "frontend_review_passed":
            _require_state(state, event, ("frontend_review", "waiting"))
            _require_wait_reason(state, event, "frontend_review")
        else:
            _require_state(state, event, ("frontend_review", "ready"))
        next_state = _active_state(
            state,
            phase="publishing",
            activity="ready",
        )
    elif kind == "frontend_review_profile_passed":
        _require_state(state, event, ("frontend_review", "waiting"))
        _require_wait_reason(state, event, "frontend_review")
        next_state = _active_state(
            state,
            phase="frontend_review",
            activity="ready",
        )
    elif kind == "frontend_review_changes_requested":
        _require_state(state, event, ("frontend_review", "waiting"))
        _require_wait_reason(state, event, "frontend_review")
        next_state = _active_state(state, phase="planning", activity="ready")
    elif kind == "publish_started":
        _require_state(state, event, ("publishing", "ready"))
        next_state = _active_state(
            state,
            phase="publishing",
            activity="running",
        )
    elif kind == "pr_bound":
        _require_state(
            state,
            event,
            ("publishing", "ready"),
            ("publishing", "running"),
        )
        next_state = _active_state(
            state,
            phase="monitoring",
            activity="waiting",
            wait_reason="pr_monitor",
        )
    elif kind == "monitor_blocked":
        _require_state(state, event, ("monitoring", "waiting"))
        _require_wait_reason(state, event, "pr_monitor")
        next_state = _active_state(state, phase="planning", activity="ready")
    elif kind == "monitor_ready":
        _require_state(state, event, ("monitoring", "waiting"))
        _require_wait_reason(state, event, "pr_monitor")
        next_state = _terminal_state(state, outcome="success")
    elif kind == "monitor_refresh":
        _require_state(state, event, ("monitoring", "waiting"))
        _require_wait_reason(state, event, "pr_monitor")
        next_state = state
    else:
        raise DeliveryTransitionError(f"Unknown Delivery event {kind!r}")

    next_state = replace(next_state, state_version=state.state_version + 1)
    validate_delivery_state(next_state)
    return DeliveryReduction(
        state=next_state,
        effects=effects_for_state(next_state),
    )
