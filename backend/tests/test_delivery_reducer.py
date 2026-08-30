"""Pure transition tests for the durable Delivery Loop state machine."""

from dataclasses import replace

import pytest

from backend.services.delivery_reducer import (
    DeliveryReducerError,
    DeliveryReducerEvent,
    DeliveryState,
    DeliveryStateVersionError,
    DeliveryTransitionError,
    effects_for_state,
    initial_delivery_state,
    reduce_delivery_state,
    validate_delivery_state,
)


def _reduce(state: DeliveryState, kind: str, **payload: object):
    return reduce_delivery_state(
        state,
        DeliveryReducerEvent(kind=kind, payload=payload),
        expected_version=state.state_version,
    )


def test_happy_path_reaches_exact_ready_to_merge_terminal():
    state = initial_delivery_state()
    assert effects_for_state(state) == ("request_plan",)

    expected = [
        ("plan_requested", "planning", "waiting", ("observe_plan",)),
        ("plan_ready", "coding", "ready", ("dispatch_code",)),
        ("code_started", "coding", "running", ()),
        ("code_completed", "pre_review", "ready", ("request_code_review",)),
        ("review_requested", "pre_review", "waiting", ("observe_code_review",)),
        (
            "review_approved",
            "frontend_review",
            "ready",
            ("request_frontend_review",),
        ),
        (
            "frontend_review_requested",
            "frontend_review",
            "waiting",
            ("observe_frontend_review",),
        ),
        (
            "frontend_review_passed",
            "publishing",
            "ready",
            ("ensure_pull_request",),
        ),
        ("publish_started", "publishing", "running", ()),
        ("pr_bound", "monitoring", "waiting", ("observe_pr_monitor",)),
        ("monitor_ready", "done", "terminal", ()),
    ]

    for version, (kind, phase, activity, effects) in enumerate(expected, start=2):
        reduction = _reduce(state, kind)
        state = reduction.state
        assert (state.phase, state.activity) == (phase, activity)
        assert state.state_version == version
        assert reduction.effects == effects

    assert state.outcome == "success"
    assert state.error_code is None


@pytest.mark.parametrize(
    ("blocking_event", "phase"),
    [
        ("review_changes_requested", "pre_review"),
        ("frontend_review_changes_requested", "frontend_review"),
        ("monitor_blocked", "monitoring"),
    ],
)
def test_blocking_evidence_starts_a_fresh_plan_cycle(blocking_event, phase):
    wait_reason = {
        "pre_review": "code_review_capability",
        "frontend_review": "frontend_review",
        "monitoring": "pr_monitor",
    }[phase]
    state = DeliveryState(
        phase=phase,
        activity="waiting",
        wait_reason=wait_reason,
        state_version=8,
    )

    reduction = _reduce(state, blocking_event)

    assert reduction.state == DeliveryState(
        phase="planning",
        activity="ready",
        state_version=9,
    )
    assert reduction.effects == ("request_plan",)


def test_developer_no_progress_retries_development():
    state = DeliveryState(
        phase="coding",
        activity="running",
        state_version=8,
    )

    reduction = _reduce(state, "developer_no_progress")

    assert reduction.state == DeliveryState(
        phase="coding",
        activity="ready",
        state_version=9,
    )
    assert reduction.effects == ("dispatch_code",)


def test_report_completion_ends_delivery_successfully():
    state = DeliveryState(
        phase="coding",
        activity="running",
        state_version=8,
    )

    reduction = _reduce(state, "report_completed")

    assert reduction.state == DeliveryState(
        phase="done",
        activity="terminal",
        outcome="success",
        state_version=9,
    )
    assert reduction.effects == ()


def test_frontend_review_can_be_explicitly_skipped_before_start():
    state = DeliveryState(phase="frontend_review", activity="ready")

    reduction = _reduce(state, "frontend_review_skipped")

    assert (reduction.state.phase, reduction.state.activity) == (
        "publishing",
        "ready",
    )
    assert reduction.effects == ("ensure_pull_request",)


@pytest.mark.parametrize(
    ("phase", "wait_reason", "effect"),
    [
        ("planning", "plan_capability", "observe_plan"),
        ("pre_review", "code_review_capability", "observe_code_review"),
        ("frontend_review", "frontend_review", "observe_frontend_review"),
        ("monitoring", "pr_monitor", "observe_pr_monitor"),
    ],
)
def test_pause_and_resume_restores_phase_observer(phase, wait_reason, effect):
    waiting = DeliveryState(
        phase=phase,
        activity="waiting",
        wait_reason=wait_reason,
    )
    paused = _reduce(waiting, "pause", reason="operator requested").state

    assert paused.activity == "paused"
    assert paused.paused_from_activity == "waiting"
    assert paused.pause_reason == "operator requested"
    assert paused.wait_reason is None

    resumed = _reduce(paused, "resume")
    assert resumed.state.activity == "waiting"
    assert resumed.state.wait_reason == wait_reason
    assert resumed.effects == (effect,)


@pytest.mark.parametrize("phase", ["coding", "publishing"])
def test_resume_from_running_preserves_exact_active_effect_fence(phase):
    running = DeliveryState(phase=phase, activity="running")
    paused = _reduce(running, "pause", reason="maintenance").state

    resumed = _reduce(paused, "resume")

    assert resumed.state.activity == "running"
    assert resumed.effects == ()


def test_resume_rejects_waiting_phase_without_an_observer():
    paused = DeliveryState(
        phase="coding",
        activity="paused",
        paused_from_activity="ready",
        pause_reason="hold",
    )

    with pytest.raises(DeliveryTransitionError, match="no resumable waiting"):
        _reduce(paused, "resume", activity="waiting")


@pytest.mark.parametrize(
    ("kind", "outcome"),
    [("cancel", "cancelled"), ("supersede", "superseded")],
)
def test_terminal_commands_clear_wait_and_are_immutable(kind, outcome):
    waiting = DeliveryState(
        phase="monitoring",
        activity="waiting",
        wait_reason="pr_monitor",
    )
    terminal = _reduce(waiting, kind).state

    assert terminal.phase == "done"
    assert terminal.activity == "terminal"
    assert terminal.outcome == outcome
    assert terminal.wait_reason is None
    with pytest.raises(DeliveryTransitionError, match="immutable"):
        _reduce(terminal, "monitor_refresh")


def test_fail_records_bounded_failure_metadata():
    result = _reduce(
        initial_delivery_state(),
        "fail",
        error_code="workspace_invalid",
        error_message="worktree does not match the delivery branch",
    ).state

    assert result.outcome == "failed"
    assert result.error_code == "workspace_invalid"
    assert "worktree" in (result.error_message or "")


@pytest.mark.parametrize(
    ("phase", "activity", "effect"),
    [
        ("planning", "ready", "request_plan"),
        ("coding", "ready", "dispatch_code"),
        ("pre_review", "ready", "request_code_review"),
        ("frontend_review", "ready", "request_frontend_review"),
        ("publishing", "ready", "ensure_pull_request"),
        ("monitoring", "waiting", "observe_pr_monitor"),
    ],
)
def test_failed_terminal_run_retries_exact_failed_stage(phase, activity, effect):
    failed = _reduce(
        DeliveryState(
            phase="planning",
            activity="waiting",
            wait_reason="plan_capability",
            state_version=7,
        ),
        "fail",
        error_code="plan_run_failed",
        error_message="Both reviewer routes were temporarily unavailable",
    ).state

    retried = _reduce(failed, "retry", phase=phase, activity=activity)

    assert (retried.state.phase, retried.state.activity) == (phase, activity)
    assert retried.state.wait_reason == (
        "pr_monitor" if phase == "monitoring" else None
    )
    assert retried.state.state_version == 9
    assert retried.effects == (effect,)

    successful = replace(failed, outcome="success", error_code=None, error_message=None)
    with pytest.raises(DeliveryTransitionError, match="Only a failed"):
        _reduce(successful, "retry", phase=phase, activity=activity)


def test_retry_rejects_an_unbounded_target_state():
    failed = DeliveryState(
        phase="done",
        activity="terminal",
        outcome="failed",
    )

    with pytest.raises(DeliveryTransitionError, match="exact supported"):
        _reduce(failed, "retry", phase="coding", activity="running")


def test_monitor_refresh_advances_version_without_changing_wait_subject():
    state = DeliveryState(
        phase="monitoring",
        activity="waiting",
        wait_reason="pr_monitor",
        state_version=12,
    )

    refreshed = _reduce(state, "monitor_refresh")

    assert refreshed.state == replace(state, state_version=13)
    assert refreshed.effects == ("observe_pr_monitor",)


def test_exact_wait_reason_is_required_for_completion_events():
    external_wait = DeliveryState(
        phase="planning",
        activity="waiting",
        wait_reason="external_evidence",
    )

    with pytest.raises(DeliveryTransitionError, match="plan_capability"):
        _reduce(external_wait, "plan_ready")


def test_stale_version_is_rejected_before_transition():
    state = initial_delivery_state()
    with pytest.raises(DeliveryStateVersionError, match="expected 99"):
        reduce_delivery_state(
            state,
            DeliveryReducerEvent("plan_requested"),
            expected_version=99,
        )


@pytest.mark.parametrize(
    "state",
    [
        DeliveryState(phase="unknown", activity="ready"),
        DeliveryState(phase="planning", activity="unknown"),
        DeliveryState(phase="planning", activity="ready", state_version=True),
        DeliveryState(phase="planning", activity="ready", state_version=1.5),
        DeliveryState(phase="done", activity="terminal", outcome=None),
        DeliveryState(
            phase="done", activity="terminal", outcome="success", error_code="bad"
        ),
        DeliveryState(phase="planning", activity="waiting", wait_reason=None),
        DeliveryState(phase="planning", activity="ready", wait_reason="stale"),
        DeliveryState(
            phase="planning",
            activity="paused",
            paused_from_activity=None,
            pause_reason="hold",
        ),
    ],
)
def test_invalid_state_shapes_fail_closed(state):
    with pytest.raises(DeliveryReducerError):
        validate_delivery_state(state)


def test_unknown_or_out_of_order_event_is_rejected():
    with pytest.raises(DeliveryTransitionError, match="Unknown Delivery event"):
        _reduce(initial_delivery_state(), "invented")

    with pytest.raises(DeliveryTransitionError, match="requires coding/running"):
        _reduce(initial_delivery_state(), "code_completed")


def test_pause_requires_a_non_empty_reason():
    with pytest.raises(DeliveryTransitionError, match="reason"):
        _reduce(initial_delivery_state(), "pause", reason="  ")
