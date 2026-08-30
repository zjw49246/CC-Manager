"""Regression tests for PID-reuse-safe process identity probes.

A bare ``os.kill(pid, 0)`` probe cannot distinguish "this exact managed
process is still running" from "an unrelated process reused this PID number".
Treating the second case as the first pins a Task as permanently un-retryable
and leaks an Instance capacity slot, which is the bug these tests cover.

These tests never signal or spawn a real process: the live PID used
throughout is the test interpreter itself, whose identity is known-good.
"""

import os
from unittest.mock import patch

import pytest

from backend.services.process_identity import (
    ProcessIdentity,
    ProcessIdentityError,
    capture_process_identity,
    decode_process_identity,
    encode_process_identity,
    persisted_process_is_definitively_dead,
    read_process_identity,
)


LIVE_PID = os.getpid()
OTHER_BOOT_ID = "11111111-2222-3333-4444-555555555555"


def _live_identity() -> str:
    """Return the encoded identity of this test process."""

    identity = capture_process_identity(LIVE_PID)
    assert identity is not None, "test host must expose its own PID identity"
    return identity


def _fields(identity: str) -> tuple[int, str]:
    decoded = decode_process_identity(identity, LIVE_PID)
    assert decoded is not None
    return decoded


def test_capture_round_trips_through_decode():
    """A captured identity decodes back for the PID it was recorded against."""

    identity = _live_identity()
    start_ticks, boot_id = _fields(identity)

    assert start_ticks > 0
    assert boot_id
    assert identity == f"v1:{LIVE_PID}:{start_ticks}:{boot_id}"


def test_capture_never_raises_for_unreadable_pid():
    """Identity capture degrades to None instead of failing a launch."""

    for error in (ProcessIdentityError("boom"), RuntimeError("ctypes boom")):
        with patch(
            "backend.services.process_identity.read_process_identity",
            side_effect=error,
        ):
            assert capture_process_identity(LIVE_PID) is None


@pytest.mark.parametrize("pid", [None, 0, 1, -5, "123", 12.0, True])
def test_capture_rejects_unsafe_pids(pid):
    """Only a real, safe PID may produce identity evidence."""

    assert capture_process_identity(pid) is None


def test_exact_live_process_is_not_dead():
    """The recorded process really is running, so cleanup must fail closed."""

    assert persisted_process_is_definitively_dead(LIVE_PID, _live_identity()) is False


def test_reused_pid_is_definitively_dead():
    """Same PID number, different start time: the recorded process is gone.

    This is the PID wraparound case that previously pinned a Task as failed
    forever, because the bare probe only saw "something answers to this PID".
    """

    _, boot_id = _fields(_live_identity())
    reused = f"v1:{LIVE_PID}:1:{boot_id}"

    assert persisted_process_is_definitively_dead(LIVE_PID, reused) is True


def test_prior_boot_session_is_definitively_dead():
    """No process survives a reboot, even if its PID is live again now."""

    start_ticks, _ = _fields(_live_identity())
    previous_boot = f"v1:{LIVE_PID}:{start_ticks}:{OTHER_BOOT_ID}"

    assert persisted_process_is_definitively_dead(LIVE_PID, previous_boot) is True


def test_vanished_pid_is_definitively_dead():
    """A PID that no longer exists is provably gone."""

    _, boot_id = _fields(_live_identity())
    with patch(
        "backend.services.process_identity.read_process_identity",
        return_value=None,
    ):
        assert (
            persisted_process_is_definitively_dead(
                LIVE_PID, f"v1:{LIVE_PID}:999:{boot_id}"
            )
            is True
        )


def test_unreadable_current_identity_fails_closed():
    """If death cannot be proven, owner evidence must be preserved."""

    identity = _live_identity()
    with patch(
        "backend.services.process_identity.read_process_identity",
        side_effect=ProcessIdentityError("permission denied"),
    ):
        assert persisted_process_is_definitively_dead(LIVE_PID, identity) is False


def test_unexpected_identity_reader_failure_fails_closed():
    """Platform reader failures cannot authorize a destructive cleanup."""

    identity = _live_identity()
    with patch(
        "backend.services.process_identity.read_process_identity",
        side_effect=RuntimeError("ctypes boom"),
    ):
        assert persisted_process_is_definitively_dead(LIVE_PID, identity) is False


def test_legacy_row_without_identity_falls_back_to_bare_probe():
    """Rows written by an older binary keep the previous conservative result."""

    assert persisted_process_is_definitively_dead(LIVE_PID, None) is False
    assert persisted_process_is_definitively_dead(999999, None) is True


def test_identity_recorded_for_a_different_pid_is_unusable():
    """A stale identity must never be read as proof that a fresh PID is dead.

    The PID is embedded in the value precisely so that a writer which updates
    ``pid`` without refreshing the identity column degrades to the
    conservative probe, rather than authorizing duplicate execution.
    """

    start_ticks, boot_id = _fields(_live_identity())
    mismatched = f"v1:{LIVE_PID + 1}:{start_ticks}:{boot_id}"

    assert persisted_process_is_definitively_dead(LIVE_PID, mismatched) is False


@pytest.mark.parametrize(
    "identity",
    [
        "",
        "garbage",
        "v1:only:three",
        "v2:1:2:11111111-2222-3333-4444-555555555555",
        "v1:x:2:11111111-2222-3333-4444-555555555555",
        "v1:1:notanint:11111111-2222-3333-4444-555555555555",
        "v1:1:2:not-a-uuid",
        "v1:1:2:",
        b"v1:1:2:11111111-2222-3333-4444-555555555555",
        12345,
    ],
)
def test_malformed_identity_falls_back_to_bare_probe(identity):
    """Corrupt evidence is discarded rather than trusted in either direction."""

    assert persisted_process_is_definitively_dead(LIVE_PID, identity) is False


def test_zero_start_ticks_is_rejected_as_evidence():
    """A non-positive start time cannot distinguish two processes."""

    _, boot_id = _fields(_live_identity())

    assert decode_process_identity(f"v1:{LIVE_PID}:0:{boot_id}", LIVE_PID) is None


@pytest.mark.parametrize("pid", [None, 0, -1])
def test_missing_pid_is_never_reported_dead(pid):
    """Without a PID there is nothing to prove dead."""

    assert persisted_process_is_definitively_dead(pid, None) is False


def test_encode_binds_pid_into_value():
    """Encoding embeds the PID so mismatches are detectable."""

    encoded = encode_process_identity(
        ProcessIdentity(pid=4242, start_ticks=99, boot_id=OTHER_BOOT_ID)
    )

    assert encoded == f"v1:4242:99:{OTHER_BOOT_ID}"
    assert decode_process_identity(encoded, 4242) == (99, OTHER_BOOT_ID)
    assert decode_process_identity(encoded, 4243) is None


@pytest.mark.parametrize("pid", [0, 1, -1, "5", None])
def test_read_process_identity_rejects_unsafe_pids(pid):
    """The low-level reader refuses PIDs that could mean "every process"."""

    with pytest.raises(ProcessIdentityError):
        read_process_identity(pid)


def test_read_process_identity_returns_none_for_missing_process():
    """A vanished PID is reported as absent, not as an error."""

    assert read_process_identity(999999) is None


def test_read_process_identity_of_self_is_consistent():
    """Two reads of a live process agree, so the value is a stable identity."""

    first = read_process_identity(LIVE_PID)
    second = read_process_identity(LIVE_PID)

    assert first is not None and second is not None
    assert first.pid == second.pid == LIVE_PID
    assert first.start_ticks == second.start_ticks > 0
    assert first.boot_id == second.boot_id
