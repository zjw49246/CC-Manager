"""Tests for exact-turn source binding and terminal-tail selection."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
import json

import pytest
from sqlalchemy import func, select

from backend.models.log_entry import LogEntry
from backend.models.task import Task
from backend.services.terminal_arbitration import (
    TerminalOutputSelection,
    TurnSourceBindingError,
    bind_turn_source,
    classify_turn_scope,
    select_terminal_output,
    select_terminal_tail,
)


@pytest.mark.parametrize(
    ("event", "detached", "expected"),
    [
        ({}, False, "foreground"),
        ({"orphan": False, "autonomous": False}, False, "foreground"),
        ({"autonomous": True}, False, "autonomous"),
        ({}, True, "autonomous"),
        ({"orphan": True}, False, "orphan"),
        ({"orphan": True, "autonomous": True}, True, "orphan"),
    ],
)
def test_classify_turn_scope_has_fail_closed_precedence(
    event: dict,
    detached: bool,
    expected: str,
):
    assert classify_turn_scope(event, detached) == expected


def test_classify_turn_scope_rejects_non_mapping_events():
    with pytest.raises(TypeError, match="mapping"):
        classify_turn_scope(None)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_bind_turn_source_binds_wholly_unscoped_log_in_place(db_session):
    task = Task(
        title="source",
        description="test",
        retry_count=3,
        turn_generation=8,
    )
    db_session.add(task)
    await db_session.flush()
    original = LogEntry(
        instance_id=None,
        task_id=task.id,
        event_type="user_message",
        role="user",
        content="continue",
        raw_json='{"raw_content":"continue"}',
        is_error=False,
    )
    db_session.add(original)
    await db_session.flush()

    source = await bind_turn_source(
        db_session,
        task,
        original.id,
        instance_id=91,
        transport="codex_app_server",
    )

    assert source is original
    assert source.task_retry_count == 3
    assert source.task_turn_generation == 8
    assert source.turn_scope == "source"
    assert source.event_type == "user_message"
    assert source.raw_json == '{"raw_content":"continue"}'
    assert source.instance_id == 91
    assert task.turn_source_log_id == original.id

    before_count = await db_session.scalar(select(func.count()).select_from(LogEntry))
    repeated = await bind_turn_source(
        db_session,
        task,
        original.id,
        instance_id=92,
        transport="codex_exec",
    )
    after_count = await db_session.scalar(select(func.count()).select_from(LogEntry))
    assert repeated is original
    assert after_count == before_count
    assert repeated.instance_id == 92


@pytest.mark.asyncio
async def test_bind_turn_source_aliases_cross_generation_without_mutating_history(
    db_session,
):
    task = Task(
        title="next turn",
        description="test",
        retry_count=4,
        turn_generation=12,
    )
    db_session.add(task)
    await db_session.flush()
    historical = LogEntry(
        instance_id=7,
        task_id=task.id,
        task_retry_count=3,
        task_turn_generation=11,
        turn_scope="source",
        event_type="user_message",
        role="user",
        content="historical",
        raw_json="historical metadata",
        is_error=False,
    )
    db_session.add(historical)
    await db_session.flush()

    source = await bind_turn_source(
        db_session,
        task,
        historical.id,
        instance_id=8,
        transport="claude_pty",
    )

    assert source is not historical
    assert source.id is not None
    assert source.event_type == "turn_source"
    assert source.role == "system"
    assert source.content is None
    assert source.instance_id == 8
    assert source.task_id == task.id
    assert source.task_retry_count == 4
    assert source.task_turn_generation == 12
    assert source.turn_scope == "source"
    assert source.is_error is False
    assert json.loads(source.raw_json) == {
        "execution_principal": {
            "kind": "system",
            "mode": "sandbox",
            "role": "member",
            "user_id": None,
        },
        "original_source_log_id": historical.id,
        "transport": "claude_pty",
    }
    assert task.turn_source_log_id == source.id
    assert (
        historical.task_retry_count,
        historical.task_turn_generation,
        historical.turn_scope,
        historical.raw_json,
    ) == (3, 11, "source", "historical metadata")

    repeated = await bind_turn_source(
        db_session,
        task,
        historical.id,
        instance_id=99,
        transport="claude_cli",
    )
    count = await db_session.scalar(select(func.count()).select_from(LogEntry))
    assert repeated is source
    assert count == 2
    assert repeated.instance_id == 99
    # The first bind is immutable audit evidence; a planned transport change
    # does not silently rewrite it or authorize selector fallback.
    assert json.loads(repeated.raw_json)["transport"] == "claude_pty"


@pytest.mark.parametrize(
    ("retry_count", "turn_generation", "scope"),
    [
        (None, 5, None),
        (2, None, None),
        (None, None, "foreground"),
        (2, 5, "foreground"),
        (2, 5, None),
    ],
)
@pytest.mark.asyncio
async def test_bind_turn_source_aliases_partial_or_non_source_rows(
    db_session,
    retry_count,
    turn_generation,
    scope,
):
    task = Task(
        title="partial",
        description="test",
        retry_count=2,
        turn_generation=5,
    )
    db_session.add(task)
    await db_session.flush()
    original = LogEntry(
        task_id=task.id,
        task_retry_count=retry_count,
        task_turn_generation=turn_generation,
        turn_scope=scope,
        event_type="user_message",
        role="user",
        content="input",
        is_error=False,
    )
    db_session.add(original)
    await db_session.flush()

    source = await bind_turn_source(db_session, task, original.id)

    assert source.id != original.id
    assert source.event_type == "turn_source"
    assert source.turn_scope == "source"
    assert (source.task_retry_count, source.task_turn_generation) == (2, 5)
    assert (
        original.task_retry_count,
        original.task_turn_generation,
        original.turn_scope,
    ) == (retry_count, turn_generation, scope)


@pytest.mark.asyncio
async def test_bind_turn_source_none_creates_one_durable_hidden_source(db_session):
    task = Task(
        title="initial",
        description="test",
        retry_count=0,
        turn_generation=1,
    )
    db_session.add(task)

    source = await bind_turn_source(
        db_session,
        task,
        None,
        instance_id=4,
        transport="codex_exec",
    )

    assert task.id is not None
    assert source.id is not None
    assert task.turn_source_log_id == source.id
    assert source.event_type == "turn_source"
    assert source.turn_scope == "source"
    assert json.loads(source.raw_json) == {
        "execution_principal": {
            "kind": "system",
            "mode": "sandbox",
            "role": "member",
            "user_id": None,
        },
        "original_source_log_id": None,
        "transport": "codex_exec",
    }

    repeated = await bind_turn_source(
        db_session,
        task,
        None,
        instance_id=5,
        transport="codex_app_server",
    )
    count = await db_session.scalar(select(func.count()).select_from(LogEntry))
    assert repeated.id == source.id
    assert count == 1


@pytest.mark.asyncio
async def test_bind_turn_source_rejects_missing_or_cross_task_logs(db_session):
    first = Task(title="first", description="test", retry_count=0, turn_generation=1)
    second = Task(title="second", description="test", retry_count=0, turn_generation=1)
    db_session.add_all([first, second])
    await db_session.flush()
    foreign = LogEntry(
        task_id=second.id,
        event_type="user_message",
        role="user",
        content="foreign",
        is_error=False,
    )
    db_session.add(foreign)
    await db_session.flush()

    with pytest.raises(TurnSourceBindingError, match="different task"):
        await bind_turn_source(db_session, first, foreign.id)
    with pytest.raises(TurnSourceBindingError, match="does not exist"):
        await bind_turn_source(db_session, first, foreign.id + 10_000)
    with pytest.raises(TurnSourceBindingError, match="positive integer"):
        await bind_turn_source(db_session, first, True)  # type: ignore[arg-type]
    assert first.turn_source_log_id is None
    assert foreign.turn_scope is None


@pytest.mark.parametrize(
    ("event_type", "role", "is_error"),
    [
        ("message", "assistant", False),
        ("tool_result", "tool", True),
        ("user_message", "assistant", False),
        ("user_message", "user", True),
    ],
)
@pytest.mark.asyncio
async def test_bind_turn_source_rejects_non_user_source_rows(
    db_session,
    event_type,
    role,
    is_error,
):
    task = Task(
        title="source shape",
        description="test",
        retry_count=0,
        turn_generation=1,
    )
    db_session.add(task)
    await db_session.flush()
    impostor = LogEntry(
        task_id=task.id,
        event_type=event_type,
        role=role,
        content="not a user request",
        is_error=is_error,
    )
    db_session.add(impostor)
    await db_session.flush()

    with pytest.raises(TurnSourceBindingError, match="successful user_message"):
        await bind_turn_source(db_session, task, impostor.id)

    assert task.turn_source_log_id is None
    assert impostor.turn_scope is None


@pytest.mark.asyncio
async def test_bind_turn_source_refuses_to_rebind_one_generation(db_session):
    task = Task(title="single", description="test", retry_count=1, turn_generation=2)
    db_session.add(task)
    await db_session.flush()
    one = LogEntry(
        task_id=task.id,
        event_type="user_message",
        role="user",
        is_error=False,
    )
    two = LogEntry(
        task_id=task.id,
        event_type="user_message",
        role="user",
        is_error=False,
    )
    db_session.add_all([one, two])
    await db_session.flush()
    await bind_turn_source(db_session, task, one.id)

    with pytest.raises(TurnSourceBindingError, match="already bound"):
        await bind_turn_source(db_session, task, two.id)

    assert task.turn_source_log_id == one.id
    assert two.turn_scope is None


@pytest.mark.asyncio
async def test_bind_turn_source_cannot_move_an_admitted_source_to_another_instance(
    db_session,
):
    task = Task(
        title="admitted source",
        description="test",
        retry_count=1,
        turn_generation=2,
    )
    db_session.add(task)
    source = await bind_turn_source(
        db_session,
        task,
        None,
        instance_id=4,
    )
    source.actual_transport = "claude_exec"
    await db_session.flush()

    with pytest.raises(TurnSourceBindingError, match="different instance"):
        await bind_turn_source(
            db_session,
            task,
            None,
            instance_id=5,
        )

    assert source.instance_id == 4


@pytest.mark.asyncio
async def test_bind_turn_source_rejects_detached_task_impostor(db_session):
    durable = Task(title="durable", description="test", retry_count=1, turn_generation=2)
    db_session.add(durable)
    await db_session.flush()
    original = LogEntry(
        task_id=durable.id,
        event_type="user_message",
        role="user",
        is_error=False,
    )
    db_session.add(original)
    await db_session.flush()
    impostor = Task(
        id=durable.id,
        title="impostor",
        description="test",
        retry_count=1,
        turn_generation=2,
    )

    with pytest.raises(TurnSourceBindingError, match="attached"):
        await bind_turn_source(db_session, impostor, original.id)

    assert original.turn_scope is None
    assert durable.turn_source_log_id is None


def _row(
    row_id: int,
    *,
    event_type: str = "message",
    role: str | None = "assistant",
    content: str | None = "output",
    raw: object = None,
    native_turn_id: str | None = None,
    instance_id: int | None = 4,
    task_id: int | None = 71,
    retry_count: int | None = 2,
    turn_generation: int | None = 9,
    scope: str | None = "foreground",
    is_error: bool = False,
) -> LogEntry:
    raw_json = (
        json.dumps(raw, separators=(",", ":"))
        if isinstance(raw, (dict, list))
        else raw
    )
    return LogEntry(
        id=row_id,
        instance_id=instance_id,
        task_id=task_id,
        task_retry_count=retry_count,
        task_turn_generation=turn_generation,
        native_turn_id=native_turn_id,
        turn_scope=scope,
        event_type=event_type,
        role=role,
        content=content,
        raw_json=raw_json,
        is_error=is_error,
    )


def _source(actual_transport: str | None = "claude_pty") -> LogEntry:
    source = _row(
        100,
        event_type="user_message",
        role="user",
        content="request",
        scope="source",
    )
    source.actual_transport = actual_transport
    return source


def _codex_message(
    row_id: int,
    native_id: str,
    text: str,
    **overrides,
) -> LogEntry:
    raw = {
        "type": "item.completed",
        "turn_id": native_id,
        "item": {"id": f"msg-{row_id}", "type": "agent_message", "text": text},
    }
    raw.update(overrides.pop("raw_overrides", {}))
    return _row(
        row_id,
        content=text,
        raw=raw,
        native_turn_id=native_id,
        **overrides,
    )


def _codex_terminal(
    row_id: int,
    native_id: str | None,
    *,
    explicit: bool = True,
    **raw_overrides,
) -> LogEntry:
    raw = {"type": "turn.completed"}
    if native_id is not None:
        raw["turn_id"] = native_id
    if explicit:
        raw.update(
            {
                "status": "completed",
                "success": True,
                "error": None,
            }
        )
    raw.update(raw_overrides)
    return _row(
        row_id,
        event_type="system_event",
        role=None,
        content="turn.completed",
        raw=raw,
        native_turn_id=native_id,
    )


def _codex_exec_message(row_id: int, text: str, **overrides) -> LogEntry:
    """Real ``codex exec --json`` agent-message shape (no turn id)."""

    return _row(
        row_id,
        content=text,
        raw={
            "type": "item.completed",
            "item": {
                "id": f"item-{row_id}",
                "type": "agent_message",
                "text": text,
            },
        },
        native_turn_id=None,
        **overrides,
    )


def _codex_exec_terminal(row_id: int, **raw_overrides) -> LogEntry:
    """Real ``codex exec --json`` terminal shape (no turn id/status flag)."""

    raw = {
        "type": "turn.completed",
        "usage": {
            "input_tokens": 10,
            "cached_input_tokens": 0,
            "output_tokens": 2,
        },
    }
    raw.update(raw_overrides)
    return _row(
        row_id,
        event_type="system_event",
        role=None,
        content="turn.completed",
        raw=raw,
        native_turn_id=None,
    )


def _turn_failure_marker(
    row_id: int,
    provider: str,
    *,
    reason: str = "process_exit_before_response",
    exit_code: int | None = 1,
) -> LogEntry:
    return _row(
        row_id,
        event_type="system_event",
        role="system",
        content="provider process failed before producing a response",
        raw={
            "type": "ccm.turn.failed",
            "version": 1,
            "provider": provider,
            "reason": reason,
            "exit_code": exit_code,
        },
        is_error=True,
    )


def test_terminal_selection_result_is_immutable():
    row = _row(101, event_type="result")
    result = TerminalOutputSelection(row, row, None)
    with pytest.raises(FrozenInstanceError):
        result.native_turn_id = "changed"  # type: ignore[misc]


@pytest.mark.parametrize(
    "candidate",
    [
        _row(101, event_type="result", task_id=72),
        _row(101, event_type="result", retry_count=3),
        _row(101, event_type="result", turn_generation=10),
        _row(101, event_type="result", scope="autonomous"),
        _row(101, event_type="result", scope="orphan"),
        _row(101, event_type="result", scope=None),
        _row(101, event_type="result", instance_id=5),
        _row(100, event_type="result"),
        _row(99, event_type="result"),
        _row(101, event_type="result", is_error=True),
        _row(101, event_type="result", retry_count=None),
        _row(101, event_type="result", turn_generation=None),
    ],
)
def test_selector_excludes_non_exact_or_non_foreground_rows(candidate):
    assert select_terminal_output("claude", _source(), [candidate], "claude_pty") is None


def test_selector_rejects_legacy_source_or_unknown_provider():
    source = _source()
    source.task_turn_generation = None
    assert select_terminal_output("claude", source, [], "claude_pty") is None
    assert select_terminal_output("other", _source(), []) is None


def test_selector_rejects_source_without_exact_instance_provenance():
    source = _source()
    source.instance_id = None
    result = _row(101, event_type="result")

    assert select_terminal_output("claude", source, [result]) is None


@pytest.mark.parametrize(
    "source",
    [
        _row(
            100,
            event_type="message",
            role="assistant",
            scope="source",
        ),
        _row(
            100,
            event_type="user_message",
            role="assistant",
            scope="source",
        ),
        _row(
            100,
            event_type="user_message",
            role="user",
            scope="source",
            is_error=True,
        ),
        _row(
            100,
            event_type="turn_source",
            role="system",
            content=None,
            raw={"original_source_log_id": 0},
            scope="source",
        ),
        _row(
            100,
            event_type="turn_source",
            role="system",
            content="not hidden",
            raw={"original_source_log_id": None},
            scope="source",
        ),
    ],
)
def test_selector_rejects_noncanonical_source_shapes(source):
    source.actual_transport = "claude_pty"
    result = _row(110, event_type="result", role=None, content="done")

    assert select_terminal_output("claude", source, [result]) is None


def test_selector_accepts_canonical_hidden_source_alias():
    source = _row(
        100,
        event_type="turn_source",
        role="system",
        content=None,
        raw={"original_source_log_id": None, "transport": "claude_exec"},
        scope="source",
    )
    source.actual_transport = "claude_exec"
    result = _row(110, event_type="result", role=None, content="done")

    selected = select_terminal_output("claude", source, [result])

    assert selected is not None
    assert selected.output_log is result


@pytest.mark.parametrize(
    ("original", "accepted"),
    [
        (None, False),
        (
            _row(
                90,
                event_type="user_message",
                role="user",
                task_id=72,
            ),
            False,
        ),
        (
            _row(
                90,
                event_type="result",
                role="assistant",
                task_id=71,
            ),
            False,
        ),
        (
            _row(
                90,
                event_type="user_message",
                role="user",
                task_id=71,
            ),
            True,
        ),
    ],
    ids=("missing", "foreign-task", "non-user", "valid-historical"),
)
def test_selector_requires_resolved_canonical_positive_alias_provenance(
    original,
    accepted,
):
    source = _row(
        100,
        event_type="turn_source",
        role="system",
        content=None,
        raw={"original_source_log_id": 90, "transport": "claude_exec"},
        scope="source",
    )
    source.actual_transport = "claude_exec"
    result = _row(110, event_type="result", role=None, content="done")

    selected = select_terminal_output(
        "claude",
        source,
        [result],
        source_original=original,
    )

    assert (selected is not None) is accepted


def test_claude_prefers_latest_result_over_later_assistant_message():
    source = _source()
    rows = [
        _row(130, event_type="message", content="later message"),
        _row(120, event_type="result", role=None, content="first result"),
        _row(125, event_type="result", role=None, content="final result"),
    ]

    selected = select_terminal_output("CLAUDE", source, rows, "claude_pty")

    assert selected is not None
    assert selected.output_log.id == 125
    assert selected.output_log.content == "final result"
    assert selected.terminal_log is selected.output_log
    assert selected.native_turn_id is None


def test_claude_later_failed_result_vetoes_older_success():
    rows = [
        _row(110, event_type="result", role=None, content="old success"),
        _row(
            120,
            event_type="result",
            role=None,
            content="later failure",
            is_error=True,
        ),
    ]

    assert select_terminal_output("claude", _source(), rows) is None


@pytest.mark.parametrize(
    ("transport", "successful_output"),
    [
        (
            "claude_exec",
            _row(110, event_type="result", role=None, content="old success"),
        ),
        (
            "claude_pty",
            _row(110, event_type="message", content="old PTY answer"),
        ),
    ],
)
def test_claude_structured_failure_marker_vetoes_older_success(
    transport,
    successful_output,
):
    marker = _turn_failure_marker(120, "claude")

    assert (
        select_terminal_output(
            "claude",
            _source(transport),
            [successful_output, marker],
            transport,
        )
        is None
    )


def test_claude_failed_result_blocks_pty_message_fallback():
    rows = [
        _row(110, event_type="message", content="assistant draft"),
        _row(
            120,
            event_type="result",
            role=None,
            content="terminal failure",
            is_error=True,
        ),
    ]

    assert (
        select_terminal_output("claude", _source(), rows, "claude_pty")
        is None
    )


@pytest.mark.parametrize(
    "later_terminal",
    [
        _row(
            120,
            event_type="message",
            role="assistant",
            content="API Error: invalid request",
            raw={"type": "assistant", "isApiErrorMessage": True},
            # The raw provider marker remains terminal even if a corrupted
            # relay lost its redundant is_error flag.
            is_error=False,
        ),
        _row(
            120,
            event_type="session_crashed",
            role=None,
            content="Process died (exit_code=1)",
            is_error=False,
        ),
        _row(
            120,
            event_type="rate_limit_event",
            role=None,
            content="rate_limit_event",
            raw={
                "type": "rate_limit_event",
                "rate_limit_info": {"status": "rejected"},
            },
            is_error=False,
        ),
        _row(
            120,
            event_type="system_event",
            role=None,
            content="api_error: turn aborted by API error",
            is_error=False,
        ),
    ],
)
def test_claude_fatal_after_success_result_vetoes_older_terminal(
    later_terminal,
):
    succeeded = _row(
        110,
        event_type="result",
        role=None,
        content="apparently complete",
        is_error=False,
    )

    assert (
        select_terminal_output(
            "claude",
            _source("claude_exec"),
            [succeeded, later_terminal],
        )
        is None
    )


def test_claude_later_success_result_recovers_after_earlier_fatal_attempt():
    failed = _row(
        110,
        event_type="session_crashed",
        role=None,
        content="Process died (exit_code=1)",
        is_error=True,
    )
    succeeded = _row(
        120,
        event_type="result",
        role=None,
        content="recovered",
        is_error=False,
    )

    selected = select_terminal_output(
        "claude",
        _source("claude_exec"),
        [succeeded, failed],
    )

    assert selected is not None
    assert selected.output_log is succeeded


@pytest.mark.parametrize(
    "fatal",
    [
        _row(
            120,
            event_type="message",
            role="assistant",
            content="API Error: invalid_request_error: unsupported beta",
            raw={"type": "assistant", "isApiErrorMessage": True},
            is_error=True,
        ),
        _row(
            120,
            event_type="session_crashed",
            role=None,
            content="Claude PTY session crashed",
            is_error=True,
        ),
        _row(
            120,
            event_type="rate_limit_event",
            role=None,
            content=None,
            raw={
                "type": "rate_limit_event",
                "rate_limit_info": {
                    "status": "rejected",
                    "rateLimitType": "five_hour",
                },
            },
            is_error=True,
        ),
        _row(
            120,
            event_type="message",
            role="assistant",
            content=(
                "usage limit reached — account hit its rate limit "
                "(detected in PTY session)"
            ),
            raw=None,
            is_error=True,
        ),
        _row(
            120,
            event_type="system_event",
            role=None,
            content=(
                "api_error: turn aborted by API error "
                "(no turn_duration sentinel follows)"
            ),
            is_error=True,
        ),
        _row(
            120,
            event_type="system_event",
            role=None,
            content="Response timed out after 1800s",
            is_error=True,
        ),
    ],
)
def test_claude_pty_fatal_event_after_assistant_vetoes_message_fallback(fatal):
    assistant = _row(110, event_type="message", content="apparently done")

    assert (
        select_terminal_output(
            "claude",
            _source(),
            [assistant, fatal],
            "claude_pty",
        )
        is None
    )


def test_claude_pty_tool_failure_is_not_a_turn_terminal():
    assistant = _row(110, event_type="message", content="final answer")
    ordinary_tool_error = _row(
        120,
        event_type="tool_result",
        role="tool",
        content=None,
        raw={
            "type": "user",
            "message": {
                "content": [
                    {
                        "type": "tool_result",
                        "is_error": True,
                        "content": "command failed",
                    }
                ]
            },
        },
        is_error=True,
    )

    selected = select_terminal_output(
        "claude",
        _source(),
        [ordinary_tool_error, assistant],
        "claude_pty",
    )

    assert selected is not None
    assert selected.output_log is assistant


@pytest.mark.parametrize(
    "raw",
    [
        None,
        {"type": "assistant", "isApiErrorMessage": False},
        {"type": "assistant", "isApiErrorMessage": "true"},
        {"type": "assistant"},
        {"type": "user", "isApiErrorMessage": True},
    ],
)
def test_claude_pty_non_api_assistant_error_does_not_veto_final_message(raw):
    final = _row(110, event_type="message", content="final answer")
    ordinary_error = _row(
        120,
        event_type="message",
        role="assistant",
        content="ordinary non-terminal error annotation",
        raw=raw,
        is_error=True,
    )

    selected = select_terminal_output(
        "claude",
        _source(),
        [final, ordinary_error],
        "claude_pty",
    )

    assert selected is not None
    assert selected.output_log is final


def test_claude_pty_soft_rate_limit_cannot_veto_even_if_error_flag_is_wrong():
    final = _row(110, event_type="message", content="final answer")
    soft_limit = _row(
        120,
        event_type="rate_limit_event",
        role=None,
        content=None,
        raw={
            "type": "rate_limit_event",
            "rate_limit_info": {
                "status": "allowed_warning",
                "rateLimitType": "seven_day",
            },
        },
        is_error=True,
    )

    selected = select_terminal_output(
        "claude",
        _source(),
        [final, soft_limit],
        "claude_pty",
    )

    assert selected is not None
    assert selected.output_log is final


def test_claude_pty_soft_quota_event_is_not_a_turn_terminal():
    assistant = _row(110, event_type="message", content="final answer")
    quota_ping = _row(
        120,
        event_type="rate_limit_event",
        role=None,
        content=None,
        raw={
            "type": "rate_limit_event",
            "rate_limit_info": {
                "status": "allowed_warning",
                "rateLimitType": "seven_day",
            },
        },
        is_error=False,
    )

    selected = select_terminal_output(
        "claude",
        _source(),
        [assistant, quota_ping],
        "claude_pty",
    )

    assert selected is not None
    assert selected.output_log is assistant


def test_claude_pty_stale_autonomous_fatal_event_cannot_veto_foreground():
    assistant = _row(110, event_type="message", content="final answer")
    stale_fatal = _row(
        120,
        event_type="session_crashed",
        role=None,
        content="old autonomous turn crashed",
        scope="autonomous",
        is_error=True,
    )

    selected = select_terminal_output(
        "claude",
        _source(),
        [assistant, stale_fatal],
        "claude_pty",
    )

    assert selected is not None
    assert selected.output_log is assistant


def test_claude_message_fallback_requires_durable_authoritative_pty_transport():
    source = _source(None)
    assistant = _row(110, event_type="message", content="pty final")
    source.raw_json = json.dumps({"transport": "claude_pty"})

    assert select_terminal_output("claude", source, [assistant], None) is None
    assert select_terminal_output("claude", source, [assistant], "claude_pty") is None

    source.actual_transport = "claude_pty"
    assert select_terminal_output("claude", source, [assistant], "claude_exec") is None
    selected = select_terminal_output("claude", source, [assistant], "claude_pty")
    assert selected is not None and selected.output_log is assistant

    user_message = _row(111, event_type="message", role="user")
    assert (
        select_terminal_output("claude", source, [user_message], "claude_pty")
        is None
    )


def test_durable_actual_transport_is_authoritative_and_conflicts_fail_closed():
    source = _source()
    source.actual_transport = "claude_pty"
    assistant = _row(110, event_type="message", content="pty final")

    selected = select_terminal_output("claude", source, [assistant])
    assert selected is not None
    assert selected.output_log is assistant
    assert (
        select_terminal_output(
            "claude",
            source,
            [assistant],
            "claude_exec",
        )
        is None
    )


def test_invalid_durable_actual_transport_fails_closed():
    source = _source()
    source.actual_transport = "CLAUDE-PTY"
    assistant = _row(110, event_type="message", content="pty final")

    assert (
        select_terminal_output(
            "claude",
            source,
            [assistant],
            "claude_pty",
        )
        is None
    )


def test_select_terminal_tail_alias_preserves_the_same_contract():
    result = _row(101, event_type="result", role=None, content="done")
    assert select_terminal_tail("claude", _source(), [result]).output_log is result


def test_codex_selects_last_agent_message_of_last_successful_native_turn():
    source = _source("codex_app_server")
    expected = _codex_message(118, "native-2", "final answer")
    terminal = _codex_terminal(120, "native-2")
    rows = [
        _codex_terminal(112, "native-1"),
        _row(
            117,
            event_type="thinking",
            raw={"type": "item.completed", "item": {"type": "reasoning"}},
            native_turn_id="native-2",
        ),
        _codex_message(111, "native-1", "old answer"),
        _row(
            119,
            event_type="message_delta",
            raw={"type": "item.agent_message.delta", "delta": "ignored"},
            native_turn_id="native-2",
        ),
        _codex_message(116, "native-2", "draft answer"),
        terminal,
        expected,
        _codex_message(119, "another-native", "wrong native"),
        _codex_message(119, "native-2", "autonomous", scope="autonomous"),
        _row(
            115,
            event_type="tool_result",
            role="tool",
            raw={
                "type": "item.completed",
                "item": {"type": "command_execution"},
            },
            native_turn_id="native-2",
        ),
    ]

    selected = select_terminal_output("codex", source, rows, "codex_app_server")

    assert selected is not None
    assert selected.output_log is expected
    assert selected.terminal_log is terminal
    assert selected.native_turn_id == "native-2"


def test_codex_latest_terminal_is_authoritative_even_without_a_matching_message():
    rows = [
        _codex_message(110, "native-1", "valid old answer"),
        _codex_terminal(111, "native-1"),
        _codex_terminal(120, "native-2"),
    ]
    assert (
        select_terminal_output("codex", _source("codex_app_server"), rows)
        is None
    )


def test_codex_never_accepts_agent_message_after_terminal():
    rows = [
        _codex_message(105, "native-1", "valid answer before terminal"),
        _codex_terminal(110, "native-1"),
        _codex_message(111, "native-1", "late"),
    ]
    assert (
        select_terminal_output("codex", _source("codex_app_server"), rows)
        is None
    )


def test_codex_malformed_agent_envelope_after_terminal_vetoes_older_success():
    malformed_late = _codex_message(130, "native-1", "canonical late text")
    malformed_late.content = "tampered persisted text"
    rows = [
        _codex_message(110, "native-1", "old successful answer"),
        _codex_terminal(120, "native-1"),
        malformed_late,
    ]

    assert (
        select_terminal_output(
            "codex",
            _source("codex_app_server"),
            rows,
        )
        is None
    )


@pytest.mark.parametrize(
    "new_attempt",
    [
        _row(
            130,
            event_type="system_event",
            role=None,
            content="thread.started",
            raw={"type": "thread.started", "thread_id": "new-attempt"},
            native_turn_id=None,
        ),
        _row(
            130,
            event_type="system_event",
            role=None,
            content="turn.started",
            raw={"type": "turn.started"},
            native_turn_id=None,
        ),
    ],
)
def test_codex_incomplete_attempt_after_success_vetoes_older_terminal(
    new_attempt,
):
    rows = [
        _codex_exec_message(110, "old successful answer"),
        _codex_exec_terminal(120),
        new_attempt,
    ]

    assert (
        select_terminal_output(
            "codex",
            _source("codex_exec"),
            rows,
        )
        is None
    )


@pytest.mark.parametrize(
    "terminal",
    [
        _codex_terminal(120, "native", explicit=False),
        _codex_terminal(
            120,
            "native",
            explicit=False,
            status="completed",
        ),
        _codex_terminal(
            120,
            "native",
            explicit=False,
            success=True,
        ),
    ],
)
def test_codex_app_server_terminal_requires_explicit_success(terminal):
    message = _codex_message(110, "native", "answer")
    assert (
        select_terminal_output(
            "codex",
            _source("codex_app_server"),
            [message, terminal],
            "codex_app_server",
        )
        is None
    )


@pytest.mark.parametrize(
    "missing_field",
    ["turn_id", "status", "success", "error"],
)
def test_codex_app_server_terminal_requires_complete_normalized_root(
    missing_field,
):
    raw = {
        "type": "turn.completed",
        "turn_id": "native",
        "status": "completed",
        "success": True,
        "error": None,
    }
    raw.pop(missing_field)
    terminal = _row(
        120,
        event_type="system_event",
        role=None,
        content="turn.completed",
        raw=raw,
        native_turn_id="native",
    )

    assert (
        select_terminal_output(
            "codex",
            _source("codex_app_server"),
            [_codex_message(110, "native", "must not be accepted"), terminal],
        )
        is None
    )


def test_codex_exec_keeps_official_legacy_success_terminal_compatibility():
    message = _codex_message(110, "native", "answer")
    terminal = _codex_terminal(120, "native", explicit=False)

    selected = select_terminal_output(
        "codex",
        _source("codex_exec"),
        [message, terminal],
        "codex_exec",
    )

    assert selected is not None
    assert selected.output_log is message
    assert selected.native_turn_id == "native"


def test_codex_exec_native_id_cannot_cross_an_earlier_terminal_boundary():
    stale = _codex_message(110, "reused-thread-like-id", "old answer")
    prior = _codex_terminal(115, "reused-thread-like-id", explicit=False)
    latest = _codex_terminal(130, "reused-thread-like-id", explicit=False)

    assert (
        select_terminal_output(
            "codex",
            _source("codex_exec"),
            [stale, latest, prior],
            "codex_exec",
        )
        is None
    )


def test_codex_exec_selects_last_real_idless_agent_message_in_boundary():
    source = _source("codex_exec")
    expected = _codex_exec_message(118, "final answer")
    terminal = _codex_exec_terminal(120)
    rows = [
        _codex_exec_message(110, "draft answer"),
        _row(
            117,
            event_type="thinking",
            role="assistant",
            content="reasoning",
            raw={
                "type": "item.completed",
                "item": {"id": "reason-1", "type": "reasoning", "text": "reasoning"},
            },
            native_turn_id=None,
        ),
        terminal,
        expected,
    ]

    selected = select_terminal_output(
        "codex",
        source,
        rows,
        "codex_exec",
    )

    assert selected is not None
    assert selected.output_log is expected
    assert selected.terminal_log is terminal
    assert selected.native_turn_id is None


@pytest.mark.parametrize(
    "boundary",
    [
        _codex_exec_terminal(115),
        _row(
            115,
            event_type="system_event",
            role=None,
            content="stream disconnected before completion",
            raw={
                "type": "turn.failed",
                "error": {"message": "stream disconnected before completion"},
            },
            native_turn_id=None,
            is_error=True,
        ),
        _row(
            115,
            event_type="system_event",
            role=None,
            content="thread.started",
            raw={"type": "thread.started", "thread_id": "new-attempt"},
            native_turn_id=None,
        ),
    ],
)
def test_codex_exec_idless_selection_never_crosses_attempt_boundary(boundary):
    stale = _codex_exec_message(110, "stale answer from an earlier attempt")
    latest = _codex_exec_terminal(130)

    assert (
        select_terminal_output(
            "codex",
            _source("codex_exec"),
            [stale, latest, boundary],
            "codex_exec",
        )
        is None
    )


def test_codex_exec_idless_selection_uses_message_after_failed_attempt():
    failed = _row(
        110,
        event_type="system_event",
        role=None,
        content="stream disconnected before completion",
        raw={
            "type": "turn.failed",
            "error": {"message": "stream disconnected before completion"},
        },
        native_turn_id=None,
        is_error=True,
    )
    expected = _codex_exec_message(120, "recovered answer")
    completed = _codex_exec_terminal(130)

    selected = select_terminal_output(
        "codex",
        _source("codex_exec"),
        [expected, completed, failed],
        "codex_exec",
    )

    assert selected is not None
    assert selected.output_log is expected
    assert selected.native_turn_id is None


def test_codex_exec_idless_selection_rejects_id_bearing_message():
    identified = _codex_message(110, "native-other-attempt", "wrong attempt")
    completed = _codex_exec_terminal(120)

    assert (
        select_terminal_output(
            "codex",
            _source("codex_exec"),
            [identified, completed],
            "codex_exec",
        )
        is None
    )


def test_codex_exec_idless_selection_rejects_tampered_message_content():
    tampered = _codex_exec_message(110, "canonical")
    tampered.content = "different persisted text"

    assert (
        select_terminal_output(
            "codex",
            _source("codex_exec"),
            [tampered, _codex_exec_terminal(120)],
            "codex_exec",
        )
        is None
    )


@pytest.mark.parametrize(
    "terminal",
    [
        _codex_exec_terminal(120, turn_id=""),
        _codex_exec_terminal(120, turn_id=0),
        _row(
            120,
            event_type="system_event",
            role=None,
            content="turn.completed",
            raw={"type": "turn.completed"},
            native_turn_id="",
        ),
    ],
)
def test_codex_exec_idless_selection_rejects_malformed_identity(terminal):
    assert (
        select_terminal_output(
            "codex",
            _source("codex_exec"),
            [_codex_exec_message(110, "answer"), terminal],
            "codex_exec",
        )
        is None
    )


def test_codex_idless_compatibility_requires_durable_exec_transport():
    source = _source(None)
    message = _codex_exec_message(110, "answer")
    completed = _codex_exec_terminal(120)

    assert select_terminal_output("codex", source, [message, completed]) is None
    assert (
        select_terminal_output(
            "codex",
            source,
            [message, completed],
            "codex_exec",
        )
        is None
    )
    assert (
        select_terminal_output(
            "codex",
            source,
            [message, completed],
            "codex_app_server",
        )
        is None
    )

    source.actual_transport = "codex_exec"
    selected = select_terminal_output("codex", source, [message, completed])
    assert selected is not None
    assert selected.output_log is message
    assert (
        select_terminal_output(
            "codex",
            source,
            [message, completed],
            "codex_app_server",
        )
        is None
    )


def test_codex_app_server_explicit_idless_success_still_requires_native_id():
    source = _source("codex_app_server")
    terminal = _codex_exec_terminal(
        120,
        status="completed",
        success=True,
        error=None,
    )

    assert (
        select_terminal_output(
            "codex",
            source,
            [_codex_exec_message(110, "answer"), terminal],
        )
        is None
    )


@pytest.mark.parametrize(
    "latest_terminal",
    [
        _codex_terminal(
            130,
            "native",
            status="failed",
            success=False,
            error={"message": "failed"},
        ),
        _codex_terminal(
            130,
            "native",
            status="interrupted",
            success=False,
            error={"message": "interrupted"},
        ),
        _codex_terminal(130, "native", explicit=False),
        _row(
            130,
            event_type="system_event",
            role=None,
            content="turn.completed",
            raw={
                "type": "turn.completed",
                "turn_id": "other-native",
                "status": "completed",
                "success": True,
                "error": None,
            },
            native_turn_id="native",
        ),
        _row(
            130,
            event_type="system_event",
            role=None,
            content="failed",
            raw={
                "type": "turn.failed",
                "turn_id": "native",
                "error": {"message": "failed"},
            },
            native_turn_id="native",
            is_error=True,
        ),
        _row(
            130,
            event_type="system_event",
            role=None,
            content="turn.completed",
            raw="not-json",
            native_turn_id="native",
        ),
        _row(
            130,
            event_type="system_event",
            role=None,
            content="provider error",
            raw="not-json",
            native_turn_id="native",
            is_error=True,
        ),
    ],
)
def test_codex_latest_failed_or_malformed_terminal_vetoes_older_success(
    latest_terminal,
):
    rows = [
        _codex_message(110, "native", "old answer"),
        _codex_terminal(120, "native"),
        latest_terminal,
    ]

    assert (
        select_terminal_output(
            "codex",
            _source("codex_app_server"),
            rows,
            "codex_app_server",
        )
        is None
    )


@pytest.mark.parametrize("transport", ["codex_app_server", "codex_exec"])
def test_codex_structured_failure_marker_vetoes_older_success(transport):
    if transport == "codex_app_server":
        message = _codex_message(110, "native", "old answer")
        terminal = _codex_terminal(120, "native")
    else:
        message = _codex_exec_message(110, "old answer")
        terminal = _codex_exec_terminal(120)
    marker = _turn_failure_marker(
        130,
        "codex",
        reason="output_consumer_failure",
        exit_code=None,
    )

    assert (
        select_terminal_output(
            "codex",
            _source(transport),
            [message, terminal, marker],
            transport,
        )
        is None
    )


def test_codex_latest_success_wins_after_an_earlier_failed_attempt():
    failed = _row(
        110,
        event_type="system_event",
        role=None,
        content="failed",
        raw={
            "type": "turn.failed",
            "turn_id": "failed-native",
            "error": {"message": "retryable failure"},
        },
        native_turn_id="failed-native",
        is_error=True,
    )
    message = _codex_message(120, "successful-native", "final answer")
    succeeded = _codex_terminal(130, "successful-native")

    selected = select_terminal_output(
        "codex",
        _source("codex_app_server"),
        [failed, message, succeeded],
        "codex_app_server",
    )

    assert selected is not None
    assert selected.output_log is message
    assert selected.native_turn_id == "successful-native"


def test_codex_latest_success_wins_after_earlier_corrupt_error_event():
    earlier_error = _row(
        110,
        event_type="system_event",
        role=None,
        content="temporary provider error",
        raw="not-json",
        native_turn_id="failed-native",
        is_error=True,
    )
    message = _codex_message(120, "successful-native", "final answer")
    succeeded = _codex_terminal(130, "successful-native")

    selected = select_terminal_output(
        "codex",
        _source("codex_app_server"),
        [earlier_error, message, succeeded],
        "codex_app_server",
    )

    assert selected is not None
    assert selected.output_log is message


@pytest.mark.parametrize(
    "terminal",
    [
        _codex_terminal(120, "native", status="failed"),
        _codex_terminal(120, "native", success=False),
        _codex_terminal(120, "native", success=0),
        _codex_terminal(120, "native", error={"message": "failed"}),
        _codex_terminal(120, "native", turn_id=""),
        _codex_terminal(120, "native", turn_id=0),
        _codex_terminal(120, "native", turnId="other-native"),
        _codex_terminal(
            120,
            "native",
            status="interrupted",
            success=False,
            error={"message": "interrupted"},
        ),
        _codex_terminal(120, ""),
        _row(
            120,
            event_type="system_event",
            role=None,
            content="turn.completed",
            raw={"type": "turn.completed", "turn_id": "raw-native"},
            native_turn_id="row-native",
        ),
        _row(
            120,
            event_type="system_event",
            role=None,
            content="turn.completed",
            raw={
                "type": "turn.completed",
                "turn_id": "native",
                "turn": "malformed",
                "status": "completed",
                "success": True,
                "error": None,
            },
            native_turn_id="native",
        ),
        _row(
            120,
            event_type="message",
            raw={"type": "turn.completed", "turn_id": "native"},
            native_turn_id="native",
        ),
    ],
)
def test_codex_rejects_failed_unidentified_or_inconsistent_terminal(terminal):
    message = _codex_message(110, "native", "answer")
    assert (
        select_terminal_output(
            "codex",
            _source("codex_app_server"),
            [message, terminal],
        )
        is None
    )


@pytest.mark.parametrize(
    "nested_or_root",
    [
        {
            "turn": {
                "id": "native",
                "status": "failed",
                "success": False,
                "error": {"message": "boom"},
            },
        },
        {
            "turn": {
                "id": "native",
                "status": "completed",
                "success": True,
                "error": {"message": "nested failure"},
            },
        },
        {
            "status": "failed",
            "turn": {
                "id": "native",
                "status": "completed",
                "success": True,
                "error": None,
            },
        },
        {
            "turn": {
                "id": "native",
                "status": "completed",
                "success": False,
                "error": None,
            },
        },
        {
            "error": None,
            "turn": {
                "id": "native",
                "status": "completed",
                "success": True,
                "error": {},
            },
        },
        {
            "turn": {
                "status": "completed",
                "success": True,
                "error": None,
            },
        },
        {
            "turn": {
                "id": "native",
            },
        },
    ],
)
def test_codex_terminal_requires_consistent_complete_root_and_nested_envelopes(
    nested_or_root,
):
    terminal = _codex_terminal(120, "native", **nested_or_root)

    assert (
        select_terminal_output(
            "codex",
            _source("codex_app_server"),
            [_codex_message(110, "native", "must not be accepted"), terminal],
        )
        is None
    )


def test_codex_rejects_whitespace_padded_native_identity():
    native_id = " native "
    message = _codex_message(110, native_id, "answer")
    terminal = _codex_terminal(120, native_id)

    assert (
        select_terminal_output(
            "codex",
            _source("codex_app_server"),
            [message, terminal],
        )
        is None
    )


@pytest.mark.parametrize(
    "message",
    [
        _row(110, raw="not-json", native_turn_id="native"),
        _row(
            110,
            raw={"type": "item.agent_message.delta", "delta": "answer"},
            native_turn_id="native",
        ),
        _row(
            110,
            raw={
                "type": "item.completed",
                "item": {"type": "command_execution", "text": "answer"},
            },
            native_turn_id="native",
        ),
        _row(
            110,
            event_type="tool_result",
            role="tool",
            raw={
                "type": "item.completed",
                "item": {"type": "agent_message", "text": "output"},
            },
            native_turn_id="native",
        ),
        _codex_message(110, "other-native", "answer"),
        _row(
            110,
            content="tampered",
            raw={
                "type": "item.completed",
                "turn_id": "native",
                "item": {"type": "agent_message", "text": "canonical"},
            },
            native_turn_id="native",
        ),
        _row(
            110,
            content="answer",
            raw={
                "type": "item.completed",
                "turn_id": "native",
                "turn": "malformed",
                "item": {
                    "id": "message",
                    "type": "agent_message",
                    "text": "answer",
                },
            },
            native_turn_id="native",
        ),
    ],
)
def test_codex_rejects_noncanonical_agent_output(message):
    terminal = _codex_terminal(120, "native")
    assert (
        select_terminal_output(
            "codex",
            _source("codex_app_server"),
            [message, terminal],
        )
        is None
    )
