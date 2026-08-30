"""Durable source binding and pure terminal-output arbitration.

This module deliberately stops before interpreting a provider's text.  It
selects the one durable output row that is allowed to be interpreted by a
later controller, but it does not parse terminal markers, create capability
invocations, or change Task status.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
import json
from typing import Any, Literal, TypeAlias

from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.log_entry import LogEntry
from backend.models.task import Task


TurnScope: TypeAlias = Literal["source", "foreground", "autonomous", "orphan"]

_ACTUAL_TRANSPORTS = frozenset(
    {"claude_pty", "claude_exec", "codex_app_server", "codex_exec"}
)
_CLAUDE_PTY_HARD_RATE_LIMIT = (
    "usage limit reached — account hit its rate limit "
    "(detected in PTY session)"
)
_TURN_FAILURE_EVENT_TYPE = "ccm.turn.failed"
_TURN_FAILURE_REASONS = frozenset(
    {"process_exit_before_response", "output_consumer_failure"}
)


class TurnSourceBindingError(ValueError):
    """A requested source row cannot safely identify the Task's current turn."""


@dataclass(frozen=True, slots=True)
class TerminalOutputSelection:
    """The exact durable provider output selected for later interpretation."""

    output_log: LogEntry
    # Exact provider terminal boundary for the selected output. Claude result
    # rows (and its PTY message fallback) are their own boundary; Codex keeps
    # the final agent message and turn.completed envelope separate.
    terminal_log: LogEntry
    native_turn_id: str | None


# A descriptive alias for callers that name the operation after its algorithm.
TerminalTailSelection = TerminalOutputSelection


def classify_turn_scope(
    event: Mapping[str, Any],
    detached_autonomous: bool = False,
) -> Literal["foreground", "autonomous", "orphan"]:
    """Classify a persisted provider event for terminal arbitration.

    Orphan evidence always wins.  In particular, a replay marked both orphan
    and autonomous must never be promoted merely because it arrived through a
    detached autonomous callback.
    """

    if not isinstance(event, Mapping):
        raise TypeError("event must be a mapping")
    if event.get("orphan"):
        return "orphan"
    if detached_autonomous or event.get("autonomous"):
        return "autonomous"
    return "foreground"


def _strict_positive_int(value: object) -> bool:
    return type(value) is int and value > 0


def _strict_generation(value: object) -> bool:
    return type(value) is int and value >= 0


def _raw_mapping(row: LogEntry) -> dict[str, Any] | None:
    raw = row.raw_json
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str) or not raw:
        return None
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError, RecursionError):
        return None
    return parsed if isinstance(parsed, dict) else None


def source_alias_original_log_id(source: LogEntry) -> int | None:
    """Return a hidden source's positive provenance id, never a guess.

    ``None`` is also the canonical value for a synthetic initial-turn source.
    Callers that need to distinguish that case from malformed metadata must
    additionally use :func:`source_shape_is_canonical`.
    """

    if source.event_type != "turn_source":
        return None
    raw = _raw_mapping(source)
    if raw is None or "original_source_log_id" not in raw:
        return None
    original_id = raw["original_source_log_id"]
    return original_id if _strict_positive_int(original_id) else None


def source_shape_is_canonical(
    source: LogEntry,
    original_source: LogEntry | None = None,
) -> bool:
    """Validate a visible source or a hidden alias and its provenance.

    A positive ``original_source_log_id`` is not self-authenticating.  The
    referenced row must be supplied and must resolve to a successful user
    message owned by the same Task.  This keeps the pure selector fail closed;
    its database loader is responsible for fetching that one historical row.
    A hidden initial-turn source with an explicit JSON ``null`` needs no row.
    """

    if source.is_error is not False:
        return False
    if source.event_type == "user_message":
        return source.role == "user"
    if (
        source.event_type != "turn_source"
        or source.role != "system"
        or source.content is not None
    ):
        return False
    raw = _raw_mapping(source)
    if raw is None or "original_source_log_id" not in raw:
        return False
    original_id = raw["original_source_log_id"]
    if original_id is None:
        return original_source is None
    if not _strict_positive_int(original_id) or original_source is None:
        return False
    return bool(
        original_source.id == original_id
        and original_source.task_id == source.task_id
        and original_source.event_type == "user_message"
        and original_source.role == "user"
        and original_source.is_error is False
    )


def _same_exact_turn(row: LogEntry, task: Task) -> bool:
    return (
        row.task_id == task.id
        and row.task_retry_count == task.retry_count
        and row.task_turn_generation == task.turn_generation
        and _strict_generation(row.task_retry_count)
        and _strict_generation(row.task_turn_generation)
    )


def _is_current_source(row: LogEntry | None, task: Task) -> bool:
    return bool(
        row is not None
        and _strict_positive_int(row.id)
        and row.turn_scope == "source"
        and _same_exact_turn(row, task)
    )


def _source_matches_request(
    row: LogEntry,
    source_log_id: int | None,
) -> bool:
    if source_log_id is not None and row.id == source_log_id:
        return True
    if row.event_type != "turn_source":
        return False
    raw = _raw_mapping(row)
    if raw is None or "original_source_log_id" not in raw:
        return False
    original = raw["original_source_log_id"]
    if original is None:
        return source_log_id is None
    return (
        _strict_positive_int(original)
        and source_log_id is not None
        and original == source_log_id
    )


def _alias_payload(
    *,
    original_source_log_id: int | None,
    transport: str | None,
    execution_principal: Mapping[str, object],
) -> str:
    return json.dumps(
        {
            "original_source_log_id": original_source_log_id,
            "transport": transport,
            "execution_principal": dict(execution_principal),
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


async def bind_turn_source(
    db: AsyncSession,
    task: Task,
    source_log_id: int | None,
    instance_id: int | None = None,
    transport: str | None = None,
) -> LogEntry:
    """Bind one immutable source row to ``task``'s current logical turn.

    A completely unscoped source can be bound in place.  Any row carrying
    prior, partial, or non-source identity is left untouched and represented by
    a hidden ``turn_source`` alias.  Repeating a recovery call for the same
    source and exact generation reuses the existing source/alias.

    The caller owns the surrounding transaction.  This function flushes so
    the returned row and ``Task.turn_source_log_id`` always have usable ids.
    """

    if source_log_id is not None and not _strict_positive_int(source_log_id):
        raise TurnSourceBindingError("source_log_id must be a positive integer")
    if instance_id is not None and not _strict_positive_int(instance_id):
        raise TurnSourceBindingError("instance_id must be a positive integer")
    if transport is not None and not isinstance(transport, str):
        raise TurnSourceBindingError("transport must be a string or None")

    # Materialize SQLAlchemy defaults and, for a just-added Task, its id before
    # validating source ownership or constructing a LogEntry alias.
    await db.flush()
    if not _strict_positive_int(task.id):
        raise TurnSourceBindingError("task must have a durable positive id")
    durable_task = await db.get(Task, task.id)
    if durable_task is not task:
        raise TurnSourceBindingError(
            "task must be the durable row attached to this database session"
        )
    if not _strict_generation(task.retry_count):
        raise TurnSourceBindingError("task retry_count must be an exact generation")
    if not _strict_generation(task.turn_generation):
        raise TurnSourceBindingError(
            "task turn_generation must be an exact generation"
        )
    from backend.services.task_creation import task_execution_principal_values

    try:
        canonical_principal = task_execution_principal_values(
            user_id=task.execution_user_id,
            role=task.execution_user_role,
            principal_kind=task.execution_principal_kind,
        )
    except ValueError as exc:
        raise TurnSourceBindingError(
            "task execution principal is invalid"
        ) from exc
    if canonical_principal["execution_mode"] != task.execution_mode:
        raise TurnSourceBindingError(
            "task execution principal mode is inconsistent"
        )
    execution_principal = {
        "user_id": canonical_principal["execution_user_id"],
        "role": canonical_principal["execution_user_role"],
        "mode": canonical_principal["execution_mode"],
        "kind": canonical_principal["execution_principal_kind"],
    }

    original: LogEntry | None = None
    if source_log_id is not None:
        original = await db.get(LogEntry, source_log_id)
        if original is None:
            raise TurnSourceBindingError("source log does not exist")
        if original.task_id != task.id:
            raise TurnSourceBindingError("source log belongs to a different task")
        if (
            original.event_type != "user_message"
            or original.role != "user"
            or original.is_error is not False
        ):
            raise TurnSourceBindingError(
                "source log must be a successful user_message from the user"
            )

    current: LogEntry | None = None
    current_id = task.turn_source_log_id
    if current_id is not None:
        if not _strict_positive_int(current_id):
            raise TurnSourceBindingError("task has an invalid source pointer")
        current = await db.get(LogEntry, current_id)
        if current is None:
            raise TurnSourceBindingError("task source pointer does not exist")
        if current.task_id != task.id:
            raise TurnSourceBindingError(
                "task source pointer belongs to a different task"
            )

    if _is_current_source(current, task):
        assert current is not None
        if _source_matches_request(current, source_log_id):
            if instance_id is not None:
                # The source may be prepared before a reusable Instance is
                # selected (chat historically persisted ``1`` as a
                # placeholder).  Until the provider boundary is crossed its
                # execution owner may be refined by an exact prelaunch
                # requeue.  Afterwards the owner is immutable: accepting a
                # different Instance would let its output borrow this turn.
                if (
                    current.actual_transport is not None
                    and current.instance_id != instance_id
                ):
                    raise TurnSourceBindingError(
                        "admitted logical turn belongs to a different instance"
                    )
                current.instance_id = instance_id
                await db.flush()
            return current
        raise TurnSourceBindingError(
            "logical turn is already bound to a different source"
        )

    # Only a wholly identity-free row may be modified in place.  Treat every
    # partial shape and every pre-classified row as immutable historical
    # evidence, even if its non-NULL generation happens to match this Task.
    if original is not None and (
        original.task_retry_count is None
        and original.task_turn_generation is None
        and original.turn_scope is None
    ):
        original.task_retry_count = task.retry_count
        original.task_turn_generation = task.turn_generation
        original.turn_scope = "source"
        if instance_id is not None:
            original.instance_id = instance_id
        source = original
    elif original is not None and _is_current_source(original, task):
        source = original
    else:
        source = LogEntry(
            # Never inherit an earlier generation's Instance.  An omitted
            # current owner is unknown provenance and must remain unusable by
            # terminal arbitration until a real admission binds it.
            instance_id=instance_id,
            task_id=task.id,
            task_retry_count=task.retry_count,
            task_turn_generation=task.turn_generation,
            turn_scope="source",
            event_type="turn_source",
            role="system",
            content=None,
            raw_json=_alias_payload(
                original_source_log_id=source_log_id,
                transport=transport,
                execution_principal=execution_principal,
            ),
            is_error=False,
        )
        db.add(source)

    if instance_id is not None:
        if (
            source.actual_transport is not None
            and source.instance_id != instance_id
        ):
            raise TurnSourceBindingError(
                "admitted logical turn belongs to a different instance"
            )
        source.instance_id = instance_id

    # Flush the source before publishing its pointer.  This ordering is also
    # safe if a future migration adds a real FK for deployments that do not
    # mirror node-local log ids.
    await db.flush()
    if not _strict_positive_int(source.id):
        raise TurnSourceBindingError("source log did not receive a durable id")
    if not _is_current_source(source, task):
        raise TurnSourceBindingError("source log lacks exact current-turn identity")

    task.turn_source_log_id = source.id
    await db.flush()
    if task.turn_source_log_id != source.id:
        raise TurnSourceBindingError("task source pointer was not persisted")
    return source


def _candidate_is_in_scope(source: LogEntry, row: LogEntry) -> bool:
    return (
        _strict_positive_int(source.id)
        and _strict_positive_int(row.id)
        and _strict_positive_int(source.instance_id)
        and _strict_positive_int(row.instance_id)
        and row.instance_id == source.instance_id
        and source.turn_scope == "source"
        and row.turn_scope == "foreground"
        and _strict_positive_int(source.task_id)
        and row.task_id == source.task_id
        and _strict_generation(source.task_retry_count)
        and _strict_generation(source.task_turn_generation)
        and row.task_retry_count == source.task_retry_count
        and row.task_turn_generation == source.task_turn_generation
        and row.id > source.id
    )


def _canonical_transport(value: object) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    return value.strip().lower().replace("-", "_")


def _effective_transport(
    source: LogEntry,
    supplied: str | None,
) -> tuple[bool, str | None]:
    """Resolve runtime route evidence without letting a caller override it.

    ``actual_transport`` is written on the immutable source row at the final
    pre-provider boundary.  The argument is only a consistency assertion: it
    can never upgrade a legacy/planned route into execution evidence.
    """

    durable_raw = getattr(source, "actual_transport", None)
    if durable_raw not in _ACTUAL_TRANSPORTS:
        return False, None
    durable = durable_raw
    if supplied is not None and _canonical_transport(supplied) != durable:
        return False, None
    return True, durable


def _claude_hard_rate_limit(row: LogEntry, raw: dict[str, Any] | None) -> bool:
    """Recognize only the structured quota shapes that abort a PTY turn."""

    if (
        row.event_type != "rate_limit_event"
        or raw is None
        or raw.get("type") != "rate_limit_event"
    ):
        return False
    if raw.get("error") == "rate_limit":
        return True
    info = raw.get("rate_limit_info")
    if not isinstance(info, dict):
        return True
    if bool(info.get("hard_limit")):
        return True
    status = str(info.get("status") or "").lower()
    return status not in {"allowed", "allowed_warning"}


def _structured_turn_failure(
    row: LogEntry,
    raw: dict[str, Any] | None,
    *,
    provider: str,
) -> bool:
    """Recognize only CCM's exact durable failed-turn marker."""

    if (
        row.event_type != "system_event"
        or row.role != "system"
        or row.is_error is not True
        or not isinstance(row.content, str)
        or not row.content
        or raw is None
        or raw.get("type") != _TURN_FAILURE_EVENT_TYPE
        or raw.get("version") != 1
        or raw.get("provider") != provider
        or raw.get("reason") not in _TURN_FAILURE_REASONS
    ):
        return False
    exit_code = raw.get("exit_code")
    return exit_code is None or type(exit_code) is int


def _claude_fatal_terminal(row: LogEntry) -> bool:
    """Recognize turn-fatal Claude rows, excluding ordinary tool failures.

    These are the durable shapes produced by ``StreamParser``, claude-pty, and
    ``FullMirrorCCMBackend`` and mirrored by InstanceManager's own fatal-error
    latch.  An arbitrary ``is_error`` row is deliberately insufficient because
    a failed tool result can be followed by a valid assistant answer.
    """

    raw = _raw_mapping(row)
    if _structured_turn_failure(row, raw, provider="claude"):
        return True
    if row.event_type == "result":
        return row.is_error is not False
    if row.event_type == "session_crashed":
        return True
    if _claude_hard_rate_limit(row, raw):
        return True
    if (
        row.event_type == "message"
        and row.role == "assistant"
        and row.is_error is True
        and raw is None
        and row.content == _CLAUDE_PTY_HARD_RATE_LIMIT
    ):
        return True
    if (
        row.event_type == "message"
        and row.role == "assistant"
        and raw is not None
        and raw.get("type") == "assistant"
        and raw.get("isApiErrorMessage") is True
    ):
        return True
    if row.event_type != "system_event" or not isinstance(row.content, str):
        return False
    return row.content.startswith("api_error:") or row.content.startswith(
        "Response timed out"
    )


def _native_turn_id(row: LogEntry, raw: dict[str, Any]) -> str | None:
    value = row.native_turn_id
    if (
        not isinstance(value, str)
        or not value.strip()
        or value != value.strip()
    ):
        return None

    raw_turn = raw.get("turn")
    if "turn" in raw and not isinstance(raw_turn, dict):
        return None
    # A missing raw identity is valid for native ``codex exec`` records: the
    # output consumer attaches its immutable native id to LogEntry.  A present
    # but empty, non-string, or inconsistent identity is corrupt evidence and
    # must be rejected. Check every supplied spelling so one matching alias
    # cannot hide a second contradictory value.
    raw_ids: list[object] = []
    for key in ("turn_id", "turnId"):
        if key in raw:
            raw_ids.append(raw[key])
    if isinstance(raw_turn, dict) and "id" in raw_turn:
        raw_ids.append(raw_turn["id"])
    for raw_id in raw_ids:
        if (
            not isinstance(raw_id, str)
            or not raw_id.strip()
            or raw_id != value
        ):
            return None
    return value


def _codex_terminal_is_successful(
    row: LogEntry,
    raw: dict[str, Any],
    *,
    require_explicit_success: bool,
) -> bool:
    if (
        row.event_type != "system_event"
        or row.is_error is not False
        or raw.get("type") != "turn.completed"
    ):
        return False

    raw_turn = raw.get("turn")
    if "turn" in raw:
        if not isinstance(raw_turn, dict):
            return False
        # A nested native Turn is an all-or-nothing identity envelope.  A
        # partial object must not borrow a matching root id/status and make a
        # failed terminal look successful.
        nested_id = raw_turn.get("id")
        if (
            not isinstance(nested_id, str)
            or not nested_id
            or nested_id != nested_id.strip()
            or nested_id != row.native_turn_id
        ):
            return False

    if require_explicit_success:
        # CodexAppServer normalizes the native notification before it reaches
        # persistence.  Its successful terminal contract is therefore one
        # complete root envelope; accepting a row that borrows identity from
        # ``LogEntry.native_turn_id`` or omits an explicit error value would
        # turn damaged audit evidence into success.  ``codex exec`` does not
        # use this branch and retains its official sparse JSONL compatibility.
        required_root_fields = {"turn_id", "status", "success", "error"}
        if not required_root_fields.issubset(raw):
            return False
        root_turn_id = raw["turn_id"]
        if (
            not isinstance(root_turn_id, str)
            or not root_turn_id
            or root_turn_id != root_turn_id.strip()
            or root_turn_id != row.native_turn_id
        ):
            return False
        if (
            raw["status"] != "completed"
            or type(raw["success"]) is not bool
            or raw["success"] is not True
            or raw["error"] is not None
        ):
            return False
        if isinstance(raw_turn, dict) and not {
            "id",
            "status",
            "success",
            "error",
        }.issubset(raw_turn):
            return False

    statuses: list[object] = []
    if "status" in raw:
        statuses.append(raw["status"])
    if isinstance(raw_turn, dict) and "status" in raw_turn:
        statuses.append(raw_turn["status"])
    if any(
        not isinstance(status, str)
        or status != "completed"
        for status in statuses
    ):
        return False
    if len(statuses) > 1 and any(status != statuses[0] for status in statuses[1:]):
        return False

    successes: list[object] = []
    if "success" in raw:
        successes.append(raw["success"])
    if isinstance(raw_turn, dict) and "success" in raw_turn:
        successes.append(raw_turn["success"])
    if any(type(success) is not bool or success is not True for success in successes):
        return False
    if len(successes) > 1 and any(
        success is not successes[0] for success in successes[1:]
    ):
        return False

    errors: list[object] = []
    if "error" in raw:
        errors.append(raw["error"])
    if isinstance(raw_turn, dict) and "error" in raw_turn:
        errors.append(raw_turn["error"])
    if any(error not in (None, "", {}, []) for error in errors):
        return False
    if len(errors) > 1 and any(error != errors[0] for error in errors[1:]):
        return False

    if require_explicit_success and (not statuses or not successes):
        return False
    return True


def _codex_identity_is_absent(row: LogEntry, raw: dict[str, Any]) -> bool:
    """True only for a genuinely id-less Codex exec event.

    Empty, numeric, conflicting, or partially persisted ids are corruption,
    not the official CLI's missing-id shape.
    """

    if row.native_turn_id is not None:
        return False
    if "turn_id" in raw or "turnId" in raw:
        return False
    if "turn" not in raw:
        return True
    raw_turn = raw["turn"]
    return isinstance(raw_turn, dict) and "id" not in raw_turn


def _codex_agent_message_envelope(raw: dict[str, Any]) -> bool:
    if raw.get("type") != "item.completed":
        return False
    item = raw.get("item")
    return isinstance(item, dict) and item.get("type") == "agent_message"


def _codex_agent_message_shape(row: LogEntry, raw: dict[str, Any]) -> bool:
    if (
        row.event_type != "message"
        or row.role != "assistant"
        or not _codex_agent_message_envelope(raw)
    ):
        return False

    item = raw["item"]
    text = item.get("text")
    if text is None:
        text = ""
    return isinstance(text, str) and row.content == text


def _codex_agent_message(
    row: LogEntry,
    raw: dict[str, Any],
    native_turn_id: str,
) -> bool:
    if not _codex_agent_message_shape(row, raw):
        return False
    if _native_turn_id(row, raw) != native_turn_id:
        return False
    return True


def _codex_idless_exec_agent_message(
    row: LogEntry,
    raw: dict[str, Any],
) -> bool:
    return _codex_agent_message_shape(
        row, raw
    ) and _codex_identity_is_absent(row, raw)


def select_terminal_output(
    provider: str,
    source: LogEntry,
    candidates: Iterable[LogEntry],
    transport: str | None = None,
    *,
    source_original: LogEntry | None = None,
) -> TerminalOutputSelection | None:
    """Select the only canonical terminal text row for one logical turn.

    The selection is intentionally pure.  It neither parses the selected text
    nor performs persistence.  Legacy, autonomous, orphan, cross-generation,
    and pre-source rows are excluded before provider-specific rules are
    considered.  In-scope terminal errors remain authoritative veto evidence,
    so a later failure can never expose an older successful output.
    """

    if not isinstance(provider, str):
        return None
    normalized_provider = provider.strip().lower()
    if normalized_provider not in {"claude", "codex"}:
        return None
    if source.turn_scope != "source" or not source_shape_is_canonical(
        source,
        source_original,
    ):
        return None
    if not (
        _strict_positive_int(source.id)
        and _strict_positive_int(source.task_id)
        and _strict_generation(source.task_retry_count)
        and _strict_generation(source.task_turn_generation)
    ):
        return None

    scoped = sorted(
        (row for row in candidates if _candidate_is_in_scope(source, row)),
        key=lambda row: row.id,
    )
    transport_valid, effective_transport = _effective_transport(
        source, transport
    )
    if not transport_valid:
        return None
    if (
        effective_transport in _ACTUAL_TRANSPORTS
        and not effective_transport.startswith(f"{normalized_provider}_")
    ):
        return None

    if normalized_provider == "claude":
        # A later failed result is authoritative and must not expose an older
        # successful result (or fall through to the PTY message fallback).
        results = [row for row in scoped if row.event_type == "result"]
        if results:
            terminal = results[-1]
            if terminal.is_error is not False:
                return None
            if any(
                row.id > terminal.id and _claude_fatal_terminal(row)
                for row in scoped
            ):
                return None
            return TerminalOutputSelection(terminal, terminal, None)
        if effective_transport != "claude_pty":
            return None
        messages = [
            row
            for row in scoped
            if (
                row.event_type == "message"
                and row.role == "assistant"
                and row.is_error is False
            )
        ]
        if not messages:
            return None
        selected_message = messages[-1]
        if any(
            row.id > selected_message.id and _claude_fatal_terminal(row)
            for row in scoped
        ):
            return None
        return TerminalOutputSelection(selected_message, selected_message, None)

    codex_app_server = effective_transport == "codex_app_server"
    # Select the latest terminal envelope before validating success. Filtering
    # for successful terminals first would let a later failed/interrupted or
    # malformed envelope fall back to an older success in the same generation.
    terminal_rows: list[tuple[LogEntry, dict[str, Any]]] = []
    for row in scoped:
        raw = _raw_mapping(row)
        if row.event_type != "system_event":
            continue
        raw_type = raw.get("type") if raw is not None else None
        if (
            raw_type in {"turn.completed", "turn.failed"}
            or _structured_turn_failure(row, raw, provider="codex")
            or row.content == "turn.completed"
            or row.is_error is True
        ):
            # A corrupt terminal-looking row remains an authoritative failure
            # boundary.  Skipping it would expose an older successful turn.
            terminal_rows.append((row, raw or {}))
    if not terminal_rows:
        return None

    terminal, terminal_raw = terminal_rows[-1]
    if not _codex_terminal_is_successful(
        terminal,
        terminal_raw,
        require_explicit_success=codex_app_server,
    ):
        return None
    terminal_native_id = _native_turn_id(terminal, terminal_raw)

    # A new attempt may have started after the last completed envelope and
    # then crashed before writing its own terminal.  Likewise, a canonical
    # agent item persisted after turn.completed is corrupt ordering.  Neither
    # case may expose the preceding attempt's successful output.
    for row in scoped:
        if row.id <= terminal.id:
            continue
        raw = _raw_mapping(row)
        if raw is None:
            continue
        if (
            row.event_type == "system_event"
            and raw.get("type") in {"thread.started", "turn.started"}
        ):
            return None
        if _codex_agent_message_envelope(raw):
            return None

    # A terminal (or durable attempt-start marker) cuts off all older output,
    # even if a legacy producer reused a thread-like value as native_turn_id.
    # Native app-server turn ids are still required below; the boundary is an
    # additional ordering fence, not a substitute for identity.
    boundary_id = source.id
    if len(terminal_rows) > 1:
        boundary_id = terminal_rows[-2][0].id
    for row in scoped:
        if row.id >= terminal.id:
            break
        raw = _raw_mapping(row)
        if (
            raw is not None
            and row.event_type == "system_event"
            and raw.get("type") in {"thread.started", "turn.started"}
        ):
            boundary_id = max(boundary_id, row.id)

    if terminal_native_id is None:
        # Official ``codex exec --json`` item/turn events carry no native turn
        # id.  That compatibility cannot be generalized to app-server or an
        # unknown route: only the exec transport plus durable row boundaries
        # can keep separate attempts from being mixed.
        if (
            effective_transport != "codex_exec"
            or not _codex_identity_is_absent(terminal, terminal_raw)
        ):
            return None

        idless_messages: list[LogEntry] = []
        for row in scoped:
            if row.id <= boundary_id or row.id >= terminal.id:
                continue
            if row.is_error is not False:
                continue
            raw = _raw_mapping(row)
            if raw is not None and _codex_idless_exec_agent_message(row, raw):
                idless_messages.append(row)
        if not idless_messages:
            return None
        return TerminalOutputSelection(idless_messages[-1], terminal, None)

    messages: list[LogEntry] = []
    for row in scoped:
        if row.id <= boundary_id or row.id >= terminal.id:
            continue
        if row.is_error is not False:
            continue
        raw = _raw_mapping(row)
        if raw is not None and _codex_agent_message(
            row,
            raw,
            terminal_native_id,
        ):
            messages.append(row)
    if not messages:
        return None
    return TerminalOutputSelection(messages[-1], terminal, terminal_native_id)


def select_terminal_tail(
    provider: str,
    source: LogEntry,
    candidates: Iterable[LogEntry],
    transport: str | None = None,
    *,
    source_original: LogEntry | None = None,
) -> TerminalOutputSelection | None:
    """Compatibility spelling for :func:`select_terminal_output`."""

    return select_terminal_output(
        provider,
        source,
        candidates,
        transport,
        source_original=source_original,
    )
