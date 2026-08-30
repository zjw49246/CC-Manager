"""Process-local admission fence shared by Test Harness owner lifecycles.

Every path that can materialize a Harness Run or an isolated Browser child
uses the same per-owner lock as Task cancellation/deletion.  The context is
re-entrant for the current asyncio Task so high-level pipelines can keep one
lease across prepare -> reserve -> attach -> activate while lower-level
helpers independently enforce the same boundary.
"""

from __future__ import annotations

import asyncio
import threading
import weakref
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import AsyncIterator, Awaitable, Callable

from sqlalchemy import and_, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.task import Task
from backend.models.test_harness import TestHarnessChildBinding, TestHarnessRun
from backend.models.workspace_review import WorkspaceReviewRun


_registry_guard = threading.Lock()
_locks_by_loop: weakref.WeakKeyDictionary[
    asyncio.AbstractEventLoop,
    dict[int, asyncio.Lock],
] = weakref.WeakKeyDictionary()
_held_owner_ids: ContextVar[
    tuple[asyncio.Task[object], frozenset[int]] | None
] = ContextVar(
    "test_harness_held_owner_ids",
    default=None,
)


class TestHarnessOwnerFenceError(RuntimeError):
    """The exact Task incarnation/generation no longer owns admission."""


@dataclass(frozen=True, slots=True)
class TestHarnessOwnerIdentity:
    task_id: int
    incarnation_id: str
    retry_count: int
    turn_generation: int
    status: str


TEST_HARNESS_TERMINAL_GATE_KEY = "test_harness_terminal_generation"
_HARNESS_GRAPH_TERMINAL_STATUSES = frozenset(
    {"completed", "failed", "cancelled", "stale"}
)
_WORKSPACE_GRAPH_TERMINAL_STATUSES = frozenset(
    {"completed", "failed", "cancelled"}
)
_BROWSER_CHILD_TERMINAL_STATES = frozenset({"stopped", "completed"})


class TestHarnessOwnerGraphConflict(RuntimeError):
    """An ordinary Task mutation raced with an active Browser graph."""


def test_harness_owner_locality_error(task: Task) -> str | None:
    """Reject Manager-side materialization for remote-authoritative shadows."""

    if getattr(task, "shared_from_id", None) is not None:
        return (
            "Shared shadow Tasks cannot materialize Test Harness runs on "
            "this Manager"
        )
    if getattr(task, "worker_id", None) is not None:
        return (
            "Worker-authoritative Tasks cannot materialize Test Harness runs "
            "on the Manager; start the run on the owning Worker"
        )
    return None


def no_active_test_harness_owner_graph_predicate():
    """Correlated SQL fence for a Task writer that must remain graph-free.

    Harness/Workspace admission writes the owner Task before inserting its
    Run.  Using this predicate on the Task mutation therefore gives every
    database one deterministic winner: either the edit commits first and the
    new Run freezes the edited Task, or Run admission commits first and the
    edit is rejected.
    """

    active_harness = (
        select(TestHarnessRun.id)
        .where(
            TestHarnessRun.task_id == Task.id,
            or_(
                TestHarnessRun.status.not_in(
                    _HARNESS_GRAPH_TERMINAL_STATUSES
                ),
                TestHarnessRun.cleanup_status != "completed",
            ),
        )
        .correlate(Task)
        .exists()
    )
    active_workspace = (
        select(WorkspaceReviewRun.id)
        .where(
            WorkspaceReviewRun.task_id == Task.id,
            or_(
                WorkspaceReviewRun.status.not_in(
                    _WORKSPACE_GRAPH_TERMINAL_STATUSES
                ),
                WorkspaceReviewRun.cleanup_status != "completed",
            ),
        )
        .correlate(Task)
        .exists()
    )
    active_child = (
        select(TestHarnessChildBinding.id)
        .where(
            TestHarnessChildBinding.owner_task_id == Task.id,
            TestHarnessChildBinding.state.not_in(
                _BROWSER_CHILD_TERMINAL_STATES
            ),
        )
        .correlate(Task)
        .exists()
    )
    return and_(~active_harness, ~active_workspace, ~active_child)


async def has_active_test_harness_owner_graph(
    db: AsyncSession,
    task_id: int,
) -> bool:
    """Read back why a graph-fenced Task mutation lost its atomic CAS."""

    active_harness = await db.scalar(
        select(TestHarnessRun.id)
        .where(
            TestHarnessRun.task_id == task_id,
            or_(
                TestHarnessRun.status.not_in(
                    _HARNESS_GRAPH_TERMINAL_STATUSES
                ),
                TestHarnessRun.cleanup_status != "completed",
            ),
        )
        .limit(1)
    )
    if active_harness is not None:
        return True
    active_workspace = await db.scalar(
        select(WorkspaceReviewRun.id)
        .where(
            WorkspaceReviewRun.task_id == task_id,
            or_(
                WorkspaceReviewRun.status.not_in(
                    _WORKSPACE_GRAPH_TERMINAL_STATUSES
                ),
                WorkspaceReviewRun.cleanup_status != "completed",
            ),
        )
        .limit(1)
    )
    if active_workspace is not None:
        return True
    active_child = await db.scalar(
        select(TestHarnessChildBinding.id)
        .where(
            TestHarnessChildBinding.owner_task_id == task_id,
            TestHarnessChildBinding.state.not_in(
                _BROWSER_CHILD_TERMINAL_STATES
            ),
        )
        .limit(1)
    )
    return active_child is not None


async def require_no_active_test_harness_owner_graph(
    db: AsyncSession,
    task_id: int,
) -> None:
    if await has_active_test_harness_owner_graph(db, task_id):
        raise TestHarnessOwnerGraphConflict(
            "Task owns an active Test Harness, Workspace Review, or Browser "
            "Agent graph; wait for it to finish before editing the Task"
        )


def _terminal_gate_value(
    identity: TestHarnessOwnerIdentity,
    *,
    reason: str | None = None,
) -> dict[str, object]:
    value: dict[str, object] = {
        "incarnation_id": identity.incarnation_id,
        "retry_count": identity.retry_count,
        "turn_generation": identity.turn_generation,
        "status": identity.status,
    }
    if reason:
        value["reason"] = reason[:500]
    return value


def test_harness_owner_terminal_gate_matches(
    task: Task,
    identity: TestHarnessOwnerIdentity,
) -> bool:
    """Return whether this exact owner generation closed Harness admission."""

    metadata = task.metadata_ if isinstance(task.metadata_, dict) else {}
    gate = metadata.get(TEST_HARNESS_TERMINAL_GATE_KEY)
    return bool(
        isinstance(gate, dict)
        and gate.get("incarnation_id") == identity.incarnation_id
        and type(gate.get("retry_count")) is int
        and gate.get("retry_count") == identity.retry_count
        and type(gate.get("turn_generation")) is int
        and gate.get("turn_generation") == identity.turn_generation
        and gate.get("status") == identity.status
    )


def test_harness_owner_identity(task: Task) -> TestHarnessOwnerIdentity:
    if not task.incarnation_id:
        raise TestHarnessOwnerFenceError(
            "Harness owner Task has no durable incarnation identity"
        )
    return TestHarnessOwnerIdentity(
        task_id=task.id,
        incarnation_id=task.incarnation_id,
        retry_count=task.retry_count,
        turn_generation=task.turn_generation,
        status=task.status,
    )


async def lock_test_harness_owner(
    db: AsyncSession,
    identity: TestHarnessOwnerIdentity,
) -> Task:
    """Take a cross-process write lock on one exact owner generation.

    The no-op UPDATE is intentional: PostgreSQL/MySQL lock the matched row,
    while SQLite WAL obtains the writer reservation before any Run/Binding
    insert in this transaction. A delete/retry winner makes rowcount zero.
    """

    locked = await db.execute(
        update(Task)
        .where(
            Task.id == identity.task_id,
            Task.incarnation_id == identity.incarnation_id,
            Task.retry_count == identity.retry_count,
            Task.turn_generation == identity.turn_generation,
            Task.status == identity.status,
        )
        .values(status=identity.status)
    )
    if locked.rowcount != 1:
        raise TestHarnessOwnerFenceError(
            "Harness owner Task incarnation or generation changed"
        )
    owner = (
        await db.execute(
            select(Task).where(
                Task.id == identity.task_id,
                Task.incarnation_id == identity.incarnation_id,
            )
        )
    ).scalar_one_or_none()
    if owner is None:
        raise TestHarnessOwnerFenceError("Harness owner Task disappeared")
    if test_harness_owner_terminal_gate_matches(owner, identity):
        raise TestHarnessOwnerFenceError(
            "Harness owner Task generation is already terminalizing"
        )
    return owner


async def install_test_harness_owner_terminal_gate(
    db: AsyncSession,
    identity: TestHarnessOwnerIdentity,
    *,
    reason: str,
    locked_owner_validator: (
        Callable[[AsyncSession], Awaitable[None]] | None
    ) = None,
) -> Task:
    """Durably close Run/Workspace/child admission for one owner generation.

    This deliberately does not use :func:`lock_test_harness_owner`, because an
    interrupted terminalizer must be able to resume an already-installed gate.
    The exact Task writer CAS is still the first statement, so a concurrent
    materializer either commits before this gate (and is drained by the
    terminalizer) or observes the gate and fails closed.
    """

    locked = await db.execute(
        update(Task)
        .where(
            Task.id == identity.task_id,
            Task.incarnation_id == identity.incarnation_id,
            Task.retry_count == identity.retry_count,
            Task.turn_generation == identity.turn_generation,
            Task.status == identity.status,
        )
        .values(status=identity.status)
    )
    if locked.rowcount != 1:
        raise TestHarnessOwnerFenceError(
            "Harness owner Task incarnation or generation changed"
        )
    if locked_owner_validator is not None:
        # The Task writer is now held.  Let specialized terminalizers prove
        # any external authority (for example, an exact live Worker receipt
        # lease) inside this same transaction before the durable gate write.
        # Sampling that authority before the Task lock wait would allow an
        # expired owner to commit the gate after it finally acquires the row.
        await locked_owner_validator(db)
    owner = (
        await db.execute(
            select(Task).where(
                Task.id == identity.task_id,
                Task.incarnation_id == identity.incarnation_id,
            )
        )
    ).scalar_one_or_none()
    if owner is None:
        raise TestHarnessOwnerFenceError("Harness owner Task disappeared")
    if not test_harness_owner_terminal_gate_matches(owner, identity):
        metadata = dict(owner.metadata_ or {})
        metadata[TEST_HARNESS_TERMINAL_GATE_KEY] = _terminal_gate_value(
            identity,
            reason=reason,
        )
        owner.metadata_ = metadata
    return owner


def _owner_lock(task_id: int) -> asyncio.Lock:
    loop = asyncio.get_running_loop()
    with _registry_guard:
        locks = _locks_by_loop.setdefault(loop, {})
        return locks.setdefault(task_id, asyncio.Lock())


@asynccontextmanager
async def test_harness_owner_fence(task_id: int) -> AsyncIterator[None]:
    """Serialize materialization and terminalization for one owner Task."""

    if isinstance(task_id, bool) or not isinstance(task_id, int) or task_id < 1:
        raise ValueError("Test Harness owner Task identity is invalid")
    current_task = asyncio.current_task()
    if current_task is None:  # pragma: no cover - an async context has a Task.
        raise RuntimeError("Test Harness owner fence requires an asyncio Task")
    inherited = _held_owner_ids.get()
    # ContextVars are copied into ``asyncio.create_task`` children.  Re-entry
    # belongs only to the exact coroutine Task that acquired the lock; a child
    # inheriting its parent's Context must take the lock normally.
    held = (
        inherited[1]
        if inherited is not None and inherited[0] is current_task
        else frozenset()
    )
    if task_id in held:
        yield
        return

    lock = _owner_lock(task_id)
    async with lock:
        token = _held_owner_ids.set((current_task, held | {task_id}))
        try:
            yield
        finally:
            _held_owner_ids.reset(token)
