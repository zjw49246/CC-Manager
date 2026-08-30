"""Shared helpers for independent Plan Task creation and staleness checks."""

from __future__ import annotations

import asyncio
import hashlib
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TypeVar

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.log_entry import LogEntry
from backend.models.sub_agent import SubAgentSession
from backend.models.task import Task
from backend.services.cancellation import settle_awaitable
from backend.services.task_termination import _finish_despite_cancellation
from backend.services.worker_task_termination import (
    active_worker_task_termination_receipt,
    no_active_worker_task_termination_predicate,
)


ACTIVE_PLAN_STATUSES = frozenset({"pending", "in_progress", "executing"})
MAX_ACTIVE_PLANS_PER_TASK = 3
PLAN_CONTEXT_SNAPSHOT_MAX_CHARS = 60_000
_T = TypeVar("_T")


class PlanTerminalQuiescenceError(RuntimeError):
    """A Plan committed terminal state but could not fully quiesce."""


@dataclass(frozen=True)
class _PlanTerminalGeneration:
    status: str
    retry_count: int
    turn_generation: int
    instance_id: int | None
    started_at: datetime | None
    completed_at: datetime | None
    pty_background_generation: str | None


async def _read_plan_terminal_generation(
    db: AsyncSession,
    plan_task_id: int,
    *,
    for_update: bool = False,
) -> _PlanTerminalGeneration | None:
    query = select(
        Task.status,
        Task.retry_count,
        Task.turn_generation,
        Task.instance_id,
        Task.started_at,
        Task.completed_at,
        Task.pty_background_generation,
    ).where(
        Task.id == plan_task_id,
        Task.mode == "plan",
    )
    if for_update:
        query = query.with_for_update()
    row = (await db.execute(query)).one_or_none()
    return _PlanTerminalGeneration(*row) if row is not None else None


async def _stage_plan_auxiliary_cancellation(
    db: AsyncSession,
    plan_task_id: int,
) -> list[tuple[int, str]]:
    """Snapshot active/retryable producers and cancel running rows atomically."""

    rows = list(
        (
            await db.execute(
                select(
                    SubAgentSession.id,
                    SubAgentSession.agent_type,
                )
                .where(
                    SubAgentSession.task_id == plan_task_id,
                    SubAgentSession.source == "ccm",
                    SubAgentSession.agent_type.in_(("monitor", "sub_agent")),
                    SubAgentSession.status.in_(("running", "cancelled")),
                )
                .with_for_update()
            )
        ).all()
    )
    if not rows:
        return []
    session_ids = [session_id for session_id, _agent_type in rows]
    await db.execute(
        update(SubAgentSession)
        .where(
            SubAgentSession.id.in_(session_ids),
            SubAgentSession.task_id == plan_task_id,
            SubAgentSession.source == "ccm",
            SubAgentSession.agent_type.in_(("monitor", "sub_agent")),
            SubAgentSession.status == "running",
        )
        .values(
            status="cancelled",
            completed_at=datetime.utcnow(),
            next_check_at=None,
            active_turn_generation=None,
            turn_started_at=None,
        )
    )
    return rows


async def run_plan_terminal_transition(
    db: AsyncSession,
    plan_task_id: int,
    terminal_status: str,
    mutate: Callable[[], Awaitable[_T]],
    *,
    authorize_effect_boundary: Callable[[], Awaitable[None]] | None = None,
) -> _T:
    """Commit and publish one Plan terminal decision under queue quiescence.

    ``authorize_effect_boundary`` may establish the global Worker-node ->
    Project -> Task -> Worker -> membership -> User fence after all queue
    cleanup rollbacks and before this helper re-enters the Plan Task row.
    ``mutate`` must then stage (but not commit) the exact Plan transition and
    any successor row. The helper commits those writes together with
    cancellation of every running CCM auxiliary producer. Once that commit
    succeeds, all process cleanup, a second queue drain, and the exact status
    publication are completed before the admission lease is released, even if
    the HTTP caller is cancelled meanwhile.
    """

    async def operation() -> _T:
        from backend.main import dispatcher
        from backend.services.dispatcher import TaskQueueAbortTimeoutError
        from backend.services.task_events import broadcast_status_change

        if dispatcher is None:
            raise PlanTerminalQuiescenceError("Dispatcher is unavailable")

        async with dispatcher.task_queue_cancellation_lease(plan_task_id):
            # clear_task_queue may commit durable delivery cancellations through
            # ``durable_db``. It must therefore run before the caller stages the
            # Plan/successor transaction.
            await db.rollback()
            try:
                await dispatcher.abort_task_queue(
                    plan_task_id,
                    cancel_durable=False,
                    durable_db=db,
                )
            except TaskQueueAbortTimeoutError as exc:
                raise PlanTerminalQuiescenceError(
                    "Plan queue worker could not be proven stopped; no "
                    "terminal decision was committed"
                ) from exc
            await db.rollback()

            pre_transition = await _read_plan_terminal_generation(
                db,
                plan_task_id,
            )
            await db.rollback()
            if (
                pre_transition is not None
                and pre_transition.pty_background_generation is not None
            ):
                raise PlanTerminalQuiescenceError(
                    "Plan still has detached PTY output; stop the session "
                    "before publishing a terminal decision"
                )

            try:
                if authorize_effect_boundary is not None:
                    await authorize_effect_boundary()
                # The read above is only an early diagnostic. This no-op CAS is
                # the transaction-local authority: it serializes a detached PTY
                # publication with the Plan decision on every supported DB and
                # keeps the row locked through successor/terminal commit.
                pty_guard = await db.execute(
                    update(Task)
                    .where(
                        Task.id == plan_task_id,
                        Task.mode == "plan",
                        Task.pty_background_generation.is_(None),
                        no_active_worker_task_termination_predicate(),
                    )
                    .values(status=Task.status)
                )
                if pty_guard.rowcount != 1:
                    if await active_worker_task_termination_receipt(
                        db,
                        plan_task_id,
                    ):
                        raise PlanTerminalQuiescenceError(
                            "Plan has an active Worker termination receipt"
                        )
                    raise PlanTerminalQuiescenceError(
                        "Plan acquired detached PTY output while its terminal "
                        "decision was starting"
                    )
                result = await mutate()
                auxiliary_sessions = await _stage_plan_auxiliary_cancellation(
                    db,
                    plan_task_id,
                )
                precommit_generation = await _read_plan_terminal_generation(
                    db,
                    plan_task_id,
                    for_update=True,
                )
                if (
                    precommit_generation is None
                    or precommit_generation.pty_background_generation is not None
                ):
                    raise PlanTerminalQuiescenceError(
                        "Plan acquired detached PTY output before its terminal "
                        "decision could commit"
                    )
                await db.commit()
            except BaseException:
                if db.in_transaction():
                    await db.rollback()
                raise

            # Freeze the database-normalized generation written by the commit.
            # Publication below compares every scalar that identifies a Plan
            # turn, rather than trusting status alone.
            # A stop failure is surfaced after the queue has still been drained
            # and the already-committed exact terminal state has been published.
            # The cancelled DB rows and advanced generation remain a durable
            # fence against any producer whose callback was already in flight.
            cleanup_failures: list[str] = []
            delayed_cancellation: asyncio.CancelledError | None = None
            expected_generation = None
            try:
                expected_generation = await _read_plan_terminal_generation(
                    db,
                    plan_task_id,
                )
            except asyncio.CancelledError as exc:
                delayed_cancellation = exc
                cleanup_failures.append(
                    "terminal generation snapshot was cancelled"
                )
            except Exception as exc:
                cleanup_failures.append(
                    f"terminal generation snapshot failed: {exc}"
                )
            finally:
                try:
                    await db.rollback()
                except asyncio.CancelledError as exc:
                    delayed_cancellation = exc
                    cleanup_failures.append(
                        "terminal generation snapshot rollback was cancelled"
                    )
                except Exception as exc:
                    cleanup_failures.append(
                        f"terminal generation snapshot rollback failed: {exc}"
                    )
            if (
                expected_generation is None
                or expected_generation.status != terminal_status
            ):
                cleanup_failures.append(
                    "Plan terminal transaction committed an unexpected generation"
                )

            for session_id, agent_type in auxiliary_sessions:
                try:
                    if agent_type == "sub_agent":
                        await dispatcher.stop_sub_agent_session_process(session_id)
                    else:
                        await dispatcher.stop_monitor_session_process(
                            session_id,
                            terminal=True,
                        )
                except asyncio.CancelledError as exc:
                    delayed_cancellation = exc
                    cleanup_failures.append(
                        f"session {session_id}: cleanup was cancelled"
                    )
                except Exception as exc:
                    cleanup_failures.append(f"session {session_id}: {exc}")

            try:
                await dispatcher.abort_task_queue(
                    plan_task_id,
                    cancel_durable=False,
                    durable_db=db,
                )
            except asyncio.CancelledError as exc:
                delayed_cancellation = exc
                cleanup_failures.append("queue drain was cancelled")
            except TaskQueueAbortTimeoutError as exc:
                cleanup_failures.append(f"queue drain: {exc}")
            except Exception as exc:
                cleanup_failures.append(f"queue drain: {exc}")

            exact_generation = None
            try:
                await db.rollback()
                db.expire_all()
                exact_generation = await _read_plan_terminal_generation(
                    db,
                    plan_task_id,
                    for_update=True,
                )
                if (
                    expected_generation is None
                    or expected_generation.status != terminal_status
                    or exact_generation != expected_generation
                ):
                    cleanup_failures.append(
                        "Plan generation changed before terminal publication "
                        f"(expected {expected_generation}, "
                        f"found {exact_generation})"
                    )
                    await db.rollback()
                else:
                    await broadcast_status_change(plan_task_id, terminal_status)
                    await db.commit()
            except asyncio.CancelledError as exc:
                delayed_cancellation = exc
                cleanup_failures.append(
                    "terminal generation verification/publication was cancelled"
                )
                try:
                    await db.rollback()
                except asyncio.CancelledError as rollback_exc:
                    delayed_cancellation = rollback_exc
                    cleanup_failures.append(
                        "terminal generation verification rollback was cancelled"
                    )
                except Exception as rollback_exc:
                    cleanup_failures.append(
                        "terminal generation verification rollback failed: "
                        f"{rollback_exc}"
                    )
            except Exception as exc:
                cleanup_failures.append(
                    f"terminal generation verification/publication failed: {exc}"
                )
                try:
                    await db.rollback()
                except asyncio.CancelledError as rollback_exc:
                    delayed_cancellation = rollback_exc
                    cleanup_failures.append(
                        "terminal generation verification rollback was cancelled"
                    )
                except Exception as rollback_exc:
                    cleanup_failures.append(
                        "terminal generation verification rollback failed: "
                        f"{rollback_exc}"
                    )

            if delayed_cancellation is not None:
                raise delayed_cancellation
            if cleanup_failures:
                raise PlanTerminalQuiescenceError(
                    "Plan terminal state was committed, but cleanup could not "
                    "be fully confirmed: " + "; ".join(cleanup_failures)
                )
            return result

    return await _finish_despite_cancellation(operation())


async def mark_plan_superseded(
    db: AsyncSession,
    source: Task,
    *,
    successor_id: int,
    completed_at: datetime | None = None,
) -> bool:
    """Atomically retire one reviewable Plan in favor of its successor.

    The status predicate is the durable race fence against a concurrent
    approve, reject, or second revision. The caller commits this update in the
    same transaction that creates ``successor_id``.
    """

    metadata = dict(source.metadata_ or {})
    metadata["plan_superseded_by_task_id"] = successor_id
    changed = await db.execute(
        update(Task)
        .where(
            Task.id == source.id,
            Task.mode == "plan",
            Task.status == "plan_review",
            no_active_worker_task_termination_predicate(),
        )
        .values(
            status="superseded",
            completed_at=completed_at or datetime.utcnow(),
            metadata_=metadata,
        )
        .execution_options(synchronize_session=False)
    )
    return changed.rowcount == 1


def _worktree_stat_fingerprint(cwd: Path, status_raw: bytes) -> bytes:
    """Add no-follow file metadata to porcelain status.

    Porcelain records alone only say that a path is modified. Two successive
    edits to the same dirty path would otherwise produce the same digest.
    Size/mtime/mode (plus a symlink target) distinguish normal worktree edits
    without reading or persisting file contents.
    """

    digest = hashlib.sha256()
    root = os.fsencode(cwd)
    for record in status_raw.split(b"\0"):
        if len(record) < 4 or record[2:3] != b" ":
            continue
        relative = record[3:]
        if (
            not relative
            or os.path.isabs(relative)
            or relative == b".."
            or relative.startswith(b".." + os.sep.encode())
        ):
            continue
        path = os.path.normpath(os.path.join(root, relative))
        try:
            stat_result = os.lstat(path)
        except OSError:
            digest.update(relative + b"\0missing\0")
            continue
        digest.update(relative)
        digest.update(
            (
                f"\0{stat_result.st_mode}:{stat_result.st_size}:"
                f"{stat_result.st_mtime_ns}\0"
            ).encode()
        )
        if os.path.islink(path):
            try:
                digest.update(os.readlink(path))
            except OSError:
                digest.update(b"<unreadable-link>")
    return digest.digest()


async def latest_task_log_id(db: AsyncSession, task_id: int) -> int | None:
    return await db.scalar(
        select(func.max(LogEntry.id)).where(
            LogEntry.task_id == task_id,
            LogEntry.role.in_(("user", "assistant")),
            LogEntry.event_type.in_(("message", "user_message")),
        )
    )


async def capture_task_context(
    db: AsyncSession,
    task_id: int,
    *,
    through_log_id: int | None = None,
    max_chars: int = PLAN_CONTEXT_SNAPSHOT_MAX_CHARS,
) -> str:
    """Capture a stable, bounded transcript for an independent Plan."""

    target = await db.get(Task, task_id)
    query = (
        select(LogEntry.id, LogEntry.role, LogEntry.content)
        .where(
            LogEntry.task_id == task_id,
            LogEntry.event_type.in_(("message", "user_message")),
            LogEntry.role.in_(("user", "assistant")),
        )
        .order_by(LogEntry.id)
    )
    if through_log_id is not None:
        query = query.where(LogEntry.id <= through_log_id)
    rows = list((await db.execute(query)).all())
    parts = []
    if target is not None and target.description:
        parts.append(f"user (initial task): {target.description}")
    parts.extend(
        f"{role}: {content}"
        for _, role, content in rows
        if role and content
    )
    transcript = "\n\n".join(parts)
    bounded = max(1_000, max_chars)
    if len(transcript) > bounded:
        return (
            "[Earlier transcript omitted due to size]\n\n"
            + transcript[-bounded:]
        )
    return transcript


async def capture_repo_revision(path: str | None) -> dict | None:
    """Capture a cheap, non-secret repo freshness fingerprint.

    The full porcelain output may contain user filenames, so persist only its
    digest. A missing/non-git path is represented explicitly rather than
    pretending it is a stable empty repository.
    """

    if not path:
        return None
    cwd = Path(path).expanduser()
    if not cwd.is_dir():
        return {"available": False, "reason": "missing"}

    async def run_git(*args: str) -> tuple[int, bytes]:
        process = None
        communicate_task = None

        async def finish_process() -> None:
            if process is not None and process.returncode is None:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
            if communicate_task is not None:
                await asyncio.gather(
                    communicate_task,
                    return_exceptions=True,
                )
            if process is not None and process.returncode is None:
                await process.wait()

        try:
            git_env = {
                key: value
                for key, value in os.environ.items()
                if key.upper() not in {"CLAUDECODE", "CLAUDE_CODE"}
            }
            # `git status` may otherwise refresh/write the index, and a
            # repository-local fsmonitor command is executable configuration.
            # Fingerprinting for a read-only Plan must do neither.
            git_env["GIT_OPTIONAL_LOCKS"] = "0"
            process = await asyncio.create_subprocess_exec(
                "git",
                "-c",
                "core.fsmonitor=false",
                *args,
                cwd=str(cwd),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                env=git_env,
            )
            communicate_task = asyncio.create_task(process.communicate())
            stdout, _ = await asyncio.wait_for(
                asyncio.shield(communicate_task),
                timeout=5,
            )
            return process.returncode or 0, stdout
        except asyncio.CancelledError:
            cleanup, _ = await settle_awaitable(finish_process())
            cleanup.result()
            raise
        except (asyncio.TimeoutError, OSError):
            cleanup, cancellation = await settle_awaitable(finish_process())
            cleanup.result()
            if cancellation is not None:
                raise cancellation
            return -1, b""

    head_rc, head_raw = await run_git("rev-parse", "--verify", "HEAD")
    status_rc, status_raw = await run_git(
        "status",
        "--porcelain=v1",
        "-z",
        "--untracked-files=all",
    )
    if head_rc != 0 and status_rc != 0:
        return {"available": False, "reason": "not_git"}
    return {
        "available": True,
        "head": (
            head_raw.decode("utf-8", errors="replace").strip()
            if head_rc == 0
            else None
        ),
        "dirty": bool(status_raw) if status_rc == 0 else None,
        "dirty_sha256": (
            hashlib.sha256(
                status_raw
                + _worktree_stat_fingerprint(cwd, status_raw)
            ).hexdigest()
            if status_rc == 0
            else None
        ),
    }


async def plan_staleness(
    db: AsyncSession,
    plan: Task,
    *,
    current_target: Task | None = None,
) -> dict:
    """Return durable reasons why a completed Plan may be out of date."""

    target = current_target
    if target is None:
        target_id = plan.plan_target_task_id or plan.id
        target = await db.get(Task, target_id)
    if target is None:
        return {
            "stale": True,
            "reasons": ["target_missing"],
            "current_log_id": None,
            "current_repo_revision": None,
        }

    reasons: list[str] = []
    current_log_id = await latest_task_log_id(db, target.id)
    if (
        plan.plan_target_task_id is not None
        and current_log_id is not None
        and (
            plan.plan_context_log_id is None
            or current_log_id > plan.plan_context_log_id
        )
    ):
        reasons.append("conversation_changed")

    current_repo_revision = await capture_repo_revision(
        target.last_cwd or target.target_repo
    )
    if (
        plan.plan_repo_revision is not None
        and current_repo_revision != plan.plan_repo_revision
    ):
        reasons.append("repository_changed")

    return {
        "stale": bool(reasons),
        "reasons": reasons,
        "current_log_id": current_log_id,
        "current_repo_revision": current_repo_revision,
    }


async def approved_plans_for_message(
    db: AsyncSession,
    target: Task,
    plan_task_ids: list[int] | None,
    *,
    confirmed_stale_plan_task_ids: list[int] | None = None,
) -> list[Task]:
    """Validate explicit Plan attachments without mutating application state."""

    raw_ids = plan_task_ids or []
    if not raw_ids:
        return []
    if len(raw_ids) > 5:
        raise ValueError("At most 5 approved Plans can be attached to one message")
    ids: list[int] = []
    seen: set[int] = set()
    for value in raw_ids:
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError("plan_task_ids must contain positive integers")
        if value in seen:
            raise ValueError("plan_task_ids must not contain duplicates")
        seen.add(value)
        ids.append(value)

    rows = await db.execute(select(Task).where(Task.id.in_(ids)))
    by_id = {plan.id: plan for plan in rows.scalars().all()}
    confirmed = set(confirmed_stale_plan_task_ids or [])
    plans: list[Task] = []
    for plan_id in ids:
        plan = by_id.get(plan_id)
        if plan is None:
            raise ValueError(f"Plan Task #{plan_id} was not found")
        if plan.mode != "plan" or plan.plan_target_task_id != target.id:
            raise ValueError(
                f"Plan Task #{plan_id} is not associated with Task #{target.id}"
            )
        if (
            plan.plan_approved is not True
            or plan.status != "completed"
            or not plan.plan_content
        ):
            raise ValueError(f"Plan Task #{plan_id} is not approved and ready")
        if plan.plan_applied_at is not None:
            raise ValueError(f"Plan Task #{plan_id} has already been applied")
        stale = await plan_staleness(db, plan, current_target=target)
        if stale["stale"] and plan_id not in confirmed:
            error = ValueError(
                f"Plan Task #{plan_id} context changed; confirm stale application"
            )
            setattr(error, "staleness", stale)
            setattr(error, "plan_task_id", plan_id)
            raise error
        plans.append(plan)
    return plans


def applied_plan_snapshots(plans: list[Task]) -> list[dict[str, object]]:
    """Freeze the exact approved Plan content used by one user message."""

    return [
        {
            "id": plan.id,
            "title": plan.title or f"Plan #{plan.id}",
            "content": plan.plan_content or "",
        }
        for plan in plans
    ]


def build_approved_plan_prompt(plans: list[Task], user_prompt: str) -> str:
    if not plans:
        return user_prompt
    parts = [
        "[Approved Plans explicitly selected by the user for this turn]",
        (
            "The plans below are context for the user's current instruction. "
            "Do not treat approval alone as permission beyond that instruction."
        ),
    ]
    for plan in plans:
        parts.append(
            f'<approved_plan task_id="{plan.id}">\n'
            f"{plan.plan_content}\n"
            "</approved_plan>"
        )
    parts.extend(["[User instruction for this turn]", user_prompt])
    return "\n\n".join(parts)
