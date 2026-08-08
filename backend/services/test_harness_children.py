"""Durable ownership and launch fencing for isolated Browser Agent Tasks.

The in-memory Browser Review job is only an execution handle.  This service
persists the authoritative owner -> child relationship before a child can be
claimed, and keeps cancellation/restart recovery independent from that
ephemeral handle.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from backend.database import async_session
from backend.models.instance import Instance
from backend.models.task import Task
from backend.models.test_harness import (
    TestHarnessChildBinding,
    TestHarnessRun,
)
from backend.models.workspace_review import WorkspaceReviewRun
from backend.services.task_creation import stage_task_record

logger = logging.getLogger(__name__)

CHILD_RESERVED = "reserved"
CHILD_READY = "ready"
CHILD_RUNNING = "running"
CHILD_STOPPING = "stopping"
CHILD_STOPPED = "stopped"
CHILD_COMPLETED = "completed"
CHILD_STOP_FAILED = "stop_failed"

CHILD_TERMINAL_STATES = frozenset({CHILD_STOPPED, CHILD_COMPLETED})
TASK_TERMINAL_STATUSES = frozenset(
    {"completed", "failed", "cancelled", "conflict", "superseded"}
)
TASK_ACTIVE_STATUSES = frozenset(
    {"pending_activation", "pending", "in_progress", "executing", "merging"}
)


class TestHarnessChildError(RuntimeError):
    """A Browser Agent child could not be safely attached or stopped."""


class TestHarnessChildRecoveryError(TestHarnessChildError):
    """Startup could not prove that every interrupted child was stopped."""


TaskStopper = Callable[[int], Awaitable[None]]


class TestHarnessChildService:
    """Own the durable lifecycle of isolated Browser Agent Tasks."""

    def __init__(
        self,
        *,
        db_factory: async_sessionmaker[AsyncSession] = async_session,
        task_stopper: TaskStopper | None = None,
    ) -> None:
        self.db_factory = db_factory
        self._task_stopper = task_stopper
        self._stop_locks: dict[str, asyncio.Lock] = {}
        self._locks_guard = asyncio.Lock()

    async def reserve_child(
        self,
        *,
        owner_task_id: int,
        browser_review_job_id: str,
        child_values: Mapping[str, Any],
        harness_run_id: str | None = None,
        workspace_review_run_id: str | None = None,
    ) -> tuple[Task, TestHarnessChildBinding]:
        """Atomically persist a non-runnable child and its durable owner."""

        if not harness_run_id and not workspace_review_run_id:
            raise TestHarnessChildError("Browser child requires a durable run owner")
        if not browser_review_job_id:
            raise TestHarnessChildError("Browser child requires a Browser Review job")

        values = dict(child_values)
        metadata = dict(values.get("metadata_") or {})
        metadata.update(
            {
                "browser_review_job_id": browser_review_job_id,
                "test_harness_run_id": harness_run_id,
                "workspace_review_run_id": workspace_review_run_id,
                "test_harness_parent_task_id": owner_task_id,
                "workspace_review_parent_task_id": owner_task_id,
                "isolated_browser_agent": True,
            }
        )
        values.update(status="pending_activation", metadata_=metadata)

        async with self.db_factory() as db:
            child = await stage_task_record(db, **values)
            binding = TestHarnessChildBinding(
                id=uuid.uuid4().hex,
                harness_run_id=harness_run_id,
                workspace_review_run_id=workspace_review_run_id,
                owner_task_id=owner_task_id,
                child_task_id=child.id,
                browser_review_job_id=browser_review_job_id,
                state=CHILD_RESERVED,
            )
            db.add(binding)
            if harness_run_id:
                run = await db.get(TestHarnessRun, harness_run_id)
                if run is None or run.task_id != owner_task_id:
                    raise TestHarnessChildError(
                        "Harness run disappeared or changed owner while reserving child"
                    )
                run.agent_task_id = child.id
                run.browser_review_job_id = browser_review_job_id
            if workspace_review_run_id:
                workspace_run = await db.get(
                    WorkspaceReviewRun, workspace_review_run_id
                )
                if workspace_run is None or workspace_run.task_id != owner_task_id:
                    raise TestHarnessChildError(
                        "Workspace review disappeared or changed owner while reserving child"
                    )
                workspace_run.agent_task_id = child.id
                workspace_run.browser_review_job_id = browser_review_job_id
            await db.commit()
            await db.refresh(child)
            await db.refresh(binding)
            return child, binding

    async def activate(self, binding_id: str) -> TestHarnessChildBinding:
        """Publish the child to TaskQueue only after its job is attached."""

        async with self.db_factory() as db:
            binding = (
                await db.execute(
                    select(TestHarnessChildBinding)
                    .where(TestHarnessChildBinding.id == binding_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if binding is None:
                raise TestHarnessChildError("Browser child binding disappeared")
            if binding.state == CHILD_READY:
                return binding
            if binding.state != CHILD_RESERVED:
                raise TestHarnessChildError(
                    f"Browser child cannot activate from state {binding.state}"
                )
            child = (
                await db.execute(
                    select(Task)
                    .where(Task.id == binding.child_task_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if child is None or child.status != "pending_activation":
                raise TestHarnessChildError(
                    "Browser child disappeared or escaped its activation gate"
                )
            now = datetime.utcnow()
            child.status = "pending"
            binding.state = CHILD_READY
            binding.activated_at = now
            binding.error = None
            if binding.workspace_review_run_id:
                workspace_run = await db.get(
                    WorkspaceReviewRun, binding.workspace_review_run_id
                )
                if workspace_run is not None:
                    workspace_run.status = "reviewing"
                    workspace_run.stage = "browser_agent_queued"
            await db.commit()
            await db.refresh(binding)
            return binding

    async def abort_reservation(self, binding_id: str, exc: BaseException) -> None:
        """Close a child that failed before its launch gate opened."""

        error = _safe_error(exc)
        async with self.db_factory() as db:
            binding = (
                await db.execute(
                    select(TestHarnessChildBinding)
                    .where(TestHarnessChildBinding.id == binding_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if binding is None:
                return
            child = await db.get(Task, binding.child_task_id)
            if child is not None and child.status == "pending_activation":
                child.status = "cancelled"
                child.completed_at = datetime.utcnow()
                child.error_message = error
            binding.state = CHILD_STOPPED
            binding.stop_requested_at = binding.stop_requested_at or datetime.utcnow()
            binding.completed_at = datetime.utcnow()
            binding.error = error
            await db.commit()

    async def mark_terminal_by_child(
        self,
        child_task_id: int,
        *,
        task_status: str | None = None,
        error: str | None = None,
    ) -> None:
        """Persist natural Task completion independently of the job watcher."""

        async with self.db_factory() as db:
            binding = await db.scalar(
                select(TestHarnessChildBinding).where(
                    TestHarnessChildBinding.child_task_id == child_task_id
                )
            )
            if binding is None or binding.state in CHILD_TERMINAL_STATES:
                return
            if task_status is None:
                child = await db.get(Task, child_task_id)
                task_status = child.status if child is not None else None
                error = error or (child.error_message if child is not None else None)
            if task_status not in TASK_TERMINAL_STATUSES:
                return
            binding.state = (
                CHILD_STOPPED if task_status == "cancelled" else CHILD_COMPLETED
            )
            binding.completed_at = datetime.utcnow()
            binding.error = error
            await db.commit()

    async def stop_for_harness_run(self, run_id: str, *, reason: str) -> bool:
        binding_id = await self._binding_id(harness_run_id=run_id)
        if binding_id is None:
            return False
        await self.stop_binding(binding_id, reason=reason)
        return True

    async def stop_for_workspace_run(self, run_id: str, *, reason: str) -> bool:
        binding_id = await self._binding_id(workspace_review_run_id=run_id)
        if binding_id is None:
            return False
        await self.stop_binding(binding_id, reason=reason)
        return True

    async def stop_for_owner(self, task_id: int, *, reason: str) -> int:
        async with self.db_factory() as db:
            binding_ids = list(
                (
                    await db.execute(
                        select(TestHarnessChildBinding.id).where(
                            TestHarnessChildBinding.owner_task_id == task_id,
                            TestHarnessChildBinding.state.not_in(
                                CHILD_TERMINAL_STATES
                            ),
                        )
                    )
                ).scalars()
            )
        for binding_id in binding_ids:
            await self.stop_binding(binding_id, reason=reason)
        return len(binding_ids)

    async def stop_binding(self, binding_id: str, *, reason: str) -> None:
        """Stop and verify one exact child generation before returning."""

        lock = await self._stop_lock(binding_id)
        async with lock:
            await _finish_despite_cancellation(
                self._stop_binding_impl(binding_id, reason=reason)
            )

    async def _stop_binding_impl(self, binding_id: str, *, reason: str) -> None:
        child_task_id: int
        job_id: str
        async with self.db_factory() as db:
            binding = (
                await db.execute(
                    select(TestHarnessChildBinding)
                    .where(TestHarnessChildBinding.id == binding_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if binding is None:
                raise TestHarnessChildError("Browser child binding disappeared")
            if binding.state in CHILD_TERMINAL_STATES:
                return
            binding.state = CHILD_STOPPING
            binding.stop_requested_at = binding.stop_requested_at or datetime.utcnow()
            binding.error = reason[:4000]
            child_task_id = binding.child_task_id
            job_id = binding.browser_review_job_id
            await db.commit()

        try:
            from backend.services.browser_review_jobs import browser_review_job_manager

            await browser_review_job_manager.mark_cancelling(job_id)
            await self._stop_task(child_task_id)
            await browser_review_job_manager.cancel(job_id)
            await self._verify_child_terminal(child_task_id)
        except BaseException as exc:
            async with self.db_factory() as db:
                binding = await db.get(TestHarnessChildBinding, binding_id)
                if binding is not None and binding.state not in CHILD_TERMINAL_STATES:
                    binding.state = CHILD_STOP_FAILED
                    binding.error = _safe_error(exc)
                    await db.commit()
            raise TestHarnessChildError(
                f"Browser child {child_task_id} cleanup could not be proven: "
                f"{_safe_error(exc)}"
            ) from exc

        async with self.db_factory() as db:
            binding = await db.get(TestHarnessChildBinding, binding_id)
            if binding is None:
                raise TestHarnessChildError("Browser child binding disappeared after stop")
            child = await db.get(Task, child_task_id)
            binding.state = (
                CHILD_COMPLETED
                if child is not None and child.status != "cancelled"
                else CHILD_STOPPED
            )
            binding.completed_at = datetime.utcnow()
            binding.error = None
            await db.commit()

    async def recover_interrupted(self) -> int:
        """Reap all nonterminal/legacy children before Dispatcher starts."""

        recovered = 0
        failures: list[str] = []
        await self._adopt_legacy_children(failures)
        async with self.db_factory() as db:
            bindings = list(
                (
                    await db.execute(
                        select(TestHarnessChildBinding).where(
                            TestHarnessChildBinding.state.not_in(
                                CHILD_TERMINAL_STATES
                            )
                        )
                    )
                ).scalars()
            )
            binding_ids = [binding.id for binding in bindings]
        for binding_id in binding_ids:
            try:
                await self.stop_binding(
                    binding_id,
                    reason="Manager restarted before Browser Agent cleanup completed",
                )
                recovered += 1
            except Exception as exc:
                logger.exception("Could not recover Browser child %s", binding_id)
                failures.append(f"{binding_id}: {_safe_error(exc)}")
        if failures:
            raise TestHarnessChildRecoveryError(
                "Interrupted Browser Agent cleanup failed: " + "; ".join(failures)
            )
        return recovered

    async def _adopt_legacy_children(self, failures: list[str]) -> None:
        """Fence active pre-migration isolated Tasks so they cannot launch."""

        async with self.db_factory() as db:
            tasks = list(
                (
                    await db.execute(
                        select(Task).where(Task.status.in_(TASK_ACTIVE_STATUSES))
                    )
                ).scalars()
            )
            for task in tasks:
                metadata = dict(task.metadata_ or {})
                if metadata.get("isolated_browser_agent") is not True:
                    continue
                existing = await db.scalar(
                    select(TestHarnessChildBinding.id).where(
                        TestHarnessChildBinding.child_task_id == task.id
                    )
                )
                if existing is not None:
                    continue
                harness_run_id = _metadata_id(metadata.get("test_harness_run_id"))
                workspace_run_id = _metadata_id(
                    metadata.get("workspace_review_run_id")
                )
                job_id = _metadata_id(metadata.get("browser_review_job_id"))
                owner_task_id = metadata.get("test_harness_parent_task_id") or metadata.get(
                    "workspace_review_parent_task_id"
                )
                if (
                    not (harness_run_id or workspace_run_id)
                    or not job_id
                    or type(owner_task_id) is not int
                ):
                    failures.append(
                        f"Task {task.id}: legacy Browser child identity is incomplete"
                    )
                    continue
                db.add(
                    TestHarnessChildBinding(
                        id=uuid.uuid4().hex,
                        harness_run_id=harness_run_id,
                        workspace_review_run_id=workspace_run_id,
                        owner_task_id=owner_task_id,
                        child_task_id=task.id,
                        browser_review_job_id=job_id,
                        state=CHILD_STOP_FAILED,
                        error="Adopted during startup recovery",
                    )
                )
            await db.commit()

    async def _binding_id(
        self,
        *,
        harness_run_id: str | None = None,
        workspace_review_run_id: str | None = None,
    ) -> str | None:
        async with self.db_factory() as db:
            predicates = []
            if harness_run_id:
                predicates.append(
                    TestHarnessChildBinding.harness_run_id == harness_run_id
                )
            if workspace_review_run_id:
                predicates.append(
                    TestHarnessChildBinding.workspace_review_run_id
                    == workspace_review_run_id
                )
            if not predicates:
                return None
            return await db.scalar(
                select(TestHarnessChildBinding.id).where(*predicates)
            )

    async def _stop_task(self, task_id: int) -> None:
        if self._task_stopper is not None:
            await self._task_stopper(task_id)
            return
        async with self.db_factory() as db:
            task = await db.get(Task, task_id)
            if task is None or task.status in TASK_TERMINAL_STATUSES:
                return
            if task.status == "pending_activation":
                owner = await db.scalar(
                    select(Instance.id).where(Instance.current_task_id == task_id)
                )
                if owner is not None:
                    raise TestHarnessChildError(
                        "A gated Browser child unexpectedly owns an Instance"
                    )
                task.status = "cancelled"
                task.completed_at = datetime.utcnow()
                task.error_message = "Browser Agent stopped before activation"
                await db.commit()
                return
        from backend.api.tasks import _cancel_local_task_under_cancellation_lease
        from backend.main import dispatcher

        async with self.db_factory() as db:
            async with dispatcher.task_queue_cancellation_lease(task_id):
                await _cancel_local_task_under_cancellation_lease(task_id, db)

    async def _verify_child_terminal(self, task_id: int) -> None:
        async with self.db_factory() as db:
            child = await db.get(Task, task_id)
            if child is not None and child.status not in TASK_TERMINAL_STATUSES:
                raise TestHarnessChildError(
                    f"Browser child remained {child.status} after cancellation"
                )
            owner = await db.scalar(
                select(Instance.id).where(Instance.current_task_id == task_id)
            )
            if owner is not None:
                raise TestHarnessChildError(
                    f"Browser child still owns Instance {owner} after cancellation"
                )

    async def _stop_lock(self, binding_id: str) -> asyncio.Lock:
        async with self._locks_guard:
            return self._stop_locks.setdefault(binding_id, asyncio.Lock())


async def _finish_despite_cancellation(awaitable: Awaitable[None]) -> None:
    operation = asyncio.create_task(awaitable)
    cancellation: asyncio.CancelledError | None = None
    while not operation.done():
        try:
            await asyncio.shield(operation)
        except asyncio.CancelledError as exc:
            cancellation = exc
    operation.result()
    if cancellation is not None:
        raise cancellation


def _metadata_id(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _safe_error(exc: BaseException) -> str:
    text = str(exc).strip() or exc.__class__.__name__
    return text[:4000]


test_harness_child_service = TestHarnessChildService()
