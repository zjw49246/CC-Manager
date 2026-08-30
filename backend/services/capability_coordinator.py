"""Durable scheduler for registered task Capability executors.

The coordinator is intentionally capability-agnostic.  Executor adapters own
their external lifecycle and durable handle; this service only decides which
idempotent callback is appropriate for the current Invocation state.

WebSocket events may wake the poller, but are never the source of truth.  Every
decision is made after re-reading the Invocation and its active Execution from
the database, which also makes a cold process restart recoverable.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime
import logging
import time
from typing import Any

from sqlalchemy import case, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.capability import CapabilityInvocation
from backend.services.capability_events import broadcast_capability_event
from backend.services.capability_registry import resolve_capability
from backend.services.capability_service import (
    CapabilityConflictError,
    CapabilityNotFoundError,
    CapabilityUnavailableError,
    CapabilityUnsupportedScopeError,
    CapabilityValidationError,
    active_execution_for,
    fail_execution,
)
from backend.services.cancellation import finish_awaitable


logger = logging.getLogger(__name__)

_POLLABLE_STATUSES = ("queued", "running", "waiting_user", "cancelling")
_PERMANENT_EXECUTOR_ERRORS = (
    CapabilityNotFoundError,
    CapabilityUnavailableError,
    CapabilityUnsupportedScopeError,
    CapabilityValidationError,
)


class CapabilityCoordinator:
    """Poll and recover durable CapabilityInvocation records.

    ``max_concurrency`` bounds executor callbacks globally.  ``_inflight`` and
    the per-Invocation lock additionally guarantee that one process never
    calls an executor twice concurrently for the same Invocation.  Database
    CAS in Capability Core remains the cross-process fence.
    """

    def __init__(
        self,
        *,
        db_factory: Callable[[], Any],
        poll_interval_seconds: float = 2.0,
        max_concurrency: int = 4,
        scan_limit: int = 64,
        initial_backoff_seconds: float = 1.0,
        max_backoff_seconds: float = 60.0,
    ) -> None:
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be at least 1")
        if scan_limit < max_concurrency:
            raise ValueError("scan_limit must be at least max_concurrency")
        if initial_backoff_seconds <= 0:
            raise ValueError("initial_backoff_seconds must be positive")
        if max_backoff_seconds < initial_backoff_seconds:
            raise ValueError(
                "max_backoff_seconds must be at least initial_backoff_seconds"
            )

        self.db_factory = db_factory
        self.poll_interval_seconds = float(poll_interval_seconds)
        self.max_concurrency = max_concurrency
        self.scan_limit = scan_limit
        self.initial_backoff_seconds = float(initial_backoff_seconds)
        self.max_backoff_seconds = float(max_backoff_seconds)

        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._dispatch_lock = asyncio.Lock()
        self._lifecycle_lock = asyncio.Lock()
        self._invocation_locks: dict[int, asyncio.Lock] = {}
        self._inflight: dict[int, asyncio.Task[None]] = {}
        self._failure_counts: dict[int, int] = {}
        self._retry_not_before: dict[int, float] = {}
        self._recovery_pending: set[int] = set()
        self._wake_event = asyncio.Event()
        self._stop_event = asyncio.Event()
        self._runner: asyncio.Task[None] | None = None

    @property
    def is_running(self) -> bool:
        return self._runner is not None and not self._runner.done()

    def wake(self) -> None:
        """Hint that durable state may have changed; polling remains canonical."""

        self._wake_event.set()

    async def start(self) -> None:
        """Recover the complete durable active set before normal polling."""

        async with self._lifecycle_lock:
            if self.is_running:
                return
            self._stop_event.clear()
            self._wake_event.clear()

            # Do not let a normal observe tick overtake crash recovery.  The
            # recovery pass is unbounded in row count but executor concurrency
            # remains bounded by ``max_concurrency``.
            await self.run_once(recovery=True, scan_limit=None)
            if self._stop_event.is_set():
                return
            self._runner = asyncio.create_task(
                self._run_loop(),
                name="capability-coordinator",
            )
            logger.info("CapabilityCoordinator started")

    async def shutdown(self) -> None:
        """Stop new admissions and await every callback already in flight."""

        async def settle() -> None:
            async with self._lifecycle_lock:
                self._stop_event.set()
                self._wake_event.set()
                runner = self._runner

            try:
                if runner is not None:
                    await runner

                while True:
                    async with self._dispatch_lock:
                        callbacks = tuple(
                            callback
                            for callback in self._inflight.values()
                            if not callback.done()
                        )
                    if not callbacks:
                        break
                    await self._await_callbacks(callbacks)
            finally:
                async with self._lifecycle_lock:
                    if self._runner is runner:
                        self._runner = None
            logger.info("CapabilityCoordinator stopped")

        # Protect the complete shutdown graph, including the locks acquired
        # after the runner exits.  Shielding only the runner would let AnyIO
        # level cancellation interrupt the next lock checkpoint.
        await finish_awaitable(settle())

    async def _run_loop(self) -> None:
        try:
            while not self._stop_event.is_set():
                try:
                    await asyncio.wait_for(
                        self._wake_event.wait(),
                        timeout=self.poll_interval_seconds,
                    )
                except asyncio.TimeoutError:
                    pass
                self._wake_event.clear()
                if self._stop_event.is_set():
                    break
                try:
                    await self.run_once()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # A database outage or malformed row must not permanently
                    # kill the only recovery loop.  The next bounded poll gets
                    # another chance.
                    logger.exception("CapabilityCoordinator scan failed")
        finally:
            # run_once awaits its own batch.  This is a defensive barrier for
            # callers that triggered another run_once concurrently.
            while True:
                async with self._dispatch_lock:
                    callbacks = tuple(
                        callback
                        for callback in self._inflight.values()
                        if not callback.done()
                    )
                if not callbacks:
                    break
                await asyncio.gather(*callbacks, return_exceptions=True)

    async def run_once(
        self,
        *,
        recovery: bool = False,
        scan_limit: int | None = -1,
    ) -> None:
        """Run one durable scan.

        ``recovery=True`` routes existing work through ``executor.recover``.
        Passing ``scan_limit=None`` scans the complete active set; the default
        uses the configured bounded batch size.
        """

        if scan_limit == -1:
            scan_limit = self.scan_limit
        invocation_ids = await self._scan_invocation_ids(
            scan_limit=scan_limit,
        )
        if recovery:
            self._recovery_pending.update(invocation_ids)
        callbacks = await self._schedule(invocation_ids, recovery=recovery)
        if callbacks:
            await self._await_callbacks(callbacks)

    @staticmethod
    async def _await_callbacks(
        callbacks: tuple[asyncio.Task[None], ...],
    ) -> None:
        """Delay caller cancellation until executor callbacks have settled."""

        waiter = asyncio.gather(*callbacks, return_exceptions=True)
        await finish_awaitable(waiter)

    async def _scan_invocation_ids(
        self,
        *,
        scan_limit: int | None,
    ) -> list[int]:
        now = time.monotonic()
        delayed_ids = [
            invocation_id
            for invocation_id, retry_at in self._retry_not_before.items()
            if retry_at > now
        ]
        priority = case(
            (CapabilityInvocation.status == "cancelling", 0),
            (CapabilityInvocation.status == "running", 1),
            (CapabilityInvocation.status == "waiting_user", 2),
            else_=3,
        )
        statement = (
            select(CapabilityInvocation.id)
            # Feature flags gate the transaction that creates an Invocation,
            # never execution of work already durably admitted.  In
            # particular, a crash may leave a queued Execution before its
            # adapter has atomically staged and claimed the external handle.
            .where(CapabilityInvocation.status.in_(_POLLABLE_STATUSES))
            .order_by(priority, CapabilityInvocation.created_at, CapabilityInvocation.id)
        )
        if delayed_ids:
            statement = statement.where(
                CapabilityInvocation.id.not_in(delayed_ids)
            )
        if scan_limit is not None:
            statement = statement.limit(scan_limit)
        async with self.db_factory() as db:
            return list((await db.scalars(statement)).all())

    async def _schedule(
        self,
        invocation_ids: list[int],
        *,
        recovery: bool,
    ) -> tuple[asyncio.Task[None], ...]:
        callbacks: list[asyncio.Task[None]] = []
        now = time.monotonic()
        async with self._dispatch_lock:
            for invocation_id in invocation_ids:
                existing = self._inflight.get(invocation_id)
                if existing is not None and not existing.done():
                    callbacks.append(existing)
                    continue
                if self._retry_not_before.get(invocation_id, 0.0) > now:
                    continue
                callback = asyncio.create_task(
                    self._run_invocation(invocation_id, recovery=recovery),
                    name=f"capability-invocation-{invocation_id}",
                )
                self._inflight[invocation_id] = callback
                # Normal successful polling is rate-limited too, preventing
                # concurrent wake/timer ticks from immediately duplicating a
                # callback that happened to finish quickly.
                self._retry_not_before[invocation_id] = (
                    now + self.poll_interval_seconds
                )
                callback.add_done_callback(
                    lambda finished, invocation_id=invocation_id: (
                        self._discard_inflight(invocation_id, finished)
                    )
                )
                callbacks.append(callback)
        return tuple(dict.fromkeys(callbacks))

    def _discard_inflight(
        self,
        invocation_id: int,
        callback: asyncio.Task[None],
    ) -> None:
        if self._inflight.get(invocation_id) is callback:
            self._inflight.pop(invocation_id, None)
        lock = self._invocation_locks.get(invocation_id)
        if (
            invocation_id not in self._retry_not_before
            and lock is not None
            and not lock.locked()
        ):
            self._invocation_locks.pop(invocation_id, None)

    async def _run_invocation(self, invocation_id: int, *, recovery: bool) -> None:
        async with self._semaphore:
            if self._stop_event.is_set() and not recovery:
                return
            lock = self._invocation_locks.setdefault(invocation_id, asyncio.Lock())
            async with lock:
                try:
                    await self._process_invocation(
                        invocation_id,
                        recovery=(recovery or invocation_id in self._recovery_pending),
                    )
                except asyncio.CancelledError:
                    raise
                except CapabilityConflictError as exc:
                    # A concurrent process may have won the state CAS.  Re-read
                    # after a short bounded delay instead of treating that as
                    # an executor failure.
                    self._record_retry(invocation_id)
                    logger.debug(
                        "Capability invocation %s changed concurrently: %s",
                        invocation_id,
                        exc,
                    )
                except _PERMANENT_EXECUTOR_ERRORS as exc:
                    await self._fail_permanently(
                        invocation_id,
                        error_code="coordinator_executor_rejected",
                        error_message=str(exc),
                    )
                except Exception as exc:
                    # Unexpected adapter/DB errors retain the same durable
                    # execution attempt.  Calling fail_execution here could
                    # duplicate an external handle created immediately before
                    # a transport error.  Idempotent recover/observe is retried
                    # with exponential, capped backoff instead.
                    self._record_retry(invocation_id)
                    logger.warning(
                        "Capability executor callback failed for invocation %s; "
                        "will retry",
                        invocation_id,
                        exc_info=exc,
                    )
                else:
                    self._failure_counts.pop(invocation_id, None)
                    self._recovery_pending.discard(invocation_id)

    async def _process_invocation(
        self,
        invocation_id: int,
        *,
        recovery: bool,
    ) -> None:
        async with self.db_factory() as db:
            invocation = await db.get(CapabilityInvocation, invocation_id)
            if invocation is None or invocation.status not in _POLLABLE_STATUSES:
                self._forget(invocation_id)
                return

            execution = await active_execution_for(db, invocation.id)

            definition = resolve_capability(invocation.capability_key)
            unavailable_reason = None
            if definition is None:
                unavailable_reason = (
                    f"Capability {invocation.capability_key!r} is not registered"
                )
            elif definition.executor_kind != invocation.executor_kind:
                unavailable_reason = (
                    "Registered executor kind does not match the Invocation "
                    f"snapshot ({definition.executor_kind!r} != "
                    f"{invocation.executor_kind!r})"
                )
            elif definition.executor is None:
                unavailable_reason = (
                    f"Capability {invocation.capability_key!r} has no executor"
                )

            if unavailable_reason is not None:
                await self._fail_or_record_unavailable(
                    db,
                    invocation=invocation,
                    execution=execution,
                    error_message=unavailable_reason,
                )
                self._recovery_pending.discard(invocation_id)
                if execution is not None:
                    self._forget(invocation_id)
                return

            executor = definition.executor
            if invocation.status == "cancelling":
                callback = getattr(executor, "cancel", None)
            elif recovery:
                callback = getattr(executor, "recover", None)
            elif invocation.status == "queued":
                callback = getattr(executor, "ensure_started", None)
            else:
                callback = getattr(executor, "observe", None)
            if not callable(callback):
                await self._fail_or_record_unavailable(
                    db,
                    invocation=invocation,
                    execution=execution,
                    error_message=(
                        f"Executor for {invocation.capability_key!r} does not "
                        "implement the required callback"
                    ),
                )
                self._recovery_pending.discard(invocation_id)
                if execution is not None:
                    self._forget(invocation_id)
                return

            try:
                await callback(db, invocation_id=invocation.id)
                await db.refresh(invocation)
                if invocation.status not in _POLLABLE_STATUSES:
                    self._forget(invocation_id)
            except BaseException:
                await db.rollback()
                raise

    async def _fail_or_record_unavailable(
        self,
        db: AsyncSession,
        *,
        invocation: CapabilityInvocation,
        execution: Any,
        error_message: str,
    ) -> None:
        if execution is not None:
            await fail_execution(
                db,
                invocation_id=invocation.id,
                expected_invocation_version=invocation.state_version,
                expected_execution_version=execution.state_version,
                error_code="coordinator_executor_unavailable",
                error_message=error_message,
                retry=False,
            )
            return
        await self._record_blocked_invocation(
            db,
            invocation=invocation,
            error_code="coordinator_executor_unavailable",
            error_message=error_message,
        )

    async def _fail_permanently(
        self,
        invocation_id: int,
        *,
        error_code: str,
        error_message: str,
    ) -> None:
        async with self.db_factory() as db:
            invocation = await db.get(CapabilityInvocation, invocation_id)
            if invocation is None or invocation.status not in _POLLABLE_STATUSES:
                self._forget(invocation_id)
                return
            execution = await active_execution_for(db, invocation.id)
            if execution is None:
                await self._record_blocked_invocation(
                    db,
                    invocation=invocation,
                    error_code=error_code,
                    error_message=error_message,
                )
                return
            try:
                await fail_execution(
                    db,
                    invocation_id=invocation.id,
                    expected_invocation_version=invocation.state_version,
                    expected_execution_version=execution.state_version,
                    error_code=error_code,
                    error_message=error_message,
                    retry=False,
                )
            except CapabilityConflictError:
                await db.rollback()
                self._record_retry(invocation_id)
            else:
                self._forget(invocation_id)

    async def _record_blocked_invocation(
        self,
        db: AsyncSession,
        *,
        invocation: CapabilityInvocation,
        error_code: str,
        error_message: str,
    ) -> None:
        bounded_message = error_message[:4000]
        if (
            invocation.error_code == error_code
            and invocation.error_message == bounded_message
        ):
            await db.rollback()
            return
        invocation.error_code = error_code[:64]
        invocation.error_message = bounded_message
        invocation.state_version += 1
        invocation.updated_at = datetime.utcnow()
        await db.commit()
        await broadcast_capability_event(
            "capability_invocation_blocked",
            invocation,
        )

    def _record_retry(self, invocation_id: int) -> None:
        failures = self._failure_counts.get(invocation_id, 0) + 1
        self._failure_counts[invocation_id] = failures
        exponent = min(failures - 1, 20)
        delay = min(
            self.max_backoff_seconds,
            self.initial_backoff_seconds * (2**exponent),
        )
        self._retry_not_before[invocation_id] = time.monotonic() + delay
        self._recovery_pending.add(invocation_id)

    def _forget(self, invocation_id: int) -> None:
        self._failure_counts.pop(invocation_id, None)
        self._retry_not_before.pop(invocation_id, None)
        self._recovery_pending.discard(invocation_id)
        lock = self._invocation_locks.get(invocation_id)
        if lock is not None and not lock.locked():
            self._invocation_locks.pop(invocation_id, None)
