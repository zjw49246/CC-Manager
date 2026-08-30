"""Cancellation-safe waiting for finite lifecycle operations.

Safety-critical cleanup commonly has to outlive cancellation of its HTTP or
dispatcher caller.  Repeatedly creating ``asyncio.shield()`` futures while the
current Task still has an outstanding cancellation request is not safe on
Python 3.14: each new shield is cancelled immediately and the loop can spin at
100% CPU, starving the operation it is meant to protect.

The helpers here consume each delivered cancellation request with
``Task.uncancel()`` while the protected operation settles.  Callers retain the
first ``CancelledError`` and can re-raise it after their durable invariant is
restored.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import TypeVar

from anyio import CancelScope


_T = TypeVar("_T")


def consume_current_task_cancellation() -> bool:
    """Consume one delivered cancellation request from the current Task.

    Returns ``False`` when the ``CancelledError`` came from an awaited child
    rather than cancellation of the current Task.
    """

    current = asyncio.current_task()
    if current is None or current.cancelling() <= 0:
        return False
    current.uncancel()
    return True


async def await_task_completion(
    operation: asyncio.Future[_T],
) -> asyncio.CancelledError | None:
    """Wait for ``operation`` without cancelling it or busy-spinning.

    The returned exception represents cancellation of the *current* Task.
    Cancellation originating from ``operation`` itself remains its result and
    is raised by ``operation.result()`` in the caller.
    """

    delayed_cancellation: asyncio.CancelledError | None = None
    first_wait = True
    while first_wait or not operation.done():
        # Even an already-settled Future needs one checkpoint.  Otherwise a
        # cancellation that was requested immediately before entering this
        # helper could be skipped and the caller would incorrectly observe a
        # successful result.
        checkpoint = first_wait
        first_wait = False
        try:
            if checkpoint:
                await asyncio.sleep(0)
                if operation.done():
                    # Do not retrieve a completed Task's cancellation through
                    # shield first: on Python 3.14 that consumes its original
                    # CancelledError message before the caller reads result().
                    break
            await asyncio.shield(operation)
        except asyncio.CancelledError as exc:
            # An inner cancellation is an operation result, not a caller
            # cancellation to defer.  A caller cancellation increments the
            # current Task's cancellation count before delivery.
            if not consume_current_task_cancellation():
                if operation.done():
                    break
                raise
            if delayed_cancellation is None:
                delayed_cancellation = exc
            # AnyIO uses level cancellation and may re-deliver at every
            # checkpoint while its outer scope remains cancelled.  After the
            # first delivery, wait passively in a nested AnyIO shield instead
            # of constructing immediately-cancelled asyncio shields.  A raw,
            # independent ``Task.cancel()`` still crosses this scope; consume
            # it below and resume the same finite settlement wait.
            try:
                with CancelScope(shield=True):
                    await asyncio.wait((operation,))
            except asyncio.CancelledError as later_exc:
                if not consume_current_task_cancellation():
                    if operation.done():
                        break
                    raise
                if delayed_cancellation is None:
                    delayed_cancellation = later_exc
        except BaseException:
            # The operation's exception is authoritative and is retrieved by
            # the caller via ``result()``.  Unexpected wrapper failures must
            # not be hidden while the operation is still live.
            if operation.done():
                break
            raise
    return delayed_cancellation


async def settle_awaitable(
    awaitable: Awaitable[_T],
) -> tuple[asyncio.Future[_T], asyncio.CancelledError | None]:
    """Schedule an awaitable and return it after finite settlement."""

    operation = asyncio.ensure_future(awaitable)
    cancellation = await await_task_completion(operation)
    return operation, cancellation


async def finish_awaitable(awaitable: Awaitable[_T]) -> _T:
    """Settle an awaitable, then re-deliver caller cancellation."""

    operation, cancellation = await settle_awaitable(awaitable)
    result = operation.result()
    if cancellation is not None:
        raise cancellation
    return result
