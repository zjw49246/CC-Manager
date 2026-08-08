"""Bounded async admission and deadlines for blocking Paramiko SFTP work."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any
from weakref import WeakKeyDictionary

from backend.config import settings


class SSHSFTPBusyError(TimeoutError):
    """No managed SFTP execution slot became available in time."""


class SSHSFTPOperationTimeout(TimeoutError):
    """The caller deadline expired while the blocking worker was still live."""


_loop_semaphores: WeakKeyDictionary = WeakKeyDictionary()


def _semaphore() -> asyncio.Semaphore:
    loop = asyncio.get_running_loop()
    limit = max(1, int(settings.ssh_sftp_max_concurrency))
    current = _loop_semaphores.get(loop)
    if current is None or getattr(current, "_ccm_limit", None) != limit:
        current = asyncio.Semaphore(limit)
        setattr(current, "_ccm_limit", limit)
        _loop_semaphores[loop] = current
    return current


def configure_sftp_channel_timeout(sftp) -> None:
    timeout = max(1.0, float(settings.ssh_sftp_channel_timeout_seconds))
    channel = sftp.get_channel()
    channel.settimeout(timeout)


async def run_bounded_sftp(
    function: Callable[..., Any],
    *args: Any,
    operation_timeout: float | None = None,
    abandoned_result_cleanup: Callable[[Any], None] | None = None,
) -> Any:
    """Run one blocking SFTP operation without releasing its slot early.

    ``asyncio`` cannot force-stop a Paramiko worker thread. On timeout or
    cancellation the slot remains held until that exact thread exits, keeping
    repeated stalled calls from bypassing the global concurrency cap.
    """

    semaphore = _semaphore()
    try:
        await asyncio.wait_for(
            semaphore.acquire(),
            timeout=max(0.1, float(settings.ssh_sftp_queue_timeout_seconds)),
        )
    except TimeoutError as exc:
        raise SSHSFTPBusyError("Managed SFTP capacity is busy") from exc

    worker = asyncio.create_task(asyncio.to_thread(function, *args))
    state = {"abandoned": False, "cleaned": False}

    def cleanup_abandoned_result(completed: asyncio.Task) -> None:
        if (
            not state["abandoned"]
            or state["cleaned"]
            or abandoned_result_cleanup is None
            or completed.cancelled()
        ):
            return
        try:
            result = completed.result()
        except BaseException:
            return
        state["cleaned"] = True
        try:
            abandoned_result_cleanup(result)
        except Exception:
            # Cleanup is best-effort and must not destabilize the event loop's
            # done-callback path. Callers choose cleanup only for temp results.
            return

    def release_slot(completed: asyncio.Task) -> None:
        semaphore.release()
        cleanup_abandoned_result(completed)

    worker.add_done_callback(release_slot)
    timeout = (
        float(settings.ssh_sftp_operation_timeout_seconds)
        if operation_timeout is None
        else float(operation_timeout)
    )
    try:
        return await asyncio.wait_for(asyncio.shield(worker), timeout=max(0.1, timeout))
    except asyncio.CancelledError:
        state["abandoned"] = True
        if worker.done():
            cleanup_abandoned_result(worker)
        raise
    except TimeoutError as exc:
        state["abandoned"] = True
        if worker.done():
            cleanup_abandoned_result(worker)
        raise SSHSFTPOperationTimeout("Managed SFTP operation timed out") from exc
