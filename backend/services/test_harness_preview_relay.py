"""Fixed-target TCP relay for exposing an isolated Harness preview.

The source container remains attached only to a Docker ``--internal`` network.
This manager-owned relay is the sole container with a loopback-published port,
and its upstream is deliberately fixed to ``source:4173``.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress


_LISTEN_PORT = 4173
_UPSTREAM_HOST = "source"
_UPSTREAM_PORT = 4173
_MAX_BYTES_PER_DIRECTION = 1024**3


async def _copy_limited(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    total = 0
    while True:
        chunk = await reader.read(64 * 1024)
        if not chunk:
            return
        total += len(chunk)
        if total > _MAX_BYTES_PER_DIRECTION:
            return
        writer.write(chunk)
        await writer.drain()


async def _handle(
    downstream_reader: asyncio.StreamReader,
    downstream_writer: asyncio.StreamWriter,
) -> None:
    upstream_writer: asyncio.StreamWriter | None = None
    tasks: set[asyncio.Task[None]] = set()
    try:
        upstream_reader, upstream_writer = await asyncio.wait_for(
            asyncio.open_connection(_UPSTREAM_HOST, _UPSTREAM_PORT),
            timeout=5,
        )
        tasks = {
            asyncio.create_task(
                _copy_limited(downstream_reader, upstream_writer)
            ),
            asyncio.create_task(
                _copy_limited(upstream_reader, downstream_writer)
            ),
        }
        _, pending = await asyncio.wait(
            tasks,
            timeout=1200,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
    except (asyncio.TimeoutError, OSError):
        pass
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if upstream_writer is not None and not upstream_writer.is_closing():
            upstream_writer.close()
            with suppress(Exception):
                await upstream_writer.wait_closed()
        if not downstream_writer.is_closing():
            downstream_writer.close()
            with suppress(Exception):
                await downstream_writer.wait_closed()


async def main() -> None:
    server = await asyncio.start_server(
        _handle,
        host="0.0.0.0",
        port=_LISTEN_PORT,
        limit=64 * 1024,
    )
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
