"""SharedRelay — real-time event relay from sharer CCMs for shared tasks.

Each active shared task gets a persistent WebSocket connection to the
sharer's /ws/shared endpoint. Events are written to local log_entries
and broadcast to the local frontend, making shadow tasks behave like
local tasks in the UI.
"""

import asyncio
import json
import logging

import httpx
import websockets
from sqlalchemy import select

from backend.models.log_entry import LogEntry
from backend.models.task import Task
from backend.models.task_share import SharedTaskReceived
from backend.services.chat_event_identity import persisted_chat_event

logger = logging.getLogger(__name__)

CHAT_EVENT_TYPES = {
    "user_message", "message", "result", "tool_use", "tool_result",
    "system_init", "system_event", "thinking", "process_exit",
}


class SharedRelay:
    def __init__(self, db_factory, broadcaster):
        self.db_factory = db_factory
        self.broadcaster = broadcaster
        self._connections: dict[int, object] = {}  # shared_received.id -> ws
        self._loops: dict[int, asyncio.Task] = {}  # shared_received.id -> relay task
        self._closing: set[int] = set()
        self._my_name: str | None = None

    async def start_relay(self, shared: SharedTaskReceived):
        """Start relay for a shared task. Idempotent."""
        if shared.id in self._connections or shared.id in self._loops:
            return
        from backend.services.shared_shadow import lock_owned_shadow

        async with self.db_factory() as db:
            owned = await lock_owned_shadow(db, shared)
            if owned is None:
                return
            _, shadow = owned
            shared.local_task_id = shadow.id
        self._closing.discard(shared.id)
        loop_task = asyncio.create_task(self._connect_and_relay(shared))
        self._loops[shared.id] = loop_task

        def _forget_finished(done: asyncio.Task, shared_id: int = shared.id):
            if self._loops.get(shared_id) is done:
                self._loops.pop(shared_id, None)

        loop_task.add_done_callback(_forget_finished)

    async def stop_relay(self, shared_id: int):
        """Stop relay for a shared task."""
        self._closing.add(shared_id)
        ws = self._connections.pop(shared_id, None)
        loop_task = self._loops.pop(shared_id, None)
        if ws is not None:
            try:
                await ws.close()
            except Exception:
                pass
        if loop_task is not None:
            loop_task.cancel()
            if loop_task is not asyncio.current_task():
                await asyncio.gather(loop_task, return_exceptions=True)

    async def recover_all(self):
        """Restart relays for all active shared tasks (called on startup).

        Also backfills shadow tasks for legacy shared records that predate
        the relay feature (local_task_id is NULL).
        """
        # Load my feishu name for self-message dedup
        try:
            from backend.models.feishu_binding import FeishuUserBinding
            async with self.db_factory() as db:
                binding = (await db.execute(select(FeishuUserBinding).limit(1))).scalar_one_or_none()
                if binding:
                    self._my_name = binding.feishu_name
        except Exception:
            pass

        async with self.db_factory() as db:
            result = await db.execute(
                select(SharedTaskReceived).where(
                    SharedTaskReceived.status == "active",
                )
            )
            all_active = result.scalars().all()

        for shared in all_active:
            # Backfill shadow task for legacy records
            if not shared.local_task_id:
                try:
                    await self._create_shadow_task(shared)
                except Exception:
                    logger.debug("failed to create shadow task for shared %d", shared.id)
                    continue

            try:
                await self.start_relay(shared)
            except Exception:
                logger.debug("recover relay for shared %d failed", shared.id)

    async def _create_shadow_task(self, shared: SharedTaskReceived):
        """Create a local shadow task for a shared record that doesn't have one."""
        from backend.models.task import Task
        from backend.services.shared_shadow import lock_shared_record
        from backend.services.task_creation import stage_task_record

        async with self.db_factory() as db:
            shared_record = await lock_shared_record(db, shared)
            if shared_record is None:
                return
            shadow = None
            if shared_record.local_task_id is not None:
                shadow = (
                    await db.execute(
                        select(Task).where(
                            Task.id == shared_record.local_task_id,
                            Task.shared_from_id == shared_record.id,
                        )
                    )
                ).scalar_one_or_none()
            if shadow is None:
                shadow = await stage_task_record(
                    db,
                    title=shared_record.task_title or "",
                    description=shared_record.task_description,
                    status="pending",
                    shared_from_id=shared_record.id,
                )
                shared_record.local_task_id = shadow.id
            await db.commit()
            shared.local_task_id = shadow.id
            logger.info("created shadow task %d for shared %d", shadow.id, shared.id)

        # Fetch live config and backfill
        try:
            from backend.services.shared_proxy import proxy_config
            config = await proxy_config(shared.owner_ccm_url, shared.remote_task_id, shared.share_token)
            async with self.db_factory() as db:
                from backend.services.shared_shadow import lock_owned_shadow

                owned = await lock_owned_shadow(db, shared)
                if owned is not None and config:
                    _, shadow = owned
                    shadow.status = config.get("status", "pending")
                    shadow.title = config.get("title") or shadow.title
                    shadow.description = config.get("description") or shadow.description
                    shadow.model = config.get("model")
                    shadow.provider = config.get("provider", "claude")
                    shadow.session_id = config.get("session_id") or shadow.session_id
                    shadow.target_repo = config.get("target_repo")
                    shadow.error_message = config.get("error_message")
                    shadow.attention_tag = config.get("attention_tag")
                    await db.commit()
        except Exception:
            logger.debug("failed to fetch config for shadow task shared=%d", shared.id)

        try:
            await self.backfill_history(shared)
        except Exception:
            logger.debug("backfill failed for shared %d", shared.id)

    async def _connect_and_relay(self, shared: SharedTaskReceived):
        """Connect to sharer's WS and relay events. Auto-reconnects."""
        ws_url = (
            shared.owner_ccm_url.replace("https://", "wss://").replace("http://", "ws://")
            + f"/ws/shared?token={shared.share_token}&task_id={shared.remote_task_id}"
        )
        for attempt in range(100):
            if shared.id in self._closing:
                return
            from backend.services.shared_shadow import lock_owned_shadow

            async with self.db_factory() as db:
                if await lock_owned_shadow(db, shared) is None:
                    return
            try:
                async with websockets.connect(ws_url, open_timeout=15) as ws:
                    self._connections[shared.id] = ws
                    logger.info("shared relay connected: shared=%d remote_task=%d url=%s", shared.id, shared.remote_task_id, ws_url[:80])
                    try:
                        while True:
                            raw = await ws.recv()
                            try:
                                parsed = json.loads(raw)
                                if parsed.get("action") == "subscribed":
                                    logger.info("shared relay subscribed shared=%d", shared.id)
                                    continue
                                et = (parsed.get("data") or parsed).get("event_type") or (parsed.get("data") or parsed).get("event") or "?"
                                logger.info("shared relay recv shared=%d event=%s", shared.id, et)
                                await self._handle(parsed, shared)
                            except Exception:
                                logger.exception("shared relay handle error shared=%d", shared.id)
                    except websockets.ConnectionClosed as e:
                        logger.warning("shared relay closed shared=%d: %s", shared.id, e)
                    except OSError as e:
                        logger.warning("shared relay OS error shared=%d: %s", shared.id, e)
                    except Exception as e:
                        logger.exception("shared relay unexpected error shared=%d", shared.id)
            except asyncio.CancelledError:
                return
            except Exception:
                pass
            finally:
                self._connections.pop(shared.id, None)

            if shared.id in self._closing:
                return
            delay = min(2 ** attempt, 60)
            logger.debug("shared relay reconnecting shared=%d in %ds", shared.id, delay)
            await asyncio.sleep(delay)

        logger.warning("shared relay gave up reconnecting shared=%d", shared.id)

    async def _handle(self, msg: dict, shared: SharedTaskReceived):
        """Process one WS message from the sharer."""
        data = msg.get("data", msg)
        if not isinstance(data, dict):
            return
        if data.get("action") == "subscribed":
            return

        event_type = data.get("event_type") or data.get("event")
        if not event_type:
            return

        # user_message: skip self-sent (already stored locally with prefix by _send_shared_chat).
        # Relay messages from sharer or other shared users (different prefix or no prefix).
        if event_type == "user_message":
            content = data.get("content") or ""
            if self._my_name and content.startswith(f"[{self._my_name}]"):
                return  # self-sent, already stored locally

        # Every mutation is fenced by both sides of the mapping.  In
        # particular, never trust the detached relay object's local_task_id:
        # the old shadow may have been deleted and its id explicitly reused.
        from backend.services.shared_shadow import lock_owned_shadow

        persisted_data = None
        async with self.db_factory() as db:
            owned = await lock_owned_shadow(db, shared)
            if owned is None:
                return
            _, shadow = owned
            local_task_id = shadow.id

            # Write chat events to local log_entries
            entry = None
            if event_type in CHAT_EVENT_TYPES:
                raw_json = data.get("raw_json")
                metadata_keys = (
                    "attachments", "image_urls", "source", "raw_content", "sender_name",
                )
                if not raw_json and any(data.get(k) is not None for k in metadata_keys):
                    raw_json = json.dumps({
                        k: data[k] for k in metadata_keys if data.get(k) is not None
                    })
                entry = LogEntry(
                    instance_id=None,
                    task_id=local_task_id,
                    event_type=event_type,
                    role=data.get("role"),
                    content=data.get("content"),
                    tool_name=data.get("tool_name"),
                    tool_input=data.get("tool_input"),
                    tool_output=data.get("tool_output"),
                    raw_json=raw_json,
                    is_error=data.get("is_error", False),
                )
                db.add(entry)
                if data.get("role") == "assistant" and event_type in ("message", "result"):
                    shadow.has_unread = True

            # Sync status changes under the same ownership proof.
            if event_type == "status_change":
                new_status = data.get("new_status")
                if new_status:
                    shadow.status = new_status
                    if data.get("error_message"):
                        shadow.error_message = data["error_message"]

            await db.commit()
            if entry is not None:
                persisted_data = persisted_chat_event(
                    entry,
                    {
                        key: value
                        for key, value in data.items()
                        if key != "raw_json"
                    },
                    provider=shadow.provider,
                )

        # Broadcast to local frontend (mirror the event on local task channel)
        await self.broadcaster.broadcast(
            f"task:{local_task_id}",
            persisted_data or data,
        )

    async def backfill_history(self, shared: SharedTaskReceived):
        """Pull full chat history from sharer and store locally."""
        if not shared.local_task_id:
            return
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(
                    f"{shared.owner_ccm_url}/api/shared-access/{shared.remote_task_id}/history",
                    params={"token": shared.share_token},
                )
                resp.raise_for_status()
                messages = resp.json()

            async with self.db_factory() as db:
                from backend.services.shared_shadow import lock_owned_shadow

                owned = await lock_owned_shadow(db, shared)
                if owned is None:
                    return
                _, shadow = owned
                local_task_id = shadow.id
                # Check if we already have entries
                existing = await db.execute(
                    select(LogEntry.id).where(LogEntry.task_id == local_task_id).limit(1)
                )
                if existing.scalar_one_or_none():
                    return  # already backfilled

                for msg in messages:
                    metadata = {
                        k: msg[k]
                        for k in ("attachments", "image_urls", "source", "raw_content", "sender_name")
                        if msg.get(k) is not None
                    }
                    db.add(LogEntry(
                        instance_id=None,
                        task_id=local_task_id,
                        event_type=msg.get("event_type", "message"),
                        role=msg.get("role"),
                        content=msg.get("content"),
                        tool_name=msg.get("tool_name"),
                        tool_input=msg.get("tool_input"),
                        tool_output=msg.get("tool_output"),
                        raw_json=json.dumps(metadata) if metadata else None,
                        is_error=msg.get("is_error", False),
                    ))
                await db.commit()
                logger.info("backfilled %d entries for shared task %d", len(messages), local_task_id)
        except Exception:
            logger.debug("backfill failed for shared %d", shared.id)
