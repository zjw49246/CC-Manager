"""Background watcher for native sub-agents.

Periodically polls transcript files of running native sub-agents
(spawned by Claude Code's Agent tool with run_in_background=true)
to track their progress and detect completion — independent of the
main session's event stream.

Similar to how MonitorDispatcher manages $monitor sessions, but for
native background agents whose transcripts live on disk.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Callable

from backend.services.ws_broadcaster import WebSocketBroadcaster

logger = logging.getLogger(__name__)

POLL_INTERVAL = 12  # seconds between transcript checks
IDLE_THRESHOLD = 60  # seconds of no transcript growth → consider completed
MAX_SUMMARY_LEN = 2000


class SubAgentWatcher:
    def __init__(self, db_factory: Callable, broadcaster: WebSocketBroadcaster):
        self.db_factory = db_factory
        self.broadcaster = broadcaster
        self._task: asyncio.Task | None = None
        # session_id -> {last_size, idle_since, agent_id, task_id, jsonl_path}
        self._tracked: dict[int, dict] = {}

    def start(self):
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._poll_loop())
            logger.info("SubAgentWatcher started")

    def stop(self):
        """Request shutdown without waiting (legacy compatibility)."""
        if self._task and not self._task.done():
            self._task.cancel()

    async def shutdown(self):
        """Cancel and await the poller so it cannot outlive app shutdown."""
        task = self._task
        if task is None:
            return
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        if self._task is task:
            self._task = None

    async def _poll_loop(self):
        while True:
            try:
                await self._tick()
            except asyncio.CancelledError:
                return
            except Exception:
                logger.debug("SubAgentWatcher tick error", exc_info=True)
            await asyncio.sleep(POLL_INTERVAL)

    async def _tick(self):
        from sqlalchemy import select
        from backend.models.sub_agent import SubAgentSession, SubAgentReport
        from backend.models.task import Task
        from backend.models.log_entry import LogEntry

        async with self.db_factory() as db:
            result = await db.execute(
                select(SubAgentSession, Task.session_id)
                .join(Task, Task.id == SubAgentSession.task_id)
                .where(
                    SubAgentSession.source == "native",
                    SubAgentSession.status == "running",
                    # Codex native children are observed authoritatively from
                    # app-server thread notifications.  They have no Claude
                    # transcript path and must not enter the idle-file poller.
                    SubAgentSession.provider != "codex",
                    SubAgentSession.agent_type.in_(["native-agent", "native-monitor"]),
                )
            )
            running = result.all()

        if not running:
            self._tracked.clear()
            return

        for sa, task_session_id in running:
            sid = sa.id
            if sid not in self._tracked:
                # Resolve transcript path from meta
                info = self._resolve_paths(sa, task_session_id)
                if not info:
                    continue
                self._tracked[sid] = {
                    "task_id": sa.task_id,
                    "agent_id": info["agent_id"],
                    "jsonl_path": info["jsonl_path"],
                    "last_size": 0,
                    "idle_since": None,
                    "description": sa.description,
                }

            tracked = self._tracked.get(sid)
            if not tracked:
                continue

            jsonl_path = tracked["jsonl_path"]
            if not os.path.exists(jsonl_path):
                continue

            try:
                current_size = os.path.getsize(jsonl_path)
            except OSError:
                continue

            if current_size > tracked["last_size"]:
                # Transcript grew — read new content for progress
                summary = self._read_latest_summary(jsonl_path, tracked["last_size"])
                tracked["last_size"] = current_size
                tracked["idle_since"] = None

                if summary:
                    async with self.db_factory() as db:
                        from backend.services.worker_node_control import (
                            WorkerNodeDrainingConflict,
                            fence_worker_node_mutation,
                        )

                        try:
                            await fence_worker_node_mutation(db)
                        except WorkerNodeDrainingConflict:
                            await db.rollback()
                            continue
                        sa_obj = await db.get(SubAgentSession, sid)
                        if not sa_obj or sa_obj.status != "running":
                            continue
                        sa_obj.checks_done = (sa_obj.checks_done or 0) + 1
                        sa_obj.last_summary = summary[:MAX_SUMMARY_LEN]

                        db.add(SubAgentReport(
                            session_id=sid,
                            check_number=sa_obj.checks_done,
                            status="running",
                            summary=summary[:MAX_SUMMARY_LEN],
                        ))

                        db.add(LogEntry(
                            instance_id=None,
                            task_id=tracked["task_id"],
                            event_type="system_event",
                            content=f"[Agent #{sid}] {tracked['description']}: {summary[:300]}",
                            is_error=False,
                        ))
                        await db.commit()

                    await self.broadcaster.broadcast(f"task:{tracked['task_id']}", {
                        "event_type": "sub_agent_report",
                        "sub_agent_session_id": sid,
                        "agent_type": "native-agent",
                        "check_number": sa_obj.checks_done if sa_obj else 1,
                        "summary": summary[:MAX_SUMMARY_LEN],
                    })
            else:
                # No growth
                if tracked["idle_since"] is None:
                    tracked["idle_since"] = datetime.utcnow()
                else:
                    idle_secs = (datetime.utcnow() - tracked["idle_since"]).total_seconds()
                    if idle_secs >= IDLE_THRESHOLD:
                        # Silence is not proof of exit: a live agent can spend
                        # minutes inside one Bash/tool call without appending to
                        # its transcript.  Only Claude's explicit final
                        # assistant end_turn marker is safe to interpret as
                        # native-agent completion.
                        if self._transcript_has_terminal_event(jsonl_path):
                            await self._mark_completed(sid, tracked)

        # Clean up tracked entries for sessions no longer running
        running_ids = {sa.id for sa, _session_id in running}
        for sid in list(self._tracked):
            if sid not in running_ids:
                del self._tracked[sid]

    @staticmethod
    def _candidate_config_dirs() -> list[Path]:
        """Return every configured/default Claude home without opening the DB."""

        from backend.config import settings

        candidates: list[Path] = []
        env_dir = os.environ.get("CLAUDE_CONFIG_DIR")
        if env_dir:
            candidates.append(Path(os.path.expanduser(env_dir)))
        candidates.append(Path.home() / ".claude")

        pool_path = Path(
            os.path.expandvars(os.path.expanduser(settings.pool_config_path))
        )
        try:
            pool_data = json.loads(pool_path.read_text(encoding="utf-8"))
            for account in pool_data.get("accounts", []):
                config_dir = account.get("config_dir")
                if isinstance(config_dir, str) and config_dir:
                    candidates.append(
                        Path(
                            os.path.expandvars(
                                os.path.expanduser(config_dir)
                            )
                        )
                    )
        except (OSError, json.JSONDecodeError, TypeError):
            pass

        cloudrouter_root = Path(
            os.path.expandvars(
                os.path.expanduser(settings.cloudrouter_accounts_dir)
            )
        )
        try:
            candidates.extend(
                account_dir / "claude"
                for account_dir in cloudrouter_root.iterdir()
                if account_dir.is_dir()
            )
        except OSError:
            pass

        try:
            candidates.extend(
                path
                for path in Path.home().iterdir()
                if path.name.startswith(".claude") and path.is_dir()
            )
        except OSError:
            pass

        unique: list[Path] = []
        seen: set[str] = set()
        for candidate in candidates:
            spelling = str(candidate)
            if spelling in seen:
                continue
            seen.add(spelling)
            unique.append(candidate)
        return unique

    def _resolve_paths(self, sa, session_id: str | None) -> dict | None:
        """Resolve agent_id and transcript path from sub-agent meta + task session."""
        try:
            meta = json.loads(sa.meta) if sa.meta else {}
        except (json.JSONDecodeError, TypeError):
            return None

        tool_use_id = meta.get("tool_use_id")
        if not tool_use_id:
            return None

        if not session_id:
            return None

        # Search for subagents directory in claude config dirs
        for config_dir in self._candidate_config_dirs():
            projects_dir = config_dir / "projects"
            if not projects_dir.exists():
                continue
            for project_dir in projects_dir.iterdir():
                sa_dir = project_dir / session_id / "subagents"
                if not sa_dir.exists():
                    continue
                # Find meta.json matching tool_use_id
                for meta_file in sa_dir.glob("*.meta.json"):
                    try:
                        with open(meta_file) as f:
                            file_meta = json.load(f)
                        if file_meta.get("toolUseId", "").startswith(tool_use_id[:20]):
                            agent_id = meta_file.name.replace(".meta.json", "").replace("agent-", "")
                            jsonl_path = str(sa_dir / f"agent-{agent_id}.jsonl")
                            return {"agent_id": agent_id, "jsonl_path": jsonl_path}
                    except (OSError, json.JSONDecodeError):
                        continue
        return None

    def _read_latest_summary(self, jsonl_path: str, from_offset: int) -> str | None:
        """Read new lines from transcript and extract the latest assistant text."""
        try:
            last_text = ""
            with open(jsonl_path, encoding="utf-8") as f:
                f.seek(max(0, from_offset))
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        raw = json.loads(line)
                        msg = raw.get("message", {})
                        if raw.get("type") in ("assistant", "message"):
                            for block in (msg.get("content") or []):
                                if isinstance(block, dict) and block.get("type") == "text":
                                    last_text = block["text"]
                                elif isinstance(block, dict) and block.get("type") == "tool_use":
                                    last_text = f"[{block.get('name', 'tool')}]"
                    except json.JSONDecodeError:
                        continue
            return last_text[:MAX_SUMMARY_LEN] if last_text else None
        except OSError:
            return None

    @staticmethod
    def _transcript_has_terminal_event(jsonl_path: str) -> bool:
        """Return True only for an explicit final assistant ``end_turn``."""

        try:
            last_event = None
            with open(jsonl_path, encoding="utf-8") as stream:
                for line in stream:
                    try:
                        parsed = json.loads(line)
                    except (json.JSONDecodeError, TypeError):
                        continue
                    if isinstance(parsed, dict):
                        last_event = parsed
            if not isinstance(last_event, dict):
                return False
            message = last_event.get("message")
            return bool(
                last_event.get("type") == "assistant"
                and isinstance(message, dict)
                and message.get("role") == "assistant"
                and message.get("stop_reason") == "end_turn"
            )
        except OSError:
            return False

    async def _mark_completed(self, sid: int, tracked: dict):
        """Mark a sub-agent as completed."""
        from backend.models.sub_agent import SubAgentSession

        # Read final summary
        final_summary = self._read_latest_summary(tracked["jsonl_path"], 0)

        async with self.db_factory() as db:
            from backend.services.worker_node_control import (
                WorkerNodeDrainingConflict,
                fence_worker_node_mutation,
            )

            try:
                await fence_worker_node_mutation(db)
            except WorkerNodeDrainingConflict:
                await db.rollback()
                return
            sa = await db.get(SubAgentSession, sid)
            if not sa or sa.status != "running":
                return
            sa.status = "completed"
            sa.completed_at = datetime.utcnow()
            if final_summary:
                sa.last_summary = final_summary[:MAX_SUMMARY_LEN]
            await db.commit()

        await self.broadcaster.broadcast(f"task:{tracked['task_id']}", {
            "event_type": "sub_agent_session_status",
            "sub_agent_session_id": sid,
            "agent_type": "native-agent",
            "status": "completed",
        })

        if sid in self._tracked:
            del self._tracked[sid]

        logger.info("SubAgentWatcher: marked SA %d as completed", sid)
