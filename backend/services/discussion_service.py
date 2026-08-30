"""Discussion service: Facilitator-driven multi-agent discussion."""
import asyncio
import json
import logging
import os
import re
import signal
import tempfile
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.models.discussion import (
    Discussion,
    DiscussionAgent,
    DiscussionEvent,
    DiscussionMessage,
)
from backend.models.project import Project
from backend.services.claude_auth_projection import (
    ClaudeAuthProjectionError,
    apply_claude_auth_projection,
    environment_has_direct_claude_auth,
    prepare_claude_auth_projection,
    remove_claude_auth_projection,
)
from backend.services.cancellation import settle_awaitable
from backend.services.process_safety import require_safe_process_group_id
from backend.services.stream_parser import StreamParser
from backend.services.task_agent_isolation import (
    CLAUDE_READ_ONLY_BUILTIN_TOOLS,
    TaskAgentIsolationError,
    generate_claude_read_only_isolation_settings,
    scrub_task_model_environment,
    validate_claude_task_isolation_settings,
)
from backend.services.task_runtime_secrets import remove_private_scope
from backend.services.task_ssh_access import (
    TaskSSHAccessError,
    task_ssh_protected_paths,
)
from backend.services.ws_broadcaster import WebSocketBroadcaster

logger = logging.getLogger(__name__)

MAX_AUTO_ROUNDS = 10
_PROCESS_EXIT_AFTER_EOF_TIMEOUT = 2.0
_PROCESS_SIGNAL_TIMEOUTS = (10.0, 5.0, 5.0)
_STDERR_DRAIN_TIMEOUT = 2.0
_CONSUMER_SHUTDOWN_TIMEOUT = sum(_PROCESS_SIGNAL_TIMEOUTS) + 5.0


class DiscussionProcessCleanupError(RuntimeError):
    """A discussion child could not be proven terminal within the deadline."""


class DiscussionSecurityError(RuntimeError):
    """A Discussion Agent could not prove its provider isolation boundary."""


DISCUSSION_ACTIVE_STATUS = "active"
DISCUSSION_CLOSING_STATUS = "closing"
DISCUSSION_CLOSED_STATUS = "closed"
DISCUSSION_SHARE_BLOCKING_STATUSES = (
    DISCUSSION_ACTIVE_STATUS,
    DISCUSSION_CLOSING_STATUS,
)


async def active_project_discussion_id(
    db: AsyncSession,
    project_id: int,
) -> int | None:
    """Return the first durable Discussion lease blocking Project sharing.

    The caller must already hold the Project writer fence.  Creation and the
    first phase of deletion take the same fence, so this read-only graph scan
    is sufficient on SQLite as well as databases with real ``FOR UPDATE`` row
    locks.  ``closing`` remains a lease until physical deletion: a cancelled
    or failed cleanup must never expose a Project while an old child may live.
    """

    if type(project_id) is not int or project_id <= 0:
        raise ValueError("project_id must be a positive integer")
    return await db.scalar(
        select(Discussion.id)
        .where(
            Discussion.project_id == project_id,
            Discussion.status.in_(DISCUSSION_SHARE_BLOCKING_STATUSES),
        )
        .order_by(Discussion.id)
        .limit(1)
        .with_for_update()
    )


async def _lock_active_discussion_provider_lease(
    db: AsyncSession,
    *,
    discussion_id: int,
    project_id: int | None,
) -> Discussion:
    """Lock Project -> Discussion and prove one provider-capable lease."""

    if type(discussion_id) is not int or discussion_id <= 0:
        raise DiscussionSecurityError("Discussion identity is invalid")
    if project_id is not None and (
        type(project_id) is not int or project_id <= 0
    ):
        raise DiscussionSecurityError("Discussion Project identity is invalid")

    if project_id is not None:
        # Import lazily: project_share_admission intentionally imports the
        # read-only graph helper above when evaluating first-share admission.
        from backend.services.project_share_admission import (
            lock_project_share_authority,
            project_has_active_share,
        )

        try:
            await lock_project_share_authority(db, project_id)
            if await project_has_active_share(db, project_id):
                raise DiscussionSecurityError(
                    "Discussion Agent execution is disabled while Project "
                    f"{project_id} is shared"
                )
        except DiscussionSecurityError:
            raise
        except Exception as exc:
            raise DiscussionSecurityError(
                "Discussion Project sharing state could not be verified"
            ) from exc

    project_predicate = (
        Discussion.project_id.is_(None)
        if project_id is None
        else Discussion.project_id == project_id
    )
    discussion = (
        await db.execute(
            select(Discussion)
            .where(
                Discussion.id == discussion_id,
                project_predicate,
                Discussion.status == DISCUSSION_ACTIVE_STATUS,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if discussion is None:
        raise DiscussionSecurityError(
            "Discussion is no longer an active provider-capable lease"
        )
    return discussion


@dataclass(frozen=True)
class _DiscussionClaudeSecurityContext:
    discussion_id: int
    namespace: str
    identifier: int
    model: str
    resume_session_id: str | None
    repository_cwd: str | None
    binding: str
    project_id: int | None = None


async def _settle_despite_cancellation(awaitable):
    """Settle a finite lifecycle operation before delivering cancellation."""
    return await settle_awaitable(awaitable)


async def _drain_stderr_task(
    stderr_task: asyncio.Task[bytes],
    *,
    owner: str,
) -> bytes:
    """Drain one reaped child's stderr without leaving a reader task behind."""

    if not stderr_task.done():
        done, _ = await asyncio.wait(
            {stderr_task},
            timeout=_STDERR_DRAIN_TIMEOUT,
        )
        if not done:
            stderr_task.cancel()
            done, _ = await asyncio.wait(
                {stderr_task},
                timeout=_STDERR_DRAIN_TIMEOUT,
            )
            if not done:
                raise DiscussionProcessCleanupError(
                    f"{owner} stderr reader ignored cancellation"
                )
    if stderr_task.cancelled():
        return b""
    try:
        return stderr_task.result()
    except Exception:
        logger.exception("Failed to drain %s stderr", owner)
        return b""


class DiscussionService:
    def __init__(
        self,
        db_factory,
        broadcaster: WebSocketBroadcaster,
        *,
        claude_pool_provider=None,
    ):
        self.db_factory = db_factory
        self.broadcaster = broadcaster
        self._claude_pool_provider = claude_pool_provider
        self.parser = StreamParser()
        self._processes: dict[int, asyncio.subprocess.Process] = {}
        self._consumers: dict[int, asyncio.Task] = {}
        self._agent_locks: dict[int, asyncio.Lock] = {}
        self._facilitator_processes: dict[
            asyncio.Task, asyncio.subprocess.Process
        ] = {}
        self._facilitator_tasks: set[asyncio.Task] = set()
        self._facilitator_discussions: dict[asyncio.Task, int] = {}
        self._facilitator_locks: dict[int, asyncio.Lock] = {}
        self._round_count: dict[int, int] = {}

    def _get_lock(self, discussion_id: int) -> asyncio.Lock:
        if discussion_id not in self._facilitator_locks:
            self._facilitator_locks[discussion_id] = asyncio.Lock()
        return self._facilitator_locks[discussion_id]

    def _get_agent_lock(self, agent_id: int) -> asyncio.Lock:
        return self._agent_locks.setdefault(agent_id, asyncio.Lock())

    @staticmethod
    def _runtime_binding(kind: str, record) -> str:
        created_at = getattr(record, "created_at", None)
        if isinstance(created_at, datetime):
            created_identity = created_at.isoformat()
        else:
            created_identity = str(created_at or "legacy")
        return f"{kind}:{getattr(record, 'id', 0)}:{created_identity}"

    async def _claim_agent_start_locked(
        self,
        db: AsyncSession,
        agent_id: int,
    ) -> DiscussionAgent:
        """Atomically claim one idle/error agent before scheduling its process."""
        retained = self._processes.get(agent_id)
        if retained is not None and self._process_tree_alive(retained):
            raise ValueError(f"Agent {agent_id} is already running")
        consumer = self._consumers.get(agent_id)
        if consumer is not None and not consumer.done():
            raise ValueError(f"Agent {agent_id} is already running")

        claimed = await db.execute(
            update(DiscussionAgent)
            .where(
                DiscussionAgent.id == agent_id,
                DiscussionAgent.status.in_(("idle", "error")),
            )
            .values(status="running")
        )
        if claimed.rowcount != 1:
            await db.rollback()
            current = await db.get(
                DiscussionAgent,
                agent_id,
                populate_existing=True,
            )
            if current is None:
                raise ValueError(f"Agent {agent_id} not found")
            raise ValueError(f"Agent {agent_id} is already running")
        await db.commit()
        agent = await db.get(
            DiscussionAgent,
            agent_id,
            populate_existing=True,
        )
        if agent is None:
            raise ValueError(f"Agent {agent_id} not found")
        return agent

    async def _resolve_project_cwd(self, db: AsyncSession, disc: Discussion) -> str | None:
        if not disc.project_id:
            return None
        project = await db.get(Project, disc.project_id)
        if project and project.local_path:
            return project.local_path
        return None

    async def _require_provider_admission(
        self,
        db: AsyncSession,
        discussion_id: int,
    ) -> None:
        """Preflight a public launch before taking Discussion/Agent locks.

        The initial lookup is deliberately unlocked and followed by rollback:
        it discovers the Project identity without inverting the required
        Project -> Discussion lock order.  The authoritative helper then takes
        the Project writer fence and revalidates the active Discussion row.
        Its transaction can be released before the in-process lock because the
        committed active lease remains visible to every first-share scan.
        """

        snapshot = await db.get(
            Discussion,
            discussion_id,
            populate_existing=True,
        )
        if snapshot is None:
            await db.rollback()
            raise ValueError(f"Discussion {discussion_id} not found")
        project_id = snapshot.project_id
        await db.rollback()
        try:
            await _lock_active_discussion_provider_lease(
                db,
                discussion_id=discussion_id,
                project_id=project_id,
            )
        finally:
            # No state is mutated here.  Rollback releases the Project writer
            # fence without holding a SQLite writer transaction across an
            # asyncio Discussion lock or provider-account admission.
            await db.rollback()

    async def _prepare_claude_security_context(
        self,
        context: _DiscussionClaudeSecurityContext,
    ) -> tuple[list[str], dict[str, str], str]:
        """Build one exact read-only Claude route immediately before spawn."""

        if not str(settings.auth_token or "").strip():
            raise DiscussionSecurityError(
                "Discussion Agent security admission requires AUTH_TOKEN"
            )
        try:
            async with self.db_factory() as db:
                from backend.models.user import User

                discussion = await _lock_active_discussion_provider_lease(
                    db,
                    discussion_id=context.discussion_id,
                    project_id=context.project_id,
                )
                creator = (
                    await db.get(User, discussion.creator_user_id)
                    if discussion.creator_user_id is not None
                    else None
                )
                if (
                    discussion.creator_user_id is None
                    or creator is None
                    or not creator.is_active
                    or creator.role not in {"admin", "super_admin"}
                ):
                    raise DiscussionSecurityError(
                        "Discussion Agent workloads are restricted to active admins"
                    )
                protected_paths = await task_ssh_protected_paths(
                    db,
                    working_directory=context.repository_cwd,
                )
            env = scrub_task_model_environment(os.environ, provider="claude")
            source_home = env.get("CLAUDE_CONFIG_DIR") or str(
                os.path.expanduser("~/.claude")
            )
            if not environment_has_direct_claude_auth(env):
                pool = (
                    self._claude_pool_provider()
                    if self._claude_pool_provider is not None
                    else None
                )
                if pool is not None:
                    refreshed = await pool.ensure_oauth_access_token(
                        source_home,
                        minimum_remaining_seconds=300.0,
                    )
                    if not refreshed:
                        raise DiscussionSecurityError(
                            "Discussion Claude account is not an active native "
                            "pool account"
                        )
            projection = prepare_claude_auth_projection(
                source_home,
                namespace=context.namespace,
                identifier=context.identifier,
                binding=context.binding,
                environment=env,
            )
            apply_claude_auth_projection(env, projection)
            isolation_path = generate_claude_read_only_isolation_settings(
                context.namespace,
                context.identifier,
                protected_paths,
            )
            await asyncio.to_thread(
                validate_claude_task_isolation_settings,
                isolation_path,
                claude_binary=settings.claude_binary,
                tools=CLAUDE_READ_ONLY_BUILTIN_TOOLS,
                include_mcp_tools=False,
            )
        except (
            ClaudeAuthProjectionError,
            TaskAgentIsolationError,
            TaskSSHAccessError,
            OSError,
            ValueError,
        ) as exc:
            raise DiscussionSecurityError(
                "Discussion Agent security admission failed"
            ) from exc

        command = self._build_cmd(
            context.model,
            resume_session_id=context.resume_session_id,
            isolation_settings_path=str(isolation_path),
            repository_cwd=context.repository_cwd,
        )
        # The repository is named explicitly in the system prompt; using a
        # neutral cwd prevents automatic CLAUDE.md/project-memory discovery.
        return command, env, os.path.abspath(os.sep)

    # ------------------------------------------------------------------
    # Public: user sends group message
    # ------------------------------------------------------------------
    async def send_broadcast(
        self,
        db: AsyncSession,
        discussion_id: int,
        user_message: str,
    ) -> list[DiscussionAgent]:
        await self._require_provider_admission(db, discussion_id)
        async with self._get_lock(discussion_id):
            return await self._send_broadcast_locked(
                db,
                discussion_id,
                user_message,
            )

    async def _send_broadcast_locked(
        self,
        db: AsyncSession,
        discussion_id: int,
        user_message: str,
    ) -> list[DiscussionAgent]:
        disc = await db.get(Discussion, discussion_id)
        if not disc or disc.status != DISCUSSION_ACTIVE_STATUS:
            raise ValueError(f"Discussion {discussion_id} not found")

        user_msg = DiscussionMessage(
            discussion_id=discussion_id,
            role="user",
            content=user_message,
            created_at=datetime.now(timezone.utc),
        )
        db.add(user_msg)
        await db.commit()

        await self.broadcaster.broadcast(f"discussion:{discussion_id}", {
            "event_type": "discussion_message",
            "message": _msg_to_dict(user_msg),
        })

        existing = await db.execute(
            select(DiscussionAgent).where(
                DiscussionAgent.discussion_id == discussion_id
            )
        )
        existing_agents = list(existing.scalars().all())

        self._round_count[discussion_id] = 0

        if existing_agents:
            asyncio.get_event_loop().create_task(
                self._facilitator_advance(discussion_id)
            )
            return existing_agents

        project_cwd = await self._resolve_project_cwd(db, disc)

        history = await self._get_history(db, discussion_id)
        history_file = self._write_history_file(discussion_id, history)

        roles = await self._run_facilitator_init(disc, history_file, cwd=project_cwd)

        agents = []
        for role in roles:
            agent = DiscussionAgent(
                discussion_id=discussion_id,
                role_name=role["role_name"],
                system_prompt=role["system_prompt"],
                status="running",
                created_at=datetime.now(timezone.utc),
            )
            db.add(agent)
            await db.flush()
            agents.append(agent)

            await self.broadcaster.broadcast(f"discussion:{discussion_id}", {
                "event_type": "agent_spawned",
                "agent": _agent_to_dict(agent),
            })

        await db.commit()

        for agent in agents:
            self._launch_agent(agent, disc, history_file, cwd=project_cwd)

        return agents

    # ------------------------------------------------------------------
    # Public: send message to a specific agent (resume session)
    # ------------------------------------------------------------------
    async def send_to_agent(
        self,
        db: AsyncSession,
        agent_id: int,
        message: str,
    ) -> None:
        agent = await db.get(DiscussionAgent, agent_id)
        if not agent:
            raise ValueError(f"Agent {agent_id} not found")
        discussion_id = agent.discussion_id
        await self._require_provider_admission(db, discussion_id)
        async with self._get_lock(discussion_id):
            agent = await db.get(
                DiscussionAgent,
                agent_id,
                populate_existing=True,
            )
            if not agent:
                raise ValueError(f"Agent {agent_id} not found")
            if agent.status == "running":
                raise ValueError(f"Agent {agent_id} is already running")

            disc = await db.get(
                Discussion,
                discussion_id,
                populate_existing=True,
            )
            if not disc or disc.status != DISCUSSION_ACTIVE_STATUS:
                raise ValueError(f"Discussion {discussion_id} not found")

            user_evt = DiscussionEvent(
                discussion_id=discussion_id,
                agent_id=agent_id,
                event_type="user_message",
                role="user",
                content=message,
                timestamp=datetime.now(timezone.utc),
            )
            db.add(user_evt)

            async with self._get_agent_lock(agent_id):
                agent = await self._claim_agent_start_locked(db, agent_id)
                self._launch_agent_resume(agent, disc, message)

            await self.broadcaster.broadcast(
                f"discussion:{discussion_id}:agent:{agent_id}",
                {
                    "event_type": "user_message",
                    "role": "user",
                    "content": message,
                    "agent_id": agent_id,
                },
            )
            await self.broadcaster.broadcast(f"discussion:{discussion_id}", {
                "event_type": "agent_status",
                "agent_id": agent_id,
                "status": "running",
            })

    # ------------------------------------------------------------------
    # Public: trigger an idle agent
    # ------------------------------------------------------------------
    async def trigger_agent(
        self,
        db: AsyncSession,
        agent_id: int,
    ) -> None:
        agent = await db.get(DiscussionAgent, agent_id)
        if not agent:
            raise ValueError(f"Agent {agent_id} not found")
        discussion_id = agent.discussion_id
        await self._require_provider_admission(db, discussion_id)
        async with self._get_lock(discussion_id):
            agent = await db.get(
                DiscussionAgent,
                agent_id,
                populate_existing=True,
            )
            if not agent:
                raise ValueError(f"Agent {agent_id} not found")
            if agent.status == "running":
                raise ValueError(f"Agent {agent_id} is already running")

            disc = await db.get(
                Discussion,
                discussion_id,
                populate_existing=True,
            )
            if not disc or disc.status != DISCUSSION_ACTIVE_STATUS:
                raise ValueError("Discussion not found")

            history = await self._get_history(db, discussion_id)
            history_file = self._write_history_file(discussion_id, history)

            prompt = f"""\
{agent.system_prompt}

The discussion has continued since your last response.
Updated discussion history is at: {history_file}
Read it, then provide your updated analysis from your perspective as "{agent.role_name}".
Write in Chinese."""

            try:
                async with self._get_agent_lock(agent_id):
                    agent = await self._claim_agent_start_locked(db, agent_id)
                    self._launch_agent_with_prompt(
                        agent,
                        disc,
                        prompt,
                        history_file,
                    )
            except BaseException:
                try:
                    os.unlink(history_file)
                except OSError:
                    pass
                raise

            await self.broadcaster.broadcast(f"discussion:{discussion_id}", {
                "event_type": "agent_status",
                "agent_id": agent_id,
                "status": "running",
            })

    # ------------------------------------------------------------------
    # Public: stop a running agent
    # ------------------------------------------------------------------
    async def stop_agent(self, agent_id: int) -> None:
        async with self._get_agent_lock(agent_id):
            await self._stop_agent_locked(agent_id)

    async def _stop_agent_locked(self, agent_id: int) -> None:
        """Cancel and reap one exact consumer while its admission lock is held."""

        consumer = self._consumers.get(agent_id)
        current = asyncio.current_task()
        if consumer is not None and consumer is not current and not consumer.done():
            consumer.cancel()
            done, pending = await asyncio.wait(
                {consumer},
                timeout=_CONSUMER_SHUTDOWN_TIMEOUT,
            )
            if pending:
                raise DiscussionProcessCleanupError(
                    f"Discussion Agent {agent_id} consumer ignored cancellation"
                )
            await asyncio.gather(*done, return_exceptions=True)
        process = self._processes.get(agent_id)
        if process is not None:
            tree_alive = self._process_tree_alive(process)
            if tree_alive:
                await self._terminate_process(process)
                tree_alive = self._process_tree_alive(process)
            if not tree_alive and self._processes.get(agent_id) is process:
                self._processes.pop(agent_id, None)
        retained = self._consumers.get(agent_id)
        if retained is not None and not retained.done():
            raise DiscussionProcessCleanupError(
                f"Discussion Agent {agent_id} consumer remains active"
            )
        if retained is not None and retained.done():
            self._consumers.pop(agent_id, None)

    async def stop_facilitator(self, discussion_id: int) -> None:
        """Cancel and reap every facilitator turn owned by one Discussion."""

        owners = [
            owner
            for owner, owned_discussion_id in self._facilitator_discussions.items()
            if owned_discussion_id == discussion_id
        ]
        for owner in owners:
            owner.cancel()
        if owners:
            done, pending = await asyncio.wait(
                owners,
                timeout=_CONSUMER_SHUTDOWN_TIMEOUT,
            )
            if done:
                await asyncio.gather(*done, return_exceptions=True)
            if pending:
                raise DiscussionProcessCleanupError(
                    "Discussion facilitator ignored cancellation"
                )

    async def cleanup_runtime(
        self,
        discussion_id: int,
        agent_ids: list[int],
    ) -> None:
        """Remove exact auth/session/settings projections after terminal proof."""

        if any(
            owned_discussion_id == discussion_id
            for owned_discussion_id in self._facilitator_discussions.values()
        ):
            raise DiscussionProcessCleanupError(
                "Discussion facilitator is still running during cleanup"
            )
        if any(
            agent_id in self._processes
            and self._process_tree_alive(self._processes[agent_id])
            for agent_id in agent_ids
        ):
            raise DiscussionProcessCleanupError(
                "Discussion Agent is still running during cleanup"
            )
        if any(
            agent_id in self._consumers
            and not self._consumers[agent_id].done()
            for agent_id in agent_ids
        ):
            raise DiscussionProcessCleanupError(
                "Discussion Agent consumer is still running during cleanup"
            )

        def remove() -> None:
            remove_claude_auth_projection(
                namespace="discussion-facilitator",
                identifier=discussion_id,
            )
            remove_private_scope("discussion-facilitator", discussion_id)
            for agent_id in agent_ids:
                remove_claude_auth_projection(
                    namespace="discussion-agent",
                    identifier=agent_id,
                )
                remove_private_scope("discussion-agent", agent_id)

        await asyncio.to_thread(remove)

    @asynccontextmanager
    async def deletion_barrier(
        self,
        discussion_id: int,
        db: AsyncSession,
    ):
        """Quiesce a Discussion and fence every concurrent launch until commit.

        Launch admission is ordered Discussion -> Agent.  Deletion takes the
        same locks, waits for exact facilitator/consumer finalization, and keeps
        them held while the API removes the database graph.  A queued launch
        can only resume after deletion commits, at which point its fresh DB
        lookup observes that the Discussion no longer exists.
        """

        discussion_lock = self._get_lock(discussion_id)
        async with discussion_lock:
            result = await db.execute(
                select(DiscussionAgent.id).where(
                    DiscussionAgent.discussion_id == discussion_id
                )
            )
            agent_ids = sorted(set(result.scalars().all()))

            acquired: list[asyncio.Lock] = []
            try:
                for agent_id in agent_ids:
                    lock = self._get_agent_lock(agent_id)
                    await lock.acquire()
                    acquired.append(lock)

                async def quiesce_runtime() -> None:
                    await self.stop_facilitator(discussion_id)
                    for agent_id in agent_ids:
                        await self._stop_agent_locked(agent_id)
                    await self.cleanup_runtime(discussion_id, agent_ids)

                # HTTP request cancellation must not tear down this safety
                # barrier halfway through process reaping.  Finish the bounded
                # quiesce while every launch lock remains held, then deliver
                # the cancellation without entering the graph-deletion body.
                quiesce, cancellation = await _settle_despite_cancellation(
                    quiesce_runtime()
                )
                try:
                    quiesce.result()
                except BaseException:
                    if cancellation is not None:
                        raise cancellation
                    raise
                if cancellation is not None:
                    raise cancellation
                yield agent_ids
            finally:
                for lock in reversed(acquired):
                    lock.release()

    async def shutdown(self) -> None:
        """Stop every discussion consumer and prove its child has exited."""
        operation, cancellation = await _settle_despite_cancellation(
            self._shutdown_bounded()
        )
        try:
            operation.result()
        except BaseException:
            if cancellation is not None:
                raise cancellation
            raise
        if cancellation is not None:
            raise cancellation

    async def _shutdown_bounded(self) -> None:
        """Bounded shutdown implementation retaining every unresolved owner."""
        owned_tasks = list(self._consumers.values()) + list(
            self._facilitator_tasks
        )
        current = asyncio.current_task()
        owned_tasks = [task for task in owned_tasks if task is not current]
        for task in owned_tasks:
            task.cancel()
        if owned_tasks:
            done, pending = await asyncio.wait(
                owned_tasks,
                timeout=_CONSUMER_SHUTDOWN_TIMEOUT,
            )
            if done:
                await asyncio.gather(*done, return_exceptions=True)
        else:
            pending = set()

        # A cancellation can race the short spawn→consumer registration
        # window. Reap every exact process still retained, including processes
        # whose consumer timed out in database/event finalization.
        failures: list[str] = []
        process_items = list(self._processes.items())
        if process_items:
            results = await asyncio.gather(
                *(
                    self._terminate_process(process)
                    for _, process in process_items
                ),
                return_exceptions=True,
            )
            for (agent_id, process), result in zip(process_items, results):
                process_reaped = (
                    process.returncode is not None
                    and not self._process_tree_alive(process)
                )
                if process_reaped:
                    if self._processes.get(agent_id) is process:
                        self._processes.pop(agent_id, None)
                elif isinstance(result, BaseException):
                    failures.append(f"agent {agent_id}: {result}")
                else:
                    failures.append(f"agent {agent_id}: still running")

        facilitator_items = list(self._facilitator_processes.items())
        if facilitator_items:
            results = await asyncio.gather(
                *(
                    self._terminate_process(process)
                    for _, process in facilitator_items
                ),
                return_exceptions=True,
            )
            for (owner, process), result in zip(
                facilitator_items,
                results,
            ):
                process_reaped = (
                    process.returncode is not None
                    and not self._process_tree_alive(process)
                )
                if process_reaped:
                    if self._facilitator_processes.get(owner) is process:
                        self._facilitator_processes.pop(owner, None)
                elif isinstance(result, BaseException):
                    failures.append(f"facilitator: {result}")
                else:
                    failures.append("facilitator: still running")

        for agent_id, consumer in list(self._consumers.items()):
            if consumer.done() and self._consumers.get(agent_id) is consumer:
                self._consumers.pop(agent_id, None)
        self._facilitator_tasks = {
            task for task in self._facilitator_tasks if not task.done()
        }

        if pending:
            failures.append(
                f"{len(pending)} discussion consumer(s) ignored cancellation"
            )
        if self._processes:
            failures.append(
                f"{len(self._processes)} discussion process(es) remain live"
            )
        if self._facilitator_processes:
            failures.append(
                f"{len(self._facilitator_processes)} facilitator process(es) "
                "remain live"
            )
        if failures:
            raise DiscussionProcessCleanupError("; ".join(failures))

    @staticmethod
    def _send_process_signal(
        process: asyncio.subprocess.Process,
        sig: signal.Signals,
    ) -> None:
        pid = getattr(process, "pid", None)
        if os.name == "posix":
            process_group_id = require_safe_process_group_id(
                pid,
                context="DiscussionService child termination",
            )
            try:
                os.killpg(process_group_id, sig)
                return
            except ProcessLookupError:
                return
        process.send_signal(sig)

    @staticmethod
    def _process_tree_alive(
        process: asyncio.subprocess.Process,
    ) -> bool:
        if os.name != "posix":
            return process.returncode is None
        process_group_id = require_safe_process_group_id(
            getattr(process, "pid", None),
            context="DiscussionService child liveness check",
        )
        try:
            os.killpg(process_group_id, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True

    async def _wait_for_process_tree_exit(
        self,
        process: asyncio.subprocess.Process,
        timeout: float,
    ) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            tree_alive = self._process_tree_alive(process)
            if process.returncode is not None and not tree_alive:
                return
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise asyncio.TimeoutError
            if (
                process.returncode is None
                or self._process_tree_alive(process)
            ):
                try:
                    await asyncio.wait_for(
                        process.wait(),
                        timeout=min(0.05, remaining),
                    )
                except asyncio.TimeoutError:
                    pass
            else:
                await asyncio.sleep(min(0.05, remaining))

    async def _terminate_process(
        self,
        process: asyncio.subprocess.Process,
    ) -> None:
        """Bounded SIGINT→SIGTERM→SIGKILL escalation for the whole process group."""
        if not self._process_tree_alive(process):
            if process.returncode is None:
                try:
                    await asyncio.wait_for(process.wait(), timeout=0.1)
                except asyncio.TimeoutError as exc:
                    raise DiscussionProcessCleanupError(
                        "Discussion process disappeared but could not be reaped"
                    ) from exc
            return
        self._send_process_signal(process, signal.SIGINT)
        try:
            await self._wait_for_process_tree_exit(
                process,
                _PROCESS_SIGNAL_TIMEOUTS[0],
            )
            return
        except asyncio.TimeoutError:
            pass

        if process.returncode is None:
            self._send_process_signal(process, signal.SIGTERM)
        elif self._process_tree_alive(process):
            self._send_process_signal(process, signal.SIGTERM)
        try:
            await self._wait_for_process_tree_exit(
                process,
                _PROCESS_SIGNAL_TIMEOUTS[1],
            )
            return
        except asyncio.TimeoutError:
            pass

        if self._process_tree_alive(process):
            self._send_process_signal(process, signal.SIGKILL)
        try:
            await self._wait_for_process_tree_exit(
                process,
                _PROCESS_SIGNAL_TIMEOUTS[2],
            )
        except asyncio.TimeoutError as exc:
            raise DiscussionProcessCleanupError(
                "Discussion process "
                f"{getattr(process, 'pid', None)} survived SIGKILL"
            ) from exc

    # ------------------------------------------------------------------
    # Public: add one more agent via facilitator
    # ------------------------------------------------------------------
    async def add_agent(
        self,
        db: AsyncSession,
        discussion_id: int,
    ) -> DiscussionAgent:
        await self._require_provider_admission(db, discussion_id)
        async with self._get_lock(discussion_id):
            return await self._add_agent_locked(db, discussion_id)

    async def _add_agent_locked(
        self,
        db: AsyncSession,
        discussion_id: int,
    ) -> DiscussionAgent:
        disc = await db.get(Discussion, discussion_id)
        if not disc or disc.status != DISCUSSION_ACTIVE_STATUS:
            raise ValueError(f"Discussion {discussion_id} not found")

        existing_agents = await db.execute(
            select(DiscussionAgent).where(DiscussionAgent.discussion_id == discussion_id)
        )
        existing_roles = [a.role_name for a in existing_agents.scalars().all()]

        project_cwd = await self._resolve_project_cwd(db, disc)

        history = await self._get_history(db, discussion_id)
        history_file = self._write_history_file(discussion_id, history)

        role = await self._run_facilitator_add_one(disc, history_file, existing_roles, cwd=project_cwd)

        agent = DiscussionAgent(
            discussion_id=discussion_id,
            role_name=role["role_name"],
            system_prompt=role["system_prompt"],
            status="running",
            created_at=datetime.now(timezone.utc),
        )
        db.add(agent)
        await db.commit()
        await db.refresh(agent)

        await self.broadcaster.broadcast(f"discussion:{discussion_id}", {
            "event_type": "agent_spawned",
            "agent": _agent_to_dict(agent),
        })

        self._launch_agent(agent, disc, history_file, cwd=project_cwd)
        return agent

    # ------------------------------------------------------------------
    # Facilitator auto-advance: triggered when an agent finishes
    # ------------------------------------------------------------------
    async def _maybe_auto_advance(self, discussion_id: int) -> None:
        """Check if all agents are idle; if so, trigger facilitator."""
        async with self.db_factory() as db:
            result = await db.execute(
                select(DiscussionAgent).where(
                    DiscussionAgent.discussion_id == discussion_id
                )
            )
            agents = list(result.scalars().all())

        if not agents:
            return
        if any(a.status == "running" for a in agents):
            return

        round_num = self._round_count.get(discussion_id, 0)
        if round_num >= MAX_AUTO_ROUNDS:
            logger.info(
                "Discussion %s reached max auto-advance rounds (%d), stopping",
                discussion_id, MAX_AUTO_ROUNDS,
            )
            return

        await self._facilitator_advance(discussion_id)

    async def _facilitator_advance(self, discussion_id: int) -> None:
        """Facilitator analyzes current state and decides next steps."""
        try:
            async with self.db_factory() as admission_db:
                await self._require_provider_admission(
                    admission_db,
                    discussion_id,
                )
        except DiscussionSecurityError as exc:
            logger.warning(
                "Discussion %s facilitator admission rejected: %s",
                discussion_id,
                exc,
            )
            await self.broadcaster.broadcast(
                f"discussion:{discussion_id}",
                {
                    "event_type": "facilitator_status",
                    "status": "error",
                    "error": str(exc),
                },
            )
            return
        except ValueError:
            return
        lock = self._get_lock(discussion_id)
        if lock.locked():
            return

        async with lock:
            async with self.db_factory() as db:
                disc = await db.get(Discussion, discussion_id)
                if not disc or disc.status != DISCUSSION_ACTIVE_STATUS:
                    return

                agents = await db.execute(
                    select(DiscussionAgent).where(
                        DiscussionAgent.discussion_id == discussion_id
                    )
                )
                agent_list = list(agents.scalars().all())
                if not agent_list:
                    return

                agent_outputs_file = await self._write_agent_outputs_file(db, discussion_id)
                agent_names = [a.role_name for a in agent_list]
                messages = await self._get_history(db, discussion_id)
                project_cwd = await self._resolve_project_cwd(db, disc)

            goal = messages[0].content if messages else disc.title
            round_num = self._round_count.get(discussion_id, 0) + 1
            self._round_count[discussion_id] = round_num

            decision = await self._run_facilitator_decide(
                disc, goal, agent_names, agent_outputs_file, round_num, cwd=project_cwd
            )

            action = decision.get("action", "complete")

            if action == "complete":
                final = decision.get("final_output", "")
                if final:
                    async with self.db_factory() as db:
                        msg = DiscussionMessage(
                            discussion_id=discussion_id,
                            role="facilitator",
                            agent_role_name="Facilitator",
                            content=final,
                            created_at=datetime.now(timezone.utc),
                        )
                        db.add(msg)
                        await db.commit()

                    await self.broadcaster.broadcast(
                        f"discussion:{discussion_id}",
                        {
                            "event_type": "discussion_message",
                            "message": {
                                "id": -1,
                                "discussion_id": discussion_id,
                                "role": "facilitator",
                                "agent_role_name": "Facilitator",
                                "content": final,
                                "created_at": datetime.now(timezone.utc).isoformat(),
                            },
                        },
                    )
                return

            instructions = decision.get("instructions", {})
            if not instructions:
                return

            async with self.db_factory() as db:
                disc = await db.get(Discussion, discussion_id)
                agents_result = await db.execute(
                    select(DiscussionAgent).where(
                        DiscussionAgent.discussion_id == discussion_id
                    )
                )
                all_agents = {a.role_name: a for a in agents_result.scalars().all()}

                for role_name, instruction in instructions.items():
                    agent = all_agents.get(role_name)
                    if not agent or agent.status == "running":
                        continue

                    try:
                        async with self._get_agent_lock(agent.id):
                            agent = await self._claim_agent_start_locked(
                                db,
                                agent.id,
                            )
                            effective_cwd = agent.last_cwd or project_cwd
                            if agent.session_id:
                                self._launch_agent_resume(
                                    agent,
                                    disc,
                                    instruction,
                                    cwd=effective_cwd,
                                )
                            else:
                                full_prompt = (
                                    f"{agent.system_prompt}\n\n{instruction}"
                                )
                                self._launch_agent_with_prompt(
                                    agent,
                                    disc,
                                    full_prompt,
                                    cwd=effective_cwd,
                                )
                    except ValueError:
                        # A manual trigger/chat request won the same DB claim.
                        continue

                    await self.broadcaster.broadcast(
                        f"discussion:{discussion_id}",
                        {
                            "event_type": "agent_status",
                            "agent_id": agent.id,
                            "status": "running",
                        },
                    )

    async def _run_facilitator_decide(
        self,
        disc: Discussion,
        goal: str,
        agent_names: list[str],
        agent_outputs_file: str,
        round_num: int,
        cwd: str | None = None,
    ) -> dict:
        """Facilitator decides: continue with instructions, or complete."""
        names_str = ", ".join(agent_names)

        prompt = f"""\
你是一场多角色讨论的协调者(Facilitator)。

## 讨论目标
{goal}

## 当前参与角色
{names_str}

## 各角色的完整产出
请阅读以下文件，里面包含每个角色的完整输出（不要跳过，仔细阅读）：
{agent_outputs_file}

## 当前轮次
第 {round_num} 轮（最多 {MAX_AUTO_ROUNDS} 轮）

## 你的任务
仔细阅读上述文件中各角色的完整产出，判断讨论目标是否已经达成。

如果还需要继续：决定哪些角色需要继续工作，给每个角色**具体的下一步指令**。
指令中要包含其他角色的关键观点（交叉分享），以及你希望该角色接下来重点分析的方向。
不需要所有角色都继续，只让有需要的角色继续。

如果目标已达成：生成最终产出，综合所有角色的分析。

## 输出格式
用以下 XML 标签输出你的决策，不要有其他文字：

继续讨论：
<decision>
<action>continue</action>
<reason>为什么需要继续，当前还缺什么</reason>
<instructions>
<role name="角色名1">给该角色的具体指令，包含交叉分享的信息...</role>
<role name="角色名2">给该角色的具体指令...</role>
</instructions>
</decision>

讨论完成：
<decision>
<action>complete</action>
<reason>为什么判断目标已达成</reason>
<final_output>
最终综合产出（Markdown格式，完整且可交付）
</final_output>
</decision>"""

        return await self._run_facilitator_structured(disc, prompt, cwd=cwd)

    async def _run_facilitator_process(
        self, disc: Discussion, prompt: str, cwd: str | None = None
    ) -> list[str]:
        """Run facilitator subprocess, stream events, capture session_id. Returns collected text."""
        discussion_id = disc.id
        collected_text: list[str] = []
        captured_session_id: str | None = None
        owner = asyncio.current_task()
        if owner is None:
            raise RuntimeError("Facilitator process has no owning task")
        self._facilitator_tasks.add(owner)
        self._facilitator_discussions[owner] = discussion_id
        process: asyncio.subprocess.Process | None = None
        stderr_task: asyncio.Task[bytes] | None = None
        cancelled: asyncio.CancelledError | None = None
        run_error: BaseException | None = None
        cleanup_error: BaseException | None = None

        try:
            cmd, env, process_cwd = await self._prepare_claude_security_context(
                _DiscussionClaudeSecurityContext(
                    discussion_id=discussion_id,
                    namespace="discussion-facilitator",
                    identifier=discussion_id,
                    model=disc.facilitator_model,
                    resume_session_id=disc.facilitator_session_id,
                    repository_cwd=cwd,
                    binding=self._runtime_binding("discussion", disc),
                    project_id=disc.project_id,
                )
            )
            cmd.extend(["--max-turns", "5", "-p", prompt])
            await self.broadcaster.broadcast(f"discussion:{discussion_id}", {
                "event_type": "facilitator_status",
                "status": "running",
            })
            spawn, spawn_cancellation = await _settle_despite_cancellation(
                asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=env,
                    cwd=process_cwd,
                    limit=10 * 1024 * 1024,
                    start_new_session=(os.name == "posix"),
                )
            )
            try:
                process = spawn.result()
            except BaseException:
                if spawn_cancellation is not None:
                    raise spawn_cancellation
                raise
            self._facilitator_processes[owner] = process
            stderr_task = asyncio.create_task(process.stderr.read())
            cancelled = spawn_cancellation

            try:
                while cancelled is None:
                    line = await process.stdout.readline()
                    if not line:
                        break
                    text = line.decode("utf-8", errors="replace").strip()
                    if not text:
                        continue

                    events = self.parser.parse_line(text)
                    for event in events:
                        sid = event.pop("session_id", None)
                        if sid and not captured_session_id:
                            captured_session_id = sid
                        event.pop("cost_usd", None)
                        event.pop("context_usage", None)
                        et = event.get("event_type", "")

                        if et in ("message", "result") and event.get("content"):
                            collected_text.append(event["content"])

                        await self._save_facilitator_event(
                            discussion_id,
                            et,
                            event,
                        )

                        broadcast_data = {
                            k: v for k, v in event.items() if k != "raw_json"
                        }
                        broadcast_data["event_type"] = (
                            f"facilitator_{et}"
                            if et
                            else "facilitator_unknown"
                        )
                        await self.broadcaster.broadcast(
                            f"discussion:{discussion_id}",
                            broadcast_data,
                        )
            except asyncio.CancelledError as exc:
                cancelled = exc
            except BaseException as exc:
                run_error = exc

            async def _finish_process() -> None:
                if (
                    cancelled is None
                    and run_error is None
                    and process.returncode is None
                ):
                    try:
                        await asyncio.wait_for(
                            process.wait(),
                            timeout=_PROCESS_EXIT_AFTER_EOF_TIMEOUT,
                        )
                        if not self._process_tree_alive(process):
                            return
                    except asyncio.TimeoutError:
                        pass
                if (
                    process.returncode is None
                    or self._process_tree_alive(process)
                ):
                    await self._terminate_process(process)

            async def _finalize_facilitator() -> bytes:
                finalization_error: BaseException | None = None
                try:
                    await _finish_process()
                except BaseException as exc:
                    finalization_error = exc

                process_reaped = (
                    process.returncode is not None
                    and not self._process_tree_alive(process)
                )
                if (
                    process_reaped
                    and self._facilitator_processes.get(owner) is process
                ):
                    self._facilitator_processes.pop(owner, None)

                finalized_stderr = b""
                if stderr_task is not None:
                    try:
                        finalized_stderr = await _drain_stderr_task(
                            stderr_task,
                            owner="facilitator",
                        )
                    except BaseException as exc:
                        if finalization_error is None:
                            finalization_error = exc

                if finalization_error is not None:
                    raise finalization_error

                if cancelled is None and run_error is None:
                    if process.returncode != 0:
                        stderr_text = finalized_stderr.decode(
                            "utf-8",
                            errors="replace",
                        ).strip()
                        raise RuntimeError(
                            "Facilitator exited with code "
                            f"{process.returncode}"
                            + (
                                f": {stderr_text[:2000]}"
                                if stderr_text
                                else ""
                            )
                        )
                    if captured_session_id:
                        async with self.db_factory() as db:
                            await db.execute(
                                update(Discussion)
                                .where(Discussion.id == discussion_id)
                                .values(
                                    facilitator_session_id=(
                                        captured_session_id
                                    )
                                )
                            )
                            await db.commit()
                        disc.facilitator_session_id = captured_session_id

                    await self.broadcaster.broadcast(
                        f"discussion:{discussion_id}",
                        {
                            "event_type": "facilitator_status",
                            "status": "done",
                        },
                    )

                return finalized_stderr

            finalization, finalization_cancellation = (
                await _settle_despite_cancellation(_finalize_facilitator())
            )
            try:
                finalization.result()
            except BaseException as exc:
                cleanup_error = exc
            if cancelled is None:
                cancelled = finalization_cancellation

            if cleanup_error is not None:
                raise cleanup_error
            if cancelled is not None:
                raise cancelled
            if run_error is not None:
                raise run_error

        except BaseException as exc:
            if not isinstance(exc, asyncio.CancelledError):
                logger.exception("Facilitator process failed")
                await self.broadcaster.broadcast(
                    f"discussion:{discussion_id}",
                    {
                        "event_type": "facilitator_status",
                        "status": "error",
                        "error": str(exc),
                    },
                )
            raise
        finally:
            if stderr_task is not None and not stderr_task.done():
                stderr_task.cancel()
                try:
                    await asyncio.wait(
                        {stderr_task},
                        timeout=_STDERR_DRAIN_TIMEOUT,
                    )
                except asyncio.CancelledError:
                    pass
            self._facilitator_tasks.discard(owner)
            self._facilitator_discussions.pop(owner, None)

        return collected_text

    async def _run_facilitator_structured(
        self, disc: Discussion, prompt: str, cwd: str | None = None,
        _retry: int = 0,
    ) -> dict:
        """Run facilitator and parse structured XML response. Retries once on parse failure."""
        MAX_PARSE_RETRIES = 1
        try:
            collected_text = await self._run_facilitator_process(disc, prompt, cwd=cwd)
        except Exception as e:
            return {"action": "complete", "reason": f"Facilitator error: {e}"}

        raw = "\n".join(collected_text).strip()
        result = self._parse_decision_xml(raw)

        if result.get("reason") == "无法解析协调者输出" and _retry < MAX_PARSE_RETRIES:
            logger.info("Facilitator output missing <decision> tags, asking to retry (attempt %d)", _retry + 1)
            retry_prompt = (
                "你刚才的输出缺少 <decision> 标签，我无法解析。"
                "请严格按照之前要求的 XML 格式重新输出你的决策，"
                "用 <decision>...</decision> 包裹。"
            )
            return await self._run_facilitator_structured(
                disc, retry_prompt, cwd=cwd, _retry=_retry + 1
            )

        return result

    @staticmethod
    def _parse_decision_xml(raw: str) -> dict:
        text = raw.strip()

        decision_match = re.search(r"<decision>(.*?)</decision>", text, re.DOTALL)
        if not decision_match:
            decision_match = re.search(r"<decision>(.*)", text, re.DOTALL)
        if not decision_match:
            logger.warning("No <decision> tag found, defaulting to complete. Raw: %s", text[:300])
            return {
                "action": "complete",
                "reason": "无法解析协调者输出",
                "final_output": text if text else "讨论未能产生结构化结论。",
            }

        body = decision_match.group(1)

        action_m = re.search(r"<action>(.*?)</action>", body, re.DOTALL)
        action = action_m.group(1).strip() if action_m else "complete"

        reason_m = re.search(r"<reason>(.*?)</reason>", body, re.DOTALL)
        reason = reason_m.group(1).strip() if reason_m else ""

        if action == "continue":
            instructions: dict[str, str] = {}
            for role_m in re.finditer(
                r'<role\s+name="([^"]+)">(.*?)</role>', body, re.DOTALL
            ):
                instructions[role_m.group(1).strip()] = role_m.group(2).strip()
            return {
                "action": "continue",
                "reason": reason,
                "instructions": instructions,
            }

        final_m = re.search(r"<final_output>(.*?)</final_output>", body, re.DOTALL)
        final_output = final_m.group(1).strip() if final_m else text

        return {
            "action": "complete",
            "reason": reason,
            "final_output": final_output,
        }

    # ------------------------------------------------------------------
    # Internal: launch agent subprocess
    # ------------------------------------------------------------------
    def _launch_agent(
        self,
        agent: DiscussionAgent,
        disc: Discussion,
        history_file: str,
        cwd: str | None = None,
    ) -> None:
        if cwd:
            agent.last_cwd = cwd
            asyncio.get_event_loop().create_task(self._persist_agent_cwd(agent.id, cwd))

        prompt = f"""\
{agent.system_prompt}

Discussion history is at: {history_file}
Read it first, then respond from your perspective as "{agent.role_name}".

Guidelines:
- Be specific and actionable, not generic
- If you disagree with another participant, say so directly and explain why
- Write in Chinese"""

        self._launch_agent_with_prompt(agent, disc, prompt, history_file)

    async def _persist_agent_cwd(self, agent_id: int, cwd: str) -> None:
        async with self.db_factory() as db:
            await db.execute(
                update(DiscussionAgent)
                .where(DiscussionAgent.id == agent_id)
                .values(last_cwd=cwd)
            )
            await db.commit()

    def _launch_agent_resume(
        self,
        agent: DiscussionAgent,
        disc: Discussion,
        message: str,
        cwd: str | None = None,
    ) -> None:
        cmd: list[str] = []
        cmd.extend(["-p", message])
        env: dict[str, str] = {}
        cwd = cwd or agent.last_cwd

        task = asyncio.get_event_loop().create_task(
            self._run_and_consume(
                agent.id,
                agent.discussion_id,
                cmd,
                env,
                cwd,
                security_context=_DiscussionClaudeSecurityContext(
                    discussion_id=agent.discussion_id,
                    namespace="discussion-agent",
                    identifier=agent.id,
                    model=disc.agent_model,
                    resume_session_id=agent.session_id,
                    repository_cwd=cwd,
                    binding=self._runtime_binding("discussion-agent", agent),
                    project_id=disc.project_id,
                ),
            )
        )
        self._consumers[agent.id] = task

    def _launch_agent_with_prompt(
        self,
        agent: DiscussionAgent,
        disc: Discussion,
        prompt: str,
        history_file: str | None = None,
        cwd: str | None = None,
    ) -> None:
        cmd: list[str] = []
        cmd.extend(["-p", prompt])
        env: dict[str, str] = {}
        repository_cwd = cwd or agent.last_cwd

        task = asyncio.get_event_loop().create_task(
            self._run_and_consume(
                agent.id, agent.discussion_id, cmd, env,
                cwd=repository_cwd,
                cleanup_file=history_file,
                security_context=_DiscussionClaudeSecurityContext(
                    discussion_id=agent.discussion_id,
                    namespace="discussion-agent",
                    identifier=agent.id,
                    model=disc.agent_model,
                    resume_session_id=agent.session_id,
                    repository_cwd=repository_cwd,
                    binding=self._runtime_binding("discussion-agent", agent),
                    project_id=disc.project_id,
                ),
            )
        )
        self._consumers[agent.id] = task

    def _build_cmd(
        self,
        model: str,
        resume_session_id: str | None = None,
        *,
        isolation_settings_path: str,
        repository_cwd: str | None,
    ) -> list[str]:
        tools = ",".join(CLAUDE_READ_ONLY_BUILTIN_TOOLS)
        cmd = [
            settings.claude_binary,
            "--permission-mode", "plan",
            "--settings", isolation_settings_path,
            "--setting-sources", "",
            "--strict-mcp-config",
            "--mcp-config", '{"mcpServers":{}}',
            "--disable-slash-commands",
            "--no-chrome",
            "--tools", tools,
            "--allowedTools", tools,
            "--exclude-dynamic-system-prompt-sections",
            "--output-format", "stream-json",
            "--verbose",
            "--model", model,
        ]
        if repository_cwd:
            cmd.extend([
                "--append-system-prompt",
                "Repository root for optional read-only inspection: "
                f"{repository_cwd}",
            ])
        if resume_session_id:
            cmd.extend(["--resume", resume_session_id])
        return cmd

    # ------------------------------------------------------------------
    # Internal: run subprocess and consume stream
    # ------------------------------------------------------------------
    async def _rollback_unspawned_agent(
        self,
        agent_id: int,
        discussion_id: int,
        *,
        status: str,
        error: BaseException | None = None,
    ) -> None:
        """Release a DB claim when subprocess creation never produced a PID."""
        async with self.db_factory() as db:
            await db.execute(
                update(DiscussionAgent)
                .where(
                    DiscussionAgent.id == agent_id,
                    DiscussionAgent.status == "running",
                )
                .values(status=status, pid=None)
            )
            await db.commit()
        try:
            await self.broadcaster.broadcast(
                f"discussion:{discussion_id}",
                {
                    "event_type": "agent_status",
                    "agent_id": agent_id,
                    "status": status,
                    "error": str(error) if error is not None else None,
                },
            )
        except Exception:
            logger.exception(
                "Failed to broadcast unspawned discussion agent %s rollback",
                agent_id,
            )

    async def _run_and_consume(
        self,
        agent_id: int,
        discussion_id: int,
        cmd: list[str],
        env: dict,
        cwd: str | None = None,
        cleanup_file: str | None = None,
        security_context: _DiscussionClaudeSecurityContext | None = None,
    ) -> None:
        process: asyncio.subprocess.Process | None = None
        stderr_task: asyncio.Task[bytes] | None = None
        cancelled: asyncio.CancelledError | None = None
        cleanup_error: BaseException | None = None
        try:
            if security_context is not None:
                base_cmd, env, cwd = await self._prepare_claude_security_context(
                    security_context
                )
                cmd = [*base_cmd, *cmd]
            spawn, spawn_cancellation = await _settle_despite_cancellation(
                asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=env,
                    cwd=cwd,
                    limit=10 * 1024 * 1024,
                    start_new_session=(os.name == "posix"),
                )
            )
            try:
                process = spawn.result()
            except BaseException:
                if spawn_cancellation is not None:
                    raise spawn_cancellation
                raise
            self._processes[agent_id] = process
            stderr_task = asyncio.create_task(process.stderr.read())
            cancelled = spawn_cancellation

            try:
                while cancelled is None:
                    try:
                        line = await process.stdout.readline()
                        if not line:
                            break
                        text = line.decode("utf-8", errors="replace").strip()
                        if not text:
                            continue

                        events = self.parser.parse_line(text)
                        for event in events:
                            try:
                                await self._process_event(agent_id, discussion_id, event)
                            except Exception:
                                logger.exception(
                                    "Failed to process event for agent %s: %s",
                                    agent_id, event.get("event_type"),
                                )
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        logger.exception("Error in consume loop for agent %s", agent_id)
            except asyncio.CancelledError as exc:
                cancelled = exc

            async def _finish_process() -> None:
                if cancelled is None and process.returncode is None:
                    try:
                        await asyncio.wait_for(
                            process.wait(),
                            timeout=_PROCESS_EXIT_AFTER_EOF_TIMEOUT,
                        )
                        if not self._process_tree_alive(process):
                            return
                    except asyncio.TimeoutError:
                        pass
                if (
                    process.returncode is None
                    or self._process_tree_alive(process)
                ):
                    await self._terminate_process(process)

            consumer_owner = asyncio.current_task()

            async def _finalize_agent() -> tuple[str, bool]:
                finalization_error: BaseException | None = None
                try:
                    await _finish_process()
                except BaseException as exc:
                    finalization_error = exc

                exit_code = process.returncode
                process_reaped = (
                    exit_code is not None
                    and not self._process_tree_alive(process)
                )
                if (
                    process_reaped
                    and self._processes.get(agent_id) is process
                ):
                    self._processes.pop(agent_id, None)

                stderr_data = b""
                if stderr_task is not None:
                    try:
                        stderr_data = await _drain_stderr_task(
                            stderr_task,
                            owner=f"discussion agent {agent_id}",
                        )
                    except BaseException as exc:
                        if finalization_error is None:
                            finalization_error = exc
                stderr_text = (
                    stderr_data.decode("utf-8", errors="replace").strip()
                    if stderr_data
                    else ""
                )

                new_status = (
                    "idle"
                    if (
                        process_reaped
                        and (
                            cancelled is not None
                            or exit_code in (0, -2, 130)
                        )
                    )
                    else "error"
                )
                values: dict[str, object] = {"status": new_status}
                if process_reaped:
                    values["pid"] = None

                registered_consumer = self._consumers.get(agent_id)
                owns_consumer = (
                    registered_consumer is None
                    or registered_consumer is consumer_owner
                )
                status_published = False
                if owns_consumer:
                    async with self.db_factory() as db:
                        updated = await db.execute(
                            update(DiscussionAgent)
                            .where(
                                DiscussionAgent.id == agent_id,
                                DiscussionAgent.discussion_id
                                == discussion_id,
                                DiscussionAgent.status == "running",
                            )
                            .values(**values)
                        )
                        await db.commit()
                    status_published = getattr(updated, "rowcount", 1) == 1

                if status_published:
                    await self.broadcaster.broadcast(
                        f"discussion:{discussion_id}:agent:{agent_id}",
                        {
                            "event_type": "process_exit",
                            "agent_id": agent_id,
                            "exit_code": exit_code,
                            "stderr": (
                                stderr_text[:2000] if stderr_text else None
                            ),
                        },
                    )
                    await self.broadcaster.broadcast(
                        f"discussion:{discussion_id}",
                        {
                            "event_type": "agent_status",
                            "agent_id": agent_id,
                            "status": new_status,
                        },
                    )

                if finalization_error is not None:
                    raise finalization_error
                return new_status, status_published

            finalization, finalization_cancellation = (
                await _settle_despite_cancellation(_finalize_agent())
            )
            try:
                new_status, status_published = finalization.result()
            except BaseException as exc:
                cleanup_error = exc
            if cancelled is None:
                cancelled = finalization_cancellation

            if cleanup_error is not None:
                raise cleanup_error
            if cancelled is not None:
                raise cancelled
            if new_status == "idle" and status_published:
                asyncio.get_event_loop().create_task(
                    self._maybe_auto_advance(discussion_id)
                )
        except asyncio.CancelledError as exc:
            if process is None:
                rollback, _ = await _settle_despite_cancellation(
                    self._rollback_unspawned_agent(
                        agent_id,
                        discussion_id,
                        status="idle",
                    )
                )
                rollback.result()
            raise exc
        except BaseException as exc:
            if process is None:
                rollback, cancellation = await _settle_despite_cancellation(
                    self._rollback_unspawned_agent(
                        agent_id,
                        discussion_id,
                        status="error",
                        error=exc,
                    )
                )
                rollback.result()
                if cancellation is not None:
                    raise cancellation
            raise
        finally:
            if stderr_task is not None and not stderr_task.done():
                stderr_task.cancel()
                try:
                    await asyncio.wait(
                        {stderr_task},
                        timeout=_STDERR_DRAIN_TIMEOUT,
                    )
                except asyncio.CancelledError:
                    # The reader no longer owns a live process at this point;
                    # retain caller cancellation without an unbounded gather.
                    pass
            current_consumer = asyncio.current_task()
            if self._consumers.get(agent_id) is current_consumer:
                self._consumers.pop(agent_id, None)
            if cleanup_file:
                try:
                    os.unlink(cleanup_file)
                except OSError:
                    pass

    async def _process_event(
        self, agent_id: int, discussion_id: int, event: dict
    ) -> None:
        session_id = event.pop("session_id", None)
        event.pop("cost_usd", None)
        event.pop("context_usage", None)

        async with self.db_factory() as db:
            if session_id:
                await db.execute(
                    update(DiscussionAgent)
                    .where(DiscussionAgent.id == agent_id)
                    .values(session_id=session_id)
                )

            evt = DiscussionEvent(
                discussion_id=discussion_id,
                agent_id=agent_id,
                event_type=event.get("event_type", "unknown"),
                role=event.get("role"),
                content=event.get("content"),
                tool_name=event.get("tool_name"),
                tool_input=event.get("tool_input"),
                tool_output=event.get("tool_output"),
                raw_json=event.get("raw_json"),
                is_error=event.get("is_error", False),
                timestamp=datetime.now(timezone.utc),
            )
            db.add(evt)
            await db.commit()

        broadcast_data = {
            k: v for k, v in event.items() if k != "raw_json"
        }
        broadcast_data["agent_id"] = agent_id
        await self.broadcaster.broadcast(
            f"discussion:{discussion_id}:agent:{agent_id}",
            broadcast_data,
        )

    async def _save_facilitator_event(
        self, discussion_id: int, event_type: str, event: dict
    ) -> None:
        async with self.db_factory() as db:
            evt = DiscussionEvent(
                discussion_id=discussion_id,
                agent_id=0,
                event_type=event_type,
                role=event.get("role"),
                content=event.get("content"),
                tool_name=event.get("tool_name"),
                tool_input=event.get("tool_input"),
                tool_output=event.get("tool_output"),
                raw_json=event.get("raw_json"),
                is_error=event.get("is_error", False),
                timestamp=datetime.now(timezone.utc),
            )
            db.add(evt)
            await db.commit()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    async def _collect_agent_summaries(
        self, db: AsyncSession, discussion_id: int
    ) -> dict[str, str]:
        agents_result = await db.execute(
            select(DiscussionAgent).where(
                DiscussionAgent.discussion_id == discussion_id
            )
        )
        summaries: dict[str, str] = {}
        for agent in agents_result.scalars().all():
            events_result = await db.execute(
                select(DiscussionEvent)
                .where(
                    DiscussionEvent.agent_id == agent.id,
                    DiscussionEvent.event_type.in_(["result", "message"]),
                    DiscussionEvent.content.isnot(None),
                    DiscussionEvent.content != "",
                )
                .order_by(DiscussionEvent.id.desc())
                .limit(1)
            )
            last_evt = events_result.scalars().first()
            if last_evt and last_evt.content:
                content = last_evt.content
                if len(content) > 2000:
                    content = content[:2000] + "..."
                summaries[agent.role_name] = content
        return summaries

    async def _get_history(
        self, db: AsyncSession, discussion_id: int
    ) -> list[DiscussionMessage]:
        result = await db.execute(
            select(DiscussionMessage)
            .where(DiscussionMessage.discussion_id == discussion_id)
            .order_by(DiscussionMessage.id)
        )
        return list(result.scalars().all())

    def _write_history_file(
        self, discussion_id: int, messages: list[DiscussionMessage]
    ) -> str:
        lines = [f"# Discussion #{discussion_id} — History\n"]
        for msg in messages:
            prefix = msg.agent_role_name or msg.role
            lines.append(f"### [{prefix}]\n{msg.content}\n")

        fd, path = tempfile.mkstemp(
            prefix=f"discussion_{discussion_id}_", suffix=".md"
        )
        with os.fdopen(fd, "w") as f:
            f.write("\n".join(lines))
        return path

    async def _write_agent_outputs_file(
        self, db: AsyncSession, discussion_id: int
    ) -> str:
        """Write all agents' full message outputs to a temp file for Facilitator to read."""
        agents_result = await db.execute(
            select(DiscussionAgent).where(
                DiscussionAgent.discussion_id == discussion_id
            )
        )
        lines = [f"# Discussion #{discussion_id} — Agent Outputs\n"]
        for agent in agents_result.scalars().all():
            lines.append(f"## {agent.role_name} (status: {agent.status})\n")
            events_result = await db.execute(
                select(DiscussionEvent)
                .where(
                    DiscussionEvent.agent_id == agent.id,
                    DiscussionEvent.event_type.in_(["message", "result"]),
                    DiscussionEvent.content.isnot(None),
                    DiscussionEvent.content != "",
                )
                .order_by(DiscussionEvent.id)
            )
            events = events_result.scalars().all()
            if events:
                for evt in events:
                    lines.append(evt.content)
                    lines.append("")
            else:
                lines.append("(no output yet)\n")

        fd, path = tempfile.mkstemp(
            prefix=f"discussion_{discussion_id}_outputs_", suffix=".md"
        )
        with os.fdopen(fd, "w") as f:
            f.write("\n".join(lines))
        return path

    # ------------------------------------------------------------------
    # Facilitator: initial role assignment
    # ------------------------------------------------------------------
    async def _run_facilitator_init(
        self, disc: Discussion, history_file: str, cwd: str | None = None
    ) -> list[dict]:
        prompt = f"""\
You are a discussion facilitator. Your job is to analyze the conversation so far
and decide which expert perspectives should respond to the latest message.

Read the discussion history at: {history_file}

After reading, decide how many experts should respond (1 to {disc.max_agents}) and
what role each should take. Choose roles that ADD NEW PERSPECTIVES not already
well-covered in the discussion.

Respond with ONLY a JSON array, no other text:
[
  {{"role_name": "角色名 (e.g. 架构师, 安全顾问, 成本分析)", "system_prompt": "你是...的专家，从...角度分析问题", "brief": "一句话说明为什么需要这个角色"}},
  ...
]

Rules:
- 1 to {disc.max_agents} roles max
- Role names in Chinese
- system_prompt should be specific and actionable
- If the discussion just started, pick 2-3 foundational perspectives
- If a perspective is already well-covered, don't repeat it
- ONLY output the JSON array"""

        try:
            collected_text = await self._run_facilitator_process(disc, prompt, cwd=cwd)
        except Exception as e:
            raise RuntimeError(f"Facilitator failed: {e}")

        raw = "\n".join(collected_text)
        return self._parse_facilitator_roles(raw, disc.max_agents)

    async def _run_facilitator_add_one(
        self, disc: Discussion, history_file: str, existing_roles: list[str], cwd: str | None = None
    ) -> dict:
        roles_str = ", ".join(existing_roles) if existing_roles else "(none)"
        prompt = f"""\
You are a discussion facilitator. The discussion already has these expert roles: {roles_str}

Read the discussion history at: {history_file}

Decide ONE new expert perspective that is currently MISSING and would add the most value.
Do NOT repeat any existing role.

Respond with ONLY a single JSON object, no other text:
{{"role_name": "角色名", "system_prompt": "你是...的专家，从...角度分析问题", "brief": "一句话说明为什么需要这个角色"}}

Rules:
- Role name in Chinese
- system_prompt should be specific and actionable
- Must be different from existing roles: {roles_str}
- ONLY output the JSON object"""

        try:
            collected_text = await self._run_facilitator_process(disc, prompt, cwd=cwd)
        except Exception as e:
            raise RuntimeError(f"Facilitator failed: {e}")

        raw = "\n".join(collected_text).strip()
        if raw.startswith("```"):
            lines = raw.split("\n")
            lines = [l for l in lines if not l.strip().startswith("```")]
            raw = "\n".join(lines).strip()

        try:
            data = json.loads(raw)
            if isinstance(data, dict) and "role_name" in data and "system_prompt" in data:
                return data
        except (json.JSONDecodeError, TypeError):
            pass

        logger.warning("Could not parse add-one response, using fallback. Raw: %s", raw[:200])
        return {
            "role_name": "补充视角",
            "system_prompt": "你是一位综合分析师，从其他角色尚未覆盖的角度提供分析和建议。",
            "brief": "补充缺失的分析视角",
        }

    def _parse_facilitator_roles(
        self, raw: str, max_agents: int
    ) -> list[dict]:
        text = raw.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            lines = [l for l in lines if not l.strip().startswith("```")]
            text = "\n".join(lines).strip()

        try:
            data = json.loads(text)
            if isinstance(data, list):
                roles = []
                for item in data[:max_agents]:
                    if (
                        isinstance(item, dict)
                        and "role_name" in item
                        and "system_prompt" in item
                    ):
                        roles.append(item)
                if roles:
                    return roles
        except (json.JSONDecodeError, TypeError):
            pass

        logger.warning("Could not parse facilitator response, using defaults. Raw: %s", text[:200])
        return [
            {
                "role_name": "技术架构师",
                "system_prompt": "你是一位资深技术架构师，擅长系统设计、可扩展性分析和技术选型。从架构层面分析问题。",
                "brief": "提供架构层面的分析",
            },
            {
                "role_name": "产品视角",
                "system_prompt": "你是一位产品经理，擅长用户需求分析、优先级排序和 MVP 定义。从产品和用户体验角度分析问题。",
                "brief": "提供产品和用户视角",
            },
        ]


def _msg_to_dict(msg: DiscussionMessage) -> dict:
    return {
        "id": msg.id,
        "discussion_id": msg.discussion_id,
        "role": msg.role,
        "agent_role_name": msg.agent_role_name,
        "content": msg.content,
        "created_at": msg.created_at.isoformat() if msg.created_at else None,
    }


def _agent_to_dict(agent: DiscussionAgent) -> dict:
    return {
        "id": agent.id,
        "discussion_id": agent.discussion_id,
        "role_name": agent.role_name,
        "system_prompt": agent.system_prompt,
        "session_id": agent.session_id,
        "status": agent.status,
        "created_at": agent.created_at.isoformat() if agent.created_at else None,
    }
