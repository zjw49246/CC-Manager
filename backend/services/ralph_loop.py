import asyncio
import logging
from datetime import datetime

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.instance import Instance
from backend.models.log_entry import LogEntry
from backend.models.task import Task
from backend.config import settings
from backend.services.cancellation import await_task_completion
from backend.services.instance_manager import InstanceManager
from backend.services.dispatcher import TaskStartPausedError
from backend.services.task_queue import (
    TaskGenerationFence,
    TaskQueue,
    append_task_generation_predicates,
    task_generation_fence,
)
from backend.services.worker_task_termination import (
    no_active_worker_task_termination_predicate,
)
from backend.services.ws_broadcaster import WebSocketBroadcaster

logger = logging.getLogger(__name__)
DEFAULT_RALPH_STOP_TIMEOUT = 15.0


class RalphLoop:
    """Auto-continuation loop: pick task -> run -> repeat.

    Claude Code handles worktree creation, git operations, and cleanup
    autonomously based on the project's CLAUDE.md instructions.
    """

    def __init__(
        self,
        db_factory,
        instance_manager: InstanceManager,
        broadcaster: WebSocketBroadcaster,
        *,
        test_harness_service=None,
    ):
        if test_harness_service is None:
            from backend.database import async_session
            from backend.services.test_harness import (
                TestHarnessService,
                test_harness_service as default_test_harness_service,
            )

            test_harness_service = (
                default_test_harness_service
                if db_factory is async_session
                else TestHarnessService(db_factory=db_factory)
            )
        self.db_factory = db_factory
        self.instance_manager = instance_manager
        self.broadcaster = broadcaster
        self.test_harness_service = test_harness_service
        self._loops: dict[int, asyncio.Task] = {}
        self._plan_lifecycles: dict[int, tuple[int, asyncio.Task]] = {}
        self._shutting_down = False

    async def start(self, instance_id: int):
        if self._shutting_down:
            raise RuntimeError("Ralph loop is shutting down")
        if instance_id in self._loops and not self._loops[instance_id].done():
            return
        self._loops[instance_id] = asyncio.create_task(self._loop(instance_id))
        logger.info(f"Ralph loop started for instance {instance_id}")

    async def stop(
        self,
        instance_id: int,
        *,
        timeout: float = DEFAULT_RALPH_STOP_TIMEOUT,
    ) -> bool:
        """Cancel one loop with a bounded, evidence-preserving wait."""

        task = self._loops.get(instance_id)
        if task and not task.done():
            task.cancel()
            done, pending = await asyncio.wait({task}, timeout=timeout)
            if pending:
                logger.error(
                    "Ralph loop for instance %s ignored cancellation for %.1fs",
                    instance_id,
                    timeout,
                )
                # Keep the exact task registered so a later admin stop or
                # shutdown can retry.  Popping here would make a still-live
                # dequeue owner invisible and allow unsafe instance deletion.
                return False
            await asyncio.gather(*done, return_exceptions=True)
        if self._loops.get(instance_id) is task:
            self._loops.pop(instance_id, None)
        logger.info(f"Ralph loop stopped for instance {instance_id}")
        return True

    async def shutdown(
        self,
        *,
        timeout: float = DEFAULT_RALPH_STOP_TIMEOUT,
    ) -> None:
        """Close admission and settle every legacy dequeue producer."""

        self._shutting_down = True
        observed = dict(self._loops)
        pending_tasks = {
            task for task in observed.values() if not task.done()
        }
        for task in pending_tasks:
            task.cancel()
        if pending_tasks:
            done, pending = await asyncio.wait(
                pending_tasks,
                timeout=timeout,
            )
            if done:
                await asyncio.gather(*done, return_exceptions=True)
            if pending:
                # Retain exact registrations so shutdown retry/diagnostics can
                # still find every producer that ignored cancellation.
                raise RuntimeError(
                    f"{len(pending)} Ralph loop(s) ignored shutdown "
                    f"cancellation for {timeout:.1f}s"
                )
        for instance_id, task in observed.items():
            if task.done() and self._loops.get(instance_id) is task:
                self._loops.pop(instance_id, None)
        logger.info("Ralph loop shutdown complete")

    def is_running(self, instance_id: int) -> bool:
        task = self._loops.get(instance_id)
        return task is not None and not task.done()

    async def stop_plan_agent_lifecycle(
        self,
        task_id: int,
        *,
        timeout: float = DEFAULT_RALPH_STOP_TIMEOUT,
    ) -> bool:
        """Cancel one exact Plan child and settle its Ralph producer."""

        registered = self._plan_lifecycles.get(task_id)
        if registered is None:
            return False
        instance_id, lifecycle = registered
        if lifecycle.done():
            return False
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        lifecycle.cancel()
        done, pending = await asyncio.wait(
            {lifecycle},
            timeout=max(0.0, deadline - loop.time()),
        )
        if pending:
            return False
        await asyncio.gather(*done, return_exceptions=True)
        # Awaiting the cancelled child propagates cancellation into the legacy
        # producer. Settle it too so it cannot reclaim the still-active DB row
        # before the API publishes the terminal generation.
        producer = self._loops.get(instance_id)
        if producer is not None and not producer.done():
            producer.cancel()
            done, pending = await asyncio.wait(
                {producer},
                timeout=max(0.0, deadline - loop.time()),
            )
            if pending:
                return False
            await asyncio.gather(*done, return_exceptions=True)
        if (
            producer is not None
            and self._loops.get(instance_id) is producer
            and producer.done()
        ):
            self._loops.pop(instance_id, None)
        from backend.services.plan_agent_runner import (
            has_unreaped_plan_agent_for_task,
        )

        if has_unreaped_plan_agent_for_task(task_id):
            return False
        return True

    async def _launch_task_on_bound_account(
        self,
        instance_id: int,
        task: Task,
        prompt: str,
        cwd: str,
        *,
        source_log_id: int,
    ) -> int:
        """Launch through the same provider-account resolver as Dispatcher.

        Ralph is a legacy dequeue path, but it still runs normal Task rows.  A
        Codex task must therefore keep its native thread and CODEX_HOME binding
        instead of silently inheriting the service's default account.
        """

        from backend.main import dispatcher
        from backend.services.dispatcher import _prepend_task_artifact_policy

        config_dir = await dispatcher._resolve_resume_config_dir(
            task.session_id,
            task.provider,
            task_id=task.id,
            model=task.model,
            codex_service_tier=task.codex_service_tier,
        )
        resume_session_id = (
            task.session_id
            if (task.provider or "claude").lower() == "codex"
            else None
        )
        return await self.instance_manager.launch(
            instance_id=instance_id,
            prompt=_prepend_task_artifact_policy(task, prompt),
            task_id=task.id,
            task_turn_generation=task.turn_generation,
            cwd=cwd,
            model=task.model,
            codex_service_tier=task.codex_service_tier,
            resume_session_id=resume_session_id,
            thinking_budget=task.thinking_budget,
            provider=task.provider,
            config_dir=config_dir,
            source_log_id=source_log_id,
            initiating_user_id=task.execution_user_id,
            initiating_user_role=task.execution_user_role,
            execution_mode=task.execution_mode,
            execution_principal_kind=task.execution_principal_kind,
        )

    async def _bind_claimed_turn_source(
        self,
        instance_id: int,
        task: Task,
    ) -> int:
        """Bind Ralph's exact dequeue generation before provider admission."""

        from backend.services.dispatcher import _turn_transport_name
        from backend.services.terminal_arbitration import bind_turn_source

        predicates = [
            Task.id == task.id,
            Task.status.in_(("in_progress", "executing")),
            Task.instance_id == instance_id,
            Task.worker_id.is_(None),
            Task.shared_from_id.is_(None),
            no_active_worker_task_termination_predicate(),
        ]
        append_task_generation_predicates(
            predicates,
            task_generation_fence(task),
        )
        async with self.db_factory() as db:
            # Share the same portable Task writer fence as provider admission.
            # Either this source binding commits first, or cancellation/retry
            # changes the exact generation and makes the bind fail closed.
            guarded = await db.execute(
                update(Task)
                .where(*predicates)
                .values(turn_source_log_id=Task.turn_source_log_id)
            )
            if guarded.rowcount != 1:
                await db.rollback()
                raise RuntimeError(
                    "Ralph Task generation changed before source binding"
                )
            current = (
                await db.execute(
                    select(Task).where(*predicates).with_for_update()
                )
            ).scalar_one_or_none()
            if current is None:
                await db.rollback()
                raise RuntimeError(
                    "Ralph Task generation disappeared before source binding"
                )
            source = await bind_turn_source(
                db,
                task=current,
                source_log_id=None,
                instance_id=instance_id,
                transport=_turn_transport_name(current),
            )
            await db.commit()

        task.turn_source_log_id = source.id
        return source.id

    async def _test_harness_terminal_context(
        self,
        task_id: int,
        instance_id: int,
        generation: TaskGenerationFence,
        *,
        reason: str,
        allow_background_handoff: bool = False,
    ):
        """Return the exact Harness owner fence, or ``None`` when stale.

        This lookup is deliberately read-only. ``owner_stop_fence`` performs
        the first writer operation and durably closes Harness admission before
        a Ralph terminalizer may touch the Task or its Instance lifecycle.
        """

        from backend.services.test_harness_owner_fence import (
            test_harness_owner_identity,
        )

        async with self.db_factory() as db:
            current = (
                await db.execute(
                    select(Task).where(
                        Task.id == task_id,
                        Task.status.in_(("in_progress", "executing")),
                        Task.instance_id == instance_id,
                        Task.worker_id.is_(None),
                        Task.shared_from_id.is_(None),
                        no_active_worker_task_termination_predicate(),
                    )
                )
            ).scalar_one_or_none()
            if current is None:
                return None
            current_generation = task_generation_fence(current)
            if allow_background_handoff:
                same_generation = bool(
                    current_generation[:4] == generation[:4]
                    and current_generation[-1] == generation[-1]
                )
            else:
                same_generation = current_generation == generation
            if not same_generation:
                return None
            identity = test_harness_owner_identity(current)

        return self.test_harness_service.owner_stop_fence(
            task_id,
            reason=reason,
            expected_identity=identity,
        )

    async def _defer_isolated_browser_claim(
        self,
        instance_id: int,
        task: Task,
    ) -> bool | None:
        """Give a proven Browser child back to Dispatcher.

        ``None`` means this is an ordinary Task, ``True`` means the exact
        Browser claim was atomically restored to ready/pending, and ``False``
        means Browser ownership was indicated but could not be proven.  The
        latter is intentionally fail-closed: Ralph must retain the durable
        claim evidence instead of launching or generically failing the child.
        """

        from backend.models.test_harness import TestHarnessChildBinding
        from backend.services.test_harness_children import (
            CHILD_RUNNING,
            browser_binding_owner_identity,
            browser_child_binding_error,
            browser_child_owner_error,
        )

        metadata = task.metadata_ if isinstance(task.metadata_, dict) else {}
        marker_present = metadata.get("isolated_browser_agent") is True
        try:
            async with self.db_factory() as db:
                current = await db.get(Task, task.id, populate_existing=True)
                binding = await db.scalar(
                    select(TestHarnessChildBinding).where(
                        TestHarnessChildBinding.child_task_id == task.id
                    )
                )
                if binding is None and not marker_present:
                    return None
                if (
                    current is None
                    or binding is None
                    or not marker_present
                    or current.status not in ("in_progress", "executing")
                    or current.instance_id != instance_id
                    or task_generation_fence(current)
                    != task_generation_fence(task)
                    or binding.state != CHILD_RUNNING
                    or binding.claimed_retry_count != current.retry_count
                    or binding.claimed_instance_id != instance_id
                    or browser_child_binding_error(binding, current) is not None
                ):
                    await db.rollback()
                    return False
                try:
                    owner_identity = browser_binding_owner_identity(binding)
                except RuntimeError:
                    await db.rollback()
                    return False
                owner = await db.get(Task, owner_identity.task_id)
                if browser_child_owner_error(binding, owner) is not None:
                    await db.rollback()
                    return False

                return await TaskQueue(db).defer(
                    task.id,
                    "Ralph yielded isolated Browser child to Dispatcher",
                    instance_id=instance_id,
                    generation_fence=task_generation_fence(current),
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "Could not validate Browser child %s claimed by Ralph instance %s",
                task.id,
                instance_id,
            )
            return False

    async def _settle_automatic_failure(
        self,
        instance_id: int,
        task: Task,
        reason: str,
        *,
        defer_if_preflight: bool,
        generation: TaskGenerationFence | None = None,
    ) -> tuple[str, TaskGenerationFence] | None:
        """Defer only a proven pre-provider rejection; otherwise fail closed."""

        generation = generation or task_generation_fence(task)
        terminal_context = await self._test_harness_terminal_context(
            task.id,
            instance_id,
            generation,
            reason=reason,
            allow_background_handoff=True,
        )
        if terminal_context is None:
            return None
        try:
            async with terminal_context:
                return await self._settle_automatic_failure_under_harness_fence(
                    instance_id,
                    task,
                    reason,
                    defer_if_preflight=defer_if_preflight,
                    generation=generation,
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "Could not prove Test Harness cleanup before settling Ralph task %s",
                task.id,
            )
            return None

    async def _settle_automatic_failure_under_harness_fence(
        self,
        instance_id: int,
        task: Task,
        reason: str,
        *,
        defer_if_preflight: bool,
        generation: TaskGenerationFence,
    ) -> tuple[str, TaskGenerationFence] | None:
        """Settle one exact claim while its Harness owner fence is held."""

        from backend.services.terminal_arbitration import (
            source_alias_original_log_id,
            source_shape_is_canonical,
        )

        predicates = [
            Task.id == task.id,
            Task.status.in_(("in_progress", "executing")),
            Task.instance_id == instance_id,
            Task.worker_id.is_(None),
            Task.shared_from_id.is_(None),
            no_active_worker_task_termination_predicate(),
        ]
        append_task_generation_predicates(predicates, generation)
        async with self.db_factory() as db:
            # This no-op write and InstanceManager's actual-transport write use
            # the same Task-first order.  A routing failure cannot observe
            # NULL, race provider admission, and then return admitted work to
            # pending after the provider-boundary transaction commits.
            guarded = await db.execute(
                update(Task)
                .where(*predicates)
                .values(turn_source_log_id=Task.turn_source_log_id)
            )
            if guarded.rowcount != 1:
                # A PTY idle callback may have changed only the detached
                # background marker after the caller captured its foreground
                # fence. Adopt that marker while rejecting every true ABA.
                await db.rollback()
                current = (
                    await db.execute(
                        select(Task).where(
                            Task.id == task.id,
                            Task.status.in_(("in_progress", "executing")),
                            Task.instance_id == instance_id,
                            Task.worker_id.is_(None),
                            Task.shared_from_id.is_(None),
                            no_active_worker_task_termination_predicate(),
                        )
                    )
                ).scalar_one_or_none()
                if (
                    current is None
                    or task_generation_fence(current)[:4] != generation[:4]
                    or task_generation_fence(current)[-1] != generation[-1]
                ):
                    await db.rollback()
                    return None
                generation = task_generation_fence(current)
                await db.rollback()
                predicates = [
                    Task.id == task.id,
                    Task.status.in_(("in_progress", "executing")),
                    Task.instance_id == instance_id,
                    Task.worker_id.is_(None),
                    Task.shared_from_id.is_(None),
                    no_active_worker_task_termination_predicate(),
                ]
                append_task_generation_predicates(predicates, generation)
                guarded = await db.execute(
                    update(Task)
                    .where(*predicates)
                    .values(turn_source_log_id=Task.turn_source_log_id)
                )
                if guarded.rowcount != 1:
                    await db.rollback()
                    return None
            current = (
                await db.execute(
                    select(Task).where(*predicates).with_for_update()
                )
            ).scalar_one_or_none()
            if current is None:
                await db.rollback()
                return None

            source = None
            original_source = None
            source_id = current.turn_source_log_id
            if type(source_id) is int and source_id > 0:
                source = (
                    await db.execute(
                        select(LogEntry)
                        .where(LogEntry.id == source_id)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                original_id = source_alias_original_log_id(source)
                if original_id is not None:
                    original_source = (
                        await db.execute(
                            select(LogEntry)
                            .where(LogEntry.id == original_id)
                            .with_for_update()
                        )
                    ).scalar_one_or_none()
            source_is_exact = bool(
                source is not None
                and source.task_id == current.id
                and source.task_retry_count == current.retry_count
                and source.task_turn_generation == current.turn_generation
                and source.turn_scope == "source"
                and source_shape_is_canonical(source, original_source)
            )
            preflight_rejection = bool(
                defer_if_preflight
                and source_is_exact
                and source.actual_transport is None
            )
            if preflight_rejection:
                current.status = "pending"
                current.instance_id = None
                current.started_at = None
                current.completed_at = None
                current.error_message = reason[:500]
                status = "pending"
            else:
                transport = (
                    source.actual_transport
                    if source_is_exact and source.actual_transport is not None
                    else "unknown"
                )
                current.status = "failed"
                current.completed_at = datetime.utcnow()
                current.error_message = (
                    f"{reason}; Ralph automatic replay was blocked because "
                    f"the exact provider outcome is uncertain "
                    f"(transport={transport})"
                )[:2000]
                status = "failed"
            await db.flush()
            resulting_generation = task_generation_fence(current)
            await db.commit()
        return status, resulting_generation

    async def _wait_for_turn(
        self,
        instance_id: int,
        task: Task,
        process,
        *,
        label: str,
    ) -> None:
        """Wait for both the CLI turn and its output/account bookkeeping."""

        if process:
            try:
                await asyncio.wait_for(
                    process.wait(), timeout=settings.task_timeout_seconds
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "%s for task %s timed out after %ss, killing process",
                    label,
                    task.id,
                    settings.task_timeout_seconds,
                )
                killed = await self.instance_manager.kill_process_generation(
                    instance_id,
                    process,
                )
                if not killed:
                    raise RuntimeError(
                        f"Timed-out process generation changed for instance {instance_id}"
                    )

        try:
            await self.instance_manager.wait_for_output_consumer(
                instance_id,
                provider=task.provider,
                timeout=30,
                expected_process=process,
            )
        except asyncio.TimeoutError as exc:
            # The CLI parent may be terminal while its output consumer still
            # owns process-group reaping, DB finalization, or account/session
            # migration.  Treating that timeout as success would let Ralph
            # mark the task complete and reuse the Instance while the exact
            # generation is still live.  Bubble into the fail-closed lifecycle
            # handler, which retains and reaps the observed process identity.
            raise RuntimeError(
                f"Output consumer did not finish after {label} for task "
                f"{task.id}"
            ) from exc

    def _effective_process_exit_code(self, instance_id: int, process) -> int:
        """Return the exact turn's provider-semantic result."""

        if process is None:
            return -1
        resolver = getattr(self.instance_manager, "effective_exit_code", None)
        if callable(resolver):
            value = resolver(instance_id, process)
            if isinstance(value, int):
                return value
        returncode = getattr(process, "returncode", None)
        return returncode if isinstance(returncode, int) else -1

    async def _mark_completed_with_background_handoff(
        self,
        task_id: int,
        instance_id: int,
        foreground_generation: TaskGenerationFence,
    ) -> TaskGenerationFence | None:
        """Complete the exact Ralph turn even if its PTY marker just changed.

        The PTY idle callback can arm the detached background marker after
        Ralph captured its foreground fence but before ``mark_completed``.
        That marker-only transition is part of the same turn, not an ABA.
        Re-read it under the Task row lock, reject every other field change,
        then retry with the newly observed exact fence.  The returned value is
        the DB-normalized post-commit snapshot used for publication.
        """

        terminal_context = await self._test_harness_terminal_context(
            task_id,
            instance_id,
            foreground_generation,
            reason="Ralph task completed",
            allow_background_handoff=True,
        )
        if terminal_context is None:
            return None
        try:
            async with terminal_context:
                return await self._mark_completed_under_harness_fence(
                    task_id,
                    instance_id,
                    foreground_generation,
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "Could not prove Test Harness cleanup before completing Ralph task %s",
                task_id,
            )
            return None

    async def _mark_completed_under_harness_fence(
        self,
        task_id: int,
        instance_id: int,
        foreground_generation: TaskGenerationFence,
    ) -> TaskGenerationFence | None:
        """Complete one exact Ralph generation under its owner fence."""

        async with self.db_factory() as db:
            completed = await self._mark_completed_generation(
                db,
                task_id,
                instance_id=instance_id,
                generation_fence=foreground_generation,
            )

        if not completed:
            async with self.db_factory() as db:
                current = (
                    await db.execute(
                        select(Task)
                        .where(Task.id == task_id)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if (
                    current is None
                    or current.status not in ("in_progress", "executing")
                    or task_generation_fence(current)[:4]
                    != foreground_generation[:4]
                    or task_generation_fence(current)[-1]
                    != foreground_generation[-1]
                ):
                    await db.rollback()
                    return None
                adopted_generation = task_generation_fence(current)
                completed = await self._mark_completed_generation(
                    db,
                    task_id,
                    instance_id=instance_id,
                    generation_fence=adopted_generation,
                )
            if not completed:
                return None

        async with self.db_factory() as db:
            current = (
                await db.execute(
                    select(Task)
                    .where(Task.id == task_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if (
                current is None
                or current.status != "completed"
                or current.retry_count != foreground_generation[0]
                or current.instance_id != foreground_generation[1]
                or current.started_at != foreground_generation[2]
                or current.turn_generation != foreground_generation[-1]
                or current.completed_at is None
            ):
                await db.rollback()
                return None
            resulting_generation = task_generation_fence(current)
            await db.commit()
            return resulting_generation

    @staticmethod
    async def _mark_completed_generation(
        db: AsyncSession,
        task_id: int,
        *,
        instance_id: int,
        generation_fence: TaskGenerationFence,
    ) -> bool:
        """Commit completion only for the exact local Ralph owner."""

        predicates = [
            Task.id == task_id,
            Task.status.in_(("in_progress", "executing")),
            Task.instance_id == instance_id,
            Task.worker_id.is_(None),
            Task.shared_from_id.is_(None),
            no_active_worker_task_termination_predicate(),
        ]
        append_task_generation_predicates(predicates, generation_fence)
        completed = await db.execute(
            update(Task)
            .where(*predicates)
            .values(
                status="completed",
                completed_at=datetime.utcnow(),
                error_message=None,
            )
        )
        await db.commit()
        return bool(completed.rowcount)

    async def _broadcast_generation_event(
        self,
        task_id: int,
        original_generation: TaskGenerationFence,
        expected_status: str,
        event: dict,
        *,
        retry_count_delta: int = 0,
        released: bool = False,
        terminal: bool = False,
    ) -> bool:
        """Publish while holding a no-op lock on the exact resulting Task.

        The state transition itself commits before publication. This second
        exact UPDATE prevents a retry/reclaim from slipping between the final
        generation check and the awaited WebSocket broadcast.
        """

        (
            original_retry_count,
            original_instance_id,
            original_started_at,
            original_completed_at,
            original_background_generation,
            original_turn_generation,
        ) = original_generation
        expected_retry_count = original_retry_count + retry_count_delta
        expected_instance_id = None if released else original_instance_id
        expected_started_at = None if released else original_started_at

        async with self.db_factory() as db:
            current = await db.get(Task, task_id)
            if (
                current is None
                or current.status != expected_status
                or current.retry_count != expected_retry_count
                or current.instance_id != expected_instance_id
                or current.started_at != expected_started_at
                or current.turn_generation != original_turn_generation
                or current.pty_background_generation
                != original_background_generation
                or (
                    terminal
                    and current.completed_at is None
                )
                or (
                    not terminal
                    and current.completed_at
                    != (None if released else original_completed_at)
                )
            ):
                return False

            resulting_generation = task_generation_fence(current)
            predicates = [
                Task.id == task_id,
                Task.status == expected_status,
            ]
            append_task_generation_predicates(
                predicates,
                resulting_generation,
            )
            predicates.append(
                no_active_worker_task_termination_predicate()
            )
            locked = await db.execute(
                update(Task)
                .where(*predicates)
                .values(status=expected_status)
            )
            if not locked.rowcount:
                await db.rollback()
                return False
            try:
                await self.broadcaster.broadcast(
                    "tasks",
                    {
                        **event,
                        # Publish the exact post-commit durable snapshot.  In
                        # particular, never infer detached PTY activity from
                        # the foreground status supplied by the caller.
                        "background_active": (
                            current.pty_background_generation is not None
                        ),
                    },
                )
            except Exception:
                logger.exception(
                    "Failed to broadcast Ralph generation event for task %s",
                    task_id,
                )
            await db.commit()
            return True

    async def _handle_account_routing_failure(
        self,
        instance_id: int,
        task: Task,
        reason: str,
        *,
        retry_after: float | None,
    ) -> float:
        """Release a Ralph-owned task when account routing cannot launch it."""

        task_id = task.id
        settled = await self._settle_automatic_failure(
            instance_id,
            task,
            reason,
            defer_if_preflight=retry_after is not None,
        )
        if settled is None:
            return 0.0
        status, resulting_generation = settled
        delay = (
            max(1.0, min(float(retry_after), 300.0))
            if status == "pending" and retry_after is not None
            else 0.0
        )

        await self._broadcast_generation_event(
            task_id,
            resulting_generation,
            status,
            {
            "event": "status_change",
            "task_id": task_id,
            "new_status": status,
            "instance_id": instance_id,
            "reason": "codex_account_wait" if status == "pending" else "codex_account_routing",
            },
            released=False,
            terminal=status == "failed",
        )
        return delay

    async def _store_plan_if_owned(
        self,
        instance_id: int,
        task: Task,
        plan_content: str,
        *,
        metadata_updates: dict | None = None,
    ) -> bool:
        """Publish a plan only while this Ralph generation owns the task."""

        predicates = [
            Task.id == task.id,
            Task.status.in_(("in_progress", "executing")),
            Task.instance_id == instance_id,
            no_active_worker_task_termination_predicate(),
        ]
        append_task_generation_predicates(
            predicates,
            task_generation_fence(task),
        )
        async with self.db_factory() as db:
            guarded = await db.execute(
                update(Task)
                .where(*predicates)
                .values(status=Task.status)
            )
            if guarded.rowcount != 1:
                await db.rollback()
                return False
            current = (
                await db.execute(
                    select(Task)
                    .where(*predicates)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if current is None:
                await db.rollback()
                return False
            metadata = dict(current.metadata_ or {})
            metadata.update(metadata_updates or {})
            current.plan_content = plan_content
            current.status = "plan_review"
            current.metadata_ = metadata
            current.error_message = None
            await db.commit()
        return True

    async def _record_cancel_cleanup_failure(
        self,
        instance_id: int,
        task_id: int,
        reason: str,
        *,
        instance_snapshot: tuple[
            str,
            int | None,
            int | None,
            datetime | None,
        ],
        generation_fence: TaskGenerationFence | None = None,
        task_statuses: tuple[str, ...] = ("in_progress", "executing"),
        broadcast_event: bool = True,
    ) -> bool:
        """Fail only the exact instance generation whose cleanup failed."""

        if generation_fence is None:
            return False
        terminal_context = await self._test_harness_terminal_context(
            task_id,
            instance_id,
            generation_fence,
            reason=reason,
            allow_background_handoff=True,
        )
        if terminal_context is None:
            return False
        try:
            async with terminal_context:
                lifecycle_lock = (
                    self.instance_manager._instance_lifecycle_lock(instance_id)
                )
                async with lifecycle_lock:
                    return await self._record_cancel_cleanup_failure_under_harness_fence(
                        instance_id,
                        task_id,
                        reason,
                        instance_snapshot=instance_snapshot,
                        generation_fence=generation_fence,
                        task_statuses=task_statuses,
                        broadcast_event=broadcast_event,
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "Could not prove Test Harness cleanup before failing Ralph task %s",
                task_id,
            )
            return False

    async def _record_cancel_cleanup_failure_under_harness_fence(
        self,
        instance_id: int,
        task_id: int,
        reason: str,
        *,
        instance_snapshot: tuple[
            str,
            int | None,
            int | None,
            datetime | None,
        ],
        generation_fence: TaskGenerationFence,
        task_statuses: tuple[str, ...] = ("in_progress", "executing"),
        broadcast_event: bool = True,
    ) -> bool:
        """Persist cleanup failure after owner graph and lifecycle fencing."""

        message = reason[:500]
        (
            expected_status,
            expected_pid,
            expected_task_id,
            expected_started_at,
        ) = instance_snapshot
        if expected_task_id != task_id:
            return False

        async with self.db_factory() as db:
            # Lock/update the Task before the Instance, matching every other
            # dual-row lifecycle transaction.  If the Instance CAS below
            # loses to a replacement generation, rolling this transaction
            # back also restores the Task atomically.
            task_predicates = [
                Task.id == task_id,
                Task.status.in_(task_statuses),
                Task.instance_id == instance_id,
                Task.worker_id.is_(None),
                Task.shared_from_id.is_(None),
                no_active_worker_task_termination_predicate(),
            ]
            append_task_generation_predicates(
                task_predicates,
                generation_fence,
            )
            task_result = await db.execute(
                update(Task)
                .where(*task_predicates)
                .values(
                    status="failed",
                    error_message=message,
                    completed_at=datetime.utcnow(),
                )
            )
            if not task_result.rowcount:
                await db.rollback()
                return False

            instance_predicates = [
                Instance.id == instance_id,
                Instance.status == expected_status,
                Instance.current_task_id == expected_task_id,
            ]
            instance_predicates.append(
                Instance.pid.is_(None)
                if expected_pid is None
                else Instance.pid == expected_pid
            )
            instance_predicates.append(
                Instance.started_at.is_(None)
                if expected_started_at is None
                else Instance.started_at == expected_started_at
            )
            instance_result = await db.execute(
                update(Instance)
                .where(*instance_predicates)
                .values(status="error")
            )
            if not instance_result.rowcount:
                await db.rollback()
                return False
            await db.commit()

        if (
            task_result.rowcount
            and generation_fence is not None
            and broadcast_event
        ):
            await self._broadcast_generation_event(
                task_id,
                generation_fence,
                "failed",
                {
                    "event": "status_change",
                    "task_id": task_id,
                    "old_status": "in_progress",
                    "new_status": "failed",
                    "instance_id": instance_id,
                    "reason": "ralph_stop_cleanup_failed",
                },
                terminal=True,
            )
        return bool(task_result.rowcount)

    async def _read_settled_failed_generation(
        self,
        task_id: int,
        instance_id: int,
        *,
        task_generation: TaskGenerationFence,
        expected_instance_started_at: datetime | None,
    ) -> TaskGenerationFence | None:
        """Read the exact failed result committed by ``InstanceManager.stop``."""

        async with self.db_factory() as db:
            task = (
                await db.execute(
                    select(Task)
                    .where(
                        Task.id == task_id,
                        no_active_worker_task_termination_predicate(),
                    )
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if (
                task is None
                or task.status != "failed"
                or task_generation_fence(task)[:4]
                != task_generation[:4]
                or task_generation_fence(task)[-1]
                != task_generation[-1]
                or task.pty_background_generation is not None
            ):
                await db.rollback()
                return None

            instance = (
                await db.execute(
                    select(Instance)
                    .where(Instance.id == instance_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if (
                instance is None
                or instance.status != "error"
                or instance.pid is not None
                or instance.current_task_id is not None
                or instance.started_at != expected_instance_started_at
            ):
                await db.rollback()
                return None
            resulting_generation = task_generation_fence(task)
            await db.commit()
            return resulting_generation

    async def _release_cancelled_claim(
        self,
        instance_id: int,
        task: Task | None,
    ) -> None:
        """Stop a Ralph-owned turn and release only a proven-clean claim.

        Cancelling only the loop used to strand the task in ``in_progress``;
        cancelling while a subprocess was active also left that process running
        without a lifecycle owner. Cleanup failures retain the instance/process
        evidence and fail the task instead of creating a second runnable copy.
        """

        if task is None:
            return

        task_id = task.id
        observed_generation = task_generation_fence(task)
        instance_snapshot: tuple[
            str,
            int | None,
            int | None,
            datetime | None,
        ] | None = None
        try:
            async with self.db_factory() as db:
                current = (
                    await db.execute(
                        select(Task)
                        .where(Task.id == task_id)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                instance = await db.get(Instance, instance_id)
                if (
                    current is None
                    or current.instance_id != instance_id
                    or current.status not in ("in_progress", "executing")
                    or task_generation_fence(current)[:4]
                    != observed_generation[:4]
                    or task_generation_fence(current)[-1]
                    != observed_generation[-1]
                ):
                    return
                # A PTY idle callback can arm or settle the detached marker
                # after Ralph captured its foreground Task.  The first four
                # fields prove this is still the same dequeue generation; use
                # the marker currently protected by the Task row lock for all
                # subsequent exact stop/defer/failure operations.
                current_generation = task_generation_fence(current)
                task.turn_source_log_id = current.turn_source_log_id
                if instance is not None:
                    instance_snapshot = (
                        instance.status,
                        instance.pid,
                        instance.current_task_id,
                        instance.started_at,
                    )
                process_owned = bool(
                    instance and instance.current_task_id == task_id
                )
        except Exception as exc:
            logger.exception(
                "Failed to inspect Ralph-owned process for task %s on instance %s",
                task_id,
                instance_id,
            )
            return

        manager_running = self.instance_manager.is_running(instance_id)
        if process_owned:
            stop_reason = (
                "Ralph loop stopped after provider admission; the exact "
                "turn outcome is uncertain"
            )
            terminal_context = await self._test_harness_terminal_context(
                task_id,
                instance_id,
                current_generation,
                reason=stop_reason,
                allow_background_handoff=True,
            )
            if terminal_context is None:
                return
            try:
                async with terminal_context:
                    try:
                        if not manager_running:
                            raise RuntimeError(
                                "Ralph could not prove that the persisted process "
                                "generation was reaped"
                            )
                        stopped = await self.instance_manager.stop(
                            instance_id,
                            expected_task_id=task_id,
                            expected_task_turn_generation=current_generation[-1],
                            expected_pid=instance_snapshot[1],
                            expected_started_at=instance_snapshot[3],
                            task_status="failed",
                            task_error_message=stop_reason,
                            terminal_consumer_timeout=30.0,
                            consumer_cancel_timeout=10.0,
                        )
                        if not stopped:
                            raise RuntimeError(
                                "Ralph process cleanup did not settle the owned "
                                "generation"
                            )
                    except Exception as exc:
                        logger.exception(
                            "Failed to stop Ralph-owned process for task %s on "
                            "instance %s",
                            task_id,
                            instance_id,
                        )
                        lifecycle_lock = (
                            self.instance_manager._instance_lifecycle_lock(
                                instance_id
                            )
                        )
                        async with lifecycle_lock:
                            await self._record_cancel_cleanup_failure_under_harness_fence(
                                instance_id,
                                task_id,
                                "Ralph loop stopped but process cleanup could not be "
                                f"confirmed: {exc}",
                                instance_snapshot=instance_snapshot,
                                generation_fence=current_generation,
                            )
                    # InstanceManager.stop owns the atomic process/consumer
                    # cleanup and claim release. Never inspect or mutate the
                    # Task after a successful commit: the slot may be reused.
                    return
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Could not prove Test Harness cleanup before stopping Ralph "
                    "task %s",
                    task_id,
                )
                return

        if manager_running:
            if instance_snapshot is not None:
                await self._record_cancel_cleanup_failure(
                    instance_id,
                    task_id,
                    "Ralph loop stopped while the Instance had a different "
                    "managed generation",
                    instance_snapshot=instance_snapshot,
                    generation_fence=current_generation,
                )
            return

        browser_handoff = await self._defer_isolated_browser_claim(
            instance_id,
            current,
        )
        if browser_handoff is not None:
            if browser_handoff:
                from backend.main import dispatcher

                dispatcher.wake()
            else:
                logger.error(
                    "Ralph cancellation retained unproven Browser child claim %s",
                    task_id,
                )
            return

        try:
            # Re-read the exact source under the same Task writer fence used by
            # provider admission.  A cancellation may return the claim to the
            # queue only while the source still proves no provider transport
            # was selected.
            settled = await self._settle_automatic_failure(
                instance_id,
                task,
                "Ralph loop stopped; task returned to the queue",
                defer_if_preflight=True,
                generation=current_generation,
            )
        except Exception:
            logger.exception(
                "Failed to release cancelled Ralph claim for task %s",
                task_id,
            )
            return

        if settled is not None:
            status, resulting_generation = settled
            await self._broadcast_generation_event(
                task_id,
                resulting_generation,
                status,
                {
                    "event": "status_change",
                    "task_id": task_id,
                    "old_status": "in_progress",
                    "new_status": status,
                    "instance_id": instance_id,
                    "reason": (
                        "ralph_stopped"
                        if status == "pending"
                        else "ralph_stop_outcome_uncertain"
                    ),
                },
                released=False,
                terminal=status == "failed",
            )

    async def _fail_unexpected_claim(
        self,
        instance_id: int,
        task: Task | None,
        exc: Exception,
    ) -> None:
        """Fail and reap the exact Ralph generation after an internal error.

        An exception may happen before launch, while the process is running, or
        in output bookkeeping.  Leaving the dequeue claim active makes Ralph
        sleep and retry its outer loop forever while the Task remains stuck.
        Mark the still-owned Task terminal first so no scheduler can duplicate
        uncertain work, then reap only the process object observed for that
        generation.  Process identity plus the persisted Instance snapshot
        prevent a rapid retry on the same reusable slot from being killed or
        overwritten by this stale error handler.
        """

        if task is None:
            return

        task_id = task.id
        reason = f"Ralph loop failed: {exc}"[:500]
        observed_generation = task_generation_fence(task)
        terminal_context = await self._test_harness_terminal_context(
            task_id,
            instance_id,
            observed_generation,
            reason=reason,
            allow_background_handoff=True,
        )
        if terminal_context is None:
            return
        try:
            async with terminal_context:
                await self._fail_unexpected_claim_under_harness_fence(
                    instance_id,
                    task_id,
                    reason,
                    observed_generation,
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "Could not prove Test Harness cleanup before failing Ralph task %s",
                task_id,
            )

    async def _fail_unexpected_claim_under_harness_fence(
        self,
        instance_id: int,
        task_id: int,
        reason: str,
        observed_generation: TaskGenerationFence,
    ) -> None:
        """Fail and reap one claim while its Harness owner fence is held."""

        instance_snapshot: tuple[
            str,
            int | None,
            int | None,
            datetime | None,
        ] | None = None
        process = None

        async with self.db_factory() as db:
            current = (
                await db.execute(
                    select(Task)
                    .where(Task.id == task_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            instance = await db.get(Instance, instance_id)
            if (
                current is None
                or current.status not in ("in_progress", "executing")
                or current.instance_id != instance_id
                or current.worker_id is not None
                or current.shared_from_id is not None
                or task_generation_fence(current)[:4]
                != observed_generation[:4]
                or task_generation_fence(current)[-1]
                != observed_generation[-1]
            ):
                return
            (
                expected_retry_count,
                _expected_instance_id,
                expected_started_at,
                expected_completed_at,
                expected_background_generation,
                expected_turn_generation,
            ) = task_generation_fence(current)
            if instance is not None:
                instance_snapshot = (
                    instance.status,
                    instance.pid,
                    instance.current_task_id,
                    instance.started_at,
                )
                if instance.current_task_id == task_id:
                    process = self.instance_manager.processes.get(instance_id)

            failed_at = datetime.utcnow()
            task_predicates = [
                Task.id == task_id,
                Task.status.in_(("in_progress", "executing")),
                Task.instance_id == instance_id,
                Task.worker_id.is_(None),
                Task.shared_from_id.is_(None),
                Task.retry_count == expected_retry_count,
                Task.turn_generation == expected_turn_generation,
                (
                    Task.started_at.is_(None)
                    if expected_started_at is None
                    else Task.started_at == expected_started_at
                ),
                (
                    Task.completed_at.is_(None)
                    if expected_completed_at is None
                    else Task.completed_at == expected_completed_at
                ),
                (
                    Task.pty_background_generation.is_(None)
                    if expected_background_generation is None
                    else Task.pty_background_generation
                    == expected_background_generation
                ),
                no_active_worker_task_termination_predicate(),
            ]
            result = await db.execute(
                update(Task)
                .where(*task_predicates)
                .values(
                    status="failed",
                    error_message=reason,
                    completed_at=failed_at,
                )
            )
            persisted_failed_at = None
            if result.rowcount:
                # MySQL DATETIME may normalize away microseconds.  Read the
                # exact stored value while this Task lock is still held before
                # using it as the cleanup generation fence.
                persisted_failed_at = await db.scalar(
                    select(Task.completed_at)
                    .where(Task.id == task_id)
                    .with_for_update()
                )
            await db.commit()

        if not result.rowcount:
            return

        failed_generation: TaskGenerationFence = (
            expected_retry_count,
            instance_id,
            expected_started_at,
            persisted_failed_at,
            expected_background_generation,
            expected_turn_generation,
        )
        publication_generation = failed_generation
        cleanup_error: Exception | None = None
        if instance_snapshot is not None and instance_snapshot[2] == task_id:
            if process is None:
                cleanup_error = RuntimeError(
                    "persisted Ralph process generation is not managed in memory"
                )
            else:
                try:
                    stopped = await self.instance_manager.stop(
                        instance_id,
                        expected_task_id=task_id,
                        expected_task_turn_generation=(
                            expected_turn_generation
                        ),
                        expected_pid=instance_snapshot[1],
                        expected_started_at=instance_snapshot[3],
                        task_status="failed",
                        task_error_message=reason,
                        terminal_consumer_timeout=30.0,
                        consumer_cancel_timeout=10.0,
                    )
                    if not stopped:
                        raise RuntimeError(
                            "exact Ralph failed generation could not be stopped"
                        )
                    settled_generation = (
                        await self._read_settled_failed_generation(
                            task_id,
                            instance_id,
                            task_generation=failed_generation,
                            expected_instance_started_at=instance_snapshot[3],
                        )
                    )
                    if settled_generation is None:
                        raise RuntimeError(
                            "settled Ralph generation lost its exact "
                            "Task/Instance cleanup CAS"
                        )
                    publication_generation = settled_generation
                    # If the map already points at a replacement process, exact
                    # identity did its job. Never fall back to task-id stop.
                except Exception as cleanup_exc:
                    cleanup_error = cleanup_exc

        if cleanup_error is not None:
            logger.error(
                "Failed to reap Ralph generation for task %s on instance %s",
                task_id,
                instance_id,
                exc_info=(
                    type(cleanup_error),
                    cleanup_error,
                    cleanup_error.__traceback__,
                ),
            )
            lifecycle_lock = self.instance_manager._instance_lifecycle_lock(
                instance_id
            )
            async with lifecycle_lock:
                await self._record_cancel_cleanup_failure_under_harness_fence(
                    instance_id,
                    task_id,
                    f"{reason}; process cleanup could not be confirmed: "
                    f"{cleanup_error}",
                    instance_snapshot=instance_snapshot,
                    generation_fence=failed_generation,
                    task_statuses=("failed",),
                    broadcast_event=False,
                )

        await self._broadcast_generation_event(
            task_id,
            publication_generation,
            "failed",
            {
                "event": "status_change",
                "task_id": task_id,
                "old_status": "in_progress",
                "new_status": "failed",
                "instance_id": instance_id,
                "reason": "ralph_internal_error",
            },
            terminal=True,
        )

    async def _loop(self, instance_id: int):
        logger.info(f"Ralph loop running for instance {instance_id}")
        dispatcher_only_ids: set[int] = set()
        while True:
            task = None
            try:
                # Dequeue next task
                from backend.main import dispatcher
                try:
                    async with dispatcher.task_start_guard():
                        async with self.db_factory() as db:
                            queue = TaskQueue(db)
                            task = await queue.dequeue(
                                exclude_ids=dispatcher_only_ids,
                                instance_id=instance_id,
                            )
                except TaskStartPausedError:
                    await dispatcher.wait_until_resumed()
                    continue

                if not task:
                    await asyncio.sleep(5)
                    continue

                browser_handoff = await self._defer_isolated_browser_claim(
                    instance_id,
                    task,
                )
                if browser_handoff is True:
                    dispatcher_only_ids.add(task.id)
                    dispatcher.wake()
                    continue
                if browser_handoff is False:
                    logger.error(
                        "Ralph retained unproven Browser child claim %s on "
                        "instance %s and stopped its producer",
                        task.id,
                        instance_id,
                    )
                    return

                logger.info(f"Instance {instance_id} picked task {task.id}: {task.title}")

                # Publish the claim while holding the exact resulting Task
                # generation. A cancellation/retry that wins after dequeue
                # must not be followed by a stale ``in_progress`` event, nor
                # by this Ralph loop launching that superseded claim.
                claim_is_current = await self._broadcast_generation_event(
                    task.id,
                    task_generation_fence(task),
                    "in_progress",
                    {
                        "event": "status_change",
                        "task_id": task.id,
                        "old_status": "pending",
                        "new_status": "in_progress",
                        "instance_id": instance_id,
                    },
                )
                if not claim_is_current:
                    continue

                cwd = task.target_repo or "."

                # Plan mode handling
                if task.mode == "plan":
                    if task.plan_approved is True:
                        from backend.services.legacy_plan_execution import (
                            is_legacy_approved_execution_carrier,
                        )

                        async with self.db_factory() as db:
                            legacy_execution = (
                                await is_legacy_approved_execution_carrier(
                                    db,
                                    task.id,
                                )
                            )
                        if not legacy_execution:
                            # Route this through the exact-generation failure
                            # cleanup below without admitting a provider.
                            raise RuntimeError(
                                "Approved Plan Tasks cannot launch ordinary "
                                "coding turns without an exact migrated "
                                "execution carrier"
                            )
                    elif task.plan_approved is None:
                        logger.info(
                            "Task %s is in Plan mode, running read-only pipeline",
                            task.id,
                        )
                        from backend.services.plan_agent_runner import (
                            PlanAgentRunner,
                        )
                        runner = PlanAgentRunner(
                            db_factory=self.db_factory,
                            instance_manager=self.instance_manager,
                            claude_pool=dispatcher.pool,
                            codex_pool=dispatcher.codex_pool,
                            cloudrouter_store=dispatcher.cloudrouter_store,
                            broadcaster=self.broadcaster,
                        )
                        plan_lifecycle = asyncio.create_task(
                            runner.run(task, cwd=cwd)
                        )
                        registered_plan = (instance_id, plan_lifecycle)
                        self._plan_lifecycles[task.id] = registered_plan
                        try:
                            plan_result = await plan_lifecycle
                        finally:
                            if self._plan_lifecycles.get(task.id) == registered_plan:
                                self._plan_lifecycles.pop(task.id, None)

                        stored = await self._store_plan_if_owned(
                            instance_id,
                            task,
                            plan_result.plan_content,
                            metadata_updates={
                                "plan_agent_run_id": plan_result.run_id,
                                "plan_review_verdict": plan_result.verdict,
                                "plan_review_feedback": plan_result.feedback,
                                "plan_review_exhausted": (
                                    plan_result.review_exhausted
                                ),
                            },
                        )
                        if stored:
                            await self._broadcast_generation_event(
                                task.id,
                                task_generation_fence(task),
                                "plan_review",
                                {
                                    "event": "plan_ready",
                                    "task_id": task.id,
                                    "instance_id": instance_id,
                                },
                            )
                        continue  # Move to next task; this one waits for approval
                    else:
                        raise RuntimeError(
                            "Rejected Plan Tasks cannot re-enter planning or "
                            "launch ordinary coding turns"
                        )

                # Normal execution — bind the exact dequeue generation before
                # account resolution or any provider transport can be admitted.
                source_log_id = await self._bind_claimed_turn_source(
                    instance_id,
                    task,
                )
                await self._launch_task_on_bound_account(
                    instance_id,
                    task,
                    task.description,
                    cwd,
                    source_log_id=source_log_id,
                )

                # Wait for process to finish (with timeout)
                process = self.instance_manager.processes.get(instance_id)
                await self._wait_for_turn(
                    instance_id,
                    task,
                    process,
                    label="Task run",
                )

                exit_code = self._effective_process_exit_code(
                    instance_id, process
                )

                # Handle result
                publication_generation = task_generation_fence(task)
                status = None
                if exit_code == 0:
                    resulting_generation = (
                        await self._mark_completed_with_background_handoff(
                            task.id,
                            instance_id,
                            publication_generation,
                        )
                    )
                    if resulting_generation is not None:
                        publication_generation = resulting_generation
                        status = "completed"
                else:
                    # A returned provider process necessarily passed
                    # InstanceManager's actual-transport boundary.  Exit text,
                    # including rate-limit/transient wording, cannot prove that
                    # tools or other external effects did not run.
                    settled = await self._settle_automatic_failure(
                        instance_id,
                        task,
                        f"Exit code: {exit_code}",
                        defer_if_preflight=False,
                    )
                    if settled is not None:
                        status, publication_generation = settled

                if status is not None:
                    await self._broadcast_generation_event(
                        task.id,
                        publication_generation,
                        status,
                        {
                            "event": "status_change",
                            "task_id": task.id,
                            "new_status": status,
                            "instance_id": instance_id,
                        },
                        released=False,
                        terminal=status in ("completed", "failed"),
                    )

            except asyncio.CancelledError:
                logger.info(f"Ralph loop cancelled for instance {instance_id}")
                # Once dequeue succeeds, cancellation must either reconcile the
                # active turn or atomically return the claim.  Await cleanup
                # before allowing stop() to report success. Repeated caller
                # cancellation must not interrupt this ownership handoff.
                cleanup = asyncio.create_task(
                    self._release_cancelled_claim(instance_id, task)
                )
                await await_task_completion(cleanup)
                cleanup.result()
                raise
            except Exception as e:
                from backend.services.codex_app_server import (
                    CodexAppServerBusyError,
                    CodexThreadHomeMismatchError,
                )
                from backend.services.dispatcher import CodexAccountRoutingError

                if task is not None and isinstance(
                    e,
                    (
                        CodexAccountRoutingError,
                        CodexAppServerBusyError,
                        CodexThreadHomeMismatchError,
                    ),
                ):
                    retry_after = (
                        e.retry_after
                        if isinstance(e, CodexAccountRoutingError)
                        else 5.0
                    )
                    delay = await self._handle_account_routing_failure(
                        instance_id,
                        task,
                        str(e),
                        retry_after=retry_after,
                    )
                    if delay:
                        await asyncio.sleep(delay)
                    continue
                logger.error(f"Ralph loop error for instance {instance_id}: {e}")
                cleanup = asyncio.create_task(
                    self._fail_unexpected_claim(instance_id, task, e)
                )
                cancellation = await await_task_completion(cleanup)
                cleanup.result()
                if cancellation is not None:
                    raise cancellation
                await asyncio.sleep(5)
