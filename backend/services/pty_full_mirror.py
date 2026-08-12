"""PTY autonomous-turn 全量镜像：让 idle-time 自主 turn 的产出进入聊天。

任务 27 实录（2026-07-13）：后台监视器（Bash run_in_background）正点回调、
session 自主醒来并写出完整报告——但 adapter 在 chat turn 结束时把
``session.on_autonomous_event`` 降级成 ``_subagent_only_callback``（只喂
子 agent 面板），assistant 的 text/thinking/tool_use 全部被丢弃：报告只
存在于 session JSONL，聊天里永久不可见（idle watcher 消费过的记录 reader
offset 已越过，下一条消息的 orphan 回填也捞不回来）。

历史包袱：降级的前身是 on_exit 直接 ``on_autonomous_event = None``，防的
是"重放旧 prompt"——idle watcher 产出的 user-role 事件曾被原样镜像成重复
的用户消息。该风险现由 ``InstanceManager._process_event`` 的 autonomous
user-role 消毒承担（<task-notification> 压成一行 system_event，其余 user
记录直接丢弃），因此这里可以安全恢复全量转发；子 agent 面板的 upsert 在
``_process_event`` 内部完成，行为不变。
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import secrets
from typing import Any

from claude_pty.adapters.ccm import CCMBackend

logger = logging.getLogger(__name__)


_BACKGROUND_TASK_ID_RE = re.compile(
    r"<task-id>\s*([^<\s]+)\s*</task-id>"
)
_BACKGROUND_TOOL_USE_ID_RE = re.compile(
    r"<tool-use-id>\s*([^<\s]+)\s*</tool-use-id>"
)
_BACKGROUND_STATUS_RE = re.compile(
    r"<status>\s*([^<\s]+)\s*</status>"
)
_BACKGROUND_TERMINAL_STATUSES = frozenset(
    {
        "cancelled",
        "completed",
        "failed",
        "killed",
        "stopped",
        "timed_out",
    }
)


class _CCMBackgroundWorkTracker:
    """Extend claude-pty's native tracker with Bash harness task ids.

    ``SubagentTracker`` intentionally knows only Agent/Task/Monitor. Claude's
    ``Bash(run_in_background=true)`` uses the same harness notification model,
    but its foreground tool result exposes a structured ``backgroundTaskId``.
    Keeping that exact tool-use ↔ task-id mapping here lets both CCM and
    SessionPool treat the native session as non-idle until its matching
    terminal notification arrives.
    """

    def __init__(self, native_tracker: Any | None):
        self.native_tracker = native_tracker
        self.background_commands: dict[str, str | None] = {}

    def __getattr__(self, name: str) -> Any:
        native = object.__getattribute__(self, "native_tracker")
        if native is None:
            raise AttributeError(name)
        return getattr(native, name)

    @property
    def has_pending(self) -> bool:
        native = self.native_tracker
        return bool(
            self.background_commands
            or (native is not None and native.has_pending)
        )

    @property
    def has_pending_background_commands(self) -> bool:
        return bool(self.background_commands)

    @staticmethod
    def _raw_payload(event: dict) -> dict | None:
        raw = event.get("raw_json")
        if isinstance(raw, dict):
            return raw
        if not isinstance(raw, str) or not raw:
            return None
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError):
            return None
        return parsed if isinstance(parsed, dict) else None

    def observe(self, event: dict) -> None:
        """Consume one ordered JSONL-derived event without guessing from text."""

        raw = self._raw_payload(event)
        event_type = str(event.get("event_type") or "")
        if raw is not None and event_type == "tool_use":
            message = raw.get("message")
            blocks = (
                message.get("content")
                if isinstance(message, dict)
                else None
            )
            if isinstance(blocks, list):
                for block in blocks:
                    if (
                        isinstance(block, dict)
                        and block.get("type") == "tool_use"
                        and block.get("name") == "Bash"
                        and isinstance(block.get("input"), dict)
                        and block["input"].get("run_in_background") is True
                        and isinstance(block.get("id"), str)
                        and block["id"]
                    ):
                        self.background_commands.setdefault(
                            block["id"], None
                        )

        if raw is not None and event_type == "tool_result":
            message = raw.get("message")
            blocks = (
                message.get("content")
                if isinstance(message, dict)
                else None
            )
            result = raw.get("toolUseResult")
            background_task_id = (
                result.get("backgroundTaskId")
                if isinstance(result, dict)
                else None
            )
            if not isinstance(background_task_id, str):
                background_task_id = None
            if isinstance(blocks, list):
                for block in blocks:
                    if (
                        not isinstance(block, dict)
                        or block.get("type") != "tool_result"
                    ):
                        continue
                    tool_use_id = block.get("tool_use_id")
                    if (
                        not isinstance(tool_use_id, str)
                        or tool_use_id not in self.background_commands
                    ):
                        continue
                    if block.get("is_error") is True:
                        self.background_commands.pop(tool_use_id, None)
                    elif background_task_id:
                        self.background_commands[tool_use_id] = (
                            background_task_id
                        )
                    else:
                        # A successful result without the documented
                        # structured id is ambiguous. Keep the candidate
                        # fail-closed; the four-hour exact-session watchdog
                        # remains the terminal safety bound.
                        logger.warning(
                            "Background Bash result omitted backgroundTaskId "
                            "for tool use %s",
                            tool_use_id,
                        )

        content = event.get("content")
        message = raw.get("message") if raw is not None else None
        origin = raw.get("origin") if raw is not None else None
        message_content = (
            message.get("content")
            if isinstance(message, dict)
            else None
        )
        if (
            event_type == "message"
            and event.get("role") == "user"
            and isinstance(content, str)
            and raw is not None
            and raw.get("type") == "user"
            and isinstance(message, dict)
            and message.get("role") == "user"
            and message_content == content
            and isinstance(origin, dict)
            and origin.get("kind") == "task-notification"
            and raw.get("promptSource") == "system"
        ):
            # Exact XML ids alone are forgeable by assistant/user prose. Only
            # the native harness-originated user envelope may close work.
            self._observe_notification(content)

    def _observe_notification(self, text: str) -> None:
        if "<task-notification>" not in text:
            return
        task_match = _BACKGROUND_TASK_ID_RE.search(text)
        status_match = _BACKGROUND_STATUS_RE.search(text)
        if (
            task_match is None
            or status_match is None
            or status_match.group(1).lower()
            not in _BACKGROUND_TERMINAL_STATUSES
        ):
            return
        task_id = task_match.group(1)
        tool_match = _BACKGROUND_TOOL_USE_ID_RE.search(text)
        tool_use_id = tool_match.group(1) if tool_match else None
        for candidate, background_task_id in list(
            self.background_commands.items()
        ):
            if background_task_id == task_id or (
                background_task_id is None and candidate == tool_use_id
            ):
                self.background_commands.pop(candidate, None)


def _background_work_tracker(
    session: Any,
    *,
    create: bool,
) -> _CCMBackgroundWorkTracker | None:
    if session is None:
        return None
    current = getattr(session, "_ccm_background_work_tracker", None)
    if isinstance(current, _CCMBackgroundWorkTracker):
        return current
    if not create:
        return None
    native = getattr(session, "_tracker", None)
    tracker = _CCMBackgroundWorkTracker(native)
    setattr(session, "_ccm_background_work_tracker", tracker)
    if native is not None:
        # Session.has_pending_subagents and every SessionPool eviction/drain
        # path read ``session._tracker.has_pending``. The delegating wrapper
        # preserves all native methods while adding Bash harness work.
        session._tracker = tracker
    return tracker


def _structured_rate_limit_is_hard(raw: dict) -> bool:
    """Return whether a raw Claude rate-limit signal aborts the turn."""

    if raw.get("error") == "rate_limit":
        return True
    info = raw.get("rate_limit_info")
    if not isinstance(info, dict):
        return True
    if bool(info.get("hard_limit")):
        return True
    status = str(info.get("status") or "").lower()
    return status not in {"allowed", "allowed_warning"}


class FullMirrorCCMBackend(CCMBackend):
    """CCMBackend with exact PTY-turn finalization and full idle mirroring.

    The dependency adapter finalizes by reusable instance/task ids, which is
    insufficient once a hot PTY Session/PID hosts several turns. This subclass
    owns terminal bookkeeping with CCM's durable generation fences and keeps
    autonomous output on the full ``_process_event`` path.
    """

    async def launch(
        self,
        key: Any,
        prompt: str,
        cwd: str,
        session_id: str | None = None,
        resume_session_id: str | None = None,
        **kwargs,
    ) -> str:
        """Install the exact autonomous callback before the consumer can run.

        BasePTYBackend installs its instance-bound callback and creates the
        consumer without yielding before returning. Replacing it immediately
        here closes the idle-watcher race between send_prompt and on_exit.
        """

        sid = await super().launch(
            key=key,
            prompt=prompt,
            cwd=cwd,
            session_id=session_id,
            resume_session_id=resume_session_id,
            **kwargs,
        )
        task_id = kwargs.get("task_id")
        session = self._sessions.get(key)
        _background_work_tracker(session, create=True)
        actual_session_id = getattr(session, "session_id", None)
        if task_id is not None and actual_session_id:
            self._im.reset_pty_autonomous_activity_handoff(
                task_id, actual_session_id
            )
        self._restore_full_autonomous_mirror(
            session,
            key,
            task_id,
            kwargs.get("loop_iteration"),
            replace_base_binding=True,
        )
        return sid

    async def on_event(self, key: Any, event_dict: dict, **context) -> None:
        """Forward a foreground event with its immutable PTY turn identity."""

        await self._im.wait_for_pty_launch_metadata(key)
        consumer = asyncio.current_task()
        record = getattr(
            consumer, "_ccm_output_consumer_record", None
        )
        process = getattr(record, "process", None)
        session = getattr(process, "session", None)
        if session is None and self._consumers.get(key) is consumer:
            session = self._sessions.get(key)
        background_tracker = _background_work_tracker(
            session, create=session is not None
        )
        if background_tracker is not None:
            # JsonlReader yields each event, and BasePTYBackend awaits this
            # callback, before it can observe turn_duration and enter on_exit.
            # Therefore the structured backgroundTaskId is an authoritative
            # foreground-exit barrier, not a heuristic transcript scan.
            background_tracker.observe(event_dict)
        # Persist the semantic terminal outcome on the exact PTY generation
        # before entering the generic event pipeline.  That pipeline may
        # legitimately decline or fail a stale DB write, but a foreground
        # provider timeout/API failure must still control ``on_exit``.  A
        # persistent Claude process otherwise reports OS exit code 0 and the
        # failed turn is incorrectly finalized as completed (production task
        # 322).
        fatal_provider_error = self._im._fatal_provider_error_for_event(
            event_dict
        )
        if fatal_provider_error and record is not None:
            if getattr(record, "fatal_provider_error", None) is None:
                object.__setattr__(
                    record,
                    "fatal_provider_error",
                    fatal_provider_error[:2000],
                )
        raw = event_dict.get("raw_json")
        if raw:
            try:
                parsed = json.loads(raw) if isinstance(raw, str) else raw
            except (TypeError, ValueError):
                parsed = None
            if (
                isinstance(parsed, dict)
                and parsed.get("type") == "rate_limit_event"
            ):
                event_dict = dict(event_dict)
                event_dict["event_type"] = "rate_limit_event"
                event_dict["rate_limit_info"] = parsed.get(
                    "rate_limit_info"
                )
                hard_limit = _structured_rate_limit_is_hard(parsed)
                event_dict["is_error"] = hard_limit
                if (
                    not hard_limit
                    or event_dict.get("orphan")
                    or event_dict.get("autonomous")
                ):
                    # Keep the compatibility clear for older claude-pty
                    # sessions that latched every structured quota event, and
                    # for hard events proven stale/autonomous. Only a hard
                    # signal from this foreground turn may abort it.
                    process = getattr(record, "process", None)
                    session = getattr(process, "session", None)
                    if (
                        session is None
                        and self._consumers.get(key) is consumer
                    ):
                        session = self._sessions.get(key)
                    if session is not None:
                        session._rate_limited_turn = False
        try:
            await self._im._process_event(
                key,
                context.get("task_id"),
                event_dict,
                context.get("loop_iteration"),
                consumer_record=record,
            )
        except Exception:
            logger.exception(
                "PTY on_event failed for instance %s task %s",
                key,
                context.get("task_id"),
            )

    async def on_exit(self, key: Any, exit_code: int | None, **context) -> None:
        # claude_pty starts its consumer before InstanceManager persists the
        # initial running metadata. A short turn must not write idle first and
        # then be overwritten by that late running commit.
        await self._im.wait_for_pty_launch_metadata(key)
        consumer = asyncio.current_task()
        record = getattr(
            consumer, "_ccm_output_consumer_record", None
        )
        # Claim terminal ownership before the first await that can overlap an
        # external stop.  If stop already owns this exact record, on_exit must
        # only finish identity-guarded proxy/map cleanup; taking the lifecycle
        # lock for retries or DB finalization would deadlock because PTY
        # backend.stop is awaiting this consumer while holding that lock.
        owns_record_before_cleanup = bool(
            record is not None
            and getattr(record, "task", None) is consumer
            and self._im._consumer_records.get(key) is record
            and self._im._tasks.get(key) is consumer
            and self._im.processes.get(key)
            is getattr(record, "process", None)
        )
        terminal_owner = (
            self._im._claim_pty_terminal_owner(record, "consumer")
            if owns_record_before_cleanup
            else None
        )
        # A docker-exec client can report exit while a detached command still
        # runs in the project container.  Prove the exact tokenized generation
        # gone before the adapter publishes an idle/reusable Instance.
        await self._im.finalize_pty_container_exec(
            key,
            expected_process=getattr(record, "process", None),
        )
        session = context.get("session")
        task_id = context.get("task_id")
        chat_initiated = bool(context.get("chat_initiated", False))
        ec = exit_code if exit_code is not None else 0

        # The upstream CCM adapter finalizes with only instance_id/task_id.
        # That is unsafe for PTY hot reuse: many turns share one Session/PID,
        # and a late old callback can clear a newer same-slot owner.  Keep pool
        # rotation, but only while this callback still owns the exact immutable
        # consumer record; all terminal DB state is committed by the manager's
        # Task+Instance generation CAS below.
        owns_record = bool(
            record is not None
            and getattr(record, "task", None) is consumer
            and self._im._consumer_records.get(key) is record
            and self._im._tasks.get(key) is consumer
            and self._im.processes.get(key) is getattr(record, "process", None)
        )
        if owns_record and terminal_owner is None:
            terminal_owner = self._im._claim_pty_terminal_owner(
                record, "consumer"
            )
        stop_owns_terminal = terminal_owner == "stop"
        provider_error = str(
            getattr(record, "fatal_provider_error", "") or ""
        ).strip()
        rate_limit_info = self._im.pty_rate_limit_info(key) or {}
        rate_limit_status = str(
            rate_limit_info.get("status") or ""
        ).lower()
        hard_rate_limit = bool(rate_limit_info.get("hard_limit")) or (
            bool(rate_limit_status)
            and rate_limit_status not in {"allowed", "allowed_warning"}
        )
        semantic_turn_failure = bool(
            owns_record
            and (
                provider_error
                or self._im.transient_error_seen(key)
                or hard_rate_limit
            )
        )
        if ec == 0 and semantic_turn_failure:
            # A PTY process is intentionally persistent. Its OS-level success
            # cannot override a failed API turn recorded in JSONL.
            ec = 1

        if (
            chat_initiated
            and task_id
            and owns_record
            and not stop_owns_terminal
            and ec not in (0, -2, 130)
        ):
            try:
                retried = await self._im._try_chat_transient_retry(
                    key, task_id, ec, provider_error
                )
                if not retried:
                    retried = await self._im._try_chat_pool_rotation(
                        key, task_id, ec, provider_error
                    )
                if retried:
                    old_proxy = record.process
                    new_proxy = self._proxies.get(key)
                    if new_proxy is not None and new_proxy is not old_proxy:
                        new_proxy.chain(old_proxy)
                    else:
                        old_proxy.complete(ec)
                    return
            except Exception:
                logger.exception(
                    "PTY retry/rotation check failed for instance %s", key
                )

        session_id = getattr(session, "session_id", None)
        transition_eligible = bool(
            task_id
            and owns_record
            and not stop_owns_terminal
            and ec == 0
            and session_id
        )

        async def _commit_terminal(
            *,
            allow_background: bool,
        ) -> tuple[str | None, str | None, Any | None]:
            background_generation = None
            background_state = None
            if allow_background:
                pending_background_activity = (
                    await self._im.pty_background_activity_pending(
                        task_id, session
                    )
                )
                pending_background_activity = (
                    pending_background_activity
                    or self._im.has_pty_autonomous_activity_handoff(
                        task_id, session_id
                    )
                )
                # Re-read under the transition lock.  An idle callback can
                # pre-arm while the pending detector yields; foreground exit
                # must reuse that winner rather than overwrite its token.
                existing = self._im.pty_background_generation_for(
                    task_id, session_id
                )
                if pending_background_activity or existing:
                    background_generation = (
                        existing or secrets.token_urlsafe(24)
                    )

            final_status = None
            if (
                task_id
                and owns_record
                and not stop_owns_terminal
                and background_generation
                and session_id
            ):
                armed = await self._im.arm_pty_background_generation(
                    key,
                    task_id,
                    session_id,
                    background_generation,
                    record,
                )
                if armed:
                    background_state = (
                        self._im.register_pty_background_generation(
                            task_id,
                            session_id,
                            background_generation,
                            session,
                            task_retry_count=record.task_retry_count,
                            task_turn_generation=record.task_turn_generation,
                        )
                    )
                else:
                    # Defensive DB CAS fallback: a same-process winner must be
                    # reused, never silently replaced or abandoned.
                    background_generation = (
                        self._im.pty_background_generation_for(
                            task_id, session_id
                        )
                    )
                    if background_generation is not None:
                        background_state = (
                            self._im.pty_background_state_for(
                                task_id,
                                session_id,
                                background_generation,
                            )
                        )
            elif (
                chat_initiated
                and task_id
                and owns_record
                and not stop_owns_terminal
            ):
                final_status = (
                    await self._im.finalize_pty_chat_generation(
                        key,
                        task_id,
                        ec,
                        record,
                        background_generation=None,
                        background_session_id=session_id,
                    )
                )
                if final_status == "background_armed":
                    final_status = None
                    background_generation = (
                        self._im.pty_background_generation_for(
                            task_id, session_id
                        )
                    )
                    if background_generation is not None:
                        background_state = (
                            self._im.pty_background_state_for(
                                task_id,
                                session_id,
                                background_generation,
                            )
                        )
                else:
                    background_generation = None
            return final_status, background_generation, background_state

        if transition_eligible:
            async with self._im.pty_background_transition(
                task_id, session_id
            ):
                final_status, background_generation, background_state = (
                    await _commit_terminal(allow_background=True)
                )
        else:
            (
                final_status,
                background_generation,
                background_state,
            ) = await _commit_terminal(allow_background=False)

        # Restore the idle watcher callback before waiting. All foreground
        # proxies remain incomplete until this exact autonomous turn reaches
        # its turn_duration sentinel, preventing completion/evaluation against
        # a half-written signal or transcript.
        try:
            session = session or self._sessions.get(key)
            self._restore_full_autonomous_mirror(
                session,
                key,
                context.get("task_id"),
                context.get("loop_iteration"),
                replace_base_binding=owns_record,
            )
        except Exception:
            logger.exception(
                "Failed to restore full autonomous mirror for key=%s", key
            )

        # Transcript growth can be the only progress signal while the main
        # JSONL is quiet. Start polling before the exact wait barrier.
        if session is not None:
            reader = getattr(session, "_reader", None)
            tracker = getattr(reader, "tracker", None)
            if tracker is None:
                # Compatibility with test doubles and older claude-pty builds.
                tracker = getattr(reader, "_tracker", None)
            if tracker is not None and tracker.has_pending:
                asyncio.create_task(
                    self._poll_subagent_transcripts(tracker, task_id)
                )

        if (
            task_id
            and session_id
            and background_generation
            and background_state is not None
        ):
            background_outcome = None
            object.__setattr__(record, "pty_background_waiting", True)
            try:
                background_outcome = (
                    await self._im.wait_pty_background_outcome(
                        background_state
                    )
                )
                if background_outcome == "failed":
                    ec = 1
            except asyncio.CancelledError:
                self._im.abandon_pty_background_generation(
                    task_id,
                    session_id,
                    background_generation,
                )
                raise
            finally:
                object.__setattr__(
                    record, "pty_background_waiting", False
                )

            stop_owns_terminal = (
                getattr(record, "pty_terminal_owner", None) == "stop"
            )
            if chat_initiated and owns_record and not stop_owns_terminal:
                if background_outcome in {"completed", "failed"}:
                    final_status = (
                        await self._im.finalize_pty_chat_generation(
                            key,
                            task_id,
                            ec,
                            record,
                            background_generation=None,
                            preserve_background_failure=(
                                background_outcome == "failed"
                            ),
                        )
                    )
                background_generation = None

        if (
            final_status == "completed"
            and ec == 0
            and background_generation is None
        ):
            await self._maybe_retry_empty_reply(key, task_id)

        # Dispatcher/Ralph own non-chat Task finalization. Completing the proxy
        # below wakes them before their DB result CAS, and the normal
        # instance-keyed consumer maps are then released. Retain one immutable
        # generation proof so an autonomous callback already arriving in that
        # narrow gap can pre-arm only this exact Task/session/consumer epoch.
        if (
            not chat_initiated
            and transition_eligible
            and background_generation is None
            and record is not None
            and session is not None
            and session_id is not None
        ):
            self._im.retain_pty_post_exit_generation(
                key,
                task_id,
                session_id,
                session,
                record,
            )

        # Exact identity cleanup replaces CCMBackend.on_exit.  Calling the
        # upstream method after a replacement is registered would let it read
        # ``session._ccm_proxy`` from the replacement and complete/pop the
        # wrong turn.  A stale callback is allowed to complete only its own
        # proxy; every instance-keyed map uses an identity guard.
        process = getattr(record, "process", None)
        if process is not None:
            if self._proxies.get(key) is process:
                self._proxies.pop(key, None)
            process.complete(ec)
        if self._consumers.get(key) is consumer:
            self._consumers.pop(key, None)
            if self._sessions.get(key) is session:
                self._sessions.pop(key, None)
        if process is not None and self._im.processes.get(key) is process:
            self._im.processes.pop(key, None)
        if self._im._tasks.get(key) is consumer:
            self._im._tasks.pop(key, None)

    async def _maybe_retry_empty_reply(
        self,
        instance_id: int,
        task_id: int,
    ) -> None:
        """Preserve the adapter's one-shot empty-response recovery."""

        params = self._im._launch_params.get(instance_id)
        if not params or params.get("_retried"):
            return
        try:
            assistant_texts = await self._get_recent_assistant_texts(task_id)
            combined = " ".join(assistant_texts).strip().lower().rstrip(".")
            if assistant_texts and combined not in {
                "no response requested",
                "no response needed",
            }:
                return
            from backend.main import dispatcher

            retry_fence = await self._im._chat_automatic_relaunch_fence(
                task_id,
                params,
                dispatcher=dispatcher,
            )
            if retry_fence is None:
                # PTY completion proves only that this proxy settled, not that
                # the model performed no tools before producing an empty final
                # message.  The durable source/transport admission fence wins
                # over this legacy one-shot convenience retry.
                logger.error(
                    "Task %d got empty/non-response (%r) after PTY provider "
                    "admission; automatic replay was blocked",
                    task_id,
                    combined[:80],
                )
                return
            from backend.services.dispatcher import PRIORITY_USER

            current_message = (
                params.get("current_message")
                or params["prompt"]
            )
            retry_kwargs = dict(
                task_id=task_id,
                prompt=current_message,
                priority=PRIORITY_USER,
                source="retry",
                current_message=current_message,
                queue_admission_fence=retry_fence,
            )
            if isinstance(params.get("enabled_skills"), dict):
                retry_kwargs["command_skills"] = dict(
                    params["enabled_skills"]
                )
            if isinstance(params.get("model"), str):
                retry_kwargs["model_override"] = params["model"]
            if params.get("source_log_id") is not None:
                retry_kwargs["source_log_id"] = params["source_log_id"]
            if params.get("queue_timestamp") is not None:
                retry_kwargs["queue_timestamp"] = params[
                    "queue_timestamp"
                ]
            admitted = await dispatcher.enqueue_message(**retry_kwargs)
            if admitted is False:
                logger.info(
                    "Discarded stale PTY empty-reply retry for task %d after "
                    "a queue clear",
                    task_id,
                )
                return
            params["_retried"] = True
            logger.warning(
                "Task %d got empty/non-response (%r), re-enqueued",
                task_id,
                combined[:80],
            )
        except Exception:
            logger.exception(
                "Empty-reply retry check failed for task %s", task_id
            )

    def _restore_full_autonomous_mirror(
        self,
        session: Any,
        key: Any,
        task_id: int | None,
        loop_iteration: int | None,
        *,
        replace_base_binding: bool = False,
    ) -> None:
        if session is None:
            return
        current = getattr(session, "on_autonomous_event", None)
        callback_name = getattr(current, "__name__", "")
        if (
            callback_name == "_full_autonomous_mirror"
            and getattr(current, "_ccm_task_id", None) == task_id
            and getattr(current, "_ccm_session_id", None)
            == getattr(session, "session_id", None)
        ):
            return
        if callback_name == "_on_autonomous" and not replace_base_binding:
            # A newer launch has rebound the same hot Session. Only the exact
            # owning on_exit may replace its own base callback.
            return
        if callback_name not in {"_subagent_only_callback", "_on_autonomous"}:
            return

        im = self._im
        expected_session_id = getattr(session, "session_id", None)
        autonomous_generation = None
        activity_handoff = None

        async def _full_autonomous_mirror(event, **ctx):
            nonlocal autonomous_generation, activity_handoff
            generation = None
            completion_state = None
            event_data = event.to_dict()
            event_data["autonomous"] = True
            background_tracker = _background_work_tracker(
                session, create=True
            )
            terminal = im._is_pty_autonomous_terminal(event_data)
            if (
                not terminal
                and task_id is not None
                and expected_session_id is not None
                and im._is_pty_autonomous_activity(event_data)
                and activity_handoff is None
            ):
                # Synchronous before begin() awaits the transition lock. An
                # on_exit currently holding that lock can still see this
                # handoff and arm/wait instead of publishing completed.
                activity_handoff = im.note_pty_autonomous_activity(
                    task_id, expected_session_id
                )
            try:
                if task_id is None or expected_session_id is None:
                    return
                # Admission, persistence, native-agent upsert, WebSocket
                # publication and terminal clearing share one transition
                # lock. Exact stop/watchdog paths use the same lock, so no old
                # event can commit and then publish after its marker is clear.
                async with im.pty_background_transition(
                    task_id, expected_session_id
                ):
                    if background_tracker is not None:
                        # The final Bash notification changes the completion
                        # predicate. Apply it only while holding the same lock
                        # as begin/finish/watchdog so a prior turn's sentinel
                        # cannot retire this epoch before the notification is
                        # persisted and this autonomous turn is fenced.
                        background_tracker.observe(event_data)
                    if terminal:
                        # Keep one immutable token from the first event through
                        # this turn's sentinel. Looking up the current token at
                        # the tail would let an old batch clear a newer epoch.
                        generation = autonomous_generation
                        state = (
                            im.pty_background_state_for(
                                task_id,
                                expected_session_id,
                                generation,
                            )
                            if generation is not None
                            else None
                        )
                        if state is None:
                            generation = None
                            return
                    else:
                        generation = (
                            await im._begin_pty_autonomous_activity_locked(
                                task_id,
                                expected_session_id,
                                session,
                                event_data,
                                instance_id=(
                                    key if isinstance(key, int) else None
                                ),
                            )
                        )
                        if generation is None:
                            return
                        autonomous_generation = generation
                    state = im.pty_background_state_for(
                        task_id,
                        expected_session_id,
                        generation,
                    )
                    if state is None:
                        return
                    await im._process_event(
                        key,
                        task_id,
                        event_data,
                        loop_iteration,
                        detached_autonomous=True,
                        expected_session_id=expected_session_id,
                        expected_background_generation=generation,
                        expected_task_retry_count=state.task_retry_count,
                        expected_task_turn_generation=(
                            state.task_turn_generation
                        ),
                    )
                    completion_state = await im._finish_pty_autonomous_activity_locked(
                        task_id,
                        expected_session_id,
                        generation,
                        event_data,
                    )
                # Owner cleanup is globally outer to the PTY transition lock.
                # Mark the sentinel while serialized above, then take the
                # durable Harness fence before clearing the background epoch.
                if (
                    completion_state is not None
                    and getattr(completion_state, "task_id", None) == task_id
                    and getattr(completion_state, "session_id", None)
                    == expected_session_id
                    and getattr(completion_state, "generation", None)
                    == generation
                ):
                    await im._try_complete_pty_background_generation(
                        completion_state
                    )
            except Exception:
                logger.exception(
                    "Autonomous mirror failed for task %s (instance %s)",
                    task_id, key,
                )
            finally:
                if terminal or generation is None:
                    if task_id is not None and expected_session_id is not None:
                        im.clear_pty_autonomous_activity_handoff(
                            task_id,
                            expected_session_id,
                            activity_handoff,
                        )
                    activity_handoff = None
                    autonomous_generation = None

        _full_autonomous_mirror._ccm_task_id = task_id
        _full_autonomous_mirror._ccm_session_id = expected_session_id
        session.on_autonomous_event = _full_autonomous_mirror
        logger.info(
            "Autonomous full mirror armed for task %s (instance %s)",
            task_id, key,
        )
