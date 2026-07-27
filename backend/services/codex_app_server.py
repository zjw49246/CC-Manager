"""Persistent Codex app-server transport.

The regular ``codex exec resume`` integration starts a new CLI process for
every turn.  App-server keeps configuration, MCP clients, and active threads in
one process while exposing the same persisted Codex thread ids.  This module
adapts one app-server turn to the small subprocess surface InstanceManager
already consumes, so task status/retry/DB logic remains shared with exec mode.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import signal
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Sequence

from backend.services.mcp_config import McpServerSpec, render_codex_mcp_config
from backend.services.process_safety import require_safe_process_group_id

logger = logging.getLogger(__name__)

_APP_SERVER_GRACEFUL_SHUTDOWN_TIMEOUT = 5.0
_APP_SERVER_TERM_SHUTDOWN_TIMEOUT = 5.0
_APP_SERVER_KILL_SHUTDOWN_TIMEOUT = 5.0
_APP_SERVER_GROUP_POLL_INTERVAL = 0.05
_ACTIVE_TURN_MISMATCH_RE = re.compile(
    r"expected active turn id\s+[`'\"]?"
    r"(?P<expected>[^\s`'\"]+)[`'\"]?\s+but found\s+[`'\"]?"
    r"(?P<actual>[^\s`'\"]+)"
)
_GOALS_FEATURE_DISABLED_RE = re.compile(
    r"\bgoals feature is disabled\b",
    re.IGNORECASE,
)
_NO_ACTIVE_GOAL_RE = re.compile(
    r"\b(?:no active goal|goal is not active|goal not found)\b",
    re.IGNORECASE,
)


class CodexAppServerError(RuntimeError):
    """Raised when app-server rejects a request or loses its transport."""


class CodexAppServerBusyError(CodexAppServerError):
    """The requested account home still has an active Codex turn."""


class CodexRequiredMcpError(CodexAppServerError):
    """A thread could not be created with its required MCP configuration."""


class CodexRequiredMcpPreTurnError(CodexRequiredMcpError):
    """Required MCP failed before turn/start, so MCP-equivalent exec is safe."""


class CodexThreadHomeMismatchError(CodexAppServerError):
    """A thread was routed to a different account without an explicit rebind."""


class _UnconfirmedTurnCancellation(asyncio.CancelledError):
    """A cancelled turn/start whose interrupt was not acknowledged."""

    def __init__(self, process: "CodexTurnProcess", reason: str) -> None:
        super().__init__(reason)
        self.process = process
        self.reason = reason


class _UnconfirmedTurnStartFailure(CodexAppServerError):
    """A timed-out turn/start that may still be executing server-side."""

    def __init__(self, process: "CodexTurnProcess", reason: str) -> None:
        super().__init__(reason)
        self.process = process
        self.reason = reason


def _active_turn_id_from_error(error: BaseException | str) -> str | None:
    """Extract app-server's authoritative active turn from a mismatch error."""

    match = _ACTIVE_TURN_MISMATCH_RE.search(str(error))
    if match is None:
        return None
    actual = match.group("actual").rstrip("`'\".,:;)")
    expected = match.group("expected").rstrip("`'\".,:;)")
    return actual if actual and actual != expected else None


def normalize_codex_home(codex_home: str | os.PathLike[str] | None = None) -> str:
    """Return the canonical, absolute CODEX_HOME used as the process key."""

    configured = codex_home or os.environ.get("CODEX_HOME") or Path.home() / ".codex"
    return str(Path(configured).expanduser().resolve(strict=False))


class CodexTurnProcess:
    """Process-like view of one app-server turn.

    InstanceManager only needs stdout/stderr readers, ``wait()``, returncode,
    pid, and interrupt/kill methods.  Keeping that contract lets the existing
    output consumer own all final task/instance state transitions.
    """

    def __init__(
        self,
        pid: int,
        interrupt: Callable[[], Awaitable[None]],
        *,
        thread_id: str | None = None,
    ) -> None:
        self.pid = pid
        self.thread_id = thread_id
        self.unsubscribe_on_terminal = False
        self.returncode: int | None = None
        self.termination_kind: str | None = None
        self.stdout = asyncio.StreamReader(limit=10 * 1024 * 1024)
        self.stderr = asyncio.StreamReader(limit=1024 * 1024)
        self._interrupt = interrupt
        self._done = asyncio.get_running_loop().create_future()

    def feed(self, payload: dict[str, Any]) -> None:
        if self.returncode is not None:
            return
        line = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        self.stdout.feed_data(line.encode("utf-8") + b"\n")

    def finish(
        self,
        returncode: int,
        stderr: str = "",
        *,
        termination_kind: str | None = None,
    ) -> None:
        if self.returncode is not None:
            return
        self.returncode = returncode
        self.termination_kind = termination_kind
        if stderr:
            self.stderr.feed_data(stderr.encode("utf-8", errors="replace"))
        self.stdout.feed_eof()
        self.stderr.feed_eof()
        if not self._done.done():
            self._done.set_result(returncode)

    async def wait(self) -> int:
        return await asyncio.shield(self._done)

    def send_signal(self, sig: int) -> None:
        if sig in (signal.SIGINT, signal.SIGTERM, signal.SIGKILL):
            asyncio.create_task(self._interrupt_safely())

    def terminate(self) -> None:
        self.send_signal(signal.SIGTERM)

    def kill(self) -> None:
        self.send_signal(signal.SIGKILL)

    async def _interrupt_safely(self) -> None:
        try:
            await self._interrupt()
        except Exception:
            logger.exception("Codex app-server turn interrupt failed")
            # A failed RPC says nothing about the real server-side turn.  In
            # particular, marking this adapter terminal would let
            # InstanceManager release the Task/Instance claim while Codex may
            # still be executing.  Keep the process active so its caller
            # retries/escalates and retains exact generation evidence.
            return


@dataclass
class _TurnContext:
    thread_id: str
    process: CodexTurnProcess
    launch_started: float
    task_id: int | None
    turn_id: str | None = None
    admitted_turn_id: str | None = None
    observed_turn_id: str | None = None
    usage: dict[str, int] | None = None
    first_input_seen: bool = False
    first_output_seen: bool = False


class CodexAppServer:
    """One lazily started app-server permanently bound to one CODEX_HOME."""

    def __init__(
        self,
        binary: str,
        request_timeout: float = 30.0,
        *,
        codex_home: str | os.PathLike[str] | None = None,
        env_remove: set[str] | None = None,
    ) -> None:
        self.binary = binary
        self.request_timeout = request_timeout
        self.codex_home = normalize_codex_home(codex_home)
        self._env_remove = {
            str(key).upper() for key in (env_remove or set())
        }
        self._process: asyncio.subprocess.Process | None = None
        # On POSIX, app-server is launched as its own session leader.  Keep
        # the exact process identity—not just its numeric PID—so a stale
        # shutdown can never signal a replacement generation after PID reuse.
        self._process_group_process: asyncio.subprocess.Process | None = None
        self._reader_task: asyncio.Task | None = None
        self._stderr_task: asyncio.Task | None = None
        self._pending: dict[int, asyncio.Future] = {}
        self._contexts_by_thread: dict[str, _TurnContext] = {}
        self._contexts_by_turn: dict[str, _TurnContext] = {}
        self._stderr_lines: deque[str] = deque(maxlen=100)
        self._request_id = 0
        self._write_lock = asyncio.Lock()
        self._lifecycle_lock = asyncio.Lock()
        self._shutdown_requested = False
        # App-server keeps completed threads loaded in memory.  The registry
        # uses this set to restart an idle target server before a migrated
        # rollout is resumed there again, avoiding stale in-memory history.
        self._known_threads: set[str] = set()

    @property
    def is_alive(self) -> bool:
        return self._process is not None and self._process.returncode is None

    @property
    def shutdown_requested(self) -> bool:
        return self._shutdown_requested

    @property
    def pid(self) -> int:
        return self._process.pid if self._process else 0

    @property
    def has_active_turns(self) -> bool:
        return any(
            context.process.returncode is None
            for context in self._contexts_by_thread.values()
        )

    def has_active_thread(self, thread_id: str) -> bool:
        context = self._contexts_by_thread.get(thread_id)
        return bool(context and context.process.returncode is None)

    def knows_thread(self, thread_id: str) -> bool:
        return thread_id in self._known_threads

    def _detach_turn_context(self, context: _TurnContext) -> None:
        """Remove every identity mapping for one exact adapter generation."""

        if self._contexts_by_thread.get(context.thread_id) is context:
            self._contexts_by_thread.pop(context.thread_id, None)
        for turn_id, candidate in list(self._contexts_by_turn.items()):
            if candidate is context:
                self._contexts_by_turn.pop(turn_id, None)

    def _bind_turn_context(
        self,
        context: _TurnContext,
        turn_id: str,
        *,
        observed: bool,
    ) -> bool:
        """Bind an adapter to one turn without leaving stale reverse mappings."""

        if not turn_id:
            return False
        existing = self._contexts_by_turn.get(turn_id)
        if existing is not None and existing is not context:
            logger.error(
                "Refusing to bind Codex turn %s for task %s over task %s",
                turn_id,
                context.task_id,
                existing.task_id,
            )
            return False
        old_turn_id = context.turn_id
        if (
            old_turn_id
            and old_turn_id != turn_id
            and self._contexts_by_turn.get(old_turn_id) is context
        ):
            self._contexts_by_turn.pop(old_turn_id, None)
        context.turn_id = turn_id
        if observed:
            context.observed_turn_id = turn_id
        self._contexts_by_turn[turn_id] = context
        return True

    def _alias_turn_context(
        self,
        context: _TurnContext,
        turn_id: str,
    ) -> bool:
        """Route a protocol alias without replacing the authoritative turn id.

        Codex can steer one ``turn/start`` submission into an already-active
        native goal turn. Notifications then legitimately alternate between
        the submission id and the active turn id. Both ids must reach the same
        adapter generation; treating the submission id as an unrelated turn
        drops assistant output and the terminal event, leaving the worker
        permanently ``running``.
        """

        if not turn_id:
            return False
        existing = self._contexts_by_turn.get(turn_id)
        if existing is not None and existing is not context:
            logger.error(
                "Refusing to alias Codex turn %s for task %s over task %s",
                turn_id,
                context.task_id,
                existing.task_id,
            )
            return False
        self._contexts_by_turn[turn_id] = context
        return True

    def _interrupt_context_is_current(self, context: _TurnContext) -> bool:
        """Return whether an exact turn context still owns live work."""

        if context.process.returncode is not None:
            return False
        if self._contexts_by_thread.get(context.thread_id) is not context:
            raise CodexAppServerError(
                f"Codex thread {context.thread_id} changed owner during interrupt"
            )
        return True

    @staticmethod
    def _notification_turn_id(
        method: str,
        params: dict[str, Any],
    ) -> str | None:
        """Return the real turn id from either v2 notification shape."""

        if method in {"turn/started", "turn/completed"}:
            turn = params.get("turn")
            if isinstance(turn, dict) and turn.get("id"):
                return str(turn["id"])
        turn_id = params.get("turnId")
        return str(turn_id) if turn_id else None

    async def _pause_active_goal(self, thread_id: str) -> None:
        """Pause an adopted native goal with one bounded protocol round trip."""

        try:
            await self._request(
                "thread/goal/set",
                {"threadId": thread_id, "status": "paused"},
            )
        except CodexAppServerError as exc:
            # Submission ids can be adopted by any steerable active turn, not
            # only a native goal turn. A build with Goals explicitly disabled
            # or a regular thread with no goal reports one of these errors.
            # There is then nothing to pause, so continue to interrupt the
            # authoritative active turn. Other failures remain fail-closed.
            if (
                _GOALS_FEATURE_DISABLED_RE.search(str(exc))
                or _NO_ACTIVE_GOAL_RE.search(str(exc))
            ):
                logger.debug(
                    "Codex thread %s has no pausable native goal; "
                    "continuing direct turn interrupt",
                    thread_id,
                )
                return
            raise

    async def _interrupt_turn_context(self, context: _TurnContext) -> None:
        """Interrupt the actual active turn, reconciling steer-style admission ids."""

        if not self._interrupt_context_is_current(context):
            return
        turn_id = context.turn_id
        if not turn_id:
            raise CodexAppServerError(
                f"Codex thread {context.thread_id} has no interruptible turn id"
            )

        goal_checked = False
        if (
            context.admitted_turn_id is not None
            and turn_id != context.admitted_turn_id
        ):
            await self._pause_active_goal(context.thread_id)
            goal_checked = True
            if not self._interrupt_context_is_current(context):
                return

        for attempt in range(2):
            try:
                await self._request(
                    "turn/interrupt",
                    {"threadId": context.thread_id, "turnId": turn_id},
                )
                return
            except CodexAppServerError as exc:
                actual_turn_id = _active_turn_id_from_error(exc)
                if attempt or not actual_turn_id:
                    raise
                if not self._interrupt_context_is_current(context):
                    return
                if not goal_checked:
                    await self._pause_active_goal(context.thread_id)
                    goal_checked = True
                    if not self._interrupt_context_is_current(context):
                        return
                if not self._bind_turn_context(
                    context,
                    actual_turn_id,
                    observed=True,
                ):
                    raise CodexAppServerError(
                        "Could not bind authoritative Codex active turn "
                        f"{actual_turn_id}"
                    ) from exc
                turn_id = actual_turn_id

    async def ensure_started(self) -> None:
        if self._shutdown_requested:
            raise CodexAppServerBusyError(
                f"Codex app-server is shutting down: {self.codex_home}"
            )
        if self.is_alive:
            return
        async with self._lifecycle_lock:
            if self._shutdown_requested:
                raise CodexAppServerBusyError(
                    f"Codex app-server is shutting down: {self.codex_home}"
                )
            if self.is_alive:
                return
            # A dead leader may still have descendants in the independent
            # app-server process group and keep stdout open.  Stop that exact
            # generation before waiting on its readers, or the reader wait can
            # hold this lifecycle lock forever and prevent shutdown itself.
            if self._process is not None:
                await self._shutdown_locked()
            elif any(
                task is not None and not task.done()
                for task in (self._reader_task, self._stderr_task)
            ):
                raise CodexAppServerError(
                    "Codex app-server reader remains without process evidence"
                )
            if self._shutdown_requested:
                raise CodexAppServerBusyError(
                    f"Codex app-server is shutting down: {self.codex_home}"
                )
            await self._start()

    async def read_rate_limits(self) -> dict[str, Any]:
        """Read the authenticated account's current rate-limit snapshot."""

        await self.ensure_started()
        # The v2 protocol declares this method with Option<()> params and the
        # official client omits the field. Sending ``params: {}`` is rejected
        # by Codex 0.144.6's serde decoder.
        return await self._request("account/rateLimits/read", None)

    async def _start(self) -> None:
        self._stderr_lines.clear()
        codex_home = Path(self.codex_home)
        codex_home.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            os.chmod(codex_home, 0o700)
        except OSError:
            logger.warning("Could not enforce 0700 on CODEX_HOME %s", codex_home)
        env = {
            key: value
            for key, value in os.environ.items()
            if (
                key.upper() not in ("CLAUDECODE", "CLAUDE_CODE")
                and key.upper() not in self._env_remove
            )
        }
        env["CODEX_HOME"] = self.codex_home
        started = time.perf_counter()
        spawn_kwargs: dict[str, Any] = {
            "stdin": asyncio.subprocess.PIPE,
            "stdout": asyncio.subprocess.PIPE,
            "stderr": asyncio.subprocess.PIPE,
            "env": env,
            "limit": 10 * 1024 * 1024,
        }
        if os.name == "posix":
            spawn_kwargs["start_new_session"] = True
        self._process = await asyncio.create_subprocess_exec(
            self.binary,
            "app-server",
            "--stdio",
            **spawn_kwargs,
        )
        process = self._process
        self._process_group_process = process if os.name == "posix" else None
        self._reader_task = asyncio.create_task(self._read_loop(process))
        self._stderr_task = asyncio.create_task(self._stderr_loop(process))
        try:
            await self._request(
                "initialize",
                {
                    "clientInfo": {
                        "name": "claude_code_manager",
                        "title": "Claude Code Manager",
                        "version": "0.1.0",
                    },
                    "capabilities": {"experimentalApi": True},
                },
            )
            await self._notify("initialized", {})
        except BaseException:
            # ``_start`` runs under the lifecycle lock, so use the locked
            # helper instead of re-entering public ``shutdown``.
            try:
                await self._shutdown_locked()
            except BaseException:
                # The transport did not initialize and its process generation
                # could not be proven gone.  Never let ``is_alive`` make a
                # later caller reuse this uninitialized server object.
                self._shutdown_requested = True
                raise
            raise
        logger.info(
            "Codex app-server ready pid=%s home=%s startup_ms=%.1f",
            self.pid,
            self.codex_home,
            (time.perf_counter() - started) * 1000,
        )

    async def start_turn(
        self,
        *,
        prompt: str,
        cwd: str,
        model: str | None,
        effort: str | None,
        resume_session_id: str | None,
        git_env: dict[str, str] | None,
        task_id: int | None,
        mcp_specs: Sequence[McpServerSpec] = (),
    ) -> tuple[CodexTurnProcess, str]:
        required_mcp = any(spec.required for spec in mcp_specs)
        try:
            thread_config: dict[str, Any] = (
                render_codex_mcp_config(mcp_specs) if mcp_specs else {}
            )
        except (TypeError, ValueError) as exc:
            if required_mcp:
                raise CodexRequiredMcpError(
                    f"Invalid required Codex MCP configuration: {exc}"
                ) from exc
            raise
        try:
            await self.ensure_started()
        except CodexAppServerBusyError:
            # Preserve maintenance/draining semantics.  InstanceManager already
            # treats this as unsafe to replay through exec.
            raise
        except Exception as exc:
            if required_mcp:
                error_type = (
                    CodexRequiredMcpError
                    if self.shutdown_requested
                    else CodexRequiredMcpPreTurnError
                )
                raise error_type(
                    "Codex app-server could not start required MCP transport: "
                    f"{exc}"
                ) from exc
            raise
        launch_started = time.perf_counter()
        if git_env:
            # Per-project git credentials must remain thread-scoped.  A global
            # app-server environment would leak one project's identity into
            # every other concurrently running task.
            thread_config["shell_environment_policy"] = {
                "inherit": "all",
                "set": git_env,
            }

        common: dict[str, Any] = {
            "cwd": os.path.abspath(cwd),
            "approvalPolicy": "never",
            "sandbox": "danger-full-access",
        }
        if model and model != "default":
            common["model"] = model
        if thread_config:
            common["config"] = thread_config

        thread_method = "thread/resume" if resume_session_id else "thread/start"
        thread_params = (
            {"threadId": resume_session_id, **common}
            if resume_session_id
            else common
        )
        try:
            response = await self._request(thread_method, thread_params)
        except Exception as exc:
            if required_mcp:
                raise CodexRequiredMcpPreTurnError(
                    f"{thread_method} could not initialize required MCP "
                    f"configuration: {exc}"
                ) from exc
            raise

        thread = response.get("thread") if isinstance(response, dict) else None
        thread_id = thread.get("id") if isinstance(thread, dict) else None
        if not thread_id:
            message = "thread start/resume returned no thread id"
            if required_mcp:
                raise CodexRequiredMcpPreTurnError(
                    f"Required MCP configuration was not admitted: {message}"
                )
            raise CodexAppServerError(message)
        self._known_threads.add(thread_id)
        existing = self._contexts_by_thread.get(thread_id)
        if existing and existing.process.returncode is None:
            raise CodexAppServerBusyError(
                f"thread {thread_id} already has an active turn"
            )

        context: _TurnContext | None = None

        async def _interrupt() -> None:
            if context is not None:
                await self._interrupt_turn_context(context)

        turn_process = CodexTurnProcess(
            self.pid,
            _interrupt,
            thread_id=thread_id,
        )
        context = _TurnContext(
            thread_id=thread_id,
            process=turn_process,
            launch_started=launch_started,
            task_id=task_id,
        )
        self._contexts_by_thread[thread_id] = context
        # Persist the native thread id through the same event path as exec.
        turn_process.feed({"type": "thread.started", "thread_id": thread_id})

        turn_params: dict[str, Any] = {
            "threadId": thread_id,
            "input": [{"type": "text", "text": prompt}],
            "cwd": os.path.abspath(cwd),
            "approvalPolicy": "never",
            "model": model if model and model != "default" else None,
            "effort": effort,
        }
        turn_request = asyncio.create_task(self._request("turn/start", turn_params))
        turn_cancelled = False
        while not turn_request.done():
            try:
                await asyncio.shield(turn_request)
            except asyncio.CancelledError:
                # Once turn/start is on the wire, abandoning the response can
                # leave real model work with no process adapter/consumer. Wait
                # for the bounded RPC to resolve, then interrupt it explicitly.
                turn_cancelled = True
            except BaseException:
                # Retrieve and classify the completed request below. In
                # particular, a timeout is an indeterminate server-side turn,
                # not an ordinary rejected admission.
                break
        try:
            turn_response = turn_request.result()
        except asyncio.TimeoutError as exc:
            # The RPC was written but no response arrived. It is impossible to
            # distinguish a rejected request from real model work that started
            # just before the timeout, and no turn id exists for an interrupt.
            # Preserve the adapter/context so the registry can fail closed by
            # stopping this account transport.
            reason = "Codex app-server turn/start timed out with unknown server state"
            if turn_cancelled:
                raise _UnconfirmedTurnCancellation(turn_process, reason) from exc
            raise _UnconfirmedTurnStartFailure(turn_process, reason) from exc
        except BaseException as exc:
            self._detach_turn_context(context)
            turn_process.finish(1, "Codex app-server rejected turn/start")
            if required_mcp and isinstance(exc, Exception):
                raise CodexRequiredMcpError(
                    "turn/start could not preserve required MCP "
                    f"configuration: {exc}"
                ) from exc
            raise

        turn = turn_response.get("turn") if isinstance(turn_response, dict) else None
        turn_id = turn.get("id") if isinstance(turn, dict) else None
        if not turn_id:
            self._detach_turn_context(context)
            turn_process.finish(1, "Codex app-server turn/start returned no turn id")
            message = "turn/start returned no turn id"
            if required_mcp:
                raise CodexRequiredMcpError(
                    f"Required MCP turn was not admitted: {message}"
                )
            raise CodexAppServerError(message)
        context.admitted_turn_id = str(turn_id)
        # An already-running steerable turn can emit notifications before this
        # response arrives. In that case its notification turn id is the real
        # active generation and must not be overwritten by this submission id.
        # The submission id remains a valid notification alias, however.
        if context.observed_turn_id is None:
            self._bind_turn_context(
                context,
                str(turn_id),
                observed=False,
            )
        elif context.process.returncode is None:
            self._alias_turn_context(context, str(turn_id))
        if turn_cancelled:
            cleanup = asyncio.create_task(self.abandon_turn(
                turn_process,
                "Codex turn admission was cancelled",
            ))
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    continue
            interrupt_confirmed = cleanup.result()
            if not interrupt_confirmed:
                raise _UnconfirmedTurnCancellation(
                    turn_process,
                    "Codex turn admission was cancelled and its interrupt "
                    "could not be confirmed",
                )
            raise asyncio.CancelledError
        logger.info(
            "Codex latency task=%s thread=%s stage=turn_started elapsed_ms=%.1f",
            task_id,
            thread_id,
            (time.perf_counter() - launch_started) * 1000,
        )
        return turn_process, thread_id

    async def read_thread(self, thread_id: str) -> dict[str, Any]:
        """Load one thread and its persisted turns from this account home."""

        if not thread_id:
            raise ValueError("thread_id is required")
        await self.ensure_started()
        response = await self._request(
            "thread/read",
            {"threadId": thread_id, "includeTurns": True},
        )
        thread = response.get("thread")
        if not isinstance(thread, dict) or thread.get("id") != thread_id:
            raise CodexAppServerError(
                f"thread/read returned an invalid thread for {thread_id}"
            )
        return thread

    async def create_thread(
        self,
        *,
        cwd: str,
        model: str | None = None,
    ) -> dict[str, Any]:
        """Create an empty persisted thread without starting a turn."""

        if not cwd:
            raise ValueError("cwd is required")
        await self.ensure_started()
        params: dict[str, Any] = {
            "cwd": os.path.abspath(cwd),
            "approvalPolicy": "never",
            "sandbox": "danger-full-access",
        }
        if model and model != "default":
            params["model"] = model
        request = asyncio.create_task(self._request("thread/start", params))
        while not request.done():
            try:
                await asyncio.shield(request)
            except asyncio.CancelledError:
                continue
        response = request.result()
        thread = response.get("thread") if isinstance(response, dict) else None
        thread_id = thread.get("id") if isinstance(thread, dict) else None
        if not thread_id:
            raise CodexAppServerError("thread/start returned no thread id")
        self._known_threads.add(str(thread_id))
        return thread

    async def fork_thread(
        self,
        thread_id: str,
        *,
        last_turn_id: str,
    ) -> dict[str, Any]:
        """Fork a persisted thread through one completed turn, inclusive."""

        if not thread_id:
            raise ValueError("thread_id is required")
        if not last_turn_id:
            raise ValueError("last_turn_id is required")
        await self.ensure_started()
        # Once the mutating RPC is on the wire, cancellation cannot tell us
        # whether Codex created the fork. Settle the request so the caller
        # always receives the new id and can either commit or compensate it.
        request = asyncio.create_task(self._request(
            "thread/fork",
            {
                "threadId": thread_id,
                "lastTurnId": last_turn_id,
                "approvalPolicy": "never",
                "sandbox": "danger-full-access",
            },
        ))
        while not request.done():
            try:
                await asyncio.shield(request)
            except asyncio.CancelledError:
                continue
        response = request.result()
        thread = response.get("thread")
        fork_id = thread.get("id") if isinstance(thread, dict) else None
        if not fork_id or fork_id == thread_id:
            raise CodexAppServerError(
                f"thread/fork returned an invalid thread for {thread_id}"
            )
        self._known_threads.add(str(fork_id))
        return thread

    async def delete_thread(self, thread_id: str) -> None:
        """Delete one terminal thread and release its thread-scoped resources."""

        if not thread_id:
            raise ValueError("thread_id is required")
        if self.has_active_thread(thread_id):
            raise CodexAppServerBusyError(
                f"Codex thread {thread_id} still has an active turn"
            )
        await self._request("thread/delete", {"threadId": thread_id})
        self._known_threads.discard(thread_id)
        self._contexts_by_thread.pop(thread_id, None)
        for turn_id, context in list(self._contexts_by_turn.items()):
            if context.thread_id == thread_id:
                self._contexts_by_turn.pop(turn_id, None)

    async def unsubscribe_thread(self, thread_id: str) -> str:
        """Release this client's idle subscription while preserving history."""

        if not thread_id:
            raise ValueError("thread_id is required")
        if self.has_active_thread(thread_id):
            raise CodexAppServerBusyError(
                f"Codex thread {thread_id} still has an active turn"
            )
        response = await self._request(
            "thread/unsubscribe",
            {"threadId": thread_id},
        )
        status = response.get("status")
        if status not in {"unsubscribed", "notSubscribed", "notLoaded"}:
            raise CodexAppServerError(
                f"thread/unsubscribe returned invalid status for {thread_id}: "
                f"{status!r}"
            )
        if status == "notLoaded":
            self._known_threads.discard(thread_id)
        return status

    async def abandon_turn(
        self,
        process: CodexTurnProcess,
        reason: str,
    ) -> bool:
        """Interrupt and detach a turn that no caller can consume.

        Returns whether the interrupt RPC was confirmed.  The registry uses a
        false result to escalate to shutting down the account transport, which
        is the only safe way to rule out untracked model work.
        """

        context = next(
            (
                candidate
                for candidate in self._contexts_by_thread.values()
                if candidate.process is process
            ),
            None,
        )
        interrupt_confirmed = False
        try:
            if context is not None and context.turn_id:
                await self._interrupt_turn_context(context)
                interrupt_confirmed = True
        except BaseException:
            logger.exception(
                "Failed to interrupt unclaimed Codex turn in %s",
                self.codex_home,
            )
        finally:
            if context is not None:
                self._detach_turn_context(context)
            process.finish(
                130,
                reason,
                termination_kind="internal_abort",
            )
        return interrupt_confirmed

    async def steer_turn(self, thread_id: str, content: str) -> bool:
        """Append user input to the currently active regular turn.

        ``expectedTurnId`` makes the request race-safe: if the turn finishes
        between the local context lookup and the RPC, app-server rejects the
        stale steer instead of attaching it to a later turn.
        """
        if not self.is_alive or not thread_id or not content:
            return False
        context = self._contexts_by_thread.get(thread_id)
        if (
            context is None
            or context.turn_id is None
            or context.process.returncode is not None
        ):
            return False

        expected_turn_id = context.turn_id
        try:
            response = await self._request(
                "turn/steer",
                {
                    "threadId": thread_id,
                    "expectedTurnId": expected_turn_id,
                    "input": [{"type": "text", "text": content}],
                },
            )
        except Exception as exc:
            actual_turn_id = _active_turn_id_from_error(exc)
            if (
                actual_turn_id
                and self._contexts_by_thread.get(thread_id) is context
                and self._bind_turn_context(
                    context,
                    actual_turn_id,
                    observed=True,
                )
            ):
                try:
                    response = await self._request(
                        "turn/steer",
                        {
                            "threadId": thread_id,
                            "expectedTurnId": actual_turn_id,
                            "input": [{"type": "text", "text": content}],
                        },
                    )
                except Exception as retry_exc:
                    exc = retry_exc
                else:
                    return response.get("turnId") == actual_turn_id
            # A normal turn-boundary race and non-steerable turns (review or
            # manual compact) are protocol rejections, not transport crashes.
            logger.info(
                "Codex steer rejected thread=%s turn=%s reason=%s",
                thread_id,
                expected_turn_id,
                exc,
            )
            return False
        return response.get("turnId") == expected_turn_id

    async def _request(
        self, method: str, params: dict[str, Any] | None,
    ) -> dict[str, Any]:
        if not self.is_alive or not self._process or not self._process.stdin:
            raise CodexAppServerError("app-server is not running")
        self._request_id += 1
        request_id = self._request_id
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        try:
            message: dict[str, Any] = {"id": request_id, "method": method}
            if params is not None:
                message["params"] = params
            await self._write(message)
            response = await asyncio.wait_for(future, timeout=self.request_timeout)
        finally:
            # Cancellation can happen while waiting for the shared write lock
            # or draining stdin, before the response wait is entered. Never
            # leave that future permanently registered.
            self._pending.pop(request_id, None)
        if "error" in response:
            error = response.get("error") or {}
            raise CodexAppServerError(
                f"{method} failed: {error.get('message') or error}"
            )
        result = response.get("result")
        return result if isinstance(result, dict) else {}

    async def _notify(self, method: str, params: dict[str, Any]) -> None:
        await self._write({"method": method, "params": params})

    async def _write(self, message: dict[str, Any]) -> None:
        if not self._process or not self._process.stdin:
            raise CodexAppServerError("app-server stdin is unavailable")
        payload = json.dumps(message, ensure_ascii=False, separators=(",", ":"))
        async with self._write_lock:
            self._process.stdin.write(payload.encode("utf-8") + b"\n")
            await self._process.stdin.drain()

    async def _read_loop(self, process: asyncio.subprocess.Process) -> None:
        assert process.stdout
        try:
            while True:
                line = await process.stdout.readline()
                if not line:
                    break
                try:
                    message = json.loads(line)
                except (UnicodeDecodeError, json.JSONDecodeError):
                    logger.warning("Ignoring malformed Codex app-server output")
                    continue
                request_id = message.get("id")
                if request_id is not None and ("result" in message or "error" in message):
                    future = self._pending.pop(request_id, None)
                    if future and not future.done():
                        future.set_result(message)
                    continue
                if request_id is not None and message.get("method"):
                    await self._handle_server_request(message)
                    continue
                if message.get("method"):
                    self._handle_notification(message["method"], message.get("params") or {})
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("Codex app-server reader failed")
        finally:
            await process.wait()
            detail = "\n".join(self._stderr_lines)[-4000:]
            error = CodexAppServerError(
                f"Codex app-server exited unexpectedly: {detail or 'no stderr'}"
            )
            for future in list(self._pending.values()):
                if not future.done():
                    future.set_exception(error)
            self._pending.clear()
            for context in list(self._contexts_by_thread.values()):
                context.process.finish(1, str(error))
            self._contexts_by_thread.clear()
            self._contexts_by_turn.clear()

    async def _stderr_loop(self, process: asyncio.subprocess.Process) -> None:
        assert process.stderr
        try:
            while True:
                line = await process.stderr.readline()
                if not line:
                    break
                self._stderr_lines.append(
                    line.decode("utf-8", errors="replace").rstrip()
                )
        except asyncio.CancelledError:
            return

    async def _handle_server_request(self, message: dict[str, Any]) -> None:
        method = str(message.get("method") or "")
        request_id = message.get("id")
        if method in {
            "item/commandExecution/requestApproval",
            "item/fileChange/requestApproval",
        }:
            # Mirrors --dangerously-bypass-approvals-and-sandbox.  These should
            # not normally arrive because approvalPolicy is "never", but an
            # explicit response prevents a protocol deadlock if they do.
            await self._write({"id": request_id, "result": {"decision": "accept"}})
            return
        if method == "item/permissions/requestApproval":
            # This newer API has a different response schema: grant the exact
            # requested profile for this turn, matching danger-full-access.
            params = message.get("params") or {}
            await self._write({
                "id": request_id,
                "result": {
                    "permissions": params.get("permissions") or {},
                    "scope": "turn",
                },
            })
            return
        if method in {"applyPatchApproval", "execCommandApproval"}:
            # Legacy v1 approval requests use ReviewDecision values.
            await self._write({
                "id": request_id,
                "result": {"decision": "approved"},
            })
            return
        if method == "currentTime/read":
            await self._write({
                "id": request_id,
                "result": {"currentTimeAt": int(time.time())},
            })
            return
        await self._write(
            {
                "id": request_id,
                "error": {"code": -32601, "message": f"Unsupported request: {method}"},
            }
        )

    def _handle_notification(self, method: str, params: dict[str, Any]) -> None:
        thread_id = params.get("threadId")
        turn_id = self._notification_turn_id(method, params)
        context = self._contexts_by_turn.get(turn_id) if turn_id else None
        if (
            context is not None
            and thread_id
            and str(thread_id) != context.thread_id
        ):
            logger.error(
                "Ignoring Codex notification with mismatched thread/turn "
                "thread=%s expected_thread=%s turn=%s method=%s",
                thread_id,
                context.thread_id,
                turn_id,
                method,
            )
            return
        if context is None and thread_id:
            candidate = self._contexts_by_thread.get(str(thread_id))
            if candidate is not None and turn_id:
                # turn/start can return a fresh submission id while its input
                # is actually steered into an existing goal turn. The first
                # real notification is authoritative. Once a real turn has
                # been observed, an unknown id belongs to another lifecycle
                # and must not be cross-routed by thread fallback.
                if (
                    candidate.observed_turn_id is not None
                    and candidate.observed_turn_id != turn_id
                ):
                    logger.debug(
                        "Ignoring Codex notification for unrelated turn "
                        "thread=%s expected=%s actual=%s method=%s",
                        thread_id,
                        candidate.observed_turn_id,
                        turn_id,
                        method,
                    )
                    return
                if not self._bind_turn_context(
                    candidate,
                    turn_id,
                    observed=True,
                ):
                    return
            context = candidate
        if context is None:
            return
        if turn_id and context.observed_turn_id is None:
            if not self._bind_turn_context(context, turn_id, observed=True):
                return

        if method == "turn/started":
            context.process.feed({"type": "turn.started"})
            return

        if method == "item/started":
            item = params.get("item") or {}
            if item.get("type") == "userMessage" and not context.first_input_seen:
                context.first_input_seen = True
                logger.info(
                    "Codex latency task=%s thread=%s stage=model_input elapsed_ms=%.1f",
                    context.task_id,
                    context.thread_id,
                    (time.perf_counter() - context.launch_started) * 1000,
                )
            normalized = self._normalize_item(item)
            if normalized and normalized.get("type") in {
                "command_execution", "file_change", "mcp_tool_call", "web_search"
            }:
                context.process.feed({
                    "type": "item.started",
                    "item": normalized,
                    "turn_id": context.turn_id,
                })
            return

        if method == "item/completed":
            item = params.get("item") or {}
            normalized = self._normalize_item(item)
            if normalized and normalized.get("type") != "user_message":
                context.process.feed({
                    "type": "item.completed",
                    "item": normalized,
                    "turn_id": context.turn_id,
                })
            return

        if method == "item/agentMessage/delta":
            if not context.first_output_seen:
                context.first_output_seen = True
                logger.info(
                    "Codex latency task=%s thread=%s stage=first_delta elapsed_ms=%.1f",
                    context.task_id,
                    context.thread_id,
                    (time.perf_counter() - context.launch_started) * 1000,
                )
            context.process.feed(
                {
                    "type": "item.agent_message.delta",
                    "delta": params.get("delta") or "",
                    "item_id": params.get("itemId"),
                    "turn_id": context.turn_id,
                }
            )
            return

        if method in {
            "item/reasoning/summaryTextDelta",
            "item/reasoning/textDelta",
        }:
            context.process.feed(
                {
                    "type": "item.reasoning.delta",
                    "delta": params.get("delta") or "",
                    "item_id": params.get("itemId"),
                    "turn_id": context.turn_id,
                }
            )
            return

        if method == "thread/tokenUsage/updated":
            token_usage = params.get("tokenUsage") or {}
            last = token_usage.get("last") or token_usage.get("total") or {}
            context.usage = {
                "input_tokens": int(last.get("inputTokens") or 0),
                "cached_input_tokens": int(last.get("cachedInputTokens") or 0),
                "output_tokens": int(last.get("outputTokens") or 0),
                "reasoning_output_tokens": int(
                    last.get("reasoningOutputTokens") or 0
                ),
                "total_tokens": int(last.get("totalTokens") or 0),
                "context_window": int(
                    token_usage.get("modelContextWindow") or 0
                ),
            }
            return

        if method == "error":
            error = params.get("error") or params
            normalized_error = self._normalize_turn_error(error)
            context.process.feed({
                "type": "turn.failed",
                "error": normalized_error,
                "turn_id": context.turn_id,
            })
            return

        if method == "turn/completed":
            turn = params.get("turn") or {}
            status = turn.get("status") or "completed"
            error = turn.get("error")
            if status == "completed":
                context.process.feed(
                    {
                        "type": "turn.completed",
                        "usage": context.usage or {},
                        "turn_id": context.turn_id,
                    }
                )
                exit_code = 0
                stderr = ""
            elif status == "interrupted":
                context.process.feed(
                    {
                        "type": "turn.completed",
                        "usage": context.usage or {},
                        "turn_id": context.turn_id,
                    }
                )
                exit_code = 130
                stderr = ""
            else:
                normalized_error = self._normalize_turn_error(
                    error,
                    fallback=f"Codex turn ended with status {status}",
                )
                message = normalized_error["message"]
                context.process.feed(
                    {"type": "turn.failed", "error": normalized_error}
                )
                exit_code = 1
                stderr = str(message)
            logger.info(
                "Codex latency task=%s thread=%s stage=completed elapsed_ms=%.1f status=%s",
                context.task_id,
                context.thread_id,
                (time.perf_counter() - context.launch_started) * 1000,
                status,
            )
            context.process.finish(
                exit_code,
                stderr,
                termination_kind=(
                    "user_interrupt"
                    if status == "interrupted"
                    else None
                ),
            )
            self._detach_turn_context(context)

    @staticmethod
    def _normalize_turn_error(
        error: Any,
        *,
        fallback: str = "Codex turn failed",
    ) -> dict[str, Any]:
        """Keep app-server error classification fields alongside its message."""

        if isinstance(error, dict):
            normalized = dict(error)
            message = normalized.get("message") or fallback
        else:
            normalized = {}
            message = error or fallback
        normalized["message"] = str(message)
        return normalized

    @staticmethod
    def _normalize_item(item: dict[str, Any]) -> dict[str, Any] | None:
        item_type = item.get("type")
        type_map = {
            "userMessage": "user_message",
            "agentMessage": "agent_message",
            "commandExecution": "command_execution",
            "fileChange": "file_change",
            "mcpToolCall": "mcp_tool_call",
            "webSearch": "web_search",
            "todoList": "todo_list",
        }
        normalized = dict(item)
        normalized["type"] = type_map.get(item_type, item_type)
        rename = {
            "aggregatedOutput": "aggregated_output",
            "exitCode": "exit_code",
        }
        for source, target in rename.items():
            if source in normalized:
                normalized[target] = normalized.pop(source)
        if normalized.get("type") == "reasoning":
            pieces = normalized.get("summary") or normalized.get("content") or []
            normalized["text"] = "\n".join(str(piece) for piece in pieces if piece)
        return normalized

    def _managed_process_group_id(
        self,
        process: asyncio.subprocess.Process,
    ) -> int | None:
        """Return the exact signal-safe PGID created for this generation."""

        if (
            os.name != "posix"
            or self._process_group_process is not process
        ):
            return None
        return require_safe_process_group_id(
            getattr(process, "pid", None),
            context=f"Codex app-server home {self.codex_home}",
        )

    def _process_group_alive(
        self,
        process: asyncio.subprocess.Process,
    ) -> bool:
        process_group_id = self._managed_process_group_id(process)
        if process_group_id is None:
            return False
        try:
            os.killpg(process_group_id, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            # Inability to signal is not proof that the group disappeared.
            return True

    def _signal_process_generation(
        self,
        process: asyncio.subprocess.Process,
        sig: signal.Signals,
    ) -> None:
        process_group_id = self._managed_process_group_id(process)
        try:
            if process_group_id is not None:
                os.killpg(process_group_id, sig)
            elif sig == signal.SIGTERM:
                process.terminate()
            elif sig == signal.SIGKILL:
                process.kill()
            else:
                process.send_signal(sig)
        except ProcessLookupError:
            # Exit may race the signal.  The bounded verification below is the
            # authority for whether the full generation is actually gone.
            return

    async def _wait_process_generation(
        self,
        process: asyncio.subprocess.Process,
        timeout: float,
    ) -> None:
        """Wait until both the app-server leader and its POSIX group are gone."""

        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        if process.returncode is None:
            await asyncio.wait_for(
                asyncio.shield(process.wait()),
                timeout=max(0.01, timeout),
            )
        while self._process_group_alive(process):
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise asyncio.TimeoutError
            await asyncio.sleep(
                min(_APP_SERVER_GROUP_POLL_INTERVAL, remaining)
            )

    async def _shutdown_locked(self) -> None:
        """Stop one generation while ``_lifecycle_lock`` is held."""

        process = self._process
        if not process:
            return
        if process.stdin:
            try:
                process.stdin.close()
            except Exception:
                logger.exception(
                    "Failed to close Codex app-server stdin home=%s",
                    self.codex_home,
                )

        try:
            await self._wait_process_generation(
                process,
                _APP_SERVER_GRACEFUL_SHUTDOWN_TIMEOUT,
            )
        except asyncio.TimeoutError:
            self._signal_process_generation(process, signal.SIGTERM)
            try:
                await self._wait_process_generation(
                    process,
                    _APP_SERVER_TERM_SHUTDOWN_TIMEOUT,
                )
            except asyncio.TimeoutError:
                self._signal_process_generation(process, signal.SIGKILL)
                try:
                    await self._wait_process_generation(
                        process,
                        _APP_SERVER_KILL_SHUTDOWN_TIMEOUT,
                    )
                except asyncio.TimeoutError as exc:
                    # Do not clear the process/tasks: callers and the
                    # registry need that evidence to remain fail-closed.
                    raise CodexAppServerError(
                        "Codex app-server process group survived SIGKILL "
                        f"for home {self.codex_home}"
                    ) from exc

        for task in (self._reader_task, self._stderr_task):
            if task and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (self._reader_task, self._stderr_task) if task),
            return_exceptions=True,
        )
        if self._process is process:
            self._process = None
        if self._process_group_process is process:
            self._process_group_process = None
        self._reader_task = None
        self._stderr_task = None

    async def shutdown(self) -> None:
        """Permanently stop and verify this server object's process group."""

        # Publish the close intent before waiting for the lifecycle barrier.
        # A start already inside the barrier will be stopped after it exits;
        # one queued behind it will observe this flag and cannot spawn later.
        self._shutdown_requested = True
        async with self._lifecycle_lock:
            await self._shutdown_locked()


class CodexAppServerRegistry:
    """Route Codex turns to one persistent app-server per account home.

    ``CODEX_HOME`` is process-scoped, so changing an environment variable per
    thread on one app-server can never provide account isolation.  This facade
    keeps the old InstanceManager-facing surface while enforcing a stable
    thread -> home owner and independent process lifecycle for every account.
    """

    def __init__(
        self,
        binary: str,
        request_timeout: float = 30.0,
        *,
        env_remove_resolver: Callable[[str], set[str]] | None = None,
    ) -> None:
        self.binary = binary
        self.request_timeout = request_timeout
        self._env_remove_resolver = env_remove_resolver
        self._servers: dict[str, CodexAppServer] = {}
        self._thread_owners: dict[str, str] = {}
        self._draining: set[str] = set()
        # A thread rebind spans an optional target-server shutdown. Keep the
        # route reserved across that await so a manual resume or account
        # maintenance operation cannot reopen the source/target midway.
        self._rebindings: dict[str, tuple[str, str]] = {}
        # Number of home-bound RPC sequences (turn start/resume or account
        # reads) admitted but not yet returned to the caller. Maintenance
        # checks this under the same lock so relogin cannot race between
        # server lookup and an RPC using the old auth.json.
        self._starting: dict[str, int] = {}
        # A home-level count protects maintenance; this per-thread token also
        # prevents two concurrent resumes of the same native thread. Without
        # it, a failed first request could remove the successful second
        # request's owner because both routes contain the same home value.
        self._starting_threads: dict[str, object] = {}
        self._shutdown_requested = False
        self._lock = asyncio.Lock()

    async def start_turn(
        self,
        *,
        codex_home: str | os.PathLike[str] | None = None,
        **kwargs: Any,
    ) -> tuple[CodexTurnProcess, str]:
        home = normalize_codex_home(codex_home)
        resume_session_id = kwargs.get("resume_session_id")
        reserved_owner = False
        start_token: object | None = None

        async with self._lock:
            if self._shutdown_requested:
                raise CodexAppServerBusyError(
                    "Codex app-server registry is shutting down"
                )
            if home in self._draining:
                raise CodexAppServerBusyError(
                    f"Codex account app-server is draining: {home}"
                )
            if resume_session_id:
                if resume_session_id in self._starting_threads:
                    raise CodexAppServerBusyError(
                        f"Codex thread {resume_session_id} already has a resume request in flight"
                    )
                if resume_session_id in self._rebindings:
                    raise CodexAppServerBusyError(
                        f"Codex thread {resume_session_id} is being rebound"
                    )
                owner = self._thread_owners.get(resume_session_id)
                if owner is not None and owner != home:
                    raise CodexThreadHomeMismatchError(
                        f"Codex thread {resume_session_id} is bound to {owner}, not {home}; "
                        "migrate and rebind it before resume"
                    )
                if owner is None:
                    # Reserve the route while the RPC is in flight so two
                    # concurrent resumes cannot load one rollout in two homes.
                    self._thread_owners[resume_session_id] = home
                    reserved_owner = True
                start_token = object()
                self._starting_threads[resume_session_id] = start_token
            server = self._servers.get(home)
            if server is None:
                server_kwargs: dict[str, Any] = {}
                if self._env_remove_resolver is not None:
                    server_kwargs["env_remove"] = self._env_remove_resolver(home)
                server = CodexAppServer(
                    self.binary,
                    request_timeout=self.request_timeout,
                    codex_home=home,
                    **server_kwargs,
                )
                self._servers[home] = server
            self._starting[home] = self._starting.get(home, 0) + 1

        process: CodexTurnProcess | None = None
        thread_id: str | None = None
        admitted = False
        starting_released = False
        try:
            try:
                process, thread_id = await server.start_turn(**kwargs)
            except (
                _UnconfirmedTurnCancellation,
                _UnconfirmedTurnStartFailure,
            ) as exc:
                # The local adapter is terminal, but the real server-side turn
                # may still be executing. Preserve it for registry escalation.
                process = exc.process
                raise

            async with self._lock:
                assert process is not None and thread_id is not None
                owner = self._thread_owners.get(thread_id)
                if owner is not None and owner != home:
                    raise CodexThreadHomeMismatchError(
                        f"Codex thread {thread_id} is already owned by {owner}, not {home}"
                    )
                self._decrement_starting_locked(home)
                starting_released = True
                self._thread_owners[thread_id] = home
                admitted = True
            return process, thread_id
        except BaseException:
            if getattr(server, "shutdown_requested", False):
                async with self._lock:
                    if self._servers.get(home) is server:
                        self._draining.add(home)
            if process is not None and not admitted:
                async def _abort_cancelled_admission() -> None:
                    await self.abort_unclaimed_turn(
                        home,
                        process,
                        reason="Codex registry admission did not complete",
                    )

                cleanup = asyncio.create_task(_abort_cancelled_admission())
                while not cleanup.done():
                    try:
                        await asyncio.shield(cleanup)
                    except asyncio.CancelledError:
                        continue
                cleanup.result()
            raise
        finally:
            # Release counters/tokens even when abort or transport shutdown
            # fails. Such failures keep the home draining but must not poison
            # unrelated lifecycle accounting. Identity-check the thread token
            # so one request can never erase another generation's reservation.
            async def _release_start_reservations() -> None:
                async with self._lock:
                    if not starting_released:
                        self._decrement_starting_locked(home)
                    if (
                        resume_session_id
                        and self._starting_threads.get(resume_session_id) is start_token
                    ):
                        self._starting_threads.pop(resume_session_id, None)
                    if (
                        not admitted
                        and reserved_owner
                        and self._thread_owners.get(resume_session_id) == home
                    ):
                        self._thread_owners.pop(resume_session_id, None)

            cleanup = asyncio.create_task(_release_start_reservations())
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    continue
            cleanup.result()

    def _decrement_starting_locked(self, home: str) -> None:
        """Release one start reservation while ``self._lock`` is held."""

        starting = self._starting.get(home, 0)
        if starting > 1:
            self._starting[home] = starting - 1
        else:
            self._starting.pop(home, None)

    async def read_thread(
        self,
        codex_home: str | os.PathLike[str] | None,
        thread_id: str,
    ) -> dict[str, Any]:
        """Read one idle native thread from its exact account home."""

        home = normalize_codex_home(codex_home)
        token = object()
        reserved_owner = False
        async with self._lock:
            if self._shutdown_requested or home in self._draining:
                raise CodexAppServerBusyError(
                    f"Codex account app-server is unavailable: {home}"
                )
            owner = self._thread_owners.get(thread_id)
            if owner is not None and owner != home:
                raise CodexThreadHomeMismatchError(
                    f"Codex thread {thread_id} is bound to {owner}, not {home}"
                )
            if thread_id in self._starting_threads or thread_id in self._rebindings:
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} already has an operation in flight"
                )
            server = self._servers.get(home)
            if server is None:
                server_kwargs: dict[str, Any] = {}
                if self._env_remove_resolver is not None:
                    server_kwargs["env_remove"] = self._env_remove_resolver(home)
                server = CodexAppServer(
                    self.binary,
                    request_timeout=self.request_timeout,
                    codex_home=home,
                    **server_kwargs,
                )
                self._servers[home] = server
            if server.has_active_thread(thread_id):
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} still has an active turn"
                )
            if owner is None:
                self._thread_owners[thread_id] = home
                reserved_owner = True
            self._starting_threads[thread_id] = token
            self._starting[home] = self._starting.get(home, 0) + 1

        succeeded = False
        try:
            result = await server.read_thread(thread_id)
            succeeded = True
            return result
        finally:
            async with self._lock:
                self._decrement_starting_locked(home)
                if self._starting_threads.get(thread_id) is token:
                    self._starting_threads.pop(thread_id, None)
                if (
                    reserved_owner
                    and not succeeded
                    and self._thread_owners.get(thread_id) == home
                ):
                    self._thread_owners.pop(thread_id, None)

    async def create_thread(
        self,
        codex_home: str | os.PathLike[str] | None,
        *,
        cwd: str,
        model: str | None = None,
    ) -> dict[str, Any]:
        """Create an empty thread in one exact account home."""

        home = normalize_codex_home(codex_home)
        async with self._lock:
            if self._shutdown_requested or home in self._draining:
                raise CodexAppServerBusyError(
                    f"Codex account app-server is unavailable: {home}"
                )
            server = self._servers.get(home)
            if server is None:
                server_kwargs: dict[str, Any] = {}
                if self._env_remove_resolver is not None:
                    server_kwargs["env_remove"] = self._env_remove_resolver(home)
                server = CodexAppServer(
                    self.binary,
                    request_timeout=self.request_timeout,
                    codex_home=home,
                    **server_kwargs,
                )
                self._servers[home] = server
            self._starting[home] = self._starting.get(home, 0) + 1

        thread_id: str | None = None
        try:
            result = await server.create_thread(cwd=cwd, model=model)
            thread_id = str(result["id"])
            async with self._lock:
                existing = self._thread_owners.get(thread_id)
                if existing is not None and existing != home:
                    raise CodexThreadHomeMismatchError(
                        f"New Codex thread {thread_id} is already bound to {existing}"
                    )
                self._thread_owners[thread_id] = home
            return result
        finally:
            async with self._lock:
                self._decrement_starting_locked(home)

    async def fork_thread(
        self,
        codex_home: str | os.PathLike[str] | None,
        thread_id: str,
        *,
        last_turn_id: str,
    ) -> dict[str, Any]:
        """Fork an idle native thread and register the new thread owner."""

        home = normalize_codex_home(codex_home)
        token = object()
        reserved_owner = False
        async with self._lock:
            if self._shutdown_requested or home in self._draining:
                raise CodexAppServerBusyError(
                    f"Codex account app-server is unavailable: {home}"
                )
            owner = self._thread_owners.get(thread_id)
            if owner is not None and owner != home:
                raise CodexThreadHomeMismatchError(
                    f"Codex thread {thread_id} is bound to {owner}, not {home}"
                )
            if thread_id in self._starting_threads or thread_id in self._rebindings:
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} already has an operation in flight"
                )
            server = self._servers.get(home)
            if server is None:
                server_kwargs: dict[str, Any] = {}
                if self._env_remove_resolver is not None:
                    server_kwargs["env_remove"] = self._env_remove_resolver(home)
                server = CodexAppServer(
                    self.binary,
                    request_timeout=self.request_timeout,
                    codex_home=home,
                    **server_kwargs,
                )
                self._servers[home] = server
            if server.has_active_thread(thread_id):
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} still has an active turn"
                )
            if owner is None:
                self._thread_owners[thread_id] = home
                reserved_owner = True
            self._starting_threads[thread_id] = token
            self._starting[home] = self._starting.get(home, 0) + 1

        fork_id: str | None = None
        try:
            result = await server.fork_thread(
                thread_id,
                last_turn_id=last_turn_id,
            )
            fork_id = str(result["id"])
            async with self._lock:
                existing = self._thread_owners.get(fork_id)
                if existing is not None and existing != home:
                    raise CodexThreadHomeMismatchError(
                        f"Forked Codex thread {fork_id} is already bound to {existing}"
                    )
                self._thread_owners[fork_id] = home
            return result
        finally:
            async with self._lock:
                self._decrement_starting_locked(home)
                if self._starting_threads.get(thread_id) is token:
                    self._starting_threads.pop(thread_id, None)
                if (
                    reserved_owner
                    and fork_id is None
                    and self._thread_owners.get(thread_id) == home
                ):
                    self._thread_owners.pop(thread_id, None)

    async def delete_thread(
        self,
        codex_home: str | os.PathLike[str] | None,
        thread_id: str,
    ) -> None:
        """Delete one terminal thread without disturbing its shared transport."""

        if not thread_id:
            raise ValueError("thread_id is required")
        home = normalize_codex_home(codex_home)
        token = object()

        async with self._lock:
            if self._shutdown_requested:
                raise CodexAppServerBusyError(
                    "Codex app-server registry is shutting down"
                )
            if home in self._draining:
                raise CodexAppServerBusyError(
                    f"Codex account app-server is draining: {home}"
                )
            owner = self._thread_owners.get(thread_id)
            if owner is not None and owner != home:
                raise CodexThreadHomeMismatchError(
                    f"Codex thread {thread_id} is bound to {owner}, not {home}"
                )
            if thread_id in self._rebindings:
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} is being rebound"
                )
            if thread_id in self._starting_threads:
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} already has a request in flight"
                )
            server = self._servers.get(home)
            if server is None:
                raise CodexAppServerError(
                    f"Codex app-server is unavailable for thread {thread_id}"
                )
            self._starting_threads[thread_id] = token
            self._starting[home] = self._starting.get(home, 0) + 1

        deleted = False
        try:
            await server.delete_thread(thread_id)
            deleted = True
        finally:
            async with self._lock:
                self._decrement_starting_locked(home)
                if self._starting_threads.get(thread_id) is token:
                    self._starting_threads.pop(thread_id, None)
                if deleted and self._thread_owners.get(thread_id) == home:
                    self._thread_owners.pop(thread_id, None)

    async def unsubscribe_thread(self, thread_id: str) -> str:
        """Release one idle subscription without dropping its resumable owner."""

        if not thread_id:
            raise ValueError("thread_id is required")
        token = object()

        async with self._lock:
            owner = self._thread_owners.get(thread_id)
            if owner is None:
                raise CodexAppServerError(
                    f"Codex thread {thread_id} has no registered owner"
                )
            if self._shutdown_requested:
                raise CodexAppServerBusyError(
                    "Codex app-server registry is shutting down"
                )
            if owner in self._draining:
                raise CodexAppServerBusyError(
                    f"Codex account app-server is draining: {owner}"
                )
            if thread_id in self._rebindings:
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} is being rebound"
                )
            if thread_id in self._starting_threads:
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} already has a request in flight"
                )
            server = self._servers.get(owner)
            if server is None:
                raise CodexAppServerError(
                    f"Codex app-server is unavailable for thread {thread_id}"
                )
            self._starting_threads[thread_id] = token
            self._starting[owner] = self._starting.get(owner, 0) + 1

        try:
            return await server.unsubscribe_thread(thread_id)
        finally:
            async with self._lock:
                self._decrement_starting_locked(owner)
                if self._starting_threads.get(thread_id) is token:
                    self._starting_threads.pop(thread_id, None)

    async def abort_unclaimed_turn(
        self,
        codex_home: str | os.PathLike[str],
        process: CodexTurnProcess,
        *,
        reason: str,
    ) -> None:
        """Ensure a successfully-started turn cannot outlive its cancelled caller."""

        home = normalize_codex_home(codex_home)
        async with self._lock:
            server = self._servers.get(home)
            if server is not None:
                # No new turn may enter while the interrupt outcome is unknown.
                # Otherwise a required transport shutdown could strand or kill
                # a second, newly admitted turn.
                self._draining.add(home)
        if server is None:
            process.finish(
                130,
                reason,
                termination_kind="internal_abort",
            )
            return

        abandon = getattr(server, "abandon_turn", None)
        interrupt_confirmed = False
        try:
            if abandon is not None:
                interrupt_confirmed = await abandon(process, reason)
            else:
                process.terminate()
                process.finish(
                    130,
                    reason,
                    termination_kind="internal_abort",
                )
        except BaseException:
            logger.exception(
                "Failed to interrupt unclaimed Codex turn before transport shutdown: %s",
                home,
            )
        if interrupt_confirmed:
            async with self._lock:
                self._draining.discard(home)
            return

        # If the interrupt was not acknowledged, stopping this account's
        # transport is the only way to rule out real model work continuing
        # without an InstanceManager consumer. A shutdown failure deliberately
        # leaves the account draining (fail-closed).
        shutdown_completed = False
        try:
            await server.shutdown()
            shutdown_completed = True
        except BaseException:
            logger.exception(
                "Failed to shut down Codex transport after unclaimed turn: %s",
                home,
            )
            raise
        finally:
            if shutdown_completed:
                async with self._lock:
                    if self._servers.get(home) is server:
                        self._servers.pop(home, None)
                    for owned_thread, owner in list(self._thread_owners.items()):
                        if owner == home:
                            self._thread_owners.pop(owned_thread, None)
                    self._draining.discard(home)

    async def steer_turn(self, thread_id: str, content: str) -> bool:
        async with self._lock:
            home = self._thread_owners.get(thread_id)
            server = self._servers.get(home) if home else None
        if server is None:
            return False
        return await server.steer_turn(thread_id, content)

    async def read_rate_limits(
        self, codex_home: str | os.PathLike[str] | None,
    ) -> dict[str, Any]:
        """Read one account's quota without crossing CODEX_HOME boundaries."""

        home = normalize_codex_home(codex_home)
        async with self._lock:
            if self._shutdown_requested:
                raise CodexAppServerBusyError(
                    "Codex app-server registry is shutting down"
                )
            if home in self._draining:
                raise CodexAppServerBusyError(
                    f"Codex account app-server is draining: {home}"
                )
            server = self._servers.get(home)
            if server is None:
                server_kwargs: dict[str, Any] = {}
                if self._env_remove_resolver is not None:
                    server_kwargs["env_remove"] = self._env_remove_resolver(home)
                server = CodexAppServer(
                    self.binary,
                    request_timeout=self.request_timeout,
                    codex_home=home,
                    **server_kwargs,
                )
                self._servers[home] = server
            self._starting[home] = self._starting.get(home, 0) + 1

        try:
            return await server.read_rate_limits()
        except BaseException:
            if getattr(server, "shutdown_requested", False):
                async with self._lock:
                    if self._servers.get(home) is server:
                        self._draining.add(home)
            raise
        finally:
            # Cancellation must not leak the home reservation and make future
            # relogin/delete operations report a permanent busy state.
            async def _release_read_reservation() -> None:
                async with self._lock:
                    self._decrement_starting_locked(home)

            cleanup = asyncio.create_task(_release_read_reservation())
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    continue
            cleanup.result()

    async def rebind_thread(
        self,
        thread_id: str,
        *,
        source_codex_home: str | os.PathLike[str] | None,
        target_codex_home: str | os.PathLike[str],
    ) -> None:
        """Move registry ownership after the rollout was safely copied.

        App-server keeps completed threads in memory.  If the target server has
        loaded this thread before (the B -> A leg of a round trip), an idle
        target process is restarted so its next ``thread/resume`` reads the
        newly copied rollout.  Active target turns are never killed; callers
        receive a retryable busy error instead.
        """

        source = normalize_codex_home(source_codex_home)
        target = normalize_codex_home(target_codex_home)
        if source == target:
            async with self._lock:
                self._thread_owners[thread_id] = target
            return

        restart_server: CodexAppServer | None = None
        async with self._lock:
            if thread_id in self._rebindings:
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} is already being rebound"
                )
            owner = self._thread_owners.get(thread_id)
            if owner is not None and owner != source:
                raise CodexThreadHomeMismatchError(
                    f"Codex thread {thread_id} is owned by {owner}, expected {source}"
                )
            source_server = self._servers.get(source)
            if self._starting.get(source, 0) > 0:
                raise CodexAppServerBusyError(
                    f"Codex source account has a start/resume request in flight: {source}"
                )
            if source_server and source_server.has_active_thread(thread_id):
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} still has an active turn in {source}"
                )
            if target in self._draining:
                raise CodexAppServerBusyError(
                    f"Codex target account app-server is draining: {target}"
                )
            target_server = self._servers.get(target)
            if target_server and target_server.knows_thread(thread_id):
                if self._starting.get(target, 0) > 0:
                    raise CodexAppServerBusyError(
                        f"Codex target account has a start/resume request in flight "
                        f"and a stale cached copy of thread {thread_id}: {target}"
                    )
                if target_server.has_active_turns:
                    raise CodexAppServerBusyError(
                        f"Codex target account has active turns and a stale cached "
                        f"copy of thread {thread_id}: {target}"
                    )
                self._draining.add(target)
                restart_server = target_server
            self._rebindings[thread_id] = (source, target)

        try:
            if restart_server is not None:
                await restart_server.shutdown()
                async with self._lock:
                    if self._servers.get(target) is restart_server:
                        self._servers.pop(target, None)
                    self._draining.discard(target)

            async with self._lock:
                owner = self._thread_owners.get(thread_id)
                if owner is not None and owner != source:
                    raise CodexThreadHomeMismatchError(
                        f"Codex thread {thread_id} changed owner to {owner} "
                        f"while rebinding from {source}"
                    )
                self._thread_owners[thread_id] = target
        finally:
            async with self._lock:
                self._rebindings.pop(thread_id, None)

    async def clear_thread_owner_for_recovery(
        self,
        thread_id: str,
        *,
        expected_codex_home: str | os.PathLike[str],
    ) -> bool:
        """Drop an idle owner reservation after a failed migration rollback.

        Completed turn contexts are already removed by ``turn/completed``;
        only the registry's thread-to-home reservation can remain split from
        the durable task binding. Clearing that one mapping is safer than
        shutting down an account server that may be serving unrelated turns.
        The next resume then cold-routes from the DB-authoritative account.
        """

        expected = normalize_codex_home(expected_codex_home)
        async with self._lock:
            if thread_id in self._rebindings:
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} is being rebound"
                )
            owner = self._thread_owners.get(thread_id)
            if owner is None:
                return True
            if owner != expected:
                raise CodexThreadHomeMismatchError(
                    f"Codex thread {thread_id} is owned by {owner}, expected {expected}"
                )
            server = self._servers.get(owner)
            if server and server.has_active_thread(thread_id):
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} still has an active turn in {owner}"
                )
            self._thread_owners.pop(thread_id, None)
            return True

    async def begin_home_maintenance(
        self,
        codex_home: str | os.PathLike[str],
        *,
        require_idle: bool = True,
    ) -> bool:
        """Reserve and stop one home until ``end_home_maintenance``.

        This is the relogin/delete primitive: once it returns, new turns for
        the home are rejected even though its old process has already stopped.
        The caller must release the reservation in a ``finally`` block.
        """

        home = normalize_codex_home(codex_home)
        async with self._lock:
            if home in self._draining:
                raise CodexAppServerBusyError(
                    f"Codex account app-server is already draining: {home}"
                )
            if any(home in pair for pair in self._rebindings.values()):
                raise CodexAppServerBusyError(
                    f"Codex account has a thread rebind in flight: {home}"
                )
            server = self._servers.get(home)
            if require_idle and (
                self._starting.get(home, 0) > 0
                or (server is not None and server.has_active_turns)
            ):
                raise CodexAppServerBusyError(
                    f"Codex account still has an active or starting turn: {home}"
                )
            self._draining.add(home)

        shutdown_completed = server is None
        try:
            if server is not None:
                await server.shutdown()
                shutdown_completed = True
            # Cancellation can land after shutdown has completed but while the
            # registry lock below is contended.  Keep that await inside the
            # reservation guard so a half-finished begin never strands the
            # home in ``_draining`` forever.
            async with self._lock:
                if server is not None and self._servers.get(home) is server:
                    self._servers.pop(home, None)
                for thread_id, owner in list(self._thread_owners.items()):
                    if owner == home:
                        self._thread_owners.pop(thread_id, None)
        except asyncio.CancelledError:
            if shutdown_completed:
                # The caller never observes a successful reservation and thus
                # cannot call end_home_maintenance.  Finish detaching the now
                # dead server before reopening the home; merely clearing
                # _draining would make start_turn reuse a closed transport.
                async def _detach_and_reopen() -> None:
                    async with self._lock:
                        if server is not None and self._servers.get(home) is server:
                            self._servers.pop(home, None)
                        for thread_id, owner in list(self._thread_owners.items()):
                            if owner == home:
                                self._thread_owners.pop(thread_id, None)
                        self._draining.discard(home)

                cleanup = asyncio.create_task(_detach_and_reopen())
                while not cleanup.done():
                    try:
                        await asyncio.shield(cleanup)
                    except asyncio.CancelledError:
                        # Repeated caller cancellation must not reopen the home
                        # before the closed server has been removed.
                        continue
                cleanup.result()
            # If shutdown itself was cancelled, retain _draining fail-closed;
            # the server may be only partially terminated and must not be reused.
            raise
        except BaseException:
            # A shutdown failure has unknown process state.  Keep the home
            # reserved rather than reopening it onto a possibly half-dead or
            # still-running transport; restart recovery clears in-memory state.
            raise
        return server is not None

    async def end_home_maintenance(
        self, codex_home: str | os.PathLike[str],
    ) -> None:
        """Release a reservation created by ``begin_home_maintenance``."""

        home = normalize_codex_home(codex_home)
        async with self._lock:
            self._draining.discard(home)

    async def shutdown_home(
        self,
        codex_home: str | os.PathLike[str],
        *,
        require_idle: bool = True,
    ) -> bool:
        """One-shot idle shutdown; unlike maintenance it immediately reopens."""

        maintenance_started = False
        try:
            stopped = await self.begin_home_maintenance(
                codex_home, require_idle=require_idle,
            )
            maintenance_started = True
            return stopped
        finally:
            if maintenance_started:
                await self.end_home_maintenance(codex_home)

    async def shutdown(self) -> None:
        async with self._lock:
            self._shutdown_requested = True
            servers = list(self._servers.items())
            self._draining.update(self._servers)
        results = await asyncio.gather(
            *(server.shutdown() for _, server in servers),
            return_exceptions=True,
        )
        failures: list[tuple[str, BaseException]] = []
        successful_homes: set[str] = set()
        for (home, _), result in zip(servers, results):
            if isinstance(result, BaseException):
                failures.append((home, result))
                logger.error(
                    "Failed to stop Codex app-server home=%s: %s",
                    home,
                    result,
                )
            else:
                successful_homes.add(home)

        async with self._lock:
            for home, server in servers:
                if home not in successful_homes:
                    # Preserve both route and draining evidence for a process
                    # generation whose termination could not be proven.
                    if self._servers.get(home) is server:
                        self._draining.add(home)
                    continue
                if self._servers.get(home) is server:
                    self._servers.pop(home, None)
                for thread_id, owner in list(self._thread_owners.items()):
                    if owner == home:
                        self._thread_owners.pop(thread_id, None)
                self._starting.pop(home, None)
                self._draining.discard(home)

            # In the normal quiescent shutdown path every server in the
            # registry was part of this snapshot.  Clear the remaining
            # coordination-only state.  If another server appeared
            # concurrently, retain its evidence instead of orphaning it.
            if not failures and not self._servers:
                self._thread_owners.clear()
                self._rebindings.clear()
                self._starting.clear()
                self._starting_threads.clear()
                self._draining.clear()

        if failures:
            homes = ", ".join(home for home, _ in failures)
            raise CodexAppServerError(
                "Failed to stop Codex app-server transport(s); "
                f"left draining for: {homes}"
            ) from failures[0][1]
