"""Goal condition evaluator using a lightweight Claude model.

Spawns a short-lived `claude -p` subprocess (default Haiku) to judge whether
the conversation so far satisfies the user's goal condition.  The evaluator
only reads the conversation transcript — it cannot call tools or read files.
"""
import asyncio
import json
import logging
import os
import signal
import tempfile
import time
import weakref
from dataclasses import dataclass

from backend.config import settings
from backend.services.cancellation import (
    await_task_completion,
    finish_awaitable,
    settle_awaitable,
)
from backend.services.codex_app_server import (
    normalize_codex_service_tier,
)
from backend.services.claude_auth_projection import (
    ClaudeAuthProjectionError,
    apply_claude_auth_projection,
    environment_has_direct_claude_auth,
    inject_cloudrouter_claude_direct_auth,
    prepare_claude_auth_projection,
    remove_claude_auth_projection,
)
from backend.services.process_safety import require_safe_process_group_id
from backend.services.task_agent_isolation import (
    TaskAgentIsolationError,
    generate_claude_zero_tool_isolation_settings,
    require_task_security_boundary_configured,
    scrub_task_model_environment,
    validate_claude_zero_tool_isolation_settings,
)
from backend.services.task_ssh_access import (
    TaskSSHAccessError,
    manager_secret_protected_paths,
)

logger = logging.getLogger(__name__)

_PROCESS_CLEANUP_TIMEOUT = 5.0
_CLOUDROUTER_CLAUDE_AUTH_ENV_KEYS = (
    "ANTHROPIC_AUTH_TOKEN",
    "ANTHROPIC_API_KEY",
    "CLAUDE_CODE_OAUTH_TOKEN",
)
_CLOUDROUTER_CODEX_AUTH_ENV_KEYS = (
    "OPENAI_API_KEY",
    "CODEX_API_KEY",
    "CLOUDROUTER_API_KEY",
    "APEX_CODEX_GATEWAY_KEY",
    "APEX_CODEX_API_KEY",
    "APEXROUTER_API_KEY",
    "APEXROUTER_CODEX_API_KEY",
)
# Exact handles remain reachable when cleanup cannot prove a process tree
# terminal.  Evaluators are otherwise short-lived local objects, so swallowing
# a reap failure would make the surviving child completely invisible.
_UNREAPED_GOAL_EVALUATOR_PROCESSES: dict[
    int, asyncio.subprocess.Process
] = {}
# Task ownership is kept separately to preserve the exact-handle registry's
# existing shape for shutdown/reap callers.  Keys are Process identity tokens,
# not PIDs: a cleanup-failed process group must not be overwritten even if its
# reaped leader's numeric PID is later reused.  Entries exist from spawn until
# terminal proof, so task deletion can fail closed for both an active evaluator
# and one retained after cleanup failure.
_GOAL_EVALUATOR_TASK_IDS: dict[int, int | None] = {}


@dataclass(frozen=True)
class _GoalEvaluatorRuntimeRoute:
    """Exact credential route used by one spawned evaluator process."""

    provider: str
    provider_home: str | None
    task_id: int | None


# Kept alongside the exact process handles above under the same Process
# identity token.  A route is registered in the same synchronous section that
# receives the spawned Process and remains present until that exact process
# group has been proven terminal.
_GOAL_EVALUATOR_RUNTIME_ROUTES: dict[
    int, _GoalEvaluatorRuntimeRoute
] = {}
# Concurrent request cleanup and shutdown reaping must share one exact cleanup
# operation.  Active entries hold the Process strongly only until that cleanup
# settles.  Successful terminal proof is then retained as a weak exact-object
# marker so a late caller holding an old registry snapshot cannot re-signal a
# numeric PID/PGID that may already have been reused.
_GOAL_EVALUATOR_PROCESS_CLEANUPS: dict[
    int,
    tuple[asyncio.subprocess.Process, asyncio.Task[None]],
] = {}
_TERMINAL_GOAL_EVALUATOR_PROCESSES: dict[
    int,
    weakref.ReferenceType[asyncio.subprocess.Process],
] = {}


@dataclass(frozen=True)
class _CodexGoalEvaluatorTurn:
    """Exact app-server turn retained until terminal state is proven."""

    registry: object
    codex_home: str
    process: object
    thread_id: str
    task_id: int | None


# ``CodexTurnProcess.pid`` is the shared app-server PID and therefore cannot be
# used as a unique key when several evaluator turns share one account process.
_UNREAPED_CODEX_GOAL_EVALUATOR_TURNS: dict[
    int, _CodexGoalEvaluatorTurn
] = {}


class GoalEvalResult:
    __slots__ = ("achieved", "reason")

    def __init__(self, achieved: bool, reason: str):
        self.achieved = achieved
        self.reason = reason


class GoalEvaluationError(RuntimeError):
    """Operational evaluator failure with output preserved for classification."""

    __slots__ = ("provider", "returncode", "stdout", "stderr")

    def __init__(
        self,
        message: str,
        *,
        provider: str,
        returncode: int | None = None,
        stdout: str = "",
        stderr: str = "",
    ):
        detail = stderr.strip() or stdout.strip()
        suffix = f": {detail[:1000]}" if detail else ""
        super().__init__(f"{message}{suffix}")
        self.provider = provider
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr

    @property
    def combined_output(self) -> str:
        """Text consumed by the provider's usage/auth failure classifier."""
        parts = [part.strip() for part in (self.stderr, self.stdout) if part.strip()]
        return "\n".join(parts) or str(self)


class GoalEvaluatorCleanupError(RuntimeError):
    """A goal-evaluator process tree could not be proven terminal."""


def _canonical_provider_home(
    home: str | os.PathLike[str] | None,
) -> str | None:
    """Canonicalize one provider home for exact, symlink-safe comparison."""

    if home is None:
        return None
    raw_home = os.fspath(home)
    if not isinstance(raw_home, str) or not raw_home.strip():
        return None
    expanded = os.path.expandvars(os.path.expanduser(raw_home))
    return os.path.normcase(os.path.realpath(os.path.abspath(expanded)))


def _register_goal_evaluator_process(
    process: asyncio.subprocess.Process,
    *,
    provider: str,
    provider_home: str | None,
    task_id: int | None,
) -> None:
    """Synchronously retain a spawned evaluator and its exact runtime route."""

    pid = getattr(process, "pid", None)
    if not isinstance(pid, int) or pid <= 0:
        return
    process_token = id(process)
    _UNREAPED_GOAL_EVALUATOR_PROCESSES[process_token] = process
    _GOAL_EVALUATOR_TASK_IDS[process_token] = task_id
    _GOAL_EVALUATOR_RUNTIME_ROUTES[process_token] = _GoalEvaluatorRuntimeRoute(
        provider=provider,
        provider_home=_canonical_provider_home(provider_home),
        task_id=task_id,
    )


def _goal_evaluator_process_is_retained(
    process: asyncio.subprocess.Process | None,
) -> bool:
    """Whether the exact process still lacks terminal cleanup proof."""

    return (
        process is not None
        and _UNREAPED_GOAL_EVALUATOR_PROCESSES.get(id(process)) is process
    )


def _goal_evaluator_process_is_terminal(
    process: asyncio.subprocess.Process,
) -> bool:
    """Whether cleanup already proved this exact Process tree terminal."""

    process_token = id(process)
    retained_ref = _TERMINAL_GOAL_EVALUATOR_PROCESSES.get(process_token)
    if retained_ref is None:
        return False
    retained_process = retained_ref()
    if retained_process is process:
        return True
    if _TERMINAL_GOAL_EVALUATOR_PROCESSES.get(process_token) is retained_ref:
        _TERMINAL_GOAL_EVALUATOR_PROCESSES.pop(process_token, None)
    return False


def _mark_goal_evaluator_process_terminal(
    process: asyncio.subprocess.Process,
) -> None:
    """Keep weak terminal proof while any stale caller still holds Process."""

    process_token = id(process)

    def forget(
        process_ref: weakref.ReferenceType[asyncio.subprocess.Process],
    ) -> None:
        if (
            _TERMINAL_GOAL_EVALUATOR_PROCESSES.get(process_token)
            is process_ref
        ):
            _TERMINAL_GOAL_EVALUATOR_PROCESSES.pop(process_token, None)

    _TERMINAL_GOAL_EVALUATOR_PROCESSES[process_token] = weakref.ref(
        process,
        forget,
    )


def goal_evaluator_runtime_users(
    provider: str,
    home: str | os.PathLike[str],
) -> list[str]:
    """Return active/retained evaluators using exactly ``provider`` + ``home``.

    This is intentionally a read-only snapshot for account-retirement fencing.
    Standard Claude/Codex subprocesses and Codex Fast app-server turns are both
    included.  Entries remain visible after cleanup failure and disappear only
    once the corresponding exact process/turn has terminal proof.
    """

    normalized_provider = (provider or "").strip().lower()
    canonical_home = _canonical_provider_home(home)
    if normalized_provider not in {"claude", "codex"} or canonical_home is None:
        return []

    users: set[str] = set()
    for process_token, process in list(
        _UNREAPED_GOAL_EVALUATOR_PROCESSES.items()
    ):
        route = _GOAL_EVALUATOR_RUNTIME_ROUTES.get(process_token)
        if (
            route is None
            or route.provider != normalized_provider
            or route.provider_home != canonical_home
            or _UNREAPED_GOAL_EVALUATOR_PROCESSES.get(process_token)
            is not process
        ):
            continue
        pid = getattr(process, "pid", None)
        task = str(route.task_id) if route.task_id is not None else "unbound"
        users.add(
            f"goal-evaluator:{normalized_provider}:task={task}:pid={pid}"
        )

    if normalized_provider == "codex":
        for retained in list(
            _UNREAPED_CODEX_GOAL_EVALUATOR_TURNS.values()
        ):
            if _canonical_provider_home(retained.codex_home) != canonical_home:
                continue
            task = (
                str(retained.task_id)
                if retained.task_id is not None
                else "unbound"
            )
            users.add(
                "goal-evaluator:codex:"
                f"task={task}:thread={retained.thread_id}"
            )
    return sorted(users)


def codex_goal_evaluator_runtime_homes() -> set[str]:
    """Return canonical homes owned by external Codex evaluator processes.

    The structured snapshot is consumed by Codex home admission.  In
    particular, it remains populated when evaluator cleanup fails after the
    caller's ephemeral-home guard has unwound.  Codex Fast turns are excluded:
    they remain owned by the app-server transport and its own admission path.
    """

    homes: set[str] = set()
    for process_token, process in list(
        _UNREAPED_GOAL_EVALUATOR_PROCESSES.items()
    ):
        route = _GOAL_EVALUATOR_RUNTIME_ROUTES.get(process_token)
        if (
            route is not None
            and route.provider == "codex"
            and route.provider_home is not None
            and _UNREAPED_GOAL_EVALUATOR_PROCESSES.get(process_token)
            is process
        ):
            homes.add(route.provider_home)
    return homes


def _managed_process_group_pid(
    process: asyncio.subprocess.Process | None,
    managed_process_group: bool,
) -> int | None:
    """Return the exact POSIX process-group id created for this evaluator."""

    if not managed_process_group or process is None:
        return None
    return require_safe_process_group_id(
        getattr(process, "pid", None),
        context="goal evaluator",
    )


def _process_group_alive(process_group_id: int | None) -> bool:
    """Conservatively report whether an exact POSIX process group remains."""

    if process_group_id is None:
        return False
    process_group_id = require_safe_process_group_id(
        process_group_id,
        context="goal evaluator liveness check",
    )
    try:
        os.killpg(process_group_id, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


async def _settle_process_spawn(
    *cmd: str,
    **spawn_kwargs,
) -> tuple[asyncio.subprocess.Process, asyncio.CancelledError | None]:
    """Return the exact spawned process even across caller cancellation."""

    spawn_task, delayed_cancellation = await settle_awaitable(
        asyncio.create_subprocess_exec(*cmd, **spawn_kwargs)
    )

    try:
        process = spawn_task.result()
    except BaseException:
        if delayed_cancellation is not None:
            raise delayed_cancellation
        raise
    return process, delayed_cancellation


async def _terminate_process(
    process: asyncio.subprocess.Process | None,
    communicate_task: asyncio.Task[tuple[bytes, bytes]] | None,
    *,
    managed_process_group: bool,
) -> None:
    """Kill and reap one evaluator process tree without leaving pipe readers."""

    if process is None:
        if communicate_task is not None and not communicate_task.done():
            communicate_task.cancel()
            await asyncio.gather(communicate_task, return_exceptions=True)
        return

    process_group_id = _managed_process_group_pid(
        process, managed_process_group
    )
    try:
        if process_group_id is not None:
            os.killpg(process_group_id, signal.SIGKILL)
        elif process.returncode is None:
            process.kill()
    except ProcessLookupError:
        # A just-exited group may race this signal.  If the parent itself is
        # still reported alive, retain the portable single-process fallback.
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
    except Exception:
        logger.exception("Failed to stop goal evaluator process")

    loop = asyncio.get_running_loop()
    deadline = loop.time() + _PROCESS_CLEANUP_TIMEOUT
    parent_reaped = process.returncode is not None

    # evaluate() keeps communicate() alive behind a shield.  Awaiting that
    # exact task drains both PIPEs while it also waits for the killed parent.
    # This matters when a child inherited either descriptor.
    if communicate_task is not None:
        try:
            await asyncio.wait_for(
                asyncio.shield(communicate_task),
                timeout=max(0.01, deadline - loop.time()),
            )
            # asyncio.subprocess.Process.communicate() includes wait().
            parent_reaped = True
        except asyncio.TimeoutError:
            logger.error("Timed out draining goal evaluator output")
            communicate_task.cancel()
            await asyncio.gather(communicate_task, return_exceptions=True)
        except Exception:
            # The original communicate failure is reported by evaluate(); the
            # remaining responsibility here is to reap the process.
            pass

    try:
        if process.returncode is None:
            await asyncio.wait_for(
                asyncio.shield(process.wait()),
                timeout=max(0.01, deadline - loop.time()),
            )
            parent_reaped = True
    except asyncio.TimeoutError:
        logger.error("Timed out reaping goal evaluator process")
    except Exception:
        logger.exception("Failed to reap goal evaluator process")

    while _process_group_alive(process_group_id):
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise RuntimeError(
                f"Goal evaluator process group {process_group_id} survived SIGKILL"
            )
        await asyncio.sleep(min(0.05, remaining))
    if not parent_reaped:
        raise RuntimeError(
            f"Goal evaluator process {getattr(process, 'pid', None)} "
            "could not be reaped"
        )


async def _terminate_process_shielded(
    process: asyncio.subprocess.Process | None,
    communicate_task: asyncio.Task[tuple[bytes, bytes]] | None,
    *,
    managed_process_group: bool,
    delayed_cancellation: asyncio.CancelledError | None = None,
) -> None:
    """Finish process cleanup before delivering caller cancellation."""

    if process is not None and _goal_evaluator_process_is_terminal(process):
        if delayed_cancellation is not None:
            raise delayed_cancellation
        return

    process_token = id(process) if process is not None else None
    cleanup: asyncio.Task[None]
    cleanup_entry = (
        _GOAL_EVALUATOR_PROCESS_CLEANUPS.get(process_token)
        if process_token is not None
        else None
    )
    if cleanup_entry is not None and cleanup_entry[0] is process:
        cleanup = cleanup_entry[1]
    else:
        cleanup = asyncio.create_task(
            _terminate_process(
                process,
                communicate_task,
                managed_process_group=managed_process_group,
            )
        )
        if process is not None and process_token is not None:
            _GOAL_EVALUATOR_PROCESS_CLEANUPS[process_token] = (
                process,
                cleanup,
            )

    cancellation = delayed_cancellation
    later_cancellation = await await_task_completion(cleanup)
    cancellation = cancellation or later_cancellation

    try:
        try:
            cleanup.result()
        except Exception as exc:
            pid = getattr(process, "pid", None)
            if isinstance(pid, int) and pid > 0 and process is not None:
                _UNREAPED_GOAL_EVALUATOR_PROCESSES[id(process)] = process
            logger.exception("Goal evaluator cleanup failed")
            raise GoalEvaluatorCleanupError(
                f"Goal evaluator process group {pid} "
                "could not be proven terminal"
            ) from exc
        else:
            pid = getattr(process, "pid", None)
            if process is not None:
                _mark_goal_evaluator_process_terminal(process)
            if (
                isinstance(pid, int)
                and process_token is not None
                and _UNREAPED_GOAL_EVALUATOR_PROCESSES.get(process_token)
                is process
            ):
                _UNREAPED_GOAL_EVALUATOR_PROCESSES.pop(process_token, None)
                _GOAL_EVALUATOR_TASK_IDS.pop(process_token, None)
                _GOAL_EVALUATOR_RUNTIME_ROUTES.pop(process_token, None)
    finally:
        cleanup_entry = (
            _GOAL_EVALUATOR_PROCESS_CLEANUPS.get(process_token)
            if process_token is not None
            else None
        )
        if (
            cleanup_entry is not None
            and cleanup_entry[0] is process
            and cleanup_entry[1] is cleanup
        ):
            _GOAL_EVALUATOR_PROCESS_CLEANUPS.pop(process_token, None)

    if cancellation is not None:
        raise cancellation


async def reap_unreaped_goal_evaluators() -> None:
    """Retry every exact evaluator process retained after cleanup failure."""

    failures: list[str] = []
    for _process_token, process in list(
        _UNREAPED_GOAL_EVALUATOR_PROCESSES.items()
    ):
        pid = getattr(process, "pid", None)
        try:
            await _terminate_process_shielded(
                process,
                None,
                managed_process_group=(os.name == "posix"),
            )
        except Exception as exc:
            failures.append(f"pid {pid}: {exc}")

    for token, retained in list(
        _UNREAPED_CODEX_GOAL_EVALUATOR_TURNS.items()
    ):
        process = retained.process
        transport_removed = False
        try:
            if getattr(process, "returncode", None) is None:
                transport_removed = bool(
                    await retained.registry.abort_unclaimed_turn(
                        retained.codex_home,
                        process,
                        reason=(
                            "CCM shutdown is reaping a retained Codex Fast "
                            "goal evaluator"
                        ),
                    )
                )
            if getattr(process, "returncode", None) is None:
                raise RuntimeError(
                    "Codex Fast goal evaluator remained active after abort"
                )
            if not transport_removed:
                await retained.registry.delete_thread(
                    retained.codex_home,
                    retained.thread_id,
                )
        except Exception as exc:
            failures.append(
                f"Codex evaluator thread {retained.thread_id}: {exc}"
            )
        finally:
            if getattr(process, "returncode", None) is not None:
                _UNREAPED_CODEX_GOAL_EVALUATOR_TURNS.pop(token, None)
    if failures:
        raise GoalEvaluatorCleanupError(
            "Could not reap retained goal evaluator processes: "
            + "; ".join(failures)
        )


def has_unreaped_goal_evaluator_for_task(task_id: int) -> bool:
    """Whether an active/retained evaluator still owns this Task."""

    subprocess_retained = any(
        process_token in _UNREAPED_GOAL_EVALUATOR_PROCESSES
        and owner_task_id == task_id
        for process_token, owner_task_id in _GOAL_EVALUATOR_TASK_IDS.items()
    )
    return subprocess_retained or any(
        retained.task_id == task_id
        for retained in _UNREAPED_CODEX_GOAL_EVALUATOR_TURNS.values()
    )


class GoalEvaluator:
    """Evaluate a goal condition against a conversation transcript."""

    async def evaluate(
        self,
        condition: str,
        conversation_summary: str,
        model: str | None = None,
        provider: str = "claude",
        codex_home: str | None = None,
        task_id: int | None = None,
        config_dir: str | None = None,
        cloudrouter_store=None,
        claude_pool=None,
        codex_service_tier: str = "default",
        codex_app_server_registry=None,
    ) -> GoalEvalResult:
        provider = (provider or "claude").lower()
        try:
            require_task_security_boundary_configured()
            protected_paths = manager_secret_protected_paths()
        except (TaskAgentIsolationError, TaskSSHAccessError) as exc:
            raise GoalEvaluationError(
                "Goal evaluation security admission failed",
                provider=provider,
                stderr=str(exc),
            ) from exc
        if provider == "codex":
            eval_model = model or settings.default_codex_goal_evaluator_model
            codex_service_tier = normalize_codex_service_tier(
                codex_service_tier
            )
        else:
            eval_model = model or settings.default_goal_evaluator_model

        prompt = self._build_eval_prompt(condition, conversation_summary)

        env = scrub_task_model_environment(os.environ, provider=provider)
        # ``codex_home`` is retained as a compatibility name because the
        # dispatcher historically passes the active provider's config_dir
        # through that argument for both providers.
        provider_home = config_dir or codex_home
        if provider_home:
            provider_home = os.path.expandvars(os.path.expanduser(provider_home))
            if provider == "codex":
                env["CODEX_HOME"] = provider_home
            else:
                env["CLAUDE_CONFIG_DIR"] = provider_home

        cloudrouter_api = bool(
            provider_home
            and self._is_cloudrouter_projection(
                cloudrouter_store, provider, provider_home,
            )
        )
        if cloudrouter_api and provider == "claude":
            try:
                inject_cloudrouter_claude_direct_auth(
                    env,
                    cloudrouter_store,
                    provider_home,
                )
            except ClaudeAuthProjectionError as exc:
                raise GoalEvaluationError(
                    "Goal evaluation security admission failed",
                    provider=provider,
                    stderr=str(exc),
                ) from exc
        elif cloudrouter_api:
            auth_keys = (
                _CLOUDROUTER_CODEX_AUTH_ENV_KEYS
                if provider == "codex"
                else _CLOUDROUTER_CLAUDE_AUTH_ENV_KEYS
            )
            for key in auth_keys:
                env.pop(key, None)

        if provider == "codex":
            if codex_app_server_registry is None or not provider_home:
                raise GoalEvaluationError(
                    "Codex goal evaluation requires an exact app-server "
                    "account route before execution",
                    provider=provider,
                )
            return await self._evaluate_codex_fast(
                prompt=prompt,
                model=eval_model,
                codex_home=provider_home,
                task_id=task_id,
                registry=codex_app_server_registry,
                codex_service_tier=codex_service_tier,
            )

        projection_identifier = (
            task_id
            if isinstance(task_id, int) and task_id > 0
            else max(1, os.getpid())
        )
        projection_binding = (
            f"goal-evaluator:{projection_identifier}:{time.monotonic_ns()}"
        )
        try:
            if (
                not cloudrouter_api
                and not environment_has_direct_claude_auth(env)
                and claude_pool is not None
            ):
                refreshed = await claude_pool.ensure_oauth_access_token(
                    provider_home,
                    minimum_remaining_seconds=300.0,
                )
                if not refreshed:
                    raise ClaudeAuthProjectionError(
                        "Selected Goal evaluator Claude account cannot refresh "
                        "a bounded access token"
                    )
            auth_projection = prepare_claude_auth_projection(
                provider_home,
                namespace="goal-evaluator",
                identifier=projection_identifier,
                binding=projection_binding,
                environment=env,
            )
            apply_claude_auth_projection(env, auth_projection)
        except ClaudeAuthProjectionError as exc:
            raise GoalEvaluationError(
                "Goal evaluation security admission failed",
                provider=provider,
                stderr=str(exc),
            ) from exc
        evaluator_cwd = os.path.abspath(os.sep)
        try:
            isolation_settings = generate_claude_zero_tool_isolation_settings(
                "goal-evaluator",
                (
                    task_id
                    if isinstance(task_id, int) and task_id > 0
                    else max(1, os.getpid())
                ),
                protected_paths,
            )
            validate_claude_zero_tool_isolation_settings(
                isolation_settings,
                claude_binary=settings.claude_binary,
            )
        except TaskAgentIsolationError as exc:
            try:
                remove_claude_auth_projection(
                    namespace="goal-evaluator",
                    identifier=projection_identifier,
                    binding=projection_binding,
                )
            except ClaudeAuthProjectionError:
                logger.exception(
                    "Could not roll back Goal evaluator auth projection"
                )
            raise GoalEvaluationError(
                "Goal evaluation security admission failed",
                provider=provider,
                stderr=str(exc),
            ) from exc
        cmd = self._build_eval_command(
            provider,
            prompt,
            eval_model,
            isolation_settings_path=isolation_settings,
        )

        process: asyncio.subprocess.Process | None = None
        process_was_registered = False
        communicate_task: asyncio.Task[tuple[bytes, bytes]] | None = None
        managed_process_group = os.name == "posix"
        spawn_kwargs: dict[str, object] = {
            "stdout": asyncio.subprocess.PIPE,
            "stderr": asyncio.subprocess.PIPE,
            "env": env,
            "cwd": evaluator_cwd,
        }
        if managed_process_group:
            spawn_kwargs["start_new_session"] = True
        try:
            process, spawn_cancellation = await _settle_process_spawn(
                *cmd,
                **spawn_kwargs,
            )
            _register_goal_evaluator_process(
                process,
                provider=provider,
                provider_home=provider_home,
                task_id=task_id,
            )
            process_was_registered = _goal_evaluator_process_is_retained(
                process
            )
            if spawn_cancellation is not None:
                await _terminate_process_shielded(
                    process,
                    communicate_task,
                    managed_process_group=managed_process_group,
                    delayed_cancellation=spawn_cancellation,
                )
            communicate_task = asyncio.create_task(process.communicate())
            stdout, stderr = await asyncio.wait_for(
                asyncio.shield(communicate_task),
                timeout=settings.goal_evaluation_timeout,
            )
            # communicate() proving the CLI parent exited does not prove its
            # dedicated group is empty: a detached tool child can close stdio
            # and continue running.  Always sweep and verify that exact group.
            await _terminate_process_shielded(
                process,
                communicate_task,
                managed_process_group=managed_process_group,
            )
        except asyncio.CancelledError as exc:
            logger.info("Goal evaluation cancelled")
            # Cancellation may have landed while the normal-path shielded
            # cleanup was already running.  That helper waits through the
            # cancellation, removes the exact identity only after terminal
            # proof, and then re-delivers CancelledError.  Do not signal the
            # old numeric PID/PGID a second time after that proof: either may
            # already have been reused by an unrelated process.  A failed
            # cleanup deliberately leaves the exact identity retained, so it
            # remains eligible for this retry.
            if (
                not process_was_registered
                or _goal_evaluator_process_is_retained(process)
            ):
                await _terminate_process_shielded(
                    process,
                    communicate_task,
                    managed_process_group=managed_process_group,
                    delayed_cancellation=exc,
                )
            raise
        except asyncio.TimeoutError as exc:
            logger.warning("Goal evaluation timed out")
            await _terminate_process_shielded(
                process,
                communicate_task,
                managed_process_group=managed_process_group,
            )
            returncode = (
                process.returncode
                if process is not None and isinstance(process.returncode, int)
                else None
            )
            raise GoalEvaluationError(
                "Goal evaluation timed out",
                provider=provider,
                returncode=returncode,
            ) from exc
        except Exception as exc:
            logger.error("Goal evaluation failed: %s", exc)
            await _terminate_process_shielded(
                process,
                communicate_task,
                managed_process_group=managed_process_group,
            )
            returncode = (
                process.returncode
                if process is not None and isinstance(process.returncode, int)
                else None
            )
            raise GoalEvaluationError(
                "Goal evaluation process failed",
                provider=provider,
                returncode=returncode,
                stderr=str(exc),
            ) from exc
        finally:
            if process is None or not _goal_evaluator_process_is_retained(process):
                try:
                    remove_claude_auth_projection(
                        namespace="goal-evaluator",
                        identifier=projection_identifier,
                        binding=projection_binding,
                    )
                except ClaudeAuthProjectionError:
                    logger.exception(
                        "Could not clean Goal evaluator auth projection"
                    )

        raw = stdout.decode("utf-8", errors="replace") if stdout else ""
        stderr_text = stderr.decode("utf-8", errors="replace") if stderr else ""
        returncode = process.returncode if isinstance(process.returncode, int) else 0
        if returncode != 0 or (not raw.strip() and stderr_text.strip()):
            logger.warning(
                "Goal evaluation exited with code %s: %s",
                returncode,
                stderr_text.strip()[:500],
            )
            raise GoalEvaluationError(
                f"Goal evaluation exited with code {returncode}",
                provider=provider,
                returncode=returncode,
                stdout=raw,
                stderr=stderr_text,
            )
        if provider == "codex":
            return self._parse_codex_response(raw)
        return self._parse_response(raw)

    async def _evaluate_codex_fast(
        self,
        *,
        prompt: str,
        model: str,
        codex_home: str,
        task_id: int | None,
        registry,
        codex_service_tier: str,
    ) -> GoalEvalResult:
        """Run a Fast evaluator through the same verified app-server path."""

        process = None
        thread_id: str | None = None
        collect_task: asyncio.Task | None = None
        turn_token: int | None = None
        transport_removed = False

        async def settle_cleanup(awaitable) -> None:
            await finish_awaitable(awaitable)

        async def abort_turn(reason: str) -> None:
            nonlocal transport_removed
            if process is None or process.returncode is not None:
                return
            transport_removed = bool(
                await registry.abort_unclaimed_turn(
                    codex_home,
                    process,
                    reason=reason,
                )
            )

        async def collect_output() -> tuple[bytes, bytes, int]:
            stdout_task = asyncio.create_task(process.stdout.read())
            stderr_task = asyncio.create_task(process.stderr.read())
            wait_task = asyncio.create_task(process.wait())
            try:
                stdout, stderr, returncode = await asyncio.gather(
                    stdout_task,
                    stderr_task,
                    wait_task,
                )
                return stdout, stderr, int(returncode)
            finally:
                for task in (stdout_task, stderr_task, wait_task):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(
                    stdout_task,
                    stderr_task,
                    wait_task,
                    return_exceptions=True,
                )

        async def cancel_collector() -> None:
            if collect_task is None or collect_task.done():
                return
            collect_task.cancel()
            await asyncio.gather(collect_task, return_exceptions=True)

        try:
            process, thread_id = await registry.start_turn(
                codex_home=codex_home,
                prompt=prompt,
                cwd=tempfile.gettempdir(),
                model=model,
                effort=None,
                resume_session_id=None,
                git_env=None,
                task_id=task_id,
                disable_project_config=True,
                disable_user_mcp=True,
                disable_autonomous_features=True,
                sandbox_mode="read-only",
                tools_disabled=True,
                codex_service_tier=codex_service_tier,
            )
            turn_token = id(process)
            _UNREAPED_CODEX_GOAL_EVALUATOR_TURNS[turn_token] = (
                _CodexGoalEvaluatorTurn(
                    registry=registry,
                    codex_home=codex_home,
                    process=process,
                    thread_id=thread_id,
                    task_id=task_id,
                )
            )
            logger.info(
                "Codex tool-free goal evaluator admitted "
                "task=%s thread=%s model=%s tier=%s",
                task_id,
                thread_id,
                model,
                codex_service_tier,
            )
            collect_task = asyncio.create_task(collect_output())
            try:
                stdout, stderr, returncode = await asyncio.wait_for(
                    asyncio.shield(collect_task),
                    timeout=settings.goal_evaluation_timeout,
                )
            except asyncio.TimeoutError as exc:
                await settle_cleanup(abort_turn(
                    "Codex Fast goal evaluation timed out",
                ))
                if collect_task is not None:
                    await settle_cleanup(asyncio.shield(collect_task))
                raise GoalEvaluationError(
                    "Goal evaluation timed out",
                    provider="codex",
                ) from exc
        except asyncio.CancelledError as exc:
            try:
                await settle_cleanup(abort_turn(
                    "Codex Fast goal evaluation was cancelled",
                ))
            except asyncio.CancelledError:
                pass
            except Exception as cleanup_exc:
                logger.exception(
                    "Could not prove cancelled Codex Fast goal evaluator "
                    "terminal"
                )
                try:
                    await settle_cleanup(cancel_collector())
                except asyncio.CancelledError:
                    pass
                raise GoalEvaluatorCleanupError(
                    "Codex Fast goal evaluator could not be proven terminal"
                ) from cleanup_exc
            if collect_task is not None:
                try:
                    await settle_cleanup(asyncio.shield(collect_task))
                except asyncio.CancelledError:
                    pass
            raise exc
        except GoalEvaluationError:
            raise
        except Exception as exc:
            try:
                await settle_cleanup(abort_turn(
                    "Codex Fast goal evaluation failed",
                ))
                if collect_task is not None:
                    await settle_cleanup(asyncio.shield(collect_task))
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Could not clean up failed Codex Fast goal evaluator"
                )
            if (
                collect_task is not None
                and not collect_task.done()
                and process is not None
                and process.returncode is None
            ):
                try:
                    await settle_cleanup(cancel_collector())
                except asyncio.CancelledError:
                    raise
            raise GoalEvaluationError(
                "Goal evaluation app-server turn failed",
                provider="codex",
                returncode=(
                    process.returncode
                    if process is not None
                    and isinstance(process.returncode, int)
                    else None
                ),
                stderr=str(exc),
            ) from exc
        finally:
            if (
                thread_id
                and process is not None
                and process.returncode is not None
                and not transport_removed
            ):
                try:
                    await settle_cleanup(
                        registry.delete_thread(codex_home, thread_id)
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "Could not delete terminal Codex Fast evaluator "
                        "thread %s",
                        thread_id,
                    )
            if (
                turn_token is not None
                and process is not None
                and process.returncode is not None
            ):
                _UNREAPED_CODEX_GOAL_EVALUATOR_TURNS.pop(
                    turn_token,
                    None,
                )

        raw = stdout.decode("utf-8", errors="replace") if stdout else ""
        stderr_text = stderr.decode("utf-8", errors="replace") if stderr else ""
        if returncode != 0 or (not raw.strip() and stderr_text.strip()):
            raise GoalEvaluationError(
                f"Goal evaluation exited with code {returncode}",
                provider="codex",
                returncode=returncode,
                stdout=raw,
                stderr=stderr_text,
            )
        return self._parse_codex_app_server_response(raw)

    def _parse_codex_app_server_response(self, raw: str) -> GoalEvalResult:
        """Extract the evaluator JSON from normalized app-server events."""

        completed_text = ""
        deltas: list[str] = []
        for line in raw.strip().splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(event, dict):
                continue
            if event.get("type") == "item.agent_message.delta":
                delta = event.get("delta")
                if isinstance(delta, str):
                    deltas.append(delta)
            item = event.get("item")
            if (
                event.get("type") == "item.completed"
                and isinstance(item, dict)
                and item.get("type") == "agent_message"
                and isinstance(item.get("text"), str)
            ):
                completed_text = item["text"]
        return self._extract_eval_json(
            completed_text or "".join(deltas) or raw
        )

    @staticmethod
    def _is_cloudrouter_projection(
        cloudrouter_store,
        provider: str,
        provider_home: str,
    ) -> bool:
        """Identify an API projection by path without reading its API key."""

        if cloudrouter_store is None:
            return False
        finder_name = (
            "account_for_codex_home"
            if provider == "codex"
            else "account_for_claude_config_dir"
        )
        finder = getattr(cloudrouter_store, finder_name, None)
        if not callable(finder):
            return False
        try:
            return finder(provider_home) is not None
        except Exception:
            logger.exception(
                "Could not resolve CloudRouter goal-evaluator home %s",
                provider_home,
            )
            return False

    def _build_eval_command(
        self,
        provider: str,
        prompt: str,
        model: str,
        *,
        isolation_settings_path=None,
    ) -> list[str]:
        if provider != "claude":
            raise ValueError(
                "Codex goal evaluation requires the audited app-server transport"
            )
        return [
            settings.claude_binary,
            "-p", prompt,
            "--output-format", "json",
            "--permission-mode", "acceptEdits",
            "--settings", str(isolation_settings_path),
            "--setting-sources", "",
            "--strict-mcp-config",
            "--mcp-config", '{"mcpServers":{}}',
            "--disable-slash-commands",
            "--no-chrome",
            "--tools", "",
            "--allowedTools", "",
            "--no-session-persistence",
            "--exclude-dynamic-system-prompt-sections",
            "--model", model,
            "--max-turns", "1",
        ]

    def _build_eval_prompt(self, condition: str, conversation_summary: str) -> str:
        return f"""\
You are a goal evaluator. Your ONLY job is to judge whether a goal condition
has been met based on the conversation transcript below.

## Goal Condition
{condition}

## Conversation Transcript (most recent work)
{conversation_summary}

## Instructions
Based on the transcript, determine if the goal condition has been fully achieved.
You must respond with EXACTLY one JSON object (no other text):

If achieved:
{{"achieved": true, "reason": "brief explanation of why the condition is met"}}

If NOT achieved:
{{"achieved": false, "reason": "brief explanation of what still needs to be done"}}

Respond with ONLY the JSON object, nothing else."""

    def _parse_codex_response(self, raw: str) -> GoalEvalResult:
        """Parse Codex JSONL output — extract the last agent_message text."""
        text = ""
        for line in raw.strip().splitlines():
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue
            item = data.get("item") if isinstance(data.get("item"), dict) else {}
            if item.get("type") == "agent_message" and item.get("text"):
                text = item["text"]
        if not text:
            text = raw
        return self._extract_eval_json(text)

    def _parse_response(self, raw: str) -> GoalEvalResult:
        text = raw.strip()

        # claude --output-format json wraps the response in a JSON envelope
        try:
            envelope = json.loads(text)
            if isinstance(envelope, dict) and "result" in envelope:
                text = envelope["result"]
            elif isinstance(envelope, dict) and "content" in envelope:
                text = envelope["content"]
        except (json.JSONDecodeError, TypeError):
            pass

        return self._extract_eval_json(text)

    def _extract_eval_json(self, text) -> GoalEvalResult:
        """Extract {achieved, reason} JSON from text (may be wrapped in markdown)."""
        if isinstance(text, str):
            cleaned = text.strip()
            if cleaned.startswith("```"):
                lines = cleaned.split("\n")
                lines = [l for l in lines if not l.strip().startswith("```")]
                cleaned = "\n".join(lines).strip()

            try:
                data = json.loads(cleaned)
                if (
                    isinstance(data, dict)
                    and isinstance(data.get("achieved"), bool)
                ):
                    reason = data.get("reason", "")
                    if not isinstance(reason, str):
                        reason = str(reason)
                    return GoalEvalResult(
                        achieved=data["achieved"],
                        reason=reason,
                    )
            except (json.JSONDecodeError, TypeError):
                pass

        logger.warning(f"Could not parse evaluator response: {str(text)[:200]}")
        return GoalEvalResult(
            achieved=False,
            reason="Could not parse evaluator response",
        )
