import asyncio
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from sqlalchemy import select

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from backend.config import settings
from backend.database import init_db, async_session
from backend.api.tasks import router as tasks_router
from backend.api.instances import router as instances_router, dispatcher_router
from backend.api.system import router as system_router
from backend.api.ws import router as ws_router
from backend.api.voice import router as voice_router
from backend.api.auth import router as auth_router
from backend.api.chat import router as chat_router
from backend.api.projects import router as projects_router
from backend.api.project_todos import router as project_todos_router
from backend.api.settings import router as settings_router
from backend.api.uploads import router as uploads_router
from backend.api.secrets import router as secrets_router
from backend.api.tags import router as tags_router
from backend.api.files import router as files_router
from backend.api.task_artifacts import router as task_artifacts_router
from backend.api.pool import router as pool_router
from backend.api.codex_pool import (
    recover_pending_codex_login_transactions,
    router as codex_pool_router,
)
from backend.api.cloudrouter_accounts import router as cloudrouter_accounts_router
from backend.api.monitor import router as monitor_router
from backend.api.sub_agents import router as sub_agents_router
from backend.api.sub_agent_tasks import router as sub_agent_tasks_router
from backend.api.discussions import router as discussions_router
from backend.api.quick_phrases import router as quick_phrases_router
from backend.api.pr_monitor import router as pr_monitor_router, webhook_router as pr_webhook_router
from backend.api.workers import router as workers_router
from backend.api.feishu import router as feishu_router
from backend.api.org import router as org_router
from backend.api.ask_user import router as ask_user_router
from backend.api.user_skills import router as user_skills_router
from backend.api.team_sharing import router as team_sharing_router
from backend.api.browser_reviews import (
    router as browser_reviews_router,
    task_router as task_browser_reviews_router,
)
from backend.api.workspace_reviews import router as workspace_reviews_router
from backend.api.test_harness import router as test_harness_router
from backend.api.plans import router as plans_router
from backend.api.plan_resources import router as plan_resources_router
from backend.middleware.auth import TokenAuthMiddleware
from backend.services.ws_broadcaster import WebSocketBroadcaster
from backend.services.instance_manager import InstanceManager
from backend.services.ralph_loop import RalphLoop
from backend.services.dispatcher import GlobalDispatcher
from backend.services.update_service import UpdateService
from backend.services.deployment_start_guard import (
    StartDecision,
    assess_deployment_start,
    deployment_task_start_fence,
)
from backend.services.sub_agent_watcher import SubAgentWatcher
from backend.services.cloudrouter_accounts import CloudRouterAccountStore
from backend.services.browser_review_jobs import browser_review_job_manager

# Logging: surface INFO from our services AND claude_pty in the server log.
# Without this, PTY delivery/turn diagnostics are invisible (learned the
# hard way while debugging silent message loss).
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

# Global singletons
logger = logging.getLogger(__name__)
broadcaster = WebSocketBroadcaster()
broadcaster.db_factory = async_session
instance_manager = InstanceManager(db_factory=async_session, broadcaster=broadcaster)
cloudrouter_store = CloudRouterAccountStore(settings.cloudrouter_accounts_dir)
instance_manager.cloudrouter_store = cloudrouter_store

shared_relay = None
ralph_loop = RalphLoop(
    db_factory=async_session,
    instance_manager=instance_manager,
    broadcaster=broadcaster,
)
dispatcher = GlobalDispatcher(
    db_factory=async_session,
    instance_manager=instance_manager,
    broadcaster=broadcaster,
)
dispatcher.cloudrouter_store = cloudrouter_store
instance_manager.task_message_enqueuer = dispatcher.enqueue_message

sub_agent_watcher = SubAgentWatcher(db_factory=async_session, broadcaster=broadcaster)

# Codex account pool (optional, CODEX_POOL_ENABLED=true)
codex_pool = None
try:
    # Recovery is independent of the runtime pool toggle. A service may be
    # restarted with CODEX_POOL_ENABLED=false while an earlier enabled process
    # left a transaction journal; default-home Codex must not observe it first.
    recovery = recover_pending_codex_login_transactions(
        settings.codex_pool_config_path
    )
except Exception:
    # Fail startup instead of falling back to an inherited default CODEX_HOME
    # that may contain a half-written auth transaction.
    logger.critical(
        "Codex login recovery could not safely restore or isolate credentials",
        exc_info=True,
    )
    raise
if recovery["recovered"] or recovery["quarantined"]:
    logger.warning(
        "Recovered pending Codex login transactions before pool init: %s",
        recovery,
    )

_has_cloudrouter_codex_accounts = any(
    account.cleanup_pending
    or (
        account.enabled
        and not account.retired
        and account.supports_model("codex", None)
    )
    for account in cloudrouter_store.all_accounts(include_retired=True)
)
if settings.codex_pool_enabled or _has_cloudrouter_codex_accounts:
    try:
        from backend.services.codex_pool import CodexPool
        codex_pool = CodexPool(
            config_path=settings.codex_pool_config_path,
            cooldown_seconds=settings.codex_pool_cooldown_seconds,
            quota_reader=instance_manager.read_codex_rate_limits,
            cloudrouter_store=cloudrouter_store,
            bootstrap_default=settings.codex_pool_enabled,
            include_native=settings.codex_pool_enabled,
        )
        dispatcher.codex_pool = codex_pool
        logger.info("Codex pool enabled with %d accounts", len(codex_pool._accounts))
    except Exception:
        logger.exception("Codex pool init failed — codex pool disabled")

_update_project_dir = str(Path(__file__).resolve().parent.parent)
_update_runtime_root = None
_legacy_update_runtime_root = "/tmp"
if os.environ.get("CCM_TESTING") == "1":
    _test_project_dir = os.environ.get("CCM_TEST_PROJECT_DIR", "").strip()
    if not _test_project_dir:
        raise RuntimeError(
            "CCM_TESTING requires an isolated CCM_TEST_PROJECT_DIR"
        )
    _update_project_dir = _test_project_dir
    # Importing backend.main in tests must never scan host /tmp or write the
    # service user's real cache, even if a test supplies a helper script.
    _update_runtime_root = str(
        Path(_test_project_dir) / ".ccm-update-runtime"
    )
    _legacy_update_runtime_root = None

update_service = UpdateService(
    broadcaster=broadcaster,
    port=settings.port,
    project_dir=_update_project_dir,
    db_factory=async_session,
    dispatcher=dispatcher,
    update_runtime_root=_update_runtime_root,
    legacy_update_runtime_root=_legacy_update_runtime_root,
)
dispatcher.deployment_task_start_fence = (
    lambda: deployment_task_start_fence(update_service.project_dir)
)

# 分布式 Worker（可选，WORKER_ENABLED=true 且装了 boto3 才启用）
worker_provisioner = None
worker_relay = None
worker_proxy = None
task_migrator = None
if settings.worker_enabled:
    try:
        from backend.services.cloud_provider import get_cloud_provider
        from backend.services.worker_provisioner import WorkerProvisioner
        from backend.services.worker_relay import WorkerRelay
        from backend.services.worker_proxy import WorkerProxy

        from backend.services.task_migrator import TaskMigrator

        worker_relay = WorkerRelay(db_factory=async_session, broadcaster=broadcaster)
        worker_proxy = WorkerProxy(db_factory=async_session, relay=worker_relay)
        task_migrator = TaskMigrator(
            db_factory=async_session, relay=worker_relay, broadcaster=broadcaster,
        )
        worker_provisioner = WorkerProvisioner(
            db_factory=async_session,
            cloud=get_cloud_provider(settings.worker_cloud_provider),
            broadcaster=broadcaster,
            relay=worker_relay,
        )
    except Exception:
        logger.exception("Worker provisioner init failed — workers disabled")


async def _sync_tags():
    """Ensure all project tags have corresponding Tag records."""
    from sqlalchemy import select
    from backend.models.project import Project
    from backend.models.tag import Tag
    async with async_session() as db:
        result = await db.execute(select(Project.tags))
        all_tag_names: set[str] = set()
        for (tags,) in result:
            if tags:
                all_tag_names.update(tags)
        if not all_tag_names:
            return
        existing = await db.execute(select(Tag.name))
        existing_names = {row[0] for row in existing}
        for name in all_tag_names - existing_names:
            db.add(Tag(name=name))
        await db.commit()


async def _reset_stale_discussion_agents():
    from backend.models.discussion import DiscussionAgent
    async with async_session() as db:
        result = await db.execute(
            select(DiscussionAgent).where(DiscussionAgent.status == "running")
        )
        stale = result.scalars().all()
        for agent in stale:
            agent.status = "idle"
            agent.pid = None
        if stale:
            await db.commit()
            logger.info("Reset %d stale discussion agents to idle", len(stale))


async def _cleanup_stale_sub_agents():
    """Close terminal parents' stale children, except owned async generations.

    A foreground Task may already be terminal while its exact persistent-PTY
    generation still owns native child output.  The dispatcher startup
    recovery is authoritative for an orphaned background marker and will fail
    both the parent and its native children; pre-emptively calling those rows
    completed here would erase that failure evidence.

    Local CCM Monitors are durable schedulers whose parent normally finishes
    between checks.  The dispatcher must likewise see their persisted
    generation: a sleeping generation is rehydrated, while an active generation
    from an unclean shutdown is failed closed.  Remote mirrors and ordinary
    one-shot children remain subject to this terminal-parent cleanup.
    """
    from backend.models.sub_agent import SubAgentSession
    from backend.models.task import Task
    from datetime import datetime
    async with async_session() as db:
        result = await db.execute(
            select(SubAgentSession).where(SubAgentSession.status == "running")
        )
        stale = []
        for sa in result.scalars().all():
            task = await db.get(Task, sa.task_id)
            if (
                task
                and not (
                    task.pty_background_generation is not None
                    and sa.source == "native"
                )
                and not (
                    sa.source == "ccm"
                    and sa.agent_type == "monitor"
                    and sa.remote_id is None
                )
                and task.status in ("completed", "failed", "cancelled")
            ):
                sa.status = "completed"
                sa.completed_at = datetime.utcnow()
                stale.append(sa)
        if stale:
            await db.commit()
            logger.info("Cleaned up %d stale sub-agents from completed tasks", len(stale))


async def _ensure_claude_warmup():
    """Ensure all Claude config dirs have completed onboarding.

    Fresh CC installs show interactive onboarding dialogs (theme picker, trust
    directory, etc.) that block PTY mode — the MCP/channel server never starts,
    so inject gets ConnectionRefused.  Running a quick `claude -p` in each
    config_dir completes the onboarding and writes hasCompletedOnboarding into
    .claude.json.  This is the same idea as worker_provisioner._step_claude_warmup
    but for the local machine at startup.
    """
    from pathlib import Path
    import json as _json
    import subprocess

    config_dirs: list[str] = []
    if settings.pool_enabled:
        try:
            from backend.services.claude_pool import ClaudePool
            pool = ClaudePool(
                config_path=settings.pool_config_path,
                cooldown_seconds=settings.pool_cooldown_seconds,
                cloudrouter_store=cloudrouter_store,
                include_native=settings.pool_enabled,
            )
            for acct in pool._accounts:
                if acct.enabled:
                    config_dirs.append(acct.config_dir)
        except Exception:
            logger.debug("Could not load pool for warmup, using default config dir")
    if not config_dirs:
        config_dirs.append(str(Path.home() / ".claude"))

    for config_dir in config_dirs:
        try:
            claude_json_path = Path(config_dir) / ".claude.json"
            needs_warmup = True
            existing = {}
            if claude_json_path.exists():
                try:
                    existing = _json.loads(claude_json_path.read_text(encoding="utf-8"))
                    if existing.get("hasCompletedOnboarding"):
                        needs_warmup = False
                except Exception:
                    pass

            if not needs_warmup:
                continue

            logger.info("Claude warmup needed for %s, running claude -p ...", config_dir)

            env = os.environ.copy()
            env["CLAUDE_CONFIG_DIR"] = config_dir
            env.pop("CLAUDECODE", None)
            env.pop("CLAUDE_CODE", None)
            try:
                subprocess.run(
                    ["claude", "-p", "reply ok", "--dangerously-skip-permissions"],
                    env=env, capture_output=True, text=True, timeout=30,
                )
            except Exception as exc:
                logger.warning("claude -p warmup failed for %s: %s", config_dir, exc)

            # Ensure hasCompletedOnboarding is set regardless of -p result
            try:
                if claude_json_path.exists():
                    existing = _json.loads(claude_json_path.read_text(encoding="utf-8"))
                existing["hasCompletedOnboarding"] = True
                claude_json_path.parent.mkdir(parents=True, exist_ok=True)
                claude_json_path.write_text(
                    _json.dumps(existing, indent=2, ensure_ascii=False),
                    encoding="utf-8",
                )
                logger.info("Claude warmup completed for %s", config_dir)
            except Exception as exc:
                logger.warning("Failed to write .claude.json for %s: %s", config_dir, exc)

        except Exception:
            logger.warning("Claude warmup failed for %s", config_dir, exc_info=True)


async def _recover_worker_relays():
    """Manager 重启后为 ready worker 上的活跃 task 重建中继 + 补缺失日志。"""
    from backend.models.worker import Worker
    try:
        async with async_session() as db:
            result = await db.execute(
                select(Worker).where(Worker.status == "ready")
            )
            workers = result.scalars().all()
        for w in workers:
            try:
                await worker_relay.recover(w)
            except Exception:
                logger.exception("recover relay for worker %s failed", w.id)
    except Exception:
        logger.exception("worker relay recovery failed")


async def _recover_stale_worker_lifecycles():
    """Make process-owned Worker transitions recoverable after a restart.

    Lifecycle work runs in fire-and-forget tasks and cannot survive process
    exit.  Leaving these rows in a busy state would hide retry/destroy actions
    forever; move them to ``error`` while preserving instance ids and account
    credentials for an idempotent operator retry.
    """
    from backend.models.worker import Worker

    stale_statuses = (
        "creating", "bootstrapping", "starting", "stopping", "destroying",
    )
    async with async_session() as db:
        result = await db.execute(
            select(Worker).where(Worker.status.in_(stale_statuses))
        )
        stale = result.scalars().all()
        for worker in stale:
            previous = worker.status
            worker.status = "error"
            worker.bootstrap_step = (
                "destroy" if previous == "destroying" else "startup-recovery"
            )
            worker.bootstrap_error = (
                f"Manager restarted while Worker was {previous}; "
                "the interrupted lifecycle operation must be retried"
            )
        if stale:
            await db.commit()
            logger.warning(
                "Recovered %d interrupted Worker lifecycle operation(s) to error",
                len(stale),
            )


async def _shutdown_runtime_services(
    *,
    heartbeat_task,
    worker_health_task,
    upload_cleanup_task,
    tmp_cleanup_task,
    backup_svc,
    pr_review_recovery_task=None,
) -> None:
    """Run every shutdown stage and re-raise the first teardown failure."""

    failures: list[BaseException] = []
    dispatcher_failure: BaseException | None = None

    # Periodic producers are real asyncio tasks.  Cancelling without awaiting
    # leaves their final DB/network operation racing the teardown below and
    # produces "Task was destroyed but it is pending" at loop close.
    background_tasks = [
        task
        for task in (
            heartbeat_task,
            worker_health_task,
            upload_cleanup_task,
            tmp_cleanup_task,
            pr_review_recovery_task,
        )
        if task is not None
    ]
    for task in background_tasks:
        task.cancel()

    # TmpSpaceManager runs filesystem deletion in a worker thread. Cancelling
    # its asyncio wrapper deliberately waits for that in-flight rename/unlink
    # to settle, so teardown must not apply the generic 10-second producer
    # timeout and then continue while deletion is still running.
    tmp_cleanup_future = (
        tmp_cleanup_task
        if isinstance(tmp_cleanup_task, asyncio.Future)
        else None
    )
    if tmp_cleanup_future is not None:
        try:
            await asyncio.gather(
                tmp_cleanup_future,
                return_exceptions=True,
            )
        except BaseException as exc:
            failures.append(exc)
            logger.exception("Temporary-space watchdog shutdown failed")

    async_tasks = {
        task
        for task in background_tasks
        if isinstance(task, asyncio.Future) and task is not tmp_cleanup_future
    }
    if async_tasks:
        try:
            done, pending = await asyncio.wait(async_tasks, timeout=10.0)
            if done:
                await asyncio.gather(*done, return_exceptions=True)
            if pending:
                failures.append(
                    RuntimeError(
                        "Background task(s) ignored shutdown cancellation"
                    )
                )
                logger.error(
                    "%d background task(s) ignored shutdown cancellation",
                    len(pending),
                )
        except BaseException as exc:
            failures.append(exc)
            logger.exception("Background task shutdown failed")

    try:
        from backend.services.test_harness import test_harness_service

        await test_harness_service.shutdown()
    except BaseException as exc:
        failures.append(exc)
        logger.exception("Test harness watcher shutdown failed")

    try:
        from backend.services.workspace_review import workspace_review_manager

        await workspace_review_manager.shutdown()
    except BaseException as exc:
        failures.append(exc)
        logger.exception("Workspace review preview shutdown failed")

    try:
        await browser_review_job_manager.shutdown()
    except BaseException as exc:
        failures.append(exc)
        logger.exception("Browser review job shutdown failed")

    # Discussion agents are independent subprocess trees and are not owned by
    # Dispatcher/InstanceManager.  Stop them explicitly before the remaining
    # process supervisors are dismantled.
    try:
        from backend.api import discussions as discussions_api

        discussion_svc = getattr(
            discussions_api,
            "_discussion_service",
            None,
        )
        if discussion_svc is not None:
            await discussion_svc.shutdown()
    except BaseException as exc:
        failures.append(exc)
        logger.exception("Discussion service shutdown failed")

    # Legacy Ralph loops are independent dequeue producers. Stop them before
    # Dispatcher takes its final InstanceManager generation snapshot.
    try:
        await ralph_loop.shutdown()
    except BaseException as exc:
        failures.append(exc)
        logger.exception("Ralph loop shutdown failed")

    # Close every Dispatcher admission path before taking down transports.
    # A failure is retained, but later cleanup must still run: those transports
    # may be the only remaining handles capable of reaping child processes.
    try:
        await dispatcher.shutdown()
    except BaseException as exc:
        dispatcher_failure = exc
        if isinstance(exc, asyncio.CancelledError):
            # Cleanup is retried below, but caller cancellation must still be
            # delivered after the exact reapers have settled.
            failures.append(exc)
        logger.exception("Dispatcher shutdown failed")

    if instance_manager._pty_backend is not None:
        try:
            await instance_manager._pty_backend.shutdown()
        except BaseException as exc:
            failures.append(exc)
            logger.exception("PTY backend shutdown failed")

    try:
        await instance_manager.shutdown_codex_app_server()
    except BaseException as exc:
        failures.append(exc)
        logger.exception("Codex app-server shutdown failed")

    if dispatcher_failure is not None:
        # A first pass can time out on a lifecycle that is already inside
        # shielded spawn/reap cleanup. PTY/Codex teardown above may settle the
        # missing exact handle, so retry against the deliberately retained
        # dispatcher maps before declaring shutdown incomplete.
        try:
            await dispatcher.shutdown()
        except BaseException as exc:
            failures.append(exc)
            logger.exception(
                "Dispatcher shutdown retry failed after transport cleanup"
            )
        else:
            logger.info(
                "Dispatcher shutdown retry proved all retained generations "
                "terminal"
            )

    try:
        await sub_agent_watcher.shutdown()
    except BaseException as exc:
        failures.append(exc)
        logger.exception("Sub-agent watcher shutdown failed")
    if backup_svc:
        try:
            backup_svc.stop()
        except BaseException as exc:
            failures.append(exc)
            logger.exception("Backup service shutdown failed")

    if failures:
        # Preserve the original exception type/traceback for systemd/test
        # visibility. Secondary cleanup failures were already logged above.
        raise failures[0]


async def _recover_pending_pr_review_publications() -> bool:
    """Run one bounded recovery pass for incomplete PR-review actions."""

    try:
        from backend.services import pr_review_service

        recover = getattr(
            pr_review_service,
            "recover_incomplete_pr_reviews",
            None,
        )
        if recover is None:
            logger.warning(
                "PR review recovery is unavailable; incomplete actions were "
                "not resumed"
            )
            return False
        await recover(async_session)
        return True
    except Exception:
        # A transient GitHub/API failure must not take the entire CCM runtime
        # offline. The durable publishing rows remain available for a later
        # recovery attempt.
        logger.exception("Incomplete PR review recovery pass failed")
        return False


async def _pr_review_recovery_loop() -> None:
    """Continuously close completion/publication crash windows.

    A healthy pass runs every 30 seconds. Infrastructure failures back off
    exponentially, while per-review publication leases prevent overlapping
    CCM processes from issuing the same GitHub mutation.
    """

    delay = 0.0
    while True:
        if delay:
            await asyncio.sleep(delay)
        try:
            recovered = await _recover_pending_pr_review_publications()
        except asyncio.CancelledError:
            raise
        except Exception:
            # The one-shot helper already isolates ordinary recovery errors;
            # this guard protects the producer itself from an unexpected bug.
            logger.exception("PR review recovery loop failed")
            delay = min(300.0, max(5.0, delay * 2.0))
        else:
            delay = (
                30.0
                if recovered
                else min(300.0, max(5.0, delay * 2.0))
            )



def _prepare_deployment_start() -> StartDecision:
    """Fail closed before startup can perform an implicit DB migration."""

    running_commit = str(
        getattr(update_service, "running_commit", "")
        or getattr(update_service, "_running_commit", "")
    )
    decision = assess_deployment_start(
        update_service.project_dir,
        port=update_service.port,
        running_commit=running_commit,
        status_file=update_service._status_file,
    )
    if decision.blocked:
        raise RuntimeError(
            "Deployment startup guard blocked application startup: "
            f"{decision.reason}"
        )

    # Recover durable deployment state before opening the database. The repair
    # API must describe the operation that caused this process to start, even
    # when /tmp was cleared by a reboot.
    update_service.recover_from_status_file()
    if decision.skip_mutations:
        logger.warning("Startup mutations skipped: %s", decision.reason)
    return decision


@asynccontextmanager
async def _runtime_lifespan(app: FastAPI):
    deployment_start = _prepare_deployment_start()
    update_service.maintenance_only = (
        deployment_start.maintenance_only
    )
    app.state.deployment_maintenance_only = (
        deployment_start.maintenance_only
    )

    # This check is database-independent and also protects the repair/update
    # endpoints used by the maintenance-only process.
    from backend.services.tmp_space_manager import tmp_space_manager
    await tmp_space_manager.ensure_capacity(reason="startup")
    # The module-level UpdateService captures the matching helper before the
    # lifespan starts. Retry materialization after the capacity gate and also
    # support lifespan re-entry from the immutable in-memory capture.
    update_service.ensure_runtime_snapshot()

    if deployment_start.maintenance_only:
        # A failed/partial migration may leave the checked-out application
        # unable to safely query the current schema. Keep the ASGI process
        # alive so an administrator can inspect status and invoke repair,
        # without starting any database-, worker-, or dispatcher-dependent
        # runtime services.
        logger.error(
            "CCM started in deployment maintenance-only mode: %s",
            deployment_start.reason,
        )
        yield
        return

    if not deployment_start.skip_mutations:
        await init_db()
    # Do not seed a shared/default administrator credential.  The registration
    # endpoint promotes the first real user to super_admin, while AUTH_TOKEN
    # remains the bootstrap administrator path for single-token deployments.
    # Build Docker sandbox image if Docker is available (for shared project isolation)
    try:
        from backend.services.container_manager import ContainerManager, build_sandbox_image
        if ContainerManager.is_docker_available():
            asyncio.create_task(build_sandbox_image())
    except Exception:
        logger.debug("Docker not available, container isolation disabled")
    # PTY 权限透传：bridge HTTP 线程需要往主循环调度协程
    instance_manager._loop = asyncio.get_running_loop()
    # Apply persisted runtime-settings override (frontend PTY toggle)
    from backend.models.global_settings import GlobalSettings
    async with async_session() as db:
        row = await db.get(GlobalSettings, 1)
        if row is not None and row.use_pty_mode is not None:
            instance_manager.set_pty_mode(row.use_pty_mode)
    await _reset_stale_discussion_agents()
    await _cleanup_stale_sub_agents()
    # Browser Agent Tasks are ordinary executable Tasks underneath, but their
    # launch authority comes from a durable Harness binding. Reap interrupted
    # bindings before any review projection is failed and, critically, before
    # Dispatcher can claim pending work.
    from backend.services.test_harness_children import test_harness_child_service

    recovered_browser_children = (
        await test_harness_child_service.recover_interrupted()
    )
    if recovered_browser_children:
        logger.warning(
            "Recovered %d interrupted Browser Agent child Task(s)",
            recovered_browser_children,
        )
    from backend.services.workspace_review import workspace_review_manager
    interrupted_workspace_reviews = (
        await workspace_review_manager.recover_interrupted_runs()
    )
    if interrupted_workspace_reviews:
        logger.warning(
            "Marked %d interrupted workspace review run(s) failed",
            interrupted_workspace_reviews,
        )
    from backend.services.test_harness import test_harness_service
    interrupted_test_harness_runs = (
        await test_harness_service.recover_interrupted_runs()
    )
    if interrupted_test_harness_runs:
        logger.warning(
            "Marked %d interrupted test harness run(s) failed",
            interrupted_test_harness_runs,
        )
    await _recover_stale_worker_lifecycles()
    await _sync_tags()
    sub_agent_watcher.start()
    await _ensure_claude_warmup()
    if settings.auto_start_dispatcher:
        await dispatcher.start()
    pr_review_recovery_task = None

    # Worker 健康监控循环 + Manager 重启后恢复所有 relay 连接
    worker_health_task = None
    if worker_provisioner is not None:
        import asyncio as _asyncio
        worker_health_task = _asyncio.create_task(worker_provisioner.health_check_loop())
        _asyncio.create_task(_recover_worker_relays())

    # Recover shared task relays

    # Start periodic database backup (optional — requires BACKUP_ENABLED=true in .env)
    backup_svc = None
    if settings.backup_enabled:
        from backend.services.backup_service import BackupService
        backup_svc = BackupService(
            db_path=settings.database_url,
            backup_type=settings.backup_type,
            interval_seconds=settings.backup_interval_seconds,
            max_copies=settings.backup_max_copies,
            destination_path=settings.backup_destination_path,
            temp_dir=settings.backup_temp_dir,
            s3_bucket=settings.backup_s3_bucket,
            s3_region=settings.backup_s3_region,
            s3_access_key=settings.backup_s3_access_key,
            s3_secret_key=settings.backup_s3_secret_key,
            oss_endpoint=settings.backup_oss_endpoint,
            oss_bucket=settings.backup_oss_bucket,
            oss_access_key=settings.backup_oss_access_key,
            oss_secret_key=settings.backup_oss_secret_key,
        )
        backup_svc.start()

    from backend.api.uploads import start_upload_cleanup_loop
    upload_cleanup_task = await start_upload_cleanup_loop()

    # /tmp capacity/inode watchdog. It only removes old, inactive,
    # allow-listed CCM artifacts and is independent from Dispatcher state.
    tmp_cleanup_task = tmp_space_manager.start_periodic()

    # Org registry heartbeat — periodically re-register with the registry
    heartbeat_task = None

    # Recovery performs bounded GitHub I/O and must never delay ASGI startup.
    # Start it only after all fallible startup stages above have completed, so
    # an exception cannot leak an unowned producer before the shutdown guard.
    pr_review_recovery_task = asyncio.create_task(
        _pr_review_recovery_loop()
    )
    try:
        yield
    finally:
        await _shutdown_runtime_services(
            heartbeat_task=heartbeat_task,
            worker_health_task=worker_health_task,
            upload_cleanup_task=upload_cleanup_task,
            tmp_cleanup_task=tmp_cleanup_task,
            backup_svc=backup_svc,
            pr_review_recovery_task=pr_review_recovery_task,
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Always release the exact process-bound update snapshot on shutdown."""

    try:
        async with _runtime_lifespan(app):
            yield
    finally:
        try:
            update_service.close_runtime_snapshot()
        except Exception:
            # Startup stale-owner recovery will retry after crashes or an
            # identity-safe graceful cleanup failure.
            logger.exception("Trusted update runtime cleanup failed")


app = FastAPI(title="Claude Code Manager", version="0.1.0", lifespan=lifespan)


@app.middleware("http")
async def remember_internal_api_endpoint(request: Request, call_next):
    # Use scope["server"], never the untrusted Host header. This captures
    # Uvicorn CLI-only --host/--port overrides before a request can create and
    # dispatch a Task whose MCP subprocess needs to call back into Manager.
    from backend.services.internal_api_endpoint import observe_asgi_server

    observe_asgi_server(request.scope.get("server"))
    return await call_next(request)

app.add_middleware(TokenAuthMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=[
        "X-Refreshed-Token",
        "Content-Disposition",
        "X-CCM-PR-Fix-Receipt",
        "X-CCM-PR-Fix-Token",
    ],
)

app.include_router(tasks_router)
app.include_router(instances_router)
app.include_router(system_router)
app.include_router(ws_router)
app.include_router(voice_router)
app.include_router(auth_router)
app.include_router(chat_router)
app.include_router(projects_router)
app.include_router(project_todos_router)
app.include_router(settings_router)
app.include_router(dispatcher_router)
app.include_router(uploads_router)
app.include_router(secrets_router)
app.include_router(tags_router)
app.include_router(files_router)
app.include_router(task_artifacts_router)
app.include_router(pool_router)
app.include_router(codex_pool_router)
app.include_router(cloudrouter_accounts_router)
app.include_router(discussions_router)
app.include_router(quick_phrases_router)
app.include_router(monitor_router)
app.include_router(sub_agents_router)
app.include_router(sub_agent_tasks_router)
app.include_router(pr_monitor_router)
app.include_router(pr_webhook_router)
app.include_router(workers_router)
app.include_router(feishu_router)
app.include_router(org_router)
app.include_router(ask_user_router)
app.include_router(user_skills_router)
app.include_router(team_sharing_router)
app.include_router(browser_reviews_router)
app.include_router(task_browser_reviews_router)
app.include_router(workspace_reviews_router)
app.include_router(test_harness_router)
app.include_router(plans_router)
app.include_router(plan_resources_router)

# Serve frontend static files in production
FRONTEND_DIST = Path(__file__).resolve().parent.parent / "frontend" / "dist"
if FRONTEND_DIST.is_dir():
    app.mount("/assets", StaticFiles(directory=str(FRONTEND_DIST / "assets")), name="static")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        """Serve index.html for all non-API routes (SPA fallback)."""
        file_path = FRONTEND_DIST / full_path
        if file_path.is_file():
            return FileResponse(str(file_path))
        return FileResponse(str(FRONTEND_DIST / "index.html"), headers={"Cache-Control": "no-cache"})
