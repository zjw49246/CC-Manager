"""Administrative API for managed API-gateway Claude/Codex accounts.

The historical route is retained so existing CloudRouter clients continue to
work; ``api_provider`` selects CloudRouter, ApexRouter, or APIBest.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, SecretStr

from backend.api.deps import require_admin
from backend.services.cloudrouter_accounts import (
    CloudRouterAccountBusyError,
    CloudRouterAccountNotFound,
    CloudRouterAccountStore,
    CloudRouterUnsafePathError,
    CloudRouterUpstreamError,
)

router = APIRouter(
    prefix="/api/cloudrouter/accounts",
    tags=["cloudrouter-accounts"],
)


class CloudRouterAccountCreate(BaseModel):
    name: str
    api_key: SecretStr
    api_provider: Literal["cloudrouter", "apex", "apibest"] = "cloudrouter"


def _get_store() -> CloudRouterAccountStore:
    from backend.main import cloudrouter_store

    return cloudrouter_store


def _runtime_pools():
    import backend.main as runtime
    from backend.config import settings

    accounts = runtime.cloudrouter_store.all_accounts(include_retired=True)
    has_claude = any(
        account.cleanup_pending
        or (
            account.enabled
            and not account.retired
            and account.supports_model("claude", None)
        )
        for account in accounts
    )
    has_codex = any(
        account.cleanup_pending
        or (
            account.enabled
            and not account.retired
            and account.supports_model("codex", None)
        )
        for account in accounts
    )

    if has_claude and runtime.dispatcher.pool is None:
        from backend.services.claude_pool import ClaudePool

        runtime.dispatcher.pool = ClaudePool(
            config_path=settings.pool_config_path,
            cooldown_seconds=settings.pool_cooldown_seconds,
            cloudrouter_store=runtime.cloudrouter_store,
            bootstrap_default=settings.pool_enabled,
            include_native=settings.pool_enabled,
        )

    if has_codex and runtime.codex_pool is None:
        from backend.services.codex_pool import CodexPool

        runtime.codex_pool = CodexPool(
            config_path=settings.codex_pool_config_path,
            cooldown_seconds=settings.codex_pool_cooldown_seconds,
            quota_reader=runtime.instance_manager.read_codex_rate_limits,
            cloudrouter_store=runtime.cloudrouter_store,
            bootstrap_default=settings.codex_pool_enabled,
            include_native=settings.codex_pool_enabled,
        )
        runtime.dispatcher.codex_pool = runtime.codex_pool

    return runtime.dispatcher.pool, runtime.codex_pool


def _reload_runtime_pools() -> None:
    """Project persisted API accounts into both already-running pools."""

    reloaded: set[int] = set()
    for pool in _runtime_pools():
        if pool is None or id(pool) in reloaded:
            continue
        pool.reload()
        reloaded.add(id(pool))


def _http_error(exc: Exception) -> HTTPException:
    if isinstance(exc, CloudRouterAccountNotFound):
        return HTTPException(404, "API account not found")
    if isinstance(exc, CloudRouterAccountBusyError):
        return HTTPException(409, {
            "message": str(exc),
            "error": str(exc),
            "code": exc.code,
            "reason": exc.reason,
            "cleanup_pending": True,
        })
    if isinstance(exc, CloudRouterUnsafePathError):
        # A storage-integrity failure can occur before the durable retirement
        # tombstone is written. Keep 409 exclusively for staged/busy cleanup
        # so the client never falsely claims this account was disabled.
        return HTTPException(500, "API account storage is unsafe")
    if isinstance(exc, CloudRouterUpstreamError):
        if exc.status_code in {401, 403}:
            return HTTPException(400, "API key is invalid or unauthorized")
        if exc.code in {
            "timeout", "network_error", "upstream_unavailable", "rate_limited",
        }:
            return HTTPException(503, "API gateway is temporarily unavailable")
        return HTTPException(400, f"API gateway validation failed: {exc.code}")
    if isinstance(exc, ValueError):
        return HTTPException(422, str(exc))
    return HTTPException(500, "API account operation failed")


@router.get("")
async def list_accounts(request: Request, force: bool = False):
    require_admin(request)
    store = _get_store()
    accounts = store.visible_accounts()
    usage = await asyncio.gather(*(
        store.fetch_usage(account.id, force=force)
        if not account.retired
        else asyncio.sleep(0, result=None)
        for account in accounts
    ))
    return [
        {**account.public_dict(), "api_quota": snapshot}
        for account, snapshot in zip(accounts, usage, strict=True)
    ]


@router.post("", status_code=201)
async def create_account(request: Request, body: CloudRouterAccountCreate):
    require_admin(request)
    try:
        store = _get_store()
        account = await store.add_account(
            body.name,
            body.api_key.get_secret_value(),
            api_provider=body.api_provider,
        )
        quota = await store.fetch_usage(account.id, force=True)
        _reload_runtime_pools()
    except Exception as exc:
        raise _http_error(exc) from exc
    return {**account.public_dict(), "api_quota": quota}


@router.post("/{account_id}/refresh")
async def refresh_account(request: Request, account_id: str):
    require_admin(request)
    store = _get_store()
    try:
        account = await store.refresh_account(account_id)
        quota = await store.fetch_usage(account_id, force=True)
        _reload_runtime_pools()
    except Exception as exc:
        raise _http_error(exc) from exc
    return {**account.public_dict(), "api_quota": quota}


@asynccontextmanager
async def _runtime_retirement_fence(account, store):
    """Prove quiescence after durable disable, without reversing lock order."""

    import backend.main as runtime

    migrator = runtime.task_migrator

    @asynccontextmanager
    async def no_migration_guard():
        yield

    migration_guard = (
        migrator.api_account_retirement_guard()
        if migrator is not None
        else no_migration_guard()
    )
    try:
        async with migration_guard:
            if store.active_credential_users(account.id):
                raise CloudRouterAccountBusyError(
                    "API account still has an active quota/credential request; "
                    "retry deletion after it finishes",
                    code="credential_busy",
                )
            blockers = await runtime.instance_manager.api_account_runtime_users(
                account
            )
            blockers.extend(
                runtime.dispatcher.api_account_aux_runtime_users(account)
            )
            blockers.extend(
                await runtime.dispatcher.codex_monitor_runtime_users(
                    account.codex_home,
                    account_id=account.id,
                )
            )
            if blockers:
                summary = ", ".join(blockers[:5])
                raise CloudRouterAccountBusyError(
                    "API account is still in use by "
                    f"{summary}; stop it and retry deletion",
                    code="runtime_busy",
                )

            maintenance_started = False
            try:
                await (
                    runtime.instance_manager
                    .begin_codex_app_server_home_maintenance(
                        account.codex_home,
                        require_idle=True,
                    )
                )
                maintenance_started = True
                # No later task/aux process can spawn after Store staging. Check
                # again after the Codex transport has stopped, then detach idle
                # Docker containers retaining a read-only bind mount.
                if store.active_credential_users(account.id):
                    raise CloudRouterAccountBusyError(
                        "API account still has an active quota/credential request; "
                        "retry deletion after it finishes",
                        code="credential_busy",
                    )
                blockers = (
                    await runtime.instance_manager.api_account_runtime_users(
                        account
                    )
                )
                blockers.extend(
                    runtime.dispatcher.api_account_aux_runtime_users(account)
                )
                blockers.extend(
                    await runtime.dispatcher.codex_monitor_runtime_users(
                        account.codex_home,
                        account_id=account.id,
                    )
                )
                if blockers:
                    raise CloudRouterAccountBusyError(
                        "API account acquired a runtime user before disable "
                        "completed; retry deletion",
                        code="runtime_busy",
                    )
                # Shared-project containers are a Claude-only execution path.
                # ApexRouter has never exposed a Claude route, so no CCM
                # container can mount its account root. CloudRouter must scan
                # even if a later model refresh removed all Claude models,
                # because an older idle container may retain the mount.
                if account.api_provider != "apex":
                    await (
                        runtime.instance_manager
                        .detach_api_account_containers(account)
                    )
            finally:
                if maintenance_started:
                    await (
                        runtime.instance_manager
                        .end_codex_app_server_home_maintenance(
                            account.codex_home
                        )
                    )
            # Codex maintenance is deliberately released before final Store
            # cleanup. The account is already disabled, so new Store admission
            # fails before reaching the home lock; this preserves the global
            # lifecycle -> Store -> home ordering.
            yield
    except (CloudRouterAccountBusyError, CloudRouterUnsafePathError):
        raise
    except Exception as exc:
        from backend.services.task_migrator import MigrationError

        if isinstance(exc, MigrationError):
            raise CloudRouterAccountBusyError(
                "API account cleanup is blocked by an active task migration; "
                "retry after it finishes",
                code="migration_busy",
            ) from exc
        raise CloudRouterAccountBusyError(
            "API account runtime state could not be verified safely; "
            "retry deletion after active work finishes",
            code="runtime_verification_failed",
        ) from exc


@router.delete("/{account_id}")
async def retire_account(request: Request, account_id: str):
    """Durably disable, prove quiescence, then remove managed credentials."""

    require_admin(request)
    store = _get_store()
    try:
        async with store.account_retirement_guard(account_id):
            account = await store.stage_retirement(account_id)
            _reload_runtime_pools()
            if account.retired and not account.cleanup_pending:
                return {"ok": True, **account.public_dict()}
            account = await store.mark_cleanup_attempt(account_id)
            try:
                async with _runtime_retirement_fence(account, store):
                    account = await store.finalize_retirement(account_id)
            except CloudRouterAccountBusyError as exc:
                await store.record_cleanup_failure(
                    account_id,
                    code=exc.code,
                    reason=exc.reason,
                )
                raise
            _reload_runtime_pools()
    except Exception as exc:
        # Stage may have succeeded before a busy/failure response. Project the
        # pending tombstone immediately so either pool tab exposes retry.
        try:
            _reload_runtime_pools()
        except Exception:
            pass
        raise _http_error(exc) from exc
    return {"ok": True, **account.public_dict()}
