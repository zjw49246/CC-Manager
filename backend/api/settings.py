import asyncio

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.api.deps import require_admin
from backend.config import settings
from backend.database import get_db
from backend.models.global_settings import GlobalSettings
from backend.models.instance import Instance
from backend.models.task import Task
from backend.schemas.global_settings import (
    CapacitySettingsResponse,
    CapacitySettingsUpdate,
    GlobalSettingsResponse,
    GlobalSettingsUpdate,
    RuntimeSettingsResponse,
    RuntimeSettingsUpdate,
    UpdateChannelResponse,
    UpdateChannelUpdate,
)
from backend.schemas.plan import PlanPipelineConfig
from backend.services.cancellation import finish_awaitable
from backend.services.instance_capacity import (
    active_capacity_predicate,
    occupied_slot_predicate,
)
from backend.services.plan_pipeline_settings import effective_plan_pipeline_config

router = APIRouter(prefix="/api/settings", tags=["settings"])

# Capacity has two authorities that must move together: the durable singleton
# row and the in-process Dispatcher.  A database row lock is insufficient for
# SQLite and would not protect the runtime value, so serialize the complete
# update in this process.
_capacity_settings_lock = asyncio.Lock()
_runtime_settings_lock = asyncio.Lock()


async def _get_or_create(db: AsyncSession) -> GlobalSettings:
    row = await db.get(GlobalSettings, 1)
    if not row:
        row = GlobalSettings(id=1)
        db.add(row)
        await db.commit()
        await db.refresh(row)
    return row


@router.get("/git", response_model=GlobalSettingsResponse)
async def get_git_settings(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    require_admin(request)
    return await _get_or_create(db)


@router.put("/git", response_model=GlobalSettingsResponse)
async def update_git_settings(
    request: Request,
    body: GlobalSettingsUpdate,
    db: AsyncSession = Depends(get_db),
):
    require_admin(request)
    row = await _get_or_create(db)
    for key, value in body.model_dump().items():
        setattr(row, key, value or None)
    await db.commit()
    await db.refresh(row)
    return row


@router.get("/update-channel", response_model=UpdateChannelResponse)
async def get_update_channel(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    require_admin(request)
    row = await _get_or_create(db)
    return UpdateChannelResponse(update_channel=row.update_channel or "stable")


@router.put("/update-channel", response_model=UpdateChannelResponse)
async def update_update_channel(
    request: Request,
    body: UpdateChannelUpdate,
    db: AsyncSession = Depends(get_db),
):
    require_admin(request)
    row = await _get_or_create(db)
    row.update_channel = body.update_channel
    await db.commit()
    await db.refresh(row)
    return UpdateChannelResponse(update_channel=row.update_channel)


@router.get("/plan-pipeline", response_model=PlanPipelineConfig)
async def get_plan_pipeline_settings(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    require_admin(request)
    return await effective_plan_pipeline_config(db)


@router.put("/plan-pipeline", response_model=PlanPipelineConfig)
async def update_plan_pipeline_settings(
    request: Request,
    body: PlanPipelineConfig,
    db: AsyncSession = Depends(get_db),
):
    require_admin(request)
    row = await _get_or_create(db)
    row.plan_pipeline_config = body.model_dump(mode="json")
    await db.commit()
    await db.refresh(row)
    return PlanPipelineConfig.model_validate(row.plan_pipeline_config)


def _pty_available() -> bool:
    try:
        import claude_pty.adapters.ccm  # noqa: F401
        return True
    except ImportError:
        return False


def _effective_compact_threshold(row: GlobalSettings) -> float:
    if row.context_compact_threshold is not None:
        return row.context_compact_threshold
    return settings.context_compact_threshold


async def _capacity_response(
    db: AsyncSession,
    row: GlobalSettings,
) -> CapacitySettingsResponse:
    from backend.main import dispatcher

    active_instances = int(
        await db.scalar(
            select(func.count(Instance.id)).where(active_capacity_predicate())
        )
        or 0
    )
    live_instances = int(
        await db.scalar(
            select(func.count(Instance.id)).where(occupied_slot_predicate())
        )
        or 0
    )
    pending_tasks = int(
        await db.scalar(
            select(func.count(Task.id)).where(
                Task.status == "pending",
                Task.worker_id.is_(None),
                Task.shared_from_id.is_(None),
            )
        )
        or 0
    )
    return CapacitySettingsResponse(
        max_concurrent_instances=dispatcher.max_concurrent_instances,
        configured_override=row.max_concurrent_instances,
        env_default=settings.max_concurrent_instances,
        min_idle_instances=settings.min_idle_instances,
        active_instances=active_instances,
        live_instances=live_instances,
        pending_tasks=pending_tasks,
    )


@router.get("/capacity", response_model=CapacitySettingsResponse)
async def get_capacity_settings(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    require_admin(request)
    # Do not expose the brief commit -> runtime-apply interval to a concurrent
    # reader as a self-contradictory response.
    async with _capacity_settings_lock:
        return await _capacity_response(db, await _get_or_create(db))


@router.put("/capacity", response_model=CapacitySettingsResponse)
async def update_capacity_settings(
    request: Request,
    body: CapacitySettingsUpdate,
    db: AsyncSession = Depends(get_db),
):
    require_admin(request)
    from backend.main import broadcaster, dispatcher

    override = body.max_concurrent_instances

    async def persist_apply_and_respond() -> CapacitySettingsResponse:
        # DB is the restart authority, but the running Dispatcher must converge
        # before this request is allowed to unwind.  Shield the pair so client
        # cancellation cannot leave the process on the old value indefinitely.
        async with _capacity_settings_lock:
            row = await _get_or_create(db)
            row.max_concurrent_instances = override
            await db.commit()
            await dispatcher.apply_capacity_override(override)
            response = await _capacity_response(db, row)
            await broadcaster.broadcast(
                "system",
                {
                    "event": "capacity_settings_changed",
                    "max_concurrent_instances": (
                        response.max_concurrent_instances
                    ),
                },
            )
            return response

    # Let the DB/runtime pair finish converging before the request-scoped
    # session is closed by dependency teardown.
    return await finish_awaitable(persist_apply_and_respond())


@router.get("/runtime", response_model=RuntimeSettingsResponse)
async def get_runtime_settings(db: AsyncSession = Depends(get_db)):
    from backend.main import instance_manager
    row = await _get_or_create(db)
    # Lifespan applies this same effective DB/env value before dispatch starts.
    # Returning the live value makes this endpoint an execution truth source.
    return RuntimeSettingsResponse(
        use_pty_mode=instance_manager.pty_mode_enabled,
        pty_available=_pty_available(),
        codex_app_server_enabled=settings.codex_app_server_enabled,
        codex_main_mcp_enabled=settings.codex_main_mcp_enabled,
        codex_monitor_enabled=settings.codex_main_mcp_enabled,
        auto_sort_on_access=(
            row.auto_sort_on_access
            if row.auto_sort_on_access is not None
            else True
        ),
        context_compact_threshold=_effective_compact_threshold(row),
    )


@router.put("/runtime", response_model=RuntimeSettingsResponse)
async def update_runtime_settings(
    request: Request,
    body: RuntimeSettingsUpdate,
    db: AsyncSession = Depends(get_db),
):
    require_admin(request)
    from backend.main import broadcaster, instance_manager

    async def persist_apply_and_respond() -> RuntimeSettingsResponse:
        async with _runtime_settings_lock:
            # ``_get_or_create`` may commit when a fresh database has no
            # singleton yet. Ensure that bootstrap commit happens before the
            # Worker drain fence; after the fence is acquired this request
            # must keep one transaction open through the settings commit.
            row = await _get_or_create(db)
            # On a headless Worker this row lock is held through the settings
            # commit. It serializes the remote PUT with node drain admission:
            # the update either commits before the drain claim or is rejected
            # after the claim, never after a clean drain proof.
            from backend.services.worker_node_control import (
                fence_worker_node_mutation,
            )

            await fence_worker_node_mutation(db)

            if body.use_pty_mode is not None:
                effective = instance_manager.set_pty_mode(body.use_pty_mode)
                if not effective:
                    drained = await instance_manager.drain_idle_pty_sessions()
                    if drained:
                        import logging
                        logging.getLogger(__name__).info(
                            "PTY mode off: drained %d idle session(s)",
                            drained,
                        )
                row.use_pty_mode = effective

            if body.auto_sort_on_access is not None:
                row.auto_sort_on_access = body.auto_sort_on_access

            if body.context_compact_threshold is not None:
                row.context_compact_threshold = body.context_compact_threshold

            await db.commit()
            auto_sort = (
                row.auto_sort_on_access
                if row.auto_sort_on_access is not None
                else True
            )
            compact_threshold = _effective_compact_threshold(row)
            response = RuntimeSettingsResponse(
                use_pty_mode=instance_manager.pty_mode_enabled,
                pty_available=_pty_available(),
                codex_app_server_enabled=settings.codex_app_server_enabled,
                codex_main_mcp_enabled=settings.codex_main_mcp_enabled,
                codex_monitor_enabled=settings.codex_main_mcp_enabled,
                auto_sort_on_access=auto_sort,
                context_compact_threshold=compact_threshold,
            )
            await broadcaster.broadcast("system", {
                "event": "runtime_settings_changed",
                **response.model_dump(mode="json"),
            })
            return response

    return await finish_awaitable(persist_apply_and_respond())


# --- Default Skills ---


class DefaultSkillsResponse(BaseModel):
    default_enabled_plugins: dict[str, bool] | None = None
    default_enabled_user_skills: list[int] | None = None


class DefaultSkillsUpdate(BaseModel):
    default_enabled_plugins: dict[str, bool] | None = None
    default_enabled_user_skills: list[int] | None = None


@router.get("/default-skills", response_model=DefaultSkillsResponse)
async def get_default_skills(db: AsyncSession = Depends(get_db)):
    row = await _get_or_create(db)
    return DefaultSkillsResponse(
        default_enabled_plugins=row.default_enabled_plugins,
        default_enabled_user_skills=row.default_enabled_user_skills,
    )


@router.put("/default-skills", response_model=DefaultSkillsResponse)
async def update_default_skills(
    body: DefaultSkillsUpdate, db: AsyncSession = Depends(get_db)
):
    row = await _get_or_create(db)
    row.default_enabled_plugins = body.default_enabled_plugins
    row.default_enabled_user_skills = body.default_enabled_user_skills
    await db.commit()
    await db.refresh(row)
    return DefaultSkillsResponse(
        default_enabled_plugins=row.default_enabled_plugins,
        default_enabled_user_skills=row.default_enabled_user_skills,
    )
