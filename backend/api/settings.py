from typing import Any

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel
from backend.api.deps import require_admin
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.database import get_db
from backend.models.global_settings import GlobalSettings
from backend.schemas.global_settings import (
    GlobalSettingsUpdate,
    GlobalSettingsResponse,
    RuntimeSettingsUpdate,
    RuntimeSettingsResponse,
)

router = APIRouter(prefix="/api/settings", tags=["settings"])


async def _get_or_create(db: AsyncSession) -> GlobalSettings:
    row = await db.get(GlobalSettings, 1)
    if not row:
        row = GlobalSettings(id=1)
        db.add(row)
        await db.commit()
        await db.refresh(row)
    return row


@router.get("/git", response_model=GlobalSettingsResponse)
async def get_git_settings(db: AsyncSession = Depends(get_db)):
    return await _get_or_create(db)


@router.put("/git", response_model=GlobalSettingsResponse)
async def update_git_settings(request: Request, body: GlobalSettingsUpdate, db: AsyncSession = Depends(get_db)):
    row = await _get_or_create(db)
    for key, value in body.model_dump().items():
        setattr(row, key, value or None)
    await db.commit()
    await db.refresh(row)
    return row


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


@router.get("/runtime", response_model=RuntimeSettingsResponse)
async def get_runtime_settings(db: AsyncSession = Depends(get_db)):
    from backend.main import instance_manager
    row = await _get_or_create(db)
    return RuntimeSettingsResponse(
        use_pty_mode=instance_manager.pty_mode_enabled,
        pty_available=_pty_available(),
        codex_app_server_enabled=settings.codex_app_server_enabled,
        codex_main_mcp_enabled=settings.codex_main_mcp_enabled,
        auto_sort_on_access=row.auto_sort_on_access if row.auto_sort_on_access is not None else True,
        context_compact_threshold=_effective_compact_threshold(row),
    )


@router.put("/runtime", response_model=RuntimeSettingsResponse)
async def update_runtime_settings(
    body: RuntimeSettingsUpdate, db: AsyncSession = Depends(get_db)
):
    from backend.main import instance_manager

    row = await _get_or_create(db)

    if body.use_pty_mode is not None:
        effective = instance_manager.set_pty_mode(body.use_pty_mode)
        if not effective:
            drained = await instance_manager.drain_idle_pty_sessions()
            if drained:
                import logging
                logging.getLogger(__name__).info(
                    "PTY mode off: drained %d idle session(s)", drained
                )
        row.use_pty_mode = effective

    if body.auto_sort_on_access is not None:
        row.auto_sort_on_access = body.auto_sort_on_access

    if body.context_compact_threshold is not None:
        row.context_compact_threshold = body.context_compact_threshold

    await db.commit()

    auto_sort = row.auto_sort_on_access if row.auto_sort_on_access is not None else True
    compact_threshold = _effective_compact_threshold(row)

    from backend.main import broadcaster
    await broadcaster.broadcast("system", {
        "event": "runtime_settings_changed",
        "use_pty_mode": instance_manager.pty_mode_enabled,
        "codex_app_server_enabled": settings.codex_app_server_enabled,
        "codex_main_mcp_enabled": settings.codex_main_mcp_enabled,
        "auto_sort_on_access": auto_sort,
        "context_compact_threshold": compact_threshold,
    })
    return RuntimeSettingsResponse(
        use_pty_mode=instance_manager.pty_mode_enabled,
        pty_available=_pty_available(),
        codex_app_server_enabled=settings.codex_app_server_enabled,
        codex_main_mcp_enabled=settings.codex_main_mcp_enabled,
        auto_sort_on_access=auto_sort,
        context_compact_threshold=compact_threshold,
    )


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
