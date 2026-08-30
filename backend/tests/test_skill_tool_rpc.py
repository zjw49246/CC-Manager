"""Security regressions for Manager-side frozen Skills RPC effects."""

import asyncio
import ast
import json
import runpy
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import text, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from backend.database import Base
from backend.models.task import Task
from backend.models.user import User
from backend.services import skill_tool_rpc
from backend.services.skill_tool_rpc import (
    SkillToolRPCOutcome,
    execute_skill_tool_rpc,
)


async def _create_scope(
    db_factory,
    *,
    role: str | None,
    active: bool = True,
) -> tuple[int, int | None]:
    async with db_factory() as db:
        owner_id = None
        if role is not None:
            owner = User(
                email=f"skill-{role}-{active}@example.com",
                name="skill owner",
                password_hash="unused",
                role=role,
                is_active=active,
            )
            db.add(owner)
            await db.flush()
            owner_id = owner.id
        task = Task(
            title="skill rpc",
            status="executing",
            provider="claude",
            created_by=owner_id,
            enabled_skills={},
        )
        db.add(task)
        await db.commit()
        return task.id, owner_id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("role", "active"),
    ((None, True), ("member", True), ("admin", False)),
)
@pytest.mark.parametrize("tool_name", ("ccm_create_skill", "ccm_distill"))
async def test_global_skill_effects_require_current_active_admin_owner(
    db_factory,
    monkeypatch,
    role,
    active,
    tool_name,
):
    task_id, _owner_id = await _create_scope(
        db_factory,
        role=role,
        active=active,
    )
    create = AsyncMock(
        return_value=SkillToolRPCOutcome('{"success":true}')
    )
    distill = AsyncMock(
        return_value=SkillToolRPCOutcome('{"success":true}')
    )
    monkeypatch.setattr(skill_tool_rpc, "_create_skill", create)
    monkeypatch.setattr(skill_tool_rpc, "_distill", distill)

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        outcome = await execute_skill_tool_rpc(task, tool_name, {}, db)

    assert json.loads(outcome.result) == {
        "success": False,
        "error": (
            "Active administrator ownership is required for global "
            "skill operations"
        ),
    }
    create.assert_not_awaited()
    distill.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("tool_name", ("ccm_create_skill", "ccm_distill"))
async def test_active_admin_owner_reaches_global_skill_handler(
    db_factory,
    monkeypatch,
    tool_name,
):
    task_id, _owner_id = await _create_scope(
        db_factory,
        role="admin",
    )
    expected = SkillToolRPCOutcome('{"success":true}')
    handler = AsyncMock(return_value=expected)
    monkeypatch.setattr(
        skill_tool_rpc,
        "_create_skill" if tool_name == "ccm_create_skill" else "_distill",
        handler,
    )

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        outcome = await execute_skill_tool_rpc(task, tool_name, {}, db)

    assert outcome is expected
    handler.assert_awaited_once_with({})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool_name", "handler_name"),
    (
        ("ccm_read_skill", "_read_skill"),
        ("ccm_enable_skill", "_toggle_skill"),
    ),
)
async def test_member_can_reach_task_local_read_and_toggle_handlers(
    db_factory,
    monkeypatch,
    tool_name,
    handler_name,
):
    task_id, _owner_id = await _create_scope(
        db_factory,
        role="member",
    )
    expected = SkillToolRPCOutcome('{"success":true}')
    handler = AsyncMock(return_value=expected)
    monkeypatch.setattr(skill_tool_rpc, handler_name, handler)

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        outcome = await execute_skill_tool_rpc(
            task,
            tool_name,
            {"skill_name": "monitor"},
            db,
        )

    assert outcome is expected
    assert handler.await_count == 1


@pytest.mark.asyncio
async def test_manager_exception_does_not_leak_secret_path_or_detail(
    db_factory,
    monkeypatch,
):
    task_id, _owner_id = await _create_scope(db_factory, role="member")
    secret = "/srv/manager/private/database.sqlite"
    monkeypatch.setattr(
        skill_tool_rpc,
        "_command_help",
        AsyncMock(side_effect=RuntimeError(f"failed at {secret}")),
    )

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        outcome = await execute_skill_tool_rpc(
            task,
            "ccm_command_help",
            {},
            db,
        )

    payload = json.loads(outcome.result)
    assert payload == {
        "success": False,
        "error": "CCM skill tool failed inside the Manager",
    }
    assert secret not in outcome.result


@pytest.mark.asyncio
async def test_admin_owner_fence_serializes_concurrent_demotion(tmp_path):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'owner-fence.db'}",
        connect_args={"timeout": 2},
    )
    db_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with engine.begin() as connection:
            journal_mode = await connection.execute(text("PRAGMA journal_mode=WAL"))
            assert journal_mode.scalar_one().lower() == "wal"
            await connection.run_sync(Base.metadata.create_all)
        task_id, owner_id = await _create_scope(db_factory, role="admin")
        assert owner_id is not None

        async with db_factory() as authority_db:
            task = await authority_db.get(Task, task_id)
            assert await skill_tool_rpc._owned_by_active_admin(task, authority_db)

            started = asyncio.Event()

            async def demote() -> None:
                async with db_factory() as demotion_db:
                    started.set()
                    await demotion_db.execute(
                        update(User)
                        .where(User.id == owner_id)
                        .values(role="member")
                    )
                    await demotion_db.commit()

            demotion = asyncio.create_task(demote())
            await started.wait()
            await asyncio.sleep(0.05)
            assert not demotion.done()
            await authority_db.commit()
            await asyncio.wait_for(demotion, timeout=2)

        async with db_factory() as db:
            task = await db.get(Task, task_id)
            assert not await skill_tool_rpc._owned_by_active_admin(task, db)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_read_skill_does_not_self_deadlock_behind_task_writer_fence(
    tmp_path,
    monkeypatch,
):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'read-skill-fence.db'}",
        connect_args={"timeout": 2},
    )
    db_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with engine.begin() as connection:
            journal_mode = await connection.execute(text("PRAGMA journal_mode=WAL"))
            assert journal_mode.scalar_one().lower() == "wal"
            await connection.run_sync(Base.metadata.create_all)
        monkeypatch.setattr(skill_tool_rpc, "async_session", db_factory)
        async with db_factory() as setup_db:
            task = Task(
                title="read skill writer fence",
                status="executing",
                provider="claude",
                enabled_skills={"monitor": True},
            )
            setup_db.add(task)
            await setup_db.commit()
            task_id = task.id

        async with db_factory() as route_db:
            fenced = await route_db.execute(
                update(Task)
                .where(Task.id == task_id)
                .values(status=Task.status)
            )
            assert fenced.rowcount == 1
            task = await route_db.get(Task, task_id, populate_existing=True)
            assert task is not None
            outcome = await asyncio.wait_for(
                execute_skill_tool_rpc(
                    task,
                    "ccm_read_skill",
                    {"skill_name": "monitor"},
                    route_db,
                ),
                timeout=1.0,
            )
            await route_db.rollback()

        assert json.loads(outcome.result)["success"] is True
    finally:
        await engine.dispose()


def test_frozen_skills_wrapper_has_no_backend_import_and_exact_rpc_route():
    path = (
        Path(__file__).resolve().parents[1]
        / "mcp"
        / "ccm_skills_http_server.py"
    )
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module or ""
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }

    assert not any(name == "backend" or name.startswith("backend.") for name in imported)
    assert '_api_url("/internal/skill-tools")' in source
    assert "/api/system/skills" not in source


def test_frozen_skills_wrapper_starts_in_legacy_no_auth_mode(monkeypatch):
    from mcp.server.fastmcp import FastMCP

    path = (
        Path(__file__).resolve().parents[1]
        / "mcp"
        / "ccm_skills_http_server.py"
    )
    run = MagicMock()
    monkeypatch.delenv("CCM_INTERNAL_SERVICE_TOKEN", raising=False)
    monkeypatch.setattr(FastMCP, "run", run)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(path),
            "--task-id",
            "42",
            "--api-base",
            "http://127.0.0.1:8000",
        ],
    )

    runpy.run_path(str(path), run_name="__main__")

    run.assert_called_once_with(transport="stdio")
