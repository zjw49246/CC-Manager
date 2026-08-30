"""File-backed SQLite integration tests for mutating Task SSH effects."""

import asyncio
from pathlib import Path
from unittest.mock import patch

from alembic import command
from alembic.config import Config
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519
from httpx import ASGITransport, AsyncClient
import pytest
from sqlalchemy import event, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from backend.config import settings
from backend.database import get_db
from backend.models.project import Project
from backend.models.ssh_profile import SSHProfile
from backend.models.task import Task
from backend.models.task_share import ProjectShare, TaskShare
from backend.models.task_ssh_effect import (
    SQLITE_TASK_SSH_EFFECT_TRIGGER_NAMES,
    TaskSSHEffectReceipt,
)
from backend.models.task_ssh_grant import TaskSSHGrant
from backend.models.team_share import TeamProjectShare, TeamTaskShare
from backend.services.ssh_executor import (
    SSHCommandResult,
    derive_openssh_public_key,
)
from backend.services.ssh_profiles import validated_profile_material


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def _upgrade_file_database(db_path: Path) -> None:
    cfg = Config(str(PROJECT_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))
    cfg.set_main_option("sqlalchemy.url", f"sqlite:///{db_path}")
    with patch(
        "backend.config.settings.database_url",
        f"sqlite+aiosqlite:///{db_path}",
    ):
        command.upgrade(cfg, "head")


def _sqlite_async_engine(db_path: Path):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{db_path}",
        connect_args={"timeout": 2},
    )

    @event.listens_for(engine.sync_engine, "connect")
    def _set_pragmas(dbapi_connection, _connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA busy_timeout=2000")
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.close()

    return engine


def _managed_private_key(tmp_path: Path) -> Path:
    managed = tmp_path / "ssh-keys" / "managed"
    managed.mkdir(mode=0o700, parents=True)
    path = managed / "effect-key"
    key = ed25519.Ed25519PrivateKey.generate()
    path.write_bytes(key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.OpenSSH,
        encryption_algorithm=serialization.NoEncryption(),
    ))
    path.chmod(0o600)
    return path


@pytest.mark.asyncio
async def test_migrated_sqlite_effects_release_global_writer_during_remote_io(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "task-ssh-effect-runtime.db"
    _upgrade_file_database(db_path)
    api_engine = _sqlite_async_engine(db_path)
    writer_engine = _sqlite_async_engine(db_path)
    api_factory = async_sessionmaker(
        api_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    writer_factory = async_sessionmaker(
        writer_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    key_path = _managed_private_key(tmp_path)
    monkeypatch.setattr(
        settings,
        "ssh_key_storage_dir",
        str(tmp_path / "ssh-keys"),
    )
    host_key = derive_openssh_public_key(key_path)
    material = validated_profile_material(
        key_path=str(key_path),
        host_key_value=host_key,
    )
    async with api_factory() as db:
        profile = SSHProfile(
            name="file-backed-effect-profile",
            host="ssh.effect.internal",
            username="deploy",
            task_access_enabled=True,
            task_capabilities=["exec", "write"],
            allowed_roots=["/"],
            **material,
        )
        task = Task(description="Hold remote I/O without holding SQLite")
        db.add_all([profile, task])
        await db.flush()
        db.add(TaskSSHGrant(
            task_id=task.id,
            ssh_profile_id=profile.id,
            profile_revision=profile.revision,
            capabilities=["exec", "write"],
        ))
        await db.commit()
        task_id = task.id
        profile_id = profile.id

    started = asyncio.Event()
    release = asyncio.Event()
    calls = []

    class FakeExecutor:
        async def run_result(self, command, **_kwargs):
            calls.append(command)
            started.set()
            await release.wait()
            return SSHCommandResult(0, "done\n", "", False, 3)

    from backend.api import task_ssh as task_ssh_api

    monkeypatch.setattr(
        task_ssh_api,
        "executor_for_profile",
        lambda _profile: FakeExecutor(),
    )
    monkeypatch.setattr(
        task_ssh_api,
        "_write_file_sync",
        lambda _profile, path, content, _overwrite: (
            path,
            len(content.encode()),
        ),
    )
    from backend.main import app as real_app

    original_override = real_app.dependency_overrides.get(get_db)

    async def override_get_db():
        async with api_factory() as db:
            yield db

    real_app.dependency_overrides[get_db] = override_get_db
    monkeypatch.setattr(settings, "auth_token", "file-effect-token")
    transport = ASGITransport(app=real_app)
    try:
        async with AsyncClient(
            transport=transport,
            base_url="http://test",
            headers={"Authorization": "Bearer file-effect-token"},
        ) as client:
            executing = asyncio.create_task(client.post(
                f"/api/tasks/{task_id}/ssh-access/{profile_id}/execute",
                json={
                    "effect_id": f"{1:032x}",
                    "command": "block-remotely",
                },
            ))
            await asyncio.wait_for(started.wait(), timeout=2)

            async def write_unrelated_rows():
                async with writer_factory() as db:
                    project = Project(
                        name="unrelated-during-ssh",
                        status="ready",
                    )
                    db.add(project)
                    await db.flush()
                    unrelated = Task(
                        description="Unrelated writer remains live",
                        project_id=project.id,
                    )
                    db.add(unrelated)
                    await db.commit()
                    return project.id, unrelated.id

            unrelated_ids = await asyncio.wait_for(
                write_unrelated_rows(),
                timeout=2,
            )
            assert all(value > 0 for value in unrelated_ids)
            assert not executing.done()

            release.set()
            executed = await executing
            assert executed.status_code == 200, executed.text
            assert executed.json()["stdout"] == "done\n"

            written = await client.post(
                f"/api/tasks/{task_id}/ssh-access/{profile_id}/write",
                json={
                    "effect_id": f"{2:032x}",
                    "path": "/tmp/effect-file",
                    "content": "payload",
                    "overwrite": True,
                },
            )
            assert written.status_code == 200, written.text
            assert written.json()["bytes_written"] == 7
    finally:
        if original_override is None:
            real_app.dependency_overrides.pop(get_db, None)
        else:
            real_app.dependency_overrides[get_db] = original_override

    async with writer_factory() as db:
        receipts = list((await db.scalars(
            select(TaskSSHEffectReceipt).order_by(TaskSSHEffectReceipt.id)
        )).all())
        trigger_rows = await db.execute(text(
            "SELECT name FROM sqlite_master WHERE type = 'trigger' "
            "AND name LIKE 'trg_task_ssh_effect_%'"
        ))
    assert [receipt.status for receipt in receipts] == ["completed", "completed"]
    assert {row[0] for row in trigger_rows} == set(
        SQLITE_TASK_SSH_EFFECT_TRIGGER_NAMES
    )
    assert calls == ["block-remotely"]
    await api_engine.dispose()
    await writer_engine.dispose()


async def _assert_trigger_blocks(session_factory, statement: str, values=None):
    async with session_factory() as db:
        with pytest.raises(IntegrityError, match="Task SSH effect"):
            await db.execute(text(statement), values or {})
            await db.commit()
        await db.rollback()


async def _assert_statement_succeeds(session_factory, statement: str, values=None):
    async with session_factory() as db:
        await db.execute(text(statement), values or {})
        await db.commit()


@pytest.mark.asyncio
async def test_sqlite_effect_permit_fences_exact_authorization_graph_only(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "task-ssh-effect-trigger-graph.db"
    _upgrade_file_database(db_path)
    engine = _sqlite_async_engine(db_path)
    factory = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    key_path = _managed_private_key(tmp_path)
    monkeypatch.setattr(
        settings,
        "ssh_key_storage_dir",
        str(tmp_path / "ssh-keys"),
    )
    material = validated_profile_material(
        key_path=str(key_path),
        host_key_value=derive_openssh_public_key(key_path),
    )

    async with factory() as db:
        active_project = Project(name="effect-active-project", status="ready")
        other_project = Project(name="effect-other-project", status="ready")
        active_profile = SSHProfile(
            name="effect-active-profile",
            host="active.effect.internal",
            username="deploy",
            task_access_enabled=True,
            task_capabilities=["exec"],
            allowed_roots=["/"],
            **material,
        )
        other_profile = SSHProfile(
            name="effect-other-profile",
            host="other.effect.internal",
            username="deploy",
            task_access_enabled=True,
            task_capabilities=["exec"],
            allowed_roots=["/"],
            **material,
        )
        db.add_all([
            active_project,
            other_project,
            active_profile,
            other_profile,
        ])
        await db.flush()
        active_task = Task(
            description="effect permit owner",
            project_id=active_project.id,
            status="executing",
        )
        other_task = Task(
            description="unrelated permit task",
            project_id=other_project.id,
            status="executing",
        )
        db.add_all([active_task, other_task])
        await db.flush()
        active_grant = TaskSSHGrant(
            task_id=active_task.id,
            ssh_profile_id=active_profile.id,
            profile_revision=active_profile.revision,
            capabilities=["exec"],
        )
        other_grant = TaskSSHGrant(
            task_id=other_task.id,
            ssh_profile_id=other_profile.id,
            profile_revision=other_profile.revision,
            capabilities=["exec"],
        )
        active_task_share = TaskShare(
            task_id=active_task.id,
            shared_to_open_id="effect-active-task",
            shared_to_ccm_url="https://active-task.invalid",
            share_token="effect-active-task-token",
            status="active",
        )
        other_task_share = TaskShare(
            task_id=other_task.id,
            shared_to_open_id="effect-other-task",
            shared_to_ccm_url="https://other-task.invalid",
            share_token="effect-other-task-token",
            status="active",
        )
        active_project_share = ProjectShare(
            project_id=active_project.id,
            shared_to_open_id="effect-active-project",
            shared_to_ccm_url="https://active-project.invalid",
            status="active",
        )
        other_project_share = ProjectShare(
            project_id=other_project.id,
            shared_to_open_id="effect-other-project",
            shared_to_ccm_url="https://other-project.invalid",
            status="active",
        )
        active_team_task = TeamTaskShare(
            task_id=active_task.id,
            target_type="user",
            target_id=101,
            permission="chat",
            shared_by=1,
        )
        other_team_task = TeamTaskShare(
            task_id=other_task.id,
            target_type="user",
            target_id=102,
            permission="chat",
            shared_by=1,
        )
        active_team_project = TeamProjectShare(
            project_id=active_project.id,
            target_type="user",
            target_id=201,
            shared_by=1,
        )
        other_team_project = TeamProjectShare(
            project_id=other_project.id,
            target_type="user",
            target_id=202,
            shared_by=1,
        )
        db.add_all([
            active_grant,
            other_grant,
            active_task_share,
            other_task_share,
            active_project_share,
            other_project_share,
            active_team_task,
            other_team_task,
            active_team_project,
            other_team_project,
        ])
        await db.flush()
        receipt = TaskSSHEffectReceipt(
            effect_id=f"{901:032x}",
            task_id=active_task.id,
            task_incarnation_id=active_task.incarnation_id,
            task_retry_count=active_task.retry_count,
            task_turn_generation=active_task.turn_generation,
            task_status=active_task.status,
            profile_id=active_profile.id,
            profile_revision=active_profile.revision,
            operation="execute",
            request_digest="a" * 64,
            status="running",
        )
        db.add(receipt)
        await db.commit()
        ids = {
            "task": active_task.id,
            "other_task": other_task.id,
            "project": active_project.id,
            "other_project": other_project.id,
            "profile": active_profile.id,
            "other_profile": other_profile.id,
            "grant": active_grant.id,
            "other_grant": other_grant.id,
            "task_share": active_task_share.id,
            "other_task_share": other_task_share.id,
            "project_share": active_project_share.id,
            "other_project_share": other_project_share.id,
            "team_task": active_team_task.id,
            "other_team_task": other_team_task.id,
            "team_project": active_team_project.id,
            "other_team_project": other_team_project.id,
        }

    # Runtime telemetry remains writable throughout a long remote command.
    async with factory() as db:
        await db.execute(
            text("UPDATE tasks SET has_unread = 1 WHERE id = :id"),
            {"id": ids["task"]},
        )
        await db.execute(
            text(
                "UPDATE ssh_profiles SET last_test_ok = 1 "
                "WHERE id = :id"
            ),
            {"id": ids["profile"]},
        )
        await db.commit()

    # Exact execution/scope and Profile security changes are fenced.
    await _assert_trigger_blocks(
        factory,
        "UPDATE tasks SET retry_count = retry_count + 1 WHERE id = :id",
        {"id": ids["task"]},
    )
    await _assert_trigger_blocks(
        factory,
        "UPDATE tasks SET project_id = :other WHERE id = :id",
        {"id": ids["task"], "other": ids["other_project"]},
    )
    await _assert_trigger_blocks(
        factory,
        "UPDATE ssh_profiles SET revision = revision + 1 WHERE id = :id",
        {"id": ids["profile"]},
    )
    await _assert_trigger_blocks(
        factory,
        "DELETE FROM tasks WHERE id = :id",
        {"id": ids["task"]},
    )
    await _assert_trigger_blocks(
        factory,
        "DELETE FROM ssh_profiles WHERE id = :id",
        {"id": ids["profile"]},
    )

    # Grant INSERT/UPDATE OLD+NEW/DELETE are all covered.
    await _assert_trigger_blocks(
        factory,
        "INSERT INTO task_ssh_grants "
        "(task_id, ssh_profile_id, profile_revision, capabilities) "
        "VALUES (:task, :profile, 1, '[\"exec\"]')",
        {"task": ids["task"], "profile": ids["other_profile"]},
    )
    await _assert_trigger_blocks(
        factory,
        "UPDATE task_ssh_grants SET task_id = :other "
        "WHERE id = :grant",
        {"grant": ids["grant"], "other": ids["other_task"]},
    )
    await _assert_trigger_blocks(
        factory,
        "UPDATE task_ssh_grants SET task_id = :task "
        "WHERE id = :grant",
        {"grant": ids["other_grant"], "task": ids["task"]},
    )
    await _assert_trigger_blocks(
        factory,
        "DELETE FROM task_ssh_grants WHERE id = :grant",
        {"grant": ids["grant"]},
    )

    # Cross-CCM federation shares change the execution trust boundary and
    # remain fenced until the exact remote effect has a known outcome.
    federation_share_cases = (
        (
            "task_shares",
            "task_id",
            ids["task"],
            ids["other_task"],
            ids["task_share"],
            ids["other_task_share"],
            "shared_to_open_id, shared_to_ccm_url, share_token, status",
            "'insert-task', 'https://insert-task.invalid', "
            "'insert-task-token', 'active'",
        ),
        (
            "project_shares",
            "project_id",
            ids["project"],
            ids["other_project"],
            ids["project_share"],
            ids["other_project_share"],
            "shared_to_open_id, shared_to_ccm_url, status",
            "'insert-project', 'https://insert-project.invalid', 'active'",
        ),
    )
    for (
        table,
        owner_column,
        active_owner,
        other_owner,
        active_row,
        other_row,
        extra_columns,
        extra_values,
    ) in federation_share_cases:
        await _assert_trigger_blocks(
            factory,
            f"INSERT INTO {table} ({owner_column}, {extra_columns}) "
            f"VALUES (:owner, {extra_values})",
            {"owner": active_owner},
        )
        await _assert_trigger_blocks(
            factory,
            f"UPDATE {table} SET {owner_column} = :other WHERE id = :row",
            {"other": other_owner, "row": active_row},
        )
        await _assert_trigger_blocks(
            factory,
            f"UPDATE {table} SET {owner_column} = :active WHERE id = :row",
            {"active": active_owner, "row": other_row},
        )
        await _assert_trigger_blocks(
            factory,
            f"DELETE FROM {table} WHERE id = :row",
            {"row": active_row},
        )

    # Team shares are Manager-local ACL rows. They neither change the Task
    # principal nor move execution across a CCM boundary, so insert, both
    # directions of UPDATE, and DELETE stay available during remote I/O.
    team_share_cases = (
        (
            "team_task_shares",
            "task_id",
            ids["task"],
            ids["other_task"],
            ids["team_task"],
            ids["other_team_task"],
            "target_type, target_id, permission, shared_by",
            "'user', 301, 'chat', 1",
        ),
        (
            "team_project_shares",
            "project_id",
            ids["project"],
            ids["other_project"],
            ids["team_project"],
            ids["other_team_project"],
            "target_type, target_id, shared_by",
            "'user', 401, 1",
        ),
    )
    for (
        table,
        owner_column,
        active_owner,
        other_owner,
        active_row,
        other_row,
        extra_columns,
        extra_values,
    ) in team_share_cases:
        await _assert_statement_succeeds(
            factory,
            f"INSERT INTO {table} ({owner_column}, {extra_columns}) "
            f"VALUES (:owner, {extra_values})",
            {"owner": active_owner},
        )
        # OLD side of the mutation references the active Task/Project.
        await _assert_statement_succeeds(
            factory,
            f"UPDATE {table} SET {owner_column} = :other WHERE id = :row",
            {"other": other_owner, "row": active_row},
        )
        await _assert_statement_succeeds(
            factory,
            f"UPDATE {table} SET {owner_column} = :active WHERE id = :row",
            {"active": active_owner, "row": active_row},
        )
        # NEW side of the mutation references the active Task/Project.
        await _assert_statement_succeeds(
            factory,
            f"UPDATE {table} SET {owner_column} = :active WHERE id = :row",
            {"active": active_owner, "row": other_row},
        )
        await _assert_statement_succeeds(
            factory,
            f"UPDATE {table} SET {owner_column} = :other WHERE id = :row",
            {"other": other_owner, "row": other_row},
        )
        await _assert_statement_succeeds(
            factory,
            f"DELETE FROM {table} WHERE id = :row",
            {"row": active_row},
        )

    await engine.dispose()
