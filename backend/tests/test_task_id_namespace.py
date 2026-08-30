"""Manager/Worker Task-id namespace invariants across supported dialects."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import select, update
from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.schema import CreateTable

from backend.config import settings
from backend.database import Base
from backend.models.task import Task
from backend.models.task_id_allocator import (
    TASK_ID_SIGNED_INT_MAX,
    TASK_ID_WORKER_NAMESPACE_START,
    TaskIdAllocator,
)
from backend.services.skill_context import WORKER_MANAGED_TASK_METADATA_KEY
from backend.services.task_creation import stage_task_record
from backend.services.task_id_namespace import (
    TASK_ID_NAMESPACE_PROTOCOL,
    TaskIdNamespaceError,
    TaskIdNamespaceProtocolError,
    bind_task_id_namespace_at_startup,
    validate_worker_task_id_namespace_config,
)


def _worker_mirror_metadata() -> dict[str, bool]:
    return {WORKER_MANAGED_TASK_METADATA_KEY: True}


@pytest.mark.asyncio
async def test_manager_auto_ids_remain_in_low_namespace(
    db_session: AsyncSession,
    monkeypatch,
):
    monkeypatch.setattr(settings, "ccm_node_role", "manager")

    first = await stage_task_record(
        db_session,
        title="manager one",
        description="low range",
    )
    second = await stage_task_record(
        db_session,
        title="manager two",
        description="low range",
    )
    await db_session.commit()

    assert 0 < first.id < TASK_ID_WORKER_NAMESPACE_START
    assert first.id < second.id < TASK_ID_WORKER_NAMESPACE_START
    allocator = await db_session.get(TaskIdAllocator, 1)
    assert allocator is not None
    assert allocator.node_role == "manager"
    assert allocator.next_worker_task_id == TASK_ID_WORKER_NAMESPACE_START


@pytest.mark.asyncio
async def test_manager_rejects_explicit_task_id(
    db_session: AsyncSession,
    monkeypatch,
):
    monkeypatch.setattr(settings, "ccm_node_role", "manager")

    with pytest.raises(
        TaskIdNamespaceError,
        match="cannot accept explicit",
    ):
        await stage_task_record(
            db_session,
            id=77,
            title="not a manager allocation",
            description="rejected",
        )


@pytest.mark.asyncio
async def test_manager_native_boundary_failure_cannot_be_accidentally_committed(
    db_session: AsyncSession,
    monkeypatch,
):
    db_session.add(Task(
        id=TASK_ID_WORKER_NAMESPACE_START - 1,
        title="last low id",
        description="simulated exhausted native allocator",
    ))
    await db_session.commit()
    monkeypatch.setattr(settings, "ccm_node_role", "manager")

    with pytest.raises(TaskIdNamespaceError, match="exhausted"):
        await stage_task_record(
            db_session,
            title="would cross boundary",
            description="must be removed before raising",
        )
    # Deliberately commit after catching the service exception.  The invalid
    # high row must still not become durable.
    await db_session.commit()
    assert await db_session.get(Task, TASK_ID_WORKER_NAMESPACE_START) is None


@pytest.mark.asyncio
async def test_worker_preserves_low_mirror_and_allocates_local_high_ids(
    db_session: AsyncSession,
    monkeypatch,
):
    monkeypatch.setattr(settings, "ccm_node_role", "worker")

    owner = await stage_task_record(
        db_session,
        id=77,
        title="manager mirror",
        description="same logical task",
        metadata_=_worker_mirror_metadata(),
    )
    child = await stage_task_record(
        db_session,
        title="worker browser child",
        description="derived locally",
    )
    await db_session.commit()

    assert owner.id == 77
    assert child.id == TASK_ID_WORKER_NAMESPACE_START
    assert child.id > owner.id
    allocator = await db_session.get(TaskIdAllocator, 1)
    assert allocator is not None
    assert allocator.node_role == "worker"
    assert allocator.next_worker_task_id == TASK_ID_WORKER_NAMESPACE_START + 1


@pytest.mark.asyncio
async def test_worker_rejects_explicit_id_in_local_high_namespace(
    db_session: AsyncSession,
    monkeypatch,
):
    monkeypatch.setattr(settings, "ccm_node_role", "worker")

    with pytest.raises(TaskIdNamespaceError, match="below"):
        await stage_task_record(
            db_session,
            id=TASK_ID_WORKER_NAMESPACE_START,
            title="invalid mirror",
            description="high explicit id",
            metadata_=_worker_mirror_metadata(),
        )


@pytest.mark.asyncio
async def test_worker_allocation_rolls_back_with_task_transaction(
    db_factory,
    monkeypatch,
):
    monkeypatch.setattr(settings, "ccm_node_role", "worker")

    async with db_factory() as db:
        await stage_task_record(
            db,
            id=5,
            title="mirror",
            description="claims worker role",
            metadata_=_worker_mirror_metadata(),
        )
        await db.commit()

    async with db_factory() as db:
        rolled_back = await stage_task_record(
            db,
            title="rolled back child",
            description="does not consume id",
        )
        assert rolled_back.id == TASK_ID_WORKER_NAMESPACE_START
        await db.rollback()

    async with db_factory() as db:
        committed = await stage_task_record(
            db,
            title="committed child",
            description="reuses safe reservation",
        )
        await db.commit()
        assert committed.id == TASK_ID_WORKER_NAMESPACE_START


@pytest.mark.asyncio
async def test_worker_allocator_exhaustion_fails_closed(
    db_session: AsyncSession,
    monkeypatch,
):
    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    await stage_task_record(
        db_session,
        id=9,
        title="mirror",
        description="claims role",
        metadata_=_worker_mirror_metadata(),
    )
    await db_session.commit()
    await db_session.execute(
        update(TaskIdAllocator)
        .where(TaskIdAllocator.id == 1)
        .values(next_worker_task_id=TASK_ID_SIGNED_INT_MAX)
    )
    await db_session.commit()

    with pytest.raises(TaskIdNamespaceError, match="exhausted"):
        await stage_task_record(
            db_session,
            title="no id remains",
            description="must fail",
        )


@pytest.mark.asyncio
async def test_bound_database_role_cannot_be_switched(
    db_session: AsyncSession,
    monkeypatch,
):
    monkeypatch.setattr(settings, "ccm_node_role", "manager")
    await stage_task_record(
        db_session,
        title="manager task",
        description="bind manager",
    )
    await db_session.commit()

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    with pytest.raises(TaskIdNamespaceError, match="already bound"):
        await stage_task_record(
            db_session,
            title="wrong role",
            description="cannot switch",
        )


@pytest.mark.asyncio
async def test_startup_aborts_on_persisted_role_mismatch(db_factory):
    await bind_task_id_namespace_at_startup(
        db_factory,
        node_role="manager",
    )

    with pytest.raises(TaskIdNamespaceError, match="already bound"):
        await bind_task_id_namespace_at_startup(
            db_factory,
            node_role="worker",
        )

    async with db_factory() as db:
        allocator = await db.get(TaskIdAllocator, 1)
        assert allocator is not None
        assert allocator.node_role == "manager"


@pytest.mark.asyncio
async def test_startup_detects_old_worker_with_missing_role_env(db_factory):
    async with db_factory() as db:
        db.add(Task(
            id=203,
            title="old Worker mirror",
            description="role env was not written yet",
            metadata_=_worker_mirror_metadata(),
        ))
        await db.commit()

    with pytest.raises(TaskIdNamespaceError, match="appears to belong to a Worker"):
        await bind_task_id_namespace_at_startup(
            db_factory,
            node_role="manager",
        )

    async with db_factory() as db:
        allocator = await db.get(TaskIdAllocator, 1)
        assert allocator is not None
        assert allocator.node_role is None


@pytest.mark.asyncio
async def test_manager_upgrade_accepts_local_user_skill_snapshots(db_factory):
    """A Skill snapshot alone is not proof that the DB belongs to a Worker."""

    async with db_factory() as db:
        db.add(Task(
            id=204,
            title="manager local skill task",
            description="ordinary local task with a frozen skill",
            metadata_={"ccm_user_skill_snapshots": []},
        ))
        await db.commit()

    await bind_task_id_namespace_at_startup(
        db_factory,
        node_role="manager",
    )

    async with db_factory() as db:
        allocator = await db.get(TaskIdAllocator, 1)
        assert allocator is not None
        assert allocator.node_role == "manager"


@pytest.mark.asyncio
async def test_worker_upgrade_accepts_legacy_skill_snapshot_mirror(db_factory):
    """Explicit Worker configuration retains the pre-marker compatibility."""

    async with db_factory() as db:
        db.add(Task(
            id=205,
            title="legacy worker mirror",
            description="predates the dedicated mirror marker",
            metadata_={"ccm_user_skill_snapshots": []},
        ))
        await db.commit()

    await bind_task_id_namespace_at_startup(
        db_factory,
        node_role="worker",
    )

    async with db_factory() as db:
        allocator = await db.get(TaskIdAllocator, 1)
        assert allocator is not None
        assert allocator.node_role == "worker"


@pytest.mark.asyncio
async def test_worker_upgrade_rejects_unproven_low_local_task(
    db_session: AsyncSession,
    monkeypatch,
):
    legacy = Task(title="legacy local", description="ambiguous owner")
    db_session.add(legacy)
    await db_session.commit()
    assert legacy.id < TASK_ID_WORKER_NAMESPACE_START
    monkeypatch.setattr(settings, "ccm_node_role", "worker")

    with pytest.raises(TaskIdNamespaceError, match="not a proven Manager mirror"):
        await stage_task_record(
            db_session,
            title="new worker task",
            description="upgrade validation",
        )
    await db_session.rollback()
    allocator = await db_session.get(TaskIdAllocator, 1)
    assert allocator is not None
    assert allocator.node_role is None


@pytest.mark.asyncio
async def test_worker_upgrade_accepts_proven_legacy_mirrors(
    db_session: AsyncSession,
    monkeypatch,
):
    legacy = Task(
        id=101,
        title="legacy mirror",
        description="durably marked",
        metadata_=_worker_mirror_metadata(),
    )
    db_session.add(legacy)
    await db_session.commit()
    monkeypatch.setattr(settings, "ccm_node_role", "worker")

    child = await stage_task_record(
        db_session,
        title="new local child",
        description="safe high range",
    )
    await db_session.commit()
    assert child.id == TASK_ID_WORKER_NAMESPACE_START


@pytest.mark.asyncio
@pytest.mark.parametrize("node_role", ["manager", "worker"])
async def test_role_claim_rejects_legacy_reserved_high_id(
    db_session: AsyncSession,
    monkeypatch,
    node_role: str,
):
    legacy = Task(
        id=TASK_ID_WORKER_NAMESPACE_START,
        title="pre-protocol high row",
        description="ambiguous allocator state",
    )
    db_session.add(legacy)
    await db_session.commit()
    monkeypatch.setattr(settings, "ccm_node_role", node_role)

    with pytest.raises(TaskIdNamespaceError, match="reserved Worker-local"):
        await stage_task_record(
            db_session,
            title="claim role",
            description="must reject legacy high row",
        )


@pytest.mark.asyncio
async def test_sqlite_file_database_serializes_worker_allocations(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    database_path = tmp_path / "worker-task-ids.sqlite3"
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{database_path}",
        connect_args={"timeout": 10},
    )
    factory = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        async with factory() as db:
            await stage_task_record(
                db,
                id=11,
                title="mirror",
                description="claims worker role",
                metadata_=_worker_mirror_metadata(),
            )
            await db.commit()

        async def create_child(index: int) -> int:
            async with factory() as db:
                child = await stage_task_record(
                    db,
                    title=f"child {index}",
                    description="concurrent allocation",
                )
                await db.commit()
                return child.id

        allocated = await asyncio.gather(create_child(1), create_child(2))
        assert sorted(allocated) == [
            TASK_ID_WORKER_NAMESPACE_START,
            TASK_ID_WORKER_NAMESPACE_START + 1,
        ]
        async with factory() as db:
            ids = list(
                (
                    await db.scalars(
                        select(Task.id).where(
                            Task.id >= TASK_ID_WORKER_NAMESPACE_START
                        )
                    )
                ).all()
            )
        assert sorted(ids) == sorted(allocated)
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    "dialect",
    [sqlite.dialect(), postgresql.dialect(), mysql.dialect()],
)
def test_allocator_table_and_atomic_update_compile_for_all_dialects(dialect):
    table_ddl = str(CreateTable(TaskIdAllocator.__table__).compile(dialect=dialect))
    statement = (
        update(TaskIdAllocator)
        .where(
            TaskIdAllocator.id == 1,
            TaskIdAllocator.node_role == "worker",
            TaskIdAllocator.next_worker_task_id < TASK_ID_SIGNED_INT_MAX,
        )
        .values(
            next_worker_task_id=TaskIdAllocator.next_worker_task_id + 1
        )
    )
    update_sql = str(statement.compile(dialect=dialect))

    assert "task_id_allocators" in table_ddl
    assert "next_worker_task_id" in table_ddl
    assert "task_id_allocators" in update_sql
    assert "next_worker_task_id" in update_sql


def test_allocator_mysql_table_is_transactional_innodb():
    ddl = str(
        CreateTable(TaskIdAllocator.__table__).compile(dialect=mysql.dialect())
    )
    assert "ENGINE=InnoDB" in ddl


@pytest.mark.parametrize(
    ("config", "message"),
    [
        ({}, "protocol"),
        (
            {
                "task_id_namespace_protocol": TASK_ID_NAMESPACE_PROTOCOL,
                "ccm_node_role": "manager",
                "task_id_namespace_boundary": TASK_ID_WORKER_NAMESPACE_START,
            },
            "CCM_NODE_ROLE=worker",
        ),
        (
            {
                "task_id_namespace_protocol": TASK_ID_NAMESPACE_PROTOCOL,
                "ccm_node_role": "worker",
                "task_id_namespace_boundary": 999,
            },
            "boundary",
        ),
    ],
)
def test_worker_namespace_config_rejects_mixed_or_wrong_nodes(config, message):
    with pytest.raises(TaskIdNamespaceProtocolError, match=message):
        validate_worker_task_id_namespace_config(config)


def test_worker_namespace_config_accepts_exact_protocol():
    validate_worker_task_id_namespace_config({
        "task_id_namespace_protocol": TASK_ID_NAMESPACE_PROTOCOL,
        "ccm_node_role": "worker",
        "task_id_namespace_boundary": TASK_ID_WORKER_NAMESPACE_START,
    })
