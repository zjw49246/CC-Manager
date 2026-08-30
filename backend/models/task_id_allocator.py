"""Durable allocator state for the split Manager/Worker Task-id namespace."""

from sqlalchemy import CheckConstraint, Integer, String, event
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


TASK_ID_ALLOCATOR_SINGLETON_ID = 1
TASK_ID_WORKER_NAMESPACE_START = 1_000_000_000
TASK_ID_SIGNED_INT_MAX = 2_147_483_647


class TaskIdAllocator(Base):
    """One row that binds a database to a node role and Worker-local range.

    Manager-owned Task ids stay below ``TASK_ID_WORKER_NAMESPACE_START`` and
    continue to use the native ``tasks.id`` allocator.  A Worker database uses
    this transactional row for every Task it creates locally, while explicit
    Manager mirrors retain their original low-range id.
    """

    __tablename__ = "task_id_allocators"
    __table_args__ = (
        CheckConstraint(
            "id = 1",
            name="ck_task_id_allocators_singleton",
        ),
        CheckConstraint(
            "node_role IS NULL OR node_role IN ('manager', 'worker')",
            name="ck_task_id_allocators_node_role",
        ),
        CheckConstraint(
            "next_worker_task_id >= 1000000000 "
            "AND next_worker_task_id <= 2147483647",
            name="ck_task_id_allocators_worker_range",
        ),
        {"mysql_engine": "InnoDB"},
    )

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=False,
        default=TASK_ID_ALLOCATOR_SINGLETON_ID,
    )
    # NULL is the upgrade state.  The first canonical Task creation validates
    # legacy rows, then atomically and durably claims the configured node role.
    node_role: Mapped[str | None] = mapped_column(String(16), nullable=True)
    # Stores the next free high-range id.  2^31-1 is the exhausted sentinel,
    # so every allocated Task id remains representable by all three dialects.
    next_worker_task_id: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=TASK_ID_WORKER_NAMESPACE_START,
        server_default=str(TASK_ID_WORKER_NAMESPACE_START),
    )


@event.listens_for(TaskIdAllocator.__table__, "after_create")
def _seed_task_id_allocator(target, connection, **_kwargs) -> None:
    """Keep ``Base.metadata.create_all`` test/dev databases canonical.

    Production schema creation is Alembic-owned and seeds the same row in its
    migration.  This hook exists because the test suite intentionally creates
    model metadata directly rather than replaying every migration.
    """

    connection.execute(
        target.insert().values(
            id=TASK_ID_ALLOCATOR_SINGLETON_ID,
            node_role=None,
            next_worker_task_id=TASK_ID_WORKER_NAMESPACE_START,
        )
    )
