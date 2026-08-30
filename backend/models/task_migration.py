"""Durable ownership for one exact in-flight Task migration."""

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


MANAGER_ACTIVE_TASK_MIGRATION_PHASES = (
    "claimed",
    "remote_prepared",
    "rollback_pending",
    "committed_pending_ack",
)
WORKER_ACTIVE_TASK_MIGRATION_PHASES = ("prepared",)
ACTIVE_TASK_MIGRATION_PHASES = (
    *MANAGER_ACTIVE_TASK_MIGRATION_PHASES,
    *WORKER_ACTIVE_TASK_MIGRATION_PHASES,
)
TERMINAL_TASK_MIGRATION_PHASES = ("committed", "rolled_back")
TASK_MIGRATION_PHASES = (
    *ACTIVE_TASK_MIGRATION_PHASES,
    *TERMINAL_TASK_MIGRATION_PHASES,
)
TASK_MIGRATION_SIDES = ("manager", "worker")
TASK_MIGRATION_SOURCE_STATUSES = (
    "cancelled",
    "completed",
    "conflict",
    "failed",
    "plan_review",
)


def _sql_values(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def _lower_hex_check(column: str) -> str:
    remainder = column
    for character in "0123456789abcdef":
        remainder = f"REPLACE({remainder}, '{character}', '')"
    return f"LENGTH({column}) = 32 AND {remainder} = ''"


class TaskMigrationOperation(Base):
    """Crash-replayable ownership of one exact Task move between nodes.

    Manager and Worker databases are independent, so the row deliberately has
    no Task, Worker, or Instance foreign keys.  Deleting any of those records
    must not erase evidence that a remote prepare/commit may already exist.
    """

    __tablename__ = "task_migration_operations"
    __table_args__ = (
        UniqueConstraint(
            "active_task_id",
            name="uq_task_migration_active_task",
        ),
        UniqueConstraint(
            "task_id",
            "operation_sequence",
            name="uq_task_migration_task_sequence",
        ),
        CheckConstraint(
            _lower_hex_check("operation_id"),
            name="ck_task_migration_operation_id_hex",
        ),
        CheckConstraint(
            _lower_hex_check("task_incarnation_id"),
            name="ck_task_migration_incarnation_hex",
        ),
        CheckConstraint(
            f"phase IN ({_sql_values(TASK_MIGRATION_PHASES)})",
            name="ck_task_migration_phase",
        ),
        CheckConstraint(
            "((phase IN ("
            f"{_sql_values(ACTIVE_TASK_MIGRATION_PHASES)}) "
            "AND active_task_id IS NOT NULL "
            "AND active_task_id = task_id) OR "
            "(phase IN ("
            f"{_sql_values(TERMINAL_TASK_MIGRATION_PHASES)}) "
            "AND active_task_id IS NULL))",
            name="ck_task_migration_active_slot",
        ),
        CheckConstraint(
            "operation_sequence > 0 AND retry_count >= 0 "
            "AND turn_generation >= 0",
            name="ck_task_migration_generation",
        ),
        CheckConstraint(
            "task_id > 0 AND (instance_id IS NULL OR instance_id > 0) "
            "AND (source_worker_id IS NULL OR source_worker_id > 0) "
            "AND (target_worker_id IS NULL OR target_worker_id > 0)",
            name="ck_task_migration_identity",
        ),
        CheckConstraint(
            "source_status IN ("
            f"{_sql_values(TASK_MIGRATION_SOURCE_STATUSES)})",
            name="ck_task_migration_source_status",
        ),
        CheckConstraint(
            "(side = 'worker' AND source_worker_id IS NULL "
            "AND target_worker_id IS NULL) OR "
            "(side = 'manager' AND ("
            "(source_worker_id IS NULL AND target_worker_id IS NOT NULL) OR "
            "(source_worker_id IS NOT NULL AND target_worker_id IS NULL) OR "
            "(source_worker_id IS NOT NULL AND target_worker_id IS NOT NULL "
            "AND source_worker_id <> target_worker_id)))",
            name="ck_task_migration_route",
        ),
        CheckConstraint(
            "(side = 'manager' AND phase IN ("
            f"{_sql_values((*MANAGER_ACTIVE_TASK_MIGRATION_PHASES, *TERMINAL_TASK_MIGRATION_PHASES))})) "
            "OR (side = 'worker' AND phase IN ("
            f"{_sql_values((*WORKER_ACTIVE_TASK_MIGRATION_PHASES, *TERMINAL_TASK_MIGRATION_PHASES))}))",
            name="ck_task_migration_side_phase",
        ),
        {"mysql_engine": "InnoDB"},
    )

    operation_id: Mapped[str] = mapped_column(
        String(32),
        primary_key=True,
        nullable=False,
    )
    operation_sequence: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
    )
    side: Mapped[str] = mapped_column(String(16), nullable=False)
    active_task_id: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
    )
    task_id: Mapped[int] = mapped_column(Integer, nullable=False)
    task_incarnation_id: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
    )
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False)
    turn_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    source_worker_id: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
    )
    target_worker_id: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
    )
    source_status: Mapped[str] = mapped_column(String(20), nullable=False)
    phase: Mapped[str] = mapped_column(String(32), nullable=False)
    instance_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime,
        nullable=True,
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime,
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
    )
