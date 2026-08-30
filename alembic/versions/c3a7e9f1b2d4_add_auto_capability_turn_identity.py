"""add exact-turn identity for Auto capability requests

Revision ID: c3a7e9f1b2d4
Revises: 9e5b2a7c4d10
Create Date: 2026-08-06
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "c3a7e9f1b2d4"
down_revision: Union[str, None] = "9e5b2a7c4d10"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _sqlite_task_id_highwater() -> int | None:
    context = op.get_context()
    bind = op.get_bind()
    if context.as_sql or bind.dialect.name != "sqlite":
        return None
    value = bind.execute(
        sa.text("SELECT seq FROM sqlite_sequence WHERE name = 'tasks'")
    ).scalar_one_or_none()
    return int(value) if value is not None else None


def _restore_sqlite_task_id_highwater(highwater: int | None) -> None:
    if highwater is None:
        return
    bind = op.get_bind()
    advanced = bind.execute(
        sa.text(
            "UPDATE sqlite_sequence "
            "SET seq = CASE WHEN seq < :highwater THEN :highwater ELSE seq END "
            "WHERE name = 'tasks'"
        ),
        {"highwater": highwater},
    )
    if advanced.rowcount == 0:
        bind.execute(
            sa.text(
                "INSERT INTO sqlite_sequence(name, seq) "
                "VALUES ('tasks', :highwater)"
            ),
            {"highwater": highwater},
        )


def _task_batch_kwargs() -> dict:
    """Preserve the external Task-id non-reuse guarantee on SQLite."""

    if op.get_bind().dialect.name != "sqlite":
        return {}
    return {
        "recreate": "always",
        "table_kwargs": {"sqlite_autoincrement": True},
    }


def upgrade() -> None:
    task_id_highwater = _sqlite_task_id_highwater()
    with op.batch_alter_table(
        "tasks",
        schema=None,
        **_task_batch_kwargs(),
    ) as batch_op:
        batch_op.add_column(
            sa.Column(
                "turn_generation",
                sa.BigInteger(),
                server_default="0",
                nullable=False,
            )
        )
        batch_op.add_column(
            sa.Column("capability_policy", sa.JSON(), nullable=True)
        )
        batch_op.add_column(
            sa.Column("worker_turn_handoff_id", sa.String(length=32), nullable=True)
        )
        batch_op.add_column(
            sa.Column("worker_turn_handoff_worker_id", sa.Integer(), nullable=True)
        )
        batch_op.add_column(
            sa.Column("worker_turn_handoff_retry_count", sa.Integer(), nullable=True)
        )
        batch_op.add_column(
            sa.Column(
                "worker_turn_handoff_from_generation",
                sa.BigInteger(),
                nullable=True,
            )
        )
        batch_op.add_column(
            sa.Column("worker_turn_handoff_source_log_id", sa.Integer(), nullable=True)
        )
        batch_op.add_column(
            sa.Column("worker_turn_handoff_acknowledged", sa.Boolean(), nullable=True)
        )
        batch_op.create_check_constraint(
            "ck_tasks_worker_turn_handoff_shape",
            "(worker_turn_handoff_id IS NULL "
            "AND worker_turn_handoff_worker_id IS NULL "
            "AND worker_turn_handoff_retry_count IS NULL "
            "AND worker_turn_handoff_from_generation IS NULL "
            "AND worker_turn_handoff_source_log_id IS NULL "
            "AND worker_turn_handoff_acknowledged IS NULL) OR "
            "(worker_turn_handoff_id IS NOT NULL "
            "AND worker_turn_handoff_worker_id IS NOT NULL "
            "AND worker_turn_handoff_retry_count IS NOT NULL "
            "AND worker_turn_handoff_retry_count >= 0 "
            "AND worker_turn_handoff_from_generation IS NOT NULL "
            "AND worker_turn_handoff_from_generation >= 0 "
            "AND worker_turn_handoff_source_log_id IS NOT NULL "
            "AND worker_turn_handoff_acknowledged IS NOT NULL)",
        )
    _restore_sqlite_task_id_highwater(task_id_highwater)

    with op.batch_alter_table("log_entries", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("task_turn_generation", sa.BigInteger(), nullable=True)
        )
        batch_op.add_column(
            sa.Column("native_turn_id", sa.String(length=200), nullable=True)
        )

    with op.batch_alter_table(
        "capability_invocations",
        schema=None,
    ) as batch_op:
        batch_op.add_column(
            sa.Column("request_output_log_id", sa.Integer(), nullable=True)
        )
        batch_op.add_column(
            sa.Column(
                "request_native_turn_id",
                sa.String(length=200),
                nullable=True,
            )
        )
        batch_op.create_check_constraint(
            "ck_cap_inv_agent_request_identity",
            "source <> 'agent_request' OR ("
            "purpose = 'advisory' AND resume_policy = 'resume_task' "
            "AND requested_by_user_id IS NULL "
            "AND request_task_retry_count IS NOT NULL "
            "AND request_task_turn_generation IS NOT NULL "
            "AND request_source_log_id IS NOT NULL "
            "AND request_output_log_id IS NOT NULL)",
        )

    op.create_table(
        "worker_turn_handoff_receipts",
        sa.Column("handoff_id", sa.String(length=32), nullable=False),
        sa.Column("task_id", sa.Integer(), nullable=False),
        sa.Column("source_log_id", sa.Integer(), nullable=False),
        sa.Column("side", sa.String(length=16), nullable=False),
        sa.Column("worker_id", sa.Integer(), nullable=True),
        sa.Column("retry_count", sa.Integer(), nullable=False),
        sa.Column("from_generation", sa.BigInteger(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("request_payload", sa.JSON(), nullable=False),
        sa.Column("request_digest", sa.String(length=64), nullable=False),
        sa.Column("queue_payload", sa.JSON(), nullable=True),
        sa.Column("queue_payload_digest", sa.String(length=64), nullable=True),
        sa.Column("response", sa.JSON(), nullable=True),
        sa.Column("claimed_turn_generation", sa.BigInteger(), nullable=True),
        sa.Column(
            "terminal_pr_review_chat",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ),
        sa.Column("cancel_reason", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "side IN ('manager', 'worker')",
            name="ck_worker_turn_handoff_side",
        ),
        sa.CheckConstraint(
            "status IN ('prepared', 'acknowledged', 'accepted', "
            "'claimed', 'launching', 'launched', 'cancelled', 'completed')",
            name="ck_worker_turn_handoff_status",
        ),
        sa.CheckConstraint(
            "retry_count >= 0 AND from_generation >= 0",
            name="ck_worker_turn_handoff_generation",
        ),
        sa.CheckConstraint(
            "(side = 'manager' AND worker_id IS NOT NULL "
            "AND status IN ('prepared', 'acknowledged', 'cancelled', "
            "'completed') AND queue_payload IS NULL "
            "AND queue_payload_digest IS NULL) OR "
            "(side = 'worker' AND worker_id IS NULL "
            "AND status IN ('accepted', 'claimed', 'launching', 'launched', "
            "'cancelled') "
            "AND queue_payload IS NOT NULL "
            "AND queue_payload_digest IS NOT NULL "
            "AND response IS NOT NULL)",
            name="ck_worker_turn_handoff_side_shape",
        ),
        sa.CheckConstraint(
            "(status IN ('claimed', 'launching', 'launched') "
            "AND claimed_turn_generation IS NOT NULL "
            "AND claimed_turn_generation "
            "= from_generation + 1) OR "
            "(status NOT IN ('claimed', 'launching', 'launched') "
            "AND claimed_turn_generation IS NULL)",
            name="ck_worker_turn_handoff_claim",
        ),
        sa.ForeignKeyConstraint(
            ["source_log_id"], ["log_entries.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("handoff_id"),
        sa.UniqueConstraint(
            "task_id",
            "source_log_id",
            name="uq_worker_turn_handoff_task_source_log",
        ),
    )
    op.create_index(
        "ix_worker_turn_handoff_task_status",
        "worker_turn_handoff_receipts",
        ["task_id", "side", "status"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_worker_turn_handoff_task_status",
        table_name="worker_turn_handoff_receipts",
    )
    op.drop_table("worker_turn_handoff_receipts")

    with op.batch_alter_table(
        "capability_invocations",
        schema=None,
    ) as batch_op:
        batch_op.drop_constraint(
            "ck_cap_inv_agent_request_identity",
            type_="check",
        )
        batch_op.drop_column("request_native_turn_id")
        batch_op.drop_column("request_output_log_id")

    with op.batch_alter_table("log_entries", schema=None) as batch_op:
        batch_op.drop_column("native_turn_id")
        batch_op.drop_column("task_turn_generation")

    task_id_highwater = _sqlite_task_id_highwater()
    with op.batch_alter_table(
        "tasks",
        schema=None,
        **_task_batch_kwargs(),
    ) as batch_op:
        batch_op.drop_constraint(
            "ck_tasks_worker_turn_handoff_shape",
            type_="check",
        )
        batch_op.drop_column("worker_turn_handoff_acknowledged")
        batch_op.drop_column("worker_turn_handoff_source_log_id")
        batch_op.drop_column("worker_turn_handoff_from_generation")
        batch_op.drop_column("worker_turn_handoff_retry_count")
        batch_op.drop_column("worker_turn_handoff_worker_id")
        batch_op.drop_column("worker_turn_handoff_id")
        batch_op.drop_column("capability_policy")
        batch_op.drop_column("turn_generation")
    _restore_sqlite_task_id_highwater(task_id_highwater)
