"""add Delivery frontend review stage and evidence handles

Revision ID: a7d4e9c2f610
Revises: e6a2c4f8b190
Create Date: 2026-08-13
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "a7d4e9c2f610"
down_revision: Union[str, None] = "e6a2c4f8b190"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_RUN_PHASES = (
    "'planning', 'coding', 'pre_review', 'frontend_review', "
    "'publishing', 'monitoring', 'done'"
)
_LEGACY_RUN_PHASES = (
    "'planning', 'coding', 'pre_review', 'publishing', 'monitoring', 'done'"
)
_CYCLE_STATUSES = (
    "'planning', 'coding', 'pre_review', 'frontend_review', 'publishing', "
    "'completed', 'failed', 'cancelled', 'superseded'"
)
_LEGACY_CYCLE_STATUSES = (
    "'planning', 'coding', 'pre_review', 'publishing', "
    "'completed', 'failed', 'cancelled', 'superseded'"
)
_ACTIVE_CYCLE_STATUSES = (
    "'planning', 'coding', 'pre_review', 'frontend_review', 'publishing'"
)
_LEGACY_ACTIVE_CYCLE_STATUSES = (
    "'planning', 'coding', 'pre_review', 'publishing'"
)


def upgrade() -> None:
    with op.batch_alter_table("delivery_runs", schema=None) as batch:
        batch.drop_constraint("ck_delivery_runs_phase", type_="check")
        batch.create_check_constraint(
            "ck_delivery_runs_phase",
            f"phase IN ({_RUN_PHASES})",
        )

    with op.batch_alter_table("delivery_cycles", schema=None) as batch:
        batch.drop_constraint("ck_delivery_cycles_status", type_="check")
        batch.drop_constraint("ck_delivery_cycles_active_slot", type_="check")
        batch.add_column(
            sa.Column("frontend_review_run_id", sa.String(length=32), nullable=True)
        )
        batch.add_column(
            sa.Column("frontend_review_verdict", sa.String(length=24), nullable=True)
        )
        batch.add_column(
            sa.Column("frontend_review_summary", sa.Text(), nullable=True)
        )
        batch.add_column(
            sa.Column("frontend_review_skip_reason", sa.Text(), nullable=True)
        )
        batch.create_unique_constraint(
            "uq_delivery_cycles_frontend_review_run",
            ["frontend_review_run_id"],
        )
        batch.create_check_constraint(
            "ck_delivery_cycles_status",
            f"status IN ({_CYCLE_STATUSES})",
        )
        batch.create_check_constraint(
            "ck_delivery_cycles_active_slot",
            "(status IN ("
            f"{_ACTIVE_CYCLE_STATUSES}"
            ") AND active_run_id IS NOT NULL AND active_run_id = run_id) OR "
            "(status NOT IN ("
            f"{_ACTIVE_CYCLE_STATUSES}"
            ") AND active_run_id IS NULL)",
        )


def downgrade() -> None:
    bind = op.get_bind()
    active_frontend = bind.execute(
        sa.text(
            "SELECT COUNT(*) FROM delivery_runs WHERE phase = 'frontend_review'"
        )
    ).scalar_one()
    cycle_frontend = bind.execute(
        sa.text(
            "SELECT COUNT(*) FROM delivery_cycles "
            "WHERE status = 'frontend_review'"
        )
    ).scalar_one()
    if active_frontend or cycle_frontend:
        raise RuntimeError(
            "Cannot downgrade while Delivery frontend review stages are active"
        )

    with op.batch_alter_table("delivery_cycles", schema=None) as batch:
        batch.drop_constraint("ck_delivery_cycles_active_slot", type_="check")
        batch.drop_constraint("ck_delivery_cycles_status", type_="check")
        batch.drop_constraint(
            "uq_delivery_cycles_frontend_review_run",
            type_="unique",
        )
        batch.drop_column("frontend_review_skip_reason")
        batch.drop_column("frontend_review_summary")
        batch.drop_column("frontend_review_verdict")
        batch.drop_column("frontend_review_run_id")
        batch.create_check_constraint(
            "ck_delivery_cycles_status",
            f"status IN ({_LEGACY_CYCLE_STATUSES})",
        )
        batch.create_check_constraint(
            "ck_delivery_cycles_active_slot",
            "(status IN ("
            f"{_LEGACY_ACTIVE_CYCLE_STATUSES}"
            ") AND active_run_id IS NOT NULL AND active_run_id = run_id) OR "
            "(status NOT IN ("
            f"{_LEGACY_ACTIVE_CYCLE_STATUSES}"
            ") AND active_run_id IS NULL)",
        )

    with op.batch_alter_table("delivery_runs", schema=None) as batch:
        batch.drop_constraint("ck_delivery_runs_phase", type_="check")
        batch.create_check_constraint(
            "ck_delivery_runs_phase",
            f"phase IN ({_LEGACY_RUN_PHASES})",
        )
