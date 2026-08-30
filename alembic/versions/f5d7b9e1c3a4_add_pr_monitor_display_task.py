"""add stable PR Monitor display Task identity

Revision ID: f5d7b9e1c3a4
Revises: b1d7e4a9c302
Create Date: 2026-08-17

The display Task is a read-only projection owned by PRMonitorRun.  Reviewer
Tasks remain internal execution records, so this migration only adds the
nullable one-to-one link; existing rows are backfilled by the normal startup
service after the schema is available.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "f5d7b9e1c3a4"
# Extend the current PR Monitor migration line after the existing merge and
# latest PR review evidence migration.  Keeping one Alembic head is required
# for automatic updates to prove the deployed schema revision.
down_revision: Union[str, Sequence[str], None] = "b1d7e4a9c302"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("pr_monitor_runs", schema=None) as batch:
        batch.add_column(
            sa.Column(
                "display_task_id",
                sa.Integer(),
                nullable=True,
            )
        )
        batch.create_index(
            "ix_pr_monitor_runs_display_task_id",
            ["display_task_id"],
            unique=True,
        )
        batch.create_foreign_key(
            "fk_pr_monitor_runs_display_task_id_tasks",
            "tasks",
            ["display_task_id"],
            ["id"],
        )


def downgrade() -> None:
    with op.batch_alter_table("pr_monitor_runs", schema=None) as batch:
        batch.drop_constraint(
            "fk_pr_monitor_runs_display_task_id_tasks",
            type_="foreignkey",
        )
        batch.drop_index("ix_pr_monitor_runs_display_task_id")
        batch.drop_column("display_task_id")
