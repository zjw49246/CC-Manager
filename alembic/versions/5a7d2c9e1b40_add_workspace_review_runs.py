"""add workspace preview configuration and durable review runs

Revision ID: 5a7d2c9e1b40
Revises: 2f6c8a1d4e90
Create Date: 2026-08-06
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "5a7d2c9e1b40"
down_revision: Union[str, None] = "2f6c8a1d4e90"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("projects", schema=None) as batch_op:
        batch_op.add_column(sa.Column("preview_config", sa.JSON(), nullable=True))

    op.create_table(
        "workspace_review_runs",
        sa.Column("id", sa.String(length=32), nullable=False),
        sa.Column("task_id", sa.Integer(), nullable=False),
        sa.Column("project_id", sa.Integer(), nullable=True),
        sa.Column("agent_task_id", sa.Integer(), nullable=True),
        sa.Column("browser_review_job_id", sa.String(length=32), nullable=True),
        sa.Column("mode", sa.String(length=20), nullable=False),
        sa.Column("profile", sa.String(length=20), nullable=False),
        sa.Column("goal", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column("stage", sa.String(length=48), nullable=False),
        sa.Column("workspace_path", sa.String(length=1000), nullable=False),
        sa.Column("git_head", sa.String(length=64), nullable=False),
        sa.Column("workspace_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("preview_config", sa.JSON(), nullable=False),
        sa.Column("preview_url", sa.String(length=2048), nullable=True),
        sa.Column("stale", sa.Boolean(), server_default="0", nullable=False),
        sa.Column("report", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("cleanup_status", sa.String(length=24), nullable=False),
        sa.Column("cleanup_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        mysql_engine="InnoDB",
    )
    op.create_index("ix_workspace_review_runs_task_id", "workspace_review_runs", ["task_id"])
    op.create_index("ix_workspace_review_runs_project_id", "workspace_review_runs", ["project_id"])
    op.create_index("ix_workspace_review_runs_agent_task_id", "workspace_review_runs", ["agent_task_id"])
    op.create_index("ix_workspace_review_runs_browser_review_job_id", "workspace_review_runs", ["browser_review_job_id"])
    op.create_index("ix_workspace_review_runs_status", "workspace_review_runs", ["status"])


def downgrade() -> None:
    op.drop_index("ix_workspace_review_runs_status", table_name="workspace_review_runs")
    op.drop_index("ix_workspace_review_runs_browser_review_job_id", table_name="workspace_review_runs")
    op.drop_index("ix_workspace_review_runs_agent_task_id", table_name="workspace_review_runs")
    op.drop_index("ix_workspace_review_runs_project_id", table_name="workspace_review_runs")
    op.drop_index("ix_workspace_review_runs_task_id", table_name="workspace_review_runs")
    op.drop_table("workspace_review_runs")
    with op.batch_alter_table("projects", schema=None) as batch_op:
        batch_op.drop_column("preview_config")
