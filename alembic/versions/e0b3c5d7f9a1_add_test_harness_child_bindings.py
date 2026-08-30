"""add durable test harness child bindings

Revision ID: e0b3c5d7f9a1
Revises: d9a2b4c6e8f1
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "e0b3c5d7f9a1"
down_revision: Union[str, Sequence[str], None] = "d9a2b4c6e8f1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "test_harness_child_bindings",
        sa.Column("id", sa.String(length=32), nullable=False),
        sa.Column("harness_run_id", sa.String(length=32), nullable=True),
        sa.Column("workspace_review_run_id", sa.String(length=32), nullable=True),
        sa.Column("owner_task_id", sa.Integer(), nullable=False),
        sa.Column("child_task_id", sa.Integer(), nullable=False),
        sa.Column("browser_review_job_id", sa.String(length=32), nullable=False),
        sa.Column("state", sa.String(length=24), nullable=False),
        sa.Column("claimed_retry_count", sa.Integer(), nullable=True),
        sa.Column("claimed_instance_id", sa.Integer(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("activated_at", sa.DateTime(), nullable=True),
        sa.Column("stop_requested_at", sa.DateTime(), nullable=True),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.CheckConstraint(
            "harness_run_id IS NOT NULL OR workspace_review_run_id IS NOT NULL",
            name="ck_test_harness_child_binding_owner",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("browser_review_job_id"),
        sa.UniqueConstraint("child_task_id"),
        sa.UniqueConstraint("harness_run_id"),
        sa.UniqueConstraint("workspace_review_run_id"),
        mysql_engine="InnoDB",
    )
    for name, column in (
        ("ix_test_harness_child_bindings_harness_run_id", "harness_run_id"),
        (
            "ix_test_harness_child_bindings_workspace_review_run_id",
            "workspace_review_run_id",
        ),
        ("ix_test_harness_child_bindings_owner_task_id", "owner_task_id"),
        ("ix_test_harness_child_bindings_child_task_id", "child_task_id"),
        (
            "ix_test_harness_child_bindings_browser_review_job_id",
            "browser_review_job_id",
        ),
        ("ix_test_harness_child_bindings_state", "state"),
    ):
        op.create_index(name, "test_harness_child_bindings", [column], unique=False)


def downgrade() -> None:
    op.drop_table("test_harness_child_bindings")
