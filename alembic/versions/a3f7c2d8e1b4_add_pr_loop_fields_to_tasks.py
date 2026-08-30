"""add PR Loop fields to tasks

Revision ID: a3f7c2d8e1b4
Revises: 2b8d4f6a1c90
Create Date: 2026-08-10
"""
from alembic import op
import sqlalchemy as sa

revision = "a3f7c2d8e1b4"
down_revision = "2b8d4f6a1c90"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("tasks", schema=None) as batch_op:
        batch_op.add_column(sa.Column("pr_loop_url", sa.String(500), nullable=True))
        batch_op.add_column(sa.Column("pr_loop_number", sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column("pr_loop_repo", sa.String(200), nullable=True))
        batch_op.add_column(sa.Column("pr_loop_state", sa.String(30), nullable=True))
        batch_op.add_column(sa.Column("pr_loop_max_turns", sa.Integer(), server_default="10", nullable=False))
        batch_op.add_column(sa.Column("pr_loop_turns_used", sa.Integer(), server_default="0", nullable=False))
        batch_op.add_column(sa.Column("pr_loop_poll_interval", sa.Integer(), server_default="60", nullable=False))


def downgrade() -> None:
    with op.batch_alter_table("tasks", schema=None) as batch_op:
        batch_op.drop_column("pr_loop_poll_interval")
        batch_op.drop_column("pr_loop_turns_used")
        batch_op.drop_column("pr_loop_max_turns")
        batch_op.drop_column("pr_loop_state")
        batch_op.drop_column("pr_loop_repo")
        batch_op.drop_column("pr_loop_number")
        batch_op.drop_column("pr_loop_url")
