"""add task ssh grants

Revision ID: 84d5b0f2c3e1
Revises: 73c4a9e1b2d0
"""

from alembic import op
import sqlalchemy as sa


revision = "84d5b0f2c3e1"
down_revision = "73c4a9e1b2d0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "task_ssh_grants",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("task_id", sa.Integer(), nullable=False),
        sa.Column("ssh_profile_id", sa.Integer(), nullable=False),
        sa.Column("profile_revision", sa.Integer(), nullable=False),
        sa.Column("capabilities", sa.JSON(), nullable=False),
        sa.Column("created_by", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["ssh_profile_id"], ["ssh_profiles.id"]),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "task_id",
            "ssh_profile_id",
            name="uq_task_ssh_grants_task_profile",
        ),
        mysql_engine="InnoDB",
    )
    op.create_index(
        op.f("ix_task_ssh_grants_task_id"),
        "task_ssh_grants",
        ["task_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_task_ssh_grants_ssh_profile_id"),
        "task_ssh_grants",
        ["ssh_profile_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_task_ssh_grants_ssh_profile_id"),
        table_name="task_ssh_grants",
    )
    op.drop_index(
        op.f("ix_task_ssh_grants_task_id"),
        table_name="task_ssh_grants",
    )
    op.drop_table("task_ssh_grants")
