"""add ssh profile task exposure policy

Revision ID: 91e6a4c8d2f0
Revises: 84d5b0f2c3e1
"""

from alembic import op
import sqlalchemy as sa


revision = "91e6a4c8d2f0"
down_revision = "84d5b0f2c3e1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("ssh_profiles") as batch_op:
        batch_op.add_column(sa.Column(
            "task_access_enabled",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ))
        batch_op.add_column(sa.Column(
            "task_capabilities",
            sa.JSON(),
            server_default="[]",
            nullable=False,
        ))

    # Managed profiles created before this policy existed were all eligible
    # for Task grants. Preserve that behavior during upgrade; newly created
    # profiles default to Files-only in the API and model.
    profiles = sa.table(
        "ssh_profiles",
        sa.column("task_access_enabled", sa.Boolean()),
        sa.column("task_capabilities", sa.JSON()),
    )
    op.execute(profiles.update().values(
        task_access_enabled=True,
        task_capabilities=["exec", "read", "write"],
    ))


def downgrade() -> None:
    with op.batch_alter_table("ssh_profiles") as batch_op:
        batch_op.drop_column("task_capabilities")
        batch_op.drop_column("task_access_enabled")
