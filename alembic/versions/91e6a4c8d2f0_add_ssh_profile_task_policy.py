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
            # MySQL 8.0.13+ accepts JSON defaults only as expressions.
            server_default=sa.text("('[]')"),
            nullable=False,
        ))

    # Existing Profiles may point at arbitrary administrator-selected paths.
    # Keep them Files-only: Task eligibility requires re-authorizing the key
    # through CCM's stable managed storage root, which a running sandbox always
    # denies even when a Profile is created after its launch snapshot.


def downgrade() -> None:
    with op.batch_alter_table("ssh_profiles") as batch_op:
        batch_op.drop_column("task_capabilities")
        batch_op.drop_column("task_access_enabled")
