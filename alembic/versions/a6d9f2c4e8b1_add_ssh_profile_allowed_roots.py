"""add SSH profile allowed remote roots

Revision ID: a6d9f2c4e8b1
Revises: 91e6a4c8d2f0
"""

from alembic import op
import sqlalchemy as sa


revision = "a6d9f2c4e8b1"
down_revision = "91e6a4c8d2f0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("ssh_profiles") as batch_op:
        batch_op.add_column(sa.Column(
            "allowed_roots",
            sa.JSON(),
            server_default='["/"]',
            nullable=False,
        ))


def downgrade() -> None:
    with op.batch_alter_table("ssh_profiles") as batch_op:
        batch_op.drop_column("allowed_roots")
