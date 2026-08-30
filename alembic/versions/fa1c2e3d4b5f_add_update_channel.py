"""add per-instance update channel

Revision ID: fa1c2e3d4b5f
Revises: f9b2c4d6e8a1, f5d7b9e1c3a4
"""

from alembic import op
import sqlalchemy as sa


revision = "fa1c2e3d4b5f"
down_revision = ("f9b2c4d6e8a1", "f5d7b9e1c3a4")
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "global_settings",
        sa.Column(
            "update_channel",
            sa.String(length=20),
            nullable=False,
            server_default="stable",
        ),
    )


def downgrade() -> None:
    op.drop_column("global_settings", "update_channel")
