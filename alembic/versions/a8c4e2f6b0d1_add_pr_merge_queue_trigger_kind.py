"""add PR Merge Queue trigger kind

Revision ID: a8c4e2f6b0d1
Revises: fa1c2e3d4b5f
"""

from alembic import op
import sqlalchemy as sa


revision = "a8c4e2f6b0d1"
down_revision = "fa1c2e3d4b5f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "pr_merge_queue_actions",
        sa.Column(
            "trigger_kind",
            sa.String(length=20),
            nullable=False,
            server_default="policy",
        ),
    )


def downgrade() -> None:
    op.drop_column("pr_merge_queue_actions", "trigger_kind")
