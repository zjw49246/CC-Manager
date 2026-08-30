"""add Agent sandbox runtime override

Revision ID: e6a2c4f8b190
Revises: c5e7a9d1f3b6
Create Date: 2026-08-12
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op


revision: str = "e6a2c4f8b190"
down_revision: Union[str, None] = "c5e7a9d1f3b6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("global_settings", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "agent_sandbox_unrestricted_enabled",
                sa.Boolean(),
                nullable=True,
            )
        )


def downgrade() -> None:
    with op.batch_alter_table("global_settings", schema=None) as batch_op:
        batch_op.drop_column("agent_sandbox_unrestricted_enabled")
