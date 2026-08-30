"""add runtime capacity override

Revision ID: a4d8e2f6b1c3
Revises: d3c8a7f1e620
Create Date: 2026-08-09 06:20:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "a4d8e2f6b1c3"
down_revision: Union[str, None] = "d3c8a7f1e620"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("global_settings", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("max_concurrent_instances", sa.Integer(), nullable=True)
        )


def downgrade() -> None:
    with op.batch_alter_table("global_settings", schema=None) as batch_op:
        batch_op.drop_column("max_concurrent_instances")
