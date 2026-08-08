"""add immutable resolved Git target to Test Harness runs

Revision ID: d9a2b4c6e8f1
Revises: c8f1a2d4e6b9
Create Date: 2026-08-07
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "d9a2b4c6e8f1"
down_revision: Union[str, None] = "c8f1a2d4e6b9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("test_harness_runs", schema=None) as batch_op:
        batch_op.add_column(sa.Column("resolved_target", sa.JSON(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("test_harness_runs", schema=None) as batch_op:
        batch_op.drop_column("resolved_target")
