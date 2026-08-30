"""rename disable_workflows to enable_workflows

Revision ID: 0aa352edf132
Revises: 2d287d9865fc
Create Date: 2026-06-08 19:14:39.145538

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0aa352edf132'
down_revision: Union[str, None] = '2d287d9865fc'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table('tasks', schema=None) as batch_op:
        batch_op.add_column(sa.Column(
            'enable_workflows',
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ))

    # Boolean columns are integers on SQLite but native BOOLEAN on PostgreSQL.
    # ``NOT`` preserves the inversion semantics on all supported dialects.
    op.execute("UPDATE tasks SET enable_workflows = NOT disable_workflows")

    with op.batch_alter_table('tasks', schema=None) as batch_op:
        batch_op.drop_column('disable_workflows')


def downgrade() -> None:
    with op.batch_alter_table('tasks', schema=None) as batch_op:
        batch_op.add_column(sa.Column(
            'disable_workflows',
            sa.Boolean(),
            server_default=sa.true(),
            nullable=False,
        ))

    op.execute("UPDATE tasks SET disable_workflows = NOT enable_workflows")

    with op.batch_alter_table('tasks', schema=None) as batch_op:
        batch_op.drop_column('enable_workflows')
