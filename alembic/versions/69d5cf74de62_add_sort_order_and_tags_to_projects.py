"""add sort_order and tags to projects

Revision ID: 69d5cf74de62
Revises: 4236103a2c1c
Create Date: 2026-03-20 00:27:46.534760

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '69d5cf74de62'
down_revision: Union[str, None] = '4236103a2c1c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    dialect = op.get_bind().dialect
    mysql_family = dialect.name in {"mysql", "mariadb"} or bool(
        getattr(dialect, "is_mariadb", False)
    )
    with op.batch_alter_table('projects', schema=None) as batch_op:
        batch_op.add_column(sa.Column('sort_order', sa.Integer(), server_default='0', nullable=False))
        # MySQL rejects a bare literal default on JSON (error 1101).  Its
        # parenthesized JSON_ARRAY expression is the equivalent canonical
        # empty-array default; SQLite/PostgreSQL retain the historical literal.
        tags_default = (
            sa.text("(JSON_ARRAY())") if mysql_family else sa.text("'[]'")
        )
        batch_op.add_column(sa.Column(
            'tags',
            sa.JSON(),
            server_default=tags_default,
            nullable=False,
        ))
        batch_op.create_index(batch_op.f('ix_projects_sort_order'), ['sort_order'], unique=False)


def downgrade() -> None:
    with op.batch_alter_table('projects', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_projects_sort_order'))
        batch_op.drop_column('tags')
        batch_op.drop_column('sort_order')
