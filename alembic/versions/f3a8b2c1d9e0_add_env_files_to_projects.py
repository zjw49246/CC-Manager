"""add env_files to projects

Revision ID: f3a8b2c1d9e0
Revises: 1b223e97e404
Create Date: 2026-03-25 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f3a8b2c1d9e0'
down_revision: Union[str, None] = '1b223e97e404'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    dialect = op.get_bind().dialect
    mysql_family = dialect.name in {"mysql", "mariadb"} or bool(
        getattr(dialect, "is_mariadb", False)
    )
    env_files_default = (
        sa.text("(JSON_ARRAY())") if mysql_family else sa.text("'[]'")
    )
    with op.batch_alter_table('projects', schema=None) as batch_op:
        batch_op.add_column(sa.Column(
            'env_files',
            sa.JSON(),
            server_default=env_files_default,
            nullable=False,
        ))


def downgrade() -> None:
    with op.batch_alter_table('projects', schema=None) as batch_op:
        batch_op.drop_column('env_files')
