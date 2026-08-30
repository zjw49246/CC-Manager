"""generalize monitor tables into sub-agent tables

monitor_sessions -> sub_agent_sessions（+ agent_type / source / meta 列）
monitor_checks   -> sub_agent_reports（monitor_session_id -> session_id）

子 agent 成为分类别的一等概念：monitor 只是 agent_type 的一个取值，
模型原生子 agent（native-agent / native-monitor）与后续类别复用同两张表。

Revision ID: a9c2e1f0b3d4
Revises: b8e2f3a4c5d6
Create Date: 2026-06-12

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a9c2e1f0b3d4'
down_revision: Union[str, None] = 'b8e2f3a4c5d6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.rename_table('monitor_sessions', 'sub_agent_sessions')
    with op.batch_alter_table('sub_agent_sessions', schema=None) as batch_op:
        batch_op.add_column(
            sa.Column('agent_type', sa.String(length=50), nullable=False,
                      server_default='monitor')
        )
        batch_op.add_column(
            sa.Column('source', sa.String(length=20), nullable=False,
                      server_default='ccm')
        )
        batch_op.add_column(sa.Column('meta', sa.Text(), nullable=True))
        batch_op.drop_index('ix_monitor_sessions_task_id')
        batch_op.create_index(
            'ix_sub_agent_sessions_task_id', ['task_id'], unique=False
        )

    op.rename_table('monitor_checks', 'sub_agent_reports')
    # 索引先删再改名（batch 模式按声明顺序对旧表反射，列改名后旧索引列名失配）
    with op.batch_alter_table('sub_agent_reports', schema=None) as batch_op:
        batch_op.drop_index('ix_monitor_checks_monitor_session_id')
        # MySQL's CHANGE/MODIFY syntax always requires the existing type even
        # when the operation only renames a column.
        batch_op.alter_column(
            'monitor_session_id',
            existing_type=sa.Integer(),
            new_column_name='session_id',
        )
    with op.batch_alter_table('sub_agent_reports', schema=None) as batch_op:
        batch_op.create_index(
            'ix_sub_agent_reports_session_id', ['session_id'], unique=False
        )


def downgrade() -> None:
    with op.batch_alter_table('sub_agent_reports', schema=None) as batch_op:
        batch_op.drop_index('ix_sub_agent_reports_session_id')
        batch_op.alter_column(
            'session_id',
            existing_type=sa.Integer(),
            new_column_name='monitor_session_id',
        )
    with op.batch_alter_table('sub_agent_reports', schema=None) as batch_op:
        batch_op.create_index(
            'ix_monitor_checks_monitor_session_id',
            ['monitor_session_id'], unique=False,
        )
    op.rename_table('sub_agent_reports', 'monitor_checks')

    with op.batch_alter_table('sub_agent_sessions', schema=None) as batch_op:
        batch_op.drop_index('ix_sub_agent_sessions_task_id')
        batch_op.drop_column('meta')
        batch_op.drop_column('source')
        batch_op.drop_column('agent_type')
    with op.batch_alter_table('sub_agent_sessions', schema=None) as batch_op:
        batch_op.create_index(
            'ix_monitor_sessions_task_id', ['task_id'], unique=False
        )
    op.rename_table('sub_agent_sessions', 'monitor_sessions')
