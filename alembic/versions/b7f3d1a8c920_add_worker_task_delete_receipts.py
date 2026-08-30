"""allow durable Manager-side Worker Task deletion receipts

Revision ID: b7f3d1a8c920
Revises: a6e4c2d9f810
Create Date: 2026-08-08
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "b7f3d1a8c920"
down_revision: Union[str, None] = "a6e4c2d9f810"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_TABLE = "worker_task_termination_receipts"
_OPERATION_CONSTRAINT = "ck_worker_task_term_operation"
_SOURCE_STATUS_CONSTRAINT = "ck_worker_task_term_source_status"
_DELETE_SIDE_CONSTRAINT = "ck_worker_task_term_delete_manager_only"
_OLD_OPERATIONS = "operation IN ('cancel', 'stop_session', 'supersede')"
_NEW_OPERATIONS = (
    "operation IN ('cancel', 'stop_session', 'supersede', 'delete')"
)
_OLD_SOURCE_STATUSES = (
    "source_task_status IN ('pending', 'in_progress', 'executing', "
    "'plan_review', 'merging', 'migrating', 'completed', 'failed', "
    "'cancelled', 'conflict')"
)
_NEW_SOURCE_STATUSES = (
    "source_task_status IN ('pending', 'in_progress', 'executing', "
    "'plan_review', 'merging', 'migrating', 'completed', 'failed', "
    "'cancelled', 'conflict', 'superseded')"
)
_DELETE_MANAGER_ONLY = "operation <> 'delete' OR side = 'manager'"


def _replace_constraints(*, upgrade: bool) -> None:
    # SQLite cannot alter a named CHECK in place.  Alembic batch mode rebuilds
    # the table while retaining every other constraint and index.  The same
    # operation is valid on PostgreSQL/MySQL and keeps the migration path
    # uniform across supported databases.
    with op.batch_alter_table(_TABLE, recreate="auto") as batch_op:
        batch_op.drop_constraint(_OPERATION_CONSTRAINT, type_="check")
        batch_op.drop_constraint(_SOURCE_STATUS_CONSTRAINT, type_="check")
        if not upgrade:
            batch_op.drop_constraint(_DELETE_SIDE_CONSTRAINT, type_="check")
        batch_op.create_check_constraint(
            _OPERATION_CONSTRAINT,
            _NEW_OPERATIONS if upgrade else _OLD_OPERATIONS,
        )
        batch_op.create_check_constraint(
            _SOURCE_STATUS_CONSTRAINT,
            _NEW_SOURCE_STATUSES if upgrade else _OLD_SOURCE_STATUSES,
        )
        if upgrade:
            batch_op.create_check_constraint(
                _DELETE_SIDE_CONSTRAINT,
                _DELETE_MANAGER_ONLY,
            )


def upgrade() -> None:
    _replace_constraints(upgrade=True)


def downgrade() -> None:
    bind = op.get_bind()
    incompatible = bind.execute(
        sa.text(
            f"SELECT COUNT(*) FROM {_TABLE} "
            "WHERE operation = 'delete' "
            "OR source_task_status = 'superseded'"
        )
    ).scalar_one()
    if incompatible:
        raise RuntimeError(
            "Cannot downgrade while Worker Task termination receipts use "
            "delete operations or superseded source status"
        )
    _replace_constraints(upgrade=False)
