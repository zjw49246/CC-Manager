"""remove Agent sandbox unrestricted setting

Revision ID: a7d3f9c2e610
Revises: a7d4e9c2f610
Create Date: 2026-08-13

The revision is deliberately replayable.  SQLite needs an actual write before
its first DDL statement so pysqlite starts a real transaction; MySQL-family
DDL commits independently of Alembic's version-row update, so both the old and
new reflected shapes are accepted while every foreign shape is rejected.
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op


revision: str = "a7d3f9c2e610"
down_revision: Union[str, None] = "a7d4e9c2f610"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_TABLE = "global_settings"
_COLUMN = "agent_sandbox_unrestricted_enabled"
_PREVIEW_PROFILE_SIBLING_REVISION = "b8e4d2f6a1c9"


def _is_offline() -> bool:
    return bool(op.get_context().as_sql)


def _acquire_sqlite_writer_fence(*, downgrade: bool) -> None:
    if op.get_bind().dialect.name != "sqlite" or _is_offline():
        return
    expected_revisions = (
        (revision,)
        if downgrade
        else (down_revision, _PREVIEW_PROFILE_SIBLING_REVISION)
    )
    fenced = op.get_bind().execute(
        sa.text(
            "UPDATE alembic_version SET version_num = version_num "
            "WHERE version_num IN :expected_revisions"
        ).bindparams(
            sa.bindparam("expected_revisions", expanding=True)
        ),
        {"expected_revisions": expected_revisions},
    )
    if fenced.rowcount != 1:
        raise RuntimeError(
            "Agent sandbox setting migration could not acquire its SQLite "
            "revision writer fence"
        )


def _is_boolean_type(reflected: sa.types.TypeEngine) -> bool:
    if isinstance(reflected, sa.Boolean):
        return True
    return (
        type(reflected).__name__.upper() in {"BOOLEAN", "TINYINT"}
        and getattr(reflected, "display_width", 1) in {None, 1}
    )


def _column_present() -> bool:
    columns = {
        str(column["name"]).lower(): column
        for column in sa.inspect(op.get_bind()).get_columns(_TABLE)
    }
    column = columns.get(_COLUMN)
    if column is None:
        return False
    if (
        not _is_boolean_type(column["type"])
        or not bool(column["nullable"])
        or column.get("default") is not None
    ):
        raise RuntimeError(
            "Agent sandbox unrestricted setting column has a foreign shape"
        )
    return True


def _drop_column() -> None:
    with op.batch_alter_table(_TABLE, schema=None) as batch_op:
        batch_op.drop_column(_COLUMN)


def _add_column() -> None:
    with op.batch_alter_table(_TABLE, schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(_COLUMN, sa.Boolean(), nullable=True)
        )


def upgrade() -> None:
    if _is_offline():
        if op.get_bind().dialect.name == "sqlite":
            raise RuntimeError(
                "Agent sandbox setting migration requires online SQLite "
                "batch reflection"
            )
        _drop_column()
        return
    _acquire_sqlite_writer_fence(downgrade=False)
    if _column_present():
        _drop_column()
    if _column_present():
        raise RuntimeError(
            "Agent sandbox unrestricted setting column removal is incomplete"
        )


def downgrade() -> None:
    if _is_offline():
        if op.get_bind().dialect.name == "sqlite":
            raise RuntimeError(
                "Agent sandbox setting migration requires online SQLite "
                "batch reflection"
            )
        _add_column()
        return
    _acquire_sqlite_writer_fence(downgrade=True)
    if not _column_present():
        _add_column()
    if not _column_present():
        raise RuntimeError(
            "Agent sandbox unrestricted setting column restore is incomplete"
        )
