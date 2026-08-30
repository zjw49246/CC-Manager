"""make user group membership unique

Revision ID: e1b7d9f3a5c2
Revises: d0a6c8e4f2b1
Create Date: 2026-08-14
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "e1b7d9f3a5c2"
down_revision: Union[str, None] = "d0a6c8e4f2b1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_TABLE = "user_group_members"
_CONSTRAINT = "uq_user_group_member"
_COLUMNS = ("group_id", "user_id")


def _is_offline() -> bool:
    return bool(op.get_context().as_sql)


def _is_mysql_family() -> bool:
    dialect = op.get_bind().dialect
    return dialect.name in {"mysql", "mariadb"} or bool(
        getattr(dialect, "is_mariadb", False)
    )


def _refuse_unsupported_offline_mode() -> None:
    if not _is_offline():
        return
    if op.get_bind().dialect.name == "sqlite":
        raise RuntimeError(
            "User group membership migration requires online SQLite batch "
            "reflection"
        )
    if _is_mysql_family():
        raise RuntimeError(
            "User group membership migration refuses MySQL/MariaDB offline "
            "SQL because the committed UNIQUE state cannot be reflected"
        )


def _acquire_sqlite_writer_fence(*, downgrade: bool) -> None:
    if op.get_bind().dialect.name != "sqlite" or _is_offline():
        return
    expected_revision = revision if downgrade else down_revision
    fenced = op.get_bind().execute(
        sa.text(
            "UPDATE alembic_version SET version_num = version_num "
            "WHERE version_num = :expected_revision"
        ),
        {"expected_revision": expected_revision},
    )
    if fenced.rowcount != 1:
        raise RuntimeError(
            "User group membership migration could not acquire its SQLite "
            "revision writer fence"
        )


def _unique_present() -> bool:
    inspector = sa.inspect(op.get_bind())
    target = None
    for item in inspector.get_unique_constraints(_TABLE):
        name = str(item.get("name") or "").lower()
        columns = tuple(
            str(column).lower()
            for column in (item.get("column_names") or ())
        )
        if name == _CONSTRAINT:
            target = columns
        elif columns == _COLUMNS:
            raise RuntimeError(
                "User group membership has a foreign equivalent UNIQUE"
            )
    if target is None:
        return False
    if target != _COLUMNS:
        raise RuntimeError(
            "User group membership UNIQUE has a foreign column shape"
        )
    return True


def _create_unique() -> None:
    with op.batch_alter_table(_TABLE, schema=None) as batch_op:
        batch_op.create_unique_constraint(_CONSTRAINT, list(_COLUMNS))


def _drop_unique() -> None:
    with op.batch_alter_table(_TABLE, schema=None) as batch_op:
        batch_op.drop_constraint(_CONSTRAINT, type_="unique")


def _delete_duplicate_memberships() -> None:
    """Keep the oldest row for every (group_id, user_id) pair."""

    dialect = op.get_bind().dialect.name
    if dialect in {"mysql", "mariadb"}:
        op.execute(
            "DELETE duplicate FROM user_group_members AS duplicate "
            "INNER JOIN user_group_members AS keeper "
            "ON keeper.group_id = duplicate.group_id "
            "AND keeper.user_id = duplicate.user_id "
            "AND keeper.id < duplicate.id"
        )
    elif dialect == "postgresql":
        op.execute(
            "DELETE FROM user_group_members AS duplicate "
            "USING user_group_members AS keeper "
            "WHERE keeper.group_id = duplicate.group_id "
            "AND keeper.user_id = duplicate.user_id "
            "AND keeper.id < duplicate.id"
        )
    else:
        # SQLite and other supported dialects accept this grouped subquery and
        # retain exactly one deterministic row from every duplicate set.
        op.execute(
            "DELETE FROM user_group_members WHERE id NOT IN ("
            "SELECT MIN(id) FROM user_group_members GROUP BY group_id, user_id"
            ")"
        )


def upgrade() -> None:
    _refuse_unsupported_offline_mode()
    if _is_offline():
        _delete_duplicate_memberships()
        _create_unique()
        return
    _acquire_sqlite_writer_fence(downgrade=False)
    if _unique_present():
        return
    _delete_duplicate_memberships()
    _create_unique()
    if not _unique_present():
        raise RuntimeError(
            "User group membership UNIQUE creation is incomplete"
        )


def downgrade() -> None:
    _refuse_unsupported_offline_mode()
    if _is_offline():
        _drop_unique()
        return
    _acquire_sqlite_writer_fence(downgrade=True)
    if _unique_present():
        _drop_unique()
    if _unique_present():
        raise RuntimeError(
            "User group membership UNIQUE removal is incomplete"
        )
