"""freeze PR review merge method

Revision ID: 1a7c9e4d2b60
Revises: 6f3b9d2a7c10
Create Date: 2026-08-10
"""

from alembic import context, op
import sqlalchemy as sa


revision: str = "1a7c9e4d2b60"
down_revision: str | None = "6f3b9d2a7c10"
branch_labels: str | None = None
depends_on: str | None = None


_TABLE = "pr_reviews"
_COLUMN = "merge_method"
_SUPPORTED_DIALECTS = {"sqlite", "postgresql", "mysql", "mariadb"}
_NONTRANSACTIONAL_DDL_DIALECTS = {"sqlite", "mysql", "mariadb"}


def _is_offline() -> bool:
    return bool(context.is_offline_mode())


def _dialect_name() -> str:
    name = op.get_context().dialect.name
    if name not in _SUPPORTED_DIALECTS:
        raise RuntimeError(
            "PR merge-method migration does not support database dialect "
            f"{name!r}"
        )
    return name


def _reflected_merge_method_column() -> dict | None:
    columns = [
        column
        for column in sa.inspect(op.get_bind()).get_columns(_TABLE)
        if column.get("name") == _COLUMN
    ]
    if len(columns) > 1:
        raise RuntimeError(
            "PR merge-method migration found duplicate reflected columns"
        )
    return columns[0] if columns else None


def _assert_merge_method_column_shape(column: dict) -> None:
    reflected_type = column.get("type")
    if (
        column.get("name") != _COLUMN
        or not isinstance(reflected_type, sa.VARCHAR)
        or reflected_type.length != 16
        or column.get("nullable") is not True
        or column.get("default") is not None
    ):
        raise RuntimeError(
            "PR merge-method migration found an incompatible existing "
            "merge_method column; expected nullable VARCHAR(16) with no "
            "server default"
        )


def _add_merge_method_column() -> None:
    with op.batch_alter_table("pr_reviews", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("merge_method", sa.String(length=16), nullable=True)
        )


def _ensure_merge_method_column() -> None:
    dialect = _dialect_name()
    if _is_offline():
        # PostgreSQL DDL is transactional, so a failed generated script rolls
        # the ADD COLUMN back with its revision stamp.  SQLite/MySQL DDL is not
        # safely replayable from static SQL because the script cannot inspect
        # a column left behind by an interrupted prior execution.
        if dialect in _NONTRANSACTIONAL_DDL_DIALECTS:
            raise RuntimeError(
                "Offline PR merge-method upgrade is refused for "
                f"{dialect}: interrupted ADD COLUMN state cannot be "
                "reflected and replayed safely"
            )
        _add_merge_method_column()
        return

    column = _reflected_merge_method_column()
    if column is None:
        _add_merge_method_column()
        # MySQL/MariaDB commits ALTER TABLE before Alembic can stamp the
        # revision.  Reflect the committed shape now, so a crash followed by
        # a rerun can skip the duplicate ADD only for the exact schema this
        # revision owns.
        column = _reflected_merge_method_column()
        if column is None:
            raise RuntimeError(
                "PR merge-method migration added no reflected column"
            )
    _assert_merge_method_column_shape(column)


def _assert_existing_merge_values() -> None:
    if _is_offline():
        return
    invalid = op.get_bind().execute(
        sa.text(
            "SELECT COUNT(*) FROM pr_reviews "
            "WHERE merge_method IS NOT NULL "
            "AND merge_method NOT IN ('merge', 'squash')"
        )
    ).scalar_one()
    if invalid:
        raise RuntimeError(
            "PR merge-method migration found invalid durable merge evidence"
        )


def _backfill_legacy_merge_outboxes() -> None:
    # Before this column existed, the only durable auto-merge publication used
    # GitHub's merge-commit method.  Preserve exactly that already-authorized
    # outbox state so a restart after an unknown/lost merge ACK can reconcile
    # its nonce-bearing remote merge instead of declaring the row corrupt.
    # Other actions and non-publishing history remain NULL: the migration must
    # not grant a new merge capability to any row that was not already armed.
    op.execute(sa.text("""
        UPDATE pr_reviews
        SET merge_method = 'merge'
        WHERE status = 'publishing'
          AND pending_action = 'approved_merged'
          AND merge_method IS NULL
          AND (delivery_id IS NULL OR delivery_id NOT LIKE 'delivery:%')
    """))


def upgrade() -> None:
    _ensure_merge_method_column()
    _assert_existing_merge_values()
    _backfill_legacy_merge_outboxes()


def _acquire_downgrade_writer_fence() -> str:
    """Prevent a new frozen method from racing the evidence preflight."""

    if _is_offline():
        raise RuntimeError(
            "Offline downgrade is refused because frozen PR merge evidence "
            "cannot be inspected"
        )
    dialect = _dialect_name()
    if dialect == "postgresql":
        # Transactional DDL keeps this lock through both the evidence check
        # and DROP COLUMN.
        op.execute(
            sa.text("LOCK TABLE pr_reviews IN ACCESS EXCLUSIVE MODE")
        )
        return dialect
    if dialect == "sqlite":
        # Alembic's SQLite connection begins a deferred transaction.  A no-op
        # write to its revision row promotes it to the single writer before
        # the evidence query and batch table rewrite.
        fenced = op.get_bind().execute(
            sa.text(
                "UPDATE alembic_version SET version_num = version_num "
                "WHERE version_num = :expected_revision"
            ),
            {"expected_revision": revision},
        )
        if fenced.rowcount != 1:
            raise RuntimeError(
                "PR merge-method downgrade could not acquire its SQLite "
                "revision writer fence"
            )
        return dialect
    # MySQL/MariaDB ALTER TABLE commits independently of the evidence query
    # and releases transactional/table locks.  Without a writer gate honored
    # by the running application, an online DROP cannot prove that a squash
    # method was not frozen between COUNT and DDL.  Refuse instead of silently
    # discarding immutable merge evidence.
    raise RuntimeError(
        "PR merge-method downgrade is refused for MySQL/MariaDB because "
        "online DROP COLUMN cannot be fenced against concurrent publishers"
    )


def _count_frozen_merge_evidence() -> int:
    return op.get_bind().execute(
        sa.text(
            "SELECT COUNT(*) FROM pr_reviews WHERE merge_method IS NOT NULL"
        )
    ).scalar_one()


def _drop_merge_method_column() -> None:
    with op.batch_alter_table("pr_reviews", schema=None) as batch_op:
        batch_op.drop_column("merge_method")


def downgrade() -> None:
    dialect = _acquire_downgrade_writer_fence()
    column = _reflected_merge_method_column()
    if column is None:
        # SQLite batch DDL is non-transactional from Alembic's perspective.
        # A process may die after the safe drop but before the revision stamp;
        # accepting only that exact missing-column replay lets downgrade
        # finish without issuing a second DROP.  PostgreSQL cannot reach this
        # state through a failed transactional migration.
        if dialect == "sqlite":
            return
        raise RuntimeError(
            "PR merge-method downgrade found its owned column missing"
        )
    _assert_merge_method_column_shape(column)
    count = _count_frozen_merge_evidence()
    if count:
        raise RuntimeError(
            "PR merge-method downgrade refused: durable merge evidence exists"
        )
    _drop_merge_method_column()
