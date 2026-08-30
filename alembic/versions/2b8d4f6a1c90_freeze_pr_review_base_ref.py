"""freeze PR review base ref

Revision ID: 2b8d4f6a1c90
Revises: 1a7c9e4d2b60
Create Date: 2026-08-10
"""

from alembic import context, op
import sqlalchemy as sa


revision: str = "2b8d4f6a1c90"
down_revision: str | None = "1a7c9e4d2b60"
branch_labels: str | None = None
depends_on: str | None = None


_TABLE = "pr_reviews"
_COLUMN = "base_ref"
_OLD_SUBJECT_CONSTRAINT = "uq_pr_reviews_repo_pr_base_head"
_NEW_SUBJECT_CONSTRAINT = "uq_pr_reviews_repo_pr_base_ref_base_head"
_OLD_SUBJECT_COLUMNS = ("repo_id", "pr_number", "base_sha", "head_sha")
_NEW_SUBJECT_COLUMNS = (
    "repo_id",
    "pr_number",
    "base_ref",
    "base_sha",
    "head_sha",
)
_QUEUE_TABLE = "pr_merge_queue_actions"
_OLD_QUEUE_CONSTRAINT = "uq_pr_merge_queue_actions_run_head"
_NEW_QUEUE_CONSTRAINT = "uq_pr_merge_queue_actions_run_review"
_OLD_QUEUE_COLUMNS = ("monitor_run_id", "trigger_head_sha")
_NEW_QUEUE_COLUMNS = ("monitor_run_id", "review_id")
_SUPPORTED_DIALECTS = {"sqlite", "postgresql", "mysql", "mariadb"}
_NONTRANSACTIONAL_DDL_DIALECTS = {"sqlite", "mysql", "mariadb"}


def _is_offline() -> bool:
    return bool(context.is_offline_mode())


def _dialect_name() -> str:
    name = op.get_context().dialect.name
    if name not in _SUPPORTED_DIALECTS:
        raise RuntimeError(
            "PR base-ref migration does not support database dialect "
            f"{name!r}"
        )
    return name


def _reflected_base_ref_column() -> dict | None:
    columns = [
        column
        for column in sa.inspect(op.get_bind()).get_columns(_TABLE)
        if column.get("name") == _COLUMN
    ]
    if len(columns) > 1:
        raise RuntimeError(
            "PR base-ref migration found duplicate reflected columns"
        )
    return columns[0] if columns else None


def _assert_base_ref_column_shape(
    column: dict,
    *,
    nullable: bool | None = None,
) -> None:
    reflected_type = column.get("type")
    if (
        column.get("name") != _COLUMN
        or not isinstance(reflected_type, sa.VARCHAR)
        or reflected_type.length != 200
        or column.get("nullable") not in {True, False}
        or (
            nullable is not None
            and column.get("nullable") is not nullable
        )
        or column.get("default") is not None
    ):
        raise RuntimeError(
            "PR base-ref migration found an incompatible existing base_ref "
            "column; expected VARCHAR(200) with the required nullability "
            "and no server default"
        )


def _add_base_ref_column() -> None:
    with op.batch_alter_table(_TABLE, schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(_COLUMN, sa.String(length=200), nullable=True)
        )


def _ensure_base_ref_column() -> None:
    dialect = _dialect_name()
    if _is_offline():
        # PostgreSQL DDL is transactional. SQLite and MySQL/MariaDB can leave
        # the column committed without the Alembic stamp, and static SQL
        # cannot reflect that crash state safely.
        if dialect in _NONTRANSACTIONAL_DDL_DIALECTS:
            raise RuntimeError(
                "Offline PR base-ref upgrade is refused for "
                f"{dialect}: interrupted ADD COLUMN state cannot be "
                "reflected and replayed safely"
            )
        _add_base_ref_column()
        return

    column = _reflected_base_ref_column()
    if column is None:
        _add_base_ref_column()
        column = _reflected_base_ref_column()
        if column is None:
            raise RuntimeError(
                "PR base-ref migration added no reflected column"
            )
    _assert_base_ref_column_shape(column)


def _backfill_existing_reviews() -> None:
    # A review was admitted only for the monitor's configured target branch.
    # Freeze that branch on every pre-column row. The correlated subquery is
    # supported by all production dialects and remains idempotent after an
    # interrupted non-transactional DDL upgrade.
    op.execute(sa.text("""
        UPDATE pr_reviews
        SET base_ref = (
            SELECT monitored_repos.default_branch
            FROM monitored_repos
            WHERE monitored_repos.id = pr_reviews.repo_id
        )
        WHERE base_ref IS NULL
    """))


def _assert_backfilled_values() -> None:
    if _is_offline():
        # Offline mode is supported only for transactional PostgreSQL. Emit a
        # server-side assertion so invalid legacy data cannot be stamped.
        op.execute(sa.text(r"""
            DO $ccm$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM pr_reviews
                    WHERE base_ref IS NULL
                       OR base_ref = ''
                       OR length(base_ref) > 200
                       OR position(E'\n' in base_ref) > 0
                       OR position(E'\r' in base_ref) > 0
                ) THEN
                    RAISE EXCEPTION
                        'PR base-ref migration found an invalid target branch';
                END IF;
            END
            $ccm$;
        """))
        return
    rows = op.get_bind().execute(
        sa.text("SELECT id, base_ref FROM pr_reviews")
    )
    for review_id, base_ref in rows:
        if (
            not isinstance(base_ref, str)
            or not base_ref
            or len(base_ref) > 200
            or "\x00" in base_ref
            or "\n" in base_ref
            or "\r" in base_ref
        ):
            raise RuntimeError(
                "PR base-ref migration could not freeze a valid target "
                f"branch for review {review_id}"
            )


def _make_base_ref_non_nullable() -> None:
    if _is_offline():
        with op.batch_alter_table(_TABLE, schema=None) as batch_op:
            batch_op.alter_column(
                _COLUMN,
                existing_type=sa.String(length=200),
                nullable=False,
            )
        return
    column = _reflected_base_ref_column()
    if column is None:
        raise RuntimeError("PR base-ref migration lost its owned column")
    _assert_base_ref_column_shape(column)
    if column.get("nullable") is True:
        with op.batch_alter_table(_TABLE, schema=None) as batch_op:
            batch_op.alter_column(
                _COLUMN,
                existing_type=sa.String(length=200),
                nullable=False,
            )
        column = _reflected_base_ref_column()
        if column is None:
            raise RuntimeError(
                "PR base-ref migration lost its column while freezing it"
            )
    _assert_base_ref_column_shape(column, nullable=False)


def _reflected_subject_constraints() -> tuple[bool, bool]:
    constraints = {
        item.get("name"): tuple(item.get("column_names") or ())
        for item in sa.inspect(op.get_bind()).get_unique_constraints(_TABLE)
    }
    old_columns = constraints.get(_OLD_SUBJECT_CONSTRAINT)
    new_columns = constraints.get(_NEW_SUBJECT_CONSTRAINT)
    if old_columns is not None and old_columns != _OLD_SUBJECT_COLUMNS:
        raise RuntimeError(
            "PR base-ref migration found an incompatible legacy subject "
            "constraint"
        )
    if new_columns is not None and new_columns != _NEW_SUBJECT_COLUMNS:
        raise RuntimeError(
            "PR base-ref migration found an incompatible frozen subject "
            "constraint"
        )
    return old_columns is not None, new_columns is not None


def _create_subject_constraint(name: str, columns: tuple[str, ...]) -> None:
    with op.batch_alter_table(_TABLE, schema=None) as batch_op:
        batch_op.create_unique_constraint(name, list(columns))


def _drop_subject_constraint(name: str) -> None:
    with op.batch_alter_table(_TABLE, schema=None) as batch_op:
        batch_op.drop_constraint(name, type_="unique")


def _ensure_frozen_subject_constraint() -> None:
    if _is_offline():
        with op.batch_alter_table(_TABLE, schema=None) as batch_op:
            batch_op.create_unique_constraint(
                _NEW_SUBJECT_CONSTRAINT,
                list(_NEW_SUBJECT_COLUMNS),
            )
            batch_op.drop_constraint(
                _OLD_SUBJECT_CONSTRAINT,
                type_="unique",
            )
        return
    has_old, has_new = _reflected_subject_constraints()
    if not has_old and not has_new:
        raise RuntimeError(
            "PR base-ref migration found no owned subject constraint"
        )
    # Create the broader key before dropping the legacy key. MySQL/MariaDB
    # autocommits DDL, so this order preserves a uniqueness fence on crashes.
    if not has_new:
        _create_subject_constraint(
            _NEW_SUBJECT_CONSTRAINT,
            _NEW_SUBJECT_COLUMNS,
        )
        has_old, has_new = _reflected_subject_constraints()
        if not has_new:
            raise RuntimeError(
                "PR base-ref migration created no frozen subject constraint"
            )
    if has_old:
        _drop_subject_constraint(_OLD_SUBJECT_CONSTRAINT)
    has_old, has_new = _reflected_subject_constraints()
    if has_old or not has_new:
        raise RuntimeError(
            "PR base-ref migration did not converge its subject constraint"
        )


def _reflected_queue_constraints() -> tuple[bool, bool]:
    constraints = {
        item.get("name"): tuple(item.get("column_names") or ())
        for item in sa.inspect(op.get_bind()).get_unique_constraints(
            _QUEUE_TABLE
        )
    }
    old_columns = constraints.get(_OLD_QUEUE_CONSTRAINT)
    new_columns = constraints.get(_NEW_QUEUE_CONSTRAINT)
    if old_columns is not None and old_columns != _OLD_QUEUE_COLUMNS:
        raise RuntimeError(
            "PR base-ref migration found an incompatible legacy Merge Queue "
            "constraint"
        )
    if new_columns is not None and new_columns != _NEW_QUEUE_COLUMNS:
        raise RuntimeError(
            "PR base-ref migration found an incompatible frozen Merge Queue "
            "constraint"
        )
    return old_columns is not None, new_columns is not None


def _create_queue_constraint(name: str, columns: tuple[str, ...]) -> None:
    with op.batch_alter_table(_QUEUE_TABLE, schema=None) as batch_op:
        batch_op.create_unique_constraint(name, list(columns))


def _drop_queue_constraint(name: str) -> None:
    with op.batch_alter_table(_QUEUE_TABLE, schema=None) as batch_op:
        batch_op.drop_constraint(name, type_="unique")


def _ensure_frozen_queue_constraint() -> None:
    if _is_offline():
        with op.batch_alter_table(_QUEUE_TABLE, schema=None) as batch_op:
            batch_op.create_unique_constraint(
                _NEW_QUEUE_CONSTRAINT,
                list(_NEW_QUEUE_COLUMNS),
            )
            batch_op.drop_constraint(
                _OLD_QUEUE_CONSTRAINT,
                type_="unique",
            )
        return
    has_old, has_new = _reflected_queue_constraints()
    if not has_old and not has_new:
        raise RuntimeError(
            "PR base-ref migration found no owned Merge Queue constraint"
        )
    if not has_new:
        _create_queue_constraint(
            _NEW_QUEUE_CONSTRAINT,
            _NEW_QUEUE_COLUMNS,
        )
        has_old, has_new = _reflected_queue_constraints()
        if not has_new:
            raise RuntimeError(
                "PR base-ref migration created no frozen Merge Queue "
                "constraint"
            )
    if has_old:
        _drop_queue_constraint(_OLD_QUEUE_CONSTRAINT)
    has_old, has_new = _reflected_queue_constraints()
    if has_old or not has_new:
        raise RuntimeError(
            "PR base-ref migration did not converge its Merge Queue "
            "constraint"
        )


def upgrade() -> None:
    _ensure_base_ref_column()
    _backfill_existing_reviews()
    _assert_backfilled_values()
    _make_base_ref_non_nullable()
    _ensure_frozen_subject_constraint()
    _ensure_frozen_queue_constraint()


def _acquire_downgrade_writer_fence() -> str:
    if _is_offline():
        raise RuntimeError(
            "Offline downgrade is refused because frozen PR base subjects "
            "cannot be inspected"
        )
    dialect = _dialect_name()
    if dialect == "postgresql":
        op.execute(sa.text("LOCK TABLE pr_reviews IN ACCESS EXCLUSIVE MODE"))
        return dialect
    if dialect == "sqlite":
        fenced = op.get_bind().execute(
            sa.text(
                "UPDATE alembic_version SET version_num = version_num "
                "WHERE version_num = :expected_revision"
            ),
            {"expected_revision": revision},
        )
        if fenced.rowcount != 1:
            raise RuntimeError(
                "PR base-ref downgrade could not acquire its SQLite "
                "revision writer fence"
            )
        return dialect
    # MySQL/MariaDB releases table/transaction locks around ALTER TABLE. An
    # old binary could insert a Review between the preflight and DROP, so the
    # frozen subject cannot be removed safely online.
    raise RuntimeError(
        "PR base-ref downgrade is refused for MySQL/MariaDB because online "
        "DROP COLUMN cannot be fenced against concurrent review creation"
    )


def _count_reviews() -> int:
    return op.get_bind().execute(
        sa.text("SELECT COUNT(*) FROM pr_reviews")
    ).scalar_one()


def _drop_base_ref_column() -> None:
    with op.batch_alter_table(_TABLE, schema=None) as batch_op:
        batch_op.drop_column(_COLUMN)


def _ensure_legacy_subject_constraint() -> None:
    has_old, has_new = _reflected_subject_constraints()
    if not has_old and not has_new:
        raise RuntimeError(
            "PR base-ref downgrade found no owned subject constraint"
        )
    if not has_old:
        _create_subject_constraint(
            _OLD_SUBJECT_CONSTRAINT,
            _OLD_SUBJECT_COLUMNS,
        )
        has_old, has_new = _reflected_subject_constraints()
        if not has_old:
            raise RuntimeError(
                "PR base-ref downgrade created no legacy subject constraint"
            )
    if has_new:
        _drop_subject_constraint(_NEW_SUBJECT_CONSTRAINT)
    has_old, has_new = _reflected_subject_constraints()
    if not has_old or has_new:
        raise RuntimeError(
            "PR base-ref downgrade did not restore its legacy constraint"
        )


def _ensure_legacy_queue_constraint() -> None:
    has_old, has_new = _reflected_queue_constraints()
    if not has_old and not has_new:
        raise RuntimeError(
            "PR base-ref downgrade found no owned Merge Queue constraint"
        )
    if not has_old:
        _create_queue_constraint(
            _OLD_QUEUE_CONSTRAINT,
            _OLD_QUEUE_COLUMNS,
        )
        has_old, has_new = _reflected_queue_constraints()
        if not has_old:
            raise RuntimeError(
                "PR base-ref downgrade created no legacy Merge Queue "
                "constraint"
            )
    if has_new:
        _drop_queue_constraint(_NEW_QUEUE_CONSTRAINT)
    has_old, has_new = _reflected_queue_constraints()
    if not has_old or has_new:
        raise RuntimeError(
            "PR base-ref downgrade did not restore its legacy Merge Queue "
            "constraint"
        )


def _assert_legacy_constraints_restored() -> None:
    has_old_queue, has_new_queue = _reflected_queue_constraints()
    has_old_subject, has_new_subject = _reflected_subject_constraints()
    if (
        not has_old_queue
        or has_new_queue
        or not has_old_subject
        or has_new_subject
    ):
        raise RuntimeError(
            "PR base-ref downgrade found a missing column without the exact "
            "legacy constraints"
        )


def downgrade() -> None:
    dialect = _acquire_downgrade_writer_fence()
    column = _reflected_base_ref_column()
    if column is None:
        # SQLite batch DDL can commit before Alembic changes its stamp. Accept
        # only the exact zero-row legacy schema left by our final DROP;
        # PostgreSQL would have rolled the DDL back.
        if dialect == "sqlite":
            _assert_legacy_constraints_restored()
            if _count_reviews():
                raise RuntimeError(
                    "PR base-ref downgrade refused: review subjects still "
                    "exist"
                )
            return
        raise RuntimeError(
            "PR base-ref downgrade found its owned column missing"
        )
    _assert_base_ref_column_shape(column, nullable=False)
    if _count_reviews():
        raise RuntimeError(
            "PR base-ref downgrade refused: review subjects still exist"
        )
    _ensure_legacy_queue_constraint()
    _ensure_legacy_subject_constraint()
    _drop_base_ref_column()
