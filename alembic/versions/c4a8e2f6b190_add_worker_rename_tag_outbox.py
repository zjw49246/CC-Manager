"""add monotonic Worker cloud Name-tag outbox

Revision ID: c4a8e2f6b190
Revises: b4e1c7d9f203
Create Date: 2026-08-14

The database name, monotonic generation, and provider-effect outbox are
committed before an EC2 Name-tag mutation.  ADD/DROP is crash-replayable on
non-transactional DDL backends, but an existing column is adopted only when
its complete reflected shape is canonical.  Downgrade refuses a pending
provider effect rather than silently discarding it.
"""

import re
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "c4a8e2f6b190"
down_revision: Union[str, None] = "b4e1c7d9f203"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_TABLE = "workers"
_GENERATION = "rename_generation"
_OUTBOX = "rename_tag_outbox"
_MANAGED_COLUMNS = {_GENERATION, _OUTBOX}
_MARIADB_OUTBOX_JSON_CHECK = "json_valid(rename_tag_outbox)"
_MYSQL_DOWNGRADE_GATE = "ck_workers_rename_tag_no_downgrade_use"
_MYSQL_DOWNGRADE_GATE_SQL = "rename_tag_outbox IS NULL"


def _is_offline() -> bool:
    return bool(op.get_context().as_sql)


def _is_mysql_family() -> bool:
    dialect = op.get_bind().dialect
    return dialect.name in {"mysql", "mariadb"} or bool(
        getattr(dialect, "is_mariadb", False)
    )


def _is_mariadb() -> bool:
    dialect = op.get_bind().dialect
    return dialect.name == "mariadb" or bool(
        getattr(dialect, "is_mariadb", False)
    )


def _require_supported_mysql_family() -> None:
    # PostgreSQL and SQLite can transactionally apply the deterministic ADD
    # statements emitted below.  MySQL-family DDL can commit one ALTER while
    # the Alembic revision remains old, so its recovery depends on reflecting
    # and validating the already-present column on the next online run.
    if not _is_mysql_family():
        return
    if _is_offline():
        raise RuntimeError(
            "Worker rename-tag outbox migration refuses MySQL/MariaDB "
            "offline SQL because partial-DDL replay and column shape cannot "
            "be inspected"
        )
    dialect = op.get_bind().dialect
    version = tuple((getattr(dialect, "server_version_info", None) or ())[:3])
    minimum = (10, 6, 1) if _is_mariadb() else (8, 0, 16)
    if not version or version < minimum:
        product = "MariaDB 10.6.1+" if _is_mariadb() else "MySQL 8.0.16+"
        raise RuntimeError(
            "Worker rename-tag outbox migration requires " + product
            + " with enforced CHECK constraints and atomic DDL"
        )
    engines = op.get_bind().execute(
        sa.text(
            "SELECT ENGINE FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table"
        ),
        {"table": _TABLE},
    ).scalars().all()
    if len(engines) != 1 or str(engines[0]).lower() != "innodb":
        raise RuntimeError("Worker table must use InnoDB")


def _acquire_transactional_fence(*, downgrade: bool) -> None:
    if _is_offline():
        return
    bind = op.get_bind()
    dialect = bind.dialect.name
    if dialect == "sqlite":
        expected_revision = revision if downgrade else down_revision
        fenced = bind.execute(
            sa.text(
                "UPDATE alembic_version SET version_num = version_num "
                "WHERE version_num = :expected_revision"
            ),
            {"expected_revision": expected_revision},
        )
        if fenced.rowcount != 1:
            raise RuntimeError(
                "Worker rename-tag outbox migration could not acquire its "
                "SQLite revision writer fence"
            )
    elif dialect == "postgresql" and downgrade:
        inspector = sa.inspect(bind)
        if inspector.has_table(_TABLE):
            op.execute(sa.text("LOCK TABLE workers IN ACCESS EXCLUSIVE MODE"))


def _normalized_sql(sqltext: object) -> str:
    value = str(sqltext or "").strip().lower().replace("`", "")
    value = re.sub(r"_[a-z0-9]+\s*'", "'", value)
    return " ".join(value.split())


def _strip_outer_parentheses(expression: str) -> str:
    expression = expression.strip()
    while expression.startswith("(") and expression.endswith(")"):
        depth = 0
        quoted = False
        balanced = True
        for index, character in enumerate(expression):
            if character == "'":
                quoted = not quoted
            elif not quoted:
                if character == "(":
                    depth += 1
                elif character == ")":
                    depth -= 1
                    if depth == 0 and index != len(expression) - 1:
                        balanced = False
                        break
        if not balanced or depth != 0:
            break
        expression = expression[1:-1].strip()
    return expression


def _default_is_zero(default: object) -> bool:
    if isinstance(default, bool) or default is None:
        return False
    if isinstance(default, int):
        return default == 0
    expression = _normalized_sql(default)
    expression = re.sub(
        r"::(?:pg_catalog\.)?(?:int|int4|integer)$",
        "",
        expression,
    ).strip()
    expression = _strip_outer_parentheses(expression)
    return expression in {"0", "'0'"}


def _check_shape(sqltext: object) -> str:
    expression = _normalized_sql(sqltext)
    if expression.startswith("check"):
        expression = expression[len("check") :].strip()
    return "".join(_strip_outer_parentheses(expression).split())


def _is_mariadb_json_check(sqltext: object) -> bool:
    return _check_shape(sqltext) == _check_shape(
        _MARIADB_OUTBOX_JSON_CHECK
    )


def _mysql_enforced_checks() -> set[str]:
    if _is_mariadb():
        return {
            str(item.get("name") or "").lower()
            for item in sa.inspect(op.get_bind()).get_check_constraints(_TABLE)
        }
    rows = op.get_bind().execute(
        sa.text(
            "SELECT CONSTRAINT_NAME, ENFORCED "
            "FROM information_schema.TABLE_CONSTRAINTS "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table "
            "AND CONSTRAINT_TYPE = 'CHECK'"
        ),
        {"table": _TABLE},
    )
    return {
        str(name).lower()
        for name, enforced in rows
        if str(enforced).upper() == "YES"
    }


def _column_state(*, allow_gate: bool = False) -> str:
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table(_TABLE):
        raise RuntimeError("workers table is missing")
    columns = {
        str(column["name"]).lower(): column
        for column in inspector.get_columns(_TABLE)
    }
    checks = {
        str(item.get("name") or "").lower(): item.get("sqltext")
        for item in inspector.get_check_constraints(_TABLE)
    }
    gate = checks.get(_MYSQL_DOWNGRADE_GATE)
    present = _MANAGED_COLUMNS.intersection(columns)

    generation = columns.get(_GENERATION)
    if generation is not None and not (
        type(generation.get("type")).__name__.upper() == "INTEGER"
        and generation.get("nullable") is False
        and _default_is_zero(generation.get("default"))
        and generation.get("computed") is None
        and generation.get("identity") is None
    ):
        raise RuntimeError("workers.rename_generation has a foreign shape")

    outbox = columns.get(_OUTBOX)
    outbox_uses_mariadb_alias = False
    if outbox is not None:
        outbox_uses_mariadb_alias = (
            _is_mariadb()
            and type(outbox.get("type")).__name__.upper() == "LONGTEXT"
        )
        if not (
            (
                type(outbox.get("type")).__name__.upper() == "JSON"
                or outbox_uses_mariadb_alias
            )
            and outbox.get("nullable") is True
            and outbox.get("default") is None
            and outbox.get("computed") is None
            and outbox.get("identity") is None
        ):
            raise RuntimeError("workers.rename_tag_outbox has a foreign shape")

    primary_key = {
        str(name).lower()
        for name in (
            inspector.get_pk_constraint(_TABLE).get("constrained_columns")
            or ()
        )
    }
    if _MANAGED_COLUMNS.intersection(primary_key):
        raise RuntimeError(
            "Worker rename-tag columns have an unexpected primary key"
        )
    for index in inspector.get_indexes(_TABLE):
        if _MANAGED_COLUMNS.intersection({
            str(name).lower() for name in (index.get("column_names") or ())
        }):
            raise RuntimeError(
                "Worker rename-tag columns have an unexpected index"
            )
    for unique in inspector.get_unique_constraints(_TABLE):
        if _MANAGED_COLUMNS.intersection({
            str(name).lower() for name in (unique.get("column_names") or ())
        }):
            raise RuntimeError(
                "Worker rename-tag columns have an unexpected UNIQUE "
                "constraint"
            )
    for foreign_key in inspector.get_foreign_keys(_TABLE):
        if _MANAGED_COLUMNS.intersection({
            str(name).lower()
            for name in (foreign_key.get("constrained_columns") or ())
        }):
            raise RuntimeError(
                "Worker rename-tag columns have an unexpected foreign key"
            )

    related_checks = {
        name: sqltext
        for name, sqltext in checks.items()
        if any(
            column in _normalized_sql(sqltext)
            for column in _MANAGED_COLUMNS
        )
    }
    mariadb_json_checks = {
        name
        for name, sqltext in related_checks.items()
        if _is_mariadb_json_check(sqltext)
    }
    if outbox_uses_mariadb_alias and len(mariadb_json_checks) != 1:
        raise RuntimeError(
            "workers.rename_tag_outbox MariaDB JSON alias is missing its "
            "exact JSON_VALID constraint"
        )
    if mariadb_json_checks and not outbox_uses_mariadb_alias:
        raise RuntimeError(
            "Worker rename-tag columns have a foreign JSON_VALID constraint"
        )
    allowed_related_checks = set(mariadb_json_checks)
    if gate is not None:
        allowed_related_checks.add(_MYSQL_DOWNGRADE_GATE)
    if set(related_checks) != allowed_related_checks:
        raise RuntimeError(
            "Worker rename-tag columns have an unexpected CHECK constraint"
        )

    if gate is not None:
        if present != _MANAGED_COLUMNS:
            raise RuntimeError(
                "Worker rename-tag downgrade gate has an incomplete column "
                "set"
            )
        if not allow_gate:
            raise RuntimeError(
                "Worker rename-tag downgrade gate is unexpected"
            )
        if _check_shape(gate) != _check_shape(_MYSQL_DOWNGRADE_GATE_SQL):
            raise RuntimeError(
                "Worker rename-tag downgrade gate is malformed"
            )
        if not _is_mysql_family():
            raise RuntimeError(
                "Worker rename-tag migration found a MySQL-only downgrade "
                "gate"
            )
        if _MYSQL_DOWNGRADE_GATE not in _mysql_enforced_checks():
            raise RuntimeError(
                "Worker rename-tag downgrade gate is not enforced"
            )
        return "canonical_gated"

    if not present:
        return "absent"
    if present == {_GENERATION}:
        return "generation_only"
    if present == {_OUTBOX}:
        return "outbox_only"
    return "canonical"


def _add_column(name: str) -> None:
    if name == _GENERATION:
        column = sa.Column(
            _GENERATION,
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        )
    elif name == _OUTBOX:
        column = sa.Column(_OUTBOX, sa.JSON(), nullable=True)
    else:  # pragma: no cover - internal invariant
        raise RuntimeError(f"Unknown Worker rename-tag column: {name}")
    op.add_column(_TABLE, column)


def _pending_outbox_count() -> int:
    workers = sa.table(
        _TABLE,
        sa.column(_OUTBOX, sa.JSON()),
    )
    return int(op.get_bind().execute(
        sa.select(sa.func.count())
        .select_from(workers)
        .where(workers.c.rename_tag_outbox.is_not(None))
    ).scalar_one())


def _drop_canonical_columns(*, state: str) -> None:
    if _is_mysql_family():
        if state != "canonical_gated":
            raise RuntimeError(
                "Worker rename-tag columns cannot be dropped without their "
                "enforced writer gate"
            )
        drop_gate = "DROP CONSTRAINT" if _is_mariadb() else "DROP CHECK"
        op.execute(sa.text(
            f"ALTER TABLE {_TABLE} {drop_gate} {_MYSQL_DOWNGRADE_GATE}, "
            f"DROP COLUMN {_OUTBOX}, DROP COLUMN {_GENERATION}"
        ))
        return
    op.drop_column(_TABLE, _OUTBOX)
    op.drop_column(_TABLE, _GENERATION)


def upgrade() -> None:
    _require_supported_mysql_family()
    if _is_offline():
        _add_column(_GENERATION)
        _add_column(_OUTBOX)
        return

    _acquire_transactional_fence(downgrade=False)
    state = _column_state(allow_gate=True)
    if state == "canonical_gated":
        raise RuntimeError(
            "Worker rename-tag upgrade found a downgrade-only gate"
        )
    if state in {"absent", "outbox_only"}:
        _add_column(_GENERATION)
    if state in {"absent", "generation_only"}:
        _add_column(_OUTBOX)
    if _column_state() != "canonical":
        raise RuntimeError("Worker rename-tag column creation is incomplete")


def downgrade() -> None:
    _require_supported_mysql_family()
    if _is_offline():
        raise RuntimeError(
            "Worker rename-tag outbox offline downgrade is refused because "
            "pending provider effects cannot be inspected"
        )

    _acquire_transactional_fence(downgrade=True)
    state = _column_state(allow_gate=True)
    if state == "absent":
        return
    if state == "generation_only":
        op.drop_column(_TABLE, _GENERATION)
        if _column_state(allow_gate=True) != "absent":
            raise RuntimeError(
                "Worker rename-tag column removal is incomplete"
            )
        return
    if state == "outbox_only":
        raise RuntimeError(
            "Worker rename-tag downgrade found an impossible partial column "
            "set"
        )
    if state == "canonical_gated" and not _is_mysql_family():
        raise RuntimeError(
            "Worker rename-tag downgrade found a foreign gate"
        )
    if _pending_outbox_count():
        raise RuntimeError(
            "Cannot downgrade while Worker cloud rename outboxes are pending"
        )
    if _is_mysql_family() and state == "canonical":
        op.create_check_constraint(
            _MYSQL_DOWNGRADE_GATE,
            _TABLE,
            _MYSQL_DOWNGRADE_GATE_SQL,
        )
        state = _column_state(allow_gate=True)
    if _is_mysql_family() and state != "canonical_gated":
        raise RuntimeError(
            "Worker rename-tag downgrade gate was not installed"
        )
    if _pending_outbox_count():
        raise RuntimeError(
            "Cannot downgrade while Worker cloud rename outboxes are pending"
        )
    _drop_canonical_columns(state=state)
    if _column_state() != "absent":
        raise RuntimeError("Worker rename-tag column removal is incomplete")
