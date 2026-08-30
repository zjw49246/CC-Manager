"""add durable Task migration operations

Revision ID: a3d9b5f7c1e4
Revises: f2c8a4e6d1b0
Create Date: 2026-08-14

The operation row is the exclusive owner of one in-flight Task migration.
CREATE and DROP are crash-replayable on non-transactional DDL backends.  A
MySQL-family downgrade first installs an enforced CHECK that prevents a new
operation from racing the final emptiness check and DROP.
"""

import re
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op


revision: str = "a3d9b5f7c1e4"
down_revision: Union[str, None] = "f2c8a4e6d1b0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_TABLE = "task_migration_operations"
_DOWNGRADE_GATE = "ck_task_migration_no_downgrade_use"
_DOWNGRADE_GATE_SQL = "operation_id IS NULL"
_MANAGER_ACTIVE_PHASES = (
    "claimed",
    "remote_prepared",
    "rollback_pending",
    "committed_pending_ack",
)
_WORKER_ACTIVE_PHASES = ("prepared",)
_ACTIVE_PHASES = (*_MANAGER_ACTIVE_PHASES, *_WORKER_ACTIVE_PHASES)
_TERMINAL_PHASES = ("committed", "rolled_back")
_PHASES = (*_ACTIVE_PHASES, *_TERMINAL_PHASES)
_SOURCE_STATUSES = (
    "cancelled",
    "completed",
    "conflict",
    "failed",
    "plan_review",
)


def _sql_values(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def _lower_hex_check(column: str) -> str:
    remainder = column
    for character in "0123456789abcdef":
        remainder = f"REPLACE({remainder}, '{character}', '')"
    return f"LENGTH({column}) = 32 AND {remainder} = ''"


_CHECKS = {
    "ck_task_migration_operation_id_hex": _lower_hex_check("operation_id"),
    "ck_task_migration_incarnation_hex": _lower_hex_check(
        "task_incarnation_id"
    ),
    "ck_task_migration_phase": f"phase IN ({_sql_values(_PHASES)})",
    "ck_task_migration_active_slot": (
        f"((phase IN ({_sql_values(_ACTIVE_PHASES)}) "
        "AND active_task_id IS NOT NULL "
        "AND active_task_id = task_id) OR "
        f"(phase IN ({_sql_values(_TERMINAL_PHASES)}) "
        "AND active_task_id IS NULL))"
    ),
    "ck_task_migration_generation": (
        "operation_sequence > 0 AND retry_count >= 0 "
        "AND turn_generation >= 0"
    ),
    "ck_task_migration_identity": (
        "task_id > 0 AND (instance_id IS NULL OR instance_id > 0) "
        "AND (source_worker_id IS NULL OR source_worker_id > 0) "
        "AND (target_worker_id IS NULL OR target_worker_id > 0)"
    ),
    "ck_task_migration_source_status": (
        f"source_status IN ({_sql_values(_SOURCE_STATUSES)})"
    ),
    "ck_task_migration_route": (
        "(side = 'worker' AND source_worker_id IS NULL "
        "AND target_worker_id IS NULL) OR "
        "(side = 'manager' AND ("
        "(source_worker_id IS NULL AND target_worker_id IS NOT NULL) OR "
        "(source_worker_id IS NOT NULL AND target_worker_id IS NULL) OR "
        "(source_worker_id IS NOT NULL AND target_worker_id IS NOT NULL "
        "AND source_worker_id <> target_worker_id)))"
    ),
    "ck_task_migration_side_phase": (
        f"(side = 'manager' AND phase IN ("
        f"{_sql_values((*_MANAGER_ACTIVE_PHASES, *_TERMINAL_PHASES))})) "
        f"OR (side = 'worker' AND phase IN ("
        f"{_sql_values((*_WORKER_ACTIVE_PHASES, *_TERMINAL_PHASES))}))"
    ),
}
_COLUMN_SPECS = {
    "operation_id": (sa.String, 32, False),
    "operation_sequence": (sa.BigInteger, None, False),
    "side": (sa.String, 16, False),
    "active_task_id": (sa.Integer, None, True),
    "task_id": (sa.Integer, None, False),
    "task_incarnation_id": (sa.String, 32, False),
    "retry_count": (sa.Integer, None, False),
    "turn_generation": (sa.BigInteger, None, False),
    "source_worker_id": (sa.Integer, None, True),
    "target_worker_id": (sa.Integer, None, True),
    "source_status": (sa.String, 20, False),
    "phase": (sa.String, 32, False),
    "instance_id": (sa.Integer, None, True),
    "started_at": (sa.DateTime, None, True),
    "completed_at": (sa.DateTime, None, True),
    "created_at": (sa.DateTime, None, False),
    "updated_at": (sa.DateTime, None, False),
}
_POSTGRESQL_CHECK_PROBE = "ccm_a3_task_migration_check_probe"


def _is_offline() -> bool:
    return bool(op.get_context().as_sql)


def _is_mysql_family() -> bool:
    dialect = op.get_bind().dialect
    return dialect.name in {"mysql", "mariadb"} or bool(
        getattr(dialect, "is_mariadb", False)
    )


def _require_supported_mysql_family() -> None:
    if not _is_mysql_family():
        return
    if _is_offline():
        raise RuntimeError(
            "Task migration operation migration refuses MySQL/MariaDB "
            "offline SQL because table shape, enforced CHECK constraints, "
            "and InnoDB cannot be proven"
        )
    dialect = op.get_bind().dialect
    version = tuple((getattr(dialect, "server_version_info", None) or ())[:3])
    is_mariadb = dialect.name == "mariadb" or bool(
        getattr(dialect, "is_mariadb", False)
    )
    minimum = (10, 6, 1) if is_mariadb else (8, 0, 16)
    if not version or version < minimum:
        product = "MariaDB 10.6.1+" if is_mariadb else "MySQL 8.0.16+"
        raise RuntimeError(
            "Task migration operation migration requires "
            + product
            + " with enforced CHECK constraints and atomic DDL"
        )


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
                "Task migration operation migration could not acquire its "
                "SQLite revision writer fence"
            )
    elif dialect == "postgresql" and downgrade:
        # PostgreSQL DDL is transactional, so an absent table cannot be a
        # committed DROP-without-stamp crash.  The guard still makes direct
        # replay of an already absent table harmless.
        if sa.inspect(bind).has_table(_TABLE):
            op.execute(
                sa.text(
                    "LOCK TABLE task_migration_operations "
                    "IN ACCESS EXCLUSIVE MODE"
                )
            )


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


def _split_top_level(expression: str, operator: str) -> list[str]:
    parts: list[str] = []
    start = 0
    depth = 0
    quoted = False
    index = 0
    while index < len(expression):
        character = expression[index]
        if character == "'":
            quoted = not quoted
            index += 1
            continue
        if not quoted:
            if character == "(":
                depth += 1
            elif character == ")":
                depth -= 1
            elif depth == 0 and expression.startswith(operator, index):
                parts.append(expression[start:index].strip())
                index += len(operator)
                start = index
                continue
        index += 1
    parts.append(expression[start:].strip())
    return parts


def _check_shape(sqltext: object):
    expression = _normalized_sql(sqltext)
    if expression.startswith("check"):
        expression = expression[len("check") :].strip()
    expression = _strip_outer_parentheses(expression)
    or_parts = _split_top_level(expression, " or ")
    if len(or_parts) > 1:
        return ("or", tuple(_check_shape(part) for part in or_parts))
    and_parts = _split_top_level(expression, " and ")
    if len(and_parts) > 1:
        return ("and", tuple(_check_shape(part) for part in and_parts))
    return ("atom", "".join(expression.split()))


def _postgresql_check_definitions(relation_name: str) -> dict[str, str]:
    rows = op.get_bind().execute(
        sa.text(
            "SELECT constraint_row.conname, "
            "pg_catalog.pg_get_constraintdef(constraint_row.oid, true), "
            "constraint_row.convalidated "
            "FROM pg_catalog.pg_constraint AS constraint_row "
            "WHERE constraint_row.conrelid = "
            "pg_catalog.to_regclass(:relation_name) "
            "AND constraint_row.contype = 'c'"
        ),
        {"relation_name": relation_name},
    )
    definitions: dict[str, str] = {}
    for raw_name, definition, validated in rows:
        name = str(raw_name).lower()
        if not bool(validated):
            raise RuntimeError(
                f"Task migration operation CHECK {name} is not validated"
            )
        definitions[name] = str(definition)
    return definitions


def _postgresql_canonical_checks() -> dict[str, str]:
    """Compare CHECKs after PostgreSQL canonicalizes casts and ``IN``."""

    expected = {**_CHECKS, _DOWNGRADE_GATE: _DOWNGRADE_GATE_SQL}
    probe_columns = []
    for name, (kind, length, nullable) in _COLUMN_SPECS.items():
        type_ = kind(length=length) if kind is sa.String else kind()
        probe_columns.append(sa.Column(name, type_, nullable=nullable))
    probe = sa.Table(
        _POSTGRESQL_CHECK_PROBE,
        sa.MetaData(),
        *probe_columns,
        *(
            sa.CheckConstraint(expression, name=name)
            for name, expression in expected.items()
        ),
        prefixes=["TEMPORARY"],
    )
    bind = op.get_bind()
    bind.execute(sa.schema.CreateTable(probe))
    try:
        actual = _postgresql_check_definitions(_TABLE)
        canonical = _postgresql_check_definitions(probe.name)
    finally:
        bind.execute(sa.schema.DropTable(probe))
    if set(canonical) != set(expected):
        raise RuntimeError(
            "Task migration operation PostgreSQL CHECK probe is incomplete"
        )
    for name, definition in actual.items():
        expected_definition = canonical.get(name)
        if expected_definition is None or _normalized_sql(
            definition
        ) != _normalized_sql(expected_definition):
            raise RuntimeError(
                f"Task migration operation CHECK {name} is malformed"
            )
    return actual


def _type_matches(
    reflected: sa.types.TypeEngine,
    expected: type[sa.types.TypeEngine],
    length: int | None = None,
) -> bool:
    if expected is sa.Integer:
        return type(reflected).__name__.upper() == "INTEGER"
    if expected is sa.BigInteger:
        return type(reflected).__name__.upper() == "BIGINT"
    return isinstance(reflected, expected) and (
        length is None or getattr(reflected, "length", None) == length
    )


def _table_state(*, allow_gate: bool = False) -> str:
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table(_TABLE):
        return "absent"

    columns = {
        str(column["name"]).lower(): column
        for column in inspector.get_columns(_TABLE)
    }
    if set(columns) != set(_COLUMN_SPECS):
        raise RuntimeError(
            "Task migration operation table has a foreign column set"
        )
    for name, (kind, length, nullable) in _COLUMN_SPECS.items():
        column = columns[name]
        if (
            not _type_matches(column["type"], kind, length)
            or bool(column["nullable"]) is not nullable
            or column.get("default") is not None
        ):
            raise RuntimeError(
                f"Task migration operation column {name} has a foreign shape"
            )

    primary_key = tuple(
        str(column).lower()
        for column in (
            inspector.get_pk_constraint(_TABLE).get("constrained_columns")
            or ()
        )
    )
    if primary_key != ("operation_id",):
        raise RuntimeError(
            "Task migration operation primary key is malformed"
        )

    unique_constraints = {
        str(item.get("name") or "").lower(): tuple(
            str(column).lower()
            for column in (item.get("column_names") or ())
        )
        for item in inspector.get_unique_constraints(_TABLE)
    }
    if unique_constraints != {
        "uq_task_migration_active_task": ("active_task_id",),
        "uq_task_migration_task_sequence": (
            "task_id",
            "operation_sequence",
        ),
    }:
        raise RuntimeError(
            "Task migration operation UNIQUE constraints are malformed"
        )
    if inspector.get_foreign_keys(_TABLE):
        raise RuntimeError(
            "Task migration operation table has unexpected foreign keys"
        )

    if op.get_bind().dialect.name == "postgresql":
        checks = _postgresql_canonical_checks()
    else:
        checks = {
            str(item.get("name") or "").lower(): item.get("sqltext")
            for item in inspector.get_check_constraints(_TABLE)
        }
    canonical_checks = set(_CHECKS)
    if set(checks) == canonical_checks:
        state = "canonical"
    elif allow_gate and set(checks) == canonical_checks | {_DOWNGRADE_GATE}:
        state = "canonical_gated"
    else:
        raise RuntimeError(
            "Task migration operation CHECK set is malformed"
        )
    if op.get_bind().dialect.name != "postgresql":
        for name, expected in _CHECKS.items():
            if _check_shape(checks[name]) != _check_shape(expected):
                raise RuntimeError(
                    f"Task migration operation CHECK {name} is malformed"
                )
        if state == "canonical_gated" and _check_shape(
            checks[_DOWNGRADE_GATE]
        ) != _check_shape(_DOWNGRADE_GATE_SQL):
            raise RuntimeError(
                "Task migration operation downgrade gate is malformed"
            )

    if _is_mysql_family():
        engines = op.get_bind().execute(
            sa.text(
                "SELECT ENGINE FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table"
            ),
            {"table": _TABLE},
        ).scalars().all()
        if len(engines) != 1 or str(engines[0]).lower() != "innodb":
            raise RuntimeError(
                "Task migration operation table must use InnoDB"
            )
    return state


def _create_table() -> None:
    columns_and_constraints: list[object] = [
        sa.Column("operation_id", sa.String(length=32), nullable=False),
        sa.Column("operation_sequence", sa.BigInteger(), nullable=False),
        sa.Column("side", sa.String(length=16), nullable=False),
        sa.Column("active_task_id", sa.Integer(), nullable=True),
        sa.Column("task_id", sa.Integer(), nullable=False),
        sa.Column(
            "task_incarnation_id",
            sa.String(length=32),
            nullable=False,
        ),
        sa.Column("retry_count", sa.Integer(), nullable=False),
        sa.Column("turn_generation", sa.BigInteger(), nullable=False),
        sa.Column("source_worker_id", sa.Integer(), nullable=True),
        sa.Column("target_worker_id", sa.Integer(), nullable=True),
        sa.Column("source_status", sa.String(length=20), nullable=False),
        sa.Column("phase", sa.String(length=32), nullable=False),
        sa.Column("instance_id", sa.Integer(), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("operation_id"),
        sa.UniqueConstraint(
            "active_task_id",
            name="uq_task_migration_active_task",
        ),
        sa.UniqueConstraint(
            "task_id",
            "operation_sequence",
            name="uq_task_migration_task_sequence",
        ),
    ]
    columns_and_constraints.extend(
        sa.CheckConstraint(expression, name=name)
        for name, expression in _CHECKS.items()
    )
    op.create_table(
        _TABLE,
        *columns_and_constraints,
        mysql_engine="InnoDB",
    )


def _assert_empty() -> None:
    operation = sa.table(
        _TABLE,
        sa.column("operation_id", sa.String(length=32)),
    )
    if op.get_bind().execute(
        sa.select(operation.c.operation_id).limit(1)
    ).scalar_one_or_none() is not None:
        raise RuntimeError(
            "Cannot downgrade while Task migration operations exist"
        )


def upgrade() -> None:
    _require_supported_mysql_family()
    if _is_offline():
        _create_table()
        return
    _acquire_transactional_fence(downgrade=False)
    state = _table_state(allow_gate=True)
    if state == "canonical_gated":
        raise RuntimeError(
            "Task migration operation upgrade found a downgrade-only gate"
        )
    if state == "absent":
        _create_table()
    if _table_state() != "canonical":
        raise RuntimeError(
            "Task migration operation table creation is incomplete"
        )


def downgrade() -> None:
    _require_supported_mysql_family()
    if _is_offline():
        raise RuntimeError(
            "Task migration operation offline downgrade is refused because "
            "operation rows cannot be inspected"
        )
    _acquire_transactional_fence(downgrade=True)
    state = _table_state(allow_gate=True)
    if state == "absent":
        return
    if state == "canonical_gated" and not _is_mysql_family():
        raise RuntimeError(
            "Task migration operation downgrade found a foreign gate"
        )

    _assert_empty()
    if _is_mysql_family() and state == "canonical":
        op.create_check_constraint(
            _DOWNGRADE_GATE,
            _TABLE,
            _DOWNGRADE_GATE_SQL,
        )
        state = _table_state(allow_gate=True)
    if _is_mysql_family() and state != "canonical_gated":
        raise RuntimeError(
            "Task migration operation downgrade gate was not installed"
        )
    _assert_empty()
    op.drop_table(_TABLE)
    if _table_state(allow_gate=True) != "absent":
        raise RuntimeError(
            "Task migration operation table removal is incomplete"
        )
