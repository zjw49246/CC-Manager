"""add durable Worker node drain control

Revision ID: f2c8a4e6d1b0
Revises: e1b7d9f3a5c2
Create Date: 2026-08-14

The control row fences effects that must survive process loss.  CREATE and
DROP are therefore crash-replayable on non-transactional DDL backends, and a
MySQL-family downgrade first installs a CHECK that blocks every control-row
mutation before the table is removed.
"""

import re
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op


revision: str = "f2c8a4e6d1b0"
down_revision: Union[str, None] = "e1b7d9f3a5c2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_TABLE = "worker_node_controls"
_DOWNGRADE_GATE = "ck_worker_node_controls_no_downgrade_use"
_CHECKS = {
    "ck_worker_node_controls_singleton": "id = 1",
    "ck_worker_node_controls_drain_phase": (
        "(drain_claim IS NULL AND drain_started_at IS NULL "
        "AND runtime_seal_claim IS NULL AND runtime_sealed_at IS NULL) "
        "OR (drain_claim IS NOT NULL AND drain_started_at IS NOT NULL "
        "AND ((runtime_seal_claim IS NULL AND runtime_sealed_at IS NULL) "
        "OR (runtime_seal_claim = drain_claim "
        "AND runtime_sealed_at IS NOT NULL)))"
    ),
}
_GATE_SQL = (
    "drain_claim IS NULL AND runtime_seal_claim IS NULL AND "
    "active_login_attempt_id IS NULL AND active_login_kind IS NULL AND "
    "drain_started_at IS NULL AND runtime_sealed_at IS NULL AND "
    "updated_at IS NULL"
)
_COLUMN_SPECS = {
    "id": (sa.Integer, None, False),
    "drain_claim": (sa.String, 64, True),
    "runtime_seal_claim": (sa.String, 64, True),
    "active_login_attempt_id": (sa.String, 64, True),
    "active_login_kind": (sa.String, 32, True),
    "drain_started_at": (sa.DateTime, None, True),
    "runtime_sealed_at": (sa.DateTime, None, True),
    "updated_at": (sa.DateTime, None, True),
}
_POSTGRESQL_CHECK_PROBE = "ccm_f2_worker_node_check_probe"


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
    if not _is_mysql_family():
        return
    if _is_offline():
        raise RuntimeError(
            "Worker node control migration refuses MySQL/MariaDB offline SQL: "
            "table shape, CHECK enforcement, and InnoDB cannot be proven"
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
            "Worker node control migration requires " + product
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
                "Worker node control migration could not acquire its SQLite "
                "revision writer fence"
            )
    elif dialect == "postgresql" and downgrade:
        # PostgreSQL DDL is transactional, so a normal DROP cannot commit
        # without Alembic's revision update.  Keep the absent-table branch
        # harmless nevertheless: it is useful for direct/manual crash replay
        # and, more importantly, avoids trying to LOCK a table which an
        # operator has already removed while repairing a failed deployment.
        if sa.inspect(bind).has_table(_TABLE):
            op.execute(
                sa.text(
                    "LOCK TABLE worker_node_controls "
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
        for index, char in enumerate(expression):
            if char == "'":
                quoted = not quoted
            elif not quoted:
                if char == "(":
                    depth += 1
                elif char == ")":
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
        char = expression[index]
        if char == "'":
            quoted = not quoted
            index += 1
            continue
        if not quoted:
            if char == "(":
                depth += 1
            elif char == ")":
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
                f"Worker node control CHECK {name} is not validated"
            )
        definitions[name] = str(definition)
    return definitions


def _postgresql_canonical_checks() -> dict[str, str]:
    """Compare CHECKs in PostgreSQL's own cast-normalized representation."""

    expected = {**_CHECKS, _DOWNGRADE_GATE: _GATE_SQL}
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
            "Worker node control PostgreSQL CHECK probe is incomplete"
        )
    for name, definition in actual.items():
        expected_definition = canonical.get(name)
        if expected_definition is None or _normalized_sql(
            definition
        ) != _normalized_sql(expected_definition):
            raise RuntimeError(
                f"Worker node control CHECK {name} is malformed"
            )
    return actual


def _type_matches(
    reflected: sa.types.TypeEngine,
    expected: type[sa.types.TypeEngine],
    length: int | None = None,
) -> bool:
    if expected is sa.Integer:
        return type(reflected).__name__.upper() == "INTEGER"
    return isinstance(reflected, expected) and (
        length is None or getattr(reflected, "length", None) == length
    )


def _mysql_enforced_checks() -> set[str]:
    # MariaDB 10.6 enforces CHECK constraints but does not expose MySQL's
    # TABLE_CONSTRAINTS.ENFORCED column.  The minimum-version gate above is
    # therefore its enforcement proof; reflection still supplies the names.
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


def _table_state(*, allow_gate: bool = False) -> str:
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table(_TABLE):
        return "absent"
    columns = {
        str(column["name"]).lower(): column
        for column in inspector.get_columns(_TABLE)
    }
    if set(columns) != set(_COLUMN_SPECS):
        raise RuntimeError("Worker node control table has a foreign column set")
    for name, (kind, length, nullable) in _COLUMN_SPECS.items():
        column = columns[name]
        if (
            not _type_matches(column["type"], kind, length)
            or bool(column["nullable"]) is not nullable
            or column.get("default") is not None
        ):
            raise RuntimeError(
                f"Worker node control column {name} has a foreign shape"
            )
    primary_key = tuple(
        str(column).lower()
        for column in (
            inspector.get_pk_constraint(_TABLE).get("constrained_columns")
            or ()
        )
    )
    if primary_key != ("id",):
        raise RuntimeError("Worker node control primary key is malformed")
    if inspector.get_unique_constraints(_TABLE):
        raise RuntimeError("Worker node control has unexpected UNIQUE constraints")
    if inspector.get_foreign_keys(_TABLE):
        raise RuntimeError("Worker node control has unexpected foreign keys")
    if op.get_bind().dialect.name == "postgresql":
        checks = _postgresql_canonical_checks()
    else:
        checks = {
            str(item.get("name") or "").lower(): item.get("sqltext")
            for item in inspector.get_check_constraints(_TABLE)
        }
    allowed = set(_CHECKS)
    if allow_gate:
        allowed.add(_DOWNGRADE_GATE)
    if set(checks) not in (set(_CHECKS), allowed):
        raise RuntimeError("Worker node control CHECK set is malformed")
    if op.get_bind().dialect.name != "postgresql":
        for name, expected in _CHECKS.items():
            if _check_shape(checks[name]) != _check_shape(expected):
                raise RuntimeError(
                    f"Worker node control CHECK {name} is malformed"
                )
        if _DOWNGRADE_GATE in checks and _check_shape(
            checks[_DOWNGRADE_GATE]
        ) != _check_shape(_GATE_SQL):
            raise RuntimeError("Worker node control downgrade gate is malformed")
    if _is_mysql_family():
        if not set(checks) <= _mysql_enforced_checks():
            raise RuntimeError(
                "Worker node control CHECK constraints are not enforced"
            )
        engines = op.get_bind().execute(
            sa.text(
                "SELECT ENGINE FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table"
            ),
            {"table": _TABLE},
        ).scalars().all()
        if len(engines) != 1 or str(engines[0]).lower() != "innodb":
            raise RuntimeError("Worker node control table must use InnoDB")
    return "canonical_gated" if _DOWNGRADE_GATE in checks else "canonical"


def _create_table() -> None:
    op.create_table(
        _TABLE,
        sa.Column("id", sa.Integer(), autoincrement=False, nullable=False),
        sa.Column("drain_claim", sa.String(length=64), nullable=True),
        sa.Column("runtime_seal_claim", sa.String(length=64), nullable=True),
        sa.Column("active_login_attempt_id", sa.String(length=64), nullable=True),
        sa.Column("active_login_kind", sa.String(length=32), nullable=True),
        sa.Column("drain_started_at", sa.DateTime(), nullable=True),
        sa.Column("runtime_sealed_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        *(
            sa.CheckConstraint(expression, name=name)
            for name, expression in _CHECKS.items()
        ),
        sa.PrimaryKeyConstraint("id"),
        mysql_engine="InnoDB",
    )


def _control_table() -> sa.TableClause:
    return sa.table(
        _TABLE,
        sa.column("id", sa.Integer()),
        sa.column("drain_claim", sa.String(length=64)),
        sa.column("runtime_seal_claim", sa.String(length=64)),
        sa.column("active_login_attempt_id", sa.String(length=64)),
        sa.column("active_login_kind", sa.String(length=32)),
        sa.column("drain_started_at", sa.DateTime()),
        sa.column("runtime_sealed_at", sa.DateTime()),
        sa.column("updated_at", sa.DateTime()),
    )


def _seed_or_validate_singleton() -> tuple[object, ...]:
    control = _control_table()
    rows = op.get_bind().execute(
        sa.select(*control.c).order_by(control.c.id)
    ).all()
    if not rows:
        op.get_bind().execute(
            control.insert().values(
                id=1,
                drain_claim=None,
                runtime_seal_claim=None,
                active_login_attempt_id=None,
                active_login_kind=None,
                drain_started_at=None,
                runtime_sealed_at=None,
                updated_at=None,
            )
        )
        rows = op.get_bind().execute(sa.select(*control.c)).all()
    if len(rows) != 1 or rows[0][0] != 1:
        raise RuntimeError("Worker node control singleton row is malformed")
    return tuple(rows[0])


def _assert_downgrade_safe() -> None:
    row = _seed_or_validate_singleton()
    # ``updated_at`` is included deliberately: every admitted Worker mutation
    # writes it, so dropping this protocol after first use would reopen the
    # destroy race even if no drain/login happens to be active at this instant.
    if any(value is not None for value in row[1:]):
        raise RuntimeError(
            "Cannot downgrade while Worker node drain/login evidence exists "
            "or the control row records a prior mutation"
        )


def upgrade() -> None:
    _require_supported_mysql_family()
    if _is_offline():
        _create_table()
        op.bulk_insert(
            _control_table(),
            [{
                "id": 1,
                "drain_claim": None,
                "runtime_seal_claim": None,
                "active_login_attempt_id": None,
                "active_login_kind": None,
                "drain_started_at": None,
                "runtime_sealed_at": None,
                "updated_at": None,
            }],
        )
        return
    _acquire_transactional_fence(downgrade=False)
    state = _table_state(allow_gate=True)
    if state == "canonical_gated":
        raise RuntimeError(
            "Worker node control upgrade found a downgrade-only gate"
        )
    if state == "absent":
        _create_table()
    if _table_state() != "canonical":
        raise RuntimeError("Worker node control table creation is incomplete")
    _seed_or_validate_singleton()


def downgrade() -> None:
    _require_supported_mysql_family()
    if _is_offline():
        raise RuntimeError(
            "Worker node control offline downgrade is refused because "
            "durable drain evidence cannot be inspected"
        )
    _acquire_transactional_fence(downgrade=True)
    state = _table_state(allow_gate=True)
    if state == "absent":
        return
    _assert_downgrade_safe()
    if _is_mysql_family() and state == "canonical":
        op.create_check_constraint(_DOWNGRADE_GATE, _TABLE, _GATE_SQL)
        state = _table_state(allow_gate=True)
    if _is_mysql_family() and state != "canonical_gated":
        raise RuntimeError("Worker node control downgrade gate was not installed")
    _assert_downgrade_safe()
    op.drop_table(_TABLE)
    if _table_state(allow_gate=True) != "absent":
        raise RuntimeError("Worker node control table removal is incomplete")
