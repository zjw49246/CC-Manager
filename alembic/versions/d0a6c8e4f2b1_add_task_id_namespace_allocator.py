"""add durable Manager/Worker Task-id namespace allocator

Revision ID: d0a6c8e4f2b1
Revises: c9f5b1e7d402
Create Date: 2026-08-14

The singleton table is a protocol boundary, not disposable cache.  This
revision therefore accepts only an absent or exact canonical table, preserves
an already-seeded row after a CREATE/stamp crash, and installs a temporary
MySQL-family downgrade gate before dropping the table.
"""

import re
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op


revision: str = "d0a6c8e4f2b1"
down_revision: Union[str, None] = "c9f5b1e7d402"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_TABLE = "task_id_allocators"
_SINGLETON_ID = 1
_WORKER_NAMESPACE_START = 1_000_000_000
_SIGNED_INT_MAX = 2_147_483_647
_DOWNGRADE_GATE = "ck_task_id_allocators_no_downgrade_use"
_CHECKS = {
    "ck_task_id_allocators_singleton": "id = 1",
    "ck_task_id_allocators_node_role": (
        "node_role IS NULL OR node_role IN ('manager', 'worker')"
    ),
    "ck_task_id_allocators_worker_range": (
        "next_worker_task_id >= 1000000000 "
        "AND next_worker_task_id <= 2147483647"
    ),
}
_GATE_SQL = (
    "node_role IS NULL AND next_worker_task_id = 1000000000"
)
_COLUMN_SPECS = {
    "id": (sa.Integer, None, False, None),
    "node_role": (sa.String, 16, True, None),
    "next_worker_task_id": (
        sa.Integer,
        None,
        False,
        str(_WORKER_NAMESPACE_START),
    ),
}
_POSTGRESQL_CHECK_PROBE = "ccm_d0_task_id_check_probe"


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
            "Task-id namespace migration refuses MySQL/MariaDB offline SQL: "
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
            "Task-id namespace migration requires " + product
            + " with enforced CHECK constraints and atomic DDL"
        )


def _acquire_transactional_fence(*, downgrade: bool) -> None:
    if _is_offline():
        return
    dialect = op.get_bind().dialect.name
    if dialect == "sqlite":
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
                "Task-id namespace migration could not acquire its SQLite "
                "revision writer fence"
            )
    elif dialect == "postgresql" and downgrade:
        # PostgreSQL normally rolls DDL and the Alembic stamp back together,
        # but keep direct/operator replay of an already committed DROP safe.
        # An absent allocator has no remaining protocol state to fence.
        if sa.inspect(op.get_bind()).has_table(_TABLE):
            op.execute(
                sa.text(
                    "LOCK TABLE task_id_allocators, tasks "
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
                f"Task-id allocator CHECK {name} is not validated"
            )
        definitions[name] = str(definition)
    return definitions


def _postgresql_canonical_checks() -> dict[str, str]:
    """Compare CHECKs after PostgreSQL has canonicalized casts and ``IN``."""

    expected = {**_CHECKS, _DOWNGRADE_GATE: _GATE_SQL}
    probe_columns = []
    for name, (kind, length, nullable, default) in _COLUMN_SPECS.items():
        type_ = kind(length=length) if kind is sa.String else kind()
        probe_columns.append(
            sa.Column(
                name,
                type_,
                nullable=nullable,
                server_default=default,
            )
        )
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
        raise RuntimeError("Task-id allocator PostgreSQL CHECK probe is incomplete")
    for name, definition in actual.items():
        expected_definition = canonical.get(name)
        if expected_definition is None or _normalized_sql(
            definition
        ) != _normalized_sql(expected_definition):
            raise RuntimeError(f"Task-id allocator CHECK {name} is malformed")
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


def _normalized_default(value: object) -> str | None:
    if value is None:
        return None
    default = _strip_outer_parentheses(str(value).strip().lower())
    if len(default) >= 2 and default[0] == default[-1] == "'":
        default = default[1:-1]
    return default


def _table_state(*, allow_gate: bool = False) -> str:
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table(_TABLE):
        return "absent"
    columns = {
        str(column["name"]).lower(): column
        for column in inspector.get_columns(_TABLE)
    }
    if set(columns) != {"id", "node_role", "next_worker_task_id"}:
        raise RuntimeError("Task-id allocator table has a foreign column set")
    for name, (kind, length, nullable, default) in _COLUMN_SPECS.items():
        column = columns[name]
        if (
            not _type_matches(column["type"], kind, length)
            or bool(column["nullable"]) is not nullable
            or _normalized_default(column.get("default")) != default
        ):
            raise RuntimeError(
                f"Task-id allocator column {name} has a foreign shape"
            )
    primary_key = tuple(
        str(column).lower()
        for column in (
            inspector.get_pk_constraint(_TABLE).get("constrained_columns")
            or ()
        )
    )
    if primary_key != ("id",):
        raise RuntimeError("Task-id allocator primary key is malformed")
    if inspector.get_unique_constraints(_TABLE):
        raise RuntimeError("Task-id allocator has unexpected UNIQUE constraints")
    if inspector.get_foreign_keys(_TABLE):
        raise RuntimeError("Task-id allocator has unexpected foreign keys")
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
        raise RuntimeError("Task-id allocator CHECK set is malformed")
    if op.get_bind().dialect.name != "postgresql":
        for name, expected in _CHECKS.items():
            if _check_shape(checks[name]) != _check_shape(expected):
                raise RuntimeError(f"Task-id allocator CHECK {name} is malformed")
        if _DOWNGRADE_GATE in checks and _check_shape(
            checks[_DOWNGRADE_GATE]
        ) != _check_shape(_GATE_SQL):
            raise RuntimeError("Task-id allocator downgrade gate is malformed")
    if _is_mysql_family():
        engines = op.get_bind().execute(
            sa.text(
                "SELECT ENGINE FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table"
            ),
            {"table": _TABLE},
        ).scalars().all()
        if len(engines) != 1 or str(engines[0]).lower() != "innodb":
            raise RuntimeError("Task-id allocator table must use InnoDB")
    return "canonical_gated" if _DOWNGRADE_GATE in checks else "canonical"


def _create_table() -> None:
    op.create_table(
        _TABLE,
        sa.Column("id", sa.Integer(), autoincrement=False, nullable=False),
        sa.Column("node_role", sa.String(length=16), nullable=True),
        sa.Column(
            "next_worker_task_id",
            sa.Integer(),
            server_default=sa.text(str(_WORKER_NAMESPACE_START)),
            nullable=False,
        ),
        *(
            sa.CheckConstraint(expression, name=name)
            for name, expression in _CHECKS.items()
        ),
        sa.PrimaryKeyConstraint("id"),
        mysql_engine="InnoDB",
    )


def _seed_or_validate_singleton() -> None:
    table = sa.table(
        _TABLE,
        sa.column("id", sa.Integer()),
        sa.column("node_role", sa.String(length=16)),
        sa.column("next_worker_task_id", sa.Integer()),
    )
    rows = op.get_bind().execute(
        sa.select(
            table.c.id,
            table.c.node_role,
            table.c.next_worker_task_id,
        ).order_by(table.c.id)
    ).all()
    if not rows:
        op.get_bind().execute(
            table.insert().values(
                id=_SINGLETON_ID,
                node_role=None,
                next_worker_task_id=_WORKER_NAMESPACE_START,
            )
        )
        rows = op.get_bind().execute(
            sa.select(
                table.c.id,
                table.c.node_role,
                table.c.next_worker_task_id,
            )
        ).all()
    if len(rows) != 1 or rows[0][0] != _SINGLETON_ID:
        raise RuntimeError("Task-id allocator singleton row is malformed")


def _assert_downgrade_safe() -> None:
    allocator = sa.table(
        _TABLE,
        sa.column("id", sa.Integer()),
        sa.column("node_role", sa.String(length=16)),
        sa.column("next_worker_task_id", sa.Integer()),
    )
    tasks = sa.table("tasks", sa.column("id", sa.Integer()))
    row = op.get_bind().execute(
        sa.select(
            allocator.c.node_role,
            allocator.c.next_worker_task_id,
        ).where(allocator.c.id == _SINGLETON_ID)
    ).one_or_none()
    if row is None:
        raise RuntimeError("Cannot downgrade: Task-id allocator singleton is missing")
    if row[0] in {"manager", "worker"}:
        raise RuntimeError(
            "Cannot downgrade a database after Task-id namespace binding; "
            "older code cannot preserve the Manager/Worker allocation split"
        )
    if row[0] is not None or row[1] != _WORKER_NAMESPACE_START:
        raise RuntimeError("Cannot downgrade: Task-id allocator state is corrupt")
    high_task_id = op.get_bind().execute(
        sa.select(tasks.c.id)
        .where(tasks.c.id >= _WORKER_NAMESPACE_START)
        .limit(1)
    ).scalar_one_or_none()
    if high_task_id is not None:
        raise RuntimeError(
            "Cannot downgrade while Worker-local high-range Tasks exist"
        )


def upgrade() -> None:
    _require_supported_mysql_family()
    if _is_offline():
        _create_table()
        op.bulk_insert(
            sa.table(
                _TABLE,
                sa.column("id", sa.Integer()),
                sa.column("node_role", sa.String(length=16)),
                sa.column("next_worker_task_id", sa.Integer()),
            ),
            [{
                "id": _SINGLETON_ID,
                "node_role": None,
                "next_worker_task_id": _WORKER_NAMESPACE_START,
            }],
        )
        return
    _acquire_transactional_fence(downgrade=False)
    state = _table_state(allow_gate=True)
    if state == "canonical_gated":
        raise RuntimeError(
            "Task-id namespace upgrade found a downgrade-only gate"
        )
    if state == "absent":
        _create_table()
    if _table_state() != "canonical":
        raise RuntimeError("Task-id allocator table creation is incomplete")
    _seed_or_validate_singleton()


def downgrade() -> None:
    _require_supported_mysql_family()
    if _is_offline():
        raise RuntimeError(
            "Task-id namespace offline downgrade is refused because durable "
            "allocator evidence cannot be inspected"
        )
    _acquire_transactional_fence(downgrade=True)
    state = _table_state(allow_gate=True)
    if state == "absent":
        return
    _seed_or_validate_singleton()
    _assert_downgrade_safe()
    if _is_mysql_family() and state == "canonical":
        op.create_check_constraint(_DOWNGRADE_GATE, _TABLE, _GATE_SQL)
        state = _table_state(allow_gate=True)
    if _is_mysql_family() and state != "canonical_gated":
        raise RuntimeError("Task-id allocator downgrade gate was not installed")
    _assert_downgrade_safe()
    op.drop_table(_TABLE)
    if _table_state(allow_gate=True) != "absent":
        raise RuntimeError("Task-id allocator table removal is incomplete")
